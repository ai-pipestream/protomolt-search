//! The layer searches: the plain beam of the paper and the filter-aware
//! beam at layer 0.

use crate::vector::Mask;

use super::beam::{Beam, Gate, LayerSearch};
use super::{FloorPrune, Hook, LinkCells, Patience, Scored, Scratch, SearchStats, View, Walk};

/// One filter-aware search at layer 0.
#[derive(Clone, Copy)]
pub(super) struct AwareSearch<'a> {
    pub(super) q: &'a [f32],
    pub(super) ef: usize,
    pub(super) allow: Mask<'a>,
    /// Layer-0 work units (`scored + bridges`) the search may spend.
    pub(super) visit_budget: Option<u64>,
    pub(super) patience: Patience,
    pub(super) floor_prune: FloorPrune,
}

/// The neighbor buffers of one filter-aware expansion.
struct AwareBuffers {
    nbrs: Vec<u32>,
    /// The disallowed neighbors of the node being expanded.
    routes: Vec<u32>,
    /// The allowed rows about to be scored.
    fresh: Vec<u32>,
}

impl AwareBuffers {
    fn take(scratch: &mut Scratch) -> Self {
        Self {
            nbrs: std::mem::take(&mut scratch.nbrs),
            routes: std::mem::take(&mut scratch.routes),
            fresh: std::mem::take(&mut scratch.fresh),
        }
    }

    fn restore(self, scratch: &mut Scratch) {
        scratch.nbrs = self.nbrs;
        scratch.routes = self.routes;
        scratch.fresh = self.fresh;
    }
}

#[inline]
fn walk_work_units(stats: &SearchStats, base_scored: u64) -> u64 {
    stats.scored.saturating_sub(base_scored) + stats.bridges
}

impl<L: LinkCells + ?Sized> View<'_, L> {
    /// Paper Algorithm 2 at one layer. Leaves up to `ef` results (allowed
    /// nodes only) in `scratch.res`. Disallowed nodes route: they enter the
    /// candidate heap and are expanded, but never the results.
    pub(in crate::hnsw) fn search_layer<H: Hook>(
        &self,
        search: &LayerSearch<'_>,
        entries: &[Scored],
        scratch: &mut Scratch,
        hook: &mut H,
        stats: &mut SearchStats,
    ) -> Walk {
        let ef = search.ef.max(1);
        scratch.begin(self.n());
        let prune = FloorPrune {
            enabled: search.layer == 0 && search.floor_prune.enabled,
            ..search.floor_prune
        };
        let mut beam = Beam::new(ef, search.patience, prune, false, scratch);
        self.seed_beam(&mut beam, entries, search.allow, scratch, hook);
        let mut nbrs = std::mem::take(&mut scratch.nbrs);
        let mut outcome = Walk::Completed;
        while let Some(c) = scratch.cand.pop() {
            // The next candidate's list is read on the next hop: fetch it
            // while this one is scored.
            if let Some(next) = scratch.cand.peek() {
                self.prefetch_links(next.node, search.layer);
            }
            match self.gate(&mut beam, c, scratch, hook) {
                Gate::Expand => {}
                Gate::Skip => continue,
                Gate::End(walk) => {
                    outcome = walk;
                    break;
                }
            }
            beam.begin_hop(scratch, hook);
            stats.hops += 1;
            self.copy_links(c.node, search.layer, search.locks, &mut nbrs);
            nbrs.retain(|&v| scratch.visit(v));
            stats.scored += nbrs.len() as u64;
            self.for_each_scored(search.q, &nbrs, |v, s| {
                let allowed = search.allow.is_none_or(|a| a.test(v as usize));
                let scored = Scored { score: s, node: v };
                self.offer(&mut beam, scored, allowed, scratch, hook);
            });
            beam.end_hop();
            if !hook.hop() {
                outcome = Walk::Stopped;
                break;
            }
        }
        scratch.nbrs = nbrs;
        beam.finish(outcome, scratch)
    }

    /// The filter-aware beam at layer 0 (the predicate-subgraph idea of
    /// ACORN-1, Patel et al., arXiv:2403.04871). Only allowed rows are
    /// scored and only they enter the candidate and result heaps, so the
    /// walk's cost follows the rows it can return. Expanding a node scores
    /// its unvisited allowed neighbors; each disallowed neighbor is a
    /// bridge, and its own allowed neighbors are scored in its place, until
    /// the expansion has scored one full neighbor list (`m0` rows). A
    /// bridge is marked visited only when it is looked through, so one cut
    /// off by the budget can still be used from another node.
    ///
    /// `entries` are already scored and may be disallowed (the descent's
    /// landing point): they are expanded but not returned.
    pub(super) fn search_layer_aware<H: Hook>(
        &self,
        search: &AwareSearch<'_>,
        entries: &[Scored],
        scratch: &mut Scratch,
        hook: &mut H,
        stats: &mut SearchStats,
    ) -> Walk {
        let ef = search.ef.max(1);
        scratch.begin(self.n());
        let mut beam = Beam::new(ef, search.patience, search.floor_prune, true, scratch);
        self.seed_beam(&mut beam, entries, Some(search.allow), scratch, hook);
        let mut buffers = AwareBuffers::take(scratch);
        let base_scored = stats.scored;
        let mut outcome = Walk::Completed;
        while let Some(c) = scratch.cand.pop() {
            if let Some(next) = scratch.cand.peek() {
                self.prefetch_links(next.node, 0);
            }
            match self.gate(&mut beam, c, scratch, hook) {
                Gate::Expand => {}
                Gate::Skip => continue,
                Gate::End(walk) => {
                    outcome = walk;
                    break;
                }
            }
            let spent = walk_work_units(stats, base_scored);
            if search.visit_budget.is_some_and(|limit| spent >= limit) {
                outcome = Walk::Budget;
                break;
            }
            beam.begin_hop(scratch, hook);
            stats.hops += 1;
            self.gather_allowed(c.node, search.allow, &mut buffers, scratch, stats);
            stats.scored += buffers.fresh.len() as u64;
            self.for_each_scored(search.q, &buffers.fresh, |v, s| {
                let scored = Scored { score: s, node: v };
                self.offer(&mut beam, scored, true, scratch, hook);
            });
            beam.end_hop();
            if !hook.hop() {
                outcome = Walk::Stopped;
                break;
            }
        }
        buffers.restore(scratch);
        beam.finish(outcome, scratch)
    }

    /// Collect in `buffers.fresh` the unvisited allowed neighbors of `node`,
    /// then look through its unseen disallowed neighbors for their allowed
    /// ones until the expansion holds one full neighbor list (`m0` rows).
    fn gather_allowed(
        &self,
        node: u32,
        allow: Mask<'_>,
        buffers: &mut AwareBuffers,
        scratch: &mut Scratch,
        stats: &mut SearchStats,
    ) {
        let AwareBuffers {
            nbrs,
            routes,
            fresh,
        } = buffers;
        let hop_budget = self.m0;
        self.copy_links(node, 0, None, nbrs);
        routes.clear();
        fresh.clear();
        for &v in nbrs.iter() {
            if allow.test(v as usize) {
                if scratch.visit(v) {
                    fresh.push(v);
                }
            } else if !scratch.seen(v) {
                routes.push(v);
            }
        }
        for &bridge in routes.iter() {
            if fresh.len() >= hop_budget {
                break;
            }
            if !scratch.visit(bridge) {
                continue;
            }
            stats.bridges += 1;
            self.copy_links(bridge, 0, None, nbrs);
            for &w in nbrs.iter() {
                if fresh.len() >= hop_budget {
                    break;
                }
                if allow.test(w as usize) && scratch.visit(w) {
                    fresh.push(w);
                }
            }
        }
    }
}
