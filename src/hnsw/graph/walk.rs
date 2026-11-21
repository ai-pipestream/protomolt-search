//! The query walk: a greedy descent through the upper layers, then a beam
//! at layer 0.

use super::beam::LayerSearch;
use super::search_layer::AwareSearch;
use super::{
    Filter, Hook, LinkCells, Route, Scored, Scratch, SearchStats, Steering, View, Walk, NO_NODE,
};
use crate::vector::Mask;

/// The fields of [`Filter::Aware`].
#[derive(Clone, Copy)]
struct AwareFilter<'a> {
    allow: Mask<'a>,
    allowed: usize,
    route: Option<Route>,
    budget: Option<u64>,
}

impl<L: LinkCells + ?Sized> View<'_, L> {
    /// Paper Algorithm 5: descend greedily from the entry to layer 1, then
    /// a beam of `ef` at layer 0, steered by `filter`, `patience`, and
    /// `floor_prune`. Results best first.
    pub fn walk<H: Hook>(
        &self,
        q: &[f32],
        ef: usize,
        steering: Steering<'_>,
        scratch: &mut Scratch,
        hook: &mut H,
    ) -> (Vec<Scored>, SearchStats, Walk) {
        let Steering {
            filter,
            patience,
            floor_prune,
        } = steering;
        let mut stats = SearchStats::default();
        if self.entry == NO_NODE {
            return (Vec::new(), stats, Walk::Completed);
        }
        let ep = self.descend(q, scratch, &mut stats);
        scratch.walked_plain = false;
        let mut search = LayerSearch {
            q,
            ef,
            layer: 0,
            allow: None,
            patience,
            floor_prune,
            locks: None,
        };
        let walk = match filter {
            Filter::None => self.search_layer(&search, &[ep], scratch, hook, &mut stats),
            Filter::Plain(allow) => {
                search.allow = Some(allow);
                self.search_layer(&search, &[ep], scratch, hook, &mut stats)
            }
            Filter::Aware {
                allow,
                allowed,
                route,
                budget,
            } => {
                let aware = AwareFilter {
                    allow,
                    allowed,
                    route,
                    budget,
                };
                return self.walk_aware(&search, aware, ep, scratch, hook, stats);
            }
        };
        (scratch.drain_results(), stats, walk)
    }

    /// Score the entry and descend greedily to its best neighbor at layer 1.
    fn descend(&self, q: &[f32], scratch: &mut Scratch, stats: &mut SearchStats) -> Scored {
        let mut ep = Scored {
            score: self.kernel.dot(q, self.row(self.entry)),
            node: self.entry,
        };
        stats.scored += 1;
        let mut nbrs = std::mem::take(&mut scratch.nbrs);
        for layer in (1..=self.top as usize).rev() {
            ep = self.greedy(q, ep, layer, None, &mut nbrs, stats);
        }
        scratch.nbrs = nbrs;
        ep
    }

    /// The layer-0 walk under [`Filter::Aware`] from the descent's landing
    /// point `ep`. With a route the far probe first decides between giving
    /// the query up ([`Walk::Far`]), walking plain, and walking
    /// filter-aware; a filter-aware walk that completes with the beam
    /// unfilled although the mask allows more rows reports [`Walk::Short`].
    fn walk_aware<H: Hook>(
        &self,
        search: &LayerSearch<'_>,
        aware: AwareFilter<'_>,
        ep: Scored,
        scratch: &mut Scratch,
        hook: &mut H,
        mut stats: SearchStats,
    ) -> (Vec<Scored>, SearchStats, Walk) {
        if let Some(route) = aware.route {
            let probe = self.probe(ep.node, aware.allow, aware.allowed, scratch, &mut stats);
            if probe.far() {
                return (Vec::new(), stats, Walk::Far);
            }
            if probe.local_share() >= route.plain_share {
                scratch.walked_plain = true;
                let plain = LayerSearch {
                    allow: Some(aware.allow),
                    ..*search
                };
                let walk = self.search_layer(&plain, &[ep], scratch, hook, &mut stats);
                return (scratch.drain_results(), stats, walk);
            }
            if aware.allowed <= route.enumerate_rows {
                return (Vec::new(), stats, Walk::Far);
            }
        }
        let adaptive = aware.route.is_some();
        let request = AwareSearch {
            q: search.q,
            ef: search.ef,
            allow: aware.allow,
            visit_budget: aware.budget,
            patience: search.patience,
            floor_prune: search.floor_prune,
        };
        let outcome = self.search_layer_aware(&request, &[ep], scratch, hook, &mut stats);
        if outcome == Walk::Completed
            && adaptive
            && scratch.res.len() < search.ef.max(1).min(aware.allowed)
        {
            return (scratch.drain_results(), stats, Walk::Short);
        }
        (scratch.drain_results(), stats, outcome)
    }
}
