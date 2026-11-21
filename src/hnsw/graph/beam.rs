//! One layer's beam: the request, the floor gate, and the patience window
//! the plain and the filter-aware layer searches share.

use crate::vector::Mask;

use super::ranked::{Kept, Results};
use super::{FloorPrune, Hook, LinkCells, Locks, Patience, Scored, Scratch, View, Walk};

/// One layer search: the query, the beam, and what steers it.
#[derive(Clone, Copy)]
pub(in crate::hnsw) struct LayerSearch<'a> {
    pub(in crate::hnsw) q: &'a [f32],
    pub(in crate::hnsw) ef: usize,
    pub(in crate::hnsw) layer: usize,
    /// Rows that may enter the results; `None` allows every row.
    pub(in crate::hnsw) allow: Option<Mask<'a>>,
    pub(in crate::hnsw) patience: Patience,
    pub(in crate::hnsw) floor_prune: FloorPrune,
    /// The per-node locks while a parallel build writes the links.
    pub(in crate::hnsw) locks: Option<&'a Locks>,
}

/// Hop-to-hop saturation of the tracked window: the full beam, or a `top_k`
/// window narrower than it.
struct Saturation {
    patience: Patience,
    use_top_k: bool,
    window: usize,
    min_survived: usize,
    /// Nodes that entered the tracked window during the current expansion.
    added: Vec<u32>,
    top: Results,
    consecutive: u32,
    /// The current expansion counts toward patience.
    counting: bool,
    /// Members the tracked window lost during the current expansion.
    lost: usize,
}

impl Saturation {
    fn new(patience: Patience, ef: usize, scratch: &mut Scratch) -> Self {
        let use_top_k = patience.enabled() && patience.top_k > 0 && patience.top_k < ef;
        let window = if use_top_k { patience.top_k } else { ef };
        let mut added = std::mem::take(&mut scratch.patience_added);
        let mut top = std::mem::take(&mut scratch.patience_top);
        added.clear();
        top.clear();
        Self {
            patience,
            use_top_k,
            window,
            min_survived: patience.min_survived(window),
            added,
            top,
            consecutive: 0,
            counting: false,
            lost: 0,
        }
    }

    /// Offer an entry point to the `top_k` window.
    #[inline]
    fn seed(&mut self, entry: Scored) {
        if self.use_top_k {
            self.top.keep(self.window, entry);
        }
    }

    #[inline]
    fn exhausted(&self) -> bool {
        self.patience.enabled() && self.consecutive >= self.patience.hops
    }

    /// Open an expansion and decide whether it counts toward patience.
    #[inline]
    fn begin_hop(&mut self, beam_full: bool, floor_live: bool, allowed_scored: usize) {
        self.counting = self.patience.enabled()
            && beam_full
            && (!floor_live || allowed_scored >= self.patience.ascent_gate)
            && (!self.use_top_k || self.top.len() >= self.window);
        if self.counting {
            self.added.clear();
        }
        self.lost = 0;
    }

    /// Keep `scored`, which [`View::offer`] let past the beam's worst, in
    /// the result beam of `ef` and offer it to the tracked window, counting
    /// the window members it displaced.
    #[inline]
    fn admit(&mut self, res: &mut Results, ef: usize, scored: Scored) {
        let kept = res.keep(ef, scored);
        debug_assert!(kept.entered(), "the beam turned an admitted row away");
        if let Kept::Replaced(evicted) = kept {
            if !self.use_top_k {
                self.displace(evicted, scored);
            }
        }
        if self.use_top_k {
            if let Kept::Replaced(evicted) = self.top.keep(self.window, scored) {
                self.displace(evicted, scored);
            }
        }
    }

    /// `scored` took `evicted`'s place in the tracked window: while the
    /// expansion counts, a member that was there when it opened is lost.
    #[inline]
    fn displace(&mut self, evicted: Scored, scored: Scored) {
        if self.counting {
            if !self.added.contains(&evicted.node) {
                self.lost += 1;
            }
            self.added.push(scored.node);
        }
    }

    #[inline]
    fn end_hop(&mut self) {
        if !self.patience.enabled() {
            return;
        }
        if self.counting && self.window.saturating_sub(self.lost) >= self.min_survived {
            self.consecutive += 1;
        } else {
            self.consecutive = 0;
        }
    }
}

/// What the beam does with its best pending candidate.
pub(super) enum Gate {
    Expand,
    /// Pruned against the floor: take the next candidate.
    Skip,
    /// The search is over with this outcome.
    End(Walk),
}

/// The running state of one layer search.
pub(super) struct Beam {
    ef: usize,
    prune: FloorPrune,
    /// Record every allowed row scored in `Scratch::scored_allowed`.
    record: bool,
    allowed_scored: usize,
    pruned_by_floor: bool,
    saturation: Saturation,
}

impl Beam {
    /// A beam of `ef` over a scratch that [`Scratch::begin`] has reset.
    pub(super) fn new(
        ef: usize,
        patience: Patience,
        prune: FloorPrune,
        record: bool,
        scratch: &mut Scratch,
    ) -> Self {
        Self {
            ef,
            prune,
            record,
            allowed_scored: 0,
            pruned_by_floor: false,
            saturation: Saturation::new(patience, ef, scratch),
        }
    }

    /// Open the expansion of a candidate [`View::gate`] let through.
    #[inline]
    pub(super) fn begin_hop<H: Hook>(&mut self, scratch: &Scratch, hook: &H) {
        let floor_live = self.saturation.patience.floor_active || hook.current_floor().is_finite();
        let beam_full = scratch.res.len() >= self.ef;
        self.saturation
            .begin_hop(beam_full, floor_live, self.allowed_scored);
    }

    #[inline]
    pub(super) fn end_hop(&mut self) {
        self.saturation.end_hop();
    }

    /// Hand the patience buffers back to `scratch` and settle the outcome:
    /// a completed search that pruned against the floor reports it.
    pub(super) fn finish(self, outcome: Walk, scratch: &mut Scratch) -> Walk {
        scratch.patience_added = self.saturation.added;
        scratch.patience_top = self.saturation.top;
        if outcome == Walk::Completed && self.pruned_by_floor {
            Walk::FloorPruned
        } else {
            outcome
        }
    }
}

impl<L: LinkCells + ?Sized> View<'_, L> {
    /// Seed the heaps with the already scored `entries`. Every entry is a
    /// candidate; only the ones `allow` admits are reported and kept.
    pub(super) fn seed_beam<H: Hook>(
        &self,
        beam: &mut Beam,
        entries: &[Scored],
        allow: Option<Mask<'_>>,
        scratch: &mut Scratch,
        hook: &mut H,
    ) {
        for &e in entries {
            if !scratch.visit(e.node) {
                continue;
            }
            scratch.cand.push(e);
            if allow.is_none_or(|a| a.test(e.node as usize)) {
                beam.allowed_scored += 1;
                hook.scored(e.node, e.score, self.row(e.node));
                if beam.record {
                    scratch.scored_allowed.push(e);
                }
                scratch.res.keep(beam.ef, e);
                beam.saturation.seed(e);
            }
        }
    }

    /// True once the floor gate is open and `item` is provably below the
    /// hook's current floor.
    #[inline]
    fn floor_prunes<H: Hook>(&self, beam: &Beam, item: Scored, hook: &H) -> bool {
        beam.prune.enabled
            && beam.allowed_scored >= beam.prune.gate
            && self.below_floor(
                item.node,
                item.score,
                hook.current_floor(),
                beam.prune.slack_per_norm,
            )
    }

    /// Decide what the beam does with `c`, its best pending candidate: stop
    /// when a full beam already beats it, skip it when the floor prunes it,
    /// stop when patience has run out, and otherwise expand it.
    #[inline]
    pub(super) fn gate<H: Hook>(
        &self,
        beam: &mut Beam,
        c: Scored,
        scratch: &Scratch,
        hook: &H,
    ) -> Gate {
        if scratch.res.len() >= beam.ef {
            let worst = scratch.res.worst().map_or(f32::NEG_INFINITY, |w| w.score);
            if c.score < worst {
                return Gate::End(Walk::Completed);
            }
        }
        if self.floor_prunes(beam, c, hook) {
            beam.pruned_by_floor = true;
            return Gate::Skip;
        }
        if beam.saturation.exhausted() {
            return Gate::End(Walk::Patience);
        }
        Gate::Expand
    }

    /// Take one scored neighbor into the beam. `allowed` rows are reported
    /// to the hook and may enter the results; the others only route.
    #[inline]
    pub(super) fn offer<H: Hook>(
        &self,
        beam: &mut Beam,
        scored: Scored,
        allowed: bool,
        scratch: &mut Scratch,
        hook: &mut H,
    ) {
        if allowed {
            beam.allowed_scored += 1;
            hook.scored(scored.node, scored.score, self.row(scored.node));
            if beam.record {
                scratch.scored_allowed.push(scored);
            }
        }
        let full = scratch.res.len() >= beam.ef;
        let worst = scratch.res.worst().map_or(f32::NEG_INFINITY, |w| w.score);
        if !full || scored.score > worst {
            if self.floor_prunes(beam, scored, hook) {
                beam.pruned_by_floor = true;
                return;
            }
            scratch.cand.push(scored);
            if allowed {
                beam.saturation.admit(&mut scratch.res, beam.ef, scored);
            }
        }
    }
}
