//! Coordinator-side merge of shard top-k lists and floor aggregation.

use std::cmp::Ordering;

/// A hit in the merged global ranking: the shard-local hit plus the index
/// of the shard it came from (for deterministic tie-breaking).
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MergedHit {
    /// Global vector id (shard slot offset + local slot).
    pub vector_id: u64,
    /// Index of the shard that produced this hit (fan-out order).
    pub shard: u32,
    /// turbovec score; higher is better.
    pub score: f32,
}

/// Product-level total order over hits: score descending, then stable vector
/// id ascending. Shard is only a final defensive key for malformed input that
/// reports the same global id from two owners. A provider's physical shard
/// layout must not change public ranking.
pub(crate) fn cmp_hits(a: &MergedHit, b: &MergedHit) -> Ordering {
    b.score
        .total_cmp(&a.score)
        .then_with(|| a.vector_id.cmp(&b.vector_id))
        .then_with(|| a.shard.cmp(&b.shard))
}

/// Merge per-shard top-k lists into the global top-k.
///
/// `shard_hits` yields `(shard_index, hits)` pairs where each hit is
/// `(global_vector_id, score)`. A shard contributing more than k hits is
/// fine — the merge is order-insensitive because the final ranking is a
/// total order ([`cmp_hits`]), so equal inputs always merge identically
/// regardless of arrival order.
pub fn merge_topk<I>(shard_hits: I, k: usize) -> Vec<MergedHit>
where
    I: IntoIterator<Item = (u32, Vec<(u64, f32)>)>,
{
    let mut all: Vec<MergedHit> = shard_hits
        .into_iter()
        .flat_map(|(shard, hits)| {
            hits.into_iter().map(move |(vector_id, score)| MergedHit {
                vector_id,
                shard,
                score,
            })
        })
        .collect();
    all.sort_by(cmp_hits);
    all.truncate(k);
    all
}

/// Tracks the running maximum of shard-published floors for one query.
///
/// Nodes publish their k-th best once their heaps fill; the highest such
/// floor seen so far is a lower bound on the global k-th best and is what
/// the coordinator pushes back to every node. Floors are monotonic by
/// contract — `observe` returns `Some` only when the max actually rose, so
/// callers can push updates only on change.
#[derive(Debug, Clone, Copy)]
pub struct FloorTracker {
    current: f32,
}

impl Default for FloorTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl FloorTracker {
    /// A fresh tracker with no floor (`-inf`: nothing pruned).
    pub fn new() -> Self {
        Self {
            current: f32::NEG_INFINITY,
        }
    }

    /// The current aggregate floor (`-inf` before any observation).
    pub fn current(&self) -> f32 {
        self.current
    }

    /// Observe one shard's floor. Returns `Some(new_max)` when this raised
    /// the aggregate (callers should broadcast it), `None` when the update
    /// was stale, equal, or NaN (NaN is ignored defensively: a NaN floor
    /// would panic turbovec's threshold assertion downstream).
    pub fn observe(&mut self, floor: f32) -> Option<f32> {
        if floor.is_nan() || floor <= self.current {
            return None;
        }
        self.current = floor;
        Some(floor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn merge_sorts_by_score_descending() {
        let merged = merge_topk(
            vec![
                (0, vec![(10, 0.5), (11, 0.9)]),
                (1, vec![(20, 0.7), (21, 0.1)]),
            ],
            10,
        );
        let scores: Vec<f32> = merged.iter().map(|h| h.score).collect();
        assert_eq!(scores, vec![0.9, 0.7, 0.5, 0.1]);
        assert_eq!(merged[0].vector_id, 11);
    }

    #[test]
    fn merge_truncates_to_k() {
        let shards: Vec<(u32, Vec<(u64, f32)>)> = (0..4)
            .map(|s| (s, (0..5).map(|i| (s as u64 * 100 + i, i as f32)).collect()))
            .collect();
        let merged = merge_topk(shards, 3);
        assert_eq!(merged.len(), 3);
        assert!(merged.iter().all(|h| h.score == 4.0));
    }

    #[test]
    fn merge_ties_break_by_stable_id() {
        let merged = merge_topk(
            vec![
                (2, vec![(7, 1.0), (3, 1.0)]),
                (0, vec![(9, 1.0)]),
                (1, vec![(1, 1.0)]),
            ],
            10,
        );
        let order: Vec<(u32, u64)> = merged.iter().map(|h| (h.shard, h.vector_id)).collect();
        assert_eq!(order, vec![(1, 1), (2, 3), (2, 7), (0, 9)]);
    }

    #[test]
    fn merge_is_order_insensitive() {
        let a = merge_topk(vec![(0, vec![(1, 0.3), (2, 0.8)]), (1, vec![(3, 0.5)])], 3);
        let b = merge_topk(vec![(1, vec![(3, 0.5)]), (0, vec![(2, 0.8), (1, 0.3)])], 3);
        assert_eq!(a, b);
    }

    #[test]
    fn merge_empty_and_zero_k() {
        assert!(merge_topk(Vec::<(u32, Vec<(u64, f32)>)>::new(), 5).is_empty());
        assert!(merge_topk(vec![(0, vec![(1, 1.0)])], 0).is_empty());
    }

    #[test]
    fn floor_tracker_starts_unbounded() {
        let t = FloorTracker::new();
        assert_eq!(t.current(), f32::NEG_INFINITY);
    }

    #[test]
    fn floor_tracker_only_reports_raises() {
        let mut t = FloorTracker::new();
        assert_eq!(t.observe(0.5), Some(0.5));
        assert_eq!(t.observe(0.4), None); // stale
        assert_eq!(t.observe(0.5), None); // equal
        assert_eq!(t.observe(0.9), Some(0.9));
        assert_eq!(t.current(), 0.9);
    }

    #[test]
    fn floor_tracker_ignores_nan() {
        let mut t = FloorTracker::new();
        assert_eq!(t.observe(f32::NAN), None);
        assert_eq!(t.current(), f32::NEG_INFINITY);
    }
}
