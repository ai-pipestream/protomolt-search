//! Fan-out result types and completion statistics.

use super::*;

/// Resolved per-leg options for one hybrid query.
#[derive(Debug, Clone, Copy)]
pub struct HybridLegs {
    /// Depth each leg (and each shard's fused list) is fetched to.
    pub leg_k: u32,
    /// Vector-leg weight (RRF weight, or blend weight under
    /// SCORE_BLEND). RESOLVED: exactly 0 disables the leg.
    pub vector_weight: f32,
    /// BM25-leg weight (same dual role, same disable rule).
    pub bm25_weight: f32,
    /// RRF constant.
    pub rrf_k: f64,
    /// Fusion strategy (default GLOBAL_RANK).
    pub fusion_mode: FusionMode,
    /// SCORE_BLEND: per-leg score normalization.
    pub normalization: fusion::Normalization,
    /// SCORE_BLEND: combination of normalized leg scores.
    pub combination: fusion::Combination,
    /// Vector-score floor on the result set (see the proto); 0 = off.
    pub min_vector_score: f32,
}

/// The per-leg payload to put on the wire, blanked for a leg this query
/// has disabled.
///
/// A weight of exactly 0 disables a leg (see `HybridLegs`) and both
/// fusion functions honor it — but they do so AFTER the shard has
/// already scanned. A shard has no weight field to read; it gates the
/// vector scan on a non-empty `vector` and the BM25 scan on non-empty
/// `terms`, so sending a payload for a disabled leg buys a full scan
/// whose result is then discarded. Measured on the 86.6M-chunk fleet: a
/// `vector_weight: 0` query still paid ~320 ms of vector scan out of
/// ~340 ms total, so "bm25-only" cost 20x what the lexical leg costs
/// alone.
///
/// `dfs` travels with `terms` because `shard_legs` and `hybrid_shard`
/// both reject a request whose lengths disagree.
pub(super) fn leg_payloads(
    vector: &[f32],
    terms: &[String],
    global: &CorpusStats,
    legs: HybridLegs,
) -> (Vec<f32>, Vec<String>, Vec<u32>) {
    let vector = if legs.vector_weight == 0.0 {
        Vec::new()
    } else {
        vector.to_vec()
    };
    let (terms, dfs) = if legs.bm25_weight == 0.0 {
        (Vec::new(), Vec::new())
    } else {
        (terms.to_vec(), global.dfs.clone())
    };
    (vector, terms, dfs)
}

/// How the shards' providers answered a request's filter, summed over their
/// terminal summaries; `None` when none reported a regime.
pub(super) fn summed_filter_regimes(
    summaries: &[StreamSearchSummary],
) -> Option<crate::pb::DenseFilterRegimes> {
    let mut total = crate::vector::FilterRegimeCounts::default();
    for summary in summaries {
        total.add(crate::vector::FilterRegimeCounts::from_proto(
            summary.filter_regimes.as_ref(),
        ));
    }
    total.to_proto()
}

pub(super) fn summed_tail_enumerated_rows(summaries: &[StreamSearchSummary]) -> u64 {
    summaries.iter().fold(0u64, |acc, summary| {
        acc.saturating_add(summary.tail_enumerated_rows)
    })
}

pub(super) fn summed_patience_stopped(summaries: &[StreamSearchSummary]) -> u32 {
    summaries.iter().fold(0u32, |acc, summary| {
        acc.saturating_add(summary.patience_stopped)
    })
}

pub(super) fn count_shards_finished_early(summaries: &[StreamSearchSummary]) -> u32 {
    summaries
        .iter()
        .filter(|summary| {
            crate::vector::VectorStreamEndReason::from_proto(summary.end_reason).is_early_finish()
        })
        .count() as u32
}

pub(super) fn count_shards_floor_pruned(summaries: &[StreamSearchSummary]) -> u32 {
    summaries
        .iter()
        .filter(|summary| {
            summary.floor_pruned > 0
                || crate::vector::VectorStreamEndReason::from_proto(summary.end_reason)
                    == crate::vector::VectorStreamEndReason::FloorPruned
        })
        .count() as u32
}

/// Quorum of active shards that must report scan completion before the
/// coordinator arms its top-`k` stillness settle window: never self-triggers
/// on a single-shard scan (`1`), `1` on 2 shards, and `floor(3*N/4)` clamped
/// to `[1, N-1]` on larger fleets (`3` on 4, `6` on 8-9, `12` on 16).
pub(super) fn finish_quorum(active_shards: usize) -> usize {
    if active_shards <= 1 {
        1
    } else {
        (active_shards * 3 / 4).max(1).min(active_shards - 1)
    }
}

/// Outcome of one coordinator fan-out: the merged global top-k plus the
/// per-shard scan statistics (in completion order), the latter powering the
/// floor-sharing benchmark.
#[derive(Debug)]
pub struct FanoutResult {
    /// Merged global top-k (score desc, shard/id tie-break).
    pub hits: Vec<ScoredHit>,
    /// Per-shard scan stats, one entry per shard, in completion order.
    pub shard_stats: Vec<Option<ShardScanStats>>,
    /// The raw per-shard lists (shard index, `(doc_id, score)`), in
    /// completion order. With `tie_complete` set, a shard's list may
    /// exceed k by its boundary tie group. The cascade pipeline routes
    /// candidates by these shard assignments.
    pub shard_hits: Vec<(u32, Vec<(u64, f32)>)>,
    /// Per-shard wall time in milliseconds as measured at the
    /// coordinator (shard index, ms), in completion order.
    pub shard_wall_ms: Vec<(u32, f32)>,
    /// Hedge legs launched during this fan-out, and how many of them
    /// returned before their primary.
    pub hedges_fired: u64,
    pub hedge_wins: u64,
    pub rerank_candidates: HashMap<u64, crate::pb::RerankCandidatePayload>,
}

/// What a document-mode streaming fan-out returns (see
/// [`CoordinatorServiceImpl::fanout_stream_search_collapse`]).
pub struct CollapseStreamResult {
    /// The top-k parents' representatives: each parent's best chunk,
    /// score descending, ties by chunk id; `parent_id` set.
    pub hits: Vec<ScoredHit>,
    /// One group per entry of `hits`, same order: every chunk of that
    /// parent scoring at or above `chunk_floor`.
    pub groups: Vec<ParentGroup>,
    /// One ULP below the k-th best parent score, or -inf when fewer
    /// than k parents exist.
    pub chunk_floor: f32,
    /// Every shard's terminal summary (all certified `completed`).
    pub summaries: Vec<StreamSearchSummary>,
    /// Parent-floor raises pushed to the fleet.
    pub floors_sent: u64,
    /// Shards whose walk ended cleanly via `FinishStreamSearch` (`FINISH_REQUESTED`
    /// or `DEADLINE`).
    pub shards_finished_early: u32,
    /// Shards where at least one segment's walk pruned candidates against the
    /// shared score floor (`#72`).
    pub shards_floor_pruned: u32,
}

/// Result of [`CoordinatorServiceImpl::fanout_stream_search`].
pub struct StreamFanoutResult {
    /// Merged global top-k, in the same total order as
    /// [`FanoutResult::hits`].
    pub hits: Vec<ScoredHit>,
    /// Per-shard terminal summaries, shard order; every one certified
    /// `completed`.
    pub summaries: Vec<StreamSearchSummary>,
    /// Floor raises this coordinator broadcast.
    pub floors_sent: u64,
    /// Shards whose walk ended cleanly via `FinishStreamSearch` (`FINISH_REQUESTED`
    /// or `DEADLINE`).
    pub shards_finished_early: u32,
    /// Shards where at least one segment's walk pruned candidates against the
    /// shared score floor (`#72`).
    pub shards_floor_pruned: u32,
}

/// Heap wrapper whose `Ord` IS the merge's total order ([`cmp_hits`]:
/// better entries compare Less), so a max-heap's peek is the worst
/// survivor — the running k-th best.
pub(super) struct StreamHeapEntry(pub(super) MergedHit);

impl PartialEq for StreamHeapEntry {
    fn eq(&self, other: &Self) -> bool {
        cmp_hits(&self.0, &other.0) == std::cmp::Ordering::Equal
    }
}

impl Eq for StreamHeapEntry {}

impl PartialOrd for StreamHeapEntry {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for StreamHeapEntry {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        cmp_hits(&self.0, &other.0)
    }
}
