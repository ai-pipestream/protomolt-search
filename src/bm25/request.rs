//! The top-k request and the four public scoring entry points.

use super::{
    exhaustive, pruned, Bm25Params, CandidateHook, ChainCtx, CorpusStats, FieldQuery, FilterCtx,
    FusedDoc, LiveFloorHook, PruneStats, ScoredDoc,
};
use crate::postings::Bm25Index;

/// A live-floor exchange callable that may outlive the request.
type LiveFloorFn<'h> = dyn FnMut(Option<f32>) -> Option<f32> + 'h;
/// A candidate sink callable that may outlive the request.
type CandidateFn<'h> = dyn FnMut(u32, f32) + 'h;

/// One top-k scoring request.
///
/// Defaults: no floor (`-inf`), no chain, no filter, no hooks, no skip
/// accounting, block-max pruning allowed. A bare `usize` converts into a
/// request with those defaults.
///
/// A finite `floor` keeps hits with `score >= floor`, so ties at a seeded
/// floor survive. The `live` hook is polled once per block-max iteration
/// and may raise the floor; `candidate` receives every admitted hit. With
/// `block_max` off the exhaustive scorer runs instead and the hooks only
/// see its final top-k.
pub struct TopK<'a> {
    /// Ranking depth.
    pub k: usize,
    /// Inclusive raw-score floor.
    pub floor: f64,
    /// Score-function chain applied before ranking (single-field only).
    pub chain: ChainCtx<'a>,
    /// Document filters gating heap insertion.
    pub filter: FilterCtx<'a>,
    /// Mid-scan floor exchange.
    pub live: Option<LiveFloorHook<'a>>,
    /// Compact candidate emission.
    pub candidate: Option<CandidateHook<'a>>,
    /// Skip accounting, accumulated into the caller's counters.
    pub prune: Option<&'a mut PruneStats>,
    /// Whether impact pruning may be used.
    pub block_max: bool,
}

impl<'a> TopK<'a> {
    /// A request for the best `k` hits with every default.
    pub fn new(k: usize) -> Self {
        Self {
            k,
            floor: f64::NEG_INFINITY,
            chain: None,
            filter: None,
            live: None,
            candidate: None,
            prune: None,
            block_max: true,
        }
    }

    /// Keep only hits with `score >= floor`.
    pub fn floor(mut self, floor: f64) -> Self {
        self.floor = floor;
        self
    }

    /// Apply a score-function chain before ranking.
    pub fn chain(mut self, chain: ChainCtx<'a>) -> Self {
        self.chain = chain;
        self
    }

    /// Drop documents failing `filter` before ranking.
    pub fn filter(mut self, filter: FilterCtx<'a>) -> Self {
        self.filter = filter;
        self
    }

    /// Exchange live floors through `live`.
    pub fn live<'h: 'a>(mut self, live: Option<&'a mut LiveFloorFn<'h>>) -> Self {
        self.live = live.map(|hook| hook as LiveFloorHook<'a>);
        self
    }

    /// Emit admitted candidates through `candidate`.
    pub fn candidate<'h: 'a>(mut self, candidate: Option<&'a mut CandidateFn<'h>>) -> Self {
        self.candidate = candidate.map(|sink| sink as CandidateHook<'a>);
        self
    }

    /// Accumulate skip accounting into `prune`.
    pub fn prune_stats(mut self, prune: &'a mut PruneStats) -> Self {
        self.prune = Some(prune);
        self
    }

    /// Allow or forbid impact pruning.
    pub fn block_max(mut self, block_max: bool) -> Self {
        self.block_max = block_max;
        self
    }

    pub(super) fn split(self) -> (Controls<'a>, Hooks<'a>, Option<&'a mut PruneStats>) {
        let controls = Controls {
            k: self.k,
            floor: self.floor,
            chain: self.chain,
            filter: self.filter,
            block_max: self.block_max,
        };
        let hooks = Hooks {
            live: self.live,
            candidate: self.candidate,
        };
        (controls, hooks, self.prune)
    }
}

impl From<usize> for TopK<'_> {
    fn from(k: usize) -> Self {
        Self::new(k)
    }
}

/// A phrase group for [`top_k_fused_exhaustive`]: field `field` contributes
/// only its strongest weighted term, `weights` parallel to its terms.
#[derive(Debug, Clone, Copy)]
pub struct Phrase<'a> {
    /// Index of the phrase field in the `fields` slice.
    pub field: usize,
    /// Per-term multipliers, parallel to that field's terms.
    pub weights: &'a [f64],
}

/// The scalar part of a [`TopK`] request.
#[derive(Clone, Copy)]
pub(super) struct Controls<'a> {
    pub k: usize,
    pub floor: f64,
    pub chain: ChainCtx<'a>,
    pub filter: FilterCtx<'a>,
    pub block_max: bool,
}

impl Controls<'_> {
    /// The final score of `doc`: the chain applied to its BM25 sum.
    pub fn finish(&self, score: f64, doc: u32) -> f64 {
        match self.chain {
            Some((chain, columns)) => chain.eval(score, doc, columns),
            None => score,
        }
    }
}

/// The callback part of a [`TopK`] request.
pub(super) struct Hooks<'a> {
    pub live: Option<LiveFloorHook<'a>>,
    pub candidate: Option<CandidateHook<'a>>,
}

impl Hooks<'_> {
    /// Publish a finished exhaustive top-k as candidates.
    pub fn emit_all(&mut self, docs: impl Iterator<Item = (u32, f64)>) {
        if let Some(sink) = self.candidate.as_mut() {
            for (doc, score) in docs {
                sink(doc, score as f32);
            }
        }
    }
}

/// A single-field query's borrowed inputs.
#[derive(Clone, Copy)]
pub(super) struct Flat<'a> {
    pub store: &'a dyn Bm25Index,
    pub terms: &'a [String],
    pub stats: &'a CorpusStats,
    pub params: Bm25Params,
}

/// Single-field top-k, block-max pruned over the impact skip run.
///
/// Scores `terms` with the supplied global `stats`; returns the top-k,
/// score descending, ties by ascending doc id, bit-identical to
/// [`top_k_exhaustive`] with the floor applied. Falls back to the
/// exhaustive scorer when pruning is disallowed or any term lacks impacts.
pub fn top_k<'a>(
    store: &dyn Bm25Index,
    terms: &[String],
    stats: &CorpusStats,
    params: Bm25Params,
    request: impl Into<TopK<'a>>,
) -> Vec<ScoredDoc> {
    let query = Flat {
        store,
        terms,
        stats,
        params,
    };
    pruned::flat(&query, request.into())
}

/// Fused multi-field top-k, block-max pruned over every (field, term)
/// pair. Bit-identical to [`top_k_fused_exhaustive`] without a phrase
/// group. Fused scoring takes no chain.
pub fn top_k_fused<'a>(fields: &[FieldQuery], request: impl Into<TopK<'a>>) -> Vec<FusedDoc> {
    let request = request.into();
    assert!(request.chain.is_none(), "fused scoring takes no chain");
    pruned::fused(fields, request)
}

/// The single-field exactness oracle: walks every posting with offsets.
/// Honors `k`, the chain, the filter, and the floor (applied before
/// truncating to `k`); ignores the hooks and skip accounting.
pub fn top_k_exhaustive<'a>(
    store: &dyn Bm25Index,
    terms: &[String],
    stats: &CorpusStats,
    params: Bm25Params,
    request: impl Into<TopK<'a>>,
) -> Vec<ScoredDoc> {
    let query = Flat {
        store,
        terms,
        stats,
        params,
    };
    let (controls, _, _) = request.into().split();
    exhaustive::oracle(&query, &controls)
}

/// The fused exhaustive scorer and oracle: weighted per-field sum, each
/// field saturating independently, accumulated in field-id then
/// term-index order. With a `phrase` group that field contributes only
/// its strongest weighted term. Honors `k`, the filter, and the floor.
pub fn top_k_fused_exhaustive<'a>(
    fields: &[FieldQuery],
    request: impl Into<TopK<'a>>,
    phrase: Option<Phrase<'_>>,
) -> Vec<FusedDoc> {
    let (controls, _, _) = request.into().split();
    assert!(controls.chain.is_none(), "fused scoring takes no chain");
    if let Some(phrase) = phrase {
        assert!(
            phrase.field < fields.len(),
            "phrase field index out of range"
        );
        assert_eq!(
            fields[phrase.field].terms.len(),
            phrase.weights.len(),
            "phrase weights must be parallel to phrase terms"
        );
    }
    exhaustive::fused(fields, &controls, phrase)
}
