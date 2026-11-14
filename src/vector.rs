//! Backend-neutral vector index contract.
//!
//! The search product owns document identity, filtering, hybrid semantics,
//! generation cutover, and public quality claims. A vector backend owns its
//! encoding, native score, query execution, and persisted image. Runtime code
//! outside this module talks only to [`VectorIndex`]; the concrete TurboVec
//! dependency is contained in [`embedded_turbovec`].

use std::borrow::Cow;
use std::fmt;
use std::ops::{ControlFlow, Range};
use std::path::Path;

use serde::{Deserialize, Serialize};

use crate::boolean_bits::Bits;

/// Stable identifier for the currently shipped embedded backend.
pub const EMBEDDED_TURBOVEC: &str = "embedded-turbovec";

/// Direction in which a backend-native score becomes more competitive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScoreDirection {
    HigherIsBetter,
    LowerIsBetter,
}

/// What completion of a vector query actually certifies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QualityContract {
    /// Every eligible row was evaluated under one backend-native quantized
    /// score and total order.
    ExhaustiveQuantized,
    /// A configured approximate traversal completed. It is not a corpus-wide
    /// optimum certificate.
    ConfiguredAnn,
    /// Pruning used a declared probabilistic bound.
    ProbabilisticBound,
}

/// Independently testable operations a backend may advertise.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VectorCapability {
    BatchQuery,
    CandidateStream,
    LiveBoundInput,
    DenseMask,
    CandidateRescore,
    Append,
    Flush,
    SnapshotInstall,
    RawVectorRebuild,
    ExhaustiveCompletion,
    /// Pinned to [`FilterRegime::Enumerate`], a masked search scores every
    /// allowed row exactly. A provider whose own search cannot certify
    /// completion can still serve a pass that needs every member's score.
    MaskEnumeration,
    /// Layer-0 graph walks support hop-to-hop saturation early stopping (`#95`).
    PatienceEarlyStop,
    /// Layer-0 graph walks support monotone candidate pruning against an active
    /// score floor behind the `#72` ascent gate and clamp.
    FloorPruning,
}

impl VectorCapability {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BatchQuery => "batch_query",
            Self::CandidateStream => "candidate_stream",
            Self::LiveBoundInput => "live_bound_input",
            Self::DenseMask => "dense_mask",
            Self::CandidateRescore => "candidate_rescore",
            Self::Append => "append",
            Self::Flush => "flush",
            Self::SnapshotInstall => "snapshot_install",
            Self::RawVectorRebuild => "raw_vector_rebuild",
            Self::ExhaustiveCompletion => "exhaustive_completion",
            Self::MaskEnumeration => "mask_enumeration",
            Self::PatienceEarlyStop => "patience_early_stop",
            Self::FloorPruning => "floor_pruning",
        }
    }
}

/// Backend identity and the semantics of one opened vector generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorBackendDescriptor {
    pub backend_kind: String,
    pub backend_version: String,
    pub dimension: Option<usize>,
    pub bits_per_dimension: Option<u32>,
    pub metric: String,
    pub score_direction: ScoreDirection,
    pub scoring_fingerprint: String,
    pub quality_contract: QualityContract,
    pub capabilities: Vec<VectorCapability>,
}

/// Opaque, backend-owned construction state carried by product manifests and
/// provisioning RPCs. The product compares the identity fields and transports
/// `payload`; only the named backend interprets its bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorBackendConfig {
    pub backend_kind: String,
    pub config_format: String,
    pub payload: Vec<u8>,
}

/// Compatibility view of the embedded backend's former public calibration
/// fields. New product metadata transports [`VectorBackendConfig`] opaquely;
/// this view exists only while the legacy calibration RPC is served.
#[derive(Debug, Clone, PartialEq)]
pub struct LegacyCalibrationConfig {
    pub bits_per_dimension: usize,
    pub shift: Vec<f32>,
    pub scale: Vec<f32>,
}

pub fn embedded_turbovec_config(
    bits_per_dimension: usize,
    shift: &[f32],
    scale: &[f32],
) -> Result<VectorBackendConfig, VectorError> {
    embedded_turbovec::config(bits_per_dimension, shift, scale)
}

pub fn legacy_calibration_config(
    config: &VectorBackendConfig,
) -> Result<Option<LegacyCalibrationConfig>, VectorError> {
    if config.backend_kind != EMBEDDED_TURBOVEC {
        return Ok(None);
    }
    embedded_turbovec::decode_config(config).map(Some)
}

/// A backend-independent operation failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorError {
    message: String,
}

impl VectorError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for VectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for VectorError {}

/// How a provider that can steer its search by the mask answers a filtered
/// query (`docs/hnsw.md`, "Filters"). A provider with one way to filter
/// ignores it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum FilterRegime {
    /// Score every allowed row: exact over the mask, linear in the allowed
    /// rows.
    Enumerate,
    /// Walk the graph through the allowed rows only.
    FilterAware,
    /// Walk the graph as if unfiltered; the mask gates results.
    Plain,
}

impl FilterRegime {
    pub fn name(self) -> &'static str {
        match self {
            FilterRegime::Enumerate => "enumerate",
            FilterRegime::FilterAware => "filter_aware",
            FilterRegime::Plain => "plain",
        }
    }
}

/// The rows a search may return, one flag per row of the index, in the
/// shape the caller holds. The scan kernel masks with one byte per row; a
/// graph walk tests single rows and counts and lists the set ones, which
/// a bitmap does a word at a time. A provider takes its own shape as it
/// is and converts the other once per search.
#[derive(Debug, Clone, Copy)]
pub enum Mask<'a> {
    Bools(&'a [bool]),
    Bits(&'a Bits),
    /// Every row in `0..len` is allowed without materializing a flag buffer.
    All(usize),
}

impl<'a> Mask<'a> {
    /// Rows covered, allowed or not.
    pub fn len(&self) -> usize {
        match self {
            Mask::Bools(allow) => allow.len(),
            Mask::Bits(bits) => bits.len(),
            Mask::All(len) => *len,
        }
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    #[inline]
    pub fn test(&self, row: usize) -> bool {
        match self {
            Mask::Bools(allow) => allow[row],
            Mask::Bits(bits) => bits.test(row),
            Mask::All(len) => {
                assert!(row < *len, "mask row {row} out of bounds for len {len}");
                true
            }
        }
    }

    /// Allowed rows.
    pub fn count(&self) -> usize {
        match self {
            Mask::Bools(allow) => allow.iter().filter(|&&on| on).count(),
            Mask::Bits(bits) => bits.count() as usize,
            Mask::All(len) => *len,
        }
    }

    /// The allowed rows ascending, until `each` breaks.
    pub fn try_for_each<B>(&self, mut each: impl FnMut(usize) -> ControlFlow<B>) -> ControlFlow<B> {
        match self {
            Mask::Bools(allow) => {
                for (row, &on) in allow.iter().enumerate() {
                    if on {
                        each(row)?;
                    }
                }
                ControlFlow::Continue(())
            }
            Mask::Bits(bits) => bits.iter().try_for_each(each),
            Mask::All(len) => {
                for row in 0..*len {
                    each(row)?;
                }
                ControlFlow::Continue(())
            }
        }
    }

    /// The rows of `[start, end)` as their own mask, row `start` at 0, or
    /// `None` when the span allows nothing.
    pub fn slice(&self, start: usize, end: usize) -> Option<OwnedMask> {
        let end = end.min(self.len());
        let start = start.min(end);
        match self {
            Mask::Bools(allow) => {
                let part = &allow[start..end];
                part.iter()
                    .any(|&on| on)
                    .then(|| OwnedMask::Bools(part.to_vec()))
            }
            Mask::Bits(bits) => {
                let part = bits.slice(start, end);
                (part.count() > 0).then_some(OwnedMask::Bits(part))
            }
            Mask::All(_) => (start < end).then_some(OwnedMask::All(end - start)),
        }
    }

    /// One byte per row, the shape the scan kernel masks with.
    pub fn bools(&self) -> Cow<'a, [bool]> {
        match self {
            Mask::Bools(allow) => Cow::Borrowed(allow),
            Mask::Bits(bits) => Cow::Owned(bits.to_bools()),
            Mask::All(len) => Cow::Owned(vec![true; *len]),
        }
    }
}

/// A mask that owns its flags (one part of a segmented search).
#[derive(Debug, Clone)]
pub enum OwnedMask {
    Bools(Vec<bool>),
    Bits(Bits),
    All(usize),
}

impl OwnedMask {
    pub fn as_mask(&self) -> Mask<'_> {
        match self {
            OwnedMask::Bools(allow) => Mask::Bools(allow),
            OwnedMask::Bits(bits) => Mask::Bits(bits),
            OwnedMask::All(len) => Mask::All(*len),
        }
    }
}

/// Result window over which hop-to-hop survival is measured for patience
/// early stopping (`docs/hnsw.md`, "Patience").
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub enum PatienceWindow {
    /// Measure hop-to-hop survival over the full `ef` result beam, avoiding
    /// the small-`k` quantization cliff (`1 / ef` granularity).
    #[default]
    Beam,
    /// Measure hop-to-hop survival over the top-`k` (`min(k, ef)`) result
    /// window (`PatienceKnnVectorQuery` top-`k` variant).
    TopK,
}

impl PatienceWindow {
    pub fn name(self) -> &'static str {
        match self {
            PatienceWindow::Beam => "beam",
            PatienceWindow::TopK => "top_k",
        }
    }
}

/// Hop-to-hop saturation early-stop parameters for a layer-0 graph walk.
/// `patience == 0` disables early stopping and reproduces the standard walk
/// bit for bit.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct PatienceSpec {
    /// Required fraction of the window from the start of an expansion that
    /// remains in the window at the end of the expansion (`(0.0, 1.0]`).
    pub saturation: f32,
    /// Optional exact parts-per-million saturation threshold (`1..=1_000_000`)
    /// carried from a dense execution policy point or protobuf request so
    /// survival counts use exact integer ceiling arithmetic.
    pub saturation_ppm: Option<u32>,
    /// Consecutive saturated expansions after the beam fills before ending
    /// the layer-0 walk early (`0` disables patience).
    pub patience: u32,
    /// Minimum local allowed rows scored at layer 0 before patience may count
    /// when a finite score floor is active (`#72` ascent gate).
    pub ascent_gate: usize,
    /// Result window over which survival is measured.
    pub window: PatienceWindow,
}

impl PatienceSpec {
    pub const OFF: Self = Self {
        saturation: 1.0,
        saturation_ppm: None,
        patience: 0,
        ascent_gate: 0,
        window: PatienceWindow::Beam,
    };

    /// Construct a beam-window patience policy from exact parts-per-million
    /// saturation (`1..=1_000_000`) and consecutive hop count.
    pub fn from_ppm(saturation_ppm: u32, patience: u32) -> Self {
        Self {
            saturation: (saturation_ppm as f64 / 1_000_000.0) as f32,
            saturation_ppm: Some(saturation_ppm),
            patience,
            ascent_gate: 0,
            window: PatienceWindow::Beam,
        }
    }
}

/// Default `clampMin` in `max(clampMin, ceil((1 - g) * gateK))` (`#72`).
pub const DEFAULT_FLOOR_CLAMP_MIN: u32 = 8;

/// Shared-floor candidate pruning specification for a layer-0 walk (`#72`).
///
/// Given a global floor of rank `floor_k` and this walk's eligible-row share
/// `p = share_ppm / 1,000,000` across the planned shard set, the ascent gate is
/// `gateK = ceil(k * p + 16 * sqrt(k * p * (1 - p)))` and the clamped gate is
/// `min(local_eligible, max(clamp_min, ceil((1 - g) * gateK)))`, where
/// `g = greediness_ppm / 1,000,000`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FloorPruneSpec {
    pub enabled: bool,
    pub floor_k: usize,
    pub share_ppm: u32,
    pub greediness_ppm: u32,
    pub clamp_min: usize,
}

impl Default for FloorPruneSpec {
    fn default() -> Self {
        Self::OFF
    }
}

impl FloorPruneSpec {
    pub const OFF: Self = Self {
        enabled: false,
        floor_k: 0,
        share_ppm: 1_000_000,
        greediness_ppm: 0,
        clamp_min: 0,
    };

    /// Minimum local allowed rows scored at layer 0 before floor pruning may
    /// discard candidates (`0` when `local_eligible == 0` or `enabled == false`).
    pub fn effective_gate(self, local_eligible: usize) -> usize {
        if !self.enabled || local_eligible == 0 {
            return 0;
        }
        let p = (f64::from(self.share_ppm) / 1_000_000.0).clamp(0.0, 1.0);
        let kf = self.floor_k as f64;
        let gate_k = if kf <= 0.0 || p <= 0.0 {
            0.0
        } else {
            (kf * p + 16.0 * (kf * p * (1.0 - p)).max(0.0).sqrt()).ceil()
        };
        let g = (f64::from(self.greediness_ppm) / 1_000_000.0).clamp(0.0, 1.0);
        let scaled = ((1.0 - g) * gate_k).ceil().max(0.0) as usize;
        scaled.max(self.clamp_min).min(local_eligible)
    }

    /// Scale `share_ppm` proportionally to a segment's share of its shard's
    /// eligible rows (`p_seg = p_shard * seg_eligible / shard_eligible`).
    pub fn for_segment(self, seg_eligible: usize, shard_eligible: usize) -> Self {
        if shard_eligible == 0 {
            return Self {
                share_ppm: 0,
                ..self
            };
        }
        let num = u128::from(self.share_ppm) * (seg_eligible as u128);
        let scaled =
            ((num + (shard_eligible as u128) / 2) / (shard_eligible as u128)).min(1_000_000) as u32;
        Self {
            share_ppm: scaled,
            ..self
        }
    }

    /// Conservative `#72` ascent gate (`g = 0`) for guarding `#95` patience
    /// early stopping when a finite score floor is active, even when floor
    /// candidate pruning itself is disabled (`enabled == false`).
    pub fn ascent_gate(self, local_eligible: usize) -> usize {
        if self.floor_k == 0 || local_eligible == 0 {
            return 0;
        }
        Self {
            enabled: true,
            greediness_ppm: 0,
            clamp_min: self.clamp_min.max(DEFAULT_FLOOR_CLAMP_MIN as usize),
            ..self
        }
        .effective_gate(local_eligible)
    }
}

/// Options common to every provider search used by this product.
#[derive(Debug, Clone, Copy, Default)]
pub struct VectorSearchOptions<'a> {
    pub allow: Option<Mask<'a>>,
    pub minimum_score: Option<f32>,
    /// How many candidates the request asked the provider to consider: the
    /// beam a configured-ANN provider walks to (`docs/hnsw.md`, "The
    /// beam"). A streaming search on such a provider refuses without it;
    /// exhaustive providers ignore it.
    pub candidate_depth: Option<usize>,
    /// The regime a filtered search must run in. `None` lets the provider
    /// choose from the mask's cardinality; a measurement pins it so a
    /// measured point is served by the regime that measured it.
    pub filter_regime: Option<FilterRegime>,
    /// The enumerate threshold of a provider that chooses the regime: a
    /// mask allowing at most this many rows is scored row by row, exactly.
    /// `None` is the provider's own threshold. A dense execution policy
    /// point carries the one it was measured under.
    pub enumerate_max_rows: Option<usize>,
    /// Optional hop-to-hop saturation early-stop policy for layer-0 walks.
    pub patience: Option<PatienceSpec>,
    /// Optional shared-floor candidate pruning policy for layer-0 walks (`#72`).
    pub floor_prune: Option<FloorPruneSpec>,
}

impl<'a> VectorSearchOptions<'a> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_allowlist(mut self, allow: &'a [bool]) -> Self {
        self.allow = Some(Mask::Bools(allow));
        self
    }

    /// The mask as a bitmap (`Mask::Bits`).
    pub fn with_bits(mut self, allow: &'a Bits) -> Self {
        self.allow = Some(Mask::Bits(allow));
        self
    }

    pub fn with_mask(self, allow: &'a [bool]) -> Self {
        self.with_allowlist(allow)
    }

    pub fn with_minimum_score(mut self, score: f32) -> Self {
        self.minimum_score = Some(score);
        self
    }

    pub fn with_initial_threshold(self, score: f32) -> Self {
        self.with_minimum_score(score)
    }

    pub fn with_candidate_depth(mut self, depth: usize) -> Self {
        self.candidate_depth = Some(depth);
        self
    }

    pub fn with_filter_regime(mut self, regime: FilterRegime) -> Self {
        self.filter_regime = Some(regime);
        self
    }

    pub fn with_enumerate_max_rows(mut self, rows: usize) -> Self {
        self.enumerate_max_rows = Some(rows);
        self
    }

    pub fn with_patience(mut self, patience: PatienceSpec) -> Self {
        self.patience = Some(patience);
        self
    }

    pub fn with_floor_prune(mut self, floor_prune: FloorPruneSpec) -> Self {
        self.floor_prune = Some(floor_prune);
        self
    }
}

/// Row-major native search results. Negative slots and negative-infinity
/// scores are padding when a supplied floor admits fewer than `k` rows.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorSearchResults {
    pub scores: Vec<f32>,
    pub slots: Vec<i64>,
    pub query_count: usize,
    pub result_count: usize,
    /// How the filtered queries were answered; all zero for a provider
    /// with one way to filter.
    pub regimes: FilterRegimeCounts,
    /// Rows scored by enumeration across unlinked HNSW parts (tail, frozen
    /// tail, or sealed segments below `MIN_GRAPH_ROWS`).
    pub tail_enumerated_rows: u64,
    /// Layer-0 graph walks that ended early via patience saturation (`#95`).
    pub patience_stopped: u32,
    /// Layer-0 graph walks that pruned candidates via the active score floor (`#72`).
    pub floor_pruned: u32,
}

impl VectorSearchResults {
    pub fn scores_for_query(&self, query: usize) -> &[f32] {
        &self.scores[query * self.result_count..(query + 1) * self.result_count]
    }

    pub fn slots_for_query(&self, query: usize) -> &[i64] {
        &self.slots[query * self.result_count..(query + 1) * self.result_count]
    }

    /// Compatibility spelling for the old engine result type. Kept local to
    /// the generic wrapper while callers migrate to `slots_for_query`.
    pub fn indices_for_query(&self, query: usize) -> &[i64] {
        self.slots_for_query(query)
    }
}

/// One provider emission batch.
#[derive(Debug, Clone, Copy)]
pub struct VectorStreamBatch<'a> {
    pub query_index: usize,
    pub block_base: usize,
    pub scores: &'a [f32],
    pub slots: &'a [i64],
}

/// Control returned to a streaming provider.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum VectorStreamControl {
    Continue,
    RaiseFloor(f32),
    /// End an approximate walk gracefully at the next hop boundary (`#100`),
    /// flushing buffered candidates at or above the floor (`completed = true`).
    /// Ignored by exhaustive scan providers.
    Finish {
        deadline: bool,
    },
    Stop,
}

/// Why a streaming provider ended its traversal (`#100`).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum VectorStreamEndReason {
    #[default]
    Unspecified,
    /// Every allowed row or logical chunk was evaluated.
    Exhausted,
    /// Approximate graph walk converged naturally (beam frontier exhausted or
    /// patience saturated).
    WalkConverged,
    /// Approximate graph walk stopped because remaining candidates fell below
    /// the active score floor.
    FloorPruned,
    /// Coordinator requested a graceful finish after global top-k stillness.
    FinishRequested,
    /// Coordinator requested a graceful finish at the request deadline.
    Deadline,
    /// Scan was abandoned (`StopStreamSearch`, UDP `CANCEL`, or closed stream);
    /// `completed` is `false`.
    Abandoned,
}

impl VectorStreamEndReason {
    /// True when the walk ended early because the coordinator requested a
    /// graceful finish (`#100`).
    #[inline]
    pub const fn is_early_finish(self) -> bool {
        matches!(self, Self::FinishRequested | Self::Deadline)
    }

    const fn rank(self) -> u8 {
        match self {
            Self::Unspecified => 0,
            Self::Exhausted => 1,
            Self::WalkConverged => 2,
            Self::FloorPruned => 3,
            Self::FinishRequested => 4,
            Self::Deadline => 5,
            Self::Abandoned => 6,
        }
    }

    /// Combine end reasons across segments or relay children, keeping the most
    /// specific termination cause.
    #[inline]
    pub const fn merge(self, other: Self) -> Self {
        if other.rank() > self.rank() {
            other
        } else {
            self
        }
    }

    #[inline]
    pub const fn to_proto(self) -> i32 {
        match self {
            Self::Unspecified => crate::pb::StreamEndReason::Unspecified as i32,
            Self::Exhausted => crate::pb::StreamEndReason::Exhausted as i32,
            Self::WalkConverged => crate::pb::StreamEndReason::WalkConverged as i32,
            Self::FloorPruned => crate::pb::StreamEndReason::FloorPruned as i32,
            Self::FinishRequested => crate::pb::StreamEndReason::FinishRequested as i32,
            Self::Deadline => crate::pb::StreamEndReason::Deadline as i32,
            Self::Abandoned => crate::pb::StreamEndReason::Abandoned as i32,
        }
    }

    #[inline]
    pub fn from_proto(raw: i32) -> Self {
        match crate::pb::StreamEndReason::try_from(raw) {
            Ok(crate::pb::StreamEndReason::Exhausted) => Self::Exhausted,
            Ok(crate::pb::StreamEndReason::WalkConverged) => Self::WalkConverged,
            Ok(crate::pb::StreamEndReason::FloorPruned) => Self::FloorPruned,
            Ok(crate::pb::StreamEndReason::FinishRequested) => Self::FinishRequested,
            Ok(crate::pb::StreamEndReason::Deadline) => Self::Deadline,
            Ok(crate::pb::StreamEndReason::Abandoned) => Self::Abandoned,
            _ => Self::Unspecified,
        }
    }
}

/// Terminal provider-native streaming summary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorStreamSummary {
    pub query_count: usize,
    pub emitted: usize,
    pub units_scanned: usize,
    pub completed: bool,
    pub end_reason: VectorStreamEndReason,
    /// How the filtered queries of this stream were answered; all zero for
    /// a provider with one way to filter.
    pub regimes: FilterRegimeCounts,
    /// Rows scored by enumeration across unlinked HNSW parts (tail, frozen
    /// tail, or sealed segments below `MIN_GRAPH_ROWS`).
    pub tail_enumerated_rows: u64,
    /// Layer-0 graph walks that ended early via patience saturation (`#95`).
    pub patience_stopped: u32,
    /// Layer-0 graph walks that pruned candidates via the active score floor (`#72`).
    pub floor_pruned: u32,
}

/// Filtered searches by the regime that answered them.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct FilterRegimeCounts {
    pub enumerate: u32,
    pub filter_aware: u32,
    pub plain: u32,
    /// Subset of `enumerate`: filter-aware walks handed over once their
    /// visit budget was spent.
    pub budget_handoffs: u32,
}

impl FilterRegimeCounts {
    pub fn count(&mut self, regime: FilterRegime) {
        let slot = match regime {
            FilterRegime::Enumerate => &mut self.enumerate,
            FilterRegime::FilterAware => &mut self.filter_aware,
            FilterRegime::Plain => &mut self.plain,
        };
        *slot = slot.saturating_add(1);
    }

    pub fn count_budget_handoff(&mut self) {
        self.enumerate = self.enumerate.saturating_add(1);
        self.budget_handoffs = self.budget_handoffs.saturating_add(1);
    }

    pub fn add(&mut self, other: FilterRegimeCounts) {
        self.enumerate = self.enumerate.saturating_add(other.enumerate);
        self.filter_aware = self.filter_aware.saturating_add(other.filter_aware);
        self.plain = self.plain.saturating_add(other.plain);
        self.budget_handoffs = self.budget_handoffs.saturating_add(other.budget_handoffs);
    }

    pub fn is_empty(&self) -> bool {
        *self == Self::default()
    }

    /// The wire form, `None` when nothing was counted.
    pub fn to_proto(self) -> Option<crate::pb::DenseFilterRegimes> {
        (!self.is_empty()).then_some(crate::pb::DenseFilterRegimes {
            enumerate: self.enumerate,
            filter_aware: self.filter_aware,
            plain: self.plain,
            budget_handoffs: self.budget_handoffs,
        })
    }

    pub fn from_proto(regimes: Option<&crate::pb::DenseFilterRegimes>) -> Self {
        regimes.map_or_else(Self::default, |r| Self {
            enumerate: r.enumerate,
            filter_aware: r.filter_aware,
            plain: r.plain,
            budget_handoffs: r.budget_handoffs,
        })
    }
}

/// Compile-time adapter contract for a vector implementation. Providers own
/// score semantics and persistence; product code owns ids, filtering, fusion,
/// distribution, and generations.
pub trait VectorProvider: Send + Sync {
    fn descriptor(&self) -> VectorBackendDescriptor;
    fn backend_config(&self) -> Result<VectorBackendConfig, VectorError>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn dimension(&self) -> Option<usize>;
    fn add(&mut self, vectors: &[f32], dimension: usize) -> Result<(), VectorError>;
    fn prepare(&mut self) -> Result<(), VectorError>;
    fn write(&self, path: &Path) -> Result<(), VectorError>;
    fn search(
        &self,
        queries: &[f32],
        k: usize,
        options: VectorSearchOptions<'_>,
    ) -> Result<VectorSearchResults, VectorError>;
    fn search_streaming_controlled(
        &self,
        queries: &[f32],
        options: VectorSearchOptions<'_>,
        sink: &mut dyn FnMut(&VectorStreamBatch<'_>) -> VectorStreamControl,
        control: &mut dyn FnMut() -> VectorStreamControl,
    ) -> Result<VectorStreamSummary, VectorError>;
    /// The segmented provider behind this index, when it is one
    /// (`src/segmented_vectors.rs`): the node seals its tail through it.
    fn as_segmented(&self) -> Option<&crate::segmented_vectors::SegmentedProvider> {
        None
    }
    /// The HNSW provider behind this index, when it is one (`src/hnsw/mod.rs`).
    fn as_hnsw(&self) -> Option<&crate::hnsw::HnswIndex> {
        None
    }
    /// Whether the provider serves from a mapped file; owned by default.
    fn is_mapped(&self) -> bool {
        false
    }
    /// Whether this image has its navigation structure built (true for scan
    /// providers and linked HNSW segments; false for an unlinked HNSW tail or
    /// tiny segment below `MIN_GRAPH_ROWS`). Segmented search runs linked
    /// parts first so their `k`-th score prunes unlinked parts.
    fn is_graph_linked(&self) -> bool {
        true
    }
    fn as_segmented_mut(&mut self) -> Option<&mut crate::segmented_vectors::SegmentedProvider> {
        None
    }
    /// The provider's stored encoding of each row in `rows`, delivered to
    /// `sink` as `(row, transcript)` in row order: the bytes its scorer
    /// reads for that row and the per-row scoring metadata kept beside
    /// them, in a fixed layout that does not depend on the row's physical
    /// position. A rewrite proof compares this by document identity
    /// (`src/segments/rewrite_proof.rs`), so equal FP32 source rows cannot
    /// stand in for it. The provider must serve the range in bounded pieces
    /// without materializing its whole image, and must leave any lazily
    /// built representation state as it found it. A provider that cannot
    /// expose its representation refuses here, and the proof refuses with
    /// it: a certificate never covers a dense image it could not read.
    fn row_transcripts(
        &self,
        _rows: Range<usize>,
        _sink: &mut dyn FnMut(usize, &[u8]),
    ) -> Result<(), VectorError> {
        Err(VectorError::new(format!(
            "vector backend {:?} does not expose its stored row encoding; a rewrite proof \
             cannot certify its dense image",
            self.descriptor().backend_kind
        )))
    }

    /// One row's transcript: [`Self::row_transcripts`] over a single row.
    fn row_transcript(&self, row: usize) -> Result<Vec<u8>, VectorError> {
        let end = row
            .checked_add(1)
            .ok_or_else(|| VectorError::new("row transcript range overflow"))?;
        let mut out = None;
        checked_row_transcripts(
            row..end,
            &mut |_, transcript| {
                out = Some(transcript.to_vec());
            },
            |rows, sink| self.row_transcripts(rows, sink),
        )?;
        out.ok_or_else(|| VectorError::new(format!("row {row} produced no transcript")))
    }

    /// Whether the provider currently holds a materialized copy of its
    /// stored rows beyond the image it serves from — for the embedded
    /// engine, its packed-row cache. `None` when the provider has no such
    /// state. A rewrite proof over a mapped image must leave this `false`.
    fn representation_materialized(&self) -> Option<bool> {
        None
    }
}

/// Rows a provider is asked for at once by the rewrite proof: bounds the
/// transcript buffer at `rows * (16 + bits * dim / 8)` bytes, 256 KiB at
/// 384 dimensions and 4 bits, 8 MiB at the engine's 16,384-dimension ceiling.
pub const TRANSCRIPT_BLOCK_ROWS: usize = 1024;

// Validate the complete transcript stream before a proof can succeed. Check
// each index before forwarding: an invalid provider must never index the
// caller's bitmap/digest array, or omit a row and leave its digest zeroed.
pub(crate) fn checked_row_transcripts(
    rows: Range<usize>,
    sink: &mut dyn FnMut(usize, &[u8]),
    emit: impl FnOnce(Range<usize>, &mut dyn FnMut(usize, &[u8])) -> Result<(), VectorError>,
) -> Result<(), VectorError> {
    if rows.start > rows.end {
        return Err(VectorError::new("row transcript range is reversed"));
    }
    let mut next = rows.start;
    let mut invalid = false;
    emit(rows.clone(), &mut |row, transcript| {
        if invalid {
            return;
        }
        if row != next || row >= rows.end {
            invalid = true;
            return;
        }
        next += 1; // row < end, so this cannot overflow even at usize::MAX.
        sink(row, transcript);
    })?;
    if invalid || next != rows.end {
        return Err(VectorError::new(
            "row transcript stream must contain every requested row exactly once in order",
        ));
    }
    Ok(())
}

/// Sized product-side handle around an arbitrary vector engine.
pub struct VectorIndex {
    engine: Box<dyn VectorProvider>,
}

impl fmt::Debug for VectorIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VectorIndex")
            .field("descriptor", &self.descriptor())
            .field("len", &self.len())
            .finish()
    }
}

impl VectorIndex {
    /// Wrap a provider supplied by another module or downstream crate.
    /// The segmented provider behind this index, when it is one.
    pub fn as_segmented(&self) -> Option<&crate::segmented_vectors::SegmentedProvider> {
        self.engine.as_segmented()
    }

    pub(crate) fn as_hnsw(&self) -> Option<&crate::hnsw::HnswIndex> {
        self.engine.as_hnsw()
    }

    pub fn as_segmented_mut(&mut self) -> Option<&mut crate::segmented_vectors::SegmentedProvider> {
        self.engine.as_segmented_mut()
    }

    pub fn from_provider(provider: impl VectorProvider + 'static) -> Self {
        Self {
            engine: Box::new(provider),
        }
    }

    /// Create the configured backend: the exhaustive embedded adapter (the
    /// default) or the HNSW graph (`docs/hnsw.md`). Unknown kinds refuse
    /// instead of silently selecting either.
    pub fn create(
        backend_kind: &str,
        dimension: usize,
        bits_per_dimension: usize,
    ) -> Result<Self, VectorError> {
        match backend_kind {
            EMBEDDED_TURBOVEC => embedded_turbovec::create(dimension, bits_per_dimension),
            crate::hnsw::HNSW => crate::hnsw::create(dimension),
            other => Err(VectorError::new(format!(
                "unknown vector backend {other:?}; available: {EMBEDDED_TURBOVEC}, {}",
                crate::hnsw::HNSW
            ))),
        }
    }

    /// Fit backend-owned construction state from a representative sample.
    /// The returned bytes can be copied unchanged into every shard manifest.
    pub fn fit_backend_config(
        backend_kind: &str,
        dimension: usize,
        bits_per_dimension: usize,
        sample: &[f32],
    ) -> Result<VectorBackendConfig, VectorError> {
        match backend_kind {
            EMBEDDED_TURBOVEC => {
                embedded_turbovec::fit_config(dimension, bits_per_dimension, sample)
            }
            // The graph scores FP32 rows and fits nothing: its shared config
            // is the default build parameters.
            crate::hnsw::HNSW => crate::hnsw::HnswConfig::default().backend_config(),
            other => Err(VectorError::new(format!(
                "cannot fit unavailable vector backend {other:?}"
            ))),
        }
    }

    /// Construct an empty generation from opaque backend state.
    pub fn from_backend_config(
        dimension: usize,
        config: &VectorBackendConfig,
    ) -> Result<Self, VectorError> {
        match config.backend_kind.as_str() {
            EMBEDDED_TURBOVEC => embedded_turbovec::from_config(dimension, config),
            crate::hnsw::HNSW => crate::hnsw::from_config(dimension, config),
            other => Err(VectorError::new(format!(
                "cannot construct unavailable vector backend {other:?}"
            ))),
        }
    }

    /// Load one backend image. The manifest or configuration chooses the
    /// backend; file extensions are never used as dispatch.
    pub fn load(backend_kind: &str, path: &Path) -> Result<Self, VectorError> {
        match backend_kind {
            EMBEDDED_TURBOVEC => embedded_turbovec::load(path),
            crate::hnsw::HNSW => crate::hnsw::load(path),
            other => Err(VectorError::new(format!(
                "cannot load unavailable vector backend {other:?}"
            ))),
        }
    }

    /// Serve one backend image from its file through a memory map
    /// (`docs/mmap-vectors.md`): the image stays on its pages and the
    /// search cache is assembled chunk by chunk as scans touch them.
    /// Scores are bit for bit those of [`Self::load`]; the index is
    /// read-only, which is what a sealed segment is.
    pub fn load_mapped(backend_kind: &str, path: &Path) -> Result<Self, VectorError> {
        match backend_kind {
            EMBEDDED_TURBOVEC => embedded_turbovec::load_mapped(path),
            crate::hnsw::HNSW => crate::hnsw::load_mapped(path),
            other => Err(VectorError::new(format!(
                "cannot load unavailable vector backend {other:?}"
            ))),
        }
    }

    /// Whether this index serves from a mapped file ([`Self::load_mapped`]).
    pub fn is_mapped(&self) -> bool {
        self.engine.is_mapped()
    }

    /// See [`VectorProvider::row_transcripts`].
    pub fn row_transcripts(
        &self,
        rows: Range<usize>,
        sink: &mut dyn FnMut(usize, &[u8]),
    ) -> Result<(), VectorError> {
        if rows.end > self.len() {
            return Err(VectorError::new(
                "row transcript range exceeds stored vectors",
            ));
        }
        checked_row_transcripts(rows, sink, |rows, sink| {
            self.engine.row_transcripts(rows, sink)
        })
    }

    /// See [`VectorProvider::row_transcript`].
    pub fn row_transcript(&self, row: usize) -> Result<Vec<u8>, VectorError> {
        let end = row
            .checked_add(1)
            .ok_or_else(|| VectorError::new("row transcript range overflow"))?;
        let mut out = None;
        self.row_transcripts(row..end, &mut |_, transcript| {
            out = Some(transcript.to_vec());
        })?;
        out.ok_or_else(|| VectorError::new(format!("row {row} produced no transcript")))
    }

    /// See [`VectorProvider::representation_materialized`].
    pub fn representation_materialized(&self) -> Option<bool> {
        self.engine.representation_materialized()
    }

    pub fn descriptor(&self) -> VectorBackendDescriptor {
        self.engine.descriptor()
    }

    pub fn backend_config(&self) -> Result<VectorBackendConfig, VectorError> {
        self.engine.backend_config()
    }

    pub fn len(&self) -> usize {
        self.engine.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_graph_linked(&self) -> bool {
        self.engine.is_graph_linked()
    }

    /// The row ranges that hold vectors, `(base, rows)` in row order. A
    /// single image is one range over its length; the segment layout
    /// lists its parts with an image, the frozen image, and the tail, so
    /// a sealed document-only segment's rows are absent.
    pub fn vector_rows(&self) -> Vec<(usize, usize)> {
        match self.as_segmented() {
            Some(segmented) => segmented.vector_rows(),
            None => vec![(0, self.len())],
        }
    }

    pub fn dimension(&self) -> Option<usize> {
        self.engine.dimension()
    }

    pub fn dim_opt(&self) -> Option<usize> {
        self.dimension()
    }

    pub fn bits_per_dimension(&self) -> Option<usize> {
        self.descriptor()
            .bits_per_dimension
            .map(|bits| bits as usize)
    }

    pub fn add(&mut self, vectors: &[f32], dimension: usize) -> Result<(), VectorError> {
        self.engine.add(vectors, dimension)
    }

    pub fn prepare(&mut self) -> Result<(), VectorError> {
        self.engine.prepare()
    }

    pub fn write(&self, path: &Path) -> Result<(), VectorError> {
        self.engine.write(path)
    }

    pub fn try_search(
        &self,
        queries: &[f32],
        k: usize,
        options: VectorSearchOptions<'_>,
    ) -> Result<VectorSearchResults, VectorError> {
        self.engine.search(queries, k, options)
    }

    pub fn search(
        &self,
        queries: &[f32],
        k: usize,
        options: VectorSearchOptions<'_>,
    ) -> VectorSearchResults {
        self.try_search(queries, k, options)
            .unwrap_or_else(|e| panic!("vector backend search failed: {e}"))
    }

    pub fn search_with_options(
        &self,
        queries: &[f32],
        k: usize,
        options: VectorSearchOptions<'_>,
    ) -> VectorSearchResults {
        self.search(queries, k, options)
    }

    pub fn search_unfiltered(&self, queries: &[f32], k: usize) -> VectorSearchResults {
        self.search(queries, k, VectorSearchOptions::new())
    }

    pub fn try_search_with_mask(
        &self,
        queries: &[f32],
        k: usize,
        allow: Option<&[bool]>,
    ) -> Result<VectorSearchResults, VectorError> {
        let mut options = VectorSearchOptions::new();
        options.allow = allow.map(Mask::Bools);
        self.try_search(queries, k, options)
    }

    pub fn try_search_streaming_controlled<F, C>(
        &self,
        queries: &[f32],
        options: VectorSearchOptions<'_>,
        mut sink: F,
        mut control: C,
    ) -> Result<VectorStreamSummary, VectorError>
    where
        F: FnMut(&VectorStreamBatch<'_>) -> VectorStreamControl,
        C: FnMut() -> VectorStreamControl,
    {
        self.engine
            .search_streaming_controlled(queries, options, &mut sink, &mut control)
    }
}

/// Product-wide coordinate validation. It deliberately does not expose a
/// backend helper: every provider receives the same finite, bounded f32
/// contract.
pub fn first_invalid_coordinate(values: &[f32], dimension: usize) -> Option<(usize, usize, f32)> {
    if dimension == 0 {
        return None;
    }
    values
        .iter()
        .copied()
        .enumerate()
        .find_map(|(flat, value)| {
            (!value.is_finite() || value.abs() >= 1.0e16).then_some((
                flat / dimension,
                flat % dimension,
                value,
            ))
        })
}

mod embedded_turbovec {
    use super::*;
    use turbovec::{CalibrationState, SearchOptions, StreamControl, TurboQuantIndex};

    const BACKEND_VERSION: &str = "1.0.0";
    const CONFIG_FORMAT: &str = "application/vnd.ai-pipestream.turbovec-config+json;version=1";

    #[derive(Debug, Serialize, Deserialize)]
    struct ConfigPayload {
        bits_per_dimension: usize,
        shift: Vec<f32>,
        scale: Vec<f32>,
    }

    pub(super) fn config(
        bits_per_dimension: usize,
        shift: &[f32],
        scale: &[f32],
    ) -> Result<VectorBackendConfig, VectorError> {
        let payload = ConfigPayload {
            bits_per_dimension,
            shift: shift.to_vec(),
            scale: scale.to_vec(),
        };
        Ok(VectorBackendConfig {
            backend_kind: EMBEDDED_TURBOVEC.to_string(),
            config_format: CONFIG_FORMAT.to_string(),
            payload: serde_json::to_vec(&payload).map_err(|e| VectorError::new(e.to_string()))?,
        })
    }

    pub(super) fn decode_config(
        config: &VectorBackendConfig,
    ) -> Result<LegacyCalibrationConfig, VectorError> {
        if config.config_format != CONFIG_FORMAT {
            return Err(VectorError::new(format!(
                "unsupported {EMBEDDED_TURBOVEC} config format {:?}",
                config.config_format
            )));
        }
        let payload: ConfigPayload = serde_json::from_slice(&config.payload)
            .map_err(|e| VectorError::new(format!("invalid backend config: {e}")))?;
        Ok(LegacyCalibrationConfig {
            bits_per_dimension: payload.bits_per_dimension,
            shift: payload.shift,
            scale: payload.scale,
        })
    }

    pub(super) fn create(
        dimension: usize,
        bits_per_dimension: usize,
    ) -> Result<VectorIndex, VectorError> {
        let index = TurboQuantIndex::new(dimension, bits_per_dimension)
            .map_err(|e| VectorError::new(e.to_string()))?;
        Ok(wrap(index))
    }

    pub(super) fn fit_config(
        dimension: usize,
        bits_per_dimension: usize,
        sample: &[f32],
    ) -> Result<VectorBackendConfig, VectorError> {
        let mut index = TurboQuantIndex::new(dimension, bits_per_dimension)
            .map_err(|e| VectorError::new(e.to_string()))?;
        index
            .calibrate(sample)
            .map_err(|e| VectorError::new(e.to_string()))?;
        wrap(index).backend_config()
    }

    pub(super) fn from_config(
        dimension: usize,
        config: &VectorBackendConfig,
    ) -> Result<VectorIndex, VectorError> {
        let payload = decode_config(config)?;
        let index = TurboQuantIndex::from_parts(
            Some(dimension),
            payload.bits_per_dimension,
            0,
            Vec::new(),
            Vec::new(),
            payload.shift,
            payload.scale,
        )
        .map_err(|e| VectorError::new(e.to_string()))?;
        Ok(wrap(index))
    }

    pub(super) fn load(path: &Path) -> Result<VectorIndex, VectorError> {
        TurboQuantIndex::load(path)
            .map(wrap)
            .map_err(|e| VectorError::new(format!("load {}: {e}", path.display())))
    }

    pub(super) fn load_mapped(path: &Path) -> Result<VectorIndex, VectorError> {
        // SAFETY: this path is reached only for immutable vector artifacts in
        // a sealed segment generation. Writers publish a different generation
        // and never modify or truncate this image while the segment is open.
        unsafe { TurboQuantIndex::load_mapped(path) }
            .map(wrap)
            .map_err(|e| VectorError::new(format!("map {}: {e}", path.display())))
    }

    fn wrap(index: TurboQuantIndex) -> VectorIndex {
        VectorIndex {
            engine: Box::new(EmbeddedTurboVec { index }),
        }
    }

    struct EmbeddedTurboVec {
        index: TurboQuantIndex,
    }

    impl EmbeddedTurboVec {
        fn payload(&self) -> ConfigPayload {
            let (shift, scale) = match self.index.calibration_state() {
                CalibrationState::Calibrated => (
                    self.index.tqplus_shift().to_vec(),
                    self.index.tqplus_scale().to_vec(),
                ),
                _ => (Vec::new(), Vec::new()),
            };
            ConfigPayload {
                bits_per_dimension: self.index.bit_width(),
                shift,
                scale,
            }
        }

        /// `allow` is the search's mask in the kernel's shape
        /// ([`Mask::bools`]), held by the caller for the search.
        fn options<'a>(
            options: VectorSearchOptions<'_>,
            allow: Option<&'a [bool]>,
        ) -> SearchOptions<'a> {
            // This provider advertises `ExhaustiveQuantized`: every top-k
            // is the whole-index scan's, bit for bit. On a bit-plane cache
            // (32,768+ rows on aarch64 or AVX-512 VBMI + VNNI hosts) turbovec
            // would otherwise search in stages, with an approximate id set
            // that also depends on the caller's thread count.
            let mut native = SearchOptions::new().with_exhaustive(true);
            if let Some(allow) = allow {
                native = native.with_mask(allow);
            }
            if let Some(score) = options.minimum_score {
                native = native.with_initial_threshold(score);
            }
            native
        }

        fn map_control(control: VectorStreamControl) -> StreamControl {
            match control {
                VectorStreamControl::Continue | VectorStreamControl::Finish { .. } => {
                    StreamControl::Continue
                }
                VectorStreamControl::RaiseFloor(floor) => StreamControl::RaiseFloor(floor),
                VectorStreamControl::Stop => StreamControl::Stop,
            }
        }
    }

    impl VectorProvider for EmbeddedTurboVec {
        fn is_mapped(&self) -> bool {
            self.index.is_mapped()
        }

        /// Bit width, dimension, the row's bit-plane packed codes and its
        /// correction scale bits, read through `TurboQuantIndex::stored_rows`
        /// in pieces of at most [`TRANSCRIPT_BLOCK_ROWS`] rows. The engine
        /// converts one 32-row block at a time from the layout it already
        /// serves, so a loaded or mapped image is never materialized as
        /// packed rows and `packed_ready()` is unchanged. Each piece is
        /// checked against the `bits * dim / 8` row stride before it is
        /// sliced, so a layout the engine changes under this adapter refuses
        /// instead of certifying the wrong bytes.
        fn row_transcripts(
            &self,
            rows: Range<usize>,
            sink: &mut dyn FnMut(usize, &[u8]),
        ) -> Result<(), VectorError> {
            let total = self.index.len();
            if rows.end > total || rows.start > rows.end {
                return Err(VectorError::new(format!(
                    "rows {rows:?} are outside the {total} stored vectors"
                )));
            }
            let dim = self
                .index
                .dim_opt()
                .ok_or_else(|| VectorError::new("vector dimension is absent"))?;
            let bits = self.index.bit_width();
            let stride = match (dim % 8, bits.checked_mul(dim)) {
                (0, Some(bits_per_row)) if bits_per_row > 0 => bits_per_row / 8,
                _ => {
                    return Err(VectorError::new(format!(
                        "packed row layout is undefined for dimension {dim} at {bits} bits"
                    )))
                }
            };
            let mut codes = Vec::new();
            let mut scales = Vec::new();
            let mut out = Vec::with_capacity(16 + stride);
            let mut start = rows.start;
            while start < rows.end {
                let end = rows.end.min(start + TRANSCRIPT_BLOCK_ROWS);
                self.index
                    .stored_rows(start..end, &mut codes, &mut scales)
                    .map_err(|e| VectorError::new(e.to_string()))?;
                if codes.len() != (end - start) * stride || scales.len() != end - start {
                    return Err(VectorError::new(format!(
                        "stored rows {start}..{end} came back as {} code bytes and {} scales \
                         for a {stride}-byte row stride; the layout differs from the adapter's",
                        codes.len(),
                        scales.len()
                    )));
                }
                for (i, row) in (start..end).enumerate() {
                    out.clear();
                    out.extend_from_slice(&(bits as u32).to_le_bytes());
                    out.extend_from_slice(&(dim as u64).to_le_bytes());
                    out.extend_from_slice(&codes[i * stride..(i + 1) * stride]);
                    out.extend_from_slice(&scales[i].to_bits().to_le_bytes());
                    sink(row, &out);
                }
                start = end;
            }
            Ok(())
        }

        fn representation_materialized(&self) -> Option<bool> {
            Some(self.index.packed_ready())
        }

        fn descriptor(&self) -> VectorBackendDescriptor {
            let config = self
                .backend_config()
                .expect("serializing the embedded backend config cannot fail");
            let dimension = (self.index.dim_opt().unwrap_or(0) as u64).to_le_bytes();
            VectorBackendDescriptor {
                backend_kind: EMBEDDED_TURBOVEC.to_string(),
                backend_version: BACKEND_VERSION.to_string(),
                dimension: self.index.dim_opt(),
                bits_per_dimension: Some(self.index.bit_width() as u32),
                metric: "inner_product".to_string(),
                score_direction: ScoreDirection::HigherIsBetter,
                scoring_fingerprint: crate::sha256::hex_digest(
                    [
                        EMBEDDED_TURBOVEC.as_bytes(),
                        BACKEND_VERSION.as_bytes(),
                        dimension.as_slice(),
                        config.config_format.as_bytes(),
                        config.payload.as_slice(),
                    ]
                    .concat()
                    .as_slice(),
                ),
                quality_contract: QualityContract::ExhaustiveQuantized,
                capabilities: vec![
                    VectorCapability::BatchQuery,
                    VectorCapability::CandidateStream,
                    VectorCapability::LiveBoundInput,
                    VectorCapability::DenseMask,
                    VectorCapability::CandidateRescore,
                    VectorCapability::Append,
                    VectorCapability::Flush,
                    VectorCapability::SnapshotInstall,
                    VectorCapability::RawVectorRebuild,
                    VectorCapability::ExhaustiveCompletion,
                ],
            }
        }

        fn backend_config(&self) -> Result<VectorBackendConfig, VectorError> {
            Ok(VectorBackendConfig {
                backend_kind: EMBEDDED_TURBOVEC.to_string(),
                config_format: CONFIG_FORMAT.to_string(),
                payload: serde_json::to_vec(&self.payload())
                    .map_err(|e| VectorError::new(e.to_string()))?,
            })
        }

        fn len(&self) -> usize {
            self.index.len()
        }

        fn dimension(&self) -> Option<usize> {
            self.index.dim_opt()
        }

        fn add(&mut self, vectors: &[f32], dimension: usize) -> Result<(), VectorError> {
            self.index
                .add_2d(vectors, dimension)
                .map_err(|e| VectorError::new(e.to_string()))
        }

        fn prepare(&mut self) -> Result<(), VectorError> {
            self.index.prepare();
            Ok(())
        }

        fn write(&self, path: &Path) -> Result<(), VectorError> {
            self.index
                .write(path)
                .map_err(|e| VectorError::new(e.to_string()))
        }

        fn search(
            &self,
            queries: &[f32],
            k: usize,
            options: VectorSearchOptions<'_>,
        ) -> Result<VectorSearchResults, VectorError> {
            let allow = options.allow.map(|mask| mask.bools());
            let result = self
                .index
                .try_search_with_options(queries, k, Self::options(options, allow.as_deref()))
                .map_err(|e| VectorError::new(e.to_string()))?;
            Ok(VectorSearchResults {
                scores: result.scores,
                slots: result.indices,
                query_count: result.nq,
                result_count: result.k,
                regimes: FilterRegimeCounts::default(),
                tail_enumerated_rows: 0,
                patience_stopped: 0,
                floor_pruned: 0,
            })
        }

        fn search_streaming_controlled(
            &self,
            queries: &[f32],
            options: VectorSearchOptions<'_>,
            sink: &mut dyn FnMut(&VectorStreamBatch<'_>) -> VectorStreamControl,
            control: &mut dyn FnMut() -> VectorStreamControl,
        ) -> Result<VectorStreamSummary, VectorError> {
            let allow = options.allow.map(|mask| mask.bools());
            let summary = self
                .index
                .try_search_streaming_controlled(
                    queries,
                    Self::options(options, allow.as_deref()),
                    |batch| {
                        Self::map_control(sink(&VectorStreamBatch {
                            query_index: batch.query_index,
                            block_base: batch.block_base,
                            scores: batch.scores,
                            slots: batch.slots,
                        }))
                    },
                    || Self::map_control(control()),
                )
                .map_err(|e| VectorError::new(e.to_string()))?;
            Ok(VectorStreamSummary {
                query_count: summary.nq,
                emitted: summary.emitted,
                units_scanned: summary.blocks_scanned,
                completed: summary.completed,
                end_reason: if summary.completed {
                    VectorStreamEndReason::Exhausted
                } else {
                    VectorStreamEndReason::Abandoned
                },
                regimes: FilterRegimeCounts::default(),
                tail_enumerated_rows: 0,
                patience_stopped: 0,
                floor_pruned: 0,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeProvider;

    impl VectorProvider for FakeProvider {
        fn descriptor(&self) -> VectorBackendDescriptor {
            VectorBackendDescriptor {
                backend_kind: "fake".into(),
                backend_version: "test".into(),
                dimension: Some(2),
                bits_per_dimension: None,
                metric: "inner_product".into(),
                score_direction: ScoreDirection::HigherIsBetter,
                scoring_fingerprint: "fake-score-v1".into(),
                quality_contract: QualityContract::ConfiguredAnn,
                capabilities: vec![VectorCapability::BatchQuery],
            }
        }

        fn backend_config(&self) -> Result<VectorBackendConfig, VectorError> {
            Ok(VectorBackendConfig {
                backend_kind: "fake".into(),
                config_format: "test/fake".into(),
                payload: vec![1, 2, 3],
            })
        }

        fn len(&self) -> usize {
            1
        }

        fn dimension(&self) -> Option<usize> {
            Some(2)
        }

        fn add(&mut self, _vectors: &[f32], _dimension: usize) -> Result<(), VectorError> {
            Ok(())
        }

        fn prepare(&mut self) -> Result<(), VectorError> {
            Ok(())
        }

        fn write(&self, _path: &Path) -> Result<(), VectorError> {
            Ok(())
        }

        fn search(
            &self,
            _queries: &[f32],
            k: usize,
            _options: VectorSearchOptions<'_>,
        ) -> Result<VectorSearchResults, VectorError> {
            Ok(VectorSearchResults {
                scores: vec![0.75; k],
                slots: vec![0; k],
                query_count: 1,
                result_count: k,
                regimes: FilterRegimeCounts::default(),
                tail_enumerated_rows: 0,
                patience_stopped: 0,
                floor_pruned: 0,
            })
        }

        fn search_streaming_controlled(
            &self,
            _queries: &[f32],
            _options: VectorSearchOptions<'_>,
            _sink: &mut dyn FnMut(&VectorStreamBatch<'_>) -> VectorStreamControl,
            _control: &mut dyn FnMut() -> VectorStreamControl,
        ) -> Result<VectorStreamSummary, VectorError> {
            Ok(VectorStreamSummary {
                query_count: 1,
                emitted: 0,
                units_scanned: 0,
                completed: true,
                end_reason: VectorStreamEndReason::Exhausted,
                regimes: FilterRegimeCounts::default(),
                tail_enumerated_rows: 0,
                patience_stopped: 0,
                floor_pruned: 0,
            })
        }
    }

    fn scripted_transcripts(
        rows: Range<usize>,
        emitted: &[usize],
    ) -> (Result<(), VectorError>, Vec<usize>) {
        let requested = rows.clone();
        let mut forwarded = Vec::new();
        let result =
            checked_row_transcripts(rows, &mut |row, _| forwarded.push(row), |seen, sink| {
                assert_eq!(seen, requested);
                for &row in emitted {
                    sink(row, &[row as u8]);
                }
                Ok(())
            });
        (result, forwarded)
    }

    #[test]
    #[allow(clippy::reversed_empty_ranges)]
    fn checked_transcripts_require_each_requested_row_once_in_order() {
        let (result, forwarded) = scripted_transcripts(3..6, &[3, 4, 5]);
        result.unwrap();
        assert_eq!(forwarded, [3, 4, 5]);

        for (emitted, expected_forwarded) in [
            (&[][..], &[][..]),
            (&[3, 4][..], &[3, 4][..]),
            (&[3, 3, 4, 5][..], &[3][..]),
            (&[3, 5, 4][..], &[3][..]),
            (&[2, 3, 4, 5][..], &[][..]),
            (&[3, 4, 5, 6][..], &[3, 4, 5][..]),
        ] {
            let (result, forwarded) = scripted_transcripts(3..6, emitted);
            assert!(result.is_err(), "accepted {emitted:?}");
            assert_eq!(
                forwarded, expected_forwarded,
                "forwarded invalid callback from {emitted:?}"
            );
        }

        let (result, forwarded) = scripted_transcripts(4..4, &[]);
        result.unwrap();
        assert!(forwarded.is_empty());
        assert!(scripted_transcripts(6..3, &[]).0.is_err());

        let mut forwarded = Vec::new();
        let error = checked_row_transcripts(0..1, &mut |row, _| forwarded.push(row), |_, _| {
            Err(VectorError::new("scripted provider failure"))
        })
        .unwrap_err();
        assert!(
            error.to_string().contains("scripted provider failure"),
            "{error}"
        );
        assert!(forwarded.is_empty());
    }

    #[test]
    fn one_row_transcript_refuses_usize_max_without_wrapping() {
        let error = VectorIndex::from_provider(FakeProvider)
            .row_transcript(usize::MAX)
            .unwrap_err();
        assert!(error.to_string().contains("overflow"), "{error}");
    }

    #[test]
    fn unknown_backend_refuses() {
        let err = VectorIndex::create("surprise", 64, 4).unwrap_err();
        assert!(err.to_string().contains("unknown vector backend"));
    }

    #[test]
    fn product_handle_accepts_a_non_turbovec_provider() {
        let index = VectorIndex::from_provider(FakeProvider);
        assert_eq!(index.descriptor().backend_kind, "fake");
        assert_eq!(
            index.descriptor().quality_contract,
            QualityContract::ConfiguredAnn
        );
        assert_eq!(
            index.search_unfiltered(&[1.0, 0.0], 2).scores,
            vec![0.75; 2]
        );
    }

    #[test]
    fn embedded_config_round_trips_without_exposing_native_types() {
        let index = VectorIndex::create(EMBEDDED_TURBOVEC, 64, 4).unwrap();
        let config = index.backend_config().unwrap();
        let rebuilt = VectorIndex::from_backend_config(64, &config).unwrap();
        assert_eq!(rebuilt.descriptor(), index.descriptor());
        assert_eq!(rebuilt.len(), 0);
    }

    #[test]
    fn descriptor_and_search_contract_are_backend_neutral() {
        let mut index = VectorIndex::create(EMBEDDED_TURBOVEC, 64, 4).unwrap();
        let vectors: Vec<f32> = (0..8 * 64)
            .map(|i| ((i % 31) as f32 - 15.0) / 31.0)
            .collect();
        index.add(&vectors, 64).unwrap();
        let descriptor = index.descriptor();
        assert_eq!(
            descriptor.quality_contract,
            QualityContract::ExhaustiveQuantized
        );
        assert!(descriptor
            .capabilities
            .contains(&VectorCapability::DenseMask));

        let result = index.search(&vectors[..64], 3, VectorSearchOptions::new());
        assert_eq!(result.query_count, 1);
        assert_eq!(result.result_count, 3);
        assert_eq!(result.slots_for_query(0).len(), 3);
    }

    /// The embedded adapter's row transcript is the stored encoding: it
    /// follows a row through reordering, differs when the stored codes do
    /// even with equal source rows, and is the same whether the image is
    /// heap-loaded or mapped. A provider without a representation refuses
    /// by name instead of certifying nothing.
    #[test]
    fn row_transcript_is_the_stored_encoding_and_refuses_without_one() {
        let dim = 64;
        let rows: Vec<Vec<f32>> = (0..4)
            .map(|r| {
                (0..dim)
                    .map(|i| ((i * (r + 3)) % 17) as f32 / 17.0)
                    .collect()
            })
            .collect();
        let build = |order: &[usize]| {
            let mut index = VectorIndex::create(EMBEDDED_TURBOVEC, dim, 4).unwrap();
            let flat: Vec<f32> = order.iter().flat_map(|&r| rows[r].clone()).collect();
            index.add(&flat, dim).unwrap();
            index.prepare().unwrap();
            index
        };
        let forward = build(&[0, 1, 2, 3]);
        let reversed = build(&[3, 2, 1, 0]);
        for r in 0..4 {
            let transcript = forward.row_transcript(r).unwrap();
            assert_eq!(transcript.len(), 16 + 4 * dim / 8);
            assert_eq!(transcript, reversed.row_transcript(3 - r).unwrap());
        }
        assert_ne!(
            forward.row_transcript(0).unwrap(),
            forward.row_transcript(1).unwrap()
        );
        assert!(forward.row_transcript(4).is_err());

        // A different calibration stores different codes for equal rows.
        let sample: Vec<f32> = (0..8 * dim)
            .map(|i| ((i % 13) as f32 - 6.0) / 6.0)
            .collect();
        let calibrated =
            VectorIndex::fit_backend_config(EMBEDDED_TURBOVEC, dim, 4, &sample).unwrap();
        let mut other = VectorIndex::from_backend_config(dim, &calibrated).unwrap();
        other.add(&rows.concat(), dim).unwrap();
        other.prepare().unwrap();
        assert_ne!(
            other.row_transcript(0).unwrap(),
            forward.row_transcript(0).unwrap()
        );

        let dir = std::env::temp_dir().join(format!("row-transcript-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("image");
        forward.write(&path).unwrap();
        let loaded = VectorIndex::load(EMBEDDED_TURBOVEC, &path).unwrap();
        let mapped = VectorIndex::load_mapped(EMBEDDED_TURBOVEC, &path).unwrap();
        for r in 0..4 {
            let expected = forward.row_transcript(r).unwrap();
            assert_eq!(loaded.row_transcript(r).unwrap(), expected);
            assert_eq!(mapped.row_transcript(r).unwrap(), expected);
        }
        std::fs::remove_dir_all(&dir).unwrap();

        let error = VectorIndex::from_provider(FakeProvider)
            .row_transcript(0)
            .unwrap_err();
        assert!(error.to_string().contains("cannot certify"), "{error}");
    }

    /// A sealed image of 32,768+ rows: on hosts whose kernels read
    /// turbovec's bit-plane layout (aarch64, x86 with AVX-512 VBMI + VNNI)
    /// it is written as a planes v8 file, which `load_mapped` must still
    /// serve, since sealed segments map by default (`vector_mmap`).
    #[test]
    fn a_sealed_image_past_the_planes_threshold_serves_mapped() {
        let dim = 64;
        let n = 40_000;
        let mut state = 0x9E37_79B9_7F4A_7C15u64;
        let rows: Vec<f32> = (0..n * dim)
            .map(|_| {
                state = state
                    .wrapping_mul(6_364_136_223_846_793_005)
                    .wrapping_add(1_442_695_040_888_963_407);
                ((state >> 33) as f32 / (1u64 << 30) as f32) - 1.0
            })
            .collect();
        let dir = std::env::temp_dir().join(format!("planes-mapped-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        for bits in [2, 4] {
            let mut index = VectorIndex::create(EMBEDDED_TURBOVEC, dim, bits).unwrap();
            index.add(&rows, dim).unwrap();
            index.prepare().unwrap();
            let path = dir.join(format!("image-{bits}"));
            index.write(&path).unwrap();
            let loaded = VectorIndex::load(EMBEDDED_TURBOVEC, &path).unwrap();
            let mut mapped = VectorIndex::load_mapped(EMBEDDED_TURBOVEC, &path)
                .unwrap_or_else(|e| panic!("{bits}-bit image did not map: {e}"));
            mapped.prepare().unwrap();
            assert!(mapped.is_mapped());
            assert_eq!(mapped.len(), n);
            for r in [0, 31, 32, 20_000, n - 1] {
                assert_eq!(
                    mapped.row_transcript(r).unwrap(),
                    loaded.row_transcript(r).unwrap(),
                    "{bits}-bit row {r}"
                );
            }
            let hits = mapped.search_unfiltered(&rows[..dim], 10);
            assert_eq!(hits.slots_for_query(0).len(), 10);
            assert_eq!(hits.slots_for_query(0)[0], 0, "{bits}-bit self match");
        }
        std::fs::remove_dir_all(&dir).unwrap();
    }
}
