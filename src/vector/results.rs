//! Search results, streaming batches and summaries, and regime counters.

use super::FilterRegime;

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
    /// Layer-0 graph walks that ended early via patience saturation.
    pub patience_stopped: u32,
    /// Layer-0 graph walks that pruned candidates via the active score floor.
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
    /// End an approximate walk gracefully at the next hop boundary,
    /// flushing buffered candidates at or above the floor (`completed = true`).
    /// Ignored by exhaustive scan providers.
    Finish {
        deadline: bool,
    },
    Stop,
}

/// Why a streaming provider ended its traversal.
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
    /// graceful finish.
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
    /// Layer-0 graph walks that ended early via patience saturation.
    pub patience_stopped: u32,
    /// Layer-0 graph walks that pruned candidates via the active score floor.
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
