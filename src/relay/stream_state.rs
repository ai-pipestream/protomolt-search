//! The state one relayed `StreamSearch` carries: its arguments, the
//! per-child bookkeeping, and the sums of the children's summaries.

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use tokio::sync::{mpsc, watch};
use tokio::time::Instant;
use tonic::Status;

use crate::coordinator::{CoordinatorServiceImpl, RequestFilters};
use crate::pb::{stream_search_response, StreamSearchResponse, StreamSearchSummary};

use super::map::MapSnapshot;
use super::signals::RelaySignals;

/// The parent's selection lane: `Ok(Some(..))` carries the identities
/// the parent picked, `Ok(None)` a lane that ended without one.
pub(super) type SelectionTask =
    tokio::task::JoinHandle<Result<Option<crate::pb::ResolveStreamIdentities>, Status>>;

/// Everything one relayed `StreamSearch` runs on.
pub(super) struct StreamArgs {
    pub(super) coordinator: CoordinatorServiceImpl,
    pub(super) pinned: MapSnapshot,
    pub(super) map_changes: watch::Receiver<u64>,
    pub(super) start: crate::pb::StartStreamSearch,
    pub(super) filters: RequestFilters,
    pub(super) signals: Arc<RelaySignals>,
    pub(super) tx: mpsc::Sender<Result<StreamSearchResponse, Status>>,
    pub(super) deadline: Option<Instant>,
    pub(super) input: SelectionTask,
    pub(super) identity_ready: Arc<AtomicBool>,
}

/// Sums carried across the children's terminal summaries.
#[derive(Default)]
pub(super) struct StreamTotals {
    emitted: u64,
    blocks_scanned: u64,
    floor_raises_applied: u64,
    segments_total: u32,
    segments_skipped: u32,
    regimes: crate::vector::FilterRegimeCounts,
    tail_enumerated_rows: u64,
    patience_stopped: u32,
    floor_pruned: u32,
    parts_total: u32,
    parts_skipped: u32,
    parts_walked: u32,
    parts_enumerated: u32,
    parts_scanned: u32,
    parts_staged: u32,
    fp32_rows_scored: u64,
    fp32_logical_bytes: u64,
    fp32_pages_touched: u64,
    fp32_tasks: u32,
    missing_parts: bool,
    pub(super) end_reason: crate::vector::VectorStreamEndReason,
}

impl StreamTotals {
    fn add(&mut self, summary: &StreamSearchSummary) {
        self.emitted += summary.emitted;
        self.blocks_scanned += summary.blocks_scanned;
        self.floor_raises_applied += summary.floor_raises_applied;
        self.segments_total = self.segments_total.saturating_add(summary.segments_total);
        self.segments_skipped = self
            .segments_skipped
            .saturating_add(summary.segments_skipped);
        self.regimes
            .add(crate::vector::FilterRegimeCounts::from_proto(
                summary.filter_regimes.as_ref(),
            ));
        self.tail_enumerated_rows = self
            .tail_enumerated_rows
            .saturating_add(summary.tail_enumerated_rows);
        self.patience_stopped = self
            .patience_stopped
            .saturating_add(summary.patience_stopped);
        self.floor_pruned = self.floor_pruned.saturating_add(summary.floor_pruned);
        self.parts_total = self.parts_total.saturating_add(summary.parts_total);
        self.parts_skipped = self.parts_skipped.saturating_add(summary.parts_skipped);
        self.parts_walked = self.parts_walked.saturating_add(summary.parts_walked);
        self.parts_enumerated = self
            .parts_enumerated
            .saturating_add(summary.parts_enumerated);
        self.parts_scanned = self.parts_scanned.saturating_add(summary.parts_scanned);
        self.parts_staged = self.parts_staged.saturating_add(summary.parts_staged);
        self.fp32_rows_scored = self
            .fp32_rows_scored
            .saturating_add(summary.fp32_rows_scored);
        self.fp32_logical_bytes = self
            .fp32_logical_bytes
            .saturating_add(summary.fp32_logical_bytes);
        self.fp32_pages_touched = self
            .fp32_pages_touched
            .saturating_add(summary.fp32_pages_touched);
        self.fp32_tasks = self.fp32_tasks.saturating_add(summary.fp32_tasks);
        self.end_reason = self
            .end_reason
            .merge(crate::vector::VectorStreamEndReason::from_proto(
                summary.end_reason,
            ));
    }
}

/// The per-child bookkeeping of the forwarding loop.
pub(super) struct ForwardState {
    pub(super) summaries: Vec<Option<StreamSearchSummary>>,
    pub(super) terminal: Vec<Option<StreamSearchSummary>>,
    pub(super) ranges: Vec<Option<crate::pb::StreamIdentityRange>>,
    pub(super) remaining: usize,
    pub(super) identity: bool,
    pub(super) stride: usize,
    child_nonempty: Vec<bool>,
    exact_fp32: bool,
    geo_known: Vec<bool>,
    tree_known: Vec<bool>,
    pub(super) totals: StreamTotals,
    scoring_fingerprint: Option<String>,
}

impl ForwardState {
    pub(super) fn new(
        args: &StreamArgs,
        mask: Option<&crate::placement::ShardMask>,
        n: usize,
    ) -> Self {
        let mut summaries: Vec<Option<StreamSearchSummary>> = vec![None; n];
        let mut remaining = n;
        for (shard, summary) in summaries.iter_mut().enumerate() {
            if mask.is_some_and(|m| m.skipped[shard]) {
                *summary = Some(StreamSearchSummary {
                    completed: true,
                    ..Default::default()
                });
                remaining -= 1;
            }
        }
        let child_nonempty = (0..n)
            .map(|i| args.coordinator.shard_is_nonempty(i))
            .collect();
        let known = super::merge_util::KnownFlags::new(&args.filters, mask);
        ForwardState {
            terminal: summaries.clone(),
            summaries,
            ranges: vec![None; n],
            remaining,
            identity: args.start.identity_limits.is_some(),
            stride: if args.start.collapse_parents { 20 } else { 12 },
            child_nonempty,
            exact_fp32: args.start.exact_fp32_scan,
            geo_known: known.geo,
            tree_known: known.tree,
            totals: StreamTotals::default(),
            scoring_fingerprint: None,
        }
    }

    /// The relay's own summary over what the children certified so far.
    pub(super) fn summary(&self, completed: bool, end_reason: i32) -> StreamSearchSummary {
        let t = &self.totals;
        let keep_parts = !t.missing_parts;
        StreamSearchSummary {
            completed,
            emitted: t.emitted,
            blocks_scanned: t.blocks_scanned,
            floor_raises_applied: t.floor_raises_applied,
            geo_columns_known: self.geo_known.clone(),
            filter_columns_known: self.tree_known.clone(),
            // Every child agreed, or the loop refused; a relay over
            // masked-out children only has no fingerprint to report and
            // the parent refuses that, as it refuses a node without one.
            scoring_fingerprint: self.scoring_fingerprint.clone().unwrap_or_default(),
            segments_total: t.segments_total,
            segments_skipped: t.segments_skipped,
            filter_regimes: t.regimes.to_proto(),
            tail_enumerated_rows: t.tail_enumerated_rows,
            patience_stopped: t.patience_stopped,
            end_reason,
            floor_pruned: t.floor_pruned,
            parts_total: if keep_parts { t.parts_total } else { 0 },
            parts_skipped: if keep_parts { t.parts_skipped } else { 0 },
            parts_walked: if keep_parts { t.parts_walked } else { 0 },
            parts_enumerated: if keep_parts { t.parts_enumerated } else { 0 },
            parts_scanned: if keep_parts { t.parts_scanned } else { 0 },
            parts_staged: if keep_parts { t.parts_staged } else { 0 },
            fp32_rows_scored: t.fp32_rows_scored,
            fp32_logical_bytes: t.fp32_logical_bytes,
            fp32_pages_touched: t.fp32_pages_touched,
            fp32_tasks: t.fp32_tasks,
        }
    }

    /// A child's readiness certificate in identity mode: its captured
    /// range must be valid and disjoint from every other child's.
    pub(super) fn accept_ready(
        &mut self,
        shard: usize,
        ready: crate::pb::StreamIdentityReady,
    ) -> Result<StreamSearchSummary, Status> {
        let summary = ready
            .scan
            .ok_or_else(|| Status::internal("relay: missing readiness certificate"))?;
        if let Some(range) = ready.range.as_ref() {
            if range.first_id > range.last_id
                || self
                    .ranges
                    .iter()
                    .flatten()
                    .any(|other| range.first_id <= other.last_id && other.first_id <= range.last_id)
            {
                return Err(Status::failed_precondition(
                    "relay: overlapping or invalid captured identity ranges",
                ));
            }
        } else if summary.emitted != 0 {
            return Err(Status::failed_precondition(
                "relay: candidates without a captured identity range",
            ));
        }
        self.ranges[shard] = ready.range;
        Ok(summary)
    }

    /// A child's terminal summary: completed, fingerprinted the same as
    /// every other child, flags sized as the request; then summed.
    pub(super) fn accept_summary(
        &mut self,
        shard: usize,
        summary: &StreamSearchSummary,
    ) -> Result<(), Status> {
        if !summary.completed {
            return Err(Status::internal(format!(
                "relay: child {shard} stopped before completing its scan"
            )));
        }
        if summary.scoring_fingerprint.is_empty() {
            return Err(Status::failed_precondition(format!(
                "relay: child {shard} completed without a vector scoring fingerprint"
            )));
        }
        match self.scoring_fingerprint.as_ref() {
            Some(expected) if expected != &summary.scoring_fingerprint => {
                return Err(Status::failed_precondition(format!(
                    "relay: child {shard} vector scoring fingerprint {} differs from {expected}",
                    summary.scoring_fingerprint
                )));
            }
            None => self.scoring_fingerprint = Some(summary.scoring_fingerprint.clone()),
            _ => {}
        }
        if summary.geo_columns_known.len() != self.geo_known.len()
            || summary.filter_columns_known.len() != self.tree_known.len()
        {
            return Err(Status::internal(format!(
                "relay: child {shard} answered {} geo and {} filter flags for {} and {}",
                summary.geo_columns_known.len(),
                summary.filter_columns_known.len(),
                self.geo_known.len(),
                self.tree_known.len()
            )));
        }
        super::merge_util::or_into(&mut self.geo_known, &summary.geo_columns_known);
        super::merge_util::or_into(&mut self.tree_known, &summary.filter_columns_known);
        self.validate_child_parts(shard, summary)?;
        self.totals.add(summary);
        Ok(())
    }

    fn validate_child_parts(
        &mut self,
        shard: usize,
        summary: &StreamSearchSummary,
    ) -> Result<(), Status> {
        let nonempty = self.child_nonempty[shard]
            || summary.emitted > 0
            || summary.blocks_scanned > 0
            || summary.fp32_rows_scored > 0;
        if nonempty && summary.parts_total == 0 {
            self.totals.missing_parts = true;
        }
        if summary.parts_skipped > summary.parts_total {
            return Err(Status::internal(format!(
                "relay: child {shard} reported {} skipped parts out of {}",
                summary.parts_skipped, summary.parts_total
            )));
        }
        let active = summary.parts_total - summary.parts_skipped;
        let executed = summary
            .parts_walked
            .saturating_add(summary.parts_enumerated)
            .saturating_add(summary.parts_scanned)
            .saturating_add(summary.parts_staged);
        let early =
            crate::vector::VectorStreamEndReason::from_proto(summary.end_reason).is_early_finish();
        if executed > active || (summary.parts_total > 0 && !early && executed != active) {
            return Err(Status::internal(format!(
                "relay: child {shard} executed {executed} parts across {active} active parts"
            )));
        }
        if self.exact_fp32
            && summary.parts_total > 0
            && ((active == 0 && summary.fp32_rows_scored != 0)
                || summary.fp32_rows_scored < u64::from(active))
        {
            return Err(Status::internal(format!(
                "relay: child {shard} scored {} FP32 rows across {active} active parts",
                summary.fp32_rows_scored
            )));
        }
        Ok(())
    }
}

/// Send one summary frame to the parent; a closed parent is ignored.
pub(super) async fn send_summary(
    tx: &mpsc::Sender<Result<StreamSearchResponse, Status>>,
    summary: StreamSearchSummary,
) {
    let _ = tx
        .send(Ok(StreamSearchResponse {
            payload: Some(stream_search_response::Payload::Summary(summary)),
        }))
        .await;
}
