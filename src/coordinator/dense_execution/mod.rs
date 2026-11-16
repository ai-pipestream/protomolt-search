//! Dense provider selection, quality checks, and exact rescoring.

use super::*;

mod exact;
mod fleet;
mod provider;
mod quality;
mod resolve;

pub(crate) struct ExactRerankScores {
    pub scores: HashMap<u64, f32>,
    pub rows: u64,
    pub logical_bytes: u64,
    pub pages_touched: u64,
    pub tasks: u32,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct DenseScanExecution {
    pub(crate) evidence: crate::dense_execution::Evidence,
    pub(crate) fp32_logical_bytes: u64,
    pub(crate) fp32_pages_touched: u64,
    pub(crate) fp32_tasks: u32,
}

pub(crate) fn dense_status(error: crate::dense_execution::DenseExecutionError) -> Status {
    use crate::dense_execution::DenseExecutionError as Err;
    match error {
        Err::InvalidRequest(msg) => Status::invalid_argument(msg),
        Err::FailedPrecondition(msg) => Status::failed_precondition(msg),
        Err::ReceiptContradiction(msg) => Status::internal(msg),
    }
}

impl CoordinatorServiceImpl {
    pub(crate) fn shard_is_nonempty(&self, shard: usize) -> bool {
        let Some(addr) = self.node_addrs.get(shard) else {
            return true;
        };
        let cache = self
            .shard_vector_rows
            .lock()
            .expect("shard vector row cache poisoned");
        cache.get(addr).copied().unwrap_or(1) > 0
    }

    pub(in crate::coordinator) fn evidence_from_shard_stats(
        &self,
        shard_stats: &[(usize, Option<&ShardScanStats>)],
        rows: u64,
    ) -> DenseScanExecution {
        let mut exec = DenseScanExecution::default();
        let mut regimes = crate::vector::FilterRegimeCounts::default();
        for &(shard, stats) in shard_stats {
            let nonempty =
                self.shard_is_nonempty(shard) || stats.is_some_and(|s| s.parts_total > 0);
            if nonempty {
                exec.evidence.consulted_shards += 1;
            }
            let Some(s) = stats else {
                continue;
            };
            if nonempty && s.parts_total > 0 {
                exec.evidence.reported_shards += 1;
            }
            exec.evidence
                .shards
                .push(crate::dense_execution::ShardPartEvidence {
                    parts_total: s.parts_total,
                    parts_skipped: s.parts_skipped,
                    parts_walked: s.parts_walked,
                    parts_enumerated: s.parts_enumerated,
                    parts_scanned: s.parts_scanned,
                    parts_staged: s.parts_staged,
                    fp32_rows_scored: s.fp32_rows_scored,
                    finished_early: false,
                });
            regimes.add(crate::vector::FilterRegimeCounts::from_proto(
                s.filter_regimes.as_ref(),
            ));
            exec.evidence.parts_total = exec.evidence.parts_total.saturating_add(s.parts_total);
            exec.evidence.parts_skipped =
                exec.evidence.parts_skipped.saturating_add(s.parts_skipped);
            exec.evidence.parts_walked = exec.evidence.parts_walked.saturating_add(s.parts_walked);
            exec.evidence.parts_enumerated = exec
                .evidence
                .parts_enumerated
                .saturating_add(s.parts_enumerated);
            exec.evidence.parts_scanned =
                exec.evidence.parts_scanned.saturating_add(s.parts_scanned);
            exec.evidence.parts_staged = exec.evidence.parts_staged.saturating_add(s.parts_staged);
            exec.evidence.tail_enumerated_rows = exec
                .evidence
                .tail_enumerated_rows
                .saturating_add(s.tail_enumerated_rows);
            exec.evidence.patience_stopped = exec
                .evidence
                .patience_stopped
                .saturating_add(s.patience_stopped);
            if s.floor_pruned > 0 {
                exec.evidence.shards_floor_pruned += 1;
            }
            exec.evidence.fp32_rows_scored = exec
                .evidence
                .fp32_rows_scored
                .saturating_add(s.fp32_rows_scored);
            exec.fp32_logical_bytes = exec.fp32_logical_bytes.saturating_add(s.fp32_logical_bytes);
            exec.fp32_pages_touched = exec.fp32_pages_touched.saturating_add(s.fp32_pages_touched);
            exec.fp32_tasks = exec.fp32_tasks.saturating_add(s.fp32_tasks);
        }
        exec.evidence.regimes = regimes.to_proto();
        exec.evidence.rows = rows;
        exec
    }

    pub(in crate::coordinator) fn evidence_from_summaries(
        &self,
        summaries: &[StreamSearchSummary],
        skipped: Option<&[bool]>,
        rows: u64,
        shards_finished_early: u32,
        shards_floor_pruned: u32,
    ) -> DenseScanExecution {
        let mut exec = DenseScanExecution::default();
        let mut regimes = crate::vector::FilterRegimeCounts::default();
        for (shard, s) in summaries.iter().enumerate() {
            if skipped.is_some_and(|mask| mask.get(shard).copied().unwrap_or(false)) {
                continue;
            }
            let nonempty = self.shard_is_nonempty(shard) || s.parts_total > 0;
            if nonempty {
                exec.evidence.consulted_shards += 1;
                if s.parts_total > 0 {
                    exec.evidence.reported_shards += 1;
                }
            }
            let finished_early =
                crate::vector::VectorStreamEndReason::from_proto(s.end_reason).is_early_finish();
            exec.evidence
                .shards
                .push(crate::dense_execution::ShardPartEvidence {
                    parts_total: s.parts_total,
                    parts_skipped: s.parts_skipped,
                    parts_walked: s.parts_walked,
                    parts_enumerated: s.parts_enumerated,
                    parts_scanned: s.parts_scanned,
                    parts_staged: s.parts_staged,
                    fp32_rows_scored: s.fp32_rows_scored,
                    finished_early,
                });
            regimes.add(crate::vector::FilterRegimeCounts::from_proto(
                s.filter_regimes.as_ref(),
            ));
            exec.evidence.parts_total = exec.evidence.parts_total.saturating_add(s.parts_total);
            exec.evidence.parts_skipped =
                exec.evidence.parts_skipped.saturating_add(s.parts_skipped);
            exec.evidence.parts_walked = exec.evidence.parts_walked.saturating_add(s.parts_walked);
            exec.evidence.parts_enumerated = exec
                .evidence
                .parts_enumerated
                .saturating_add(s.parts_enumerated);
            exec.evidence.parts_scanned =
                exec.evidence.parts_scanned.saturating_add(s.parts_scanned);
            exec.evidence.parts_staged = exec.evidence.parts_staged.saturating_add(s.parts_staged);
            exec.evidence.tail_enumerated_rows = exec
                .evidence
                .tail_enumerated_rows
                .saturating_add(s.tail_enumerated_rows);
            exec.evidence.patience_stopped = exec
                .evidence
                .patience_stopped
                .saturating_add(s.patience_stopped);
            exec.evidence.fp32_rows_scored = exec
                .evidence
                .fp32_rows_scored
                .saturating_add(s.fp32_rows_scored);
            exec.fp32_logical_bytes = exec.fp32_logical_bytes.saturating_add(s.fp32_logical_bytes);
            exec.fp32_pages_touched = exec.fp32_pages_touched.saturating_add(s.fp32_pages_touched);
            exec.fp32_tasks = exec.fp32_tasks.saturating_add(s.fp32_tasks);
        }
        exec.evidence.regimes = regimes.to_proto();
        exec.evidence.shards_finished_early = shards_finished_early;
        exec.evidence.shards_floor_pruned = shards_floor_pruned;
        exec.evidence.rows = rows;
        exec
    }
}

pub(crate) fn clustered_scan_execution(rows: u64) -> DenseScanExecution {
    DenseScanExecution {
        evidence: crate::dense_execution::Evidence {
            rows,
            ..Default::default()
        },
        ..Default::default()
    }
}

/// The provider-mismatch line of a cluster health report: empty when
/// every reachable, configured shard scores under one backend kind and
/// scoring fingerprint, else every distinct pair with the shards that
/// serve it.
pub(super) fn provider_mismatch_of(targets: &[ShardHealth]) -> String {
    let mut seen: Vec<((String, String), Vec<String>)> = Vec::new();
    for target in targets {
        let Some(health) = &target.health else {
            continue;
        };
        if health.vector_backend.is_empty() {
            continue;
        }
        let key = (
            health.vector_backend.clone(),
            health.scoring_fingerprint.clone(),
        );
        let who = format!("shard {} ({})", target.shard, target.addr);
        match seen.iter_mut().find(|(k, _)| *k == key) {
            Some((_, shards)) => shards.push(who),
            None => seen.push((key, vec![who])),
        }
    }
    if seen.len() <= 1 {
        return String::new();
    }
    seen.iter()
        .map(|((kind, fingerprint), shards)| format!("{kind}/{fingerprint}: {}", shards.join(", ")))
        .collect::<Vec<_>>()
        .join("; ")
}

/// One provider identity the shard fleet agrees on
/// (`CoordinatorServiceImpl::fleet_vector_identity`).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FleetVectorIdentity {
    pub provider: String,
    pub scoring_fingerprint: String,
    pub dimensions: u32,
    pub rows: u64,
}

/// Request-side key for dense execution policy lookup: raw `k`, optional
/// `candidate_depth` (`selection_k`), and compiled filters for live selectivity.
#[derive(Debug, Clone, Copy)]
pub struct DenseRequestKey<'a> {
    pub k: u32,
    pub candidate_depth: u32,
    pub filters: Option<&'a RequestFilters>,
}

/// Hold `actual` as the fleet-wide value on first sight, and refuse with
/// `mismatch` when a later shard reports a different one.
fn hold_uniform<T: PartialEq>(
    held: &mut Option<T>,
    actual: T,
    mismatch: &str,
) -> Result<(), Status> {
    match held {
        Some(value) if *value != actual => Err(Status::failed_precondition(mismatch)),
        Some(_) => Ok(()),
        None => {
            *held = Some(actual);
            Ok(())
        }
    }
}
