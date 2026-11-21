//! `OptimizeDensePolicy`: measure the dense techniques this fleet serves on
//! its own hardware against the exhaustive technique for the same sample
//! queries, through the coordinator's `Query` route, and write the version-2
//! policy `AUTO` routes through.

mod fleet;
mod measure;
mod sweep;

use std::time::{Duration, Instant};

use tonic::Status;

use super::CoordinatorServiceImpl;
use crate::pb::{
    DenseExecutionMode as Mode, DenseExecutionPolicyDocument, DenseExecutionShape,
    DensePolicyPoint, DensePrecisionRequest, DenseScorePrecision, DenseTechnique,
    OptimizeDensePolicyRequest, OptimizeDensePolicyResponse,
};

const DEFAULT_KS: [u32; 4] = [10, 100, 1_000, 10_000];
const MAX_RUNTIME_MS: u32 = 3_600_000;
const MAX_VERIFY_RESERVE: Duration = Duration::from_millis(250);

/// One approximate technique supported by the fleet.
#[derive(Clone, Copy)]
pub(super) struct CandidateTechnique {
    pub(super) mode: Mode,
    pub(super) technique: DenseTechnique,
    pub(super) patience: Option<(u32, u32)>,
}

/// Completed measurement cells and whether the runtime budget truncated the sweep.
pub(super) struct Sweep {
    pub(super) points: Vec<DensePolicyPoint>,
    pub(super) cells: u32,
    pub(super) truncated: bool,
}

/// The fleet facts a sweep binds its evidence to.
pub(super) struct Fleet {
    pub(super) caps: crate::dense_execution::ProviderCaps,
    pub(super) identity: crate::dense_policy::LiveIdentity,
    pub(super) hosts: Vec<crate::pb::DenseHostProfile>,
    pub(super) shape: DenseExecutionShape,
}

#[derive(Clone, Copy)]
pub(super) struct PrecisionPass {
    pub(super) precision_req: DensePrecisionRequest,
    pub(super) output_precision: DenseScorePrecision,
    pub(super) pooled_fp32: bool,
    pub(super) deadline: Instant,
}

fn refuse(why: impl Into<String>) -> Status {
    Status::invalid_argument(format!("optimize dense policy: {}", why.into()))
}

fn validate(req: &OptimizeDensePolicyRequest) -> Result<(), Status> {
    if req.policy_id.trim().is_empty() || req.embedding_model.trim().is_empty() {
        return Err(refuse("policy_id and embedding_model are required"));
    }
    if !(1..=1_000_000).contains(&req.target_recall_ppm) {
        return Err(refuse("target_recall_ppm must be in 1..=1000000"));
    }
    if !(1..=MAX_RUNTIME_MS).contains(&req.max_runtime_ms) {
        return Err(refuse(format!(
            "max_runtime_ms bounds the sweep and must be in 1..={MAX_RUNTIME_MS}"
        )));
    }
    if req.k.contains(&0) {
        return Err(refuse("k values must be positive"));
    }
    if req.query_vectors.is_empty() || req.query_vectors.iter().any(|v| !v.is_finite()) {
        return Err(refuse(
            "at least one finite sample query vector is required",
        ));
    }
    Ok(())
}

fn resolve_ks(req_ks: &[u32], limit: u64) -> Result<Vec<u32>, Status> {
    let mut raw = if req_ks.is_empty() {
        DEFAULT_KS.to_vec()
    } else {
        req_ks.to_vec()
    };
    raw.sort_unstable();
    raw.dedup();
    let ks: Vec<u32> = raw.into_iter().filter(|&k| u64::from(k) <= limit).collect();
    if ks.is_empty() {
        return Err(refuse(format!(
            "every requested k {req_ks:?} exceeds max_k/corpus_rows limit {limit}"
        )));
    }
    Ok(ks)
}

impl CoordinatorServiceImpl {
    pub(crate) async fn run_optimize_dense_policy(
        &self,
        req: OptimizeDensePolicyRequest,
    ) -> Result<OptimizeDensePolicyResponse, Status> {
        validate(&req)?;
        if req.install && self.dense_execution_policy.path().is_none() {
            return Err(Status::failed_precondition(
                "optimize dense policy: install needs the coordinator's \
                 --dense-execution-policy path",
            ));
        }
        let _single_flight = self
            .dense_execution_policy
            .try_lock_optimize()
            .ok_or_else(|| {
                Status::aborted("optimize dense policy: another sweep is already running")
            })?;
        let started = Instant::now();
        let budget = Duration::from_millis(u64::from(req.max_runtime_ms));
        let deadline = started + budget;
        let sweep_deadline = deadline - (budget / 10).min(MAX_VERIFY_RESERVE);
        let fleet = tokio::time::timeout_at(
            tokio::time::Instant::from_std(sweep_deadline),
            self.optimize_fleet(),
        )
        .await
        .map_err(|_| {
            Status::deadline_exceeded(
                "optimize dense policy: fleet discovery exceeded max_runtime_ms",
            )
        })??;
        if sweep::techniques(&fleet.caps).is_empty() {
            return Err(Status::failed_precondition(format!(
                "provider {} serves no approximate technique to measure",
                fleet.caps.provider
            )));
        }
        let dim = fleet.identity.dimensions as usize;
        if dim == 0 || !req.query_vectors.len().is_multiple_of(dim) {
            return Err(refuse(format!(
                "query_vectors holds {} floats, not whole queries of dimension {dim}",
                req.query_vectors.len()
            )));
        }
        let limit = u64::from(self.knobs.max_k()).min(fleet.identity.corpus_rows);
        if limit == 0 {
            return Err(Status::failed_precondition(
                "optimize dense policy requires a non-empty corpus",
            ));
        }
        let ks = resolve_ks(&req.k, limit)?;
        let queries: Vec<Vec<f32>> = req.query_vectors.chunks(dim).map(<[f32]>::to_vec).collect();
        let sweep = self.sweep(&fleet, &queries, &ks, sweep_deadline).await?;
        if !sweep.points.is_empty() {
            self.verify_unchanged_fleet(&fleet, deadline).await?;
        }
        self.finish_optimize(req, fleet, sweep, started).await
    }

    async fn finish_optimize(
        &self,
        req: OptimizeDensePolicyRequest,
        fleet: Fleet,
        sweep: Sweep,
        started: Instant,
    ) -> Result<OptimizeDensePolicyResponse, Status> {
        if sweep.points.is_empty() {
            if sweep.truncated {
                return Err(Status::deadline_exceeded(
                    "optimize dense policy: no cell completed within max_runtime_ms",
                ));
            }
            return Err(Status::failed_precondition(
                "optimize dense policy: all measured cells had zero worst-query recall",
            ));
        }
        let hosts: Vec<crate::pb::DenseHostProfile> = fleet
            .hosts
            .iter()
            .map(crate::dense_policy::normalize_host)
            .collect();
        let document = DenseExecutionPolicyDocument {
            format_version: crate::dense_policy::FORMAT_VERSION,
            policy_id: req.policy_id,
            embedding_model: req.embedding_model,
            provider_backend: fleet.identity.provider_backend,
            scoring_fingerprint: fleet.identity.scoring_fingerprint,
            corpus_generation: fleet.identity.corpus_generation,
            corpus_rows: fleet.identity.corpus_rows,
            deleted_docs: fleet.identity.deleted_docs,
            corpus_fingerprint: fleet.identity.corpus_fingerprint,
            dimensions: fleet.identity.dimensions,
            measured_queries: (req.query_vectors.len() / fleet.identity.dimensions as usize) as u32,
            target_recall_ppm: req.target_recall_ppm,
            points: sweep.points,
            hardware_fingerprint: crate::dense_policy::fleet_fingerprint(&hosts),
            node_count: hosts.len() as u32,
            hosts,
        };
        let policy = crate::dense_policy::DenseExecutionPolicy::from_document(document)
            .map_err(Status::internal)?;
        let fingerprint = policy.fingerprint().to_string();
        let document = policy.document().clone();
        if req.install {
            self.dense_execution_policy
                .install(policy)
                .await
                .map_err(Status::failed_precondition)?;
            self.refresh_fixed_knobs();
        }
        Ok(OptimizeDensePolicyResponse {
            policy: Some(document),
            policy_fingerprint: fingerprint,
            elapsed_ms: started.elapsed().as_millis() as u64,
            cells_measured: sweep.cells,
            truncated: sweep.truncated,
            installed: req.install,
        })
    }
}
