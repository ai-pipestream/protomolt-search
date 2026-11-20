//! `OptimizeDensePolicy`: measure the dense techniques this fleet serves on
//! its own hardware against the exhaustive technique for the same sample
//! queries, through the coordinator's `Query` route, and write the version-2
//! policy AUTO routes through.

mod measure;

use std::time::{Duration, Instant};

use tonic::Status;

use self::measure::{run, CellSpec, Run};
use super::CoordinatorServiceImpl;
use crate::dense_execution::{HNSW_BALANCED, HNSW_FAST};
use crate::pb::{
    DenseExecutionMode as Mode, DenseExecutionPolicyDocument, DenseExecutionShape,
    DenseFilterStrategy as Strategy, DensePolicyPoint, DensePrecisionRequest, DenseScorePrecision,
    DenseTechnique, OptimizeDensePolicyRequest, OptimizeDensePolicyResponse,
};

const DEFAULT_KS: [u32; 4] = [10, 100, 1_000, 10_000];
const MAX_RUNTIME_MS: u32 = 3_600_000;

/// One approximate technique supported by the fleet.
#[derive(Clone, Copy)]
struct CandidateTechnique {
    mode: Mode,
    technique: DenseTechnique,
    strategy: Strategy,
    patience: Option<(u32, u32)>,
}

/// What the sweep measured.
struct Sweep {
    points: Vec<DensePolicyPoint>,
    cells: u32,
    truncated: bool,
}

/// The fleet facts a sweep binds its evidence to.
struct Fleet {
    caps: crate::dense_execution::ProviderCaps,
    identity: crate::dense_policy::LiveIdentity,
    hosts: Vec<crate::pb::DenseHostProfile>,
    shape: DenseExecutionShape,
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

fn techniques(caps: &crate::dense_execution::ProviderCaps) -> Vec<CandidateTechnique> {
    let walk = |mode, preset: crate::dense_execution::Preset| CandidateTechnique {
        mode,
        technique: DenseTechnique::HnswWalk,
        strategy: preset.default_strategy,
        patience: preset.patience,
    };
    let mut out = Vec::new();
    if caps.walks {
        out.push(walk(Mode::Balanced, HNSW_BALANCED));
        if caps.patience {
            out.push(walk(Mode::Fast, HNSW_FAST));
        }
    }
    if caps.staged {
        out.push(CandidateTechnique {
            mode: Mode::Balanced,
            technique: DenseTechnique::BitplaneStaged,
            strategy: Strategy::Default,
            patience: None,
        });
    }
    out
}

fn build_point(
    spec: &CellSpec,
    (approximate, exhaustive): (&Run, &Run),
    fleet: &Fleet,
) -> Option<DensePolicyPoint> {
    let measured_recall_ppm = approximate.min_recall_ppm(exhaustive, spec.k as usize);
    if measured_recall_ppm == 0 {
        return None;
    }
    let (approximate_p50_us, approximate_p99_us, approximate_qps) = approximate.latency();
    let (exhaustive_p50_us, exhaustive_p99_us, exhaustive_qps) = exhaustive.latency();
    let strategy = match spec.strategy {
        Strategy::Default => Strategy::Speed,
        other => other,
    };
    Some(DensePolicyPoint {
        k: spec.k,
        filter_selectivity_ppm_min: 1_000_000,
        filter_selectivity_ppm_max: 1_000_000,
        candidates: spec.depth,
        measured_recall_ppm,
        patience_saturation_ppm: spec.patience.map(|(ppm, _)| ppm),
        patience_hops: spec.patience.map(|(_, hops)| hops),
        mode: spec.mode as i32,
        technique: spec.technique as i32,
        selection_precision: fleet.caps.native as i32,
        output_precision: spec.output_precision as i32,
        filter_strategy: strategy as i32,
        shape: fleet.shape as i32,
        approximate_p50_us,
        approximate_p99_us,
        exhaustive_p50_us,
        exhaustive_p99_us,
        approximate_qps,
        exhaustive_qps,
        ..Default::default()
    })
}

fn depths(k: u32, limit: u32) -> Vec<u32> {
    (0..4)
        .map(|shift| k.saturating_mul(1 << shift))
        .filter(|depth| *depth <= limit)
        .collect()
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

#[derive(Clone, Copy)]
struct PrecisionPass {
    precision_req: DensePrecisionRequest,
    output_precision: DenseScorePrecision,
    pooled_fp32: bool,
    deadline: Instant,
}

impl CoordinatorServiceImpl {
    /// Discover and validate the live fleet's capabilities, identity, and
    /// homogeneous shard hardware profiles.
    async fn optimize_fleet(&self) -> Result<Fleet, Status> {
        if self.clustered_quality_identity().await?.is_some() {
            return Err(Status::failed_precondition(
                "optimize dense policy measures shard providers; a clustered backend serves \
                 exhaustively",
            ));
        }
        let profile = self.dense_fleet_profile().await?;
        if profile.hosts.is_empty()
            || profile
                .hosts
                .iter()
                .any(|h| !crate::dense_policy::is_profiled_host(h))
        {
            return Err(Status::failed_precondition(
                "optimize dense policy requires every shard and relay to report a profiled \
                 homogeneous DenseHostProfile",
            ));
        }
        let first = crate::dense_policy::normalize_host(&profile.hosts[0]);
        if profile.hosts[1..]
            .iter()
            .any(|h| crate::dense_policy::normalize_host(h) != first)
        {
            return Err(Status::failed_precondition(
                "optimize dense policy requires homogeneous shard host profiles across the fleet",
            ));
        }
        let shape = if self.stream_search {
            DenseExecutionShape::Streaming
        } else {
            DenseExecutionShape::Unary
        };
        Ok(Fleet {
            caps: profile.caps,
            identity: profile.identity,
            hosts: profile.hosts,
            shape,
        })
    }

    async fn sweep_precision_pass(
        &self,
        fleet: &Fleet,
        queries: &[Vec<f32>],
        ks: &[u32],
        pass: PrecisionPass,
        sweep: &mut Sweep,
    ) -> Result<(), Status> {
        let limit = self
            .knobs
            .max_k()
            .min(u32::try_from(fleet.identity.corpus_rows).unwrap_or(u32::MAX));
        for &k in ks {
            let truth_spec = CellSpec {
                k,
                depth: k,
                mode: Mode::Exhaustive,
                technique: DenseTechnique::Fp32Scan,
                strategy: Strategy::Default,
                patience: None,
                precision_req: pass.precision_req,
                output_precision: pass.output_precision,
                pooled_fp32: false,
            };
            let truth = match run(self, queries, &truth_spec, pass.deadline).await {
                Ok(Some(run)) => run,
                Ok(None) => {
                    sweep.truncated = true;
                    return Ok(());
                }
                Err(status)
                    if pass.pooled_fp32 && status.code() == tonic::Code::FailedPrecondition =>
                {
                    return Ok(());
                }
                Err(err) => return Err(err),
            };
            for depth in depths(k, limit) {
                for cand in techniques(&fleet.caps) {
                    let spec = CellSpec {
                        k,
                        depth,
                        mode: cand.mode,
                        technique: cand.technique,
                        strategy: cand.strategy,
                        patience: cand.patience,
                        precision_req: pass.precision_req,
                        output_precision: pass.output_precision,
                        pooled_fp32: pass.pooled_fp32,
                    };
                    let Some(approximate) = run(self, queries, &spec, pass.deadline).await? else {
                        sweep.truncated = true;
                        return Ok(());
                    };
                    sweep.cells += 1;
                    if let Some(pt) = build_point(&spec, (&approximate, &truth), fleet) {
                        sweep.points.push(pt);
                    }
                }
            }
        }
        Ok(())
    }

    async fn sweep(
        &self,
        fleet: &Fleet,
        queries: &[Vec<f32>],
        ks: &[u32],
        deadline: Instant,
    ) -> Result<Sweep, Status> {
        let mut sweep = Sweep {
            points: Vec::new(),
            cells: 0,
            truncated: false,
        };
        let native_pass = PrecisionPass {
            precision_req: DensePrecisionRequest::Auto,
            output_precision: fleet.caps.native,
            pooled_fp32: false,
            deadline,
        };
        self.sweep_precision_pass(fleet, queries, ks, native_pass, &mut sweep)
            .await?;
        if !sweep.truncated && fleet.caps.native == DenseScorePrecision::Quantized {
            let fp32_pass = PrecisionPass {
                precision_req: DensePrecisionRequest::Fp32,
                output_precision: DenseScorePrecision::Fp32,
                pooled_fp32: true,
                deadline,
            };
            self.sweep_precision_pass(fleet, queries, ks, fp32_pass, &mut sweep)
                .await?;
        }
        Ok(sweep)
    }

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
        let deadline = started + Duration::from_millis(u64::from(req.max_runtime_ms));
        let fleet = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            self.optimize_fleet(),
        )
        .await
        .map_err(|_| {
            Status::deadline_exceeded(
                "optimize dense policy: fleet discovery exceeded max_runtime_ms",
            )
        })??;
        if techniques(&fleet.caps).is_empty() {
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
        let sweep = match self.sweep(&fleet, &queries, &ks, deadline).await {
            Ok(sweep) => sweep,
            Err(err) => {
                self.verify_unchanged_identity(&fleet.identity).await?;
                return Err(err);
            }
        };
        if !sweep.points.is_empty() {
            self.verify_unchanged_identity(&fleet.identity).await?;
        }
        self.finish_optimize(req, fleet, sweep, started)
    }

    async fn verify_unchanged_identity(
        &self,
        expected: &crate::dense_policy::LiveIdentity,
    ) -> Result<(), Status> {
        let post = tokio::time::timeout(Duration::from_secs(5), self.live_dense_identity())
            .await
            .map_err(|_| {
                Status::deadline_exceeded(
                    "optimize dense policy: post-sweep identity check timed out",
                )
            })??;
        if &post != expected {
            return Err(Status::aborted(
                "optimize dense policy: corpus mutated during sweep; retry on quiescent corpus",
            ));
        }
        Ok(())
    }

    fn finish_optimize(
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
            hardware_fingerprint: crate::dense_policy::fleet_fingerprint(&fleet.hosts),
            node_count: fleet.hosts.len() as u32,
            hosts: fleet.hosts,
        };
        let policy = crate::dense_policy::DenseExecutionPolicy::from_document(document)
            .map_err(Status::internal)?;
        let fingerprint = policy.fingerprint().to_string();
        let document = policy.document().clone();
        if req.install {
            self.dense_execution_policy
                .install(policy)
                .map_err(Status::failed_precondition)?;
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
