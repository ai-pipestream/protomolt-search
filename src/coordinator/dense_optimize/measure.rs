//! Timing one dense execution cell over the sample queries through the
//! coordinator's `Query` route, bounded by `max_runtime_ms` and verified
//! against each response's `DenseExecutionOutcome` receipt.

use std::collections::HashSet;
use std::time::{Duration, Instant};

use tonic::{Request, Status};

use crate::coordinator::CoordinatorServiceImpl;
use crate::pb::search_service_server::SearchService;
use crate::pb::{
    search_query, selection_query, DenseCoverage, DenseExecutionMode as Mode, DenseFilterStrategy,
    DensePrecisionRequest, DenseQuery, DenseScorePrecision, DenseTechnique, QueryRequest,
    SearchQuery, SelectionQuery,
};

/// One technique and precision cell to measure over the sample queries.
#[derive(Debug, Clone, Copy)]
pub(super) struct CellSpec {
    pub(super) k: u32,
    pub(super) depth: u32,
    pub(super) mode: Mode,
    pub(super) technique: DenseTechnique,
    pub(super) patience: Option<(u32, u32)>,
    pub(super) precision_req: DensePrecisionRequest,
    pub(super) output_precision: DenseScorePrecision,
    pub(super) pooled_fp32: bool,
}

/// Per-query top-`k` ids, latencies, and bound filter strategy of one cell.
pub(super) struct Run {
    pub(super) ids: Vec<Vec<u64>>,
    pub(super) latencies: Vec<Duration>,
    pub(super) filter_strategy: DenseFilterStrategy,
}

/// Pre-indexed exhaustive ground truth at `k` so every candidate cell scores
/// per-query recall in `O(k)` hash lookups rather than `O(k^2)` slice scans.
pub(super) struct TruthBaseline {
    want_sets: Vec<HashSet<u64>>,
    latencies: (u32, u32, f64),
}

impl TruthBaseline {
    pub(super) fn from_run(truth: &Run, k: usize) -> Self {
        let want_sets = truth
            .ids
            .iter()
            .map(|want| want.iter().copied().take(k).collect::<HashSet<u64>>())
            .collect();
        Self {
            want_sets,
            latencies: truth.latency(),
        }
    }

    pub(super) fn latency(&self) -> (u32, u32, f64) {
        self.latencies
    }
}

impl Run {
    /// Lower-median and 99th-percentile latency in microseconds (at least 1)
    /// and positive finite sequential queries per second over the run.
    pub(super) fn latency(&self) -> (u32, u32, f64) {
        let mut micros: Vec<u32> = self
            .latencies
            .iter()
            .map(|d| u32::try_from(d.as_micros()).unwrap_or(u32::MAX).max(1))
            .collect();
        micros.sort_unstable();
        let n = micros.len();
        let p50 = micros[(n - 1) / 2];
        let p99 = micros[(n * 99).div_ceil(100).saturating_sub(1).min(n - 1)];
        let total = self
            .latencies
            .iter()
            .map(Duration::as_secs_f64)
            .sum::<f64>()
            .max(1e-9);
        (p50, p99, n as f64 / total)
    }

    /// The worst query's recall of `truth` at `k`, in parts per million.
    pub(super) fn min_recall_ppm(&self, truth: &TruthBaseline, k: usize) -> u32 {
        self.ids
            .iter()
            .zip(&truth.want_sets)
            .map(|(got, want)| {
                if want.is_empty() {
                    return 1_000_000;
                }
                let mut seen = HashSet::with_capacity(k.min(got.len()));
                let hits = got
                    .iter()
                    .take(k)
                    .filter(|id| seen.insert(**id) && want.contains(id))
                    .count();
                (hits as u64 * 1_000_000 / want.len() as u64) as u32
            })
            .min()
            .unwrap_or(0)
    }
}

/// Build the in-process `QueryRequest` for one sample vector. Served `AUTO`
/// executes native approximate points by fetching `policy_depth = candidates`
/// hits per shard (`fetch_k = depth` in `query::prepare_depth`) and truncating
/// in memory to `k`; explicit `BALANCED`/`FAST` single-leaf queries forbid
/// `selection_k != k`, so `k = depth` reproduces the exact shard work and wire
/// payload while `run` retains the top `spec.k` IDs.
fn build_query_request(vector: &[f32], spec: &CellSpec) -> QueryRequest {
    let (k, selection_k) = if spec.mode == Mode::Exhaustive {
        (spec.k, 0)
    } else if spec.pooled_fp32 {
        (spec.k, spec.depth)
    } else {
        (spec.depth, 0)
    };
    QueryRequest {
        k,
        selection_k,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Search(SearchQuery {
                id: "opt".into(),
                query: Some(search_query::Query::Dense(DenseQuery {
                    vector: vector.to_vec(),
                    execution_mode: spec.mode as i32,
                    precision: spec.precision_req as i32,
                    filter_strategy: DenseFilterStrategy::Default as i32,
                    ..Default::default()
                })),
            })),
        }),
        ..Default::default()
    }
}

fn verify_cell_receipt(
    spec: &CellSpec,
    outcome: Option<&crate::pb::DenseExecutionOutcome>,
) -> Result<DenseFilterStrategy, Status> {
    let outcome = outcome.ok_or_else(|| {
        Status::internal("optimize dense policy: Query response omitted dense_execution receipt")
    })?;
    if !outcome.fallback_reason.is_empty() {
        return Err(Status::failed_precondition(format!(
            "optimize dense policy: cell {:?} fell back instead of measuring {:?}: {}",
            spec.mode, spec.technique, outcome.fallback_reason
        )));
    }
    if spec.mode == Mode::Exhaustive {
        if outcome.coverage != DenseCoverage::ExhaustiveCertified as i32 {
            return Err(Status::internal(format!(
                "optimize dense policy: exhaustive truth did not certify coverage (got {})",
                outcome.coverage
            )));
        }
        return Ok(DenseFilterStrategy::Default);
    }
    let ran_technique = outcome
        .techniques
        .iter()
        .any(|t| t.technique == spec.technique as i32);
    if outcome.resolved_mode != spec.mode as i32 || !ran_technique {
        return Err(Status::failed_precondition(format!(
            "optimize dense policy: cell {:?} resolved to mode {} without running {:?}",
            spec.mode, outcome.resolved_mode, spec.technique
        )));
    }
    let strategy = DenseFilterStrategy::try_from(outcome.filter_strategy).map_err(|_| {
        Status::internal(format!(
            "optimize dense policy: unknown filter_strategy {} on receipt",
            outcome.filter_strategy
        ))
    })?;
    let valid_strategy = match spec.technique {
        DenseTechnique::BitplaneStaged => strategy == DenseFilterStrategy::Default,
        DenseTechnique::HnswWalk => matches!(
            strategy,
            DenseFilterStrategy::Speed | DenseFilterStrategy::EnumerateSmall
        ),
        _ => false,
    };
    if !valid_strategy {
        return Err(Status::internal(format!(
            "optimize dense policy: receipt bound unexpected filter_strategy {strategy:?} for {:?}",
            spec.technique
        )));
    }
    Ok(strategy)
}

/// Execute one untimed exhaustive query before timing a precision pass so
/// shard channel initialization and cold page caches do not penalize the first
/// timed `Exhaustive` truth baseline. Returns `Ok(false)` if `deadline` expires.
pub(super) async fn warmup(
    coordinator: &CoordinatorServiceImpl,
    vector: &[f32],
    k: u32,
    precision_req: DensePrecisionRequest,
    output_precision: DenseScorePrecision,
    deadline: Instant,
) -> Result<bool, Status> {
    if Instant::now() >= deadline {
        return Ok(false);
    }
    let spec = CellSpec {
        k,
        depth: k,
        mode: Mode::Exhaustive,
        technique: DenseTechnique::Fp32Scan,
        patience: None,
        precision_req,
        output_precision,
        pooled_fp32: false,
    };
    let request = build_query_request(vector, &spec);
    let rpc = SearchService::query(coordinator, Request::new(request));
    let Ok(result) = tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), rpc).await
    else {
        return Ok(false);
    };
    let response = result?.into_inner();
    verify_cell_receipt(&spec, response.dense_execution.as_ref())?;
    Ok(true)
}

/// Run every sample query under `spec`, keeping each query's top `k`.
/// Returns `Ok(None)` as soon as `deadline` expires before or during an RPC.
pub(super) async fn run(
    coordinator: &CoordinatorServiceImpl,
    queries: &[Vec<f32>],
    spec: &CellSpec,
    deadline: Instant,
) -> Result<Option<Run>, Status> {
    let mut ids = Vec::with_capacity(queries.len());
    let mut latencies = Vec::with_capacity(queries.len());
    let mut bound_strategy = None;
    let tokio_deadline = tokio::time::Instant::from_std(deadline);
    for vector in queries {
        if Instant::now() >= deadline {
            return Ok(None);
        }
        let request = build_query_request(vector, spec);
        let started = Instant::now();
        let rpc = SearchService::query(coordinator, Request::new(request));
        let Ok(result) = tokio::time::timeout_at(tokio_deadline, rpc).await else {
            return Ok(None);
        };
        let response = result?.into_inner();
        let elapsed = started.elapsed();
        if Instant::now() > deadline && ids.len() + 1 == queries.len() {
            return Ok(None);
        }
        let strategy = verify_cell_receipt(spec, response.dense_execution.as_ref())?;
        if let Some(prev) = bound_strategy {
            if prev != strategy {
                return Err(Status::aborted(
                    "optimize dense policy: filter_strategy drifted across sample queries",
                ));
            }
        } else {
            bound_strategy = Some(strategy);
        }
        latencies.push(elapsed);
        ids.push(
            response
                .hits
                .iter()
                .take(spec.k as usize)
                .map(|hit| hit.doc_id)
                .collect(),
        );
    }
    Ok(Some(Run {
        ids,
        latencies,
        filter_strategy: bound_strategy.unwrap_or(DenseFilterStrategy::Default),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::{DenseExecutionOutcome, DenseTechniqueCount};

    fn slice_recall_ppm(got_rows: &[Vec<u64>], want_rows: &[Vec<u64>], k: usize) -> u32 {
        got_rows
            .iter()
            .zip(want_rows)
            .map(|(got, want)| {
                let want = &want[..want.len().min(k)];
                if want.is_empty() {
                    return 1_000_000;
                }
                let hits = got.iter().take(k).filter(|id| want.contains(id)).count();
                (hits as u64 * 1_000_000 / want.len() as u64) as u32
            })
            .min()
            .unwrap_or(0)
    }

    #[test]
    fn recall_ppm_matches_slice_reference_and_scores_large_k_in_linear_time() {
        let truth_small = Run {
            ids: vec![vec![], vec![10, 20, 30, 40], vec![1, 2, 3, 4, 5]],
            latencies: vec![Duration::from_micros(50); 3],
            filter_strategy: DenseFilterStrategy::Default,
        };
        let cand_small = Run {
            ids: vec![vec![99], vec![40, 10, 77, 20], vec![5, 4, 9, 8]],
            latencies: vec![Duration::from_micros(10); 3],
            filter_strategy: DenseFilterStrategy::Speed,
        };
        for k in [1usize, 3, 4, 5, 10] {
            let base = TruthBaseline::from_run(&truth_small, k);
            assert_eq!(
                cand_small.min_recall_ppm(&base, k),
                slice_recall_ppm(&cand_small.ids, &truth_small.ids, k),
                "k={k}"
            );
        }
        let dup_base = TruthBaseline::from_run(&truth_small, 4);
        let dup_cand = Run {
            ids: vec![vec![], vec![10, 10, 10, 10], vec![1, 1, 1, 1]],
            latencies: vec![Duration::from_micros(10); 3],
            filter_strategy: DenseFilterStrategy::Speed,
        };
        assert_eq!(dup_cand.min_recall_ppm(&dup_base, 4), 250_000);

        const K: usize = 10_000;
        const QUERIES: usize = 64;
        let truth_large = Run {
            ids: (0..QUERIES)
                .map(|q| ((q * K) as u64..((q + 1) * K) as u64).collect())
                .collect(),
            latencies: vec![Duration::from_micros(500); QUERIES],
            filter_strategy: DenseFilterStrategy::Default,
        };
        let cand_large = Run {
            ids: (0..QUERIES)
                .map(|q| {
                    let start = (q * K) as u64;
                    let missed = if q == 17 { 250u64 } else { 100u64 };
                    (start + missed..start + K as u64 + missed).collect()
                })
                .collect(),
            latencies: vec![Duration::from_micros(50); QUERIES],
            filter_strategy: DenseFilterStrategy::Speed,
        };
        let started = Instant::now();
        let baseline = TruthBaseline::from_run(&truth_large, K);
        let ppm = cand_large.min_recall_ppm(&baseline, K);
        assert_eq!(ppm, 975_000);
        assert!(
            started.elapsed() < Duration::from_secs(2),
            "64x10k linear recall took {:?}",
            started.elapsed()
        );
    }

    #[test]
    fn verify_cell_receipt_rejects_fallbacks_and_returns_bound_strategy() {
        let staged_spec = CellSpec {
            k: 10,
            depth: 20,
            mode: Mode::Balanced,
            technique: DenseTechnique::BitplaneStaged,
            patience: None,
            precision_req: DensePrecisionRequest::Fp32,
            output_precision: DenseScorePrecision::Fp32,
            pooled_fp32: true,
        };
        let mut staged_ok = DenseExecutionOutcome {
            resolved_mode: Mode::Balanced as i32,
            filter_strategy: DenseFilterStrategy::Default as i32,
            techniques: vec![DenseTechniqueCount {
                technique: DenseTechnique::BitplaneStaged as i32,
                parts: 2,
            }],
            ..Default::default()
        };
        assert_eq!(
            verify_cell_receipt(&staged_spec, Some(&staged_ok)).unwrap(),
            DenseFilterStrategy::Default
        );
        staged_ok.fallback_reason = "fell back to uncertified pool".into();
        assert_eq!(
            verify_cell_receipt(&staged_spec, Some(&staged_ok))
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );

        let walk_spec = CellSpec {
            technique: DenseTechnique::HnswWalk,
            ..staged_spec
        };
        let downgraded_walk = DenseExecutionOutcome {
            resolved_mode: Mode::Balanced as i32,
            filter_strategy: DenseFilterStrategy::Speed as i32,
            techniques: vec![DenseTechniqueCount {
                technique: DenseTechnique::HnswWalk as i32,
                parts: 2,
            }],
            ..Default::default()
        };
        assert_eq!(
            verify_cell_receipt(&walk_spec, Some(&downgraded_walk)).unwrap(),
            DenseFilterStrategy::Speed
        );
    }
}
