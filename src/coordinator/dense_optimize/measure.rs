//! Timing one dense execution cell over the sample queries through the
//! coordinator's `Query` route, bounded by `max_runtime_ms` and verified
//! against each response's `DenseExecutionOutcome` receipt.

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
    pub(super) strategy: DenseFilterStrategy,
    pub(super) patience: Option<(u32, u32)>,
    pub(super) precision_req: DensePrecisionRequest,
    pub(super) output_precision: DenseScorePrecision,
    pub(super) pooled_fp32: bool,
}

/// Per-query top-`k` ids and latencies of one cell.
pub(super) struct Run {
    pub(super) ids: Vec<Vec<u64>>,
    pub(super) latencies: Vec<Duration>,
}

impl Run {
    /// Lower-median and 99th-percentile latency in microseconds (at least 1)
    /// and positive finite queries per second over the run.
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
    pub(super) fn min_recall_ppm(&self, truth: &Run, k: usize) -> u32 {
        self.ids
            .iter()
            .zip(&truth.ids)
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
}

fn build_query_request(vector: &[f32], spec: &CellSpec) -> QueryRequest {
    let (k, selection_k) = if spec.mode == Mode::Exhaustive {
        (spec.k, 0)
    } else if spec.pooled_fp32 {
        (spec.k, spec.depth)
    } else {
        (spec.depth, 0)
    };
    let filter_strategy = if spec.mode == Mode::Exhaustive {
        DenseFilterStrategy::Default as i32
    } else {
        spec.strategy as i32
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
                    filter_strategy,
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
) -> Result<(), Status> {
    let outcome = outcome.ok_or_else(|| {
        Status::internal("optimize dense policy: Query response omitted dense_execution receipt")
    })?;
    if spec.mode == Mode::Exhaustive {
        if outcome.coverage != DenseCoverage::ExhaustiveCertified as i32 {
            return Err(Status::internal(format!(
                "optimize dense policy: exhaustive truth did not certify coverage (got {})",
                outcome.coverage
            )));
        }
        return Ok(());
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
    Ok(())
}

/// Run every sample query under `spec`, keeping each query's top `k`.
/// Returns `Ok(None)` as soon as `deadline` expires before or during an RPC.
pub(super) async fn run(
    coordinator: &CoordinatorServiceImpl,
    queries: &[Vec<f32>],
    spec: &CellSpec,
    deadline: Instant,
) -> Result<Option<Run>, Status> {
    let mut out = Run {
        ids: Vec::with_capacity(queries.len()),
        latencies: Vec::with_capacity(queries.len()),
    };
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
        if Instant::now() > deadline && out.ids.len() + 1 == queries.len() {
            return Ok(None);
        }
        verify_cell_receipt(spec, response.dense_execution.as_ref())?;
        out.latencies.push(elapsed);
        out.ids.push(
            response
                .hits
                .iter()
                .take(spec.k as usize)
                .map(|hit| hit.doc_id)
                .collect(),
        );
    }
    Ok(Some(out))
}
