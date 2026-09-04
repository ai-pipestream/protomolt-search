//! Producing a dense quality profile from measurement
//! (`docs/dense-quality-profile.md`).
//!
//! Everything here goes through the public `Query` route the profile
//! will later serve: for every `k` and every candidate depth on the
//! ladder, each held-out query runs `DENSE_SCORE_MODE_FP32_RERANK` at
//! `selection_k = depth` with `profile = true`, and its top-`k` is compared
//! with the exhaustive FP32 top-`k` (the ground truth). Recall is counted
//! per query, so the ladder records the worst query as well as the mean;
//! [`super::choose_points`] then picks the smallest depth whose WORST
//! query meets each target. `examples/dense_profile.rs` is the CLI shell
//! over [`measure`]; the tests run it against the in-process harness.

use std::collections::HashSet;
use std::future::Future;
use std::time::Instant;

use tonic::{Request, Status};

use super::{choose_points, DenseQualityProfile, ProfileIdentity, ProfileMeasurement, UnmetTarget};
use crate::pb::search_service_server::SearchService;
use crate::pb::{
    search_query, selection_query, ClusterHealthRequest, ClusterHealthResponse, DenseExecutionMode,
    DenseQuery, DenseScoreMode, QueryRequest, QueryResponse, SearchQuery, SelectionQuery,
};

const PPM: u64 = 1_000_000;

/// The two public RPCs the measurement needs, over whichever transport
/// reaches the coordinator: the gRPC client (the CLI) or the handler
/// itself in-process (the tests). Both are the same route.
pub trait ProfileRoute {
    fn query(
        &mut self,
        request: QueryRequest,
    ) -> impl Future<Output = Result<QueryResponse, Status>> + Send;
    fn cluster_health(
        &mut self,
        collection: String,
    ) -> impl Future<Output = Result<ClusterHealthResponse, Status>> + Send;
}

impl ProfileRoute for crate::coordinator::CoordinatorServiceImpl {
    async fn query(&mut self, request: QueryRequest) -> Result<QueryResponse, Status> {
        SearchService::query(&*self, Request::new(request))
            .await
            .map(|reply| reply.into_inner())
    }

    async fn cluster_health(
        &mut self,
        collection: String,
    ) -> Result<ClusterHealthResponse, Status> {
        SearchService::cluster_health(&*self, Request::new(ClusterHealthRequest { collection }))
            .await
            .map(|reply| reply.into_inner())
    }
}

/// The gRPC client over any transport the generated client accepts: a
/// plain channel, or one behind the tools' bearer interceptor
/// (`security::PublicChannel`).
#[cfg(feature = "net")]
impl<T> ProfileRoute for crate::pb::search_service_client::SearchServiceClient<T>
where
    T: tonic::client::GrpcService<tonic::body::BoxBody> + Send,
    T::Future: Send,
    T::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    T::ResponseBody: http_body::Body<Data = bytes::Bytes> + Send + 'static,
    <T::ResponseBody as http_body::Body>::Error:
        Into<Box<dyn std::error::Error + Send + Sync>> + Send,
{
    async fn query(&mut self, request: QueryRequest) -> Result<QueryResponse, Status> {
        crate::pb::search_service_client::SearchServiceClient::query(self, request)
            .await
            .map(|reply| reply.into_inner())
    }

    async fn cluster_health(
        &mut self,
        collection: String,
    ) -> Result<ClusterHealthResponse, Status> {
        crate::pb::search_service_client::SearchServiceClient::cluster_health(
            self,
            ClusterHealthRequest { collection },
        )
        .await
        .map(|reply| reply.into_inner())
    }
}

/// The identity of the generation being measured, read from
/// `ClusterHealth`: what the profile binds to and what the coordinator's
/// quality preflight later checks it against.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveIdentity {
    pub provider_backend: String,
    pub scoring_fingerprint: String,
    pub dimensions: u32,
    pub rows: u64,
    pub topology_generation: u64,
}

/// Where the exhaustive FP32 top-`k` comes from.
pub enum GroundTruth<'a> {
    /// The public route itself at `selection_k = corpus rows`: FP32 rerank
    /// over every row IS the exhaustive order. Needs `rows <= max_k`.
    FullDepth,
    /// An exhaustive FP32 top-`k` over these rows (`rows * dimensions`
    /// floats, row `i` = global doc id `i`), the same dot product the
    /// rerank uses. The row count must equal the live corpus exactly.
    Brute { rows: &'a [f32] },
}

pub struct MeasureSpec<'a> {
    /// Empty for the unnamed dataset.
    pub collection: String,
    pub profile_id: String,
    pub embedding_model: String,
    /// Held-out query vectors, `dimensions` floats each.
    pub queries: &'a [f32],
    pub dimensions: u32,
    pub ks: Vec<u32>,
    /// The candidate-depth ladder. Every depth must be at or below the
    /// corpus rows and at or above the smallest `k`; for each `k` the
    /// depths at or above it are measured.
    pub depths: Vec<u32>,
    pub targets: Vec<u32>,
    pub default_target_recall_ppm: Option<u32>,
    pub ground_truth: GroundTruth<'a>,
}

/// One rung as measured, with the per-query recall the summary came from
/// and the client-side wall time the server phases do not include.
#[derive(Clone, Debug, PartialEq)]
pub struct LadderRow {
    pub measurement: ProfileMeasurement,
    pub p50_client_ms: f64,
    /// Per held-out query, in query order.
    pub recall_ppm: Vec<u32>,
}

#[derive(Debug)]
pub struct MeasuredProfile {
    pub identity: LiveIdentity,
    pub ladder: Vec<LadderRow>,
    pub unmet: Vec<UnmetTarget>,
    pub profile: DenseQualityProfile,
}

/// Read the live identity through `ClusterHealth` and refuse anything a
/// measured profile could not later bind to: an unreachable or empty
/// shard, a provider mismatch, missing exact rows, tombstones.
pub async fn live_identity<R: ProfileRoute>(
    route: &mut R,
    collection: &str,
) -> Result<LiveIdentity, String> {
    let health = route
        .cluster_health(collection.to_string())
        .await
        .map_err(|status| format!("ClusterHealth: {}", status.message()))?;
    if !health.collections.is_empty() {
        let names: Vec<&str> = health.collections.iter().map(|c| c.name.as_str()).collect();
        return Err(format!(
            "the coordinator serves named collections {names:?}; name the one to measure"
        ));
    }
    if !health.provider_mismatch.is_empty() {
        return Err(format!(
            "the fleet does not score in one space: {}",
            health.provider_mismatch
        ));
    }
    let mut provider: Option<String> = None;
    let mut fingerprint: Option<String> = None;
    let mut dimensions: Option<u32> = None;
    let mut rows = 0u64;
    let mut exact_rows = 0u64;
    for target in health.targets.iter().filter(|t| !t.is_replica) {
        let shard = match &target.health {
            Some(shard) if target.reachable => shard,
            _ => {
                return Err(format!(
                    "shard {} at {} is unreachable: {}",
                    target.shard, target.addr, target.error
                ))
            }
        };
        if !target.error.is_empty() {
            return Err(format!(
                "shard {} at {}: {}",
                target.shard, target.addr, target.error
            ));
        }
        if shard.num_vectors == 0 {
            return Err(format!(
                "shard at slot_offset={} has no vectors; a profile measures a populated generation",
                shard.slot_offset
            ));
        }
        if !shard.exact_vectors_available || shard.exact_vector_rows != shard.num_vectors {
            return Err(format!(
                "shard at slot_offset={} owns {} exact FP32 rows for {} vectors; FP32 rerank \
                 needs the aligned sidecar for every row",
                shard.slot_offset, shard.exact_vector_rows, shard.num_vectors
            ));
        }
        if shard.deleted_docs != 0 {
            return Err(format!(
                "shard at slot_offset={} has {} tombstoned rows; a profile describes an all-live \
                 generation, compact before measuring",
                shard.slot_offset, shard.deleted_docs
            ));
        }
        for (name, held, seen) in [
            ("vector_backend", &mut provider, &shard.vector_backend),
            (
                "scoring_fingerprint",
                &mut fingerprint,
                &shard.scoring_fingerprint,
            ),
        ] {
            match held {
                Some(value) if value != seen => {
                    return Err(format!(
                        "shards disagree on {name}: {value:?} vs {seen:?} at slot_offset={}",
                        shard.slot_offset
                    ))
                }
                None => *held = Some(seen.clone()),
                _ => {}
            }
        }
        match dimensions {
            Some(dim) if dim != shard.dim => {
                return Err(format!(
                    "shards disagree on dimensions: {dim} vs {} at slot_offset={}",
                    shard.dim, shard.slot_offset
                ))
            }
            None => dimensions = Some(shard.dim),
            _ => {}
        }
        rows = rows
            .checked_add(shard.num_vectors)
            .ok_or("row count overflow")?;
        exact_rows = exact_rows
            .checked_add(shard.exact_vector_rows)
            .ok_or("exact row count overflow")?;
    }
    if rows == 0 {
        return Err("the coordinator reports no populated primary shard".into());
    }
    if let Some(clustered) = &health.clustered_vector {
        if !clustered.reachable || !clustered.servable {
            return Err(format!(
                "clustered vector provider {} is not servable: {}",
                clustered.backend_kind, clustered.error
            ));
        }
        if clustered.scoring_fingerprint.is_empty() || clustered.dimensions == 0 {
            return Err(format!(
                "clustered vector provider {} reports no scoring identity",
                clustered.backend_kind
            ));
        }
        if clustered.rows != exact_rows {
            return Err(format!(
                "clustered vector provider holds {} rows but product shards own {exact_rows} exact rows",
                clustered.rows
            ));
        }
        return Ok(LiveIdentity {
            provider_backend: clustered.backend_kind.clone(),
            scoring_fingerprint: clustered.scoring_fingerprint.clone(),
            dimensions: clustered.dimensions,
            rows: clustered.rows,
            topology_generation: clustered.topology_generation,
        });
    }
    Ok(LiveIdentity {
        provider_backend: provider.unwrap_or_default(),
        scoring_fingerprint: fingerprint.unwrap_or_default(),
        dimensions: dimensions.unwrap_or_default(),
        rows,
        topology_generation: health.topology_generation,
    })
}

fn sorted_positive(name: &str, values: &[u32]) -> Result<Vec<u32>, String> {
    if values.is_empty() {
        return Err(format!("{name} must name at least one value"));
    }
    if values.contains(&0) {
        return Err(format!("{name} must be positive"));
    }
    let mut values = values.to_vec();
    values.sort_unstable();
    values.dedup();
    Ok(values)
}

fn fp32_leaf(vector: &[f32]) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: "dense".into(),
            query: Some(search_query::Query::Dense(DenseQuery {
                vector: vector.to_vec(),
                score_mode: DenseScoreMode::Fp32Rerank as i32,
                // The profile describes an exhaustive selection reranked
                // in FP32; a provider that cannot prove one refuses here
                // rather than measuring a different thing.
                execution_mode: DenseExecutionMode::Exact as i32,
                ..Default::default()
            })),
        })),
    }
}

/// The lower median by rank: sorted samples, index `(n - 1) / 2`. Exact,
/// no interpolation between samples.
fn p50(samples: &mut [f64]) -> f64 {
    samples.sort_by(f64::total_cmp);
    samples[(samples.len() - 1) / 2]
}

/// Exhaustive FP32 top-`k` over `rows`, the rerank's own dot product and
/// total order (score descending, id ascending).
fn brute_topk(rows: &[f32], dim: usize, query: &[f32], k: usize) -> Vec<u64> {
    let mut scored: Vec<(u64, f32)> = rows
        .chunks_exact(dim)
        .enumerate()
        .map(|(id, row)| (id as u64, crate::exact_vectors::dot(row, query)))
        .collect();
    scored.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    scored.truncate(k);
    scored.into_iter().map(|(id, _)| id).collect()
}

/// Run the ladder and build the profile. Every refusal names its cause:
/// a shape the identity cannot serve, a depth the corpus cannot supply,
/// a response short of `k`, a provider that did not run exhaustively, a
/// full-depth rung that disagrees with the ground truth, or a target no
/// rung met.
pub async fn measure<R: ProfileRoute>(
    route: &mut R,
    spec: &MeasureSpec<'_>,
) -> Result<MeasuredProfile, String> {
    let dim = spec.dimensions as usize;
    if dim == 0 {
        return Err("dimensions must be positive".into());
    }
    if spec.queries.is_empty() || !spec.queries.len().is_multiple_of(dim) {
        return Err(format!(
            "queries carry {} floats, not a positive multiple of dimensions={dim}",
            spec.queries.len()
        ));
    }
    let n_queries = spec.queries.len() / dim;
    let n_queries_u32 =
        u32::try_from(n_queries).map_err(|_| "more queries than measured_queries can count")?;
    let ks = sorted_positive("k", &spec.ks)?;
    let depths = sorted_positive("depths", &spec.depths)?;
    let k_max = *ks.last().expect("non-empty");
    if depths[0] < ks[0] {
        return Err(format!(
            "depth {} is below the smallest k {}; a depth is a candidate pool for some k",
            depths[0], ks[0]
        ));
    }
    if let Some(k) = ks.iter().find(|&&k| !depths.iter().any(|&d| d >= k)) {
        return Err(format!(
            "k={k} has no depth at or above it on the ladder {depths:?}"
        ));
    }

    let identity = live_identity(route, &spec.collection).await?;
    if identity.dimensions != spec.dimensions {
        return Err(format!(
            "queries have dimensions={} but the live provider serves {}",
            spec.dimensions, identity.dimensions
        ));
    }
    if u64::from(k_max) > identity.rows {
        return Err(format!(
            "k={k_max} exceeds the corpus rows {}",
            identity.rows
        ));
    }
    if let Some(depth) = depths.iter().find(|&&d| u64::from(d) > identity.rows) {
        return Err(format!(
            "depth {depth} exceeds the corpus rows {}; the ladder cannot select more candidates \
             than the corpus holds",
            identity.rows
        ));
    }
    if let GroundTruth::Brute { rows } = spec.ground_truth {
        let have = rows.len() / dim;
        if !rows.len().is_multiple_of(dim) || have as u64 != identity.rows {
            return Err(format!(
                "brute ground truth carries {have} rows of dimensions={dim} ({} floats) but the \
                 live corpus has {} rows; the file must cover the corpus exactly",
                rows.len(),
                identity.rows
            ));
        }
    }

    // Ground truth: the exhaustive FP32 top-k_max per query. Every smaller
    // k is its prefix, since the rerank order is total (score descending,
    // id ascending) and a top-k under a total order is prefix-stable.
    let mut truth: Vec<Vec<u64>> = Vec::with_capacity(n_queries);
    for (index, vector) in spec.queries.chunks_exact(dim).enumerate() {
        let ids = match spec.ground_truth {
            GroundTruth::Brute { rows } => brute_topk(rows, dim, vector, k_max as usize),
            GroundTruth::FullDepth => {
                let selection_k = u32::try_from(identity.rows).map_err(|_| {
                    format!(
                        "full-depth ground truth needs selection_k = {} rows, above the wire's u32",
                        identity.rows
                    )
                })?;
                let response = route
                    .query(QueryRequest {
                        request_id: format!("dense-profile-truth-q{index}"),
                        k: k_max,
                        selection_k,
                        selection: Some(fp32_leaf(vector)),
                        ..Default::default()
                    })
                    .await
                    .map_err(|status| {
                        format!(
                            "full-depth ground truth at selection_k={selection_k} refused: {}; \
                             raise the coordinator's --max-k or use --ground-truth=brute:<rows>",
                            status.message()
                        )
                    })?;
                check_exact(&response, k_max, index)?;
                response.hits.iter().map(|hit| hit.doc_id).collect()
            }
        };
        if ids.len() != k_max as usize {
            return Err(format!(
                "ground truth for query {index} has {} ids, not k={k_max}",
                ids.len()
            ));
        }
        truth.push(ids);
    }

    let mut ladder = Vec::new();
    for &k in &ks {
        let truth_k: Vec<HashSet<u64>> = truth
            .iter()
            .map(|ids| ids[..k as usize].iter().copied().collect())
            .collect();
        for &depth in depths.iter().filter(|&&d| d >= k) {
            let mut hits_per_query = Vec::with_capacity(n_queries);
            let mut total_ms = Vec::with_capacity(n_queries);
            let mut selection_ms = Vec::with_capacity(n_queries);
            let mut rerank_ms = Vec::with_capacity(n_queries);
            let mut client_ms = Vec::with_capacity(n_queries);
            for (index, vector) in spec.queries.chunks_exact(dim).enumerate() {
                let started = Instant::now();
                let response = route
                    .query(QueryRequest {
                        request_id: format!("dense-profile-k{k}-d{depth}-q{index}"),
                        k,
                        selection_k: depth,
                        selection: Some(fp32_leaf(vector)),
                        profile: true,
                        ..Default::default()
                    })
                    .await
                    .map_err(|status| {
                        format!(
                            "k={k} selection_k={depth} query {index} refused: {}",
                            status.message()
                        )
                    })?;
                client_ms.push(started.elapsed().as_secs_f64() * 1e3);
                check_exact(&response, k, index)?;
                let profile = response.profile.as_ref().ok_or_else(|| {
                    format!("k={k} selection_k={depth} query {index} carries no QueryProfile")
                })?;
                total_ms.push(f64::from(profile.total_ms));
                selection_ms.push(f64::from(profile.selection_ms));
                rerank_ms.push(f64::from(profile.rerank_ms));
                let hits = response
                    .hits
                    .iter()
                    .filter(|hit| truth_k[index].contains(&hit.doc_id))
                    .count() as u64;
                if u64::from(depth) == identity.rows && hits != u64::from(k) {
                    return Err(format!(
                        "full-depth rerank at selection_k={depth} recovered {hits} of k={k} for \
                         query {index}; the ground truth does not describe this generation"
                    ));
                }
                hits_per_query.push(hits);
            }
            let sum: u64 = hits_per_query.iter().sum();
            let min = *hits_per_query.iter().min().expect("at least one query");
            let recall_ppm: Vec<u32> = hits_per_query
                .iter()
                .map(|&h| (h * PPM / u64::from(k)) as u32)
                .collect();
            ladder.push(LadderRow {
                measurement: ProfileMeasurement {
                    k,
                    candidates: depth,
                    queries: n_queries_u32,
                    mean_recall_ppm: (sum * PPM / (n_queries as u64 * u64::from(k))) as u32,
                    min_recall_ppm: (min * PPM / u64::from(k)) as u32,
                    p50_total_ms: p50(&mut total_ms),
                    p50_selection_ms: p50(&mut selection_ms),
                    p50_rerank_ms: p50(&mut rerank_ms),
                },
                p50_client_ms: p50(&mut client_ms),
                recall_ppm,
            });
        }
    }

    let measurements: Vec<ProfileMeasurement> =
        ladder.iter().map(|row| row.measurement.clone()).collect();
    let chosen = choose_points(&measurements, &spec.targets)?;
    if chosen.points.is_empty() {
        return Err(format!(
            "no depth on the ladder met any target on its worst query; unmet: {}",
            describe_unmet(&chosen.unmet)
        ));
    }
    let profile = DenseQualityProfile::from_measurements(
        ProfileIdentity {
            profile_id: spec.profile_id.clone(),
            embedding_model: spec.embedding_model.clone(),
            corpus_generation: identity.topology_generation,
            corpus_rows: identity.rows,
            dimensions: identity.dimensions,
            provider_backend: identity.provider_backend.clone(),
            scoring_fingerprint: identity.scoring_fingerprint.clone(),
        },
        n_queries_u32,
        spec.default_target_recall_ppm,
        measurements,
        chosen.points,
    )
    .map_err(|error| {
        if chosen.unmet.is_empty() {
            error
        } else {
            format!("{error}; unmet targets: {}", describe_unmet(&chosen.unmet))
        }
    })?;
    Ok(MeasuredProfile {
        identity,
        ladder,
        unmet: chosen.unmet,
        profile,
    })
}

fn check_exact(response: &QueryResponse, k: u32, index: usize) -> Result<(), String> {
    if response.hits.len() != k as usize {
        return Err(format!(
            "query {index} returned {} hits for k={k}",
            response.hits.len()
        ));
    }
    match &response.dense_execution {
        Some(outcome) if outcome.resolved_mode == DenseExecutionMode::Exact as i32 => Ok(()),
        Some(outcome) => Err(format!(
            "query {index} did not run an exhaustive selection: {}",
            outcome.planner_reason
        )),
        None => Err(format!(
            "query {index} carries no dense_execution provenance"
        )),
    }
}

pub fn describe_unmet(unmet: &[UnmetTarget]) -> String {
    unmet
        .iter()
        .map(|u| {
            format!(
                "k={} target={} (best worst-query recall {} at {} candidates)",
                u.k, u.target_recall_ppm, u.best_min_recall_ppm, u.best_candidates
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// The ladder as a fixed-width table: one line per rung with expansion
/// over `k`, mean and worst-query recall, and the p50 phases.
pub fn ladder_table(measured: &MeasuredProfile) -> String {
    let mut out = String::from(
        "k        candidates  expansion  mean_recall  min_recall  p50_total_ms  p50_selection_ms  p50_rerank_ms  p50_client_ms\n",
    );
    for row in &measured.ladder {
        let m = &row.measurement;
        out.push_str(&format!(
            "{:<8} {:>10}  {:>8.4}x  {:>11.6}  {:>10.6}  {:>12.3}  {:>16.3}  {:>13.3}  {:>13.3}\n",
            m.k,
            m.candidates,
            f64::from(m.candidates) / f64::from(m.k),
            f64::from(m.mean_recall_ppm) / 1e6,
            f64::from(m.min_recall_ppm) / 1e6,
            m.p50_total_ms,
            m.p50_selection_ms,
            m.p50_rerank_ms,
            row.p50_client_ms,
        ));
    }
    out
}
