//! Running the ladder: validate the spec against the live identity, fix
//! the ground truth, measure every `(k, depth)` rung through the public
//! route, and build the profile from the rungs.

use std::collections::HashSet;
use std::time::Instant;

use crate::pb::QueryRequest;

use super::super::{choose_points, DenseQualityProfile, ProfileIdentity, ProfileMeasurement};
use super::brute::brute_topk_many;
use super::report::{check_exact, describe_unmet, fp32_leaf, p50, sorted_positive};
use super::{
    live_identity, GroundTruth, LadderRow, LiveIdentity, MeasureSpec, MeasuredProfile, ProfileRoute,
};

const PPM: u64 = 1_000_000;

/// The ladder's shape, validated before anything is sent.
struct Ladder {
    dim: usize,
    n_queries: usize,
    n_queries_u32: u32,
    ks: Vec<u32>,
    depths: Vec<u32>,
    k_max: u32,
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
    let shape = ladder_shape(spec)?;
    let identity = live_identity(route, &spec.collection).await?;
    check_against_identity(spec, &shape, &identity)?;
    let truth = ground_truth(route, spec, &shape, &identity).await?;
    let mut ladder = Vec::new();
    for &k in &shape.ks {
        let truth_k: Vec<HashSet<u64>> = truth
            .iter()
            .map(|ids| ids[..k as usize].iter().copied().collect())
            .collect();
        for &depth in shape.depths.iter().filter(|&&d| d >= k) {
            let rung = Rung {
                k,
                depth,
                corpus_rows: identity.rows,
            };
            ladder.push(rung.measure(route, spec, &shape, &truth_k).await?);
        }
    }
    build_profile(spec, &shape, identity, ladder)
}

fn ladder_shape(spec: &MeasureSpec<'_>) -> Result<Ladder, String> {
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
    Ok(Ladder {
        dim,
        n_queries,
        n_queries_u32,
        ks,
        depths,
        k_max,
    })
}

/// The queries' dimensions, the largest k, every depth, and a brute
/// ground truth must all fit the live corpus.
fn check_against_identity(
    spec: &MeasureSpec<'_>,
    shape: &Ladder,
    identity: &LiveIdentity,
) -> Result<(), String> {
    if identity.dimensions != spec.dimensions {
        return Err(format!(
            "queries have dimensions={} but the live provider serves {}",
            spec.dimensions, identity.dimensions
        ));
    }
    if u64::from(shape.k_max) > identity.rows {
        return Err(format!(
            "k={} exceeds the corpus rows {}",
            shape.k_max, identity.rows
        ));
    }
    if let Some(depth) = shape.depths.iter().find(|&&d| u64::from(d) > identity.rows) {
        return Err(format!(
            "depth {depth} exceeds the corpus rows {}; the ladder cannot select more candidates \
             than the corpus holds",
            identity.rows
        ));
    }
    if let GroundTruth::Brute { rows } = spec.ground_truth {
        let dim = shape.dim;
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
    Ok(())
}

/// The exhaustive FP32 top-`k_max` per query. Every smaller k is its
/// prefix, since the rerank order is total (score descending, id
/// ascending) and a top-k under a total order is prefix-stable.
async fn ground_truth<R: ProfileRoute>(
    route: &mut R,
    spec: &MeasureSpec<'_>,
    shape: &Ladder,
    identity: &LiveIdentity,
) -> Result<Vec<Vec<u64>>, String> {
    let k_max = shape.k_max;
    let truth: Vec<Vec<u64>> = match spec.ground_truth {
        GroundTruth::Brute { rows } => {
            brute_topk_many(rows, shape.dim, spec.queries, k_max as usize)
                .into_iter()
                .map(|ids| match spec.brute_id_map {
                    Some(map) => ids.into_iter().map(|row| map.id_of(row)).collect(),
                    None => ids,
                })
                .collect()
        }
        GroundTruth::FullDepth => {
            let mut truth = Vec::with_capacity(shape.n_queries);
            for (index, vector) in spec.queries.chunks_exact(shape.dim).enumerate() {
                truth.push(full_depth_truth(route, vector, index, k_max, identity.rows).await?);
            }
            truth
        }
    };
    for (index, ids) in truth.iter().enumerate() {
        if ids.len() != k_max as usize {
            return Err(format!(
                "ground truth for query {index} has {} ids, not k={k_max}",
                ids.len()
            ));
        }
    }
    Ok(truth)
}

/// One query's ground truth through the public route at
/// `selection_k = corpus rows`: FP32 rerank over every row.
async fn full_depth_truth<R: ProfileRoute>(
    route: &mut R,
    vector: &[f32],
    index: usize,
    k_max: u32,
    rows: u64,
) -> Result<Vec<u64>, String> {
    let selection_k = u32::try_from(rows).map_err(|_| {
        format!("full-depth ground truth needs selection_k = {rows} rows, above the wire's u32")
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
    Ok(response.hits.iter().map(|hit| hit.doc_id).collect())
}

/// One `(k, depth)` rung of the ladder.
struct Rung {
    k: u32,
    depth: u32,
    corpus_rows: u64,
}

/// Per-query samples of one rung.
#[derive(Default)]
struct RungSamples {
    hits: Vec<u64>,
    total_ms: Vec<f64>,
    selection_ms: Vec<f64>,
    rerank_ms: Vec<f64>,
    client_ms: Vec<f64>,
}

impl Rung {
    /// Run every held-out query at this rung and summarize its recall
    /// against the ground truth and its p50 phases.
    async fn measure<R: ProfileRoute>(
        &self,
        route: &mut R,
        spec: &MeasureSpec<'_>,
        shape: &Ladder,
        truth_k: &[HashSet<u64>],
    ) -> Result<LadderRow, String> {
        let mut samples = RungSamples::default();
        for (index, vector) in spec.queries.chunks_exact(shape.dim).enumerate() {
            self.sample(route, vector, index, &truth_k[index], &mut samples)
                .await?;
        }
        Ok(self.summarize(shape, samples))
    }

    async fn sample<R: ProfileRoute>(
        &self,
        route: &mut R,
        vector: &[f32],
        index: usize,
        truth: &HashSet<u64>,
        samples: &mut RungSamples,
    ) -> Result<(), String> {
        let (k, depth) = (self.k, self.depth);
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
        samples
            .client_ms
            .push(started.elapsed().as_secs_f64() * 1e3);
        check_exact(&response, k, index)?;
        let profile = response.profile.as_ref().ok_or_else(|| {
            format!("k={k} selection_k={depth} query {index} carries no QueryProfile")
        })?;
        samples.total_ms.push(f64::from(profile.total_ms));
        samples.selection_ms.push(f64::from(profile.selection_ms));
        samples.rerank_ms.push(f64::from(profile.rerank_ms));
        let hits = response
            .hits
            .iter()
            .filter(|hit| truth.contains(&hit.doc_id))
            .count() as u64;
        if u64::from(depth) == self.corpus_rows && hits != u64::from(k) {
            return Err(format!(
                "full-depth rerank at selection_k={depth} recovered {hits} of k={k} for \
                 query {index}; the ground truth does not describe this generation"
            ));
        }
        samples.hits.push(hits);
        Ok(())
    }

    fn summarize(&self, shape: &Ladder, mut samples: RungSamples) -> LadderRow {
        let k = u64::from(self.k);
        let sum: u64 = samples.hits.iter().sum();
        let min = *samples.hits.iter().min().expect("at least one query");
        let recall_ppm: Vec<u32> = samples.hits.iter().map(|&h| (h * PPM / k) as u32).collect();
        LadderRow {
            measurement: ProfileMeasurement {
                k: self.k,
                candidates: self.depth,
                queries: shape.n_queries_u32,
                mean_recall_ppm: (sum * PPM / (shape.n_queries as u64 * k)) as u32,
                min_recall_ppm: (min * PPM / k) as u32,
                p50_total_ms: p50(&mut samples.total_ms),
                p50_selection_ms: p50(&mut samples.selection_ms),
                p50_rerank_ms: p50(&mut samples.rerank_ms),
            },
            p50_client_ms: p50(&mut samples.client_ms),
            recall_ppm,
        }
    }
}

/// Choose the points from the measured rungs and build the profile
/// bound to the live identity; no point met means no profile.
fn build_profile(
    spec: &MeasureSpec<'_>,
    shape: &Ladder,
    identity: LiveIdentity,
    ladder: Vec<LadderRow>,
) -> Result<MeasuredProfile, String> {
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
        shape.n_queries_u32,
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
