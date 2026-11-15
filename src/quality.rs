//! Generation-bound candidate-depth profiles for FP32 vector reranking
//! (`docs/dense-quality-profile.md`).
//!
//! A profile is measured data, not an engine default. It binds a set of
//! `(k, recall target) -> candidate depth` observations to the provider score
//! identity and corpus generation that produced them. Resolution is exact:
//! the server never interpolates an unmeasured target or silently falls back
//! to an expansion factor.
//!
//! Format version 2 adds the evidence: the measured ladder every point is
//! drawn from (per-depth mean and worst-query recall, per-phase p50
//! latency) and an optional default target `DENSE_EXECUTION_MODE_AUTO`
//! resolves through. A point that claims more than the ladder measured is
//! refused at load; that is the thing this format exists to prevent.
//! Version 1 files (points only) still load unchanged and never carry the
//! new fields. [`measure`] produces version 2 files from a live cluster.

use std::path::Path;

use serde::Deserialize;

/// The version [`DenseQualityProfile::to_toml`] writes.
const FORMAT_VERSION: u32 = 2;
/// Points-only files: still read, never written.
const LEGACY_FORMAT_VERSION: u32 = 1;
const PPM: u32 = 1_000_000;

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfileFile {
    format_version: u32,
    profile_id: String,
    embedding_model: String,
    corpus_generation: u64,
    corpus_rows: u64,
    dimensions: u32,
    provider_backend: String,
    scoring_fingerprint: String,
    measured_queries: u32,
    #[serde(default)]
    default_target_recall_ppm: Option<u32>,
    #[serde(default)]
    measurements: Vec<ProfileMeasurement>,
    points: Vec<ProfilePoint>,
}

/// One resolvable `(k, target) -> candidates` entry.
#[derive(Clone, Copy, Debug, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub struct ProfilePoint {
    pub k: u32,
    pub target_recall_ppm: u32,
    pub candidates: u32,
}

/// One rung of the measured ladder: `queries` held-out queries ran the
/// public FP32 rerank at `selection_k = candidates`, and their top-`k`
/// was compared with the exhaustive FP32 top-`k`. Recall is in parts per
/// million; latencies are the p50 of `QueryProfile` phases in
/// milliseconds.
#[derive(Clone, Debug, Deserialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ProfileMeasurement {
    pub k: u32,
    pub candidates: u32,
    pub queries: u32,
    pub mean_recall_ppm: u32,
    pub min_recall_ppm: u32,
    pub p50_total_ms: f64,
    pub p50_selection_ms: f64,
    pub p50_rerank_ms: f64,
}

/// The live identity a profile binds to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProfileIdentity {
    pub profile_id: String,
    pub embedding_model: String,
    pub corpus_generation: u64,
    pub corpus_rows: u64,
    pub dimensions: u32,
    pub provider_backend: String,
    pub scoring_fingerprint: String,
}

/// A `(k, target)` no measured depth satisfied on its worst query.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct UnmetTarget {
    pub k: u32,
    pub target_recall_ppm: u32,
    /// The best worst-query recall any depth reached for this `k`.
    pub best_min_recall_ppm: u32,
    /// The depth that reached it.
    pub best_candidates: u32,
}

/// What [`choose_points`] picked and what it could not.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ChosenPoints {
    pub points: Vec<ProfilePoint>,
    pub unmet: Vec<UnmetTarget>,
}

/// Validated immutable profile held by the coordinator.
#[derive(Clone, Debug)]
pub struct DenseQualityProfile {
    identity: ProfileIdentity,
    measured_queries: u32,
    default_target_recall_ppm: Option<u32>,
    /// Sorted by `(k, candidates)`.
    measurements: Vec<ProfileMeasurement>,
    /// Sorted by `(k, target_recall_ppm)`.
    points: Vec<ProfilePoint>,
    fingerprint: String,
}

/// One exact profile resolution.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DenseQualityResolution {
    pub target_recall_ppm: u32,
    pub selection_k: u32,
    pub profile_fingerprint: String,
    pub profile_id: String,
    pub embedding_model: String,
    pub corpus_generation: u64,
    pub corpus_rows: u64,
    pub dimensions: u32,
    pub provider_backend: String,
    pub scoring_fingerprint: String,
}

/// Pick, for every measured `k` and every target, the smallest measured
/// depth whose WORST-query recall meets the target. The mean is reported
/// but never decides: a point promises every query, not the average one.
/// A `(k, target)` no depth satisfies is reported in `unmet`, never
/// interpolated. Targets are deduplicated; each must lie in
/// `1..=1000000`.
pub fn choose_points(
    measurements: &[ProfileMeasurement],
    targets: &[u32],
) -> Result<ChosenPoints, String> {
    let mut targets = targets.to_vec();
    targets.sort_unstable();
    targets.dedup();
    if let Some(bad) = targets.iter().find(|t| !(1..=PPM).contains(t)) {
        return Err(format!("target_recall_ppm {bad} is outside 1..=1000000"));
    }
    let mut ladder: Vec<&ProfileMeasurement> = measurements.iter().collect();
    ladder.sort_by_key(|m| (m.k, m.candidates));
    let mut ks: Vec<u32> = ladder.iter().map(|m| m.k).collect();
    ks.dedup();
    let mut chosen = ChosenPoints::default();
    for k in ks {
        let rungs: Vec<&&ProfileMeasurement> = ladder.iter().filter(|m| m.k == k).collect();
        for &target in &targets {
            match rungs.iter().find(|m| m.min_recall_ppm >= target) {
                Some(rung) => chosen.points.push(ProfilePoint {
                    k,
                    target_recall_ppm: target,
                    candidates: rung.candidates,
                }),
                None => {
                    let best = rungs
                        .iter()
                        .max_by_key(|m| (m.min_recall_ppm, std::cmp::Reverse(m.candidates)))
                        .expect("a measured k has at least one rung");
                    chosen.unmet.push(UnmetTarget {
                        k,
                        target_recall_ppm: target,
                        best_min_recall_ppm: best.min_recall_ppm,
                        best_candidates: best.candidates,
                    });
                }
            }
        }
    }
    Ok(chosen)
}

impl DenseQualityProfile {
    pub fn load(path: &Path) -> Result<Self, String> {
        let bytes = std::fs::read(path)
            .map_err(|error| format!("read dense quality profile {}: {error}", path.display()))?;
        let text = std::str::from_utf8(&bytes).map_err(|error| {
            format!(
                "dense quality profile {} is not UTF-8: {error}",
                path.display()
            )
        })?;
        Self::parse(text)
            .map_err(|error| format!("dense quality profile {}: {error}", path.display()))
    }

    pub fn parse(text: &str) -> Result<Self, String> {
        let file: ProfileFile = toml::from_str(text).map_err(|error| error.to_string())?;
        Self::from_file(file, crate::sha256::hex_digest(text.as_bytes()))
    }

    /// Build a version 2 profile from a measured ladder. Every point must
    /// be justified by a rung at its `(k, candidates)` whose worst-query
    /// recall meets its target; [`choose_points`] produces such points.
    /// The fingerprint is that of the document [`Self::to_toml`] writes,
    /// so a saved-then-loaded copy resolves identically.
    pub fn from_measurements(
        identity: ProfileIdentity,
        measured_queries: u32,
        default_target_recall_ppm: Option<u32>,
        measurements: Vec<ProfileMeasurement>,
        points: Vec<ProfilePoint>,
    ) -> Result<Self, String> {
        if measurements.is_empty() {
            return Err("a measured profile needs at least one measurement".into());
        }
        let file = ProfileFile {
            format_version: FORMAT_VERSION,
            profile_id: identity.profile_id,
            embedding_model: identity.embedding_model,
            corpus_generation: identity.corpus_generation,
            corpus_rows: identity.corpus_rows,
            dimensions: identity.dimensions,
            provider_backend: identity.provider_backend,
            scoring_fingerprint: identity.scoring_fingerprint,
            measured_queries,
            default_target_recall_ppm,
            measurements,
            points,
        };
        let unfingerprinted = Self::from_file(file, String::new())?;
        Self::parse(&unfingerprinted.to_toml())
    }

    /// Write the profile as the strict TOML document `load` reads, fsynced.
    pub fn save(&self, path: &Path) -> Result<(), String> {
        let text = self.to_toml();
        let write = |path: &Path| -> std::io::Result<()> {
            use std::io::Write;
            let mut file = std::fs::File::create(path)?;
            file.write_all(text.as_bytes())?;
            file.sync_all()
        };
        write(path)
            .map_err(|error| format!("write dense quality profile {}: {error}", path.display()))
    }

    /// The strict document form, always `format_version = 2`. Every string
    /// field is validated at construction to need no escaping, so this is
    /// exact; floats print through `{:?}`, which round-trips.
    pub fn to_toml(&self) -> String {
        let id = &self.identity;
        let mut out = String::new();
        out.push_str(&format!("format_version = {FORMAT_VERSION}\n"));
        out.push_str(&format!("profile_id = \"{}\"\n", id.profile_id));
        out.push_str(&format!("embedding_model = \"{}\"\n", id.embedding_model));
        out.push_str(&format!("corpus_generation = {}\n", id.corpus_generation));
        out.push_str(&format!("corpus_rows = {}\n", id.corpus_rows));
        out.push_str(&format!("dimensions = {}\n", id.dimensions));
        out.push_str(&format!("provider_backend = \"{}\"\n", id.provider_backend));
        out.push_str(&format!(
            "scoring_fingerprint = \"{}\"\n",
            id.scoring_fingerprint
        ));
        out.push_str(&format!("measured_queries = {}\n", self.measured_queries));
        if let Some(target) = self.default_target_recall_ppm {
            out.push_str(&format!("default_target_recall_ppm = {target}\n"));
        }
        for m in &self.measurements {
            out.push_str("\n[[measurements]]\n");
            out.push_str(&format!("k = {}\n", m.k));
            out.push_str(&format!("candidates = {}\n", m.candidates));
            out.push_str(&format!("queries = {}\n", m.queries));
            out.push_str(&format!("mean_recall_ppm = {}\n", m.mean_recall_ppm));
            out.push_str(&format!("min_recall_ppm = {}\n", m.min_recall_ppm));
            out.push_str(&format!("p50_total_ms = {:?}\n", m.p50_total_ms));
            out.push_str(&format!("p50_selection_ms = {:?}\n", m.p50_selection_ms));
            out.push_str(&format!("p50_rerank_ms = {:?}\n", m.p50_rerank_ms));
        }
        for point in &self.points {
            out.push_str("\n[[points]]\n");
            out.push_str(&format!("k = {}\n", point.k));
            out.push_str(&format!(
                "target_recall_ppm = {}\n",
                point.target_recall_ppm
            ));
            out.push_str(&format!("candidates = {}\n", point.candidates));
        }
        out
    }

    pub fn resolve(
        &self,
        k: u32,
        target_recall_ppm: u32,
        required_fingerprint: &str,
        max_candidates: u32,
    ) -> Result<DenseQualityResolution, String> {
        if k == 0 {
            return Err("a measured dense quality policy requires explicit k".into());
        }
        if !(1..=PPM).contains(&target_recall_ppm) {
            return Err(format!(
                "target_recall_ppm={target_recall_ppm} is outside 1..=1000000"
            ));
        }
        if !required_fingerprint.is_empty() && required_fingerprint != self.fingerprint {
            return Err(format!(
                "required quality profile fingerprint {required_fingerprint} does not match loaded {}",
                self.fingerprint
            ));
        }
        let candidates = self
            .points
            .binary_search_by_key(&(k, target_recall_ppm), |point| {
                (point.k, point.target_recall_ppm)
            })
            .ok()
            .map(|index| self.points[index].candidates)
            .ok_or_else(|| {
                format!(
                    "quality profile {:?} has no measured point for k={k}, target_recall_ppm={target_recall_ppm}; interpolation and fallback factors are forbidden",
                    self.identity.profile_id
                )
            })?;
        if max_candidates != 0 && candidates > max_candidates {
            return Err(format!(
                "quality profile resolves to {candidates} candidates, above request max_candidates={max_candidates}; the depth is not clamped"
            ));
        }
        Ok(DenseQualityResolution {
            target_recall_ppm,
            selection_k: candidates,
            profile_fingerprint: self.fingerprint.clone(),
            profile_id: self.identity.profile_id.clone(),
            embedding_model: self.identity.embedding_model.clone(),
            corpus_generation: self.identity.corpus_generation,
            corpus_rows: self.identity.corpus_rows,
            dimensions: self.identity.dimensions,
            provider_backend: self.identity.provider_backend.clone(),
            scoring_fingerprint: self.identity.scoring_fingerprint.clone(),
        })
    }

    pub fn measured_queries(&self) -> u32 {
        self.measured_queries
    }

    pub fn profile_id(&self) -> &str {
        &self.identity.profile_id
    }

    /// SHA-256 of the file bytes; what `required_profile_fingerprint` pins.
    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    pub fn identity(&self) -> &ProfileIdentity {
        &self.identity
    }

    /// The target `DENSE_EXECUTION_MODE_AUTO` with FP32 rerank resolves
    /// through when the request names no policy and no `selection_k`.
    /// Absent on version 1 files and on profiles measured without one.
    pub fn default_target_recall_ppm(&self) -> Option<u32> {
        self.default_target_recall_ppm
    }

    /// The measured ladder, sorted by `(k, candidates)`; empty on
    /// version 1 files.
    pub fn measurements(&self) -> &[ProfileMeasurement] {
        &self.measurements
    }

    /// The resolvable points, sorted by `(k, target_recall_ppm)`.
    pub fn points(&self) -> &[ProfilePoint] {
        &self.points
    }
}

mod file;
pub mod measure;
#[cfg(test)]
mod tests;
