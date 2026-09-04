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

pub mod measure;

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

    fn from_file(mut file: ProfileFile, fingerprint: String) -> Result<Self, String> {
        if file.format_version != FORMAT_VERSION && file.format_version != LEGACY_FORMAT_VERSION {
            return Err(format!(
                "format_version {} is unsupported; expected {LEGACY_FORMAT_VERSION} or {FORMAT_VERSION}",
                file.format_version
            ));
        }
        if file.format_version == LEGACY_FORMAT_VERSION
            && (file.default_target_recall_ppm.is_some() || !file.measurements.is_empty())
        {
            return Err(format!(
                "format_version {LEGACY_FORMAT_VERSION} carries neither default_target_recall_ppm \
                 nor measurements; write format_version {FORMAT_VERSION}"
            ));
        }
        for (name, value) in [
            ("profile_id", file.profile_id.as_str()),
            ("embedding_model", file.embedding_model.as_str()),
            ("provider_backend", file.provider_backend.as_str()),
            ("scoring_fingerprint", file.scoring_fingerprint.as_str()),
        ] {
            if value.trim().is_empty() {
                return Err(format!("{name} must not be empty"));
            }
            if let Some(bad) = value
                .chars()
                .find(|c| c.is_control() || matches!(c, '"' | '\\'))
            {
                return Err(format!(
                    "{name} contains {bad:?}; profile strings are plain text without quotes, \
                     backslashes, or control characters"
                ));
            }
        }
        if file.corpus_rows == 0 || file.dimensions == 0 || file.measured_queries == 0 {
            return Err(
                "corpus_rows, dimensions, and measured_queries must all be positive".into(),
            );
        }
        if file.points.is_empty() {
            return Err("points must not be empty".into());
        }

        file.measurements.sort_by_key(|m| (m.k, m.candidates));
        let mut previous = None;
        for m in &file.measurements {
            if m.k == 0 {
                return Err("measurement k must be positive".into());
            }
            if m.candidates < m.k || u64::from(m.candidates) > file.corpus_rows {
                return Err(format!(
                    "measurement k={} has invalid candidate depth {} for {} rows",
                    m.k, m.candidates, file.corpus_rows
                ));
            }
            if m.queries == 0 {
                return Err(format!(
                    "measurement k={} candidates={} measured no queries",
                    m.k, m.candidates
                ));
            }
            if m.mean_recall_ppm > PPM
                || m.min_recall_ppm > PPM
                || m.min_recall_ppm > m.mean_recall_ppm
            {
                return Err(format!(
                    "measurement k={} candidates={} has recall mean={} min={} outside 0..=1000000 \
                     or with the minimum above the mean",
                    m.k, m.candidates, m.mean_recall_ppm, m.min_recall_ppm
                ));
            }
            for (name, ms) in [
                ("p50_total_ms", m.p50_total_ms),
                ("p50_selection_ms", m.p50_selection_ms),
                ("p50_rerank_ms", m.p50_rerank_ms),
            ] {
                if !ms.is_finite() || ms < 0.0 {
                    return Err(format!(
                        "measurement k={} candidates={} has {name} = {ms:?}; latencies are finite \
                         and non-negative",
                        m.k, m.candidates
                    ));
                }
            }
            let key = (m.k, m.candidates);
            if previous == Some(key) {
                return Err(format!(
                    "duplicate measurement k={} candidates={}",
                    m.k, m.candidates
                ));
            }
            previous = Some(key);
        }

        file.points
            .sort_by_key(|point| (point.k, point.target_recall_ppm));
        let mut previous = None;
        for point in &file.points {
            if point.k == 0 {
                return Err("profile point k must be positive".into());
            }
            if !(1..=PPM).contains(&point.target_recall_ppm) {
                return Err(format!(
                    "profile point target_recall_ppm {} is outside 1..=1000000",
                    point.target_recall_ppm
                ));
            }
            if point.candidates < point.k || u64::from(point.candidates) > file.corpus_rows {
                return Err(format!(
                    "profile point k={} target={} has invalid candidate depth {} for {} rows",
                    point.k, point.target_recall_ppm, point.candidates, file.corpus_rows
                ));
            }
            let key = (point.k, point.target_recall_ppm);
            if previous == Some(key) {
                return Err(format!(
                    "duplicate profile point k={} target_recall_ppm={}",
                    point.k, point.target_recall_ppm
                ));
            }
            previous = Some(key);
            if !file.measurements.is_empty() {
                // With a ladder present every point must be one of its rungs
                // and promise no more than that rung's worst query delivered.
                let rung = file
                    .measurements
                    .iter()
                    .find(|m| m.k == point.k && m.candidates == point.candidates)
                    .ok_or_else(|| {
                        format!(
                            "profile point k={} target_recall_ppm={} candidates={} is not \
                             justified: no measurement at that k and depth",
                            point.k, point.target_recall_ppm, point.candidates
                        )
                    })?;
                if rung.min_recall_ppm < point.target_recall_ppm {
                    return Err(format!(
                        "profile point k={} target_recall_ppm={} candidates={} claims more than \
                         was measured: min_recall_ppm={} over {} queries at that depth",
                        point.k,
                        point.target_recall_ppm,
                        point.candidates,
                        rung.min_recall_ppm,
                        rung.queries
                    ));
                }
            }
        }
        if let Some(target) = file.default_target_recall_ppm {
            if !(1..=PPM).contains(&target) {
                return Err(format!(
                    "default_target_recall_ppm {target} is outside 1..=1000000"
                ));
            }
            if !file.points.iter().any(|p| p.target_recall_ppm == target) {
                let mut targets: Vec<u32> =
                    file.points.iter().map(|p| p.target_recall_ppm).collect();
                targets.sort_unstable();
                targets.dedup();
                return Err(format!(
                    "default_target_recall_ppm {target} names no point; the points carry targets {targets:?}"
                ));
            }
        }
        Ok(Self {
            identity: ProfileIdentity {
                profile_id: file.profile_id,
                embedding_model: file.embedding_model,
                corpus_generation: file.corpus_generation,
                corpus_rows: file.corpus_rows,
                dimensions: file.dimensions,
                provider_backend: file.provider_backend,
                scoring_fingerprint: file.scoring_fingerprint,
            },
            measured_queries: file.measured_queries,
            default_target_recall_ppm: file.default_target_recall_ppm,
            measurements: file.measurements,
            points: file.points,
            fingerprint,
        })
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

#[cfg(test)]
mod tests {
    use super::*;

    const PROFILE: &str = r#"
format_version = 1
profile_id = "court-held-out-v1"
embedding_model = "minilm-static-256"
corpus_generation = 7
corpus_rows = 1000000
dimensions = 256
provider_backend = "embedded-turbovec"
scoring_fingerprint = "score-abc"
measured_queries = 128

[[points]]
k = 10000
target_recall_ppm = 990000
candidates = 12078

[[points]]
k = 10000
target_recall_ppm = 1000000
candidates = 17265
"#;

    /// The 100k / k=10,000 ladder from the challenge report, as a v2 file.
    const MEASURED: &str = r#"
format_version = 2
profile_id = "challenge-100k-k10000"
embedding_model = "synthetic-64"
corpus_generation = 3
corpus_rows = 100000
dimensions = 64
provider_backend = "embedded-turbovec"
scoring_fingerprint = "score-abc"
measured_queries = 16
default_target_recall_ppm = 990000

[[measurements]]
k = 10000
candidates = 10000
queries = 16
mean_recall_ppm = 891631
min_recall_ppm = 854800
p50_total_ms = 41.5
p50_selection_ms = 30.25
p50_rerank_ms = 9.0

[[measurements]]
k = 10000
candidates = 20000
queries = 16
mean_recall_ppm = 995543
min_recall_ppm = 991700
p50_total_ms = 52.0
p50_selection_ms = 33.0
p50_rerank_ms = 17.5

[[measurements]]
k = 10000
candidates = 30000
queries = 16
mean_recall_ppm = 999962
min_recall_ppm = 999800
p50_total_ms = 63.0
p50_selection_ms = 36.0
p50_rerank_ms = 25.0

[[measurements]]
k = 10000
candidates = 50000
queries = 16
mean_recall_ppm = 1000000
min_recall_ppm = 1000000
p50_total_ms = 90.0
p50_selection_ms = 42.0
p50_rerank_ms = 46.0

[[points]]
k = 10000
target_recall_ppm = 950000
candidates = 20000

[[points]]
k = 10000
target_recall_ppm = 990000
candidates = 20000

[[points]]
k = 10000
target_recall_ppm = 1000000
candidates = 50000
"#;

    fn ladder() -> Vec<ProfileMeasurement> {
        DenseQualityProfile::parse(MEASURED)
            .unwrap()
            .measurements()
            .to_vec()
    }

    #[test]
    fn exact_resolution_never_interpolates_or_clamps() {
        let profile = DenseQualityProfile::parse(PROFILE).unwrap();
        let point = profile.resolve(10_000, 990_000, "", 20_000).unwrap();
        assert_eq!(point.selection_k, 12_078);
        assert_eq!(point.corpus_generation, 7);
        assert_eq!(profile.measured_queries(), 128);
        assert!(profile.resolve(1_000, 990_000, "", 0).is_err());
        assert!(profile.resolve(10_000, 990_000, "", 12_000).is_err());
        assert!(profile
            .resolve(10_000, 990_000, "wrong-fingerprint", 0)
            .is_err());
    }

    #[test]
    fn duplicate_points_are_rejected() {
        let duplicate =
            format!("{PROFILE}\n[[points]]\nk=10000\ntarget_recall_ppm=990000\ncandidates=13000\n");
        assert!(DenseQualityProfile::parse(&duplicate)
            .unwrap_err()
            .contains("duplicate"));
    }

    #[test]
    fn a_version_1_file_loads_unchanged_and_carries_no_default() {
        let profile = DenseQualityProfile::parse(PROFILE).unwrap();
        assert_eq!(profile.default_target_recall_ppm(), None);
        assert!(profile.measurements().is_empty());
        assert_eq!(profile.points().len(), 2);
        assert_eq!(
            profile.fingerprint(),
            crate::sha256::hex_digest(PROFILE.as_bytes())
        );
        // The new fields are not smuggled into the old version.
        let with_default = PROFILE.replace(
            "measured_queries = 128",
            "measured_queries = 128\ndefault_target_recall_ppm = 990000",
        );
        let error = DenseQualityProfile::parse(&with_default).unwrap_err();
        assert!(
            error.contains("format_version 1 carries neither"),
            "{error}"
        );
        let error = DenseQualityProfile::parse(
            &PROFILE.replace("format_version = 1", "format_version = 3"),
        )
        .unwrap_err();
        assert!(error.contains("format_version 3 is unsupported"), "{error}");
        let error = DenseQualityProfile::parse(&format!("{PROFILE}\nexpansion_factor = 2.0\n"))
            .unwrap_err();
        assert!(error.contains("unknown field"), "{error}");
    }

    #[test]
    fn a_version_2_file_carries_its_evidence_and_default() {
        let profile = DenseQualityProfile::parse(MEASURED).unwrap();
        assert_eq!(profile.default_target_recall_ppm(), Some(990_000));
        assert_eq!(profile.measurements().len(), 4);
        assert_eq!(profile.measurements()[1].p50_rerank_ms, 17.5);
        assert_eq!(
            profile.resolve(10_000, 990_000, "", 0).unwrap().selection_k,
            20_000
        );
        assert_eq!(
            profile
                .resolve(10_000, 1_000_000, "", 0)
                .unwrap()
                .selection_k,
            50_000
        );
        assert!(profile.resolve(10_000, 999_000, "", 0).is_err());
    }

    #[test]
    fn choose_points_takes_the_smallest_depth_whose_worst_query_meets_the_target() {
        let chosen =
            choose_points(&ladder(), &[1_000_000, 950_000, 999_000, 990_000, 990_000]).unwrap();
        assert_eq!(
            chosen.points,
            vec![
                ProfilePoint {
                    k: 10_000,
                    target_recall_ppm: 950_000,
                    candidates: 20_000
                },
                ProfilePoint {
                    k: 10_000,
                    target_recall_ppm: 990_000,
                    candidates: 20_000
                },
                ProfilePoint {
                    k: 10_000,
                    target_recall_ppm: 999_000,
                    candidates: 30_000
                },
                ProfilePoint {
                    k: 10_000,
                    target_recall_ppm: 1_000_000,
                    candidates: 50_000
                },
            ]
        );
        assert!(chosen.unmet.is_empty());

        // The mean at 20,000 (99.55%) clears 99.5%; the worst query (99.17%)
        // does not, so the target moves to the next rung.
        let chosen = choose_points(&ladder(), &[995_000]).unwrap();
        assert_eq!(chosen.points[0].candidates, 30_000);

        // A target no rung's worst query reaches is reported, not invented.
        let shallow: Vec<ProfileMeasurement> = ladder().into_iter().take(2).collect();
        let chosen = choose_points(&shallow, &[990_000, 999_000]).unwrap();
        assert_eq!(chosen.points.len(), 1);
        assert_eq!(
            chosen.unmet,
            vec![UnmetTarget {
                k: 10_000,
                target_recall_ppm: 999_000,
                best_min_recall_ppm: 991_700,
                best_candidates: 20_000,
            }]
        );
        assert!(choose_points(&shallow, &[0]).is_err());
        assert!(choose_points(&shallow, &[1_000_001]).is_err());
    }

    #[test]
    fn an_unjustified_point_is_refused_by_name() {
        // A depth the ladder never measured.
        let error = DenseQualityProfile::parse(&MEASURED.replace(
            "target_recall_ppm = 950000\ncandidates = 20000",
            "target_recall_ppm = 950000\ncandidates = 15000",
        ))
        .unwrap_err();
        assert!(
            error.contains("candidates=15000 is not justified"),
            "{error}"
        );
        // A measured depth that promises more than its worst query delivered.
        let error = DenseQualityProfile::parse(&MEASURED.replace(
            "target_recall_ppm = 1000000\ncandidates = 50000",
            "target_recall_ppm = 1000000\ncandidates = 30000",
        ))
        .unwrap_err();
        assert!(
            error.contains("candidates=30000 claims more than was measured: min_recall_ppm=999800"),
            "{error}"
        );
        // A default that resolves nothing.
        let error = DenseQualityProfile::parse(&MEASURED.replace(
            "default_target_recall_ppm = 990000",
            "default_target_recall_ppm = 999000",
        ))
        .unwrap_err();
        assert!(
            error.contains("default_target_recall_ppm 999000 names no point"),
            "{error}"
        );
        // Malformed rungs.
        for (from, to, needle) in [
            (
                "min_recall_ppm = 991700",
                "min_recall_ppm = 999999",
                "minimum above the mean",
            ),
            (
                "p50_rerank_ms = 17.5",
                "p50_rerank_ms = -1.0",
                "p50_rerank_ms",
            ),
            (
                "p50_rerank_ms = 17.5",
                "p50_rerank_ms = inf",
                "p50_rerank_ms",
            ),
            (
                "queries = 16\nmean_recall_ppm = 995543",
                "queries = 0\nmean_recall_ppm = 995543",
                "measured no queries",
            ),
        ] {
            let error = DenseQualityProfile::parse(&MEASURED.replacen(from, to, 1)).unwrap_err();
            assert!(error.contains(needle), "{from} -> {to}: {error}");
        }
        let duplicate = format!(
            "{MEASURED}\n[[measurements]]\nk = 10000\ncandidates = 20000\nqueries = 16\n\
             mean_recall_ppm = 995543\nmin_recall_ppm = 991700\np50_total_ms = 1.0\n\
             p50_selection_ms = 1.0\np50_rerank_ms = 1.0\n"
        );
        assert!(DenseQualityProfile::parse(&duplicate)
            .unwrap_err()
            .contains("duplicate measurement"));
    }

    #[test]
    fn the_document_round_trips_and_the_fingerprint_is_the_bytes() {
        let parsed = DenseQualityProfile::parse(MEASURED).unwrap();
        let text = parsed.to_toml();
        let again = DenseQualityProfile::parse(&text).unwrap();
        assert_eq!(again.to_toml(), text);
        assert_eq!(again.measurements(), parsed.measurements());
        assert_eq!(again.points(), parsed.points());
        assert_eq!(again.identity(), parsed.identity());
        assert_eq!(again.default_target_recall_ppm(), Some(990_000));
        assert_eq!(
            again.fingerprint(),
            crate::sha256::hex_digest(text.as_bytes())
        );

        let chosen = choose_points(parsed.measurements(), &[950_000, 990_000, 1_000_000]).unwrap();
        let built = DenseQualityProfile::from_measurements(
            parsed.identity().clone(),
            16,
            Some(990_000),
            parsed.measurements().to_vec(),
            chosen.points,
        )
        .unwrap();
        assert_eq!(built.to_toml(), text);
        assert_eq!(built.fingerprint(), again.fingerprint());
        let dir = std::env::temp_dir().join(format!("dense-quality-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("profile.toml");
        built.save(&path).unwrap();
        let loaded = DenseQualityProfile::load(&path).unwrap();
        assert_eq!(loaded.fingerprint(), built.fingerprint());
        assert_eq!(
            loaded
                .resolve(10_000, 990_000, built.fingerprint(), 0)
                .unwrap(),
            built.resolve(10_000, 990_000, "", 0).unwrap()
        );
        let _ = std::fs::remove_dir_all(dir);

        // Points that the ladder does not justify are refused at build time
        // too, so the tool cannot write what the server would refuse.
        let error = DenseQualityProfile::from_measurements(
            parsed.identity().clone(),
            16,
            None,
            parsed.measurements().to_vec(),
            vec![ProfilePoint {
                k: 10_000,
                target_recall_ppm: 1_000_000,
                candidates: 20_000,
            }],
        )
        .unwrap_err();
        assert!(error.contains("claims more than was measured"), "{error}");
    }
}
