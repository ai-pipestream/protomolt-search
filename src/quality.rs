//! Generation-bound candidate-depth profiles for FP32 vector reranking.
//!
//! A profile is measured data, not an engine default. It binds a set of
//! `(k, recall target) -> candidate depth` observations to the provider score
//! identity and corpus generation that produced them. Resolution is exact:
//! the server never interpolates an unmeasured target or silently falls back
//! to an expansion factor.

use std::path::Path;

use serde::Deserialize;

const FORMAT_VERSION: u32 = 1;

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
    points: Vec<ProfilePoint>,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct ProfilePoint {
    k: u32,
    target_recall_ppm: u32,
    candidates: u32,
}

/// Validated immutable profile held by the coordinator.
#[derive(Clone, Debug)]
pub struct DenseQualityProfile {
    profile_id: String,
    embedding_model: String,
    corpus_generation: u64,
    corpus_rows: u64,
    dimensions: u32,
    provider_backend: String,
    scoring_fingerprint: String,
    measured_queries: u32,
    points: Vec<(u32, u32, u32)>,
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
        let file: ProfileFile = toml::from_str(text)
            .map_err(|error| format!("parse dense quality profile {}: {error}", path.display()))?;
        Self::from_file(file, crate::sha256::hex_digest(&bytes))
            .map_err(|error| format!("dense quality profile {}: {error}", path.display()))
    }

    #[cfg(test)]
    pub(crate) fn parse(text: &str) -> Result<Self, String> {
        let file: ProfileFile = toml::from_str(text).map_err(|error| error.to_string())?;
        Self::from_file(file, crate::sha256::hex_digest(text.as_bytes()))
    }

    fn from_file(mut file: ProfileFile, fingerprint: String) -> Result<Self, String> {
        if file.format_version != FORMAT_VERSION {
            return Err(format!(
                "format_version {} is unsupported; expected {FORMAT_VERSION}",
                file.format_version
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
        }
        if file.corpus_rows == 0 || file.dimensions == 0 || file.measured_queries == 0 {
            return Err(
                "corpus_rows, dimensions, and measured_queries must all be positive".into(),
            );
        }
        if file.points.is_empty() {
            return Err("points must not be empty".into());
        }
        file.points
            .sort_by_key(|point| (point.k, point.target_recall_ppm));
        let mut points = Vec::with_capacity(file.points.len());
        let mut previous = None;
        for point in file.points {
            if point.k == 0 {
                return Err("profile point k must be positive".into());
            }
            if !(1..=1_000_000).contains(&point.target_recall_ppm) {
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
            points.push((point.k, point.target_recall_ppm, point.candidates));
        }
        Ok(Self {
            profile_id: file.profile_id,
            embedding_model: file.embedding_model,
            corpus_generation: file.corpus_generation,
            corpus_rows: file.corpus_rows,
            dimensions: file.dimensions,
            provider_backend: file.provider_backend,
            scoring_fingerprint: file.scoring_fingerprint,
            measured_queries: file.measured_queries,
            points,
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
        if !(1..=1_000_000).contains(&target_recall_ppm) {
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
            .binary_search_by_key(&(k, target_recall_ppm), |&(pk, target, _)| (pk, target))
            .ok()
            .map(|index| self.points[index].2)
            .ok_or_else(|| {
                format!(
                    "quality profile {:?} has no measured point for k={k}, target_recall_ppm={target_recall_ppm}; interpolation and fallback factors are forbidden",
                    self.profile_id
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
            profile_id: self.profile_id.clone(),
            embedding_model: self.embedding_model.clone(),
            corpus_generation: self.corpus_generation,
            corpus_rows: self.corpus_rows,
            dimensions: self.dimensions,
            provider_backend: self.provider_backend.clone(),
            scoring_fingerprint: self.scoring_fingerprint.clone(),
        })
    }

    pub fn measured_queries(&self) -> u32 {
        self.measured_queries
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
}
