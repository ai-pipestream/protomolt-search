//! The dense execution policy behind `DENSE_EXECUTION_MODE_AUTO`
//! (`docs/dense-execution-policy.md`): a persisted measurement profile,
//! bound to one generation of one corpus on one provider, that says at
//! which request keys an approximate traversal was measured good enough
//! to stand in for the exhaustive one.
//!
//! The file is the persistence: a strict TOML document (unknown keys
//! refused), fingerprinted by SHA-256 of its bytes, written by
//! [`DenseExecutionPolicy::save`] and read by [`DenseExecutionPolicy::load`].
//! Its identity fields (embedding model, corpus generation and row count,
//! dimensions, provider kind, scoring fingerprint) are checked against
//! the live cluster before any point is consulted; a mismatch names the
//! field. Its points are keyed on the requested `k`, a filter selectivity
//! band in parts per million of the corpus, and the candidate depth the
//! provider is asked for. A request qualifies only at a point whose key
//! it matches exactly: no interpolation between points, no candidate
//! depth the file does not name, no default band.

use std::path::Path;

use serde::Deserialize;

pub const FORMAT_VERSION: u32 = 1;

/// Selectivity of a request without a filter: the whole corpus.
pub const UNFILTERED_PPM: u32 = 1_000_000;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PolicyFile {
    format_version: u32,
    policy_id: String,
    embedding_model: String,
    corpus_generation: u64,
    corpus_rows: u64,
    dimensions: u32,
    provider_backend: String,
    scoring_fingerprint: String,
    measured_queries: u32,
    points: Vec<PointFile>,
}

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
struct PointFile {
    k: u32,
    filter_selectivity_ppm_min: u32,
    filter_selectivity_ppm_max: u32,
    candidates: u32,
    measured_recall_ppm: u32,
}

/// One measured point: the key AUTO matches exactly, and the recall
/// measured there against the exhaustive traversal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PolicyPoint {
    pub k: u32,
    pub selectivity_min_ppm: u32,
    pub selectivity_max_ppm: u32,
    pub candidates: u32,
    pub measured_recall_ppm: u32,
}

/// What the live cluster reports; the policy must match it field by field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveIdentity {
    pub provider_backend: String,
    pub scoring_fingerprint: String,
    pub corpus_generation: u64,
    pub corpus_rows: u64,
    pub dimensions: u32,
}

/// The request side of the key. `candidate_depth` is the request's
/// `selection_k`; 0 means the request named none, which qualifies only
/// when the policy measured exactly one depth for that `k` and band.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestKey {
    pub k: u32,
    pub candidate_depth: u32,
    pub filter_selectivity_ppm: u32,
}

/// A qualified resolution: the policy and the point that admitted ANN.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Qualified {
    pub policy_id: String,
    pub policy_fingerprint: String,
    pub embedding_model: String,
    pub point: PolicyPoint,
}

/// A validated, immutable policy.
#[derive(Clone, Debug)]
pub struct DenseExecutionPolicy {
    policy_id: String,
    embedding_model: String,
    corpus_generation: u64,
    corpus_rows: u64,
    dimensions: u32,
    provider_backend: String,
    scoring_fingerprint: String,
    measured_queries: u32,
    /// Sorted by `(k, candidates, selectivity_min_ppm)`.
    points: Vec<PolicyPoint>,
    fingerprint: String,
}

impl DenseExecutionPolicy {
    pub fn load(path: &Path) -> Result<Self, String> {
        let bytes = std::fs::read(path)
            .map_err(|error| format!("read dense execution policy {}: {error}", path.display()))?;
        let text = std::str::from_utf8(&bytes).map_err(|error| {
            format!(
                "dense execution policy {} is not UTF-8: {error}",
                path.display()
            )
        })?;
        Self::parse(text)
            .map_err(|error| format!("dense execution policy {}: {error}", path.display()))
    }

    pub fn parse(text: &str) -> Result<Self, String> {
        let file: PolicyFile = toml::from_str(text).map_err(|error| error.to_string())?;
        Self::from_file(file, crate::sha256::hex_digest(text.as_bytes()))
    }

    /// Write the policy as the strict TOML document `load` reads, fsynced.
    /// The file's fingerprint is that of the bytes written, which
    /// [`Self::to_toml`] reproduces.
    pub fn save(&self, path: &Path) -> Result<(), String> {
        let text = self.to_toml();
        let write = |path: &Path| -> std::io::Result<()> {
            use std::io::Write;
            let mut file = std::fs::File::create(path)?;
            file.write_all(text.as_bytes())?;
            file.sync_all()
        };
        write(path)
            .map_err(|error| format!("write dense execution policy {}: {error}", path.display()))
    }

    /// The strict document form. Every string field is validated at
    /// construction to need no escaping, so this is exact.
    pub fn to_toml(&self) -> String {
        let mut out = String::new();
        out.push_str(&format!("format_version = {FORMAT_VERSION}\n"));
        out.push_str(&format!("policy_id = \"{}\"\n", self.policy_id));
        out.push_str(&format!("embedding_model = \"{}\"\n", self.embedding_model));
        out.push_str(&format!("corpus_generation = {}\n", self.corpus_generation));
        out.push_str(&format!("corpus_rows = {}\n", self.corpus_rows));
        out.push_str(&format!("dimensions = {}\n", self.dimensions));
        out.push_str(&format!(
            "provider_backend = \"{}\"\n",
            self.provider_backend
        ));
        out.push_str(&format!(
            "scoring_fingerprint = \"{}\"\n",
            self.scoring_fingerprint
        ));
        out.push_str(&format!("measured_queries = {}\n", self.measured_queries));
        for point in &self.points {
            out.push_str("\n[[points]]\n");
            out.push_str(&format!("k = {}\n", point.k));
            out.push_str(&format!(
                "filter_selectivity_ppm_min = {}\n",
                point.selectivity_min_ppm
            ));
            out.push_str(&format!(
                "filter_selectivity_ppm_max = {}\n",
                point.selectivity_max_ppm
            ));
            out.push_str(&format!("candidates = {}\n", point.candidates));
            out.push_str(&format!(
                "measured_recall_ppm = {}\n",
                point.measured_recall_ppm
            ));
        }
        out
    }

    fn from_file(mut file: PolicyFile, fingerprint: String) -> Result<Self, String> {
        if file.format_version != FORMAT_VERSION {
            return Err(format!(
                "format_version {} is unsupported; expected {FORMAT_VERSION}",
                file.format_version
            ));
        }
        for (name, value) in [
            ("policy_id", file.policy_id.as_str()),
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
                    "{name} contains {bad:?}; policy strings are plain text without quotes, \
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
        file.points.sort_by_key(|p| {
            (
                p.k,
                p.candidates,
                p.filter_selectivity_ppm_min,
                p.filter_selectivity_ppm_max,
            )
        });
        let mut points: Vec<PolicyPoint> = Vec::with_capacity(file.points.len());
        for p in file.points {
            if p.k == 0 {
                return Err("policy point k must be positive".into());
            }
            let ppm = 1..=UNFILTERED_PPM;
            if !ppm.contains(&p.filter_selectivity_ppm_min)
                || !ppm.contains(&p.filter_selectivity_ppm_max)
                || p.filter_selectivity_ppm_min > p.filter_selectivity_ppm_max
            {
                return Err(format!(
                    "policy point k={} has selectivity band {}..={} outside 1..=1000000 or \
                     inverted",
                    p.k, p.filter_selectivity_ppm_min, p.filter_selectivity_ppm_max
                ));
            }
            if !ppm.contains(&p.measured_recall_ppm) {
                return Err(format!(
                    "policy point k={} has measured_recall_ppm {} outside 1..=1000000",
                    p.k, p.measured_recall_ppm
                ));
            }
            if p.candidates < p.k || u64::from(p.candidates) > file.corpus_rows {
                return Err(format!(
                    "policy point k={} has invalid candidate depth {} for {} rows",
                    p.k, p.candidates, file.corpus_rows
                ));
            }
            let point = PolicyPoint {
                k: p.k,
                selectivity_min_ppm: p.filter_selectivity_ppm_min,
                selectivity_max_ppm: p.filter_selectivity_ppm_max,
                candidates: p.candidates,
                measured_recall_ppm: p.measured_recall_ppm,
            };
            if let Some(prev) = points
                .last()
                .filter(|q| q.k == point.k && q.candidates == point.candidates)
            {
                if point.selectivity_min_ppm <= prev.selectivity_max_ppm {
                    return Err(format!(
                        "policy points for k={} candidates={} overlap on selectivity: {}..={} \
                         and {}..={}",
                        point.k,
                        point.candidates,
                        prev.selectivity_min_ppm,
                        prev.selectivity_max_ppm,
                        point.selectivity_min_ppm,
                        point.selectivity_max_ppm
                    ));
                }
            }
            points.push(point);
        }
        Ok(DenseExecutionPolicy {
            policy_id: file.policy_id,
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

    /// The policy binds to one live identity; the first field that
    /// differs is named.
    pub fn verify_identity(&self, live: &LiveIdentity) -> Result<(), String> {
        for (name, expected, actual) in [
            (
                "provider_backend",
                self.provider_backend.as_str(),
                live.provider_backend.as_str(),
            ),
            (
                "scoring_fingerprint",
                self.scoring_fingerprint.as_str(),
                live.scoring_fingerprint.as_str(),
            ),
        ] {
            if expected != actual {
                return Err(format!(
                    "dense execution policy {:?} {name} {expected:?} does not match live {actual:?}",
                    self.policy_id
                ));
            }
        }
        for (name, expected, actual) in [
            (
                "corpus_generation",
                self.corpus_generation,
                live.corpus_generation,
            ),
            ("corpus_rows", self.corpus_rows, live.corpus_rows),
            (
                "dimensions",
                u64::from(self.dimensions),
                u64::from(live.dimensions),
            ),
        ] {
            if expected != actual {
                return Err(format!(
                    "dense execution policy {:?} {name} {expected} does not match live {actual}",
                    self.policy_id
                ));
            }
        }
        Ok(())
    }

    /// The point a request key matches exactly, or a refusal naming the
    /// key and what the policy measured for that `k`.
    pub fn qualify(&self, key: RequestKey) -> Result<Qualified, String> {
        if key.k == 0 {
            return Err("AUTO through a dense execution policy requires an explicit k".into());
        }
        if !(1..=UNFILTERED_PPM).contains(&key.filter_selectivity_ppm) {
            return Err(format!(
                "filter selectivity {} ppm is outside 1..=1000000",
                key.filter_selectivity_ppm
            ));
        }
        let for_k: Vec<&PolicyPoint> = self.points.iter().filter(|p| p.k == key.k).collect();
        if for_k.is_empty() {
            return Err(format!(
                "dense execution policy {:?} measured no point for k={}; measured k values: {:?}",
                self.policy_id,
                key.k,
                self.measured_ks()
            ));
        }
        let in_band: Vec<&PolicyPoint> = for_k
            .iter()
            .copied()
            .filter(|p| {
                (p.selectivity_min_ppm..=p.selectivity_max_ppm)
                    .contains(&key.filter_selectivity_ppm)
            })
            .collect();
        if in_band.is_empty() {
            let bands: Vec<String> = for_k
                .iter()
                .map(|p| {
                    format!(
                        "{}..={} ppm at depth {}",
                        p.selectivity_min_ppm, p.selectivity_max_ppm, p.candidates
                    )
                })
                .collect();
            return Err(format!(
                "dense execution policy {:?} measured no point for k={} at filter selectivity \
                 {} ppm; measured bands for that k: [{}]",
                self.policy_id,
                key.k,
                key.filter_selectivity_ppm,
                bands.join(", ")
            ));
        }
        let point = if key.candidate_depth == 0 {
            match in_band.as_slice() {
                [only] => **only,
                many => {
                    let depths: Vec<u32> = many.iter().map(|p| p.candidates).collect();
                    return Err(format!(
                        "dense execution policy {:?} measured k={} at selectivity {} ppm at \
                         several candidate depths {depths:?}; set selection_k to one of them",
                        self.policy_id, key.k, key.filter_selectivity_ppm
                    ));
                }
            }
        } else {
            match in_band.iter().find(|p| p.candidates == key.candidate_depth) {
                Some(point) => **point,
                None => {
                    let depths: Vec<u32> = in_band.iter().map(|p| p.candidates).collect();
                    return Err(format!(
                        "dense execution policy {:?} measured k={} at selectivity {} ppm at \
                         candidate depths {depths:?}, not at the requested {}; the depth is not \
                         interpolated",
                        self.policy_id, key.k, key.filter_selectivity_ppm, key.candidate_depth
                    ));
                }
            }
        };
        Ok(Qualified {
            policy_id: self.policy_id.clone(),
            policy_fingerprint: self.fingerprint.clone(),
            embedding_model: self.embedding_model.clone(),
            point,
        })
    }

    fn measured_ks(&self) -> Vec<u32> {
        let mut ks: Vec<u32> = self.points.iter().map(|p| p.k).collect();
        ks.dedup();
        ks
    }

    pub fn policy_id(&self) -> &str {
        &self.policy_id
    }

    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    pub fn measured_queries(&self) -> u32 {
        self.measured_queries
    }

    pub fn points(&self) -> &[PolicyPoint] {
        &self.points
    }

    pub fn identity(&self) -> LiveIdentity {
        LiveIdentity {
            provider_backend: self.provider_backend.clone(),
            scoring_fingerprint: self.scoring_fingerprint.clone(),
            corpus_generation: self.corpus_generation,
            corpus_rows: self.corpus_rows,
            dimensions: self.dimensions,
        }
    }
}

/// Filter selectivity in parts per million: `admitted` rows of `rows`,
/// floored, never below 1 when anything is admitted, and the whole corpus
/// (1,000,000) when nothing filters.
pub fn selectivity_ppm(admitted: u64, rows: u64) -> u32 {
    if rows == 0 || admitted >= rows {
        return UNFILTERED_PPM;
    }
    if admitted == 0 {
        return 0;
    }
    let ppm = admitted.saturating_mul(u64::from(UNFILTERED_PPM)) / rows;
    u32::try_from(ppm.max(1)).unwrap_or(UNFILTERED_PPM)
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEXT: &str = r#"
format_version = 1
policy_id = "court-ann-v1"
embedding_model = "bge-m3"
corpus_generation = 7
corpus_rows = 1000
dimensions = 64
provider_backend = "fake-ann"
scoring_fingerprint = "fake-ann:v1"
measured_queries = 32

[[points]]
k = 10
filter_selectivity_ppm_min = 1000000
filter_selectivity_ppm_max = 1000000
candidates = 100
measured_recall_ppm = 990000

[[points]]
k = 10
filter_selectivity_ppm_min = 100000
filter_selectivity_ppm_max = 500000
candidates = 200
measured_recall_ppm = 970000

[[points]]
k = 10
filter_selectivity_ppm_min = 100000
filter_selectivity_ppm_max = 500000
candidates = 400
measured_recall_ppm = 995000
"#;

    fn live() -> LiveIdentity {
        LiveIdentity {
            provider_backend: "fake-ann".into(),
            scoring_fingerprint: "fake-ann:v1".into(),
            corpus_generation: 7,
            corpus_rows: 1000,
            dimensions: 64,
        }
    }

    #[test]
    fn the_document_round_trips_and_the_fingerprint_is_the_bytes() {
        let policy = DenseExecutionPolicy::parse(TEXT).unwrap();
        assert_eq!(policy.points().len(), 3);
        let text = policy.to_toml();
        let again = DenseExecutionPolicy::parse(&text).unwrap();
        assert_eq!(again.points(), policy.points());
        assert_eq!(again.identity(), policy.identity());
        assert_eq!(again.policy_id(), "court-ann-v1");
        assert_eq!(
            again.fingerprint(),
            crate::sha256::hex_digest(text.as_bytes())
        );
        // The saved file is the same document.
        let dir = std::env::temp_dir().join(format!("dense-policy-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("policy.toml");
        policy.save(&path).unwrap();
        let loaded = DenseExecutionPolicy::load(&path).unwrap();
        assert_eq!(loaded.fingerprint(), again.fingerprint());
        assert_eq!(loaded.points(), policy.points());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_key_qualifies_only_at_an_exact_point() {
        let policy = DenseExecutionPolicy::parse(TEXT).unwrap();
        policy.verify_identity(&live()).unwrap();
        // Unfiltered, one depth measured: the depth need not be named.
        let hit = policy
            .qualify(RequestKey {
                k: 10,
                candidate_depth: 0,
                filter_selectivity_ppm: UNFILTERED_PPM,
            })
            .unwrap();
        assert_eq!(hit.point.candidates, 100);
        assert_eq!(hit.point.measured_recall_ppm, 990_000);
        assert_eq!(hit.policy_fingerprint, policy.fingerprint());
        // A named depth must be a measured one.
        let miss = policy
            .qualify(RequestKey {
                k: 10,
                candidate_depth: 150,
                filter_selectivity_ppm: UNFILTERED_PPM,
            })
            .unwrap_err();
        assert!(miss.contains("not at the requested 150"), "{miss}");
        // Inside the filtered band, two depths were measured: ambiguous
        // without selection_k, exact with it.
        let ambiguous = policy
            .qualify(RequestKey {
                k: 10,
                candidate_depth: 0,
                filter_selectivity_ppm: 250_000,
            })
            .unwrap_err();
        assert!(ambiguous.contains("[200, 400]"), "{ambiguous}");
        let deep = policy
            .qualify(RequestKey {
                k: 10,
                candidate_depth: 400,
                filter_selectivity_ppm: 500_000,
            })
            .unwrap();
        assert_eq!(deep.point.measured_recall_ppm, 995_000);
        // Outside every band, or at an unmeasured k: refused by name.
        let outside = policy
            .qualify(RequestKey {
                k: 10,
                candidate_depth: 0,
                filter_selectivity_ppm: 50_000,
            })
            .unwrap_err();
        assert!(outside.contains("50000 ppm"), "{outside}");
        let other_k = policy
            .qualify(RequestKey {
                k: 11,
                candidate_depth: 0,
                filter_selectivity_ppm: UNFILTERED_PPM,
            })
            .unwrap_err();
        assert!(
            other_k.contains("k=11") && other_k.contains("[10]"),
            "{other_k}"
        );
    }

    #[test]
    fn identity_mismatches_name_the_field() {
        let policy = DenseExecutionPolicy::parse(TEXT).unwrap();
        let mut rows = live();
        rows.corpus_rows = 1001;
        assert!(policy
            .verify_identity(&rows)
            .unwrap_err()
            .contains("corpus_rows 1000 does not match live 1001"));
        let mut generation = live();
        generation.corpus_generation = 8;
        assert!(policy
            .verify_identity(&generation)
            .unwrap_err()
            .contains("corpus_generation"));
        let mut backend = live();
        backend.provider_backend = "embedded-turbovec".into();
        assert!(policy
            .verify_identity(&backend)
            .unwrap_err()
            .contains("provider_backend \"fake-ann\" does not match live \"embedded-turbovec\""));
        let mut fingerprint = live();
        fingerprint.scoring_fingerprint = "other".into();
        assert!(policy
            .verify_identity(&fingerprint)
            .unwrap_err()
            .contains("scoring_fingerprint"));
        let mut dims = live();
        dims.dimensions = 65;
        assert!(policy
            .verify_identity(&dims)
            .unwrap_err()
            .contains("dimensions"));
    }

    #[test]
    fn malformed_documents_are_refused_by_name() {
        let overlap = TEXT.replace("candidates = 400", "candidates = 200");
        assert!(DenseExecutionPolicy::parse(&overlap)
            .unwrap_err()
            .contains("overlap on selectivity"));
        let shallow = TEXT.replace("candidates = 100", "candidates = 9");
        assert!(DenseExecutionPolicy::parse(&shallow)
            .unwrap_err()
            .contains("invalid candidate depth 9"));
        let unknown = format!("{TEXT}\nnprobe = 4\n");
        assert!(DenseExecutionPolicy::parse(&unknown)
            .unwrap_err()
            .contains("nprobe"));
        let quoted = TEXT.replace("court-ann-v1", "court\\\"ann");
        assert!(DenseExecutionPolicy::parse(&quoted).is_err());
        let version = TEXT.replace("format_version = 1", "format_version = 2");
        assert!(DenseExecutionPolicy::parse(&version)
            .unwrap_err()
            .contains("format_version 2"));
    }

    #[test]
    fn selectivity_is_floored_parts_per_million() {
        assert_eq!(selectivity_ppm(0, 1000), 0);
        assert_eq!(selectivity_ppm(1, 3_000_000), 1);
        assert_eq!(selectivity_ppm(250, 1000), 250_000);
        assert_eq!(selectivity_ppm(1000, 1000), UNFILTERED_PPM);
        assert_eq!(selectivity_ppm(5, 0), UNFILTERED_PPM);
    }
}
