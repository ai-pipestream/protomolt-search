//! The dense execution policy behind `DENSE_EXECUTION_MODE_AUTO`: a
//! persisted measurement profile, bound to one generation of one corpus
//! on one provider, that says at which request keys an approximate
//! traversal was measured good enough to stand in for the exhaustive one.
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
//!
//! A point measured on a provider with filter regimes also carries the
//! enumerate threshold it was measured under, and the shards answer the
//! request under that threshold: the recall a point claims is served by
//! the regimes that measured it.

use std::path::Path;

mod file;
mod qualify;
#[cfg(test)]
mod tests;

use file::PolicyFile;

pub const FORMAT_VERSION: u32 = 1;

/// Selectivity of a request without a filter: the whole corpus.
pub const UNFILTERED_PPM: u32 = 1_000_000;

/// Upper bound on `finish_settle_us` in a policy point (60 seconds).
pub const MAX_FINISH_SETTLE_US: u32 = 60_000_000;

/// One measured point: the key AUTO matches exactly, and the recall
/// measured there against the exhaustive traversal.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PolicyPoint {
    pub k: u32,
    pub selectivity_min_ppm: u32,
    pub selectivity_max_ppm: u32,
    pub candidates: u32,
    pub measured_recall_ppm: u32,
    /// The enumerate threshold the point was measured under: a segment
    /// whose mask allows at most this many rows is scored row by row,
    /// exactly, and a larger one is walked. `None` on a point for a
    /// provider without filter regimes, and on an unfiltered point.
    pub enumerate_max_rows: Option<u32>,
    /// Optional hop-to-hop beam saturation threshold in parts per million
    /// (`1..=1_000_000`) and consecutive hop count (`>= 1`) the point was
    /// measured under. Set together or absent together.
    pub patience_saturation_ppm: Option<u32>,
    pub patience_hops: Option<u32>,
    /// Optional coordinator streaming settle window in microseconds
    /// (`1..=MAX_FINISH_SETTLE_US`) the point was measured under.
    pub finish_settle_us: Option<u32>,
    /// Optional shared-floor pruning greediness in parts per million
    /// (`0..=1_000_000`) and minimum local allowed-row clamp (`<= corpus_rows`)
    /// the point was measured under. Set together or absent together.
    pub floor_greediness_ppm: Option<u32>,
    pub floor_clamp_min: Option<u32>,
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
        Self::from_file(file, turbo_sha256::sha256(text.as_bytes()).to_hex())
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
            if let Some(rows) = point.enumerate_max_rows {
                out.push_str(&format!("enumerate_max_rows = {rows}\n"));
            }
            if let Some(ppm) = point.patience_saturation_ppm {
                out.push_str(&format!("patience_saturation_ppm = {ppm}\n"));
            }
            if let Some(hops) = point.patience_hops {
                out.push_str(&format!("patience_hops = {hops}\n"));
            }
            if let Some(settle_us) = point.finish_settle_us {
                out.push_str(&format!("finish_settle_us = {settle_us}\n"));
            }
            if let Some(greediness) = point.floor_greediness_ppm {
                out.push_str(&format!("floor_greediness_ppm = {greediness}\n"));
            }
            if let Some(clamp_min) = point.floor_clamp_min {
                out.push_str(&format!("floor_clamp_min = {clamp_min}\n"));
            }
        }
        out
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
