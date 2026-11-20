//! Live identity and selectivity primitives for dense execution policies.
//!
//! Version-1 TOML policy documents are retired and refused at load time;
//! without a version-2 binary protobuf policy installed, `AUTO` runs the
//! provider's exhaustive technique.

use std::path::Path;

#[cfg(test)]
mod tests;

/// Selectivity of a request without a filter: the whole corpus.
pub const UNFILTERED_PPM: u32 = 1_000_000;

/// Upper bound on `finish_settle_us` in a policy point (60 seconds).
pub const MAX_FINISH_SETTLE_US: u32 = 60_000_000;

/// What the live cluster reports; a policy must match it field by field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveIdentity {
    pub provider_backend: String,
    pub scoring_fingerprint: String,
    pub corpus_generation: u64,
    pub corpus_rows: u64,
    pub dimensions: u32,
}

/// A validated, immutable dense execution policy.
#[derive(Clone, Debug)]
pub struct DenseExecutionPolicy {
    _unconstructible: (),
}

impl DenseExecutionPolicy {
    /// Read a persisted dense execution policy document, refusing retired
    /// version-1 TOML files.
    pub fn load(path: &Path) -> Result<Self, String> {
        std::fs::read(path)
            .map_err(|error| format!("read dense execution policy {}: {error}", path.display()))?;
        Err(format!(
            "dense execution policy {}: not a version-2 binary protobuf policy document \
             (the version-1 TOML schema is not read)",
            path.display()
        ))
    }

    pub fn policy_id(&self) -> &str {
        ""
    }

    pub fn measured_queries(&self) -> u32 {
        0
    }

    pub fn points(&self) -> &[()] {
        &[]
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
