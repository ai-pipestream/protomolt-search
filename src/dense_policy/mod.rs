//! The dense execution policy behind `DENSE_EXECUTION_MODE_AUTO`: measured
//! evidence, bound to one generation of one corpus on one provider and score
//! space, that says at which request keys an approximate technique met the
//! policy's recall target against the exhaustive selection.
//!
//! The document is the version-2 `DenseExecutionPolicyDocument` protobuf,
//! persisted as its binary encoding and fingerprinted by SHA-256 of those
//! bytes. [`DenseExecutionPolicy::decode`] validates it strictly: any other
//! format version (including the version-1 TOML schema), an unknown enum,
//! an inverted band, a half-set control pair or a duplicate key refuses.
//! Its identity is checked against the live cluster before any point is
//! consulted, and a mismatch names the field. A request qualifies only at a
//! point whose key it matches exactly (`k`, selectivity band, output
//! precision, execution shape, and a named candidate depth): no
//! interpolation between points and no evidence carried across a changed
//! precision, routing or shape.

use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};

use prost::Message;

use crate::pb::{DenseExecutionPolicyDocument, DensePolicyPoint};

mod qualify;
#[cfg(test)]
mod tests;
mod validate;

pub use qualify::{Qualification, Qualified, RequestKey};

pub const FORMAT_VERSION: u32 = 2;

static SAVE_SEQ: AtomicU64 = AtomicU64::new(0);

/// Selectivity of a request without a filter: the whole corpus.
pub const UNFILTERED_PPM: u32 = 1_000_000;

/// Upper bound on `finish_settle_us` in a policy point (60 seconds).
pub const MAX_FINISH_SETTLE_US: u32 = 60_000_000;

/// What the live cluster reports; the policy must match it field by field.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveIdentity {
    pub provider_backend: String,
    pub scoring_fingerprint: String,
    pub corpus_generation: u64,
    pub corpus_rows: u64,
    pub deleted_docs: u64,
    pub dimensions: u32,
    pub corpus_fingerprint: String,
}

/// A validated, immutable policy.
#[derive(Clone, Debug)]
pub struct DenseExecutionPolicy {
    document: DenseExecutionPolicyDocument,
    fingerprint: String,
}

impl DenseExecutionPolicy {
    pub fn load(path: &Path) -> Result<Self, String> {
        let bytes = std::fs::read(path)
            .map_err(|error| format!("read dense execution policy {}: {error}", path.display()))?;
        Self::decode(&bytes)
            .map_err(|error| format!("dense execution policy {}: {error}", path.display()))
    }

    /// Decode and validate the binary document; the fingerprint is that of
    /// these exact bytes.
    pub fn decode(bytes: &[u8]) -> Result<Self, String> {
        let document = DenseExecutionPolicyDocument::decode(bytes).map_err(|error| {
            format!("not a version-{FORMAT_VERSION} binary protobuf policy document: {error}")
        })?;
        validate::document(&document)?;
        Ok(Self {
            document,
            fingerprint: turbo_sha256::sha256(bytes).to_hex(),
        })
    }

    /// Validate a document built in memory, fingerprinting its encoding.
    pub fn from_document(document: DenseExecutionPolicyDocument) -> Result<Self, String> {
        Self::decode(&document.encode_to_vec())
    }

    /// Write the binary document `load` reads, fsynced, through a unique
    /// temporary file renamed into place with parent-directory fsync so
    /// concurrent saves never race and readers never see a partial policy.
    pub fn save(&self, path: &Path) -> Result<(), String> {
        let bytes = self.document.encode_to_vec();
        let file_name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("policy");
        let seq = SAVE_SEQ.fetch_add(1, Ordering::Relaxed);
        let temporary =
            path.with_file_name(format!(".{file_name}.tmp.{}.{seq}", std::process::id()));
        let write = || -> std::io::Result<()> {
            use std::io::Write;
            let mut file = std::fs::File::create(&temporary)?;
            if let Err(err) = file.write_all(&bytes).and_then(|()| file.sync_all()) {
                let _ = std::fs::remove_file(&temporary);
                return Err(err);
            }
            drop(file);
            if let Err(err) = std::fs::rename(&temporary, path) {
                let _ = std::fs::remove_file(&temporary);
                return Err(err);
            }
            if let Some(parent) = path.parent().filter(|p| !p.as_os_str().is_empty()) {
                if let Ok(dir) = std::fs::File::open(parent) {
                    let _ = dir.sync_all();
                }
            }
            Ok(())
        };
        write().map_err(|error| format!("write dense execution policy {}: {error}", path.display()))
    }

    /// The policy binds to one live identity; the first field that
    /// differs is named.
    pub fn verify_identity(&self, live: &LiveIdentity) -> Result<(), String> {
        let doc = &self.document;
        let strings = [
            (
                "provider_backend",
                &doc.provider_backend,
                &live.provider_backend,
            ),
            (
                "scoring_fingerprint",
                &doc.scoring_fingerprint,
                &live.scoring_fingerprint,
            ),
            (
                "corpus_fingerprint",
                &doc.corpus_fingerprint,
                &live.corpus_fingerprint,
            ),
        ];
        for (name, expected, actual) in strings {
            if expected != actual {
                return Err(format!(
                    "dense execution policy {:?} {name} {expected:?} does not match live {actual:?}",
                    doc.policy_id
                ));
            }
        }
        let numbers = [
            (
                "corpus_generation",
                doc.corpus_generation,
                live.corpus_generation,
            ),
            ("corpus_rows", doc.corpus_rows, live.corpus_rows),
            ("deleted_docs", doc.deleted_docs, live.deleted_docs),
            (
                "dimensions",
                u64::from(doc.dimensions),
                u64::from(live.dimensions),
            ),
        ];
        for (name, expected, actual) in numbers {
            if expected != actual {
                return Err(format!(
                    "dense execution policy {:?} {name} {expected} does not match live {actual}",
                    doc.policy_id
                ));
            }
        }
        Ok(())
    }

    pub fn policy_id(&self) -> &str {
        &self.document.policy_id
    }

    pub fn fingerprint(&self) -> &str {
        &self.fingerprint
    }

    pub fn measured_queries(&self) -> u32 {
        self.document.measured_queries
    }

    pub fn target_recall_ppm(&self) -> u32 {
        self.document.target_recall_ppm
    }

    pub fn points(&self) -> &[DensePolicyPoint] {
        &self.document.points
    }

    pub fn document(&self) -> &DenseExecutionPolicyDocument {
        &self.document
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
