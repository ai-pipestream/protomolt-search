//! The snapshot repository format: a directory of
//! generation artifacts plus `snapshot-manifest.json`, which names every
//! artifact with its byte size and SHA-256, the provider descriptor the
//! image scores under, the shard's identity (slot offset, collection),
//! its row counts, and the WAL cutoff the artifacts contain.
//!
//! `ExportSnapshot` writes one; `InstallSnapshotFrom` reads one from a
//! directory, over HTTP(S), or from a peer's `StreamSnapshot`, verifies
//! every artifact against the manifest, and only then installs. This
//! module is the format and its checks; it links no network stack, so
//! the embedded runtime carries it too.

use std::path::{Component, Path};

use serde::{Deserialize, Serialize};

use crate::pb::{SnapshotArtifact, SnapshotRepositoryManifest};

mod files;

pub use files::{
    check_expected_sha, copy_and_hash, hash_file, read_manifest, verify_artifacts, walk_files,
    write_manifest,
};

/// The manifest's file name inside a repository directory.
pub const MANIFEST_FILE: &str = "snapshot-manifest.json";
/// The manifest format this build writes and reads.
pub const FORMAT_VERSION: u32 = 1;
/// Retained vector artifacts require readers that preserve materialized tiers.
pub const RETAINED_FORMAT_VERSION: u32 = 2;
/// The `layout` of a repository holding one provider image plus sidecars.
pub const LAYOUT_SINGLE_IMAGE: &str = "single-image";
/// The `layout` of a repository holding a segment catalog tree.
pub const LAYOUT_SEGMENTS: &str = "segments";
/// The directory a segment-layout repository keeps the catalog under.
pub const CATALOG_DIR: &str = "catalog";

/// One artifact: path relative to the repository, size, digest.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Artifact {
    pub file: String,
    pub bytes: u64,
    pub sha256: String,
}

/// The manifest as it is written to `snapshot-manifest.json`. Field
/// order is the encoding order, so the same manifest always encodes to
/// the same bytes and its SHA-256 pins it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RepositoryManifest {
    pub format_version: u32,
    pub layout: String,
    pub backend_kind: String,
    pub scoring_fingerprint: String,
    pub dim: u32,
    pub slot_offset: u64,
    pub collection: String,
    pub vector_rows: u64,
    pub document_rows: u64,
    pub live_rows: u64,
    pub analysis_fingerprints: Vec<u64>,
    pub wal_generation: u64,
    pub wal_high_watermark: u64,
    pub wal_clocked: bool,
    pub artifacts: Vec<Artifact>,
}

impl RepositoryManifest {
    /// The canonical bytes of the manifest file.
    pub fn encode(&self) -> Vec<u8> {
        let mut bytes = serde_json::to_vec_pretty(self).expect("manifest serializes");
        bytes.push(b'\n');
        bytes
    }

    /// Parse manifest bytes, refusing a format this build does not read.
    pub fn parse(bytes: &[u8]) -> Result<Self, String> {
        let manifest: RepositoryManifest = serde_json::from_slice(bytes)
            .map_err(|error| format!("parse {MANIFEST_FILE}: {error}"))?;
        manifest.validate()?;
        Ok(manifest)
    }

    /// Structural checks every reader applies: the format, a known layout,
    /// artifact paths that stay inside the repository, no repeats.
    pub fn validate(&self) -> Result<(), String> {
        if !(FORMAT_VERSION..=RETAINED_FORMAT_VERSION).contains(&self.format_version) {
            return Err(format!(
                "{MANIFEST_FILE} has format_version {}, this build reads {FORMAT_VERSION}..={RETAINED_FORMAT_VERSION}",
                self.format_version
            ));
        }
        if self.layout != LAYOUT_SINGLE_IMAGE && self.layout != LAYOUT_SEGMENTS {
            return Err(format!(
                "{MANIFEST_FILE} names layout {:?}; this build knows {LAYOUT_SINGLE_IMAGE:?} and \
                 {LAYOUT_SEGMENTS:?}",
                self.layout
            ));
        }
        let has_retained = self.artifacts.iter().any(|artifact| {
            artifact.file == "vectors.multi" || artifact.file.ends_with("/vectors.multi")
        });
        if has_retained && self.format_version < RETAINED_FORMAT_VERSION {
            return Err("retained vector snapshot requires repository format 2".into());
        }
        if self.layout == LAYOUT_SINGLE_IMAGE
            && (self.artifact("vectors.multi").is_some()
                != self.artifact("vectors.multi.json").is_some())
        {
            return Err(
                "retained vector snapshot requires payload and declaration together".into(),
            );
        }
        let mut seen = std::collections::BTreeSet::new();
        for artifact in &self.artifacts {
            validate_artifact_path(&artifact.file)?;
            if artifact.sha256.len() != 64
                || !artifact
                    .sha256
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
            {
                return Err(format!(
                    "artifact {:?} has a malformed sha256 {:?}",
                    artifact.file, artifact.sha256
                ));
            }
            if !seen.insert(artifact.file.as_str()) {
                return Err(format!("artifact {:?} is listed twice", artifact.file));
            }
        }
        Ok(())
    }

    /// The hex SHA-256 of the manifest's canonical bytes.
    pub fn sha256(&self) -> String {
        crate::sha256::hex_digest(&self.encode())
    }

    pub fn to_pb(&self) -> SnapshotRepositoryManifest {
        SnapshotRepositoryManifest {
            format_version: self.format_version,
            layout: self.layout.clone(),
            backend_kind: self.backend_kind.clone(),
            scoring_fingerprint: self.scoring_fingerprint.clone(),
            dim: self.dim,
            slot_offset: self.slot_offset,
            collection: self.collection.clone(),
            vector_rows: self.vector_rows,
            document_rows: self.document_rows,
            live_rows: self.live_rows,
            analysis_fingerprints: self.analysis_fingerprints.clone(),
            wal_generation: self.wal_generation,
            wal_high_watermark: self.wal_high_watermark,
            wal_clocked: self.wal_clocked,
            artifacts: self
                .artifacts
                .iter()
                .map(|artifact| SnapshotArtifact {
                    file: artifact.file.clone(),
                    bytes: artifact.bytes,
                    sha256: artifact.sha256.clone(),
                })
                .collect(),
        }
    }

    pub fn from_pb(manifest: &SnapshotRepositoryManifest) -> Result<Self, String> {
        let parsed = RepositoryManifest {
            format_version: manifest.format_version,
            layout: manifest.layout.clone(),
            backend_kind: manifest.backend_kind.clone(),
            scoring_fingerprint: manifest.scoring_fingerprint.clone(),
            dim: manifest.dim,
            slot_offset: manifest.slot_offset,
            collection: manifest.collection.clone(),
            vector_rows: manifest.vector_rows,
            document_rows: manifest.document_rows,
            live_rows: manifest.live_rows,
            analysis_fingerprints: manifest.analysis_fingerprints.clone(),
            wal_generation: manifest.wal_generation,
            wal_high_watermark: manifest.wal_high_watermark,
            wal_clocked: manifest.wal_clocked,
            artifacts: manifest
                .artifacts
                .iter()
                .map(|artifact| Artifact {
                    file: artifact.file.clone(),
                    bytes: artifact.bytes,
                    sha256: artifact.sha256.clone(),
                })
                .collect(),
        };
        parsed.validate()?;
        Ok(parsed)
    }

    /// The artifact named `file`, when the manifest lists it.
    pub fn artifact(&self, file: &str) -> Option<&Artifact> {
        self.artifacts.iter().find(|artifact| artifact.file == file)
    }

    /// Total artifact bytes.
    pub fn total_bytes(&self) -> u64 {
        self.artifacts.iter().map(|artifact| artifact.bytes).sum()
    }
}

/// An artifact path is relative, normal (no `.`/`..`/root/prefix
/// components), non-empty, and uses `/` separators.
pub fn validate_artifact_path(file: &str) -> Result<(), String> {
    if file.is_empty() {
        return Err("an artifact has an empty path".to_string());
    }
    if file.contains('\\') {
        return Err(format!(
            "artifact path {file:?} uses a backslash; repository paths use '/'"
        ));
    }
    let path = Path::new(file);
    for component in path.components() {
        match component {
            Component::Normal(_) => {}
            other => {
                return Err(format!(
                    "artifact path {file:?} has a {other:?} component; paths stay inside the \
                     repository"
                ))
            }
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests;
