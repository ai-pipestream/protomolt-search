//! The snapshot repository format (`docs/snapshots.md`): a directory of
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

use std::io::{Read, Write};
use std::path::{Component, Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::pb::{SnapshotArtifact, SnapshotRepositoryManifest};
use crate::sha256::Sha256;

/// The manifest's file name inside a repository directory.
pub const MANIFEST_FILE: &str = "snapshot-manifest.json";
/// The manifest format this build writes and reads.
pub const FORMAT_VERSION: u32 = 1;
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
        if self.format_version != FORMAT_VERSION {
            return Err(format!(
                "{MANIFEST_FILE} has format_version {}, this build reads {FORMAT_VERSION}",
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

/// Copy buffer for hashing and copying: 1 MiB amortizes the syscalls
/// without holding much heap.
const COPY_BUF: usize = 1024 * 1024;

/// Stream-hash a file: `(bytes, hex sha256)`.
pub fn hash_file(path: &Path) -> std::io::Result<(u64, String)> {
    let mut file = std::fs::File::open(path)?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; COPY_BUF];
    let mut total = 0u64;
    loop {
        let n = file.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        total += n as u64;
    }
    Ok((total, crate::sha256::to_hex(&hasher.finalize())))
}

/// Copy `source` to `destination` in one pass, hashing the bytes as they
/// go, fsyncing the copy. Parent directories are created. Returns the
/// artifact record for `file`.
pub fn copy_and_hash(source: &Path, destination: &Path, file: &str) -> std::io::Result<Artifact> {
    if let Some(parent) = destination.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut input = std::fs::File::open(source)?;
    let mut output = std::fs::File::create(destination)?;
    let mut hasher = Sha256::new();
    let mut buf = vec![0u8; COPY_BUF];
    let mut total = 0u64;
    loop {
        let n = input.read(&mut buf)?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
        output.write_all(&buf[..n])?;
        total += n as u64;
    }
    output.sync_all()?;
    Ok(Artifact {
        file: file.to_string(),
        bytes: total,
        sha256: crate::sha256::to_hex(&hasher.finalize()),
    })
}

/// Every file under `root`, as `(relative path with '/' separators,
/// absolute path)`, in sorted order.
pub fn walk_files(root: &Path) -> std::io::Result<Vec<(String, PathBuf)>> {
    fn visit(root: &Path, dir: &Path, out: &mut Vec<(String, PathBuf)>) -> std::io::Result<()> {
        let mut entries: Vec<_> = std::fs::read_dir(dir)?.collect::<Result<_, _>>()?;
        entries.sort_by_key(|entry| entry.file_name());
        for entry in entries {
            let path = entry.path();
            let kind = entry.file_type()?;
            if kind.is_dir() {
                visit(root, &path, out)?;
            } else if kind.is_file() {
                let relative = path
                    .strip_prefix(root)
                    .map_err(|_| std::io::Error::other("path escapes its root"))?;
                let name = relative
                    .components()
                    .map(|c| c.as_os_str().to_string_lossy().into_owned())
                    .collect::<Vec<_>>()
                    .join("/");
                out.push((name, path));
            }
        }
        Ok(())
    }
    let mut out = Vec::new();
    if root.exists() {
        visit(root, root, &mut out)?;
    }
    Ok(out)
}

/// Verify every artifact under `directory` against the manifest: it
/// must exist with the declared size and SHA-256. The first mismatch is
/// named; nothing else is touched.
pub fn verify_artifacts(directory: &Path, manifest: &RepositoryManifest) -> Result<(), String> {
    for artifact in &manifest.artifacts {
        let path = directory.join(&artifact.file);
        let (bytes, sha256) = hash_file(&path).map_err(|error| {
            format!(
                "artifact {:?} is unreadable at {}: {error}",
                artifact.file,
                path.display()
            )
        })?;
        if bytes != artifact.bytes {
            return Err(format!(
                "artifact {:?} has {bytes} bytes, the manifest declares {}",
                artifact.file, artifact.bytes
            ));
        }
        if sha256 != artifact.sha256 {
            return Err(format!(
                "artifact {:?} hashes to {sha256}, the manifest declares {}",
                artifact.file, artifact.sha256
            ));
        }
    }
    Ok(())
}

/// Write the manifest file into `directory` with fsync; returns its path
/// and the SHA-256 of the bytes written.
pub fn write_manifest(
    directory: &Path,
    manifest: &RepositoryManifest,
) -> std::io::Result<(PathBuf, String)> {
    let bytes = manifest.encode();
    let path = directory.join(MANIFEST_FILE);
    let mut file = std::fs::File::create(&path)?;
    file.write_all(&bytes)?;
    file.sync_all()?;
    Ok((path, crate::sha256::hex_digest(&bytes)))
}

/// Read and parse the manifest of a repository directory, returning it
/// with the SHA-256 of the file's bytes.
pub fn read_manifest(directory: &Path) -> Result<(RepositoryManifest, String), String> {
    let path = directory.join(MANIFEST_FILE);
    let bytes =
        std::fs::read(&path).map_err(|error| format!("read {}: {error}", path.display()))?;
    let manifest = RepositoryManifest::parse(&bytes)?;
    Ok((manifest, crate::sha256::hex_digest(&bytes)))
}

/// Refuse a manifest digest other than the one the caller expects.
pub fn check_expected_sha(expected: &str, actual: &str) -> Result<(), String> {
    if expected.is_empty() {
        return Ok(());
    }
    let expected = expected.trim().to_ascii_lowercase();
    if expected != actual {
        return Err(format!(
            "manifest sha256 is {actual}, the request expected {expected}"
        ));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn manifest() -> RepositoryManifest {
        RepositoryManifest {
            format_version: FORMAT_VERSION,
            layout: LAYOUT_SINGLE_IMAGE.into(),
            backend_kind: "embedded-turbovec".into(),
            scoring_fingerprint: "fp".into(),
            dim: 4,
            slot_offset: 100,
            collection: String::new(),
            vector_rows: 2,
            document_rows: 2,
            live_rows: 2,
            analysis_fingerprints: vec![7],
            wal_generation: 1,
            wal_high_watermark: 9,
            wal_clocked: true,
            artifacts: vec![Artifact {
                file: "vector.index".into(),
                bytes: 3,
                sha256: crate::sha256::hex_digest(b"abc"),
            }],
        }
    }

    #[test]
    fn encoding_is_deterministic_and_round_trips() {
        let m = manifest();
        let bytes = m.encode();
        assert_eq!(bytes, m.encode());
        let parsed = RepositoryManifest::parse(&bytes).unwrap();
        assert_eq!(parsed, m);
        assert_eq!(parsed.sha256(), crate::sha256::hex_digest(&bytes));
        let pb = m.to_pb();
        assert_eq!(RepositoryManifest::from_pb(&pb).unwrap(), m);
    }

    #[test]
    fn refuses_escaping_paths_unknown_layouts_and_formats() {
        let mut escaping = manifest();
        escaping.artifacts[0].file = "../vector.index".into();
        assert!(escaping.validate().unwrap_err().contains("stay inside"));
        let mut absolute = manifest();
        absolute.artifacts[0].file = "/etc/passwd".into();
        assert!(absolute.validate().is_err());
        let mut layout = manifest();
        layout.layout = "tar".into();
        assert!(layout.validate().unwrap_err().contains("names layout"));
        let mut format = manifest();
        format.format_version = 2;
        assert!(format.validate().unwrap_err().contains("format_version 2"));
        let mut twice = manifest();
        twice.artifacts.push(twice.artifacts[0].clone());
        assert!(twice.validate().unwrap_err().contains("listed twice"));
        let mut digest = manifest();
        digest.artifacts[0].sha256 = "ABC".into();
        assert!(digest.validate().unwrap_err().contains("malformed sha256"));
    }

    #[test]
    fn copy_hash_and_verify_agree_and_name_mismatches() {
        let dir = std::env::temp_dir().join(format!("snap-repo-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("src")).unwrap();
        let source = dir.join("src/vector.index");
        std::fs::write(&source, b"abc").unwrap();
        let repo = dir.join("repo");
        let artifact = copy_and_hash(&source, &repo.join("vector.index"), "vector.index").unwrap();
        assert_eq!(artifact, manifest().artifacts[0]);
        assert_eq!(
            hash_file(&repo.join("vector.index")).unwrap(),
            (3, crate::sha256::hex_digest(b"abc"))
        );
        let m = manifest();
        verify_artifacts(&repo, &m).unwrap();
        std::fs::write(repo.join("vector.index"), b"abd").unwrap();
        let error = verify_artifacts(&repo, &m).unwrap_err();
        assert!(
            error.contains("hashes to") && error.contains("vector.index"),
            "{error}"
        );
        std::fs::write(repo.join("vector.index"), b"ab").unwrap();
        let error = verify_artifacts(&repo, &m).unwrap_err();
        assert!(error.contains("has 2 bytes"), "{error}");
        std::fs::remove_file(repo.join("vector.index")).unwrap();
        assert!(verify_artifacts(&repo, &m)
            .unwrap_err()
            .contains("unreadable"));
        let (path, sha) = write_manifest(&repo, &m).unwrap();
        assert_eq!(path, repo.join(MANIFEST_FILE));
        assert_eq!(read_manifest(&repo).unwrap(), (m.clone(), sha.clone()));
        assert!(check_expected_sha("", &sha).is_ok());
        assert!(check_expected_sha(&sha.to_ascii_uppercase(), &sha).is_ok());
        assert!(check_expected_sha("00", &sha)
            .unwrap_err()
            .contains("expected 00"));
        let files = walk_files(&repo).unwrap();
        assert_eq!(files.len(), 1);
        assert_eq!(files[0].0, MANIFEST_FILE);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
