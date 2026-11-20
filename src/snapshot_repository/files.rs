//! Repository files on disk: hashing, copying, walking, and the manifest
//! file's read, write and digest checks.

use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use super::{Artifact, RepositoryManifest, MANIFEST_FILE};
use turbo_sha256::Sha256;

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
    Ok((total, hasher.finish().to_hex()))
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
        sha256: hasher.finish().to_hex(),
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
    Ok((path, turbo_sha256::sha256(&bytes).to_hex()))
}

/// Read and parse the manifest of a repository directory, returning it
/// with the SHA-256 of the file's bytes.
pub fn read_manifest(directory: &Path) -> Result<(RepositoryManifest, String), String> {
    let path = directory.join(MANIFEST_FILE);
    let bytes =
        std::fs::read(&path).map_err(|error| format!("read {}: {error}", path.display()))?;
    let manifest = RepositoryManifest::parse(&bytes)?;
    Ok((manifest, turbo_sha256::sha256(&bytes).to_hex()))
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
