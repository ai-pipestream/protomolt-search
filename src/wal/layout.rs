//! Directory and file layout of a WAL.

use std::io;
use std::path::{Path, PathBuf};

/// The on-disk format version (manifest `format_version`). Every
/// generation is written at this version, whether or not it carries
/// retained multi-resolution vector tiers; any other version is refused.
pub const FORMAT_VERSION: u32 = 9;

/// The WAL directory of a shard: `<index path>.wal/`.
pub fn wal_dir(index_path: &Path) -> PathBuf {
    let mut p = index_path.as_os_str().to_owned();
    p.push(".wal");
    PathBuf::from(p)
}

/// The directory of one generation inside a WAL directory.
pub fn gen_dir(wal_dir: &Path, generation: u64) -> PathBuf {
    wal_dir.join(format!("gen-{generation:06}"))
}

/// The bucket file for `bucket` inside a generation directory.
pub fn bucket_path(gen_dir: &Path, bucket: u32) -> PathBuf {
    gen_dir.join(format!("bucket-{bucket:03}.wal"))
}

/// The marker log inside a generation directory.
pub fn markers_path(gen_dir: &Path) -> PathBuf {
    gen_dir.join("markers.wal")
}

/// The manifest path inside a generation directory.
pub fn manifest_path(gen_dir: &Path) -> PathBuf {
    gen_dir.join("manifest.toml")
}

/// Parse a generation directory name (`gen-000042`) back into its number.
pub(super) fn parse_gen_name(name: &str) -> Option<u64> {
    name.strip_prefix("gen-")?.parse().ok()
}

/// Parse a bucket filename (`bucket-007.wal`) back into its index.
pub(super) fn parse_bucket_name(name: &str) -> Option<u32> {
    name.strip_prefix("bucket-")?
        .strip_suffix(".wal")?
        .parse()
        .ok()
}

/// The newest generation directory in a WAL directory, if any.
pub fn latest_gen(wal_dir: &Path) -> io::Result<Option<(u64, PathBuf)>> {
    let mut latest: Option<(u64, PathBuf)> = None;
    if !wal_dir.exists() {
        return Ok(None);
    }
    for entry in std::fs::read_dir(wal_dir)? {
        let entry = entry?;
        if let Some(gen) = parse_gen_name(&entry.file_name().to_string_lossy()) {
            if latest.as_ref().is_none_or(|(g, _)| gen > *g) {
                latest = Some((gen, entry.path()));
            }
        }
    }
    Ok(latest)
}
