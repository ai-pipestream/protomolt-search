//! Persisted snapshot files: naming, loading, and the directory scan.

use std::collections::HashMap;
use std::io;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

use prost::Message;

use crate::pb::storage::VocabSnapshot;

/// The index entry of one persisted snapshot: everything needed to name it
/// in a drift comparison, without loading its sketches.
#[derive(Debug, Clone)]
pub struct SnapshotMeta {
    /// File name of the snapshot, e.g. "snapshot-3-1780000000000.pb".
    pub name: String,
    /// Monotonic sequence number on this shard's history.
    pub sequence: u64,
    /// When the window that produced the snapshot started, epoch millis.
    pub started_epoch_millis: i64,
    /// When the window was sealed and persisted, epoch millis.
    pub sealed_epoch_millis: i64,
    /// Documents the window accumulated (per channel).
    pub documents: u64,
    /// Size of the snapshot file in bytes.
    pub size_bytes: u64,
}

/// The vocabulary snapshot directory of a shard: `<index path>.vocab/`,
/// mirroring the WAL's `<index path>.wal/` convention.
pub fn vocab_dir(index_path: &Path) -> PathBuf {
    let mut p = index_path.as_os_str().to_owned();
    p.push(".vocab");
    PathBuf::from(p)
}

pub(super) fn now_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Parses `snapshot-<seq>-<millis>.pb` file names (no regex dependency).
fn parse_snapshot_name(name: &str) -> Option<(u64, i64)> {
    let rest = name.strip_prefix("snapshot-")?.strip_suffix(".pb")?;
    let (seq, millis) = rest.split_once('-')?;
    Some((seq.parse().ok()?, millis.parse().ok()?))
}

/// Reads and decodes one persisted snapshot.
pub fn load_snapshot_file(path: &Path) -> Result<VocabSnapshot, String> {
    let bytes = std::fs::read(path).map_err(|e| format!("read {}: {e}", path.display()))?;
    VocabSnapshot::decode(bytes.as_slice()).map_err(|e| format!("decode {}: {e}", path.display()))
}

/// Read-only scan of a snapshot directory (the offline-tooling counterpart
/// of the listener's startup scan — no write probe, no live window).
/// Returns `(metadata, path)` pairs ordered by sequence ascending;
/// unreadable snapshots are skipped with a warning.
pub fn scan_snapshot_dir(vocab_dir: &Path) -> io::Result<Vec<(SnapshotMeta, PathBuf)>> {
    let (found, files, _) = scan_snapshots(vocab_dir)?;
    Ok(found
        .into_iter()
        .map(|meta| {
            let path = files
                .get(&meta.sequence)
                .expect("every scanned snapshot has a file")
                .clone();
            (meta, path)
        })
        .collect())
}

/// Indexes the prior snapshots in the directory: metadata only — the
/// sketches are parsed and immediately dropped; they are loaded again when
/// a drift request names the snapshot. An unreadable snapshot is skipped
/// with a warning: one corrupt file must not disable the listener.
pub(super) fn scan_snapshots(
    vocab_dir: &Path,
) -> io::Result<(Vec<SnapshotMeta>, HashMap<u64, PathBuf>, u64)> {
    let mut found = Vec::new();
    let mut files = HashMap::new();
    for entry in std::fs::read_dir(vocab_dir)? {
        let entry = entry?;
        let path = entry.path();
        if !path.is_file() {
            continue;
        }
        let name = entry.file_name().to_string_lossy().into_owned();
        if parse_snapshot_name(&name).is_none() {
            continue;
        }
        match load_snapshot_file(&path) {
            Ok(snapshot) => {
                let documents = snapshot
                    .channels
                    .iter()
                    .map(|c| c.documents)
                    .max()
                    .unwrap_or(0);
                found.push(SnapshotMeta {
                    size_bytes: entry.metadata().map(|m| m.len()).unwrap_or(0),
                    name,
                    sequence: snapshot.sequence,
                    started_epoch_millis: snapshot.started_epoch_millis,
                    sealed_epoch_millis: snapshot.sealed_epoch_millis,
                    documents,
                });
                files.insert(snapshot.sequence, path);
            }
            Err(e) => eprintln!(
                "vocab: skipping unreadable snapshot {}: {e}",
                path.display()
            ),
        }
    }
    found.sort_by_key(|meta| meta.sequence);
    let next_sequence = found
        .iter()
        .map(|meta| meta.sequence)
        .max()
        .map_or(0, |max| max + 1);
    Ok((found, files, next_sequence))
}
