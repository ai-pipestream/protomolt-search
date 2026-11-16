//! The commit marker of a cutover and the recovery of an interrupted one.

use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};

use crate::segments::SegmentCatalog;
use crate::wal;

use super::{manifest_backup_path, marker_path, MARKER_FORMAT};

/// The on-disk commit marker (`<index path>.compact-commit`): written
/// and fsynced before the cutover renames anything, removed by the
/// closing flush. Its presence at open is the evidence of an
/// interrupted cutover, and everything a rollback needs is in it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub(super) struct CommitMarker {
    pub(super) format: u32,
    pub(super) layout: String,
    pub(super) old_wal_generation: u64,
    pub(super) new_wal_generation: u64,
    /// The from-segments path (a catalog without a WAL) writes no
    /// rewritten generation: both generations above are 0 and a rollback
    /// removes no log.
    #[serde(default)]
    pub(super) walless: bool,
    pub(super) work_dir: PathBuf,
    /// Single-image: whether the shard served a snapshot generation
    /// before the cutover (moved to `<index>.snap-old`), as opposed to
    /// the legacy `<index>` file layout (its files listed below).
    pub(super) previous_snapshot: bool,
    pub(super) legacy_files: Vec<PathBuf>,
    /// Segments: the outputs staged under the catalog and the inputs
    /// they replace, by id.
    pub(super) staged_segments: Vec<String>,
    pub(super) replaced_segments: Vec<String>,
}

/// A cutover that has swapped state and files but not yet run its
/// closing flush. `Flush` completes it after the images are on disk.
#[derive(Debug)]
pub(crate) struct PendingCommit {
    pub(super) index_path: PathBuf,
    pub(super) marker: CommitMarker,
}

impl PendingCommit {
    /// Commit: remove the marker (the commit point, fsynced), then retire
    /// what the marker lists. Retirement failures are logged, not
    /// returned — the commit already happened and nothing that remains
    /// can be mistaken for live state.
    pub(crate) fn complete(self) -> std::io::Result<()> {
        let path = marker_path(&self.index_path);
        std::fs::remove_file(&path)?;
        crate::postings::fsync_parent(&path)?;
        let retire_dir = |dir: &Path| {
            if dir.exists() {
                if let Err(error) = std::fs::remove_dir_all(dir) {
                    eprintln!(
                        "compaction: retiring {} failed ({error}); remove it by hand",
                        dir.display()
                    );
                }
            }
        };
        let retire_file = |file: &Path| {
            if file.exists() {
                if let Err(error) = std::fs::remove_file(file) {
                    eprintln!(
                        "compaction: retiring {} failed ({error}); remove it by hand",
                        file.display()
                    );
                }
            }
        };
        if self.marker.layout == "segments" {
            let root = crate::node::segments_root(&self.index_path);
            for id in &self.marker.replaced_segments {
                retire_dir(&SegmentCatalog::segment_dir(&root, id));
            }
            retire_file(&manifest_backup_path(&root));
        } else {
            retire_dir(&crate::node::generation_old_dir(&self.index_path));
            for file in &self.marker.legacy_files {
                retire_file(file);
            }
            retire_dir(&crate::node::bm25_build_dir(
                &crate::node::bm25_sidecar_path(&self.index_path),
            ));
        }
        Ok(())
    }
}

pub(super) fn write_marker(index_path: &Path, marker: &CommitMarker) -> std::io::Result<()> {
    use std::io::Write;
    let path = marker_path(index_path);
    let bytes = serde_json::to_vec_pretty(marker)
        .map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
    let mut tmp = path.as_os_str().to_owned();
    tmp.push(format!(".tmp-{}", std::process::id()));
    let tmp = PathBuf::from(tmp);
    {
        let mut file = std::fs::File::create(&tmp)?;
        file.write_all(&bytes)?;
        file.sync_all()?;
    }
    std::fs::rename(&tmp, &path)?;
    crate::postings::fsync_parent(&path)
}

/// Roll back a cutover whose closing flush never ran: the marker at
/// `<index path>.compact-commit` names what moved. Every rename the
/// cutover makes is undone in reverse where it happened and skipped
/// where it did not, so any crash point recovers to the generation the
/// compaction replaced — which lost nothing that was ever flushed, the
/// product's durability point. Runs first thing in
/// [`crate::node::recover_generation`]; a shard without a marker is
/// untouched. Loud on stderr; a marker it cannot read is a hard stop
/// rather than a guess.
pub(crate) fn recover_interrupted(index_path: &Path) {
    let path = marker_path(index_path);
    if !path.exists() {
        return;
    }
    let marker = read_marker(&path);
    if marker.layout == "segments" {
        roll_back_segments(index_path, &path, &marker);
    } else {
        roll_back_generation(index_path, &marker);
    }
    if !marker.walless {
        let new_gen = wal::gen_dir(&wal::wal_dir(index_path), marker.new_wal_generation);
        if new_gen.exists() {
            must(
                "remove the rewritten WAL generation",
                std::fs::remove_dir_all(&new_gen),
            );
        }
    }
    must("remove the marker", std::fs::remove_file(&path));
    must("fsync", crate::postings::fsync_parent(&path));
    eprintln!(
        "compaction: rolled back; the work directory {} was left for inspection and a retry \
         refuses it until it is removed",
        marker.work_dir.display()
    );
}

fn must(what: &str, result: std::io::Result<()>) {
    if let Err(error) = result {
        panic!("compaction rollback: {what}: {error}");
    }
}

/// Read and announce the marker; one that cannot be read is a hard stop.
fn read_marker(path: &Path) -> CommitMarker {
    let bytes = std::fs::read(path)
        .unwrap_or_else(|error| panic!("read compaction marker {}: {error}", path.display()));
    let marker: CommitMarker = serde_json::from_slice(&bytes).unwrap_or_else(|error| {
        panic!(
            "compaction marker {} is unreadable ({error}); a cutover was interrupted and this \
             shard must not be served until an operator resolves it",
            path.display()
        )
    });
    if marker.format != MARKER_FORMAT {
        panic!(
            "compaction marker {} has format {}, expected {MARKER_FORMAT}",
            path.display(),
            marker.format
        );
    }
    if marker.walless {
        eprintln!(
            "compaction: {} names a from-segments cutover that never reached its closing \
             flush; rolling back to the manifest the cutover kept",
            path.display()
        );
    } else {
        eprintln!(
            "compaction: {} names a cutover to WAL generation {} that never reached its closing \
             flush; rolling back to generation {}",
            path.display(),
            marker.new_wal_generation,
            marker.old_wal_generation
        );
    }
    marker
}

fn roll_back_segments(index_path: &Path, path: &Path, marker: &CommitMarker) {
    let root = crate::node::segments_root(index_path);
    let backup = manifest_backup_path(&root);
    if backup.exists() {
        must(
            "restore the segment set manifest",
            std::fs::rename(&backup, SegmentCatalog::manifest_path(&root)),
        );
    } else if !marker.staged_segments.is_empty() {
        // The backup is written before the marker and removed only
        // by the commit, which removes the marker first: a marker
        // without a backup is a state the protocol cannot produce.
        panic!(
            "compaction rollback: {} exists but {} does not; the on-disk state is not one \
             the cutover protocol produces — resolve by hand",
            path.display(),
            backup.display()
        );
    }
    for id in &marker.staged_segments {
        let dir = SegmentCatalog::segment_dir(&root, id);
        if dir.exists() {
            must("remove a staged segment", std::fs::remove_dir_all(&dir));
        }
    }
}

fn roll_back_generation(index_path: &Path, marker: &CommitMarker) {
    let snap = crate::node::generation_dir(index_path);
    let old = crate::node::generation_old_dir(index_path);
    if old.exists() {
        // The swap began: the old generation is aside. Whether the
        // new one got renamed in or not, put the old one back.
        if snap.exists() {
            must(
                "remove the compacted generation",
                std::fs::remove_dir_all(&snap),
            );
        }
        must(
            "restore the previous generation",
            std::fs::rename(&old, &snap),
        );
    } else if !marker.previous_snapshot && snap.exists() {
        // Legacy layout: nothing was moved aside, so a generation
        // directory can only be the compacted one.
        must(
            "remove the compacted generation",
            std::fs::remove_dir_all(&snap),
        );
    }
}
