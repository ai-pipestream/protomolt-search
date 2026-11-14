//! Online compaction of one shard while it keeps taking writes
//! (`docs/mutations.md`): the live-reshard shape, in-process, for both
//! layouts.
//!
//! 1. Fix a cutoff `(WAL generation, high watermark)` and fsync the log
//!    through it. Writes continue.
//! 2. Replay the log through the cutoff into a dense all-live image
//!    (`reshard::compact_log`), handing every live row to a sink that
//!    writes the REWRITTEN full-history WAL generation: the same records,
//!    dense new ids, tombstoned rows gone. The replay also yields the
//!    old-to-new id map.
//! 3. Open the image as a shadow [`ShardState`] whose WAL is that new
//!    generation, and tail the live log into it through the same apply
//!    functions ingest uses, no lock held, until fewer than `tail_bound`
//!    records remain.
//! 4. Prepare the final tail without the shard's write lock. Under the
//!    lock, verify the WAL has not advanced; otherwise release and retry.
//!    Once caught up, write a commit marker, move the new generation into
//!    place, and swap the state. Existing query snapshots finish on it.
//! 5. The next flush (this call makes one at once) writes the new
//!    generation's images, removes the marker, and retires the old
//!    files. A marker found at open means the closing flush never ran:
//!    the cutover rolls back to the intact old generation, which lost
//!    nothing that was ever flushed.
//!
//! Why the log is rewritten rather than rotated the way a snapshot
//! install rotates it: a generation that records the compacted image as
//! `preexisting_*` is partial history, and `reshard` refuses partial
//! history — the shard could then be compacted exactly once, ever. The
//! rewrite costs one pass over the live rows and keeps every later
//! compaction, split, merge, and replica catch-up possible.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Instant;

use serde::{Deserialize, Serialize};
use tonic::Status;

use crate::exact_vectors::ExactVectorStore;
use crate::live_docs::LiveDocs;
use crate::node::{Bm25Shard, NodeServiceImpl, ShardState};
use crate::pb::wal::{wal_record, LoggedAddDocuments, LoggedAddVectors, LoggedBinding};
use crate::pb::{
    AddDocumentsRequest, AddVectorsRequest, CompactShardRequest, CompactShardResponse,
};
use crate::segments::{SegmentCatalog, SegmentMetadata, SegmentSetManifest, SegmentSource};
use crate::vector::VectorIndex;
use crate::wal::{self, ClockedTail, WalWriter};

mod cutover;
mod from_segments;
mod preflight;
mod shadow;
#[cfg(all(test, feature = "net"))]
mod tests;

/// Tail-pass size below which cutover preparation starts by default.
const DEFAULT_TAIL_BOUND: u32 = 256;
/// Unlocked tail passes before compaction gives up on a log that grows
/// faster than the shadow applies it. A backstop only: the stall rule
/// below is what fires in practice.
const MAX_TAIL_PASSES: u64 = 10_000;
/// Cutover attempts before refusing writes that keep advancing the WAL
/// during preparation.
const CUTOVER_RETRIES: usize = 16;
/// Consecutive unlocked passes that fail to read fewer records than the
/// smallest pass so far. Each pass pays an fsync the writer does not, so
/// once a pass is slow enough for the writer to refill the tail the loop
/// makes no progress and no later pass will: the refusal names it at
/// once instead of after the pass cap.
const STALLED_TAIL_PASSES: u64 = 8;
/// How long a segment seal waits for a two-call append (AddDocuments,
/// then AddVectors for the same rows) caught between its calls to land
/// its vectors before the misaligned tail is refused by name. A loaded
/// host can stretch the gap between the two calls well past a second.
const PAIRED_APPEND_WAIT: std::time::Duration = std::time::Duration::from_secs(30);
/// The pause between those checks.
const PAIRED_APPEND_PAUSE: std::time::Duration = std::time::Duration::from_millis(10);
/// Concurrent analysis streams the build and tail open per spec.
const ANALYSIS_STREAMS: usize = 2;
/// The commit marker's format, for a reader that finds a newer one.
const MARKER_FORMAT: u32 = 1;

#[cfg(test)]
pub(crate) mod segment_staging_hook {
    //! A test seam in the from-segments compaction: called on the
    /// compaction thread once the outputs are staged, before the tail
    /// catch-up, so a test can write through the node exactly then (or
    /// fail the build there). Keyed by index path; one-shot.
    use std::collections::BTreeMap;
    use std::path::{Path, PathBuf};

    type Hook = Box<dyn FnOnce(&super::NodeServiceImpl) -> Result<(), String> + Send>;
    static HOOKS: std::sync::Mutex<BTreeMap<PathBuf, Hook>> =
        std::sync::Mutex::new(BTreeMap::new());

    pub(crate) fn install(
        index_path: PathBuf,
        hook: impl FnOnce(&super::NodeServiceImpl) -> Result<(), String> + Send + 'static,
    ) {
        assert!(HOOKS
            .lock()
            .unwrap()
            .insert(index_path, Box::new(hook))
            .is_none());
    }

    pub(super) fn run(node: &super::NodeServiceImpl, index_path: &Path) -> Result<(), String> {
        let hook = HOOKS.lock().unwrap().remove(index_path);
        match hook {
            Some(hook) => hook(node),
            None => Ok(()),
        }
    }
}

/// The analyzer the build and the tail share: [`crate::reshard::Analyzer`]
/// over the node's own backend.
type Analyze<'a> = dyn FnMut(
        &[(
            &str,
            Option<&crate::pb::AnalysisSpec>,
            crate::analyzer::SessionLayers,
        )],
    ) -> Result<Vec<crate::postings::AnalyzedDoc>, String>
    + 'a;

/// The compaction work directory beside the index when the request
/// names none.
pub fn default_work_dir(index_path: &Path) -> PathBuf {
    let mut name = index_path.as_os_str().to_owned();
    name.push(".compact");
    PathBuf::from(name)
}

/// The commit marker's path: `<index path>.compact-commit`.
pub fn marker_path(index_path: &Path) -> PathBuf {
    let mut name = index_path.as_os_str().to_owned();
    name.push(".compact-commit");
    PathBuf::from(name)
}

/// The copy of the segment-set manifest a segmented cutover keeps for
/// rollback, beside the live one.
fn manifest_backup_path(root: &Path) -> PathBuf {
    let mut name = SegmentCatalog::manifest_path(root).as_os_str().to_owned();
    name.push(".pre-compact");
    PathBuf::from(name)
}

/// The on-disk commit marker (`<index path>.compact-commit`): written
/// and fsynced before the cutover renames anything, removed by the
/// closing flush. Its presence at open is the evidence of an
/// interrupted cutover, and everything a rollback needs is in it.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CommitMarker {
    format: u32,
    layout: String,
    old_wal_generation: u64,
    new_wal_generation: u64,
    /// The from-segments path (a catalog without a WAL) writes no
    /// rewritten generation: both generations above are 0 and a rollback
    /// removes no log.
    #[serde(default)]
    walless: bool,
    work_dir: PathBuf,
    /// Single-image: whether the shard served a snapshot generation
    /// before the cutover (moved to `<index>.snap-old`), as opposed to
    /// the legacy `<index>` file layout (its files listed below).
    previous_snapshot: bool,
    legacy_files: Vec<PathBuf>,
    /// Segments: the outputs staged under the catalog and the inputs
    /// they replace, by id.
    staged_segments: Vec<String>,
    replaced_segments: Vec<String>,
}

/// A cutover that has swapped state and files but not yet run its
/// closing flush. `Flush` completes it after the images are on disk.
#[derive(Debug)]
pub(crate) struct PendingCommit {
    index_path: PathBuf,
    marker: CommitMarker,
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

fn write_marker(index_path: &Path, marker: &CommitMarker) -> std::io::Result<()> {
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
    let bytes = std::fs::read(&path)
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
    let must = |what: &str, result: std::io::Result<()>| {
        if let Err(error) = result {
            panic!("compaction rollback: {what}: {error}");
        }
    };
    if marker.layout == "segments" {
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
    } else {
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

/// Releases the per-shard compaction gate on drop.
struct CompactingGuard(std::sync::Arc<std::sync::atomic::AtomicBool>);

impl Drop for CompactingGuard {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::Release);
    }
}

/// What the preflight learned under the read lock.
struct Preflight {
    index_path: PathBuf,
    work_dir: PathBuf,
    segmented: bool,
    gen_dir: PathBuf,
    cutoff_generation: u64,
    cutoff_clock: u64,
    manifest: wal::WalManifest,
    rows_now: u64,
    tombstones_now: u64,
    /// The live BM25 table and per-field fingerprints, `None` on a
    /// shard without documents.
    fields: Option<(Vec<String>, Vec<u64>)>,
    backend_kind: String,
    scoring_fingerprint: String,
    stats_epoch: u64,
    /// The integer column the outputs are ordered by
    /// (docs/immutable-segments.md "Partitioned layout"); `None` keeps
    /// the bucket layout.
    partition: Option<String>,
    /// The live shard's column tables on the segment layout, given to
    /// the build so every output declares them.
    columns: Option<crate::reshard::ColumnTables>,
}

/// The row source the cutoff qualified for: the log (the historical
/// path), or the sealed segments of a catalog without one
/// (docs/replay-from-segments.md, "Partitioned compaction of a catalog
/// without a log").
enum PreflightKind {
    Wal(Box<Preflight>),
    Segments(Box<SegmentPreflight>),
}

/// What the preflight of a catalog WITHOUT a WAL learned under the read
/// lock. The cutoff is the sealed set as it stood once the tail was
/// sealed at the start of the run; the tail that arrives after it is
/// caught up by sealing, never replayed (there is no log).
struct SegmentPreflight {
    index_path: PathBuf,
    work_dir: PathBuf,
    /// The cutoff snapshot: the sealed set and the shard-wide overlay as
    /// of the cutoff seal. The build reads only these.
    set: std::sync::Arc<crate::segments::OpenedSegmentSet>,
    overlay: LiveDocs,
    /// The catalog epoch at the cutoff.
    epoch: u64,
    rows_now: u64,
    tombstones_now: u64,
    /// The partition column the outputs are ordered by. Required on this
    /// path: there is no log whose bucket layout an unkeyed compaction
    /// would keep.
    partition: String,
    /// The shard's field table and per-field fingerprints.
    fields: Option<(Vec<String>, Vec<u64>)>,
    backend_kind: String,
    scoring_fingerprint: String,
    stats_epoch: u64,
}

/// A segment-layout tail caught between the two calls of a legacy append:
/// the documents count and the vectors count differ.
struct MidRow {
    documents: usize,
    vectors: usize,
}

/// The cutover preparation of the from-segments path
/// (`docs/replay-from-segments.md`): the catalog over the final manifest
/// (staged, uncommitted), the shadow serving state opened over it, the
/// translated overlay, and the fence values the install re-checks.
struct PreparedSegmentCutover {
    catalog: SegmentCatalog,
    shard: Option<crate::segmented::SegmentedShard>,
    index: Option<VectorIndex>,
    exact_vectors: Option<ExactVectorStore>,
    /// The live overlay translated onto the new labels.
    overlay: LiveDocs,
    /// The live catalog epoch this was prepared against.
    epoch: u64,
    /// The live overlay revision this was prepared against.
    revision: u64,
    /// The staged outputs (the rebuilt partitions), for the marker.
    staged_ids: Vec<String>,
    /// The cutoff set's segments, which the outputs replace.
    replaced_ids: Vec<String>,
    /// Rows sealed after the cutoff, carried over by the cutover.
    carried_rows: u64,
}

/// The shadow: a [`ShardState`] over the compacted image whose WAL is the
/// rewritten generation, plus the id map it extends as the tail applies.
struct Shadow {
    state: ShardState,
    /// Source global id -> shadow global id.
    id_map: BTreeMap<u64, u64>,
    /// Segments layout: the staged outputs and the live inputs they
    /// replace.
    staged: Vec<SegmentMetadata>,
    replaced: Vec<String>,
    tail_records: u64,
    epoch_at_open: u64,
}

fn layout_name(segmented: bool) -> &'static str {
    if segmented {
        "segments"
    } else {
        "single-image"
    }
}

fn map_tailed(id_map: &mut BTreeMap<u64, u64>, old: u64, new: u64) -> Result<(), Status> {
    match id_map.insert(old, new) {
        None => Ok(()),
        Some(held) if held == new => Ok(()),
        Some(held) => Err(Status::internal(format!(
            "tailed source id {old} landed at shadow id {new} but its other leg landed at \
             {held}; the shadow's legs diverged"
        ))),
    }
}

fn mapped(id_map: &BTreeMap<u64, u64>, old: u64) -> Result<u64, Status> {
    id_map.get(&old).copied().ok_or_else(|| {
        Status::internal(format!(
            "the tail names source id {old}, which the compaction never saw as a live row; \
             the log and the dense image disagree"
        ))
    })
}

/// The rewritten generation is renamed, not copied, into the WAL
/// directory, so the work directory must share its filesystem. Proven
/// with a probe rename up front rather than discovered at cutover.
fn probe_same_filesystem(work_dir: &Path, wal_dir: &Path) -> Result<(), Status> {
    std::fs::create_dir_all(wal_dir)
        .map_err(|e| Status::internal(format!("mkdir {}: {e}", wal_dir.display())))?;
    let probe = work_dir.join(".fs-probe");
    let target = wal_dir.join(format!(".compact-probe-{}", std::process::id()));
    std::fs::write(&probe, b"probe")
        .map_err(|e| Status::internal(format!("write {}: {e}", probe.display())))?;
    let moved = std::fs::rename(&probe, &target);
    let _ = std::fs::remove_file(&probe);
    let _ = std::fs::remove_file(&target);
    moved.map_err(|e| {
        Status::failed_precondition(format!(
            "compaction work directory {} must be on the same filesystem as the WAL at {} (the \
             rewritten generation is renamed into place): {e}",
            work_dir.display(),
            wal_dir.display()
        ))
    })
}
