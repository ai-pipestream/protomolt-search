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

mod cutover;
mod from_segments;
mod preflight;
mod recovery;
mod shadow;
#[cfg(all(test, feature = "net"))]
mod tests;

pub(crate) use self::recovery::{recover_interrupted, PendingCommit};

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use tonic::Status;

use crate::exact_vectors::ExactVectorStore;
use crate::live_docs::LiveDocs;
use crate::node::{NodeServiceImpl, ShardState};
use crate::pb::{CompactShardRequest, CompactShardResponse};
use crate::segments::{SegmentCatalog, SegmentMetadata};
use crate::vector::VectorIndex;
use crate::wal;

use self::preflight::probe_same_filesystem;

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
    /// Sealed HNSW segments available for donor transplant and `#99` join-set
    /// graph merge during compaction.
    hnsw_sources: Option<crate::reshard::HnswCompactionSources>,
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
    multi_res_vectors: crate::multi_res::MultiResVectors,
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

impl NodeServiceImpl {
    /// Compact this shard online (`docs/mutations.md`): the blocking
    /// entry point behind `NodeService.CompactShard`, also for an
    /// in-process control-plane worker. The WAL path needs a Tokio
    /// runtime on the calling thread's context for the analysis sessions
    /// (`spawn_blocking` threads have one); the from-segments path of a
    /// catalog without a WAL analyzes nothing and needs none.
    pub fn compact_shard(
        &self,
        request: &CompactShardRequest,
    ) -> Result<CompactShardResponse, Status> {
        let tail_bound = if request.tail_bound == 0 {
            DEFAULT_TAIL_BOUND
        } else {
            request.tail_bound
        } as usize;
        if self
            .compacting
            .compare_exchange(
                false,
                true,
                std::sync::atomic::Ordering::Acquire,
                std::sync::atomic::Ordering::Relaxed,
            )
            .is_err()
        {
            return Err(Status::failed_precondition(
                "a compaction is already running on this shard",
            ));
        }
        let _gate = CompactingGuard(std::sync::Arc::clone(&self.compacting));
        let preflight = self.preflight_at_row_boundary(request)?;
        let preflight = match preflight {
            PreflightKind::Wal(preflight) => preflight,
            PreflightKind::Segments(preflight) => {
                return self.compact_from_segments(request, *preflight, tail_bound);
            }
        };
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            Status::failed_precondition(
                "compaction analyzes documents through the node's analysis backend and needs a \
                 Tokio runtime context",
            )
        })?;
        if request.dry_run {
            return Ok(CompactShardResponse {
                rows_before: preflight.rows_now,
                rows_after: preflight.rows_now,
                tombstones_reclaimed: preflight.tombstones_now,
                wal_generation: preflight.cutoff_generation + 1,
                cutoff_clock: preflight.cutoff_clock,
                layout: layout_name(preflight.segmented).to_string(),
                dry_run: true,
                stats_epoch: preflight.stats_epoch,
                partition_column: preflight.partition.clone().unwrap_or_default(),
                ..Default::default()
            });
        }
        // The prefix through the cutoff goes to disk before the replay
        // reads it; writes keep landing on the live shard meanwhile.
        {
            let mut guard = crate::node::write_shard(&self.state);
            if let Some(wal) = guard.wal.as_mut() {
                wal.flush()
                    .map_err(|e| Status::internal(format!("wal fsync before compaction: {e}")))?;
            }
        }
        std::fs::create_dir_all(&preflight.work_dir).map_err(|e| {
            Status::internal(format!("mkdir {}: {e}", preflight.work_dir.display()))
        })?;
        probe_same_filesystem(&preflight.work_dir, &wal::wal_dir(&preflight.index_path))?;

        let mut analyze = self.analyzer(&handle);
        let outcome = self.build_and_cut_over(&preflight, tail_bound, &mut analyze);
        match outcome {
            Ok(response) => {
                // Everything the work directory held was moved or copied
                // into place; the closing flush wrote the images it
                // still mapped from there.
                if let Err(error) = std::fs::remove_dir_all(&preflight.work_dir) {
                    eprintln!(
                        "compaction: removing the work directory {} failed: {error}",
                        preflight.work_dir.display()
                    );
                }
                Ok(response)
            }
            Err(status) => Err(status),
        }
    }
}

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
