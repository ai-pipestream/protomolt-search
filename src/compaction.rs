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

    /// Preflight at a row boundary. The cut is the log's high-water mark
    /// (or, for a catalog without a WAL, the sealed set once the cutoff
    /// seal lands), read under the shard lock; a legacy two-RPC append
    /// (AddDocuments, then AddVectors) can be halfway through at that
    /// instant, and on the segment layout the replay through such a cut
    /// builds a bucket with one document more than vectors, which the
    /// layout refuses to seal. The row completes with the client's next
    /// call, so this waits that out for a bounded time, then refuses
    /// naming the counts.
    fn preflight_at_row_boundary(
        &self,
        request: &CompactShardRequest,
    ) -> Result<PreflightKind, Status> {
        const ATTEMPTS: usize = 200;
        const PAUSE: std::time::Duration = std::time::Duration::from_millis(10);
        for attempt in 1..=ATTEMPTS {
            match self.preflight(request)? {
                Ok(preflight) => return Ok(preflight),
                Err(_) if attempt < ATTEMPTS => std::thread::sleep(PAUSE),
                Err(MidRow { documents, vectors }) => {
                    return Err(Status::failed_precondition(format!(
                        "the tail has {documents} documents and {vectors} vectors after {:?}; \
                         compaction cuts at a row boundary, so finish the append (AddVectors \
                         after AddDocuments) or ingest through the mapped path, then retry",
                        PAUSE * ATTEMPTS as u32
                    )))
                }
            }
        }
        unreachable!("the last attempt returns")
    }

    /// One preflight under the read lock: `Ok(Err(_))` when the segment
    /// layout's tail is mid-row (see [`Self::preflight_at_row_boundary`]).
    /// A shard without a WAL qualifies for the from-segments path here or
    /// refuses by name.
    fn preflight(
        &self,
        request: &CompactShardRequest,
    ) -> Result<Result<PreflightKind, MidRow>, Status> {
        let index_path = self.config.index_path.clone().ok_or_else(|| {
            Status::failed_precondition(
                "compaction needs a persisted shard (index_path); an in-memory shard has no log \
                 to compact",
            )
        })?;
        let guard = crate::node::read_shard(&self.state);
        guard.check_legacy_mutation()?;
        if guard.pending_compaction.is_some() {
            return Err(Status::failed_precondition(
                "a compaction cutover is pending its closing flush on this shard; call Flush",
            ));
        }
        let Some(wal) = guard.wal.as_ref() else {
            return self
                .preflight_segments(request, &guard, &index_path)
                .map(|r| r.map(|p| PreflightKind::Segments(Box::new(p))));
        };
        if request.build_threads != 0 || request.build_queue != 0 || request.build_memory != 0 {
            return Err(Status::invalid_argument(
                "build_threads, build_queue and build_memory bound the from-segments build of a \
                 catalog without a WAL; this shard compacts from its log",
            ));
        }
        if wal.has_legacy_clock_records() {
            return Err(Status::failed_precondition(format!(
                "WAL generation {} carries legacy unclocked records; compaction needs a fully \
                 clocked generation (install a snapshot to rotate the log)",
                wal.generation()
            )));
        }
        let manifest = wal.manifest().clone();
        if manifest.preexisting_vectors > 0 || manifest.preexisting_documents > 0 {
            return Err(Status::failed_precondition(format!(
                "WAL generation {} began with {} preexisting vector(s) and {} preexisting \
                 document(s) its log does not contain; compaction replays the log and would \
                 drop them — rebuild the shard from source",
                manifest.generation, manifest.preexisting_vectors, manifest.preexisting_documents
            )));
        }
        if matches!(guard.bm25, Some(Bm25Shard::Spilling(_))) {
            return Err(Status::failed_precondition(
                "a bulk BM25 build is in progress on this shard; Flush it before compacting",
            ));
        }
        let segmented = matches!(guard.bm25, Some(Bm25Shard::Segmented(_)));
        let fields = guard.bm25.as_ref().map(|shard| {
            (0..shard.field_count())
                .map(|f| {
                    (
                        shard.field_name(f).to_string(),
                        shard.analysis_fingerprint(f),
                    )
                })
                .unzip()
        });
        let partition = if request.partition_column.is_empty() {
            None
        } else {
            let column = request.partition_column.as_str();
            if !segmented {
                return Err(Status::failed_precondition(format!(
                    "partition_column {column:?} needs the segment layout; this shard is a \
                     single image"
                )));
            }
            let Some(shard) = guard.bm25.as_ref() else {
                return Err(Status::failed_precondition(format!(
                    "partition_column {column:?}: this shard holds no documents, so no column \
                     can order it"
                )));
            };
            let Some(ii) = shard.integer_index(column) else {
                let kind = if shard.numeric_index(column).is_some() {
                    "a double column"
                } else if shard.facet_index(column).is_some() {
                    "a facet column"
                } else {
                    "not a column of this shard"
                };
                return Err(Status::invalid_argument(format!(
                    "partition_column {column:?} is {kind}; a partitioned compaction orders \
                     by an integer column (--integer-fields, timestamps included)"
                )));
            };
            let Bm25Shard::Segmented(segmented) = shard else {
                return Err(Status::internal(
                    "the segment layout serves a segmented shard",
                ));
            };
            // Before any work: a column no document of the shard carries
            // cannot order it. The sealed summaries count the segments'
            // rows; the tail is scanned.
            let sealed: u64 = segmented
                .snapshot()
                .manifest()
                .segments
                .iter()
                .filter_map(|segment| segment.summary.as_ref())
                .flat_map(|summary| summary.int_columns.iter())
                .filter(|c| c.name == column)
                .map(|c| c.present)
                .sum();
            let tail = segmented.tail();
            let in_tail = (0..tail.next_doc_id()).any(|doc| tail.integer_value(ii, doc).is_some());
            if sealed == 0 && !in_tail {
                return Err(Status::failed_precondition(format!(
                    "partition_column {column:?}: no document of this shard carries it, so it \
                     cannot order the rows"
                )));
            }
            Some(column.to_string())
        };
        // Retain every declared column, including columns with no surviving
        // values. Both storage layouts must reopen with the same schema.
        macro_rules! column_tables {
            ($store:expr) => {{
                let s = $store;
                crate::reshard::ColumnTables {
                    facets: (0..s.facet_count())
                        .map(|i| s.facet_name(i).to_string())
                        .collect(),
                    numerics: (0..s.numeric_count())
                        .map(|i| s.numeric_name(i).to_string())
                        .collect(),
                    map_facets: (0..s.map_facet_count())
                        .map(|i| s.map_facet_name(i).to_string())
                        .collect(),
                    map_numerics: (0..s.map_numeric_count())
                        .map(|i| s.map_numeric_name(i).to_string())
                        .collect(),
                    map_integers: (0..s.map_integer_count())
                        .map(|i| s.map_integer_name(i).to_string())
                        .collect(),
                    map_unsigned_integers: (0..s.map_unsigned_integer_count())
                        .map(|i| s.map_unsigned_integer_name(i).to_string())
                        .collect(),
                    integers: (0..s.integer_count())
                        .map(|i| s.integer_name(i).to_string())
                        .collect(),
                    unsigned_integers: (0..s.unsigned_integer_count())
                        .map(|i| s.unsigned_integer_name(i).to_string())
                        .collect(),
                    geo: (0..s.geo_count())
                        .map(|i| s.geo_name(i).to_string())
                        .collect(),
                }
            }};
        }
        let columns = match guard.bm25.as_ref() {
            Some(Bm25Shard::Building(s)) => Some(column_tables!(s)),
            Some(Bm25Shard::Resident(s)) => Some(column_tables!(s)),
            Some(Bm25Shard::Segmented(s)) => Some(column_tables!(s)),
            Some(Bm25Shard::Spilling(_)) => unreachable!("bulk build refused above"),
            None => None,
        };
        if fields.is_some() && self.config.analysis_addr.is_none() {
            return Err(Status::unavailable(
                "no analysis backend configured for this shard (analysis_addr); compaction \
                 re-analyzes every live document",
            ));
        }
        let (backend_kind, scoring_fingerprint) = guard
            .index
            .as_ref()
            .map(|index| {
                let d = index.descriptor();
                (d.backend_kind, d.scoring_fingerprint)
            })
            .unwrap_or_default();
        let work_dir = if request.work_dir.is_empty() {
            default_work_dir(&index_path)
        } else {
            PathBuf::from(&request.work_dir)
        };
        if work_dir.exists()
            && std::fs::read_dir(&work_dir)
                .map_err(|e| Status::internal(format!("read {}: {e}", work_dir.display())))?
                .next()
                .is_some()
        {
            return Err(Status::failed_precondition(format!(
                "compaction work directory {} is not empty; a previous compaction left it — \
                 inspect and remove it",
                work_dir.display()
            )));
        }
        let cutoff_generation = wal.generation();
        if segmented {
            let root = crate::node::segments_root(&index_path);
            let prefix = format!("cmp-{:06}-", cutoff_generation + 1);
            if let Ok(entries) = std::fs::read_dir(root.join("segments")) {
                for entry in entries.flatten() {
                    let name = entry.file_name().to_string_lossy().into_owned();
                    if name.starts_with(&prefix) {
                        return Err(Status::failed_precondition(format!(
                            "staged segment directory {} exists; a previous compaction left it \
                             — remove it before retrying",
                            entry.path().display()
                        )));
                    }
                }
            }
        }
        if let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() {
            if let Some(provider) = guard.index.as_ref().and_then(VectorIndex::as_segmented) {
                let documents = shard.tail().next_doc_id() as usize;
                let vectors = provider.tail().len();
                if documents != vectors {
                    return Ok(Err(MidRow { documents, vectors }));
                }
            }
        }
        let rows_now = crate::node::physical_rows(&guard);
        Ok(Ok(PreflightKind::Wal(Box::new(Preflight {
            index_path,
            work_dir,
            segmented,
            gen_dir: wal.dir().to_path_buf(),
            cutoff_generation,
            cutoff_clock: wal.high_watermark(),
            manifest,
            rows_now,
            tombstones_now: guard.live_docs.deleted_count().min(rows_now),
            fields,
            backend_kind,
            scoring_fingerprint,
            stats_epoch: guard.stats_epoch,
            partition,
            columns,
        }))))
    }

    /// The preflight of a catalog WITHOUT a WAL: the from-segments path
    /// (docs/replay-from-segments.md, "Partitioned compaction of a
    /// catalog without a log"). Only a catalog that qualifies enters it:
    /// the segmented layout with at least one sealed segment and the
    /// generation binding the outputs must carry. Anything less refuses
    /// by name, as does a missing partition column: with no log there is
    /// no bucket layout to keep, so this path builds the partitioned
    /// layout only.
    fn preflight_segments(
        &self,
        request: &CompactShardRequest,
        guard: &ShardState,
        index_path: &Path,
    ) -> Result<Result<SegmentPreflight, MidRow>, Status> {
        let no_wal = "this shard has no WAL; compaction replays the log, so a shard without one \
                      can only be rebuilt from source";
        let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
            return Err(Status::failed_precondition(no_wal));
        };
        let set = shard.snapshot().clone();
        if set.is_empty() {
            return Err(Status::failed_precondition(format!(
                "{no_wal} — and this catalog has no sealed segments to compact from; flush it \
                 first"
            )));
        }
        if set.binding().is_none() {
            return Err(Status::failed_precondition(format!(
                "{no_wal} — and this catalog carries no generation binding for the outputs to \
                 keep"
            )));
        }
        if request.partition_column.is_empty() {
            return Err(Status::failed_precondition(
                "compaction of a catalog without a WAL replays its sealed segments and needs \
                 partition_column to order the outputs; without one there is no layout to build",
            ));
        }
        let column = request.partition_column.as_str();
        if shard.integer_index(column).is_none() {
            let kind = if shard.numeric_index(column).is_some() {
                "a double column"
            } else if shard.facet_index(column).is_some() {
                "a facet column"
            } else {
                "not a column of this shard"
            };
            return Err(Status::invalid_argument(format!(
                "partition_column {column:?} is {kind}; a partitioned compaction orders by an \
                 integer column (--integer-fields, timestamps included)"
            )));
        }
        crate::node::check_attached_derived(
            &self.config,
            shard.derived(),
            shard.doc_count(),
            "the catalog",
        )
        .map_err(Status::failed_precondition)?;
        let fields = Some(
            (0..shard.field_count())
                .map(|f| {
                    (
                        shard.field_name(f).to_string(),
                        shard.analysis_fingerprint(f),
                    )
                })
                .unzip(),
        );
        let (backend_kind, scoring_fingerprint) = guard
            .index
            .as_ref()
            .map(|index| {
                let d = index.descriptor();
                (d.backend_kind, d.scoring_fingerprint)
            })
            .unwrap_or_default();
        let work_dir = if request.work_dir.is_empty() {
            default_work_dir(index_path)
        } else {
            PathBuf::from(&request.work_dir)
        };
        if work_dir.exists()
            && std::fs::read_dir(&work_dir)
                .map_err(|e| Status::internal(format!("read {}: {e}", work_dir.display())))?
                .next()
                .is_some()
        {
            return Err(Status::failed_precondition(format!(
                "compaction work directory {} is not empty; a previous compaction left it — \
                 inspect and remove it",
                work_dir.display()
            )));
        }
        // A staged leftover of an interrupted run is never adopted: a
        // retry refuses it by name until an operator removes it.
        let root = crate::node::segments_root(index_path);
        if let Ok(entries) = std::fs::read_dir(root.join("segments")) {
            let published: std::collections::BTreeSet<&str> = set
                .manifest()
                .segments
                .iter()
                .map(|s| s.segment_id.as_str())
                .collect();
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().into_owned();
                if name.starts_with("cmp-") && !published.contains(name.as_str()) {
                    return Err(Status::failed_precondition(format!(
                        "staged segment directory {} exists; a previous compaction left it — \
                         remove it before retrying",
                        entry.path().display()
                    )));
                }
            }
        }
        if let Some(provider) = guard.index.as_ref().and_then(VectorIndex::as_segmented) {
            let documents = shard.tail().next_doc_id() as usize;
            let vectors = provider.tail().len();
            if documents != vectors {
                return Ok(Err(MidRow { documents, vectors }));
            }
        }
        let rows_now = crate::node::physical_rows(guard);
        Ok(Ok(SegmentPreflight {
            index_path: index_path.to_path_buf(),
            work_dir,
            set,
            overlay: guard.live_docs.clone(),
            epoch: shard.snapshot().epoch(),
            rows_now,
            tombstones_now: guard.live_docs.deleted_count().min(rows_now),
            partition: column.to_string(),
            fields,
            backend_kind,
            scoring_fingerprint,
            stats_epoch: guard.stats_epoch,
        }))
    }

    /// The analyzer for the replay and the tail: the node's own analysis
    /// backend, sidecar or native, through the same batch sessions the
    /// offline reshard uses.
    fn analyzer<'a>(&'a self, handle: &'a tokio::runtime::Handle) -> Box<Analyze<'a>> {
        Box::new(move |docs| {
            let addr = self.config.analysis_addr.as_deref().ok_or_else(|| {
                "no analysis backend configured for this shard (analysis_addr)".to_string()
            })?;
            handle
                .block_on(crate::analyzer::analyze_batch_streams(
                    addr,
                    docs,
                    ANALYSIS_STREAMS,
                ))
                .map_err(|status| status.message().to_string())
        })
    }

    fn build_and_cut_over(
        &self,
        pre: &Preflight,
        tail_bound: usize,
        analyze: &mut Analyze<'_>,
    ) -> Result<CompactShardResponse, Status> {
        let slot_offset = self.config.slot_offset;
        // The rewritten generation: the source manifest one generation
        // on, full history (nothing preexisting), same geometry.
        let mut new_manifest = pre.manifest.clone();
        new_manifest.generation = pre.cutoff_generation + 1;
        new_manifest.preexisting_vectors = 0;
        new_manifest.preexisting_documents = 0;
        let wal_stage = pre.work_dir.join("wal");
        let mut new_wal = WalWriter::create(&wal_stage, new_manifest)
            .map_err(|e| Status::internal(format!("create the rewritten WAL generation: {e}")))?;
        let build_dir = pre.work_dir.join("build");
        // The binding goes first in the rewritten log (a replica applies
        // it to an empty shard only), so it is read and logged before any
        // row is emitted.
        let bound_first = crate::reshard::read_generation_binding(&pre.gen_dir)
            .map_err(Status::failed_precondition)?;
        if let Some(binding) = &bound_first {
            new_wal
                .append(wal_record::Op::Bind(LoggedBinding {
                    plan_fingerprint: binding.plan_fingerprint.clone(),
                    body_path: binding.body_path.clone(),
                    materialize_sha: binding.materialize_sha.clone(),
                    analysis_sha: binding.analysis_sha.clone(),
                    analysis_contract: binding.analysis_contract.clone(),
                    vector_binding: binding.vector_binding.clone(),
                    index_contract: binding.index_contract.clone(),
                }))
                .map_err(|e| Status::internal(format!("rewrite binding record: {e}")))?;
        }
        let names: Option<Vec<String>> = pre.fields.as_ref().map(|(n, _)| n.clone());
        let pins: Option<Vec<u64>> = pre.fields.as_ref().map(|(_, p)| p.clone());
        let build = {
            let mut sink = |row: crate::reshard::CompactedRow<'_>| -> Result<(), String> {
                let first_id = slot_offset + row.new_local;
                let keys: Vec<Vec<u8>> = row.stable_key.map(<[u8]>::to_vec).into_iter().collect();
                // Document before vector, the order mapped ingest logs
                // them in, so a replica applying this generation lands
                // both legs at the same id.
                if let Some(document) = row.document {
                    new_wal
                        .append(wal_record::Op::AddDocuments(LoggedAddDocuments {
                            first_id,
                            documents: vec![document.clone()],
                            stable_routing_keys: keys.clone(),
                            source_references: Vec::new(),
                        }))
                        .map_err(|e| format!("rewrite document record: {e}"))?;
                }
                if let Some(vector) = row.vector {
                    new_wal
                        .append(wal_record::Op::AddVectors(LoggedAddVectors {
                            first_id,
                            batch: Some(AddVectorsRequest {
                                vectors: vector.to_vec(),
                                dim: pre.manifest.dim,
                            }),
                            stable_routing_keys: keys,
                        }))
                        .map_err(|e| format!("rewrite vector record: {e}"))?;
                }
                Ok(())
            };
            let derived = crate::node::stored_derived(&self.config);
            match pre.partition.as_deref() {
                Some(column) => crate::reshard::compact_log_partitioned(
                    &pre.gen_dir,
                    pre.cutoff_clock,
                    &build_dir,
                    crate::reshard::PartitionSpec {
                        column,
                        bound: tail_bound,
                    },
                    names.as_deref(),
                    pins.as_deref(),
                    pre.columns.as_ref(),
                    derived.as_ref(),
                    analyze,
                    &mut sink,
                ),
                None => crate::reshard::compact_log(
                    &pre.gen_dir,
                    pre.cutoff_clock,
                    &build_dir,
                    pre.segmented,
                    names.as_deref(),
                    pins.as_deref(),
                    pre.columns.as_ref(),
                    derived.as_ref(),
                    analyze,
                    &mut sink,
                ),
            }
            .map_err(|e| Status::failed_precondition(format!("compaction build: {e}")))?
        };
        if build.binding != bound_first {
            return Err(Status::internal(
                "the rewritten generation's binding differs from the replay's; the log carries \
                 contradictory bindings",
            ));
        }
        new_wal
            .flush()
            .map_err(|e| Status::internal(format!("fsync the rewritten WAL generation: {e}")))?;

        let mut shadow = if pre.segmented {
            self.open_segmented_shadow(pre, &build, new_wal)?
        } else {
            self.open_single_image_shadow(pre, &build, new_wal)?
        };
        shadow.id_map = build.id_map;
        let dense_rows = build.rows_before - build.tombstones;
        let outcome = self.tail_and_cut_over(pre, &mut shadow, tail_bound, analyze);
        match outcome {
            Ok((locked_records, write_lock_ms, tail_passes)) => {
                let closing = Instant::now();
                self.closing_flush()?;
                let closing_flush_ms = closing.elapsed().as_millis() as u64;
                let (rows_after, stats_epoch) = {
                    let guard = crate::node::read_shard(&self.state);
                    (crate::node::physical_rows(&guard), guard.stats_epoch)
                };
                Ok(CompactShardResponse {
                    rows_before: build.rows_before,
                    rows_after,
                    tombstones_reclaimed: build.tombstones,
                    tail_records_applied: shadow.tail_records,
                    locked_tail_records: locked_records,
                    write_lock_ms,
                    wal_generation: pre.cutoff_generation + 1,
                    cutoff_clock: pre.cutoff_clock,
                    layout: layout_name(pre.segmented).to_string(),
                    dry_run: false,
                    closing_flush_ms,
                    tail_passes,
                    stats_epoch,
                    partition_column: pre.partition.clone().unwrap_or_default(),
                })
            }
            Err(status) => {
                // Nothing was swapped: the live shard is untouched. The
                // staged segments are the one thing outside the work
                // directory, and they go.
                let _ = dense_rows;
                if pre.segmented {
                    let root = crate::node::segments_root(&pre.index_path);
                    crate::segments::remove_segment_dirs(&root, &shadow.staged);
                }
                Err(status)
            }
        }
    }

    /// The flush that completes a cutover. On the segment layout a flush
    /// that meets a legacy two-RPC append mid-row (documents one ahead of
    /// vectors) refuses to seal, by the layout's own rule; the row
    /// completes with the client's next call, so the closing flush waits
    /// that out for a bounded time rather than reporting a committed
    /// cutover as a failure. Past the bound it returns the seal's refusal
    /// and names the pending cutover, which the next Flush completes.
    fn closing_flush(&self) -> Result<(), Status> {
        const ATTEMPTS: usize = 200;
        const PAUSE: std::time::Duration = std::time::Duration::from_millis(10);
        for attempt in 1..=ATTEMPTS {
            match self.flush_index() {
                Ok(_) => return Ok(()),
                Err(status)
                    if attempt < ATTEMPTS
                        && status.code() == tonic::Code::FailedPrecondition
                        && status
                            .message()
                            .contains("a segment's artifacts cover the same rows") =>
                {
                    std::thread::sleep(PAUSE);
                }
                Err(status) => {
                    return Err(Status::new(
                        status.code(),
                        format!(
                            "the compaction cut over and is pending its closing flush, which \
                             refused: {}; the next Flush completes it",
                            status.message()
                        ),
                    ))
                }
            }
        }
        unreachable!("the last attempt returns")
    }

    /// The single-image shadow: the built image laid out as a generation
    /// directory under the work dir, opened the way a snapshot install
    /// opens one.
    fn open_single_image_shadow(
        &self,
        pre: &Preflight,
        build: &crate::reshard::CompactionBuild,
        new_wal: WalWriter,
    ) -> Result<Shadow, Status> {
        let [image] = build.images.as_slice() else {
            return Err(Status::internal(format!(
                "a single-image compaction built {} images",
                build.images.len()
            )));
        };
        let gen = pre.work_dir.join("generation");
        std::fs::create_dir_all(&gen)
            .map_err(|e| Status::internal(format!("mkdir {}: {e}", gen.display())))?;
        let mv = |from: &Path, to: &Path| {
            std::fs::rename(from, to).map_err(|e| {
                Status::internal(format!("move {} -> {}: {e}", from.display(), to.display()))
            })
        };
        let vector_path = crate::node::generation_vector(&gen);
        let exact_path = crate::node::generation_exact_vectors(&gen);
        let bm25_path = crate::node::generation_bm25(&gen);
        let live_path = crate::node::generation_live_docs(&gen);
        let rows = image.row_parent_ids.len() as u64;
        let has_vectors = image.num_vectors > 0;
        if has_vectors {
            mv(&image.vector_path, &vector_path)?;
            mv(&image.exact_vector_path, &exact_path)?;
        }
        if let Some(path) = &image.bm25_path {
            mv(path, &bm25_path)?;
        }
        LiveDocs::default()
            .write(&live_path, rows)
            .map_err(|e| Status::internal(format!("write {}: {e}", live_path.display())))?;
        crate::postings::fsync_parent(&live_path)
            .map_err(|e| Status::internal(format!("fsync {}: {e}", gen.display())))?;

        let index = if !has_vectors {
            // No vectors through the cutoff: keep the provider state the
            // log locked, so a vector the tail brings lands in the same
            // scoring space (and calibration is not silently lost).
            self.empty_configured_index(pre)?
        } else {
            let mut loaded = VectorIndex::load(&self.config.vector_backend, &vector_path)
                .map_err(|e| Status::internal(format!("load {}: {e}", vector_path.display())))?;
            loaded
                .prepare()
                .map_err(|e| Status::internal(format!("prepare {}: {e}", vector_path.display())))?;
            let d = loaded.descriptor();
            if d.backend_kind != pre.backend_kind
                || d.scoring_fingerprint != pre.scoring_fingerprint
            {
                return Err(Status::failed_precondition(format!(
                    "the compacted image scores under {}/{} but the shard serves {}/{}; the WAL \
                     manifest's provider state does not reproduce the live generation",
                    d.backend_kind,
                    d.scoring_fingerprint,
                    pre.backend_kind,
                    pre.scoring_fingerprint
                )));
            }
            Some(loaded)
        };
        let exact_vectors = if has_vectors {
            Some(
                ExactVectorStore::open(&exact_path)
                    .map_err(|e| Status::internal(format!("open {}: {e}", exact_path.display())))?,
            )
        } else {
            index
                .as_ref()
                .and_then(VectorIndex::dim_opt)
                .map(|dim| ExactVectorStore::spilling(&exact_path, Some(dim)))
                .transpose()
                .map_err(|e| {
                    Status::internal(format!(
                        "exact-vector builder {}: {e}",
                        exact_path.display()
                    ))
                })?
        };
        let bm25 = if image.bm25_path.is_some() {
            let shard = Bm25Shard::open(&bm25_path)
                .map_err(|e| Status::internal(format!("open {}: {e}", bm25_path.display())))?;
            self.check_shadow_fingerprints(pre.fields.as_ref(), &shard)?;
            Some(shard)
        } else {
            // No documents through the cutoff. A document the tail brings
            // goes into a heap store rather than a spill builder, whose
            // directory would live under the generation directory the
            // cutover renames away.
            let mut store =
                crate::node::heap_store(&self.config).map_err(Status::failed_precondition)?;
            store.set_binding(
                crate::reshard::read_generation_binding(&pre.gen_dir)
                    .map_err(Status::failed_precondition)?,
            );
            Some(Bm25Shard::Building(store))
        };
        let live_docs = LiveDocs::open(&live_path)
            .map_err(|e| Status::internal(format!("open {}: {e}", live_path.display())))?;
        let mapped_binding = bm25.as_ref().and_then(|b| b.binding().cloned());
        Ok(Shadow {
            state: ShardState {
                index,
                exact_vectors,
                bm25,
                live_docs,
                generation: Some(gen),
                wal: Some(new_wal),
                parents: None,
                mapped_binding,
                stats_epoch: 0,
                files_current: false,
                stats_incarnation: Default::default(),
                pending_compaction: None,
            },
            id_map: BTreeMap::new(),
            staged: Vec::new(),
            replaced: Vec::new(),
            tail_records: 0,
            epoch_at_open: 0,
        })
    }

    /// The segmented shadow: the built images staged as sealed segments
    /// under the live catalog root (unpublished), a staged catalog over
    /// them with a fresh tail, the dense FP32 sidecar assembled from
    /// their rows, and no tombstones.
    fn open_segmented_shadow(
        &self,
        pre: &Preflight,
        build: &crate::reshard::CompactionBuild,
        new_wal: WalWriter,
    ) -> Result<Shadow, Status> {
        let root = crate::node::segments_root(&pre.index_path);
        let live_manifest = SegmentCatalog::read_manifest(&root)
            .map_err(Status::internal)?
            .unwrap_or_default();
        let replaced: Vec<String> = live_manifest
            .segments
            .iter()
            .map(|s| s.segment_id.clone())
            .collect();
        let epoch_at_open = {
            let guard = crate::node::read_shard(&self.state);
            match guard.bm25.as_ref() {
                Some(Bm25Shard::Segmented(shard)) => shard.snapshot().epoch(),
                _ => live_manifest.epoch,
            }
        };
        let generation = epoch_at_open + 1;
        let mut live_paths = Vec::with_capacity(build.images.len());
        for image in &build.images {
            if image.num_vectors != 0 && image.num_vectors as usize != image.row_parent_ids.len() {
                return Err(Status::failed_precondition(format!(
                    "compaction output {} has {} vectors over {} rows; a segment's artifacts \
                     cover the same rows",
                    image.vector_path.display(),
                    image.num_vectors,
                    image.row_parent_ids.len()
                )));
            }
            if image.bm25_path.is_none() {
                return Err(Status::failed_precondition(format!(
                    "compaction output {} has no BM25 image; the segment layout seals documents",
                    image.vector_path.display()
                )));
            }
            let path = crate::node::live_docs_sidecar_path(&image.vector_path);
            LiveDocs::default()
                .write(&path, image.row_parent_ids.len() as u64)
                .map_err(|e| Status::internal(format!("write {}: {e}", path.display())))?;
            live_paths.push(path);
        }
        let ids: Vec<String> = (0..build.images.len())
            .map(|i| format!("cmp-{:06}-{i:04}", pre.cutoff_generation + 1))
            .collect();
        let sources: Vec<SegmentSource<'_>> = build
            .images
            .iter()
            .zip(&ids)
            .zip(&live_paths)
            .map(|((image, id), live)| SegmentSource {
                segment_id: id,
                generation,
                base_label: image.slot_offset,
                backend_kind: &pre.backend_kind,
                vector_path: (image.num_vectors > 0).then_some(image.vector_path.as_path()),
                exact_vector_path: (image.num_vectors > 0)
                    .then_some(image.exact_vector_path.as_path()),
                bm25_path: image.bm25_path.as_deref().expect("checked above"),
                live_docs_path: live,
                partition_column: pre.partition.as_deref(),
            })
            .collect();
        let staged = crate::segments::stage_segments(&root, sources)
            .map_err(|e| Status::internal(format!("stage compacted segments: {e}")))?;
        let cleanup =
            |staged: &[SegmentMetadata]| crate::segments::remove_segment_dirs(&root, staged);
        let opened = (|| -> Result<Shadow, Status> {
            // The staged set carries the epoch it was cut from; the
            // cutover commits it one past whatever the live set reached.
            let manifest = SegmentSetManifest {
                epoch: epoch_at_open,
                segments: staged.clone(),
                partition_key: pre.partition.clone(),
                ..Default::default()
            }
            .with_binding(
                crate::reshard::read_generation_binding(&pre.gen_dir)
                    .map_err(Status::failed_precondition)?
                    .as_ref(),
            )
            .map_err(Status::failed_precondition)?;
            let catalog = SegmentCatalog::open_staged(&root, manifest, self.config.vector_load())
                .map_err(|e| Status::internal(format!("open the compacted set: {e}")))?;
            let tail =
                crate::node::heap_store(&self.config).map_err(Status::failed_precondition)?;
            let shard = crate::segmented::SegmentedShard::open_catalog(catalog, tail)
                .map_err(|e| Status::internal(format!("open the compacted shard: {e}")))?;
            let set = shard.snapshot().clone();
            let bm25 = Bm25Shard::Segmented(shard);
            self.check_shadow_fingerprints(pre.fields.as_ref(), &bm25)?;
            let mut index = None;
            let mut exact_vectors = None;
            if let Some(empty) = self.empty_configured_index(pre)? {
                // Even an empty compacted generation replaces the old FP32
                // sidecar at the closing flush. Leaving this absent retains
                // the retired generation's rows on disk.
                exact_vectors = Some(ExactVectorStore::empty(empty.dim_opt()));
                let provider =
                    crate::segmented_vectors::SegmentedProvider::open(set.clone(), empty)
                        .map_err(|e| Status::internal(format!("segment vectors: {e}")))?;
                index = Some(VectorIndex::from_provider(provider));
            }
            if let Some(first) = (0..set.len()).find_map(|i| set.vector(i)) {
                let d = first.descriptor();
                if d.backend_kind != pre.backend_kind
                    || d.scoring_fingerprint != pre.scoring_fingerprint
                {
                    return Err(Status::failed_precondition(format!(
                        "the compacted segments score under {}/{} but the shard serves {}/{}; \
                         the WAL manifest's provider state does not reproduce the live generation",
                        d.backend_kind,
                        d.scoring_fingerprint,
                        pre.backend_kind,
                        pre.scoring_fingerprint
                    )));
                }
                let backend = first
                    .backend_config()
                    .map_err(|e| Status::internal(format!("segment vector backend: {e}")))?;
                let dim = first
                    .dim_opt()
                    .ok_or_else(|| Status::internal("segment vector image has no dimension"))?;
                let tail_image = VectorIndex::from_backend_config(dim, &backend)
                    .map_err(|e| Status::internal(format!("segment tail image: {e}")))?;
                let provider =
                    crate::segmented_vectors::SegmentedProvider::open(set.clone(), tail_image)
                        .map_err(|e| Status::internal(format!("segment vectors: {e}")))?;
                index = Some(VectorIndex::from_provider(provider));
                exact_vectors =
                    Some(ExactVectorStore::from_segments(&set, dim).map_err(|error| {
                        Status::internal(format!("compacted exact-vector view: {error}"))
                    })?);
            }
            let mapped_binding = bm25.binding().cloned();
            Ok(Shadow {
                state: ShardState {
                    index,
                    exact_vectors,
                    bm25: Some(bm25),
                    live_docs: LiveDocs::default(),
                    generation: None,
                    wal: Some(new_wal),
                    parents: None,
                    mapped_binding,
                    stats_epoch: 0,
                    files_current: false,
                    stats_incarnation: Default::default(),
                    pending_compaction: None,
                },
                id_map: BTreeMap::new(),
                staged: staged.clone(),
                replaced,
                tail_records: 0,
                epoch_at_open,
            })
        })();
        match opened {
            Ok(shadow) => Ok(shadow),
            Err(status) => {
                cleanup(&staged);
                Err(status)
            }
        }
    }

    /// An empty index under the provider state the WAL manifest locked,
    /// for a shadow whose dense image holds no vectors; `None` when the
    /// log never locked one.
    fn empty_configured_index(&self, pre: &Preflight) -> Result<Option<VectorIndex>, Status> {
        let dim = pre.manifest.dim as usize;
        if dim == 0 {
            return Ok(None);
        }
        let Ok(config) = pre.manifest.backend_config() else {
            return Ok(None);
        };
        VectorIndex::from_backend_config(dim, &config)
            .map(Some)
            .map_err(|e| Status::internal(format!("construct the shadow's empty index: {e}")))
    }

    /// The compacted store's field table and analyzer fingerprints must
    /// be the live shard's: same names, and the same fingerprint wherever
    /// both record one (a field whose every document was tombstoned has
    /// none in the dense image).
    fn check_shadow_fingerprints(
        &self,
        fields: Option<&(Vec<String>, Vec<u64>)>,
        built: &Bm25Shard,
    ) -> Result<(), Status> {
        let Some((names, fingerprints)) = fields else {
            return Ok(());
        };
        if built.field_count() != names.len() {
            return Err(Status::failed_precondition(format!(
                "the compacted store has {} fields but the shard has {}",
                built.field_count(),
                names.len()
            )));
        }
        for (f, (name, fingerprint)) in names.iter().zip(fingerprints).enumerate() {
            if built.field_name(f) != name {
                return Err(Status::failed_precondition(format!(
                    "compacted field {f} is {:?} but the shard's is {name:?}",
                    built.field_name(f)
                )));
            }
            let got = built.analysis_fingerprint(f);
            if got != 0 && *fingerprint != 0 && got != *fingerprint {
                return Err(Status::failed_precondition(format!(
                    "field {name:?}: the replay analyzed under fingerprint {got:#x} but the \
                     shard holds {fingerprint:#x}; the analysis backend does not reproduce the \
                     shard's term identity",
                )));
            }
        }
        Ok(())
    }

    /// Apply one pass of tailed records to the shadow, in clock order.
    /// Every document of the pass is analyzed in one batch first (one
    /// session per spec, as ingest opens), then the records apply.
    fn apply_pass(
        &self,
        shadow: &mut Shadow,
        records: Vec<crate::pb::wal::WalRecord>,
        analyze: &mut Analyze<'_>,
    ) -> Result<(), Status> {
        let docs: Vec<&AddDocumentsRequest> = records
            .iter()
            .filter_map(|record| match &record.op {
                Some(wal_record::Op::AddDocuments(add)) => Some(add.documents.iter()),
                _ => None,
            })
            .flatten()
            .collect();
        let mut analyzed = self.analyze_records(&docs, analyze)?.into_iter();
        for record in records {
            self.apply_to_shadow(shadow, record, &mut analyzed)?;
        }
        if analyzed.next().is_some() {
            return Err(Status::internal(
                "the tail analyzed more documents than it applied",
            ));
        }
        Ok(())
    }

    /// Apply one tailed record to the shadow. Appends get shadow ids and
    /// extend the id map; deletes and replacements map through it, and
    /// an id it does not know is an error; a Bind applies; a Flush marker
    /// is nothing; a Snapshot marker aborts by name.
    fn apply_to_shadow(
        &self,
        shadow: &mut Shadow,
        record: crate::pb::wal::WalRecord,
        analyzed: &mut std::vec::IntoIter<crate::postings::AnalyzedDoc>,
    ) -> Result<(), Status> {
        match record.op {
            Some(wal_record::Op::AddVectors(add)) => {
                let batch = add
                    .batch
                    .ok_or_else(|| Status::internal("WAL vector record has no batch"))?;
                let dim = batch.dim as usize;
                if dim == 0 || !batch.vectors.len().is_multiple_of(dim) {
                    return Err(Status::internal("WAL vector record has invalid dimensions"));
                }
                let rows = (batch.vectors.len() / dim) as u64;
                let key = add.stable_routing_keys.into_iter().next();
                let (added, first) = self.apply_batch_locked(&mut shadow.state, batch, key)?;
                if added != rows {
                    return Err(Status::internal(format!(
                        "the shadow applied {added} of {rows} tailed vectors"
                    )));
                }
                for i in 0..rows {
                    map_tailed(&mut shadow.id_map, add.first_id + i, first + i)?;
                }
            }
            Some(wal_record::Op::AddDocuments(add)) => {
                if add.stable_routing_keys.len() > add.documents.len() {
                    return Err(Status::internal(
                        "WAL document record carries more stable keys than documents",
                    ));
                }
                let mut keys = add.stable_routing_keys.into_iter();
                for (i, doc) in add.documents.into_iter().enumerate() {
                    let analyzed = analyzed.next().ok_or_else(|| {
                        Status::internal("the tail applied more documents than it analyzed")
                    })?;
                    let key = keys.next();
                    let (doc, analyzed) =
                        self.materialize_document(doc, analyzed, key.as_deref())?;
                    let mut added = 0u64;
                    let mut first = 0u64;
                    self.apply_document_locked(
                        &mut shadow.state,
                        doc,
                        analyzed,
                        None,
                        key,
                        &mut added,
                        &mut first,
                    )?;
                    map_tailed(&mut shadow.id_map, add.first_id + i as u64, first)?;
                }
            }
            Some(wal_record::Op::DeleteDocument(delete)) => {
                let new_id = mapped(&shadow.id_map, delete.doc_id)?;
                let response = self.delete_documents_locked(&mut shadow.state, &[new_id], None)?;
                if response.deleted != 1 {
                    return Err(Status::internal(format!(
                        "tailed delete of source id {} (shadow id {new_id}) hit a row the shadow \
                         already tombstoned; the log and the shadow disagree",
                        delete.doc_id
                    )));
                }
            }
            Some(wal_record::Op::Replacement(replacement)) => {
                let old = mapped(&shadow.id_map, replacement.old_doc_id)?;
                let new = mapped(&shadow.id_map, replacement.new_doc_id)?;
                let response = self.commit_replacements_locked(
                    &mut shadow.state,
                    &[crate::pb::Replacement {
                        old_doc_id: old,
                        new_doc_id: new,
                    }],
                    None,
                )?;
                if response.committed != 1 {
                    return Err(Status::internal(format!(
                        "tailed replacement of source id {} hit a row the shadow already \
                         tombstoned; the log and the shadow disagree",
                        replacement.old_doc_id
                    )));
                }
            }
            Some(wal_record::Op::Bind(bind)) => {
                Self::apply_binding_locked(
                    &mut shadow.state,
                    crate::postings::StoredBinding {
                        plan_fingerprint: bind.plan_fingerprint,
                        body_path: bind.body_path,
                        materialize_sha: bind.materialize_sha,
                        analysis_sha: bind.analysis_sha,
                        analysis_contract: bind.analysis_contract,
                        vector_binding: bind.vector_binding,
                        index_contract: bind.index_contract,
                    },
                )?;
            }
            Some(wal_record::Op::Snapshot(snapshot)) => {
                return Err(Status::aborted(format!(
                    "a snapshot was installed on this shard during compaction (marker at clock \
                     {}, superseding generation {}); the compaction is aborted and the live \
                     shard is untouched",
                    record.clock, snapshot.source_generation
                )));
            }
            Some(wal_record::Op::Flush(_)) => return Ok(()),
            None => return Err(Status::internal("WAL record without an operation")),
        }
        shadow.tail_records += 1;
        if shadow.state.wal.is_none() {
            return Err(Status::internal(
                "the rewritten WAL generation failed to append and was retired; the compaction \
                 is aborted",
            ));
        }
        Ok(())
    }

    /// Analyze logged documents the way ingest did: body and extra
    /// fields through the node's backend, with the layers each record
    /// names, one batch for the lot, assembled positionally per document.
    fn analyze_records(
        &self,
        docs: &[&AddDocumentsRequest],
        analyze: &mut Analyze<'_>,
    ) -> Result<Vec<crate::postings::AnalyzedDoc>, Status> {
        if docs.is_empty() {
            return Ok(Vec::new());
        }
        let mut batch: Vec<(
            &str,
            Option<&crate::pb::AnalysisSpec>,
            crate::analyzer::SessionLayers,
        )> = Vec::new();
        let mut slots: Vec<Vec<usize>> = Vec::with_capacity(docs.len());
        for doc in docs {
            batch.push((
                doc.text.as_str(),
                doc.analysis.as_ref(),
                crate::analyzer::SessionLayers {
                    sentences: !doc.sentence_fields.is_empty(),
                    dual_cased: !doc.cased_field.is_empty(),
                    ..Default::default()
                },
            ));
            let mut own = Vec::with_capacity(doc.fields.len());
            for field in &doc.fields {
                let fi = self
                    .config
                    .bm25_fields
                    .iter()
                    .position(|n| *n == field.field)
                    .ok_or_else(|| {
                        Status::failed_precondition(format!(
                            "logged document names field {:?}, which this node's table {:?} \
                             lacks",
                            field.field, self.config.bm25_fields
                        ))
                    })?;
                own.push(fi);
                batch.push((
                    field.text.as_str(),
                    field.analysis.as_ref(),
                    crate::analyzer::SessionLayers {
                        sentences: doc.sentence_fields.iter().any(|n| n == &field.field),
                        ..Default::default()
                    },
                ));
            }
            slots.push(own);
        }
        let results = analyze(&batch)
            .map_err(|e| Status::unavailable(format!("analyze tailed documents: {e}")))?;
        if results.len() != batch.len() {
            return Err(Status::internal(format!(
                "analysis returned {} results for {} texts",
                results.len(),
                batch.len()
            )));
        }
        let mut results = results.into_iter();
        let mut out = Vec::with_capacity(docs.len());
        for (doc, own) in docs.iter().zip(slots) {
            let body = results.next().expect("counted above");
            let mut extras = Vec::with_capacity(own.len());
            for fi in own {
                let analyzed = results.next().expect("counted above");
                extras.push((fi, Some(analyzed.into_body())));
            }
            let cased =
                crate::node::cased_field_index(&self.config, self.phrase_index.as_deref(), doc)?;
            out.push(crate::node::join_fields(body, extras, cased)?);
        }
        Ok(out)
    }

    /// Fsync the live log and return its high watermark: the bound of
    /// one tail pass. Brief, under the write lock.
    fn flush_live_wal(&self, pre: &Preflight) -> Result<u64, Status> {
        let mut guard = crate::node::write_shard(&self.state);
        Self::flush_live_wal_locked(&mut guard, pre)
    }

    /// [`Self::flush_live_wal`] on a guard the caller holds. A snapshot
    /// install rotates the log to a new generation (its marker lands in
    /// THAT generation), so the rotation itself is the abort signal.
    fn flush_live_wal_locked(guard: &mut ShardState, pre: &Preflight) -> Result<u64, Status> {
        let wal = guard.wal.as_mut().ok_or_else(|| {
            Status::failed_precondition("the shard lost its WAL during compaction")
        })?;
        if wal.generation() != pre.cutoff_generation {
            return Err(Status::aborted(format!(
                "a snapshot was installed on this shard during compaction (the WAL rotated from \
                 generation {} to {}); the compaction is aborted and the live shard is untouched",
                pre.cutoff_generation,
                wal.generation()
            )));
        }
        wal.flush()
            .map_err(|e| Status::internal(format!("wal fsync during compaction: {e}")))?;
        Ok(wal.high_watermark())
    }

    /// The tail loop and the cutover. Returns `(records applied under the
    /// lock, write-lock hold in ms, unlocked passes)`.
    fn tail_and_cut_over(
        &self,
        pre: &Preflight,
        shadow: &mut Shadow,
        tail_bound: usize,
        analyze: &mut Analyze<'_>,
    ) -> Result<(u64, u64, u64), Status> {
        let mut tail = ClockedTail::start(&pre.gen_dir, pre.cutoff_clock);
        let mut passes = 0u64;
        let mut smallest = usize::MAX;
        let mut stalled = 0u64;
        loop {
            passes += 1;
            if passes > MAX_TAIL_PASSES {
                return Err(Status::resource_exhausted(format!(
                    "writes outpace compaction: {MAX_TAIL_PASSES} tail passes never left fewer \
                     than {tail_bound} records to apply"
                )));
            }
            let watermark = self.flush_live_wal(pre)?;
            let records = tail
                .read_through(watermark)
                .map_err(|e| Status::failed_precondition(format!("tail the live WAL: {e}")))?;
            let count = records.len();
            self.apply_pass(shadow, records, analyze)?;
            if count < tail_bound {
                break;
            }
            if count < smallest {
                smallest = count;
                stalled = 0;
            } else {
                stalled += 1;
                if stalled >= STALLED_TAIL_PASSES {
                    return Err(Status::resource_exhausted(format!(
                        "writes outpace compaction: {STALLED_TAIL_PASSES} consecutive tail \
                         passes read no fewer than {smallest} records (last {count}, \
                         tail_bound {tail_bound}) after {passes} passes; pause writes or \
                         raise tail_bound"
                    )));
                }
            }
        }

        // Reserve commits before taking the seal lock, matching Flush's
        // order. Writers wait asynchronously while analysis and reads can
        // still run. The state lock is held only for the final fence/swap.
        let _mutation = self.mutation_gate.blocking_write();
        let _seal = self.seal_lock.lock().expect("seal lock poisoned");
        let mut attempt = 0usize;
        loop {
            attempt += 1;
            let watermark = self.flush_live_wal(pre)?;
            let records = tail
                .read_through(watermark)
                .map_err(|e| Status::failed_precondition(format!("tail the live WAL: {e}")))?;
            // Analysis may use asynchronous I/O. A live-state lock here
            // can block the runtime workers needed to complete that I/O.
            self.apply_pass(shadow, records, analyze)?;
            passes += 1;
            let mut guard = crate::node::write_shard(&self.state);
            let started = Instant::now();
            if Self::flush_live_wal_locked(&mut guard, pre)? != watermark {
                drop(guard);
                if attempt >= CUTOVER_RETRIES {
                    return Err(Status::resource_exhausted(format!(
                        "writes outpace compaction: the live WAL advanced during all \
                         {CUTOVER_RETRIES} cutover preparations"
                    )));
                }
                continue;
            }
            shadow
                .state
                .wal
                .as_mut()
                .expect("checked after every apply")
                .flush()
                .map_err(|e| Status::internal(format!("fsync the rewritten generation: {e}")))?;
            self.install(pre, shadow, &guard)?;
            shadow.state.stats_epoch = guard.stats_epoch;
            shadow.state.advance_stats_epoch();
            shadow.state.parents = None;
            let previous = std::mem::replace(&mut *guard, std::mem::take(&mut shadow.state));
            let held = started.elapsed().as_millis() as u64;
            drop(guard);
            drop(previous);
            return Ok((0, held, passes));
        }
    }

    /// The on-disk cutover, under the write lock the caller holds:
    /// marker first, then the rewritten WAL generation into the shard's
    /// WAL directory, then the layout's own swap.
    fn install(
        &self,
        pre: &Preflight,
        shadow: &mut Shadow,
        live: &ShardState,
    ) -> Result<(), Status> {
        let wal_dir = wal::wal_dir(&pre.index_path);
        let new_generation = pre.cutoff_generation + 1;
        let root = crate::node::segments_root(&pre.index_path);
        let previous_snapshot = live.generation.is_some();
        let legacy_files: Vec<PathBuf> = if pre.segmented || previous_snapshot {
            Vec::new()
        } else {
            [
                pre.index_path.clone(),
                crate::node::exact_vector_sidecar_path(&pre.index_path),
                crate::node::bm25_sidecar_path(&pre.index_path),
                crate::node::live_docs_sidecar_path(&pre.index_path),
            ]
            .into_iter()
            .filter(|p| p.exists())
            .collect()
        };
        if pre.segmented {
            let current = SegmentCatalog::read_manifest(&root)
                .map_err(Status::internal)?
                .unwrap_or_default();
            crate::segments::write_manifest_file(&manifest_backup_path(&root), &current)
                .map_err(Status::internal)?;
        }
        let marker = CommitMarker {
            format: MARKER_FORMAT,
            layout: layout_name(pre.segmented).to_string(),
            old_wal_generation: pre.cutoff_generation,
            new_wal_generation: new_generation,
            walless: false,
            work_dir: pre.work_dir.clone(),
            previous_snapshot,
            legacy_files,
            staged_segments: shadow.staged.iter().map(|s| s.segment_id.clone()).collect(),
            replaced_segments: shadow.replaced.clone(),
        };
        write_marker(&pre.index_path, &marker)
            .map_err(|e| Status::internal(format!("write the compaction marker: {e}")))?;
        // From here every failure leaves the marker, and a restart rolls
        // back; the live state is not swapped until everything is in
        // place, so a failure returned here also rolls back at once.
        let result = (|| -> Result<(), Status> {
            let target = wal::gen_dir(&wal_dir, new_generation);
            let staged_gen = wal::gen_dir(&pre.work_dir.join("wal"), new_generation);
            std::fs::rename(&staged_gen, &target).map_err(|e| {
                Status::internal(format!(
                    "move the rewritten generation {} -> {}: {e}",
                    staged_gen.display(),
                    target.display()
                ))
            })?;
            crate::postings::fsync_parent(&target)
                .map_err(|e| Status::internal(format!("fsync {}: {e}", wal_dir.display())))?;
            shadow
                .state
                .wal
                .as_mut()
                .expect("the shadow keeps its writer")
                .relocate(target)
                .map_err(|e| Status::internal(format!("relocate the rewritten generation: {e}")))?;
            if pre.segmented {
                let Some(Bm25Shard::Segmented(shard)) = shadow.state.bm25.as_ref() else {
                    return Err(Status::internal("the segmented shadow lost its catalog"));
                };
                let live_epoch = match live.bm25.as_ref() {
                    Some(Bm25Shard::Segmented(live)) => live.snapshot().epoch(),
                    _ => shadow.epoch_at_open,
                };
                let epoch = live_epoch
                    .max(shadow.epoch_at_open)
                    .checked_add(1)
                    .ok_or_else(|| {
                        Status::out_of_range("compaction publication epoch exhausted")
                    })?;
                let published = shard
                    .catalog()
                    .commit_current(epoch)
                    .map_err(|e| Status::internal(format!("publish the compacted set: {e}")))?;
                // Publication returns a new immutable view. Adopt it in both
                // legs; previously an in-place epoch mutation hid this step.
                if let Some(Bm25Shard::Segmented(shard)) = shadow.state.bm25.as_mut() {
                    shard
                        .republish(published.clone())
                        .map_err(|e| Status::internal(format!("adopt compacted documents: {e}")))?;
                }
                if let Some(provider) = shadow
                    .state
                    .index
                    .as_mut()
                    .and_then(VectorIndex::as_segmented_mut)
                {
                    provider
                        .republish(published)
                        .map_err(|e| Status::internal(format!("adopt compacted vectors: {e}")))?;
                }
            } else {
                let staged = shadow
                    .state
                    .generation
                    .clone()
                    .expect("the single-image shadow serves a generation directory");
                let snap = Self::adopt_generation(&pre.index_path, &staged, false)?;
                shadow.state.generation = Some(snap);
            }
            Ok(())
        })();
        match result {
            Ok(()) => {
                shadow.state.pending_compaction = Some(PendingCommit {
                    index_path: pre.index_path.clone(),
                    marker,
                });
                Ok(())
            }
            Err(status) => {
                recover_interrupted(&pre.index_path);
                Err(status)
            }
        }
    }

    // ----- The from-segments path: a catalog without a WAL -----
    //
    // docs/replay-from-segments.md, "Partitioned compaction of a catalog
    // without a log". The cutoff is the sealed catalog once the current
    // tail is sealed; the build transplants its live rows through
    // `reshard::compact_segments_partitioned` (the analyzer never runs);
    // writes that arrive after the cutoff are caught up by the same seal
    // ingest uses (`seal_tail`), landing in the live catalog as unordered
    // segments the cutover carries over; the cutover keeps the
    // shadow/marker/closing-flush contract of docs/mutations.md.

    /// Seal the tail when it holds rows (or a frozen part is waiting on
    /// its publication), waiting out a legacy two-RPC append caught
    /// mid-row the way the closing flush waits it out. Returns the rows
    /// the seal covered, 0 when there was nothing to seal.
    fn seal_tail_wait(&self) -> Result<u64, Status> {
        const ATTEMPTS: usize = 200;
        const PAUSE: std::time::Duration = std::time::Duration::from_millis(10);
        for attempt in 1..=ATTEMPTS {
            let (pending, frozen) = {
                let guard = crate::node::read_shard(&self.state);
                let (documents, vectors) = match guard.bm25.as_ref() {
                    Some(Bm25Shard::Segmented(shard)) => (
                        shard.tail().next_doc_id() as usize,
                        guard
                            .index
                            .as_ref()
                            .and_then(VectorIndex::as_segmented)
                            .map_or(0, |p| p.tail().len()),
                    ),
                    _ => (0, 0),
                };
                (
                    documents.max(vectors) as u64,
                    matches!(guard.bm25.as_ref(), Some(Bm25Shard::Segmented(shard)) if shard.frozen().is_some()),
                )
            };
            if pending == 0 && !frozen {
                return Ok(0);
            }
            match self.seal_tail() {
                Ok(_) => return Ok(pending),
                Err(status)
                    if attempt < ATTEMPTS
                        && status.code() == tonic::Code::FailedPrecondition
                        && status
                            .message()
                            .contains("a segment's artifacts cover the same rows") =>
                {
                    std::thread::sleep(PAUSE);
                }
                Err(status) => return Err(status),
            }
        }
        unreachable!("the last attempt returns")
    }

    /// The tail is empty on both legs and no seal is in flight.
    fn segment_tail_is_clear(guard: &ShardState) -> bool {
        let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
            return false;
        };
        if shard.tail().next_doc_id() != 0 || shard.frozen().is_some() {
            return false;
        }
        guard
            .index
            .as_ref()
            .and_then(VectorIndex::as_segmented)
            .is_none_or(|p| p.tail().is_empty() && p.frozen().is_none())
    }

    /// The from-segments compaction of a catalog without a WAL, start to
    /// closing flush. On any failure before the cutover commits, the live
    /// shard is untouched and the staged outputs are removed.
    fn compact_from_segments(
        &self,
        request: &CompactShardRequest,
        mut pre: SegmentPreflight,
        tail_bound: usize,
    ) -> Result<CompactShardResponse, Status> {
        let options = crate::reshard::SegmentCompactionOptions {
            build_threads: request.build_threads.max(1) as usize,
            build_queue: (request.build_queue != 0).then_some(request.build_queue as usize),
            build_memory: (request.build_memory != 0).then_some(request.build_memory),
        };
        if request.dry_run {
            return Ok(CompactShardResponse {
                rows_before: pre.rows_now,
                rows_after: pre.rows_now,
                tombstones_reclaimed: pre.tombstones_now,
                layout: layout_name(true).to_string(),
                dry_run: true,
                stats_epoch: pre.stats_epoch,
                partition_column: pre.partition.clone(),
                ..Default::default()
            });
        }
        std::fs::create_dir_all(&pre.work_dir)
            .map_err(|e| Status::internal(format!("mkdir {}: {e}", pre.work_dir.display())))?;
        let outcome = self.build_stage_and_cut_over_segments(&mut pre, tail_bound, &options);
        if outcome.is_ok() {
            // Everything the work directory held was copied into place at
            // staging (the catalog hashes its own copies).
            if let Err(error) = std::fs::remove_dir_all(&pre.work_dir) {
                eprintln!(
                    "compaction: removing the work directory {} failed: {error}",
                    pre.work_dir.display()
                );
            }
        }
        outcome
    }

    fn build_stage_and_cut_over_segments(
        &self,
        pre: &mut SegmentPreflight,
        tail_bound: usize,
        options: &crate::reshard::SegmentCompactionOptions,
    ) -> Result<CompactShardResponse, Status> {
        // The cutoff seal: the current tail joins the sealed set, so the
        // cutoff is the sealed catalog and only later writes are caught
        // up. Writes continue; nothing here locks them out.
        self.seal_tail_wait()?;
        {
            let guard = crate::node::read_shard(&self.state);
            let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
                return Err(Status::internal(
                    "the shard changed layout while a compaction was starting",
                ));
            };
            if !Self::segment_tail_is_clear(&guard) {
                return Err(Status::internal("the cutoff seal left an unsealed tail"));
            }
            pre.set = shard.snapshot().clone();
            pre.overlay = guard.live_docs.clone();
            pre.epoch = shard.snapshot().epoch();
            pre.stats_epoch = guard.stats_epoch;
        }
        // A column no sealed row carries cannot order the outputs (the
        // build refuses a column no LIVE row carries; this refuses before
        // any work when even the physical rows lack it).
        let sealed: u64 = pre
            .set
            .manifest()
            .segments
            .iter()
            .filter_map(|segment| segment.summary.as_ref())
            .flat_map(|summary| summary.int_columns.iter())
            .filter(|c| c.name == pre.partition)
            .map(|c| c.present)
            .sum();
        if sealed == 0 {
            return Err(Status::failed_precondition(format!(
                "partition_column {:?}: no document of this shard carries it, so it cannot \
                 order the rows",
                pre.partition
            )));
        }
        let build_dir = pre.work_dir.join("build");
        let build = crate::reshard::compact_segments_partitioned(
            &pre.set,
            &pre.overlay,
            self.config.slot_offset,
            &build_dir,
            crate::reshard::PartitionSpec {
                column: &pre.partition,
                bound: tail_bound,
            },
            options,
        )
        .map_err(|e| Status::failed_precondition(format!("compaction build: {e}")))?;
        let staged = self.stage_segment_outputs(pre, &build)?;
        let root = crate::node::segments_root(&pre.index_path);
        #[cfg(test)]
        if let Err(error) = segment_staging_hook::run(self, &pre.index_path) {
            crate::segments::remove_segment_dirs(&root, &staged);
            return Err(Status::internal(format!(
                "segment compaction staging hook: {error}"
            )));
        }

        // The tail catch-up: whatever arrived after the cutoff seals into
        // the live catalog as unordered segments through the same seal
        // ingest uses. Each pass seals the whole tail; the loop ends when
        // one finds nothing.
        let mut tail_passes = 0u64;
        let mut smallest = u64::MAX;
        let mut stalled = 0u64;
        loop {
            tail_passes += 1;
            if tail_passes > MAX_TAIL_PASSES {
                crate::segments::remove_segment_dirs(&root, &staged);
                return Err(Status::resource_exhausted(format!(
                    "writes outpace compaction: {MAX_TAIL_PASSES} tail seals never left the \
                     tail empty"
                )));
            }
            let sealed = self.seal_tail_wait()?;
            if sealed == 0 {
                break;
            }
            if sealed < smallest {
                smallest = sealed;
                stalled = 0;
            } else {
                stalled += 1;
                if stalled >= STALLED_TAIL_PASSES {
                    crate::segments::remove_segment_dirs(&root, &staged);
                    return Err(Status::resource_exhausted(format!(
                        "writes outpace compaction: {STALLED_TAIL_PASSES} consecutive tail \
                         seals each sealed at least {smallest} rows; pause writes, then retry"
                    )));
                }
            }
        }

        // The cutover. Reserve commits first: with the gate held, no
        // write but a gate-bypassing one can land, and the fence below
        // catches those. The seal lock keeps a seal from racing the
        // manifest read.
        let _mutation = self.mutation_gate.blocking_write();
        let mut attempt = 0usize;
        let (write_lock_ms, carried_rows) = loop {
            attempt += 1;
            self.seal_tail_wait()?;
            let _seal = self.seal_lock.lock().expect("seal lock poisoned");
            {
                let guard = crate::node::read_shard(&self.state);
                if !Self::segment_tail_is_clear(&guard) {
                    if attempt >= CUTOVER_RETRIES {
                        crate::segments::remove_segment_dirs(&root, &staged);
                        return Err(Status::resource_exhausted(format!(
                            "writes outpace compaction: the tail was not empty after \
                             {CUTOVER_RETRIES} cutover preparations"
                        )));
                    }
                    continue;
                }
            }
            let prepared = match self.prepare_segment_cutover(pre, &build, &staged) {
                Ok(prepared) => prepared,
                Err(status) => {
                    crate::segments::remove_segment_dirs(&root, &staged);
                    return Err(status);
                }
            };
            let mut guard = crate::node::write_shard(&self.state);
            let started = Instant::now();
            if !self.segment_cutover_fence(&guard, &prepared) {
                drop(guard);
                if attempt >= CUTOVER_RETRIES {
                    crate::segments::remove_segment_dirs(&root, &staged);
                    return Err(Status::resource_exhausted(format!(
                        "writes outpace compaction: the live catalog or its overlay advanced \
                         during all {CUTOVER_RETRIES} cutover preparations"
                    )));
                }
                continue;
            }
            let carried_rows = prepared.carried_rows;
            match self.install_segments(pre, prepared, &mut guard) {
                Ok(()) => break (started.elapsed().as_millis() as u64, carried_rows),
                Err(status) => return Err(status),
            }
        };
        // rows_after is the cutover's answer: the dense partitions plus
        // the carried rows. Read it before the closing flush, whose seal
        // would fold in writes that landed after the swap.
        let (rows_after, stats_epoch) = {
            let guard = crate::node::read_shard(&self.state);
            (crate::node::physical_rows(&guard), guard.stats_epoch)
        };
        let closing = Instant::now();
        self.closing_flush()?;
        let closing_flush_ms = closing.elapsed().as_millis() as u64;
        let stats_epoch = {
            let guard = crate::node::read_shard(&self.state);
            guard.stats_epoch.max(stats_epoch)
        };
        Ok(CompactShardResponse {
            rows_before: build.rows_before,
            rows_after,
            tombstones_reclaimed: build.tombstones,
            tail_records_applied: carried_rows,
            locked_tail_records: 0,
            write_lock_ms,
            wal_generation: 0,
            cutoff_clock: 0,
            layout: layout_name(true).to_string(),
            dry_run: false,
            closing_flush_ms,
            tail_passes,
            stats_epoch,
            partition_column: pre.partition.clone(),
        })
    }

    /// Stage the build's images under the live catalog root (hashed,
    /// fsynced, unpublished, `cmp-<epoch>-NNNN` ids), then open them once
    /// as a set for validation: the field table and fingerprints are the
    /// shard's, the declaration the segments', the provider state the
    /// live shard's.
    fn stage_segment_outputs(
        &self,
        pre: &SegmentPreflight,
        build: &crate::reshard::SegmentCompactionBuild,
    ) -> Result<Vec<SegmentMetadata>, Status> {
        let root = crate::node::segments_root(&pre.index_path);
        let generation = pre
            .epoch
            .checked_add(1)
            .ok_or_else(|| Status::out_of_range("compaction staging epoch exhausted"))?;
        let mut live_paths = Vec::with_capacity(build.images.len());
        for image in &build.images {
            if image.num_vectors != 0 && image.num_vectors as usize != image.row_parent_ids.len() {
                return Err(Status::failed_precondition(format!(
                    "compaction output {} has {} vectors over {} rows; a segment's artifacts \
                     cover the same rows",
                    image.vector_path.display(),
                    image.num_vectors,
                    image.row_parent_ids.len()
                )));
            }
            if image.bm25_path.is_none() {
                return Err(Status::failed_precondition(format!(
                    "compaction output {} has no BM25 image; the segment layout seals documents",
                    image.vector_path.display()
                )));
            }
            let path = crate::node::live_docs_sidecar_path(&image.vector_path);
            LiveDocs::default()
                .write(&path, image.row_parent_ids.len() as u64)
                .map_err(|e| Status::internal(format!("write {}: {e}", path.display())))?;
            live_paths.push(path);
        }
        let ids: Vec<String> = (0..build.images.len())
            .map(|i| format!("cmp-{generation:06}-{i:04}"))
            .collect();
        let sources: Vec<SegmentSource<'_>> = build
            .images
            .iter()
            .zip(&ids)
            .zip(&live_paths)
            .map(|((image, id), live)| SegmentSource {
                segment_id: id,
                generation,
                base_label: image.slot_offset,
                backend_kind: &pre.backend_kind,
                vector_path: (image.num_vectors > 0).then_some(image.vector_path.as_path()),
                exact_vector_path: (image.num_vectors > 0)
                    .then_some(image.exact_vector_path.as_path()),
                bm25_path: image.bm25_path.as_deref().expect("checked above"),
                live_docs_path: live,
                partition_column: Some(&pre.partition),
            })
            .collect();
        let staged = crate::segments::stage_segments(&root, sources)
            .map_err(|e| Status::internal(format!("stage compacted segments: {e}")))?;
        let opened = (|| -> Result<(), Status> {
            let manifest = SegmentSetManifest {
                epoch: pre.epoch,
                segments: staged.clone(),
                partition_key: Some(pre.partition.clone()),
                generation_declaration: pre.set.manifest().generation_declaration.clone(),
                ..Default::default()
            }
            .with_binding(build.binding.as_ref())
            .map_err(Status::failed_precondition)?;
            let catalog = SegmentCatalog::open_staged(&root, manifest, self.config.vector_load())
                .map_err(|e| Status::internal(format!("open the compacted set: {e}")))?;
            let tail =
                crate::node::heap_store(&self.config).map_err(Status::failed_precondition)?;
            let shard =
                crate::segmented::SegmentedShard::open_catalog(catalog, tail).map_err(|e| {
                    Status::failed_precondition(format!("the compacted set does not open: {e}"))
                })?;
            let set = shard.snapshot().clone();
            self.check_shadow_fingerprints(pre.fields.as_ref(), &Bm25Shard::Segmented(shard))?;
            if let Some(first) = (0..set.len()).find_map(|i| set.vector(i)) {
                let d = first.descriptor();
                if d.backend_kind != pre.backend_kind
                    || d.scoring_fingerprint != pre.scoring_fingerprint
                {
                    return Err(Status::failed_precondition(format!(
                        "the compacted segments score under {}/{} but the shard serves {}/{}; \
                         the catalog's provider state does not reproduce the live generation",
                        d.backend_kind,
                        d.scoring_fingerprint,
                        pre.backend_kind,
                        pre.scoring_fingerprint
                    )));
                }
            }
            Ok(())
        })();
        if opened.is_err() {
            crate::segments::remove_segment_dirs(&root, &staged);
        }
        opened?;
        Ok(staged)
    }

    /// Prepare the cutover with no lock held past a read: the final
    /// manifest (the rebuilt partitions, then every segment sealed after
    /// the cutoff, re-based onto the dense row count), the live overlay
    /// translated onto the new labels, and the shadow serving state
    /// opened over the prepared catalog. The fence at install re-checks
    /// the epoch, the overlay revision, and the tail.
    fn prepare_segment_cutover(
        &self,
        pre: &SegmentPreflight,
        build: &crate::reshard::SegmentCompactionBuild,
        staged: &[SegmentMetadata],
    ) -> Result<PreparedSegmentCutover, Status> {
        let slot_offset = self.config.slot_offset;
        let root = crate::node::segments_root(&pre.index_path);
        let (manifest_now, overlay_now) = {
            let guard = crate::node::read_shard(&self.state);
            let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
                return Err(Status::internal(
                    "the shard changed layout while a compaction was running",
                ));
            };
            (shard.snapshot().manifest().clone(), guard.live_docs.clone())
        };
        let source = &pre.set.manifest().segments;
        let s_rows = source
            .iter()
            .try_fold(0u64, |rows, s| rows.checked_add(s.rows))
            .ok_or_else(|| Status::internal("cutoff set row count overflow"))?;
        let prefix_holds = manifest_now.segments.len() >= source.len()
            && manifest_now.segments[..source.len()]
                .iter()
                .zip(source)
                .all(|(now, at_cutoff)| {
                    now.segment_id == at_cutoff.segment_id && now.base_label == at_cutoff.base_label
                });
        if !prefix_holds {
            return Err(Status::aborted(
                "the catalog's sealed set changed under the compaction; the build transplanted \
                 the set at its cutoff, so this run cannot cut over — retry",
            ));
        }
        let carried = &manifest_now.segments[source.len()..];
        let mut total = s_rows;
        for segment in carried {
            if segment.base_label != total {
                return Err(Status::internal(format!(
                    "carried segment {} starts at label {} but the segments before it end at \
                     {total}; the live catalog is not one contiguous id space",
                    segment.segment_id, segment.base_label
                )));
            }
            total += segment.rows;
        }
        let dense = build
            .images
            .iter()
            .try_fold(0u64, |rows, image| {
                rows.checked_add(image.row_parent_ids.len() as u64)
            })
            .ok_or_else(|| Status::internal("compacted row count overflow"))?;
        // The outputs, then the carried segments re-based onto them.
        let mut segments: Vec<SegmentMetadata> = staged.to_vec();
        let mut base = dense;
        for segment in carried {
            let mut rebased = segment.clone();
            rebased.base_label = base;
            base += segment.rows;
            segments.push(rebased);
        }
        // The live overlay is the tombstone authority. A bit under the
        // cutoff's rows names a rebuilt row through the id map (absent =
        // already dead at the cutoff, dropped by the build); a bit above
        // them names a carried row, shifted by the reclaimed tombstones.
        let mut overlay = LiveDocs::default();
        if let Some(words) = overlay_now.words() {
            for (wi, &word) in words.iter().enumerate() {
                let mut bits = word;
                while bits != 0 {
                    let bit = bits.trailing_zeros() as usize;
                    bits &= bits - 1;
                    let label = (wi * 64 + bit) as u64;
                    if label < s_rows {
                        let global = slot_offset
                            .checked_add(label)
                            .ok_or_else(|| Status::internal("compaction id overflow"))?;
                        if let Some(&new_local) = build.id_map.get(&global) {
                            overlay.delete(new_local as usize);
                        }
                    } else if label < total {
                        overlay.delete((dense + (label - s_rows)) as usize);
                    } else {
                        return Err(Status::internal(format!(
                            "the live overlay names row {label}, past the catalog's {total} rows"
                        )));
                    }
                }
            }
        }
        let final_manifest = SegmentSetManifest {
            epoch: manifest_now.epoch,
            segments,
            partition_key: Some(pre.partition.clone()),
            generation_declaration: manifest_now.generation_declaration.clone(),
            ..Default::default()
        }
        .with_binding(build.binding.as_ref())
        .map_err(Status::failed_precondition)?;
        let catalog = SegmentCatalog::open_staged(&root, final_manifest, self.config.vector_load())
            .map_err(|e| Status::internal(format!("open the compacted set: {e}")))?;
        let tail = crate::node::heap_store(&self.config).map_err(Status::failed_precondition)?;
        let shard =
            crate::segmented::SegmentedShard::open_catalog(catalog.clone(), tail).map_err(|e| {
                Status::failed_precondition(format!("the compacted set does not open: {e}"))
            })?;
        let set = shard.snapshot().clone();
        let mut index = None;
        let mut exact_vectors = None;
        if let Some(first) = (0..set.len()).find_map(|i| set.vector(i)) {
            let backend = first
                .backend_config()
                .map_err(|e| Status::internal(format!("segment vector backend: {e}")))?;
            let dim = first
                .dim_opt()
                .ok_or_else(|| Status::internal("segment vector image has no dimension"))?;
            let tail_image = VectorIndex::from_backend_config(dim, &backend)
                .map_err(|e| Status::internal(format!("segment tail image: {e}")))?;
            let provider =
                crate::segmented_vectors::SegmentedProvider::open(set.clone(), tail_image)
                    .map_err(|e| Status::internal(format!("segment vectors: {e}")))?;
            index = Some(VectorIndex::from_provider(provider));
            exact_vectors = Some(ExactVectorStore::from_segments(&set, dim).map_err(|error| {
                Status::internal(format!("compacted exact-vector view: {error}"))
            })?);
        }
        Ok(PreparedSegmentCutover {
            catalog,
            shard: Some(shard),
            index,
            exact_vectors,
            overlay,
            epoch: manifest_now.epoch,
            revision: overlay_now.revision(),
            staged_ids: staged.iter().map(|s| s.segment_id.clone()).collect(),
            replaced_ids: source.iter().map(|s| s.segment_id.clone()).collect(),
            carried_rows: total - s_rows,
        })
    }

    /// The fence under the write lock: the catalog, the overlay, and the
    /// tail are exactly what the preparation read. Anything else means a
    /// gate-bypassing write landed; release and prepare again.
    fn segment_cutover_fence(&self, guard: &ShardState, prepared: &PreparedSegmentCutover) -> bool {
        let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
            return false;
        };
        shard.snapshot().epoch() == prepared.epoch
            && guard.live_docs.revision() == prepared.revision
            && Self::segment_tail_is_clear(guard)
    }

    /// The on-disk cutover, under the write lock the caller holds: the
    /// manifest backup, the commit marker, one manifest publish through
    /// the catalog's own path, the state swap. No WAL moves: this path
    /// writes none.
    fn install_segments(
        &self,
        pre: &SegmentPreflight,
        mut prepared: PreparedSegmentCutover,
        guard: &mut ShardState,
    ) -> Result<(), Status> {
        let root = crate::node::segments_root(&pre.index_path);
        let current = SegmentCatalog::read_manifest(&root)
            .map_err(Status::internal)?
            .unwrap_or_default();
        crate::segments::write_manifest_file(&manifest_backup_path(&root), &current)
            .map_err(Status::internal)?;
        let marker = CommitMarker {
            format: MARKER_FORMAT,
            layout: layout_name(true).to_string(),
            old_wal_generation: 0,
            new_wal_generation: 0,
            walless: true,
            work_dir: pre.work_dir.clone(),
            previous_snapshot: false,
            legacy_files: Vec::new(),
            staged_segments: prepared.staged_ids.clone(),
            replaced_segments: prepared.replaced_ids.clone(),
        };
        write_marker(&pre.index_path, &marker)
            .map_err(|e| Status::internal(format!("write the compaction marker: {e}")))?;
        // From here every failure leaves the marker, and a restart rolls
        // back; a failure returned here also rolls back at once.
        let result = (|| -> Result<(), Status> {
            let epoch = prepared
                .epoch
                .checked_add(1)
                .ok_or_else(|| Status::out_of_range("compaction publication epoch exhausted"))?;
            let published = prepared
                .catalog
                .commit_current(epoch)
                .map_err(|e| Status::internal(format!("publish the compacted set: {e}")))?;
            let shard = prepared.shard.as_mut().expect("prepared once");
            shard
                .republish(published.clone())
                .map_err(|e| Status::internal(format!("adopt compacted documents: {e}")))?;
            if let Some(provider) = prepared
                .index
                .as_mut()
                .and_then(VectorIndex::as_segmented_mut)
            {
                provider
                    .republish(published)
                    .map_err(|e| Status::internal(format!("adopt compacted vectors: {e}")))?;
            }
            let shard = prepared.shard.take().expect("checked above");
            let mapped_binding = shard.binding().cloned();
            let mut state = ShardState {
                index: prepared.index.take(),
                exact_vectors: prepared.exact_vectors.take(),
                bm25: Some(Bm25Shard::Segmented(shard)),
                live_docs: prepared.overlay.clone(),
                generation: None,
                wal: None,
                parents: None,
                mapped_binding,
                stats_epoch: guard.stats_epoch,
                files_current: false,
                stats_incarnation: Default::default(),
                pending_compaction: Some(PendingCommit {
                    index_path: pre.index_path.clone(),
                    marker,
                }),
            };
            state.advance_stats_epoch();
            let previous = std::mem::replace(guard, state);
            drop(previous);
            Ok(())
        })();
        match result {
            Ok(()) => Ok(()),
            Err(status) => {
                recover_interrupted(&pre.index_path);
                Err(status)
            }
        }
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

#[cfg(all(test, feature = "net"))]
mod tests {
    use super::*;
    use crate::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
    use crate::node::NodeConfig;
    use crate::vector::EMBEDDED_TURBOVEC;
    use prost::Message;
    use std::sync::Arc;

    #[tokio::test]
    async fn a_reserved_cutover_yields_writers_without_blocking_reads() {
        use crate::pb::node_service_server::NodeService;
        let node = NodeServiceImpl::new(None, NodeConfig::default());
        let reservation = node.mutation_gate.clone().write_owned().await;
        let writer_node = node.clone();
        let (started, entered) = tokio::sync::oneshot::channel();
        let writer = tokio::spawn(async move {
            started.send(()).unwrap();
            writer_node
                .delete_documents(tonic::Request::new(
                    crate::pb::DeleteDocumentsRequest::default(),
                ))
                .await
        });
        entered.await.unwrap();
        tokio::task::yield_now().await;
        assert!(
            !writer.is_finished(),
            "write bypassed the cutover reservation"
        );
        node.health(tonic::Request::new(crate::pb::HealthRequest {}))
            .await
            .unwrap();
        drop(reservation);
        let deleted = writer.await.unwrap().unwrap().into_inner();
        assert_eq!(deleted.deleted, 0);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cutover_analysis_allows_reads_and_includes_writes_during_preparation() {
        cutover_with_writes_during_analysis(false).await;
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cutover_refuses_persistent_writes_without_replacing_the_live_generation() {
        cutover_with_writes_during_analysis(true).await;
    }

    async fn cutover_with_writes_during_analysis(always_advance: bool) {
        tokio::task::spawn_blocking(move || {
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let dir = std::env::temp_dir()
                .join(format!("psearch-cutover-{}-{nonce}", std::process::id()));
            std::fs::create_dir_all(&dir).unwrap();
            let sample = crate::harness::unit_vectors(32, 64, 19);
            let config =
                VectorIndex::fit_backend_config(EMBEDDED_TURBOVEC, 64, 4, &sample).unwrap();
            let index = VectorIndex::from_backend_config(64, &config).unwrap();
            let node = NodeServiceImpl::new(
                Some(index),
                NodeConfig {
                    index_path: Some(dir.join("shard.vector")),
                    analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
                    wal: true,
                    ..Default::default()
                },
            );
            let handle = tokio::runtime::Handle::current();
            let spec = body_spec();
            let texts = ["seed", "first tail", "second tail", "late tail"];
            let analyzed = texts
                .iter()
                .map(|text| {
                    handle
                        .block_on(crate::analyzer::analyze_document(
                            NATIVE_ANALYSIS_BACKEND,
                            text,
                            Some(&spec),
                        ))
                        .unwrap()
                })
                .collect::<Vec<_>>();
            let append = |i: usize| {
                let mut guard = node.state.write().unwrap();
                node.apply_document_locked(
                    &mut guard,
                    AddDocumentsRequest {
                        text: texts[i].into(),
                        analysis: Some(spec.clone()),
                        ..Default::default()
                    },
                    analyzed[i].clone(),
                    Some(sample[..64].to_vec()),
                    None,
                    &mut 0,
                    &mut 0,
                )
                .unwrap();
            };
            append(0);
            node.flush_index().unwrap();
            let preflight = node
                .preflight_at_row_boundary(&CompactShardRequest::default())
                .unwrap();
            let PreflightKind::Wal(pre) = preflight else {
                panic!("a WAL-backed shard preflights to the log path")
            };
            std::fs::create_dir_all(&pre.work_dir).unwrap();
            let mut calls = 0;
            let mut analyze = |docs: &[(
                &str,
                Option<&crate::pb::AnalysisSpec>,
                crate::analyzer::SessionLayers,
            )]| {
                assert!(
                    node.state.try_read().is_ok(),
                    "analysis ran while the live shard was write-locked"
                );
                calls += 1;
                // Build, catch-up, and the first cutover preparation each
                // receive another committed row. The final fence must retry.
                if always_advance || calls <= 3 {
                    append(calls.min(3));
                }
                handle
                    .block_on(crate::analyzer::analyze_batch_streams(
                        NATIVE_ANALYSIS_BACKEND,
                        docs,
                        2,
                    ))
                    .map_err(|error| error.to_string())
            };
            let result = node.build_and_cut_over(&pre, 256, &mut analyze);
            let expected = if always_advance {
                let error = result.unwrap_err();
                assert_eq!(error.code(), tonic::Code::ResourceExhausted);
                assert!(error.message().contains("writes outpace compaction"));
                assert_eq!(calls, 2 + CUTOVER_RETRIES);
                let live = node.state.read().unwrap();
                assert_eq!(live.wal.as_ref().unwrap().generation(), 0);
                assert!(!marker_path(&pre.index_path).exists());
                (0..=calls).map(|i| texts[i.min(3)]).collect::<Vec<_>>()
            } else {
                let response = result.unwrap();
                assert_eq!(calls, 4);
                assert_eq!(response.rows_after, 4);
                assert_eq!(response.locked_tail_records, 0);
                texts.to_vec()
            };
            let fetched = handle
                .block_on(crate::pb::node_service_server::NodeService::get_documents(
                    &node,
                    tonic::Request::new(crate::pb::GetDocumentsRequest {
                        doc_ids: (0..expected.len() as u64).collect(),
                    }),
                ))
                .unwrap()
                .into_inner();
            let actual = fetched
                .documents
                .into_iter()
                .map(|doc| doc.text)
                .collect::<Vec<_>>();
            assert_eq!(actual, expected);
            drop(node);
            std::fs::remove_dir_all(dir).unwrap();
        })
        .await
        .unwrap();
    }

    /// A bound, WAL-less, segmented catalog served in-process: the shape
    /// the children of a re-placement split come in
    /// (docs/replay-from-segments.md).
    fn walless_fixture(label: &str, rows: usize) -> (PathBuf, NodeServiceImpl) {
        let nonce = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!(
            "psearch-walless-{label}-{}-{nonce}",
            std::process::id()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let index_path = dir.join("shard.tv");
        let plan = crate::mapping::derive_plan(
            include_bytes!("../tests/fixtures/vector-binding/descriptor.bin"),
            "vector_binding.Named",
        )
        .unwrap();
        let binding = crate::postings::StoredBinding {
            plan_fingerprint: plan.fingerprint,
            body_path: "body".into(),
            vector_binding: plan.vector_binding.unwrap().encode_to_vec(),
            ..Default::default()
        };
        SegmentCatalog::open(crate::node::segments_root(&index_path))
            .unwrap()
            .publish_binding(&binding)
            .unwrap();
        let node = NodeServiceImpl::open(
            NodeConfig {
                index_path: Some(index_path.clone()),
                analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
                wal: false,
                layout: crate::node::Layout::Segments,
                integer_fields: vec!["num".into()],
                ..Default::default()
            },
            None,
            false,
        )
        .unwrap();
        let sample = crate::harness::unit_vectors(rows.max(64), 64, 7);
        let config =
            VectorIndex::fit_backend_config(EMBEDDED_TURBOVEC, 64, 4, &sample[..64 * 64]).unwrap();
        {
            let mut guard = crate::node::write_shard(&node.state);
            let index = VectorIndex::from_backend_config(64, &config).unwrap();
            guard.index = Some(NodeServiceImpl::adopt_layout(guard.bm25.as_ref(), index).unwrap());
        }
        let spec = body_spec();
        for i in 0..rows {
            let text = format!("row{i} common {}", ["red", "green", "blue"][i % 3]);
            let analyzed = crate::analyzer::analyze_document_native(&text, Some(&spec)).unwrap();
            let mut guard = crate::node::write_shard(&node.state);
            node.apply_document_locked(
                &mut guard,
                AddDocumentsRequest {
                    text,
                    analysis: Some(spec.clone()),
                    integers: vec![crate::pb::IntegerValue {
                        field: "num".into(),
                        value: (i / 3) as i64,
                    }],
                    position_fields: Vec::new(),
                    ..Default::default()
                },
                analyzed,
                Some(sample[i * 64..(i + 1) * 64].to_vec()),
                None,
                &mut 0,
                &mut 0,
            )
            .unwrap();
        }
        node.flush_index().unwrap();
        (index_path, node)
    }

    /// The texts the shard serves, in label order.
    fn served_texts(node: &NodeServiceImpl) -> Vec<String> {
        let guard = crate::node::read_shard(&node.state);
        let Some(Bm25Shard::Segmented(shard)) = guard.bm25.as_ref() else {
            panic!("a segmented fixture")
        };
        let rows = crate::node::physical_rows(&guard);
        (0..rows)
            .filter(|&row| !guard.live_docs.is_deleted(row as usize))
            .map(|row| {
                shard
                    .text(row as u32)
                    .unwrap_or_else(|| panic!("row {row} has no text"))
            })
            .collect()
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn walless_compaction_catches_up_writes_and_deletes_made_after_the_build() {
        let (index_path, node) = walless_fixture("tail", 120);
        // A tombstone the build must reclaim.
        {
            let mut guard = crate::node::write_shard(&node.state);
            node.delete_documents_locked(&mut guard, &[3], None)
                .unwrap();
        }
        // After the outputs are staged and before the tail catch-up, one
        // more row lands in the tail and one sealed row is deleted. The
        // cutover must carry the row and the tombstone.
        segment_staging_hook::install(index_path.clone(), |node| {
            let spec = body_spec();
            let text = "late arrival common".to_string();
            let analyzed = crate::analyzer::analyze_document_native(&text, Some(&spec)).unwrap();
            let mut guard = crate::node::write_shard(&node.state);
            node.apply_document_locked(
                &mut guard,
                AddDocumentsRequest {
                    text,
                    analysis: Some(spec),
                    integers: vec![crate::pb::IntegerValue {
                        field: "num".into(),
                        value: 10_000,
                    }],
                    ..Default::default()
                },
                analyzed,
                Some(crate::harness::unit_vectors(1, 64, 99)),
                None,
                &mut 0,
                &mut 0,
            )
            .map_err(|status| status.message().to_string())?;
            let deleted = node
                .delete_documents_locked(&mut guard, &[5], None)
                .map_err(|status| status.message().to_string())?;
            assert_eq!(deleted.deleted, 1);
            Ok(())
        });
        let worker = node.clone();
        let response = tokio::task::spawn_blocking(move || {
            worker.compact_shard(&CompactShardRequest {
                partition_column: "num".into(),
                tail_bound: 16,
                ..Default::default()
            })
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(response.rows_before, 120);
        assert_eq!(response.tombstones_reclaimed, 1);
        assert_eq!(
            response.tail_records_applied, 1,
            "the late row was caught up"
        );
        // The dense partitions plus the caught-up row; the tombstone the
        // hook wrote is a physical row the overlay marks.
        assert_eq!(response.rows_after, 120);
        let texts = served_texts(&node);
        assert_eq!(texts.len(), 119);
        assert_eq!(
            texts.iter().filter(|t| *t == "late arrival common").count(),
            1,
            "the late row appears exactly once"
        );
        for gone in ["row3 common red", "row5 common blue"] {
            assert!(!texts.iter().any(|t| t == gone), "{gone} is tombstoned");
        }
        assert_eq!(texts.iter().filter(|t| *t == "row0 common red").count(), 1);
        drop(node);
        std::fs::remove_dir_all(index_path.parent().unwrap()).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn an_interrupted_walless_build_leaves_the_serving_catalog_untouched() {
        let (index_path, node) = walless_fixture("interrupted", 120);
        let manifest_before = std::fs::read(SegmentCatalog::manifest_path(
            &crate::node::segments_root(&index_path),
        ))
        .unwrap();
        segment_staging_hook::install(index_path.clone(), |_| Err("injected kill".to_string()));
        let worker = node.clone();
        let status = tokio::task::spawn_blocking(move || {
            worker.compact_shard(&CompactShardRequest {
                partition_column: "num".into(),
                tail_bound: 16,
                ..Default::default()
            })
        })
        .await
        .unwrap()
        .unwrap_err();
        assert!(status.message().contains("injected kill"), "{status}");
        // The serving catalog is untouched, and no staged output survived.
        assert_eq!(served_texts(&node).len(), 120);
        assert_eq!(
            std::fs::read(SegmentCatalog::manifest_path(&crate::node::segments_root(
                &index_path
            )))
            .unwrap(),
            manifest_before
        );
        let segments_dir = crate::node::segments_root(&index_path).join("segments");
        assert!(
            !std::fs::read_dir(&segments_dir).unwrap().any(|entry| entry
                .unwrap()
                .file_name()
                .to_string_lossy()
                .starts_with("cmp-")),
            "the failed build's staging was removed"
        );
        // A retry from clean staging succeeds, serves the same rows, and
        // writes byte-identical outputs to an uninterrupted run's.
        std::fs::remove_dir_all(default_work_dir(&index_path)).unwrap();
        let worker = node.clone();
        let response = tokio::task::spawn_blocking(move || {
            worker.compact_shard(&CompactShardRequest {
                partition_column: "num".into(),
                tail_bound: 16,
                ..Default::default()
            })
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(response.rows_before, 120);
        assert_eq!(response.rows_after, 120);
        assert_eq!(served_texts(&node).len(), 120);
        let (clean_path, clean_node) = walless_fixture("uninterrupted", 120);
        let clean = clean_node.clone();
        let clean_response = tokio::task::spawn_blocking(move || {
            clean.compact_shard(&CompactShardRequest {
                partition_column: "num".into(),
                tail_bound: 16,
                ..Default::default()
            })
        })
        .await
        .unwrap()
        .unwrap();
        assert_eq!(clean_response.rows_after, 120);
        let bytes_of = |index_path: &Path| {
            let root = crate::node::segments_root(index_path);
            let mut out = BTreeMap::new();
            let mut stack = vec![root.clone()];
            while let Some(dir) = stack.pop() {
                for entry in std::fs::read_dir(&dir).unwrap() {
                    let entry = entry.unwrap();
                    if entry.file_type().unwrap().is_dir() {
                        stack.push(entry.path());
                    } else {
                        let relative = entry
                            .path()
                            .strip_prefix(&root)
                            .unwrap()
                            .to_string_lossy()
                            .into_owned();
                        out.insert(relative, std::fs::read(entry.path()).unwrap());
                    }
                }
            }
            out
        };
        assert_eq!(bytes_of(&index_path), bytes_of(&clean_path));
        drop(node);
        drop(clean_node);
        std::fs::remove_dir_all(index_path.parent().unwrap()).unwrap();
        std::fs::remove_dir_all(clean_path.parent().unwrap()).unwrap();
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn walless_compaction_under_a_changed_declaration_refuses_naming_both() {
        use crate::pb::{DerivedColumn, DerivedColumns, DerivedDisclosure, MaterializeKind};
        let declaration = |name: &str| {
            Arc::new(
                crate::derived::Declaration::compile(&DerivedColumns {
                    columns: vec![DerivedColumn {
                        name: name.into(),
                        expression: "num + 1".into(),
                        kind: MaterializeKind::I64 as i32,
                        disclosure: DerivedDisclosure::Inputs as i32,
                    }],
                })
                .unwrap(),
            )
        };
        let (index_path, mut node) = {
            let nonce = std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos();
            let dir = std::env::temp_dir().join(format!(
                "psearch-walless-derived-{}-{nonce}",
                std::process::id()
            ));
            std::fs::create_dir_all(&dir).unwrap();
            let index_path = dir.join("shard.tv");
            let plan = crate::mapping::derive_plan(
                include_bytes!("../tests/fixtures/vector-binding/descriptor.bin"),
                "vector_binding.Named",
            )
            .unwrap();
            let binding = crate::postings::StoredBinding {
                plan_fingerprint: plan.fingerprint,
                body_path: "body".into(),
                vector_binding: plan.vector_binding.unwrap().encode_to_vec(),
                ..Default::default()
            };
            SegmentCatalog::open(crate::node::segments_root(&index_path))
                .unwrap()
                .publish_binding(&binding)
                .unwrap();
            let node = NodeServiceImpl::open(
                NodeConfig {
                    index_path: Some(index_path.clone()),
                    analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
                    wal: false,
                    layout: crate::node::Layout::Segments,
                    integer_fields: vec!["num".into()],
                    derived: Some(declaration("num_d")),
                    ..Default::default()
                },
                None,
                false,
            )
            .unwrap();
            (index_path, node)
        };
        let spec = body_spec();
        for i in 0..30 {
            let text = format!("row{i} common");
            let analyzed = crate::analyzer::analyze_document_native(&text, Some(&spec)).unwrap();
            let mut guard = crate::node::write_shard(&node.state);
            node.apply_document_locked(
                &mut guard,
                AddDocumentsRequest {
                    text,
                    analysis: Some(spec.clone()),
                    integers: vec![crate::pb::IntegerValue {
                        field: "num".into(),
                        value: i as i64,
                    }],
                    ..Default::default()
                },
                analyzed,
                None,
                None,
                &mut 0,
                &mut 0,
            )
            .unwrap();
        }
        node.flush_index().unwrap();
        // The node now declares a changed declaration; the catalog's rows
        // carry the old one's values. The refusal names both.
        node.config.derived = Some(declaration("num_e"));
        let status = node
            .compact_shard(&CompactShardRequest {
                partition_column: "num".into(),
                tail_bound: 16,
                ..Default::default()
            })
            .unwrap_err();
        let message = status.message();
        assert!(
            message.contains("was written under derived-column declaration"),
            "{message}"
        );
        assert!(
            message.contains(&declaration("num_d").fingerprint().to_string()),
            "{message}"
        );
        assert!(
            message.contains(&declaration("num_e").fingerprint().to_string()),
            "{message}"
        );
        drop(node);
        std::fs::remove_dir_all(index_path.parent().unwrap()).unwrap();
    }
}
