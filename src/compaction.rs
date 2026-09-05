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
/// faster than the shadow applies it.
const MAX_TAIL_PASSES: u64 = 10_000;
/// Cutover attempts before refusing writes that keep advancing the WAL
/// during preparation.
const CUTOVER_RETRIES: usize = 16;
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
    eprintln!(
        "compaction: {} names a cutover to WAL generation {} that never reached its closing \
         flush; rolling back to generation {}",
        path.display(),
        marker.new_wal_generation,
        marker.old_wal_generation
    );
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
    let new_gen = wal::gen_dir(&wal::wal_dir(index_path), marker.new_wal_generation);
    if new_gen.exists() {
        must(
            "remove the rewritten WAL generation",
            std::fs::remove_dir_all(&new_gen),
        );
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
}

/// A segment-layout tail caught between the two calls of a legacy append:
/// the documents count and the vectors count differ.
struct MidRow {
    documents: usize,
    vectors: usize,
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
    /// in-process control-plane worker. Needs a Tokio runtime on the
    /// calling thread's context for the analysis sessions
    /// (`spawn_blocking` threads have one).
    pub fn compact_shard(
        &self,
        request: &CompactShardRequest,
    ) -> Result<CompactShardResponse, Status> {
        let handle = tokio::runtime::Handle::try_current().map_err(|_| {
            Status::failed_precondition(
                "compaction analyzes documents through the node's analysis backend and needs a \
                 Tokio runtime context",
            )
        })?;
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
                ..Default::default()
            });
        }
        // The prefix through the cutoff goes to disk before the replay
        // reads it; writes keep landing on the live shard meanwhile.
        {
            let mut guard = self.state.write().expect("shard state lock poisoned");
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

    /// Preflight at a row boundary. The cut is the log's high-water mark,
    /// read under the shard lock; a legacy two-RPC append (AddDocuments,
    /// then AddVectors) can be halfway through at that instant, and on
    /// the segment layout the replay through such a cut builds a bucket
    /// with one document more than vectors, which the layout refuses to
    /// seal. The row completes with the client's next call, so this waits
    /// that out for a bounded time, then refuses naming the counts.
    fn preflight_at_row_boundary(
        &self,
        request: &CompactShardRequest,
    ) -> Result<Preflight, Status> {
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
    fn preflight(
        &self,
        request: &CompactShardRequest,
    ) -> Result<Result<Preflight, MidRow>, Status> {
        let index_path = self.config.index_path.clone().ok_or_else(|| {
            Status::failed_precondition(
                "compaction needs a persisted shard (index_path); an in-memory shard has no log \
                 to compact",
            )
        })?;
        let guard = self.state.read().expect("shard state lock poisoned");
        if guard.pending_compaction.is_some() {
            return Err(Status::failed_precondition(
                "a compaction cutover is pending its closing flush on this shard; call Flush",
            ));
        }
        let wal = guard.wal.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "this shard has no WAL; compaction replays the log, so a shard without one can \
                 only be rebuilt from source",
            )
        })?;
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
        Ok(Ok(Preflight {
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
            crate::reshard::compact_log(
                &pre.gen_dir,
                pre.cutoff_clock,
                &build_dir,
                pre.segmented,
                names.as_deref(),
                pins.as_deref(),
                analyze,
                &mut sink,
            )
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
                    let guard = self.state.read().expect("shard state lock poisoned");
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
            self.check_shadow_fingerprints(pre, &shard)?;
            Some(shard)
        } else {
            // No documents through the cutoff. A document the tail brings
            // goes into a heap store rather than a spill builder, whose
            // directory would live under the generation directory the
            // cutover renames away.
            Some(Bm25Shard::Building(
                crate::node::heap_store(&self.config).map_err(Status::failed_precondition)?,
            ))
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
            let guard = self.state.read().expect("shard state lock poisoned");
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
                ..Default::default()
            };
            let catalog = SegmentCatalog::open_staged(&root, manifest, self.config.vector_load())
                .map_err(|e| Status::internal(format!("open the compacted set: {e}")))?;
            let tail =
                crate::node::heap_store(&self.config).map_err(Status::failed_precondition)?;
            let shard = crate::segmented::SegmentedShard::open_catalog(catalog, tail)
                .map_err(|e| Status::internal(format!("open the compacted shard: {e}")))?;
            let set = shard.snapshot().clone();
            let bm25 = Bm25Shard::Segmented(shard);
            self.check_shadow_fingerprints(pre, &bm25)?;
            let mut index = None;
            let mut exact_vectors = None;
            if let Some(empty) = self.empty_configured_index(pre)? {
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
                let parts: Vec<PathBuf> = (0..set.len())
                    .filter(|i| set.vector(*i).is_some())
                    .map(|i| {
                        SegmentCatalog::segment_dir(&root, &set.metadata(i).segment_id)
                            .join(&set.metadata(i).exact_vectors.file)
                    })
                    .collect();
                let part_refs: Vec<&Path> = parts.iter().map(PathBuf::as_path).collect();
                let exact_path = pre.work_dir.join("vectors.exact");
                exact_vectors = Some(
                    ExactVectorStore::write_concatenated(dim, &part_refs, &exact_path).map_err(
                        |e| Status::internal(format!("assemble {}: {e}", exact_path.display())),
                    )?,
                );
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
    fn check_shadow_fingerprints(&self, pre: &Preflight, built: &Bm25Shard) -> Result<(), Status> {
        let Some((names, fingerprints)) = pre.fields.as_ref() else {
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
                    let (doc, analyzed) = self.materialize_document(doc, analyzed)?;
                    let mut added = 0u64;
                    let mut first = 0u64;
                    self.apply_document_locked(
                        &mut shadow.state,
                        doc,
                        analyzed,
                        None,
                        keys.next(),
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
        let mut guard = self.state.write().expect("shard state lock poisoned");
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
            let mut guard = self.state.write().expect("shard state lock poisoned");
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
            shadow.state.stats_epoch = guard.stats_epoch + 1;
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
                shard
                    .catalog()
                    .commit_current(live_epoch.max(shadow.epoch_at_open) + 1)
                    .map_err(|e| Status::internal(format!("publish the compacted set: {e}")))?;
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
            let pre = node
                .preflight_at_row_boundary(&CompactShardRequest::default())
                .unwrap();
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
}
