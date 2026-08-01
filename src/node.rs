//! Shard-owner side: serves [`NodeService`] over one turbovec index.
//!
//! The shard is a small state machine behind a write lock:
//!
//! ```text
//! empty (no index) ──SetCalibration──▶ seeded empty index ──AddVectors──▶ live index
//!       │                                    │
//!       └──AddVectors(dim=..)──▶ unseeded index (calibration fitted from first batch)
//! ```
//!
//! Calibration locks for the index's lifetime (turbovec's own rule):
//! `SetCalibration` is only ever accepted on an empty shard. Adds hold the
//! write lock on the blocking pool; searches hold the read lock for the
//! duration of their chunked scan, so a search never observes a
//! half-applied batch.

use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};

use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};
use turbovec::TurboQuantIndex;

use crate::bm25::{self, Bm25Params};
use crate::chunked::{chunked_topk_collapsed, 
    chunked_topk, chunked_topk_batch, BatchQuery, ChunkHit, ScanStats, DEFAULT_CHUNK_BLOCKS,
};
use crate::fusion::{self, Leg};
use crate::pb::node_service_server::{NodeService, NodeServiceServer};
use crate::pb::{
    search_shard_request, search_shard_response, snapshot_chunk, stream_search_request,
    stream_search_response, AddDocumentsRequest,
    AddDocumentsResponse, AddVectorsRequest, AddVectorsResponse, Bm25Hit, Bm25QueryRequest,
    Bm25QueryResponse, Bm25RescoreRequest, Bm25RescoreResponse, FloorUpdate, FlushRequest,
    FlushResponse, GetCalibrationRequest, GetCalibrationResponse, GetDocumentsRequest,
    GetDocumentsResponse, HealthRequest, HealthResponse, HybridLegHit, HybridShardRequest,
    HybridShardResponse,
    InstallSnapshotResponse, OffsetSpan, RawLegHit, ScoredHit, SearchShardDone,
    SearchShardRequest, SearchShardResponse, SetCalibrationRequest, SetCalibrationResponse,
    ShardLegsRequest, ShardLegsResponse, ShardScanStats, SnapshotChunk, SnapshotManifest,
    StartShardSearch, StoredDocument, StreamSearchBatch, StreamSearchRequest,
    StreamSearchResponse, StreamSearchSummary, TermOccurrences, TermStatsRequest,
    TermStatsResponse,
};
use crate::pb::wal::{wal_record, FlushMarker, LoggedAddDocuments, LoggedAddVectors, SnapshotMarker};
use crate::postings::{Bm25Index, Bm25Reader, Bm25Store, SpillBuilder};
use crate::wal::{self, WalWriter};

/// How a node scans and whether it participates in floor sharing.
#[derive(Debug, Clone)]
pub struct NodeConfig {
    /// Added to every local slot to produce the global vector id reported
    /// in [`SearchShardDone`]. Shards must have disjoint ranges.
    pub slot_offset: u64,
    /// Chunk size in SIMD blocks for the scan (see [`chunked_topk`]).
    pub chunk_blocks: usize,
    /// When false, the node still scans in chunks but ignores coordinator
    /// floor updates and does not publish its own floor — the
    /// "sharing disabled" baseline for A/B benchmarking.
    pub share_floors: bool,
    /// When false, BM25 scoring always takes the exhaustive path
    /// (`top_k`) even on v5 shards with a skip run — the "block-max
    /// disabled" baseline for A/B benchmarking. Results are identical
    /// either way; only the cost changes.
    pub block_max: bool,
    /// Minimum improvement over the last PUBLISHED floor before the next
    /// one goes on the wire. 0.0 publishes every raise (the historical
    /// behavior); a small positive delta trades a sliver of pruning
    /// reactivity for far fewer floor messages on real networks.
    pub floor_delta: f32,
    /// Bit width used when `AddVectors` constructs an index from scratch
    /// (no loaded index, no seeded calibration).
    pub bit_width: usize,
    /// Persistence target for `Flush` / save-on-shutdown. `None` makes the
    /// shard purely in-memory (flush is a no-op).
    pub index_path: Option<PathBuf>,
    /// Analysis sidecar address (`http://host:port`) for AddDocuments.
    /// `None` makes AddDocuments fail UNAVAILABLE.
    pub analysis_addr: Option<String>,
    /// Keep a write-ahead log at `<index path>.wal/` (see [`crate::wal`]).
    /// Requires `index_path`; the config layer defaults this on for
    /// persisted shards and off for demo shards.
    pub wal: bool,
    /// Number of WAL hash buckets (`bucket-NNN.wal` files per
    /// generation). Fixed at WAL creation; a resumed log keeps its own.
    pub wal_buckets: u32,
    /// Coalesce concurrent shard scans into batched kernel calls (up to
    /// [`MAX_COALESCE`] queries share each pass over the packed codes —
    /// the scan is bandwidth-bound, so batched queries ride the same
    /// memory traffic). `false` runs one scan per RPC — the A/B
    /// baseline; results are identical either way.
    pub coalesce: bool,
    /// Concurrent batched scans (blocking threads). 0 sizes from the
    /// machine: half the available cores, at least one.
    pub scan_parallel: usize,
}

impl Default for NodeConfig {
    fn default() -> Self {
        Self {
            slot_offset: 0,
            chunk_blocks: DEFAULT_CHUNK_BLOCKS,
            share_floors: true,
            block_max: true,
            floor_delta: 0.0,
            bit_width: 4,
            index_path: None,
            analysis_addr: None,
            wal: false,
            wal_buckets: 64,
            coalesce: true,
            scan_parallel: 0,
        }
    }
}

/// Raw leg hits as `(global_doc_id, raw_score)`, score-descending.
type RawLeg = Vec<(u64, f64)>;

/// The BM25 half's two storage shapes: the heap builder used during
/// ingest, and the disk-resident mmap reader used after Flush and on
/// startup. Once resident, a shard holds no postings or document texts
/// in heap — only the small per-doc tables.
pub enum Bm25Shard {
    /// Heap builder (small or append ingests; searchable mid-build).
    Building(Bm25Store),
    /// Disk-spilling bulk builder (fresh persisted shards): bounded heap,
    /// NOT searchable until flushed.
    Spilling(SpillBuilder),
    /// Disk-resident mmap reader over the v3 file.
    Resident(Bm25Reader),
}

impl Bm25Shard {
    /// The searchable read surface; `None` while bulk-building (a spill
    /// builder cannot answer term lookups without scanning every run).
    fn as_index(&self) -> Option<&dyn Bm25Index> {
        match self {
            Bm25Shard::Building(s) => Some(s),
            Bm25Shard::Spilling(_) => None,
            Bm25Shard::Resident(r) => Some(r),
        }
    }

    fn next_doc_id(&self) -> u32 {
        match self {
            Bm25Shard::Building(s) => s.next_doc_id(),
            Bm25Shard::Spilling(s) => s.next_doc_id(),
            Bm25Shard::Resident(r) => r.next_doc_id(),
        }
    }

    fn doc_count(&self) -> u64 {
        match self {
            Bm25Shard::Building(s) => s.doc_count(),
            Bm25Shard::Spilling(s) => s.doc_count(),
            Bm25Shard::Resident(r) => Bm25Index::doc_count(r),
        }
    }

    /// Open a `.bm25` path in the right shape: v3 files map
    /// disk-resident; older formats load into the heap builder (and are
    /// upgraded to v3 on the next flush).
    pub fn open(path: &std::path::Path) -> std::io::Result<Self> {
        let mut magic = [0u8; 8];
        std::fs::File::open(path)?.read_exact(&mut magic)?;
        if &magic == b"TVBM2503" || &magic == b"TVBM2504" {
            Ok(Bm25Shard::Resident(Bm25Reader::open(path)?))
        } else {
            Ok(Bm25Shard::Building(Bm25Store::load(path)?))
        }
    }
}

/// The shard's two indexes behind one lock: the turbovec vector index and
/// the BM25 postings store. Either may be absent (vector-only shards,
/// docs-only shards, from-scratch shards).
#[derive(Default)]
struct ShardState {
    index: Option<TurboQuantIndex>,
    bm25: Option<Bm25Shard>,
    /// The active snapshot generation directory, when the shard's files
    /// came from (or were replaced by) an `InstallSnapshot` image.
    /// `Flush` and the AddDocuments reload path read/write THERE, never
    /// the legacy `<index path>` layout, so the two never split-brain.
    generation: Option<PathBuf>,
    /// The write-ahead log (`<index path>.wal/`), behind the same lock as
    /// the index it precedes. `None` when the shard runs without one.
    wal: Option<WalWriter>,
    /// Cached slot -> parent map for collapse scans (lineage opinion_id
    /// per slot). Self-validating: rebuilt whenever its length disagrees
    /// with the index, cleared on snapshot install.
    parents: Option<std::sync::Arc<Vec<u64>>>,
}

/// The persistence path of a shard's BM25 store: `<index path>.bm25`.
pub fn bm25_sidecar_path(index_path: &std::path::Path) -> PathBuf {
    let mut p = index_path.as_os_str().to_owned();
    p.push(".bm25");
    PathBuf::from(p)
}

/// Snapshot generation layout, next to the shard's configured index path:
/// `<index path>.snap/` is the active generation holding the installed
/// image as `index.tv` + `index.tv.bm25`. Because BOTH files live inside
/// one directory, installing them is a single directory rename — which is
/// atomic, so the pair can never tear.
pub fn generation_dir(index_path: &Path) -> PathBuf {
    let mut p = index_path.as_os_str().to_owned();
    p.push(".snap");
    PathBuf::from(p)
}

/// The image paths inside a generation directory.
pub fn generation_tv(dir: &Path) -> PathBuf {
    dir.join("index.tv")
}
/// The BM25 sidecar path inside a generation directory.
pub fn generation_bm25(dir: &Path) -> PathBuf {
    dir.join("index.tv.bm25")
}

/// Receive staging (`<index path>.snap-tmp/`) and swap-out
/// (`<index path>.snap-old/`) directories for the generation swap.
fn generation_tmp_dir(index_path: &Path) -> PathBuf {
    let mut p = index_path.as_os_str().to_owned();
    p.push(".snap-tmp");
    PathBuf::from(p)
}
fn generation_old_dir(index_path: &Path) -> PathBuf {
    let mut p = index_path.as_os_str().to_owned();
    p.push(".snap-old");
    PathBuf::from(p)
}

/// Where the shard's files live: the active snapshot generation when one
/// was installed, else the legacy `<index path>` (+`.bm25`) layout.
/// Returns `(index, bm25)` paths.
fn storage_paths(index_path: &Path, generation: Option<&PathBuf>) -> (PathBuf, PathBuf) {
    match generation {
        Some(dir) => (generation_tv(dir), generation_bm25(dir)),
        None => (index_path.to_path_buf(), bm25_sidecar_path(index_path)),
    }
}

/// Crash recovery for the generation swap, and the startup answer to
/// "does this shard have an installed snapshot?". Every interleave of the
/// two swap renames has a defined outcome:
///
/// - `snap-old` present, `snap` missing: crashed between the renames —
///   the previous generation is whole, rename it back.
/// - both present: crashed after the second rename — the new generation
///   is live, delete the old one.
/// - a stray `snap-tmp` is always deleted: only a COMPLETE staging dir is
///   ever renamed into place, so a leftover one is unreceived garbage.
///
/// Returns the active generation directory when it holds an index.
pub fn recover_generation(index_path: &Path) -> Option<PathBuf> {
    let snap = generation_dir(index_path);
    let old = generation_old_dir(index_path);
    let tmp = generation_tmp_dir(index_path);
    let _ = std::fs::remove_dir_all(&tmp);
    if old.exists() {
        if snap.exists() {
            let _ = std::fs::remove_dir_all(&old);
        } else {
            let _ = std::fs::rename(&old, &snap);
        }
    }
    generation_tv(&snap).exists().then_some(snap)
}

/// The manifest describing a shard's current shape: calibration and dim
/// from the loaded index when it has them (a seeded or fitted shard),
/// zeros otherwise — an empty shard completes the manifest lazily, via
/// `SetCalibration` or its first batch, until calibration locks.
///
/// `preexisting` is the (vectors, documents) the shard already holds
/// that this generation's log will NOT contain — the installed image on
/// a snapshot rotation, or the whole shard when logging is enabled on an
/// already-populated index. Nonzero preexisting state marks the log as
/// partial history, which the reshard tool refuses (a log-only replay
/// would silently drop that state).
fn wal_manifest(
    index: Option<&TurboQuantIndex>,
    config: &NodeConfig,
    generation: u64,
    preexisting: (u64, u64),
) -> wal::WalManifest {
    let (dim, bit_width, shift, scale) = match index {
        Some(index) => {
            let (shift, scale) = index.calibration().unwrap_or((&[], &[]));
            (
                index.dim_opt().unwrap_or(0) as u32,
                index.bit_width() as u32,
                shift.to_vec(),
                scale.to_vec(),
            )
        }
        None => (0, config.bit_width as u32, Vec::new(), Vec::new()),
    };
    wal::WalManifest {
        dim,
        bit_width,
        calibration_shift: shift,
        calibration_scale: scale,
        slot_offset: config.slot_offset,
        generation,
        bucket_bits: config.wal_buckets.trailing_zeros(),
        bucket_count: config.wal_buckets,
        preexisting_vectors: preexisting.0,
        preexisting_documents: preexisting.1,
        format_version: wal::FORMAT_VERSION,
    }
}

/// The persisted document tip of a shard: `next_doc_id` of the on-disk
/// BM25 sidecar (generation-aware), 0 when none exists. Opened read-only
/// and dropped — the serving copy is attached separately (`with_bm25`);
/// this exists so WAL reconciliation can know the applied tip without
/// depending on attachment order.
fn persisted_doc_tip(index_path: &Path) -> u64 {
    let generation = recover_generation(index_path);
    let (_, bm25_path) = storage_paths(index_path, generation.as_ref());
    if !bm25_path.exists() {
        return 0;
    }
    match Bm25Shard::open(&bm25_path) {
        Ok(store) => u64::from(store.next_doc_id()),
        // Panic, like the rest of the WAL open path: guessing a tip of 0
        // would truncate legitimate document records, and the binary
        // would refuse to serve this sidecar at attach time anyway.
        Err(e) => panic!(
            "wal reconciliation: cannot read {}: {e}",
            bm25_path.display()
        ),
    }
}

/// Open the shard's WAL at `<index path>.wal/`: resume the newest
/// generation after a restart (truncating any torn tails, continuing the
/// per-file sequences) or start generation 0. A resumed log keeps its own
/// bucket count — the configured `--wal-buckets` only applies at WAL
/// creation. Panics on IO failure, like the BM25 load path in the
/// binary — a shard that cannot log must not silently run unlogged.
///
/// Resume reconciles the log against the applied state first: records at
/// or above the applied tip (`slot_offset + max(vector tip, document
/// tip)`) are truncated, because appends are buffered and a crash can
/// leave the on-disk log ahead of the on-disk indexes — the reopening
/// shard would otherwise re-assign ids the log already holds. The
/// dropped records were never durable-acked (Flush is the durability
/// point).
///
/// A log CREATED over an already-populated shard records the shard's
/// current contents as `preexisting_*` in its manifest: it can serve and
/// recover, but it is not full history and cannot drive a reshard.
fn open_wal(index: Option<&TurboQuantIndex>, config: &NodeConfig) -> Option<WalWriter> {
    if !config.wal {
        return None;
    }
    let index_path = config
        .index_path
        .as_ref()
        .expect("wal requires an index path");
    let vector_tip = index.map_or(0, |i| i.len() as u64);
    let doc_tip = persisted_doc_tip(index_path);
    let dir = wal::wal_dir(index_path);
    let result = match wal::latest_gen(&dir) {
        Ok(Some((_, gen))) => wal::read_manifest(&gen).and_then(|m| {
            let cutoff = config.slot_offset + vector_tip.max(doc_tip);
            let dropped = wal::truncate_records_at_or_above(&gen, cutoff)?;
            if dropped > 0 {
                eprintln!(
                    "wal: truncated {dropped} record(s) at or above applied tip {cutoff} in {} \
                     (buffered appends that outlived a crash; never durable-acked)",
                    gen.display()
                );
            }
            WalWriter::resume(&gen, m)
        }),
        Ok(None) => {
            if vector_tip > 0 || doc_tip > 0 {
                eprintln!(
                    "wal: shard already holds {vector_tip} vectors / {doc_tip} documents; the new \
                     log records them as preexisting — this shard can serve but cannot be \
                     resharded from this log (rebuild via InstallSnapshot for full history)"
                );
            }
            WalWriter::create(&dir, wal_manifest(index, config, 0, (vector_tip, doc_tip)))
        }
        Err(e) => Err(e),
    };
    let mut writer = result.unwrap_or_else(|e| panic!("open WAL at {}: {e}", dir.display()));
    if writer.manifest().bucket_count != config.wal_buckets {
        eprintln!(
            "wal: --wal-buckets={} ignored; the existing log at {} has bucket_count={}",
            config.wal_buckets,
            writer.dir().display(),
            writer.manifest().bucket_count
        );
    }
    // A resumed generation whose calibration never locked (no records
    // yet) still accepts manifest completion.
    writer.update_manifest(|m| {
        let fresh = wal_manifest(index, config, m.generation, (0, 0));
        if m.dim == 0 {
            m.dim = fresh.dim;
            m.bit_width = fresh.bit_width;
            m.slot_offset = fresh.slot_offset;
        }
        if m.calibration_shift.is_empty() {
            m.calibration_shift = fresh.calibration_shift;
            m.calibration_scale = fresh.calibration_scale;
        }
    });
    eprintln!("wal: logging to {}", writer.dir().display());
    Some(writer)
}

/// Log `op`, or degrade the shard to unlogged if the append fails.
///
/// The mutation was already applied when this runs (apply-then-log, see
/// [`WalWriter::append`]), so failing the client's request would report a
/// write that in fact happened. Instead the shard keeps serving and the
/// log is retired loudly: the generation directory is renamed `.broken`
/// (so the reshard tool and a restarting node cannot mistake it for
/// history) and the writer is dropped. Per the resharding policy, a
/// shard without a WAL serves fine but can only be rebuilt, never
/// resharded.
fn wal_append_or_degrade(wal_slot: &mut Option<WalWriter>, op: wal_record::Op) {
    let Some(wal) = wal_slot.as_mut() else { return };
    if let Err(e) = wal.append(op) {
        let dir = wal.dir().to_path_buf();
        let broken = dir.with_extension("broken");
        eprintln!(
            "wal: append to {} failed ({e}); retiring the log as {} — this shard continues \
             UNLOGGED and can no longer be resharded from its log (rebuild required)",
            dir.display(),
            broken.display()
        );
        *wal_slot = None;
        if let Err(e) = std::fs::rename(&dir, &broken) {
            eprintln!("wal: could not rename the broken generation: {e}");
        }
    }
}

/// The shard-owner gRPC service. Cheap to clone (state is shared).
#[derive(Clone)]
pub struct NodeServiceImpl {
    /// Locked shard state; see [`ShardState`].
    state: Arc<RwLock<ShardState>>,
    /// Single-writer gate for ingest streams. Two concurrent AddDocuments
    /// (or AddVectors) streams would interleave positional ids into one
    /// shard — every doc logged, none attributable — so the second stream
    /// is refused outright rather than merged.
    ingest_busy: Arc<std::sync::atomic::AtomicBool>,
    config: NodeConfig,
    /// Shared scan queue for coalesced searches; the scheduler task is
    /// spawned on first use (shared across service clones).
    scan_jobs: Arc<std::sync::OnceLock<mpsc::Sender<ScanJob>>>,
}

/// Kernel batch width: turbovec's multi-query scan scores up to four
/// queries per pass over each block, so batches beyond four stop
/// amortizing memory traffic.
const MAX_COALESCE: usize = 4;

static SCAN_BATCHES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static SCAN_BATCHED_JOBS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Process-wide coalescing telemetry: `(batches formed, jobs in them)`.
/// Jobs exceeding batches means multi-query batches actually formed —
/// the observable that coalescing engaged, used by tests and benchmarks.
pub fn scan_batch_counters() -> (u64, u64) {
    (
        SCAN_BATCHES.load(std::sync::atomic::Ordering::Relaxed),
        SCAN_BATCHED_JOBS.load(std::sync::atomic::Ordering::Relaxed),
    )
}

/// One shard scan queued for a batched kernel pass.
struct ScanJob {
    vector: Vec<f32>,
    k: usize,
    tie_complete: bool,
    /// Polled between chunks for the best coordinator-pushed floor
    /// (returns `None` when floor sharing is off or no floor arrived).
    external: Box<dyn FnMut() -> Option<f32> + Send>,
    /// Receives this query's k-th-best raises (the caller bakes in the
    /// share gate and delta filter).
    publish: Box<dyn FnMut(f32) + Send>,
    done: tokio::sync::oneshot::Sender<Result<(Vec<ChunkHit>, ScanStats), Status>>,
}

/// Batch former: one scan slot at a time per permit, and every job that
/// queued while all slots were busy coalesces into the next drain. Under
/// light load batches are singletons and scans run as parallel as before;
/// under heavy load freed slots pick up to [`MAX_COALESCE`] waiting
/// queries and score them in one pass over the packed codes.
async fn scan_scheduler(
    state: Arc<std::sync::RwLock<ShardState>>,
    chunk_blocks: usize,
    parallel: usize,
    mut jobs: mpsc::Receiver<ScanJob>,
) {
    let slots = Arc::new(tokio::sync::Semaphore::new(parallel.max(1)));
    loop {
        // A slot first, then the batch: the wait for a slot is exactly
        // when coalescable jobs accumulate.
        let permit = slots
            .clone()
            .acquire_owned()
            .await
            .expect("scan semaphore never closes");
        let Some(first) = jobs.recv().await else {
            break;
        };
        let mut batch = vec![first];
        while batch.len() < MAX_COALESCE {
            match jobs.try_recv() {
                Ok(job) => batch.push(job),
                Err(_) => break,
            }
        }
        SCAN_BATCHES.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        SCAN_BATCHED_JOBS.fetch_add(batch.len() as u64, std::sync::atomic::Ordering::Relaxed);
        let state = state.clone();
        tokio::task::spawn_blocking(move || {
            let _slot = permit;
            run_scan_batch(&state, chunk_blocks, batch);
        });
    }
}

/// Run one batched scan under the shard read lock and deliver every job's
/// result. Blocking-pool context.
fn run_scan_batch(
    state: &std::sync::RwLock<ShardState>,
    chunk_blocks: usize,
    batch: Vec<ScanJob>,
) {
    let guard = state.read().expect("shard state lock poisoned");
    let index = match guard.index.as_ref() {
        Some(index) => index,
        None => {
            for job in batch {
                let _ = job.done.send(Err(Status::failed_precondition(
                    "shard has no index yet (set calibration or add vectors)",
                )));
            }
            return;
        }
    };
    // Re-validate dimensions against the CURRENT index: the shard may have
    // been swapped (InstallSnapshot) between the RPC's validation and this
    // batch winning a slot.
    let dim = index.dim_opt();
    let mut specs: Vec<(Vec<f32>, usize, bool)> = Vec::with_capacity(batch.len());
    let mut externals: Vec<Box<dyn FnMut() -> Option<f32> + Send>> = Vec::new();
    let mut publishers: Vec<Box<dyn FnMut(f32) + Send>> = Vec::new();
    let mut dones = Vec::new();
    for job in batch {
        if Some(job.vector.len()) != dim {
            let _ = job.done.send(Err(Status::failed_precondition(format!(
                "query dim {} no longer matches the index",
                job.vector.len()
            ))));
            continue;
        }
        specs.push((job.vector, job.k, job.tie_complete));
        externals.push(job.external);
        publishers.push(job.publish);
        dones.push(job.done);
    }
    if dones.is_empty() {
        return;
    }
    let queries: Vec<BatchQuery> = specs
        .iter()
        .map(|(vector, k, keep_ties)| BatchQuery {
            vector,
            k: *k,
            keep_ties: *keep_ties,
        })
        .collect();
    let results = chunked_topk_batch(
        index,
        &queries,
        chunk_blocks,
        &mut |qi| (externals[qi])(),
        &mut |qi, floor| (publishers[qi])(floor),
    );
    for (done, result) in dones.into_iter().zip(results) {
        let _ = done.send(Ok(result));
    }
}

/// RAII release for the ingest gate.
struct IngestGuard(Arc<std::sync::atomic::AtomicBool>);

impl Drop for IngestGuard {
    fn drop(&mut self) {
        self.0.store(false, std::sync::atomic::Ordering::Release);
    }
}

impl NodeServiceImpl {
    /// Wrap an optional preloaded index in a node service.
    pub fn new(index: Option<TurboQuantIndex>, config: NodeConfig) -> Self {
        let wal = open_wal(index.as_ref(), &config);
        Self {
            state: Arc::new(RwLock::new(ShardState {
                index,
                bm25: None,
                generation: None,
                wal,
                parents: None,
            })),
            ingest_busy: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            config,
            scan_jobs: Arc::new(std::sync::OnceLock::new()),
        }
    }

    /// The shared scan queue, spawning the scheduler on first use (RPC
    /// handlers guarantee a runtime here).
    fn scan_queue(&self) -> mpsc::Sender<ScanJob> {
        self.scan_jobs
            .get_or_init(|| {
                let (tx, rx) = mpsc::channel(4096);
                let parallel = if self.config.scan_parallel > 0 {
                    self.config.scan_parallel
                } else {
                    std::thread::available_parallelism()
                        .map(|n| n.get())
                        .unwrap_or(2)
                        .div_ceil(2)
                };
                tokio::spawn(scan_scheduler(
                    self.state.clone(),
                    self.config.chunk_blocks,
                    parallel,
                    rx,
                ));
                tx
            })
            .clone()
    }

    /// The builder shape for a fresh ingest: persisted shards bulk-build
    /// through the disk spiller (bounded heap, not searchable until
    /// Flush); path-less demo shards build in heap.
    fn new_builder(&self, generation: Option<&PathBuf>) -> Result<Bm25Shard, Status> {
        match self.config.index_path.as_ref() {
            Some(p) => {
                let bm25_path = storage_paths(p, generation).1;
                let mut dir = bm25_path.as_os_str().to_owned();
                dir.push(".build");
                let dir = PathBuf::from(dir);
                SpillBuilder::create(&dir)
                    .map(Bm25Shard::Spilling)
                    .map_err(|e| Status::internal(format!("spill dir {}: {e}", dir.display())))
            }
            None => Ok(Bm25Shard::Building(Bm25Store::new())),
        }
    }

    /// Claim the single-writer ingest gate, or refuse the stream.
    fn claim_ingest(&self) -> Result<IngestGuard, Status> {
        use std::sync::atomic::Ordering;
        if self
            .ingest_busy
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return Err(Status::failed_precondition(
                "another ingest stream is active on this shard",
            ));
        }
        Ok(IngestGuard(self.ingest_busy.clone()))
    }

    /// Attach a preloaded BM25 shard (from `<index path>.bm25`).
    pub fn with_bm25(self, store: Option<Bm25Shard>) -> Self {
        self.state.write().expect("shard state lock poisoned").bm25 = store;
        self
    }

    /// Mark the shard as serving from a snapshot generation directory
    /// (startup found one via [`recover_generation`]): Flush and the
    /// AddDocuments reload path then read/write inside it.
    pub fn with_generation(self, dir: Option<PathBuf>) -> Self {
        self.state.write().expect("shard state lock poisoned").generation = dir;
        self
    }

    /// Build the tonic server for this service with explicit message size
    /// limits (see [`crate::MAX_MESSAGE_BYTES`]). tonic's 4 MiB default
    /// decoding cap is comfortably above even k=10000 shard responses
    /// (~160 KiB), but the limit is set explicitly so it never silently
    /// depends on a library default. NOTE: the cap also bounds AddVectors
    /// batch messages; clients should keep batches well under it.
    pub fn into_server(self, max_message_bytes: usize) -> NodeServiceServer<Self> {
        NodeServiceServer::new(self)
            .max_decoding_message_size(max_message_bytes)
            .max_encoding_message_size(max_message_bytes)
    }

    /// Validate an incoming `StartShardSearch` against the index shape.
    /// turbovec panics on wrong-dim or non-finite queries; the service
    /// turns both into `INVALID_ARGUMENT` before the scan starts.
    /// The slot -> parent map for collapse scans: lineage `opinion_id`
    /// per slot, or a high-bit-tagged global id for slots without
    /// lineage (self-parents; the tag keeps them disjoint from real
    /// opinion ids). Cached on the shard and rebuilt whenever the index
    /// length disagrees (append-only ingest makes length the only
    /// staleness signal; snapshot installs clear the cache explicitly).
    fn parent_map(
        state: &Arc<std::sync::RwLock<ShardState>>,
        slot_offset: u64,
        n: usize,
    ) -> Arc<Vec<u64>> {
        const SELF_PARENT_TAG: u64 = 1 << 63;
        {
            let guard = state.read().expect("shard state lock poisoned");
            if let Some(p) = guard.parents.as_ref() {
                if p.len() == n {
                    return Arc::clone(p);
                }
            }
        }
        let built = {
            let guard = state.read().expect("shard state lock poisoned");
            let store = guard.bm25.as_ref().and_then(|b| b.as_index());
            let mut parents = Vec::with_capacity(n);
            for slot in 0..n {
                let parent = store
                    .and_then(|s| s.lineage(slot as u32))
                    .map(|l| l.opinion_id)
                    .unwrap_or(SELF_PARENT_TAG | (slot_offset + slot as u64));
                parents.push(parent);
            }
            Arc::new(parents)
        };
        state
            .write()
            .expect("shard state lock poisoned")
            .parents = Some(Arc::clone(&built));
        built
    }

    fn validate_start(index: &TurboQuantIndex, start: &StartShardSearch) -> Result<(), Status> {
        let dim = index
            .dim_opt()
            .ok_or_else(|| Status::failed_precondition("index has no vectors"))?;
        if start.vector.len() != dim {
            return Err(Status::invalid_argument(format!(
                "query vector has dim {}, index expects {dim}",
                start.vector.len()
            )));
        }
        if let Some((_, coord, value)) = turbovec::first_invalid_coord(&start.vector, dim) {
            return Err(Status::invalid_argument(format!(
                "query coordinate {coord} is invalid: {value}"
            )));
        }
        Ok(())
    }

    /// Persist the index to its configured path, if any. Shared by the
    /// `Flush` RPC and save-on-shutdown in the binary.
    pub fn flush_index(&self) -> Result<FlushResponse, Status> {
        let mut guard = self.state.write().expect("shard state lock poisoned");
        let num_vectors = guard.index.as_ref().map_or(0, |i| i.len() as u64);
        let num_documents = guard.bm25.as_ref().map_or(0, |b| b.doc_count());
        let Some(config_path) = self.config.index_path.clone() else {
            return Ok(FlushResponse {
                path: String::new(),
                num_vectors,
                num_documents,
                written: false,
            });
        };
        // Log before data: fsync the WAL BEFORE the index images are
        // written, so a crash between the two leaves the log a superset
        // of the on-disk indexes — never the reverse. An index image
        // whose records the log lost would silently drop those records
        // from every future replay (reshard, recovery).
        if let Some(wal) = guard.wal.as_mut() {
            wal.flush()
                .map_err(|e| Status::internal(format!("wal fsync {}: {e}", wal.dir().display())))?;
        }
        // Flush into the active snapshot generation when one was
        // installed, else the legacy layout — never split the two.
        let (tv_path, bm25_path) = storage_paths(&config_path, guard.generation.as_ref());
        if let Some(index) = guard.index.as_ref() {
            index
                .write(&tv_path)
                .map_err(|e| Status::internal(format!("write {}: {e}", tv_path.display())))?;
        }
        // Save the builder as v3 and immediately reopen it disk-resident:
        // after Flush a shard holds no postings or texts in heap.
        // Already-resident shards have nothing to write.
        let built = match guard.bm25.as_mut() {
            Some(Bm25Shard::Building(store)) => {
                store
                    .save(&bm25_path)
                    .map_err(|e| Status::internal(format!("write {}: {e}", bm25_path.display())))?;
                true
            }
            Some(Bm25Shard::Spilling(builder)) => {
                builder
                    .finish(&bm25_path)
                    .map_err(|e| Status::internal(format!("write {}: {e}", bm25_path.display())))?;
                true
            }
            _ => false,
        };
        if built {
            guard.bm25 = Some(
                Bm25Reader::open(&bm25_path)
                    .map(Bm25Shard::Resident)
                    .map_err(|e| {
                        Status::internal(format!("reopen {}: {e}", bm25_path.display()))
                    })?,
            );
        }
        let written = guard.index.is_some() || guard.bm25.is_some();
        // Durability point reached: the log was fsynced above, then the
        // indexes hit disk. The marker records that a flush happened;
        // its own fsync failing degrades the log rather than un-flushing
        // the indexes (they are already durable and consistent).
        wal_append_or_degrade(&mut guard.wal, wal_record::Op::Flush(FlushMarker {}));
        if let Some(wal) = guard.wal.as_mut() {
            if let Err(e) = wal.flush() {
                eprintln!("wal: post-flush marker fsync failed: {e}");
            }
        }
        Ok(FlushResponse {
            path: tv_path.display().to_string(),
            num_vectors,
            num_documents,
            written,
        })
    }

    /// Receive one snapshot image into the staging generation directory
    /// (`index.tv`, plus `index.tv.bm25` when declared). The first
    /// `manifest.tv_bytes` of data land in the index, the rest in the
    /// sidecar; both are synced before the caller swaps anything. Returns
    /// with the staging dir complete or not at all — on error the caller
    /// removes it.
    async fn receive_image(
        inbound: &mut Streaming<SnapshotChunk>,
        manifest: &SnapshotManifest,
        tmp_dir: &Path,
    ) -> Result<(), Status> {
        use tokio::io::AsyncWriteExt;
        let io_err = |what: &Path, e: std::io::Error| {
            Status::internal(format!("snapshot receive {}: {e}", what.display()))
        };
        tokio::fs::create_dir_all(tmp_dir)
            .await
            .map_err(|e| io_err(tmp_dir, e))?;
        let tv_tmp = generation_tv(tmp_dir);
        let bm25_tmp = generation_bm25(tmp_dir);
        let mut tv = tokio::fs::File::create(&tv_tmp)
            .await
            .map_err(|e| io_err(&tv_tmp, e))?;
        let mut bm25 = if manifest.bm25_bytes > 0 {
            Some(
                tokio::fs::File::create(&bm25_tmp)
                    .await
                    .map_err(|e| io_err(&bm25_tmp, e))?,
            )
        } else {
            None
        };
        let (mut tv_written, mut bm25_written) = (0u64, 0u64);
        while let Some(chunk) = inbound.message().await? {
            let Some(snapshot_chunk::Payload::Data(mut data)) = chunk.payload else {
                return Err(Status::invalid_argument(
                    "SnapshotChunk after the manifest must carry data",
                ));
            };
            // Fill the .tv first; overflow spills into the .bm25.
            let tv_take = (manifest.tv_bytes - tv_written).min(data.len() as u64) as usize;
            if tv_take > 0 {
                tv.write_all(&data[..tv_take])
                    .await
                    .map_err(|e| io_err(&tv_tmp, e))?;
                tv_written += tv_take as u64;
                data.drain(..tv_take);
            }
            if !data.is_empty() {
                let Some(sidecar) = bm25.as_mut() else {
                    return Err(Status::invalid_argument(
                        "snapshot carries more data than the manifest declares",
                    ));
                };
                if bm25_written + data.len() as u64 > manifest.bm25_bytes {
                    return Err(Status::invalid_argument(
                        "snapshot carries more data than the manifest declares",
                    ));
                }
                sidecar
                    .write_all(&data)
                    .await
                    .map_err(|e| io_err(&bm25_tmp, e))?;
                bm25_written += data.len() as u64;
            }
        }
        if tv_written != manifest.tv_bytes || bm25_written != manifest.bm25_bytes {
            return Err(Status::invalid_argument(format!(
                "truncated snapshot: received {tv_written}+{} of declared {}+{} bytes",
                bm25_written, manifest.tv_bytes, manifest.bm25_bytes
            )));
        }
        tv.sync_all().await.map_err(|e| io_err(&tv_tmp, e))?;
        if let Some(sidecar) = bm25.as_mut() {
            sidecar.sync_all().await.map_err(|e| io_err(&bm25_tmp, e))?;
        }
        Ok(())
    }

    /// Validate a received snapshot image and atomically swap it in (the
    /// blocking half of `InstallSnapshot`). Everything that can fail —
    /// loading the index, opening the sidecar, the calibration check —
    /// happens BEFORE the swap, so a rejected install leaves the live
    /// shard and the on-disk generation untouched.
    ///
    /// The swap itself is one directory rename: the whole `.tv` + `.bm25`
    /// pair travels inside the staging dir, so the two files can never
    /// tear. Replacing an existing generation renames it aside first; the
    /// crash window between the two renames is covered by
    /// [`recover_generation`] at startup.
    fn apply_snapshot(&self, tmp_dir: &Path, with_bm25: bool) -> Result<InstallSnapshotResponse, Status> {
        let path = self
            .config
            .index_path
            .as_ref()
            .expect("handler requires index_path")
            .clone();
        let snap = generation_dir(&path);
        let old = generation_old_dir(&path);
        let tv_tmp = generation_tv(tmp_dir);
        let bm25_tmp = generation_bm25(tmp_dir);

        let loaded = TurboQuantIndex::load(&tv_tmp).map_err(|e| {
            Status::invalid_argument(format!("snapshot is not a valid turbovec index: {e}"))
        })?;
        if with_bm25 {
            // Open-check the sidecar (and drop it again) before the swap;
            // the live shard re-opens from the generation dir.
            drop(Bm25Shard::open(&bm25_tmp).map_err(|e| {
                Status::invalid_argument(format!("snapshot sidecar is not a valid BM25 store: {e}"))
            })?);
        }

        let mut guard = self.state.write().expect("shard state lock poisoned");
        // Calibration comparability: a shard with a locked calibration
        // (seeded or fitted) only accepts an identically calibrated image.
        if let Some(index) = guard.index.as_ref() {
            if let Some((shift, scale)) = index.calibration() {
                let matches = loaded
                    .calibration()
                    .is_some_and(|(s, c)| s == shift && c == scale);
                if !matches {
                    return Err(Status::failed_precondition(
                        "snapshot calibration differs from the calibration locked on this \
                         shard; mixed calibrations make scores incomparable across shards",
                    ));
                }
            }
        }

        // The atomic swap: previous generation aside (if any), staging
        // dir into place. Both files move inside ONE directory rename.
        if snap.exists() {
            std::fs::rename(&snap, &old)
                .map_err(|e| Status::internal(format!("retire {}: {e}", old.display())))?;
        }
        if let Err(e) = std::fs::rename(tmp_dir, &snap) {
            // Best-effort rollback so startup recovery sees a clean state.
            if old.exists() && !snap.exists() {
                let _ = std::fs::rename(&old, &snap);
            }
            return Err(Status::internal(format!("install {}: {e}", snap.display())));
        }
        let _ = std::fs::remove_dir_all(&old);

        guard.bm25 = if with_bm25 {
            Some(Bm25Shard::open(&generation_bm25(&snap)).map_err(|e| {
                Status::internal(format!("open installed {}: {e}", generation_bm25(&snap).display()))
            })?)
        } else {
            // Wholesale replacement: an image without a sidecar replaces
            // any existing postings store (its ids would describe a
            // different corpus). The old store's files left with the old
            // generation.
            None
        };
        let num_documents = guard.bm25.as_ref().map_or(0, |b| b.doc_count());
        let num_vectors = loaded.len() as u64;
        guard.index = Some(loaded);
        guard.generation = Some(snap.clone());
        // The snapshot supersedes the log: fsync and retire the current
        // generation, open gen-(g+1) with the installed image's
        // calibration (same bucket geometry), and mark where it came
        // from. Records before this point describe the OLD shard
        // contents.
        if guard.wal.is_some() {
            let source_generation = guard.wal.as_ref().map_or(0, WalWriter::generation);
            // The installed image is state this fresh log does NOT
            // contain: record it as preexisting so the reshard tool
            // refuses a log-only replay that would drop the image.
            let mut manifest = wal_manifest(
                guard.index.as_ref(),
                &self.config,
                source_generation + 1,
                (num_vectors, num_documents),
            );
            let previous = guard.wal.as_ref().expect("checked above").manifest();
            manifest.bucket_bits = previous.bucket_bits;
            manifest.bucket_count = previous.bucket_count;
            let wal_err = |e: std::io::Error| Status::internal(format!("wal rotate: {e}"));
            let wal = guard.wal.as_mut().expect("checked above");
            wal.flush().map_err(wal_err)?;
            *wal = WalWriter::create(&wal::wal_dir(&path), manifest).map_err(wal_err)?;
            wal.append(wal_record::Op::Snapshot(SnapshotMarker {
                source_generation,
            }))
            .map_err(wal_err)?;
            wal.flush().map_err(wal_err)?;
        }
        Ok(InstallSnapshotResponse {
            path: generation_tv(&snap).display().to_string(),
            num_vectors,
            num_documents,
        })
    }

    /// Apply one `SetCalibration`: lock the calibration on an empty shard.
    fn apply_calibration(&self, req: &SetCalibrationRequest) -> Result<bool, Status> {
        let dim = req.dim as usize;
        let bit_width = req.bit_width as usize;
        let build = || {
            TurboQuantIndex::new_with_calibration(dim, bit_width, &req.shift, &req.scale)
                .map_err(|e| Status::invalid_argument(format!("invalid calibration: {e}")))
        };
        let mut guard = self.state.write().expect("shard state lock poisoned");
        let result = match guard.index.as_ref() {
            Some(index) if !index.is_empty() => Err(Status::failed_precondition(format!(
                "shard holds {} vectors; calibration is locked for the index lifetime",
                index.len()
            ))),
            Some(index) => {
                let same = index.dim_opt() == Some(dim)
                    && index.bit_width() == bit_width
                    && index.calibration().is_some_and(|(s, c)| {
                        s == req.shift.as_slice() && c == req.scale.as_slice()
                    });
                if same {
                    return Ok(true); // idempotent retry
                }
                if index.calibration().is_some() {
                    return Err(Status::already_exists(
                        "a different calibration is already locked on this shard",
                    ));
                }
                // Empty, unseeded index: replace with the seeded one.
                guard.index = Some(build()?);
                Ok(false)
            }
            None => {
                guard.index = Some(build()?);
                Ok(false)
            }
        };
        // Complete the pending WAL manifest with the locked calibration
        // (no-op once calibration is on disk).
        if result.is_ok() {
            if let Some(wal) = guard.wal.as_mut() {
                wal.update_manifest(|m| {
                    m.dim = dim as u32;
                    m.bit_width = bit_width as u32;
                    m.calibration_shift = req.shift.clone();
                    m.calibration_scale = req.scale.clone();
                });
            }
        }
        result
    }

    /// Apply one ingested batch under the write lock. Returns
    /// `(added, global id of the batch's first vector)`.
    fn apply_batch(&self, batch: AddVectorsRequest) -> Result<(u64, u64), Status> {
        if batch.vectors.is_empty() {
            return Ok((0, 0));
        }
        let mut guard = self.state.write().expect("shard state lock poisoned");
        let known_dim = guard.index.as_ref().and_then(|i| i.dim_opt());
        let dim = if batch.dim != 0 {
            let d = batch.dim as usize;
            if let Some(known) = known_dim {
                if known != d {
                    return Err(Status::invalid_argument(format!(
                        "batch dim {d} does not match shard dim {known}"
                    )));
                }
            }
            d
        } else {
            known_dim.ok_or_else(|| {
                Status::failed_precondition(
                    "shard has no index or calibration yet; set calibration first or pass dim",
                )
            })?
        };
        if !batch.vectors.len().is_multiple_of(dim) {
            return Err(Status::invalid_argument(format!(
                "batch of {} floats is not a multiple of dim {dim}",
                batch.vectors.len()
            )));
        }
        if let Some((vi, ci, v)) = turbovec::first_invalid_coord(&batch.vectors, dim) {
            return Err(Status::invalid_argument(format!(
                "invalid input value at vector {vi}, coord {ci}: {v}"
            )));
        }
        let (first_id, index_bit_width) = {
            let index = match guard.index.as_mut() {
                Some(index) => index,
                None => {
                    // From-scratch, unseeded: turbovec fits calibration from
                    // this first batch. Seeded deployment is the SetCalibration
                    // path; this exists for single-shard convenience.
                    guard.index = Some(
                        TurboQuantIndex::new(dim, self.config.bit_width)
                            .map_err(|e| Status::invalid_argument(format!("{e}")))?,
                    );
                    guard.index.as_mut().expect("just constructed")
                }
            };
            (self.config.slot_offset + index.len() as u64, index.bit_width())
        };
        // Apply first, log after, under this one lock. A failed apply
        // must never reach the log: its assigned ids would be reused by
        // the next batch and the duplicate would poison every replay.
        // Durability is unaffected — both sides are volatile until
        // Flush, which fsyncs the log BEFORE the index images.
        guard
            .index
            .as_mut()
            .expect("constructed or present above")
            .add_2d(&batch.vectors, dim)
            .map_err(|e| Status::invalid_argument(format!("{e}")))?;
        // One record PER VECTOR: contiguous ids hash to different
        // buckets, and a bucket file must never hold vectors that belong
        // to another bucket. Buffered (no fsync per batch); Flush and
        // generation rotation fsync.
        if let Some(wal) = guard.wal.as_mut() {
            wal.update_manifest(|m| {
                if m.dim == 0 {
                    m.dim = dim as u32;
                    m.bit_width = index_bit_width as u32;
                }
            });
        }
        for (i, vector) in batch.vectors.chunks_exact(dim).enumerate() {
            wal_append_or_degrade(
                &mut guard.wal,
                wal_record::Op::AddVectors(LoggedAddVectors {
                    first_id: first_id + i as u64,
                    batch: Some(AddVectorsRequest {
                        vectors: vector.to_vec(),
                        dim: dim as u32,
                    }),
                }),
            );
        }
        Ok(((batch.vectors.len() / dim) as u64, first_id))
    }

    /// Compute both raw legs for a hybrid query: `(vector_leg, bm25_leg)`
    /// as `(global_doc_id, raw_score)` lists, score-descending.
    ///
    /// Vector leg: the chunked scan (local floor seeding only — the
    /// cross-shard floor-sharing protocol lives on SearchShard's bidi
    /// stream and is not part of the unary hybrid path). A shard with no
    /// vector index, or an empty query vector, contributes an empty leg
    /// rather than failing the whole hybrid query. BM25 leg: scored with
    /// the coordinator-supplied GLOBAL stats.
    fn compute_legs(
        &self,
        vector: &[f32],
        terms: &[String],
        global_doc_count: u64,
        global_total_doc_length: u64,
        global_doc_frequencies: &[u32],
        k: usize,
    ) -> Result<(RawLeg, RawLeg), Status> {
        let guard = self.state.read().expect("shard state lock poisoned");

        let mut vector_leg: Vec<(u64, f64)> = Vec::new();
        if k > 0 && !vector.is_empty() {
            if let Some(index) = guard.index.as_ref() {
                let dim = index.dim_opt().unwrap_or(0);
                if vector.len() != dim {
                    return Err(Status::invalid_argument(format!(
                        "hybrid vector has dim {}, index expects {dim}",
                        vector.len()
                    )));
                }
                if let Some((_, coord, value)) = turbovec::first_invalid_coord(vector, dim) {
                    return Err(Status::invalid_argument(format!(
                        "hybrid vector coordinate {coord} is invalid: {value}"
                    )));
                }
                let (hits, _) = chunked_topk(
                    index,
                    vector,
                    k,
                    self.config.chunk_blocks,
                    &mut || None,
                    &mut |_| {},
                    false,
                );
                vector_leg = hits
                    .into_iter()
                    .map(|h| {
                        (
                            self.config.slot_offset + u64::from(h.slot),
                            f64::from(h.score),
                        )
                    })
                    .collect();
            }
        }

        let mut bm25_leg: Vec<(u64, f64)> = Vec::new();
        if k > 0 && !terms.is_empty() {
            if let Some(store) = guard.bm25.as_ref() {
                let stats = bm25::CorpusStats {
                    doc_count: global_doc_count,
                    total_doc_length: global_total_doc_length,
                    dfs: global_doc_frequencies.to_vec(),
                };
                let index = store.as_index().ok_or_else(|| {
                    Status::failed_precondition("bm25 bulk build in progress; Flush first")
                })?;
                // Block-max path when every scored term has impacts
                // (v5 shards) and the node flag allows it; heap store,
                // v3/v4, and --block-max=false keep top_k. The results
                // are bit-identical either way.
                let prunable = self.config.block_max
                    && terms
                        .iter()
                        .enumerate()
                        .all(|(ti, t)| stats.dfs[ti] == 0 || index.has_impacts(t));
                let docs = if prunable {
                    bm25::top_k_pruned(
                        index,
                        terms,
                        &stats,
                        Bm25Params::default(),
                        k,
                        f64::NEG_INFINITY,
                    )
                } else {
                    bm25::top_k(index, terms, &stats, Bm25Params::default(), k)
                };
                bm25_leg = docs
                    .into_iter()
                    .map(|d| (self.config.slot_offset + u64::from(d.doc_id), d.score))
                    .collect();
            }
        }

        Ok((vector_leg, bm25_leg))
    }

    /// Level one of the two-level hybrid fusion: run both legs locally
    /// and RRF-fuse them (see `SearchService.HybridSearch`).
    fn run_hybrid(&self, req: HybridShardRequest) -> Result<HybridShardResponse, Status> {
        let k = req.k as usize;
        if req.terms.len() != req.global_doc_frequencies.len() {
            return Err(Status::invalid_argument(
                "terms and global_doc_frequencies must have the same length",
            ));
        }
        let vector_weight = weight_or_default(req.vector_weight, "vector_weight")?;
        let bm25_weight = weight_or_default(req.bm25_weight, "bm25_weight")?;
        let rrf_k = if req.rrf_k == 0.0 {
            fusion::DEFAULT_RRF_K
        } else {
            f64::from(req.rrf_k)
        };
        if rrf_k.is_nan() || rrf_k <= 0.0 {
            return Err(Status::invalid_argument("rrf_k must be positive"));
        }

        let (vector_leg, bm25_leg) = self.compute_legs(
            &req.vector,
            &req.terms,
            req.global_doc_count,
            req.global_total_doc_length,
            &req.global_doc_frequencies,
            k,
        )?;

        let fused = fusion::rrf_fuse(
            &[
                Leg {
                    hits: vector_leg,
                    weight: vector_weight,
                },
                Leg {
                    hits: bm25_leg,
                    weight: bm25_weight,
                },
            ],
            rrf_k,
            k,
        );
        Ok(HybridShardResponse {
            hits: fused
                .into_iter()
                .map(|h| HybridLegHit {
                    doc_id: h.doc_id,
                    fused_score: h.fused_score as f32,
                    vector_rank: h.leg_ranks[0],
                    vector_score: h.leg_scores[0].unwrap_or(0.0) as f32,
                    bm25_rank: h.leg_ranks[1],
                    bm25_score: h.leg_scores[1].unwrap_or(0.0) as f32,
                })
                .collect(),
        })
    }
}

/// Request weights default to 1.0 (0 means "unset" in the proto);
/// negatives are rejected.
fn weight_or_default(value: f32, name: &str) -> Result<f64, Status> {
    if value == 0.0 {
        return Ok(1.0);
    }
    if value < 0.0 || value.is_nan() {
        return Err(Status::invalid_argument(format!("{name} must be >= 0")));
    }
    Ok(f64::from(value))
}

/// Bulk-ingest internals: the two analysis transports and the shared
/// per-document apply step.
impl NodeServiceImpl {
    /// Apply one analyzed document: id assignment, store insert, WAL
    /// append. Must be called in arrival order — both transports
    /// guarantee it.
    fn apply_analyzed_document(
        &self,
        doc: AddDocumentsRequest,
        analyzed: crate::postings::AnalyzedDoc,
        added: &mut u64,
        first_id: &mut u64,
    ) -> Result<(), Status> {
        let mut guard = self.state.write().expect("shard state lock poisoned");
        // A disk-resident shard that receives more documents is first
        // reloaded into the heap builder (the append path is
        // bulk-load: build in memory, flush back to v3).
        if matches!(guard.bm25, Some(Bm25Shard::Resident(_))) {
            let bm25_path = self
                .config
                .index_path
                .as_ref()
                .map(|p| storage_paths(p, guard.generation.as_ref()).1)
                .ok_or_else(|| {
                    Status::failed_precondition("resident shard has no index path to reload from")
                })?;
            let store = Bm25Store::load(&bm25_path)
                .map_err(|e| Status::internal(format!("reload {}: {e}", bm25_path.display())))?;
            guard.bm25 = Some(Bm25Shard::Building(store));
        }
        // Shared positional id space with the vector side: the next id
        // is past both indexes' tips.
        let vector_tip = guard.index.as_ref().map_or(0, |i| i.len() as u32);
        if guard.bm25.is_none() {
            let builder = self.new_builder(guard.generation.as_ref())?;
            guard.bm25 = Some(builder);
        }
        let doc_id = vector_tip.max(
            guard
                .bm25
                .as_ref()
                .expect("builder just ensured")
                .next_doc_id(),
        );
        let global_id = self.config.slot_offset + u64::from(doc_id);
        if *added == 0 {
            *first_id = global_id;
        }
        // Apply first, log after, as for vectors: a document that
        // fails to enter the store must never reach the log, or its
        // id would be reassigned and poison the replay.
        let lineage = doc.lineage.map(|l| crate::postings::DocLineage {
            opinion_id: l.opinion_id,
            cluster_id: l.cluster_id,
            span_start: l.span_start,
            span_end: l.span_end,
        });
        match guard.bm25.as_mut().expect("builder just ensured") {
            Bm25Shard::Building(store) => {
                store.add_document_with_lineage(doc_id, doc.text.clone(), analyzed, lineage);
            }
            Bm25Shard::Spilling(builder) => {
                builder
                    .add_document_with_lineage(doc_id, doc.text.clone(), analyzed, lineage)
                    .map_err(|e| Status::internal(format!("spill write: {e}")))?;
            }
            Bm25Shard::Resident(_) => {
                return Err(Status::internal("shard builder unavailable"));
            }
        }
        wal_append_or_degrade(
            &mut guard.wal,
            wal_record::Op::AddDocuments(LoggedAddDocuments {
                first_id: global_id,
                documents: vec![doc],
            }),
        );
        *added += 1;
        Ok(())
    }

    /// Bulk ingest over one AnalyzeStream: submissions run ahead of the
    /// apply point as far as the sidecar grants credit, results return
    /// in completion order, and the apply wavefront advances over
    /// consecutive sequences so application stays in arrival order.
    async fn ingest_streamed(
        &self,
        mut session: crate::analyzer::AnalyzeStream,
        first: AddDocumentsRequest,
        inbound: &mut Streaming<AddDocumentsRequest>,
        addr: &str,
        added: &mut u64,
        first_id: &mut u64,
    ) -> Result<(), Status> {
        // Documents held for ordered apply; bounds this side's memory the
        // way ANALYZE_PIPELINE bounded the unary path.
        const MAX_PENDING: usize = 32;
        fn store_result(
            results: &mut std::collections::HashMap<u64, crate::postings::AnalyzedDoc>,
            item: Option<(u64, Result<crate::postings::AnalyzedDoc, Status>)>,
        ) -> Result<(), Status> {
            match item {
                Some((sequence, Ok(analyzed))) => {
                    results.insert(sequence, analyzed);
                    Ok(())
                }
                // One document failing fails the ingest call, exactly as
                // a failed unary analysis did.
                Some((_, Err(status))) => Err(status),
                None => Err(Status::internal(
                    "analysis stream completed with documents in flight",
                )),
            }
        }
        enum Step {
            Doc(AddDocumentsRequest),
            InboundClosed,
            Result(Option<(u64, Result<crate::postings::AnalyzedDoc, Status>)>),
        }
        let mut spec = first.analysis.clone();
        let mut submit = Some(session.submitter());
        let mut pending: std::collections::BTreeMap<u64, AddDocumentsRequest> =
            std::collections::BTreeMap::new();
        let mut results: std::collections::HashMap<u64, crate::postings::AnalyzedDoc> =
            std::collections::HashMap::new();
        submit
            .as_ref()
            .expect("submitter set above")
            .submit(0, &first.text)
            .await?;
        pending.insert(0, first);
        let mut next_seq = 1u64;
        let mut next_apply = 0u64;
        let mut inbound_open = true;
        loop {
            while let Some(analyzed) = results.remove(&next_apply) {
                let doc = pending
                    .remove(&next_apply)
                    .expect("every result has a pending document");
                self.apply_analyzed_document(doc, analyzed, added, first_id)?;
                next_apply += 1;
            }
            if pending.is_empty() && !inbound_open {
                break;
            }
            let step = if inbound_open && pending.len() < MAX_PENDING {
                tokio::select! {
                    message = inbound.message() => match message? {
                        Some(doc) => Step::Doc(doc),
                        None => Step::InboundClosed,
                    },
                    result = session.next() => Step::Result(result?),
                }
            } else {
                Step::Result(session.next().await?)
            };
            match step {
                Step::Doc(doc) => {
                    if doc.analysis != spec {
                        // A mid-stream spec change (rare): drain the
                        // current session completely so ordering holds,
                        // then open a new one for the new spec. Dropping
                        // the submitter clone is what lets the old
                        // session half-close and drain.
                        drop(submit.take());
                        session.finish();
                        while !pending.is_empty() {
                            store_result(&mut results, session.next().await?)?;
                            while let Some(analyzed) = results.remove(&next_apply) {
                                let done = pending
                                    .remove(&next_apply)
                                    .expect("every result has a pending document");
                                self.apply_analyzed_document(done, analyzed, added, first_id)?;
                                next_apply += 1;
                            }
                        }
                        session =
                            crate::analyzer::AnalyzeStream::open(addr, doc.analysis.as_ref())
                                .await?;
                        spec = doc.analysis.clone();
                        submit = Some(session.submitter());
                    }
                    submit
                        .as_ref()
                        .expect("stream open while inbound open")
                        .submit(next_seq, &doc.text)
                        .await?;
                    pending.insert(next_seq, doc);
                    next_seq += 1;
                }
                Step::InboundClosed => {
                    inbound_open = false;
                    submit = None;
                    session.finish();
                }
                Step::Result(item) => store_result(&mut results, item)?,
            }
        }
        Ok(())
    }

    /// The pre-stream transport, kept for sidecars that predate
    /// AnalyzeStream: up to ANALYZE_PIPELINE unary sidecar calls run
    /// ahead of the apply point.
    async fn ingest_unary(
        &self,
        first: AddDocumentsRequest,
        inbound: &mut Streaming<AddDocumentsRequest>,
        addr: &str,
        added: &mut u64,
        first_id: &mut u64,
    ) -> Result<(), Status> {
        const ANALYZE_PIPELINE: usize = 8;
        let spawn_analysis = |doc: &AddDocumentsRequest| {
            let addr = addr.to_string();
            let text = doc.text.clone();
            let spec = doc.analysis.clone();
            tokio::spawn(
                async move { crate::analyzer::analyze_document(&addr, &text, spec.as_ref()).await },
            )
        };
        let mut in_flight: std::collections::VecDeque<(
            AddDocumentsRequest,
            tokio::task::JoinHandle<Result<crate::postings::AnalyzedDoc, Status>>,
        )> = std::collections::VecDeque::new();
        let handle = spawn_analysis(&first);
        in_flight.push_back((first, handle));
        let mut inbound_open = true;
        loop {
            while inbound_open && in_flight.len() < ANALYZE_PIPELINE {
                match inbound.message().await? {
                    Some(doc) => {
                        let handle = spawn_analysis(&doc);
                        in_flight.push_back((doc, handle));
                    }
                    None => inbound_open = false,
                }
            }
            let Some((doc, handle)) = in_flight.pop_front() else {
                break;
            };
            let analyzed = handle
                .await
                .map_err(|e| Status::internal(format!("analysis task failed: {e}")))??;
            self.apply_analyzed_document(doc, analyzed, added, first_id)?;
        }
        Ok(())
    }
}

#[tonic::async_trait]
impl NodeService for NodeServiceImpl {
    type SearchShardStream = ReceiverStream<Result<SearchShardResponse, Status>>;
    type StreamSearchStream = ReceiverStream<Result<StreamSearchResponse, Status>>;

    async fn search_shard(
        &self,
        request: Request<Streaming<SearchShardRequest>>,
    ) -> Result<Response<Self::SearchShardStream>, Status> {
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel::<Result<SearchShardResponse, Status>>(64);
        let state = self.state.clone();
        let config = self.config.clone();
        let scan_queue = config.coalesce.then(|| self.scan_queue());

        tokio::spawn(async move {
            // Protocol: the first message must be Start.
            let start = match inbound.message().await {
                Ok(Some(SearchShardRequest {
                    payload: Some(search_shard_request::Payload::Start(start)),
                })) => start,
                Ok(_) => {
                    let _ = tx
                        .send(Err(Status::invalid_argument(
                            "first SearchShardRequest must be StartShardSearch",
                        )))
                        .await;
                    return;
                }
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            };

            // Floor updates arrive on the same stream; a pump task folds
            // them into a watch cell the blocking scan polls between chunks.
            // Updates are monotone maxes, so only raises are stored.
            let (floor_tx, floor_rx) = watch::channel(f32::NEG_INFINITY);
            tokio::spawn(async move {
                loop {
                    match inbound.message().await {
                        Ok(Some(SearchShardRequest {
                            payload: Some(search_shard_request::Payload::FloorUpdate(u)),
                        })) => {
                            floor_tx.send_if_modified(|cur| {
                                if !u.floor.is_nan() && u.floor > *cur {
                                    *cur = u.floor;
                                    true
                                } else {
                                    false
                                }
                            });
                        }
                        // Duplicate Start or empty payload: ignore.
                        Ok(Some(_)) => {}
                        // Client closed (end of updates or cancellation) or
                        // the stream broke: stop pumping; the scan finishes
                        // on its own either way.
                        Ok(None) | Err(_) => break,
                    }
                }
            });

            let share = config.share_floors;
            let floor_delta = config.floor_delta;
            let chunk_blocks = config.chunk_blocks;
            let slot_offset = config.slot_offset;
            let scan_tx = tx.clone();
            // Publish only raises that clear the delta gate, and never
            // block the scan on a full channel: intermediate floors are
            // disposable (they are monotone, so the next chunk's publish
            // supersedes any dropped one). The terminal Done is sent
            // with `.await` below and cannot be dropped.
            let mut last_published = f32::NEG_INFINITY;
            let publish_floor = move |floor: f32| {
                if share && floor > last_published + floor_delta {
                    last_published = floor;
                    let _ = scan_tx.try_send(Ok(SearchShardResponse {
                        payload: Some(search_shard_response::Payload::FloorUpdate(
                            FloorUpdate { floor },
                        )),
                    }));
                }
            };
            let external_floor = move || {
                if share {
                    let f = *floor_rx.borrow();
                    (f != f32::NEG_INFINITY).then_some(f)
                } else {
                    None
                }
            };

            // Collapse-by-parent scans run their own solo path: the
            // collection semantics (one entry per parent, parent floors,
            // saturation escalation) do not batch with plain scans.
            if start.collapse_parents {
                if start.tie_complete {
                    let _ = tx
                        .send(Err(Status::invalid_argument(
                            "collapse_parents and tie_complete are mutually exclusive",
                        )))
                        .await;
                    return;
                }
                let mut external_floor = external_floor;
                let mut publish_floor = publish_floor;
                let scan = tokio::task::spawn_blocking(move || {
                    let n = {
                        let guard = state.read().expect("shard state lock poisoned");
                        let index = guard.index.as_ref().ok_or_else(|| {
                            Status::failed_precondition(
                                "shard has no index yet (set calibration or add vectors)",
                            )
                        })?;
                        Self::validate_start(index, &start)?;
                        index.len()
                    };
                    // parent_map takes its own locks (read to build, write
                    // to cache), so the validation guard is dropped first.
                    let parents = Self::parent_map(&state, slot_offset, n);
                    let guard = state.read().expect("shard state lock poisoned");
                    let index = guard.index.as_ref().ok_or_else(|| {
                        Status::failed_precondition("shard index disappeared mid-setup")
                    })?;
                    if index.len() != parents.len() {
                        return Err(Status::aborted(
                            "shard grew between setup and scan; retry",
                        ));
                    }
                    Ok(chunked_topk_collapsed(
                        index,
                        &start.vector,
                        start.k as usize,
                        chunk_blocks,
                        &parents,
                        &mut external_floor,
                        &mut publish_floor,
                    ))
                });
                let outcome = match scan.await {
                    Ok(result) => result,
                    Err(e) => Err(Status::internal(format!("collapse scan task failed: {e}"))),
                };
                match outcome {
                    Ok((hits, stats)) => {
                        let done = SearchShardDone {
                            hits: hits
                                .into_iter()
                                .map(|h| ScoredHit {
                                    vector_id: slot_offset + u64::from(h.slot),
                                    score: h.score,
                                    parent_id: h.parent,
                                })
                                .collect(),
                            stats: Some(ShardScanStats {
                                chunk_calls: stats.chunk_calls,
                                candidates_collected: stats.candidates_collected,
                                floors_published: stats.floors_published,
                                floor_updates_applied: stats.floor_updates_applied,
                            }),
                        };
                        let _ = tx
                            .send(Ok(SearchShardResponse {
                                payload: Some(search_shard_response::Payload::Done(done)),
                            }))
                            .await;
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                    }
                }
                return;
            }

            let outcome: Result<(Vec<ChunkHit>, ScanStats), Status> = match scan_queue {
                Some(jobs) => {
                    // Coalesced path: validate against the current index
                    // cheaply, then queue for a batched kernel pass. The
                    // batch runner holds the read lock for the scan, the
                    // same consistency the solo path gets.
                    let validated = {
                        let guard = state.read().expect("shard state lock poisoned");
                        match guard.index.as_ref() {
                            Some(index) => Self::validate_start(index, &start),
                            None => Err(Status::failed_precondition(
                                "shard has no index yet (set calibration or add vectors)",
                            )),
                        }
                    };
                    match validated {
                        Ok(()) => {
                            let (done_tx, done_rx) = tokio::sync::oneshot::channel();
                            let job = ScanJob {
                                vector: start.vector.clone(),
                                k: start.k as usize,
                                tie_complete: start.tie_complete,
                                external: Box::new(external_floor),
                                publish: Box::new(publish_floor),
                                done: done_tx,
                            };
                            if jobs.send(job).await.is_err() {
                                Err(Status::internal("scan scheduler unavailable"))
                            } else {
                                match done_rx.await {
                                    Ok(result) => result,
                                    Err(_) => {
                                        Err(Status::internal("scan batch dropped before finishing"))
                                    }
                                }
                            }
                        }
                        Err(e) => Err(e),
                    }
                }
                None => {
                    // Solo path (the coalescing A/B baseline): one
                    // blocking scan per RPC, exactly the historical
                    // behavior.
                    let mut external_floor = external_floor;
                    let mut publish_floor = publish_floor;
                    let scan = tokio::task::spawn_blocking(move || {
                        // The read guard is held for the whole chunked
                        // scan: adds (write lock) never interleave with a
                        // scan, so a search sees one consistent index
                        // snapshot.
                        let guard = state.read().expect("shard state lock poisoned");
                        let index = guard.index.as_ref().ok_or_else(|| {
                            Status::failed_precondition(
                                "shard has no index yet (set calibration or add vectors)",
                            )
                        })?;
                        Self::validate_start(index, &start)?;
                        Ok(chunked_topk(
                            index,
                            &start.vector,
                            start.k as usize,
                            chunk_blocks,
                            &mut external_floor,
                            &mut publish_floor,
                            start.tie_complete,
                        ))
                    });
                    match scan.await {
                        Ok(result) => result,
                        Err(e) => Err(Status::internal(format!("scan task failed: {e}"))),
                    }
                }
            };

            match outcome {
                Ok((hits, stats)) => {
                    let done = SearchShardDone {
                        hits: hits
                            .into_iter()
                            .map(|h| ScoredHit {
                                vector_id: slot_offset + u64::from(h.slot),
                                score: h.score,
                                parent_id: 0,
                            })
                            .collect(),
                        stats: Some(ShardScanStats {
                            chunk_calls: stats.chunk_calls,
                            candidates_collected: stats.candidates_collected,
                            floors_published: stats.floors_published,
                            floor_updates_applied: stats.floor_updates_applied,
                        }),
                    };
                    let _ = tx
                        .send(Ok(SearchShardResponse {
                            payload: Some(search_shard_response::Payload::Done(done)),
                        }))
                        .await;
                }
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn health(
        &self,
        _request: Request<HealthRequest>,
    ) -> Result<Response<HealthResponse>, Status> {
        let guard = self.state.read().expect("shard state lock poisoned");
        let (num_vectors, dim, bit_width) = match guard.index.as_ref() {
            Some(index) => (
                index.len() as u64,
                index.dim_opt().unwrap_or(0) as u32,
                index.bit_width() as u32,
            ),
            None => (0, 0, self.config.bit_width as u32),
        };
        let (bm25_docs, bm25_building) = match guard.bm25.as_ref() {
            Some(shard) => (shard.doc_count(), matches!(shard, Bm25Shard::Spilling(_))),
            None => (0, false),
        };
        Ok(Response::new(HealthResponse {
            num_vectors,
            dim,
            bit_width,
            slot_offset: self.config.slot_offset,
            bm25_docs,
            bm25_building,
            ingest_active: self
                .ingest_busy
                .load(std::sync::atomic::Ordering::Acquire),
        }))
    }

    async fn stream_search(
        &self,
        request: Request<Streaming<StreamSearchRequest>>,
    ) -> Result<Response<Self::StreamSearchStream>, Status> {
        let mut inbound = request.into_inner();
        let (tx, rx) = mpsc::channel::<Result<StreamSearchResponse, Status>>(64);
        let state = self.state.clone();
        let slot_offset = self.config.slot_offset;

        tokio::spawn(async move {
            // Protocol: the first message must be Start.
            let start = match inbound.message().await {
                Ok(Some(StreamSearchRequest {
                    payload: Some(stream_search_request::Payload::Start(start)),
                })) => start,
                Ok(_) => {
                    let _ = tx
                        .send(Err(Status::invalid_argument(
                            "first StreamSearchRequest must be StartStreamSearch",
                        )))
                        .await;
                    return;
                }
                Err(e) => {
                    let _ = tx.send(Err(e)).await;
                    return;
                }
            };
            if start.initial_floor.is_some_and(f32::is_nan) {
                let _ = tx
                    .send(Err(Status::invalid_argument("initial_floor must not be NaN")))
                    .await;
                return;
            }

            // Floor raises and Stop fold into cells the blocking scan
            // polls after each emitted block — the same pump shape as
            // search_shard's (monotone maxes, everything else ignored).
            let (floor_tx, floor_rx) = watch::channel(f32::NEG_INFINITY);
            let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
            let stop_pump = Arc::clone(&stop);
            tokio::spawn(async move {
                loop {
                    match inbound.message().await {
                        Ok(Some(StreamSearchRequest {
                            payload: Some(stream_search_request::Payload::FloorUpdate(u)),
                        })) => {
                            floor_tx.send_if_modified(|cur| {
                                if !u.floor.is_nan() && u.floor > *cur {
                                    *cur = u.floor;
                                    true
                                } else {
                                    false
                                }
                            });
                        }
                        Ok(Some(StreamSearchRequest {
                            payload: Some(stream_search_request::Payload::Stop(_)),
                        })) => {
                            stop_pump.store(true, std::sync::atomic::Ordering::Release);
                            break;
                        }
                        // Duplicate Start or empty payload: ignore.
                        Ok(Some(_)) => {}
                        // Client closed or the stream broke: no more
                        // raises can arrive; the scan finishes (or hits
                        // the dead response channel) on its own.
                        Ok(None) | Err(_) => break,
                    }
                }
            });

            let scan_tx = tx.clone();
            let scan =
                tokio::task::spawn_blocking(move || -> Result<StreamSearchSummary, Status> {
                    let guard = state.read().expect("shard state lock poisoned");
                    let index = guard.index.as_ref().ok_or_else(|| {
                        Status::failed_precondition(
                            "shard has no index yet (set calibration or add vectors)",
                        )
                    })?;
                    let dim = index
                        .dim_opt()
                        .ok_or_else(|| Status::failed_precondition("index has no vectors"))?;
                    if start.vector.len() != dim {
                        return Err(Status::invalid_argument(format!(
                            "query vector has dim {}, index expects {dim}",
                            start.vector.len()
                        )));
                    }
                    if let Some((_, coord, value)) =
                        turbovec::first_invalid_coord(&start.vector, dim)
                    {
                        return Err(Status::invalid_argument(format!(
                            "query coordinate {coord} is invalid: {value}"
                        )));
                    }

                    let mut options = turbovec::SearchOptions::new();
                    let mut floor_now = f32::NEG_INFINITY;
                    if let Some(f) = start.initial_floor {
                        options = options.with_initial_threshold(f);
                        floor_now = f;
                    }
                    let mut raises = 0u64;
                    let summary = index
                        .try_search_streaming(&start.vector, options, |batch| {
                            // Pack the batch as 12-byte LE records
                            // (u64 global id, f32 score), fused into the
                            // slot-to-global-id rebase — one pass, no
                            // per-hit messages. Real emissions only
                            // carry live slots; a negative would be an
                            // engine contract break, dropped rather
                            // than wrapped into a bogus global id.
                            let mut hits: Vec<u8> = Vec::with_capacity(12 * batch.slots.len());
                            for (&slot, &score) in batch.slots.iter().zip(batch.scores) {
                                if slot < 0 {
                                    continue;
                                }
                                hits.extend_from_slice(
                                    &(slot_offset + slot as u64).to_le_bytes(),
                                );
                                hits.extend_from_slice(&score.to_le_bytes());
                            }
                            let sent = scan_tx.blocking_send(Ok(StreamSearchResponse {
                                payload: Some(stream_search_response::Payload::Batch(
                                    StreamSearchBatch { hits },
                                )),
                            }));
                            // A dead response channel means the client is
                            // gone: stop scanning, nobody is listening.
                            if sent.is_err() || stop.load(std::sync::atomic::Ordering::Acquire) {
                                return turbovec::StreamControl::Stop;
                            }
                            let f = *floor_rx.borrow();
                            if f > floor_now {
                                floor_now = f;
                                raises += 1;
                                turbovec::StreamControl::RaiseFloor(f)
                            } else {
                                turbovec::StreamControl::Continue
                            }
                        })
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    Ok(StreamSearchSummary {
                        completed: summary.completed,
                        emitted: summary.emitted as u64,
                        blocks_scanned: summary.blocks_scanned as u64,
                        floor_raises_applied: raises,
                    })
                });
            match scan.await {
                Ok(Ok(summary)) => {
                    let _ = tx
                        .send(Ok(StreamSearchResponse {
                            payload: Some(stream_search_response::Payload::Summary(summary)),
                        }))
                        .await;
                }
                Ok(Err(status)) => {
                    let _ = tx.send(Err(status)).await;
                }
                Err(e) => {
                    let _ = tx
                        .send(Err(Status::internal(format!("stream scan panicked: {e}"))))
                        .await;
                }
            }
        });

        Ok(Response::new(ReceiverStream::new(rx)))
    }

    async fn get_calibration(
        &self,
        _request: Request<GetCalibrationRequest>,
    ) -> Result<Response<GetCalibrationResponse>, Status> {
        let guard = self.state.read().expect("shard state lock poisoned");
        let (dim, bit_width, num_vectors, shift, scale) = match guard.index.as_ref() {
            Some(index) => {
                let (shift, scale) = index
                    .calibration()
                    .map(|(s, c)| (s.to_vec(), c.to_vec()))
                    .unwrap_or_default();
                (
                    index.dim_opt().unwrap_or(0) as u32,
                    index.bit_width() as u32,
                    index.len() as u64,
                    shift,
                    scale,
                )
            }
            None => (0, 0, 0, Vec::new(), Vec::new()),
        };
        Ok(Response::new(GetCalibrationResponse {
            dim,
            bit_width,
            num_vectors,
            shift,
            scale,
        }))
    }

    async fn set_calibration(
        &self,
        request: Request<SetCalibrationRequest>,
    ) -> Result<Response<SetCalibrationResponse>, Status> {
        let already_seeded = self.apply_calibration(&request.into_inner())?;
        Ok(Response::new(SetCalibrationResponse { already_seeded }))
    }

    async fn add_vectors(
        &self,
        request: Request<Streaming<AddVectorsRequest>>,
    ) -> Result<Response<AddVectorsResponse>, Status> {
        let _ingest = self.claim_ingest()?;
        let mut inbound = request.into_inner();
        let mut added = 0u64;
        let mut first_id = 0u64;
        while let Some(batch) = inbound.message().await? {
            let service = self.clone();
            let (batch_added, batch_first_id) =
                tokio::task::spawn_blocking(move || service.apply_batch(batch))
                    .await
                    .map_err(|e| Status::internal(format!("add task failed: {e}")))??;
            if added == 0 && batch_added > 0 {
                first_id = batch_first_id;
            }
            added += batch_added;
        }
        let total = self
            .state
            .read()
            .expect("shard state lock poisoned")
            .index
            .as_ref()
            .map_or(0, |i| i.len() as u64);
        Ok(Response::new(AddVectorsResponse {
            added,
            total,
            first_id,
        }))
    }

    async fn flush(
        &self,
        _request: Request<FlushRequest>,
    ) -> Result<Response<FlushResponse>, Status> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || service.flush_index())
            .await
            .map_err(|e| Status::internal(format!("flush task failed: {e}")))?
            .map(Response::new)
    }

    async fn install_snapshot(
        &self,
        request: Request<Streaming<SnapshotChunk>>,
    ) -> Result<Response<InstallSnapshotResponse>, Status> {
        let path = self.config.index_path.clone().ok_or_else(|| {
            Status::failed_precondition(
                "shard has no persistence path (index_path); a snapshot install IS persistence",
            )
        })?;
        let tmp_dir = generation_tmp_dir(&path);

        let mut inbound = request.into_inner();
        // Protocol: the first message must be the manifest.
        let manifest = match inbound.message().await? {
            Some(SnapshotChunk {
                payload: Some(snapshot_chunk::Payload::Manifest(m)),
            }) if m.tv_bytes > 0 => m,
            _ => {
                return Err(Status::invalid_argument(
                    "first SnapshotChunk must be a SnapshotManifest with tv_bytes > 0",
                ))
            }
        };

        if let Err(e) = Self::receive_image(&mut inbound, &manifest, &tmp_dir).await {
            let _ = tokio::fs::remove_dir_all(&tmp_dir).await;
            return Err(e);
        }

        let service = self.clone();
        let cleanup = tmp_dir.clone();
        let with_bm25 = manifest.bm25_bytes > 0;
        let result =
            tokio::task::spawn_blocking(move || service.apply_snapshot(&tmp_dir, with_bm25))
                .await
                .map_err(|e| Status::internal(format!("install task failed: {e}")))?;
        if result.is_err() {
            // Rejected AFTER receive (bad image, calibration mismatch):
            // leave no staging dir behind either.
            let _ = tokio::fs::remove_dir_all(&cleanup).await;
        }
        result.map(Response::new)
    }

    async fn add_documents(
        &self,
        request: Request<Streaming<AddDocumentsRequest>>,
    ) -> Result<Response<AddDocumentsResponse>, Status> {
        let _ingest = self.claim_ingest()?;
        let addr = self.config.analysis_addr.clone().ok_or_else(|| {
            Status::unavailable("no analysis sidecar configured for this shard (analysis_addr)")
        })?;
        let mut inbound = request.into_inner();
        let mut added = 0u64;
        let mut first_id = 0u64;
        // Analysis dominates bulk ingest. The preferred transport is one
        // AnalyzeStream for the whole call, paced by the sidecar's own
        // flow control; a sidecar that predates the RPC (UNIMPLEMENTED
        // on open) gets the previous pipelined-unary path. Either way,
        // documents are applied strictly in arrival order — ids and WAL
        // order stay deterministic.
        if let Some(first) = inbound.message().await? {
            match crate::analyzer::AnalyzeStream::open(&addr, first.analysis.as_ref()).await {
                Ok(session) => {
                    self.ingest_streamed(
                        session,
                        first,
                        &mut inbound,
                        &addr,
                        &mut added,
                        &mut first_id,
                    )
                    .await?;
                }
                Err(status) if status.code() == tonic::Code::Unimplemented => {
                    self.ingest_unary(first, &mut inbound, &addr, &mut added, &mut first_id)
                        .await?;
                }
                Err(status) => return Err(status),
            }
        }
        let total = self
            .state
            .read()
            .expect("shard state lock poisoned")
            .bm25
            .as_ref()
            .map_or(0, |b| b.doc_count());
        Ok(Response::new(AddDocumentsResponse {
            added,
            total,
            first_id,
        }))
    }

    async fn term_stats(
        &self,
        request: Request<TermStatsRequest>,
    ) -> Result<Response<TermStatsResponse>, Status> {
        let req = request.into_inner();
        let guard = self.state.read().expect("shard state lock poisoned");
        let (doc_count, total_doc_length, doc_frequencies) = match guard.bm25.as_ref() {
            Some(store) => {
                let index = store.as_index().ok_or_else(|| {
                    Status::failed_precondition("bm25 bulk build in progress; Flush first")
                })?;
                (
                    store.doc_count(),
                    index.total_doc_length(),
                    req.terms.iter().map(|t| index.df(t)).collect(),
                )
            }
            None => (0, 0, req.terms.iter().map(|_| 0).collect()),
        };
        Ok(Response::new(TermStatsResponse {
            doc_count,
            total_doc_length,
            doc_frequencies,
        }))
    }

    async fn bm25_query(
        &self,
        request: Request<Bm25QueryRequest>,
    ) -> Result<Response<Bm25QueryResponse>, Status> {
        let req = request.into_inner();
        if req.min_score.is_nan() || req.min_score == f32::NEG_INFINITY {
            return Err(Status::invalid_argument(
                "min_score must be finite (NaN and -inf are not valid floors)",
            ));
        }
        let params = Bm25Params {
            k1: if req.k1 == 0.0 {
                bm25::DEFAULT_K1
            } else {
                f64::from(req.k1)
            },
            b: if req.b == 0.0 {
                bm25::DEFAULT_B
            } else {
                f64::from(req.b)
            },
        };
        let stats = bm25::CorpusStats {
            doc_count: req.global_doc_count,
            total_doc_length: req.global_total_doc_length,
            dfs: req.global_doc_frequencies.clone(),
        };
        if req.terms.len() != stats.dfs.len() {
            return Err(Status::invalid_argument(
                "terms and global_doc_frequencies must have the same length",
            ));
        }
        let guard = self.state.read().expect("shard state lock poisoned");
        let hits = match guard.bm25.as_ref() {
            Some(store) if req.k > 0 => {
                let index = store.as_index().ok_or_else(|| {
                    Status::failed_precondition("bm25 bulk build in progress; Flush first")
                })?;
                // 0/absent means unseeded (scores are always positive).
                let floor = if req.min_score == 0.0 {
                    f64::NEG_INFINITY
                } else {
                    f64::from(req.min_score)
                };
                // Block-max path when every scored term has impacts (v5
                // shards) and the node flag allows it; the heap store,
                // v3/v4 files, and --block-max=false keep top_k with the
                // floor applied as a filter — same contract.
                let prunable = self.config.block_max
                    && req
                        .terms
                        .iter()
                        .enumerate()
                        .all(|(ti, t)| stats.dfs[ti] == 0 || index.has_impacts(t));
                let docs = if prunable {
                    bm25::top_k_pruned(index, &req.terms, &stats, params, req.k as usize, floor)
                } else {
                    bm25::filter_to_floor(
                        bm25::top_k(index, &req.terms, &stats, params, req.k as usize),
                        floor,
                    )
                };
                docs.into_iter()
                    .map(|doc| Bm25Hit {
                        doc_id: self.config.slot_offset + u64::from(doc.doc_id),
                        score: doc.score as f32,
                        terms: doc
                            .term_offsets
                            .into_iter()
                            .map(|(ti, offsets)| TermOccurrences {
                                term: req.terms[ti].clone(),
                                offsets: offsets
                                    .into_iter()
                                    .map(|(start, end)| OffsetSpan { start, end })
                                    .collect(),
                            })
                            .collect(),
                    })
                    .collect()
            }
            _ => Vec::new(),
        };
        // The shard's k-th best: one f32 ULP below the last hit's score
        // when the heap filled (so a later f32 seed never exceeds the
        // true k-th best — ties at the floor survive), 0 otherwise.
        let kth_best = if hits.len() == req.k as usize {
            hits.last().map(|h| bm25::floor_seed(h.score)).unwrap_or(0.0)
        } else {
            0.0
        };
        Ok(Response::new(Bm25QueryResponse { hits, kth_best }))
    }

    async fn bm25_rescore(
        &self,
        request: Request<Bm25RescoreRequest>,
    ) -> Result<Response<Bm25RescoreResponse>, Status> {
        let req = request.into_inner();
        if req.terms.len() != req.global_doc_frequencies.len() {
            return Err(Status::invalid_argument(
                "terms and global_doc_frequencies must have the same length",
            ));
        }
        let params = Bm25Params {
            k1: if req.k1 == 0.0 {
                bm25::DEFAULT_K1
            } else {
                f64::from(req.k1)
            },
            b: if req.b == 0.0 {
                bm25::DEFAULT_B
            } else {
                f64::from(req.b)
            },
        };
        let stats = bm25::CorpusStats {
            doc_count: req.global_doc_count,
            total_doc_length: req.global_total_doc_length,
            dfs: req.global_doc_frequencies.clone(),
        };
        let offset = self.config.slot_offset;
        let guard = self.state.read().expect("shard state lock poisoned");
        let hits = match guard.bm25.as_ref() {
            Some(store) => {
                // Route global ids to this shard's local range.
                let local: Vec<u32> = req
                    .candidate_ids
                    .iter()
                    .filter(|&&id| id >= offset && (id - offset) <= u64::from(u32::MAX))
                    .map(|id| (id - offset) as u32)
                    .collect();
                let index = store.as_index().ok_or_else(|| {
                    Status::failed_precondition("bm25 bulk build in progress; Flush first")
                })?;
                bm25::score_candidates(index, &req.terms, &stats, params, &local)
                    .into_iter()
                    .map(|doc| Bm25Hit {
                        doc_id: offset + u64::from(doc.doc_id),
                        score: doc.score as f32,
                        terms: doc
                            .term_offsets
                            .into_iter()
                            .map(|(ti, offsets)| TermOccurrences {
                                term: req.terms[ti].clone(),
                                offsets: offsets
                                    .into_iter()
                                    .map(|(start, end)| OffsetSpan { start, end })
                                    .collect(),
                            })
                            .collect(),
                    })
                    .collect()
            }
            None => Vec::new(),
        };
        Ok(Response::new(Bm25RescoreResponse { hits }))
    }

    async fn get_documents(
        &self,
        request: Request<GetDocumentsRequest>,
    ) -> Result<Response<GetDocumentsResponse>, Status> {
        let req = request.into_inner();
        let offset = self.config.slot_offset;
        let guard = self.state.read().expect("shard state lock poisoned");
        let mut documents = Vec::new();
        if let Some(store) = guard.bm25.as_ref() {
            let store = store.as_index().ok_or_else(|| {
                Status::failed_precondition("bm25 bulk build in progress; Flush first")
            })?;
            for id in req.doc_ids {
                if id < offset {
                    continue;
                }
                let local = (id - offset) as u32;
                if let Some(text) = store.text(local) {
                    documents.push(StoredDocument {
                        doc_id: id,
                        text,
                        lineage: store.lineage(local).map(|l| crate::pb::DocLineage {
                            opinion_id: l.opinion_id,
                            cluster_id: l.cluster_id,
                            span_start: l.span_start,
                            span_end: l.span_end,
                        }),
                    });
                }
            }
        }
        Ok(Response::new(GetDocumentsResponse { documents }))
    }

    async fn hybrid_shard(
        &self,
        request: Request<HybridShardRequest>,
    ) -> Result<Response<HybridShardResponse>, Status> {
        let service = self.clone();
        tokio::task::spawn_blocking(move || service.run_hybrid(request.into_inner()))
            .await
            .map_err(|e| Status::internal(format!("hybrid task failed: {e}")))?
            .map(Response::new)
    }

    async fn shard_legs(
        &self,
        request: Request<ShardLegsRequest>,
    ) -> Result<Response<ShardLegsResponse>, Status> {
        let req = request.into_inner();
        if req.terms.len() != req.global_doc_frequencies.len() {
            return Err(Status::invalid_argument(
                "terms and global_doc_frequencies must have the same length",
            ));
        }
        let service = self.clone();
        tokio::task::spawn_blocking(move || {
            let (vector_hits, bm25_hits) = service.compute_legs(
                &req.vector,
                &req.terms,
                req.global_doc_count,
                req.global_total_doc_length,
                &req.global_doc_frequencies,
                req.k as usize,
            )?;
            Ok(ShardLegsResponse {
                vector_hits: vector_hits
                    .into_iter()
                    .map(|(doc_id, score)| RawLegHit {
                        doc_id,
                        score: score as f32,
                    })
                    .collect(),
                bm25_hits: bm25_hits
                    .into_iter()
                    .map(|(doc_id, score)| RawLegHit {
                        doc_id,
                        score: score as f32,
                    })
                    .collect(),
            })
        })
        .await
        .map_err(|e| Status::internal(format!("shard legs task failed: {e}")))?
        .map(Response::new)
    }
}
