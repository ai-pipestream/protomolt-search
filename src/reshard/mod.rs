//! Offline resharding: replay a shard's WAL into N child images (split) or
//! several shards' WALs into one image (merge). This is the library half of
//! `examples/reshard.rs`; the integration test drives the same code.
//!
//! The WAL is pre-partitioned: every record was routed to
//! `bucket_of(id, bucket_count)` at write time, so a split with
//! `N <= bucket_count` (and `bucket_count % N == 0`) hands each child a
//! contiguous range of bucket files and never re-hashes a record. A
//! finer split (`N > bucket_count`) still works but re-partitions every
//! record — the bucket count caps CHEAP split granularity.
//!
//! Invariants (enforced here, documented in the README):
//!
//! - ONE VECTOR CONFIGURATION per split/merge: the WAL manifest must carry
//!   locked provider state, and merge requires byte-identical backend
//!   configurations AND identical bucket counts across all
//!   inputs. Provider state that cannot reproduce byte-comparable scores
//!   cannot be resharded. This is a hard error, as is installing an image
//!   whose scoring fingerprint differs from the active generation.
//! - Ids are GENERATION-SCOPED: records carry the global ids the server
//!   assigned; children re-assign dense local slots in original id order
//!   and get their slot base from the new shard map, so parent ids never
//!   leak into a child by accident. [`ChildImage::parent_ids`] is the
//!   explicit remap (child local slot -> parent global id).
//! - The split partition is hash-based and stable: id `v` goes to bucket
//!   `fnv1a64(v) >> (64 - log2(N))`, so every id lands in exactly one
//!   child and a re-split of a child bisects its range again.

mod compaction_build;
mod hash_split;
mod placement_split;
#[cfg(test)]
mod tests;
mod tree_split;

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use crate::analyzer::SessionLayers;
use crate::pb::wal::wal_record;
use crate::pb::{AddDocumentsRequest, AnalysisSpec};
use crate::postings::{AnalyzedDoc, AnalyzedField, SpillBuilder};
use crate::vector::VectorIndex;
use crate::wal::{self, RecordReader, WalManifest};

/// Text analyzer for rebuilding BM25 stores: a batch of (raw text, the
/// analysis spec it was ingested with) -> analyzed terms, one result
/// per input in order. The example wires this to the analysis sidecar
/// (the same one ingest used) with the batch's requests in flight
/// concurrently — replay throughput is bounded by sidecar round-trips,
/// exactly like ingest. Tests wire it to the mock sidecar. Split/merge
/// only re-partitions — term identity must be reproduced EXACTLY as at
/// ingest, hence the spec round-trip. Multi-field documents
/// (docs/multi-field.md) flatten into the batch as one entry per field
/// (body first, extras in record order); the caller reassembles.
pub type Analyzer<'a> = dyn FnMut(&[(&str, Option<&AnalysisSpec>, SessionLayers)]) -> Result<Vec<AnalyzedDoc>, String>
    + 'a;

/// Batch size handed to the analyzer (the analyzer's concurrency window).
const ANALYZE_BATCH: usize = 32;

/// FNV-1a 64 over the 8 little-endian bytes of an id (hand-rolled; no
/// dependency). Stable across platforms and runs — it is the split
/// partition function (and the WAL routing function), so it must never
/// change.
pub fn fnv1a64(id: u64) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325u64;
    for byte in id.to_le_bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0000_0100_0000_01b3);
    }
    hash
}

/// The bucket (child index) of a vector or document id for an N-way
/// partition. `n` must be a power of two; the top log2(n) bits of the
/// id's hash select the bucket. This is ALSO the WAL routing function:
/// bucket file `bucket-NNN.wal` holds exactly the records with
/// `bucket_of(id, bucket_count) == NNN`.
pub fn bucket_of(id: u64, n: usize) -> usize {
    assert!(n.is_power_of_two(), "split factor must be a power of two");
    // Top log2(1) = 0 bits select bucket 0. Spelled out because the
    // general expression would shift by 64, which panics in debug
    // builds and wraps to a shift of 0 in release: every id would
    // route to its own hash-numbered bucket, silently.
    if n == 1 {
        return 0;
    }
    let shift = 64 - n.trailing_zeros();
    (fnv1a64(id) >> shift) as usize
}

/// One built shard image: the opaque vector index, its optional `.bm25` sidecar,
/// and the shard-map metadata the coordinator needs to route to it.
#[derive(Debug)]
pub struct ChildImage {
    /// The written index file.
    pub vector_path: PathBuf,
    /// Product-owned original FP32 rows aligned with `vector_path`.
    pub exact_vector_path: PathBuf,
    /// The written BM25 sidecar (absent when the child received no
    /// documents).
    pub bm25_path: Option<PathBuf>,
    /// Global id base for this child in the NEW generation.
    pub slot_offset: u64,
    /// Inclusive hash-range bounds this child covers (see [`bucket_of`]).
    pub hash_lo: u64,
    pub hash_hi: u64,
    pub num_vectors: u64,
    pub num_documents: u64,
    /// Parent global id per child local vector slot, ascending (the id
    /// remap).
    pub parent_ids: Vec<u64>,
    /// Parent global id per child local ROW: `parent_ids` followed by the
    /// document-only rows (documents whose id named no vector), in
    /// parent-id order — the complete old-to-new id map of the child,
    /// which compaction extends as the tail applies.
    pub row_parent_ids: Vec<u64>,
}

/// The result of one split or merge: the images plus the NEW generation
/// number (one past the highest input generation) for the shard map.
#[derive(Debug)]
pub struct ReshardOutput {
    pub generation: u64,
    pub children: Vec<ChildImage>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WalCutoff {
    pub generation: u64,
    pub high_watermark: u64,
}

#[derive(Debug)]
pub struct StableReshardOutput {
    pub images: ReshardOutput,
    /// Durable source prefixes included in `images`, parallel to the input
    /// generation list. Child catch-up resumes strictly after these clocks.
    pub source_cutoffs: Vec<WalCutoff>,
}

/// One immutable segment emitted by bucket-bounded resharding. Several
/// segments may belong to one logical hash shard; [`crate::segments`] opens
/// and searches them as one snapshot with global BM25 statistics.
#[derive(Debug)]
pub struct SegmentedChildImage {
    pub logical_shard: usize,
    pub segment_ordinal: usize,
    pub physical_rows: u64,
    pub image: ChildImage,
}

/// Bucket-bounded reshard result. Memory holds at most one WAL bucket's live
/// replay plus one segment build, never a whole child or corpus.
#[derive(Debug)]
pub struct SegmentedReshardOutput {
    pub generation: u64,
    pub logical_shards: usize,
    pub segments: Vec<SegmentedChildImage>,
    pub peak_replay_rows: u64,
}

impl SegmentedReshardOutput {
    /// The images of one logical shard as a segmented output, in order.
    pub fn from_images(generation: u64, images: Vec<ChildImage>) -> Self {
        let peak_replay_rows = images
            .iter()
            .map(|image| image.num_vectors.max(image.num_documents))
            .max()
            .unwrap_or(0);
        Self {
            generation,
            logical_shards: 1,
            segments: images
                .into_iter()
                .enumerate()
                .map(|(ordinal, image)| SegmentedChildImage {
                    logical_shard: 0,
                    segment_ordinal: ordinal,
                    physical_rows: image.num_vectors.max(image.num_documents),
                    image,
                })
                .collect(),
            peak_replay_rows,
        }
    }
}

/// Vectors and documents replayed out of bucket files, keyed by their
/// server-assigned global ids (BTreeMap iteration IS id order).
#[derive(Default)]
struct Replay {
    /// global id -> one vector (`dim` floats).
    vectors: BTreeMap<u64, Vec<f32>>,
    /// global id -> the document as ingested.
    documents: BTreeMap<u64, AddDocumentsRequest>,
    /// Rows hidden by delete or replacement records. Build drops them,
    /// producing a dense all-live child generation.
    deleted: std::collections::BTreeSet<u64>,
    /// Stable product identities learned from routed WAL envelopes.
    stable_keys: BTreeMap<u64, Vec<u8>>,
    /// global id -> the document's analyzed fields, table order, when
    /// the replay carries a transplant sidecar
    /// (`docs/replay-from-segments.md`) and the child build takes the
    /// fields from here instead of the analyzer.
    analyzed: BTreeMap<u64, Vec<AnalyzedField>>,
}

impl Replay {
    fn compact(&mut self) {
        for id in &self.deleted {
            self.vectors.remove(id);
            self.documents.remove(id);
            self.stable_keys.remove(id);
            self.analyzed.remove(id);
        }
        self.deleted.clear();
    }
}

/// Resolve a `--log` argument: a generation directory is used directly; a
/// WAL directory contributes its newest generation (a snapshot install
/// supersedes earlier generations, so the latest one is the live log).
pub fn resolve_gen(path: &Path) -> Result<PathBuf, String> {
    if path.is_dir() {
        if wal::manifest_path(path).exists() {
            return Ok(path.to_path_buf());
        }
        wal::latest_gen(path)
            .map_err(|e| format!("list {}: {e}", path.display()))?
            .map(|(_, p)| p)
            .ok_or_else(|| format!("no WAL generation directories in {}", path.display()))
    } else {
        Err(format!(
            "{}: not a WAL directory or generation directory",
            path.display()
        ))
    }
}

fn read_gen_manifest(gen: &Path) -> Result<WalManifest, String> {
    wal::read_manifest(gen).map_err(|e| format!("read manifest of {}: {e}", gen.display()))
}

/// Replay the bucket files in `buckets` of one generation into `out`.
/// Every record must route to the file it was read from (the node routes
/// by the same [`bucket_of`]); a violation means the log is corrupt or
/// the bucket geometry changed mid-generation.
fn replay_buckets(
    gen: &Path,
    buckets: std::ops::Range<u32>,
    bucket_count: usize,
    dim: usize,
    vectors_only: bool,
    out: &mut Replay,
) -> Result<(), String> {
    replay_buckets_through(gen, buckets, bucket_count, dim, vectors_only, u64::MAX, out)
}

/// [`replay_buckets`] bounded by the generation clock: records past
/// `upto_clock` are skipped, which is how an online compaction builds
/// its dense image from a prefix of a log that keeps growing under it
/// (`docs/mutations.md`). A legacy unclocked record (clock 0) is refused
/// under a bound, because it cannot be placed against the cutoff.
fn replay_buckets_through(
    gen: &Path,
    buckets: std::ops::Range<u32>,
    bucket_count: usize,
    dim: usize,
    vectors_only: bool,
    upto_clock: u64,
    out: &mut Replay,
) -> Result<(), String> {
    replay_buckets_routed(
        gen,
        buckets,
        bucket_count,
        dim,
        vectors_only,
        upto_clock,
        true,
        out,
    )
}

/// [`replay_buckets_through`] with the hash-routing check optional: a
/// spill a re-placement split cut by a column
/// (`docs/replay-from-segments.md`) files records by value, not by id
/// hash, and is the split's to replay; a node's log is always checked.
#[allow(clippy::too_many_arguments)]
fn replay_buckets_routed(
    gen: &Path,
    buckets: std::ops::Range<u32>,
    bucket_count: usize,
    dim: usize,
    vectors_only: bool,
    upto_clock: u64,
    check_routing: bool,
    out: &mut Replay,
) -> Result<(), String> {
    for bucket in buckets {
        let path = wal::bucket_path(gen, bucket);
        if !path.exists() {
            continue; // bucket never received a record (files open lazily)
        }
        let mut reader =
            RecordReader::open(&path).map_err(|e| format!("open {}: {e}", path.display()))?;
        while let Some(record) = reader
            .next_record()
            .map_err(|e| format!("replay {}: {e}", path.display()))?
        {
            if upto_clock != u64::MAX {
                if record.clock == 0 {
                    return Err(format!(
                        "replay {}: a legacy unclocked record cannot be placed against the \
                         cutoff clock {upto_clock}",
                        path.display()
                    ));
                }
                if record.clock > upto_clock {
                    continue;
                }
            }
            match record.op {
                Some(wal_record::Op::AddVectors(a)) => {
                    if check_routing && bucket_of(a.first_id, bucket_count) as u32 != bucket {
                        return Err(format!(
                            "replay {}: record id {} routes to bucket {}, not {bucket} — \
                             corrupt log or changed bucket geometry",
                            path.display(),
                            a.first_id,
                            bucket_of(a.first_id, bucket_count)
                        ));
                    }
                    let batch = a.batch.unwrap_or_default();
                    if dim == 0 || !batch.vectors.len().is_multiple_of(dim) {
                        return Err(format!(
                            "replay {}: batch of {} floats is not a multiple of dim {dim}",
                            path.display(),
                            batch.vectors.len()
                        ));
                    }
                    for (i, vector) in batch.vectors.chunks_exact(dim).enumerate() {
                        if !a.stable_routing_keys.is_empty()
                            && a.stable_routing_keys.len() != batch.vectors.len() / dim
                        {
                            return Err(format!(
                                "replay {}: vector stable-key count does not match rows",
                                path.display()
                            ));
                        }
                        let id = a.first_id + i as u64;
                        if check_routing && bucket_of(id, bucket_count) as u32 != bucket {
                            return Err(format!(
                                "replay {}: vector batch straddles WAL buckets at id {id}",
                                path.display()
                            ));
                        }
                        if out.vectors.insert(id, vector.to_vec()).is_some() {
                            return Err(format!(
                                "replay {}: duplicate vector id {}",
                                path.display(),
                                id
                            ));
                        }
                        if let Some(key) = a.stable_routing_keys.get(i) {
                            match out.stable_keys.insert(id, key.clone()) {
                                Some(existing) if existing != *key => {
                                    return Err(format!(
                                    "replay {}: vector/document stable keys disagree for id {id}",
                                    path.display()
                                ))
                                }
                                _ => {}
                            }
                        }
                    }
                }
                Some(wal_record::Op::AddDocuments(a)) => {
                    if check_routing && bucket_of(a.first_id, bucket_count) as u32 != bucket {
                        return Err(format!(
                            "replay {}: document id {} routes to bucket {}, not {bucket}",
                            path.display(),
                            a.first_id,
                            bucket_of(a.first_id, bucket_count)
                        ));
                    }
                    if vectors_only {
                        continue;
                    }
                    if !a.stable_routing_keys.is_empty()
                        && a.stable_routing_keys.len() != a.documents.len()
                    {
                        return Err(format!(
                            "replay {}: document stable-key count does not match rows",
                            path.display()
                        ));
                    }
                    for (i, doc) in a.documents.into_iter().enumerate() {
                        let id = a.first_id + i as u64;
                        if check_routing && bucket_of(id, bucket_count) as u32 != bucket {
                            return Err(format!(
                                "replay {}: document batch straddles WAL buckets at id {id}",
                                path.display()
                            ));
                        }
                        if out.documents.insert(id, doc).is_some() {
                            return Err(format!(
                                "replay {}: duplicate document id {}",
                                path.display(),
                                id
                            ));
                        }
                        if let Some(key) = a.stable_routing_keys.get(i) {
                            match out.stable_keys.insert(id, key.clone()) {
                                Some(existing) if existing != *key => {
                                    return Err(format!(
                                    "replay {}: vector/document stable keys disagree for id {id}",
                                    path.display()
                                ))
                                }
                                _ => {}
                            }
                        }
                    }
                }
                Some(wal_record::Op::Bind(_)) => {
                    return Err(format!(
                        "replay {}: a binding record in a bucket file — binds route to \
                         markers.wal, so this log is corrupt or foreign",
                        path.display()
                    ));
                }
                Some(wal_record::Op::DeleteDocument(d)) => {
                    if bucket_of(d.doc_id, bucket_count) as u32 != bucket {
                        return Err(format!(
                            "replay {}: delete id {} routes to another bucket",
                            path.display(),
                            d.doc_id
                        ));
                    }
                    out.deleted.insert(d.doc_id);
                }
                Some(wal_record::Op::Replacement(r)) => {
                    if bucket_of(r.old_doc_id, bucket_count) as u32 != bucket {
                        return Err(format!(
                            "replay {}: replacement old id {} routes to another bucket",
                            path.display(),
                            r.old_doc_id
                        ));
                    }
                    out.deleted.insert(r.old_doc_id);
                }
                Some(wal_record::Op::Flush(_)) | Some(wal_record::Op::Snapshot(_)) | None => {}
            }
        }
    }
    Ok(())
}

/// Require usable, locked provider state in the manifest (see the module
/// docs for why incomplete provider state cannot be resharded).
fn require_backend_config(manifest: &WalManifest, what: &Path) -> Result<(), String> {
    let dim = manifest.dim as usize;
    let config = manifest.backend_config();
    let configured = config.as_ref().is_ok_and(|config| {
        crate::vector::legacy_calibration_config(config)
            .ok()
            .flatten()
            .is_none_or(|legacy| legacy.shift.len() == dim && legacy.scale.len() == dim)
    });
    if dim == 0 || !configured {
        return Err(format!(
            "{}: WAL manifest carries no usable locked vector backend configuration (dim {}); \
             resharding requires provider state that can reproduce byte-comparable shard scores",
            what.display(),
            manifest.dim
        ));
    }
    Ok(())
}

/// Require full history: a generation whose manifest records preexisting
/// state (an installed image, or logging enabled on an already-populated
/// shard) does not contain everything the shard holds, and a log-only
/// replay would silently drop that state. Hard error, like missing provider
/// configuration.
fn require_complete_history(manifest: &WalManifest, what: &Path) -> Result<(), String> {
    if manifest.preexisting_vectors > 0 || manifest.preexisting_documents > 0 {
        return Err(format!(
            "{}: generation {} began with {} preexisting vector(s) and {} preexisting \
             document(s) that are NOT in this log (an installed snapshot image, or logging \
             enabled after data existed); replaying it would drop them. Reshard from a \
             full-history generation, or rebuild the shard from source",
            what.display(),
            manifest.generation,
            manifest.preexisting_vectors,
            manifest.preexisting_documents
        ));
    }
    Ok(())
}

/// The mapped-plan binding carried by the input generations' markers
/// files (`docs/descriptor-mappings.md` section 4a): every Bind record
/// across every input must agree, because the children are ONE corpus
/// under ONE plan — contradictory bindings mean the inputs were mapped
/// under different plans and their columns must not merge. `None` when
/// no input was ever bound (hand-built columns bind nothing).
fn read_gens_binding(gens: &[PathBuf]) -> Result<Option<crate::postings::StoredBinding>, String> {
    let mut bound: Option<(crate::postings::StoredBinding, PathBuf)> = None;
    for gen in gens {
        let path = wal::markers_path(gen);
        if !path.exists() {
            continue;
        }
        let mut reader =
            RecordReader::open(&path).map_err(|e| format!("open {}: {e}", path.display()))?;
        while let Some(record) = reader
            .next_record()
            .map_err(|e| format!("replay {}: {e}", path.display()))?
        {
            let Some(wal_record::Op::Bind(bind)) = record.op else {
                continue;
            };
            let binding = crate::postings::StoredBinding {
                plan_fingerprint: bind.plan_fingerprint,
                body_path: bind.body_path,
                materialize_sha: bind.materialize_sha,
                analysis_sha: bind.analysis_sha,
                analysis_contract: bind.analysis_contract,
                vector_binding: bind.vector_binding,
                index_contract: bind.index_contract,
            };
            crate::mapped_analysis::decode_contract(
                &binding.analysis_sha,
                &binding.analysis_contract,
                &binding.body_path,
            )?;
            crate::mapped_vector::decode(&binding.vector_binding, &binding.plan_fingerprint)
                .map_err(|e| e.to_string())?;
            crate::index_contract::validate_binding(
                &binding.index_contract,
                &binding.plan_fingerprint,
                &binding.vector_binding,
            )
            .map_err(|e| e.to_string())?;
            match &bound {
                Some((first, first_gen)) if *first != binding => {
                    return Err(format!(
                        "{}: bound to plan {} but {} is bound to plan {}; the inputs were \
                         mapped under different plans and their columns must not combine",
                        path.display(),
                        binding.plan_fingerprint,
                        first_gen.display(),
                        first.plan_fingerprint
                    ));
                }
                Some(_) => {}
                None => bound = Some((binding, gen.clone())),
            }
        }
    }
    Ok(bound.map(|(binding, _)| binding))
}

/// The mapped-plan binding one generation's markers carry, if any
/// (`read_gens_binding` over one input).
pub fn read_generation_binding(
    gen: &Path,
) -> Result<Option<crate::postings::StoredBinding>, String> {
    read_gens_binding(std::slice::from_ref(&gen.to_path_buf()))
}

/// Provider scoring identity for the merge check (slot offset and generation
/// legitimately differ between inputs; the shape and provider state must not).
fn same_backend_config(a: &WalManifest, b: &WalManifest) -> bool {
    // One dataset, one provider state: generations of two collections
    // never merge, whatever their vectors look like (docs/collections.md).
    a.collection == b.collection
        && a.dim == b.dim
        && a.backend_config().ok() == b.backend_config().ok()
}

/// Build one child image from its share of the replay: the vector index
/// (constructed from manifest provider state, vectors in id order), the BM25
/// sidecar from the documents, and the two output files.
///
/// Document local ids mirror the vector side's dense remap: a document
/// whose id also names a vector gets that vector's child slot (the
/// aligned-ingest case stays aligned); documents with no vector (the doc
/// side ran ahead) are appended above the vector space in id order.
/// `columns` gives the image its column tables; without them the
/// tables are derived from the records.
#[allow(clippy::too_many_arguments)]
/// One child of a re-placement split (`docs/placement.md`, "Changing
/// the tree"): the leaf it serves under the NEW tree and the stable-key
/// hash range it takes inside that leaf (the full range on a one-shard
/// leaf).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeChild {
    /// Dotted leaf name under the new tree.
    pub leaf: String,
    /// The leaf's path code, which the child's rows carry after the
    /// split and the shard map names as `placement`.
    pub code: i64,
    pub hash_lo: u64,
    pub hash_hi: u64,
}

/// Built bucket images by plan position, handed from the sealing
/// threads to the appending one.
type BuiltBuckets = BTreeMap<usize, Result<(ChildImage, u64), String>>;

/// The bytes a spilled row costs the child build while its bucket is
/// replayed: the conservative end of the documented 40-70 KB band
/// (docs/replay-from-segments.md): the document text, the transplanted
/// spans and ordinals, the vector and the FP32 row. The build-memory
/// budget is checked against it.
pub const BUILD_BYTES_PER_ROW: u64 = 70 * 1024;

/// The result of a re-placement split: the images, the child list
/// (parallel to `images.children`), and what moved.
#[derive(Debug)]
pub struct TreeReshardOutput {
    /// One entry per child. Under [`TreeChildLayout::Segmented`] a child's
    /// `vector_path` is the index path a node serves (`<out>/shard-<i>.tv`,
    /// its catalog beside it under `.segments`), `bm25_path` is `None`, and
    /// the id maps are empty: the segments' own metadata carry the rows.
    /// Under [`TreeChildLayout::SingleImage`] the entries are the images
    /// as any split writes them.
    pub images: ReshardOutput,
    pub children: Vec<TreeChild>,
    /// Live documents whose code under the new tree differs from the
    /// code they carried (or that carried none).
    pub moved: u64,
    /// Live documents per child.
    pub placed: Vec<u64>,
    /// How the children were written.
    pub layout: TreeChildLayout,
    /// Sealed segments per child (1 per child under a single image).
    pub segments: Vec<usize>,
    /// The most live source rows any one replay held: one spill bucket
    /// under the segmented layout, one child under a single image.
    pub peak_replay_rows: u64,
    /// The bucket count the spill logs were written with.
    pub spill_bucket_count: u32,
    /// Where the documents' analyzed fields came from.
    pub source: TreeRowSource,
    /// How the spill was cut.
    pub cut: SpillCut,
    /// The largest transpose held at once, in bytes (0 under the log
    /// replay): one field of one source segment at a time is the bound.
    pub peak_transpose_bytes: u64,
    /// The derived-column declaration the children were written under
    /// (`docs/derived-columns.md`), as the shard map's `[[derived]]`
    /// table; empty without one.
    pub derived: Vec<crate::derived::DerivedColumnConfig>,
    /// Documents whose fields were transplanted (0 under the log replay).
    pub transplanted_rows: u64,
}

/// How a re-placement split writes each child.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TreeChildLayout {
    /// A segment catalog per child (`<out>/shard-<i>.tv.segments`,
    /// `docs/immutable-segments.md`): the child's spill log replays one
    /// WAL bucket at a time and each non-empty bucket seals as one
    /// segment, so memory is one bucket's rows plus one segment build,
    /// and a node starts on the child with `--index=<out>/shard-<i>.tv`.
    Segmented,
    /// One image per child, the way the other splits write: the child's
    /// rows are held together while the image is built. The caller states
    /// the most rows a child may have; a larger child is refused by name
    /// before any image is written.
    SingleImage { max_child_rows: u64 },
}

/// The knobs of a re-placement split.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TreeSplitOptions {
    pub layout: TreeChildLayout,
    /// log2 of the spill logs' bucket count; `None` takes the largest
    /// bucket count among the inputs, so a replay of one spill bucket is
    /// bounded the way a replay of one source bucket is. Ignored under
    /// a column cut, whose bucket count the cut plan sets.
    pub spill_bucket_bits: Option<u32>,
    /// Where each document's analyzed fields come from
    /// (`docs/replay-from-segments.md`).
    pub source: TreeRowSource,
    /// How each child's spill is cut into buckets, and so into segments.
    pub cut: SpillCut,
    /// Build only this child (by index in leaf order) and leave the
    /// others' catalogs unwritten: the routing pass still spills every
    /// child, so the rows and the map are the full split's, but a
    /// child whose build failed (an out-of-memory kill on the archive,
    /// 2026-09-06) is redone without the five that finished. Segmented
    /// layout only; a single-image split is refused by name.
    pub only_child: Option<usize>,
    /// The open-files limit the spill pass is checked against; `None`
    /// reads the process's own soft limit. A test sets a small number
    /// to see the refusal.
    pub open_files_limit: Option<u64>,
    /// The derived-column declaration the children are written under
    /// (`--derived-columns`, docs/derived-columns.md); `None` keeps the
    /// sources' own, from the log's manifest. With a declaration the
    /// sources' column tables must declare every input.
    pub derived: Option<std::sync::Arc<crate::derived::Declaration>>,
    /// Derived columns to compute on every row (`--derive=<name>`): a
    /// column added to the declaration, or one whose expression changed.
    /// Rows already derived under `derived` refuse; rows never derived
    /// need every declared column named (`Declaration::rederive`).
    pub derive: Vec<String>,
    /// Spill buckets built at once in a segmented child under
    /// `--from-segments` (`--build-threads`): each thread replays one
    /// bucket into memory and seals it, so the build's memory is this
    /// many buckets' replays; the segments append in bucket order and
    /// the catalog is the same as the one-thread build's, byte for
    /// byte. The log replay analyzes through one sidecar session and
    /// refuses more than one.
    pub build_threads: usize,
    /// Sealed-but-unappended bucket images a threaded build holds at
    /// once (`--build-queue`): a worker waits for backlog space before
    /// claiming its next bucket, so the build holds the threads'
    /// in-flight replays plus a backlog of at most this many finished
    /// images' id maps (plus one per thread when all finish at once).
    /// `None` takes the thread count; zero is refused.
    pub build_queue: Option<usize>,
    /// A fixed 70 KiB/row planning admission estimate in bytes
    /// (`--build-memory`, segmented layout only): after the routing
    /// pass, each child's spill counts at [`BUILD_BYTES_PER_ROW`] a row
    /// must fit it — the largest bucket alone, and the thread count
    /// times it. A plan that cannot fit is refused by name; neither
    /// the thread count nor the queue depth is lowered to make it fit.
    /// This is not an actual process-memory limit: wide values, queued id maps,
    /// and page cache are outside the estimate. `None` enforces no admission check.
    pub build_memory: Option<u64>,
}

impl Default for TreeSplitOptions {
    fn default() -> Self {
        Self {
            layout: TreeChildLayout::Segmented,
            spill_bucket_bits: None,
            source: TreeRowSource::Logs,
            cut: SpillCut::Hash,
            only_child: None,
            open_files_limit: None,
            derived: None,
            derive: Vec::new(),
            build_threads: 1,
            build_queue: None,
            build_memory: None,
        }
    }
}

/// The children a tree implies: `shards` per leaf, in leaf (code) order,
/// each leaf's shards tiling the stable-key hash space the way the
/// coordinator routes inside a leaf (`route_stable_key_in`). A leaf's
/// shard count must be a power of two for the tiles to be exact.
pub fn tree_children(placement: &crate::placement::Placement) -> Result<Vec<TreeChild>, String> {
    let mut out = Vec::new();
    for leaf in placement.leaves() {
        let n = leaf.shards.max(1) as usize;
        if !n.is_power_of_two() {
            return Err(format!(
                "placement leaf {:?} has {n} shards; a re-placement split tiles a leaf's hash \
                 space in powers of two",
                leaf.name
            ));
        }
        let shift = if n == 1 { 0 } else { 64 - n.trailing_zeros() };
        for shard in 0..n {
            let hash_lo = if n == 1 { 0 } else { (shard as u64) << shift };
            let hash_hi = if shard + 1 == n {
                u64::MAX
            } else {
                ((shard as u64 + 1) << shift) - 1
            };
            out.push(TreeChild {
                leaf: leaf.name.clone(),
                code: leaf.code,
                hash_lo,
                hash_hi,
            });
        }
    }
    Ok(out)
}
/// The live source rows a replay holds (vectors and documents by id).
fn live_rows(replay: &Replay) -> u64 {
    replay
        .vectors
        .keys()
        .chain(replay.documents.keys())
        .collect::<BTreeSet<_>>()
        .len() as u64
}

/// The slots an image takes.
fn image_rows(image: &ChildImage) -> u64 {
    image.num_vectors.max(image.num_documents)
}

/// A child's slots must end at or before the next child's offset.
fn check_slot_bound(
    index: usize,
    leaf: &str,
    base: u64,
    rows: u64,
    bound: Option<u64>,
) -> Result<(), String> {
    let end = base
        .checked_add(rows)
        .ok_or_else(|| "re-placement split slot overflow".to_string())?;
    if let Some(bound) = bound {
        if end > bound {
            return Err(format!(
                "child {index} ({leaf}) needs slots through {end}, past the next child's offset \
                 {bound}; widen the slot stride"
            ));
        }
    }
    Ok(())
}

/// The shard map for a re-placement split: the new tree under
/// `[placement]`, one `[[shards]]` per child with its code and hash
/// range, addresses left for the operator.
pub fn tree_shard_map_toml(
    out: &TreeReshardOutput,
    tree: &crate::placement::PlacementTreeConfig,
) -> Result<String, String> {
    let map = crate::config::ShardMap {
        generation: out.images.generation,
        shards: out
            .images
            .children
            .iter()
            .zip(&out.children)
            .map(|(image, child)| crate::config::ShardMapShard {
                addr: "TODO".to_string(),
                replica: None,
                slot_offset: image.slot_offset,
                hash_lo: Some(image.hash_lo),
                hash_hi: Some(image.hash_hi),
                placement: Some(child.code as u64),
            })
            .collect(),
        placement: Some(tree.clone()),
        derived: out.derived.clone(),
    };
    toml::to_string(&map).map_err(|error| format!("render shard map: {error}"))
}
/// The layout a partitioned compaction builds (docs/immutable-segments.md
/// "Partitioned layout"): live rows ordered by an integer column and cut
/// into segments of at most `bound` rows.
#[derive(Debug, Clone, Copy)]
pub struct PartitionSpec<'a> {
    /// An integer column of the shard (timestamps included).
    pub column: &'a str,
    /// The most rows one output segment may hold.
    pub bound: usize,
}

/// The column tables a compacted image must declare, in the shard's
/// order: the live shard's own, so an output that holds no record of a
/// column still declares it and opens under the same node configuration
/// (a segment's tables must equal the tail's). Without them an image
/// derives its tables from the records it holds.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ColumnTables {
    pub facets: Vec<String>,
    pub numerics: Vec<String>,
    pub map_facets: Vec<String>,
    pub map_numerics: Vec<String>,
    pub map_integers: Vec<String>,
    pub map_unsigned_integers: Vec<String>,
    pub integers: Vec<String>,
    pub unsigned_integers: Vec<String>,
    pub geo: Vec<String>,
}

// ---------------------------------------------------------------------
// Replay from segments (docs/replay-from-segments.md)
// ---------------------------------------------------------------------

/// Where a re-placement split takes each document's analyzed fields.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum TreeRowSource {
    /// Replay the logs and analyze each document's text again.
    #[default]
    Logs,
    /// Read the sealed segments beside each log: the postings transposed
    /// per document, the columns, text, vectors and identities copied,
    /// the analyzer not called.
    Segments,
}

/// How a child's spill is cut into buckets, and so into segments.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub enum SpillCut {
    /// By id hash, one bucket per source bucket: each segment then
    /// spans the column's range and a partitioned compaction follows.
    #[default]
    Hash,
    /// By the integer column's value in ascending order, at most
    /// `rows_per_cut` rows per cut, the rows without the column last:
    /// the child comes out partitioned by the column.
    Column { column: String, rows_per_cut: u64 },
}

/// One row's analyzed fields in the spill's analysis sidecar.
fn write_analysis_entry(
    out: &mut impl std::io::Write,
    id: u64,
    fields: &[AnalyzedField],
) -> std::io::Result<()> {
    out.write_all(&id.to_le_bytes())?;
    out.write_all(&(fields.len() as u32).to_le_bytes())?;
    for field in fields {
        out.write_all(&field.length.to_le_bytes())?;
        out.write_all(&[
            u8::from(field.positions.is_some()),
            u8::from(field.sentences.is_some()),
        ])?;
        out.write_all(&(field.terms.len() as u32).to_le_bytes())?;
        for (i, (term, tf, spans)) in field.terms.iter().enumerate() {
            let bytes = term.as_bytes();
            let len = u16::try_from(bytes.len()).map_err(|_| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "term longer than 65535 bytes",
                )
            })?;
            out.write_all(&len.to_le_bytes())?;
            out.write_all(bytes)?;
            out.write_all(&tf.to_le_bytes())?;
            out.write_all(&(spans.len() as u32).to_le_bytes())?;
            for (start, end) in spans {
                out.write_all(&start.to_le_bytes())?;
                out.write_all(&end.to_le_bytes())?;
            }
            if let Some(positions) = &field.positions {
                let ordinals = positions.get(i).ok_or_else(|| {
                    std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "positions table shorter than the term table",
                    )
                })?;
                if ordinals.len() != spans.len() {
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::InvalidData,
                        "positions count differs from occurrence count",
                    ));
                }
                for ordinal in ordinals {
                    out.write_all(&ordinal.to_le_bytes())?;
                }
            }
        }
        if let Some(sentences) = &field.sentences {
            out.write_all(&(sentences.len() as u32).to_le_bytes())?;
            for (start, end) in sentences {
                out.write_all(&start.to_le_bytes())?;
                out.write_all(&end.to_le_bytes())?;
            }
        }
    }
    Ok(())
}

/// The sidecar of one spill bucket, keyed by source id.
fn read_analysis_file(path: &Path) -> Result<BTreeMap<u64, Vec<AnalyzedField>>, String> {
    use std::io::Read;
    let mut bytes = Vec::new();
    match std::fs::File::open(path) {
        Ok(mut file) => file
            .read_to_end(&mut bytes)
            .map_err(|e| format!("read {}: {e}", path.display()))?,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(BTreeMap::new()),
        Err(e) => return Err(format!("open {}: {e}", path.display())),
    };
    let mut at = 0usize;
    let take = |at: &mut usize, n: usize| -> Result<&[u8], String> {
        let end = at
            .checked_add(n)
            .filter(|end| *end <= bytes.len())
            .ok_or_else(|| format!("{}: truncated analysis sidecar", path.display()))?;
        let out = &bytes[*at..end];
        *at = end;
        Ok(out)
    };
    let u32_at = |at: &mut usize| -> Result<u32, String> {
        Ok(u32::from_le_bytes(
            take(at, 4)?.try_into().expect("four bytes"),
        ))
    };
    let mut out = BTreeMap::new();
    while at < bytes.len() {
        let id = u64::from_le_bytes(take(&mut at, 8)?.try_into().expect("eight bytes"));
        let n_fields = u32_at(&mut at)? as usize;
        let mut fields = Vec::with_capacity(n_fields);
        for _ in 0..n_fields {
            let length = u32_at(&mut at)?;
            let flags = take(&mut at, 2)?;
            let (has_positions, has_sentences) = (flags[0] != 0, flags[1] != 0);
            let n_terms = u32_at(&mut at)? as usize;
            let mut terms = Vec::with_capacity(n_terms);
            let mut positions = has_positions.then(|| Vec::with_capacity(n_terms));
            for _ in 0..n_terms {
                let len =
                    u16::from_le_bytes(take(&mut at, 2)?.try_into().expect("two bytes")) as usize;
                let term = std::str::from_utf8(take(&mut at, len)?)
                    .map_err(|e| format!("{}: term is not UTF-8: {e}", path.display()))?
                    .to_string();
                let tf = u32_at(&mut at)?;
                let n_occ = u32_at(&mut at)? as usize;
                let mut spans = Vec::with_capacity(n_occ);
                for _ in 0..n_occ {
                    let start = u32_at(&mut at)?;
                    let end = u32_at(&mut at)?;
                    spans.push((start, end));
                }
                if let Some(positions) = positions.as_mut() {
                    let mut ordinals = Vec::with_capacity(n_occ);
                    for _ in 0..n_occ {
                        ordinals.push(u32_at(&mut at)?);
                    }
                    positions.push(ordinals);
                }
                terms.push((term, tf, spans));
            }
            let sentences = if has_sentences {
                let n = u32_at(&mut at)? as usize;
                let mut table = Vec::with_capacity(n);
                for _ in 0..n {
                    let start = u32_at(&mut at)?;
                    let end = u32_at(&mut at)?;
                    table.push((start, end));
                }
                Some(table)
            } else {
                None
            };
            fields.push(AnalyzedField {
                terms,
                length,
                positions,
                sentences,
            });
        }
        if out.insert(id, fields).is_some() {
            return Err(format!(
                "{}: source id {id} has two analysis entries",
                path.display()
            ));
        }
    }
    Ok(out)
}

/// The sidecar file of spill bucket `bucket` under a child's spill dir.
fn analysis_sidecar_path(spill_dir: &Path, bucket: u32) -> PathBuf {
    spill_dir.join(format!("analysis-{bucket:05}.bin"))
}

/// Ask the kernel to drop the page cache of a file this pass is done
/// with (`posix_fadvise` DONTNEED). A split reads hundreds of gigabytes
/// of segments once and writes as much spill and output; without this
/// the run's cache footprint is that whole volume, taken from the nodes
/// serving beside it, and its resident set with it. Advice only: dirty
/// pages stay until written back, and the run's result does not depend
/// on it. Off the network build (no libc) it does nothing.
#[cfg(all(feature = "net", target_os = "linux"))]
fn drop_page_cache(path: &Path) {
    use std::os::unix::io::AsRawFd;
    if let Ok(file) = std::fs::File::open(path) {
        // SAFETY: a plain advisory call on an open descriptor.
        unsafe {
            libc::posix_fadvise(file.as_raw_fd(), 0, 0, libc::POSIX_FADV_DONTNEED);
        }
    }
}

#[cfg(not(all(feature = "net", target_os = "linux")))]
fn drop_page_cache(_path: &Path) {}

/// [`drop_page_cache`] over every file of a sealed segment's directory.
fn drop_segment_cache(root: &Path, segment_id: &str) {
    let dir = crate::segments::SegmentCatalog::segment_dir(root, segment_id);
    if let Ok(entries) = std::fs::read_dir(&dir) {
        for entry in entries.flatten() {
            drop_page_cache(&entry.path());
        }
    }
}

/// One source shard's catalog beside its log, with the rows its shard
/// overlay and its log have deleted since the segments sealed.
pub(crate) struct SegmentSourceShard {
    pub(crate) gen: PathBuf,
    pub(crate) root: PathBuf,
    pub(crate) set: std::sync::Arc<crate::segments::OpenedSegmentSet>,
    /// The shard-wide live-document overlay (`<index>.tv.live`), when
    /// written: a delete after a seal lands there, not in the segment's
    /// own sidecar.
    pub(crate) overlay: Option<crate::live_docs::LiveDocs>,
    /// Rows the log deletes or replaces, as local labels.
    pub(crate) deleted: BTreeSet<u64>,
    /// The shard's slot offset: a global id is the offset plus the label.
    pub(crate) slot_offset: u64,
}

impl SegmentSourceShard {
    /// Whether shard-local row `label` (segment `i`, row `row` in it) is
    /// live: the segment's sidecar, the shard overlay, and the log agree.
    pub(crate) fn is_live(&self, i: usize, row: u32, label: u64) -> bool {
        if self.set.live_docs(i).is_deleted(row as usize) {
            return false;
        }
        if let Some(overlay) = &self.overlay {
            if overlay.is_deleted(label as usize) {
                return false;
            }
        }
        !self.deleted.contains(&label)
    }
}

/// The tables every source shares, pinned on the children.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SourceTables {
    pub(crate) fields: Vec<String>,
    pub(crate) fingerprints: Vec<u64>,
    pub(crate) position_fields: Vec<String>,
    pub(crate) sentence_fields: Vec<String>,
    pub(crate) columns: ColumnTables,
}

/// The exact-vector sidecar of segment `i`, when the segment has vectors.
pub(crate) fn open_segment_exact(
    shard: &SegmentSourceShard,
    i: usize,
) -> Result<Option<crate::exact_vectors::ExactVectorStore>, String> {
    let meta = shard.set.metadata(i);
    if shard.set.vector(i).is_none() {
        return Ok(None);
    }
    if meta.exact_vectors.file.is_empty() {
        return Err(format!(
            "{} segment {}: has a vector image but no exact-vector sidecar; a transplant copies \
             FP32 rows, rebuild or backfill the source",
            shard.root.display(),
            meta.segment_id
        ));
    }
    let path = crate::segments::SegmentCatalog::segment_dir(&shard.root, &meta.segment_id)
        .join(&meta.exact_vectors.file);
    let store = crate::exact_vectors::ExactVectorStore::open(&path)
        .map_err(|e| format!("open {}: {e}", path.display()))?;
    if store.len() as u64 != meta.rows {
        return Err(format!(
            "{} segment {}: exact sidecar has {} rows, the segment {}",
            shard.root.display(),
            meta.segment_id,
            store.len(),
            meta.rows
        ));
    }
    Ok(Some(store))
}
pub use compaction_build::{
    compact_log, compact_log_partitioned, compact_segments_partitioned, CompactedRow,
    CompactionBuild, RowSink, SegmentCompactionBuild, SegmentCompactionOptions,
};
pub use hash_split::{
    merge, open_files_soft_limit, raise_open_files_limit, shard_map_toml, shards_toml, split,
    split_logs, split_logs_segmented, split_stable_logs, split_stable_logs_ranged,
};
pub(crate) use hash_split::{open_segment_sources, reconstruct_document, segment_tables};
pub use placement_split::{
    manifest_derived, placement_shard_map_toml, split_placement_logs, validate_placement_children,
    PlacementChild, PlacementReshardOutput,
};
pub use tree_split::split_placement_tree_logs;
