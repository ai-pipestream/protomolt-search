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
fn build_child(
    manifest: &WalManifest,
    vectors: Vec<(u64, Vec<f32>)>,
    documents: Vec<(u64, AddDocumentsRequest)>,
    vector_path: &Path,
    bm25_fields: Option<&[String]>,
    pinned_fingerprints: Option<&[u64]>,
    binding: Option<&crate::postings::StoredBinding>,
    columns: Option<&ColumnTables>,
    mut transplanted: Option<BTreeMap<u64, Vec<AnalyzedField>>>,
    analyze: &mut Analyzer,
) -> Result<ChildImage, String> {
    let dim = manifest.dim as usize;
    let backend_config = manifest
        .backend_config()
        .map_err(|e| format!("construct child backend config: {e}"))?;
    let mut index = VectorIndex::from_backend_config(dim, &backend_config)
        .map_err(|e| format!("construct child index: {e}"))?;
    let mut parent_ids = Vec::with_capacity(vectors.len());
    let mut flat = Vec::with_capacity(vectors.len() * dim);
    for (id, vector) in &vectors {
        parent_ids.push(*id);
        flat.extend_from_slice(vector);
    }
    index
        .add(&flat, dim)
        .map_err(|e| format!("add child vectors: {e}"))?;
    index
        .prepare()
        .map_err(|e| format!("prepare child index: {e}"))?;
    index
        .write(vector_path)
        .map_err(|e| format!("write {}: {e}", vector_path.display()))?;
    let exact_vector_path = crate::node::exact_vector_sidecar_path(vector_path);
    crate::exact_vectors::ExactVectorStore::from_values(dim, flat)
        .and_then(|store| store.write(&exact_vector_path))
        .map_err(|e| format!("write {}: {e}", exact_vector_path.display()))?;

    // parent id -> child local slot, for the document remap.
    let slot_of: BTreeMap<u64, u32> = parent_ids
        .iter()
        .enumerate()
        .map(|(slot, &id)| (id, slot as u32))
        .collect();
    let mut mapped: Vec<(u32, AddDocumentsRequest)> = Vec::with_capacity(documents.len());
    let mut row_parent_ids = parent_ids.clone();
    let mut parent_of_local: BTreeMap<u32, u64> = BTreeMap::new();
    let mut spare = vectors.len() as u64;
    for (id, doc) in documents {
        let local = match slot_of.get(&id) {
            Some(&slot) => slot,
            None => {
                spare += 1;
                row_parent_ids.push(id);
                u32::try_from(spare - 1).map_err(|_| "document id space exceeds u32".to_string())?
            }
        };
        parent_of_local.insert(local, id);
        mapped.push((local, doc));
    }
    mapped.sort_by_key(|(local, _)| *local);
    let num_documents = mapped.len() as u64;

    let mut bm25_path = None;
    if !mapped.is_empty() {
        // The child field table (docs/multi-field.md): the caller's
        // fleet table when given, else "body" plus every extra field
        // name in this child's records in canonical lexical order. First-sight
        // order is not sufficient: a sparse optional field can first appear
        // after an always-present phrase field in one hash child and before it
        // in another, producing incompatible positional schemas. Old logs
        // carry no extra fields and derive the single-field table.
        let analysis_contract = match binding {
            Some(binding) => crate::mapped_analysis::decode_contract(
                &binding.analysis_sha,
                &binding.analysis_contract,
                &binding.body_path,
            )?,
            None => crate::pb::MappedAnalysisContract::default(),
        };
        let table: Vec<String> = match bm25_fields {
            Some(t) => t.to_vec(),
            None => {
                let mut extras = BTreeSet::new();
                for (_, doc) in &mapped {
                    for f in &doc.fields {
                        extras.insert(f.field.clone());
                    }
                    for phrase in &doc.phrases {
                        extras.insert(phrase.field.clone());
                    }
                    if !doc.phrase_field.is_empty() {
                        extras.insert(doc.phrase_field.clone());
                    }
                    for bigram in &doc.bigram_fields {
                        extras.insert(bigram.field.clone());
                    }
                }
                extras.extend(
                    analysis_contract
                        .fields
                        .iter()
                        .map(|field| field.name.clone()),
                );
                extras.remove("body");
                let mut t = vec!["body".to_string()];
                t.extend(extras);
                t
            }
        };
        if table.first().map(String::as_str) != Some("body") {
            return Err("bm25 field table must start with \"body\"".to_string());
        }
        // The child's facet and numeric tables are derived from the
        // replayed records (first-seen order): the WAL is the durable
        // column record, so a field no record values never reaches the
        // child.
        let facet_table: Vec<String> = match columns {
            Some(columns) => columns.facets.clone(),
            None => {
                let mut t: Vec<String> = Vec::new();
                for (_, doc) in &mapped {
                    for fv in &doc.facets {
                        if !t.iter().any(|n| n == &fv.field) {
                            t.push(fv.field.clone());
                        }
                    }
                }
                t
            }
        };
        let numeric_table: Vec<String> = match columns {
            Some(columns) => columns.numerics.clone(),
            None => {
                let mut t: Vec<String> = Vec::new();
                for (_, doc) in &mapped {
                    for nv in &doc.numerics {
                        if !t.iter().any(|n| n == &nv.field) {
                            t.push(nv.field.clone());
                        }
                    }
                }
                t
            }
        };
        let map_facet_table: Vec<String> = match columns {
            Some(columns) => columns.map_facets.clone(),
            None => {
                let mut t: Vec<String> = Vec::new();
                for (_, doc) in &mapped {
                    for e in &doc.map_facets {
                        if !t.iter().any(|n| n == &e.field) {
                            t.push(e.field.clone());
                        }
                    }
                }
                t
            }
        };
        let map_numeric_table: Vec<String> = match columns {
            Some(columns) => columns.map_numerics.clone(),
            None => {
                let mut t: Vec<String> = Vec::new();
                for (_, doc) in &mapped {
                    for e in &doc.map_numerics {
                        if !t.iter().any(|n| n == &e.field) {
                            t.push(e.field.clone());
                        }
                    }
                }
                t
            }
        };
        let map_integer_table: Vec<String> = match columns {
            Some(columns) => columns.map_integers.clone(),
            None => {
                let mut t: Vec<String> = Vec::new();
                for (_, doc) in &mapped {
                    for e in &doc.map_integers {
                        if !t.iter().any(|n| n == &e.field) {
                            t.push(e.field.clone());
                        }
                    }
                }
                t
            }
        };
        let map_unsigned_integer_table: Vec<String> = match columns {
            Some(columns) => columns.map_unsigned_integers.clone(),
            None => {
                let mut t: Vec<String> = Vec::new();
                for (_, doc) in &mapped {
                    for e in &doc.map_unsigned_integers {
                        if !t.iter().any(|n| n == &e.field) {
                            t.push(e.field.clone());
                        }
                    }
                }
                t
            }
        };
        // Integers and timestamps name the SAME i64 columns
        // (docs/range-facets.md), so both lists feed one table — a
        // child whose records only ever carried timestamps still gets
        // the column, or the re-apply below would have nowhere to put
        // them.
        let integer_table: Vec<String> = match columns {
            Some(columns) => columns.integers.clone(),
            None => {
                let mut t: Vec<String> = Vec::new();
                for (_, doc) in &mapped {
                    for name in doc
                        .integers
                        .iter()
                        .map(|e| &e.field)
                        .chain(doc.timestamps.iter().map(|e| &e.field))
                    {
                        if !t.iter().any(|n| n == name) {
                            t.push(name.clone());
                        }
                    }
                }
                t
            }
        };
        let unsigned_integer_table: Vec<String> = match columns {
            Some(columns) => columns.unsigned_integers.clone(),
            None => {
                let mut names = Vec::new();
                for (_, doc) in &mapped {
                    for value in &doc.unsigned_integers {
                        if !names.contains(&value.field) {
                            names.push(value.field.clone());
                        }
                    }
                }
                names
            }
        };
        // Geo columns come from one list, so the derivation is the
        // plain first-seen union (docs/geo-columns.md).
        let geo_table: Vec<String> = match columns {
            Some(columns) => columns.geo.clone(),
            None => {
                let mut t: Vec<String> = Vec::new();
                for (_, doc) in &mapped {
                    for e in &doc.geo_points {
                        if !t.iter().any(|n| n == &e.field) {
                            t.push(e.field.clone());
                        }
                    }
                }
                t
            }
        };
        // Children rebuild through the disk spiller for the same reason
        // nodes do: a full-scale child's postings do not fit in heap.
        let path = crate::node::bm25_sidecar_path(vector_path);
        let mut spill_dir = path.as_os_str().to_owned();
        spill_dir.push(".build");
        let spill_dir = std::path::PathBuf::from(spill_dir);
        let names: Vec<&str> = table.iter().map(String::as_str).collect();
        let facet_names: Vec<&str> = facet_table.iter().map(String::as_str).collect();
        let numeric_names: Vec<&str> = numeric_table.iter().map(String::as_str).collect();
        let map_facet_names: Vec<&str> = map_facet_table.iter().map(String::as_str).collect();
        let map_numeric_names: Vec<&str> = map_numeric_table.iter().map(String::as_str).collect();
        let map_integer_names: Vec<&str> = map_integer_table.iter().map(String::as_str).collect();
        let map_unsigned_integer_names: Vec<&str> = map_unsigned_integer_table
            .iter()
            .map(String::as_str)
            .collect();
        let integer_names: Vec<&str> = integer_table.iter().map(String::as_str).collect();
        let unsigned_integer_names: Vec<&str> =
            unsigned_integer_table.iter().map(String::as_str).collect();
        let geo_names: Vec<&str> = geo_table.iter().map(String::as_str).collect();
        // The child's positional fields (docs/phrase-proximity.md) come
        // from the records' proximity record, which every record of one
        // generation carries identically; a field it names outside the
        // table is a corrupt record, refused.
        let position_table: Vec<String> = {
            let mut t = BTreeSet::new();
            for (local, doc) in &mapped {
                for name in &doc.position_fields {
                    if !table.contains(name) {
                        return Err(format!(
                            "record at child slot {local} keeps positions on {name:?} outside the \
                             table {table:?}"
                        ));
                    }
                    t.insert(name.clone());
                }
            }
            t.into_iter().collect()
        };
        let position_names: Vec<&str> = position_table.iter().map(String::as_str).collect();
        // The child's sentence fields (docs/highlighting.md), by the same
        // rule.
        let sentence_table: Vec<String> = {
            let mut t = BTreeSet::new();
            for (local, doc) in &mapped {
                for name in &doc.sentence_fields {
                    if !table.contains(name) {
                        return Err(format!(
                            "record at child slot {local} keeps sentence spans on {name:?} \
                             outside the table {table:?}"
                        ));
                    }
                    t.insert(name.clone());
                }
            }
            t.into_iter().collect()
        };
        let sentence_names: Vec<&str> = sentence_table.iter().map(String::as_str).collect();
        let mut builder = SpillBuilder::create_with_fields(&spill_dir, &names)
            .map_err(|e| format!("spill dir {}: {e}", spill_dir.display()))?
            .with_facet_fields(&facet_names)
            .with_numeric_fields(&numeric_names)
            .with_map_facet_fields(&map_facet_names)
            .with_map_numeric_fields(&map_numeric_names)
            .with_map_integer_fields(&map_integer_names)
            .with_map_unsigned_integer_fields(&map_unsigned_integer_names)
            .with_integer_fields(&integer_names)
            .with_unsigned_integer_fields(&unsigned_integer_names)
            .with_geo_fields(&geo_names)
            .with_position_fields(&position_names)
            .with_sentence_fields(&sentence_names);
        // A compaction pins the source shard's per-field analyzer
        // fingerprints on every output up front, so a bucket whose rows
        // never carry an optional field still records the field's
        // identity and the outputs open as one set; a record that
        // contradicts a pinned fingerprint refuses below as it would at
        // ingest.
        if let Some(pinned) = pinned_fingerprints {
            if pinned.len() != table.len() {
                return Err(format!(
                    "pinned analysis fingerprints cover {} fields but the table has {}",
                    pinned.len(),
                    table.len()
                ));
            }
            for (fi, fingerprint) in pinned.iter().enumerate() {
                builder
                    .set_analysis_fingerprint(fi, *fingerprint)
                    .map_err(|error| format!("pin field {:?}: {error}", table[fi]))?;
            }
        }
        for field in &analysis_contract.fields {
            let fi = table
                .iter()
                .position(|name| name == &field.name)
                .ok_or_else(|| {
                    format!(
                        "bound analysis field {:?} is absent from child table",
                        field.name
                    )
                })?;
            builder.set_analysis_fingerprint(
                fi,
                crate::analyzer::analysis_fingerprint(field.analysis.as_ref()),
            )?;
        }
        let mut i = 0;
        while i < mapped.len() {
            // Batch by document, one analyzer entry per field (body
            // first, extras in record order), batches aligned to
            // document boundaries so reassembly is positional.
            let mut end = i;
            let mut entries = 0usize;
            while end < mapped.len() && (entries == 0 || entries < ANALYZE_BATCH) {
                entries += 1 + mapped[end].1.fields.len();
                end += 1;
            }
            let analyzed = if transplanted.is_some() {
                Vec::new()
            } else {
                let mut batch: Vec<(&str, Option<&AnalysisSpec>, SessionLayers)> =
                    Vec::with_capacity(entries);
                for (_, d) in &mapped[i..end] {
                    // The layers each text needs on replay: sentence
                    // spans for a sentence field, and the cased identity
                    // for the body when the record names a cased field.
                    batch.push((
                        d.text.as_str(),
                        d.analysis.as_ref(),
                        SessionLayers {
                            sentences: !d.sentence_fields.is_empty(),
                            dual_cased: !d.cased_field.is_empty(),
                            ..SessionLayers::default()
                        },
                    ));
                    for f in &d.fields {
                        batch.push((
                            f.text.as_str(),
                            f.analysis.as_ref(),
                            SessionLayers {
                                sentences: d.sentence_fields.iter().any(|n| n == &f.field),
                                ..SessionLayers::default()
                            },
                        ));
                    }
                }
                analyze(&batch)
                    .map_err(|e| format!("analyze batch at child slot {}: {e}", mapped[i].0))?
            };
            if transplanted.is_none() && analyzed.len() != entries {
                return Err(format!(
                    "analyzer returned {} results for {entries} field texts",
                    analyzed.len()
                ));
            }
            let mut results = analyzed.into_iter();
            for (local, doc) in &mut mapped[i..end] {
                // Under a transplant the fields come from the spill's
                // analysis sidecar, keyed by the source id
                // (docs/replay-from-segments.md); the record carries no
                // extra field texts, cased field, or phrases then, since
                // those are fields of the table like any other.
                let (mut fields, cased) = match transplanted.as_mut() {
                    Some(map) => {
                        let id = parent_of_local[local];
                        let fields = map.remove(&id).ok_or_else(|| {
                            format!(
                                "record at child slot {local} (source id {id}) has no \
                                 transplanted analysis in the spill sidecar"
                            )
                        })?;
                        if fields.len() != table.len() {
                            return Err(format!(
                                "record at child slot {local} (source id {id}): {} transplanted \
                                 fields for a table of {}",
                                fields.len(),
                                table.len()
                            ));
                        }
                        if !doc.fields.is_empty()
                            || !doc.cased_field.is_empty()
                            || !doc.phrases.is_empty()
                        {
                            return Err(format!(
                                "record at child slot {local} (source id {id}) carries field \
                                 texts, a cased field, or phrases under a transplant"
                            ));
                        }
                        (fields, None)
                    }
                    None => {
                        let mut body = results.next().expect("counted above");
                        let cased = body.cased.take();
                        let mut fields =
                            vec![crate::postings::AnalyzedField::default(); table.len()];
                        fields[0] = body.into_body();
                        (fields, cased)
                    }
                };
                // The cased identity came out of the body's one pass; it
                // lands at the field the record named (docs/dual-cased.md).
                if !doc.cased_field.is_empty() {
                    let Some(ci) = table.iter().position(|n| n == &doc.cased_field) else {
                        return Err(format!(
                            "record at child slot {local} names cased field {:?} outside the \
                             table {table:?}",
                            doc.cased_field
                        ));
                    };
                    fields[ci] = cased.ok_or_else(|| {
                        format!(
                            "record at child slot {local}: the replay analysis carried no cased \
                             identity for {:?}",
                            doc.cased_field
                        )
                    })?;
                }
                for f in &doc.fields {
                    let analyzed_field = results.next().expect("counted above").into_body();
                    let Some(fi) = table.iter().position(|n| n == &f.field) else {
                        return Err(format!(
                            "record at child slot {local} names field {:?} outside the \
                             table {table:?}",
                            f.field
                        ));
                    };
                    fields[fi] = analyzed_field;
                }
                if !doc.phrases.is_empty() {
                    let phrase_field = &doc.phrases[0].field;
                    if doc
                        .phrases
                        .iter()
                        .any(|posting| &posting.field != phrase_field)
                    {
                        return Err(format!("record at child slot {local} mixes phrase fields"));
                    }
                    let Some(fi) = table.iter().position(|name| name == phrase_field) else {
                        return Err(format!(
                            "record at child slot {local} names phrase field {phrase_field:?} outside table {table:?}"
                        ));
                    };
                    fields[fi] = crate::phrases::analyzed_field(&doc.phrases);
                }
                // Proximity payloads (docs/phrase-proximity.md): the
                // re-analysis carries token positions from the same
                // fingerprinted tokenizer, so a positional field takes
                // them and a bigram column derives from its source
                // exactly as at first ingest. A field that needs
                // positions and got none is refused, never re-indexed
                // without them.
                for name in &doc.position_fields {
                    let fi = table.iter().position(|n| n == name).expect("checked above");
                    if !fields[fi].terms.is_empty() && fields[fi].positions.is_none() {
                        return Err(format!(
                            "record at child slot {local}: field {name:?} keeps token positions \
                             but the replay analysis carried none"
                        ));
                    }
                }
                for name in &doc.sentence_fields {
                    let fi = table.iter().position(|n| n == name).expect("checked above");
                    if fields[fi].sentences.is_none() {
                        return Err(format!(
                            "record at child slot {local}: field {name:?} keeps sentence spans \
                             but the replay analysis carried none"
                        ));
                    }
                    if let Err(error) = fields[fi].check_sentences() {
                        return Err(format!(
                            "record at child slot {local}: field {name:?}: malformed sentence \
                             spans: {error}"
                        ));
                    }
                }
                for bigram in &doc.bigram_fields {
                    let Some(source) = table.iter().position(|n| n == &bigram.source) else {
                        return Err(format!(
                            "record at child slot {local} derives {:?} from {:?}, which is outside \
                             the table {table:?}",
                            bigram.field, bigram.source
                        ));
                    };
                    let derived = table
                        .iter()
                        .position(|n| n == &bigram.field)
                        .expect("bigram columns joined the table above");
                    if fields[source].terms.is_empty() {
                        continue;
                    }
                    fields[derived] = crate::proximity::derive_bigrams(&fields[source]).map_err(
                        |error| {
                            format!(
                                "record at child slot {local}: bigram column {:?} from {:?}: {error}",
                                bigram.field, bigram.source
                            )
                        },
                    )?;
                }
                builder
                    .set_analysis_fingerprint(
                        0,
                        crate::analyzer::analysis_fingerprint(doc.analysis.as_ref()),
                    )
                    .map_err(|error| format!("body analysis fingerprint: {error}"))?;
                // The cased field carries the twin of the body's spec, as
                // at first ingest (docs/dual-cased.md).
                if let (false, Some(spec)) = (doc.cased_field.is_empty(), doc.analysis.as_ref()) {
                    let ci = table
                        .iter()
                        .position(|name| name == &doc.cased_field)
                        .expect("cased field was resolved above");
                    let twin = crate::analyzer::cased_twin_spec(spec);
                    builder
                        .set_analysis_fingerprint(
                            ci,
                            crate::analyzer::analysis_fingerprint(Some(&twin)),
                        )
                        .map_err(|error| {
                            format!(
                                "cased field {:?} analysis fingerprint: {error}",
                                doc.cased_field
                            )
                        })?;
                }
                for field in &doc.fields {
                    let fi = table
                        .iter()
                        .position(|name| name == &field.field)
                        .expect("field was resolved above");
                    builder
                        .set_analysis_fingerprint(
                            fi,
                            crate::analyzer::analysis_fingerprint(field.analysis.as_ref()),
                        )
                        .map_err(|error| {
                            format!("field {:?} analysis fingerprint: {error}", field.field)
                        })?;
                }
                if doc.phrase_fingerprint != 0 && !doc.phrase_field.is_empty() {
                    let phrase_field = doc.phrase_field.as_str();
                    let fi = table
                        .iter()
                        .position(|name| name == phrase_field)
                        .expect("phrase field was resolved above");
                    builder
                        .set_analysis_fingerprint(fi, doc.phrase_fingerprint)
                        .map_err(|error| {
                            format!("phrase field {phrase_field:?} fingerprint: {error}")
                        })?;
                }
                for bigram in &doc.bigram_fields {
                    let source = table
                        .iter()
                        .position(|n| n == &bigram.source)
                        .expect("bigram source was resolved above");
                    let derived = table
                        .iter()
                        .position(|n| n == &bigram.field)
                        .expect("bigram column was resolved above");
                    let source_fingerprint = builder.analysis_fingerprint(source);
                    if source_fingerprint != 0 {
                        builder
                            .set_analysis_fingerprint(
                                derived,
                                crate::proximity::bigram_fingerprint(source_fingerprint),
                            )
                            .map_err(|error| {
                                format!("bigram column {:?} fingerprint: {error}", bigram.field)
                            })?;
                    }
                }
                let text = std::mem::take(&mut doc.text);
                if let Some(source) = &doc.original_source {
                    builder
                        .source_archive_mut()
                        .attach_source_with_identity(
                            *local,
                            source,
                            doc.source_chunk_ordinal,
                            doc.identity.as_ref(),
                        )
                        .map_err(|e| format!("restore original source: {e}"))?;
                } else if doc.source_chunk_ordinal.is_some() || doc.identity.is_some() {
                    return Err("source chunk ordinal or identity has no original source".into());
                }
                builder
                    .add_document_with_lineage(
                        *local,
                        text,
                        // Reshard replays LOGGED requests, whose quality
                        // and geography values were materialized into the
                        // ordinary column lists at first ingest, so there
                        // is nothing to derive here
                        // (docs/quality-columns.md,
                        // docs/geography-columns.md).
                        crate::postings::AnalyzedDoc {
                            cased: None,
                            fields,
                            quality: None,
                            geography: None,
                            entities: Vec::new(),
                        },
                        doc.lineage.map(|l| crate::postings::DocLineage {
                            parent_id: l.parent_id,
                            group_id: l.group_id,
                            span_start: l.span_start,
                            span_end: l.span_end,
                        }),
                    )
                    .map_err(|e| format!("spill write (child slot {local}): {e}"))?;
                for fv in &doc.facets {
                    let fi = facet_table
                        .iter()
                        .position(|n| n == &fv.field)
                        .expect("facet table was derived from these records");
                    builder.set_facet(fi, *local, &fv.value);
                }
                for nv in &doc.numerics {
                    let ni = numeric_table
                        .iter()
                        .position(|n| n == &nv.field)
                        .expect("numeric table was derived from these records");
                    builder.set_numeric(ni, *local, nv.value);
                }
                for e in &doc.map_facets {
                    let ci = map_facet_table
                        .iter()
                        .position(|n| n == &e.field)
                        .expect("map-facet table was derived from these records");
                    builder.set_map_facet(ci, *local, &e.key, &e.value);
                }
                for e in &doc.map_numerics {
                    let ci = map_numeric_table
                        .iter()
                        .position(|n| n == &e.field)
                        .expect("map-numeric table was derived from these records");
                    builder.set_map_numeric(ci, *local, &e.key, e.value);
                }
                for e in &doc.map_integers {
                    let ci = map_integer_table
                        .iter()
                        .position(|n| n == &e.field)
                        .ok_or_else(|| {
                            format!(
                                "integer map column {:?} is absent from the replay table",
                                e.field
                            )
                        })?;
                    builder
                        .set_map_integer(ci, *local, &e.key, e.value)
                        .map_err(|e| format!("integer map replay: {e}"))?;
                }
                for e in &doc.map_unsigned_integers {
                    let ci = map_unsigned_integer_table
                        .iter()
                        .position(|n| n == &e.field)
                        .ok_or_else(|| {
                            format!(
                                "integer map column {:?} is absent from the replay table",
                                e.field
                            )
                        })?;
                    builder
                        .set_map_unsigned_integer(ci, *local, &e.key, e.value)
                        .map_err(|e| format!("integer map replay: {e}"))?;
                }
                for e in &doc.integers {
                    let ii = integer_table
                        .iter()
                        .position(|n| n == &e.field)
                        .expect("integer table was derived from these records");
                    builder.set_integer(ii, *local, e.value);
                }
                let mut unsigned_seen = std::collections::HashSet::new();
                for value in &doc.unsigned_integers {
                    if !unsigned_seen.insert(&value.field) {
                        return Err(format!(
                            "record at child slot {local}: unsigned field {:?} repeats",
                            value.field
                        ));
                    }
                    let column = unsigned_integer_table
                        .iter()
                        .position(|name| name == &value.field)
                        .ok_or_else(|| {
                            format!(
                                "record at child slot {local}: undeclared unsigned field {:?}",
                                value.field
                            )
                        })?;
                    builder.set_unsigned_integer(column, *local, value.value);
                }
                // Timestamps re-run the node's conversion, from the
                // instant the WAL kept: the child gets the same epoch
                // micros the parent stored, not a copy of a copy.
                for e in &doc.timestamps {
                    let ii = integer_table
                        .iter()
                        .position(|n| n == &e.field)
                        .expect("integer table was derived from these records");
                    let ts = e.value.as_ref().ok_or_else(|| {
                        format!(
                            "record at child slot {local}: timestamp field {:?} carries no \
                             instant",
                            e.field
                        )
                    })?;
                    let micros = crate::node::timestamp_to_epoch_micros(&e.field, ts)
                        .map_err(|e| format!("record at child slot {local}: {}", e.message()))?;
                    builder.set_integer(ii, *local, micros);
                }
                for e in &doc.geo_points {
                    let gi = geo_table
                        .iter()
                        .position(|n| n == &e.field)
                        .expect("geo table was derived from these records");
                    builder.set_geo(gi, *local, e.lat, e.lon);
                }
            }
            i = end;
        }
        // The children stay bound to the plan the parents were written
        // under; replay must not launder a binding away.
        builder.set_binding(binding.cloned());
        builder
            .finish(&path)
            .map_err(|e| format!("write {}: {e}", path.display()))?;
        bm25_path = Some(path);
    }

    Ok(ChildImage {
        vector_path: vector_path.to_path_buf(),
        exact_vector_path,
        bm25_path,
        slot_offset: 0, // filled in by the caller
        hash_lo: 0,
        hash_hi: 0,
        num_vectors: vectors.len() as u64,
        num_documents,
        parent_ids,
        row_parent_ids,
    })
}

/// Assemble one child: replay its vectors/documents into an image under
/// `out_dir` named `shard-<ordinal>.vector`.
#[allow(clippy::too_many_arguments)]
fn finish_child(
    manifest: &WalManifest,
    replay: Replay,
    ordinal: usize,
    out_dir: &Path,
    slot_offset: u64,
    hash_lo: u64,
    hash_hi: u64,
    bm25_fields: Option<&[String]>,
    binding: Option<&crate::postings::StoredBinding>,
    analyze: &mut Analyzer,
) -> Result<ChildImage, String> {
    finish_child_pinned(
        manifest,
        replay,
        ordinal,
        out_dir,
        slot_offset,
        hash_lo,
        hash_hi,
        bm25_fields,
        None,
        binding,
        None,
        analyze,
    )
}

/// [`finish_child`] with the analyzer fingerprints a compaction pins.
#[allow(clippy::too_many_arguments)]
fn finish_child_pinned(
    manifest: &WalManifest,
    mut replay: Replay,
    ordinal: usize,
    out_dir: &Path,
    slot_offset: u64,
    hash_lo: u64,
    hash_hi: u64,
    bm25_fields: Option<&[String]>,
    pinned_fingerprints: Option<&[u64]>,
    binding: Option<&crate::postings::StoredBinding>,
    columns: Option<&ColumnTables>,
    analyze: &mut Analyzer,
) -> Result<ChildImage, String> {
    replay.compact();
    let order = child_order(&replay);
    finish_child_ordered(
        manifest,
        replay,
        &order,
        ordinal,
        out_dir,
        slot_offset,
        hash_lo,
        hash_hi,
        bm25_fields,
        pinned_fingerprints,
        binding,
        columns,
        analyze,
    )
}

/// [`finish_child_pinned`] with the child's slot order given: `order`
/// lists the live source ids, vector-bearing rows first and document-only
/// rows after them (the shape [`build_child`] assigns slots in), and the
/// image takes its slots in that sequence. [`emit_rows_in`] with the same
/// order writes the matching log.
#[allow(clippy::too_many_arguments)]
fn finish_child_ordered(
    manifest: &WalManifest,
    mut replay: Replay,
    order: &[u64],
    ordinal: usize,
    out_dir: &Path,
    slot_offset: u64,
    hash_lo: u64,
    hash_hi: u64,
    bm25_fields: Option<&[String]>,
    pinned_fingerprints: Option<&[u64]>,
    binding: Option<&crate::postings::StoredBinding>,
    columns: Option<&ColumnTables>,
    analyze: &mut Analyzer,
) -> Result<ChildImage, String> {
    replay.compact();
    let vector_path = out_dir.join(format!("shard-{ordinal}.vector"));
    eprintln!(
        "reshard: child {ordinal}: {} vectors, {} documents -> {}",
        replay.vectors.len(),
        replay.documents.len(),
        vector_path.display()
    );
    let mut vectors = Vec::with_capacity(replay.vectors.len());
    let mut documents = Vec::with_capacity(replay.documents.len());
    for id in order {
        if let Some(vector) = replay.vectors.remove(id) {
            vectors.push((*id, vector));
        }
        if let Some(document) = replay.documents.remove(id) {
            documents.push((*id, document));
        }
    }
    if !replay.vectors.is_empty() || !replay.documents.is_empty() {
        return Err(format!(
            "child {ordinal}: the slot order names {} rows but the replay holds {} more",
            order.len(),
            replay.vectors.len() + replay.documents.len()
        ));
    }
    let transplanted = (!replay.analyzed.is_empty()).then(|| std::mem::take(&mut replay.analyzed));
    let mut child = build_child(
        manifest,
        vectors,
        documents,
        &vector_path,
        bm25_fields,
        pinned_fingerprints,
        binding,
        columns,
        transplanted,
        analyze,
    )?;
    child.slot_offset = slot_offset;
    child.hash_lo = hash_lo;
    child.hash_hi = hash_hi;
    Ok(child)
}

/// Rewrite one or more WAL generations into `n` child images. `n = 1` is
/// compaction; larger powers of two repartition.
///
/// With `n <= bucket_count` and `bucket_count % n == 0` this is the cheap
/// path: child `i` owns the contiguous bucket range
/// `[i*bucket_count/n, (i+1)*bucket_count/n)`, replays only those files,
/// and its hash range is exactly those buckets. A finer split
/// (`n > bucket_count`) re-partitions every record by
/// [`bucket_of`]`(id, n)` — correct but slower; the WAL bucket count
/// caps cheap split granularity. Either way children get
/// `slot_offset = slot_base + i * slot_stride` (stride defaults to 25M
/// in the example, matching deploy/court-e2e) and dense local slots in
/// original id order.
#[allow(clippy::too_many_arguments)]
pub fn split(
    gen: &Path,
    n: usize,
    out_dir: &Path,
    slot_base: u64,
    slot_stride: u64,
    vectors_only: bool,
    bm25_fields: Option<&[String]>,
    analyze: &mut Analyzer,
) -> Result<ReshardOutput, String> {
    split_logs(
        std::slice::from_ref(&gen.to_path_buf()),
        n,
        out_dir,
        slot_base,
        slot_stride,
        vectors_only,
        bm25_fields,
        analyze,
    )
}

/// Split ANY number of shards' WAL generations into `n` child images —
/// the general N -> M reshard. One input log is a plain split; several
/// input logs redistribute their union across the children with no
/// intermediate merge artifact: child `i`'s input is its bucket range
/// read from EVERY input generation, and global ids from different
/// shards never collide (disjoint slot ranges), so the per-child replay
/// map assembles the union in id order for free.
///
/// All inputs must carry byte-identical provider configurations and one bucket
/// geometry, and be full-history — the same preconditions as
/// [`merge`], enforced the same way.
#[allow(clippy::too_many_arguments)]
pub fn split_logs(
    gens: &[PathBuf],
    n: usize,
    out_dir: &Path,
    slot_base: u64,
    slot_stride: u64,
    vectors_only: bool,
    bm25_fields: Option<&[String]>,
    analyze: &mut Analyzer,
) -> Result<ReshardOutput, String> {
    if !n.is_power_of_two() {
        return Err(format!(
            "split factor must be a positive power of two, got {n}"
        ));
    }
    let Some(first_gen) = gens.first() else {
        return Err("split requires at least one input generation".to_string());
    };
    let manifest = read_gen_manifest(first_gen)?;
    let mut top_generation = manifest.generation;
    for gen in gens {
        let m = read_gen_manifest(gen)?;
        require_backend_config(&m, gen)?;
        require_complete_history(&m, gen)?;
        if !same_backend_config(&manifest, &m) {
            return Err(format!(
                "{}: vector backend configuration differs from the first input; \
                 split/merge requires one scoring configuration",
                gen.display()
            ));
        }
        if m.bucket_count != manifest.bucket_count {
            return Err(format!(
                "{}: bucket count {} differs from the first input's {}; split inputs \
                 must share one bucket geometry",
                gen.display(),
                m.bucket_count,
                manifest.bucket_count
            ));
        }
        top_generation = top_generation.max(m.generation);
    }
    let binding = read_gens_binding(gens)?;
    std::fs::create_dir_all(out_dir).map_err(|e| format!("mkdir {}: {e}", out_dir.display()))?;

    let bucket_count = manifest.bucket_count as usize;
    let bucket_bits = manifest.bucket_count.trailing_zeros();
    let mut children = Vec::with_capacity(n);

    if n <= bucket_count && bucket_count.is_multiple_of(n) {
        // Cheap path: whole bucket ranges, no re-hashing. Memory is
        // bounded per child: only its bucket range is resident, read
        // from each input in turn.
        let per_child = bucket_count / n;
        for i in 0..n {
            let first = (i * per_child) as u32;
            let mut replay = Replay::default();
            for gen in gens {
                replay_buckets(
                    gen,
                    first..first + per_child as u32,
                    bucket_count,
                    manifest.dim as usize,
                    vectors_only,
                    &mut replay,
                )?;
            }
            let hash_lo = u64::from(first) << (64 - bucket_bits);
            let hash_hi = if i + 1 == n {
                u64::MAX
            } else {
                ((first as u64 + per_child as u64) << (64 - bucket_bits)) - 1
            };
            children.push(finish_child(
                &manifest,
                replay,
                i,
                out_dir,
                slot_base + i as u64 * slot_stride,
                hash_lo,
                hash_hi,
                bm25_fields,
                binding.as_ref(),
                analyze,
            )?);
        }
    } else {
        eprintln!(
            "reshard: WAL bucket count {bucket_count} caps cheap splits at {bucket_count} \
             children; re-partitioning every record for split={n}"
        );
        let mut replay = Replay::default();
        for gen in gens {
            replay_buckets(
                gen,
                0..manifest.bucket_count,
                bucket_count,
                manifest.dim as usize,
                vectors_only,
                &mut replay,
            )?;
        }
        replay.compact();
        let shift = 64 - n.trailing_zeros();
        type ReshardBucket = (Vec<(u64, Vec<f32>)>, Vec<(u64, AddDocumentsRequest)>);
        let mut buckets: Vec<ReshardBucket> = (0..n).map(|_| (Vec::new(), Vec::new())).collect();
        for (id, vector) in replay.vectors {
            buckets[bucket_of(id, n)].0.push((id, vector));
        }
        for (id, doc) in replay.documents {
            buckets[bucket_of(id, n)].1.push((id, doc));
        }
        for (i, (vectors, documents)) in buckets.into_iter().enumerate() {
            let hash_lo = (i as u64) << shift;
            let hash_hi = if i + 1 == n {
                u64::MAX
            } else {
                ((i as u64 + 1) << shift) - 1
            };
            children.push(finish_child(
                &manifest,
                Replay {
                    vectors: vectors.into_iter().collect(),
                    documents: documents.into_iter().collect(),
                    deleted: Default::default(),
                    stable_keys: Default::default(),
                    analyzed: Default::default(),
                },
                i,
                out_dir,
                slot_base + i as u64 * slot_stride,
                hash_lo,
                hash_hi,
                bm25_fields,
                binding.as_ref(),
                analyze,
            )?);
        }
    }
    Ok(ReshardOutput {
        generation: top_generation + 1,
        children,
    })
}

/// Repartition full-history WALs into immutable, bucket-sized segments.
///
/// This is the spillable counterpart of [`split_logs`]. The ordinary path
/// accumulates every row of one output child before encoding it; this path
/// replays one WAL bucket across all input generations, applies its
/// deletes/replacements, and immediately seals each non-empty output segment.
/// WAL writers emit one vector/document row per record; replay rejects a
/// foreign batch that straddles buckets. A logical shard can contain many
/// physical segments, merged exactly by [`crate::segments::OpenedSegmentSet`].
/// WAL bucket count controls the replay bound without changing query semantics.
#[allow(clippy::too_many_arguments)]
pub fn split_logs_segmented(
    gens: &[PathBuf],
    n: usize,
    out_dir: &Path,
    slot_base: u64,
    slot_stride: u64,
    vectors_only: bool,
    bm25_fields: Option<&[String]>,
    analyze: &mut Analyzer,
) -> Result<SegmentedReshardOutput, String> {
    if !n.is_power_of_two() {
        return Err(format!(
            "segmented split factor must be a positive power of two, got {n}"
        ));
    }
    if slot_stride == 0 {
        return Err("segmented split slot_stride must be positive".into());
    }
    let Some(first_gen) = gens.first() else {
        return Err("segmented split requires at least one input generation".into());
    };
    let manifest = read_gen_manifest(first_gen)?;
    require_backend_config(&manifest, first_gen)?;
    require_complete_history(&manifest, first_gen)?;
    let mut top_generation = manifest.generation;
    for gen in gens.iter().skip(1) {
        let current = read_gen_manifest(gen)?;
        require_backend_config(&current, gen)?;
        require_complete_history(&current, gen)?;
        if !same_backend_config(&manifest, &current) {
            return Err(format!(
                "{}: vector backend configuration differs from the first input",
                gen.display()
            ));
        }
        if current.bucket_count != manifest.bucket_count {
            return Err(format!(
                "{}: bucket count {} differs from the first input's {}",
                gen.display(),
                current.bucket_count,
                manifest.bucket_count
            ));
        }
        top_generation = top_generation.max(current.generation);
    }
    let binding = read_gens_binding(gens)?;
    std::fs::create_dir_all(out_dir)
        .map_err(|error| format!("mkdir {}: {error}", out_dir.display()))?;
    let mut next_slots: Vec<u64> = (0..n)
        .map(|shard| {
            let shard = u64::try_from(shard)
                .map_err(|_| "segmented split shard index does not fit u64".to_string())?;
            let delta = shard
                .checked_mul(slot_stride)
                .ok_or_else(|| "segmented split slot base overflow".to_string())?;
            slot_base
                .checked_add(delta)
                .ok_or_else(|| "segmented split slot base overflow".to_string())
        })
        .collect::<Result<_, _>>()?;
    let mut segment_counts = vec![0usize; n];
    let mut segments = Vec::new();
    let mut peak_replay_rows = 0u64;

    for bucket in 0..manifest.bucket_count {
        let mut replay = Replay::default();
        for gen in gens {
            replay_buckets(
                gen,
                bucket..bucket + 1,
                manifest.bucket_count as usize,
                manifest.dim as usize,
                vectors_only,
                &mut replay,
            )?;
        }
        replay.compact();
        let live_ids: BTreeSet<u64> = replay
            .vectors
            .keys()
            .chain(replay.documents.keys())
            .copied()
            .collect();
        peak_replay_rows = peak_replay_rows.max(live_ids.len() as u64);
        if live_ids.is_empty() {
            continue;
        }
        let stable_keys = replay.stable_keys;
        let mut partitions: Vec<Replay> = (0..n).map(|_| Replay::default()).collect();
        for (id, vector) in replay.vectors {
            let shard = bucket_of(id, n);
            partitions[shard].vectors.insert(id, vector);
            if let Some(key) = stable_keys.get(&id) {
                partitions[shard].stable_keys.insert(id, key.clone());
            }
        }
        for (id, document) in replay.documents {
            let shard = bucket_of(id, n);
            partitions[shard].documents.insert(id, document);
            if let Some(key) = stable_keys.get(&id) {
                partitions[shard].stable_keys.insert(id, key.clone());
            }
        }
        for (logical_shard, partition) in partitions.into_iter().enumerate() {
            let ids: BTreeSet<u64> = partition
                .vectors
                .keys()
                .chain(partition.documents.keys())
                .copied()
                .collect();
            let physical_rows = ids.len() as u64;
            if physical_rows == 0 {
                continue;
            }
            let logical_ordinal = u64::try_from(logical_shard)
                .map_err(|_| "segmented split shard index does not fit u64")?;
            let logical_end = logical_ordinal
                .checked_add(1)
                .and_then(|ordinal| ordinal.checked_mul(slot_stride))
                .and_then(|delta| slot_base.checked_add(delta))
                .ok_or_else(|| "segmented split slot range overflow".to_string())?;
            let end = next_slots[logical_shard]
                .checked_add(physical_rows)
                .ok_or_else(|| "segmented split physical row range overflow".to_string())?;
            if end > logical_end {
                return Err(format!(
                    "segmented child {logical_shard} needs stable ids through {end}, above its reserved slot range ending at {logical_end}; raise slot_stride"
                ));
            }
            let shift = if n == 1 { 0 } else { 64 - n.trailing_zeros() };
            let hash_lo = if n == 1 { 0 } else { logical_ordinal << shift };
            let hash_hi = if logical_shard + 1 == n {
                u64::MAX
            } else {
                ((logical_ordinal + 1) << shift) - 1
            };
            let ordinal = segments.len();
            let image = finish_child(
                &manifest,
                partition,
                ordinal,
                out_dir,
                next_slots[logical_shard],
                hash_lo,
                hash_hi,
                bm25_fields,
                binding.as_ref(),
                analyze,
            )?;
            segments.push(SegmentedChildImage {
                logical_shard,
                segment_ordinal: segment_counts[logical_shard],
                physical_rows,
                image,
            });
            segment_counts[logical_shard] += 1;
            next_slots[logical_shard] = end;
        }
    }
    Ok(SegmentedReshardOutput {
        generation: top_generation
            .checked_add(1)
            .ok_or_else(|| "segmented reshard generation overflow".to_string())?,
        logical_shards: n,
        segments,
        peak_replay_rows,
    })
}

/// Repartition full-history, generation-clocked WALs by their opaque stable
/// product keys. This is the baseline-build half of a hitless reshard: it
/// returns the exact source high watermarks included in the child images, so
/// the live tail can resume without a gap before an atomic map publication.
#[allow(clippy::too_many_arguments)]
pub fn split_stable_logs(
    gens: &[PathBuf],
    n: usize,
    out_dir: &Path,
    slot_base: u64,
    slot_stride: u64,
    vectors_only: bool,
    bm25_fields: Option<&[String]>,
    analyze: &mut Analyzer,
) -> Result<StableReshardOutput, String> {
    if !n.is_power_of_two() {
        return Err(format!(
            "stable split factor must be a positive power of two, got {n}"
        ));
    }
    let Some(first_gen) = gens.first() else {
        return Err("stable split requires at least one input generation".to_string());
    };
    let manifest = read_gen_manifest(first_gen)?;
    let mut top_generation = manifest.generation;
    let mut cutoffs = Vec::with_capacity(gens.len());
    let mut replay = Replay::default();
    for gen in gens {
        let current = read_gen_manifest(gen)?;
        require_backend_config(&current, gen)?;
        require_complete_history(&current, gen)?;
        if !same_backend_config(&manifest, &current) {
            return Err(format!(
                "{}: vector backend configuration differs from the first input",
                gen.display()
            ));
        }
        let clocked = wal::read_clocked_records(gen, 0)
            .map_err(|error| format!("clocked replay {}: {error}", gen.display()))?;
        cutoffs.push(WalCutoff {
            generation: current.generation,
            high_watermark: clocked.last().map_or(0, |record| record.clock),
        });
        replay_buckets(
            gen,
            0..current.bucket_count,
            current.bucket_count as usize,
            current.dim as usize,
            vectors_only,
            &mut replay,
        )?;
        top_generation = top_generation.max(current.generation);
    }
    replay.compact();
    for id in replay.vectors.keys().chain(replay.documents.keys()) {
        if replay.stable_keys.get(id).is_none_or(|key| key.is_empty()) {
            return Err(format!(
                "stable split requires a routed stable key for live source id {id}; rebuild legacy explicitly addressed rows before live reshard"
            ));
        }
    }
    let binding = read_gens_binding(gens)?;
    std::fs::create_dir_all(out_dir)
        .map_err(|error| format!("mkdir {}: {error}", out_dir.display()))?;
    let shift = 64 - n.trailing_zeros();
    let mut buckets: Vec<Replay> = (0..n).map(|_| Replay::default()).collect();
    let keys = replay.stable_keys;
    for (id, vector) in replay.vectors {
        let key = keys.get(&id).expect("validated above");
        let shard = if n == 1 {
            0
        } else {
            (crate::coordinator::stable_routing_hash(key) >> shift) as usize
        };
        buckets[shard].vectors.insert(id, vector);
        buckets[shard].stable_keys.insert(id, key.clone());
    }
    for (id, document) in replay.documents {
        let key = keys.get(&id).expect("validated above");
        let shard = if n == 1 {
            0
        } else {
            (crate::coordinator::stable_routing_hash(key) >> shift) as usize
        };
        buckets[shard].documents.insert(id, document);
        buckets[shard].stable_keys.insert(id, key.clone());
    }
    let mut children = Vec::with_capacity(n);
    for (shard, replay) in buckets.into_iter().enumerate() {
        let hash_lo = if n == 1 { 0 } else { (shard as u64) << shift };
        let hash_hi = if shard + 1 == n {
            u64::MAX
        } else {
            ((shard as u64 + 1) << shift) - 1
        };
        children.push(finish_child(
            &manifest,
            replay,
            shard,
            out_dir,
            slot_base + shard as u64 * slot_stride,
            hash_lo,
            hash_hi,
            bm25_fields,
            binding.as_ref(),
            analyze,
        )?);
    }
    Ok(StableReshardOutput {
        images: ReshardOutput {
            generation: top_generation + 1,
            children,
        },
        source_cutoffs: cutoffs,
    })
}

/// Repartition full-history, generation-clocked WALs by their stable
/// product keys into children covering the given hash ranges
/// (`docs/cluster-control.md`, "Shard split"): child `i` receives the
/// rows whose key hash falls in `ranges[i]` and takes `slot_offsets[i]`.
/// The ranges must tile the source's range without a gap; a live row
/// whose hash falls outside every range refuses by name, since the
/// children would not conserve the source. Legacy rows without a stable
/// key refuse as [`split_stable_logs`] does.
#[allow(clippy::too_many_arguments)]
pub fn split_stable_logs_ranged(
    gens: &[PathBuf],
    ranges: &[(u64, u64)],
    out_dir: &Path,
    slot_offsets: &[u64],
    vectors_only: bool,
    bm25_fields: Option<&[String]>,
    analyze: &mut Analyzer,
) -> Result<StableReshardOutput, String> {
    if ranges.len() < 2 {
        return Err("a ranged split needs at least two child ranges".to_string());
    }
    if slot_offsets.len() != ranges.len() {
        return Err(format!(
            "a ranged split needs one slot offset per child: {} offsets for {} ranges",
            slot_offsets.len(),
            ranges.len()
        ));
    }
    for window in ranges.windows(2) {
        let (_, hi) = window[0];
        let (lo, _) = window[1];
        if hi.checked_add(1) != Some(lo) {
            return Err(format!(
                "child ranges must be adjacent and ascending: ..={hi} is followed by {lo}.."
            ));
        }
    }
    if ranges.iter().any(|(lo, hi)| lo > hi) {
        return Err("a child range is inverted".to_string());
    }
    let Some(first_gen) = gens.first() else {
        return Err("a ranged split requires at least one input generation".to_string());
    };
    let manifest = read_gen_manifest(first_gen)?;
    let mut top_generation = manifest.generation;
    let mut cutoffs = Vec::with_capacity(gens.len());
    let mut replay = Replay::default();
    for gen in gens {
        let current = read_gen_manifest(gen)?;
        require_backend_config(&current, gen)?;
        require_complete_history(&current, gen)?;
        if !same_backend_config(&manifest, &current) {
            return Err(format!(
                "{}: vector backend configuration differs from the first input",
                gen.display()
            ));
        }
        let clocked = wal::read_clocked_records(gen, 0)
            .map_err(|error| format!("clocked replay {}: {error}", gen.display()))?;
        cutoffs.push(WalCutoff {
            generation: current.generation,
            high_watermark: clocked.last().map_or(0, |record| record.clock),
        });
        replay_buckets(
            gen,
            0..current.bucket_count,
            current.bucket_count as usize,
            current.dim as usize,
            vectors_only,
            &mut replay,
        )?;
        top_generation = top_generation.max(current.generation);
    }
    replay.compact();
    for id in replay.vectors.keys().chain(replay.documents.keys()) {
        if replay.stable_keys.get(id).is_none_or(|key| key.is_empty()) {
            return Err(format!(
                "stable split requires a routed stable key for live source id {id}; rebuild \
                 legacy explicitly addressed rows before a live split"
            ));
        }
    }
    let binding = read_gens_binding(gens)?;
    std::fs::create_dir_all(out_dir)
        .map_err(|error| format!("mkdir {}: {error}", out_dir.display()))?;
    let child_of = |key: &[u8]| -> Result<usize, String> {
        let hash = crate::coordinator::stable_routing_hash(key);
        ranges
            .iter()
            .position(|(lo, hi)| hash >= *lo && hash <= *hi)
            .ok_or_else(|| {
                format!(
                    "a live row's key hashes to {hash}, outside every child range of this \
                     split; the source holds rows the split's range does not cover"
                )
            })
    };
    let mut buckets: Vec<Replay> = ranges.iter().map(|_| Replay::default()).collect();
    let keys = replay.stable_keys;
    for (id, vector) in replay.vectors {
        let key = keys.get(&id).expect("validated above");
        let shard = child_of(key)?;
        buckets[shard].vectors.insert(id, vector);
        buckets[shard].stable_keys.insert(id, key.clone());
    }
    for (id, document) in replay.documents {
        let key = keys.get(&id).expect("validated above");
        let shard = child_of(key)?;
        buckets[shard].documents.insert(id, document);
        buckets[shard].stable_keys.insert(id, key.clone());
    }
    let mut children = Vec::with_capacity(ranges.len());
    for (shard, replay) in buckets.into_iter().enumerate() {
        let (hash_lo, hash_hi) = ranges[shard];
        children.push(finish_child(
            &manifest,
            replay,
            shard,
            out_dir,
            slot_offsets[shard],
            hash_lo,
            hash_hi,
            bm25_fields,
            binding.as_ref(),
            analyze,
        )?);
    }
    Ok(StableReshardOutput {
        images: ReshardOutput {
            generation: top_generation + 1,
            children,
        },
        source_cutoffs: cutoffs,
    })
}

/// One child of a placement-keyed split: the inclusive code range it
/// takes (`docs/placement.md`, "Changing the tree"). A single code is a
/// range of one; a prefix range takes a subtree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlacementChild {
    pub lo: i64,
    pub hi: i64,
}

/// The result of a placement-keyed split: the images plus, per child,
/// the code range it holds, and the cutoffs a live catch-up resumes from.
#[derive(Debug)]
pub struct PlacementReshardOutput {
    pub images: ReshardOutput,
    pub source_cutoffs: Vec<WalCutoff>,
    /// Parallel to `images.children`.
    pub ranges: Vec<PlacementChild>,
    /// The child that took the rows with no value in the column, when
    /// one was named.
    pub default_child: Option<usize>,
}

impl PlacementChild {
    /// A child with no code range of its own: only the default child may
    /// be one, and it takes the rows no range covers.
    pub const NONE: PlacementChild = PlacementChild { lo: 0, hi: -1 };

    pub fn is_none(&self) -> bool {
        self.lo > self.hi
    }
}

/// Validate a placement split's children: at least one, no range
/// inverted (a rangeless child is allowed only as the default), none
/// overlapping, and a default index inside the list.
pub fn validate_placement_children(
    children: &[PlacementChild],
    default_child: Option<usize>,
) -> Result<(), String> {
    if children.is_empty() {
        return Err("a placement split needs at least one child".to_string());
    }
    for (i, child) in children.iter().enumerate() {
        if child.is_none() {
            if default_child != Some(i) {
                return Err(format!(
                    "placement child {i} has no code range and is not the default child"
                ));
            }
            continue;
        }
        if child.lo < 0 {
            return Err(format!(
                "placement child {i} starts below zero ({}); codes are never negative",
                child.lo
            ));
        }
    }
    let mut sorted: Vec<(usize, PlacementChild)> = children
        .iter()
        .copied()
        .enumerate()
        .filter(|(_, c)| !c.is_none())
        .collect();
    sorted.sort_by_key(|(_, c)| c.lo);
    for pair in sorted.windows(2) {
        let (i, a) = pair[0];
        let (j, b) = pair[1];
        if b.lo <= a.hi {
            return Err(format!(
                "placement children {i} ({}..={}) and {j} ({}..={}) overlap",
                a.lo, a.hi, b.lo, b.hi
            ));
        }
    }
    if let Some(d) = default_child {
        if d >= children.len() {
            return Err(format!(
                "default child {d} is outside the {} children",
                children.len()
            ));
        }
    }
    Ok(())
}

/// Split one or more full-history generations into children keyed by
/// the placement code each logged document carries in `column`
/// (`docs/placement.md`). No CEL runs at replay: the code was written at
/// ingest and the log holds it. A document whose code falls in no
/// child's range, a document with no value, and a vector row with no
/// document all go to `default_child`; without one, any such row
/// refuses the split by id. Every child keeps the parent's stable-key
/// hash coverage (the whole space), because routing under a tree is by
/// code first and by hash inside the group.
#[allow(clippy::too_many_arguments)]
pub fn split_placement_logs(
    gens: &[PathBuf],
    column: &str,
    children: &[PlacementChild],
    default_child: Option<usize>,
    out_dir: &Path,
    slot_offsets: &[u64],
    vectors_only: bool,
    bm25_fields: Option<&[String]>,
    analyze: &mut Analyzer,
) -> Result<PlacementReshardOutput, String> {
    if column.trim().is_empty() {
        return Err("a placement split needs the placement column's name".to_string());
    }
    validate_placement_children(children, default_child)?;
    if slot_offsets.len() != children.len() {
        return Err(format!(
            "a placement split needs one slot offset per child: {} offsets for {} children",
            slot_offsets.len(),
            children.len()
        ));
    }
    let Some(first_gen) = gens.first() else {
        return Err("a placement split requires at least one input generation".to_string());
    };
    let manifest = read_gen_manifest(first_gen)?;
    let mut top_generation = manifest.generation;
    let mut cutoffs = Vec::with_capacity(gens.len());
    let mut replay = Replay::default();
    for gen in gens {
        let current = read_gen_manifest(gen)?;
        require_backend_config(&current, gen)?;
        require_complete_history(&current, gen)?;
        if !same_backend_config(&manifest, &current) {
            return Err(format!(
                "{}: vector backend configuration differs from the first input",
                gen.display()
            ));
        }
        let clocked = wal::read_clocked_records(gen, 0)
            .map_err(|error| format!("clocked replay {}: {error}", gen.display()))?;
        cutoffs.push(WalCutoff {
            generation: current.generation,
            high_watermark: clocked.last().map_or(0, |record| record.clock),
        });
        replay_buckets(
            gen,
            0..current.bucket_count,
            current.bucket_count as usize,
            current.dim as usize,
            vectors_only,
            &mut replay,
        )?;
        top_generation = top_generation.max(current.generation);
    }
    replay.compact();
    let binding = read_gens_binding(gens)?;
    std::fs::create_dir_all(out_dir)
        .map_err(|error| format!("mkdir {}: {error}", out_dir.display()))?;
    let child_of = |id: u64, code: Option<i64>| -> Result<usize, String> {
        if let Some(code) = code {
            if let Some(i) = children
                .iter()
                .position(|child| code >= child.lo && code <= child.hi)
            {
                return Ok(i);
            }
        }
        default_child.ok_or_else(|| match code {
            Some(code) => format!(
                "live source id {id} carries placement code {code}, outside every child's \
                 range, and the split names no default child"
            ),
            None => format!(
                "live source id {id} carries no value in {column:?} and the split names no \
                 default child"
            ),
        })
    };
    let mut codes: BTreeMap<u64, Option<i64>> = BTreeMap::new();
    for (id, document) in &replay.documents {
        codes.insert(*id, partition_key_of(document, column)?);
    }
    let mut buckets: Vec<Replay> = children.iter().map(|_| Replay::default()).collect();
    let keys = replay.stable_keys;
    for (id, vector) in replay.vectors {
        let shard = child_of(id, codes.get(&id).copied().flatten())?;
        buckets[shard].vectors.insert(id, vector);
        if let Some(key) = keys.get(&id) {
            buckets[shard].stable_keys.insert(id, key.clone());
        }
    }
    for (id, document) in replay.documents {
        let shard = child_of(id, codes.get(&id).copied().flatten())?;
        buckets[shard].documents.insert(id, document);
        if let Some(key) = keys.get(&id) {
            buckets[shard].stable_keys.insert(id, key.clone());
        }
    }
    let mut out = Vec::with_capacity(children.len());
    for (shard, replay) in buckets.into_iter().enumerate() {
        out.push(finish_child(
            &manifest,
            replay,
            shard,
            out_dir,
            slot_offsets[shard],
            0,
            u64::MAX,
            bm25_fields,
            binding.as_ref(),
            analyze,
        )?);
    }
    Ok(PlacementReshardOutput {
        images: ReshardOutput {
            generation: top_generation + 1,
            children: out,
        },
        source_cutoffs: cutoffs,
        ranges: children.to_vec(),
        default_child,
    })
}

/// The shard map for a placement split: `placement = <code>` on a child
/// that holds one code; a child holding a range is left for the
/// operator to place, with the range in a comment, because the map
/// names one code per shard.
pub fn placement_shard_map_toml(out: &PlacementReshardOutput) -> String {
    let mut s = format!("generation = {}\n", out.images.generation);
    for (child, range) in out.images.children.iter().zip(&out.ranges) {
        s.push_str(&format!(
            "\n[[shards]]\naddr = \"TODO\"\nslot_offset = {}\nhash_lo = {}\nhash_hi = {}\n",
            child.slot_offset, child.hash_lo, child.hash_hi
        ));
        if range.is_none() {
            s.push_str("# the default child: rows with no code or a code no child covers\n");
        } else if range.lo == range.hi {
            s.push_str(&format!("placement = {}\n", range.lo));
        } else {
            s.push_str(&format!(
                "# placement codes {}..={}: one code per shard in the map; choose one\n",
                range.lo, range.hi
            ));
        }
    }
    s
}

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

/// Split one or more full-history generations under a NEW placement
/// tree (`docs/placement.md`, "Changing the tree"): the tree is
/// evaluated on each live document's stored values, the way the
/// coordinator routes at ingest, the placement column is rewritten to
/// the code the new tree assigns, and the document goes to the child
/// serving that leaf (inside a leaf with several shards, by its stable
/// key's hash, which each such row must then carry). A vector row with
/// no document cannot be evaluated and refuses the split by id. Memory
/// is bounded by one WAL bucket during routing and by the largest child
/// while its image is built: rows are spilled per child under
/// `out_dir/spill` first, as a partitioned compaction does, and under
/// [`TreeChildLayout::Segmented`] (the default) each child is a catalog
/// sealed one spill bucket at a time, so memory is one bucket. Offline
/// only: no live cutoff is recorded, so the sources must be quiescent.
#[allow(clippy::too_many_arguments)]
pub fn split_placement_tree_logs(
    gens: &[PathBuf],
    tree: &crate::placement::PlacementTreeConfig,
    out_dir: &Path,
    slot_offsets: &[u64],
    bm25_fields: Option<&[String]>,
    options: TreeSplitOptions,
    analyze: &mut Analyzer,
) -> Result<TreeReshardOutput, String> {
    let placement = crate::placement::Placement::validate(tree)?;
    let children = tree_children(&placement)?;
    if let Some(only) = options.only_child {
        if matches!(options.layout, TreeChildLayout::SingleImage { .. }) {
            return Err(
                "--only-child builds one segmented child; a single-image split writes every child"
                    .to_string(),
            );
        }
        if only >= children.len() {
            return Err(format!(
                "--only-child={only} names no child; the tree has {} leaf shards (0..{})",
                children.len(),
                children.len().saturating_sub(1)
            ));
        }
    }
    if slot_offsets.len() != children.len() {
        return Err(format!(
            "a re-placement split needs one slot offset per child: {} offsets for {} children \
             ({} leaves, shards summed)",
            slot_offsets.len(),
            children.len(),
            placement.leaves().len()
        ));
    }
    let Some(first_gen) = gens.first() else {
        return Err("a re-placement split requires at least one input generation".to_string());
    };
    let manifest = read_gen_manifest(first_gen)?;
    let mut top_generation = manifest.generation;
    for gen in gens {
        let current = read_gen_manifest(gen)?;
        require_backend_config(&current, gen)?;
        require_complete_history(&current, gen)?;
        if !same_backend_config(&manifest, &current) {
            return Err(format!(
                "{}: vector backend configuration differs from the first input",
                gen.display()
            ));
        }
        top_generation = top_generation.max(current.generation);
    }
    let binding = read_gens_binding(gens)?;
    std::fs::create_dir_all(out_dir)
        .map_err(|error| format!("mkdir {}: {error}", out_dir.display()))?;
    let column = placement.column().to_string();
    let first_child_of_leaf = |code: i64| children.iter().position(|child| child.code == code);
    let shards_of_leaf = |code: i64| children.iter().filter(|child| child.code == code).count();

    // Pass 1: route each live row under the new tree into its child's
    // spill log, one WAL bucket (or one source segment) in memory at a
    // time (`docs/replay-from-segments.md`).
    let spill_root = out_dir.join("spill");
    if matches!(options.cut, SpillCut::Column { .. })
        && matches!(options.layout, TreeChildLayout::SingleImage { .. })
    {
        return Err(
            "a column cut orders a child's segments; it needs the segmented layout".to_string(),
        );
    }
    let sources = match options.source {
        TreeRowSource::Logs => None,
        TreeRowSource::Segments => Some(open_segment_sources(gens)?),
    };
    if options.source == TreeRowSource::Segments {
        if let Some(child) = children.iter().find(|child| shards_of_leaf(child.code) > 1) {
            return Err(format!(
                "leaf {:?} has {} shards, which are tiled by stable routing keys, and sealed \
                 segments carry no routing keys; give the leaf one shard or split from the logs",
                child.leaf,
                shards_of_leaf(child.code)
            ));
        }
    }
    // Where a live row goes: the child index, the leaf, and whether its
    // code changed.
    let route = |id: u64,
                 document: &AddDocumentsRequest,
                 stable_key: Option<&Vec<u8>>|
     -> Result<(usize, i64, bool), String> {
        let carried = partition_key_of(document, &column)?;
        let leaf = placement
            .evaluate(document)
            .map_err(|e| format!("live source id {id}: {e}"))?;
        let moved = carried != Some(leaf.code);
        let base = first_child_of_leaf(leaf.code).expect("children cover every leaf");
        let n = shards_of_leaf(leaf.code);
        let index = if n == 1 {
            base
        } else {
            let Some(key) = stable_key.filter(|key| !key.is_empty()) else {
                return Err(format!(
                    "live source id {id} routes to leaf {:?}, which has {n} shards, and \
                     carries no stable key to pick one by; give the leaf one shard or \
                     rebuild the rows through routed ingest",
                    leaf.name
                ));
            };
            let hash = crate::coordinator::stable_routing_hash(key);
            base + children[base..base + n]
                .iter()
                .position(|child| hash >= child.hash_lo && hash <= child.hash_hi)
                .expect("the tiles cover the hash space")
        };
        Ok((index, leaf.code, moved))
    };
    // A column cut plans its buckets from a first pass over the columns.
    let mut cut_plans: Option<Vec<ChildCutPlan>> = match &options.cut {
        SpillCut::Hash => None,
        SpillCut::Column {
            column: cut_column,
            rows_per_cut,
        } => {
            if *rows_per_cut == 0 {
                return Err("a column cut needs a positive row bound".to_string());
            }
            if cut_column == &column {
                return Err(format!(
                    "the cut column {cut_column:?} is the placement column; every row of a \
                     child carries one code there, cut by another column"
                ));
            }
            let mut histograms: Vec<BTreeMap<i64, u64>> =
                (0..children.len()).map(|_| BTreeMap::new()).collect();
            let mut unkeyed = vec![0u64; children.len()];
            match &sources {
                Some((shards, tables)) => {
                    if !tables
                        .columns
                        .integers
                        .iter()
                        .any(|name| name == cut_column)
                    {
                        return Err(format!(
                            "the cut column {cut_column:?} is not an integer column of the \
                             sources ({:?})",
                            tables.columns.integers
                        ));
                    }
                    for shard in shards {
                        for i in 0..shard.set.len() {
                            let bm25 = shard.set.bm25(i);
                            let rows = bm25.next_doc_id();
                            for row in 0..rows {
                                let label = shard.set.metadata(i).base_label + u64::from(row);
                                if !shard.is_live(i, row, label) {
                                    continue;
                                }
                                let id = shard.slot_offset + label;
                                let document = reconstruct_document(bm25, row, tables, false)?;
                                let (index, _, _) = route(id, &document, None)?;
                                match partition_key_of(&document, cut_column)? {
                                    Some(key) => *histograms[index].entry(key).or_insert(0) += 1,
                                    None => unkeyed[index] += 1,
                                }
                            }
                        }
                    }
                }
                None => {
                    for gen in gens {
                        let current = read_gen_manifest(gen)?;
                        for bucket in 0..current.bucket_count {
                            let mut replay = Replay::default();
                            replay_buckets_through(
                                gen,
                                bucket..bucket + 1,
                                current.bucket_count as usize,
                                current.dim as usize,
                                false,
                                u64::MAX,
                                &mut replay,
                            )?;
                            replay.compact();
                            for (id, document) in &replay.documents {
                                let (index, _, _) =
                                    route(*id, document, replay.stable_keys.get(id))?;
                                match partition_key_of(document, cut_column)? {
                                    Some(key) => *histograms[index].entry(key).or_insert(0) += 1,
                                    None => unkeyed[index] += 1,
                                }
                            }
                        }
                    }
                }
            }
            Some(plan_cuts(&histograms, &unkeyed, *rows_per_cut))
        }
    };
    let spill_bucket_bits = match (&cut_plans, options.spill_bucket_bits) {
        (Some(plans), _) => {
            let widest = plans
                .iter()
                .map(|plan| plan.buckets)
                .max()
                .unwrap_or(1)
                .max(1);
            let bits = widest.next_power_of_two().trailing_zeros();
            if bits > 16 {
                return Err(format!(
                    "the column cut needs {widest} buckets in one child, above 65,536; raise the \
                     row bound"
                ));
            }
            bits
        }
        (None, Some(bits)) => {
            if bits > 16 {
                return Err(format!(
                    "spill bucket bits {bits} exceed 16 (65,536 bucket files per child)"
                ));
            }
            bits
        }
        (None, None) => {
            let mut widest = manifest.bucket_count;
            for gen in gens {
                widest = widest.max(read_gen_manifest(gen)?.bucket_count);
            }
            widest.trailing_zeros()
        }
    };
    let mut spill_manifest = manifest.clone();
    spill_manifest.bucket_bits = spill_bucket_bits;
    spill_manifest.bucket_count = 1u32 << spill_bucket_bits;
    let spill_bucket_count = spill_manifest.bucket_count;
    // The spill pass holds a log file and an analysis sidecar open per
    // bucket per child, and the sources' segments beside them; a plan
    // past the process's open-files limit dies deep into the pass (the
    // archive at 150,000 rows per cut: "Too many open files" after 34
    // minutes), so it is refused here by name.
    {
        let spill_files = (0..children.len())
            .map(|index| match &cut_plans {
                Some(plans) => u64::from(plans[index].buckets),
                None => u64::from(spill_bucket_count),
            })
            .sum::<u64>()
            .saturating_mul(2);
        let source_files = sources.as_ref().map_or(0, |(shards, _)| {
            shards
                .iter()
                .map(|shard| shard.set.len() as u64)
                .sum::<u64>()
                .saturating_mul(2)
        });
        let needed = spill_files.saturating_add(source_files).saturating_add(64);
        let limit = match options.open_files_limit {
            Some(limit) => Some(limit),
            None => open_files_soft_limit(),
        };
        if let Some(limit) = limit {
            if needed > limit {
                return Err(format!(
                    "the split would hold about {needed} files open ({spill_files} spill logs and \
                     analysis sidecars over {} children, {source_files} for the sources' segments) \
                     above this process's open-files limit of {limit}; raise it (ulimit -n, or \
                     the reshard raises its own to the hard limit at start) or use fewer, larger \
                     cuts (--cut-rows)",
                    children.len()
                ));
            }
        }
    }
    let mut spills = Vec::with_capacity(children.len());
    for index in 0..children.len() {
        let dir = spill_root.join(format!("c{index:05}"));
        let writer = wal::WalWriter::create(&dir, spill_manifest.clone())
            .map_err(|error| format!("create spill log {}: {error}", dir.display()))?;
        spills.push(writer);
    }
    let mut sidecars: Vec<Vec<Option<std::io::BufWriter<std::fs::File>>>> = (0..children.len())
        .map(|_| (0..spill_bucket_count).map(|_| None).collect())
        .collect();
    let mut moved = 0u64;
    let mut placed = vec![0u64; children.len()];
    let mut transplanted_rows = 0u64;
    let mut peak_transpose_bytes = 0u64;
    let mut seen: BTreeSet<u64> = BTreeSet::new();
    let cut_column: Option<&str> = match &options.cut {
        SpillCut::Column { column, .. } => Some(column.as_str()),
        SpillCut::Hash => None,
    };
    let rows_per_cut = match &options.cut {
        SpillCut::Column { rows_per_cut, .. } => *rows_per_cut,
        SpillCut::Hash => 0,
    };
    // File one routed row: its records into the child's spill, in the
    // bucket the cut names (or the one the id hashes to), and its
    // transplanted fields into the bucket's sidecar.
    let mut file_row = |index: usize,
                        id: u64,
                        rewritten: AddDocumentsRequest,
                        vector: Option<(Vec<f32>, u32)>,
                        keys: Vec<Vec<u8>>,
                        fields: Option<&[AnalyzedField]>|
     -> Result<(), String> {
        let bucket = match (cut_plans.as_mut(), cut_column) {
            (Some(plans), Some(cut_column)) => {
                let key = partition_key_of(&rewritten, cut_column)?;
                Some(plans[index].bucket(key, rows_per_cut))
            }
            _ => None,
        };
        let document = wal_record::Op::AddDocuments(crate::pb::wal::LoggedAddDocuments {
            source_references: Vec::new(),
            first_id: id,
            documents: vec![rewritten],
            stable_routing_keys: keys.clone(),
        });
        match bucket {
            Some(bucket) => spills[index].append_to_bucket(bucket, document),
            None => spills[index].append(document),
        }
        .map_err(|error| format!("spill document {id}: {error}"))?;
        if let Some((vector, dim)) = vector {
            let record = wal_record::Op::AddVectors(crate::pb::wal::LoggedAddVectors {
                first_id: id,
                batch: Some(crate::pb::AddVectorsRequest {
                    vectors: vector,
                    dim,
                }),
                stable_routing_keys: keys,
            });
            match bucket {
                Some(bucket) => spills[index].append_to_bucket(bucket, record),
                None => spills[index].append(record),
            }
            .map_err(|error| format!("spill vector {id}: {error}"))?;
        }
        if let Some(fields) = fields {
            let bucket = bucket.unwrap_or(bucket_of(id, spill_bucket_count as usize) as u32);
            let slot = &mut sidecars[index][bucket as usize];
            if slot.is_none() {
                let path = analysis_sidecar_path(spills[index].dir(), bucket);
                let file = std::fs::File::create(&path)
                    .map_err(|error| format!("create {}: {error}", path.display()))?;
                *slot = Some(std::io::BufWriter::with_capacity(1 << 20, file));
            }
            write_analysis_entry(slot.as_mut().expect("opened above"), id, fields)
                .map_err(|error| format!("spill analysis of {id}: {error}"))?;
        }
        Ok(())
    };
    match &sources {
        None => {
            for gen in gens {
                let current = read_gen_manifest(gen)?;
                let bucket_count = current.bucket_count as usize;
                let dim = current.dim as usize;
                for bucket in 0..current.bucket_count {
                    let mut replay = Replay::default();
                    replay_buckets_through(
                        gen,
                        bucket..bucket + 1,
                        bucket_count,
                        dim,
                        false,
                        u64::MAX,
                        &mut replay,
                    )?;
                    replay.compact();
                    for id in child_order(&replay) {
                        if !seen.insert(id) {
                            return Err(format!(
                                "live source id {id} appears in more than one input generation; \
                                 the inputs overlap"
                            ));
                        }
                        let Some(document) = replay.documents.get(&id) else {
                            return Err(format!(
                                "live source id {id} has a vector and no document; a re-placement \
                                 split evaluates the tree on documents (a code-keyed split, \
                                 --placement-ranges, routes such a row to its default child)"
                            ));
                        };
                        let (index, code, changed) =
                            route(id, document, replay.stable_keys.get(&id))?;
                        if changed {
                            moved += 1;
                        }
                        let mut rewritten = document.clone();
                        rewritten.integers.retain(|value| value.field != column);
                        rewritten.integers.push(crate::pb::IntegerValue {
                            field: column.clone(),
                            value: code,
                        });
                        let keys: Vec<Vec<u8>> =
                            replay.stable_keys.get(&id).cloned().into_iter().collect();
                        let vector = replay
                            .vectors
                            .get(&id)
                            .map(|vector| (vector.clone(), current.dim));
                        file_row(index, id, rewritten, vector, keys, None)?;
                        placed[index] += 1;
                    }
                }
            }
        }
        Some((shards, tables)) => {
            for shard in shards {
                let current = read_gen_manifest(&shard.gen)?;
                for i in 0..shard.set.len() {
                    let meta = shard.set.metadata(i);
                    let bm25 = shard.set.bm25(i);
                    let exact = open_segment_exact(shard, i)?;
                    let rows = bm25.next_doc_id();
                    if u64::from(rows) != meta.rows {
                        return Err(format!(
                            "{} segment {}: the store has {rows} rows, the metadata {}",
                            shard.root.display(),
                            meta.segment_id,
                            meta.rows
                        ));
                    }
                    let mut transposes = Vec::with_capacity(bm25.field_count());
                    let mut bytes = 0u64;
                    for f in 0..bm25.field_count() {
                        let transpose = bm25.field(f).transpose().map_err(|e| {
                            format!("{} segment {}: {e}", shard.root.display(), meta.segment_id)
                        })?;
                        bytes += transpose.bytes() as u64;
                        transposes.push(transpose);
                    }
                    peak_transpose_bytes = peak_transpose_bytes.max(bytes);
                    for row in 0..rows {
                        let label = meta.base_label + u64::from(row);
                        if !shard.is_live(i, row, label) {
                            continue;
                        }
                        let id = shard.slot_offset + label;
                        if !seen.insert(id) {
                            return Err(format!(
                                "live source id {id} appears in more than one source; the inputs \
                                 overlap"
                            ));
                        }
                        let document =
                            reconstruct_document(bm25, row, tables, true).map_err(|e| {
                                format!("{} segment {}: {e}", shard.root.display(), meta.segment_id)
                            })?;
                        let (index, code, changed) = route(id, &document, None)?;
                        if changed {
                            moved += 1;
                        }
                        let mut rewritten = document;
                        rewritten.integers.retain(|value| value.field != column);
                        rewritten.integers.push(crate::pb::IntegerValue {
                            field: column.clone(),
                            value: code,
                        });
                        let vector = exact.as_ref().map(|store| {
                            (
                                store.row_values(row as usize, row as usize + 1),
                                current.dim,
                            )
                        });
                        let fields: Vec<AnalyzedField> = transposes
                            .iter()
                            .map(|transpose| {
                                transpose.field(row).ok_or_else(|| {
                                    format!(
                                        "{} segment {}: row {row} is past the transpose",
                                        shard.root.display(),
                                        meta.segment_id
                                    )
                                })
                            })
                            .collect::<Result<_, _>>()?;
                        file_row(index, id, rewritten, vector, Vec::new(), Some(&fields))?;
                        transplanted_rows += 1;
                        placed[index] += 1;
                    }
                }
            }
        }
    }
    drop(seen);
    for child in &mut sidecars {
        for writer in child.iter_mut().flatten() {
            use std::io::Write;
            writer
                .flush()
                .map_err(|error| format!("flush analysis sidecar: {error}"))?;
        }
    }
    drop(sidecars);
    let spill_dirs: Vec<PathBuf> = spills
        .iter_mut()
        .map(|writer| {
            writer
                .flush()
                .map(|()| writer.dir().to_path_buf())
                .map_err(|error| format!("flush spill log {}: {error}", writer.dir().display()))
        })
        .collect::<Result<_, _>>()?;
    drop(spills);
    let (pinned_fingerprints, pinned_columns, pinned_fields): (
        Option<Vec<u64>>,
        Option<ColumnTables>,
        Option<Vec<String>>,
    ) = match &sources {
        Some((_, tables)) => (
            Some(tables.fingerprints.clone()),
            Some(tables.columns.clone()),
            Some(tables.fields.clone()),
        ),
        None => (None, None, None),
    };
    let bm25_fields: Option<&[String]> = match &pinned_fields {
        Some(fields) => Some(fields.as_slice()),
        None => bm25_fields,
    };
    let check_routing = cut_plans.is_none();
    drop(cut_plans);

    // Pass 2: each child from its spill.
    let dim = manifest.dim as usize;
    let generation = top_generation
        .checked_add(1)
        .ok_or_else(|| "re-placement split generation overflow".to_string())?;
    // A child's rows take slots from its offset up to the next offset
    // above it; the last child is unbounded.
    let slot_bound = |index: usize| -> Option<u64> {
        slot_offsets
            .iter()
            .copied()
            .filter(|offset| *offset > slot_offsets[index])
            .min()
    };
    let mut images = Vec::with_capacity(children.len());
    let mut segments = vec![0usize; children.len()];
    let mut peak_replay_rows = 0u64;
    match options.layout {
        TreeChildLayout::SingleImage { max_child_rows } => {
            for (index, child) in children.iter().enumerate() {
                if placed[index] > max_child_rows {
                    return Err(format!(
                        "child {index} ({}) has {} documents, above the single-image bound of \
                         {max_child_rows}; use the segmented layout or raise the bound",
                        child.leaf, placed[index]
                    ));
                }
            }
            for (index, (child, spill_dir)) in children.iter().zip(&spill_dirs).enumerate() {
                let mut replay = Replay::default();
                replay_buckets_routed(
                    spill_dir,
                    0..spill_bucket_count,
                    spill_bucket_count as usize,
                    dim,
                    false,
                    u64::MAX,
                    check_routing,
                    &mut replay,
                )?;
                if sources.is_some() {
                    for bucket in 0..spill_bucket_count {
                        replay
                            .analyzed
                            .extend(read_analysis_file(&analysis_sidecar_path(
                                spill_dir, bucket,
                            ))?);
                    }
                }
                replay.compact();
                if replay.documents.len() as u64 != placed[index] {
                    return Err(format!(
                        "child {index} ({}): the spill log holds {} documents, routing counted {}",
                        child.leaf,
                        replay.documents.len(),
                        placed[index]
                    ));
                }
                peak_replay_rows = peak_replay_rows.max(live_rows(&replay));
                let image = finish_child_pinned(
                    &manifest,
                    replay,
                    index,
                    out_dir,
                    slot_offsets[index],
                    child.hash_lo,
                    child.hash_hi,
                    bm25_fields,
                    pinned_fingerprints.as_deref(),
                    binding.as_ref(),
                    pinned_columns.as_ref(),
                    analyze,
                )?;
                check_slot_bound(
                    index,
                    &child.leaf,
                    image.slot_offset,
                    image_rows(&image),
                    slot_bound(index),
                )?;
                segments[index] = 1;
                images.push(image);
            }
        }
        TreeChildLayout::Segmented => {
            let backend_kind = manifest.backend_config()?.backend_kind;
            let work_root = out_dir.join("work");
            for (index, (child, spill_dir)) in children.iter().zip(&spill_dirs).enumerate() {
                let index_path = out_dir.join(format!("shard-{index}.tv"));
                let root = crate::node::segments_root(&index_path);
                if options.only_child.is_some_and(|only| only != index) {
                    eprintln!(
                        "reshard: child {index} ({}): not built (--only-child names another child)",
                        child.leaf
                    );
                    images.push(ChildImage {
                        vector_path: index_path,
                        exact_vector_path: root,
                        bm25_path: None,
                        slot_offset: slot_offsets[index],
                        hash_lo: child.hash_lo,
                        hash_hi: child.hash_hi,
                        num_vectors: 0,
                        num_documents: 0,
                        parent_ids: Vec::new(),
                        row_parent_ids: Vec::new(),
                    });
                    continue;
                }
                if root.exists() {
                    return Err(format!(
                        "child {index} ({}): {} already exists; a re-placement split writes a \
                         fresh catalog",
                        child.leaf,
                        root.display()
                    ));
                }
                let catalog = crate::segments::SegmentCatalog::open(&root)?;
                let work = work_root.join(format!("c{index:05}"));
                std::fs::create_dir_all(&work)
                    .map_err(|error| format!("mkdir {}: {error}", work.display()))?;
                let bound = slot_bound(index);
                let mut next_slot = slot_offsets[index];
                let mut documents = 0u64;
                let mut vectors = 0u64;
                for bucket in 0..spill_bucket_count {
                    let mut replay = Replay::default();
                    replay_buckets_routed(
                        spill_dir,
                        bucket..bucket + 1,
                        spill_bucket_count as usize,
                        dim,
                        false,
                        u64::MAX,
                        check_routing,
                        &mut replay,
                    )?;
                    if sources.is_some() {
                        replay.analyzed =
                            read_analysis_file(&analysis_sidecar_path(spill_dir, bucket))?;
                    }
                    replay.compact();
                    let rows = live_rows(&replay);
                    peak_replay_rows = peak_replay_rows.max(rows);
                    if rows == 0 {
                        continue;
                    }
                    let ordinal = segments[index];
                    let image = finish_child_pinned(
                        &manifest,
                        replay,
                        ordinal,
                        &work,
                        next_slot,
                        child.hash_lo,
                        child.hash_hi,
                        bm25_fields,
                        pinned_fingerprints.as_deref(),
                        binding.as_ref(),
                        pinned_columns.as_ref(),
                        analyze,
                    )?;
                    let image_rows = image_rows(&image);
                    check_slot_bound(index, &child.leaf, next_slot, image_rows, bound)?;
                    if image.num_vectors != 0 && image.num_vectors != image.num_documents {
                        return Err(format!(
                            "child {index} ({}) bucket {bucket}: {} vectors but {} documents; a \
                             sealed segment is aligned (one document per vector) or has no \
                             vectors at all",
                            child.leaf, image.num_vectors, image.num_documents
                        ));
                    }
                    let Some(bm25_path) = image.bm25_path.as_deref() else {
                        return Err(format!(
                            "child {index} ({}) bucket {bucket}: no BM25 image was written for \
                             {} documents",
                            child.leaf, image.num_documents
                        ));
                    };
                    let live_path = crate::node::live_docs_sidecar_path(&image.vector_path);
                    crate::live_docs::LiveDocs::default()
                        .write(&live_path, image_rows)
                        .map_err(|error| {
                            format!("write live docs {}: {error}", live_path.display())
                        })?;
                    let segment_id = format!("g{generation}-c{index}-p{ordinal}");
                    let (vector_path, exact_path) = if image.num_vectors == 0 {
                        (None, None)
                    } else {
                        (
                            Some(image.vector_path.as_path()),
                            Some(image.exact_vector_path.as_path()),
                        )
                    };
                    catalog
                        .append(crate::segments::SegmentSource {
                            segment_id: &segment_id,
                            generation,
                            // Labels are shard-local: the node adds its
                            // slot offset when it serves the catalog.
                            base_label: next_slot - slot_offsets[index],
                            backend_kind: &backend_kind,
                            vector_path,
                            exact_vector_path: exact_path,
                            bm25_path,
                            live_docs_path: &live_path,
                            partition_column: cut_column,
                        })
                        .map_err(|error| {
                            format!(
                                "child {index} ({}): seal segment {segment_id}: {error}",
                                child.leaf
                            )
                        })?;
                    for path in [
                        Some(image.vector_path.as_path()),
                        Some(image.exact_vector_path.as_path()),
                        Some(bm25_path),
                        Some(live_path.as_path()),
                    ]
                    .into_iter()
                    .flatten()
                    {
                        if path.exists() {
                            std::fs::remove_file(path).map_err(|error| {
                                format!("remove staged {}: {error}", path.display())
                            })?;
                        }
                    }
                    next_slot = next_slot
                        .checked_add(image_rows)
                        .ok_or_else(|| "re-placement split slot overflow".to_string())?;
                    documents += image.num_documents;
                    vectors += image.num_vectors;
                    segments[index] += 1;
                }
                if documents != placed[index] {
                    return Err(format!(
                        "child {index} ({}): the sealed segments hold {documents} documents, \
                         routing counted {}",
                        child.leaf, placed[index]
                    ));
                }
                if let Some(column) = cut_column {
                    catalog
                        .publish_partition_key(Some(column.to_string()))
                        .map_err(|error| {
                            format!(
                                "child {index} ({}): publish partition key: {error}",
                                child.leaf
                            )
                        })?;
                }
                std::fs::remove_dir_all(&work)
                    .map_err(|error| format!("remove work {}: {error}", work.display()))?;
                eprintln!(
                    "reshard: child {index} ({}): {vectors} vectors, {documents} documents in {} \
                     segments -> {}",
                    child.leaf,
                    segments[index],
                    index_path.display()
                );
                images.push(ChildImage {
                    vector_path: index_path,
                    exact_vector_path: root,
                    bm25_path: None,
                    slot_offset: slot_offsets[index],
                    hash_lo: child.hash_lo,
                    hash_hi: child.hash_hi,
                    num_vectors: vectors,
                    num_documents: documents,
                    parent_ids: Vec::new(),
                    row_parent_ids: Vec::new(),
                });
            }
            if work_root.exists() {
                std::fs::remove_dir_all(&work_root)
                    .map_err(|error| format!("remove work {}: {error}", work_root.display()))?;
            }
        }
    }
    std::fs::remove_dir_all(&spill_root)
        .map_err(|error| format!("remove spill {}: {error}", spill_root.display()))?;
    Ok(TreeReshardOutput {
        images: ReshardOutput {
            generation,
            children: images,
        },
        children,
        moved,
        placed,
        layout: options.layout,
        segments,
        peak_replay_rows,
        spill_bucket_count,
        source: options.source,
        cut: options.cut,
        peak_transpose_bytes,
        transplanted_rows,
    })
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
    };
    toml::to_string(&map).map_err(|error| format!("render shard map: {error}"))
}

/// One live row of a compaction's dense image, in the order the sink
/// receives them: new local slot order, which is also the order the
/// rewritten log records them in.
pub struct CompactedRow<'a> {
    /// The row's local slot in the dense image (the node adds its slot
    /// offset for the global id).
    pub new_local: u64,
    /// The global id the row had in the source generation.
    pub old_id: u64,
    pub vector: Option<&'a [f32]>,
    pub document: Option<&'a AddDocumentsRequest>,
    pub stable_key: Option<&'a [u8]>,
}

/// Where a compaction's live rows go besides the image: the rewritten
/// full-history WAL generation (`docs/mutations.md`). Called once per row
/// in slot order, before the image is built from the same replay.
pub type RowSink<'a> = dyn FnMut(CompactedRow<'_>) -> Result<(), String> + 'a;

/// What [`compact_log`] built: the dense image(s), the id map, and the
/// counts the operator asked for.
#[derive(Debug)]
pub struct CompactionBuild {
    /// The rewritten generation's number: one past the source.
    pub generation: u64,
    /// Rows the source generation held through the cutoff, tombstoned or
    /// not.
    pub rows_before: u64,
    /// Rows the cutoff had tombstoned, which the dense image drops.
    pub tombstones: u64,
    /// Source global id -> dense LOCAL slot, for every live row.
    pub id_map: BTreeMap<u64, u64>,
    /// The dense image(s): one for the single-image layout; one per
    /// non-empty WAL bucket for the segment layout, each with its
    /// `slot_offset` set to its local base label.
    pub images: Vec<ChildImage>,
    /// The mapped-plan binding the source carried, to be logged first in
    /// the rewritten generation.
    pub binding: Option<crate::postings::StoredBinding>,
}

/// Hand every live row of `replay` to `sink` in dense slot order — the
/// order [`build_child`] assigns: vector rows in parent-id order, then
/// the document-only rows in parent-id order — and extend `id_map`
/// with the mapping. Returns the number of rows emitted.
fn emit_rows(
    replay: &Replay,
    slot_base: u64,
    id_map: &mut BTreeMap<u64, u64>,
    sink: &mut RowSink,
) -> Result<u64, String> {
    let order = child_order(replay);
    emit_rows_in(replay, &order, slot_base, id_map, sink)
}

/// The slot order [`build_child`] assigns a replay by default: vector
/// rows in source-id order, then the document-only rows in source-id
/// order.
fn child_order(replay: &Replay) -> Vec<u64> {
    let mut order: Vec<u64> = replay.vectors.keys().copied().collect();
    order.extend(
        replay
            .documents
            .keys()
            .filter(|id| !replay.vectors.contains_key(id))
            .copied(),
    );
    order
}

/// [`emit_rows`] in an explicit slot order.
fn emit_rows_in(
    replay: &Replay,
    order: &[u64],
    slot_base: u64,
    id_map: &mut BTreeMap<u64, u64>,
    sink: &mut RowSink,
) -> Result<u64, String> {
    for (slot, old_id) in order.iter().enumerate() {
        let new_local = slot_base + slot as u64;
        if id_map.insert(*old_id, new_local).is_some() {
            return Err(format!(
                "compaction saw source id {old_id} twice; the log is corrupt"
            ));
        }
        sink(CompactedRow {
            new_local,
            old_id: *old_id,
            vector: replay.vectors.get(old_id).map(Vec::as_slice),
            document: replay.documents.get(old_id),
            stable_key: replay.stable_keys.get(old_id).map(Vec::as_slice),
        })?;
    }
    Ok(order.len() as u64)
}

/// Rows a replay holds before its tombstones are applied, and how many of
/// them are tombstoned: `(present, tombstoned)`.
fn replay_counts(replay: &Replay) -> (u64, u64) {
    let present: BTreeSet<&u64> = replay
        .vectors
        .keys()
        .chain(replay.documents.keys())
        .collect();
    let tombstoned = replay
        .deleted
        .iter()
        .filter(|id| present.contains(id))
        .count() as u64;
    (present.len() as u64, tombstoned)
}

/// Build the dense all-live image of ONE generation through
/// `cutoff_clock`, the baseline of an online compaction
/// (`docs/mutations.md`): the log prefix is replayed with its deletes and
/// replacements applied, every live row is handed to `sink` in dense slot
/// order (the rewritten log), and the image is written under `out_dir`.
/// Local slots start at 0; the caller owns the global offset.
///
/// `segmented` selects the bucket-bounded shape: one sealed image per
/// non-empty WAL bucket, slots dense across them in bucket order, memory
/// holding one bucket's replay at a time — the same shape
/// [`split_logs_segmented`] gives a catalog. Otherwise one image over the
/// whole replay, as [`split`] with a factor of one.
///
/// The generation must be full history with a usable provider
/// configuration, exactly as for a reshard; those checks refuse here
/// before anything is read. `columns` gives each image the shard's
/// column tables, so a bucket that holds no record of a declared column
/// still declares it and the outputs open under the node's
/// configuration.
#[allow(clippy::too_many_arguments)]
pub fn compact_log(
    gen: &Path,
    cutoff_clock: u64,
    out_dir: &Path,
    segmented: bool,
    bm25_fields: Option<&[String]>,
    pinned_fingerprints: Option<&[u64]>,
    columns: Option<&ColumnTables>,
    analyze: &mut Analyzer,
    sink: &mut RowSink,
) -> Result<CompactionBuild, String> {
    let manifest = read_gen_manifest(gen)?;
    require_backend_config(&manifest, gen)?;
    require_complete_history(&manifest, gen)?;
    let binding = read_gens_binding(std::slice::from_ref(&gen.to_path_buf()))?;
    std::fs::create_dir_all(out_dir)
        .map_err(|error| format!("mkdir {}: {error}", out_dir.display()))?;
    let bucket_count = manifest.bucket_count as usize;
    let dim = manifest.dim as usize;
    let generation = manifest
        .generation
        .checked_add(1)
        .ok_or_else(|| "compaction generation overflow".to_string())?;
    let mut id_map = BTreeMap::new();
    let mut images = Vec::new();
    let mut rows_before = 0u64;
    let mut tombstones = 0u64;
    if !segmented {
        let mut replay = Replay::default();
        replay_buckets_through(
            gen,
            0..manifest.bucket_count,
            bucket_count,
            dim,
            false,
            cutoff_clock,
            &mut replay,
        )?;
        let (present, tombstoned) = replay_counts(&replay);
        rows_before = present;
        tombstones = tombstoned;
        replay.compact();
        emit_rows(&replay, 0, &mut id_map, sink)?;
        images.push(finish_child_pinned(
            &manifest,
            replay,
            0,
            out_dir,
            0,
            0,
            u64::MAX,
            bm25_fields,
            pinned_fingerprints,
            binding.as_ref(),
            columns,
            analyze,
        )?);
    } else {
        let mut next_slot = 0u64;
        for bucket in 0..manifest.bucket_count {
            let mut replay = Replay::default();
            replay_buckets_through(
                gen,
                bucket..bucket + 1,
                bucket_count,
                dim,
                false,
                cutoff_clock,
                &mut replay,
            )?;
            let (present, tombstoned) = replay_counts(&replay);
            rows_before += present;
            tombstones += tombstoned;
            replay.compact();
            let rows = emit_rows(&replay, next_slot, &mut id_map, sink)?;
            if rows == 0 {
                continue;
            }
            let ordinal = images.len();
            images.push(finish_child_pinned(
                &manifest,
                replay,
                ordinal,
                out_dir,
                next_slot,
                0,
                u64::MAX,
                bm25_fields,
                pinned_fingerprints,
                binding.as_ref(),
                columns,
                analyze,
            )?);
            next_slot = next_slot
                .checked_add(rows)
                .ok_or_else(|| "compaction slot overflow".to_string())?;
        }
    }
    Ok(CompactionBuild {
        generation,
        rows_before,
        tombstones,
        id_map,
        images,
        binding,
    })
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

/// One source shard's catalog beside its log, with the rows its shard
/// overlay and its log have deleted since the segments sealed.
struct SegmentSourceShard {
    gen: PathBuf,
    root: PathBuf,
    set: std::sync::Arc<crate::segments::OpenedSegmentSet>,
    /// The shard-wide live-document overlay (`<index>.tv.live`), when
    /// written: a delete after a seal lands there, not in the segment's
    /// own sidecar.
    overlay: Option<crate::live_docs::LiveDocs>,
    /// Rows the log deletes or replaces, as local labels.
    deleted: BTreeSet<u64>,
    /// The shard's slot offset: a global id is the offset plus the label.
    slot_offset: u64,
}

impl SegmentSourceShard {
    /// Whether shard-local row `label` (segment `i`, row `row` in it) is
    /// live: the segment's sidecar, the shard overlay, and the log agree.
    fn is_live(&self, i: usize, row: u32, label: u64) -> bool {
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
struct SourceTables {
    fields: Vec<String>,
    fingerprints: Vec<u64>,
    position_fields: Vec<String>,
    sentence_fields: Vec<String>,
    columns: ColumnTables,
}

/// The catalog root of the shard whose log generation is `gen`:
/// `<index>.tv.wal/gen-N` -> `<index>.tv.segments`.
fn segments_root_of_gen(gen: &Path) -> Result<PathBuf, String> {
    let wal_dir = gen.parent().ok_or_else(|| {
        format!(
            "{}: a generation directory has a log directory above it",
            gen.display()
        )
    })?;
    let name = wal_dir
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| format!("{}: log directory has no name", wal_dir.display()))?;
    let Some(index) = name.strip_suffix(".wal") else {
        return Err(format!(
            "{}: the log directory is not named <index>.wal, so the segment catalog beside it \
             cannot be found; split from the logs instead",
            wal_dir.display()
        ));
    };
    let index_path = wal_dir.with_file_name(index);
    Ok(crate::node::segments_root(&index_path))
}

/// `<index>.tv` of a catalog root `<index>.tv.segments`.
fn index_path_of_root(root: &Path) -> PathBuf {
    let name = root
        .file_name()
        .and_then(|n| n.to_str())
        .and_then(|n| n.strip_suffix(".segments"))
        .unwrap_or_default();
    root.with_file_name(name)
}

/// The highest row the log has appended plus one, over its bucket
/// files, and the rows its records delete or replace.
fn wal_row_watermark(gen: &Path, bucket_count: u32) -> Result<(u64, BTreeSet<u64>), String> {
    let mut watermark = 0u64;
    let mut deleted = BTreeSet::new();
    for bucket in 0..bucket_count {
        let path = wal::bucket_path(gen, bucket);
        if !path.exists() {
            continue;
        }
        let mut reader =
            RecordReader::open(&path).map_err(|e| format!("open {}: {e}", path.display()))?;
        while let Some(record) = reader
            .next_record()
            .map_err(|e| format!("read {}: {e}", path.display()))?
        {
            let end = match record.op {
                Some(wal_record::Op::AddVectors(a)) => {
                    let rows = a.batch.as_ref().map_or(0, |b| {
                        if b.dim == 0 {
                            0
                        } else {
                            (b.vectors.len() / b.dim as usize) as u64
                        }
                    });
                    Some(a.first_id.saturating_add(rows))
                }
                Some(wal_record::Op::AddDocuments(a)) => {
                    Some(a.first_id.saturating_add(a.documents.len() as u64))
                }
                Some(wal_record::Op::DeleteDocument(d)) => {
                    deleted.insert(d.doc_id);
                    None
                }
                Some(wal_record::Op::Replacement(r)) => {
                    deleted.insert(r.old_doc_id);
                    None
                }
                _ => None,
            };
            if let Some(end) = end {
                watermark = watermark.max(end);
            }
        }
    }
    Ok((watermark, deleted))
}

/// Open the catalogs beside the logs and check what a child pins
/// (`docs/replay-from-segments.md`, "The replay").
fn open_segment_sources(
    gens: &[PathBuf],
) -> Result<(Vec<SegmentSourceShard>, SourceTables), String> {
    let mut shards = Vec::with_capacity(gens.len());
    let mut tables: Option<SourceTables> = None;
    for gen in gens {
        let root = segments_root_of_gen(gen)?;
        if !root.exists() {
            return Err(format!(
                "{}: no segment catalog at {}; the source is a single image (or was never \
                 flushed on the segment layout), split it from the logs",
                gen.display(),
                root.display()
            ));
        }
        let set = std::sync::Arc::new(crate::segments::OpenedSegmentSet::open(&root)?);
        let manifest = read_gen_manifest(gen)?;
        let mut rows = 0u64;
        for i in 0..set.len() {
            let meta = set.metadata(i);
            if meta.base_label != rows {
                return Err(format!(
                    "{}: segment {} starts at label {} but the segments before it end at {rows}",
                    root.display(),
                    meta.segment_id,
                    meta.base_label
                ));
            }
            rows = meta.end_label_exclusive()?;
        }
        let (watermark, deleted) = wal_row_watermark(gen, manifest.bucket_count)?;
        // The log's ids are global (the shard's slot offset plus the
        // local row); the catalog's labels are local. Compare in local
        // terms, and name a log whose ids sit below its own offset.
        let local_watermark = if watermark == 0 {
            0
        } else {
            watermark.checked_sub(manifest.slot_offset).ok_or_else(|| {
                format!(
                    "{}: the log holds rows through {} but its manifest places the shard at slot \
                     offset {}; the log and its manifest disagree",
                    gen.display(),
                    watermark - 1,
                    manifest.slot_offset
                )
            })?
        };
        if local_watermark > rows {
            return Err(format!(
                "{}: the log holds rows through {} (local row {}) but the catalog at {} ends at \
                 {rows}; the unsealed tail would be lost, flush the source first",
                gen.display(),
                watermark - 1,
                local_watermark - 1,
                root.display()
            ));
        }
        for i in 0..set.len() {
            let bm25 = set.bm25(i);
            let fields: Vec<String> = (0..bm25.field_count())
                .map(|f| bm25.field_name(f).to_string())
                .collect();
            let fingerprints: Vec<u64> = (0..bm25.field_count())
                .map(|f| bm25.analysis_fingerprint(f))
                .collect();
            let position_fields: Vec<String> = (0..bm25.field_count())
                .filter(|&f| bm25.field_has_positions(f))
                .map(|f| bm25.field_name(f).to_string())
                .collect();
            let sentence_fields: Vec<String> = (0..bm25.field_count())
                .filter(|&f| bm25.field_has_sentences(f))
                .map(|f| bm25.field_name(f).to_string())
                .collect();
            let columns = ColumnTables {
                facets: (0..bm25.facet_count())
                    .map(|c| bm25.facet_name(c).to_string())
                    .collect(),
                numerics: (0..bm25.numeric_count())
                    .map(|c| bm25.numeric_name(c).to_string())
                    .collect(),
                map_facets: (0..bm25.map_facet_count())
                    .map(|c| bm25.map_facet_name(c).to_string())
                    .collect(),
                map_numerics: (0..bm25.map_numeric_count())
                    .map(|c| bm25.map_numeric_name(c).to_string())
                    .collect(),
                map_integers: (0..bm25.map_integer_count())
                    .map(|c| bm25.map_integer_name(c).to_string())
                    .collect(),
                map_unsigned_integers: (0..bm25.map_unsigned_integer_count())
                    .map(|c| bm25.map_unsigned_integer_name(c).to_string())
                    .collect(),
                integers: (0..bm25.integer_count())
                    .map(|c| bm25.integer_name(c).to_string())
                    .collect(),
                unsigned_integers: (0..bm25.unsigned_integer_count())
                    .map(|c| bm25.unsigned_integer_name(c).to_string())
                    .collect(),
                geo: (0..bm25.geo_count())
                    .map(|c| bm25.geo_name(c).to_string())
                    .collect(),
            };
            let this = SourceTables {
                fields,
                fingerprints,
                position_fields,
                sentence_fields,
                columns,
            };
            match &tables {
                None => tables = Some(this),
                Some(first) => {
                    let what = format!("{} segment {}", root.display(), set.metadata(i).segment_id);
                    if first.fields != this.fields {
                        return Err(format!(
                            "{what}: field table {:?} differs from the first source's {:?}; a \
                             child is one store and cannot mix tables",
                            this.fields, first.fields
                        ));
                    }
                    if first.fingerprints != this.fingerprints {
                        return Err(format!(
                            "{what}: analysis fingerprints {:?} differ from the first source's \
                             {:?}; a child cannot mix analyzers, split from the logs to re-analyze",
                            this.fingerprints, first.fingerprints
                        ));
                    }
                    if first.position_fields != this.position_fields {
                        return Err(format!(
                            "{what}: positional fields {:?} differ from the first source's {:?}",
                            this.position_fields, first.position_fields
                        ));
                    }
                    if first.sentence_fields != this.sentence_fields {
                        return Err(format!(
                            "{what}: sentence fields {:?} differ from the first source's {:?}",
                            this.sentence_fields, first.sentence_fields
                        ));
                    }
                    if first.columns != this.columns {
                        return Err(format!(
                            "{what}: column tables differ from the first source's; a child is \
                             one store and cannot mix column tables"
                        ));
                    }
                }
            }
        }
        let overlay_path = crate::node::live_docs_sidecar_path(&index_path_of_root(&root));
        let overlay = if overlay_path.exists() {
            Some(
                crate::live_docs::LiveDocs::open(&overlay_path)
                    .map_err(|e| format!("open {}: {e}", overlay_path.display()))?,
            )
        } else {
            None
        };
        // The log names deleted and replaced rows by global id; the
        // catalog's labels are local, so the set is kept in local terms.
        let deleted = deleted
            .into_iter()
            .map(|id| {
                id.checked_sub(manifest.slot_offset).ok_or_else(|| {
                    format!(
                        "{}: the log deletes row {id}, below the shard's slot offset {}; the log \
                         and its manifest disagree",
                        gen.display(),
                        manifest.slot_offset
                    )
                })
            })
            .collect::<Result<BTreeSet<u64>, String>>()?;
        shards.push(SegmentSourceShard {
            gen: gen.clone(),
            root,
            set,
            overlay,
            deleted,
            slot_offset: manifest.slot_offset,
        });
    }
    let tables = tables.ok_or_else(|| {
        "the sources have no sealed segment; there is nothing to transplant".to_string()
    })?;
    Ok((shards, tables))
}

/// The logged document row `row` of `bm25` stands for: its columns,
/// and with `with_text` its text, lineage, source and identity too.
fn reconstruct_document(
    bm25: &crate::postings::Bm25Reader,
    row: u32,
    tables: &SourceTables,
    with_text: bool,
) -> Result<AddDocumentsRequest, String> {
    use crate::postings::Bm25Index;
    let mut doc = AddDocumentsRequest::default();
    if with_text {
        let text =
            Bm25Index::text(bm25, row).ok_or_else(|| format!("row {row} has no stored text"))?;
        doc.text = text;
        doc.lineage = Bm25Index::lineage(bm25, row).map(|l| crate::pb::DocLineage {
            parent_id: l.parent_id,
            group_id: l.group_id,
            span_start: l.span_start,
            span_end: l.span_end,
        });
        if let Some((source, ordinal)) = bm25
            .protobuf_source(row)
            .map_err(|e| format!("row {row}: read original source: {e}"))?
        {
            doc.original_source = Some(source);
            doc.source_chunk_ordinal = ordinal;
        }
        doc.identity = bm25.document_identity(row);
        doc.position_fields = tables.position_fields.clone();
        doc.sentence_fields = tables.sentence_fields.clone();
    }
    for (fi, name) in tables.columns.facets.iter().enumerate() {
        if let Some(ord) = bm25.facet_ord(fi, row) {
            doc.facets.push(crate::pb::FacetValue {
                field: name.clone(),
                value: bm25.facet_value(fi, ord).to_string(),
            });
        }
    }
    for (ni, name) in tables.columns.numerics.iter().enumerate() {
        if let Some(value) = bm25.numeric_value(ni, row) {
            doc.numerics.push(crate::pb::NumericValue {
                field: name.clone(),
                value,
            });
        }
    }
    for (ii, name) in tables.columns.integers.iter().enumerate() {
        if let Some(value) = bm25.integer_value(ii, row) {
            doc.integers.push(crate::pb::IntegerValue {
                field: name.clone(),
                value,
            });
        }
    }
    for (ui, name) in tables.columns.unsigned_integers.iter().enumerate() {
        if let Some(value) = bm25.unsigned_integer_value(ui, row) {
            doc.unsigned_integers.push(crate::pb::UnsignedIntegerValue {
                field: name.clone(),
                value,
            });
        }
    }
    for (gi, name) in tables.columns.geo.iter().enumerate() {
        if let Some((lat, lon)) = bm25.geo_value(gi, row) {
            doc.geo_points.push(crate::pb::GeoPointValue {
                field: name.clone(),
                lat,
                lon,
            });
        }
    }
    for (ci, name) in tables.columns.map_facets.iter().enumerate() {
        for (key_ord, key) in bm25.map_facet_keys(ci).iter().enumerate() {
            if let Some(ord) = bm25.map_facet_value_ord(ci, key_ord as u32, row) {
                doc.map_facets.push(crate::pb::MapFacetEntry {
                    field: name.clone(),
                    key: key.clone(),
                    value: bm25.map_facet_value(ci, ord).to_string(),
                });
            }
        }
    }
    for (ci, name) in tables.columns.map_numerics.iter().enumerate() {
        for (key_ord, key) in bm25.map_numeric_keys(ci).iter().enumerate() {
            if let Some(value) = bm25.map_numeric_value(ci, key_ord as u32, row) {
                doc.map_numerics.push(crate::pb::MapNumericEntry {
                    field: name.clone(),
                    key: key.clone(),
                    value,
                });
            }
        }
    }
    for (ci, name) in tables.columns.map_integers.iter().enumerate() {
        for (key_ord, key) in bm25.map_integer_keys(ci).iter().enumerate() {
            if let Some(value) = bm25.map_integer_value(ci, key_ord as u32, row) {
                doc.map_integers.push(crate::pb::MapIntegerEntry {
                    field: name.clone(),
                    key: key.clone(),
                    value,
                });
            }
        }
    }
    for (ci, name) in tables.columns.map_unsigned_integers.iter().enumerate() {
        for (key_ord, key) in bm25.map_unsigned_integer_keys(ci).iter().enumerate() {
            if let Some(value) = bm25.map_unsigned_integer_value(ci, key_ord as u32, row) {
                doc.map_unsigned_integers
                    .push(crate::pb::MapUnsignedIntegerEntry {
                        field: name.clone(),
                        key: key.clone(),
                        value,
                    });
            }
        }
    }
    Ok(doc)
}

/// The exact-vector sidecar of segment `i`, when the segment has vectors.
fn open_segment_exact(
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

/// Where a child's rows go under a column cut: the cut of each value,
/// a run longer than the bound split by row count, unkeyed rows last.
#[derive(Debug, Clone, Default)]
struct ChildCutPlan {
    /// Inclusive upper key of each cut, ascending.
    bounds: Vec<i64>,
    /// A key whose run is longer than the bound: `(first cut, cuts)`.
    long_runs: BTreeMap<i64, (u32, u32)>,
    /// Rows of a long-run key filed so far, for the split by count.
    filed: BTreeMap<i64, u64>,
    /// The bucket for rows without the column.
    unkeyed: u32,
    /// Buckets in use, unkeyed included.
    buckets: u32,
}

impl ChildCutPlan {
    fn plan(histogram: &BTreeMap<i64, u64>, unkeyed_rows: u64, rows_per_cut: u64) -> Self {
        let mut bounds = Vec::new();
        let mut long_runs = BTreeMap::new();
        let mut current = 0u64;
        let mut open = false;
        for (&key, &count) in histogram {
            if count > rows_per_cut {
                if open {
                    bounds.push(key - 1);
                    open = false;
                    current = 0;
                }
                let cuts = count.div_ceil(rows_per_cut) as u32;
                long_runs.insert(key, (bounds.len() as u32, cuts));
                for _ in 0..cuts {
                    bounds.push(key);
                }
                continue;
            }
            if open && current + count > rows_per_cut {
                bounds.push(key - 1);
                current = 0;
            }
            open = true;
            current += count;
        }
        if open {
            bounds.push(i64::MAX);
        }
        let keyed = bounds.len() as u32;
        let has_unkeyed = unkeyed_rows > 0;
        Self {
            bounds,
            long_runs,
            filed: BTreeMap::new(),
            unkeyed: keyed,
            buckets: keyed + u32::from(has_unkeyed),
        }
    }

    fn bucket(&mut self, key: Option<i64>, rows_per_cut: u64) -> u32 {
        let Some(key) = key else {
            return self.unkeyed;
        };
        if let Some(&(first, cuts)) = self.long_runs.get(&key) {
            let filed = self.filed.entry(key).or_insert(0);
            let cut = (*filed / rows_per_cut) as u32;
            *filed += 1;
            return first + cut.min(cuts - 1);
        }
        self.bounds.partition_point(|bound| *bound < key) as u32
    }
}

/// Cut plans for every child from the (child, value) histograms.
fn plan_cuts(
    histograms: &[BTreeMap<i64, u64>],
    unkeyed: &[u64],
    rows_per_cut: u64,
) -> Vec<ChildCutPlan> {
    histograms
        .iter()
        .zip(unkeyed)
        .map(|(histogram, &unkeyed)| ChildCutPlan::plan(histogram, unkeyed, rows_per_cut))
        .collect()
}

/// The partition key of one logged document: its value in `column`,
/// from the integer list or the timestamp list (both file into the same
/// column at ingest), or `None` when the document does not carry it.
fn partition_key_of(document: &AddDocumentsRequest, column: &str) -> Result<Option<i64>, String> {
    if let Some(value) = document.integers.iter().find(|value| value.field == column) {
        return Ok(Some(value.value));
    }
    if let Some(value) = document
        .timestamps
        .iter()
        .find(|value| value.field == column)
    {
        let Some(instant) = value.value.as_ref() else {
            return Ok(None);
        };
        return crate::node::timestamp_to_epoch_micros(column, instant)
            .map(Some)
            .map_err(|status| status.message().to_string());
    }
    Ok(None)
}

/// One output of the cut: the live source ids it holds, in slot order
/// (key ascending, then source id; unkeyed rows in id order).
struct Partition {
    ids: Vec<u64>,
}

/// Cut keyed rows (sorted by key, then id) into partitions of at most
/// `bound` rows. A cut prefers a key boundary: a run of equal keys moves
/// to the next partition as a unit when it would overflow the current
/// one, so partitions cover disjoint key ranges wherever the data allows;
/// only a run longer than the bound is split, and then the two
/// partitions share that one key. Unkeyed rows follow in id order, cut
/// at the same bound.
fn cut_partitions(keyed: &[(i64, u64)], unkeyed: &[u64], bound: usize) -> Vec<Partition> {
    let bound = bound.max(1);
    let mut partitions = Vec::new();
    let mut current: Vec<u64> = Vec::new();
    let mut i = 0;
    while i < keyed.len() {
        let key = keyed[i].0;
        let mut j = i;
        while j < keyed.len() && keyed[j].0 == key {
            j += 1;
        }
        let run = &keyed[i..j];
        if !current.is_empty() && current.len() + run.len() > bound {
            partitions.push(Partition {
                ids: std::mem::take(&mut current),
            });
        }
        for piece in run.chunks(bound) {
            if !current.is_empty() && current.len() + piece.len() > bound {
                partitions.push(Partition {
                    ids: std::mem::take(&mut current),
                });
            }
            current.extend(piece.iter().map(|(_, id)| *id));
        }
        i = j;
    }
    if !current.is_empty() {
        partitions.push(Partition { ids: current });
    }
    for piece in unkeyed.chunks(bound) {
        partitions.push(Partition {
            ids: piece.to_vec(),
        });
    }
    partitions
}

/// [`compact_log`] for the partitioned layout: the dense all-live rows
/// of ONE generation through `cutoff_clock`, ordered by `spec.column`
/// and cut into segments of at most `spec.bound` rows
/// (docs/immutable-segments.md "Partitioned layout"). Three passes, none
/// holding more than one WAL bucket or one output partition in memory:
///
/// 1. each bucket replays in turn and yields `(key, id)` per live row,
///    which the cut turns into partitions;
/// 2. each bucket replays again and its live rows are appended to one
///    spill log per partition, a single-bucket WAL generation under
///    `out_dir`;
/// 3. each spill log replays into its image, rows in key order, and
///    `sink` receives the rows in that slot order.
///
/// Rows whose document lacks the column, and vector-only rows, form the
/// unkeyed partitions after the keyed ones. A column no live row carries
/// is an error, as is a value that is not an integer. `columns` gives
/// every output the shard's column tables, so a partition that holds no
/// record of a column (the unkeyed one has no partition column at all)
/// still declares it.
#[allow(clippy::too_many_arguments)]
pub fn compact_log_partitioned(
    gen: &Path,
    cutoff_clock: u64,
    out_dir: &Path,
    spec: PartitionSpec<'_>,
    bm25_fields: Option<&[String]>,
    pinned_fingerprints: Option<&[u64]>,
    columns: Option<&ColumnTables>,
    analyze: &mut Analyzer,
    sink: &mut RowSink,
) -> Result<CompactionBuild, String> {
    let manifest = read_gen_manifest(gen)?;
    require_backend_config(&manifest, gen)?;
    require_complete_history(&manifest, gen)?;
    let binding = read_gens_binding(std::slice::from_ref(&gen.to_path_buf()))?;
    std::fs::create_dir_all(out_dir)
        .map_err(|error| format!("mkdir {}: {error}", out_dir.display()))?;
    let bucket_count = manifest.bucket_count as usize;
    let dim = manifest.dim as usize;
    let generation = manifest
        .generation
        .checked_add(1)
        .ok_or_else(|| "compaction generation overflow".to_string())?;
    let column = spec.column;

    // Pass 1: keys.
    let mut keyed: Vec<(i64, u64)> = Vec::new();
    let mut unkeyed: Vec<u64> = Vec::new();
    let mut rows_before = 0u64;
    let mut tombstones = 0u64;
    for bucket in 0..manifest.bucket_count {
        let mut replay = Replay::default();
        replay_buckets_through(
            gen,
            bucket..bucket + 1,
            bucket_count,
            dim,
            false,
            cutoff_clock,
            &mut replay,
        )?;
        let (present, tombstoned) = replay_counts(&replay);
        rows_before += present;
        tombstones += tombstoned;
        replay.compact();
        for id in child_order(&replay) {
            match replay.documents.get(&id) {
                Some(document) => match partition_key_of(document, column)? {
                    Some(key) => keyed.push((key, id)),
                    None => unkeyed.push(id),
                },
                None => unkeyed.push(id),
            }
        }
    }
    if keyed.is_empty() {
        return Err(format!(
            "partition column {column:?}: no live document of this shard carries it"
        ));
    }
    keyed.sort_unstable();
    unkeyed.sort_unstable();
    let partitions = cut_partitions(&keyed, &unkeyed, spec.bound);
    let mut partition_of: Vec<(u64, u32)> = partitions
        .iter()
        .enumerate()
        .flat_map(|(index, partition)| partition.ids.iter().map(move |id| (*id, index as u32)))
        .collect();
    partition_of.sort_unstable();
    drop(keyed);
    drop(unkeyed);

    // Pass 2: spill each live row into its partition's log.
    let spill_root = out_dir.join("spill");
    let mut spill_manifest = manifest.clone();
    spill_manifest.bucket_bits = 0;
    spill_manifest.bucket_count = 1;
    let mut spills = Vec::with_capacity(partitions.len());
    for index in 0..partitions.len() {
        let dir = spill_root.join(format!("p{index:05}"));
        let writer = wal::WalWriter::create(&dir, spill_manifest.clone())
            .map_err(|error| format!("create spill log {}: {error}", dir.display()))?;
        spills.push(writer);
    }
    for bucket in 0..manifest.bucket_count {
        let mut replay = Replay::default();
        replay_buckets_through(
            gen,
            bucket..bucket + 1,
            bucket_count,
            dim,
            false,
            cutoff_clock,
            &mut replay,
        )?;
        replay.compact();
        for id in child_order(&replay) {
            let index = partition_of
                .binary_search_by_key(&id, |(id, _)| *id)
                .map(|at| partition_of[at].1 as usize)
                .map_err(|_| format!("compaction lost track of source id {id} between passes"))?;
            let keys: Vec<Vec<u8>> = replay.stable_keys.get(&id).cloned().into_iter().collect();
            if let Some(document) = replay.documents.get(&id) {
                spills[index]
                    .append(wal_record::Op::AddDocuments(
                        crate::pb::wal::LoggedAddDocuments {
                            source_references: Vec::new(),
                            first_id: id,
                            documents: vec![document.clone()],
                            stable_routing_keys: keys.clone(),
                        },
                    ))
                    .map_err(|error| format!("spill document {id}: {error}"))?;
            }
            if let Some(vector) = replay.vectors.get(&id) {
                spills[index]
                    .append(wal_record::Op::AddVectors(
                        crate::pb::wal::LoggedAddVectors {
                            first_id: id,
                            batch: Some(crate::pb::AddVectorsRequest {
                                vectors: vector.clone(),
                                dim: manifest.dim,
                            }),
                            stable_routing_keys: keys,
                        },
                    ))
                    .map_err(|error| format!("spill vector {id}: {error}"))?;
            }
        }
    }
    let spill_dirs: Vec<PathBuf> = spills
        .iter_mut()
        .map(|writer| {
            writer
                .flush()
                .map(|()| writer.dir().to_path_buf())
                .map_err(|error| format!("flush spill log {}: {error}", writer.dir().display()))
        })
        .collect::<Result<_, _>>()?;
    drop(spills);

    // Pass 3: one image per partition, rows in key order.
    let mut id_map = BTreeMap::new();
    let mut images = Vec::with_capacity(partitions.len());
    let mut next_slot = 0u64;
    for (index, (partition, spill_dir)) in partitions.iter().zip(&spill_dirs).enumerate() {
        let mut replay = Replay::default();
        replay_buckets_through(spill_dir, 0..1, 1, dim, false, u64::MAX, &mut replay)?;
        replay.compact();
        let held = replay.vectors.len().max(replay.documents.len());
        if held != partition.ids.len() {
            return Err(format!(
                "partition {index}: the spill log holds {held} rows, the cut assigned {}",
                partition.ids.len()
            ));
        }
        // Vector-bearing rows first, document-only rows after them, each
        // group in the cut's key order: the slot shape the image builder
        // assigns.
        let mut order: Vec<u64> = partition
            .ids
            .iter()
            .copied()
            .filter(|id| replay.vectors.contains_key(id))
            .collect();
        order.extend(
            partition
                .ids
                .iter()
                .copied()
                .filter(|id| !replay.vectors.contains_key(id)),
        );
        let rows = emit_rows_in(&replay, &order, next_slot, &mut id_map, sink)?;
        images.push(finish_child_ordered(
            &manifest,
            replay,
            &order,
            index,
            out_dir,
            next_slot,
            0,
            u64::MAX,
            bm25_fields,
            pinned_fingerprints,
            binding.as_ref(),
            columns,
            analyze,
        )?);
        next_slot = next_slot
            .checked_add(rows)
            .ok_or_else(|| "compaction slot overflow".to_string())?;
    }
    let _ = std::fs::remove_dir_all(&spill_root);
    Ok(CompactionBuild {
        generation,
        rows_before,
        tombstones,
        id_map,
        images,
        binding,
    })
}

/// Merge several shards' WAL generations into ONE image. All inputs must
/// carry byte-identical provider configurations AND identical bucket counts
/// (split/merge only within one scoring configuration and bucket geometry);
/// records replay bucket-by-bucket in global id order across all inputs.
/// The child's slot base defaults to the lowest input slot offset. Its
/// hash range is the full range: the merge inputs' ranges are not
/// recorded in the WAL manifest, and full-range is the safe default for
/// the shard map.
pub fn merge(
    gens: &[PathBuf],
    out_dir: &Path,
    slot_base: Option<u64>,
    vectors_only: bool,
    bm25_fields: Option<&[String]>,
    analyze: &mut Analyzer,
) -> Result<ReshardOutput, String> {
    if gens.is_empty() {
        return Err("merge requires at least one --logs generation".to_string());
    }
    let mut manifests = Vec::with_capacity(gens.len());
    for gen in gens {
        let manifest = read_gen_manifest(gen)?;
        require_backend_config(&manifest, gen)?;
        require_complete_history(&manifest, gen)?;
        if let Some(first) = manifests.first() {
            if !same_backend_config(first, &manifest) {
                return Err(format!(
                    "{}: vector backend configuration differs from the first input; \
                     split/merge requires one scoring configuration",
                    gen.display()
                ));
            }
            if first.bucket_count != manifest.bucket_count {
                return Err(format!(
                    "{}: bucket count {} differs from the first input's {}; merge inputs \
                     must share one bucket geometry",
                    gen.display(),
                    manifest.bucket_count,
                    first.bucket_count
                ));
            }
        }
        manifests.push(manifest);
    }
    let bucket_count = manifests[0].bucket_count;
    let dim = manifests[0].dim as usize;
    let binding = read_gens_binding(gens)?;
    let max_generation = manifests.iter().map(|m| m.generation).max().unwrap_or(0);
    let min_slot_offset = manifests.iter().map(|m| m.slot_offset).min().unwrap_or(0);

    // Bucket-major replay: one bucket index at a time, across all inputs.
    let mut merged = Replay::default();
    for bucket in 0..bucket_count {
        for gen in gens {
            replay_buckets(
                gen,
                bucket..bucket + 1,
                bucket_count as usize,
                dim,
                vectors_only,
                &mut merged,
            )?;
        }
    }
    std::fs::create_dir_all(out_dir).map_err(|e| format!("mkdir {}: {e}", out_dir.display()))?;
    eprintln!(
        "reshard: merge of {} generations: {} vectors, {} documents",
        gens.len(),
        merged.vectors.len(),
        merged.documents.len()
    );
    let child = finish_child(
        &manifests[0],
        merged,
        0,
        out_dir,
        slot_base.unwrap_or(min_slot_offset),
        0,
        u64::MAX,
        bm25_fields,
        binding.as_ref(),
        analyze,
    )?;
    Ok(ReshardOutput {
        generation: max_generation + 1,
        children: vec![child],
    })
}

/// Render the coordinator shard map for a reshard result (see
/// `config::ShardMap`). `addr` is a TODO placeholder — the operator fills
/// in where each child will listen.
pub fn shard_map_toml(out: &ReshardOutput) -> String {
    let mut s = format!("generation = {}\n", out.generation);
    for child in &out.children {
        s.push_str(&format!(
            "\n[[shards]]\naddr = \"TODO\"\nslot_offset = {}\nhash_lo = {}\nhash_hi = {}\n",
            child.slot_offset, child.hash_lo, child.hash_hi
        ));
    }
    s
}

/// Render the node-side `[[shards]]` config blocks for a reshard result
/// (same print idiom as `harness::write_shards`).
pub fn shards_toml(out: &ReshardOutput) -> String {
    let mut s = String::new();
    for child in &out.children {
        s.push_str(&format!(
            "[[shards]]\nlisten = \"0.0.0.0:50051\"  # TODO: one listener per child\nindex = \
             \"{}\"\nslot_offset = {}\n",
            child.vector_path.display(),
            child.slot_offset
        ));
    }
    s
}

/// The process's soft open-files limit, from `/proc/self/limits` on
/// Linux; `None` where that file is absent or unreadable.
pub fn open_files_soft_limit() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/self/limits").ok()?;
    let line = text
        .lines()
        .find(|line| line.starts_with("Max open files"))?;
    let mut fields = line["Max open files".len()..].split_whitespace();
    let soft = fields.next()?;
    if soft == "unlimited" {
        return Some(u64::MAX);
    }
    soft.parse().ok()
}

/// Raise the soft open-files limit to the hard limit, so a spill plan
/// sized for the machine is not stopped by the shell's default of
/// 1024. Returns the limit in force afterwards; `None` when the build
/// cannot set limits (no `net` feature) or the call failed.
#[cfg(feature = "net")]
pub fn raise_open_files_limit() -> Option<u64> {
    // SAFETY: getrlimit/setrlimit read and write a plain struct the
    // caller owns; no memory is shared past the calls.
    unsafe {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) != 0 {
            return None;
        }
        if limit.rlim_cur < limit.rlim_max {
            limit.rlim_cur = limit.rlim_max;
            if libc::setrlimit(libc::RLIMIT_NOFILE, &limit) != 0 {
                return None;
            }
        }
        // rlim_t is u64 on the fleet's targets and wider elsewhere.
        #[allow(clippy::unnecessary_cast)]
        Some(limit.rlim_cur as u64)
    }
}

#[cfg(not(feature = "net"))]
pub fn raise_open_files_limit() -> Option<u64> {
    None
}

#[cfg(test)]
mod partition_tests {
    use super::cut_partitions;

    fn ids(keys: &[i64]) -> Vec<(i64, u64)> {
        keys.iter()
            .enumerate()
            .map(|(i, key)| (*key, i as u64))
            .collect()
    }

    fn shape(keyed: &[(i64, u64)], unkeyed: &[u64], bound: usize) -> Vec<Vec<u64>> {
        cut_partitions(keyed, unkeyed, bound)
            .into_iter()
            .map(|p| p.ids)
            .collect()
    }

    #[test]
    fn a_cut_prefers_key_boundaries() {
        // Three rows of key 1, three of key 2, two of key 3, bound 5: the
        // run of key 2 would overflow the first partition (six rows), so
        // it moves as a unit; keys 2 and 3 then fit together in five.
        let keyed = ids(&[1, 1, 1, 2, 2, 2, 3, 3]);
        assert_eq!(
            shape(&keyed, &[], 5),
            vec![vec![0, 1, 2], vec![3, 4, 5, 6, 7]]
        );
        // At bound 4 the pair of key-3 rows does not fit either.
        assert_eq!(
            shape(&keyed, &[], 4),
            vec![vec![0, 1, 2], vec![3, 4, 5], vec![6, 7]]
        );
    }

    #[test]
    fn a_run_longer_than_the_bound_is_split() {
        let keyed = ids(&[5, 5, 5, 5, 5, 6]);
        assert_eq!(
            shape(&keyed, &[], 2),
            vec![vec![0, 1], vec![2, 3], vec![4, 5]]
        );
    }

    #[test]
    fn unkeyed_rows_follow_in_bounded_pieces() {
        let keyed = ids(&[1, 2]);
        assert_eq!(
            shape(&keyed, &[10, 11, 12], 2),
            vec![vec![0, 1], vec![10, 11], vec![12]]
        );
    }

    #[test]
    fn a_zero_bound_means_one_row_per_partition() {
        let keyed = ids(&[1, 1]);
        assert_eq!(shape(&keyed, &[], 0), vec![vec![0], vec![1]]);
    }
}
