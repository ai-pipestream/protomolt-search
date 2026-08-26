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
//! - ONE CALIBRATION per split/merge: the WAL manifest must carry a
//!   locked calibration (a seeded shard), and merge requires
//!   byte-identical calibrations AND identical bucket counts across all
//!   inputs. Unseeded shards fit calibration from their own first batch,
//!   so their scores are not comparable across buckets and
//!   repartitioning them is meaningless — a hard error, like mixed
//!   calibrations on `InstallSnapshot`.
//! - Ids are GENERATION-SCOPED: records carry the global ids the server
//!   assigned; children re-assign dense local slots in original id order
//!   and get their slot base from the new shard map, so parent ids never
//!   leak into a child by accident. [`ChildImage::parent_ids`] is the
//!   explicit remap (child local slot -> parent global id).
//! - The split partition is hash-based and stable: id `v` goes to bucket
//!   `fnv1a64(v) >> (64 - log2(N))`, so every id lands in exactly one
//!   child and a re-split of a child bisects its range again.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use turbovec::TurboQuantIndex;

use crate::pb::wal::wal_record;
use crate::pb::{AddDocumentsRequest, AnalysisSpec};
use crate::postings::{AnalyzedDoc, SpillBuilder};
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
pub type Analyzer<'a> =
    dyn FnMut(&[(&str, Option<&AnalysisSpec>)]) -> Result<Vec<AnalyzedDoc>, String> + 'a;

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

/// One built shard image: the `.tv` index, its optional `.bm25` sidecar,
/// and the shard-map metadata the coordinator needs to route to it.
#[derive(Debug)]
pub struct ChildImage {
    /// The written index file.
    pub tv_path: PathBuf,
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
}

/// The result of one split or merge: the images plus the NEW generation
/// number (one past the highest input generation) for the shard map.
#[derive(Debug)]
pub struct ReshardOutput {
    pub generation: u64,
    pub children: Vec<ChildImage>,
}

/// Vectors and documents replayed out of bucket files, keyed by their
/// server-assigned global ids (BTreeMap iteration IS id order).
#[derive(Default)]
struct Replay {
    /// global id -> one vector (`dim` floats).
    vectors: BTreeMap<u64, Vec<f32>>,
    /// global id -> the document as ingested.
    documents: BTreeMap<u64, AddDocumentsRequest>,
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
            match record.op {
                Some(wal_record::Op::AddVectors(a)) => {
                    if bucket_of(a.first_id, bucket_count) as u32 != bucket {
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
                        if out
                            .vectors
                            .insert(a.first_id + i as u64, vector.to_vec())
                            .is_some()
                        {
                            return Err(format!(
                                "replay {}: duplicate vector id {}",
                                path.display(),
                                a.first_id + i as u64
                            ));
                        }
                    }
                }
                Some(wal_record::Op::AddDocuments(a)) => {
                    if bucket_of(a.first_id, bucket_count) as u32 != bucket {
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
                    for (i, doc) in a.documents.into_iter().enumerate() {
                        if out.documents.insert(a.first_id + i as u64, doc).is_some() {
                            return Err(format!(
                                "replay {}: duplicate document id {}",
                                path.display(),
                                a.first_id + i as u64
                            ));
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
                Some(wal_record::Op::Flush(_)) | Some(wal_record::Op::Snapshot(_)) | None => {}
            }
        }
    }
    Ok(())
}

/// Require a usable, locked calibration in the manifest (see the module
/// docs for why unseeded logs cannot be resharded).
fn require_calibration(manifest: &WalManifest, what: &Path) -> Result<(), String> {
    let dim = manifest.dim as usize;
    if dim == 0
        || manifest.calibration_shift.len() != dim
        || manifest.calibration_scale.len() != dim
    {
        return Err(format!(
            "{}: WAL manifest carries no locked calibration (dim {}); resharding requires a \
             seeded shard — an unseeded shard's calibration is fitted from its own first \
             batch and its scores are not comparable across buckets",
            what.display(),
            manifest.dim
        ));
    }
    Ok(())
}

/// Require full history: a generation whose manifest records preexisting
/// state (an installed image, or logging enabled on an already-populated
/// shard) does not contain everything the shard holds, and a log-only
/// replay would silently drop that state. Hard error, like a missing
/// calibration.
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
fn read_gens_binding(
    gens: &[PathBuf],
) -> Result<Option<crate::postings::StoredBinding>, String> {
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
            };
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

/// Calibration identity for the merge check (slot offset and generation
/// legitimately differ between inputs; the shape and calibration must not).
fn same_calibration(a: &WalManifest, b: &WalManifest) -> bool {
    a.dim == b.dim
        && a.bit_width == b.bit_width
        && a.calibration_shift == b.calibration_shift
        && a.calibration_scale == b.calibration_scale
}

/// Build one child image from its share of the replay: the vector index
/// (seeded with the manifest calibration, vectors in id order), the BM25
/// sidecar from the documents, and the two output files.
///
/// Document local ids mirror the vector side's dense remap: a document
/// whose id also names a vector gets that vector's child slot (the
/// aligned-ingest case stays aligned); documents with no vector (the doc
/// side ran ahead) are appended above the vector space in id order.
fn build_child(
    manifest: &WalManifest,
    vectors: Vec<(u64, Vec<f32>)>,
    documents: Vec<(u64, AddDocumentsRequest)>,
    tv_path: &Path,
    bm25_fields: Option<&[String]>,
    binding: Option<&crate::postings::StoredBinding>,
    analyze: &mut Analyzer,
) -> Result<ChildImage, String> {
    let dim = manifest.dim as usize;
    // An empty manifest pair means the parent was uncalibrated; the
    // child is then uncalibrated too, which from_parts expresses as
    // empty TQ+ arrays.
    let mut index = TurboQuantIndex::from_parts(
        Some(dim),
        manifest.bit_width as usize,
        0,
        Vec::new(),
        Vec::new(),
        manifest.calibration_shift.clone(),
        manifest.calibration_scale.clone(),
    )
    .map_err(|e| format!("construct child index: {e}"))?;
    let mut parent_ids = Vec::with_capacity(vectors.len());
    let mut flat = Vec::with_capacity(vectors.len() * dim);
    for (id, vector) in &vectors {
        parent_ids.push(*id);
        flat.extend_from_slice(vector);
    }
    index.add(&flat);
    index.prepare();
    index
        .write(tv_path)
        .map_err(|e| format!("write {}: {e}", tv_path.display()))?;

    // parent id -> child local slot, for the document remap.
    let slot_of: BTreeMap<u64, u32> = parent_ids
        .iter()
        .enumerate()
        .map(|(slot, &id)| (id, slot as u32))
        .collect();
    let mut mapped: Vec<(u32, AddDocumentsRequest)> = Vec::with_capacity(documents.len());
    let mut spare = vectors.len() as u64;
    for (id, doc) in documents {
        let local = match slot_of.get(&id) {
            Some(&slot) => slot,
            None => {
                spare += 1;
                u32::try_from(spare - 1).map_err(|_| "document id space exceeds u32".to_string())?
            }
        };
        mapped.push((local, doc));
    }
    mapped.sort_by_key(|(local, _)| *local);
    let num_documents = mapped.len() as u64;

    let mut bm25_path = None;
    if !mapped.is_empty() {
        // The child field table (docs/multi-field.md): the caller's
        // fleet table when given, else "body" plus every extra field
        // name in this child's records in first-sight order (the replay
        // is id-ordered, so the derivation is deterministic). Old logs
        // carry no extra fields and derive the single-field table.
        let table: Vec<String> = match bm25_fields {
            Some(t) => t.to_vec(),
            None => {
                let mut t = vec!["body".to_string()];
                for (_, doc) in &mapped {
                    for f in &doc.fields {
                        if !t.iter().any(|n| n == &f.field) {
                            t.push(f.field.clone());
                        }
                    }
                }
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
        let facet_table: Vec<String> = {
            let mut t: Vec<String> = Vec::new();
            for (_, doc) in &mapped {
                for fv in &doc.facets {
                    if !t.iter().any(|n| n == &fv.field) {
                        t.push(fv.field.clone());
                    }
                }
            }
            t
        };
        let numeric_table: Vec<String> = {
            let mut t: Vec<String> = Vec::new();
            for (_, doc) in &mapped {
                for nv in &doc.numerics {
                    if !t.iter().any(|n| n == &nv.field) {
                        t.push(nv.field.clone());
                    }
                }
            }
            t
        };
        let map_facet_table: Vec<String> = {
            let mut t: Vec<String> = Vec::new();
            for (_, doc) in &mapped {
                for e in &doc.map_facets {
                    if !t.iter().any(|n| n == &e.field) {
                        t.push(e.field.clone());
                    }
                }
            }
            t
        };
        let map_numeric_table: Vec<String> = {
            let mut t: Vec<String> = Vec::new();
            for (_, doc) in &mapped {
                for e in &doc.map_numerics {
                    if !t.iter().any(|n| n == &e.field) {
                        t.push(e.field.clone());
                    }
                }
            }
            t
        };
        // Integers and timestamps name the SAME i64 columns
        // (docs/range-facets.md), so both lists feed one table — a
        // child whose records only ever carried timestamps still gets
        // the column, or the re-apply below would have nowhere to put
        // them.
        let integer_table: Vec<String> = {
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
        };
        // Geo columns come from one list, so the derivation is the
        // plain first-seen union (docs/geo-columns.md).
        let geo_table: Vec<String> = {
            let mut t: Vec<String> = Vec::new();
            for (_, doc) in &mapped {
                for e in &doc.geo_points {
                    if !t.iter().any(|n| n == &e.field) {
                        t.push(e.field.clone());
                    }
                }
            }
            t
        };
        // Children rebuild through the disk spiller for the same reason
        // nodes do: a full-scale child's postings do not fit in heap.
        let path = crate::node::bm25_sidecar_path(tv_path);
        let mut spill_dir = path.as_os_str().to_owned();
        spill_dir.push(".build");
        let spill_dir = std::path::PathBuf::from(spill_dir);
        let names: Vec<&str> = table.iter().map(String::as_str).collect();
        let facet_names: Vec<&str> = facet_table.iter().map(String::as_str).collect();
        let numeric_names: Vec<&str> = numeric_table.iter().map(String::as_str).collect();
        let map_facet_names: Vec<&str> = map_facet_table.iter().map(String::as_str).collect();
        let map_numeric_names: Vec<&str> =
            map_numeric_table.iter().map(String::as_str).collect();
        let integer_names: Vec<&str> = integer_table.iter().map(String::as_str).collect();
        let geo_names: Vec<&str> = geo_table.iter().map(String::as_str).collect();
        let mut builder = SpillBuilder::create_with_fields(&spill_dir, &names)
            .map_err(|e| format!("spill dir {}: {e}", spill_dir.display()))?
            .with_facet_fields(&facet_names)
            .with_numeric_fields(&numeric_names)
            .with_map_facet_fields(&map_facet_names)
            .with_map_numeric_fields(&map_numeric_names)
            .with_integer_fields(&integer_names)
            .with_geo_fields(&geo_names);
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
            let analyzed = {
                let mut batch: Vec<(&str, Option<&AnalysisSpec>)> = Vec::with_capacity(entries);
                for (_, d) in &mapped[i..end] {
                    batch.push((d.text.as_str(), d.analysis.as_ref()));
                    for f in &d.fields {
                        batch.push((f.text.as_str(), f.analysis.as_ref()));
                    }
                }
                analyze(&batch)
                    .map_err(|e| format!("analyze batch at child slot {}: {e}", mapped[i].0))?
            };
            if analyzed.len() != entries {
                return Err(format!(
                    "analyzer returned {} results for {entries} field texts",
                    analyzed.len()
                ));
            }
            let mut results = analyzed.into_iter();
            for (local, doc) in &mut mapped[i..end] {
                let body = results.next().expect("counted above").into_body();
                let mut fields = vec![crate::postings::AnalyzedField::default(); table.len()];
                fields[0] = body;
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
                let text = std::mem::take(&mut doc.text);
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
                            fields,
                            quality: None,
                            geography: None,
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
                for e in &doc.integers {
                    let ii = integer_table
                        .iter()
                        .position(|n| n == &e.field)
                        .expect("integer table was derived from these records");
                    builder.set_integer(ii, *local, e.value);
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
        tv_path: tv_path.to_path_buf(),
        bm25_path,
        slot_offset: 0, // filled in by the caller
        hash_lo: 0,
        hash_hi: 0,
        num_vectors: vectors.len() as u64,
        num_documents,
        parent_ids,
    })
}

/// Assemble one child: replay its vectors/documents into an image under
/// `out_dir` named `shard-<ordinal>.tv`.
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
    let tv_path = out_dir.join(format!("shard-{ordinal}.tv"));
    eprintln!(
        "reshard: child {ordinal}: {} vectors, {} documents -> {}",
        replay.vectors.len(),
        replay.documents.len(),
        tv_path.display()
    );
    let mut child = build_child(
        manifest,
        replay.vectors.into_iter().collect(),
        replay.documents.into_iter().collect(),
        &tv_path,
        bm25_fields,
        binding,
        analyze,
    )?;
    child.slot_offset = slot_offset;
    child.hash_lo = hash_lo;
    child.hash_hi = hash_hi;
    Ok(child)
}

/// Split one shard's WAL generation into `n` child images (a power of
/// two).
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
/// All inputs must carry byte-identical calibrations and one bucket
/// geometry, and be full-history — the same preconditions as
/// [`merge`], enforced the same way.
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
    if !n.is_power_of_two() || n < 2 {
        return Err(format!("split factor must be a power of two >= 2, got {n}"));
    }
    let Some(first_gen) = gens.first() else {
        return Err("split requires at least one input generation".to_string());
    };
    let manifest = read_gen_manifest(first_gen)?;
    let mut top_generation = manifest.generation;
    for gen in gens {
        let m = read_gen_manifest(gen)?;
        require_calibration(&m, gen)?;
        require_complete_history(&m, gen)?;
        if !same_calibration(&manifest, &m) {
            return Err(format!(
                "{}: calibration differs from the first input; split/merge is only \
                 defined within ONE calibration",
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
        let shift = 64 - n.trailing_zeros();
        let mut buckets: Vec<(Vec<(u64, Vec<f32>)>, Vec<(u64, AddDocumentsRequest)>)> =
            (0..n).map(|_| (Vec::new(), Vec::new())).collect();
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

/// Merge several shards' WAL generations into ONE image. All inputs must
/// carry byte-identical calibrations AND identical bucket counts
/// (split/merge only within one calibration and one bucket geometry);
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
        require_calibration(&manifest, gen)?;
        require_complete_history(&manifest, gen)?;
        if let Some(first) = manifests.first() {
            if !same_calibration(first, &manifest) {
                return Err(format!(
                    "{}: calibration differs from the first input; split/merge is only \
                     defined within ONE calibration",
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
            child.tv_path.display(),
            child.slot_offset
        ));
    }
    s
}
