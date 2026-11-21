//! Building one child image from replayed records, and the steps that seal it.

mod bm25;
pub(super) use bm25::write_store_range;
mod columns;
mod document;
mod tables;
mod vectors;

use std::collections::BTreeMap;
use std::path::Path;

use crate::pb::AddDocumentsRequest;
use crate::postings::{AnalyzedField, StoredBinding, StoredDerived};
use crate::vector::VectorIndex;
use crate::wal::WalManifest;

#[cfg(doc)]
use super::compact_log::emit_rows_in;
use super::hnsw_sources::HnswCompactionSources;
use super::replay::Replay;
use super::{Analyzer, ChildImage, ColumnTables};

/// What every image of one split, merge, or compaction is written under:
/// the BM25 field table, the analyzer fingerprints a compaction pins, the
/// mapped-plan binding and derived-column declaration the children keep,
/// the column tables they declare, and the HNSW graphs a compaction may
/// reuse. A field left `None` is derived from the records.
#[derive(Clone, Copy, Default)]
pub(super) struct ChildSchema<'a> {
    pub(super) bm25_fields: Option<&'a [String]>,
    pub(super) pinned_fingerprints: Option<&'a [u64]>,
    pub(super) binding: Option<&'a StoredBinding>,
    pub(super) derived: Option<&'a StoredDerived>,
    pub(super) columns: Option<&'a ColumnTables>,
    pub(super) hnsw_sources: Option<&'a HnswCompactionSources>,
}

/// Where one child image lands: its file ordinal under `out_dir`, the
/// global slot base it takes, and the inclusive hash range it covers.
#[derive(Clone, Copy)]
pub(super) struct ChildSlot<'a> {
    pub(super) ordinal: usize,
    pub(super) out_dir: &'a Path,
    pub(super) slot_offset: u64,
    pub(super) hash_lo: u64,
    pub(super) hash_hi: u64,
}

impl<'a> ChildSlot<'a> {
    /// A child covering the whole hash space.
    pub(super) fn full_range(ordinal: usize, out_dir: &'a Path, slot_offset: u64) -> Self {
        Self {
            ordinal,
            out_dir,
            slot_offset,
            hash_lo: 0,
            hash_hi: u64::MAX,
        }
    }
}

/// One child's rows in slot order, as [`build_child`] consumes them.
struct ChildRows {
    vectors: Vec<(u64, Vec<f32>)>,
    retained: BTreeMap<u64, Vec<u8>>,
    documents: Vec<(u64, AddDocumentsRequest)>,
    /// Fields carried over from sealed segments, keyed by source id; the
    /// analyzer is not called when present.
    transplanted: Option<BTreeMap<u64, Vec<AnalyzedField>>>,
}

/// Build one child image from its share of the replay: the vector index
/// (constructed from manifest provider state, vectors in slot order), the
/// BM25 sidecar from the documents, and the retained-row payload.
///
/// Document local ids mirror the vector side's dense remap: a document
/// whose id also names a vector gets that vector's child slot (the
/// aligned-ingest case stays aligned); documents with no vector (the doc
/// side ran ahead) are appended above the vector space in id order.
fn build_child(
    manifest: &WalManifest,
    rows: ChildRows,
    vector_path: &Path,
    schema: &ChildSchema<'_>,
    prebuilt_index: Option<VectorIndex>,
    analyze: &mut Analyzer,
) -> Result<ChildImage, String> {
    let vector_side = vectors::write_vector_side(
        manifest,
        &rows.vectors,
        rows.retained,
        vector_path,
        schema.hnsw_sources,
        prebuilt_index,
    )?;
    let mapped = document::MappedDocuments::assign(&vector_side.parent_ids, rows.documents)?;
    let num_documents = mapped.rows.len() as u64;
    let row_parent_ids = mapped.row_parent_ids.clone();
    let bm25_path = if mapped.rows.is_empty() {
        None
    } else {
        let build = bm25::Bm25Build {
            vector_path,
            schema,
            transplanted: rows.transplanted,
        };
        Some(build.write(mapped, analyze)?)
    };
    let multi_res_path =
        vectors::publish_retained(&vector_side.remapped, vector_path, row_parent_ids.len())?;
    Ok(ChildImage {
        multi_res_path,
        vector_path: vector_path.to_path_buf(),
        exact_vector_path: vector_side.exact_vector_path,
        bm25_path,
        slot_offset: 0,
        hash_lo: 0,
        hash_hi: 0,
        num_vectors: rows.vectors.len() as u64,
        num_documents,
        parent_ids: vector_side.parent_ids,
        row_parent_ids,
        parents_colocated: false,
    })
}

/// Assemble one child: replay its vectors and documents, in the slot
/// order [`child_order`] gives, into an image under `slot.out_dir` named
/// `shard-<ordinal>.vector`.
pub(super) fn finish_child(
    manifest: &WalManifest,
    mut replay: Replay,
    slot: ChildSlot<'_>,
    schema: &ChildSchema<'_>,
    analyze: &mut Analyzer,
) -> Result<ChildImage, String> {
    replay.compact();
    let order = child_order(&replay);
    finish_child_ordered(manifest, replay, &order, slot, schema, None, analyze)
}

/// [`finish_child`] with the child's slot order given: `order` lists the
/// live source ids, vector-bearing rows first and document-only rows after
/// them (the shape [`build_child`] assigns slots in), and the image takes
/// its slots in that sequence. [`emit_rows_in`] with the same order writes
/// the matching log. `prebuilt_index` is a vector index already built in
/// that order.
pub(super) fn finish_child_ordered(
    manifest: &WalManifest,
    mut replay: Replay,
    order: &[u64],
    slot: ChildSlot<'_>,
    schema: &ChildSchema<'_>,
    prebuilt_index: Option<VectorIndex>,
    analyze: &mut Analyzer,
) -> Result<ChildImage, String> {
    replay.compact();
    let ordinal = slot.ordinal;
    let vector_path = slot.out_dir.join(format!("shard-{ordinal}.vector"));
    eprintln!(
        "reshard: child {ordinal}: {} vectors, {} documents -> {}",
        replay.vectors.len(),
        replay.documents.len(),
        vector_path.display()
    );
    let parents_colocated = rows_prove_parents_colocated(&replay);
    let rows = take_in_order(&mut replay, order, ordinal)?;
    let mut child = build_child(
        manifest,
        rows,
        &vector_path,
        schema,
        prebuilt_index,
        analyze,
    )?;
    child.slot_offset = slot.slot_offset;
    child.hash_lo = slot.hash_lo;
    child.hash_hi = slot.hash_hi;
    child.parents_colocated = parents_colocated;
    Ok(child)
}

/// Whether every document of `replay` proves its lineage parent's rows
/// shared one source shard; a caller that routes parents apart clears it.
fn rows_prove_parents_colocated(replay: &Replay) -> bool {
    replay.documents.iter().all(|(id, doc)| {
        crate::colocation::row_proves_parent_colocated(
            doc,
            replay.stable_keys.get(id).is_some_and(|k| !k.is_empty()),
        )
    })
}

/// Move the rows `order` names out of `replay`, in that order; a row the
/// order leaves out is an error.
fn take_in_order(replay: &mut Replay, order: &[u64], ordinal: usize) -> Result<ChildRows, String> {
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
    Ok(ChildRows {
        vectors,
        retained: std::mem::take(&mut replay.retained),
        documents,
        transplanted,
    })
}

/// The derived-column declaration a log's manifest records, in the
/// store's form; `None` for a log without one. A manifest whose
/// fingerprint does not cover its own `[[derived]]` table was edited by
/// hand and is refused.
pub fn manifest_derived(manifest: &WalManifest) -> Result<Option<StoredDerived>, String> {
    if manifest.derived.is_empty() {
        if !manifest.derived_fingerprint.is_empty() {
            return Err(format!(
                "the manifest records derived fingerprint {:?} and no [[derived]] table",
                manifest.derived_fingerprint
            ));
        }
        return Ok(None);
    }
    let spec = crate::derived::spec_from_config(&manifest.derived)?;
    let declaration = crate::derived::Declaration::compile(&spec)?;
    if declaration.fingerprint() != manifest.derived_fingerprint {
        return Err(format!(
            "the manifest's [[derived]] table hashes to {:?}, its recorded fingerprint is {:?}",
            declaration.fingerprint(),
            manifest.derived_fingerprint
        ));
    }
    Ok(Some(StoredDerived::of(&declaration)))
}

/// The slot order [`build_child`] assigns a replay by default: vector
/// rows in source-id order, then the document-only rows in source-id
/// order.
pub(super) fn child_order(replay: &Replay) -> Vec<u64> {
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
