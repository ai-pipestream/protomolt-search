//! Sealed-segment sources: reading rows back out of a segmented shard.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::pb::wal::wal_record;
use crate::pb::AddDocumentsRequest;
use crate::wal::{self, RecordReader};

use super::replay::read_gen_manifest;
use super::ColumnTables;

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

/// The field and column tables a sealed store declares, in index order.
pub(crate) fn segment_tables(bm25: &crate::postings::Bm25Reader) -> SourceTables {
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
    SourceTables {
        fields,
        fingerprints,
        position_fields,
        sentence_fields,
        columns,
    }
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
pub(crate) fn open_segment_sources(
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
            let this = segment_tables(bm25);
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
pub(crate) fn reconstruct_document(
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
    // The store's declaration entry says what the row's derived values
    // mean (docs/derived-columns.md); a logged form of the row carries it.
    doc.derived_fingerprint = bm25
        .derived()
        .map(|derived| derived.fingerprint.clone())
        .unwrap_or_default();
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
