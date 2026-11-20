//! Sealed-segment sources: reading rows back out of a segmented shard.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::pb::wal::wal_record;
use crate::wal::{self, RecordReader, WalManifest};

use super::replay::read_gen_manifest;
use super::source_agreement::PinnedSources;
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

/// Open the catalogs beside the logs and check what a child pins.
pub(crate) fn open_segment_sources(
    gens: &[PathBuf],
) -> Result<(Vec<SegmentSourceShard>, SourceTables), String> {
    let mut shards = Vec::with_capacity(gens.len());
    let mut pinned = PinnedSources::default();
    for gen in gens {
        shards.push(open_source_shard(gen, &mut pinned)?);
    }
    let tables = pinned.tables.ok_or_else(|| {
        "the sources have no sealed segment; there is nothing to transplant".to_string()
    })?;
    Ok((shards, tables))
}

/// One shard's catalog beside its log, checked against what the
/// sources before it pinned.
fn open_source_shard(gen: &Path, pinned: &mut PinnedSources) -> Result<SegmentSourceShard, String> {
    let root = segments_root_of_gen(gen)?;
    if !root.exists() {
        return Err(format!(
            "{}: no segment catalog at {}; the source was never flushed, split it from \
             the logs",
            gen.display(),
            root.display()
        ));
    }
    let set = std::sync::Arc::new(crate::segments::OpenedSegmentSet::open(&root)?);
    let manifest = read_gen_manifest(gen)?;
    let rows = contiguous_rows(&set, &root)?;
    let deleted = sealed_tail_deletes(gen, &manifest, rows, &root)?;
    pinned.admit(&set, &root, &manifest)?;
    let overlay_path = crate::node::live_docs_sidecar_path(&index_path_of_root(&root));
    let overlay = if overlay_path.exists() {
        Some(
            crate::live_docs::LiveDocs::open(&overlay_path)
                .map_err(|e| format!("open {}: {e}", overlay_path.display()))?,
        )
    } else {
        None
    };
    Ok(SegmentSourceShard {
        gen: gen.to_path_buf(),
        root,
        set,
        overlay,
        deleted: local_labels(deleted, &manifest, gen)?,
        slot_offset: manifest.slot_offset,
    })
}

/// The catalog's row count, with every segment starting where the one
/// before it ends.
fn contiguous_rows(set: &crate::segments::OpenedSegmentSet, root: &Path) -> Result<u64, String> {
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
    Ok(rows)
}

/// The rows the log deletes or replaces, by global id, once the log is
/// shown to hold no row past the sealed catalog.
fn sealed_tail_deletes(
    gen: &Path,
    manifest: &WalManifest,
    rows: u64,
    root: &Path,
) -> Result<BTreeSet<u64>, String> {
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
    Ok(deleted)
}

/// The log's deleted rows as catalog-local labels: the log names rows by
/// global id, the catalog by local label.
fn local_labels(
    deleted: BTreeSet<u64>,
    manifest: &WalManifest,
    gen: &Path,
) -> Result<BTreeSet<u64>, String> {
    deleted
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
        .collect()
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
