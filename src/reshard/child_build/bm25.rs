//! The BM25 half of a child image, rebuilt through the disk spiller.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::analyzer::SessionLayers;
use crate::pb::{AddDocumentsRequest, AnalysisSpec, MappedAnalysisContract};
use crate::postings::{AnalyzedDoc, AnalyzedField, SpillBuilder};

use super::document::{self, FieldSource, MappedDocuments};
use super::tables::ChildTables;
use super::{Analyzer, ChildSchema};
use crate::reshard::ANALYZE_BATCH;

/// One child's BM25 store build.
pub(super) struct Bm25Build<'a> {
    pub(super) vector_path: &'a Path,
    pub(super) schema: &'a ChildSchema<'a>,
    pub(super) transplanted: Option<BTreeMap<u64, Vec<AnalyzedField>>>,
}

impl Bm25Build<'_> {
    /// Build the store from the child's documents and write it beside the
    /// vector image. Children rebuild through the disk spiller for the
    /// same reason nodes do: a full-scale child's postings do not fit in
    /// heap.
    pub(super) fn write(
        mut self,
        mut mapped: MappedDocuments,
        analyze: &mut Analyzer,
    ) -> Result<PathBuf, String> {
        let schema = self.schema;
        let contract = match schema.binding {
            Some(binding) => crate::mapped_analysis::decode_contract(
                &binding.analysis_sha,
                &binding.analysis_contract,
                &binding.body_path,
            )?,
            None => MappedAnalysisContract::default(),
        };
        let tables = ChildTables::derive(&mapped.rows, schema, &contract)?;
        let rows = u32::try_from(mapped.row_parent_ids.len())
            .map_err(|_| "a child's rows exceed u32".to_string())?;
        let path = documents_path(self.vector_path);
        let mut builder = open_builder(&path, &tables)?;
        pin_fingerprints(&mut builder, &tables, schema.pinned_fingerprints, &contract)?;
        let mut i = 0;
        while i < mapped.rows.len() {
            let end = batch_end(&mapped.rows, i);
            let batch = &mut mapped.rows[i..end];
            let mut source = match self.transplanted.as_mut() {
                Some(fields) => FieldSource::Transplanted {
                    fields,
                    parent_of_local: &mapped.parent_of_local,
                },
                None => FieldSource::Analyzed(analyze_batch(batch, analyze)?.into_iter()),
            };
            for (local, doc) in batch.iter_mut() {
                add_row(&mut builder, &tables, *local, doc, &mut source)?;
            }
            i = end;
        }
        // The children stay bound to the plan the parents were written
        // under, and to the derived-column declaration (the kind-15
        // entry); replay must not launder either away.
        // Rows past the last document carry vectors only: empty slots keep
        // the store covering every row of the child, as a segment requires.
        builder
            .reserve_slots(rows)
            .map_err(|e| format!("reserve {rows} rows in {}: {e}", path.display()))?;
        builder.set_binding(schema.binding.cloned());
        builder.set_derived(schema.derived.cloned());
        builder
            .finish(&path)
            .map_err(|e| format!("write {}: {e}", path.display()))?;
        Ok(path)
    }
}

fn names(table: &[String]) -> Vec<&str> {
    table.iter().map(String::as_str).collect()
}

fn open_builder(path: &Path, tables: &ChildTables) -> Result<SpillBuilder, String> {
    let mut spill_dir = path.as_os_str().to_owned();
    spill_dir.push(".build");
    let spill_dir = PathBuf::from(spill_dir);
    let c = &tables.columns;
    let builder = SpillBuilder::create_with_text_fields(&spill_dir, &names(&tables.fields))
        .map_err(|e| format!("spill dir {}: {e}", spill_dir.display()))?
        .with_columns(crate::postings::ColumnKind::Facet, &names(&c.facets))
        .with_columns(crate::postings::ColumnKind::Numeric, &names(&c.numerics))
        .with_columns(crate::postings::ColumnKind::MapFacet, &names(&c.map_facets))
        .with_columns(
            crate::postings::ColumnKind::MapNumeric,
            &names(&c.map_numerics),
        )
        .with_columns(
            crate::postings::ColumnKind::MapInteger,
            &names(&c.map_integers),
        )
        .with_columns(
            crate::postings::ColumnKind::MapUnsignedInteger,
            &names(&c.map_unsigned_integers),
        )
        .with_columns(crate::postings::ColumnKind::Integer, &names(&c.integers))
        .with_columns(
            crate::postings::ColumnKind::UnsignedInteger,
            &names(&c.unsigned_integers),
        )
        .with_columns(crate::postings::ColumnKind::Geo, &names(&c.geo))
        .with_position_fields(&names(&tables.positions))
        .with_sentence_fields(&names(&tables.sentences));
    Ok(builder)
}

/// A compaction pins the source shard's per-field analyzer fingerprints
/// on every output up front, so a bucket whose rows never carry an
/// optional field still records the field's identity and the outputs
/// open as one set; a record that contradicts a pinned fingerprint
/// refuses as it would at ingest. A bound plan's fields are pinned to
/// the contract's specs the same way.
fn pin_fingerprints(
    builder: &mut SpillBuilder,
    tables: &ChildTables,
    pinned: Option<&[u64]>,
    contract: &MappedAnalysisContract,
) -> Result<(), String> {
    let table = &tables.fields;
    if let Some(pinned) = pinned {
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
    for field in &contract.fields {
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
    Ok(())
}

/// The end of the batch starting at `start`: whole documents, one
/// analyzer entry per field text (body first, extras in record order),
/// so reassembly is positional.
fn batch_end(rows: &[(u32, AddDocumentsRequest)], start: usize) -> usize {
    let mut end = start;
    let mut entries = 0usize;
    while end < rows.len() && (entries == 0 || entries < ANALYZE_BATCH) {
        entries += 1 + rows[end].1.fields.len();
        end += 1;
    }
    end
}

/// Analyze one batch's texts with the layers each needs on replay:
/// sentence spans for a sentence field, and the cased identity for the
/// body when the record names a cased field.
fn analyze_batch(
    batch: &[(u32, AddDocumentsRequest)],
    analyze: &mut Analyzer,
) -> Result<Vec<AnalyzedDoc>, String> {
    let mut entries: Vec<(&str, Option<&AnalysisSpec>, SessionLayers)> = Vec::new();
    for (_, d) in batch {
        entries.push((
            d.text.as_str(),
            d.analysis.as_ref(),
            SessionLayers {
                sentences: !d.sentence_fields.is_empty(),
                dual_cased: !d.cased_field.is_empty(),
                ..SessionLayers::default()
            },
        ));
        for f in &d.fields {
            entries.push((
                f.text.as_str(),
                f.analysis.as_ref(),
                SessionLayers {
                    sentences: d.sentence_fields.iter().any(|n| n == &f.field),
                    ..SessionLayers::default()
                },
            ));
        }
    }
    let analyzed = analyze(&entries)
        .map_err(|e| format!("analyze batch at child slot {}: {e}", batch[0].0))?;
    if analyzed.len() != entries.len() {
        return Err(format!(
            "analyzer returned {} results for {} field texts",
            analyzed.len(),
            entries.len()
        ));
    }
    Ok(analyzed)
}

fn add_row(
    builder: &mut SpillBuilder,
    tables: &ChildTables,
    local: u32,
    doc: &mut AddDocumentsRequest,
    source: &mut FieldSource<'_>,
) -> Result<(), String> {
    let fields = document::assemble_fields(&tables.fields, local, doc, source)?;
    document::set_fingerprints(builder, &tables.fields, doc)?;
    document::add_document(builder, local, doc, fields)?;
    super::columns::set_columns(builder, &tables.slots, local, doc)
}

/// Rewrite rows `[lo, hi)` of an already-built child store as a standalone
/// store renumbered from 0, through the same transplant path a segment
/// compaction uses (no re-analysis). `stem` names the output the way
/// [`Bm25Build::write`] does: the file lands at `<stem>.bm25`.
/// `parent_ids` is the child's full row-to-parent table.
pub(in crate::reshard) fn write_store_range(
    stem: &Path,
    reader: &crate::postings::Bm25Reader,
    parent_ids: &[u64],
    lo: u32,
    hi: u32,
) -> Result<PathBuf, String> {
    use crate::postings::Bm25Index;
    let tables = crate::reshard::segment_sources::segment_tables(reader);
    let context = || "split child store".to_string();
    let transposes = super::super::reconstruct::transpose_segment(reader, &context)?;
    let mut rows = Vec::new();
    let mut parent_of_local = BTreeMap::new();
    let mut transplanted: BTreeMap<u64, Vec<AnalyzedField>> = BTreeMap::new();
    for row in lo..hi {
        if Bm25Index::text(reader, row).is_none() {
            continue;
        }
        let parent = parent_ids[row as usize];
        let local = row - lo;
        let doc = super::super::reconstruct::reconstruct_document(reader, row, &tables, true)?;
        transplanted.insert(
            parent,
            super::super::reconstruct::transposed_fields(&transposes, row, &context)?,
        );
        parent_of_local.insert(local, parent);
        rows.push((local, doc));
    }
    let mapped = MappedDocuments {
        rows,
        row_parent_ids: parent_ids[lo as usize..hi as usize].to_vec(),
        parent_of_local,
    };
    let schema = ChildSchema {
        bm25_fields: Some(&tables.fields),
        pinned_fingerprints: Some(&tables.fingerprints),
        binding: reader.binding(),
        derived: reader.derived(),
        columns: Some(&tables.columns),
        hnsw_sources: None,
    };
    let mut no_analyze = |_: &[(&str, Option<&AnalysisSpec>, SessionLayers)]| {
        Err("a store-range split transplants analysis; it never re-analyzes".to_string())
    };
    Bm25Build {
        vector_path: stem,
        schema: &schema,
        transplanted: Some(transplanted),
    }
    .write(mapped, &mut no_analyze)
}

/// Where a child image's documents are written: `<vector path>.bm25`.
fn documents_path(vector_path: &std::path::Path) -> std::path::PathBuf {
    let mut path = vector_path.as_os_str().to_owned();
    path.push(".bm25");
    std::path::PathBuf::from(path)
}
