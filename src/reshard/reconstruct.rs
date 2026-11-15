//! A sealed segment's row read back as the logged document it stands
//! for, and its analyzed fields transposed out of the postings.

use crate::pb::AddDocumentsRequest;
use crate::postings::{AnalyzedField, Bm25Reader, FieldTranspose};

use super::segment_sources::SourceTables;

/// The logged document row `row` of `bm25` stands for: its columns,
/// and with `with_text` its text, lineage, source and identity too.
pub(crate) fn reconstruct_document(
    bm25: &Bm25Reader,
    row: u32,
    tables: &SourceTables,
    with_text: bool,
) -> Result<AddDocumentsRequest, String> {
    let mut doc = AddDocumentsRequest::default();
    if with_text {
        restore_text(bm25, row, tables, &mut doc)?;
    }
    // The store's declaration entry says what the row's derived values
    // mean; a logged form of the row carries it.
    doc.derived_fingerprint = bm25
        .derived()
        .map(|derived| derived.fingerprint.clone())
        .unwrap_or_default();
    restore_scalar_columns(bm25, row, tables, &mut doc);
    restore_map_columns(bm25, row, tables, &mut doc);
    Ok(doc)
}

fn restore_text(
    bm25: &Bm25Reader,
    row: u32,
    tables: &SourceTables,
    doc: &mut AddDocumentsRequest,
) -> Result<(), String> {
    use crate::postings::Bm25Index;
    doc.text = Bm25Index::text(bm25, row).ok_or_else(|| format!("row {row} has no stored text"))?;
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
    Ok(())
}

fn restore_scalar_columns(
    bm25: &Bm25Reader,
    row: u32,
    tables: &SourceTables,
    doc: &mut AddDocumentsRequest,
) {
    let columns = &tables.columns;
    for (fi, name) in columns.facets.iter().enumerate() {
        if let Some(ord) = bm25.facet_ord(fi, row) {
            doc.facets.push(crate::pb::FacetValue {
                field: name.clone(),
                value: bm25.facet_value(fi, ord).to_string(),
            });
        }
    }
    for (ni, name) in columns.numerics.iter().enumerate() {
        if let Some(value) = bm25.numeric_value(ni, row) {
            let field = name.clone();
            doc.numerics.push(crate::pb::NumericValue { field, value });
        }
    }
    for (ii, name) in columns.integers.iter().enumerate() {
        if let Some(value) = bm25.integer_value(ii, row) {
            let field = name.clone();
            doc.integers.push(crate::pb::IntegerValue { field, value });
        }
    }
    for (ui, name) in columns.unsigned_integers.iter().enumerate() {
        if let Some(value) = bm25.unsigned_integer_value(ui, row) {
            let field = name.clone();
            doc.unsigned_integers
                .push(crate::pb::UnsignedIntegerValue { field, value });
        }
    }
    for (gi, name) in columns.geo.iter().enumerate() {
        if let Some((lat, lon)) = bm25.geo_value(gi, row) {
            let field = name.clone();
            doc.geo_points
                .push(crate::pb::GeoPointValue { field, lat, lon });
        }
    }
}

fn restore_map_columns(
    bm25: &Bm25Reader,
    row: u32,
    tables: &SourceTables,
    doc: &mut AddDocumentsRequest,
) {
    let columns = &tables.columns;
    for (ci, name) in columns.map_facets.iter().enumerate() {
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
    for (ci, name) in columns.map_numerics.iter().enumerate() {
        for (key_ord, key) in bm25.map_numeric_keys(ci).iter().enumerate() {
            if let Some(value) = bm25.map_numeric_value(ci, key_ord as u32, row) {
                let (field, key) = (name.clone(), key.clone());
                doc.map_numerics
                    .push(crate::pb::MapNumericEntry { field, key, value });
            }
        }
    }
    for (ci, name) in columns.map_integers.iter().enumerate() {
        for (key_ord, key) in bm25.map_integer_keys(ci).iter().enumerate() {
            if let Some(value) = bm25.map_integer_value(ci, key_ord as u32, row) {
                let (field, key) = (name.clone(), key.clone());
                doc.map_integers
                    .push(crate::pb::MapIntegerEntry { field, key, value });
            }
        }
    }
    for (ci, name) in columns.map_unsigned_integers.iter().enumerate() {
        for (key_ord, key) in bm25.map_unsigned_integer_keys(ci).iter().enumerate() {
            if let Some(value) = bm25.map_unsigned_integer_value(ci, key_ord as u32, row) {
                let (field, key) = (name.clone(), key.clone());
                doc.map_unsigned_integers
                    .push(crate::pb::MapUnsignedIntegerEntry { field, key, value });
            }
        }
    }
}

/// Every field of a segment's store transposed per document, one segment
/// held at a time. `context` names the segment in a failure.
pub(super) fn transpose_segment(
    bm25: &Bm25Reader,
    context: &dyn Fn() -> String,
) -> Result<Vec<FieldTranspose>, String> {
    (0..bm25.field_count())
        .map(|f| {
            bm25.field(f)
                .transpose()
                .map_err(|e| format!("{}: {e}", context()))
        })
        .collect()
}

/// Row `row`'s analyzed fields, one per transposed field.
pub(super) fn transposed_fields(
    transposes: &[FieldTranspose],
    row: u32,
    context: &dyn Fn() -> String,
) -> Result<Vec<AnalyzedField>, String> {
    transposes
        .iter()
        .map(|transpose| {
            transpose
                .field(row)
                .ok_or_else(|| format!("{}: row {row} is past the transpose", context()))
        })
        .collect()
}
