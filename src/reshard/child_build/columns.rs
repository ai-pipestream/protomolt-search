//! A document's column values written into the child's BM25 store.

use crate::pb::AddDocumentsRequest;
use crate::postings::SpillBuilder;

use super::ColumnTables;

fn index_of(table: &[String], name: &str) -> Option<usize> {
    table.iter().position(|n| n == name)
}

/// Write every column value the document carries at `local`.
pub(super) fn set_columns(
    builder: &mut SpillBuilder,
    tables: &ColumnTables,
    local: u32,
    doc: &AddDocumentsRequest,
) -> Result<(), String> {
    set_keyword_columns(builder, tables, local, doc);
    set_map_integer_columns(builder, tables, local, doc)?;
    set_integer_columns(builder, tables, local, doc)?;
    for e in &doc.geo_points {
        let gi = index_of(&tables.geo, &e.field).expect("geo table was derived from these records");
        builder.set_geo(gi, local, e.lat, e.lon);
    }
    Ok(())
}

fn set_keyword_columns(
    builder: &mut SpillBuilder,
    tables: &ColumnTables,
    local: u32,
    doc: &AddDocumentsRequest,
) {
    for fv in &doc.facets {
        let fi = index_of(&tables.facets, &fv.field)
            .expect("facet table was derived from these records");
        builder.set_facet(fi, local, &fv.value);
    }
    for nv in &doc.numerics {
        let ni = index_of(&tables.numerics, &nv.field)
            .expect("numeric table was derived from these records");
        builder.set_numeric(ni, local, nv.value);
    }
    for e in &doc.map_facets {
        let ci = index_of(&tables.map_facets, &e.field)
            .expect("map-facet table was derived from these records");
        builder.set_map_facet(ci, local, &e.key, &e.value);
    }
    for e in &doc.map_numerics {
        let ci = index_of(&tables.map_numerics, &e.field)
            .expect("map-numeric table was derived from these records");
        builder.set_map_numeric(ci, local, &e.key, e.value);
    }
}

fn set_map_integer_columns(
    builder: &mut SpillBuilder,
    tables: &ColumnTables,
    local: u32,
    doc: &AddDocumentsRequest,
) -> Result<(), String> {
    let absent =
        |field: &str| format!("integer map column {field:?} is absent from the replay table");
    for e in &doc.map_integers {
        let ci = index_of(&tables.map_integers, &e.field).ok_or_else(|| absent(&e.field))?;
        builder
            .set_map_integer(ci, local, &e.key, e.value)
            .map_err(|e| format!("integer map replay: {e}"))?;
    }
    for e in &doc.map_unsigned_integers {
        let ci =
            index_of(&tables.map_unsigned_integers, &e.field).ok_or_else(|| absent(&e.field))?;
        builder
            .set_map_unsigned_integer(ci, local, &e.key, e.value)
            .map_err(|e| format!("integer map replay: {e}"))?;
    }
    Ok(())
}

/// Integers, unsigned integers, and timestamps. Timestamps re-run the
/// node's conversion from the instant the WAL kept: the child gets the
/// same epoch micros the parent stored, not a copy of a copy.
fn set_integer_columns(
    builder: &mut SpillBuilder,
    tables: &ColumnTables,
    local: u32,
    doc: &AddDocumentsRequest,
) -> Result<(), String> {
    for e in &doc.integers {
        let ii = index_of(&tables.integers, &e.field)
            .expect("integer table was derived from these records");
        builder.set_integer(ii, local, e.value);
    }
    let mut unsigned_seen = std::collections::HashSet::new();
    for value in &doc.unsigned_integers {
        if !unsigned_seen.insert(&value.field) {
            return Err(format!(
                "record at child slot {local}: unsigned field {:?} repeats",
                value.field
            ));
        }
        let column = index_of(&tables.unsigned_integers, &value.field).ok_or_else(|| {
            format!(
                "record at child slot {local}: undeclared unsigned field {:?}",
                value.field
            )
        })?;
        builder.set_unsigned_integer(column, local, value.value);
    }
    for e in &doc.timestamps {
        let ii = index_of(&tables.integers, &e.field)
            .expect("integer table was derived from these records");
        let ts = e.value.as_ref().ok_or_else(|| {
            format!(
                "record at child slot {local}: timestamp field {:?} carries no instant",
                e.field
            )
        })?;
        let micros = crate::node::timestamp_to_epoch_micros(&e.field, ts)
            .map_err(|e| format!("record at child slot {local}: {}", e.message()))?;
        builder.set_integer(ii, local, micros);
    }
    Ok(())
}
