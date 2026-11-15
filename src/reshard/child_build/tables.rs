//! The child's BM25 field table and column tables: given by the caller,
//! or derived from the records the child holds.

use std::collections::BTreeSet;

use crate::pb::{AddDocumentsRequest, MappedAnalysisContract};

use super::{ChildSchema, ColumnTables};

/// Every table one child's BM25 store is built with.
pub(super) struct ChildTables {
    /// Text fields, body first.
    pub(super) fields: Vec<String>,
    pub(super) columns: ColumnTables,
    /// Fields that keep token positions.
    pub(super) positions: Vec<String>,
    /// Fields that keep sentence spans.
    pub(super) sentences: Vec<String>,
}

type Rows = [(u32, AddDocumentsRequest)];

impl ChildTables {
    pub(super) fn derive(
        rows: &Rows,
        schema: &ChildSchema<'_>,
        contract: &MappedAnalysisContract,
    ) -> Result<Self, String> {
        let fields = field_table(rows, schema.bm25_fields, contract);
        if fields.first().map(String::as_str) != Some("body") {
            return Err("bm25 field table must start with \"body\"".to_string());
        }
        let columns = match schema.columns {
            Some(columns) => columns.clone(),
            None => derive_columns(rows),
        };
        let positions = layer_table(rows, &fields, "keeps positions on", |doc| {
            &doc.position_fields
        })?;
        let sentences = layer_table(rows, &fields, "keeps sentence spans on", |doc| {
            &doc.sentence_fields
        })?;
        Ok(Self {
            fields,
            columns,
            positions,
            sentences,
        })
    }
}

/// The child field table: the caller's fleet table
/// when given, else "body" plus every extra field name in this child's
/// records in canonical lexical order. First-sight order is not
/// sufficient: a sparse optional field can first appear after an
/// always-present phrase field in one hash child and before it in
/// another, producing incompatible positional schemas. Old logs carry no
/// extra fields and derive the single-field table.
fn field_table(
    rows: &Rows,
    bm25_fields: Option<&[String]>,
    contract: &MappedAnalysisContract,
) -> Vec<String> {
    if let Some(table) = bm25_fields {
        return table.to_vec();
    }
    let mut extras = BTreeSet::new();
    for (_, doc) in rows {
        extras.extend(doc.fields.iter().map(|f| f.field.clone()));
        extras.extend(doc.phrases.iter().map(|phrase| phrase.field.clone()));
        if !doc.phrase_field.is_empty() {
            extras.insert(doc.phrase_field.clone());
        }
        extras.extend(doc.bigram_fields.iter().map(|bigram| bigram.field.clone()));
    }
    extras.extend(contract.fields.iter().map(|field| field.name.clone()));
    extras.remove("body");
    let mut table = vec!["body".to_string()];
    table.extend(extras);
    table
}

/// Names in first-seen order across the records.
fn first_seen<'a, I>(rows: &'a Rows, names: impl Fn(&'a AddDocumentsRequest) -> I) -> Vec<String>
where
    I: Iterator<Item = &'a String>,
{
    let mut table: Vec<String> = Vec::new();
    for (_, doc) in rows {
        for name in names(doc) {
            if !table.contains(name) {
                table.push(name.clone());
            }
        }
    }
    table
}

/// The column tables derived from the replayed records (first-seen
/// order): the WAL is the durable column record, so a field no record
/// values never reaches the child. Integers and timestamps name the SAME
/// i64 columns, so both lists feed one table — a
/// child whose records only ever carried timestamps still gets the
/// column. Geo columns come from one list.
fn derive_columns(rows: &Rows) -> ColumnTables {
    ColumnTables {
        facets: first_seen(rows, |doc| doc.facets.iter().map(|v| &v.field)),
        numerics: first_seen(rows, |doc| doc.numerics.iter().map(|v| &v.field)),
        map_facets: first_seen(rows, |doc| doc.map_facets.iter().map(|v| &v.field)),
        map_numerics: first_seen(rows, |doc| doc.map_numerics.iter().map(|v| &v.field)),
        map_integers: first_seen(rows, |doc| doc.map_integers.iter().map(|v| &v.field)),
        map_unsigned_integers: first_seen(rows, |doc| {
            doc.map_unsigned_integers.iter().map(|v| &v.field)
        }),
        integers: first_seen(rows, |doc| {
            doc.integers
                .iter()
                .map(|v| &v.field)
                .chain(doc.timestamps.iter().map(|v| &v.field))
        }),
        unsigned_integers: first_seen(rows, |doc| doc.unsigned_integers.iter().map(|v| &v.field)),
        geo: first_seen(rows, |doc| doc.geo_points.iter().map(|v| &v.field)),
    }
}

/// A per-field layer table (positions or sentence spans), from the
/// records' own lists, which every record of one generation carries
/// identically; a field named outside the table is a corrupt record,
/// refused.
fn layer_table(
    rows: &Rows,
    fields: &[String],
    keeps: &str,
    names: impl Fn(&AddDocumentsRequest) -> &Vec<String>,
) -> Result<Vec<String>, String> {
    let mut table = BTreeSet::new();
    for (local, doc) in rows {
        for name in names(doc) {
            if !fields.contains(name) {
                return Err(format!(
                    "record at child slot {local} {keeps} {name:?} outside the table {fields:?}"
                ));
            }
            table.insert(name.clone());
        }
    }
    Ok(table.into_iter().collect())
}
