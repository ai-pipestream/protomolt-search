//! Declared column lists, one per column kind, over one shared name space.

use super::file::FileConfig;
use super::option::Options;

/// The declared columns of the unnamed dataset, one list per column kind.
pub(super) struct ColumnFields {
    pub(super) bm25_fields: Vec<String>,
    pub(super) facet_fields: Vec<String>,
    pub(super) numeric_fields: Vec<String>,
    pub(super) map_facet_fields: Vec<String>,
    pub(super) map_numeric_fields: Vec<String>,
    pub(super) map_integer_fields: Vec<String>,
    pub(super) map_unsigned_integer_fields: Vec<String>,
    pub(super) integer_fields: Vec<String>,
    pub(super) unsigned_integer_fields: Vec<String>,
    pub(super) geo_fields: Vec<String>,
}

/// The first name that repeats in `names`.
pub(super) fn first_repeat(names: &[String]) -> Option<&String> {
    names
        .iter()
        .enumerate()
        .find(|(i, name)| names[..*i].contains(name))
        .map(|(_, name)| name)
}

/// The BM25 field table: "body" first, then the extra indexed fields.
fn bm25_fields(o: Options, file: &FileConfig) -> Result<Vec<String>, String> {
    let default = vec!["body".to_string()];
    let fields = o.list(
        "bm25-fields",
        Some(file.bm25_fields.as_ref().unwrap_or(&default)),
    );
    if fields.first().map(String::as_str) != Some("body") {
        return Err(
            "bm25 fields must start with \"body\" (field 0 is the stored body)".to_string(),
        );
    }
    if let Some(name) = first_repeat(&fields) {
        return Err(format!("bm25 field {name:?} repeats in the field table"));
    }
    Ok(fields)
}

/// One name space across all column kinds: the v7 column table refuses
/// duplicates, so the config does too, early and by name. The placement
/// column is an integer column whether or not the integer list names it.
fn check_one_name_space(
    columns: &ColumnFields,
    placement_column: Option<&String>,
) -> Result<(), String> {
    let placement = placement_column.filter(|column| !columns.integer_fields.contains(column));
    let mut seen: Vec<&String> = Vec::new();
    for name in columns
        .facet_fields
        .iter()
        .chain(&columns.numeric_fields)
        .chain(&columns.map_facet_fields)
        .chain(&columns.map_numeric_fields)
        .chain(&columns.map_integer_fields)
        .chain(&columns.map_unsigned_integer_fields)
        .chain(&columns.integer_fields)
        .chain(&columns.unsigned_integer_fields)
        .chain(placement)
        .chain(&columns.geo_fields)
    {
        if seen.contains(&name) {
            return Err(format!(
                "column {name:?} is declared under more than one column kind; \
                 the v7 column table holds one column per name"
            ));
        }
        seen.push(name);
    }
    Ok(())
}

/// Facet and numeric tables refuse a repeat within themselves, and a name
/// declared as both.
fn check_facet_numeric(columns: &ColumnFields) -> Result<(), String> {
    if let Some(name) = first_repeat(&columns.facet_fields) {
        return Err(format!("facet field {name:?} repeats in the facet table"));
    }
    if let Some(name) = first_repeat(&columns.numeric_fields) {
        return Err(format!(
            "numeric field {name:?} repeats in the numeric table"
        ));
    }
    if let Some(name) = columns
        .numeric_fields
        .iter()
        .find(|name| columns.facet_fields.contains(name))
    {
        return Err(format!(
            "column {name:?} is declared as both a facet and a numeric field; \
             the v7 column table holds one column per name"
        ));
    }
    Ok(())
}

/// The column lists, refused when one name appears under two kinds.
pub(super) fn parse_column_fields(
    o: Options,
    file: &FileConfig,
    placement_column: Option<&String>,
) -> Result<ColumnFields, String> {
    let columns = ColumnFields {
        bm25_fields: bm25_fields(o, file)?,
        facet_fields: o.list("facet-fields", file.facet_fields.as_ref()),
        numeric_fields: o.list("numeric-fields", file.numeric_fields.as_ref()),
        map_facet_fields: o.list("map-facet-fields", file.map_facet_fields.as_ref()),
        map_numeric_fields: o.list("map-numeric-fields", file.map_numeric_fields.as_ref()),
        map_integer_fields: o.list("map-integer-fields", file.map_integer_fields.as_ref()),
        map_unsigned_integer_fields: o.list(
            "map-unsigned-integer-fields",
            file.map_unsigned_integer_fields.as_ref(),
        ),
        integer_fields: o.list("integer-fields", file.integer_fields.as_ref()),
        unsigned_integer_fields: o.list(
            "unsigned-integer-fields",
            file.unsigned_integer_fields.as_ref(),
        ),
        geo_fields: o.list("geo-fields", file.geo_fields.as_ref()),
    };
    check_facet_numeric(&columns)?;
    check_one_name_space(&columns, placement_column)?;
    Ok(columns)
}
