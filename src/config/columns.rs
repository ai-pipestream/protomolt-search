//! Declared column lists, and the phrase and proximity fields over them.

use std::path::PathBuf;

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

/// The column lists, refused when one name appears under two kinds.
pub(super) fn parse_column_fields(
    o: Options<'_>,
    file: &FileConfig,
    placement_column: &Option<String>,
) -> Result<ColumnFields, String> {
    let bm25_fields = match o.given("bm25-fields") {
        Some(raw) => super::option::split_list(&raw),
        None => file
            .bm25_fields
            .clone()
            .unwrap_or_else(|| vec!["body".to_string()]),
    };
    let c = ColumnFields {
        bm25_fields,
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
    check_field_tables(&c)?;
    check_one_name_space(&c, placement_column)?;
    Ok(c)
}

/// The BM25, facet and numeric tables: body first, no repeats, and no
/// name that is both a facet and a numeric column.
fn check_field_tables(c: &ColumnFields) -> Result<(), String> {
    if c.bm25_fields.first().map(String::as_str) != Some("body") {
        return Err(
            "bm25 fields must start with \"body\" (field 0 is the stored body)".to_string(),
        );
    }
    for (i, name) in c.bm25_fields.iter().enumerate() {
        if c.bm25_fields[..i].contains(name) {
            return Err(format!("bm25 field {name:?} repeats in the field table"));
        }
    }

    for (i, name) in c.facet_fields.iter().enumerate() {
        if c.facet_fields[..i].contains(name) {
            return Err(format!("facet field {name:?} repeats in the facet table"));
        }
    }

    for (i, name) in c.numeric_fields.iter().enumerate() {
        if c.numeric_fields[..i].contains(name) {
            return Err(format!(
                "numeric field {name:?} repeats in the numeric table"
            ));
        }
        if c.facet_fields.contains(name) {
            return Err(format!(
                "column {name:?} is declared as both a facet and a numeric field; \
                 the v7 column table holds one column per name"
            ));
        }
    }
    Ok(())
}

/// One name per column across every kind. The placement column is an
/// integer column whether or not the integer list names it.
fn check_one_name_space(c: &ColumnFields, placement_column: &Option<String>) -> Result<(), String> {
    let mut all: Vec<&String> = Vec::new();
    let placement_as_integer: Vec<String> = placement_column
        .iter()
        .filter(|column| !c.integer_fields.contains(column))
        .cloned()
        .collect();
    for name in c
        .facet_fields
        .iter()
        .chain(&c.numeric_fields)
        .chain(&c.map_facet_fields)
        .chain(&c.map_numeric_fields)
        .chain(&c.map_integer_fields)
        .chain(&c.map_unsigned_integer_fields)
        .chain(&c.integer_fields)
        .chain(&c.unsigned_integer_fields)
        .chain(&placement_as_integer)
        .chain(&c.geo_fields)
    {
        if all.contains(&name) {
            return Err(format!(
                "column {name:?} is declared under more than one column kind; \
                 the v7 column table holds one column per name"
            ));
        }
        all.push(name);
    }
    Ok(())
}

/// The phrase field and the proximity payloads declared over the BM25 fields.
pub(super) struct PhraseFields {
    pub(super) phrase_glossary: Option<PathBuf>,
    pub(super) phrase_field: String,
    pub(super) entity_map_field: Option<String>,
    pub(super) phrase_ignore_case: bool,
    pub(super) phrase_ner: bool,
    pub(super) position_fields: Vec<String>,
    pub(super) bigram_fields: Vec<String>,
    pub(super) sentence_fields: Vec<String>,
}

/// Phrase and proximity declarations, each checked against the field table.
pub(super) fn parse_phrase_fields(
    o: Options<'_>,
    file: &FileConfig,
    bm25_fields: &[String],
    map_facet_fields: &[String],
) -> Result<PhraseFields, String> {
    let p = PhraseFields {
        phrase_glossary: o.path("phrase-glossary", file.phrase_glossary.as_deref()),
        phrase_field: o
            .text("phrase-field", file.phrase_field.as_deref())
            .unwrap_or_else(|| "phrases".to_string()),
        entity_map_field: o.nonblank("entity-map-field", file.entity_map_field.as_deref()),
        phrase_ignore_case: o.switch("phrase-ignore-case", file.phrase_ignore_case, true),
        phrase_ner: o.switch_or_bare("phrase-ner", file.phrase_ner, false),
        position_fields: o.list("position-fields", file.position_fields.as_ref()),
        bigram_fields: o.list("bigram-fields", file.bigram_fields.as_ref()),
        sentence_fields: o.list("sentence-fields", file.sentence_fields.as_ref()),
    };
    check_phrase_field(&p, bm25_fields, map_facet_fields)?;
    check_sentence_and_position_fields(&p, bm25_fields)?;
    check_bigram_fields(&p, bm25_fields)?;
    Ok(p)
}

/// The glossary's phrase field is a non-body BM25 field, and the entity
/// map and NER options need the glossary.
fn check_phrase_field(
    p: &PhraseFields,
    bm25_fields: &[String],
    map_facet_fields: &[String],
) -> Result<(), String> {
    let phrase_field = &p.phrase_field;
    if p.phrase_glossary.is_some() {
        if p.phrase_field == "body" || !bm25_fields.contains(&p.phrase_field) {
            return Err(format!(
                "phrase glossary requires phrase field {phrase_field:?} as a non-body entry in --bm25-fields"
            ));
        }
        if let Some(field) = &p.entity_map_field {
            if !map_facet_fields.contains(field) {
                return Err(format!(
                    "entity map field {field:?} must be declared in --map-facet-fields"
                ));
            }
        }
        if p.phrase_ner && p.entity_map_field.is_none() {
            return Err("--phrase-ner requires --entity-map-field".to_string());
        }
    } else if p.entity_map_field.is_some() || p.phrase_ner {
        return Err("--entity-map-field and --phrase-ner require --phrase-glossary".to_string());
    }
    Ok(())
}

/// Sentence and positional fields are declared BM25 fields: sentences on
/// the body only, positions never on the glossary's phrase field.
fn check_sentence_and_position_fields(
    p: &PhraseFields,
    bm25_fields: &[String],
) -> Result<(), String> {
    for (i, name) in p.sentence_fields.iter().enumerate() {
        if p.sentence_fields[..i].contains(name) {
            return Err(format!(
                "sentence field {name:?} repeats in --sentence-fields"
            ));
        }
        if !bm25_fields.contains(name) {
            return Err(format!(
                "sentence field {name:?} must be declared in --bm25-fields"
            ));
        }
        if name != "body" {
            return Err(format!(
                "sentence field {name:?}: snippets are cut from stored text, and only the body's text is stored; --sentence-fields accepts body only"
            ));
        }
    }
    for (i, name) in p.position_fields.iter().enumerate() {
        if p.position_fields[..i].contains(name) {
            return Err(format!(
                "positional field {name:?} repeats in --position-fields"
            ));
        }
        if !bm25_fields.contains(name) {
            return Err(format!(
                "positional field {name:?} must be declared in --bm25-fields"
            ));
        }
        if p.phrase_glossary.is_some() && *name == p.phrase_field {
            return Err(format!(
                "phrase field {name:?} holds glossary concepts, not tokens; it cannot keep token positions"
            ));
        }
    }
    Ok(())
}

/// A bigram column is a declared BM25 entry named after its analyzed
/// source field, and keeps no positions.
fn check_bigram_fields(p: &PhraseFields, bm25_fields: &[String]) -> Result<(), String> {
    let phrase_field = &p.phrase_field;
    for (i, source) in p.bigram_fields.iter().enumerate() {
        let derived = crate::proximity::bigram_field_name(source);
        if p.bigram_fields[..i].contains(source) {
            return Err(format!(
                "bigram source {source:?} repeats in --bigram-fields"
            ));
        }
        if !bm25_fields.contains(source) {
            return Err(format!(
                "bigram source field {source:?} must be declared in --bm25-fields"
            ));
        }
        if !bm25_fields.contains(&derived) {
            return Err(format!(
                "bigram column {derived:?} (derived from {source:?}) must be declared in --bm25-fields"
            ));
        }
        if crate::proximity::bigram_source(source).is_some() {
            return Err(format!(
                "bigram source {source:?} is itself a bigram column; columns derive from analyzed fields only"
            ));
        }
        if p.position_fields.contains(&derived) {
            return Err(format!(
                "bigram column {derived:?} cannot keep token positions; it is a term column derived from {source:?}"
            ));
        }
        if p.phrase_glossary.is_some() && (*source == p.phrase_field || derived == p.phrase_field) {
            return Err(format!(
                "bigram source {source:?} and the phrase field {phrase_field:?} must be distinct"
            ));
        }
    }
    Ok(())
}
