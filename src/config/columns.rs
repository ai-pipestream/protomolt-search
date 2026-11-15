//! Declared column lists, and the phrase and proximity fields over them.

use std::path::PathBuf;

use super::args::{flag_present, opt, parse_env_bool};
use super::file::FileConfig;

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
    args: &[String],
    file: &FileConfig,
    placement_column: &Option<String>,
) -> Result<ColumnFields, String> {
    let bm25_fields = match opt(args, "bm25-fields", "PIPESTREAM_SEARCH_BM25_FIELDS", None) {
        Some(s) => s
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect(),
        None => file
            .bm25_fields
            .clone()
            .unwrap_or_else(|| vec!["body".to_string()]),
    };
    if bm25_fields.first().map(String::as_str) != Some("body") {
        return Err(
            "bm25 fields must start with \"body\" (field 0 is the stored body)".to_string(),
        );
    }
    for (i, name) in bm25_fields.iter().enumerate() {
        if bm25_fields[..i].contains(name) {
            return Err(format!("bm25 field {name:?} repeats in the field table"));
        }
    }

    let facet_fields: Vec<String> =
        match opt(args, "facet-fields", "PIPESTREAM_SEARCH_FACET_FIELDS", None) {
            Some(s) => s
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect(),
            None => file.facet_fields.clone().unwrap_or_default(),
        };
    for (i, name) in facet_fields.iter().enumerate() {
        if facet_fields[..i].contains(name) {
            return Err(format!("facet field {name:?} repeats in the facet table"));
        }
    }

    let numeric_fields: Vec<String> = match opt(
        args,
        "numeric-fields",
        "PIPESTREAM_SEARCH_NUMERIC_FIELDS",
        None,
    ) {
        Some(s) => s
            .split(',')
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .map(str::to_string)
            .collect(),
        None => file.numeric_fields.clone().unwrap_or_default(),
    };
    for (i, name) in numeric_fields.iter().enumerate() {
        if numeric_fields[..i].contains(name) {
            return Err(format!(
                "numeric field {name:?} repeats in the numeric table"
            ));
        }
        if facet_fields.contains(name) {
            return Err(format!(
                "column {name:?} is declared as both a facet and a numeric field; \
                 the v7 column table holds one column per name"
            ));
        }
    }

    let parse_list = |flag: &str, env: &str, file_val: &Option<Vec<String>>| -> Vec<String> {
        match opt(args, flag, env, None) {
            Some(s) => s
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect(),
            None => file_val.clone().unwrap_or_default(),
        }
    };
    let map_facet_fields = parse_list(
        "map-facet-fields",
        "PIPESTREAM_SEARCH_MAP_FACET_FIELDS",
        &file.map_facet_fields,
    );
    let map_numeric_fields = parse_list(
        "map-numeric-fields",
        "PIPESTREAM_SEARCH_MAP_NUMERIC_FIELDS",
        &file.map_numeric_fields,
    );
    let map_integer_fields = parse_list(
        "map-integer-fields",
        "PIPESTREAM_SEARCH_MAP_INTEGER_FIELDS",
        &file.map_integer_fields,
    );
    let map_unsigned_integer_fields = parse_list(
        "map-unsigned-integer-fields",
        "PIPESTREAM_SEARCH_MAP_UNSIGNED_INTEGER_FIELDS",
        &file.map_unsigned_integer_fields,
    );
    let integer_fields = parse_list(
        "integer-fields",
        "PIPESTREAM_SEARCH_INTEGER_FIELDS",
        &file.integer_fields,
    );
    let unsigned_integer_fields = parse_list(
        "unsigned-integer-fields",
        "PIPESTREAM_SEARCH_UNSIGNED_INTEGER_FIELDS",
        &file.unsigned_integer_fields,
    );
    let geo_fields = parse_list(
        "geo-fields",
        "PIPESTREAM_SEARCH_GEO_FIELDS",
        &file.geo_fields,
    );
    // One name space across all column kinds: the v7 column table
    // refuses duplicates, so the config does too, early and by name.
    // The placement column is an integer column whether or not the
    // integer list names it.
    {
        let mut all: Vec<&String> = Vec::new();
        let placement_as_integer: Vec<String> = placement_column
            .iter()
            .filter(|column| !integer_fields.contains(column))
            .cloned()
            .collect();
        for name in facet_fields
            .iter()
            .chain(&numeric_fields)
            .chain(&map_facet_fields)
            .chain(&map_numeric_fields)
            .chain(&map_integer_fields)
            .chain(&map_unsigned_integer_fields)
            .chain(&integer_fields)
            .chain(&unsigned_integer_fields)
            .chain(&placement_as_integer)
            .chain(&geo_fields)
        {
            if all.contains(&name) {
                return Err(format!(
                    "column {name:?} is declared under more than one column kind; \
                     the v7 column table holds one column per name"
                ));
            }
            all.push(name);
        }
    }
    Ok(ColumnFields {
        bm25_fields,
        facet_fields,
        numeric_fields,
        map_facet_fields,
        map_numeric_fields,
        map_integer_fields,
        map_unsigned_integer_fields,
        integer_fields,
        unsigned_integer_fields,
        geo_fields,
    })
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
    args: &[String],
    file: &FileConfig,
    bm25_fields: &[String],
    map_facet_fields: &[String],
) -> Result<PhraseFields, String> {
    let phrase_glossary = opt(
        args,
        "phrase-glossary",
        "PIPESTREAM_SEARCH_PHRASE_GLOSSARY",
        file.phrase_glossary.as_deref(),
    )
    .filter(|value| !value.trim().is_empty())
    .map(PathBuf::from);
    let phrase_field = opt(
        args,
        "phrase-field",
        "PIPESTREAM_SEARCH_PHRASE_FIELD",
        file.phrase_field.as_deref(),
    )
    .unwrap_or_else(|| "phrases".to_string());
    let entity_map_field = opt(
        args,
        "entity-map-field",
        "PIPESTREAM_SEARCH_ENTITY_MAP_FIELD",
        file.entity_map_field.as_deref(),
    )
    .filter(|value| !value.trim().is_empty());
    let phrase_ignore_case = opt(
        args,
        "phrase-ignore-case",
        "PIPESTREAM_SEARCH_PHRASE_IGNORE_CASE",
        file.phrase_ignore_case
            .map(|value| value.to_string())
            .as_deref(),
    )
    .map(|value| parse_env_bool(&value))
    .unwrap_or(true);
    let phrase_ner = if flag_present(args, "phrase-ner") {
        true
    } else {
        opt(
            args,
            "phrase-ner",
            "PIPESTREAM_SEARCH_PHRASE_NER",
            file.phrase_ner.map(|value| value.to_string()).as_deref(),
        )
        .map(|value| parse_env_bool(&value))
        .unwrap_or(false)
    };
    if phrase_glossary.is_some() {
        if phrase_field == "body" || !bm25_fields.contains(&phrase_field) {
            return Err(format!(
                "phrase glossary requires phrase field {phrase_field:?} as a non-body entry in --bm25-fields"
            ));
        }
        if let Some(field) = &entity_map_field {
            if !map_facet_fields.contains(field) {
                return Err(format!(
                    "entity map field {field:?} must be declared in --map-facet-fields"
                ));
            }
        }
        if phrase_ner && entity_map_field.is_none() {
            return Err("--phrase-ner requires --entity-map-field".to_string());
        }
    } else if entity_map_field.is_some() || phrase_ner {
        return Err("--entity-map-field and --phrase-ner require --phrase-glossary".to_string());
    }

    // Proximity payloads (docs/phrase-proximity.md). Both are explicit
    // storage declarations, like the phrase field: a positional field
    // must be in the table, and a bigram column is a declared table
    // entry named after its source, never an implicit extra field.
    let list = |name: &str, env: &str, file: Option<&Vec<String>>| -> Vec<String> {
        match opt(args, name, env, None) {
            Some(s) => s
                .split(',')
                .map(str::trim)
                .filter(|s| !s.is_empty())
                .map(str::to_string)
                .collect(),
            None => file.cloned().unwrap_or_default(),
        }
    };
    let position_fields = list(
        "position-fields",
        "PIPESTREAM_SEARCH_POSITION_FIELDS",
        file.position_fields.as_ref(),
    );
    let bigram_fields = list(
        "bigram-fields",
        "PIPESTREAM_SEARCH_BIGRAM_FIELDS",
        file.bigram_fields.as_ref(),
    );
    let sentence_fields = list(
        "sentence-fields",
        "PIPESTREAM_SEARCH_SENTENCE_FIELDS",
        file.sentence_fields.as_ref(),
    );
    for (i, name) in sentence_fields.iter().enumerate() {
        if sentence_fields[..i].contains(name) {
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
    for (i, name) in position_fields.iter().enumerate() {
        if position_fields[..i].contains(name) {
            return Err(format!(
                "positional field {name:?} repeats in --position-fields"
            ));
        }
        if !bm25_fields.contains(name) {
            return Err(format!(
                "positional field {name:?} must be declared in --bm25-fields"
            ));
        }
        if phrase_glossary.is_some() && *name == phrase_field {
            return Err(format!(
                "phrase field {name:?} holds glossary concepts, not tokens; it cannot keep token positions"
            ));
        }
    }
    for (i, source) in bigram_fields.iter().enumerate() {
        let derived = crate::proximity::bigram_field_name(source);
        if bigram_fields[..i].contains(source) {
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
        if position_fields.contains(&derived) {
            return Err(format!(
                "bigram column {derived:?} cannot keep token positions; it is a term column derived from {source:?}"
            ));
        }
        if phrase_glossary.is_some() && (*source == phrase_field || derived == phrase_field) {
            return Err(format!(
                "bigram source {source:?} and the phrase field {phrase_field:?} must be distinct"
            ));
        }
    }
    Ok(PhraseFields {
        phrase_glossary,
        phrase_field,
        entity_map_field,
        phrase_ignore_case,
        phrase_ner,
        position_fields,
        bigram_fields,
        sentence_fields,
    })
}
