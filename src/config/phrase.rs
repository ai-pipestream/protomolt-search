//! The phrase glossary field and the proximity payloads declared over the
//! BM25 field table. Each is an explicit storage declaration: a positional
//! field must be in the table, and a bigram column is a declared table entry
//! named after its source, never an implicit extra field.

use std::path::PathBuf;

use super::columns::{first_repeat, ColumnFields};
use super::file::FileConfig;
use super::option::Options;

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

fn check_glossary(phrases: &PhraseFields, columns: &ColumnFields) -> Result<(), String> {
    let field = &phrases.phrase_field;
    if phrases.phrase_glossary.is_none() {
        if phrases.entity_map_field.is_some() || phrases.phrase_ner {
            return Err(
                "--entity-map-field and --phrase-ner require --phrase-glossary".to_string(),
            );
        }
        return Ok(());
    }
    if field == "body" || !columns.bm25_fields.contains(field) {
        return Err(format!(
            "phrase glossary requires phrase field {field:?} as a non-body entry in --bm25-fields"
        ));
    }
    if let Some(entity) = &phrases.entity_map_field {
        if !columns.map_facet_fields.contains(entity) {
            return Err(format!(
                "entity map field {entity:?} must be declared in --map-facet-fields"
            ));
        }
    }
    if phrases.phrase_ner && phrases.entity_map_field.is_none() {
        return Err("--phrase-ner requires --entity-map-field".to_string());
    }
    Ok(())
}

/// Snippets are cut from stored text, and only the body's text is stored.
fn check_sentences(phrases: &PhraseFields, bm25: &[String]) -> Result<(), String> {
    if let Some(name) = first_repeat(&phrases.sentence_fields) {
        return Err(format!(
            "sentence field {name:?} repeats in --sentence-fields"
        ));
    }
    for name in &phrases.sentence_fields {
        if !bm25.contains(name) {
            return Err(format!(
                "sentence field {name:?} must be declared in --bm25-fields"
            ));
        }
        if name != "body" {
            return Err(format!(
                "sentence field {name:?}: snippets are cut from stored text, and only the \
                 body's text is stored; --sentence-fields accepts body only"
            ));
        }
    }
    Ok(())
}

fn check_positions(phrases: &PhraseFields, bm25: &[String]) -> Result<(), String> {
    if let Some(name) = first_repeat(&phrases.position_fields) {
        return Err(format!(
            "positional field {name:?} repeats in --position-fields"
        ));
    }
    for name in &phrases.position_fields {
        if !bm25.contains(name) {
            return Err(format!(
                "positional field {name:?} must be declared in --bm25-fields"
            ));
        }
        if phrases.phrase_glossary.is_some() && *name == phrases.phrase_field {
            return Err(format!(
                "phrase field {name:?} holds glossary concepts, not tokens; it cannot keep \
                 token positions"
            ));
        }
    }
    Ok(())
}

fn check_bigram(source: &str, phrases: &PhraseFields, bm25: &[String]) -> Result<(), String> {
    let derived = crate::proximity::bigram_field_name(source);
    let phrase_field = &phrases.phrase_field;
    if !bm25.iter().any(|name| name == source) {
        return Err(format!(
            "bigram source field {source:?} must be declared in --bm25-fields"
        ));
    }
    if !bm25.contains(&derived) {
        return Err(format!(
            "bigram column {derived:?} (derived from {source:?}) must be declared in --bm25-fields"
        ));
    }
    if crate::proximity::bigram_source(source).is_some() {
        return Err(format!(
            "bigram source {source:?} is itself a bigram column; columns derive from analyzed \
             fields only"
        ));
    }
    if phrases.position_fields.contains(&derived) {
        return Err(format!(
            "bigram column {derived:?} cannot keep token positions; it is a term column derived \
             from {source:?}"
        ));
    }
    if phrases.phrase_glossary.is_some() && (source == phrase_field || &derived == phrase_field) {
        return Err(format!(
            "bigram source {source:?} and the phrase field {phrase_field:?} must be distinct"
        ));
    }
    Ok(())
}

fn check_bigrams(phrases: &PhraseFields, bm25: &[String]) -> Result<(), String> {
    if let Some(source) = first_repeat(&phrases.bigram_fields) {
        return Err(format!(
            "bigram source {source:?} repeats in --bigram-fields"
        ));
    }
    phrases
        .bigram_fields
        .iter()
        .try_for_each(|source| check_bigram(source, phrases, bm25))
}

/// Phrase and proximity declarations, each checked against the field table.
pub(super) fn parse_phrase_fields(
    o: Options,
    file: &FileConfig,
    columns: &ColumnFields,
) -> Result<PhraseFields, String> {
    let phrases = PhraseFields {
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
    check_glossary(&phrases, columns)?;
    check_sentences(&phrases, &columns.bm25_fields)?;
    check_positions(&phrases, &columns.bm25_fields)?;
    check_bigrams(&phrases, &columns.bm25_fields)?;
    Ok(phrases)
}
