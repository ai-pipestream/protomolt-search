#[path = "turbo_native_parity_gate/glossary_support.rs"]
mod glossary_support;
#[path = "common/scratch.rs"]
mod scratch;
#[cfg(feature = "local-core")]
#[path = "turbo_native_parity_gate/static_embed.rs"]
mod static_embed;

use pipestream_search::analyzer::{
    analysis_fingerprint, analyze_document_native, analyze_document_turbo, body_spec,
    turbo_field_config, uax29_body_spec, SessionLayers,
};
use pipestream_search::pb::AnalysisSpec;
use pipestream_search::postings::AnalyzedField;
use protomolt_analyzer::{Glossary as NativeGlossary, GlossaryEntry as NativeEntry};

fn native(text: &str, spec: &AnalysisSpec) -> AnalyzedField {
    analyze_document_native(text, Some(spec))
        .unwrap()
        .fields
        .remove(0)
}

fn turbo(text: &str, spec: &AnalysisSpec) -> AnalyzedField {
    analyze_document_turbo(text, Some(spec), SessionLayers::default())
        .unwrap()
        .fields
        .remove(0)
}

fn porter_spec() -> AnalysisSpec {
    AnalysisSpec {
        tokenizer: pipestream_search::analyzer::TOKENIZER_WHITESPACE,
        term_vector_source: pipestream_search::analyzer::SOURCE_NORMALIZED_STEMS,
        char_filters: Vec::new(),
        ..uax29_body_spec()
    }
}

fn term(field: &AnalyzedField, name: &str) -> (u32, Vec<u32>, Vec<(u32, u32)>) {
    let index = field
        .terms
        .iter()
        .position(|(term, _, _)| term == name)
        .unwrap_or_else(|| panic!("missing term {name:?} in {:?}", field.terms));
    let (_, frequency, occurrences) = &field.terms[index];
    let positions = field.positions.as_ref().unwrap()[index].clone();
    (*frequency, positions, occurrences.clone())
}

fn native_entries(entries: &[(&str, &str)]) -> Vec<NativeEntry> {
    entries
        .iter()
        .map(|(id, term)| NativeEntry {
            id: id.to_string(),
            term: term.to_string(),
        })
        .collect()
}

#[test]
fn shared_uax29_baseline_is_bit_identical() {
    let spec = uax29_body_spec();
    for text in ["The quick brown fox", "Running café\nélève"] {
        let native = native(text, &spec);
        let turbo = turbo(text, &spec);
        assert_eq!(native.terms, turbo.terms, "terms: {text:?}");
        assert_eq!(native.length, turbo.length, "length: {text:?}");
        assert_eq!(native.positions, turbo.positions, "positions: {text:?}");
        assert_eq!(native.pos_lengths, turbo.pos_lengths, "lengths: {text:?}");
        for (_, _, occurrences) in &native.terms {
            for &(start, end) in occurrences {
                assert!(start < end && end <= text.len() as u32);
                assert!(start as usize <= text.len());
            }
        }
        assert!(native.sentences.is_some());
        assert!(turbo.sentences.is_some());
        native.check_sentences().unwrap();
        turbo.check_sentences().unwrap();
    }
    let field = native("The quick brown fox", &spec);
    assert_eq!(term(&field, "quick").0, 1);
    assert_eq!(term(&field, "brown").1, vec![2]);
    assert_eq!(term(&field, "fox").2, vec![(16, 19)]);
}

#[test]
fn empty_normalizing_tokens_move_positions_and_spans() {
    let spec = AnalysisSpec {
        tokenizer: pipestream_search::analyzer::TOKENIZER_WHITESPACE,
        ..uax29_body_spec()
    };
    let text = "new \u{00AD} york";
    let native_mid = native(text, &spec);
    let turbo_mid = turbo(text, &spec);
    assert_eq!(term(&native_mid, "new").1, vec![0]);
    assert_eq!(term(&native_mid, "york").1, vec![2]);
    assert_eq!(term(&native_mid, "york").2, vec![(7, 11)]);
    assert_eq!(term(&turbo_mid, "new").1, vec![0]);
    assert_eq!(term(&turbo_mid, "york").1, vec![1]);
    assert_eq!(term(&turbo_mid, "york").2, vec![(7, 11)]);

    let leading = "\u{00AD}abc";
    let native_lead = native(leading, &spec);
    let turbo_lead = turbo(leading, &spec);
    assert_eq!(term(&native_lead, "abc").2, vec![(0, 5)]);
    assert_eq!(term(&turbo_lead, "abc").2, vec![(2, 5)]);
}

#[test]
fn supplementary_scalars_use_different_porter_units() {
    let spec = porter_spec();
    let text = "ba😀ing";
    let native = native(text, &spec);
    let turbo = turbo(text, &spec);
    assert_eq!(term(&native, "ba😀e").0, 1);
    assert_eq!(term(&turbo, "ba😀").0, 1);
    assert_eq!(native.terms.len(), 1);
    assert_eq!(turbo.terms.len(), 1);
}

#[test]
fn letter_and_other_number_token_handling_is_recorded() {
    let spec = porter_spec();
    let native_porter = native("² Ⅷ", &spec);
    let turbo_porter = turbo("² Ⅷ", &spec);
    assert_eq!(term(&native_porter, "²").1, vec![0]);
    assert_eq!(term(&native_porter, "Ⅷ").1, vec![1]);
    assert_eq!(term(&turbo_porter, "²").1, vec![0]);
    assert_eq!(term(&turbo_porter, "ⅷ").1, vec![1]);
    assert_eq!(turbo_porter.length, 2);

    let uax = uax29_body_spec();
    let native_uax = native("² Ⅷ", &uax);
    let turbo_uax = turbo("² Ⅷ", &uax);
    assert_eq!(term(&native_uax, "²").1, vec![0]);
    assert_eq!(term(&native_uax, "ⅷ").1, vec![1]);
    assert_eq!(native_uax.length, 2);
    assert!(turbo_uax.terms.is_empty());
    assert_eq!(turbo_uax.length, 0);
}

#[test]
fn non_overlapping_glossary_spans_and_ids_agree() {
    let entries = [("ny", "new york"), ("york", "york")];
    let native = NativeGlossary::new(native_entries(&entries), false).unwrap();
    let native = native.matches("new york");
    assert_eq!(native.len(), 1);
    assert_eq!(native[0].id, "ny");
    assert_eq!((native[0].span.start, native[0].span.end), (0, 8));

    assert_eq!(glossary_support::outcome().baseline, (0, 0, 8));
}

#[test]
fn glossary_case_folding_and_overlap_tie_breaks_diverge() {
    let native = NativeGlossary::new(native_entries(&[("strasse", "STRASSE")]), true).unwrap();
    let native_match = native.matches("Straße");
    assert_eq!(native_match.len(), 1);
    assert_eq!(native_match[0].id, "strasse");
    assert_eq!(glossary_support::outcome().case_count, 0);

    let entries = [("zeta", "cat"), ("alpha", "cat")];
    let native = NativeGlossary::new(native_entries(&entries), false).unwrap();
    assert_eq!(native.matches("cat")[0].id, "alpha");
    assert_eq!(glossary_support::outcome().tie_entry, 0);
}

#[test]
fn glossary_fingerprints_have_distinct_algorithms_and_order_rules() {
    let entries = [("zeta", "dog"), ("alpha", "cat")];
    let native = NativeGlossary::new(native_entries(&entries), true).unwrap();
    let reversed = NativeGlossary::new(native_entries(&[entries[1], entries[0]]), true).unwrap();
    assert_eq!(native.fingerprint(), reversed.fingerprint());

    let observed = glossary_support::outcome();
    assert_ne!(observed.fingerprint, observed.reversed_fingerprint);
    assert_ne!(native.fingerprint(), observed.fingerprint);
    assert_ne!(native.fingerprint(), observed.reversed_fingerprint);
}

#[test]
fn persisted_analysis_fingerprints_are_not_interchangeable() {
    let spec = body_spec();
    let native = analysis_fingerprint(Some(&spec));
    let turbo = turbo_field_config(Some(&spec)).unwrap().fingerprint();
    assert_eq!(native, 6_191_274_826_942_720_707);
    assert_eq!(turbo.id64, 15_619_606_854_316_132_687);
    assert_ne!(native, turbo.id64);
    assert_eq!(turbo.to_hex().len(), 64);
}
