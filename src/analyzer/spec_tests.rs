use super::*;
use crate::pb::AnalysisSpec;

#[test]
fn folded_and_cased_are_actually_different_analyzers() {
    let folded = analyzer_by_name("folded").expect("known name").unwrap();
    let cased = analyzer_by_name("cased").expect("known name").unwrap();
    assert_ne!(folded.term_vector_source, cased.term_vector_source);
    assert_eq!(folded.term_vector_source, SOURCE_NORMALIZED_STEMS);
    assert_eq!(cased.term_vector_source, SOURCE_STEMS);
    assert_eq!(folded.tokenizer, cased.tokenizer);
    assert_eq!(folded.stemmer, cased.stemmer);
    assert!(cased.char_filters.is_empty());
    assert!(folded.char_filters.contains(&CHAR_FILTER_FULL_CASE_FOLD));
}

#[test]
fn ingest_is_the_corpus_analyzer() {
    assert_eq!(analyzer_by_name("ingest").unwrap(), Some(body_spec()));
    assert_eq!(analyzer_by_name("folded").unwrap(), Some(body_spec()));
}

#[test]
fn server_means_no_spec() {
    assert_eq!(analyzer_by_name("server").unwrap(), None);
}

#[test]
fn an_unknown_analyzer_is_refused_and_lists_the_known_ones() {
    let err = analyzer_by_name("porter").expect_err("not a known name");
    assert!(err.contains("porter"), "{err}");
    for name in ANALYZER_NAMES {
        assert!(err.contains(name), "error should list {name}: {err}");
    }
}

#[test]
fn the_fingerprint_separates_the_analyzers_it_exists_to_separate() {
    let folded = analysis_fingerprint(Some(&body_spec()));
    let cased = analysis_fingerprint(Some(&cased_body_spec()));
    assert_ne!(folded, cased);
    assert_ne!(folded, 0);
    assert_ne!(cased, 0);
    assert_eq!(folded, analysis_fingerprint(Some(&body_spec())));
}

#[test]
fn an_absent_spec_is_unknown_not_a_default() {
    assert_eq!(analysis_fingerprint(None), 0);
}

#[test]
fn char_filter_order_and_arity_both_change_the_fingerprint() {
    let base = body_spec();
    let mut reordered = base.clone();
    reordered.char_filters.reverse();
    assert_ne!(
        analysis_fingerprint(Some(&base)),
        analysis_fingerprint(Some(&reordered)),
        "a reordered filter chain is a different analyzer"
    );
    let mut dropped = base.clone();
    dropped.char_filters.pop();
    assert_ne!(
        analysis_fingerprint(Some(&base)),
        analysis_fingerprint(Some(&dropped))
    );
    let mut one = base.clone();
    one.char_filters = vec![12];
    let mut two = base.clone();
    two.char_filters = vec![1, 2];
    assert_ne!(
        analysis_fingerprint(Some(&one)),
        analysis_fingerprint(Some(&two)),
        "[12] must not collide with [1, 2]"
    );
}

#[test]
fn every_field_of_the_spec_changes_the_fingerprint() {
    let base = body_spec();
    let fp = analysis_fingerprint(Some(&base));
    for (name, mutate) in [
        (
            "tokenizer",
            (|s: &mut AnalysisSpec| s.tokenizer += 1) as fn(&mut AnalysisSpec),
        ),
        ("stemmer", |s: &mut AnalysisSpec| s.stemmer += 1),
        ("term_vector_mode", |s: &mut AnalysisSpec| {
            s.term_vector_mode += 1
        }),
        ("term_vector_source", |s: &mut AnalysisSpec| {
            s.term_vector_source += 1
        }),
    ] {
        let mut changed = base.clone();
        mutate(&mut changed);
        assert_ne!(
            fp,
            analysis_fingerprint(Some(&changed)),
            "changing {name} must change the fingerprint"
        );
    }
}

#[test]
fn the_corpus_fingerprint_is_pinned() {
    assert_eq!(
        analysis_fingerprint(Some(&body_spec())),
        0x55eb_d3a6_febd_2ac3,
        "the corpus analyzer fingerprint changed"
    );
}

#[test]
fn the_corpus_analyzer_folds_accents_and_skips_the_pdf_steps() {
    let filters = body_spec().char_filters;
    assert!(filters.contains(&CHAR_FILTER_ACCENT_FOLD));
    assert!(!filters.contains(&CHAR_FILTER_DEHYPHENATE));
    assert!(!filters.contains(&CHAR_FILTER_NFKC));
    assert!(cased_body_spec().char_filters.is_empty());
    assert_ne!(
        analysis_fingerprint(Some(&body_spec())),
        analysis_fingerprint(Some(&cased_body_spec())),
    );
}
