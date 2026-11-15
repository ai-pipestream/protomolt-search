use super::*;
use crate::postings::{AnalyzedDoc, Bm25Index, Bm25Store};

/// One query term: its text, its token ordinals, and its spans.
type PositionedTerm<'a> = (&'a str, &'a [u32], &'a [(u32, u32)]);

fn positioned(terms: &[PositionedTerm<'_>]) -> AnalyzedField {
    AnalyzedField {
        sentences: None,
        terms: terms
            .iter()
            .map(|(t, ords, spans)| (t.to_string(), ords.len() as u32, spans.to_vec()))
            .collect(),
        length: terms.iter().map(|(_, ords, _)| ords.len() as u32).sum(),
        positions: Some(terms.iter().map(|(_, ords, _)| ords.to_vec()).collect()),
        ..Default::default()
    }
}

#[test]
fn bigrams_pair_only_ordinal_neighbors() {
    // "new york new jersey" with a dropped token between "new" and
    // "jersey": ordinals 0 1 2 (3 dropped) 4.
    let field = positioned(&[
        ("new", &[0, 2], &[(0, 3), (9, 12)]),
        ("york", &[1], &[(4, 8)]),
        ("jersey", &[4], &[(15, 21)]),
    ]);
    let bigrams = derive_bigrams(&field).unwrap();
    assert_eq!(
        bigrams.terms,
        vec![
            ("new york".to_string(), 1, vec![(0, 8)]),
            ("york new".to_string(), 1, vec![(4, 12)]),
        ],
        "new/jersey are not ordinal neighbors, so no bigram"
    );
    assert_eq!(bigrams.length, 2);
    assert!(bigrams.positions.is_none());
}

#[test]
fn bigrams_refuse_without_positions_and_never_guess_from_spans() {
    let mut field = positioned(&[("new", &[0], &[(0, 3)]), ("york", &[1], &[(4, 8)])]);
    field.positions = None;
    let error = derive_bigrams(&field).unwrap_err();
    assert!(error.contains("requires token positions"), "{error}");
}

#[test]
fn bigrams_repeat_within_a_document() {
    let field = positioned(&[
        ("hot", &[0, 2], &[(0, 3), (8, 11)]),
        ("dog", &[1, 3], &[(4, 7), (12, 15)]),
    ]);
    let bigrams = derive_bigrams(&field).unwrap();
    assert_eq!(
        bigrams.terms[0],
        ("hot dog".to_string(), 2, vec![(0, 7), (8, 15)])
    );
    assert_eq!(bigrams.terms[1], ("dog hot".to_string(), 1, vec![(4, 11)]));
}

fn store_with(docs: &[&[(&str, &[u32])]]) -> Bm25Store {
    let mut store = Bm25Store::with_fields(&["body"]).with_positions(&["body"]);
    for (i, doc) in docs.iter().enumerate() {
        let terms: DocTerms = doc
            .iter()
            .map(|(t, ords)| {
                (
                    t.to_string(),
                    ords.len() as u32,
                    ords.iter().map(|&o| (o * 4, o * 4 + 3)).collect(),
                )
            })
            .collect();
        let positions: DocPositions = doc.iter().map(|(_, ords)| ords.to_vec()).collect();
        let length = terms.iter().map(|(_, tf, _)| *tf).sum();
        store.add_document(
            i as u32,
            format!("doc {i}"),
            AnalyzedDoc::body_positioned(terms, positions, length),
        );
    }
    store
}

#[test]
fn window_is_ordered_and_exact_at_every_slop() {
    // doc 0: "a b c"        doc 1: "a x b c"       doc 2: "b a c"
    // doc 3: "a b x x c"    doc 4: "a a b"
    let store = store_with(&[
        &[("a", &[0]), ("b", &[1]), ("c", &[2])],
        &[("a", &[0]), ("x", &[1]), ("b", &[2]), ("c", &[3])],
        &[("b", &[0]), ("a", &[1]), ("c", &[2])],
        &[("a", &[0]), ("b", &[1]), ("x", &[2, 3]), ("c", &[4])],
        &[("a", &[0, 1]), ("b", &[2])],
    ]);
    let terms = vec!["a".to_string(), "b".to_string(), "c".to_string()];
    let abc = [0usize, 1, 2];
    let matches = |slop: u32| -> Vec<u32> {
        (0..5)
            .filter(|&d| phrase_matches(&store, &terms, &abc, slop, d))
            .collect()
    };
    assert_eq!(matches(0), vec![0], "exact adjacency");
    assert_eq!(matches(1), vec![0, 1], "one intervening token");
    assert_eq!(matches(2), vec![0, 1, 3], "two intervening tokens");
    assert!(
        !phrase_matches(&store, &terms, &[1, 0, 2], 0, 0),
        "order matters: b a c is not a b c"
    );
    assert!(
        phrase_matches(&store, &terms, &[1, 0, 2], 0, 2),
        "b a c matches doc 2 exactly"
    );
    // A repeated term needs two distinct tokens.
    assert!(phrase_matches(&store, &terms, &[0, 0, 1], 0, 4), "a a b");
    assert!(
        !phrase_matches(&store, &terms, &[0, 0, 1], 0, 0),
        "a b c has one 'a'"
    );
    // A term absent from the document never matches.
    let terms_z = vec!["a".to_string(), "z".to_string()];
    assert!(!phrase_matches(&store, &terms_z, &[0, 1], 100, 0));
}

#[test]
fn window_refuses_a_field_without_positions_instead_of_guessing() {
    let mut store = Bm25Store::with_fields(&["body"]);
    store.add_document(
        0,
        "new york".into(),
        AnalyzedDoc::body(
            vec![
                ("new".into(), 1, vec![(0, 3)]),
                ("york".into(), 1, vec![(4, 8)]),
            ],
            2,
        ),
    );
    let terms = vec!["new".to_string(), "york".to_string()];
    assert!(!store.has_positions());
    assert!(!phrase_matches(&store, &terms, &[0, 1], 0, 0));
}

#[test]
fn route_choice_names_what_is_missing() {
    assert_eq!(
        choose_route("body", 2, 0, true, false).unwrap(),
        PhraseRoute::BigramColumn("body.bigrams".into())
    );
    assert_eq!(
        choose_route("body", 2, 0, true, true).unwrap(),
        PhraseRoute::BigramColumn("body.bigrams".into())
    );
    assert_eq!(
        choose_route("body", 2, 1, true, true).unwrap(),
        PhraseRoute::Positions
    );
    assert_eq!(
        choose_route("body", 3, 0, true, true).unwrap(),
        PhraseRoute::Positions
    );
    let error = choose_route("body", 3, 0, true, false).unwrap_err();
    assert!(error.contains("answers only two-term phrases"), "{error}");
    let error = choose_route("body", 2, 1, true, false).unwrap_err();
    assert!(error.contains("slop needs ordinals"), "{error}");
    let error = choose_route("title", 2, 0, false, false).unwrap_err();
    assert!(error.contains("title.bigrams"), "{error}");
}

#[test]
fn query_sequence_follows_token_order_with_repeats() {
    let terms: DocTerms = vec![
        ("york".into(), 1, vec![(4, 8)]),
        ("new".into(), 2, vec![(0, 3), (9, 12)]),
    ];
    let positions: DocPositions = vec![vec![1], vec![0, 2]];
    assert_eq!(query_sequence(&terms, &positions).unwrap(), vec![1, 0, 1]);
}
