use super::*;

fn folded() -> AnalysisSpec {
    AnalysisSpec {
        tokenizer: Tokenizer::Whitespace,
        stemmer: Stemmer::Porter,
        term_vector_mode: TermVectorMode::Full,
        term_vector_source: TermVectorSource::NormalizedStems,
        normalizers: vec![
            NormalizerStep::StripInvisible,
            NormalizerStep::Whitespace,
            NormalizerStep::AccentFold,
            NormalizerStep::FullCaseFold,
        ],
    }
}

#[test]
fn folded_terms_group_and_keep_utf8_offsets() {
    let doc = analyze("😀 Running Rodríguez running", &folded()).unwrap();
    assert_eq!(doc.offset_unit, OffsetUnit::Utf8Bytes);
    assert_eq!(
        doc.term_vectors,
        vec![
            TermVector {
                term: "😀".into(),
                frequency: 1,
                occurrences: vec![Span { start: 0, end: 4 }],
                positions: vec![0],
            },
            TermVector {
                term: "run".into(),
                frequency: 2,
                occurrences: vec![Span { start: 5, end: 12 }, Span { start: 24, end: 31 }],
                positions: vec![1, 3],
            },
            TermVector {
                term: "rodriguez".into(),
                frequency: 1,
                occurrences: vec![Span { start: 13, end: 23 }],
                positions: vec![2],
            },
        ]
    );
    assert_eq!(doc.length, 4);
}

#[test]
fn positions_count_dropped_tokens_so_spans_cannot_fake_adjacency() {
    // The soft hyphen token normalizes to nothing under STRIP_INVISIBLE
    // and is never emitted as a term, yet it occupies ordinal 1: "new"
    // and "york" are NOT adjacent here, while their character spans are
    // separated by exactly the whitespace a plain "new york" would have.
    let doc = analyze("new \u{00AD} york", &folded()).unwrap();
    let by_term: std::collections::HashMap<_, _> = doc
        .term_vectors
        .iter()
        .map(|vector| (vector.term.as_str(), vector.positions.clone()))
        .collect();
    assert_eq!(by_term["new"], vec![0]);
    assert_eq!(by_term["york"], vec![2]);
    assert_eq!(doc.tokens.len(), 3);
    assert_eq!(doc.length, 2, "the dropped token adds no length");

    let adjacent = analyze("new york", &folded()).unwrap();
    let york = adjacent
        .term_vectors
        .iter()
        .find(|vector| vector.term == "york")
        .unwrap();
    assert_eq!(york.positions, vec![1]);
}

#[test]
fn utf8_mode_changes_only_the_original_text_ruler() {
    let text = "A 😀 café 東京";
    let utf8 = analyze(text, &folded()).unwrap();
    let utf16 = analyze_with_offset_unit(text, &folded(), OffsetUnit::Utf16CodeUnits).unwrap();

    assert_eq!(utf8.offset_unit, OffsetUnit::Utf8Bytes);
    assert_eq!(utf16.offset_unit, OffsetUnit::Utf16CodeUnits);
    assert_eq!(utf8.tokens, utf16.tokens);
    assert_eq!(utf8.length, utf16.length);
    assert_eq!(
        utf8.term_vectors
            .iter()
            .map(|term| (&term.term, term.frequency))
            .collect::<Vec<_>>(),
        utf16
            .term_vectors
            .iter()
            .map(|term| (&term.term, term.frequency))
            .collect::<Vec<_>>()
    );
    assert_eq!(
        utf8.term_vectors
            .iter()
            .map(|term| term.occurrences[0])
            .collect::<Vec<_>>(),
        [
            Span { start: 0, end: 1 },
            Span { start: 2, end: 6 },
            Span { start: 7, end: 12 },
            Span { start: 13, end: 19 },
        ]
    );
}

#[test]
fn full_case_fold_is_non_turkic_and_expanding() {
    assert_eq!(
        normalize("Maße İ", &[NormalizerStep::FullCaseFold]),
        "masse i\u{307}"
    );
    assert_eq!(
        full_case_fold("\u{A7CE}\u{A7D2}\u{A7D4}\u{16EA0}\u{16EB8}"),
        "\u{A7CF}\u{A7D3}\u{A7D5}\u{16EBB}\u{16ED3}"
    );
}

#[test]
fn accent_fold_keeps_marks_on_non_folded_scripts() {
    assert_eq!(accent_fold("café ά й"), "cafe α и");
    assert_eq!(accent_fold("بَ אָ"), "بَ אָ");
}

#[test]
fn whitespace_matches_the_complete_unicode_property() {
    let separators: String = [
        '\u{0009}', '\u{000A}', '\u{000B}', '\u{000C}', '\u{000D}', '\u{0020}', '\u{0085}',
        '\u{00A0}', '\u{1680}', '\u{2000}', '\u{2001}', '\u{2002}', '\u{2003}', '\u{2004}',
        '\u{2005}', '\u{2006}', '\u{2007}', '\u{2008}', '\u{2009}', '\u{200A}', '\u{2028}',
        '\u{2029}', '\u{202F}', '\u{205F}', '\u{3000}',
    ]
    .into_iter()
    .collect();
    let text = format!("a{}b", separators);
    let doc = analyze(&text, &folded()).unwrap();
    assert_eq!(doc.tokens, ["a", "b"]);
}

#[test]
fn scoring_only_omits_offsets() {
    let mut spec = folded();
    spec.term_vector_mode = TermVectorMode::ScoringOnly;
    let doc = analyze("running runs run", &spec).unwrap();
    assert_eq!(doc.term_vectors[0].term, "run");
    assert_eq!(doc.term_vectors[0].frequency, 3);
    assert!(doc.term_vectors[0].occurrences.is_empty());
    assert!(doc.term_vectors[0].positions.is_empty());
}

#[test]
fn uax29_matches_words_ideographs_emoji_and_utf8_offsets() {
    let mut spec = folded();
    spec.tokenizer = Tokenizer::Uax29;
    spec.stemmer = Stemmer::None;
    spec.term_vector_source = TermVectorSource::Tokens;
    let doc = analyze("😀 U.S. 東京 カタカナ hot-dog!", &spec).unwrap();
    assert_eq!(
        doc.tokens,
        ["😀", "U.S", "東", "京", "カタカナ", "hot", "dog"]
    );
    assert_eq!(doc.term_vectors[0].occurrences, [Span { start: 0, end: 4 }]);
    assert_eq!(doc.term_vectors[1].term, "u.s");
}

#[test]
fn uax29_splits_long_tokens_without_splitting_surrogate_pairs() {
    let mut spec = folded();
    spec.tokenizer = Tokenizer::Uax29;
    spec.stemmer = Stemmer::None;
    spec.term_vector_source = TermVectorSource::Tokens;
    let text = format!("{}𐐀b", "a".repeat(254));
    let doc = analyze(&text, &spec).unwrap();
    assert_eq!(doc.tokens[0].encode_utf16().count(), 254);
    assert_eq!(doc.tokens[1], "𐐀b");
}

fn glossary() -> Glossary {
    Glossary::new(
        vec![
            GlossaryEntry {
                id: "nyc".into(),
                term: "New York City".into(),
            },
            GlossaryEntry {
                id: "new-york".into(),
                term: "New York".into(),
            },
            GlossaryEntry {
                id: "nyc".into(),
                term: "NYC".into(),
            },
            GlossaryEntry {
                id: "hot-dog".into(),
                term: "hot dog".into(),
            },
        ],
        true,
    )
    .unwrap()
}

#[test]
fn glossary_indexes_only_registered_overlapping_concepts() {
    let matches = glossary().index_matches("A NEW YORK CITY hot dog stand");
    assert_eq!(
        matches
            .iter()
            .map(|item| (item.id.as_str(), item.token_count))
            .collect::<Vec<_>>(),
        [("nyc", 3), ("new-york", 2), ("hot-dog", 2)]
    );
    assert_eq!(glossary().matches("New York City").len(), 1);
    assert_eq!(glossary().matches("New York City")[0].id, "nyc");
}

#[test]
fn glossary_preserves_original_utf8_spans_across_folding() {
    let glossary = Glossary::new(
        vec![GlossaryEntry {
            id: "street".into(),
            term: "STRASSE".into(),
        }],
        true,
    )
    .unwrap();
    let found = glossary.matches("😀 Straße!");
    assert_eq!(found[0].span, Span { start: 5, end: 12 });
    assert!(glossary.matches("Grosses").is_empty());
    assert_eq!(
        glossary.matches_with_offset_unit("😀 Straße!", OffsetUnit::Utf16CodeUnits)[0].span,
        Span { start: 3, end: 9 }
    );
}

#[test]
fn glossary_requires_word_boundaries() {
    let glossary = Glossary::new(
        vec![GlossaryEntry {
            id: "york".into(),
            term: "York".into(),
        }],
        true,
    )
    .unwrap();
    assert!(glossary.matches("New Yorkshire").is_empty());
    assert_eq!(glossary.matches("New York").len(), 1);
}

#[test]
fn glossary_keeps_every_concept_for_a_shared_normalized_surface() {
    let glossary = Glossary::new(
        vec![
            GlossaryEntry {
                id: "city".into(),
                term: "New York City".into(),
            },
            GlossaryEntry {
                id: "metro-area".into(),
                term: "NEW YORK CITY".into(),
            },
        ],
        true,
    )
    .unwrap();
    assert_eq!(
        glossary
            .index_matches("New York City")
            .into_iter()
            .map(|found| found.id)
            .collect::<Vec<_>>(),
        ["city", "metro-area"]
    );
}

#[test]
fn glossary_fingerprint_is_order_independent_and_alias_sensitive() {
    let mut entries = glossary().entries.clone();
    let forward = Glossary::new(entries.clone(), true).unwrap().fingerprint();
    entries.reverse();
    assert_eq!(
        forward,
        Glossary::new(entries.clone(), true).unwrap().fingerprint()
    );
    entries[0].term.push('!');
    assert_ne!(forward, Glossary::new(entries, true).unwrap().fingerprint());
    assert_ne!(phrase_posting_term("a:b"), phrase_posting_term("a") + ":b");
}
