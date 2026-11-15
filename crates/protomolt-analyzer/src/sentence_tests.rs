use super::*;

fn spans(text: &str, unit: OffsetUnit) -> Vec<(u32, u32)> {
    split_sentences(text, unit)
        .into_iter()
        .map(|s| (s.start, s.end))
        .collect()
}

#[test]
fn lines_become_sentences_trimmed_and_blank_lines_yield_none() {
    let text = "  first line  \n\n\t \nsecond\r\nthird";
    assert_eq!(
        spans(text, OffsetUnit::Utf8Bytes),
        vec![(2, 12), (19, 25), (27, 32)]
    );
    assert_eq!(spans("", OffsetUnit::Utf8Bytes), vec![]);
    assert_eq!(spans(" \n \n", OffsetUnit::Utf8Bytes), vec![]);
    assert_eq!(spans("one", OffsetUnit::Utf8Bytes), vec![(0, 3)]);
}

#[test]
fn sentence_offsets_follow_the_requested_unit() {
    // The emoji is one code point, two UTF-16 units, four UTF-8 bytes.
    let text = "a 😀 b\ncafé";
    assert_eq!(
        spans(text, OffsetUnit::Utf16CodeUnits),
        vec![(0, 6), (7, 11)]
    );
    assert_eq!(spans(text, OffsetUnit::Utf8Bytes), vec![(0, 8), (9, 14)]);
}

#[test]
fn every_token_lies_inside_one_sentence() {
    let text = "the court held\n  that the claim  \nfails";
    let spec = AnalysisSpec {
        tokenizer: Tokenizer::Whitespace,
        stemmer: Stemmer::Porter,
        term_vector_mode: TermVectorMode::Full,
        term_vector_source: TermVectorSource::NormalizedStems,
        normalizers: vec![NormalizerStep::FullCaseFold],
    };
    let doc = analyze(text, &spec).unwrap();
    assert_eq!(doc.sentences.len(), 3);
    for vector in &doc.term_vectors {
        for span in &vector.occurrences {
            assert!(
                doc.sentences
                    .iter()
                    .any(|s| s.start <= span.start && span.end <= s.end),
                "{:?} at {span:?} lies in no sentence",
                vector.term
            );
        }
    }
}
