use super::*;
use crate::domain_error::assert_status;

fn u8len(s: &str) -> u32 {
    s.len() as u32
}

/// UTF-8 byte span of the `n`th occurrence of `word` in `text`.
fn at(text: &str, word: &str, n: usize) -> (u32, u32) {
    let byte = text.match_indices(word).nth(n).expect("occurrence").0 as u32;
    (byte, byte + u8len(word))
}

fn sentences_of(text: &str) -> Vec<(u32, u32)> {
    let mut out = Vec::new();
    let mut cursor = 0u32;
    for line in text.split('\n') {
        let trimmed = line.trim();
        if !trimmed.is_empty() {
            let lead = u8len(&line[..line.find(trimmed).unwrap()]);
            out.push((cursor + lead, cursor + lead + u8len(trimmed)));
        }
        cursor += u8len(line) + 1;
    }
    out
}

#[test]
fn overlapping_and_touching_spans_merge() {
    let mut spans = vec![(10, 14), (3, 5), (12, 20), (20, 22), (30, 31)];
    merge_spans(&mut spans);
    assert_eq!(spans, vec![(3, 5), (10, 22), (30, 31)]);
}

#[test]
fn sentence_mode_picks_the_richest_sentences_in_text_order() {
    let text = "one two three\nfour court five\nsix court court seven\ncourt appeal here\nnothing";
    let sents = sentences_of(text);
    let occ = vec![
        (0, at(text, "court", 0)),
        (0, at(text, "court", 1)),
        (0, at(text, "court", 2)),
        (0, at(text, "court", 3)),
        (1, at(text, "appeal", 0)),
    ];
    let got = snippets(text, Some(&sents), &occ, Mode::Sentence, 2, 300).unwrap();
    // "court appeal here" has two distinct terms; "six court court
    // seven" has one term twice; they come back in text order.
    assert_eq!(got.len(), 2);
    assert_eq!(got[0].text, "six court court seven");
    assert_eq!(got[0].cut, Cut::Sentence);
    assert_eq!(got[0].sentence_index, Some(2));
    assert_eq!(got[1].sentence_index, Some(3));
    assert_eq!(
        got[0].highlights,
        vec![at(text, "court", 1), at(text, "court", 2)]
    );
    assert_eq!(got[1].text, "court appeal here");
    assert_eq!(
        got[1].highlights,
        vec![at(text, "court", 3), at(text, "appeal", 0)],
        "a space keeps neighbouring highlights apart"
    );
    assert_eq!((got[1].start, got[1].end), sents[3]);
    let one = snippets(text, Some(&sents), &occ, Mode::Sentence, 1, 300).unwrap();
    assert_eq!(one.len(), 1);
    assert_eq!(one[0].text, "court appeal here");
}

#[test]
fn a_long_sentence_is_cut_at_whitespace_around_its_first_highlight() {
    let mut text = String::new();
    for i in 0..40 {
        text.push_str(&format!("w{i:02}abcdef "));
    }
    text.push_str("court");
    for i in 0..40 {
        text.push_str(&format!(" x{i:02}abcdef"));
    }
    let sents = vec![(0, u8len(&text))];
    let occ = vec![(0, at(&text, "court", 0))];
    let got = snippets(&text, Some(&sents), &occ, Mode::Sentence, 3, 64).unwrap();
    assert_eq!(got.len(), 1);
    let s = &got[0];
    assert_eq!(s.cut, Cut::TruncatedSentence);
    assert!(s.end - s.start <= 64, "{}", s.end - s.start);
    assert!(s.text.contains("court"));
    // Whole tokens only: the slice starts and ends at token edges.
    assert!(!s.text.starts_with(' ') && !s.text.ends_with(' '));
    let before = &text[..text.find(&s.text).unwrap()];
    assert!(before.is_empty() || before.ends_with(' '));
    let after = &text[text.find(&s.text).unwrap() + s.text.len()..];
    assert!(after.is_empty() || after.starts_with(' '));
    assert_eq!(s.highlights, vec![at(&text, "court", 0)]);
}

#[test]
fn window_mode_ignores_sentences_and_clusters_nearby_highlights() {
    let text = "alpha court beta\ngamma delta court epsilon zeta eta theta iota kappa lambda mu nu xi omicron pi rho sigma tau upsilon phi chi psi omega appeal";
    let occ = vec![
        (0, at(text, "court", 0)),
        (0, at(text, "court", 1)),
        (1, at(text, "appeal", 0)),
    ];
    let got = snippets(text, None, &occ, Mode::Window, 3, 40).unwrap();
    assert!(got.iter().all(|s| s.cut == Cut::Window));
    assert!(got.iter().all(|s| s.sentence_index.is_none()));
    // The two courts sit within 40 bytes of each other: one window;
    // the appeal is far away: another.
    assert_eq!(got.len(), 2, "{got:?}");
    assert!(got[0].text.contains("alpha court beta"));
    assert_eq!(got[0].highlights.len(), 2);
    assert!(got[1].text.ends_with("appeal"));
    assert!(got[1].end - got[1].start <= 40);
    // Sentence mode without a table refuses by name.
    let error = snippets(text, None, &occ, Mode::Sentence, 3, 40).unwrap_err();
    assert!(error.message().contains("without stored sentence spans"));
}

#[test]
fn offsets_stay_utf8_and_slices_stay_on_character_boundaries() {
    let text = "😀 café court\nnaïve 𝔘nicode court";
    let sents = sentences_of(text);
    let occ = vec![(0, at(text, "court", 0)), (0, at(text, "court", 1))];
    let got = snippets(text, Some(&sents), &occ, Mode::Sentence, 3, 300).unwrap();
    assert_eq!(got.len(), 2);
    // 😀 is 4 UTF-8 bytes, é is 2, 𝔘 is 4: the offsets count UTF-8 bytes
    // of the original text.
    assert_eq!(got[0].text, "😀 café court");
    assert_eq!((got[0].start, got[0].end), (0, 16));
    assert_eq!(got[0].highlights, vec![(11, 16)]);
    assert_eq!(got[1].text, "naïve 𝔘nicode court");
    assert_eq!((got[1].start, got[1].end), (17, 40));
    assert_eq!(got[1].highlights, vec![(35, 40)]);
    // A span that splits a multi-byte UTF-8 scalar is refused, not sliced.
    let bad = vec![(0, (1, 2))];
    let error = snippets(text, None, &bad, Mode::Window, 3, 300).unwrap_err();
    assert!(error.message().contains("not a character boundary"));
    // A window cut near multi-byte characters still lands on a boundary,
    // and the text is the UTF-8 byte slice at the reported offsets.
    let win = snippets(text, None, &occ, Mode::Window, 3, 16).unwrap();
    assert!(!win.is_empty());
    for s in &win {
        assert_eq!(s.text, &text[s.start as usize..s.end as usize]);
    }
}

#[test]
fn an_occurrence_outside_every_sentence_is_a_contract_break() {
    let text = "one\ntwo court";
    let sents = vec![(0, 3)];
    let occ = vec![(0, at(text, "court", 0))];
    let error = snippets(text, Some(&sents), &occ, Mode::Sentence, 3, 300).unwrap_err();
    assert!(error
        .message()
        .contains("outside every stored sentence span"));
    assert_eq!(
        check_coverage(&sents, occ.iter().map(|o| o.1)),
        Err(at(text, "court", 0))
    );
    assert_eq!(
        check_coverage(&[(0, 3), (4, 13)], occ.iter().map(|o| o.1)),
        Ok(())
    );
    assert!(check_sentence_table(&[(0, 3), (4, 13)]).is_ok());
    assert!(check_sentence_table(&[(0, 3), (2, 13)]).is_err());
    assert!(check_sentence_table(&[(3, 3)]).is_err());
    let overrun = vec![(0, (0, 99))];
    let error = snippets(text, None, &overrun, Mode::Window, 3, 300).unwrap_err();
    assert!(error.message().contains("outruns the stored text"));
}

#[test]
fn a_highlight_wider_than_the_budget_keeps_its_whole_token() {
    let long = "x".repeat(50);
    let text = format!("start {long} end");
    let occ = vec![(0, at(&text, &long, 0))];
    let got = snippets(&text, None, &occ, Mode::Window, 1, 16).unwrap();
    assert_eq!(got.len(), 1);
    assert_eq!(got[0].text, long);
    assert_eq!(got[0].highlights, vec![at(&text, &long, 0)]);
}

#[test]
fn domain_error_is_compact_and_formats_readably() {
    assert!(size_of::<HighlightError>() <= 32);
    assert!(size_of::<Result<(), HighlightError>>() <= 32);
    let original = HighlightError::internal("original highlight error");
    let cloned = original.clone();
    assert_eq!(original, cloned);
    assert_eq!(original.message(), "original highlight error");
    assert_eq!(original.to_string(), "original highlight error");
    assert!(format!("{original:?}").contains("original highlight error"));
}

#[test]
fn plan_errors_preserve_every_code_and_message() {
    let spec = |mode, max_snippets, max_chars, fields: &[&str]| crate::pb::HighlightSpec {
        mode,
        max_snippets,
        max_chars,
        fields: fields.iter().map(|field| field.to_string()).collect(),
    };
    let cases = [
        (
            spec(999, 0, 0, &[]),
            "HighlightSpec.mode 999 is not a HighlightMode".to_string(),
        ),
        (
            spec(0, 65, 0, &[]),
            "HighlightSpec.max_snippets 65 exceeds the maximum 64".to_string(),
        ),
        (
            spec(0, 0, 15, &[]),
            "HighlightSpec.max_chars 15 is below the minimum 16".to_string(),
        ),
        (
            spec(0, 0, 4097, &[]),
            "HighlightSpec.max_chars 4097 exceeds the maximum 4096".to_string(),
        ),
        (
            spec(0, 0, 0, &[""]),
            "HighlightSpec.fields: a field name is empty".to_string(),
        ),
        (
            spec(0, 0, 0, &["body", "body"]),
            "HighlightSpec.fields: \"body\" repeats".to_string(),
        ),
    ];
    for (spec, message) in cases {
        let error = Plan::from_spec(&spec).unwrap_err();
        assert_eq!(error.message(), message);
        assert_status(error, tonic::Code::InvalidArgument, &message);
    }
}

#[test]
fn snippet_errors_preserve_every_code_and_message() {
    let missing = snippets("one two", None, &[(0, (0, 1))], Mode::Sentence, 1, 30).unwrap_err();
    assert_status(
        missing,
        tonic::Code::FailedPrecondition,
        "highlight: sentence mode over a field without stored sentence spans",
    );
    let overrun = snippets("one", None, &[(0, (0, 99))], Mode::Window, 1, 30).unwrap_err();
    assert_status(
        overrun,
        tonic::Code::Internal,
        "highlight: occurrence [0, 99) outruns the stored text of 3 UTF-8 bytes",
    );
    let split = snippets("😀", None, &[(0, (1, 2))], Mode::Window, 1, 30).unwrap_err();
    assert_status(
        split,
        tonic::Code::Internal,
        "highlight: UTF-8 byte offset 1 is not a character boundary of the stored text \
         (a multi-byte scalar is split, or the span outruns the text)",
    );
    let before = snippets(
        "one two",
        Some(&[(4, 7)]),
        &[(0, (0, 1))],
        Mode::Sentence,
        1,
        30,
    )
    .unwrap_err();
    assert_status(
        before,
        tonic::Code::Internal,
        "highlight: occurrence [0, 1) lies before every stored sentence span",
    );
    let outside = snippets(
        "one two",
        Some(&[(0, 3)]),
        &[(0, (4, 7))],
        Mode::Sentence,
        1,
        30,
    )
    .unwrap_err();
    assert_status(
        outside,
        tonic::Code::Internal,
        "highlight: occurrence [4, 7) lies outside every stored sentence span (nearest [0, 3))",
    );
}

#[test]
fn internal_window_errors_preserve_their_exact_messages() {
    for message in [
        "highlight: window start off a character boundary",
        "highlight: window end off a character boundary",
        "highlight: anchor off a character boundary",
        "highlight: anchor end off a character boundary",
    ] {
        assert_status(
            HighlightError::internal(message),
            tonic::Code::Internal,
            message,
        );
    }
}
