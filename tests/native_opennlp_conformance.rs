//! Differential oracle for the native analyzer. Run against the real sidecar:
//!
//! `OPENNLP_ANALYSIS_ADDR=http://127.0.0.1:59222 cargo test \
//!   --test native_opennlp_conformance -- --ignored --nocapture`

use pipestream_search::analyzer::{
    analyze_document, analyze_document_native, body_spec, cased_body_spec, CHAR_FILTER_ACCENT_FOLD,
    CHAR_FILTER_FULL_CASE_FOLD, STEMMER_NONE, TERM_VECTOR_MODE_FULL, TOKENIZER_WHITESPACE,
};
use pipestream_search::pb::AnalysisSpec;

#[tokio::test]
#[ignore = "requires OPENNLP_ANALYSIS_ADDR and scans every Unicode scalar"]
async fn native_matches_opennlp_contract() {
    let address = std::env::var("OPENNLP_ANALYSIS_ADDR")
        .expect("OPENNLP_ANALYSIS_ADDR is required for the ignored differential test");
    compare_product_analyzers(&address).await;
    compare_every_unicode_scalar(&address).await;
}

async fn compare_product_analyzers(address: &str) {
    let unicode_cases = [
        "😀 Running Rodríguez running",
        "Maße STRASSE İ i\u{307} ﬀ DESERET:𐐀𐐨",
        "Café café ά α й и بَ אָ",
        "word\u{00ad}join zero\u{200b}width keep\u{200d}joiner",
        "one\u{00a0}two\u{202f}three\u{3000}four",
        "punctuation,stays-attached (including) §1983",
        "ba😀ing",
    ];
    for spec in [body_spec(), cased_body_spec()] {
        for text in unicode_cases {
            let native = analyze_document_native(text, Some(&spec)).unwrap();
            let opennlp = analyze_document(address, text, Some(&spec)).await.unwrap();
            assert_eq!(native, opennlp, "analyzer mismatch for {text:?}");
        }

        // Exercise every Porter suffix against varied consonant/vowel shapes
        // in one request. This catches state-machine drift beyond a short
        // hand-selected stemming list while retaining the real sidecar as the
        // oracle.
        let prefixes = [
            "a", "be", "cat", "deny", "hop", "rate", "relate", "sensibil", "triplic", "vietnam",
            "predic", "conform", "radic", "differ", "vile", "analog",
        ];
        let suffixes = [
            "", "s", "sses", "ies", "eed", "ed", "ing", "y", "ational", "tional", "enci", "anci",
            "izer", "bli", "alli", "entli", "eli", "ousli", "ization", "ation", "ator", "alism",
            "iveness", "fulness", "ousness", "aliti", "iviti", "biliti", "logi", "icate", "ative",
            "alize", "iciti", "ical", "ful", "ness", "al", "ance", "ence", "er", "ic", "able",
            "ible", "ant", "ement", "ment", "ent", "ion", "ou", "ism", "ate", "iti", "ous", "ive",
            "ize", "e", "ll",
        ];
        let mut generated = String::new();
        for prefix in prefixes {
            for suffix in suffixes {
                if !generated.is_empty() {
                    generated.push(' ');
                }
                generated.push_str(prefix);
                generated.push_str(suffix);
            }
        }
        let native = analyze_document_native(&generated, Some(&spec)).unwrap();
        let opennlp = analyze_document(address, &generated, Some(&spec))
            .await
            .unwrap();
        assert_eq!(native, opennlp, "generated Porter corpus differs");
    }
}

async fn compare_every_unicode_scalar(address: &str) {
    let spec = AnalysisSpec {
        tokenizer: TOKENIZER_WHITESPACE,
        stemmer: STEMMER_NONE,
        term_vector_mode: TERM_VECTOR_MODE_FULL,
        term_vector_source: 1,
        char_filters: vec![CHAR_FILTER_ACCENT_FOLD, CHAR_FILTER_FULL_CASE_FOLD],
    };
    let mut text = String::new();
    let mut first = 0u32;
    let mut count = 0usize;
    let mut mismatches = Vec::new();
    for value in 0..=char::MAX as u32 {
        let Some(ch) = char::from_u32(value) else {
            continue;
        };
        if text.is_empty() {
            first = value;
        }
        text.push('x');
        text.push(ch);
        text.push('z');
        text.push(' ');
        count += 1;
        if count == 4096 {
            compare_chunk(address, &spec, &text, first, value, &mut mismatches).await;
            text.clear();
            count = 0;
        }
    }
    if !text.is_empty() {
        compare_chunk(
            address,
            &spec,
            &text,
            first,
            char::MAX as u32,
            &mut mismatches,
        )
        .await;
    }
    assert!(
        mismatches.is_empty(),
        "normalization mismatches:\n{}",
        mismatches.join("\n")
    );
}

async fn compare_chunk(
    address: &str,
    spec: &AnalysisSpec,
    text: &str,
    first: u32,
    last: u32,
    mismatches: &mut Vec<String>,
) {
    let native = analyze_document_native(text, Some(spec)).unwrap();
    let opennlp = analyze_document(address, text, Some(spec)).await.unwrap();
    if native == opennlp {
        return;
    }

    // Keep a failure useful: locate and report each offending scalar instead
    // of dumping two multi-megabyte analyzed documents.
    for value in first..=last {
        let Some(ch) = char::from_u32(value) else {
            continue;
        };
        let input = format!("x{ch}z");
        let native = analyze_document_native(&input, Some(spec)).unwrap();
        let opennlp = analyze_document(address, &input, Some(spec)).await.unwrap();
        if native != opennlp {
            mismatches.push(format!(
                "U+{value:04X} in {input:?}: native={native:?}, OpenNLP={opennlp:?}"
            ));
        }
    }
}
