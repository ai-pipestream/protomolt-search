use super::*;

#[test]
fn normalizer_matches_bert_semantics() {
    // Accents strip, case folds, NBSP becomes a space, soft hyphen and
    // zero-width space (Cf) vanish, CJK ideographs isolate, kana stay.
    assert_eq!(normalize("Café RÉSUMÉ"), "cafe resume");
    assert_eq!(normalize("a\u{00a0}b"), "a b");
    assert_eq!(normalize("so\u{00ad}ft ze\u{200b}ro"), "soft zero");
    assert_eq!(normalize("日本語テスト"), " 日  本  語 テスト");
    assert_eq!(normalize("İstanbul"), "istanbul");
}

#[test]
fn pre_tokenizer_isolates_punctuation() {
    assert_eq!(pre_tokenize("hello, world"), vec!["hello", ",", "world"]);
    assert_eq!(pre_tokenize("§1983"), vec!["§", "1983"]);
    assert_eq!(pre_tokenize("  spaced  out  "), vec!["spaced", "out"]);
    assert!(pre_tokenize("   ").is_empty());
}

#[test]
fn wordpiece_greedy_longest_match() {
    let vocab: HashMap<String, u32> =
        [("un", 10), ("##happi", 11), ("##ness", 12), ("unhappy", 13)]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();
    let mut out = Vec::new();
    wordpiece(&vocab, 1, 100, "unhappiness", &mut out);
    assert_eq!(out, vec![10, 11, 12]);
    // No match at some position discards the partial result entirely.
    out.clear();
    wordpiece(&vocab, 1, 100, "unhappix", &mut out);
    assert_eq!(out, vec![1]);
    // Over the length cap: a single UNK without attempting a match.
    out.clear();
    wordpiece(&vocab, 1, 3, "unhappiness", &mut out);
    assert_eq!(out, vec![1]);
}

/// Writes a synthetic model directory: tokenizer.json from the given
/// vocab/added-tokens, safetensors from the given rows (vocab id order)
/// plus an optional weights tensor appended after the table.
fn write_model(
    name: &str,
    vocab: &[(&str, u32)],
    added_tokens: serde_json::Value,
    rows: &[&[f32]],
    weights: Option<&[f32]>,
) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("pm-embedder-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let vocab_json: serde_json::Map<String, serde_json::Value> = vocab
        .iter()
        .map(|(k, v)| ((*k).to_string(), serde_json::json!(v)))
        .collect();
    let tokenizer = serde_json::json!({
        "normalizer": {"type": "BertNormalizer", "clean_text": true,
                        "handle_chinese_chars": true, "strip_accents": null, "lowercase": true},
        "pre_tokenizer": {"type": "BertPreTokenizer"},
        "added_tokens": added_tokens,
        "model": {"type": "WordPiece", "unk_token": "[UNK]",
                   "continuing_subword_prefix": "##", "max_input_chars_per_word": 100,
                   "vocab": vocab_json}
    });
    std::fs::write(dir.join("tokenizer.json"), tokenizer.to_string()).unwrap();

    let dim = rows[0].len();
    let table_bytes = rows.len() * dim * 4;
    let weights_bytes = weights.map_or(0, <[f32]>::len) * 4;
    let mut header = format!(
        r#"{{"embeddings":{{"dtype":"F32","shape":[{},{}],"data_offsets":[0,{}]}}"#,
        rows.len(),
        dim,
        table_bytes
    );
    if let Some(w) = weights {
        header.push_str(&format!(
            r#","weights":{{"dtype":"F32","shape":[{}],"data_offsets":[{},{}]}}"#,
            w.len(),
            table_bytes,
            table_bytes + weights_bytes
        ));
    }
    header.push('}');
    let mut st = Vec::new();
    st.extend_from_slice(&(header.len() as u64).to_le_bytes());
    st.extend_from_slice(header.as_bytes());
    for row in rows {
        for &v in *row {
            st.extend_from_slice(&v.to_le_bytes());
        }
    }
    if let Some(ws) = weights {
        for &w in ws {
            st.extend_from_slice(&w.to_le_bytes());
        }
    }
    std::fs::write(dir.join("model.safetensors"), st).unwrap();
    dir
}

fn basic_vocab() -> Vec<(&'static str, u32)> {
    vec![("[UNK]", 0), ("left", 1), ("right", 2)]
}

/// End-to-end over a synthetic two-word model written to a temp dir:
/// exercises the safetensors reader, UNK exclusion, and normalization
/// without any model download.
#[test]
fn embed_pools_and_excludes_unk() {
    // rows follow vocab id order; [UNK] deliberately NON-zero so a
    // pooling bug that includes it cannot pass.
    let dir = write_model(
        "pool",
        &basic_vocab(),
        serde_json::json!([{"id": 0, "content": "[UNK]", "special": true}]),
        &[&[9.0, 9.0], &[3.0, 0.0], &[0.0, 4.0]],
        None,
    );

    let e = StaticEmbedder::load(&dir).unwrap();
    assert_eq!((e.rows(), e.dim()), (3, 2));
    // mean([3,0],[0,4]) = [1.5,2] -> normalized [0.6,0.8]; the zzz word
    // becomes [UNK] and must not move the result.
    let v = e.embed("LEFT zzz right").unwrap();
    assert!(
        (v[0] - 0.6).abs() < 1e-7 && (v[1] - 0.8).abs() < 1e-7,
        "{v:?}"
    );
    assert_eq!(e.embed("zzz qqq"), None);
    assert_eq!(e.embed("   "), None);
    std::fs::remove_dir_all(&dir).ok();
}

/// The Model2Vec rule: per-token weights scale the pooled rows, but the
/// sum divides by the COUNT of pooled pieces, never the weight sum.
/// Mirrors the Java reference's
/// testEmbedAppliesPerTokenWeightsButDividesByTokenCount.
#[test]
fn embed_applies_per_token_weights_but_divides_by_token_count() {
    let dir = write_model(
        "weights",
        &basic_vocab(),
        serde_json::json!([{"id": 0, "content": "[UNK]", "special": true}]),
        &[&[0.0, 0.0], &[3.0, 0.0], &[0.0, 4.0]],
        Some(&[1.0, 2.0, 1.0]),
    );

    let e = StaticEmbedder::load(&dir).unwrap();
    // weighted sum = 2*[3,0] + 1*[0,4] = [6,4]; divide by count 2 -> [3,2]
    // -> normalized [3/sqrt(13), 2/sqrt(13)].
    let v = e.embed("left right").unwrap();
    let s = 13.0f32.sqrt();
    assert!(
        (v[0] - 3.0 / s).abs() < 1e-7 && (v[1] - 2.0 / s).abs() < 1e-7,
        "{v:?}"
    );
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn load_rejects_malformed_content_instead_of_panicking() {
    // One-dimensional shape is rejected as a format error without panicking.
    let dir = std::env::temp_dir().join(format!("pm-embedder-bad-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let tokenizer = serde_json::json!({
        "normalizer": {"type": "BertNormalizer", "clean_text": true,
                        "handle_chinese_chars": true, "strip_accents": null, "lowercase": true},
        "pre_tokenizer": {"type": "BertPreTokenizer"},
        "model": {"type": "WordPiece", "unk_token": "[UNK]",
                   "continuing_subword_prefix": "##", "vocab": {"[UNK]": 0, "a": 1}}
    });
    std::fs::write(dir.join("tokenizer.json"), tokenizer.to_string()).unwrap();
    let header = br#"{"embeddings":{"dtype":"F32","shape":[2],"data_offsets":[0,8]}}"#;
    let mut st = Vec::new();
    st.extend_from_slice(&(header.len() as u64).to_le_bytes());
    st.extend_from_slice(header);
    st.extend_from_slice(&[0u8; 8]);
    std::fs::write(dir.join("model.safetensors"), st).unwrap();
    assert!(matches!(
        StaticEmbedder::load(&dir),
        Err(LoadError::Format(_))
    ));
    std::fs::remove_dir_all(&dir).ok();

    // Duplicate vocab ids: two pieces claiming one row.
    let dir = write_model(
        "dup",
        &[("[UNK]", 0), ("left", 1), ("right", 1)],
        serde_json::json!([]),
        &[&[1.0], &[2.0]],
        None,
    );
    assert!(matches!(
        StaticEmbedder::load(&dir),
        Err(LoadError::Format(_))
    ));
    std::fs::remove_dir_all(&dir).ok();

    // An added-token overlay id that contradicts the vocabulary.
    let dir = write_model(
        "overlay",
        &basic_vocab(),
        serde_json::json!([{"id": 2, "content": "left", "special": true}]),
        &[&[9.0, 9.0], &[3.0, 0.0], &[0.0, 4.0]],
        None,
    );
    assert!(matches!(
        StaticEmbedder::load(&dir),
        Err(LoadError::Format(_))
    ));
    std::fs::remove_dir_all(&dir).ok();

    // A non-finite table value.
    let dir = write_model(
        "nan",
        &basic_vocab(),
        serde_json::json!([]),
        &[&[f32::NAN, 0.0], &[3.0, 0.0], &[0.0, 4.0]],
        None,
    );
    assert!(matches!(
        StaticEmbedder::load(&dir),
        Err(LoadError::Format(_))
    ));
    std::fs::remove_dir_all(&dir).ok();

    // A vocab id naming a row outside the table.
    let dir = write_model(
        "oor",
        &[("[UNK]", 0), ("left", 1), ("right", 7)],
        serde_json::json!([]),
        &[&[9.0, 9.0], &[3.0, 0.0], &[0.0, 4.0]],
        None,
    );
    assert!(matches!(
        StaticEmbedder::load(&dir),
        Err(LoadError::Format(_))
    ));
    std::fs::remove_dir_all(&dir).ok();
}
