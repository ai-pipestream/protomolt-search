//! Differential oracle for the static embedder, mirroring
//! `tests/native_opennlp_conformance.rs` at the repo root. The checked-in
//! fixture holds token ids and vectors produced by the `model2vec` 0.9
//! reference implementation over potion-retrieval-32M (regenerate with
//! `tools/make_reference.py`). Run against a local model directory:
//!
//! `POTION_MODEL_DIR=/path/to/potion-retrieval-32M cargo test \
//!   --test model2vec_conformance -- --ignored --nocapture`
//!
//! Tokenization must match EXACTLY. Vectors match to 1e-6 max-abs, not
//! bit-exactly: the reference pools in numpy f32, this crate pools in f64,
//! and the ~1e-8 disagreement between those is float error, not drift.
//! Real drift (a normalizer, vocab, or pooling change) fails the id
//! comparison or exceeds the tolerance by orders of magnitude.

use protomolt_embedder::StaticEmbedder;
use std::path::Path;

#[test]
#[ignore = "requires POTION_MODEL_DIR pointing at a local potion-retrieval-32M download"]
fn matches_model2vec_reference() {
    let dir = std::env::var("POTION_MODEL_DIR")
        .expect("POTION_MODEL_DIR is required for the ignored conformance test");
    let embedder = StaticEmbedder::load(Path::new(&dir)).expect("load model dir");
    assert_eq!((embedder.rows(), embedder.dim()), (63_091, 512));

    let fixture = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/potion-retrieval-32M.reference.json"
    );
    let cases: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(fixture).unwrap()).unwrap();

    let mut worst = 0.0f64;
    for case in cases.as_array().unwrap() {
        let text = case["text"].as_str().unwrap();
        let want_ids: Vec<u32> = case["ids"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_u64().unwrap() as u32)
            .collect();
        // The fixture stores the id stream that reproduces encode():
        // specials and [UNK] already excluded. Apply the same exclusions.
        let got_ids: Vec<u32> = embedder
            .tokenize(text)
            .into_iter()
            .filter(|id| *id != 1 && !(0..=4).contains(id))
            .collect();
        assert_eq!(got_ids, want_ids, "tokenization mismatch for {text:?}");

        let want: Vec<f32> = case["vector"]
            .as_array()
            .unwrap()
            .iter()
            .map(|v| v.as_f64().unwrap() as f32)
            .collect();
        let got = embedder.embed(text).expect("reference cases all pool");
        assert_eq!(got.len(), want.len());
        let diff = got
            .iter()
            .zip(&want)
            .map(|(a, b)| (f64::from(*a) - f64::from(*b)).abs())
            .fold(0.0f64, f64::max);
        assert!(diff <= 1e-6, "vector drift {diff:.3e} for {text:?}");
        worst = worst.max(diff);
    }
    println!("worst max-abs difference vs reference: {worst:.3e}");
}
