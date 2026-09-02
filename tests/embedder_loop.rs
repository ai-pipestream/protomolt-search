//! The full on-device loop: text -> `StaticEmbedder` vector -> embedded
//! ingest -> hybrid retrieval, over a SYNTHETIC Model2Vec table so the test
//! needs no model download and runs everywhere the workspace tests run.
//!
//! The synthetic vocabulary places finance words and food words in disjoint
//! subspaces, so the decisive assertion is semantic: the query "home loan"
//! shares NO token with any document, its BM25 leg is empty, and the
//! mortgage document must still win purely through the vector leg — the
//! capability an embedder adds to the embedded runtime. The second query
//! shares tokens AND meaning with the smoothie document and must carry
//! provenance from both legs.

mod common;

use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use common::fit_calibration;
use pipestream_search::analyzer::body_spec;
use pipestream_search::embedded::{EmbeddedSearch, EmbeddedSearchConfig, EmbeddedShardConfig};
use pipestream_search::pb::{
    AddDocumentsRequest, AddVectorsRequest, BroadcastCalibrationRequest, FusionMode, HybridLegOptions, HybridSearchRequest,
};
use protomolt_embedder::StaticEmbedder;

const DIM: usize = 32;
const BIT_WIDTH: usize = 4;

/// Vocabulary in id order. Finance words share dims 0..4, food words share
/// dims 16..20, and every word gets a private dim so no two rows are equal.
/// `[UNK]` is deliberately non-zero: the embedder must exclude it, and a bug
/// that pooled it would drag every vector toward the same junk direction.
const WORDS: [(&str, usize, usize); 9] = [
    ("[UNK]", 24, 28),
    ("mortgage", 0, 8),
    ("refinancing", 0, 9),
    ("paperwork", 0, 10),
    ("banana", 16, 11),
    ("smoothie", 16, 12),
    ("recipe", 16, 13),
    ("home", 0, 14),
    ("loan", 0, 15),
];

fn synthetic_model_dir() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let dir = std::env::temp_dir().join(format!(
        "protomolt-embedder-loop-{}-{nonce}",
        std::process::id()
    ));
    std::fs::create_dir_all(&dir).unwrap();

    let vocab: serde_json::Map<String, serde_json::Value> = WORDS
        .iter()
        .enumerate()
        .map(|(id, (word, _, _))| ((*word).to_string(), serde_json::json!(id)))
        .collect();
    let tokenizer = serde_json::json!({
        "normalizer": {"type": "BertNormalizer", "clean_text": true,
                        "handle_chinese_chars": true, "strip_accents": null, "lowercase": true},
        "pre_tokenizer": {"type": "BertPreTokenizer"},
        "added_tokens": [{"id": 0, "content": "[UNK]", "special": true}],
        "model": {"type": "WordPiece", "unk_token": "[UNK]",
                   "continuing_subword_prefix": "##", "max_input_chars_per_word": 100,
                   "vocab": vocab}
    });
    std::fs::write(dir.join("tokenizer.json"), tokenizer.to_string()).unwrap();

    let mut rows = Vec::with_capacity(WORDS.len() * DIM);
    for (_, base, private) in WORDS {
        let mut row = [0.0f32; DIM];
        for d in base..base + 4 {
            row[d] = 1.0;
        }
        row[private] = 0.5;
        rows.extend_from_slice(&row);
    }
    let header = format!(
        r#"{{"embeddings":{{"dtype":"F32","shape":[{},{DIM}],"data_offsets":[0,{}]}}}}"#,
        WORDS.len(),
        rows.len() * 4
    );
    let mut safetensors = Vec::new();
    safetensors.extend_from_slice(&(header.len() as u64).to_le_bytes());
    safetensors.extend_from_slice(header.as_bytes());
    for value in rows {
        safetensors.extend_from_slice(&value.to_le_bytes());
    }
    std::fs::write(dir.join("model.safetensors"), safetensors).unwrap();
    dir
}

#[tokio::test]
async fn embed_ingest_hybrid_loop() {
    let model_dir = synthetic_model_dir();
    let embedder = StaticEmbedder::load(&model_dir).unwrap();
    assert_eq!((embedder.rows(), embedder.dim()), (WORDS.len(), DIM));

    let documents = ["mortgage refinancing paperwork", "banana smoothie recipe"];
    let doc_vectors: Vec<Vec<f32>> = documents
        .iter()
        .map(|text| embedder.embed(text).expect("documents pool"))
        .collect();

    // Calibrate from the embedder's own outputs: for a static table the
    // coordinate distribution is a property of the model, so the fit sample
    // is every single-word vector plus the documents themselves.
    let mut sample = Vec::new();
    for (word, _, _) in &WORDS[1..] {
        sample.extend(embedder.embed(word).expect("vocabulary words pool"));
    }
    for vector in &doc_vectors {
        sample.extend_from_slice(vector);
    }
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &sample);

    let runtime = EmbeddedSearch::create(EmbeddedSearchConfig::single(
        EmbeddedShardConfig::in_memory(0),
    ))
    .await
    .unwrap();
    // Through the COORDINATOR, not shard_client: hybrid's vector leg fans
    // out from coordinator state, so calibration must be broadcast, not
    // set per shard behind the coordinator's back.
    let applied = runtime
        .broadcast_calibration(BroadcastCalibrationRequest {
            dim: DIM as u32,
            bit_width: BIT_WIDTH as u32,
            shift,
            scale,
        })
        .await
        .unwrap();
    assert!(applied.results.iter().all(|result| result.ok));
    runtime
        .add_documents(
            0,
            documents
                .iter()
                .map(|text| AddDocumentsRequest {
                    text: (*text).into(),
                    analysis: Some(body_spec()),
                    ..Default::default()
                })
                .collect(),
        )
        .await
        .unwrap();
    // Vectors stream in DOCUMENT ORDER: AddVectors assigns ids by position,
    // and this ordering is the entire doc/vector alignment contract.
    runtime
        .add_vectors(
            0,
            vec![AddVectorsRequest {
                vectors: doc_vectors.concat(),
                dim: DIM as u32,
            }],
        )
        .await
        .unwrap();

    // Semantic-only retrieval: "home loan" appears in NEITHER document, so
    // the BM25 leg is empty and only the vector leg can rank the mortgage
    // document first.
    // GLOBAL_RANK explicitly: the default cascade mode generates vector
    // candidates and RERANKS them by BM25, so a query whose lexical leg is
    // empty surfaces nothing there — the proto directs true fusion to
    // GLOBAL_RANK, and an SDK doing "semantic search with optional
    // keywords" must select it the same way.
    let legs = Some(HybridLegOptions {
        fusion_mode: FusionMode::GlobalRank as i32,
        ..Default::default()
    });
    let semantic = runtime
        .hybrid_search(HybridSearchRequest {
            text: "home loan".into(),
            vector: embedder.embed("home loan").unwrap(),
            k: 2,
            analysis: Some(body_spec()),
            legs: legs.clone(),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(semantic.hits.len(), 2);
    assert_eq!(semantic.hits[0].doc_id, 0, "mortgage doc must win on meaning");
    assert_eq!(semantic.hits[0].vector_rank, Some(1));
    assert_eq!(
        semantic.hits[0].bm25_rank, None,
        "no query token occurs in any document; a BM25 rank here means the \
         lexical leg matched something it should not have"
    );

    // Fused retrieval: "smoothie recipe" matches the food document by both
    // token and meaning, and the winning hit must carry provenance from
    // BOTH legs.
    let fused = runtime
        .hybrid_search(HybridSearchRequest {
            text: "smoothie recipe".into(),
            vector: embedder.embed("smoothie recipe").unwrap(),
            k: 2,
            analysis: Some(body_spec()),
            legs,
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(fused.hits[0].doc_id, 1);
    assert_eq!(fused.hits[0].vector_rank, Some(1));
    assert_eq!(fused.hits[0].bm25_rank, Some(1));

    drop(runtime);
    std::fs::remove_dir_all(&model_dir).ok();
}
