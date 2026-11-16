use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};

use protomolt_embedder::StaticEmbedder;
use turbo_client::{EmbedPromptRole, LocalEmbedEngine, LocalEncoderOptions, MultiResOptions};

use crate::scratch::{scratch_path, ScratchPath};

const VOCAB_SIZE: usize = 8;
const DIM: usize = 16;
const MAX_SEQ: u32 = 8;

static NEXT_FIXTURE_ID: AtomicUsize = AtomicUsize::new(1);

struct TempFixtureDir {
    bundle_dir: PathBuf,
    native_dir: PathBuf,
    _root: ScratchPath,
}

impl TempFixtureDir {
    fn new(tag: &str) -> Self {
        let id = NEXT_FIXTURE_ID.fetch_add(1, Ordering::Relaxed);
        let root = scratch_path(&format!(
            "turbo-static-parity-{tag}-{}-{id}",
            std::process::id()
        ));
        let bundle_dir = root.join("bundle");
        let native_dir = root.join("native");
        std::fs::create_dir_all(bundle_dir.join("weights")).unwrap();
        std::fs::create_dir_all(bundle_dir.join("reference")).unwrap();
        std::fs::create_dir_all(&native_dir).unwrap();
        Self {
            bundle_dir,
            native_dir,
            _root: root,
        }
    }
}

fn encode_safetensors(header_json: &str, payload: &[u8]) -> Vec<u8> {
    let mut header = header_json.as_bytes().to_vec();
    while !(8 + header.len()).is_multiple_of(8) {
        header.push(b' ');
    }
    let mut out = Vec::with_capacity(8 + header.len() + payload.len());
    out.extend_from_slice(&(header.len() as u64).to_le_bytes());
    out.extend_from_slice(&header);
    out.extend_from_slice(payload);
    out
}

fn synthetic_tokenizer_bytes() -> Vec<u8> {
    let vocab = [
        ("[PAD]", 0u32),
        ("[UNK]", 1),
        ("the", 2),
        ("quick", 3),
        ("brown", 4),
        ("fox", 5),
        ("zero", 6),
        ("tail", 7),
    ];
    let vocab_map: serde_json::Map<String, serde_json::Value> = vocab
        .into_iter()
        .map(|(piece, id)| (piece.to_string(), serde_json::json!(id)))
        .collect();
    let doc = serde_json::json!({
        "normalizer": {
            "type": "BertNormalizer",
            "clean_text": true,
            "handle_chinese_chars": true,
            "strip_accents": null,
            "lowercase": true
        },
        "pre_tokenizer": { "type": "BertPreTokenizer" },
        "added_tokens": [
            {
                "id": 0,
                "content": "[PAD]",
                "special": true,
                "normalized": false,
                "lstrip": false,
                "rstrip": false,
                "single_word": false
            },
            {
                "id": 1,
                "content": "[UNK]",
                "special": true,
                "normalized": false,
                "lstrip": false,
                "rstrip": false,
                "single_word": false
            }
        ],
        "model": {
            "type": "WordPiece",
            "unk_token": "[UNK]",
            "continuing_subword_prefix": "##",
            "max_input_chars_per_word": 100,
            "vocab": vocab_map
        }
    });
    serde_json::to_vec(&doc).unwrap()
}

fn synthetic_embeddings_bytes() -> Vec<u8> {
    let mut payload = Vec::with_capacity(VOCAB_SIZE * DIM * 4);
    for row in 0..VOCAB_SIZE {
        for col in 0..DIM {
            let value = if matches!(row, 0 | 1 | 6) {
                0.0_f32
            } else {
                let num = (((row * 17 + col * 31 + 5) % 97) as i32 - 48) as f32;
                let den = (7 + ((row * 5 + col * 3) % 13)) as f32;
                num / den
            };
            payload.extend_from_slice(&value.to_le_bytes());
        }
    }
    let header = format!(
        r#"{{"embeddings":{{"dtype":"F32","shape":[{VOCAB_SIZE},{DIM}],"data_offsets":[0,{}]}}}}"#,
        payload.len()
    );
    encode_safetensors(&header, &payload)
}

fn synthetic_reference_bytes() -> Vec<u8> {
    let ids_bytes = (MAX_SEQ as usize) * 4;
    let len_end = ids_bytes + 4;
    let emb_end = len_end + DIM * 4;
    let mut payload = Vec::with_capacity(emb_end);
    for _ in 0..MAX_SEQ {
        payload.extend_from_slice(&5_i32.to_le_bytes());
    }
    payload.extend_from_slice(&(MAX_SEQ as i32).to_le_bytes());
    for _ in 0..DIM {
        payload.extend_from_slice(&0.0_f32.to_le_bytes());
    }
    let header = format!(
        r#"{{"embeddings":{{"dtype":"F32","shape":[1,{DIM}],"data_offsets":[{len_end},{emb_end}]}},"ids":{{"dtype":"I32","shape":[1,{MAX_SEQ}],"data_offsets":[0,{ids_bytes}]}},"lengths":{{"dtype":"I32","shape":[1],"data_offsets":[{ids_bytes},{len_end}]}}}}"#
    );
    encode_safetensors(&header, &payload)
}

fn manifest_tokenizer_spec() -> serde_json::Value {
    serde_json::json!({
        "file": "tokenizer.json",
        "normalizer": {
            "clean_text": true,
            "lowercase": true,
            "strip_accents": true,
            "split_cjk": true,
            "unicode_form": "UNICODE_NONE"
        },
        "wordpiece": {
            "continuing_prefix": "##",
            "max_chars_per_word": 100
        },
        "special_tokens": [
            { "role": "SPECIAL_PAD", "content": "[PAD]", "id": 0 },
            { "role": "SPECIAL_UNK", "content": "[UNK]", "id": 1 }
        ],
        "template": ["$TEXT"],
        "truncation": "TRUNCATE_RIGHT"
    })
}

fn manifest_produced_by() -> serde_json::Value {
    serde_json::json!({
        "tool": "parity-fixture",
        "tool_version": "0.1.0",
        "container": "host",
        "reproducible": true
    })
}

fn write_bundle_manifest(bundle_dir: &Path, files: &[(&str, &[u8])]) {
    let file_entries: Vec<serde_json::Value> = files
        .iter()
        .map(|(rel, bytes)| {
            serde_json::json!({
                "path": rel,
                "size": bytes.len(),
                "sha256": pipestream_search::sha256::hex_digest(bytes),
            })
        })
        .collect();
    let produced_by = manifest_produced_by();
    let manifest = serde_json::json!({
        "bundle_version": 1,
        "model": {
            "id": "parity-testdata/tiny-static-16",
            "revision": "1",
            "source": {
                "repository": "https://example.invalid/tiny-static-16",
                "commit": "0123456789abcdef0123456789abcdef01234567"
            },
            "license": "Apache-2.0"
        },
        "task": "TASK_EMBED",
        "embed": {
            "dim": DIM,
            "pooling": "POOLING_MEAN",
            "normalize": "NORMALIZE_L2",
            "max_seq": MAX_SEQ,
            "max_batch": 16
        },
        "tokenizer": manifest_tokenizer_spec(),
        "static_embedding": {
            "vocab_size": VOCAB_SIZE,
            "max_length": MAX_SEQ
        },
        "artifacts": [
            {
                "name": "table-f32",
                "format": "FORMAT_SAFETENSORS",
                "files": ["weights/static.safetensors"],
                "backends": ["cpu"],
                "graph_input": "INPUT_TOKEN_IDS",
                "graph_output": "OUTPUT_HIDDEN_STATES",
                "tensor_names": { "static_embeddings": "embeddings" },
                "produced_by": produced_by.clone()
            }
        ],
        "reference": {
            "file": "reference/reference.safetensors",
            "cases": [
                {
                    "text": "fox fox fox fox fox fox fox fox fox",
                    "prompt_role": "PROMPT_NONE"
                }
            ],
            "produced_by": produced_by
        },
        "files": file_entries
    });
    std::fs::write(
        bundle_dir.join("manifest.json"),
        serde_json::to_vec_pretty(&manifest).unwrap(),
    )
    .unwrap();
}

fn create_static_fixture(tag: &str) -> TempFixtureDir {
    let fx = TempFixtureDir::new(tag);
    let tokenizer = synthetic_tokenizer_bytes();
    let weights = synthetic_embeddings_bytes();
    let reference = synthetic_reference_bytes();
    std::fs::write(fx.bundle_dir.join("tokenizer.json"), &tokenizer).unwrap();
    std::fs::write(fx.bundle_dir.join("weights/static.safetensors"), &weights).unwrap();
    std::fs::write(
        fx.bundle_dir.join("reference/reference.safetensors"),
        &reference,
    )
    .unwrap();
    write_bundle_manifest(
        &fx.bundle_dir,
        &[
            ("reference/reference.safetensors", &reference),
            ("tokenizer.json", &tokenizer),
            ("weights/static.safetensors", &weights),
        ],
    );
    std::fs::write(fx.native_dir.join("tokenizer.json"), &tokenizer).unwrap();
    std::fs::write(fx.native_dir.join("model.safetensors"), &weights).unwrap();
    fx
}

fn vector_digest(vector: &[f32]) -> String {
    let mut bytes = Vec::with_capacity(vector.len() * 4);
    for value in vector {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    pipestream_search::sha256::hex_digest(&bytes)
}

#[test]
fn static_pooling_pins_f64_native_and_f32_turbo_outputs() {
    let fx = create_static_fixture("pooling");
    let native = StaticEmbedder::load(&fx.native_dir).unwrap();
    let turbo = LocalEmbedEngine::open(&fx.bundle_dir, &LocalEncoderOptions::default(), None, None)
        .unwrap();
    let text = "The quick brown fox";
    let native_vector = native.embed(text).unwrap();
    let turbo_vectors = turbo
        .embed_texts_multires(&[text], EmbedPromptRole::None, &MultiResOptions::default())
        .unwrap();
    let turbo_repeat = turbo
        .embed_texts_multires(&[text], EmbedPromptRole::None, &MultiResOptions::default())
        .unwrap();
    assert_eq!(native_vector.len(), DIM);
    assert_eq!(turbo_vectors.full.data.len(), DIM);
    let native_digest = vector_digest(&native_vector);
    let turbo_digest = vector_digest(&turbo_vectors.full.data);
    assert_eq!(turbo_digest, vector_digest(&turbo_repeat.full.data));
    assert_ne!(native_digest, turbo_digest);
    let mut dot = 0.0_f64;
    for (&lhs, &rhs) in native_vector.iter().zip(&turbo_vectors.full.data) {
        assert!((lhs - rhs).abs() < 1e-6, "{lhs} vs {rhs}");
        dot += f64::from(lhs) * f64::from(rhs);
    }
    assert!((dot - 1.0).abs() < 1e-6, "cosine {dot}");
}

#[test]
fn static_empty_pooling_is_none_natively_and_zero_rows_in_turbo() {
    let fx = create_static_fixture("empty");
    let native = StaticEmbedder::load(&fx.native_dir).unwrap();
    let turbo = LocalEmbedEngine::open(&fx.bundle_dir, &LocalEncoderOptions::default(), None, None)
        .unwrap();
    assert_eq!(native.embed("zero"), None);
    assert_eq!(native.embed("   "), None);
    let empty_error = turbo
        .embed_texts_multires(&["   "], EmbedPromptRole::None, &MultiResOptions::default())
        .unwrap_err();
    assert!(empty_error.to_string().contains("neither may be 0"));
    let output = turbo
        .embed_texts_multires(
            &["zero"],
            EmbedPromptRole::None,
            &MultiResOptions::default(),
        )
        .unwrap();
    assert_eq!(output.full.rows, 1);
    assert_eq!(output.full.data.len(), DIM);
    assert!(output.full.data.iter().all(|value| *value == 0.0));
    assert!(output.full.inv_norm.iter().all(|value| *value == 0.0));
}
