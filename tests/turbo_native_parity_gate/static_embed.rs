use protomolt_embedder::StaticEmbedder;
use std::path::{Path, PathBuf};
use turbo_client::{EmbedPromptRole, LocalEmbedEngine, LocalEncoderOptions, MultiResOptions};

fn dependency_bundle() -> PathBuf {
    let cargo_home = std::env::var_os("CARGO_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".cargo")))
        .unwrap();
    let checkouts = cargo_home.join("git/checkouts");
    for entry in std::fs::read_dir(checkouts).unwrap() {
        let path = entry.unwrap().path();
        if path
            .file_name()
            .unwrap()
            .to_string_lossy()
            .starts_with("turboembed-")
        {
            let candidate = path.join("31e9857/testdata/tiny-static-bundle");
            if candidate.is_dir() {
                return candidate;
            }
        }
    }
    panic!("the pinned static bundle is missing from the dependency checkout")
}

fn f16_to_f32(bits: u16) -> f32 {
    let sign = if bits & 0x8000 == 0 { 1.0 } else { -1.0 };
    let exponent = (bits >> 10) & 0x1f;
    let fraction = bits & 0x03ff;
    match exponent {
        0 => sign * (fraction as f32) * 2.0f32.powi(-24),
        31 if fraction == 0 => sign * f32::INFINITY,
        31 => f32::NAN,
        _ => sign * (1.0 + (fraction as f32) / 1024.0) * 2.0f32.powi(exponent as i32 - 15),
    }
}

fn native_model(bundle: &Path, name: &str) -> PathBuf {
    let dir =
        std::env::temp_dir().join(format!("turbo-native-parity-{name}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::copy(bundle.join("tokenizer.json"), dir.join("tokenizer.json")).unwrap();
    let bytes = std::fs::read(bundle.join("weights/static.safetensors")).unwrap();
    let header_len = u64::from_le_bytes(bytes[..8].try_into().unwrap()) as usize;
    let header: serde_json::Value = serde_json::from_slice(&bytes[8..8 + header_len]).unwrap();
    let tensor = &header["embeddings"];
    assert_eq!(tensor["dtype"], "F16");
    let shape = tensor["shape"].as_array().unwrap();
    let elements = shape[0].as_u64().unwrap() as usize * shape[1].as_u64().unwrap() as usize;
    let payload = &bytes[8 + header_len..];
    let mut converted = Vec::with_capacity(elements * 4);
    for chunk in payload.as_chunks::<2>().0.iter().take(elements) {
        let value = f16_to_f32(u16::from_le_bytes(*chunk));
        converted.extend_from_slice(&value.to_le_bytes());
    }
    let converted_header = format!(
        "{{\"embeddings\":{{\"dtype\":\"F32\",\"shape\":[{},{}],\"data_offsets\":[0,{}]}}}}",
        shape[0],
        shape[1],
        elements * 4
    );
    let mut model = Vec::new();
    model.extend_from_slice(&(converted_header.len() as u64).to_le_bytes());
    model.extend_from_slice(converted_header.as_bytes());
    model.extend_from_slice(&converted);
    std::fs::write(dir.join("model.safetensors"), model).unwrap();
    dir
}

fn zero_bundle(bundle: &Path) -> (PathBuf, String) {
    let dir = std::env::temp_dir().join(format!("turbo-zero-parity-{}", std::process::id()));
    for subdir in ["weights", "quality", "reference"] {
        std::fs::create_dir_all(dir.join(subdir)).unwrap();
    }
    for (source, target) in [
        ("manifest.json", "manifest.json"),
        ("tokenizer.json", "tokenizer.json"),
        ("quality/texts.jsonl", "quality/texts.jsonl"),
        (
            "reference/reference.safetensors",
            "reference/reference.safetensors",
        ),
        ("weights/static.safetensors", "weights/static.safetensors"),
    ] {
        std::fs::copy(bundle.join(source), dir.join(target)).unwrap();
    }
    let tokenizer: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.join("tokenizer.json")).unwrap()).unwrap();
    let (piece, id) = tokenizer["model"]["vocab"]
        .as_object()
        .unwrap()
        .iter()
        .find(|(piece, _)| !piece.starts_with('['))
        .unwrap();
    let id = id.as_u64().unwrap() as usize;
    let header_len = u64::from_le_bytes(
        std::fs::read(dir.join("weights/static.safetensors")).unwrap()[..8]
            .try_into()
            .unwrap(),
    ) as usize;
    let dim = 16;
    let mut bytes = std::fs::read(dir.join("weights/static.safetensors")).unwrap();
    bytes[8 + header_len + id * dim * 2..8 + header_len + (id + 1) * dim * 2].fill(0);
    std::fs::write(dir.join("weights/static.safetensors"), &bytes).unwrap();
    let mut manifest: serde_json::Value =
        serde_json::from_slice(&std::fs::read(dir.join("manifest.json")).unwrap()).unwrap();
    for file in manifest["files"].as_array_mut().unwrap() {
        if file["path"] == "weights/static.safetensors" {
            file["sha256"] =
                serde_json::Value::String(pipestream_search::sha256::hex_digest(&bytes));
        }
    }
    std::fs::write(dir.join("manifest.json"), manifest.to_string()).unwrap();
    (dir, piece.clone())
}

fn vector_digest(vector: &[f32]) -> String {
    let mut bytes = Vec::with_capacity(vector.len() * 4);
    for value in vector {
        bytes.extend_from_slice(&value.to_le_bytes());
    }
    pipestream_search::sha256::hex_digest(&bytes)
}

fn compare_pooling(bundle: &Path) {
    let native_dir = native_model(bundle, "pooling");
    let native = StaticEmbedder::load(&native_dir).unwrap();
    let turbo =
        LocalEmbedEngine::open(bundle, &LocalEncoderOptions::default(), None, None).unwrap();
    let text = "The quick brown fox";
    let native_vector = native.embed(text).unwrap();
    let turbo_vectors = turbo
        .embed_texts_multires(&[text], EmbedPromptRole::None, &MultiResOptions::default())
        .unwrap();
    assert_eq!(native_vector.len(), 16);
    assert_eq!(turbo_vectors.full.data.len(), 16);
    let native_digest = vector_digest(&native_vector);
    let turbo_digest = vector_digest(&turbo_vectors.full.data);
    assert_eq!(
        native_digest,
        "b49df11743b22b80860901760f64dab78a7bea3d3028681a87b82142c2c30003"
    );
    assert_eq!(
        turbo_digest,
        "91102bf06956529a141d648017a9bb246a08fb5d65517bcb783e554af195a3ae"
    );
    assert_ne!(native_digest, turbo_digest);
    std::fs::remove_dir_all(native_dir).unwrap();
}

fn compare_empty_pooling(bundle: &Path) {
    let (zero_bundle, zero_text) = zero_bundle(bundle);
    let native_dir = native_model(&zero_bundle, "empty");
    let native = StaticEmbedder::load(&native_dir).unwrap();
    let turbo =
        LocalEmbedEngine::open(&zero_bundle, &LocalEncoderOptions::default(), None, None).unwrap();
    let texts = [zero_text.as_str()];
    for text in texts {
        assert_eq!(native.embed(text), None);
    }
    let empty_error = turbo
        .embed_texts_multires(&["   "], EmbedPromptRole::None, &MultiResOptions::default())
        .unwrap_err();
    assert!(empty_error.to_string().contains("neither may be 0"));
    let output = turbo
        .embed_texts_multires(&texts, EmbedPromptRole::None, &MultiResOptions::default())
        .unwrap();
    assert_eq!(output.full.rows as usize, texts.len());
    for row in output.full.data.chunks_exact(output.full.dim as usize) {
        assert!(row.iter().all(|value| *value == 0.0));
    }
    assert!(output.full.inv_norm.iter().all(|value| *value == 0.0));
    std::fs::remove_dir_all(native_dir).unwrap();
    std::fs::remove_dir_all(zero_bundle).unwrap();
}

#[test]
fn static_pooling_pins_f64_native_and_f32_turbo_outputs() {
    compare_pooling(&dependency_bundle());
}

#[test]
fn static_empty_pooling_is_none_natively_and_zero_rows_in_turbo() {
    compare_empty_pooling(&dependency_bundle());
}
