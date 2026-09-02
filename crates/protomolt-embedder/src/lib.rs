//! Model2Vec static-embedding provider (spike).
//!
//! Turns text into the unit-length pooled vector a Model2Vec WordPiece table
//! defines: HF-tokenizers `BertNormalizer` + `BertPreTokenizer` semantics,
//! greedy longest-match WordPiece, mean pool, L2 normalize. There is no
//! neural runtime and no network: the model is an mmapped `[vocab x dim]`
//! f32 table plus its `tokenizer.json`, loaded from a directory in the
//! layout `deploy/court-e2e/model/download_model.sh` fetches.
//!
//! Contract notes that are NOT written down upstream and were learned by
//! differential test against the `model2vec` 0.9 reference implementation
//! (`tests/model2vec_conformance.rs`):
//!
//! - the saved tokenizer's post-processor injects `[CLS]`/`[SEP]`; pooling
//!   EXCLUDES them (the reference encodes without special tokens);
//! - `[UNK]` ids are DROPPED before pooling. The `[UNK]` row is not a zero
//!   vector (norm ~21 in potion-retrieval-32M), so this exclusion changes
//!   every vector for text containing out-of-vocabulary words — it is
//!   load-bearing, not cosmetic;
//! - the pooled mean is L2-normalized (the model's `Normalize` module);
//!   turbovec scores true dot products, so unit length is required, not
//!   optional;
//! - text that pools nothing (empty, whitespace, or all-`[UNK]`) has NO
//!   vector. The engine refuses zero vectors, so this surfaces as `None`
//!   here rather than a zero row there.
//!
//! Case mapping is deliberately `char::to_lowercase` (Rust std), NOT
//! icu_casemap: HF tokenizers is itself Rust and lowercases with std, so
//! std is oracle-exact. The lexical analyzer's fuller case folding serves a
//! different persisted contract (term identity); this crate serves vector
//! identity, and each must match its own oracle.

use std::collections::HashMap;
use std::fmt;
use std::path::Path;

use icu_normalizer::DecomposingNormalizerBorrowed;
use icu_properties::props::GeneralCategory;
use icu_properties::CodePointMapData;

/// Everything that can go wrong loading a model directory. String payloads
/// name the file and reason; a load error is a packaging problem, never a
/// per-text condition.
#[derive(Debug)]
pub enum LoadError {
    Io(String),
    Format(String),
}

impl fmt::Display for LoadError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            LoadError::Io(m) => write!(f, "embedder model io: {m}"),
            LoadError::Format(m) => write!(f, "embedder model format: {m}"),
        }
    }
}

impl std::error::Error for LoadError {}

fn is_control(c: char) -> bool {
    if c == '\t' || c == '\n' || c == '\r' {
        return false;
    }
    matches!(
        CodePointMapData::<GeneralCategory>::new().get(c),
        GeneralCategory::Control
            | GeneralCategory::Format
            | GeneralCategory::PrivateUse
            | GeneralCategory::Surrogate
            | GeneralCategory::Unassigned
    )
}

fn is_ws(c: char) -> bool {
    c == ' '
        || c == '\t'
        || c == '\n'
        || c == '\r'
        || CodePointMapData::<GeneralCategory>::new().get(c) == GeneralCategory::SpaceSeparator
}

fn is_punct(c: char) -> bool {
    c.is_ascii_punctuation()
        || matches!(
            CodePointMapData::<GeneralCategory>::new().get(c),
            GeneralCategory::ConnectorPunctuation
                | GeneralCategory::DashPunctuation
                | GeneralCategory::OpenPunctuation
                | GeneralCategory::ClosePunctuation
                | GeneralCategory::InitialPunctuation
                | GeneralCategory::FinalPunctuation
                | GeneralCategory::OtherPunctuation
        )
}

/// The BERT "Chinese character" ranges: CJK ideographs and compatibility
/// ideographs, NOT kana or Hangul — those stay inside their words, which is
/// why 日本語テスト splits the ideographs but keeps テスト whole.
fn is_cjk(c: char) -> bool {
    matches!(c as u32,
        0x4E00..=0x9FFF | 0x3400..=0x4DBF | 0x20000..=0x2A6DF | 0x2A700..=0x2B73F
        | 0x2B740..=0x2B81F | 0x2B820..=0x2CEAF | 0xF900..=0xFAFF | 0x2F800..=0x2FA1F)
}

/// `BertNormalizer { clean_text, handle_chinese_chars, strip_accents: null,
/// lowercase: true }` — the configuration saved in potion-retrieval-32M's
/// tokenizer.json. A null strip_accents resolves to the lowercase flag,
/// matching HF's `unwrap_or(self.lowercase)`.
pub fn normalize(text: &str) -> String {
    let mut cleaned = String::with_capacity(text.len());
    for c in text.chars() {
        if c == '\0' || c == '\u{fffd}' || is_control(c) {
            continue;
        }
        if is_ws(c) {
            cleaned.push(' ');
        } else if is_cjk(c) {
            cleaned.push(' ');
            cleaned.push(c);
            cleaned.push(' ');
        } else {
            cleaned.push(c);
        }
    }
    // Strip accents: NFD, then drop nonspacing marks — the same shape as
    // protomolt-analyzer's accent fold, against the same pinned ICU data.
    let decomposed = DecomposingNormalizerBorrowed::new_nfd().normalize(&cleaned);
    let categories = CodePointMapData::<GeneralCategory>::new();
    decomposed
        .chars()
        .filter(|c| categories.get(*c) != GeneralCategory::NonspacingMark)
        .flat_map(char::to_lowercase)
        .collect()
}

/// `BertPreTokenizer`: split on whitespace, then isolate every punctuation
/// character as its own pre-token.
pub fn pre_tokenize(normalized: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();
    for c in normalized.chars() {
        if c == ' ' {
            if !current.is_empty() {
                words.push(std::mem::take(&mut current));
            }
        } else if is_punct(c) {
            if !current.is_empty() {
                words.push(std::mem::take(&mut current));
            }
            words.push(c.to_string());
        } else {
            current.push(c);
        }
    }
    if !current.is_empty() {
        words.push(current);
    }
    words
}

/// Greedy longest-match WordPiece over one pre-token. A word longer than
/// `max_word_chars`, or any position with no vocabulary match, collapses the
/// WHOLE word to a single `[UNK]` — partial matches are discarded, per the
/// reference algorithm.
pub fn wordpiece(vocab: &HashMap<String, u32>, unk_id: u32, max_word_chars: usize, word: &str, out: &mut Vec<u32>) {
    let chars: Vec<char> = word.chars().collect();
    if chars.len() > max_word_chars {
        out.push(unk_id);
        return;
    }
    let mut ids = Vec::new();
    let mut start = 0;
    while start < chars.len() {
        let mut end = chars.len();
        let mut found = None;
        while end > start {
            let piece: String = chars[start..end].iter().collect();
            let key = if start > 0 { format!("##{piece}") } else { piece };
            if let Some(&id) = vocab.get(&key) {
                found = Some(id);
                break;
            }
            end -= 1;
        }
        match found {
            Some(id) => {
                ids.push(id);
                start = end;
            }
            None => {
                out.push(unk_id);
                return;
            }
        }
    }
    out.extend(ids);
}

/// A loaded Model2Vec model: WordPiece vocabulary plus the mmapped f32
/// table. The table never fully loads; pages are clean and evictable, which
/// is what a mobile host wants from a 100 MB-class asset.
pub struct StaticEmbedder {
    vocab: HashMap<String, u32>,
    unk_id: u32,
    special: Vec<u32>,
    max_word_chars: usize,
    table: memmap2::Mmap,
    data_offset: usize,
    dim: usize,
    rows: usize,
}

impl StaticEmbedder {
    pub fn load(dir: &Path) -> Result<Self, LoadError> {
        let tok_path = dir.join("tokenizer.json");
        let tok: serde_json::Value = serde_json::from_str(
            &std::fs::read_to_string(&tok_path)
                .map_err(|e| LoadError::Io(format!("{}: {e}", tok_path.display())))?,
        )
        .map_err(|e| LoadError::Format(format!("{}: {e}", tok_path.display())))?;

        // Refuse anything but the exact tokenizer family this implementation
        // reproduces. A different normalizer or prefix would not fail — it
        // would produce silently different vectors, so it must not load.
        let model = &tok["model"];
        if model["type"] != "WordPiece" {
            return Err(LoadError::Format(format!("tokenizer model {} is not WordPiece", model["type"])));
        }
        if model["continuing_subword_prefix"] != "##" {
            return Err(LoadError::Format("continuing_subword_prefix is not ##".into()));
        }
        let norm = &tok["normalizer"];
        if norm["type"] != "BertNormalizer"
            || norm["clean_text"] != true
            || norm["handle_chinese_chars"] != true
            || !norm["strip_accents"].is_null()
            || norm["lowercase"] != true
        {
            return Err(LoadError::Format(format!("unsupported normalizer {norm}")));
        }
        if tok["pre_tokenizer"]["type"] != "BertPreTokenizer" {
            return Err(LoadError::Format("pre_tokenizer is not BertPreTokenizer".into()));
        }

        let vocab: HashMap<String, u32> = model["vocab"]
            .as_object()
            .ok_or_else(|| LoadError::Format("vocab is not an object".into()))?
            .iter()
            .map(|(k, v)| (k.clone(), v.as_u64().unwrap_or(u64::MAX) as u32))
            .collect();
        let unk = model["unk_token"]
            .as_str()
            .ok_or_else(|| LoadError::Format("missing unk_token".into()))?;
        let unk_id = *vocab
            .get(unk)
            .ok_or_else(|| LoadError::Format(format!("unk token {unk:?} not in vocab")))?;
        let special = tok["added_tokens"]
            .as_array()
            .map(|a| {
                a.iter()
                    .filter(|t| t["special"].as_bool().unwrap_or(false))
                    .map(|t| t["id"].as_u64().unwrap_or(u64::MAX) as u32)
                    .collect()
            })
            .unwrap_or_default();
        let max_word_chars = model["max_input_chars_per_word"].as_u64().unwrap_or(100) as usize;

        let st_path = dir.join("model.safetensors");
        let file = std::fs::File::open(&st_path)
            .map_err(|e| LoadError::Io(format!("{}: {e}", st_path.display())))?;
        let table = unsafe { memmap2::Mmap::map(&file) }
            .map_err(|e| LoadError::Io(format!("{}: {e}", st_path.display())))?;
        if table.len() < 8 {
            return Err(LoadError::Format("safetensors shorter than its length header".into()));
        }
        let header_len = u64::from_le_bytes(table[0..8].try_into().unwrap()) as usize;
        let header: serde_json::Value = serde_json::from_slice(
            table
                .get(8..8 + header_len)
                .ok_or_else(|| LoadError::Format("safetensors header exceeds file".into()))?,
        )
        .map_err(|e| LoadError::Format(format!("safetensors header: {e}")))?;
        let emb = &header["embeddings"];
        if emb["dtype"] != "F32" {
            return Err(LoadError::Format(format!("embeddings dtype {} is not F32", emb["dtype"])));
        }
        let shape = emb["shape"]
            .as_array()
            .ok_or_else(|| LoadError::Format("embeddings shape missing".into()))?;
        let rows = shape[0].as_u64().unwrap_or(0) as usize;
        let dim = shape[1].as_u64().unwrap_or(0) as usize;
        let start = emb["data_offsets"][0].as_u64().unwrap_or(0) as usize;
        let data_offset = 8 + header_len + start;
        if rows != vocab.len() {
            return Err(LoadError::Format(format!(
                "table rows {rows} != vocabulary size {}; table and tokenizer are from different models",
                vocab.len()
            )));
        }
        if table.len() < data_offset + rows * dim * 4 {
            return Err(LoadError::Format("safetensors data shorter than shape".into()));
        }
        Ok(Self { vocab, unk_id, special, max_word_chars, table, data_offset, dim, rows })
    }

    pub fn dim(&self) -> usize {
        self.dim
    }

    pub fn rows(&self) -> usize {
        self.rows
    }

    /// Raw WordPiece ids including `[UNK]`s, for diagnostics and
    /// conformance tests. Pooling exclusions happen in [`Self::embed`].
    pub fn tokenize(&self, text: &str) -> Vec<u32> {
        let mut ids = Vec::new();
        for word in pre_tokenize(&normalize(text)) {
            wordpiece(&self.vocab, self.unk_id, self.max_word_chars, &word, &mut ids);
        }
        ids
    }

    /// The unit-length pooled vector, or `None` when nothing pools (empty,
    /// whitespace-only, or all-`[UNK]` text). Accumulates in f64 — the same
    /// choice as the engine's `embed_text` pooling — then narrows once.
    pub fn embed(&self, text: &str) -> Option<Vec<f32>> {
        let ids: Vec<u32> = self
            .tokenize(text)
            .into_iter()
            .filter(|id| *id != self.unk_id && !self.special.contains(id))
            .collect();
        if ids.is_empty() {
            return None;
        }
        let stride = self.dim * 4;
        let mut acc = vec![0.0f64; self.dim];
        for &id in &ids {
            let at = self.data_offset + id as usize * stride;
            let row = &self.table[at..at + stride];
            for (a, chunk) in acc.iter_mut().zip(row.chunks_exact(4)) {
                *a += f64::from(f32::from_le_bytes(chunk.try_into().unwrap()));
            }
        }
        let n = ids.len() as f64;
        let norm = acc.iter().map(|v| (v / n).powi(2)).sum::<f64>().sqrt();
        if norm == 0.0 {
            return None;
        }
        Some(acc.iter().map(|v| ((v / n) / norm) as f32).collect())
    }
}

#[cfg(test)]
mod tests {
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

    /// End-to-end over a synthetic two-word model written to a temp dir:
    /// exercises the safetensors reader, UNK exclusion, and normalization
    /// without any model download.
    #[test]
    fn embed_pools_and_excludes_unk() {
        let dir = std::env::temp_dir().join(format!("pm-embedder-test-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let tokenizer = serde_json::json!({
            "normalizer": {"type": "BertNormalizer", "clean_text": true,
                            "handle_chinese_chars": true, "strip_accents": null, "lowercase": true},
            "pre_tokenizer": {"type": "BertPreTokenizer"},
            "added_tokens": [{"id": 0, "content": "[UNK]", "special": true}],
            "model": {"type": "WordPiece", "unk_token": "[UNK]",
                       "continuing_subword_prefix": "##", "max_input_chars_per_word": 100,
                       "vocab": {"[UNK]": 0, "left": 1, "right": 2}}
        });
        std::fs::write(dir.join("tokenizer.json"), tokenizer.to_string()).unwrap();
        // rows follow vocab id order; [UNK] deliberately NON-zero so a
        // pooling bug that includes it cannot pass.
        let rows: [[f32; 2]; 3] = [[9.0, 9.0], [3.0, 0.0], [0.0, 4.0]];
        let header = br#"{"embeddings":{"dtype":"F32","shape":[3,2],"data_offsets":[0,24]}}"#;
        let mut st = Vec::new();
        st.extend_from_slice(&(header.len() as u64).to_le_bytes());
        st.extend_from_slice(header);
        for row in rows {
            for v in row {
                st.extend_from_slice(&v.to_le_bytes());
            }
        }
        std::fs::write(dir.join("model.safetensors"), st).unwrap();

        let e = StaticEmbedder::load(&dir).unwrap();
        assert_eq!((e.rows(), e.dim()), (3, 2));
        // mean([3,0],[0,4]) = [1.5,2] -> normalized [0.6,0.8]; the zzz word
        // becomes [UNK] and must not move the result.
        let v = e.embed("LEFT zzz right").unwrap();
        assert!((v[0] - 0.6).abs() < 1e-7 && (v[1] - 0.8).abs() < 1e-7, "{v:?}");
        assert_eq!(e.embed("zzz qqq"), None);
        assert_eq!(e.embed("   "), None);
        std::fs::remove_dir_all(&dir).ok();
    }
}
