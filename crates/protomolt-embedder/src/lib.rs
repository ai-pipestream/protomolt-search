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
//! Further rules, mirrored from the OpenNLP-side Java implementation
//! (`StaticEmbeddingModel`), which serves as the second reference:
//!
//! - rows are resolved by piece STRING through the vocabulary map, never by
//!   trusting numeric id spaces beyond them: the vocabulary is validated to
//!   be a bijection of piece to row, and the added-token overlay must agree
//!   with it;
//! - an optional `weights` tensor scales each pooled row by its per-token
//!   weight, but the pooled sum still divides by the COUNT of pooled pieces,
//!   never the weight sum;
//! - loading is strict: every malformed-content path is a `LoadError`, sizes
//!   are bounded before allocation, and non-finite table values are rejected
//!   rather than allowed to poison every pooled vector silently.
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

/// Largest `tokenizer.json` and safetensors header accepted: both are parsed
/// fully in memory, so their size is bounded before allocation.
const MAX_TOKENIZER_BYTES: u64 = 64 * 1024 * 1024;
const MAX_SAFETENSORS_HEADER_BYTES: usize = 64 * 1024 * 1024;

/// A loaded Model2Vec model: WordPiece vocabulary plus the mmapped f32
/// table. The table never fully loads; pages are clean and evictable, which
/// is what a mobile host wants from a 100 MB-class asset.
///
/// Row resolution is string-keyed: `vocab` maps each piece to its table row,
/// and `[UNK]`/special rows are looked up through that map by content, so
/// nothing trusts the numeric id spaces of `tokenizer.json` and the table
/// beyond what the validated vocabulary bijection states.
pub struct StaticEmbedder {
    vocab: HashMap<String, u32>,
    unk_id: u32,
    special: Vec<u32>,
    max_word_chars: usize,
    table: memmap2::Mmap,
    weights: Option<Vec<f32>>,
    data_offset: usize,
    dim: usize,
    rows: usize,
}

impl StaticEmbedder {
    pub fn load(dir: &Path) -> Result<Self, LoadError> {
        let tok_path = dir.join("tokenizer.json");
        let tok_len = std::fs::metadata(&tok_path)
            .map_err(|e| LoadError::Io(format!("{}: {e}", tok_path.display())))?
            .len();
        if tok_len > MAX_TOKENIZER_BYTES {
            return Err(LoadError::Format(format!(
                "{}: {tok_len} bytes exceeds the {MAX_TOKENIZER_BYTES}-byte tokenizer limit",
                tok_path.display()
            )));
        }
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

        let vocab_json = model["vocab"]
            .as_object()
            .ok_or_else(|| LoadError::Format("vocab is not an object".into()))?;
        let mut vocab: HashMap<String, u32> = HashMap::with_capacity(vocab_json.len());
        for (piece, id) in vocab_json {
            let id = id
                .as_u64()
                .filter(|&v| v <= u64::from(u32::MAX))
                .ok_or_else(|| {
                    LoadError::Format(format!("vocab id for {piece:?} is not a u32"))
                })? as u32;
            vocab.insert(piece.clone(), id);
        }
        // The piece-to-row map must be a bijection: two pieces naming one row
        // would make added-token and unk resolution ambiguous.
        let mut seen_ids = std::collections::HashSet::with_capacity(vocab.len());
        for (piece, &id) in &vocab {
            if !seen_ids.insert(id) {
                return Err(LoadError::Format(format!(
                    "vocab id {id} is claimed by more than one piece ({piece:?})"
                )));
            }
        }
        let unk = model["unk_token"]
            .as_str()
            .ok_or_else(|| LoadError::Format("missing unk_token".into()))?;
        let unk_id = *vocab
            .get(unk)
            .ok_or_else(|| LoadError::Format(format!("unk token {unk:?} not in vocab")))?;
        let mut special = Vec::new();
        if let Some(added) = tok["added_tokens"].as_array() {
            for entry in added {
                let id = entry["id"]
                    .as_u64()
                    .filter(|&v| v <= u64::from(u32::MAX))
                    .ok_or_else(|| {
                        LoadError::Format("added_tokens entry with a missing or invalid id".into())
                    })? as u32;
                let content = entry["content"].as_str().ok_or_else(|| {
                    LoadError::Format("added_tokens entry without content".into())
                })?;
                // The overlay may alias a vocab piece, but it must not
                // contradict it: an id that disagrees with the vocabulary is
                // a packaging bug, not a variant to tolerate.
                if let Some(&vocab_id) = vocab.get(content) {
                    if vocab_id != id {
                        return Err(LoadError::Format(format!(
                            "added_tokens id {id} for {content:?} disagrees with vocab id {vocab_id}"
                        )));
                    }
                }
                if entry["special"].as_bool().unwrap_or(false) {
                    special.push(id);
                }
            }
        }
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
        if header_len > MAX_SAFETENSORS_HEADER_BYTES {
            return Err(LoadError::Format(format!(
                "safetensors header of {header_len} bytes exceeds the {MAX_SAFETENSORS_HEADER_BYTES}-byte limit"
            )));
        }
        let header: serde_json::Value = serde_json::from_slice(
            table
                .get(8..8 + header_len)
                .ok_or_else(|| LoadError::Format("safetensors header exceeds file".into()))?,
        )
        .map_err(|e| LoadError::Format(format!("safetensors header: {e}")))?;
        let header_end = 8 + header_len;
        let (shape, emb_offset, emb_bytes) =
            tensor_region(&header, "embeddings", 2, header_end, table.len())?;
        let rows = shape[0];
        let dim = shape[1];
        if rows == 0 || dim == 0 {
            return Err(LoadError::Format("embeddings shape has a zero dimension".into()));
        }
        if rows != vocab.len() {
            return Err(LoadError::Format(format!(
                "table rows {rows} != vocabulary size {}; table and tokenizer are from different models",
                vocab.len()
            )));
        }
        if let Some(&id) = vocab.values().find(|&&id| id as usize >= rows) {
            return Err(LoadError::Format(format!(
                "vocab id {id} names a row outside the {rows}-row table"
            )));
        }
        if let Some(&id) = special.iter().find(|&&id| id as usize >= rows) {
            return Err(LoadError::Format(format!(
                "special token id {id} names a row outside the {rows}-row table"
            )));
        }
        // A non-finite row would silently poison every pooled vector, so the
        // table is scanned once at load. This touches every page once; the
        // pages stay clean and evictable, so the resident cost is transient.
        for chunk in table[emb_offset..emb_offset + emb_bytes].chunks_exact(4) {
            if !f32::from_le_bytes(chunk.try_into().unwrap()).is_finite() {
                return Err(LoadError::Format(
                    "embeddings table holds a non-finite value".into(),
                ));
            }
        }
        let weights = match header.get("weights") {
            Some(tensor) if !tensor.is_null() => {
                let (wshape, woffset, wbytes) =
                    tensor_region(&header, "weights", 1, header_end, table.len())?;
                if wshape[0] != rows {
                    return Err(LoadError::Format(format!(
                        "weights length {} != table rows {rows}",
                        wshape[0]
                    )));
                }
                let mut values = Vec::with_capacity(wshape[0]);
                for chunk in table[woffset..woffset + wbytes].chunks_exact(4) {
                    let w = f32::from_le_bytes(chunk.try_into().unwrap());
                    if !w.is_finite() || w < 0.0 {
                        return Err(LoadError::Format(
                            "weights must be finite and non-negative".into(),
                        ));
                    }
                    values.push(w);
                }
                Some(values)
            }
            _ => None,
        };
        Ok(Self { vocab, unk_id, special, max_word_chars, table, weights, data_offset: emb_offset, dim, rows })
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
    ///
    /// With a `weights` tensor, each row is scaled by its per-token weight,
    /// but the sum still divides by the COUNT of pooled pieces, never the
    /// weight sum — the Model2Vec rule, pinned by
    /// `tests::embed_applies_per_token_weights_but_divides_by_token_count`.
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
            let w = self
                .weights
                .as_ref()
                .map_or(1.0, |ws| f64::from(ws[id as usize]));
            let at = self.data_offset + id as usize * stride;
            let row = &self.table[at..at + stride];
            for (a, chunk) in acc.iter_mut().zip(row.chunks_exact(4)) {
                *a += w * f64::from(f32::from_le_bytes(chunk.try_into().unwrap()));
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

/// Validates one safetensors tensor entry and returns its shape, its absolute
/// data offset in the file, and its byte length. Every bound is checked
/// before it is used: malformed shapes, offsets, and sizes are `LoadError`s,
/// never panics.
fn tensor_region(
    header: &serde_json::Value,
    name: &str,
    expected_dims: usize,
    header_end: usize,
    table_len: usize,
) -> Result<(Vec<usize>, usize, usize), LoadError> {
    let tensor = &header[name];
    if tensor.is_null() {
        return Err(LoadError::Format(format!("safetensors has no {name} tensor")));
    }
    if tensor["dtype"] != "F32" {
        return Err(LoadError::Format(format!("{name} dtype {} is not F32", tensor["dtype"])));
    }
    let shape: Vec<usize> = tensor["shape"]
        .as_array()
        .ok_or_else(|| LoadError::Format(format!("{name} shape missing")))?
        .iter()
        .map(|d| {
            d.as_u64()
                .filter(|&v| v <= usize::MAX as u64)
                .map(|v| v as usize)
                .ok_or_else(|| LoadError::Format(format!("{name} shape has an invalid dimension")))
        })
        .collect::<Result<_, _>>()?;
    if shape.len() != expected_dims {
        return Err(LoadError::Format(format!(
            "{name} shape {shape:?} is not {expected_dims}-dimensional"
        )));
    }
    let elements = shape
        .iter()
        .try_fold(1usize, |acc, &d| acc.checked_mul(d))
        .ok_or_else(|| LoadError::Format(format!("{name} shape overflows addressable memory")))?;
    let bytes = elements
        .checked_mul(4)
        .ok_or_else(|| LoadError::Format(format!("{name} byte size overflows addressable memory")))?;
    let offsets = tensor["data_offsets"]
        .as_array()
        .ok_or_else(|| LoadError::Format(format!("{name} data_offsets missing")))?;
    if offsets.len() != 2 {
        return Err(LoadError::Format(format!("{name} data_offsets is not [start, end]")));
    }
    let start = offsets[0]
        .as_u64()
        .and_then(|v| usize::try_from(v).ok())
        .ok_or_else(|| LoadError::Format(format!("{name} data start is invalid")))?;
    let end = offsets[1]
        .as_u64()
        .and_then(|v| usize::try_from(v).ok())
        .ok_or_else(|| LoadError::Format(format!("{name} data end is invalid")))?;
    if end < start || end - start != bytes {
        return Err(LoadError::Format(format!(
            "{name} data_offsets span {} bytes, shape requires {bytes}",
            end.saturating_sub(start)
        )));
    }
    let data_offset = header_end
        .checked_add(start)
        .ok_or_else(|| LoadError::Format(format!("{name} data offset overflows")))?;
    let data_end = data_offset
        .checked_add(bytes)
        .ok_or_else(|| LoadError::Format(format!("{name} data end overflows")))?;
    if data_end > table_len {
        return Err(LoadError::Format(format!("{name} data extends past end of file")));
    }
    Ok((shape, data_offset, bytes))
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
        if weights.is_some() {
            header.push_str(&format!(
                r#","weights":{{"dtype":"F32","shape":[{}],"data_offsets":[{},{}]}}"#,
                weights.unwrap().len(),
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
        assert!((v[0] - 0.6).abs() < 1e-7 && (v[1] - 0.8).abs() < 1e-7, "{v:?}");
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
        // One-dimensional shape: previously an index-out-of-bounds panic.
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
}
