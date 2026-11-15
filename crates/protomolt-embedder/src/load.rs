//! Loading a model directory: the tokenizer family check, the vocabulary
//! bijection, the added-token overlay, and the mmapped safetensors table.

use std::collections::{HashMap, HashSet};
use std::path::Path;

use super::{LoadError, StaticEmbedder};

/// Largest `tokenizer.json` and safetensors header accepted: both are parsed
/// fully in memory, so their size is bounded before allocation.
const MAX_TOKENIZER_BYTES: u64 = 64 * 1024 * 1024;
const MAX_SAFETENSORS_HEADER_BYTES: usize = 64 * 1024 * 1024;

fn io_error(path: &Path, e: impl std::fmt::Display) -> LoadError {
    LoadError::Io(format!("{}: {e}", path.display()))
}

impl StaticEmbedder {
    pub fn load(dir: &Path) -> Result<Self, LoadError> {
        let tok = read_tokenizer(&dir.join("tokenizer.json"))?;
        check_tokenizer_family(&tok)?;
        let model = &tok["model"];
        let vocab = read_vocab(model)?;
        let unk = model["unk_token"]
            .as_str()
            .ok_or_else(|| LoadError::Format("missing unk_token".into()))?;
        let unk_id = *vocab
            .get(unk)
            .ok_or_else(|| LoadError::Format(format!("unk token {unk:?} not in vocab")))?;
        let special = added_specials(&tok, &vocab)?;
        let max_word_chars = model["max_input_chars_per_word"].as_u64().unwrap_or(100) as usize;
        let table = map_table(&dir.join("model.safetensors"))?;
        let (header, header_end) = read_header(&table)?;
        let (shape, emb_offset, emb_bytes) =
            tensor_region(&header, "embeddings", 2, header_end, table.len())?;
        let (rows, dim) = (shape[0], shape[1]);
        check_table_shape(rows, dim, &vocab, &special)?;
        // A non-finite row would silently poison every pooled vector, so the
        // table is scanned once at load. This touches every page once; the
        // pages stay clean and evictable, so the resident cost is transient.
        for chunk in table[emb_offset..emb_offset + emb_bytes].as_chunks::<4>().0 {
            if !f32::from_le_bytes(*chunk).is_finite() {
                return Err(LoadError::Format(
                    "embeddings table holds a non-finite value".into(),
                ));
            }
        }
        let weights = read_weights(&header, header_end, &table, rows)?;
        Ok(Self {
            vocab,
            unk_id,
            special,
            max_word_chars,
            table,
            weights,
            data_offset: emb_offset,
            dim,
            rows,
        })
    }
}

fn read_tokenizer(path: &Path) -> Result<serde_json::Value, LoadError> {
    let len = std::fs::metadata(path)
        .map_err(|e| io_error(path, e))?
        .len();
    if len > MAX_TOKENIZER_BYTES {
        return Err(LoadError::Format(format!(
            "{}: {len} bytes exceeds the {MAX_TOKENIZER_BYTES}-byte tokenizer limit",
            path.display()
        )));
    }
    let text = std::fs::read_to_string(path).map_err(|e| io_error(path, e))?;
    serde_json::from_str(&text).map_err(|e| LoadError::Format(format!("{}: {e}", path.display())))
}

/// Refuse anything but the exact tokenizer family this implementation
/// reproduces. A different normalizer or prefix would not fail — it would
/// produce silently different vectors, so it must not load.
fn check_tokenizer_family(tok: &serde_json::Value) -> Result<(), LoadError> {
    let model = &tok["model"];
    if model["type"] != "WordPiece" {
        return Err(LoadError::Format(format!(
            "tokenizer model {} is not WordPiece",
            model["type"]
        )));
    }
    if model["continuing_subword_prefix"] != "##" {
        return Err(LoadError::Format(
            "continuing_subword_prefix is not ##".into(),
        ));
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
        return Err(LoadError::Format(
            "pre_tokenizer is not BertPreTokenizer".into(),
        ));
    }
    Ok(())
}

fn as_u32(value: &serde_json::Value) -> Option<u32> {
    value
        .as_u64()
        .filter(|&v| v <= u64::from(u32::MAX))
        .map(|v| v as u32)
}

/// The piece-to-row map, which must be a bijection: two pieces naming one
/// row would make added-token and unk resolution ambiguous.
fn read_vocab(model: &serde_json::Value) -> Result<HashMap<String, u32>, LoadError> {
    let vocab_json = model["vocab"]
        .as_object()
        .ok_or_else(|| LoadError::Format("vocab is not an object".into()))?;
    let mut vocab: HashMap<String, u32> = HashMap::with_capacity(vocab_json.len());
    for (piece, id) in vocab_json {
        let id = as_u32(id)
            .ok_or_else(|| LoadError::Format(format!("vocab id for {piece:?} is not a u32")))?;
        vocab.insert(piece.clone(), id);
    }
    let mut seen_ids = HashSet::with_capacity(vocab.len());
    for (piece, &id) in &vocab {
        if !seen_ids.insert(id) {
            return Err(LoadError::Format(format!(
                "vocab id {id} is claimed by more than one piece ({piece:?})"
            )));
        }
    }
    Ok(vocab)
}

/// The special token ids of the added-token overlay. The overlay may alias
/// a vocab piece, but it must not contradict it: an id that disagrees with
/// the vocabulary is a packaging bug, not a variant to tolerate.
fn added_specials(
    tok: &serde_json::Value,
    vocab: &HashMap<String, u32>,
) -> Result<Vec<u32>, LoadError> {
    let mut special = Vec::new();
    let Some(added) = tok["added_tokens"].as_array() else {
        return Ok(special);
    };
    for entry in added {
        let id = as_u32(&entry["id"]).ok_or_else(|| {
            LoadError::Format("added_tokens entry with a missing or invalid id".into())
        })?;
        let content = entry["content"]
            .as_str()
            .ok_or_else(|| LoadError::Format("added_tokens entry without content".into()))?;
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
    Ok(special)
}

fn map_table(path: &Path) -> Result<memmap2::Mmap, LoadError> {
    let file = std::fs::File::open(path).map_err(|e| io_error(path, e))?;
    // SAFETY: the model file is a read-only packaged asset; the mapping is
    // validated below before any row is read.
    let table = unsafe { memmap2::Mmap::map(&file) }.map_err(|e| io_error(path, e))?;
    if table.len() < 8 {
        return Err(LoadError::Format(
            "safetensors shorter than its length header".into(),
        ));
    }
    Ok(table)
}

/// The safetensors JSON header and the offset its data starts at.
fn read_header(table: &[u8]) -> Result<(serde_json::Value, usize), LoadError> {
    let header_len = u64::from_le_bytes(table[0..8].try_into().unwrap()) as usize;
    if header_len > MAX_SAFETENSORS_HEADER_BYTES {
        return Err(LoadError::Format(format!(
            "safetensors header of {header_len} bytes exceeds the {MAX_SAFETENSORS_HEADER_BYTES}-byte limit"
        )));
    }
    let bytes = table
        .get(8..8 + header_len)
        .ok_or_else(|| LoadError::Format("safetensors header exceeds file".into()))?;
    let header = serde_json::from_slice(bytes)
        .map_err(|e| LoadError::Format(format!("safetensors header: {e}")))?;
    Ok((header, 8 + header_len))
}

/// The table must have one row per vocabulary piece, and every vocab and
/// special id must name a row inside it.
fn check_table_shape(
    rows: usize,
    dim: usize,
    vocab: &HashMap<String, u32>,
    special: &[u32],
) -> Result<(), LoadError> {
    if rows == 0 || dim == 0 {
        return Err(LoadError::Format(
            "embeddings shape has a zero dimension".into(),
        ));
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
    Ok(())
}

/// The optional per-row `weights` tensor: one finite, non-negative weight
/// per table row.
fn read_weights(
    header: &serde_json::Value,
    header_end: usize,
    table: &[u8],
    rows: usize,
) -> Result<Option<Vec<f32>>, LoadError> {
    match header.get("weights") {
        Some(tensor) if !tensor.is_null() => {}
        _ => return Ok(None),
    }
    let (wshape, woffset, wbytes) = tensor_region(header, "weights", 1, header_end, table.len())?;
    if wshape[0] != rows {
        return Err(LoadError::Format(format!(
            "weights length {} != table rows {rows}",
            wshape[0]
        )));
    }
    let mut values = Vec::with_capacity(wshape[0]);
    for chunk in table[woffset..woffset + wbytes].as_chunks::<4>().0 {
        let w = f32::from_le_bytes(*chunk);
        if !w.is_finite() || w < 0.0 {
            return Err(LoadError::Format(
                "weights must be finite and non-negative".into(),
            ));
        }
        values.push(w);
    }
    Ok(Some(values))
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
        return Err(LoadError::Format(format!(
            "safetensors has no {name} tensor"
        )));
    }
    if tensor["dtype"] != "F32" {
        return Err(LoadError::Format(format!(
            "{name} dtype {} is not F32",
            tensor["dtype"]
        )));
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
    let bytes = elements.checked_mul(4).ok_or_else(|| {
        LoadError::Format(format!("{name} byte size overflows addressable memory"))
    })?;
    let offsets = tensor["data_offsets"]
        .as_array()
        .ok_or_else(|| LoadError::Format(format!("{name} data_offsets missing")))?;
    if offsets.len() != 2 {
        return Err(LoadError::Format(format!(
            "{name} data_offsets is not [start, end]"
        )));
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
        return Err(LoadError::Format(format!(
            "{name} data extends past end of file"
        )));
    }
    Ok((shape, data_offset, bytes))
}
