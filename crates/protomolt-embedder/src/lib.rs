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

mod load;
#[cfg(test)]
mod tests;
mod tokenizer;

use std::collections::HashMap;
use std::fmt;

pub use self::tokenizer::{normalize, pre_tokenize, wordpiece};

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
            wordpiece(
                &self.vocab,
                self.unk_id,
                self.max_word_chars,
                &word,
                &mut ids,
            );
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
            for (a, chunk) in acc.iter_mut().zip(row.as_chunks::<4>().0) {
                *a += w * f64::from(f32::from_le_bytes(*chunk));
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
