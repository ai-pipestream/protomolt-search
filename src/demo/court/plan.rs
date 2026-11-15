//! Chunk planning for the static-embedding path and the pooling of
//! per-block vectors.

use super::{tokens_in, Span};

/// Where a chunk's vector comes from in the static-embedding path.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ChunkSource {
    /// Whole blocks (inclusive range of sentence/block indices): the
    /// chunk vector is the token-weighted pool of the blocks' vectors.
    Blocks {
        /// First block index.
        first: usize,
        /// Last block index (inclusive).
        last: usize,
    },
    /// A piece of one oversized block: embed the span solo (the block's
    /// own pool does not exist at this granularity).
    SoloPiece,
}

/// One planned chunk: its span and how to compute its vector.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ChunkPlan {
    /// The chunk span in original-text (char) coordinates.
    pub span: Span,
    /// How the chunk's embedding is derived.
    pub source: ChunkSource,
}

/// Plan chunks for the static-embedding path: pack whole blocks
/// (newline-delimited "sentences") up to `target_tokens`; a block larger
/// than `hard_cap_tokens` is split at its own token boundaries into
/// SoloPiece chunks. The contiguity invariant is identical to
/// [`assemble_chunks`]: spans are contiguous and gap-free, so
/// concatenated chunk texts reproduce the original byte-for-byte.
///
/// Block indices in the plan refer to `sentences` order, for pairing
/// with the sidecar's per-block embeddings.
pub fn plan_chunks(
    text: &str,
    sentences: &[Span],
    tokens: &[Span],
    target_tokens: usize,
    hard_cap_tokens: usize,
) -> Vec<ChunkPlan> {
    let text_len = text.chars().count() as u32;
    if text_len == 0 {
        return Vec::new();
    }
    let target = target_tokens.max(1);
    let cap = hard_cap_tokens.max(1);
    let mut plans: Vec<ChunkPlan> = Vec::new();

    let mut cur_start = 0u32;
    let mut cur_first = 0usize;
    let mut cur_tokens = 0usize;

    let blocks: Vec<Span> = sentences
        .iter()
        .copied()
        .filter(|s| s.end.min(text_len) > s.start)
        .collect();

    for (i, block) in blocks.iter().enumerate() {
        let b_end = block.end.min(text_len);
        let b_tokens = tokens_in(tokens, block.start, b_end);
        let next_start = blocks.get(i + 1).map(|s| s.start).unwrap_or(text_len);

        if b_tokens > cap {
            // Close the current chunk, then split the oversized block at
            // token boundaries.
            if cur_start < block.start {
                plans.push(blocks_plan(cur_start, block.start, cur_first, i - 1));
            }
            let oversized = Span {
                start: block.start,
                end: b_end,
            };
            push_solo_pieces(&mut plans, tokens, oversized, next_start, cap);
            cur_start = next_start;
            cur_first = i + 1;
            cur_tokens = 0;
        } else if cur_tokens + b_tokens > target && cur_start < block.start {
            plans.push(blocks_plan(cur_start, block.start, cur_first, i - 1));
            cur_start = block.start;
            cur_first = i;
            cur_tokens = b_tokens;
        } else {
            cur_tokens += b_tokens;
        }
    }
    if cur_start < text_len {
        let source = if blocks.is_empty() {
            ChunkSource::SoloPiece
        } else {
            ChunkSource::Blocks {
                first: cur_first,
                last: blocks.len() - 1,
            }
        };
        plans.push(ChunkPlan {
            span: Span {
                start: cur_start,
                end: text_len,
            },
            source,
        });
    }
    plans
}

/// The plan for whole blocks `first..=last` spanning `[start, end)`.
fn blocks_plan(start: u32, end: u32, first: usize, last: usize) -> ChunkPlan {
    ChunkPlan {
        span: Span { start, end },
        source: ChunkSource::Blocks { first, last },
    }
}

/// Splits the oversized `block` at its own token boundaries, `cap` tokens
/// per piece. The last piece extends to `next_start` (the next block's
/// start, or the text end) to keep contiguity.
fn push_solo_pieces(
    plans: &mut Vec<ChunkPlan>,
    tokens: &[Span],
    block: Span,
    next_start: u32,
    cap: usize,
) {
    let block_tokens: Vec<Span> = tokens
        .iter()
        .filter(|t| t.start >= block.start && t.start < block.end)
        .copied()
        .collect();
    let mut piece_start = block.start;
    for window in block_tokens.chunks(cap) {
        let piece_end = if window.len() == cap {
            block_tokens
                .iter()
                .find(|t| t.start >= window[window.len() - 1].end)
                .map(|t| t.start)
                .unwrap_or(next_start)
        } else {
            next_start
        };
        if piece_end > piece_start {
            plans.push(ChunkPlan {
                span: Span {
                    start: piece_start,
                    end: piece_end,
                },
                source: ChunkSource::SoloPiece,
            });
        }
        piece_start = piece_end;
    }
}

/// Token-weighted mean of per-block static embeddings, L2-normalized.
///
/// EXACTNESS: for a mean-pooled static embedding table (Model2Vec
/// family), a block's embedding is the mean of its token embeddings, so
/// the embedding of the concatenated blocks — the mean over ALL their
/// token embeddings — is exactly the token-count-weighted mean of the
/// block means. The weights MUST be the analyzer's own token counts
/// (token identity decides what the table averaged); recounting with a
/// different tokenizer would silently change the weighting. Only float
/// accumulation order separates this from a direct whole-chunk mean.
///
/// The result is L2-NORMALIZED: the model pipeline ends in a `Normalize`
/// stage, and turbovec scores true dot products (not cosines), so an
/// unnormalized mean would score `||v||^2` against itself instead of ~1
/// — the model's own output for the same text is unit-length, and ours
/// must match it. (Cosine-based checks are unaffected either way.)
pub fn pool_block_vectors(weighted: &[(&[f32], u32)], dim: usize) -> Vec<f32> {
    let mut out = vec![0.0f64; dim];
    let mut total = 0u64;
    for &(vector, weight) in weighted {
        debug_assert_eq!(vector.len(), dim);
        for (o, &v) in out.iter_mut().zip(vector.iter()) {
            *o += f64::from(v) * f64::from(weight);
        }
        total += u64::from(weight);
    }
    if total > 0 {
        for o in out.iter_mut() {
            *o /= total as f64;
        }
    }
    let norm = out.iter().map(|v| v * v).sum::<f64>().sqrt();
    if norm > 0.0 {
        for o in out.iter_mut() {
            *o /= norm;
        }
    }
    out.iter().map(|&v| v as f32).collect()
}

/// Cosine similarity (pooling-agreement checks).
pub fn cosine(a: &[f32], b: &[f32]) -> f64 {
    let mut dot = 0.0f64;
    let mut na = 0.0f64;
    let mut nb = 0.0f64;
    for (&x, &y) in a.iter().zip(b.iter()) {
        dot += f64::from(x) * f64::from(y);
        na += f64::from(x) * f64::from(x);
        nb += f64::from(y) * f64::from(y);
    }
    if na == 0.0 || nb == 0.0 {
        return 0.0;
    }
    dot / (na.sqrt() * nb.sqrt())
}
