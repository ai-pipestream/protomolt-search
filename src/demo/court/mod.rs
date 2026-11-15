//! Court-opinion pipeline support: NDJSON opinion records, sentence-aware
//! chunking with a contiguity invariant, and the streamable/resumable
//! chunk and embedding file formats used by the `court_chunks`,
//! `court_embed`, and `court_ingest` binaries.

mod assemble;
mod files;
mod plan;
#[cfg(test)]
mod tests;

use serde::{Deserialize, Serialize};

pub use assemble::{assemble_chunks, is_contiguous};
pub use files::{
    chunks_resume_state, embedded_keys, read_chunks, ChunkWriter, EmbeddingReader, EmbeddingRecord,
    EmbeddingWriter, EMBEDDINGS_MAGIC,
};
pub use plan::{cosine, plan_chunks, pool_block_vectors, ChunkPlan, ChunkSource};

/// One line of the opinions NDJSON corpus. Ids arrive as quoted strings
/// in the source data.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct Opinion {
    /// Opinion id (string in the corpus).
    pub id: String,
    /// Cluster id (string in the corpus).
    pub cluster_id: String,
    /// Full-length opinion text.
    pub plain_text: String,
}

/// Parse one NDJSON line into an [`Opinion`], normalizing ids to u64.
pub fn parse_opinion(line: &str) -> Result<(u64, u64, String), String> {
    let opinion: Opinion = serde_json::from_str(line).map_err(|e| format!("bad ndjson: {e}"))?;
    let id = opinion
        .id
        .parse::<u64>()
        .map_err(|e| format!("opinion id {:?} is not a u64: {e}", opinion.id))?;
    let cluster_id = opinion
        .cluster_id
        .parse::<u64>()
        .map_err(|e| format!("cluster id {:?} is not a u64: {e}", opinion.cluster_id))?;
    Ok((id, cluster_id, opinion.plain_text))
}

/// One chunk of an opinion, with full lineage.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Chunk {
    /// Sequential id = record index in the chunks file (assigned at
    /// write time). Positional shard ids derive from it at ingest.
    pub chunk_id: u64,
    /// Owning opinion.
    pub opinion_id: u64,
    /// Owning cluster.
    pub cluster_id: u64,
    /// Chunk span start in ORIGINAL text coordinates (char offsets,
    /// matching the sidecar's spans).
    pub span_start: u32,
    /// Chunk span end (exclusive).
    pub span_end: u32,
    /// Ordinal within the opinion (0-based).
    pub ordinal: u32,
    /// Input line number of the opinion (resume support).
    pub src_line: u64,
    /// The chunk text, `text[span_start..span_end]` of the opinion.
    pub text: String,
}

/// A span in original-text (char) coordinates, half-open.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Span {
    /// Start (inclusive).
    pub start: u32,
    /// End (exclusive).
    pub end: u32,
}

/// Slice `text` by char offsets (the sidecar's span coordinates).
pub fn slice_chars(text: &str, start: u32, end: u32) -> String {
    text.chars()
        .skip(start as usize)
        .take((end - start) as usize)
        .collect()
}

/// Count the tokens (by span) inside `[start, end)`.
fn tokens_in(tokens: &[Span], start: u32, end: u32) -> usize {
    tokens
        .iter()
        .filter(|t| t.start >= start && t.start < end)
        .count()
}
