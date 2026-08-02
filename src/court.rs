//! Court-opinion pipeline support: NDJSON opinion records, sentence-aware
//! chunking with a contiguity invariant, and the streamable/resumable
//! chunk and embedding file formats used by the `court_chunks`,
//! `court_embed`, and `court_ingest` binaries.

use std::io::{BufRead, Read, Write};
use std::path::Path;

use serde::{Deserialize, Serialize};

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

/// Assemble sentence-aware chunks of ~`target_tokens` whitespace tokens
/// from one opinion's analysis.
///
/// Boundaries are sentence starts; whole sentences are packed while they
/// fit the target. A single sentence LONGER than the target is split at
/// its own token boundaries (truncation of the text is never involved, so
/// the contiguity invariant below cannot be broken by oversize input).
///
/// CONTIGUITY INVARIANT: the returned spans are contiguous and gap-free —
/// `spans[0].start == 0`, `spans.last().end == text.chars().count()`, and
/// `spans[i+1].start == spans[i].end` — so concatenating the chunk texts
/// in ordinal order reproduces the original text byte-for-byte.
pub fn assemble_chunks(
    text: &str,
    sentences: &[Span],
    tokens: &[Span],
    target_tokens: usize,
) -> Vec<Span> {
    let text_len = text.chars().count() as u32;
    if text_len == 0 {
        return Vec::new();
    }
    let target = target_tokens.max(1) as u32;
    let mut boundaries: Vec<u32> = vec![0];

    for sentence in sentences {
        let s_start = sentence.start;
        let s_end = sentence.end.min(text_len);
        if s_end <= s_start {
            continue;
        }
        let chunk_start = *boundaries.last().unwrap();
        let sentence_tokens = tokens_in(tokens, s_start, s_end);
        let current_tokens = tokens_in(tokens, chunk_start, s_end);

        if sentence_tokens as u32 > target {
            // Oversize sentence: close the current chunk at the sentence
            // start, then split the sentence at token boundaries.
            if chunk_start < s_start {
                boundaries.push(s_start);
            }
            let mut split_start = s_start;
            let sentence_token_spans: Vec<Span> = tokens
                .iter()
                .filter(|t| t.start >= s_start && t.start < s_end)
                .copied()
                .collect();
            for window in sentence_token_spans.chunks(target as usize) {
                let split_end = if window.len() as u32 == target {
                    // Next chunk starts at the token after this window.
                    let next = sentence_token_spans
                        .iter()
                        .find(|t| t.start >= window[window.len() - 1].end);
                    next.map(|t| t.start).unwrap_or(s_end)
                } else {
                    s_end
                };
                if split_end > split_start {
                    boundaries.push(split_end);
                }
                split_start = split_end;
            }
        } else if current_tokens as u32 > target && chunk_start < s_start {
            // Adding this sentence would exceed the target: close the
            // current chunk at the sentence start.
            boundaries.push(s_start);
        }
    }
    if *boundaries.last().unwrap() < text_len {
        boundaries.push(text_len);
    }

    boundaries
        .windows(2)
        .map(|w| Span {
            start: w[0],
            end: w[1],
        })
        .filter(|s| s.end > s.start)
        .collect()
}

/// Verify the contiguity invariant on assembled spans (used by the
/// pipeline's per-opinion assertion and by tests).
pub fn is_contiguous(text: &str, spans: &[Span]) -> bool {
    if spans.is_empty() {
        return text.is_empty();
    }
    let text_len = text.chars().count() as u32;
    spans[0].start == 0
        && spans.last().unwrap().end == text_len
        && spans.windows(2).all(|w| w[0].end == w[1].start)
}

/// Chunk-file records as NDJSON lines (streamable, resumable).
pub struct ChunkWriter<W: Write> {
    writer: std::io::BufWriter<W>,
    next_id: u64,
}

impl<W: Write> ChunkWriter<W> {
    /// Wrap a sink; `next_id` continues the record count on resume.
    pub fn new(writer: W, next_id: u64) -> Self {
        Self {
            writer: std::io::BufWriter::new(writer),
            next_id,
        }
    }

    /// Append one opinion's chunks in ordinal order, assigning sequential
    /// chunk ids. Returns the ids assigned.
    pub fn write_opinion_chunks(
        &mut self,
        opinion_id: u64,
        cluster_id: u64,
        src_line: u64,
        text: &str,
        spans: &[Span],
    ) -> std::io::Result<()> {
        debug_assert!(is_contiguous(text, spans), "contiguity invariant violated");
        for (ordinal, span) in spans.iter().enumerate() {
            let chunk = Chunk {
                chunk_id: self.next_id,
                opinion_id,
                cluster_id,
                span_start: span.start,
                span_end: span.end,
                ordinal: ordinal as u32,
                src_line,
                text: slice_chars(text, span.start, span.end),
            };
            serde_json::to_writer(&mut self.writer, &chunk).map_err(std::io::Error::other)?;
            self.writer.write_all(b"\n")?;
            self.next_id += 1;
        }
        Ok(())
    }

    /// Flush the underlying writer.
    pub fn finish(mut self) -> std::io::Result<u64> {
        self.writer.flush()?;
        Ok(self.next_id)
    }
}

/// Stream chunk records from a chunks file, one per line.
pub fn read_chunks(path: &Path) -> std::io::Result<impl Iterator<Item = std::io::Result<Chunk>>> {
    let file = std::fs::File::open(path)?;
    Ok(std::io::BufReader::new(file).lines().map(|line| {
        line.and_then(|l| {
            serde_json::from_str(&l)
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
        })
    }))
}

/// Resume state of a chunks file: how many records it holds and the
/// highest input line fully processed.
pub fn chunks_resume_state(path: &Path) -> std::io::Result<(u64, u64)> {
    if !path.exists() {
        return Ok((0, 0));
    }
    let mut records = 0u64;
    let mut max_line = 0u64;
    for chunk in read_chunks(path)? {
        let chunk = chunk?;
        records += 1;
        max_line = max_line.max(chunk.src_line + 1);
    }
    Ok((records, max_line))
}

/// Embeddings file: magic + dim header, then `(opinion_id, ordinal,
/// vector)` records. The (opinion_id, ordinal) pair is the durable
/// resume key — stable across reruns of the chunking pass, unlike the
/// file-order chunk_id.
pub const EMBEDDINGS_MAGIC: &[u8; 8] = b"TVEMB001";

/// Append embeddings records.
pub struct EmbeddingWriter<W: Write> {
    writer: std::io::BufWriter<W>,
}

impl<W: Write> EmbeddingWriter<W> {
    /// Create a file writer with the header for `dim`.
    pub fn create(writer: W, dim: u32) -> std::io::Result<Self> {
        let mut writer = std::io::BufWriter::new(writer);
        writer.write_all(EMBEDDINGS_MAGIC)?;
        writer.write_all(&dim.to_le_bytes())?;
        Ok(Self { writer })
    }

    /// Append to an existing embeddings file (header already present).
    pub fn append(writer: W) -> Self {
        Self {
            writer: std::io::BufWriter::new(writer),
        }
    }

    /// Write one `(opinion_id, ordinal, vector)` record.
    pub fn write(&mut self, opinion_id: u64, ordinal: u32, vector: &[f32]) -> std::io::Result<()> {
        self.writer.write_all(&opinion_id.to_le_bytes())?;
        self.writer.write_all(&ordinal.to_le_bytes())?;
        for v in vector {
            self.writer.write_all(&v.to_le_bytes())?;
        }
        Ok(())
    }

    /// Flush.
    pub fn finish(mut self) -> std::io::Result<()> {
        self.writer.flush()
    }
}

/// One embeddings record.
#[derive(Debug, Clone, PartialEq)]
pub struct EmbeddingRecord {
    /// Owning opinion.
    pub opinion_id: u64,
    /// Chunk ordinal within the opinion.
    pub ordinal: u32,
    /// The embedding vector (dim from the file header).
    pub vector: Vec<f32>,
}

/// Streaming reader over an embeddings file (record at a time).
pub struct EmbeddingReader {
    reader: std::io::BufReader<std::fs::File>,
    dim: u32,
}

impl EmbeddingReader {
    /// Open an embeddings file, consuming its header.
    pub fn open(path: &Path) -> std::io::Result<(u32, Self)> {
        let mut file = std::fs::File::open(path)?;
        let mut header = [0u8; 12];
        file.read_exact(&mut header)?;
        if &header[..8] != EMBEDDINGS_MAGIC {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("{}: bad embeddings header", path.display()),
            ));
        }
        let dim = u32::from_le_bytes(header[8..12].try_into().unwrap());
        Ok((
            dim,
            Self {
                reader: std::io::BufReader::new(file),
                dim,
            },
        ))
    }
}

impl Iterator for EmbeddingReader {
    type Item = std::io::Result<EmbeddingRecord>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut fixed = [0u8; 12];
        match self.reader.read_exact(&mut fixed) {
            Ok(()) => {}
            Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return None,
            Err(e) => return Some(Err(e)),
        }
        let mut buf = vec![0u8; self.dim as usize * 4];
        if let Err(e) = self.reader.read_exact(&mut buf) {
            return Some(Err(e));
        }
        let opinion_id = u64::from_le_bytes(fixed[..8].try_into().unwrap());
        let ordinal = u32::from_le_bytes(fixed[8..12].try_into().unwrap());
        let vector = buf
            .chunks_exact(4)
            .map(|c| f32::from_le_bytes(c.try_into().unwrap()))
            .collect();
        Some(Ok(EmbeddingRecord {
            opinion_id,
            ordinal,
            vector,
        }))
    }
}

/// The set of already-embedded `(opinion_id, ordinal)` pairs, for resume.
pub fn embedded_keys(path: &Path) -> std::io::Result<std::collections::HashSet<(u64, u32)>> {
    let mut keys = std::collections::HashSet::new();
    if !path.exists() {
        return Ok(keys);
    }
    let (_, reader) = EmbeddingReader::open(path)?;
    for record in reader {
        let record = record?;
        keys.insert((record.opinion_id, record.ordinal));
    }
    Ok(keys)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spans(v: &[(u32, u32)]) -> Vec<Span> {
        v.iter().map(|&(start, end)| Span { start, end }).collect()
    }

    #[test]
    fn parse_opinion_extracts_fields() {
        let (id, cluster, text) =
            parse_opinion(r#"{"id": "4245481", "cluster_id": "4468228", "plain_text": "hello"}"#)
                .unwrap();
        assert_eq!(id, 4245481);
        assert_eq!(cluster, 4468228);
        assert_eq!(text, "hello");
        assert!(parse_opinion(r#"{"id": "x", "cluster_id": "1", "plain_text": "y"}"#).is_err());
    }

    #[test]
    fn chunks_pack_sentences_up_to_target() {
        // Text: three sentences at [0,10), [10,20), [20,30); tokens 4/4/4.
        let text = "0123456789abcdefghijABCDEFGHIJ";
        let sentences = spans(&[(0, 10), (10, 20), (20, 30)]);
        let tokens = spans(&[
            (0, 1),
            (3, 4),
            (6, 7),
            (8, 9),
            (10, 11),
            (13, 14),
            (16, 17),
            (18, 19),
            (20, 21),
            (23, 24),
            (26, 27),
            (28, 29),
        ]);
        // Target 9 tokens: the first two sentences (4+4=8) pack into one
        // chunk; adding the third (12 total) exceeds, so one boundary
        // lands at 20.
        let chunks = assemble_chunks(text, &sentences, &tokens, 9);
        assert_eq!(chunks, spans(&[(0, 20), (20, 30)]));
        assert!(is_contiguous(text, &chunks));
        // Target 12: everything packs into one chunk.
        let chunks = assemble_chunks(text, &sentences, &tokens, 12);
        assert_eq!(chunks, spans(&[(0, 30)]));
        assert!(is_contiguous(text, &chunks));
    }

    #[test]
    fn oversize_sentence_splits_at_token_boundaries() {
        let text = "0123456789abcdefghij";
        let sentences = spans(&[(0, 20)]);
        let tokens = spans(&[
            (0, 1),
            (2, 3),
            (4, 5),
            (6, 7),
            (8, 9),
            (10, 11),
            (12, 13),
            (14, 15),
        ]);
        // 8 tokens, target 3: splits at 3-token boundaries -> [0,6),[6,12),[12,20).
        let chunks = assemble_chunks(text, &sentences, &tokens, 3);
        assert_eq!(chunks, spans(&[(0, 6), (6, 12), (12, 20)]));
        assert!(is_contiguous(text, &chunks));
        let joined: String = chunks
            .iter()
            .map(|s| slice_chars(text, s.start, s.end))
            .collect();
        assert_eq!(
            joined, text,
            "contiguity: chunks must reproduce the original"
        );
    }

    #[test]
    fn gap_free_invariant_holds_with_inter_sentence_whitespace() {
        // Sentence spans exclude the gaps; chunk boundaries must NOT.
        let text = "aa  bb  cc";
        let sentences = spans(&[(0, 2), (4, 6), (8, 10)]);
        let tokens = spans(&[(0, 2), (4, 6), (8, 10)]);
        let chunks = assemble_chunks(text, &sentences, &tokens, 2);
        assert!(is_contiguous(text, &chunks));
        let joined: String = chunks
            .iter()
            .map(|s| slice_chars(text, s.start, s.end))
            .collect();
        assert_eq!(joined, text);
    }

    #[test]
    fn plan_chunks_packs_and_splits_with_block_attribution() {
        // Three blocks [0,10),[10,20),[20,30), 4 tokens each.
        let text = "0123456789abcdefghijABCDEFGHIJ";
        let sentences = spans(&[(0, 10), (10, 20), (20, 30)]);
        let tokens = spans(&[
            (0, 1),
            (3, 4),
            (6, 7),
            (8, 9),
            (10, 11),
            (13, 14),
            (16, 17),
            (18, 19),
            (20, 21),
            (23, 24),
            (26, 27),
            (28, 29),
        ]);
        // Target 9: first two blocks pack, third is its own chunk.
        let plans = plan_chunks(text, &sentences, &tokens, 9, 100);
        assert_eq!(plans.len(), 2);
        assert_eq!(plans[0].span, Span { start: 0, end: 20 });
        assert_eq!(plans[0].source, ChunkSource::Blocks { first: 0, last: 1 });
        assert_eq!(plans[1].span, Span { start: 20, end: 30 });
        assert_eq!(plans[1].source, ChunkSource::Blocks { first: 2, last: 2 });
        let spans: Vec<Span> = plans.iter().map(|p| p.span).collect();
        assert!(is_contiguous(text, &spans));
    }

    #[test]
    fn plan_chunks_splits_oversized_block_into_solo_pieces() {
        let text = "0123456789abcdefghij";
        let sentences = spans(&[(0, 20)]);
        let tokens = spans(&[
            (0, 1),
            (2, 3),
            (4, 5),
            (6, 7),
            (8, 9),
            (10, 11),
            (12, 13),
            (14, 15),
        ]);
        // 8 tokens, cap 3: pieces [0,6),[6,12),[12,20), all SoloPiece.
        let plans = plan_chunks(text, &sentences, &tokens, 2, 3);
        assert_eq!(plans.len(), 3);
        assert!(plans.iter().all(|p| p.source == ChunkSource::SoloPiece));
        let spans: Vec<Span> = plans.iter().map(|p| p.span).collect();
        assert!(is_contiguous(text, &spans));
        let joined: String = spans
            .iter()
            .map(|s| slice_chars(text, s.start, s.end))
            .collect();
        assert_eq!(joined, text);
    }

    #[test]
    fn pooling_is_token_weighted_mean() {
        let v1 = [1.0f32, 0.0];
        let v2 = [0.0f32, 1.0];
        // 3 tokens at v1, 1 token at v2 -> mean [0.75, 0.25], then
        // L2-normalized (the model's Normalize stage).
        let pooled = pool_block_vectors(&[(&v1, 3), (&v2, 1)], 2);
        let norm = (0.75f64 * 0.75 + 0.25 * 0.25).sqrt();
        assert!((f64::from(pooled[0]) - 0.75 / norm).abs() < 1e-6);
        assert!((f64::from(pooled[1]) - 0.25 / norm).abs() < 1e-6);
        let n: f32 = pooled.iter().map(|x| x * x).sum::<f32>().sqrt();
        assert!((n - 1.0).abs() < 1e-5);
        // Cosine sanity.
        assert!((cosine(&[1.0, 0.0], &[1.0, 0.0]) - 1.0).abs() < 1e-9);
        assert!(cosine(&[1.0, 0.0], &[0.0, 1.0]).abs() < 1e-9);
    }

    #[test]
    fn chunk_and_embedding_files_round_trip() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target/tmp")
            .join(format!("tvcourt_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let chunks_path = dir.join("chunks.ndjson");
        {
            let mut writer = ChunkWriter::new(std::fs::File::create(&chunks_path).unwrap(), 0);
            writer
                .write_opinion_chunks(7, 9, 0, "abcdef", &spans(&[(0, 3), (3, 6)]))
                .unwrap();
            writer.finish().unwrap();
        }
        let chunks: Vec<Chunk> = read_chunks(&chunks_path)
            .unwrap()
            .collect::<std::io::Result<_>>()
            .unwrap();
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].chunk_id, 0);
        assert_eq!(chunks[1].text, "def");
        assert_eq!(chunks[1].opinion_id, 7);
        let (records, max_line) = chunks_resume_state(&chunks_path).unwrap();
        assert_eq!(records, 2);
        assert_eq!(max_line, 1);

        let emb_path = dir.join("emb.bin");
        {
            let mut writer =
                EmbeddingWriter::create(std::fs::File::create(&emb_path).unwrap(), 2).unwrap();
            writer.write(7, 0, &[1.0, 2.0]).unwrap();
            writer.write(7, 1, &[3.0, 4.0]).unwrap();
            writer.finish().unwrap();
        }
        let keys = embedded_keys(&emb_path).unwrap();
        assert!(keys.contains(&(7, 1)));
        let (dim, reader) = EmbeddingReader::open(&emb_path).unwrap();
        assert_eq!(dim, 2);
        let records: Vec<EmbeddingRecord> = reader.collect::<std::io::Result<_>>().unwrap();
        assert_eq!(records[1].vector, vec![3.0, 4.0]);
        let _ = std::fs::remove_dir_all(&dir);
    }
}

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
            // token boundaries. The last piece extends to the next
            // block's start (or text end) to keep contiguity.
            if cur_start < block.start {
                plans.push(ChunkPlan {
                    span: Span {
                        start: cur_start,
                        end: block.start,
                    },
                    source: ChunkSource::Blocks {
                        first: cur_first,
                        last: i - 1,
                    },
                });
            }
            let block_tokens: Vec<Span> = tokens
                .iter()
                .filter(|t| t.start >= block.start && t.start < b_end)
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
            cur_start = next_start;
            cur_first = i + 1;
            cur_tokens = 0;
        } else if cur_tokens + b_tokens > target && cur_start < block.start {
            plans.push(ChunkPlan {
                span: Span {
                    start: cur_start,
                    end: block.start,
                },
                source: ChunkSource::Blocks {
                    first: cur_first,
                    last: i - 1,
                },
            });
            cur_start = block.start;
            cur_first = i;
            cur_tokens = b_tokens;
        } else {
            cur_tokens += b_tokens;
        }
    }
    if cur_start < text_len {
        plans.push(ChunkPlan {
            span: Span {
                start: cur_start,
                end: text_len,
            },
            source: if blocks.is_empty() {
                ChunkSource::SoloPiece
            } else {
                ChunkSource::Blocks {
                    first: cur_first,
                    last: blocks.len() - 1,
                }
            },
        });
    }
    plans
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
