//! The streamable, resumable chunk and embedding file formats.

use std::io::{BufRead, Read, Write};
use std::path::Path;

use super::{is_contiguous, slice_chars, Chunk, Span};

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
            .as_chunks::<4>()
            .0
            .iter()
            .map(|c| f32::from_le_bytes(*c))
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
