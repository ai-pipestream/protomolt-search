//! Parser for the Lucene-era distributed-test corpus format (bge-m3
//! embeddings + paired sentences), as produced by the user's earlier
//! OpenSearch/Lucene distributed testing. NOT a general format — see the
//! README's shakedown section for provenance.
//!
//! Binary layout (all big-endian):
//!
//! ```text
//! i32 record_count | i32 dim | record_count × dim × f32 vectors
//! ```
//!
//! Text side: one sentence per line. Files from this pipeline do NOT end
//! with a trailing newline, so `wc -l` reports one fewer than the record
//! count — the final newline-less segment is a record.

use std::io::{self, Read};
use std::path::Path;

const HEADER_BYTES: usize = 8;

fn invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

/// Read an embeddings `.bin` file: returns `(vectors, count, dim)` with
/// `vectors.len() == count * dim`.
///
/// Guards: the file must hold exactly `8 + count*dim*4` bytes — a
/// truncated or padded file is an error, not a silent partial read.
pub fn read_embeddings(path: &Path) -> io::Result<(Vec<f32>, usize, usize)> {
    let bytes = std::fs::read(path)?;
    if bytes.len() < HEADER_BYTES {
        return Err(invalid(format!(
            "{}: {} bytes, smaller than the 8-byte header",
            path.display(),
            bytes.len()
        )));
    }
    let count = i32::from_be_bytes(bytes[0..4].try_into().unwrap());
    let dim = i32::from_be_bytes(bytes[4..8].try_into().unwrap());
    if count <= 0 || dim <= 0 {
        return Err(invalid(format!(
            "{}: non-positive count ({count}) or dim ({dim})",
            path.display()
        )));
    }
    let (count, dim) = (count as usize, dim as usize);
    let expected = count
        .checked_mul(dim)
        .and_then(|n| n.checked_mul(4))
        .and_then(|n| n.checked_add(HEADER_BYTES))
        .ok_or_else(|| invalid(format!("{}: count*dim overflows", path.display())))?;
    if bytes.len() != expected {
        return Err(invalid(format!(
            "{}: {} bytes, expected {expected} for {count} x {dim} f32",
            path.display(),
            bytes.len()
        )));
    }

    let mut vectors = vec![0.0f32; count * dim];
    for (i, chunk) in bytes[HEADER_BYTES..].as_chunks::<4>().0.iter().enumerate() {
        vectors[i] = f32::from_be_bytes(*chunk);
    }
    Ok((vectors, count, dim))
}

/// Read the paired sentence file: one record per line, with a
/// newline-less final segment counting as a record (these files have no
/// trailing newline).
pub fn read_sentences(path: &Path) -> io::Result<Vec<String>> {
    let text = std::fs::read_to_string(path)?;
    let mut records: Vec<String> = text
        .split('\n')
        .map(|line| line.strip_suffix('\r').unwrap_or(line).to_string())
        .collect();
    // A trailing newline produces an empty final segment that is NOT a
    // record; a newline-less final segment IS one.
    if records.last().is_some_and(|last| last.is_empty()) {
        records.pop();
    }
    Ok(records)
}

/// Read a single embedding record by index without loading the whole
/// file (used to fetch query vectors after ingest).
pub fn read_embedding_at(path: &Path, index: usize) -> io::Result<(Vec<f32>, usize)> {
    let mut file = std::fs::File::open(path)?;
    let mut header = [0u8; HEADER_BYTES];
    file.read_exact(&mut header)?;
    let count = i32::from_be_bytes(header[0..4].try_into().unwrap()) as usize;
    let dim = i32::from_be_bytes(header[4..8].try_into().unwrap()) as usize;
    if index >= count {
        return Err(invalid(format!(
            "{}: record index {index} out of range ({count})",
            path.display()
        )));
    }
    use std::io::{Seek, SeekFrom};
    file.seek(SeekFrom::Start((HEADER_BYTES + index * dim * 4) as u64))?;
    let mut buf = vec![0u8; dim * 4];
    file.read_exact(&mut buf)?;
    let mut vector = vec![0.0f32; dim];
    for (i, chunk) in buf.as_chunks::<4>().0.iter().enumerate() {
        vector[i] = f32::from_be_bytes(*chunk);
    }
    Ok((vector, dim))
}

/// Load one corpus part and assert exact vector/text pairing.
pub fn read_part(
    embeddings: &Path,
    sentences: &Path,
) -> io::Result<(Vec<f32>, usize, Vec<String>)> {
    let (vectors, count, dim) = read_embeddings(embeddings)?;
    let texts = read_sentences(sentences)?;
    if texts.len() != count {
        return Err(invalid(format!(
            "pairing mismatch: {} has {count} vectors but {} has {} sentences",
            embeddings.display(),
            sentences.display(),
            texts.len()
        )));
    }
    Ok((vectors, dim, texts))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fixture(dir: &std::path::Path, count: i32, dim: i32, vectors: &[f32]) -> std::path::PathBuf {
        let path = dir.join("fixture.bin");
        let mut bytes = Vec::new();
        bytes.extend_from_slice(&count.to_be_bytes());
        bytes.extend_from_slice(&dim.to_be_bytes());
        for v in vectors {
            bytes.extend_from_slice(&v.to_be_bytes());
        }
        std::fs::write(&path, bytes).unwrap();
        path
    }

    fn tmpdir(tag: &str) -> std::path::PathBuf {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target/tmp")
            .join(format!("tvdataset_{tag}_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    #[test]
    fn embeddings_round_trip() {
        let dir = tmpdir("rt");
        let path = fixture(&dir, 2, 3, &[1.5, -2.0, 3.25, 4.0, 5.5, -6.75]);
        let (vectors, count, dim) = read_embeddings(&path).unwrap();
        assert_eq!((count, dim), (2, 3));
        assert_eq!(vectors, vec![1.5, -2.0, 3.25, 4.0, 5.5, -6.75]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn corrupt_lengths_are_rejected() {
        let dir = tmpdir("corrupt");
        // Smaller than the header.
        let tiny = dir.join("tiny.bin");
        std::fs::write(&tiny, [0u8; 4]).unwrap();
        assert!(read_embeddings(&tiny).is_err());
        // Truncated payload.
        let path = fixture(&dir, 2, 3, &[1.0; 5]);
        assert!(read_embeddings(&path).is_err());
        // Padded payload.
        let mut bytes = std::fs::read(fixture(&dir, 1, 2, &[1.0, 2.0])).unwrap();
        bytes.push(0);
        let padded = dir.join("padded.bin");
        std::fs::write(&padded, bytes).unwrap();
        assert!(read_embeddings(&padded).is_err());
        // Non-positive dim.
        let bad = dir.join("baddim.bin");
        std::fs::write(&bad, [0, 0, 0, 1, 0, 0, 0, 0]).unwrap();
        assert!(read_embeddings(&bad).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn sentences_without_trailing_newline_count_as_records() {
        let dir = tmpdir("nl");
        let path = dir.join("s.txt");
        // Three sentences, no trailing newline: wc -l would say 2.
        std::fs::write(&path, "one\ntwo\nthree").unwrap();
        assert_eq!(read_sentences(&path).unwrap(), vec!["one", "two", "three"]);
        // With trailing newline: same records.
        std::fs::write(&path, "one\ntwo\nthree\n").unwrap();
        assert_eq!(read_sentences(&path).unwrap(), vec!["one", "two", "three"]);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn read_single_record_by_index() {
        let dir = tmpdir("at");
        let path = fixture(&dir, 3, 2, &[1.0, 2.0, 3.0, 4.0, 5.0, 6.0]);
        let (vector, dim) = read_embedding_at(&path, 1).unwrap();
        assert_eq!(dim, 2);
        assert_eq!(vector, vec![3.0, 4.0]);
        assert!(read_embedding_at(&path, 3).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn pairing_mismatch_is_an_error() {
        let dir = tmpdir("pair");
        let bin = fixture(&dir, 2, 2, &[1.0, 2.0, 3.0, 4.0]);
        let txt = dir.join("s.txt");
        std::fs::write(&txt, "only one").unwrap();
        assert!(read_part(&bin, &txt).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
