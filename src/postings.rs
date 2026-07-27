//! Per-shard BM25 postings index and doc store, with persistence.
//!
//! The shard owns: term → postings (doc id, tf, occurrence offsets in
//! original-text coordinates), per-document lengths and corpus totals, and
//! the raw document texts (the highlight source). Append-only: no deletes,
//! no updates, so postings for a term stay doc-id-ordered by construction.
//!
//! Persistence is a compact custom binary format (no extra dependencies),
//! versioned by a magic header, written atomically (tmp file + rename)
//! next to the shard's `.tv` file as `<index path>.bm25`.

use std::collections::HashMap;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

const MAGIC: &[u8; 8] = b"TVBM2501";

/// One posting: a term occurrence set within one document.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Posting {
    /// Local document id (global id = slot_offset + this).
    pub doc_id: u32,
    /// Term frequency within the document.
    pub tf: u32,
    /// Occurrence spans `(start, end)` in original-text coordinates
    /// (half-open). Empty when ingested with SCORING_ONLY term vectors.
    pub offsets: Vec<(u32, u32)>,
}

/// Term data for one document, as produced from the sidecar's term
/// vectors: `(term, tf, original-text offsets)`.
pub type DocTerms = Vec<(String, u32, Vec<(u32, u32)>)>;

/// One analyzed document ready to be indexed.
#[derive(Debug, Clone, Default)]
pub struct AnalyzedDoc {
    /// Per-term data; see [`DocTerms`].
    pub terms: DocTerms,
    /// Document length in terms (sum of frequencies); used by BM25 length
    /// normalization.
    pub length: u32,
}

/// The shard's lexical half: postings, corpus stats, and raw texts.
#[derive(Debug, Default)]
pub struct Bm25Store {
    /// term → postings, kept ascending by doc id (append-only).
    postings: HashMap<String, Vec<Posting>>,
    /// Per-document length in terms, indexed by local doc id. Sparse
    /// slots (ids consumed by the vector side) hold 0.
    doc_lengths: Vec<u32>,
    /// Sum of all document lengths (for avgdl).
    total_length: u64,
    /// Raw texts indexed by local doc id; sparse slots hold `None`.
    texts: Vec<Option<String>>,
}

impl Bm25Store {
    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// The number of document slots ever allocated (the next local doc id).
    pub fn next_doc_id(&self) -> u32 {
        self.doc_lengths.len() as u32
    }

    /// Number of documents with postings.
    pub fn doc_count(&self) -> u64 {
        self.doc_lengths.iter().filter(|&&l| l > 0).count() as u64
    }

    /// Sum of all document lengths (BM25 avgdl numerator).
    pub fn total_doc_length(&self) -> u64 {
        self.total_length
    }

    /// Postings for `term`, if present.
    pub fn postings(&self, term: &str) -> Option<&[Posting]> {
        self.postings.get(term).map(Vec::as_slice)
    }

    /// Document length in terms (0 for unknown/sparse slots).
    pub fn doc_length(&self, doc_id: u32) -> u32 {
        self.doc_lengths.get(doc_id as usize).copied().unwrap_or(0)
    }

    /// The raw text of a document, if stored.
    pub fn text(&self, doc_id: u32) -> Option<&str> {
        self.texts.get(doc_id as usize).and_then(|t| t.as_deref())
    }

    /// Append one analyzed document with the given local doc id.
    ///
    /// `doc_id` must be `>= next_doc_id()` (append-only); ids above the
    /// current tip create sparse slots, which is how the vector and
    /// document sides share one positional id space.
    pub fn add_document(&mut self, doc_id: u32, text: String, doc: AnalyzedDoc) {
        let slot = doc_id as usize;
        assert!(
            slot >= self.doc_lengths.len(),
            "doc id {doc_id} already used"
        );
        self.doc_lengths.resize(slot + 1, 0);
        self.texts.resize_with(slot + 1, || None);
        self.doc_lengths[slot] = doc.length;
        self.total_length += u64::from(doc.length);
        self.texts[slot] = Some(text);
        for (term, tf, offsets) in doc.terms {
            self.postings.entry(term).or_default().push(Posting {
                doc_id,
                tf,
                offsets,
            });
        }
    }

    /// Persist to `path` (atomically: write tmp, rename).
    pub fn save(&self, path: &Path) -> io::Result<()> {
        let tmp: PathBuf = path.with_extension("bm25tmp");
        {
            let mut w = io::BufWriter::new(std::fs::File::create(&tmp)?);
            self.write_to(&mut w)?;
            w.flush()?;
        }
        std::fs::rename(&tmp, path)?;
        Ok(())
    }

    /// Load from `path`.
    pub fn load(path: &Path) -> io::Result<Self> {
        let bytes = std::fs::read(path)?;
        Self::read_from(&mut &bytes[..])
    }

    fn write_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        w.write_all(MAGIC)?;
        write_u32(w, self.doc_lengths.len() as u32)?;
        for &len in &self.doc_lengths {
            write_u32(w, len)?;
        }
        write_u64(w, self.total_length)?;
        write_u32(w, self.texts.len() as u32)?;
        for text in &self.texts {
            match text {
                Some(t) => write_str(w, t)?,
                None => write_u32(w, u32::MAX)?,
            }
        }
        write_u32(w, self.postings.len() as u32)?;
        // Term order must be deterministic for a stable file format.
        let mut terms: Vec<&String> = self.postings.keys().collect();
        terms.sort();
        for term in terms {
            let postings = &self.postings[term];
            write_str(w, term)?;
            write_u32(w, postings.len() as u32)?;
            for p in postings {
                write_u32(w, p.doc_id)?;
                write_u32(w, p.tf)?;
                write_u32(w, p.offsets.len() as u32)?;
                for &(start, end) in &p.offsets {
                    write_u32(w, start)?;
                    write_u32(w, end)?;
                }
            }
        }
        Ok(())
    }

    fn read_from(r: &mut &[u8]) -> io::Result<Self> {
        let invalid = |msg: &str| io::Error::new(io::ErrorKind::InvalidData, msg.to_string());
        if take(r, 8)? != MAGIC {
            return Err(invalid("bad magic"));
        }
        let n_slots = read_u32(r)? as usize;
        let mut doc_lengths = Vec::with_capacity(n_slots);
        for _ in 0..n_slots {
            doc_lengths.push(read_u32(r)?);
        }
        let total_length = read_u64(r)?;
        let n_texts = read_u32(r)? as usize;
        if n_texts != n_slots {
            return Err(invalid("text slots != length slots"));
        }
        let mut texts = Vec::with_capacity(n_texts);
        for _ in 0..n_texts {
            let len = read_u32(r)?;
            if len == u32::MAX {
                texts.push(None);
            } else {
                let bytes = take(r, len as usize)?;
                texts.push(Some(
                    String::from_utf8(bytes.to_vec())
                        .map_err(|_| invalid("invalid utf-8 in doc text"))?,
                ));
            }
        }
        let n_terms = read_u32(r)? as usize;
        let mut postings = HashMap::with_capacity(n_terms);
        for _ in 0..n_terms {
            let term_len = read_u32(r)? as usize;
            let term = String::from_utf8(take(r, term_len)?.to_vec())
                .map_err(|_| invalid("invalid utf-8 in term"))?;
            let n_postings = read_u32(r)? as usize;
            let mut plist = Vec::with_capacity(n_postings);
            for _ in 0..n_postings {
                let doc_id = read_u32(r)?;
                let tf = read_u32(r)?;
                let n_offsets = read_u32(r)? as usize;
                let mut offsets = Vec::with_capacity(n_offsets);
                for _ in 0..n_offsets {
                    offsets.push((read_u32(r)?, read_u32(r)?));
                }
                plist.push(Posting {
                    doc_id,
                    tf,
                    offsets,
                });
            }
            postings.insert(term, plist);
        }
        if !r.is_empty() {
            return Err(invalid("trailing bytes"));
        }
        Ok(Self {
            postings,
            doc_lengths,
            total_length,
            texts,
        })
    }
}

fn write_u32<W: Write>(w: &mut W, v: u32) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}

fn write_u64<W: Write>(w: &mut W, v: u64) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}

fn write_str<W: Write>(w: &mut W, s: &str) -> io::Result<()> {
    write_u32(w, s.len() as u32)?;
    w.write_all(s.as_bytes())
}

fn take<'a>(r: &mut &'a [u8], n: usize) -> io::Result<&'a [u8]> {
    if r.len() < n {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "truncated"));
    }
    let (head, tail) = r.split_at(n);
    *r = tail;
    Ok(head)
}

fn read_u32(r: &mut &[u8]) -> io::Result<u32> {
    Ok(u32::from_le_bytes(take(r, 4)?.try_into().expect("4 bytes")))
}

fn read_u64(r: &mut &[u8]) -> io::Result<u64> {
    Ok(u64::from_le_bytes(take(r, 8)?.try_into().expect("8 bytes")))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn doc_a() -> AnalyzedDoc {
        AnalyzedDoc {
            terms: vec![
                ("rust".to_string(), 2, vec![(0, 4), (10, 14)]),
                ("search".to_string(), 1, vec![(5, 11)]),
            ],
            length: 3,
        }
    }

    fn doc_b() -> AnalyzedDoc {
        AnalyzedDoc {
            terms: vec![
                ("rust".to_string(), 1, vec![(0, 4)]),
                ("vector".to_string(), 2, vec![(5, 11), (12, 18)]),
            ],
            length: 3,
        }
    }

    #[test]
    fn build_and_query_postings() {
        let mut store = Bm25Store::new();
        store.add_document(0, "rust search rust".to_string(), doc_a());
        store.add_document(1, "rust vector vector".to_string(), doc_b());

        assert_eq!(store.doc_count(), 2);
        assert_eq!(store.total_doc_length(), 6);
        assert_eq!(store.doc_length(1), 3);
        let postings = store.postings("rust").unwrap();
        assert_eq!(postings.len(), 2);
        assert_eq!(
            postings[0],
            Posting {
                doc_id: 0,
                tf: 2,
                offsets: vec![(0, 4), (10, 14)]
            }
        );
        assert_eq!(store.postings("vector").unwrap()[0].tf, 2);
        assert!(store.postings("missing").is_none());
        assert_eq!(store.text(1), Some("rust vector vector"));
    }

    #[test]
    fn sparse_slots_for_shared_id_space() {
        let mut store = Bm25Store::new();
        // Ids 0..2 were consumed by the vector side; first document lands
        // at id 2, leaving slots 0..1 sparse.
        store.add_document(
            2,
            "hello world".to_string(),
            AnalyzedDoc {
                terms: vec![("hello".to_string(), 1, vec![(0, 5)])],
                length: 2,
            },
        );
        assert_eq!(store.next_doc_id(), 3);
        assert_eq!(store.doc_count(), 1);
        assert_eq!(store.doc_length(0), 0);
        assert_eq!(store.doc_length(2), 2);
        assert!(store.text(0).is_none());
        assert_eq!(store.text(2), Some("hello world"));
    }

    #[test]
    fn round_trip_through_disk() {
        let dir = std::env::temp_dir().join(format!("tvbm25_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("shard.tv.bm25");

        let mut store = Bm25Store::new();
        store.add_document(0, "rust search rust".to_string(), doc_a());
        store.add_document(2, "rust vector vector".to_string(), doc_b());
        store.save(&path).unwrap();

        let loaded = Bm25Store::load(&path).unwrap();
        assert_eq!(loaded.doc_lengths, store.doc_lengths);
        assert_eq!(loaded.total_length, store.total_length);
        assert_eq!(loaded.texts, store.texts);
        assert_eq!(loaded.postings, store.postings);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_rejects_garbage() {
        let dir = std::env::temp_dir().join(format!("tvbm25_bad_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("bad.bm25");
        std::fs::write(&path, b"not a postings file").unwrap();
        assert!(Bm25Store::load(&path).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
