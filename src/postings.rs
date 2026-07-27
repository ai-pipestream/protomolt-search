//! Per-shard BM25 postings index and doc store, with persistence.
//!
//! The shard owns: term → postings (doc id, tf, occurrence offsets in
//! original-text coordinates), per-document lengths and corpus totals, and
//! the raw document texts (the highlight source). Append-only: no deletes,
//! no updates, so postings for a term stay doc-id-ordered by construction.
//!
//! Two storage shapes share one read surface ([`Bm25Index`]):
//!
//! - [`Bm25Store`] — the heap builder. Ingest appends here; `save` writes
//!   the v3 format atomically next to the shard's `.tv` as `<index>.bm25`.
//! - [`Bm25Reader`] — the disk-resident shape. The v3 file is memory
//!   mapped; postings slices and document texts are read from the map on
//!   demand (the OS page cache is the buffer pool, the Lucene model), so
//!   a shard far larger than RAM serves from a few MB of heap (per-doc
//!   length/offset tables only).

use std::collections::HashMap;
use std::io::{self, Write};
use std::path::{Path, PathBuf};

const MAGIC_V1: &[u8; 8] = b"TVBM2501";
const MAGIC_V2: &[u8; 8] = b"TVBM2502";
const MAGIC: &[u8; 8] = b"TVBM2503";

/// Callback for [`Bm25Index::for_each_posting`]: `(doc_id, tf,
/// original-text offsets)`; the offsets slice is valid only inside the
/// call.
pub type PostingCallback<'a> = dyn FnMut(u32, u32, &[(u32, u32)]) + 'a;

/// The read surface of a shard's lexical half, shared by the heap
/// builder ([`Bm25Store`]) and the disk-resident reader ([`Bm25Reader`]).
/// Scoring (`bm25::top_k`, `bm25::score_candidates`) and the node's RPC
/// handlers code against this trait only.
pub trait Bm25Index {
    /// Documents with postings.
    fn doc_count(&self) -> u64;
    /// Sum of all document lengths (BM25 avgdl numerator).
    fn total_doc_length(&self) -> u64;
    /// Document length in terms (0 for unknown/sparse slots).
    fn doc_length(&self, doc_id: u32) -> u32;
    /// Document frequency of `term` (0 when absent).
    fn df(&self, term: &str) -> u32;
    /// Call `f` for every posting of `term`, in doc-id order, as
    /// `(doc_id, tf, original-text offsets)`. Implementations may decode
    /// lazily; the offsets slice is valid only inside the call.
    fn for_each_posting(&self, term: &str, f: &mut PostingCallback);
    /// The raw text of a document, if stored.
    fn text(&self, doc_id: u32) -> Option<String>;
    /// The lineage of a document, if ingested with one.
    fn lineage(&self, doc_id: u32) -> Option<DocLineage>;
}

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

/// Where a document came from in the source corpus (court pipeline).
/// Persisted with the doc store; `None` for documents ingested without
/// lineage (all pre-lineage shards).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DocLineage {
    /// Source opinion id.
    pub opinion_id: u64,
    /// Source cluster id.
    pub cluster_id: u64,
    /// Chunk span start in original-text (char) coordinates.
    pub span_start: u32,
    /// Chunk span end (exclusive).
    pub span_end: u32,
}

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
    /// Per-document lineage, parallel to `texts` (`None` when the
    /// document was ingested without lineage).
    lineages: Vec<Option<DocLineage>>,
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

    /// The lineage of a document, if it was ingested with one.
    pub fn lineage(&self, doc_id: u32) -> Option<DocLineage> {
        self.lineages.get(doc_id as usize).copied().flatten()
    }

    /// Append one analyzed document with the given local doc id.
    ///
    /// `doc_id` must be `>= next_doc_id()` (append-only); ids above the
    /// current tip create sparse slots, which is how the vector and
    /// document sides share one positional id space.
    pub fn add_document(&mut self, doc_id: u32, text: String, doc: AnalyzedDoc) {
        self.add_document_with_lineage(doc_id, text, doc, None);
    }

    /// Like [`Self::add_document`], with corpus lineage attached.
    pub fn add_document_with_lineage(
        &mut self,
        doc_id: u32,
        text: String,
        doc: AnalyzedDoc,
        lineage: Option<DocLineage>,
    ) {
        let slot = doc_id as usize;
        assert!(
            slot >= self.doc_lengths.len(),
            "doc id {doc_id} already used"
        );
        self.doc_lengths.resize(slot + 1, 0);
        self.texts.resize_with(slot + 1, || None);
        self.lineages.resize_with(slot + 1, || None);
        self.doc_lengths[slot] = doc.length;
        self.total_length += u64::from(doc.length);
        self.texts[slot] = Some(text);
        self.lineages[slot] = lineage;
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

    /// The v3 layout. Sections in file order, all offsets precomputed
    /// and written into the fixed header so the reader never walks the
    /// file to find them:
    ///
    /// ```text
    /// magic "TVBM2503" | header (total_length, section offsets, n_slots)
    /// doc_lengths (n_slots x u32)
    /// texts (n_slots x (u32 len | bytes), len == u32::MAX when absent)
    /// text_index (n_slots x (u64 offset, u32 len))   <- on-disk text directory
    /// lineages (n_slots x (u8 flag + 24B))
    /// postings (u32 n_terms, then per-term entries, sorted)
    /// directory (u32 n_terms, then n_terms x 18B fixed-stride entries
    ///   (u64 postings_off, u32 df, u32 blob_off, u16 term_len), then the
    ///   term blob) — binary-searchable by term
    /// ```
    fn write_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        let n_slots = self.doc_lengths.len() as u64;
        let header_size = 8 + 8 + 8 * 4 + 4;
        let doc_lengths_size = 4 * n_slots;
        let texts_size: u64 = self
            .texts
            .iter()
            .map(|t| 4 + t.as_ref().map_or(0, |s| s.len() as u64))
            .sum();
        let text_index_size = 12 * n_slots;
        let lineages_size: u64 = self
            .lineages
            .iter()
            .map(|l| if l.is_some() { 25 } else { 1 })
            .sum();
        let mut terms: Vec<&String> = self.postings.keys().collect();
        terms.sort();
        let postings_size: u64 = 4 + terms
            .iter()
            .map(|t| {
                4 + t.len() as u64
                    + 4
                    + self.postings[*t]
                        .iter()
                        .map(|p| 12 + 8 * p.offsets.len() as u64)
                        .sum::<u64>()
            })
            .sum::<u64>();

        let doc_lengths_off = header_size as u64;
        let texts_off = doc_lengths_off + doc_lengths_size;
        let text_index_off = texts_off + texts_size;
        let lineages_off = text_index_off + text_index_size;
        let postings_off = lineages_off + lineages_size;
        let directory_off = postings_off + postings_size;

        w.write_all(MAGIC)?;
        write_u64(w, self.total_length)?;
        write_u64(w, texts_off)?;
        write_u64(w, lineages_off)?;
        write_u64(w, postings_off)?;
        write_u64(w, directory_off)?;
        write_u32(w, self.doc_lengths.len() as u32)?;

        // doc_lengths
        for &len in &self.doc_lengths {
            write_u32(w, len)?;
        }
        // texts (+ build the on-disk index)
        let mut text_index: Vec<(u64, u32)> = Vec::with_capacity(self.texts.len());
        let mut cursor = texts_off;
        for text in &self.texts {
            match text {
                Some(t) => {
                    write_u32(w, t.len() as u32)?;
                    w.write_all(t.as_bytes())?;
                    text_index.push((cursor + 4, t.len() as u32));
                    cursor += 4 + t.len() as u64;
                }
                None => {
                    write_u32(w, u32::MAX)?;
                    text_index.push((0, u32::MAX));
                }
            }
        }
        // text_index
        for &(offset, len) in &text_index {
            write_u64(w, offset)?;
            write_u32(w, len)?;
        }
        // lineages
        for lineage in &self.lineages {
            match lineage {
                Some(l) => {
                    w.write_all(&[1u8])?;
                    write_u64(w, l.opinion_id)?;
                    write_u64(w, l.cluster_id)?;
                    write_u32(w, l.span_start)?;
                    write_u32(w, l.span_end)?;
                }
                None => w.write_all(&[0u8])?,
            }
        }
        // postings (+ directory entries)
        write_u32(w, terms.len() as u32)?;
        let mut directory: Vec<(u64, u32)> = Vec::with_capacity(terms.len());
        let mut cursor = postings_off + 4;
        for term in &terms {
            let postings = &self.postings[*term];
            directory.push((cursor, postings.len() as u32));
            write_str(w, term)?;
            write_u32(w, postings.len() as u32)?;
            cursor += 4 + term.len() as u64 + 4;
            for p in postings {
                write_u32(w, p.doc_id)?;
                write_u32(w, p.tf)?;
                write_u32(w, p.offsets.len() as u32)?;
                for &(start, end) in &p.offsets {
                    write_u32(w, start)?;
                    write_u32(w, end)?;
                }
                cursor += 12 + 8 * p.offsets.len() as u64;
            }
        }
        // directory: fixed-stride entries (binary-searchable), then the
        // term blob.
        write_u32(w, terms.len() as u32)?;
        let mut blob_off = directory_off + 4 + 18 * terms.len() as u64;
        for (term, &(offset, df)) in terms.iter().zip(directory.iter()) {
            write_u64(w, offset)?;
            write_u32(w, df)?;
            write_u32(w, blob_off as u32)?;
            write_u16(w, term.len() as u16)?;
            blob_off += term.len() as u64;
        }
        for term in &terms {
            w.write_all(term.as_bytes())?;
        }
        Ok(())
    }

    fn read_from(r: &mut &[u8]) -> io::Result<Self> {
        let invalid = |msg: &str| io::Error::new(io::ErrorKind::InvalidData, msg.to_string());
        let magic = take(r, 8)?;
        if magic == MAGIC {
            return Self::read_v3_from(r);
        }
        let has_lineage = if magic == MAGIC_V2 {
            true
        } else if magic == MAGIC_V1 {
            false
        } else {
            return Err(invalid("bad magic"));
        };
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
        let mut lineages = vec![None; n_texts];
        if has_lineage {
            for lineage in lineages.iter_mut() {
                let present = take(r, 1)?[0];
                if present != 0 {
                    let opinion_id = read_u64(r)?;
                    let cluster_id = read_u64(r)?;
                    let span_start = read_u32(r)?;
                    let span_end = read_u32(r)?;
                    *lineage = Some(DocLineage {
                        opinion_id,
                        cluster_id,
                        span_start,
                        span_end,
                    });
                }
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
            lineages,
        })
    }
}

fn write_u16<W: Write>(w: &mut W, v: u16) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
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

impl Bm25Index for Bm25Store {
    fn doc_count(&self) -> u64 {
        self.doc_count()
    }
    fn total_doc_length(&self) -> u64 {
        self.total_doc_length()
    }
    fn doc_length(&self, doc_id: u32) -> u32 {
        self.doc_length(doc_id)
    }
    fn df(&self, term: &str) -> u32 {
        self.postings.get(term).map_or(0, |p| p.len() as u32)
    }
    fn for_each_posting(&self, term: &str, f: &mut PostingCallback) {
        if let Some(postings) = self.postings.get(term) {
            for p in postings {
                f(p.doc_id, p.tf, &p.offsets);
            }
        }
    }
    fn text(&self, doc_id: u32) -> Option<String> {
        self.text(doc_id).map(str::to_string)
    }
    fn lineage(&self, doc_id: u32) -> Option<DocLineage> {
        self.lineage(doc_id)
    }
}

impl Bm25Store {
    /// Parse the v3 sections back into a heap store (used when a
    /// disk-resident shard receives more documents: the append path is
    /// bulk-load, build-in-memory-then-flush again).
    fn read_v3_from(r: &mut &[u8]) -> io::Result<Self> {
        let invalid = |msg: &str| io::Error::new(io::ErrorKind::InvalidData, msg.to_string());
        let total_length = read_u64(r)?;
        let texts_off = read_u64(r)?;
        let _lineages_off = read_u64(r)?;
        let _postings_off = read_u64(r)?;
        let _directory_off = read_u64(r)?;
        let n_slots = read_u32(r)? as usize;
        let mut doc_lengths = Vec::with_capacity(n_slots);
        for _ in 0..n_slots {
            doc_lengths.push(read_u32(r)?);
        }
        // texts (skip the on-disk index by tracking the cursor)
        let mut texts = Vec::with_capacity(n_slots);
        for _ in 0..n_slots {
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
        let _ = texts_off;
        // text_index (on disk; skip — we just rebuilt texts in heap)
        let mut lineages = Vec::with_capacity(n_slots);
        for _ in 0..n_slots {
            let _offset = read_u64(r)?;
            let _len = read_u32(r)?;
            lineages.push(None);
        }
        for lineage in lineages.iter_mut() {
            let present = take(r, 1)?[0];
            if present != 0 {
                let opinion_id = read_u64(r)?;
                let cluster_id = read_u64(r)?;
                let span_start = read_u32(r)?;
                let span_end = read_u32(r)?;
                *lineage = Some(DocLineage {
                    opinion_id,
                    cluster_id,
                    span_start,
                    span_end,
                });
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
        Ok(Self {
            postings,
            doc_lengths,
            total_length,
            texts,
            lineages,
        })
    }
}

/// A memory-mapped, disk-resident view of a v3 `.bm25` file. Postings
/// and document texts are read from the map on demand; the only heap
/// state is the per-document length table and a term count.
pub struct Bm25Reader {
    map: memmap2::Mmap,
    doc_lengths: Vec<u32>,
    doc_count: u64,
    total_length: u64,
    lineages_off: u64,
    directory_off: u64,
    n_terms: u32,
}

impl Bm25Reader {
    /// The next local doc id (number of document slots).
    pub fn next_doc_id(&self) -> u32 {
        self.doc_lengths.len() as u32
    }

    /// Open a v3 `.bm25` file read-only. Touches only the header, the
    /// doc-length table, and the directory header — no postings or text
    /// pages are faulted in until queries ask for them.
    pub fn open(path: &Path) -> io::Result<Self> {
        let invalid = |msg: &str| io::Error::new(io::ErrorKind::InvalidData, msg.to_string());
        let file = std::fs::File::open(path)?;
        let map = unsafe { memmap2::MmapOptions::new().map(&file)? };
        if map.len() < 52 || &map[..8] != MAGIC {
            return Err(invalid("not a v3 .bm25 file"));
        }
        let mut cur = 8usize;
        let rd_u64 = |cur: &mut usize| -> u64 {
            let v = u64::from_le_bytes(map[*cur..*cur + 8].try_into().unwrap());
            *cur += 8;
            v
        };
        let total_length = rd_u64(&mut cur);
        let _texts_off = rd_u64(&mut cur);
        let lineages_off = rd_u64(&mut cur);
        let _postings_off = rd_u64(&mut cur);
        let directory_off = rd_u64(&mut cur);
        let n_slots = u32::from_le_bytes(map[cur..cur + 4].try_into().unwrap()) as usize;
        cur += 4;
        let mut doc_lengths = Vec::with_capacity(n_slots);
        for _ in 0..n_slots {
            doc_lengths.push(u32::from_le_bytes(map[cur..cur + 4].try_into().unwrap()));
            cur += 4;
        }
        let doc_count = doc_lengths.iter().filter(|&&l| l > 0).count() as u64;
        let n_terms = u32::from_le_bytes(
            map[directory_off as usize..directory_off as usize + 4]
                .try_into()
                .unwrap(),
        );
        Ok(Self {
            map,
            doc_lengths,
            doc_count,
            total_length,
            lineages_off,
            directory_off,
            n_terms,
        })
    }

    fn directory_entry(&self, i: u32) -> (&[u8], u64, u32) {
        let e = self.directory_off as usize + 4 + 18 * i as usize;
        let postings_off = u64::from_le_bytes(self.map[e..e + 8].try_into().unwrap());
        let df = u32::from_le_bytes(self.map[e + 8..e + 12].try_into().unwrap());
        let blob_off = u32::from_le_bytes(self.map[e + 12..e + 16].try_into().unwrap()) as usize;
        let len = u16::from_le_bytes(self.map[e + 16..e + 18].try_into().unwrap()) as usize;
        (&self.map[blob_off..blob_off + len], postings_off, df)
    }

    fn directory_lookup(&self, term: &str) -> Option<(u64, u32)> {
        let (mut lo, mut hi) = (0u32, self.n_terms);
        while lo < hi {
            let mid = (lo + hi) / 2;
            let (bytes, _, _) = self.directory_entry(mid);
            match bytes.cmp(term.as_bytes()) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => {
                    let (_, off, df) = self.directory_entry(mid);
                    return Some((off, df));
                }
            }
        }
        None
    }
}

impl Bm25Index for Bm25Reader {
    fn doc_count(&self) -> u64 {
        self.doc_count
    }
    fn total_doc_length(&self) -> u64 {
        self.total_length
    }
    fn doc_length(&self, doc_id: u32) -> u32 {
        self.doc_lengths.get(doc_id as usize).copied().unwrap_or(0)
    }
    fn df(&self, term: &str) -> u32 {
        self.directory_lookup(term).map_or(0, |(_, df)| df)
    }
    fn for_each_posting(&self, term: &str, f: &mut PostingCallback) {
        let Some((off, df)) = self.directory_lookup(term) else {
            return;
        };
        // The directory offset points at the term's entry inside the
        // postings section: u32 term_len, term, u32 n_postings, then the
        // postings records. Decode sequentially from the map; offsets
        // land in a reusable buffer so per-posting allocation is one Vec.
        let mut cur = off as usize;
        let term_len = u32::from_le_bytes(self.map[cur..cur + 4].try_into().unwrap()) as usize;
        cur += 4 + term_len;
        let n = u32::from_le_bytes(self.map[cur..cur + 4].try_into().unwrap());
        cur += 4;
        debug_assert_eq!(n, df);
        let mut offsets: Vec<(u32, u32)> = Vec::new();
        for _ in 0..n {
            let doc_id = u32::from_le_bytes(self.map[cur..cur + 4].try_into().unwrap());
            let tf = u32::from_le_bytes(self.map[cur + 4..cur + 8].try_into().unwrap());
            let n_offsets =
                u32::from_le_bytes(self.map[cur + 8..cur + 12].try_into().unwrap()) as usize;
            cur += 12;
            offsets.clear();
            for _ in 0..n_offsets {
                let start = u32::from_le_bytes(self.map[cur..cur + 4].try_into().unwrap());
                let end = u32::from_le_bytes(self.map[cur + 4..cur + 8].try_into().unwrap());
                cur += 8;
                offsets.push((start, end));
            }
            f(doc_id, tf, &offsets);
        }
    }
    fn text(&self, doc_id: u32) -> Option<String> {
        let slot = doc_id as usize;
        if slot >= self.doc_lengths.len() {
            return None;
        }
        // The on-disk text index sits between the texts and lineages
        // sections: lineages_off - 12 * n_slots, 12-byte entries.
        let n_slots = self.doc_lengths.len() as u64;
        let index_start = self.lineages_off - 12 * n_slots;
        let e = (index_start + 12 * doc_id as u64) as usize;
        let offset = u64::from_le_bytes(self.map[e..e + 8].try_into().unwrap());
        let len = u32::from_le_bytes(self.map[e + 8..e + 12].try_into().unwrap());
        if len == u32::MAX {
            return None;
        }
        let bytes = &self.map[offset as usize..offset as usize + len as usize];
        String::from_utf8(bytes.to_vec()).ok()
    }
    fn lineage(&self, doc_id: u32) -> Option<DocLineage> {
        let slot = doc_id as usize;
        if slot >= self.doc_lengths.len() {
            return None;
        }
        let e = self.lineages_off as usize + 25 * slot;
        if self.map[e] == 0 {
            return None;
        }
        let opinion_id = u64::from_le_bytes(self.map[e + 1..e + 9].try_into().unwrap());
        let cluster_id = u64::from_le_bytes(self.map[e + 9..e + 17].try_into().unwrap());
        let span_start = u32::from_le_bytes(self.map[e + 17..e + 21].try_into().unwrap());
        let span_end = u32::from_le_bytes(self.map[e + 21..e + 25].try_into().unwrap());
        Some(DocLineage {
            opinion_id,
            cluster_id,
            span_start,
            span_end,
        })
    }
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
