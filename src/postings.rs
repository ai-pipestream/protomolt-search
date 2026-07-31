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
//!   the v5 format atomically next to the shard's `.tv` as `<index>.bm25`.
//! - [`Bm25Reader`] — the disk-resident shape. The v5 file is memory
//!   mapped; postings slices and document texts are read from the map on
//!   demand (the OS page cache is the buffer pool, the Lucene model), so
//!   a shard far larger than RAM serves from a few MB of heap (per-doc
//!   length/offset tables only). v3/v4 files stay readable.
//!
//! v5 (`TVBM2505`, see `docs/block-max.md`) splits each term into three
//! runs so the scorer never decodes occurrence offsets for non-survivors:
//! a fixed-stride doc run (12 B/posting: `doc_id | tf | occ_start`, one
//! trailing sentinel `occ_start` per term), an occurrence run (`start,
//! end` u32 pairs in posting order), and a skip run of per-block `(tf,
//! dl)` Pareto frontiers (level 0: one record per 128 postings; level 1:
//! one per 32 level-0 blocks). Stage 1 writes the skip run but no scorer
//! consumes it yet.

use std::collections::HashMap;
use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

const MAGIC_V1: &[u8; 8] = b"TVBM2501";
const MAGIC_V2: &[u8; 8] = b"TVBM2502";
/// v3 stored ABSOLUTE term-blob offsets as u32, which overflow once the
/// file passes 4 GiB; kept readable for files below that size.
const MAGIC_V3: &[u8; 8] = b"TVBM2503";
/// v4 layout: identical to v3 except directory blob offsets are relative
/// to the term blob's start (the blob is at most a few hundred MB, so
/// u32 offsets are safe at any file size).
const MAGIC_V4: &[u8; 8] = b"TVBM2504";
/// v5 layout: per-term doc run / occurrence run / skip run (see the
/// module docs and `docs/block-max.md`); 34 B directory entries.
const MAGIC_V5: &[u8; 8] = b"TVBM2505";

/// Postings per level-0 skip block (Lucene uses 128/256; 128 here).
const BLOCK: usize = 128;
/// Level-0 blocks per level-1 record.
const LEVEL1_FACTOR: usize = 32;
/// Max `(tf, dl)` pairs kept per skip record; larger frontiers are
/// COLLAPSED (never dropped) to their dominating corner, which only
/// raises the bound — see `docs/block-max.md`.
const MAX_FRONTIER: usize = 8;

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
    /// Call `f` for every posting of `term`, in doc-id order, as
    /// `(doc_id, tf)` — the scored path, which must NOT touch occurrence
    /// bytes. The default falls back to [`Self::for_each_posting`] and
    /// drops the offsets; the v5 reader overrides it to walk only the
    /// fixed-stride doc run.
    fn for_each_doc_tf(&self, term: &str, f: &mut dyn FnMut(u32, u32)) {
        self.for_each_posting(term, &mut |doc_id, tf, _offsets| {
            f(doc_id, tf);
        });
    }
    /// The occurrence spans `(start, end)` of `term` within `doc_id`
    /// (empty when the doc has no posting for the term or was ingested
    /// SCORING_ONLY). The default scans [`Self::for_each_posting`]; the
    /// v5 reader overrides it to binary-search the doc run and decode
    /// only that posting's occurrence slice.
    fn posting_offsets(&self, term: &str, doc_id: u32) -> Vec<(u32, u32)> {
        let mut found = Vec::new();
        self.for_each_posting(term, &mut |d, _tf, offsets| {
            if d == doc_id {
                found = offsets.to_vec();
            }
        });
        found
    }
    /// An impact cursor over `term`'s skip run and doc run — the
    /// block-max surface (`docs/block-max.md`). `None` for the heap
    /// store and v3/v4 files, which keep the exhaustive scorers.
    fn impacts(&self, term: &str) -> Option<ImpactCursor<'_>> {
        let _ = term;
        None
    }
    /// Whether [`Self::impacts`] would return a cursor, WITHOUT
    /// building one (the v5 reader answers with a directory lookup
    /// only). Used by callers selecting the pruned path, which builds
    /// the cursors itself.
    fn has_impacts(&self, term: &str) -> bool {
        self.impacts(term).is_some()
    }
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

    /// Sizes of the sections every format version shares (doc_lengths,
    /// texts, text_index, lineages) plus the sorted term list.
    fn common_section_sizes(&self) -> (u64, u64, u64, u64, Vec<&String>) {
        let n_slots = self.doc_lengths.len() as u64;
        let texts_size: u64 = self
            .texts
            .iter()
            .map(|t| 4 + t.as_ref().map_or(0, |s| s.len() as u64))
            .sum();
        let lineages_size: u64 = self
            .lineages
            .iter()
            .map(|l| if l.is_some() { 25 } else { 1 })
            .sum();
        let mut terms: Vec<&String> = self.postings.keys().collect();
        terms.sort();
        (4 * n_slots, texts_size, 12 * n_slots, lineages_size, terms)
    }

    /// Write the sections between the header and the postings section
    /// (identical bytes in every format version).
    fn write_common_sections<W: Write>(&self, w: &mut W, texts_off: u64) -> io::Result<()> {
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
                    // The absent marker still occupies 4 bytes; skipping
                    // this advance pointed every later text_index entry 4
                    // bytes early per gap slot.
                    cursor += 4;
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
        Ok(())
    }

    /// The v5 layout (see the module docs and `docs/block-max.md`).
    /// Sections in file order, all offsets precomputed and written into
    /// the fixed header so the reader never walks the file to find them:
    ///
    /// ```text
    /// magic "TVBM2505" | header (total_length, section offsets, n_slots)
    /// doc_lengths (n_slots x u32)
    /// texts (n_slots x (u32 len | bytes), len == u32::MAX when absent)
    /// text_index (n_slots x (u64 offset, u32 len))   <- on-disk text directory
    /// lineages (n_slots x (u8 flag + 24B))
    /// postings (u32 n_terms, then per term, sorted by term:
    ///   doc run (df x 12B (doc_id, tf, occ_start) + 4B sentinel occ_start)
    ///   occurrence run (8B (start, end) pairs, posting order)
    ///   skip run (see SkipRunBuilder))
    /// directory (u32 n_terms, then n_terms x 34B fixed-stride entries
    ///   (u64 doc_run_off, u64 skip_run_off, u64 occ_run_off, u32 df,
    ///   u32 blob_off, u16 term_len), then the term blob)
    /// ```
    fn write_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        let header_size = 8 + 8 + 8 * 4 + 4;
        let (doc_lengths_size, texts_size, text_index_size, lineages_size, terms) =
            self.common_section_sizes();
        // Size pass. The skip run's size needs the frontier computation,
        // so run the same builder over a sink; the write pass below
        // re-runs it for real (deterministic, so the sizes agree).
        let mut run_sizes: Vec<(u64, u64)> = Vec::with_capacity(terms.len()); // (occ, skip)
        for term in &terms {
            let postings = &self.postings[*term];
            let occ_bytes: u64 = postings.iter().map(|p| 8 * p.offsets.len() as u64).sum();
            let mut skip = SkipRunBuilder::new();
            let mut sink = io::sink();
            for p in postings {
                skip.push(p.tf, self.doc_length(p.doc_id), p.doc_id, &mut sink)?;
            }
            let (l0_bytes, l1) = skip.finish(&mut sink)?;
            run_sizes.push((occ_bytes, skip_run_size(l0_bytes, &l1)));
        }
        let postings_size: u64 = 4 + terms
            .iter()
            .enumerate()
            .map(|(i, t)| {
                let df = self.postings[*t].len() as u64;
                12 * df + 4 + run_sizes[i].0 + run_sizes[i].1
            })
            .sum::<u64>();

        let doc_lengths_off = header_size as u64;
        let texts_off = doc_lengths_off + doc_lengths_size;
        let text_index_off = texts_off + texts_size;
        let lineages_off = text_index_off + text_index_size;
        let postings_off = lineages_off + lineages_size;
        let directory_off = postings_off + postings_size;

        w.write_all(MAGIC_V5)?;
        write_u64(w, self.total_length)?;
        write_u64(w, texts_off)?;
        write_u64(w, lineages_off)?;
        write_u64(w, postings_off)?;
        write_u64(w, directory_off)?;
        write_u32(w, self.doc_lengths.len() as u32)?;
        self.write_common_sections(w, texts_off)?;

        // postings section: doc run streams straight out; the occurrence
        // run and the level-0 skip records stage in per-term buffers
        // (the heap store already holds every posting, so the stage is
        // never the memory ceiling) and are appended after the sentinel.
        write_u32(w, terms.len() as u32)?;
        let mut directory: Vec<(u64, u64, u64, u32)> = Vec::with_capacity(terms.len());
        let mut cursor = postings_off + 4;
        for (i, term) in terms.iter().enumerate() {
            let postings = &self.postings[*term];
            let df = postings.len() as u64;
            let (occ_bytes, skip_bytes) = run_sizes[i];
            let doc_run_off = cursor;
            let occ_run_off = doc_run_off + 12 * df + 4;
            let skip_run_off = occ_run_off + occ_bytes;
            directory.push((doc_run_off, skip_run_off, occ_run_off, df as u32));
            let mut occ_stage: Vec<u8> = Vec::with_capacity(occ_bytes as usize);
            let mut skip_l0: Vec<u8> = Vec::new();
            let mut skip = SkipRunBuilder::new();
            let mut occ_start = 0u32;
            for p in postings {
                occ_start = push_posting_v5(
                    w,
                    &mut occ_stage,
                    &mut skip,
                    &mut skip_l0,
                    p.doc_id,
                    p.tf,
                    &p.offsets,
                    self.doc_length(p.doc_id),
                    occ_start,
                )?;
            }
            write_u32(w, occ_start)?; // sentinel
            w.write_all(&occ_stage)?;
            let (l0_bytes, l1) = skip.finish(&mut skip_l0)?;
            debug_assert_eq!(l0_bytes, skip_l0.len() as u64);
            debug_assert_eq!(skip_bytes, skip_run_size(l0_bytes, &l1));
            write_skip_run(w, &skip_l0, &l1)?;
            cursor = skip_run_off + skip_bytes;
        }
        // directory: fixed-stride entries (binary-searchable), then the
        // term blob.
        write_u32(w, terms.len() as u32)?;
        let mut blob_off = 0u64; // relative to the term blob start
        for (term, &(doc_off, skip_off, occ_off, df)) in terms.iter().zip(directory.iter()) {
            write_u64(w, doc_off)?;
            write_u64(w, skip_off)?;
            write_u64(w, occ_off)?;
            write_u32(w, df)?;
            write_u32(w, u32::try_from(blob_off).expect("term blob exceeds u32"))?;
            write_u16(w, term.len() as u16)?;
            blob_off += term.len() as u64;
        }
        for term in &terms {
            w.write_all(term.as_bytes())?;
        }
        Ok(())
    }

    /// Write the v4 format. Exists for benchmarking (v4-vs-v5 scorer
    /// comparisons) and migration checks; new shards are always v5.
    pub fn write_v4_for_bench<W: Write>(&self, w: &mut W) -> io::Result<()> {
        self.write_v4_to(w)
    }

    /// The v3/v4 layout. Sections in file order, all offsets precomputed
    /// and written into the fixed header so the reader never walks the
    /// file to find them:
    ///
    /// ```text
    /// magic "TVBM2504" | header (total_length, section offsets, n_slots)
    /// doc_lengths (n_slots x u32)
    /// texts (n_slots x (u32 len | bytes), len == u32::MAX when absent)
    /// text_index (n_slots x (u64 offset, u32 len))   <- on-disk text directory
    /// lineages (n_slots x (u8 flag + 24B))
    /// postings (u32 n_terms, then per-term entries, sorted)
    /// directory (u32 n_terms, then n_terms x 18B fixed-stride entries
    ///   (u64 postings_off, u32 df, u32 blob_off, u16 term_len), then the
    ///   term blob) — binary-searchable by term
    /// ```
    fn write_v4_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
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

        w.write_all(MAGIC_V4)?;
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
                    // The absent marker still occupies 4 bytes; skipping
                    // this advance pointed every later text_index entry 4
                    // bytes early per gap slot.
                    cursor += 4;
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
        let mut blob_off = 0u64; // relative to the term blob start (v4)
        for (term, &(offset, df)) in terms.iter().zip(directory.iter()) {
            write_u64(w, offset)?;
            write_u32(w, df)?;
            write_u32(w, u32::try_from(blob_off).expect("term blob exceeds u32"))?;
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
        if magic == MAGIC_V5 {
            return Self::read_v5_from(r);
        }
        if magic == MAGIC_V4 || magic == MAGIC_V3 {
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

// --- v5 skip-run machinery ---------------------------------------------

/// The `(tf, dl)` Pareto frontier of one block of postings, sorted by tf
/// ascending (dl ascending with it, strictly), dominated pairs pruned,
/// collapsed to at most [`MAX_FRONTIER`] entries.
///
/// `tf_norm` is non-decreasing in `tf` and non-increasing in `dl` for
/// every `k1 >= 0`, `b >= 0`, so a dominated pair (`tf' >= tf` and
/// `dl' <= dl` for some other pair) can never be the block maximum under
/// any parameter choice and is pruned at build time. When the frontier
/// exceeds the cap, adjacent entries are COLLAPSED into their dominating
/// corner `(max tf, min dl)` — never dropped, because which entry
/// maximizes `tf_norm` depends on the query's `avgdl`; collapsing only
/// ever raises the bound, costing tightness and never exactness.
fn pareto_frontier(pairs: &[(u32, u32)]) -> Vec<(u32, u32)> {
    let mut sorted: Vec<(u32, u32)> = pairs.to_vec();
    // tf descending, dl ascending within equal tf: walking forward and
    // keeping only strictly decreasing dl drops every dominated pair
    // (an equal-tf higher-dl pair is seen after its dominator).
    sorted.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    let mut kept: Vec<(u32, u32)> = Vec::new();
    let mut min_dl = u32::MAX;
    for &(tf, dl) in &sorted {
        if dl < min_dl {
            kept.push((tf, dl));
            min_dl = dl;
        }
    }
    kept.reverse();
    while kept.len() > MAX_FRONTIER {
        // Collapse the closest adjacent pair into its dominating corner
        // (max tf, min dl) = (kept[i+1].0, kept[i].1). The staircase
        // stays strictly increasing in both coordinates.
        let mut best = 0;
        let mut best_gap = u64::MAX;
        for i in 0..kept.len() - 1 {
            let gap = u64::from(kept[i + 1].0 - kept[i].0) + u64::from(kept[i + 1].1 - kept[i].1);
            if gap < best_gap {
                best_gap = gap;
                best = i;
            }
        }
        let corner = (kept[best + 1].0, kept[best].1);
        kept.splice(best..=best + 1, [corner]);
    }
    kept
}

/// One flushed level-1 skip record (covers up to [`LEVEL1_FACTOR`]
/// level-0 blocks, i.e. up to 4096 postings).
struct L1Record {
    last_doc_id: u32,
    /// Offset of the group's first level-0 record, relative to the start
    /// of the term's skip run.
    l0_off: u64,
    pairs: Vec<(u32, u32)>,
}

impl L1Record {
    fn encoded_len(&self) -> u64 {
        13 + 8 * self.pairs.len() as u64
    }
}

/// Streaming skip-run accumulator for ONE term: fed `(tf, dl, doc_id)`
/// per posting in doc-id order, it writes each flushed level-0 record to
/// `l0_out` and accumulates the (few: df/4096) level-1 records in heap.
/// State per term is the current 128-posting block plus the current
/// level-1 group's merged frontier entries (<= 32 * 8 pairs), so build
/// memory stays O(1) in the term's df.
///
/// Skip-run layout (all integers little-endian):
///
/// ```text
/// u64 l1_region_off          <- offset of the first level-1 record,
///                               relative to the skip run's start
/// level-0 records:           one per 128-posting block, doc-id order
///   u32 last_doc_id, u8 n_pairs, n_pairs x (u32 tf, u32 dl)
/// level-1 records:           one per 32 level-0 blocks
///   u32 last_doc_id, u64 l0_off, u8 n_pairs, n_pairs x (u32 tf, u32 dl)
/// ```
///
/// The level-1 region needs the explicit offset because level-0 records
/// are variable stride, so its start is not derivable from df alone.
struct SkipRunBuilder {
    /// `(tf, dl)` of the current level-0 block (<= BLOCK entries).
    block: Vec<(u32, u32)>,
    /// Merged frontier entries of the current level-1 group.
    l1_pairs: Vec<(u32, u32)>,
    l1_records: Vec<L1Record>,
    l0_in_group: usize,
    /// Bytes written to `l0_out` so far.
    l0_bytes: u64,
    last_doc_id: u32,
}

impl SkipRunBuilder {
    fn new() -> Self {
        Self {
            block: Vec::with_capacity(BLOCK),
            l1_pairs: Vec::new(),
            l1_records: Vec::new(),
            l0_in_group: 0,
            l0_bytes: 0,
            last_doc_id: 0,
        }
    }

    fn push<W: Write>(&mut self, tf: u32, dl: u32, doc_id: u32, l0_out: &mut W) -> io::Result<()> {
        self.block.push((tf, dl));
        self.last_doc_id = doc_id;
        if self.block.len() == BLOCK {
            self.flush_block(l0_out)?;
        }
        Ok(())
    }

    fn flush_block<W: Write>(&mut self, l0_out: &mut W) -> io::Result<()> {
        if self.block.is_empty() {
            return Ok(());
        }
        if self.l0_in_group == 0 {
            // First level-0 record of a new level-1 group: its offset is
            // 8 (the prefix) plus the level-0 bytes written so far.
            self.l1_records.push(L1Record {
                last_doc_id: 0, // patched when the group closes
                l0_off: 8 + self.l0_bytes,
                pairs: Vec::new(),
            });
        }
        let frontier = pareto_frontier(&self.block);
        write_u32(l0_out, self.last_doc_id)?;
        l0_out.write_all(&[frontier.len() as u8])?;
        for &(tf, dl) in &frontier {
            write_u32(l0_out, tf)?;
            write_u32(l0_out, dl)?;
        }
        self.l0_bytes += 5 + 8 * frontier.len() as u64;
        self.l1_pairs.extend_from_slice(&frontier);
        self.l0_in_group += 1;
        self.block.clear();
        if self.l0_in_group == LEVEL1_FACTOR {
            self.close_group();
        }
        Ok(())
    }

    /// Close the current level-1 group: frontier of the merged block
    /// frontiers (a bound over the whole group, by monotonicity).
    fn close_group(&mut self) {
        let group = self.l1_records.last_mut().expect("open group");
        group.last_doc_id = self.last_doc_id;
        group.pairs = pareto_frontier(&self.l1_pairs);
        self.l1_pairs.clear();
        self.l0_in_group = 0;
    }

    /// Flush any partial block/group at the end of a term; returns the
    /// level-0 bytes and the level-1 records.
    fn finish<W: Write>(mut self, l0_out: &mut W) -> io::Result<(u64, Vec<L1Record>)> {
        self.flush_block(l0_out)?;
        if self.l0_in_group > 0 {
            self.close_group();
        }
        Ok((self.l0_bytes, self.l1_records))
    }
}

/// Write a term's complete skip run: the level-1-region prefix, the
/// level-0 bytes (already encoded), then the level-1 records.
fn write_skip_run<W: Write>(w: &mut W, l0: &[u8], l1: &[L1Record]) -> io::Result<()> {
    write_u64(w, 8 + l0.len() as u64)?;
    w.write_all(l0)?;
    for rec in l1 {
        write_u32(w, rec.last_doc_id)?;
        write_u64(w, rec.l0_off)?;
        w.write_all(&[rec.pairs.len() as u8])?;
        for &(tf, dl) in &rec.pairs {
            write_u32(w, tf)?;
            write_u32(w, dl)?;
        }
    }
    Ok(())
}

/// Skip-run byte size without writing anything (the store's size pass).
fn skip_run_size(l0_bytes: u64, l1: &[L1Record]) -> u64 {
    8 + l0_bytes + l1.iter().map(L1Record::encoded_len).sum::<u64>()
}

/// Feed one posting into the three v5 runs: doc-run entry to `doc_out`,
/// occurrence pairs to `occ_out`, `(tf, dl)` to the skip builder (whose
/// level-0 records stream to `skip_l0_out`). `occ_start` is the running
/// occurrence count; returns the new count.
fn push_posting_v5<D: Write, O: Write, S: Write>(
    doc_out: &mut D,
    occ_out: &mut O,
    skip: &mut SkipRunBuilder,
    skip_l0_out: &mut S,
    doc_id: u32,
    tf: u32,
    offsets: &[(u32, u32)],
    dl: u32,
    occ_start: u32,
) -> io::Result<u32> {
    write_u32(doc_out, doc_id)?;
    write_u32(doc_out, tf)?;
    write_u32(doc_out, occ_start)?;
    for &(start, end) in offsets {
        write_u32(occ_out, start)?;
        write_u32(occ_out, end)?;
    }
    skip.push(tf, dl, doc_id, skip_l0_out)?;
    Ok(occ_start + offsets.len() as u32)
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
    fn for_each_doc_tf(&self, term: &str, f: &mut dyn FnMut(u32, u32)) {
        if let Some(postings) = self.postings.get(term) {
            for p in postings {
                f(p.doc_id, p.tf);
            }
        }
    }
    fn posting_offsets(&self, term: &str, doc_id: u32) -> Vec<(u32, u32)> {
        let Some(postings) = self.postings.get(term) else {
            return Vec::new();
        };
        match postings.binary_search_by_key(&doc_id, |p| p.doc_id) {
            Ok(i) => postings[i].offsets.clone(),
            Err(_) => Vec::new(),
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

    /// Parse a v5 file back into a heap store (same caller contract as
    /// [`Self::read_v3_from`]: a disk-resident shard about to receive more
    /// documents). The skip run is not needed in heap form and is skipped.
    fn read_v5_from(r: &mut &[u8]) -> io::Result<Self> {
        let invalid = |msg: &str| io::Error::new(io::ErrorKind::InvalidData, msg.to_string());
        // `r` starts at file offset 8 (magic consumed); section offsets
        // are absolute file offsets.
        let all: &[u8] = r;
        let at = |off: u64, len: u64| -> io::Result<&[u8]> {
            let i = (off as usize)
                .checked_sub(8)
                .ok_or_else(|| invalid("bad offset"))?;
            all.get(i..i + len as usize)
                .ok_or_else(|| invalid("offset out of range"))
        };
        let u32_at = |off: u64| -> io::Result<u32> {
            Ok(u32::from_le_bytes(at(off, 4)?.try_into().expect("4 bytes")))
        };
        let u64_at = |off: u64| -> io::Result<u64> {
            Ok(u64::from_le_bytes(at(off, 8)?.try_into().expect("8 bytes")))
        };
        let total_length = read_u64(r)?;
        let _texts_off = read_u64(r)?;
        let _lineages_off = read_u64(r)?;
        let _postings_off = read_u64(r)?;
        let directory_off = read_u64(r)?;
        let n_slots = read_u32(r)? as usize;
        let mut doc_lengths = Vec::with_capacity(n_slots);
        for _ in 0..n_slots {
            doc_lengths.push(read_u32(r)?);
        }
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
        // text_index (on disk; skip — we just rebuilt texts in heap)
        take(r, 12 * n_slots)?;
        let mut lineages = vec![None; n_slots];
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
        // postings: locate each term's runs through the directory.
        let n_terms = u32_at(directory_off)? as usize;
        let blob_start = directory_off + 4 + 34 * n_terms as u64;
        let mut postings = HashMap::with_capacity(n_terms);
        for i in 0..n_terms {
            let e = directory_off + 4 + 34 * i as u64;
            let doc_run_off = u64_at(e)?;
            let occ_run_off = u64_at(e + 16)?;
            let df = u32_at(e + 24)? as usize;
            let blob_off = u64::from(u32_at(e + 28)?);
            let term_len = u64::from(u16::from_le_bytes(
                at(e + 32, 2)?.try_into().expect("2 bytes"),
            ));
            let term = String::from_utf8(at(blob_start + blob_off, term_len)?.to_vec())
                .map_err(|_| invalid("invalid utf-8 in term"))?;
            let mut plist = Vec::with_capacity(df);
            for j in 0..df {
                let p = doc_run_off + 12 * j as u64;
                let doc_id = u32_at(p)?;
                let tf = u32_at(p + 4)?;
                let occ_start = u64::from(u32_at(p + 8)?);
                // The next entry's occ_start (or the trailing sentinel
                // for the last posting) bounds this occurrence slice.
                let occ_end = u64::from(if j + 1 < df {
                    u32_at(p + 12 + 8)?
                } else {
                    u32_at(doc_run_off + 12 * df as u64)?
                });
                let mut offsets = Vec::with_capacity((occ_end - occ_start) as usize);
                for o in occ_start..occ_end {
                    offsets.push((u32_at(occ_run_off + 8 * o)?, u32_at(occ_run_off + 8 * o + 4)?));
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

/// Disk-spilling builder producing the SAME v5 file as [`Bm25Store::save`]
/// (byte-identical), with bounded memory at any corpus size.
///
/// The in-memory store keeps every posting and every text in heap until
/// flush — ~100 GB for a 10M-chunk shard. This builder instead:
///
/// - streams document texts to a spill file AT ADD TIME, already in the
///   final texts-section encoding (gap slots get their `u32::MAX` marker
///   immediately), so flush byte-copies the file into place;
/// - accumulates postings in a bounded buffer; when it fills, the buffer
///   is sorted by `(term, doc_id)` and written out as a run. Doc ids
///   only grow, so runs never overlap within a term and the flush-time
///   merge is a heap of run heads concatenating per-term lists in run
///   order — one sequential pass, no random access.
///
/// Heap while building: the sort buffer (default 256 MB) plus the per-doc
/// length/lineage tables. A spilling shard is NOT searchable (that would
/// mean scanning every run per term); flush it first.
pub struct SpillBuilder {
    dir: PathBuf,
    /// Pending postings: (term, doc_id, tf, offsets).
    buf: Vec<(String, u32, u32, Vec<(u32, u32)>)>,
    buf_bytes: usize,
    cap_bytes: usize,
    runs: Vec<PathBuf>,
    /// Texts spill, encoded exactly as the v3 texts section.
    texts: io::BufWriter<std::fs::File>,
    texts_bytes: u64,
    /// Per-slot text byte length (`u32::MAX` = absent); sizes the final
    /// sections without rereading the spill.
    text_lens: Vec<u32>,
    doc_lengths: Vec<u32>,
    lineages: Vec<Option<DocLineage>>,
    total_length: u64,
    doc_count: u64,
    /// Write the v4 format instead of v5 (benchmarking/migration only).
    v4_only: bool,
}

impl SpillBuilder {
    /// Default sort-buffer capacity before a run is spilled.
    pub const DEFAULT_BUF_BYTES: usize = 256 << 20;

    /// Create a builder spilling into `dir` (created; must not hold a
    /// previous builder's files). `finish` writes the v5 format.
    pub fn create(dir: &Path) -> io::Result<Self> {
        Self::create_format(dir, false)
    }

    /// A builder whose `finish` writes the v4 format. Exists for
    /// benchmarking (v4-vs-v5 scorer comparisons) and migration checks;
    /// new shards are always v5.
    pub fn create_v4_for_bench(dir: &Path) -> io::Result<Self> {
        Self::create_format(dir, true)
    }

    fn create_format(dir: &Path, v4_only: bool) -> io::Result<Self> {
        std::fs::create_dir_all(dir)?;
        let texts = io::BufWriter::new(std::fs::File::create(dir.join("texts.spill"))?);
        Ok(Self {
            dir: dir.to_path_buf(),
            buf: Vec::new(),
            buf_bytes: 0,
            cap_bytes: Self::DEFAULT_BUF_BYTES,
            runs: Vec::new(),
            texts,
            texts_bytes: 0,
            text_lens: Vec::new(),
            doc_lengths: Vec::new(),
            lineages: Vec::new(),
            total_length: 0,
            doc_count: 0,
            v4_only,
        })
    }

    /// Override the sort-buffer capacity (tests force multi-run merges
    /// with tiny caps).
    pub fn with_buffer_bytes(mut self, cap: usize) -> Self {
        self.cap_bytes = cap.max(1);
        self
    }

    /// The number of document slots ever allocated (the next local doc id).
    pub fn next_doc_id(&self) -> u32 {
        self.doc_lengths.len() as u32
    }

    /// Number of documents with postings.
    pub fn doc_count(&self) -> u64 {
        self.doc_count
    }

    /// Sum of all document lengths (BM25 avgdl numerator).
    pub fn total_doc_length(&self) -> u64 {
        self.total_length
    }

    /// Append one analyzed document; same contract as
    /// [`Bm25Store::add_document_with_lineage`].
    pub fn add_document_with_lineage(
        &mut self,
        doc_id: u32,
        text: String,
        doc: AnalyzedDoc,
        lineage: Option<DocLineage>,
    ) -> io::Result<()> {
        let slot = doc_id as usize;
        assert!(
            slot >= self.doc_lengths.len(),
            "doc id {doc_id} already used"
        );
        // Gap slots (ids consumed by the vector side) are written to the
        // spill NOW so the final texts section is a straight copy.
        while self.doc_lengths.len() < slot {
            write_u32(&mut self.texts, u32::MAX)?;
            self.texts_bytes += 4;
            self.text_lens.push(u32::MAX);
            self.doc_lengths.push(0);
            self.lineages.push(None);
        }
        write_u32(&mut self.texts, text.len() as u32)?;
        self.texts.write_all(text.as_bytes())?;
        self.texts_bytes += 4 + text.len() as u64;
        self.text_lens.push(text.len() as u32);
        self.doc_lengths.push(doc.length);
        self.lineages.push(lineage);
        self.total_length += u64::from(doc.length);
        if doc.length > 0 {
            self.doc_count += 1;
        }
        for (term, tf, offsets) in doc.terms {
            self.buf_bytes += term.len() + 24 + 16 * offsets.len();
            self.buf.push((term, doc_id, tf, offsets));
        }
        if self.buf_bytes >= self.cap_bytes {
            self.spill_run()?;
        }
        Ok(())
    }

    /// Sort the buffer by `(term, doc_id)` and write it as one run.
    fn spill_run(&mut self) -> io::Result<()> {
        if self.buf.is_empty() {
            return Ok(());
        }
        self.buf
            .sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
        let path = self.dir.join(format!("run-{:06}", self.runs.len()));
        let mut w = io::BufWriter::new(std::fs::File::create(&path)?);
        let mut i = 0;
        while i < self.buf.len() {
            let term = &self.buf[i].0;
            let group_end = self.buf[i..]
                .iter()
                .position(|e| &e.0 != term)
                .map_or(self.buf.len(), |p| i + p);
            write_u16(&mut w, term.len() as u16)?;
            w.write_all(term.as_bytes())?;
            write_u32(&mut w, (group_end - i) as u32)?;
            for (_, doc_id, tf, offsets) in &self.buf[i..group_end] {
                write_u32(&mut w, *doc_id)?;
                write_u32(&mut w, *tf)?;
                write_u32(&mut w, offsets.len() as u32)?;
                for &(start, end) in offsets {
                    write_u32(&mut w, start)?;
                    write_u32(&mut w, end)?;
                }
            }
            i = group_end;
        }
        w.flush()?;
        self.runs.push(path);
        self.buf.clear();
        self.buf_bytes = 0;
        Ok(())
    }

    /// Merge the runs and assemble the file at `path` (atomically:
    /// write tmp, rename). The spill directory is removed on success.
    /// Writes v5, byte-identical to [`Bm25Store::save`] on the same
    /// corpus, unless built with [`Self::create_v4_for_bench`].
    pub fn finish(&mut self, path: &Path) -> io::Result<()> {
        if self.v4_only {
            self.finish_v4(path)
        } else {
            self.finish_v5(path)
        }
    }

    /// Merge the runs and assemble the v5 file at `path`. Merge side of
    /// the v5 format: the doc run streams into the postings body while
    /// occurrence bytes divert to a per-term stage file and the skip
    /// builder accumulates `(tf, dl)` per 128-posting block — all
    /// single-pass with O(1) state per term, so the sub-1 GB build
    /// memory the spill builder buys is untouched.
    fn finish_v5(&mut self, path: &Path) -> io::Result<()> {
        self.spill_run()?;
        self.texts.flush()?;

        let postings_body = self.dir.join("postings.body");
        let occ_stage_path = self.dir.join("occ.stage");
        // (term, doc_run_rel, skip_run_rel, occ_run_rel, df), offsets
        // relative to the postings-section body start.
        let mut directory: Vec<(String, u64, u64, u64, u32)> = Vec::new();
        {
            let mut out = io::BufWriter::new(std::fs::File::create(&postings_body)?);
            let mut heads: Vec<RunHead> = Vec::new();
            for run in &self.runs {
                if let Some(head) = RunHead::open(run)? {
                    heads.push(head);
                }
            }
            let mut cursor = 0u64;
            while !heads.is_empty() {
                // Smallest term across run heads; runs are time-ordered,
                // and ids only grow, so draining matching heads in run
                // order keeps postings doc-ascending.
                let term = heads
                    .iter()
                    .map(|h| h.term.clone())
                    .min()
                    .expect("nonempty heads");
                let doc_rel = cursor;
                let mut df = 0u64;
                let mut occ_bytes = 0u64;
                let mut occ_start = 0u32;
                let mut skip_l0: Vec<u8> = Vec::new();
                let mut skip = SkipRunBuilder::new();
                {
                    let mut occ_stage =
                        io::BufWriter::new(std::fs::File::create(&occ_stage_path)?);
                    let mut idx = 0;
                    while idx < heads.len() {
                        if heads[idx].term == term {
                            for _ in 0..heads[idx].n_postings {
                                let (doc_id, tf, offsets) = heads[idx].next_posting_raw()?;
                                let dl = self.doc_lengths[doc_id as usize];
                                write_u32(&mut out, doc_id)?;
                                write_u32(&mut out, tf)?;
                                write_u32(&mut out, occ_start)?;
                                // The run encoding's offset bytes are
                                // exactly the v5 occurrence-run pairs.
                                occ_stage.write_all(&offsets)?;
                                skip.push(tf, dl, doc_id, &mut skip_l0)?;
                                occ_start += offsets.len() as u32 / 8;
                                occ_bytes += offsets.len() as u64;
                                df += 1;
                            }
                            if !heads[idx].advance()? {
                                heads.remove(idx);
                                continue;
                            }
                        }
                        idx += 1;
                    }
                    occ_stage.flush()?;
                }
                write_u32(&mut out, occ_start)?; // sentinel
                {
                    let mut stage = std::fs::File::open(&occ_stage_path)?;
                    io::copy(&mut stage, &mut out)?;
                }
                let (l0_bytes, l1) = skip.finish(&mut skip_l0)?;
                debug_assert_eq!(l0_bytes, skip_l0.len() as u64);
                let occ_rel = doc_rel + 12 * df + 4;
                let skip_rel = occ_rel + occ_bytes;
                let skip_bytes = skip_run_size(l0_bytes, &l1);
                write_skip_run(&mut out, &skip_l0, &l1)?;
                directory.push((term, doc_rel, skip_rel, occ_rel, df as u32));
                cursor = skip_rel + skip_bytes;
            }
            out.flush()?;
        }
        let postings_body_len = std::fs::metadata(&postings_body)?.len();

        // Section geometry, identical to Bm25Store::write_to.
        let n_slots = self.doc_lengths.len() as u64;
        let header_size = 8 + 8 + 8 * 4 + 4;
        let doc_lengths_off = header_size as u64;
        let texts_off = doc_lengths_off + 4 * n_slots;
        let text_index_off = texts_off + self.texts_bytes;
        let lineages_off = text_index_off + 12 * n_slots;
        let lineages_size: u64 = self
            .lineages
            .iter()
            .map(|l| if l.is_some() { 25 } else { 1 })
            .sum();
        let postings_off = lineages_off + lineages_size;
        let directory_off = postings_off + 4 + postings_body_len;

        let tmp = path.with_extension("bm25tmp");
        {
            let mut w = io::BufWriter::new(std::fs::File::create(&tmp)?);
            w.write_all(MAGIC_V5)?;
            write_u64(&mut w, self.total_length)?;
            write_u64(&mut w, texts_off)?;
            write_u64(&mut w, lineages_off)?;
            write_u64(&mut w, postings_off)?;
            write_u64(&mut w, directory_off)?;
            write_u32(&mut w, self.doc_lengths.len() as u32)?;
            for &len in &self.doc_lengths {
                write_u32(&mut w, len)?;
            }
            // texts: byte-copy of the spill (already section-encoded).
            let mut spill = std::fs::File::open(self.dir.join("texts.spill"))?;
            io::copy(&mut spill, &mut w)?;
            // text_index, from the in-memory length table.
            let mut cursor = texts_off;
            for &len in &self.text_lens {
                if len == u32::MAX {
                    write_u64(&mut w, 0)?;
                    write_u32(&mut w, u32::MAX)?;
                    cursor += 4;
                } else {
                    write_u64(&mut w, cursor + 4)?;
                    write_u32(&mut w, len)?;
                    cursor += 4 + len as u64;
                }
            }
            for lineage in &self.lineages {
                match lineage {
                    Some(l) => {
                        w.write_all(&[1u8])?;
                        write_u64(&mut w, l.opinion_id)?;
                        write_u64(&mut w, l.cluster_id)?;
                        write_u32(&mut w, l.span_start)?;
                        write_u32(&mut w, l.span_end)?;
                    }
                    None => w.write_all(&[0u8])?,
                }
            }
            write_u32(&mut w, directory.len() as u32)?;
            let mut body = std::fs::File::open(&postings_body)?;
            io::copy(&mut body, &mut w)?;
            write_u32(&mut w, directory.len() as u32)?;
            let mut blob_off = 0u64; // relative to the term blob start
            for (term, doc_rel, skip_rel, occ_rel, df) in &directory {
                write_u64(&mut w, postings_off + 4 + doc_rel)?;
                write_u64(&mut w, postings_off + 4 + skip_rel)?;
                write_u64(&mut w, postings_off + 4 + occ_rel)?;
                write_u32(&mut w, *df)?;
                write_u32(&mut w, u32::try_from(blob_off).expect("term blob exceeds u32"))?;
                write_u16(&mut w, term.len() as u16)?;
                blob_off += term.len() as u64;
            }
            for (term, _, _, _, _) in &directory {
                w.write_all(term.as_bytes())?;
            }
            w.flush()?;
        }
        std::fs::rename(&tmp, path)?;
        std::fs::remove_dir_all(&self.dir)?;
        Ok(())
    }

    /// Merge the runs and assemble the v4 file at `path` (the pre-v5
    /// behavior, kept for benchmarking and migration checks).
    fn finish_v4(&mut self, path: &Path) -> io::Result<()> {
        self.spill_run()?;
        self.texts.flush()?;

        // Pass 1: merge runs into the postings section body, collecting
        // the directory (term, absolute offset comes later, df).
        let postings_body = self.dir.join("postings.body");
        let mut directory: Vec<(String, u64, u32)> = Vec::new();
        {
            let mut out = io::BufWriter::new(std::fs::File::create(&postings_body)?);
            let mut heads: Vec<RunHead> = Vec::new();
            for run in &self.runs {
                if let Some(head) = RunHead::open(run)? {
                    heads.push(head);
                }
            }
            let mut cursor = 0u64;
            while !heads.is_empty() {
                // Smallest term across run heads; runs are time-ordered,
                // and ids only grow, so draining matching heads in run
                // order keeps postings doc-ascending.
                let term = heads
                    .iter()
                    .map(|h| h.term.clone())
                    .min()
                    .expect("nonempty heads");
                let df: u32 = heads
                    .iter()
                    .filter(|h| h.term == term)
                    .map(|h| h.n_postings)
                    .sum();
                write_str(&mut out, &term)?;
                write_u32(&mut out, df)?;
                directory.push((term.clone(), cursor, df));
                cursor += 4 + term.len() as u64 + 4;
                let mut idx = 0;
                while idx < heads.len() {
                    if heads[idx].term == term {
                        cursor += heads[idx].copy_postings(&mut out)?;
                        if !heads[idx].advance()? {
                            heads.remove(idx);
                            continue;
                        }
                    }
                    idx += 1;
                }
            }
            out.flush()?;
        }
        let postings_body_len = std::fs::metadata(&postings_body)?.len();

        // Section geometry, identical to Bm25Store::write_to.
        let n_slots = self.doc_lengths.len() as u64;
        let header_size = 8 + 8 + 8 * 4 + 4;
        let doc_lengths_off = header_size as u64;
        let texts_off = doc_lengths_off + 4 * n_slots;
        let text_index_off = texts_off + self.texts_bytes;
        let lineages_off = text_index_off + 12 * n_slots;
        let lineages_size: u64 = self
            .lineages
            .iter()
            .map(|l| if l.is_some() { 25 } else { 1 })
            .sum();
        let postings_off = lineages_off + lineages_size;
        let directory_off = postings_off + 4 + postings_body_len;

        let tmp = path.with_extension("bm25tmp");
        {
            let mut w = io::BufWriter::new(std::fs::File::create(&tmp)?);
            w.write_all(MAGIC_V4)?;
            write_u64(&mut w, self.total_length)?;
            write_u64(&mut w, texts_off)?;
            write_u64(&mut w, lineages_off)?;
            write_u64(&mut w, postings_off)?;
            write_u64(&mut w, directory_off)?;
            write_u32(&mut w, self.doc_lengths.len() as u32)?;
            for &len in &self.doc_lengths {
                write_u32(&mut w, len)?;
            }
            // texts: byte-copy of the spill (already section-encoded).
            let mut spill = std::fs::File::open(self.dir.join("texts.spill"))?;
            io::copy(&mut spill, &mut w)?;
            // text_index, from the in-memory length table.
            let mut cursor = texts_off;
            for &len in &self.text_lens {
                if len == u32::MAX {
                    write_u64(&mut w, 0)?;
                    write_u32(&mut w, u32::MAX)?;
                    cursor += 4;
                } else {
                    write_u64(&mut w, cursor + 4)?;
                    write_u32(&mut w, len)?;
                    cursor += 4 + len as u64;
                }
            }
            for lineage in &self.lineages {
                match lineage {
                    Some(l) => {
                        w.write_all(&[1u8])?;
                        write_u64(&mut w, l.opinion_id)?;
                        write_u64(&mut w, l.cluster_id)?;
                        write_u32(&mut w, l.span_start)?;
                        write_u32(&mut w, l.span_end)?;
                    }
                    None => w.write_all(&[0u8])?,
                }
            }
            write_u32(&mut w, directory.len() as u32)?;
            let mut body = std::fs::File::open(&postings_body)?;
            io::copy(&mut body, &mut w)?;
            write_u32(&mut w, directory.len() as u32)?;
            let mut blob_off = 0u64; // relative to the term blob start (v4)
            for (term, rel_off, df) in &directory {
                write_u64(&mut w, postings_off + 4 + rel_off)?;
                write_u32(&mut w, *df)?;
                write_u32(&mut w, u32::try_from(blob_off).expect("term blob exceeds u32"))?;
                write_u16(&mut w, term.len() as u16)?;
                blob_off += term.len() as u64;
            }
            for (term, _, _) in &directory {
                w.write_all(term.as_bytes())?;
            }
            w.flush()?;
        }
        std::fs::rename(&tmp, path)?;
        std::fs::remove_dir_all(&self.dir)?;
        Ok(())
    }
}

/// One run's read cursor for the merge: the current term group's header
/// plus a reader positioned at its postings.
struct RunHead {
    reader: io::BufReader<std::fs::File>,
    term: String,
    n_postings: u32,
}

impl RunHead {
    fn open(path: &Path) -> io::Result<Option<Self>> {
        let reader = io::BufReader::new(std::fs::File::open(path)?);
        let mut head = Self {
            reader,
            term: String::new(),
            n_postings: 0,
        };
        Ok(if head.advance()? { Some(head) } else { None })
    }

    /// Read the next term group header; false at end of run.
    fn advance(&mut self) -> io::Result<bool> {
        let mut len_buf = [0u8; 2];
        match self.reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(false),
            Err(e) => return Err(e),
        }
        let term_len = u16::from_le_bytes(len_buf) as usize;
        let mut term = vec![0u8; term_len];
        self.reader.read_exact(&mut term)?;
        self.term = String::from_utf8(term)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid utf-8 in run"))?;
        let mut n = [0u8; 4];
        self.reader.read_exact(&mut n)?;
        self.n_postings = u32::from_le_bytes(n);
        Ok(true)
    }

    /// Copy this group's postings to `out` in the final v3 encoding
    /// (which is the run encoding), returning bytes written.
    fn copy_postings<W: Write>(&mut self, out: &mut W) -> io::Result<u64> {
        let mut written = 0u64;
        for _ in 0..self.n_postings {
            let mut fixed = [0u8; 12];
            self.reader.read_exact(&mut fixed)?;
            out.write_all(&fixed)?;
            let n_offsets = u32::from_le_bytes(fixed[8..12].try_into().expect("4 bytes"));
            let mut offsets = vec![0u8; 8 * n_offsets as usize];
            self.reader.read_exact(&mut offsets)?;
            out.write_all(&offsets)?;
            written += 12 + 8 * u64::from(n_offsets);
        }
        Ok(written)
    }

    /// Read one posting of the current group as `(doc_id, tf, raw offset
    /// bytes)` — the v5 merge splits the runs, and the run encoding's
    /// offset bytes are already the v5 occurrence-run pairs.
    fn next_posting_raw(&mut self) -> io::Result<(u32, u32, Vec<u8>)> {
        let mut fixed = [0u8; 12];
        self.reader.read_exact(&mut fixed)?;
        let doc_id = u32::from_le_bytes(fixed[0..4].try_into().expect("4 bytes"));
        let tf = u32::from_le_bytes(fixed[4..8].try_into().expect("4 bytes"));
        let n_offsets = u32::from_le_bytes(fixed[8..12].try_into().expect("4 bytes")) as usize;
        let mut offsets = vec![0u8; 8 * n_offsets];
        self.reader.read_exact(&mut offsets)?;
        Ok((doc_id, tf, offsets))
    }
}

/// Full structural validation of a `.bm25` file, run at open for every
/// supported version. Every read is bounds-checked; anything malformed
/// is an `InvalidData` error, never a panic and never a silently wrong
/// offset. Checked: header fields and section offsets (ordered, within
/// the file), the text index and lineage walk, the directory (entries
/// within the directory region, blob offsets, strict term ordering),
/// and per term the run offsets — v3/v4 postings headers must match the
/// directory entry exactly, v5 run offsets must be consistent
/// (doc_run + sentinel == occ_run, occ run length == sentinel x 8,
/// skip run walks out exactly, block `last_doc_id`s cross-checked
/// against the doc run). Payload bytes (postings, occurrences, frontier
/// pairs, texts) are data, not structure: they are not checksum-able
/// without a format change and are not validated.
fn validate_structure(map: &[u8], v5: bool, blob_relative: bool) -> io::Result<()> {
    let invalid = |msg: String| io::Error::new(io::ErrorKind::InvalidData, msg);
    let file_len = map.len() as u64;
    let u32_at = |off: u64| -> io::Result<u32> {
        let b = map
            .get(off as usize..off as usize + 4)
            .ok_or_else(|| invalid(format!("read past end at {off}")))?;
        Ok(u32::from_le_bytes(b.try_into().expect("4 bytes")))
    };
    let u64_at = |off: u64| -> io::Result<u64> {
        let b = map
            .get(off as usize..off as usize + 8)
            .ok_or_else(|| invalid(format!("read past end at {off}")))?;
        Ok(u64::from_le_bytes(b.try_into().expect("8 bytes")))
    };
    let bytes_at = |off: u64, len: u64| -> io::Result<&[u8]> {
        map.get(off as usize..(off + len) as usize)
            .ok_or_else(|| invalid(format!("range [{off}, {}) out of file", off + len)))
    };

    // Header (fixed 52 bytes; magic already checked by the caller).
    let total_length = u64_at(8)?;
    let texts_off = u64_at(16)?;
    let lineages_off = u64_at(24)?;
    let postings_off = u64_at(32)?;
    let directory_off = u64_at(40)?;
    let n_slots = u64::from(u32_at(48)?);
    let doc_lengths_off = 52u64;
    if texts_off != doc_lengths_off + 4 * n_slots || texts_off > file_len {
        return Err(invalid(format!("doc_lengths section [{doc_lengths_off}, {texts_off}) out of file ({file_len})")));
    }
    // The header total must agree with the doc-length table.
    let mut length_sum = 0u64;
    for slot in 0..n_slots {
        length_sum += u64::from(u32_at(doc_lengths_off + 4 * slot)?);
    }
    if length_sum != total_length {
        return Err(invalid("header total_length != sum of doc lengths".into()));
    }
    let text_index_off = lineages_off
        .checked_sub(12 * n_slots)
        .ok_or_else(|| invalid("lineages_off before the text index".into()))?;
    if text_index_off < texts_off || lineages_off > file_len {
        return Err(invalid("texts/text_index sections out of file".into()));
    }
    if postings_off < lineages_off || directory_off < postings_off + 4 || directory_off + 4 > file_len
    {
        return Err(invalid("section offsets unordered or out of file".into()));
    }
    // Text index: each entry within the texts section (or the absent
    // marker).
    for slot in 0..n_slots {
        let e = text_index_off + 12 * slot;
        let offset = u64_at(e)?;
        let len = u32_at(e + 8)?;
        if len != u32::MAX && (offset < texts_off + 4 || offset + u64::from(len) > text_index_off) {
            return Err(invalid(format!("text index entry {slot} out of the texts section")));
        }
    }
    // Lineage walk: variable-stride entries must end exactly at the
    // postings section.
    let mut cur = lineages_off;
    for _ in 0..n_slots {
        let flag = *bytes_at(cur, 1)?
            .first()
            .ok_or_else(|| invalid("lineage section overruns postings".into()))?;
        cur += if flag == 0 { 1 } else { 25 };
    }
    if cur != postings_off {
        return Err(invalid("lineage section does not end at the postings section".into()));
    }
    // Directory: count, fixed-stride entries within the file, then the
    // term blob (which runs to EOF).
    let stride: u64 = if v5 { 34 } else { 18 };
    let n_terms = u64::from(u32_at(directory_off)?);
    if u64::from(u32_at(postings_off)?) != n_terms {
        return Err(invalid("postings and directory term counts differ".into()));
    }
    let blob_start = directory_off + 4 + stride * n_terms;
    if blob_start > file_len {
        return Err(invalid("directory overruns the file".into()));
    }
    let mut prev_term: Vec<u8> = Vec::new();
    let mut prev_skip_end: u64 = 0;
    for i in 0..n_terms {
        let e = directory_off + 4 + stride * i;
        let (term, df) = if v5 {
            let doc_run_off = u64_at(e)?;
            let skip_run_off = u64_at(e + 8)?;
            let occ_run_off = u64_at(e + 16)?;
            let df = u32_at(e + 24)?;
            let blob_off = u64::from(u32_at(e + 28)?);
            let term_len = u64::from(u16_at(bytes_at(e + 32, 2)?));
            let term = bytes_at(blob_start + blob_off, term_len)?;
            if term_len == 0 {
                return Err(invalid(format!("directory entry {i}: empty term")));
            }
            // Run offsets: consistent with each other and with the
            // previous term's region.
            if doc_run_off < prev_skip_end || doc_run_off < postings_off + 4 {
                return Err(invalid(format!("directory entry {i}: doc run overlaps previous regions")));
            }
            if occ_run_off != doc_run_off + 12 * u64::from(df) + 4 || occ_run_off > skip_run_off {
                return Err(invalid(format!("directory entry {i}: inconsistent run offsets")));
            }
            let skip_end = if i + 1 < n_terms {
                u64_at(e + stride)?
            } else {
                directory_off
            };
            if skip_end < skip_run_off + 8 || skip_end > directory_off {
                return Err(invalid(format!("directory entry {i}: skip run out of the postings section")));
            }
            // Occurrence run: length divisible by 8 and equal to the
            // sentinel occ_start.
            if (skip_run_off - occ_run_off) % 8 != 0 {
                return Err(invalid(format!("directory entry {i}: occurrence run not pair-aligned")));
            }
            let sentinel = u32_at(doc_run_off + 12 * u64::from(df))?;
            if u64::from(sentinel) != (skip_run_off - occ_run_off) / 8 {
                return Err(invalid(format!("directory entry {i}: sentinel occ_start mismatch")));
            }
            if df > 0 && u32_at(doc_run_off + 8)? != 0 {
                return Err(invalid(format!("directory entry {i}: first occ_start is not 0")));
            }
            // Skip run: walk level-0 and level-1 records exactly,
            // cross-checking block last_doc_ids against the doc run.
            let region = skip_run_off;
            let region_end = skip_end;
            let l1_rel = u64_at(region)?;
            let n_l0 = u64::from(df).div_ceil(BLOCK as u64);
            let mut cur = region + 8;
            let mut prev_last = 0u32;
            let mut l0_record_offs: Vec<u64> = Vec::with_capacity(n_l0 as usize);
            for b in 0..n_l0 {
                l0_record_offs.push(cur - region);
                let last_doc = u32_at(cur)?;
                let n_pairs = u64::from(bytes_at(cur + 4, 1)?[0]);
                if n_pairs == 0 || n_pairs > MAX_FRONTIER as u64 {
                    return Err(invalid(format!("term {i} block {b}: n_pairs {n_pairs} out of range")));
                }
                if last_doc < prev_last {
                    return Err(invalid(format!("term {i} block {b}: last_doc_id goes backwards")));
                }
                prev_last = last_doc;
                // Cross-check the block bound against the doc run.
                let last_posting = ((b + 1) * BLOCK as u64).min(u64::from(df)) - 1;
                if u32_at(doc_run_off + 12 * last_posting)? != last_doc {
                    return Err(invalid(format!("term {i} block {b}: last_doc_id != doc run")));
                }
                cur += 5 + 8 * n_pairs;
                if cur > region_end {
                    return Err(invalid(format!("term {i}: level-0 records overrun the skip run")));
                }
            }
            if cur != region + l1_rel {
                return Err(invalid(format!("term {i}: level-1 region offset mismatch")));
            }
            let n_l1 = n_l0.div_ceil(LEVEL1_FACTOR as u64);
            for g in 0..n_l1 {
                let last_doc = u32_at(cur)?;
                let l0_off = u64_at(cur + 4)?;
                let n_pairs = u64::from(bytes_at(cur + 12, 1)?[0]);
                if n_pairs == 0 || n_pairs > MAX_FRONTIER as u64 {
                    return Err(invalid(format!("term {i} group {g}: n_pairs out of range")));
                }
                let last_block = ((g + 1) * LEVEL1_FACTOR as u64).min(n_l0) - 1;
                let last_posting = ((last_block + 1) * BLOCK as u64).min(u64::from(df)) - 1;
                if u32_at(doc_run_off + 12 * last_posting)? != last_doc {
                    return Err(invalid(format!("term {i} group {g}: last_doc_id != doc run")));
                }
                if l0_off != l0_record_offs[(g * LEVEL1_FACTOR as u64) as usize] {
                    return Err(invalid(format!("term {i} group {g}: l0_off != group start")));
                }
                cur += 13 + 8 * n_pairs;
            }
            if cur != region_end {
                return Err(invalid(format!("term {i}: skip run does not end at the next region")));
            }
            prev_skip_end = skip_end;
            (term, df)
        } else {
            let postings_entry_off = u64_at(e)?;
            let df = u32_at(e + 8)?;
            let stored = u32_at(e + 12)?;
            let term_len = u64::from(u16_at(bytes_at(e + 16, 2)?));
            let term = if blob_relative {
                bytes_at(blob_start + u64::from(stored), term_len)?
            } else {
                bytes_at(u64::from(stored), term_len)?
            };
            if term_len == 0 {
                return Err(invalid(format!("directory entry {i}: empty term")));
            }
            if postings_entry_off < postings_off + 4 || postings_entry_off >= directory_off {
                return Err(invalid(format!("directory entry {i}: postings offset out of section")));
            }
            // The postings entry's inline header must match the
            // directory entry exactly.
            let inline_len = u64::from(u32_at(postings_entry_off)?);
            if inline_len != term_len
                || bytes_at(postings_entry_off + 4, term_len)? != term
                || u32_at(postings_entry_off + 4 + term_len)? != df
            {
                return Err(invalid(format!("directory entry {i}: postings header mismatch")));
            }
            (term, df)
        };
        let _ = df;
        if !prev_term.is_empty() && term <= prev_term.as_slice() {
            return Err(invalid(format!("directory entry {i}: terms not strictly ordered")));
        }
        prev_term = term.to_vec();
    }
    Ok(())
}

fn u16_at(bytes: &[u8]) -> u16 {
    u16::from_le_bytes(bytes.try_into().expect("2 bytes"))
}

/// A memory-mapped, disk-resident view of a `.bm25` file (v3, v4, or
/// v5). Postings and document texts are read from the map on demand; the
/// only heap state is the per-document length table and a term count.
pub struct Bm25Reader {
    map: memmap2::Mmap,
    doc_lengths: Vec<u32>,
    doc_count: u64,
    total_length: u64,
    lineages_off: u64,
    directory_off: u64,
    n_terms: u32,
    /// v5 file: 34 B directory entries and the doc/occurrence/skip run
    /// layout; v3/v4: 18 B entries and interleaved postings.
    v5: bool,
    /// v4+ directories store blob offsets relative to the blob start;
    /// v3 stored absolute file offsets.
    blob_relative: bool,
    /// Lazily built lineage-section index: per-slot byte offset relative
    /// to `lineages_off`. The section is variable stride (1 B absent, 25
    /// B present), so random access needs this — one O(n_slots) decode
    /// on the first `lineage()` call, ~4 B/slot of heap, cached.
    lineage_index: std::sync::OnceLock<Vec<u32>>,
}

impl Bm25Reader {
    /// The next local doc id (number of document slots).
    pub fn next_doc_id(&self) -> u32 {
        self.doc_lengths.len() as u32
    }

    /// Open a v3/v4/v5 `.bm25` file read-only after full structural
    /// validation (see [`validate_structure`] — malformed files error,
    /// never panic). Touches only the header, the doc-length table, the
    /// directory, and the skip runs — no postings or text pages beyond
    /// those are faulted in until queries ask for them.
    pub fn open(path: &Path) -> io::Result<Self> {
        let invalid = |msg: &str| io::Error::new(io::ErrorKind::InvalidData, msg.to_string());
        let file = std::fs::File::open(path)?;
        let map = unsafe { memmap2::MmapOptions::new().map(&file)? };
        if map.len() < 52
            || (&map[..8] != MAGIC_V5 && &map[..8] != MAGIC_V4 && &map[..8] != MAGIC_V3)
        {
            return Err(invalid("not a v3/v4/v5 .bm25 file"));
        }
        let v5 = &map[..8] == MAGIC_V5;
        let blob_relative = &map[..8] != MAGIC_V3;
        validate_structure(&map, v5, blob_relative)?;
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
            v5,
            blob_relative,
            lineage_index: std::sync::OnceLock::new(),
        })
    }

    fn directory_entry(&self, i: u32) -> (&[u8], u64, u32) {
        let e = self.directory_off as usize + 4 + 18 * i as usize;
        let postings_off = u64::from_le_bytes(self.map[e..e + 8].try_into().unwrap());
        let df = u32::from_le_bytes(self.map[e + 8..e + 12].try_into().unwrap());
        let stored = u32::from_le_bytes(self.map[e + 12..e + 16].try_into().unwrap()) as usize;
        let len = u16::from_le_bytes(self.map[e + 16..e + 18].try_into().unwrap()) as usize;
        // v4 stores offsets relative to the term blob; v3 stored absolute
        // file offsets (only valid below 4 GiB).
        let blob_off = if self.blob_relative {
            self.directory_off as usize + 4 + 18 * self.n_terms as usize + stored
        } else {
            stored
        };
        (&self.map[blob_off..blob_off + len], postings_off, df)
    }

    /// The 34 B v5 directory entry: `(term bytes, doc_run_off,
    /// skip_run_off, occ_run_off, df)`.
    fn directory_entry_v5(&self, i: u32) -> (&[u8], u64, u64, u64, u32) {
        let e = self.directory_off as usize + 4 + 34 * i as usize;
        let doc_run_off = u64::from_le_bytes(self.map[e..e + 8].try_into().unwrap());
        let skip_run_off = u64::from_le_bytes(self.map[e + 8..e + 16].try_into().unwrap());
        let occ_run_off = u64::from_le_bytes(self.map[e + 16..e + 24].try_into().unwrap());
        let df = u32::from_le_bytes(self.map[e + 24..e + 28].try_into().unwrap());
        let stored = u32::from_le_bytes(self.map[e + 28..e + 32].try_into().unwrap()) as usize;
        let len = u16::from_le_bytes(self.map[e + 32..e + 34].try_into().unwrap()) as usize;
        let blob_off = self.directory_off as usize + 4 + 34 * self.n_terms as usize + stored;
        (
            &self.map[blob_off..blob_off + len],
            doc_run_off,
            skip_run_off,
            occ_run_off,
            df,
        )
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

    /// `(doc_run_off, skip_run_off, occ_run_off, df)` for `term`, or
    /// `None` when the term is absent. v5 files only.
    fn directory_lookup_v5(&self, term: &str) -> Option<(u64, u64, u64, u32)> {
        let (mut lo, mut hi) = (0u32, self.n_terms);
        while lo < hi {
            let mid = (lo + hi) / 2;
            let (bytes, _, _, _, _) = self.directory_entry_v5(mid);
            match bytes.cmp(term.as_bytes()) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => {
                    let (_, doc, skip, occ, df) = self.directory_entry_v5(mid);
                    return Some((doc, skip, occ, df));
                }
            }
        }
        None
    }

    fn u32_at(&self, off: usize) -> u32 {
        u32::from_le_bytes(self.map[off..off + 4].try_into().unwrap())
    }

    /// The v5 doc-run entry for posting `i`: `(doc_id, tf, occ_start)`.
    fn v5_doc_entry(&self, doc_run_off: usize, i: usize) -> (u32, u32, u32) {
        let e = doc_run_off + 12 * i;
        (
            self.u32_at(e),
            self.u32_at(e + 4),
            self.u32_at(e + 8),
        )
    }

    /// The occ_start of posting `j`; `j == df` reads the trailing
    /// sentinel, so any posting's occurrence count is
    /// `v5_occ_start(j + 1) - v5_occ_start(j)` without a scan.
    fn v5_occ_start(&self, doc_run_off: usize, df: usize, j: usize) -> u32 {
        if j < df {
            self.u32_at(doc_run_off + 12 * j + 8)
        } else {
            self.u32_at(doc_run_off + 12 * df) // sentinel
        }
    }

    /// Decode the occurrence pairs `[occ_start, occ_end)` of a term's
    /// occurrence run.
    fn v5_occ_slice(&self, occ_run_off: usize, occ_start: u32, occ_end: u32) -> Vec<(u32, u32)> {
        let mut out = Vec::with_capacity((occ_end - occ_start) as usize);
        for o in occ_start..occ_end {
            let e = occ_run_off + 8 * o as usize;
            out.push((self.u32_at(e), self.u32_at(e + 4)));
        }
        out
    }

    /// The level-0 skip records of a term: `(last_doc_id, frontier
    /// pairs)` per 128-posting block. Test/stage-2 surface; v5 files
    /// only.
    #[allow(dead_code)]
    fn v5_l0_records(&self, skip_run_off: u64, df: u32) -> Vec<(u32, Vec<(u32, u32)>)> {
        let n_blocks = (df as usize).div_ceil(BLOCK);
        let mut cur = skip_run_off as usize + 8; // past the level-1 prefix
        let mut out = Vec::with_capacity(n_blocks);
        for _ in 0..n_blocks {
            let last_doc_id = self.u32_at(cur);
            let n_pairs = self.map[cur + 4] as usize;
            cur += 5;
            let mut pairs = Vec::with_capacity(n_pairs);
            for _ in 0..n_pairs {
                pairs.push((self.u32_at(cur), self.u32_at(cur + 4)));
                cur += 8;
            }
            out.push((last_doc_id, pairs));
        }
        out
    }

    /// v5 walk of the fixed-stride doc run only — no occurrence bytes
    /// are touched.
    fn v5_for_each_doc_tf(&self, doc_run_off: usize, df: u32, f: &mut dyn FnMut(u32, u32)) {
        for i in 0..df as usize {
            let (doc_id, tf, _) = self.v5_doc_entry(doc_run_off, i);
            f(doc_id, tf);
        }
    }

    fn v3_for_each_posting(&self, off: u64, df: u32, f: &mut PostingCallback) {
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
}

/// Zero-copy cursor over one term's v5 doc run and skip run — the
/// block-max surface (`docs/block-max.md`). Level-0 impact records (one
/// per 128-posting block) are read straight from the map, and
/// `advance_shallow` first leaps whole level-1 groups (32 blocks = 4096
/// postings) by their `last_doc_id`, then skips level-0 blocks the same
/// way, never decoding doc-run entries it passes.
pub struct ImpactCursor<'a> {
    map: &'a [u8],
    doc_run_off: usize,
    occ_run_off: usize,
    skip_run_off: usize,
    df: u32,
    n_blocks: u32,
    /// Current level-0 block index and its record's map offset.
    block: u32,
    block_rec_off: usize,
    block_last_doc: u32,
    block_pairs: Vec<(u32, u32)>,
    /// Level-1 state: the group covering `block` (invariant
    /// `l1_idx == block / LEVEL1_FACTOR`), its record's map offset,
    /// last doc id, merged frontier, and the level-0 offset its first
    /// record lives at (the leap target).
    l1_idx: u32,
    n_l1: u32,
    l1_rec_off: usize,
    l1_last_doc: u32,
    l1_l0_off: u64,
    l1_pairs: Vec<(u32, u32)>,
    /// Current posting index in `0..=df` (`df` = exhausted).
    pos: u32,
    /// The whole term's `(tf, dl)` frontier, merged from the level-1
    /// records: a static upper bound over every posting of the term.
    term_pairs: Vec<(u32, u32)>,
    /// Blocks bypassed by [`Self::advance_shallow`] without their
    /// postings being read (skip accounting for the pruned scorer).
    pub blocks_skipped: u64,
    /// Level-1 groups (32 blocks = 4096 postings) leapt by
    /// [`Self::advance_shallow`] without reading a single level-0
    /// record inside them.
    pub l1_groups_skipped: u64,
}

impl<'a> ImpactCursor<'a> {
    fn new(
        map: &'a [u8],
        doc_run_off: usize,
        occ_run_off: usize,
        skip_run_off: usize,
        df: u32,
    ) -> Self {
        let n_blocks = df.div_ceil(BLOCK as u32);
        let n_l1 = n_blocks.div_ceil(LEVEL1_FACTOR as u32);
        // The whole-term frontier: merge every level-1 record's pairs
        // (each covers 32 blocks) and take the Pareto frontier of the
        // union — a static bound over every posting of the term.
        let l1_region = skip_run_off
            + u64::from_le_bytes(map[skip_run_off..skip_run_off + 8].try_into().unwrap()) as usize;
        let mut term_pairs: Vec<(u32, u32)> = Vec::new();
        let mut cur = l1_region;
        for _ in 0..n_l1 {
            // u32 last_doc_id, u64 l0_off, u8 n_pairs, pairs
            let n_pairs = map[cur + 12] as usize;
            for i in 0..n_pairs {
                let p = cur + 13 + 8 * i;
                term_pairs.push((
                    u32::from_le_bytes(map[p..p + 4].try_into().unwrap()),
                    u32::from_le_bytes(map[p + 4..p + 8].try_into().unwrap()),
                ));
            }
            cur += 13 + 8 * n_pairs;
        }
        let term_pairs = pareto_frontier(&term_pairs);
        let mut cur = Self {
            map,
            doc_run_off,
            occ_run_off,
            skip_run_off,
            df,
            n_blocks,
            block: 0,
            block_rec_off: skip_run_off + 8, // past the level-1 prefix
            block_last_doc: 0,
            block_pairs: Vec::new(),
            l1_idx: 0,
            n_l1,
            l1_rec_off: l1_region,
            l1_last_doc: u32::MAX,
            l1_l0_off: 0,
            l1_pairs: Vec::new(),
            pos: 0,
            term_pairs,
            blocks_skipped: 0,
            l1_groups_skipped: 0,
        };
        cur.load_l1();
        cur.load_block();
        cur
    }

    fn u32_at(&self, off: usize) -> u32 {
        u32::from_le_bytes(self.map[off..off + 4].try_into().unwrap())
    }

    /// Decode the level-1 record at `l1_rec_off` into the cursor.
    fn load_l1(&mut self) {
        if self.l1_idx >= self.n_l1 {
            self.l1_last_doc = u32::MAX;
            self.l1_pairs.clear();
            return;
        }
        let e = self.l1_rec_off;
        self.l1_last_doc = self.u32_at(e);
        self.l1_l0_off = u64::from_le_bytes(self.map[e + 4..e + 12].try_into().unwrap());
        let n_pairs = self.map[e + 12] as usize;
        self.l1_pairs.clear();
        for i in 0..n_pairs {
            self.l1_pairs
                .push((self.u32_at(e + 13 + 8 * i), self.u32_at(e + 13 + 8 * i + 4)));
        }
    }

    /// Advance to the next level-1 group (its record follows the
    /// current one contiguously).
    fn next_l1(&mut self) {
        if self.l1_idx >= self.n_l1 {
            return;
        }
        self.l1_rec_off += 13 + 8 * self.l1_pairs.len();
        self.l1_idx += 1;
        self.load_l1();
    }

    /// Decode the level-0 record at `block_rec_off` into the cursor.
    fn load_block(&mut self) {
        if self.block >= self.n_blocks {
            self.pos = self.df;
            self.block_pairs.clear();
            return;
        }
        let e = self.block_rec_off;
        self.block_last_doc = self.u32_at(e);
        let n_pairs = self.map[e + 4] as usize;
        self.block_pairs.clear();
        for i in 0..n_pairs {
            self.block_pairs
                .push((self.u32_at(e + 5 + 8 * i), self.u32_at(e + 5 + 8 * i + 4)));
        }
    }

    /// Advance to the next level-0 block (its record follows the current
    /// one contiguously), keeping the level-1 pointer in sync. False
    /// when the term is exhausted.
    fn next_block(&mut self) -> bool {
        if self.block >= self.n_blocks {
            return false;
        }
        self.block_rec_off += 5 + 8 * self.block_pairs.len();
        self.block += 1;
        if self.block % LEVEL1_FACTOR as u32 == 0 {
            self.next_l1();
        }
        self.pos = (self.block * BLOCK as u32).min(self.df);
        self.load_block();
        self.block < self.n_blocks
    }

    /// Postings in the term.
    pub fn df(&self) -> u32 {
        self.df
    }

    /// Blocks in the term.
    pub fn n_blocks(&self) -> u32 {
        self.n_blocks
    }

    /// True when every posting has been consumed.
    pub fn exhausted(&self) -> bool {
        self.pos >= self.df
    }

    /// The current level-0 block index.
    pub fn block(&self) -> u32 {
        self.block
    }

    /// The current block's `(tf, dl)` Pareto frontier (<= 8 pairs).
    pub fn block_frontier(&self) -> &[(u32, u32)] {
        &self.block_pairs
    }

    /// The whole term's `(tf, dl)` frontier from the level-1 records —
    /// a static bound over every posting, used for termination.
    pub fn term_frontier(&self) -> &[(u32, u32)] {
        &self.term_pairs
    }

    /// The last doc id in the current block — the shallow-advance bound.
    pub fn block_last_doc(&self) -> u32 {
        self.block_last_doc
    }

    /// The doc id at the cursor position (`u32::MAX` when exhausted).
    pub fn doc_id(&self) -> u32 {
        if self.exhausted() {
            u32::MAX
        } else {
            self.u32_at(self.doc_run_off + 12 * self.pos as usize)
        }
    }

    /// The term frequency at the cursor position.
    pub fn tf(&self) -> u32 {
        self.u32_at(self.doc_run_off + 12 * self.pos as usize + 4)
    }

    /// The occurrence spans of the posting at the cursor position.
    pub fn offsets(&self) -> Vec<(u32, u32)> {
        let e = self.doc_run_off + 12 * self.pos as usize;
        let occ_start = self.u32_at(e + 8);
        let occ_end = if self.pos + 1 < self.df {
            self.u32_at(e + 12 + 8)
        } else {
            self.u32_at(self.doc_run_off + 12 * self.df as usize) // sentinel
        };
        let mut out = Vec::with_capacity((occ_end - occ_start) as usize);
        for o in occ_start..occ_end {
            let p = self.occ_run_off + 8 * o as usize;
            out.push((self.u32_at(p), self.u32_at(p + 4)));
        }
        out
    }

    /// Advance one posting, crossing into the next block when the
    /// current one is consumed. False when the term is exhausted.
    pub fn next_posting(&mut self) -> bool {
        if self.exhausted() {
            return false;
        }
        self.pos += 1;
        if self.pos < self.df && self.pos % BLOCK as u32 == 0 {
            self.next_block();
        }
        !self.exhausted()
    }

    /// Position the cursor on the first posting with doc id >= `target`:
    /// first leap whole level-1 groups (32 blocks = 4096 postings) whose
    /// `last_doc_id` is below it without reading a level-0 record inside
    /// them, then bypass level-0 blocks the same way, then binary-search
    /// the landing block.
    pub fn advance_shallow(&mut self, target: u32) {
        while !self.exhausted() && self.l1_last_doc < target {
            self.l1_groups_skipped += 1;
            self.next_l1();
            if self.l1_idx >= self.n_l1 {
                // The last group's last doc is the term's last doc:
                // nothing at or after the target exists.
                self.block = self.n_blocks;
                self.block_pairs.clear();
                self.pos = self.df;
                return;
            }
            // Land directly on the group's first level-0 record.
            self.block = self.l1_idx * LEVEL1_FACTOR as u32;
            self.block_rec_off = self.skip_run_off + self.l1_l0_off as usize;
            self.pos = (self.block * BLOCK as u32).min(self.df);
            self.load_block();
        }
        while !self.exhausted() && self.block_last_doc < target {
            self.blocks_skipped += 1;
            self.next_block();
        }
        if self.exhausted() {
            return;
        }
        // Binary search within the block for the first doc >= target
        // (doc ids ascending; pos already points into this block).
        let block_end = ((self.block + 1) * BLOCK as u32).min(self.df);
        let (mut lo, mut hi) = (self.pos, block_end);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.u32_at(self.doc_run_off + 12 * mid as usize) < target {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        self.pos = lo;
    }

    /// The current level-1 group index.
    pub fn l1_group(&self) -> u32 {
        self.l1_idx
    }

    /// The last doc id in the current level-1 group.
    pub fn l1_last_doc(&self) -> u32 {
        self.l1_last_doc
    }

    /// The current level-1 group's merged `(tf, dl)` frontier.
    pub fn l1_frontier(&self) -> &[(u32, u32)] {
        &self.l1_pairs
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
        if self.v5 {
            self.directory_lookup_v5(term).map_or(0, |(_, _, _, df)| df)
        } else {
            self.directory_lookup(term).map_or(0, |(_, df)| df)
        }
    }
    fn for_each_posting(&self, term: &str, f: &mut PostingCallback) {
        if self.v5 {
            let Some((doc_run_off, _, occ_run_off, df)) = self.directory_lookup_v5(term) else {
                return;
            };
            let (doc_run_off, occ_run_off) = (doc_run_off as usize, occ_run_off as usize);
            for i in 0..df as usize {
                let (doc_id, tf, occ_start) = self.v5_doc_entry(doc_run_off, i);
                let occ_end = self.v5_occ_start(doc_run_off, df as usize, i + 1);
                let offsets = self.v5_occ_slice(occ_run_off, occ_start, occ_end);
                f(doc_id, tf, &offsets);
            }
        } else {
            let Some((off, df)) = self.directory_lookup(term) else {
                return;
            };
            self.v3_for_each_posting(off, df, f);
        }
    }
    fn for_each_doc_tf(&self, term: &str, f: &mut dyn FnMut(u32, u32)) {
        if self.v5 {
            let Some((doc_run_off, _, _, df)) = self.directory_lookup_v5(term) else {
                return;
            };
            self.v5_for_each_doc_tf(doc_run_off as usize, df, f);
        } else {
            self.for_each_posting(term, &mut |doc_id, tf, _offsets| {
                f(doc_id, tf);
            });
        }
    }
    fn posting_offsets(&self, term: &str, doc_id: u32) -> Vec<(u32, u32)> {
        if self.v5 {
            let Some((doc_run_off, _, occ_run_off, df)) = self.directory_lookup_v5(term) else {
                return Vec::new();
            };
            // Binary search the fixed-stride doc run (doc ids ascending).
            let doc_run_off = doc_run_off as usize;
            let (mut lo, mut hi) = (0usize, df as usize);
            while lo < hi {
                let mid = (lo + hi) / 2;
                if self.u32_at(doc_run_off + 12 * mid) < doc_id {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            let i = lo;
            if i >= df as usize || self.u32_at(doc_run_off + 12 * i) != doc_id {
                return Vec::new();
            }
            let occ_start = self.u32_at(doc_run_off + 12 * i + 8);
            let occ_end = self.v5_occ_start(doc_run_off, df as usize, i + 1);
            self.v5_occ_slice(occ_run_off as usize, occ_start, occ_end)
        } else {
            // v3/v4: sequential walk with early exit — postings are
            // doc-id-ordered by construction, so stop at the first doc
            // past the target; offset bytes before it are stepped over
            // undecoded. (The trait default walks the whole list, which
            // costs O(df) per survivor at k=1000.)
            let Some((off, df)) = self.directory_lookup(term) else {
                return Vec::new();
            };
            let mut cur = off as usize;
            let term_len =
                u32::from_le_bytes(self.map[cur..cur + 4].try_into().unwrap()) as usize;
            cur += 4 + term_len + 4; // term header + posting count
            for _ in 0..df {
                let doc = self.u32_at(cur);
                let n_offsets = self.u32_at(cur + 8) as usize;
                if doc == doc_id {
                    let mut offsets = Vec::with_capacity(n_offsets);
                    let mut o = cur + 12;
                    for _ in 0..n_offsets {
                        offsets.push((self.u32_at(o), self.u32_at(o + 4)));
                        o += 8;
                    }
                    return offsets;
                }
                if doc > doc_id {
                    return Vec::new();
                }
                cur += 12 + 8 * n_offsets;
            }
            Vec::new()
        }
    }
    fn impacts(&self, term: &str) -> Option<ImpactCursor<'_>> {
        if !self.v5 {
            return None;
        }
        let (doc_run_off, skip_run_off, occ_run_off, df) = self.directory_lookup_v5(term)?;
        Some(ImpactCursor::new(
            &self.map,
            doc_run_off as usize,
            occ_run_off as usize,
            skip_run_off as usize,
            df,
        ))
    }
    fn has_impacts(&self, term: &str) -> bool {
        self.v5 && self.directory_lookup_v5(term).is_some()
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
        // The lineage section is variable stride (1 B absent marker,
        // 25 B present), so a fixed 25B-stride read lands anywhere —
        // correct only for dense all-present lineages. The lazily built
        // per-slot offset index makes random access exact.
        let index = self.lineage_index.get_or_init(|| {
            let base = self.lineages_off as usize;
            let mut offsets = Vec::with_capacity(self.doc_lengths.len());
            let mut cur = base;
            for _ in 0..self.doc_lengths.len() {
                offsets.push((cur - base) as u32);
                cur += if self.map[cur] == 0 { 1 } else { 25 };
            }
            offsets
        });
        let e = self.lineages_off as usize + index[slot] as usize;
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

    /// A deterministic synthetic corpus: gappy ids, mixed lineages,
    /// offsets on some terms, shared and unique terms across docs.
    fn synthetic_corpus() -> Vec<(u32, String, AnalyzedDoc, Option<DocLineage>)> {
        let vocab = ["court", "plaintiff", "rust", "search", "vector", "quant"];
        let mut docs = Vec::new();
        let mut id = 0u32;
        for i in 0..200u32 {
            // Every 7th id is a gap (vector-side slot).
            id += if i % 7 == 0 { 2 } else { 1 };
            let mut terms: DocTerms = Vec::new();
            let mut length = 0;
            for (vi, term) in vocab.iter().enumerate() {
                if (i as usize + vi) % (vi + 2) == 0 {
                    let tf = 1 + (i % 3);
                    let offsets = if vi % 2 == 0 {
                        (0..tf).map(|o| (o * 10, o * 10 + 4)).collect()
                    } else {
                        Vec::new()
                    };
                    terms.push((term.to_string(), tf, offsets));
                    length += tf;
                }
            }
            let lineage = (i % 3 == 0).then_some(DocLineage {
                opinion_id: u64::from(i) * 17,
                cluster_id: u64::from(i) * 31,
                span_start: i,
                span_end: i + 100,
            });
            docs.push((id, format!("document {i} body text"), AnalyzedDoc { terms, length }, lineage));
        }
        docs
    }

    #[test]
    fn spill_builder_output_is_byte_identical_to_store() {
        let base = std::env::temp_dir().join(format!("spill-eq-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();

        let mut store = Bm25Store::new();
        // Tiny buffer forces many runs, so the merge path is exercised
        // with terms split across runs.
        let mut builder = SpillBuilder::create(&base.join("build"))
            .unwrap()
            .with_buffer_bytes(256);
        for (id, text, doc, lineage) in synthetic_corpus() {
            store.add_document_with_lineage(id, text.clone(), doc.clone(), lineage);
            builder
                .add_document_with_lineage(id, text, doc, lineage)
                .unwrap();
        }
        assert_eq!(store.doc_count(), builder.doc_count());
        assert_eq!(store.total_doc_length(), builder.total_doc_length());
        assert_eq!(store.next_doc_id(), builder.next_doc_id());

        let store_path = base.join("store.bm25");
        let spill_path = base.join("spill.bm25");
        store.save(&store_path).unwrap();
        builder.finish(&spill_path).unwrap();

        let a = std::fs::read(&store_path).unwrap();
        let b = std::fs::read(&spill_path).unwrap();
        assert_eq!(a.len(), b.len(), "file sizes differ");
        assert!(a == b, "spill output is not byte-identical to the store");
        // Spill scratch is cleaned up on success.
        assert!(!base.join("build").exists());

        // And the reader serves it.
        let reader = Bm25Reader::open(&spill_path).unwrap();
        assert_eq!(Bm25Index::doc_count(&reader), store.doc_count());
        assert_eq!(reader.df("court"), store.df("court"));
        let mut seen = Vec::new();
        reader.for_each_posting("vector", &mut |doc_id, tf, offsets| {
            seen.push((doc_id, tf, offsets.to_vec()));
        });
        let expected: Vec<_> = store
            .postings("vector")
            .unwrap()
            .iter()
            .map(|p| (p.doc_id, p.tf, p.offsets.clone()))
            .collect();
        assert_eq!(seen, expected);

        let _ = std::fs::remove_dir_all(&base);
    }

    /// Same byte-identity contract on the legacy v4 path (bench/migration
    /// writer): the store and the spill builder must agree there too.
    #[test]
    fn v4_writers_are_byte_identical() {
        let base = std::env::temp_dir().join(format!("spill-eq-v4-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();

        let mut store = Bm25Store::new();
        let mut builder = SpillBuilder::create_v4_for_bench(&base.join("build"))
            .unwrap()
            .with_buffer_bytes(256);
        for (id, text, doc, lineage) in synthetic_corpus() {
            store.add_document_with_lineage(id, text.clone(), doc.clone(), lineage);
            builder
                .add_document_with_lineage(id, text, doc, lineage)
                .unwrap();
        }
        let store_path = base.join("store.bm25");
        let spill_path = base.join("spill.bm25");
        {
            let mut w = io::BufWriter::new(std::fs::File::create(&store_path).unwrap());
            store.write_v4_for_bench(&mut w).unwrap();
            w.flush().unwrap();
        }
        builder.finish(&spill_path).unwrap();
        let a = std::fs::read(&store_path).unwrap();
        let b = std::fs::read(&spill_path).unwrap();
        assert!(a == b, "v4 spill output is not byte-identical to the store");
        let _ = std::fs::remove_dir_all(&base);
    }

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
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/tmp").join(format!("tvbm25_{}", std::process::id()));
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

    /// Regression test for the reader's lineage lookup: the on-disk
    /// lineage section is variable stride (1 B absent, 25 B present),
    /// and the reader used to index it at a fixed 25B stride — correct
    /// only for dense all-present lineages, garbage after any gap slot
    /// or missing lineage (which is all the old tests had, so it
    /// passed). Gap slots + missing lineages here.
    #[test]
    fn reader_lineage_with_gaps_and_missing_entries() {
        let dir = test_dir("lineage");
        let path = dir.join("shard.bm25");
        let mut store = Bm25Store::new();
        let lineage = |i: u32| DocLineage {
            opinion_id: 1000 + u64::from(i),
            cluster_id: 2000 + u64::from(i),
            span_start: i * 7,
            span_end: i * 7 + 90,
        };
        // Gap slots at 0..1, then a mix of present and missing lineages.
        store.add_document_with_lineage(
            2,
            "a".to_string(),
            AnalyzedDoc {
                terms: vec![("rust".into(), 1, vec![(0, 4)])],
                length: 1,
            },
            Some(lineage(2)),
        );
        store.add_document_with_lineage(
            3,
            "b".to_string(),
            AnalyzedDoc {
                terms: vec![("rust".into(), 2, vec![(0, 4), (6, 10)])],
                length: 2,
            },
            None,
        );
        store.add_document_with_lineage(
            7,
            "c".to_string(),
            AnalyzedDoc {
                terms: vec![("search".into(), 1, vec![(0, 6)])],
                length: 1,
            },
            Some(lineage(7)),
        );
        store.save(&path).unwrap();
        let reader = Bm25Reader::open(&path).unwrap();
        for slot in 0..store.next_doc_id() {
            assert_eq!(
                reader.lineage(slot),
                store.lineage(slot),
                "lineage({slot})"
            );
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The v3/v4 reader's posting_offsets walks with early exit over the
    /// doc-id-ordered postings (the trait default full-scans — O(df) per
    /// survivor). Correctness across first/middle/last/absent targets;
    /// the win itself is the bench's v4 k=1000 column.
    #[test]
    fn v4_posting_offsets_early_exit_correctness() {
        let dir = test_dir("v4early");
        let path = dir.join("shard.bm25");
        let mut store = Bm25Store::new();
        let n = 5000u32;
        for i in 0..n {
            store.add_document(
                i * 2, // gaps: even ids only
                format!("doc {i}"),
                AnalyzedDoc {
                    terms: vec![("court".to_string(), 1, vec![(i, i + 3)])],
                    length: 5,
                },
            );
        }
        {
            let mut w = io::BufWriter::new(std::fs::File::create(&path).unwrap());
            store.write_v4_for_bench(&mut w).unwrap();
            w.flush().unwrap();
        }
        let reader = Bm25Reader::open(&path).unwrap();
        assert!(!reader.v5, "expected the v4 path");
        // First posting of the list (early exit after one record).
        assert_eq!(reader.posting_offsets("court", 0), vec![(0, 3)]);
        // Middle and last.
        assert_eq!(reader.posting_offsets("court", 4998), vec![(2499, 2502)]);
        assert_eq!(reader.posting_offsets("court", 9998), vec![(4999, 5002)]);
        // Absent: a gap id, beyond the last, and an unknown term.
        assert!(reader.posting_offsets("court", 9997).is_empty());
        assert!(reader.posting_offsets("court", 2_000_000).is_empty());
        assert!(reader.posting_offsets("nope", 0).is_empty());
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn load_rejects_garbage() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("target/tmp").join(format!("tvbm25_bad_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("bad.bm25");
        std::fs::write(&path, b"not a postings file").unwrap();
        assert!(Bm25Store::load(&path).is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }

    // --- v5 (TVBM2505) tests -------------------------------------------

    /// Hand-rolled deterministic RNG (LCG, same style as
    /// `harness::unit_vectors`) — no proptest dependency.
    struct Lcg(u64);

    impl Lcg {
        fn new(seed: u64) -> Self {
            Self(seed.wrapping_add(0x9E3779B97F4A7C15))
        }
        fn next(&mut self) -> u64 {
            self.0 = self
                .0
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            self.0 >> 33
        }
        fn below(&mut self, n: u64) -> u64 {
            self.next() % n
        }
    }

    fn test_dir(name: &str) -> PathBuf {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target/tmp")
            .join(format!("tvbm25_{name}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    /// A random corpus: gappy ids, a small vocabulary so terms repeat
    /// across docs, mixed tf, some SCORING_ONLY postings (no offsets).
    fn random_corpus(rng: &mut Lcg, n_docs: u64, vocab: &[String]) -> Vec<(u32, String, AnalyzedDoc)> {
        let mut docs = Vec::new();
        let mut id = 0u32;
        for _ in 0..n_docs {
            id += 1 + rng.below(3) as u32;
            let mut terms: DocTerms = Vec::new();
            let mut length = 0;
            for _ in 0..1 + rng.below(6) {
                let term = vocab[rng.below(vocab.len() as u64) as usize].clone();
                if terms.iter().any(|(t, _, _)| *t == term) {
                    continue;
                }
                let tf = 1 + rng.below(4) as u32;
                let offsets: Vec<(u32, u32)> = if rng.below(3) == 0 {
                    Vec::new()
                } else {
                    (0..tf)
                        .map(|o| {
                            let start = o * 10 + rng.below(5) as u32;
                            (start, start + 1 + rng.below(4) as u32)
                        })
                        .collect()
                };
                length += tf;
                terms.push((term, tf, offsets));
            }
            docs.push((id, format!("doc {id}"), AnalyzedDoc { terms, length }));
        }
        docs
    }

    fn build_store(corpus: &[(u32, String, AnalyzedDoc)]) -> Bm25Store {
        let mut store = Bm25Store::new();
        for (id, text, doc) in corpus {
            store.add_document(*id, text.clone(), doc.clone());
        }
        store
    }

    fn vocab(rng: &mut Lcg, n: u64) -> Vec<String> {
        (0..n.max(1)).map(|i| format!("t{}", i + rng.below(1))).collect()
    }

    /// v5 round trip: the reader serves exactly what the heap store
    /// holds, through every trait method the scorer uses.
    #[test]
    fn v5_round_trip_matches_heap_store() {
        let dir = test_dir("v5rt");
        let path = dir.join("shard.bm25");
        let corpus = synthetic_corpus();
        let mut store = Bm25Store::new();
        for (id, text, doc, lineage) in &corpus {
            store.add_document_with_lineage(*id, text.clone(), doc.clone(), *lineage);
        }
        store.save(&path).unwrap();
        let reader = Bm25Reader::open(&path).unwrap();

        assert_eq!(Bm25Index::doc_count(&reader), store.doc_count());
        assert_eq!(reader.total_doc_length(), store.total_doc_length());
        assert_eq!(reader.next_doc_id(), store.next_doc_id());
        for term in ["court", "plaintiff", "rust", "search", "vector", "quant", "missing"] {
            assert_eq!(reader.df(term), store.df(term), "df({term})");
            // for_each_posting: identical (doc, tf, offsets) stream.
            let mut got = Vec::new();
            reader.for_each_posting(term, &mut |d, tf, o| got.push((d, tf, o.to_vec())));
            let want: Vec<(u32, u32, Vec<(u32, u32)>)> = store
                .postings(term)
                .map(|ps| ps.iter().map(|p| (p.doc_id, p.tf, p.offsets.clone())).collect())
                .unwrap_or_default();
            assert_eq!(got, want, "for_each_posting({term})");
            // for_each_doc_tf: identical (doc, tf) stream.
            let mut got_tf = Vec::new();
            reader.for_each_doc_tf(term, &mut |d, tf| got_tf.push((d, tf)));
            let want_tf: Vec<(u32, u32)> = want.iter().map(|(d, tf, _)| (*d, *tf)).collect();
            assert_eq!(got_tf, want_tf, "for_each_doc_tf({term})");
            // posting_offsets: exact per (term, doc).
            for (d, _, offs) in &want {
                assert_eq!(&reader.posting_offsets(term, *d), offs, "posting_offsets({term}, {d})");
            }
            assert!(reader.posting_offsets(term, u32::MAX).is_empty());
        }
        // text / lineage / doc_length (lineage exercises the lazily
        // built offset index: this corpus has gap slots and missing
        // lineages).
        for (id, text, _, lineage) in &corpus {
            assert_eq!(reader.text(*id).as_deref(), Some(text.as_str()), "text({id})");
            assert_eq!(reader.lineage(*id), *lineage, "lineage({id})");
        }

        // Scoring surface: top_k and score_candidates identical to heap.
        let terms = vec!["court".to_string(), "rust".to_string(), "missing".to_string()];
        let stats = crate::bm25::CorpusStats {
            doc_count: store.doc_count(),
            total_doc_length: store.total_doc_length(),
            dfs: terms.iter().map(|t| store.df(t)).collect(),
        };
        let params = crate::bm25::Bm25Params::default();
        let heap_hits = crate::bm25::top_k(&store, &terms, &stats, params, 20);
        let disk_hits = crate::bm25::top_k(&reader, &terms, &stats, params, 20);
        assert_eq!(heap_hits, disk_hits, "top_k heap vs v5 reader");
        let oracle = crate::bm25::top_k_exhaustive(&store, &terms, &stats, params, 20);
        assert_eq!(heap_hits, oracle, "top_k vs exhaustive oracle");
        let ids: Vec<u32> = (0..store.next_doc_id()).filter(|i| i % 3 == 0).collect();
        let heap_c = crate::bm25::score_candidates(&store, &terms, &stats, params, &ids);
        let disk_c = crate::bm25::score_candidates(&reader, &terms, &stats, params, &ids);
        assert_eq!(heap_c, disk_c, "score_candidates heap vs v5 reader");

        // And the heap reload path (shard append) parses v5.
        let loaded = Bm25Store::load(&path).unwrap();
        assert_eq!(loaded.postings, store.postings);
        assert_eq!(loaded.doc_lengths, store.doc_lengths);
        assert_eq!(loaded.texts, store.texts);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Bitwise cross-format property test: the SAME corpus written to v4
    /// and v5 must answer top_k with identical `(doc_id, score.to_bits(),
    /// term_offsets)` sequences — over random corpora, random k, random
    /// term counts, random k1/b — and identical to the exhaustive oracle.
    #[test]
    fn v5_v4_top_k_bitwise_identical() {
        let dir = test_dir("v5x");
        let mut rng = Lcg::new(0xB10C);
        let k1s = [0.0, 0.6, 1.2, 2.0];
        let bs = [0.0, 0.4, 0.75, 1.0];
        for round in 0..24 {
            let n_voc = 3 + rng.below(20);
            let voc = vocab(&mut rng, n_voc);
            let n_docs = 30 + rng.below(170);
            let corpus = random_corpus(&mut rng, n_docs, &voc);
            let store = build_store(&corpus);
            let v5_path = dir.join(format!("r{round}.v5.bm25"));
            let v4_path = dir.join(format!("r{round}.v4.bm25"));
            store.save(&v5_path).unwrap();
            {
                let mut w = io::BufWriter::new(std::fs::File::create(&v4_path).unwrap());
                store.write_v4_for_bench(&mut w).unwrap();
                w.flush().unwrap();
            }
            let v5r = Bm25Reader::open(&v5_path).unwrap();
            let v4r = Bm25Reader::open(&v4_path).unwrap();
            for _ in 0..6 {
                let n_terms = 1 + rng.below(4) as usize;
                let terms: Vec<String> = voc
                    .iter()
                    .filter(|_| rng.below(2) == 0)
                    .take(n_terms.max(1))
                    .cloned()
                    .collect();
                let terms = if terms.is_empty() { vec![voc[0].clone()] } else { terms };
                let stats = crate::bm25::CorpusStats {
                    doc_count: store.doc_count(),
                    total_doc_length: store.total_doc_length(),
                    dfs: terms.iter().map(|t| store.df(t)).collect(),
                };
                let params = crate::bm25::Bm25Params {
                    k1: k1s[rng.below(k1s.len() as u64) as usize],
                    b: bs[rng.below(bs.len() as u64) as usize],
                };
                let k = 1 + rng.below(30) as usize;
                let sig = |hits: &[crate::bm25::ScoredDoc]| -> Vec<(u32, u64, Vec<(usize, Vec<(u32, u32)>)>)> {
                    hits.iter()
                        .map(|h| (h.doc_id, h.score.to_bits(), h.term_offsets.clone()))
                        .collect()
                };
                let heap = sig(&crate::bm25::top_k(&store, &terms, &stats, params, k));
                let oracle = sig(&crate::bm25::top_k_exhaustive(&store, &terms, &stats, params, k));
                let v5 = sig(&crate::bm25::top_k(&v5r, &terms, &stats, params, k));
                let v4 = sig(&crate::bm25::top_k(&v4r, &terms, &stats, params, k));
                assert_eq!(heap, oracle, "round {round}: top_k diverged from oracle");
                assert_eq!(heap, v5, "round {round}: v5 reader diverged");
                assert_eq!(heap, v4, "round {round}: v4 reader diverged");
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn pareto_frontier_prunes_dominated() {
        let f = pareto_frontier(&[(5, 10), (3, 10), (5, 12), (1, 1)]);
        assert_eq!(f, vec![(1, 1), (5, 10)]);
        // Empty block, single pair, duplicates.
        assert!(pareto_frontier(&[]).is_empty());
        assert_eq!(pareto_frontier(&[(7, 7), (7, 7)]), vec![(7, 7)]);
    }

    #[test]
    fn pareto_frontier_collapses_never_drops() {
        // A 20-point staircase is entirely on the frontier and forces
        // collapsing down to the cap.
        let pairs: Vec<(u32, u32)> = (1..=20u32).map(|i| (i, i * 10)).collect();
        let f = pareto_frontier(&pairs);
        assert!(f.len() <= MAX_FRONTIER, "frontier not capped: {f:?}");
        // Bound property: every original point is weakly dominated by
        // some stored pair (tf' >= tf, dl' <= dl) — collapsing only ever
        // raises the bound, it never drops coverage.
        for &(tf, dl) in &pairs {
            assert!(
                f.iter().any(|&(t, d)| t >= tf && d <= dl),
                "({tf},{dl}) not bounded by collapsed frontier {f:?}"
            );
        }
        // The staircase stays strictly increasing in both coordinates.
        for w in f.windows(2) {
            assert!(w[1].0 > w[0].0 && w[1].1 > w[0].1, "not a staircase: {f:?}");
        }
    }

    /// Frontier safety gate: for every level-0 block of every term, the
    /// stored (possibly collapsed) frontier's max over tf_norm is >= the
    /// true block max, at several avgdl values and k1/b combos.
    #[test]
    fn v5_frontier_never_underbounds() {
        use crate::bm25::{tf_norm, Bm25Params};
        let dir = test_dir("v5frontier");

        fn check(store: &Bm25Store, reader: &Bm25Reader, terms: &[String]) -> bool {
            let params_list = [
                Bm25Params { k1: 0.0, b: 0.0 },
                Bm25Params { k1: 1.2, b: 0.75 },
                Bm25Params { k1: 2.0, b: 1.0 },
            ];
            let avgdl = store.total_doc_length() as f64 / store.doc_count() as f64;
            let mut saw_collapsed = false;
            for term in terms {
                let Some((_, skip_off, _, df)) = reader.directory_lookup_v5(term) else {
                    continue;
                };
                let records = reader.v5_l0_records(skip_off, df);
                let postings = store.postings(term).unwrap();
                assert_eq!(records.len(), postings.len().div_ceil(BLOCK));
                for (block_i, (_, pairs)) in records.iter().enumerate() {
                    assert!(!pairs.is_empty() && pairs.len() <= MAX_FRONTIER);
                    saw_collapsed |= pairs.len() == MAX_FRONTIER;
                    let block =
                        &postings[block_i * BLOCK..((block_i + 1) * BLOCK).min(postings.len())];
                    // Block frontier covers every posting (tf' >= tf, dl' <= dl).
                    for p in block {
                        let dl = store.doc_length(p.doc_id);
                        assert!(
                            pairs.iter().any(|&(t, d)| t >= p.tf && d <= dl),
                            "{term} block {block_i}: posting ({},{dl}) not bounded by {pairs:?}",
                            p.tf
                        );
                    }
                    for params in params_list {
                        for a in [avgdl, 1.0, 50.0, 1000.0] {
                            let true_max = block
                                .iter()
                                .map(|p| tf_norm(params, p.tf, store.doc_length(p.doc_id), a))
                                .fold(f64::NEG_INFINITY, f64::max);
                            let stored_max = pairs
                                .iter()
                                .map(|&(t, d)| tf_norm(params, t, d, a))
                                .fold(f64::NEG_INFINITY, f64::max);
                            assert!(
                                stored_max >= true_max,
                                "{term} block {block_i} underbounds at avgdl {a}, {params:?}: \
                                 stored {stored_max} < true {true_max}"
                            );
                        }
                    }
                }
            }
            saw_collapsed
        }

        // Small vocab + many docs: every term spans several 128-posting
        // blocks.
        let mut rng = Lcg::new(0xFE07);
        let voc = vocab(&mut rng, 4);
        let corpus = random_corpus(&mut rng, 900, &voc);
        let store = build_store(&corpus);
        let path = dir.join("random.bm25");
        store.save(&path).unwrap();
        let reader = Bm25Reader::open(&path).unwrap();
        check(&store, &reader, &voc);

        // A (tf, dl) staircase forces frontier collapse: within a block,
        // tf and dl both strictly increase, so every posting is on the
        // frontier and the cap must collapse, never drop.
        let mut stair = Bm25Store::new();
        for i in 0..300u32 {
            stair.add_document(
                i,
                format!("doc {i}"),
                AnalyzedDoc {
                    terms: vec![("stair".to_string(), 1 + i % 150, vec![(i, i + 2)])],
                    length: 100 + i,
                },
            );
        }
        let path = dir.join("stair.bm25");
        stair.save(&path).unwrap();
        let reader = Bm25Reader::open(&path).unwrap();
        assert!(
            check(&stair, &reader, &["stair".to_string()]),
            "collapse path never exercised"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Laziness proof: corrupt the occurrence run of a COPY of a v5 file;
    /// the for_each_doc_tf-based scored path (top_k) must return identical
    /// scores/doc ids, while posting_offsets returns the garbage — the
    /// scored path never touched the occurrence bytes.
    #[test]
    fn v5_scored_path_never_reads_occurrence_run() {
        let dir = test_dir("v5lazy");
        let path = dir.join("shard.bm25");
        // One term, several docs, offsets on every posting.
        let mut store = Bm25Store::new();
        for i in 0..50u32 {
            let tf = 1 + i % 3;
            store.add_document(
                i,
                format!("doc {i}"),
                AnalyzedDoc {
                    terms: vec![(
                        "court".to_string(),
                        tf,
                        (0..tf).map(|o| (o * 10 + i, o * 10 + i + 4)).collect(),
                    )],
                    length: tf,
                },
            );
        }
        store.save(&path).unwrap();
        let reader = Bm25Reader::open(&path).unwrap();
        let (_, skip_off, occ_off, _) = reader.directory_lookup_v5("court").unwrap();

        let corrupted = dir.join("corrupted.bm25");
        let mut bytes = std::fs::read(&path).unwrap();
        for b in &mut bytes[occ_off as usize..skip_off as usize] {
            *b ^= 0xFF;
        }
        std::fs::write(&corrupted, &bytes).unwrap();
        drop(reader);
        let good = Bm25Reader::open(&path).unwrap();
        let bad = Bm25Reader::open(&corrupted).unwrap();

        let terms = vec!["court".to_string()];
        let stats = crate::bm25::CorpusStats {
            doc_count: store.doc_count(),
            total_doc_length: store.total_doc_length(),
            dfs: vec![store.df("court")],
        };
        let params = crate::bm25::Bm25Params::default();
        let sig = |hits: &[crate::bm25::ScoredDoc]| -> Vec<(u32, u64)> {
            hits.iter().map(|h| (h.doc_id, h.score.to_bits())).collect()
        };
        let a = crate::bm25::top_k(&good, &terms, &stats, params, 10);
        let b = crate::bm25::top_k(&bad, &terms, &stats, params, 10);
        assert_eq!(sig(&a), sig(&b), "scored path touched occurrence bytes");
        // The survivors' offsets DO come from the occurrence run, so they
        // reflect the corruption — proving the corruption took and that
        // offsets really are fetched separately.
        assert_ne!(
            a.iter().map(|h| &h.term_offsets).collect::<Vec<_>>(),
            b.iter().map(|h| &h.term_offsets).collect::<Vec<_>>(),
            "corruption did not reach the occurrence run"
        );
        // posting_offsets decodes the corrupted run: garbage, same shape.
        let good_offs = good.posting_offsets("court", 7);
        let bad_offs = bad.posting_offsets("court", 7);
        assert_eq!(good_offs.len(), bad_offs.len());
        assert_ne!(good_offs, bad_offs);

        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[cfg(test)]
mod malformed_tests {
    use super::*;

    /// Build a store whose v5 file exercises every validated structure:
    /// one term with >4096 postings (2 level-1 groups), other terms,
    /// gap slots, lineages, offsets.
    fn malformed_corpus_store() -> Bm25Store {
        let mut store = Bm25Store::new();
        for i in 0..4300u32 {
            let id = i * 2; // gap slots
            let lineage = (i % 3 == 0).then_some(DocLineage {
                opinion_id: u64::from(i),
                cluster_id: u64::from(i) * 7,
                span_start: i,
                span_end: i + 10,
            });
            let mut terms: DocTerms = vec![
                ("hot".to_string(), 1 + i % 3, vec![(i, i + 2)]),
                (format!("t{}", i % 7), 1, vec![(0, 1)]),
            ];
            if i % 11 == 0 {
                terms.push(("cold".to_string(), 2, vec![(3, 9)]));
            }
            let length = terms.iter().map(|t| t.1).sum();
            store.add_document_with_lineage(
                id,
                format!("d{i}"),
                AnalyzedDoc { terms, length },
                lineage,
            );
        }
        store
    }

    fn write_v4(store: &Bm25Store, path: &Path) {
        let mut w = io::BufWriter::new(std::fs::File::create(path).unwrap());
        store.write_v4_for_bench(&mut w).unwrap();
        w.flush().unwrap();
    }

    /// A small store (~few KB on disk) for the every-byte truncation
    /// sweep: gaps, lineages, one multi-block term, offsets.
    fn small_corpus_store() -> Bm25Store {
        let mut store = Bm25Store::new();
        for i in 0..140u32 {
            let id = i * 2;
            let lineage = (i % 3 == 0).then_some(DocLineage {
                opinion_id: u64::from(i),
                cluster_id: u64::from(i) * 7,
                span_start: i,
                span_end: i + 10,
            });
            let terms: DocTerms = vec![
                ("hot".to_string(), 1 + i % 3, vec![(i, i + 2)]),
                (format!("t{}", i % 3), 1, vec![(0, 1)]),
            ];
            let length = terms.iter().map(|t| t.1).sum();
            store.add_document_with_lineage(
                id,
                format!("d{i}"),
                AnalyzedDoc { terms, length },
                lineage,
            );
        }
        store
    }

    /// Truncation at EVERY byte length must error, never panic — on a
    /// small file so the sweep stays fast. v5 and v4.
    #[test]
    fn truncated_open_errors_never_panics() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target/tmp")
            .join(format!("truncate_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = small_corpus_store();
        let v5_path = dir.join("small.v5.bm25");
        let v4_path = dir.join("small.v4.bm25");
        store.save(&v5_path).unwrap();
        write_v4(&store, &v4_path);
        for (tag, src) in [("v5", &v5_path), ("v4", &v4_path)] {
            let bytes = std::fs::read(src).unwrap();
            Bm25Reader::open(src).unwrap();
            let trunc = dir.join(format!("trunc.{tag}"));
            for len in 0..bytes.len() {
                std::fs::write(&trunc, &bytes[..len]).unwrap();
                assert!(
                    Bm25Reader::open(&trunc).is_err(),
                    "{tag}: truncation to {len} bytes opened successfully"
                );
            }
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn corrupted_open_errors_never_panics() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target/tmp")
            .join(format!("malformed_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = malformed_corpus_store();
        let v5_path = dir.join("corpus.v5.bm25");
        let v4_path = dir.join("corpus.v4.bm25");
        store.save(&v5_path).unwrap();
        write_v4(&store, &v4_path);

        for (tag, src) in [("v5", &v5_path), ("v4", &v4_path)] {
            let bytes = std::fs::read(src).unwrap();
            // Sanity: the intact file opens.
            Bm25Reader::open(src).unwrap();

            // Random single-byte flips in the header, directory, and
            // skip-run regions: error or valid open, never a panic.
            let mut s = 0xC0FFEEu64;
            let mut next = move || {
                s = s
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                (s >> 33) as usize
            };
            // Skip-run regions (v5 only): from the directory. Header:
            // magic(8) total_length(8) texts(8) lineages(8) postings(8)
            // directory(8) n_slots(4).
            let directory_off = u64::from_le_bytes(bytes[40..48].try_into().unwrap()) as usize;
            let n_terms = u32::from_le_bytes(
                bytes[directory_off..directory_off + 4].try_into().unwrap(),
            ) as usize;
            let mut regions: Vec<(usize, usize)> = vec![
                (0, 52),
                (directory_off, bytes.len()),
            ];
            if tag == "v5" {
                for i in 0..n_terms {
                    let e = directory_off + 4 + 34 * i;
                    let skip_off = u64::from_le_bytes(
                        bytes[e + 8..e + 16].try_into().unwrap(),
                    ) as usize;
                    let end = if i + 1 < n_terms {
                        u64::from_le_bytes(bytes[e + 34..e + 42].try_into().unwrap()) as usize
                    } else {
                        directory_off
                    };
                    regions.push((skip_off, end));
                }
            }
            // v3/v4 flips stay in the header and directory: postings
            // record payloads are data (like occurrence bytes and
            // frontier pairs), not validated structure.
            let flipped = dir.join(format!("flip.{tag}"));
            let (mut errors, mut opens) = (0usize, 0usize);
            for _ in 0..2000 {
                let &(lo, hi) = &regions[next() % regions.len()];
                let pos = lo + next() % (hi - lo).max(1);
                let mut bad = bytes.clone();
                bad[pos] ^= 1 + (next() % 255) as u8;
                std::fs::write(&flipped, &bad).unwrap();
                match Bm25Reader::open(&flipped) {
                    Ok(reader) => {
                        opens += 1;
                        // A valid open must stay usable on the read
                        // paths the structure guards (no panics).
                        let _ = reader.df("hot");
                        reader.for_each_doc_tf("hot", &mut |_, _| {});
                        let _ = reader.posting_offsets("hot", 0);
                    }
                    Err(_) => errors += 1,
                }
            }
            assert!(errors > 0, "{tag}: no flip was ever rejected");
            eprintln!("{tag}: flips: {errors} rejected, {opens} opened-valid");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }
}
