//! Per-shard BM25 postings index and doc store, with persistence.
//!
//! The shard owns: term → postings (doc id, tf, occurrence offsets in
//! original-text coordinates), per-document lengths and corpus totals, and
//! the raw document texts (the highlight source). Append-only: no deletes,
//! no updates, so postings for a term stay doc-id-ordered by construction.
//!
//! Two storage shapes share one read surface ([`Bm25Index`]):
//!
//! - [`Bm25Store`] — the heap builder. Ingest appends here; `save`
//!   writes the current format (v8: a v6/v7 payload wearing a CRC
//!   integrity table, see `MAGIC_V8`) atomically next to the shard's
//!   `.tv` as `<index>.bm25`.
//! - [`Bm25Reader`] — the disk-resident shape. The file is memory
//!   mapped; postings slices and document texts are read from the map on
//!   demand (the OS page cache is the buffer pool, the Lucene model), so
//!   a shard far larger than RAM serves from a few MB of heap (per-doc
//!   length/offset tables only). v3 through v7 files stay readable.
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
/// v6 layout: multi-field (`docs/multi-field.md`). Variable-length
/// header holding an explicit section table (shared texts/text_index/
/// lineages offsets plus a field table locating each field's
/// doc_lengths/postings/directory sections); per-field sections keep
/// the v5 shape, but section-internal pointers (text_index entries,
/// directory run offsets) are RELATIVE to their section's start — the
/// v4 blob lesson generalized, so sections survive relocation.
const MAGIC_V6: &[u8; 8] = b"TVBM2506";
/// v7 layout: v6 plus a kinded per-document column table
/// (`docs/facets.md`, `docs/score-functions.md`). The header gains a
/// column table after the field table (u32 n_columns, then per column:
/// name, u8 kind, kind-specific payload); the column sections follow
/// the last field group in table order. Kind 0 is a dictionary-encoded
/// string facet column (u32 n_values, u64 dict_off, u64 ords_off →
/// dict + ords sections); kind 1 is an f64 numeric column (u64
/// min_bits, u64 max_bits, u64 vals_off → one n_slots x f64 section,
/// NaN = absent); kinds 2 and 3 are the map columns
/// (`docs/map-columns.md`); kind 4 is an i64 column (same three-word
/// payload as kind 1, min/max as i64 bits → one n_slots x i64 section,
/// `i64::MIN` = absent, `docs/range-facets.md`); kind 5 is a geo-point
/// column (four bbox words + u64 vals_off → one n_slots x (f64, f64)
/// section, both NaN = absent, `docs/geo-columns.md`). An unknown kind
/// refuses at open by number. A store with NO declared columns still
/// writes v6, byte-identical to every pre-facet build — the format
/// break is opt-in per shard.
const MAGIC_V7: &[u8; 8] = b"TVBM2507";

/// v7 column-table kind: dictionary-encoded string facet column.
const COLUMN_KIND_FACET: u8 = 0;
/// v7 column-table kind: f64 numeric column (NaN = absent).
const COLUMN_KIND_F64: u8 = 1;
/// v7 column-table kind: map<string, string> column
/// (`docs/map-columns.md`): key dict + value dict + per-doc
/// (key_ord, value_ord) pair lists behind a fixed-stride offsets
/// section. ONE column regardless of key cardinality — the structural
/// answer to the field-per-key explosion flattening causes.
const COLUMN_KIND_MAP_FACET: u8 = 2;
/// v7 column-table kind: map<string, f64> column: key dict WITH
/// per-key min/max bound metadata + per-doc (key_ord, f64) pair lists.
const COLUMN_KIND_MAP_F64: u8 = 3;
/// v7 column-table kind: i64 column (`docs/range-facets.md`), a
/// fixed-stride per-slot section like kind 1. Exists because f64 stops
/// being exact past 2^53 and this engine argues from exactness: an id,
/// a citation count, or an epoch-micros timestamp must come back the
/// integer it went in as.
const COLUMN_KIND_I64: u8 = 4;
/// v7 column-table kind: geo-point column (`docs/geo-columns.md`), a
/// per-slot (lat, lon) f64 pair at a fixed 16 B stride. BOTH halves NaN
/// means absent; a half-NaN pair is refused at open as corruption,
/// because a point with one coordinate is not a point. The table entry
/// carries the column's bounding box (min/max lat and lon), validated
/// against a full scan at open like every other kind's metadata.
const COLUMN_KIND_GEO: u8 = 5;
/// Kind 6 is the shard-level mapped-plan BINDING record
/// (`docs/descriptor-mappings.md` section 4a) — not a column: at most
/// one entry, a pinned name, an inline payload (three length-prefixed
/// strings: plan fingerprint, bound body path, materialize-spec hash),
/// and NO sections. Riding the kinded table keeps the binding inside
/// the v8 integrity envelope with zero new format machinery, so it
/// lives and dies with the columns it describes — a binding that could
/// vanish separately from them would protect nothing. A user column
/// declared under the reserved name collides with the table's
/// name-uniqueness rule and refuses at save, loudly.
const COLUMN_KIND_BINDING: u8 = 6;
/// The binding record's reserved entry name.
const BINDING_ENTRY_NAME: &str = "plan-binding";

/// v8 layout: a v6 or v7 payload, byte-identical, wearing integrity.
/// The leading magic becomes `TVBM2508`; after the last payload byte
/// sits an integrity section (u32 n_entries, then per entry u16
/// name_len + name + u64 off + u64 len + u32 crc32, then a u32 crc32
/// of the section itself), and the file ends with a fixed 24-byte
/// trailer: u64 integrity_off, u64 base_version (6 or 7, naming the
/// payload shape), `TVBMINTG`. The entries PARTITION the payload —
/// offset 0, contiguous, ending exactly at integrity_off — one entry
/// per section (`header`, `texts`, `text_index`, `lineages`,
/// `field:<name>:{doc_lengths,postings,directory}`,
/// `column:<name>:<part>`), so a mismatch names what rotted.
///
/// Structural validation (above) proves the skeleton is well-formed;
/// the CRCs prove the payload bytes are the ones the build wrote —
/// the half no walk could check. Open verifies the table, the
/// partition, and every section the open already reads (everything
/// but the big lazily-paged blobs: `texts` and each field's
/// `postings`); [`Bm25Reader::verify_integrity`] reads and checks all
/// of them. Because the magic changes, a v8 file that loses its tail
/// is REFUSED, never quietly demoted to an integrity-less v6/v7 —
/// checksums that can vanish silently protect nothing. Pre-v8 files
/// keep opening as before; they simply have nothing to verify.
const MAGIC_V8: &[u8; 8] = b"TVBM2508";
/// The v8 trailer's closing magic; the trailer is `u64 integrity_off,
/// u64 base_version, TVBMINTG`.
const TRAILER_MAGIC: &[u8; 8] = b"TVBMINTG";
/// Byte length of the v8 trailer.
const TRAILER_LEN: u64 = 24;

/// fsync the directory holding `path`, making a just-renamed entry
/// durable. `sync_all` on the file covers its bytes and inode, not the
/// directory entry pointing at it: a crash between the rename and the
/// directory's own writeback can lose the new name while keeping the
/// bytes. Every atomic tmp-write-rename persist in this crate ends
/// with this call. Directories cannot be opened for fsync on Windows;
/// there the rename's durability rides the metadata journal and this
/// is a no-op.
pub(crate) fn fsync_parent(path: &Path) -> io::Result<()> {
    #[cfg(unix)]
    {
        let dir = match path.parent() {
            Some(p) if !p.as_os_str().is_empty() => p,
            _ => Path::new("."),
        };
        std::fs::File::open(dir)?.sync_all()
    }
    #[cfg(not(unix))]
    {
        let _ = path;
        Ok(())
    }
}

/// One v8 integrity-table entry: a named payload byte range and the
/// CRC32 its build recorded for it.
#[derive(Debug, Clone)]
pub(crate) struct IntegrityEntry {
    pub name: String,
    pub off: u64,
    pub len: u64,
    pub crc: u32,
}

/// The parsed v8 integrity envelope.
#[derive(Debug)]
pub(crate) struct IntegrityTable {
    pub entries: Vec<IntegrityEntry>,
    /// Where the payload ends and the integrity section begins.
    pub payload_len: u64,
    /// Whether the payload is v7-shaped (column table) or v6-shaped.
    pub base_v7: bool,
}

/// The big lazily-paged blobs are the only sections open does not
/// CRC-verify; everything else is read at open anyway, so checking it
/// there is nearly free. `verify_integrity` checks these too.
fn integrity_eager(name: &str) -> bool {
    name != "texts" && !name.ends_with(":postings")
}

/// Parse and check the v8 envelope of `full` (a whole file, leading
/// magic already matched): trailer, table, table CRC, and that the
/// entries partition the payload exactly. Section CRCs are NOT
/// verified here — callers decide eager vs deep.
fn parse_integrity(full: &[u8]) -> io::Result<IntegrityTable> {
    let invalid = |msg: String| io::Error::new(io::ErrorKind::InvalidData, msg);
    let file_len = full.len() as u64;
    if file_len < 52 + TRAILER_LEN {
        return Err(invalid("v8 file shorter than header plus trailer".into()));
    }
    let trailer = file_len - TRAILER_LEN;
    let u64_at = |off: u64| -> u64 {
        u64::from_le_bytes(
            full[off as usize..off as usize + 8]
                .try_into()
                .expect("8 bytes"),
        )
    };
    if &full[(trailer + 16) as usize..] != TRAILER_MAGIC {
        return Err(invalid(
            "v8 trailer magic missing: truncated or overwritten tail".into(),
        ));
    }
    let integrity_off = u64_at(trailer);
    let base_version = u64_at(trailer + 8);
    let base_v7 = match base_version {
        6 => false,
        7 => true,
        v => {
            return Err(invalid(format!(
                "v8 trailer names base version {v}, not 6 or 7"
            )))
        }
    };
    if integrity_off < 52 || integrity_off > trailer {
        return Err(invalid(format!(
            "v8 integrity section offset {integrity_off} outside [52, {trailer}]"
        )));
    }
    let mut cur = integrity_off;
    let need = |cur: u64, n: u64| -> io::Result<()> {
        if cur + n > trailer {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("v8 integrity table runs past its section at {cur}"),
            ));
        }
        Ok(())
    };
    need(cur, 4)?;
    let n_entries = u32::from_le_bytes(full[cur as usize..cur as usize + 4].try_into().unwrap());
    cur += 4;
    if n_entries == 0 {
        return Err(invalid("v8 integrity table with zero entries".into()));
    }
    let mut entries = Vec::with_capacity(n_entries as usize);
    for i in 0..n_entries {
        need(cur, 2)?;
        let name_len = u64::from(u16::from_le_bytes(
            full[cur as usize..cur as usize + 2].try_into().unwrap(),
        ));
        need(cur + 2, name_len + 20)?;
        if name_len == 0 {
            return Err(invalid(format!("v8 integrity entry {i}: empty name")));
        }
        let name = std::str::from_utf8(&full[(cur + 2) as usize..(cur + 2 + name_len) as usize])
            .map_err(|_| invalid(format!("v8 integrity entry {i}: name is not UTF-8")))?
            .to_string();
        let base = cur + 2 + name_len;
        let off = u64_at(base);
        let len = u64_at(base + 8);
        let crc = u32::from_le_bytes(
            full[(base + 16) as usize..(base + 20) as usize]
                .try_into()
                .unwrap(),
        );
        entries.push(IntegrityEntry {
            name,
            off,
            len,
            crc,
        });
        cur = base + 20;
    }
    need(cur, 4)?;
    let stored_table_crc =
        u32::from_le_bytes(full[cur as usize..cur as usize + 4].try_into().unwrap());
    let computed = crate::wal::crc32(&full[integrity_off as usize..cur as usize]);
    if stored_table_crc != computed {
        return Err(invalid(format!(
            "v8 integrity table CRC mismatch: stored {stored_table_crc:08x}, computed {computed:08x}"
        )));
    }
    if cur + 4 != trailer {
        return Err(invalid(format!(
            "v8 integrity section ends at {} but the trailer starts at {trailer}",
            cur + 4
        )));
    }
    // The entries must partition [0, integrity_off): a gap would be
    // bytes no checksum covers, which is the disease this format
    // exists to refuse.
    let mut expected = 0u64;
    for e in &entries {
        if e.off != expected {
            return Err(invalid(format!(
                "v8 integrity entry {} starts at {} but the previous section ended at {expected}",
                e.name, e.off
            )));
        }
        expected = e
            .off
            .checked_add(e.len)
            .ok_or_else(|| invalid(format!("v8 integrity entry {}: length overflows", e.name)))?;
    }
    if expected != integrity_off {
        return Err(invalid(format!(
            "v8 integrity entries cover [0, {expected}) but the payload is [0, {integrity_off})"
        )));
    }
    Ok(IntegrityTable {
        entries,
        payload_len: integrity_off,
        base_v7,
    })
}

/// Every section start of a STRUCTURALLY VALIDATED v6/v7 payload, in
/// file order with its integrity name. The walk mirrors
/// [`Bm25Reader::open_v6v7`]'s header parse; ranges are the gaps
/// between consecutive starts (the writer lays sections with one
/// cursor, so they are contiguous by construction).
fn v6v7_section_starts(map: &[u8], v7: bool) -> io::Result<Vec<(String, u64)>> {
    let u32_at = |off: usize| u32::from_le_bytes(map[off..off + 4].try_into().expect("4 bytes"));
    let u64_at = |off: usize| u64::from_le_bytes(map[off..off + 8].try_into().expect("8 bytes"));
    let mut starts: Vec<(String, u64)> = vec![("header".to_string(), 0)];
    let n_fields = u32_at(8) as usize;
    starts.push(("texts".to_string(), u64_at(16)));
    starts.push(("text_index".to_string(), u64_at(24)));
    starts.push(("lineages".to_string(), u64_at(32)));
    let mut cursor = 40usize;
    for _ in 0..n_fields {
        let name_len = u16::from_le_bytes(map[cursor..cursor + 2].try_into().unwrap()) as usize;
        let name = String::from_utf8_lossy(&map[cursor + 2..cursor + 2 + name_len]).into_owned();
        let base = cursor + 2 + name_len;
        starts.push((format!("field:{name}:doc_lengths"), u64_at(base + 16)));
        starts.push((format!("field:{name}:postings"), u64_at(base + 24)));
        starts.push((format!("field:{name}:directory"), u64_at(base + 32)));
        cursor = base + 40;
    }
    if v7 {
        let n_columns = u32_at(cursor) as usize;
        cursor += 4;
        for _ in 0..n_columns {
            let name_len = u16::from_le_bytes(map[cursor..cursor + 2].try_into().unwrap()) as usize;
            let name =
                String::from_utf8_lossy(&map[cursor + 2..cursor + 2 + name_len]).into_owned();
            let kind = map[cursor + 2 + name_len];
            let base = cursor + 2 + name_len + 1;
            match kind {
                COLUMN_KIND_FACET => {
                    starts.push((format!("column:{name}:dict"), u64_at(base + 4)));
                    starts.push((format!("column:{name}:ords"), u64_at(base + 12)));
                    cursor = base + 20;
                }
                COLUMN_KIND_F64 => {
                    starts.push((format!("column:{name}:vals"), u64_at(base + 16)));
                    cursor = base + 24;
                }
                COLUMN_KIND_MAP_FACET => {
                    starts.push((format!("column:{name}:keys"), u64_at(base + 8)));
                    starts.push((format!("column:{name}:values"), u64_at(base + 16)));
                    starts.push((format!("column:{name}:offsets"), u64_at(base + 24)));
                    starts.push((format!("column:{name}:pairs"), u64_at(base + 32)));
                    cursor = base + 40;
                }
                COLUMN_KIND_MAP_F64 => {
                    starts.push((format!("column:{name}:keys"), u64_at(base + 4)));
                    starts.push((format!("column:{name}:offsets"), u64_at(base + 12)));
                    starts.push((format!("column:{name}:pairs"), u64_at(base + 20)));
                    cursor = base + 28;
                }
                COLUMN_KIND_I64 => {
                    starts.push((format!("column:{name}:vals"), u64_at(base + 16)));
                    cursor = base + 24;
                }
                COLUMN_KIND_GEO => {
                    starts.push((format!("column:{name}:vals"), u64_at(base + 32)));
                    cursor = base + 40;
                }
                COLUMN_KIND_BINDING => {
                    // Inline payload only: three length-prefixed
                    // strings, no sections to name.
                    let mut skip = base;
                    for _ in 0..3 {
                        let len =
                            u16::from_le_bytes(map[skip..skip + 2].try_into().unwrap()) as usize;
                        skip += 2 + len;
                    }
                    cursor = skip;
                }
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("column {name}: unknown kind {other}"),
                    ))
                }
            }
        }
    }
    // Contiguity belt-and-braces: file order and strictly ascending,
    // or the derived ranges would be nonsense.
    for pair in starts.windows(2) {
        if pair[1].1 <= pair[0].1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "section {} at {} does not follow {} at {}",
                    pair[1].0, pair[1].1, pair[0].0, pair[0].1
                ),
            ));
        }
    }
    Ok(starts)
}

/// The v8 post-pass: turn a finished v6/v7 file into a v8 one in
/// place. Validates the payload (never stamp integrity onto malformed
/// bytes), CRCs every section, appends the integrity section and
/// trailer, and patches the leading magic LAST — a crash mid-pass
/// leaves a v6/v7 magic with a garbage tail, which the validator's
/// section-extent checks refuse loudly. Ends with `sync_all`; the
/// caller renames and fsyncs the parent.
pub(crate) fn finalize_v8(path: &Path) -> io::Result<()> {
    use std::io::{Seek, SeekFrom};
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?;
    let tail: Vec<u8> = {
        let map = unsafe { memmap2::MmapOptions::new().map(&file)? };
        let base_v7 = match map.get(..8) {
            Some(m) if m == MAGIC_V7 => true,
            Some(m) if m == MAGIC_V6 => false,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "finalize_v8 expects a finished v6 or v7 file",
                ))
            }
        };
        validate_structure_v6(&map, base_v7)?;
        let starts = v6v7_section_starts(&map, base_v7)?;
        let integrity_off = map.len() as u64;
        let mut section: Vec<u8> = Vec::with_capacity(starts.len() * 48 + 32);
        section.extend_from_slice(&(starts.len() as u32).to_le_bytes());
        for (i, (name, off)) in starts.iter().enumerate() {
            let end = starts.get(i + 1).map_or(integrity_off, |s| s.1);
            // The header's CRC must describe the FINAL bytes, whose
            // magic this pass is about to patch to v8 — computing it
            // over the still-v6/v7 bytes would refuse every file at
            // open.
            let crc = if *off == 0 {
                let mut header = Vec::with_capacity(end as usize);
                header.extend_from_slice(MAGIC_V8);
                header.extend_from_slice(&map[8..end as usize]);
                crate::wal::crc32(&header)
            } else {
                crate::wal::crc32(&map[*off as usize..end as usize])
            };
            section.extend_from_slice(&(name.len() as u16).to_le_bytes());
            section.extend_from_slice(name.as_bytes());
            section.extend_from_slice(&off.to_le_bytes());
            section.extend_from_slice(&(end - off).to_le_bytes());
            section.extend_from_slice(&crc.to_le_bytes());
        }
        let table_crc = crate::wal::crc32(&section);
        section.extend_from_slice(&table_crc.to_le_bytes());
        section.extend_from_slice(&integrity_off.to_le_bytes());
        section.extend_from_slice(&if base_v7 { 7u64 } else { 6u64 }.to_le_bytes());
        section.extend_from_slice(TRAILER_MAGIC);
        section
    };
    file.seek(SeekFrom::End(0))?;
    file.write_all(&tail)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(MAGIC_V8)?;
    file.sync_all()
}

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
    pub parent_id: u64,
    /// Source cluster id.
    pub group_id: u64,
    /// Chunk span start in original-text (char) coordinates.
    pub span_start: u32,
    /// Chunk span end (exclusive).
    pub span_end: u32,
}

/// One analyzed field of one document: per-term data plus the field's
/// length in terms (sum of frequencies, the BM25 length-normalization
/// input).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct AnalyzedField {
    /// Per-term data; see [`DocTerms`].
    pub terms: DocTerms,
    /// Field length in terms.
    pub length: u32,
}

/// One analyzed document ready to be indexed: one entry per field,
/// positionally matching the store's field table. Field 0 is the body
/// (the stored text). A document may carry fewer entries than the
/// store has fields; missing trailing fields index as empty.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct AnalyzedDoc {
    /// Per-field analyzed data, indexed by field id.
    pub fields: Vec<AnalyzedField>,
    /// Per-document quality scalars from the sidecar's noise and
    /// artifact layers, when the ingest asked for them
    /// (`docs/quality-columns.md`). `None` means they were not
    /// requested, which is deliberately distinct from a clean document
    /// (all zeros): the ingest writes a column only when it has a real
    /// measurement for it.
    pub quality: Option<DocQuality>,
    /// Per-document geography reduction from the sidecar's geocoding
    /// layer, when the ingest asked for it
    /// (`docs/geography-columns.md`). `None` means not requested;
    /// `Some` with empty fields means measured-and-found-nothing,
    /// which materializes as column ABSENCE (there is no neutral
    /// coordinate to write).
    pub geography: Option<DocGeography>,
}

/// One document's geography reduction, derived at INGEST and stored
/// as ordinary typed columns (`docs/geography-columns.md`). Every
/// field is optional on purpose: a document that mentions no
/// resolvable place has nothing honest to store.
#[derive(Debug, Clone, PartialEq, Default)]
pub struct DocGeography {
    /// Best resolved location `(lat, lon)`: the highest-confidence
    /// finding, ties broken by text order.
    pub point: Option<(f64, f64)>,
    /// The chosen location's resolution confidence in [0, 1].
    /// Meaningless without `point`; read it only when `point` is set.
    pub confidence: f64,
    /// Top region vote's ISO country code, empty when the document's
    /// location evidence voted for nothing.
    pub country: String,
}

/// One document's quality measurements, all derived at INGEST and
/// stored as ordinary typed columns (`docs/quality-columns.md`). The
/// query path never recomputes them and never calls a model.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct DocQuality {
    /// Worst noise finding's score, in [0, 1]; exactly 0 with no
    /// findings.
    pub noise: f64,
    /// Characters covered by the UNION of the noise findings' spans.
    pub noise_chars: i64,
    /// Number of flagged text artifacts.
    pub artifacts: i64,
}

impl AnalyzedDoc {
    /// A document with only the body field — every single-field ingest
    /// path.
    pub fn body(terms: DocTerms, length: u32) -> Self {
        Self {
            fields: vec![AnalyzedField { terms, length }],
            quality: None,
            geography: None,
        }
    }

    /// The body field's analyzed data, consuming the document (empty
    /// when the document carried no fields).
    pub fn into_body(self) -> AnalyzedField {
        self.fields.into_iter().next().unwrap_or_default()
    }
}

/// One field's slice of the store: its own postings, per-document
/// lengths, and running total. Structurally a complete single-field
/// BM25 index over the shared slot space (`docs/multi-field.md`).
#[derive(Debug)]
struct FieldStore {
    /// Field name from the schema ("body" for field 0).
    name: String,
    /// Hash of the field's AnalysisSpec, persisted in the v6 field
    /// table. 0 until the ingest layer wires real fingerprints.
    analysis_fingerprint: u64,
    /// term → postings, kept ascending by doc id (append-only).
    postings: HashMap<String, Vec<Posting>>,
    /// Per-document length in terms, indexed by local doc id. Sparse
    /// slots (ids consumed by the vector side) hold 0. Every field's
    /// table has the same length (the shared slot count).
    doc_lengths: Vec<u32>,
    /// Sum of this field's document lengths (for avgdl).
    total_length: u64,
}

impl FieldStore {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            analysis_fingerprint: 0,
            postings: HashMap::new(),
            doc_lengths: Vec::new(),
            total_length: 0,
        }
    }
}

/// The ordinal marking "this document has no value for this facet
/// field" in a facet column, in heap and on disk alike.
pub const FACET_ABSENT: u32 = u32::MAX;

/// The value marking "this document has no value" in an i64 column
/// (`docs/range-facets.md`). An i64 column has no NaN to spend, so one
/// value of the domain pays for absence; ingest refuses it explicitly
/// rather than letting a real `i64::MIN` disappear.
pub const INTEGER_ABSENT: i64 = i64::MIN;

/// The pair marking "this document has no point" in a geo-point column
/// (`docs/geo-columns.md`). BOTH halves are NaN: a geo column has a NaN
/// to spend on each axis, and spending both keeps the sentinel
/// unambiguous. A pair with exactly one NaN is neither a point nor an
/// absence — the reader refuses it as corruption rather than guessing
/// which half to believe.
pub const GEO_ABSENT: (f64, f64) = (f64::NAN, f64::NAN);

/// One dictionary-encoded facet column: the value dictionary in
/// ordinal (first-seen) order, and a per-slot ordinal table parallel
/// to the shared slot space. Facet values are opaque strings — never
/// analyzed — counted exactly as ingested.
#[derive(Debug)]
struct FacetStore {
    /// Facet field name from the schema.
    name: String,
    /// Values in ordinal order.
    dict: Vec<String>,
    /// value → ordinal (the dictionary's inverse, heap only).
    index: HashMap<String, u32>,
    /// Per-slot ordinal ([`FACET_ABSENT`] = no value). May be shorter
    /// than the slot count; missing trailing slots read as absent.
    ords: Vec<u32>,
}

impl FacetStore {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            dict: Vec::new(),
            index: HashMap::new(),
            ords: Vec::new(),
        }
    }

    fn ord(&self, slot: usize) -> Option<u32> {
        match self.ords.get(slot).copied() {
            None | Some(FACET_ABSENT) => None,
            some => some,
        }
    }

    /// Record `doc_id`'s value, interning it in the dictionary. One
    /// value per (document, facet field); a second write for the same
    /// slot panics (callers validate duplicates into a refusal before
    /// applying).
    fn set(&mut self, doc_id: u32, value: &str) {
        let slot = doc_id as usize;
        let ord = match self.index.get(value) {
            Some(&ord) => ord,
            None => {
                let ord = u32::try_from(self.dict.len()).expect("facet dictionary exceeds u32");
                assert!(
                    ord != FACET_ABSENT,
                    "facet dictionary exhausted u32 ordinals"
                );
                self.dict.push(value.to_string());
                self.index.insert(value.to_string(), ord);
                ord
            }
        };
        if self.ords.len() <= slot {
            self.ords.resize(slot + 1, FACET_ABSENT);
        }
        assert!(
            self.ords[slot] == FACET_ABSENT,
            "doc {doc_id} already has a value for facet field {:?}",
            self.name
        );
        self.ords[slot] = ord;
    }
}

/// Validate a facet-field name list (shared by the heap store and the
/// spill builder): non-empty unique names that fit u16 lengths.
fn validate_facet_names(names: &[&str]) {
    for (i, name) in names.iter().enumerate() {
        assert!(!name.is_empty(), "facet field {i} has an empty name");
        assert!(
            u16::try_from(name.len()).is_ok(),
            "facet field name exceeds u16 length"
        );
        assert!(
            !names[..i].contains(name),
            "duplicate facet field name {name:?}"
        );
    }
}

/// One f64 numeric column (`docs/score-functions.md`): per-slot values
/// parallel to the shared slot space, NaN = the document has no value.
/// min/max over present values are computed at write time into the v7
/// column table (the bound metadata score-function chains lift with).
#[derive(Debug)]
struct NumericStore {
    /// Numeric field name from the schema.
    name: String,
    /// Per-slot value (NaN = no value). May be shorter than the slot
    /// count; missing trailing slots read as absent.
    vals: Vec<f64>,
}

impl NumericStore {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            vals: Vec::new(),
        }
    }

    fn value(&self, slot: usize) -> Option<f64> {
        match self.vals.get(slot).copied() {
            None => None,
            Some(v) if v.is_nan() => None,
            some => some,
        }
    }

    /// Record `doc_id`'s value. Finite values only (NaN is the absence
    /// sentinel, infinities break the bound algebra — callers validate
    /// into a refusal); one value per (document, field), a second write
    /// panics.
    fn set(&mut self, doc_id: u32, value: f64) {
        assert!(
            value.is_finite(),
            "numeric field {:?}: non-finite value for doc {doc_id}",
            self.name
        );
        let slot = doc_id as usize;
        if self.vals.len() <= slot {
            self.vals.resize(slot + 1, f64::NAN);
        }
        assert!(
            self.vals[slot].is_nan(),
            "doc {doc_id} already has a value for numeric field {:?}",
            self.name
        );
        self.vals[slot] = value;
    }

    /// (min, max) over present values; (NaN, NaN) when none are.
    fn min_max(&self) -> (f64, f64) {
        let mut min = f64::NAN;
        let mut max = f64::NAN;
        for &v in &self.vals {
            if v.is_nan() {
                continue;
            }
            if min.is_nan() || v < min {
                min = v;
            }
            if max.is_nan() || v > max {
                max = v;
            }
        }
        (min, max)
    }
}

/// One i64 column (`docs/range-facets.md`): per-slot values parallel
/// to the shared slot space, [`INTEGER_ABSENT`] = the document has no
/// value. The exact-integer sibling of [`NumericStore`] — same shape,
/// same fixed stride, no rounding above 2^53. min/max over present
/// values are computed at write time into the v7 column table.
#[derive(Debug)]
struct IntStore {
    /// Integer field name from the schema.
    name: String,
    /// Per-slot value ([`INTEGER_ABSENT`] = no value). May be shorter
    /// than the slot count; missing trailing slots read as absent.
    vals: Vec<i64>,
}

impl IntStore {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            vals: Vec::new(),
        }
    }

    fn value(&self, slot: usize) -> Option<i64> {
        match self.vals.get(slot).copied() {
            None | Some(INTEGER_ABSENT) => None,
            some => some,
        }
    }

    /// Record `doc_id`'s value. [`INTEGER_ABSENT`] is refused (callers
    /// validate it into a loud refusal first); one value per (document,
    /// field), a second write panics.
    fn set(&mut self, doc_id: u32, value: i64) {
        assert!(
            value != INTEGER_ABSENT,
            "integer field {:?}: i64::MIN is the absence sentinel and cannot be a value \
             (doc {doc_id})",
            self.name
        );
        let slot = doc_id as usize;
        if self.vals.len() <= slot {
            self.vals.resize(slot + 1, INTEGER_ABSENT);
        }
        assert!(
            self.vals[slot] == INTEGER_ABSENT,
            "doc {doc_id} already has a value for integer field {:?}",
            self.name
        );
        self.vals[slot] = value;
    }

    /// (min, max) over present values. A column with none folds to
    /// `(i64::MAX, i64::MIN)` — min > max is the empty range, which is
    /// self-describing and cannot collide with any real pair (the NaN
    /// role, played by an impossible interval instead of a value).
    fn min_max(&self) -> (i64, i64) {
        let mut min = i64::MAX;
        let mut max = i64::MIN;
        for &v in &self.vals {
            if v == INTEGER_ABSENT {
                continue;
            }
            min = min.min(v);
            max = max.max(v);
        }
        (min, max)
    }
}

/// One geo-point column (`docs/geo-columns.md`): per-slot (lat, lon)
/// pairs parallel to the shared slot space, BOTH NaN = the document has
/// no point. The column's bounding box is computed at write time into
/// the v7 column table, on the pattern kind 1 set: metadata the reader
/// re-derives and compares, never trusts.
///
/// One pair per slot at a fixed 16 B stride rather than two f64
/// columns: a point is one value, and splitting it would let a lat
/// survive a lost lon. The absence sentinel is the pair (NaN, NaN) for
/// the same reason — a half-NaN pair is not a sparser point, it is a
/// corrupt one, and the reader refuses it.
#[derive(Debug)]
struct GeoStore {
    /// Geo field name from the schema.
    name: String,
    /// Per-slot (lat, lon) ((NaN, NaN) = no value). May be shorter than
    /// the slot count; missing trailing slots read as absent.
    vals: Vec<(f64, f64)>,
}

impl GeoStore {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            vals: Vec::new(),
        }
    }

    fn value(&self, slot: usize) -> Option<(f64, f64)> {
        match self.vals.get(slot).copied() {
            None => None,
            Some((lat, lon)) if lat.is_nan() && lon.is_nan() => None,
            some => some,
        }
    }

    /// Record `doc_id`'s point. Coordinates must be finite and on the
    /// globe (callers validate these into refusals first); one point per
    /// (document, field), a second write panics.
    fn set(&mut self, doc_id: u32, lat: f64, lon: f64) {
        assert!(
            lat.is_finite() && (-90.0..=90.0).contains(&lat),
            "geo field {:?}: latitude {lat} is not a finite degree in [-90, 90] (doc {doc_id})",
            self.name
        );
        assert!(
            lon.is_finite() && (-180.0..=180.0).contains(&lon),
            "geo field {:?}: longitude {lon} is not a finite degree in [-180, 180] (doc {doc_id})",
            self.name
        );
        let slot = doc_id as usize;
        if self.vals.len() <= slot {
            self.vals.resize(slot + 1, (f64::NAN, f64::NAN));
        }
        assert!(
            self.vals[slot].0.is_nan() && self.vals[slot].1.is_nan(),
            "doc {doc_id} already has a point for geo field {:?}",
            self.name
        );
        self.vals[slot] = (lat, lon);
    }

    /// The column's bounding box `(min_lat, max_lat, min_lon, max_lon)`
    /// over present points; all four NaN when none are, the same empty
    /// convention kind 1 uses.
    fn bbox(&self) -> (f64, f64, f64, f64) {
        let (mut min_lat, mut max_lat) = (f64::NAN, f64::NAN);
        let (mut min_lon, mut max_lon) = (f64::NAN, f64::NAN);
        for &(lat, lon) in &self.vals {
            if lat.is_nan() && lon.is_nan() {
                continue;
            }
            if min_lat.is_nan() || lat < min_lat {
                min_lat = lat;
            }
            if max_lat.is_nan() || lat > max_lat {
                max_lat = lat;
            }
            if min_lon.is_nan() || lon < min_lon {
                min_lon = lon;
            }
            if max_lon.is_nan() || lon > max_lon {
                max_lon = lon;
            }
        }
        (min_lat, max_lat, min_lon, max_lon)
    }
}

/// One map<string, string> column (`docs/map-columns.md`): interned
/// key and value dictionaries plus per-slot (key_ord, value_ord) pair
/// lists, kept sorted by key ordinal within each document. At most one
/// value per (document, key) — map semantics.
#[derive(Debug)]
struct MapFacetStore {
    /// Map column name from the schema.
    name: String,
    /// Keys in ordinal (first-seen) order.
    keys: Vec<String>,
    /// key → ordinal (heap only).
    key_index: HashMap<String, u32>,
    /// Values in ordinal (first-seen) order, one dictionary for the
    /// whole column (values are shared across keys).
    values: Vec<String>,
    /// value → ordinal (heap only).
    value_index: HashMap<String, u32>,
    /// Per-slot pair lists, sorted by key ordinal. May be shorter than
    /// the slot count; missing trailing slots read as empty.
    pairs: Vec<Vec<(u32, u32)>>,
}

impl MapFacetStore {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            keys: Vec::new(),
            key_index: HashMap::new(),
            values: Vec::new(),
            value_index: HashMap::new(),
            pairs: Vec::new(),
        }
    }

    /// The value ordinal of `doc`'s entry under `key_ord`, `None` when
    /// the document has no entry for the key.
    fn value_ord(&self, slot: usize, key_ord: u32) -> Option<u32> {
        let list = self.pairs.get(slot)?;
        list.binary_search_by_key(&key_ord, |&(k, _)| k)
            .ok()
            .map(|i| list[i].1)
    }

    /// Record `doc_id`'s entry: intern key and value, insert sorted by
    /// key ordinal. A second entry under the same (document, key)
    /// panics (callers validate duplicates into a refusal).
    fn set(&mut self, doc_id: u32, key: &str, value: &str) {
        let key_ord = intern(&mut self.keys, &mut self.key_index, key, &self.name);
        let value_ord = intern(&mut self.values, &mut self.value_index, value, &self.name);
        let slot = doc_id as usize;
        if self.pairs.len() <= slot {
            self.pairs.resize_with(slot + 1, Vec::new);
        }
        let list = &mut self.pairs[slot];
        match list.binary_search_by_key(&key_ord, |&(k, _)| k) {
            Ok(_) => panic!(
                "doc {doc_id} already has an entry under key {key:?} in map column {:?}",
                self.name
            ),
            Err(pos) => list.insert(pos, (key_ord, value_ord)),
        }
    }
}

/// One map<string, f64> column: interned key dictionary plus per-slot
/// (key_ord, value) pair lists, sorted by key ordinal. Per-key min/max
/// are computed at write time into the column table — the bound
/// metadata map-keyed score stages lift with.
#[derive(Debug)]
struct MapNumericStore {
    /// Map column name from the schema.
    name: String,
    /// Keys in ordinal (first-seen) order.
    keys: Vec<String>,
    /// key → ordinal (heap only).
    key_index: HashMap<String, u32>,
    /// Per-slot pair lists, sorted by key ordinal; values are finite
    /// (callers validate).
    pairs: Vec<Vec<(u32, f64)>>,
}

impl MapNumericStore {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            keys: Vec::new(),
            key_index: HashMap::new(),
            pairs: Vec::new(),
        }
    }

    /// `doc`'s value under `key_ord`, `None` when absent.
    fn value(&self, slot: usize, key_ord: u32) -> Option<f64> {
        let list = self.pairs.get(slot)?;
        list.binary_search_by_key(&key_ord, |&(k, _)| k)
            .ok()
            .map(|i| list[i].1)
    }

    /// Record `doc_id`'s entry; finite values only, one per
    /// (document, key) — same contract shape as [`MapFacetStore::set`].
    fn set(&mut self, doc_id: u32, key: &str, value: f64) {
        assert!(
            value.is_finite(),
            "map column {:?}: non-finite value under key {key:?} for doc {doc_id}",
            self.name
        );
        let key_ord = intern(&mut self.keys, &mut self.key_index, key, &self.name);
        let slot = doc_id as usize;
        if self.pairs.len() <= slot {
            self.pairs.resize_with(slot + 1, Vec::new);
        }
        let list = &mut self.pairs[slot];
        match list.binary_search_by_key(&key_ord, |&(k, _)| k) {
            Ok(_) => panic!(
                "doc {doc_id} already has an entry under key {key:?} in map column {:?}",
                self.name
            ),
            Err(pos) => list.insert(pos, (key_ord, value)),
        }
    }

    /// (min, max) per key ordinal over present values; (NaN, NaN) for
    /// keys that ended up with no entries (cannot happen through `set`,
    /// but the encoding tolerates it).
    fn key_min_max(&self) -> Vec<(f64, f64)> {
        let mut mm = vec![(f64::NAN, f64::NAN); self.keys.len()];
        for list in &self.pairs {
            for &(k, v) in list {
                let (min, max) = &mut mm[k as usize];
                if min.is_nan() || v < *min {
                    *min = v;
                }
                if max.is_nan() || v > *max {
                    *max = v;
                }
            }
        }
        mm
    }
}

/// Write a map column's offsets section ((n_slots + 1) x u32 prefix
/// sums of per-doc pair counts) followed by its pairs section, one
/// `write_pair` per entry in slot order. Shared by both map kinds and
/// both writers.
fn write_map_offsets_and_pairs<W: Write, T>(
    w: &mut W,
    n_slots: usize,
    pairs: &[Vec<T>],
    mut write_pair: impl FnMut(&mut W, &T) -> io::Result<()>,
) -> io::Result<()> {
    let mut running = 0u64;
    write_u32(w, 0)?;
    for slot in 0..n_slots {
        running += pairs.get(slot).map_or(0, |l| l.len()) as u64;
        write_u32(
            w,
            u32::try_from(running).expect("map column exceeds u32 total pairs"),
        )?;
    }
    for list in pairs.iter().take(n_slots) {
        for pair in list {
            write_pair(w, pair)?;
        }
    }
    Ok(())
}

/// Intern `value` into a dictionary, returning its ordinal.
fn intern(dict: &mut Vec<String>, index: &mut HashMap<String, u32>, value: &str, col: &str) -> u32 {
    match index.get(value) {
        Some(&ord) => ord,
        None => {
            let ord = u32::try_from(dict.len())
                .unwrap_or_else(|_| panic!("dictionary of column {col:?} exceeds u32"));
            assert!(
                ord != FACET_ABSENT,
                "dictionary of column {col:?} exhausted u32 ordinals"
            );
            dict.push(value.to_string());
            index.insert(value.to_string(), ord);
            ord
        }
    }
}

/// The shard-level mapped-plan binding: the identity of the plan this
/// store's mapped columns were written under (`docs/descriptor-mappings.md`
/// section 4a). Persisted as the kind-6 entry of the kinded column
/// table; an index only ever pairs with the plan it was written under,
/// and a contradiction at bind time is an index compatibility event.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct StoredBinding {
    /// The plan fingerprint (`MappedPlan.fingerprint`, lowercase hex).
    pub plan_fingerprint: String,
    /// The TEXT field bound as the document body.
    pub body_path: String,
    /// SHA-256 over the canonical encoding of the bind's materialize
    /// spec, empty when the bind carried none. Changing materialization
    /// changes what an index means (`docs/cel-values.md`), so it is
    /// part of the bound identity.
    pub materialize_sha: String,
}

/// Header bytes of the binding's column-table entry, 0 when unbound.
fn binding_entry_size(binding: Option<&StoredBinding>) -> u64 {
    binding.map_or(0, |b| {
        2 + BINDING_ENTRY_NAME.len() as u64
            + 1
            + 2
            + b.plan_fingerprint.len() as u64
            + 2
            + b.body_path.len() as u64
            + 2
            + b.materialize_sha.len() as u64
    })
}

/// Emit the binding's column-table entry (a no-op when unbound).
fn write_binding_entry<W: Write>(w: &mut W, binding: Option<&StoredBinding>) -> io::Result<()> {
    let Some(b) = binding else { return Ok(()) };
    write_u16(w, BINDING_ENTRY_NAME.len() as u16)?;
    w.write_all(BINDING_ENTRY_NAME.as_bytes())?;
    w.write_all(&[COLUMN_KIND_BINDING])?;
    for value in [&b.plan_fingerprint, &b.body_path, &b.materialize_sha] {
        write_u16(w, value.len() as u16)?;
        w.write_all(value.as_bytes())?;
    }
    Ok(())
}

/// The shard's lexical half: per-field postings and corpus stats over a
/// shared slot space, plus the raw texts.
#[derive(Debug)]
pub struct Bm25Store {
    /// Per-field indexes; field 0 is the body (the stored text). The
    /// single-field public surface reads field 0.
    fields: Vec<FieldStore>,
    /// Raw texts indexed by local doc id; sparse slots hold `None`.
    texts: Vec<Option<String>>,
    /// Per-document lineage, parallel to `texts` (`None` when the
    /// document was ingested without lineage).
    lineages: Vec<Option<DocLineage>>,
    /// Facet columns in facet-id order.
    facets: Vec<FacetStore>,
    /// Numeric columns in numeric-id order.
    numerics: Vec<NumericStore>,
    /// map<string, string> columns in map-facet-id order.
    map_facets: Vec<MapFacetStore>,
    /// map<string, f64> columns in map-numeric-id order.
    map_numerics: Vec<MapNumericStore>,
    /// i64 columns in integer-id order.
    integers: Vec<IntStore>,
    /// Geo-point columns in geo-id order (`docs/geo-columns.md`). A
    /// store with no columns of any kind persists as v6.
    geos: Vec<GeoStore>,
    /// The mapped-plan binding, persisted as the kind-6 table entry.
    /// `Some` forces v7 even with no columns.
    binding: Option<StoredBinding>,
}

impl Default for Bm25Store {
    fn default() -> Self {
        Self {
            fields: vec![FieldStore::new("body")],
            texts: Vec::new(),
            lineages: Vec::new(),
            facets: Vec::new(),
            numerics: Vec::new(),
            map_facets: Vec::new(),
            map_numerics: Vec::new(),
            integers: Vec::new(),
            geos: Vec::new(),
            binding: None,
        }
    }
}

impl Bm25Store {
    /// The mapped-plan binding this store was written under, if any.
    pub fn binding(&self) -> Option<&StoredBinding> {
        self.binding.as_ref()
    }

    /// Set the binding persisted by the next save. The caller (the
    /// shard's flush path) owns the no-contradiction rule; this is
    /// plain storage.
    pub fn set_binding(&mut self, binding: Option<StoredBinding>) {
        self.binding = binding;
    }

    /// An empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// An empty store with the given field table, in field-id order.
    /// Field 0 is the body (the stored text); `new()` is
    /// `with_fields(&["body"])`. Names must be non-empty and unique
    /// (the v6 reader validates the same on open).
    pub fn with_fields(names: &[&str]) -> Self {
        assert!(!names.is_empty(), "a store needs at least one field");
        for (i, name) in names.iter().enumerate() {
            assert!(!name.is_empty(), "field {i} has an empty name");
            assert!(
                u16::try_from(name.len()).is_ok(),
                "field name exceeds u16 length"
            );
            assert!(!names[..i].contains(name), "duplicate field name {name:?}");
        }
        Self {
            fields: names.iter().map(|n| FieldStore::new(n)).collect(),
            texts: Vec::new(),
            lineages: Vec::new(),
            facets: Vec::new(),
            numerics: Vec::new(),
            map_facets: Vec::new(),
            map_numerics: Vec::new(),
            integers: Vec::new(),
            geos: Vec::new(),
            binding: None,
        }
    }

    /// Declare the facet field table, in facet-id order (builder style:
    /// `Bm25Store::with_fields(&["body"]).with_facets(&["court"])`).
    /// Must be called before any document is added. A store with facet
    /// fields persists as v7; without, as v6.
    pub fn with_facets(mut self, names: &[&str]) -> Self {
        assert!(
            self.texts.is_empty(),
            "facet fields must be declared before documents are added"
        );
        validate_facet_names(names);
        self.facets = names.iter().map(|n| FacetStore::new(n)).collect();
        self
    }

    /// Number of fields in the field table.
    pub fn field_count(&self) -> usize {
        self.fields.len()
    }

    /// Number of facet fields in the facet table.
    pub fn facet_count(&self) -> usize {
        self.facets.len()
    }

    /// The name of facet field `fi`. Panics when out of range.
    pub fn facet_name(&self, fi: usize) -> &str {
        &self.facets[fi].name
    }

    /// The index of the facet field named `name`, if the table has it.
    pub fn facet_index(&self, name: &str) -> Option<usize> {
        self.facets.iter().position(|f| f.name == name)
    }

    /// Number of distinct values facet field `fi` holds.
    pub fn facet_value_count(&self, fi: usize) -> usize {
        self.facets[fi].dict.len()
    }

    /// The value of facet field `fi` at ordinal `ord`. Panics when out
    /// of range.
    pub fn facet_value(&self, fi: usize, ord: u32) -> &str {
        &self.facets[fi].dict[ord as usize]
    }

    /// The ordinal of `doc_id`'s value for facet field `fi`, `None`
    /// when the document has no value.
    pub fn facet_ord(&self, fi: usize, doc_id: u32) -> Option<u32> {
        self.facets[fi].ord(doc_id as usize)
    }

    /// The ordinal of `value` in facet field `fi`'s dictionary, `None`
    /// when this store never ingested it — the value then matches none
    /// of its documents, which is the exact answer, not a refusal
    /// (`docs/cel-filters.md`: the typo rule guards structure, not
    /// data).
    pub fn facet_value_ord_of(&self, fi: usize, value: &str) -> Option<u32> {
        self.facets[fi].index.get(value).copied()
    }

    /// Record `doc_id`'s value for facet field `fi`, interning it in
    /// the dictionary; see [`FacetStore::set`] for the contract.
    pub fn set_facet(&mut self, fi: usize, doc_id: u32, value: &str) {
        self.facets[fi].set(doc_id, value);
    }

    /// Declare the numeric field table, in numeric-id order (builder
    /// style, like [`Self::with_facets`]). Must be called before any
    /// document is added; a store with columns persists as v7.
    pub fn with_numerics(mut self, names: &[&str]) -> Self {
        assert!(
            self.texts.is_empty(),
            "numeric fields must be declared before documents are added"
        );
        validate_facet_names(names);
        self.numerics = names.iter().map(|n| NumericStore::new(n)).collect();
        self
    }

    /// Number of numeric fields in the numeric table.
    pub fn numeric_count(&self) -> usize {
        self.numerics.len()
    }

    /// The name of numeric field `ni`. Panics when out of range.
    pub fn numeric_name(&self, ni: usize) -> &str {
        &self.numerics[ni].name
    }

    /// The index of the numeric field named `name`, if the table has it.
    pub fn numeric_index(&self, name: &str) -> Option<usize> {
        self.numerics.iter().position(|n| n.name == name)
    }

    /// `doc_id`'s value for numeric field `ni`, `None` when absent.
    pub fn numeric_value(&self, ni: usize, doc_id: u32) -> Option<f64> {
        self.numerics[ni].value(doc_id as usize)
    }

    /// (min, max) of numeric field `ni` over present values; (NaN, NaN)
    /// when no document has one.
    pub fn numeric_min_max(&self, ni: usize) -> (f64, f64) {
        self.numerics[ni].min_max()
    }

    /// Record `doc_id`'s value for numeric field `ni`; see
    /// [`NumericStore::set`] for the contract (finite values only).
    pub fn set_numeric(&mut self, ni: usize, doc_id: u32, value: f64) {
        self.numerics[ni].set(doc_id, value);
    }

    /// Declare the i64 field table, in integer-id order (builder
    /// style, like [`Self::with_numerics`]). Must be called before any
    /// document is added; a store with columns persists as v7.
    pub fn with_integers(mut self, names: &[&str]) -> Self {
        assert!(
            self.texts.is_empty(),
            "integer fields must be declared before documents are added"
        );
        validate_facet_names(names);
        self.integers = names.iter().map(|n| IntStore::new(n)).collect();
        self
    }

    /// Number of i64 fields in the integer table.
    pub fn integer_count(&self) -> usize {
        self.integers.len()
    }

    /// The name of integer field `ii`. Panics when out of range.
    pub fn integer_name(&self, ii: usize) -> &str {
        &self.integers[ii].name
    }

    /// The index of the integer field named `name`, if the table has it.
    pub fn integer_index(&self, name: &str) -> Option<usize> {
        self.integers.iter().position(|n| n.name == name)
    }

    /// `doc_id`'s value for integer field `ii`, `None` when absent.
    pub fn integer_value(&self, ii: usize, doc_id: u32) -> Option<i64> {
        self.integers[ii].value(doc_id as usize)
    }

    /// (min, max) of integer field `ii` over present values; the empty
    /// range `(i64::MAX, i64::MIN)` when no document has one (see
    /// [`IntStore::min_max`]).
    pub fn integer_min_max(&self, ii: usize) -> (i64, i64) {
        self.integers[ii].min_max()
    }

    /// Record `doc_id`'s value for integer field `ii`; see
    /// [`IntStore::set`] for the contract (never [`INTEGER_ABSENT`]).
    pub fn set_integer(&mut self, ii: usize, doc_id: u32, value: i64) {
        self.integers[ii].set(doc_id, value);
    }

    /// Declare the geo-point column table, in geo-id order (builder
    /// style, like [`Self::with_numerics`]). Must be called before any
    /// document is added; a store with columns persists as v7.
    pub fn with_geos(mut self, names: &[&str]) -> Self {
        assert!(
            self.texts.is_empty(),
            "geo fields must be declared before documents are added"
        );
        validate_facet_names(names);
        self.geos = names.iter().map(|n| GeoStore::new(n)).collect();
        self
    }

    /// Number of geo fields in the geo table.
    pub fn geo_count(&self) -> usize {
        self.geos.len()
    }

    /// The name of geo field `gi`. Panics when out of range.
    pub fn geo_name(&self, gi: usize) -> &str {
        &self.geos[gi].name
    }

    /// The index of the geo field named `name`, if the table has it.
    pub fn geo_index(&self, name: &str) -> Option<usize> {
        self.geos.iter().position(|n| n.name == name)
    }

    /// `doc_id`'s (lat, lon) for geo field `gi`, `None` when absent.
    pub fn geo_value(&self, gi: usize, doc_id: u32) -> Option<(f64, f64)> {
        self.geos[gi].value(doc_id as usize)
    }

    /// Geo field `gi`'s bounding box `(min_lat, max_lat, min_lon,
    /// max_lon)` over present points; all four NaN when there are none.
    pub fn geo_bbox(&self, gi: usize) -> (f64, f64, f64, f64) {
        self.geos[gi].bbox()
    }

    /// Record `doc_id`'s point for geo field `gi`; see [`GeoStore::set`]
    /// for the contract (finite degrees on the globe, one per document).
    pub fn set_geo(&mut self, gi: usize, doc_id: u32, lat: f64, lon: f64) {
        self.geos[gi].set(doc_id, lat, lon);
    }

    /// Declare the map<string, string> column table (builder style,
    /// like [`Self::with_facets`]); columns make the store persist v7.
    pub fn with_map_facets(mut self, names: &[&str]) -> Self {
        assert!(
            self.texts.is_empty(),
            "map columns must be declared before documents are added"
        );
        validate_facet_names(names);
        self.map_facets = names.iter().map(|n| MapFacetStore::new(n)).collect();
        self
    }

    /// Declare the map<string, f64> column table (builder style).
    pub fn with_map_numerics(mut self, names: &[&str]) -> Self {
        assert!(
            self.texts.is_empty(),
            "map columns must be declared before documents are added"
        );
        validate_facet_names(names);
        self.map_numerics = names.iter().map(|n| MapNumericStore::new(n)).collect();
        self
    }

    /// Number of map<string, string> columns.
    pub fn map_facet_count(&self) -> usize {
        self.map_facets.len()
    }

    /// The name of map-facet column `ci`. Panics when out of range.
    pub fn map_facet_name(&self, ci: usize) -> &str {
        &self.map_facets[ci].name
    }

    /// The index of the map-facet column named `name`.
    pub fn map_facet_index(&self, name: &str) -> Option<usize> {
        self.map_facets.iter().position(|c| c.name == name)
    }

    /// The key ordinal of `key` in map-facet column `ci`, if any
    /// document ever carried it.
    pub fn map_facet_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        self.map_facets[ci].key_index.get(key).copied()
    }

    /// Number of distinct values map-facet column `ci` holds.
    pub fn map_facet_value_count(&self, ci: usize) -> usize {
        self.map_facets[ci].values.len()
    }

    /// The value of map-facet column `ci` at ordinal `ord`.
    pub fn map_facet_value(&self, ci: usize, ord: u32) -> &str {
        &self.map_facets[ci].values[ord as usize]
    }

    /// The ordinal of `value` in map-facet column `ci`'s value
    /// dictionary, `None` when never ingested — the exact
    /// matches-nothing answer, like [`Self::facet_value_ord_of`].
    pub fn map_facet_value_ord_of(&self, ci: usize, value: &str) -> Option<u32> {
        self.map_facets[ci].value_index.get(value).copied()
    }

    /// The value ordinal of `doc_id`'s entry under `key_ord` in
    /// map-facet column `ci`, `None` when absent.
    pub fn map_facet_value_ord(&self, ci: usize, key_ord: u32, doc_id: u32) -> Option<u32> {
        self.map_facets[ci].value_ord(doc_id as usize, key_ord)
    }

    /// Record `doc_id`'s map-facet entry; see [`MapFacetStore::set`].
    pub fn set_map_facet(&mut self, ci: usize, doc_id: u32, key: &str, value: &str) {
        self.map_facets[ci].set(doc_id, key, value);
    }

    /// Number of map<string, f64> columns.
    pub fn map_numeric_count(&self) -> usize {
        self.map_numerics.len()
    }

    /// The name of map-numeric column `ci`. Panics when out of range.
    pub fn map_numeric_name(&self, ci: usize) -> &str {
        &self.map_numerics[ci].name
    }

    /// The index of the map-numeric column named `name`.
    pub fn map_numeric_index(&self, name: &str) -> Option<usize> {
        self.map_numerics.iter().position(|c| c.name == name)
    }

    /// The key ordinal of `key` in map-numeric column `ci`.
    pub fn map_numeric_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        self.map_numerics[ci].key_index.get(key).copied()
    }

    /// (min, max) of map-numeric column `ci` under `key_ord`.
    pub fn map_numeric_key_min_max(&self, ci: usize, key_ord: u32) -> (f64, f64) {
        self.map_numerics[ci].key_min_max()[key_ord as usize]
    }

    /// `doc_id`'s value under `key_ord` in map-numeric column `ci`.
    pub fn map_numeric_value(&self, ci: usize, key_ord: u32, doc_id: u32) -> Option<f64> {
        self.map_numerics[ci].value(doc_id as usize, key_ord)
    }

    /// Record `doc_id`'s map-numeric entry; see [`MapNumericStore::set`].
    pub fn set_map_numeric(&mut self, ci: usize, doc_id: u32, key: &str, value: f64) {
        self.map_numerics[ci].set(doc_id, key, value);
    }

    /// The name of field `f`. Panics when out of range.
    pub fn field_name(&self, f: usize) -> &str {
        &self.fields[f].name
    }

    /// Record field `f`'s analyzer fingerprint, or refuse if it
    /// contradicts what the field already holds.
    ///
    /// A fingerprint is written once, by the first document that carries
    /// one, and is immutable after. A LATER document analyzed differently
    /// into the same column is exactly the drift this exists to catch:
    /// the two halves of the column would hold different term identities
    /// and every score over it would silently mix them. 0 means the
    /// caller does not know its own spec, which neither sets nor checks.
    pub fn set_analysis_fingerprint(&mut self, f: usize, fingerprint: u64) -> Result<(), String> {
        if fingerprint == 0 {
            return Ok(());
        }
        let field = &mut self.fields[f];
        match field.analysis_fingerprint {
            0 => {
                field.analysis_fingerprint = fingerprint;
                Ok(())
            }
            held if held == fingerprint => Ok(()),
            held => Err(format!(
                "field {:?} was built with analyzer fingerprint {held:#x} but this \
                 document carries {fingerprint:#x}; one column holds one term identity",
                field.name
            )),
        }
    }

    /// Field `f`'s analyzer fingerprint (0 = unknown).
    pub fn analysis_fingerprint(&self, f: usize) -> u64 {
        self.fields[f].analysis_fingerprint
    }

    /// The index of the field named `name`, if the table has it.
    pub fn field_index(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|f| f.name == name)
    }

    /// Field `f` as its own [`Bm25Index`]: per-field postings, lengths,
    /// and total; shared texts, lineages, and doc count. Every scorer
    /// runs against one of these views (`docs/multi-field.md`). Panics
    /// when `f` is out of range.
    pub fn field(&self, f: usize) -> StoreFieldView<'_> {
        StoreFieldView {
            store: self,
            field: &self.fields[f],
        }
    }

    /// The number of document slots ever allocated (the next local doc id).
    pub fn next_doc_id(&self) -> u32 {
        self.texts.len() as u32
    }

    /// Number of documents with postings (in any field).
    pub fn doc_count(&self) -> u64 {
        (0..self.texts.len())
            .filter(|&slot| self.fields.iter().any(|f| f.doc_lengths[slot] > 0))
            .count() as u64
    }

    /// Sum of all body document lengths (BM25 avgdl numerator).
    pub fn total_doc_length(&self) -> u64 {
        self.fields[0].total_length
    }

    /// Body postings for `term`, if present.
    pub fn postings(&self, term: &str) -> Option<&[Posting]> {
        self.fields[0].postings.get(term).map(Vec::as_slice)
    }

    /// Body document length in terms (0 for unknown/sparse slots).
    pub fn doc_length(&self, doc_id: u32) -> u32 {
        self.fields[0]
            .doc_lengths
            .get(doc_id as usize)
            .copied()
            .unwrap_or(0)
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
        assert!(slot >= self.texts.len(), "doc id {doc_id} already used");
        assert!(
            doc.fields.len() <= self.fields.len(),
            "document carries {} fields, store has {}",
            doc.fields.len(),
            self.fields.len()
        );
        for field in &mut self.fields {
            field.doc_lengths.resize(slot + 1, 0);
        }
        self.texts.resize_with(slot + 1, || None);
        self.lineages.resize_with(slot + 1, || None);
        self.texts[slot] = Some(text);
        self.lineages[slot] = lineage;
        for (fi, analyzed) in doc.fields.into_iter().enumerate() {
            let field = &mut self.fields[fi];
            field.doc_lengths[slot] = analyzed.length;
            field.total_length += u64::from(analyzed.length);
            for (term, tf, offsets) in analyzed.terms {
                field.postings.entry(term).or_default().push(Posting {
                    doc_id,
                    tf,
                    offsets,
                });
            }
        }
    }

    /// Persist to `path` (atomically: write tmp, fsync, rename, fsync
    /// parent). Writes v8: the payload is [`Self::write_v6_to`]'s v6
    /// bytes when no columns are declared and v7 bytes when they are,
    /// then [`finalize_v8`] stamps the integrity table over it, so
    /// every saved file can prove its bytes are the ones this write
    /// produced.
    pub fn save(&self, path: &Path) -> io::Result<()> {
        let tmp: PathBuf = path.with_extension("bm25tmp");
        {
            let mut w = io::BufWriter::new(std::fs::File::create(&tmp)?);
            self.write_v6_to(&mut w)?;
            w.flush()?;
        }
        finalize_v8(&tmp)?;
        std::fs::rename(&tmp, path)?;
        fsync_parent(path)
    }

    /// Persist in the v5 format. Correctness oracle only — the
    /// v5-vs-v6 section-parity and query-identity tests; production
    /// saves are v6 and multi-field stores are refused here.
    pub fn save_v5(&self, path: &Path) -> io::Result<()> {
        let tmp: PathBuf = path.with_extension("bm25tmp");
        {
            let mut w = io::BufWriter::new(std::fs::File::create(&tmp)?);
            self.write_to(&mut w)?;
            w.flush()?;
            w.get_ref().sync_all()?;
        }
        std::fs::rename(&tmp, path)?;
        fsync_parent(path)
    }

    /// Load from `path`.
    pub fn load(path: &Path) -> io::Result<Self> {
        let bytes = std::fs::read(path)?;
        Self::read_from(&mut &bytes[..])
    }

    /// Sizes of the shared sections (texts, text_index, lineages),
    /// identical bytes in every format version modulo text_index
    /// basing.
    fn shared_section_sizes(&self) -> (u64, u64, u64) {
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
        (texts_size, 12 * self.texts.len() as u64, lineages_size)
    }

    /// One field's term list, sorted (the on-disk directory order).
    fn sorted_terms(field: &FieldStore) -> Vec<&String> {
        let mut terms: Vec<&String> = field.postings.keys().collect();
        terms.sort();
        terms
    }

    /// Per-term `(occurrence, skip)` run byte sizes of one field — the
    /// size pass. The skip run's size needs the frontier computation,
    /// so run the same builder over a sink; the write pass re-runs it
    /// for real (deterministic, so the sizes agree).
    fn field_run_sizes(field: &FieldStore, terms: &[&String]) -> io::Result<Vec<(u64, u64)>> {
        let mut run_sizes: Vec<(u64, u64)> = Vec::with_capacity(terms.len());
        for term in terms {
            let postings = &field.postings[*term];
            let occ_bytes: u64 = postings.iter().map(|p| 8 * p.offsets.len() as u64).sum();
            let mut skip = SkipRunBuilder::new();
            let mut sink = io::sink();
            for p in postings {
                let dl = field.doc_lengths[p.doc_id as usize];
                skip.push(p.tf, dl, p.doc_id, &mut sink)?;
            }
            let (l0_bytes, l1) = skip.finish(&mut sink)?;
            run_sizes.push((occ_bytes, skip_run_size(l0_bytes, &l1)));
        }
        Ok(run_sizes)
    }

    /// Byte size of one field's postings section (v5 shape).
    fn field_postings_size(field: &FieldStore, terms: &[&String], run_sizes: &[(u64, u64)]) -> u64 {
        4 + terms
            .iter()
            .enumerate()
            .map(|(i, t)| {
                let df = field.postings[*t].len() as u64;
                12 * df + 4 + run_sizes[i].0 + run_sizes[i].1
            })
            .sum::<u64>()
    }

    /// Byte size of one field's directory section (v5 shape).
    fn field_directory_size(terms: &[&String]) -> u64 {
        4 + 34 * terms.len() as u64 + terms.iter().map(|t| t.len() as u64).sum::<u64>()
    }

    /// Write the shared sections (texts, text_index, lineages).
    /// `index_base` bases the text_index entries: the absolute texts
    /// section offset in v3/v4/v5 (absolute entries), 0 in v6
    /// (section-relative entries).
    fn write_shared_sections<W: Write>(&self, w: &mut W, index_base: u64) -> io::Result<()> {
        // texts (+ build the on-disk index)
        let mut text_index: Vec<(u64, u32)> = Vec::with_capacity(self.texts.len());
        let mut cursor = index_base;
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
                    write_u64(w, l.parent_id)?;
                    write_u64(w, l.group_id)?;
                    write_u32(w, l.span_start)?;
                    write_u32(w, l.span_end)?;
                }
                None => w.write_all(&[0u8])?,
            }
        }
        Ok(())
    }

    /// Write one field's doc_lengths section.
    fn write_field_doc_lengths<W: Write>(w: &mut W, field: &FieldStore) -> io::Result<()> {
        for &len in &field.doc_lengths {
            write_u32(w, len)?;
        }
        Ok(())
    }

    /// Write one field's postings section (u32 n_terms, then per-term
    /// doc/occurrence/skip runs), returning the directory tuples
    /// `(doc_run_off, skip_run_off, occ_run_off, df)`. Offsets are
    /// based at `run_base`: the absolute postings section offset in v5,
    /// 0 in v6 (section-relative entries).
    ///
    /// The doc run streams straight out; the occurrence run and the
    /// level-0 skip records stage in per-term buffers (the heap store
    /// already holds every posting, so the stage is never the memory
    /// ceiling) and are appended after the sentinel.
    fn write_field_postings<W: Write>(
        w: &mut W,
        field: &FieldStore,
        terms: &[&String],
        run_sizes: &[(u64, u64)],
        run_base: u64,
    ) -> io::Result<Vec<(u64, u64, u64, u32)>> {
        write_u32(w, terms.len() as u32)?;
        let mut directory: Vec<(u64, u64, u64, u32)> = Vec::with_capacity(terms.len());
        let mut cursor = run_base + 4;
        for (i, term) in terms.iter().enumerate() {
            let postings = &field.postings[*term];
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
                let dl = field.doc_lengths[p.doc_id as usize];
                occ_start = push_posting_v5(
                    w,
                    &mut occ_stage,
                    &mut skip,
                    &mut skip_l0,
                    p.doc_id,
                    p.tf,
                    &p.offsets,
                    dl,
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
        Ok(directory)
    }

    /// Write one field's directory section: fixed-stride 34 B entries
    /// (binary-searchable), then the term blob.
    fn write_field_directory<W: Write>(
        w: &mut W,
        terms: &[&String],
        directory: &[(u64, u64, u64, u32)],
    ) -> io::Result<()> {
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
        for term in terms {
            w.write_all(term.as_bytes())?;
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
        if self.fields.len() > 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "v5 carries exactly one field; multi-field stores write v6",
            ));
        }
        if !self.facets.is_empty() || !self.numerics.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "v5 carries no columns; column-bearing stores write v7",
            ));
        }
        let field = &self.fields[0];
        let n_slots = self.texts.len() as u64;
        let header_size = 8 + 8 + 8 * 4 + 4;
        let (texts_size, text_index_size, lineages_size) = self.shared_section_sizes();
        let terms = Self::sorted_terms(field);
        let run_sizes = Self::field_run_sizes(field, &terms)?;
        let postings_size = Self::field_postings_size(field, &terms, &run_sizes);

        let doc_lengths_off = header_size as u64;
        let texts_off = doc_lengths_off + 4 * n_slots;
        let text_index_off = texts_off + texts_size;
        let lineages_off = text_index_off + text_index_size;
        let postings_off = lineages_off + lineages_size;
        let directory_off = postings_off + postings_size;

        w.write_all(MAGIC_V5)?;
        write_u64(w, field.total_length)?;
        write_u64(w, texts_off)?;
        write_u64(w, lineages_off)?;
        write_u64(w, postings_off)?;
        write_u64(w, directory_off)?;
        write_u32(w, n_slots as u32)?;
        Self::write_field_doc_lengths(w, field)?;
        self.write_shared_sections(w, texts_off)?;
        let directory = Self::write_field_postings(w, field, &terms, &run_sizes, postings_off)?;
        Self::write_field_directory(w, &terms, &directory)?;
        Ok(())
    }

    /// The v6 layout (`TVBM2506`, see `docs/multi-field.md`).
    /// Variable-length header holding an explicit section table, shared
    /// sections, then one doc_lengths/postings/directory group per
    /// field in field-id order. Per-field postings and directory
    /// sections keep the v5 shape, but the directory's run offsets are
    /// RELATIVE to the field's postings section start, and text_index
    /// entries are relative to the texts section start (the v4 blob
    /// lesson generalized: sections survive relocation). Blob offsets
    /// stay blob-relative. Every section is located by an explicit
    /// header offset, nothing derived by arithmetic.
    ///
    /// ```text
    /// magic "TVBM2506"
    /// u32 n_fields | u32 n_slots
    /// u64 texts_off | u64 text_index_off | u64 lineages_off
    /// field table, n_fields entries:
    ///   u16 name_len | name bytes
    ///   u64 analysis_fingerprint | u64 total_length
    ///   u64 doc_lengths_off | u64 postings_off | u64 directory_off
    /// texts | text_index | lineages          <- v5 bytes (index rebased)
    /// per field: doc_lengths | postings | directory   <- v5-shape sections
    /// ```
    ///
    /// A store with declared facet or numeric fields writes v7
    /// (`TVBM2507`) instead: the same layout with a kinded column
    /// table appended to the header (u32 n_columns, then per column:
    /// u16 name_len | name | u8 kind | kind payload — kind 0 facet:
    /// u32 n_values | u64 dict_off | u64 ords_off; kind 1 f64: u64
    /// min_bits | u64 max_bits | u64 vals_off) and, after the last
    /// field group in table order, per facet a dict section (n_values
    /// x (u16 len | value bytes), ordinal order) and an ords section
    /// (n_slots x u32, [`FACET_ABSENT`] = no value), then per numeric
    /// a vals section (n_slots x f64 bits, NaN = no value). Every
    /// column-less byte is identical to the v6 writer's, by
    /// construction (the additions are gated, not forked).
    pub fn write_v6_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        let n_slots = self.texts.len() as u64;
        let (texts_size, text_index_size, lineages_size) = self.shared_section_sizes();
        let has_columns = !self.facets.is_empty()
            || !self.numerics.is_empty()
            || !self.map_facets.is_empty()
            || !self.map_numerics.is_empty()
            || !self.integers.is_empty()
            || !self.geos.is_empty()
            || self.binding.is_some();
        let column_table_size: u64 = if !has_columns {
            0
        } else {
            4 + self
                .facets
                .iter()
                .map(|f| 2 + f.name.len() as u64 + 1 + 4 + 8 + 8)
                .sum::<u64>()
                + self
                    .numerics
                    .iter()
                    .map(|n| 2 + n.name.len() as u64 + 1 + 8 + 8 + 8)
                    .sum::<u64>()
                + self
                    .map_facets
                    .iter()
                    .map(|c| 2 + c.name.len() as u64 + 1 + 4 + 4 + 8 * 4)
                    .sum::<u64>()
                + self
                    .map_numerics
                    .iter()
                    .map(|c| 2 + c.name.len() as u64 + 1 + 4 + 8 * 3)
                    .sum::<u64>()
                + self
                    .integers
                    .iter()
                    .map(|c| 2 + c.name.len() as u64 + 1 + 8 * 3)
                    .sum::<u64>()
                + self
                    .geos
                    .iter()
                    .map(|c| 2 + c.name.len() as u64 + 1 + 8 * 5)
                    .sum::<u64>()
                + binding_entry_size(self.binding.as_ref())
        };
        let header_size: u64 = 8
            + 4
            + 4
            + 8 * 3
            + self
                .fields
                .iter()
                .map(|f| 2 + f.name.len() as u64 + 8 * 5)
                .sum::<u64>()
            + column_table_size;
        // Size pass per field.
        let mut field_terms: Vec<Vec<&String>> = Vec::with_capacity(self.fields.len());
        let mut field_runs: Vec<Vec<(u64, u64)>> = Vec::with_capacity(self.fields.len());
        for field in &self.fields {
            let terms = Self::sorted_terms(field);
            let run_sizes = Self::field_run_sizes(field, &terms)?;
            field_terms.push(terms);
            field_runs.push(run_sizes);
        }
        let texts_off = header_size;
        let text_index_off = texts_off + texts_size;
        let lineages_off = text_index_off + text_index_size;
        let mut cursor = lineages_off + lineages_size;
        // (doc_lengths_off, postings_off, directory_off) per field.
        let mut section_offs: Vec<(u64, u64, u64)> = Vec::with_capacity(self.fields.len());
        for (fi, field) in self.fields.iter().enumerate() {
            let doc_lengths_off = cursor;
            let postings_off = doc_lengths_off + 4 * n_slots;
            let directory_off =
                postings_off + Self::field_postings_size(field, &field_terms[fi], &field_runs[fi]);
            section_offs.push((doc_lengths_off, postings_off, directory_off));
            cursor = directory_off + Self::field_directory_size(&field_terms[fi]);
        }
        // (dict_off, ords_off) per facet, then vals_off per numeric,
        // after the last field group — column-table order.
        let mut facet_offs: Vec<(u64, u64)> = Vec::with_capacity(self.facets.len());
        for facet in &self.facets {
            let dict_off = cursor;
            let dict_size: u64 = facet.dict.iter().map(|v| 2 + v.len() as u64).sum();
            let ords_off = dict_off + dict_size;
            facet_offs.push((dict_off, ords_off));
            cursor = ords_off + 4 * n_slots;
        }
        let mut numeric_offs: Vec<u64> = Vec::with_capacity(self.numerics.len());
        for _ in &self.numerics {
            numeric_offs.push(cursor);
            cursor += 8 * n_slots;
        }
        // (keys_off, values_off, offsets_off, pairs_off) per map-facet
        // column; the offsets section holds n_slots + 1 prefix sums so
        // a document's pair count is off[i+1] - off[i].
        let mut map_facet_offs: Vec<(u64, u64, u64, u64)> =
            Vec::with_capacity(self.map_facets.len());
        for c in &self.map_facets {
            let keys_off = cursor;
            let keys_size: u64 = c.keys.iter().map(|k| 2 + k.len() as u64).sum();
            let values_off = keys_off + keys_size;
            let values_size: u64 = c.values.iter().map(|v| 2 + v.len() as u64).sum();
            let offsets_off = values_off + values_size;
            let pairs_off = offsets_off + 4 * (n_slots + 1);
            let total_pairs: u64 = c.pairs.iter().map(|l| l.len() as u64).sum();
            map_facet_offs.push((keys_off, values_off, offsets_off, pairs_off));
            cursor = pairs_off + 8 * total_pairs;
        }
        // (keys_off, offsets_off, pairs_off) per map-numeric column;
        // the key dict entries carry per-key min/max bound metadata.
        let mut map_numeric_offs: Vec<(u64, u64, u64)> =
            Vec::with_capacity(self.map_numerics.len());
        for c in &self.map_numerics {
            let keys_off = cursor;
            let keys_size: u64 = c.keys.iter().map(|k| 2 + k.len() as u64 + 16).sum();
            let offsets_off = keys_off + keys_size;
            let pairs_off = offsets_off + 4 * (n_slots + 1);
            let total_pairs: u64 = c.pairs.iter().map(|l| l.len() as u64).sum();
            map_numeric_offs.push((keys_off, offsets_off, pairs_off));
            cursor = pairs_off + 12 * total_pairs;
        }
        // vals_off per i64 column, last in table order (a new kind
        // appends; the earlier kinds' geometry must not shift).
        let mut integer_offs: Vec<u64> = Vec::with_capacity(self.integers.len());
        for _ in &self.integers {
            integer_offs.push(cursor);
            cursor += 8 * n_slots;
        }
        // vals_off per geo column, last in table order. Kind 5 appends
        // for the same reason kind 4 did: kinds 0 through 4 must keep
        // byte-for-byte the geometry they already have.
        let mut geo_offs: Vec<u64> = Vec::with_capacity(self.geos.len());
        for _ in &self.geos {
            geo_offs.push(cursor);
            cursor += 16 * n_slots;
        }

        w.write_all(if has_columns { MAGIC_V7 } else { MAGIC_V6 })?;
        write_u32(w, self.fields.len() as u32)?;
        write_u32(w, n_slots as u32)?;
        write_u64(w, texts_off)?;
        write_u64(w, text_index_off)?;
        write_u64(w, lineages_off)?;
        for (field, &(dl_off, p_off, d_off)) in self.fields.iter().zip(&section_offs) {
            write_u16(w, field.name.len() as u16)?;
            w.write_all(field.name.as_bytes())?;
            write_u64(w, field.analysis_fingerprint)?;
            write_u64(w, field.total_length)?;
            write_u64(w, dl_off)?;
            write_u64(w, p_off)?;
            write_u64(w, d_off)?;
        }
        if has_columns {
            write_u32(
                w,
                (self.facets.len()
                    + self.numerics.len()
                    + self.map_facets.len()
                    + self.map_numerics.len()
                    + self.integers.len()
                    + self.geos.len()
                    + usize::from(self.binding.is_some())) as u32,
            )?;
            for (facet, &(dict_off, ords_off)) in self.facets.iter().zip(&facet_offs) {
                write_u16(w, facet.name.len() as u16)?;
                w.write_all(facet.name.as_bytes())?;
                w.write_all(&[COLUMN_KIND_FACET])?;
                write_u32(w, facet.dict.len() as u32)?;
                write_u64(w, dict_off)?;
                write_u64(w, ords_off)?;
            }
            for (numeric, &vals_off) in self.numerics.iter().zip(&numeric_offs) {
                let (min, max) = numeric.min_max();
                write_u16(w, numeric.name.len() as u16)?;
                w.write_all(numeric.name.as_bytes())?;
                w.write_all(&[COLUMN_KIND_F64])?;
                write_u64(w, min.to_bits())?;
                write_u64(w, max.to_bits())?;
                write_u64(w, vals_off)?;
            }
            for (c, &(keys_off, values_off, offsets_off, pairs_off)) in
                self.map_facets.iter().zip(&map_facet_offs)
            {
                write_u16(w, c.name.len() as u16)?;
                w.write_all(c.name.as_bytes())?;
                w.write_all(&[COLUMN_KIND_MAP_FACET])?;
                write_u32(w, c.keys.len() as u32)?;
                write_u32(w, c.values.len() as u32)?;
                write_u64(w, keys_off)?;
                write_u64(w, values_off)?;
                write_u64(w, offsets_off)?;
                write_u64(w, pairs_off)?;
            }
            for (c, &(keys_off, offsets_off, pairs_off)) in
                self.map_numerics.iter().zip(&map_numeric_offs)
            {
                write_u16(w, c.name.len() as u16)?;
                w.write_all(c.name.as_bytes())?;
                w.write_all(&[COLUMN_KIND_MAP_F64])?;
                write_u32(w, c.keys.len() as u32)?;
                write_u64(w, keys_off)?;
                write_u64(w, offsets_off)?;
                write_u64(w, pairs_off)?;
            }
            for (c, &vals_off) in self.integers.iter().zip(&integer_offs) {
                let (min, max) = c.min_max();
                write_u16(w, c.name.len() as u16)?;
                w.write_all(c.name.as_bytes())?;
                w.write_all(&[COLUMN_KIND_I64])?;
                write_u64(w, min as u64)?;
                write_u64(w, max as u64)?;
                write_u64(w, vals_off)?;
            }
            for (c, &vals_off) in self.geos.iter().zip(&geo_offs) {
                let (min_lat, max_lat, min_lon, max_lon) = c.bbox();
                write_u16(w, c.name.len() as u16)?;
                w.write_all(c.name.as_bytes())?;
                w.write_all(&[COLUMN_KIND_GEO])?;
                write_u64(w, min_lat.to_bits())?;
                write_u64(w, max_lat.to_bits())?;
                write_u64(w, min_lon.to_bits())?;
                write_u64(w, max_lon.to_bits())?;
                write_u64(w, vals_off)?;
            }
            write_binding_entry(w, self.binding.as_ref())?;
        }
        self.write_shared_sections(w, 0)?;
        for (fi, field) in self.fields.iter().enumerate() {
            Self::write_field_doc_lengths(w, field)?;
            let directory =
                Self::write_field_postings(w, field, &field_terms[fi], &field_runs[fi], 0)?;
            Self::write_field_directory(w, &field_terms[fi], &directory)?;
        }
        for facet in &self.facets {
            for value in &facet.dict {
                write_u16(w, value.len() as u16)?;
                w.write_all(value.as_bytes())?;
            }
            for slot in 0..n_slots as usize {
                write_u32(w, facet.ords.get(slot).copied().unwrap_or(FACET_ABSENT))?;
            }
        }
        for numeric in &self.numerics {
            for slot in 0..n_slots as usize {
                write_u64(
                    w,
                    numeric
                        .vals
                        .get(slot)
                        .copied()
                        .unwrap_or(f64::NAN)
                        .to_bits(),
                )?;
            }
        }
        for c in &self.map_facets {
            for key in &c.keys {
                write_u16(w, key.len() as u16)?;
                w.write_all(key.as_bytes())?;
            }
            for value in &c.values {
                write_u16(w, value.len() as u16)?;
                w.write_all(value.as_bytes())?;
            }
            write_map_offsets_and_pairs(w, n_slots as usize, &c.pairs, |w, &(k, v)| {
                write_u32(w, k)?;
                write_u32(w, v)
            })?;
        }
        for c in &self.map_numerics {
            let mm = c.key_min_max();
            for (key, &(min, max)) in c.keys.iter().zip(&mm) {
                write_u16(w, key.len() as u16)?;
                w.write_all(key.as_bytes())?;
                write_u64(w, min.to_bits())?;
                write_u64(w, max.to_bits())?;
            }
            write_map_offsets_and_pairs(w, n_slots as usize, &c.pairs, |w, &(k, v)| {
                write_u32(w, k)?;
                write_u64(w, v.to_bits())
            })?;
        }
        for c in &self.integers {
            for slot in 0..n_slots as usize {
                write_u64(
                    w,
                    c.vals.get(slot).copied().unwrap_or(INTEGER_ABSENT) as u64,
                )?;
            }
        }
        for c in &self.geos {
            for slot in 0..n_slots as usize {
                let (lat, lon) = c.vals.get(slot).copied().unwrap_or(GEO_ABSENT);
                write_u64(w, lat.to_bits())?;
                write_u64(w, lon.to_bits())?;
            }
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
        if self.fields.len() > 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "v4 carries exactly one field",
            ));
        }
        if !self.facets.is_empty() || !self.numerics.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "v4 carries no columns; column-bearing stores write v7",
            ));
        }
        let field = &self.fields[0];
        let n_slots = self.texts.len() as u64;
        let header_size = 8 + 8 + 8 * 4 + 4;
        let doc_lengths_size = 4 * n_slots;
        let (texts_size, text_index_size, lineages_size) = self.shared_section_sizes();
        let terms = Self::sorted_terms(field);
        let postings_size: u64 = 4 + terms
            .iter()
            .map(|t| {
                4 + t.len() as u64
                    + 4
                    + field.postings[*t]
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
        write_u64(w, field.total_length)?;
        write_u64(w, texts_off)?;
        write_u64(w, lineages_off)?;
        write_u64(w, postings_off)?;
        write_u64(w, directory_off)?;
        write_u32(w, n_slots as u32)?;
        Self::write_field_doc_lengths(w, field)?;
        self.write_shared_sections(w, texts_off)?;

        // postings (+ directory entries)
        write_u32(w, terms.len() as u32)?;
        let mut directory: Vec<(u64, u32)> = Vec::with_capacity(terms.len());
        let mut cursor = postings_off + 4;
        for term in &terms {
            let postings = &field.postings[*term];
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

    /// A single-field ("body") store from already-decoded parts — the
    /// shape every pre-v6 read path produces.
    fn from_single_field(
        postings: HashMap<String, Vec<Posting>>,
        doc_lengths: Vec<u32>,
        total_length: u64,
        texts: Vec<Option<String>>,
        lineages: Vec<Option<DocLineage>>,
    ) -> Self {
        Self {
            fields: vec![FieldStore {
                name: "body".to_string(),
                analysis_fingerprint: 0,
                postings,
                doc_lengths,
                total_length,
            }],
            texts,
            lineages,
            facets: Vec::new(),
            numerics: Vec::new(),
            map_facets: Vec::new(),
            map_numerics: Vec::new(),
            integers: Vec::new(),
            geos: Vec::new(),
            binding: None,
        }
    }

    fn read_from(r: &mut &[u8]) -> io::Result<Self> {
        let invalid = |msg: &str| io::Error::new(io::ErrorKind::InvalidData, msg.to_string());
        let magic = take(r, 8)?;
        if magic == MAGIC_V8 {
            // Rebuild the full-file view (the trailer's offsets are
            // absolute), check the whole envelope and every CRC — the
            // bytes are all in memory anyway — then read the payload
            // as its base version.
            let mut full = Vec::with_capacity(8 + r.len());
            full.extend_from_slice(MAGIC_V8);
            full.extend_from_slice(r);
            let table = parse_integrity(&full)?;
            for e in &table.entries {
                let got = crate::wal::crc32(&full[e.off as usize..(e.off + e.len) as usize]);
                if got != e.crc {
                    return Err(invalid(&format!(
                        "section {} CRC mismatch: stored {:08x}, computed {got:08x}",
                        e.name, e.crc
                    )));
                }
            }
            let mut payload = &full[8..table.payload_len as usize];
            return Self::read_v6v7_from(&mut payload, table.base_v7);
        }
        if magic == MAGIC_V7 {
            return Self::read_v6v7_from(r, true);
        }
        if magic == MAGIC_V6 {
            return Self::read_v6v7_from(r, false);
        }
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
                    let parent_id = read_u64(r)?;
                    let group_id = read_u64(r)?;
                    let span_start = read_u32(r)?;
                    let span_end = read_u32(r)?;
                    *lineage = Some(DocLineage {
                        parent_id,
                        group_id,
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
        Ok(Self::from_single_field(
            postings,
            doc_lengths,
            total_length,
            texts,
            lineages,
        ))
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

/// One field of a [`Bm25Store`] as its own [`Bm25Index`]
/// (`docs/multi-field.md`): postings, document lengths, and total
/// length come from the field; texts, lineages, and the document count
/// are the store's shared ones (a document is a document — idf's N is
/// corpus-wide, never per field).
pub struct StoreFieldView<'a> {
    store: &'a Bm25Store,
    field: &'a FieldStore,
}

impl Bm25Index for StoreFieldView<'_> {
    fn doc_count(&self) -> u64 {
        self.store.doc_count()
    }
    fn total_doc_length(&self) -> u64 {
        self.field.total_length
    }
    fn doc_length(&self, doc_id: u32) -> u32 {
        self.field
            .doc_lengths
            .get(doc_id as usize)
            .copied()
            .unwrap_or(0)
    }
    fn df(&self, term: &str) -> u32 {
        self.field.postings.get(term).map_or(0, |p| p.len() as u32)
    }
    fn for_each_posting(&self, term: &str, f: &mut PostingCallback) {
        if let Some(postings) = self.field.postings.get(term) {
            for p in postings {
                f(p.doc_id, p.tf, &p.offsets);
            }
        }
    }
    fn for_each_doc_tf(&self, term: &str, f: &mut dyn FnMut(u32, u32)) {
        if let Some(postings) = self.field.postings.get(term) {
            for p in postings {
                f(p.doc_id, p.tf);
            }
        }
    }
    fn posting_offsets(&self, term: &str, doc_id: u32) -> Vec<(u32, u32)> {
        let Some(postings) = self.field.postings.get(term) else {
            return Vec::new();
        };
        match postings.binary_search_by_key(&doc_id, |p| p.doc_id) {
            Ok(i) => postings[i].offsets.clone(),
            Err(_) => Vec::new(),
        }
    }
    fn text(&self, doc_id: u32) -> Option<String> {
        self.store.text(doc_id).map(str::to_string)
    }
    fn lineage(&self, doc_id: u32) -> Option<DocLineage> {
        self.store.lineage(doc_id)
    }
}

/// The store itself scores as its body field (field 0) — the surface
/// every single-field caller uses; multi-field scoring goes through
/// [`Bm25Store::field`].
impl Bm25Index for Bm25Store {
    fn doc_count(&self) -> u64 {
        self.field(0).doc_count()
    }
    fn total_doc_length(&self) -> u64 {
        self.field(0).total_doc_length()
    }
    fn doc_length(&self, doc_id: u32) -> u32 {
        self.field(0).doc_length(doc_id)
    }
    fn df(&self, term: &str) -> u32 {
        self.field(0).df(term)
    }
    fn for_each_posting(&self, term: &str, f: &mut PostingCallback) {
        self.field(0).for_each_posting(term, f);
    }
    fn for_each_doc_tf(&self, term: &str, f: &mut dyn FnMut(u32, u32)) {
        self.field(0).for_each_doc_tf(term, f);
    }
    fn posting_offsets(&self, term: &str, doc_id: u32) -> Vec<(u32, u32)> {
        self.field(0).posting_offsets(term, doc_id)
    }
    fn text(&self, doc_id: u32) -> Option<String> {
        self.field(0).text(doc_id)
    }
    fn lineage(&self, doc_id: u32) -> Option<DocLineage> {
        self.field(0).lineage(doc_id)
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
                let parent_id = read_u64(r)?;
                let group_id = read_u64(r)?;
                let span_start = read_u32(r)?;
                let span_end = read_u32(r)?;
                *lineage = Some(DocLineage {
                    parent_id,
                    group_id,
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
        Ok(Self::from_single_field(
            postings,
            doc_lengths,
            total_length,
            texts,
            lineages,
        ))
    }

    /// Decode one v5-shaped postings/directory section pair back into a
    /// heap postings map (shared by the v5 reload path and every field
    /// of the v6 reload path). `all` starts at file offset 8 (the magic
    /// is consumed); `run_base` rebases the directory's run offsets — 0
    /// for v5 (absolute entries), the field's postings section offset
    /// for v6 (section-relative entries). The skip run is not needed in
    /// heap form and is skipped.
    fn read_v5_shaped_postings(
        all: &[u8],
        directory_off: u64,
        run_base: u64,
    ) -> io::Result<HashMap<String, Vec<Posting>>> {
        let invalid = |msg: &str| io::Error::new(io::ErrorKind::InvalidData, msg.to_string());
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
        let n_terms = u32_at(directory_off)? as usize;
        let blob_start = directory_off + 4 + 34 * n_terms as u64;
        let mut postings = HashMap::with_capacity(n_terms);
        for i in 0..n_terms {
            let e = directory_off + 4 + 34 * i as u64;
            let doc_run_off = u64_at(e)? + run_base;
            let occ_run_off = u64_at(e + 16)? + run_base;
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
                    offsets.push((
                        u32_at(occ_run_off + 8 * o)?,
                        u32_at(occ_run_off + 8 * o + 4)?,
                    ));
                }
                plist.push(Posting {
                    doc_id,
                    tf,
                    offsets,
                });
            }
            postings.insert(term, plist);
        }
        Ok(postings)
    }

    /// Parse a v5 file back into a heap store (same caller contract as
    /// [`Self::read_v3_from`]: a disk-resident shard about to receive more
    /// documents). The skip run is not needed in heap form and is skipped.
    fn read_v5_from(r: &mut &[u8]) -> io::Result<Self> {
        let invalid = |msg: &str| io::Error::new(io::ErrorKind::InvalidData, msg.to_string());
        // `r` starts at file offset 8 (magic consumed); section offsets
        // are absolute file offsets.
        let all: &[u8] = r;
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
                let parent_id = read_u64(r)?;
                let group_id = read_u64(r)?;
                let span_start = read_u32(r)?;
                let span_end = read_u32(r)?;
                *lineage = Some(DocLineage {
                    parent_id,
                    group_id,
                    span_start,
                    span_end,
                });
            }
        }
        // postings: locate each term's runs through the directory.
        let postings = Self::read_v5_shaped_postings(all, directory_off, 0)?;
        Ok(Self::from_single_field(
            postings,
            doc_lengths,
            total_length,
            texts,
            lineages,
        ))
    }

    /// Parse a v6 file back into a heap store (same caller contract as
    /// [`Self::read_v3_from`]). All fields are decoded; shared sections
    /// once, then one postings map per field.
    fn read_v6v7_from(r: &mut &[u8], v7: bool) -> io::Result<Self> {
        let invalid = |msg: &str| io::Error::new(io::ErrorKind::InvalidData, msg.to_string());
        // `r` starts at file offset 8 (magic consumed); header offsets
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
        let n_fields = u32_at(8)? as usize;
        if n_fields == 0 {
            return Err(invalid("v6 file with zero fields"));
        }
        let n_slots = u32_at(12)? as usize;
        let texts_off = u64_at(16)?;
        let lineages_off = u64_at(32)?;
        // Field table.
        let mut metas: Vec<(String, u64, u64, u64, u64, u64)> = Vec::with_capacity(n_fields);
        let mut cursor = 40u64;
        for _ in 0..n_fields {
            let name_len = u64::from(u16_at(at(cursor, 2)?));
            let name = String::from_utf8(at(cursor + 2, name_len)?.to_vec())
                .map_err(|_| invalid("invalid utf-8 in field name"))?;
            let base = cursor + 2 + name_len;
            metas.push((
                name,
                u64_at(base)?,      // analysis_fingerprint
                u64_at(base + 8)?,  // total_length
                u64_at(base + 16)?, // doc_lengths_off
                u64_at(base + 24)?, // postings_off
                u64_at(base + 32)?, // directory_off
            ));
            cursor = base + 40;
        }
        // Column table (v7 only): kinded entries. min/max metadata is
        // skipped here — the heap store recomputes it at the next
        // write. Unknown kinds refuse by number.
        let mut facet_metas: Vec<(String, u32, u64, u64)> = Vec::new();
        let mut numeric_metas: Vec<(String, u64)> = Vec::new();
        // (name, n_keys, n_values, keys, values, offsets, pairs)
        let mut map_facet_metas: Vec<(String, u32, u32, u64, u64, u64, u64)> = Vec::new();
        // (name, n_keys, keys, offsets, pairs)
        let mut map_numeric_metas: Vec<(String, u32, u64, u64, u64)> = Vec::new();
        let mut integer_metas: Vec<(String, u64)> = Vec::new();
        let mut geo_metas: Vec<(String, u64)> = Vec::new();
        let mut binding_meta: Option<StoredBinding> = None;
        if v7 {
            let n_columns = u32_at(cursor)? as usize;
            cursor += 4;
            for _ in 0..n_columns {
                let name_len = u64::from(u16_at(at(cursor, 2)?));
                let name = String::from_utf8(at(cursor + 2, name_len)?.to_vec())
                    .map_err(|_| invalid("invalid utf-8 in column name"))?;
                let kind = at(cursor + 2 + name_len, 1)?[0];
                let base = cursor + 2 + name_len + 1;
                match kind {
                    COLUMN_KIND_FACET => {
                        facet_metas.push((
                            name,
                            u32_at(base)?,      // n_values
                            u64_at(base + 4)?,  // dict_off
                            u64_at(base + 12)?, // ords_off
                        ));
                        cursor = base + 20;
                    }
                    COLUMN_KIND_F64 => {
                        numeric_metas.push((name, u64_at(base + 16)?));
                        cursor = base + 24;
                    }
                    COLUMN_KIND_MAP_FACET => {
                        map_facet_metas.push((
                            name,
                            u32_at(base)?,      // n_keys
                            u32_at(base + 4)?,  // n_values
                            u64_at(base + 8)?,  // keys_off
                            u64_at(base + 16)?, // values_off
                            u64_at(base + 24)?, // offsets_off
                            u64_at(base + 32)?, // pairs_off
                        ));
                        cursor = base + 40;
                    }
                    COLUMN_KIND_MAP_F64 => {
                        map_numeric_metas.push((
                            name,
                            u32_at(base)?,      // n_keys
                            u64_at(base + 4)?,  // keys_off
                            u64_at(base + 12)?, // offsets_off
                            u64_at(base + 20)?, // pairs_off
                        ));
                        cursor = base + 28;
                    }
                    COLUMN_KIND_I64 => {
                        integer_metas.push((name, u64_at(base + 16)?));
                        cursor = base + 24;
                    }
                    COLUMN_KIND_GEO => {
                        geo_metas.push((name, u64_at(base + 32)?));
                        cursor = base + 40;
                    }
                    COLUMN_KIND_BINDING => {
                        let mut vals: Vec<String> = Vec::with_capacity(3);
                        let mut cur = base;
                        for _ in 0..3 {
                            let len = u64::from(u16_at(at(cur, 2)?));
                            vals.push(
                                String::from_utf8(at(cur + 2, len)?.to_vec())
                                    .map_err(|_| invalid("invalid utf-8 in binding record"))?,
                            );
                            cur += 2 + len;
                        }
                        let mut it = vals.into_iter();
                        binding_meta = Some(StoredBinding {
                            plan_fingerprint: it.next().expect("three strings"),
                            body_path: it.next().expect("three strings"),
                            materialize_sha: it.next().expect("three strings"),
                        });
                        cursor = cur;
                    }
                    k => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("column kind {k} unknown to this binary"),
                        ))
                    }
                }
            }
        }
        // Shared sections.
        let mut texts = Vec::with_capacity(n_slots);
        let mut tcur = texts_off;
        for _ in 0..n_slots {
            let len = u32_at(tcur)?;
            if len == u32::MAX {
                texts.push(None);
                tcur += 4;
            } else {
                let bytes = at(tcur + 4, u64::from(len))?;
                texts.push(Some(
                    String::from_utf8(bytes.to_vec())
                        .map_err(|_| invalid("invalid utf-8 in doc text"))?,
                ));
                tcur += 4 + u64::from(len);
            }
        }
        let mut lineages = Vec::with_capacity(n_slots);
        let mut lcur = lineages_off;
        for _ in 0..n_slots {
            if at(lcur, 1)?[0] == 0 {
                lineages.push(None);
                lcur += 1;
            } else {
                lineages.push(Some(DocLineage {
                    parent_id: u64_at(lcur + 1)?,
                    group_id: u64_at(lcur + 9)?,
                    span_start: u32_at(lcur + 17)?,
                    span_end: u32_at(lcur + 21)?,
                }));
                lcur += 25;
            }
        }
        // Per-field sections.
        let mut fields = Vec::with_capacity(n_fields);
        for (name, fingerprint, total_length, dl_off, p_off, d_off) in metas {
            let mut doc_lengths = Vec::with_capacity(n_slots);
            for slot in 0..n_slots as u64 {
                doc_lengths.push(u32_at(dl_off + 4 * slot)?);
            }
            let postings = Self::read_v5_shaped_postings(all, d_off, p_off)?;
            fields.push(FieldStore {
                name,
                analysis_fingerprint: fingerprint,
                postings,
                doc_lengths,
                total_length,
            });
        }
        // Facet columns (v7 only): dict then per-slot ordinals.
        let mut facets = Vec::with_capacity(facet_metas.len());
        for (name, n_values, dict_off, ords_off) in facet_metas {
            let mut dict = Vec::with_capacity(n_values as usize);
            let mut index = HashMap::with_capacity(n_values as usize);
            let mut dcur = dict_off;
            for ord in 0..n_values {
                let len = u64::from(u16_at(at(dcur, 2)?));
                let value = String::from_utf8(at(dcur + 2, len)?.to_vec())
                    .map_err(|_| invalid("invalid utf-8 in facet value"))?;
                index.insert(value.clone(), ord);
                dict.push(value);
                dcur = dcur + 2 + len;
            }
            let mut ords = Vec::with_capacity(n_slots);
            for slot in 0..n_slots as u64 {
                let ord = u32_at(ords_off + 4 * slot)?;
                if ord != FACET_ABSENT && ord >= n_values {
                    return Err(invalid("facet ordinal out of dictionary range"));
                }
                ords.push(ord);
            }
            facets.push(FacetStore {
                name,
                dict,
                index,
                ords,
            });
        }
        // Numeric columns (v7 only): n_slots x f64 bits, NaN = absent.
        let mut numerics = Vec::with_capacity(numeric_metas.len());
        for (name, vals_off) in numeric_metas {
            let mut vals = Vec::with_capacity(n_slots);
            for slot in 0..n_slots as u64 {
                vals.push(f64::from_bits(u64_at(vals_off + 8 * slot)?));
            }
            numerics.push(NumericStore { name, vals });
        }
        // Map columns (v7 only): decode dictionaries, then split the
        // pairs section back into per-slot lists via the offsets
        // section's prefix sums.
        let read_dict = |off: u64, n: u32| -> io::Result<(Vec<String>, HashMap<String, u32>)> {
            let mut dict = Vec::with_capacity(n as usize);
            let mut index = HashMap::with_capacity(n as usize);
            let mut cur = off;
            for ord in 0..n {
                let len = u64::from(u16_at(at(cur, 2)?));
                let entry = String::from_utf8(at(cur + 2, len)?.to_vec())
                    .map_err(|_| invalid("invalid utf-8 in map dictionary"))?;
                index.insert(entry.clone(), ord);
                dict.push(entry);
                cur += 2 + len;
            }
            Ok((dict, index))
        };
        let pair_range = |offsets_off: u64, slot: usize| -> io::Result<(u64, u64)> {
            Ok((
                u64::from(u32_at(offsets_off + 4 * slot as u64)?),
                u64::from(u32_at(offsets_off + 4 * (slot as u64 + 1))?),
            ))
        };
        let mut map_facets = Vec::with_capacity(map_facet_metas.len());
        for (name, n_keys, n_values, keys_off, values_off, offsets_off, pairs_off) in
            map_facet_metas
        {
            let (keys, key_index) = read_dict(keys_off, n_keys)?;
            let (values, value_index) = read_dict(values_off, n_values)?;
            let mut pairs = Vec::with_capacity(n_slots);
            for slot in 0..n_slots {
                let (start, end) = pair_range(offsets_off, slot)?;
                let mut list = Vec::with_capacity((end - start) as usize);
                for p in start..end {
                    list.push((u32_at(pairs_off + 8 * p)?, u32_at(pairs_off + 8 * p + 4)?));
                }
                pairs.push(list);
            }
            map_facets.push(MapFacetStore {
                name,
                keys,
                key_index,
                values,
                value_index,
                pairs,
            });
        }
        let mut map_numerics = Vec::with_capacity(map_numeric_metas.len());
        for (name, n_keys, keys_off, offsets_off, pairs_off) in map_numeric_metas {
            // Key entries interleave min/max metadata (recomputed at
            // the next write), so read_dict does not apply.
            let mut keys = Vec::with_capacity(n_keys as usize);
            let mut key_index = HashMap::with_capacity(n_keys as usize);
            let mut cur = keys_off;
            for ord in 0..n_keys {
                let len = u64::from(u16_at(at(cur, 2)?));
                let entry = String::from_utf8(at(cur + 2, len)?.to_vec())
                    .map_err(|_| invalid("invalid utf-8 in map dictionary"))?;
                key_index.insert(entry.clone(), ord);
                keys.push(entry);
                cur += 2 + len + 16;
            }
            let mut pairs = Vec::with_capacity(n_slots);
            for slot in 0..n_slots {
                let (start, end) = pair_range(offsets_off, slot)?;
                let mut list = Vec::with_capacity((end - start) as usize);
                for p in start..end {
                    list.push((
                        u32_at(pairs_off + 12 * p)?,
                        f64::from_bits(u64_at(pairs_off + 12 * p + 4)?),
                    ));
                }
                pairs.push(list);
            }
            map_numerics.push(MapNumericStore {
                name,
                keys,
                key_index,
                pairs,
            });
        }
        // i64 columns (v7 only): n_slots x i64, INTEGER_ABSENT = absent.
        let mut integers = Vec::with_capacity(integer_metas.len());
        for (name, vals_off) in integer_metas {
            let mut vals = Vec::with_capacity(n_slots);
            for slot in 0..n_slots as u64 {
                vals.push(u64_at(vals_off + 8 * slot)? as i64);
            }
            integers.push(IntStore { name, vals });
        }
        // Geo columns (v7 only): n_slots x (f64 lat, f64 lon) at a
        // 16 B stride, (NaN, NaN) = absent. Validation already refused a
        // half-NaN pair, so the loader takes the bytes as they are.
        let mut geos = Vec::with_capacity(geo_metas.len());
        for (name, vals_off) in geo_metas {
            let mut vals = Vec::with_capacity(n_slots);
            for slot in 0..n_slots as u64 {
                vals.push((
                    f64::from_bits(u64_at(vals_off + 16 * slot)?),
                    f64::from_bits(u64_at(vals_off + 16 * slot + 8)?),
                ));
            }
            geos.push(GeoStore { name, vals });
        }
        Ok(Self {
            fields,
            texts,
            lineages,
            facets,
            numerics,
            map_facets,
            map_numerics,
            integers,
            geos,
            binding: binding_meta,
        })
    }
}

/// One field's slice of a [`SpillBuilder`]: its pending postings
/// buffer, spilled runs, and per-document lengths — the spill-side
/// mirror of [`FieldStore`].
struct FieldSpill {
    name: String,
    /// Hash of the field's AnalysisSpec, persisted in the v6 field
    /// table. 0 until the ingest layer wires real fingerprints.
    analysis_fingerprint: u64,
    /// Pending postings: (term, doc_id, tf, offsets).
    buf: Vec<(String, u32, u32, Vec<(u32, u32)>)>,
    runs: Vec<PathBuf>,
    doc_lengths: Vec<u32>,
    total_length: u64,
}

impl FieldSpill {
    fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            analysis_fingerprint: 0,
            buf: Vec::new(),
            runs: Vec::new(),
            doc_lengths: Vec::new(),
            total_length: 0,
        }
    }
}

/// Disk-spilling builder producing the SAME v6 file as [`Bm25Store::save`]
/// (byte-identical), with bounded memory at any corpus size.
///
/// The in-memory store keeps every posting and every text in heap until
/// flush — ~100 GB for a 10M-chunk shard. This builder instead:
///
/// - streams document texts to a spill file AT ADD TIME, already in the
///   final texts-section encoding (gap slots get their `u32::MAX` marker
///   immediately), so flush byte-copies the file into place;
/// - accumulates postings in a bounded per-field buffer; when the total
///   fills, every field's buffer is sorted by `(term, doc_id)` and
///   written out as that field's next run. Doc ids only grow, so runs
///   never overlap within a term and the flush-time merge is a heap of
///   run heads concatenating per-term lists in run order — one
///   sequential pass per field, no random access.
///
/// Heap while building: the sort buffers (default 256 MB total) plus the
/// per-doc length/lineage tables. A spilling shard is NOT searchable
/// (that would mean scanning every run per term); flush it first.
pub struct SpillBuilder {
    dir: PathBuf,
    /// Per-field state, field-id order; never empty. Field 0 is the
    /// body.
    fields: Vec<FieldSpill>,
    /// Total bytes buffered across every field's `buf`.
    buf_bytes: usize,
    cap_bytes: usize,
    /// Texts spill, encoded exactly as the v3 texts section.
    texts: io::BufWriter<std::fs::File>,
    texts_bytes: u64,
    /// Per-slot text byte length (`u32::MAX` = absent); sizes the final
    /// sections without rereading the spill.
    text_lens: Vec<u32>,
    lineages: Vec<Option<DocLineage>>,
    /// Documents with postings in any field.
    doc_count: u64,
    /// Facet columns in facet-id order (dict + per-slot ordinals stay
    /// in heap: kilobytes of dictionary plus 4 B per slot, never the
    /// memory ceiling the spill exists for). Non-empty makes `finish`
    /// write v7.
    facets: Vec<FacetStore>,
    /// Numeric columns in numeric-id order (8 B per slot in heap, same
    /// argument as `facets`). Non-empty makes `finish` write v7.
    numerics: Vec<NumericStore>,
    /// map<string, string> columns (dictionaries plus per-doc pair
    /// lists in heap — bytes per entry, never the spill's memory
    /// ceiling). Non-empty makes `finish` write v7.
    map_facets: Vec<MapFacetStore>,
    /// map<string, f64> columns. Non-empty makes `finish` write v7.
    map_numerics: Vec<MapNumericStore>,
    /// i64 columns (8 B per slot in heap, same argument as `numerics`).
    /// Non-empty makes `finish` write v7.
    integers: Vec<IntStore>,
    /// Geo-point columns (16 B per slot in heap). Non-empty makes
    /// `finish` write v7.
    geos: Vec<GeoStore>,
    /// The mapped-plan binding, persisted as the kind-6 table entry
    /// when `finish` writes v7 (`Some` forces v7). The v4 oracle
    /// format cannot carry it and refuses.
    binding: Option<StoredBinding>,
    /// Write the v4 format instead of v6 (benchmarking/migration only).
    v4_only: bool,
}

impl SpillBuilder {
    /// Default sort-buffer capacity before runs are spilled.
    pub const DEFAULT_BUF_BYTES: usize = 256 << 20;

    /// Create a single-field ("body") builder spilling into `dir`
    /// (created; must not hold a previous builder's files). `finish`
    /// writes the v6 format.
    pub fn create(dir: &Path) -> io::Result<Self> {
        Self::create_format(dir, &["body"], false)
    }

    /// Create a builder with the given field table; same contract as
    /// [`Bm25Store::with_fields`].
    pub fn create_with_fields(dir: &Path, names: &[&str]) -> io::Result<Self> {
        Self::create_format(dir, names, false)
    }

    /// A builder whose `finish` writes the v4 format. Exists for
    /// benchmarking (v4-vs-v6 scorer comparisons) and migration checks;
    /// new shards are always v6.
    pub fn create_v4_for_bench(dir: &Path) -> io::Result<Self> {
        Self::create_format(dir, &["body"], true)
    }

    fn create_format(dir: &Path, names: &[&str], v4_only: bool) -> io::Result<Self> {
        assert!(!names.is_empty(), "a builder needs at least one field");
        for (i, name) in names.iter().enumerate() {
            assert!(!name.is_empty(), "field {i} has an empty name");
            assert!(
                u16::try_from(name.len()).is_ok(),
                "field name exceeds u16 length"
            );
            assert!(!names[..i].contains(name), "duplicate field name {name:?}");
        }
        std::fs::create_dir_all(dir)?;
        let texts = io::BufWriter::new(std::fs::File::create(dir.join("texts.spill"))?);
        Ok(Self {
            dir: dir.to_path_buf(),
            fields: names.iter().map(|n| FieldSpill::new(n)).collect(),
            buf_bytes: 0,
            cap_bytes: Self::DEFAULT_BUF_BYTES,
            texts,
            texts_bytes: 0,
            text_lens: Vec::new(),
            lineages: Vec::new(),
            doc_count: 0,
            facets: Vec::new(),
            numerics: Vec::new(),
            map_facets: Vec::new(),
            map_numerics: Vec::new(),
            integers: Vec::new(),
            geos: Vec::new(),
            binding: None,
            v4_only,
        })
    }

    /// Declare the facet field table, in facet-id order; same contract
    /// as [`Bm25Store::with_facets`]. Must be called before any
    /// document is added; makes `finish` write v7.
    pub fn with_facet_fields(mut self, names: &[&str]) -> Self {
        assert!(
            self.fields[0].doc_lengths.is_empty(),
            "facet fields must be declared before documents are added"
        );
        assert!(!self.v4_only, "the v4 format carries no facet columns");
        validate_facet_names(names);
        self.facets = names.iter().map(|n| FacetStore::new(n)).collect();
        self
    }

    /// Number of facet fields in the facet table.
    pub fn facet_count(&self) -> usize {
        self.facets.len()
    }

    /// The name of facet field `fi`. Panics when out of range.
    pub fn facet_name(&self, fi: usize) -> &str {
        &self.facets[fi].name
    }

    /// The index of the facet field named `name`, if the table has it.
    pub fn facet_index(&self, name: &str) -> Option<usize> {
        self.facets.iter().position(|f| f.name == name)
    }

    /// Record `doc_id`'s value for facet field `fi`; see
    /// [`FacetStore::set`] for the contract.
    pub fn set_facet(&mut self, fi: usize, doc_id: u32, value: &str) {
        self.facets[fi].set(doc_id, value);
    }

    /// Declare the numeric field table; same contract as
    /// [`Bm25Store::with_numerics`]. Must be called before any
    /// document is added; makes `finish` write v7.
    pub fn with_numeric_fields(mut self, names: &[&str]) -> Self {
        assert!(
            self.fields[0].doc_lengths.is_empty(),
            "numeric fields must be declared before documents are added"
        );
        assert!(!self.v4_only, "the v4 format carries no columns");
        validate_facet_names(names);
        self.numerics = names.iter().map(|n| NumericStore::new(n)).collect();
        self
    }

    /// Number of numeric fields in the numeric table.
    pub fn numeric_count(&self) -> usize {
        self.numerics.len()
    }

    /// The name of numeric field `ni`. Panics when out of range.
    pub fn numeric_name(&self, ni: usize) -> &str {
        &self.numerics[ni].name
    }

    /// The index of the numeric field named `name`, if the table has it.
    pub fn numeric_index(&self, name: &str) -> Option<usize> {
        self.numerics.iter().position(|n| n.name == name)
    }

    /// Record `doc_id`'s value for numeric field `ni`; see
    /// [`NumericStore::set`] for the contract (finite values only).
    pub fn set_numeric(&mut self, ni: usize, doc_id: u32, value: f64) {
        self.numerics[ni].set(doc_id, value);
    }

    /// Declare the i64 field table; same contract as
    /// [`Bm25Store::with_integers`]. Must be called before any
    /// document is added; makes `finish` write v7.
    pub fn with_integer_fields(mut self, names: &[&str]) -> Self {
        assert!(
            self.fields[0].doc_lengths.is_empty(),
            "integer fields must be declared before documents are added"
        );
        assert!(!self.v4_only, "the v4 format carries no columns");
        validate_facet_names(names);
        self.integers = names.iter().map(|n| IntStore::new(n)).collect();
        self
    }

    /// Number of i64 fields in the integer table.
    pub fn integer_count(&self) -> usize {
        self.integers.len()
    }

    /// The name of integer field `ii`. Panics when out of range.
    pub fn integer_name(&self, ii: usize) -> &str {
        &self.integers[ii].name
    }

    /// The index of the integer field named `name`, if the table has it.
    pub fn integer_index(&self, name: &str) -> Option<usize> {
        self.integers.iter().position(|n| n.name == name)
    }

    /// Record `doc_id`'s value for integer field `ii`; see
    /// [`IntStore::set`] for the contract.
    pub fn set_integer(&mut self, ii: usize, doc_id: u32, value: i64) {
        self.integers[ii].set(doc_id, value);
    }

    /// Declare the geo-point column table; same contract as
    /// [`Bm25Store::with_geos`]. Must be called before any document is
    /// added; makes `finish` write v7.
    pub fn with_geo_fields(mut self, names: &[&str]) -> Self {
        assert!(
            self.fields[0].doc_lengths.is_empty(),
            "geo fields must be declared before documents are added"
        );
        assert!(!self.v4_only, "the v4 format carries no columns");
        validate_facet_names(names);
        self.geos = names.iter().map(|n| GeoStore::new(n)).collect();
        self
    }

    /// Number of geo fields in the geo table.
    pub fn geo_count(&self) -> usize {
        self.geos.len()
    }

    /// The name of geo field `gi`. Panics when out of range.
    pub fn geo_name(&self, gi: usize) -> &str {
        &self.geos[gi].name
    }

    /// The index of the geo field named `name`, if the table has it.
    pub fn geo_index(&self, name: &str) -> Option<usize> {
        self.geos.iter().position(|n| n.name == name)
    }

    /// Record `doc_id`'s point for geo field `gi`; see [`GeoStore::set`]
    /// for the contract.
    pub fn set_geo(&mut self, gi: usize, doc_id: u32, lat: f64, lon: f64) {
        self.geos[gi].set(doc_id, lat, lon);
    }

    /// Declare the map<string, string> column table; same contract as
    /// [`Bm25Store::with_map_facets`].
    pub fn with_map_facet_fields(mut self, names: &[&str]) -> Self {
        assert!(
            self.fields[0].doc_lengths.is_empty(),
            "map columns must be declared before documents are added"
        );
        assert!(!self.v4_only, "the v4 format carries no columns");
        validate_facet_names(names);
        self.map_facets = names.iter().map(|n| MapFacetStore::new(n)).collect();
        self
    }

    /// Declare the map<string, f64> column table; same contract as
    /// [`Bm25Store::with_map_numerics`].
    pub fn with_map_numeric_fields(mut self, names: &[&str]) -> Self {
        assert!(
            self.fields[0].doc_lengths.is_empty(),
            "map columns must be declared before documents are added"
        );
        assert!(!self.v4_only, "the v4 format carries no columns");
        validate_facet_names(names);
        self.map_numerics = names.iter().map(|n| MapNumericStore::new(n)).collect();
        self
    }

    /// The index of the map-facet column named `name`.
    pub fn map_facet_index(&self, name: &str) -> Option<usize> {
        self.map_facets.iter().position(|c| c.name == name)
    }

    /// The index of the map-numeric column named `name`.
    pub fn map_numeric_index(&self, name: &str) -> Option<usize> {
        self.map_numerics.iter().position(|c| c.name == name)
    }

    /// Record `doc_id`'s map-facet entry; see [`MapFacetStore::set`].
    pub fn set_map_facet(&mut self, ci: usize, doc_id: u32, key: &str, value: &str) {
        self.map_facets[ci].set(doc_id, key, value);
    }

    /// Record `doc_id`'s map-numeric entry; see [`MapNumericStore::set`].
    pub fn set_map_numeric(&mut self, ci: usize, doc_id: u32, key: &str, value: f64) {
        self.map_numerics[ci].set(doc_id, key, value);
    }

    /// Override the sort-buffer capacity (tests force multi-run merges
    /// with tiny caps).
    pub fn with_buffer_bytes(mut self, cap: usize) -> Self {
        self.cap_bytes = cap.max(1);
        self
    }

    /// The number of document slots ever allocated (the next local doc id).
    pub fn next_doc_id(&self) -> u32 {
        self.fields[0].doc_lengths.len() as u32
    }

    /// Number of documents with postings (in any field).
    pub fn doc_count(&self) -> u64 {
        self.doc_count
    }

    /// Number of fields in the field table.
    pub fn field_count(&self) -> usize {
        self.fields.len()
    }

    /// The name of field `f`. Panics when out of range.
    pub fn field_name(&self, f: usize) -> &str {
        &self.fields[f].name
    }

    /// Record field `f`'s analyzer fingerprint, or refuse if it
    /// contradicts what the field already holds.
    ///
    /// A fingerprint is written once, by the first document that carries
    /// one, and is immutable after. A LATER document analyzed differently
    /// into the same column is exactly the drift this exists to catch:
    /// the two halves of the column would hold different term identities
    /// and every score over it would silently mix them. 0 means the
    /// caller does not know its own spec, which neither sets nor checks.
    pub fn set_analysis_fingerprint(&mut self, f: usize, fingerprint: u64) -> Result<(), String> {
        if fingerprint == 0 {
            return Ok(());
        }
        let field = &mut self.fields[f];
        match field.analysis_fingerprint {
            0 => {
                field.analysis_fingerprint = fingerprint;
                Ok(())
            }
            held if held == fingerprint => Ok(()),
            held => Err(format!(
                "field {:?} was built with analyzer fingerprint {held:#x} but this \
                 document carries {fingerprint:#x}; one column holds one term identity",
                field.name
            )),
        }
    }

    /// Field `f`'s analyzer fingerprint (0 = unknown).
    pub fn analysis_fingerprint(&self, f: usize) -> u64 {
        self.fields[f].analysis_fingerprint
    }

    /// Sum of all body document lengths (BM25 avgdl numerator).
    pub fn total_doc_length(&self) -> u64 {
        self.fields[0].total_length
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
        assert!(
            doc.fields.len() <= self.fields.len(),
            "document carries {} fields, builder has {}",
            doc.fields.len(),
            self.fields.len()
        );
        let slot = doc_id as usize;
        assert!(
            slot >= self.fields[0].doc_lengths.len(),
            "doc id {doc_id} already used"
        );
        // Gap slots (ids consumed by the vector side) are written to the
        // spill NOW so the final texts section is a straight copy.
        while self.fields[0].doc_lengths.len() < slot {
            write_u32(&mut self.texts, u32::MAX)?;
            self.texts_bytes += 4;
            self.text_lens.push(u32::MAX);
            for field in &mut self.fields {
                field.doc_lengths.push(0);
            }
            self.lineages.push(None);
        }
        write_u32(&mut self.texts, text.len() as u32)?;
        self.texts.write_all(text.as_bytes())?;
        self.texts_bytes += 4 + text.len() as u64;
        self.text_lens.push(text.len() as u32);
        self.lineages.push(lineage);
        let mut lengths = vec![0u32; self.fields.len()];
        for (fi, analyzed) in doc.fields.into_iter().enumerate() {
            lengths[fi] = analyzed.length;
            let field = &mut self.fields[fi];
            field.total_length += u64::from(analyzed.length);
            for (term, tf, offsets) in analyzed.terms {
                self.buf_bytes += term.len() + 24 + 16 * offsets.len();
                field.buf.push((term, doc_id, tf, offsets));
            }
        }
        if lengths.iter().any(|&l| l > 0) {
            self.doc_count += 1;
        }
        for (field, &length) in self.fields.iter_mut().zip(&lengths) {
            field.doc_lengths.push(length);
        }
        if self.buf_bytes >= self.cap_bytes {
            self.spill_run()?;
        }
        Ok(())
    }

    /// Sort every field's buffer by `(term, doc_id)` and write each as
    /// that field's next run.
    fn spill_run(&mut self) -> io::Result<()> {
        for (fi, field) in self.fields.iter_mut().enumerate() {
            if field.buf.is_empty() {
                continue;
            }
            field
                .buf
                .sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
            let path = self
                .dir
                .join(format!("run-f{fi:03}-{:06}", field.runs.len()));
            let mut w = io::BufWriter::new(std::fs::File::create(&path)?);
            let mut i = 0;
            while i < field.buf.len() {
                let term = &field.buf[i].0;
                let group_end = field.buf[i..]
                    .iter()
                    .position(|e| &e.0 != term)
                    .map_or(field.buf.len(), |p| i + p);
                write_u16(&mut w, term.len() as u16)?;
                w.write_all(term.as_bytes())?;
                write_u32(&mut w, (group_end - i) as u32)?;
                for (_, doc_id, tf, offsets) in &field.buf[i..group_end] {
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
            field.runs.push(path);
            field.buf.clear();
        }
        self.buf_bytes = 0;
        Ok(())
    }

    /// Merge the runs and assemble the file at `path` (atomically:
    /// write tmp, rename). The spill directory is removed on success.
    /// Writes v6, byte-identical to [`Bm25Store::save`] on the same
    /// corpus, unless built with [`Self::create_v4_for_bench`].
    pub fn finish(&mut self, path: &Path) -> io::Result<()> {
        if self.v4_only {
            if self.binding.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "the v4 oracle format cannot carry a mapped-plan binding",
                ));
            }
            self.finish_v4(path)
        } else {
            self.finish_v6(path)
        }
    }

    /// The mapped-plan binding the finish will persist, if any.
    pub fn binding(&self) -> Option<&StoredBinding> {
        self.binding.as_ref()
    }

    /// Set the binding `finish` persists (see [`Bm25Store::set_binding`]).
    pub fn set_binding(&mut self, binding: Option<StoredBinding>) {
        self.binding = binding;
    }

    /// Merge one field's runs into a postings-section body file at
    /// `body_path` (per-term doc/occurrence/skip runs, WITHOUT the
    /// section's leading u32 n_terms), returning the directory
    /// `(term, doc_rel, skip_rel, occ_rel, df)` with offsets relative
    /// to the body start. Merge side of the v5-shaped postings layout:
    /// the doc run streams into the body while occurrence bytes divert
    /// to a per-term stage file and the skip builder accumulates
    /// `(tf, dl)` per 128-posting block — all single-pass with O(1)
    /// state per term, so the sub-1 GB build memory the spill builder
    /// buys is untouched.
    fn merge_field_runs(
        runs: &[PathBuf],
        doc_lengths: &[u32],
        body_path: &Path,
        occ_stage_path: &Path,
    ) -> io::Result<Vec<(String, u64, u64, u64, u32)>> {
        let mut directory: Vec<(String, u64, u64, u64, u32)> = Vec::new();
        let mut out = io::BufWriter::new(std::fs::File::create(body_path)?);
        let mut heads: Vec<RunHead> = Vec::new();
        for run in runs {
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
                let mut occ_stage = io::BufWriter::new(std::fs::File::create(occ_stage_path)?);
                let mut idx = 0;
                while idx < heads.len() {
                    if heads[idx].term == term {
                        for _ in 0..heads[idx].n_postings {
                            let (doc_id, tf, offsets) = heads[idx].next_posting_raw()?;
                            let dl = doc_lengths[doc_id as usize];
                            write_u32(&mut out, doc_id)?;
                            write_u32(&mut out, tf)?;
                            write_u32(&mut out, occ_start)?;
                            // The run encoding's offset bytes are
                            // exactly the occurrence-run pairs.
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
                let mut stage = std::fs::File::open(occ_stage_path)?;
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
        Ok(directory)
    }

    /// Merge every field's runs and assemble the v6 file at `path` —
    /// v7 when facet fields are declared — mirroring
    /// [`Bm25Store::write_v6_to`] section for section (the dual-writer
    /// byte-identity test pins the two).
    fn finish_v6(&mut self, path: &Path) -> io::Result<()> {
        self.spill_run()?;
        self.texts.flush()?;

        let occ_stage_path = self.dir.join("occ.stage");
        let mut directories: Vec<Vec<(String, u64, u64, u64, u32)>> =
            Vec::with_capacity(self.fields.len());
        let mut body_paths: Vec<PathBuf> = Vec::with_capacity(self.fields.len());
        let mut body_lens: Vec<u64> = Vec::with_capacity(self.fields.len());
        for (fi, field) in self.fields.iter().enumerate() {
            let body_path = self.dir.join(format!("postings-f{fi:03}.body"));
            let directory = Self::merge_field_runs(
                &field.runs,
                &field.doc_lengths,
                &body_path,
                &occ_stage_path,
            )?;
            body_lens.push(std::fs::metadata(&body_path)?.len());
            body_paths.push(body_path);
            directories.push(directory);
        }

        // Section geometry, identical to Bm25Store::write_v6_to.
        let n_slots = self.fields[0].doc_lengths.len() as u64;
        let has_columns = !self.facets.is_empty()
            || !self.numerics.is_empty()
            || !self.map_facets.is_empty()
            || !self.map_numerics.is_empty()
            || !self.integers.is_empty()
            || !self.geos.is_empty()
            || self.binding.is_some();
        let column_table_size: u64 = if !has_columns {
            0
        } else {
            4 + self
                .facets
                .iter()
                .map(|f| 2 + f.name.len() as u64 + 1 + 4 + 8 + 8)
                .sum::<u64>()
                + self
                    .numerics
                    .iter()
                    .map(|n| 2 + n.name.len() as u64 + 1 + 8 + 8 + 8)
                    .sum::<u64>()
                + self
                    .map_facets
                    .iter()
                    .map(|c| 2 + c.name.len() as u64 + 1 + 4 + 4 + 8 * 4)
                    .sum::<u64>()
                + self
                    .map_numerics
                    .iter()
                    .map(|c| 2 + c.name.len() as u64 + 1 + 4 + 8 * 3)
                    .sum::<u64>()
                + self
                    .integers
                    .iter()
                    .map(|c| 2 + c.name.len() as u64 + 1 + 8 * 3)
                    .sum::<u64>()
                + self
                    .geos
                    .iter()
                    .map(|c| 2 + c.name.len() as u64 + 1 + 8 * 5)
                    .sum::<u64>()
                + binding_entry_size(self.binding.as_ref())
        };
        let header_size: u64 = 8
            + 4
            + 4
            + 8 * 3
            + self
                .fields
                .iter()
                .map(|f| 2 + f.name.len() as u64 + 8 * 5)
                .sum::<u64>()
            + column_table_size;
        let texts_off = header_size;
        let text_index_off = texts_off + self.texts_bytes;
        let lineages_off = text_index_off + 12 * n_slots;
        let lineages_size: u64 = self
            .lineages
            .iter()
            .map(|l| if l.is_some() { 25 } else { 1 })
            .sum();
        let mut cursor = lineages_off + lineages_size;
        // (doc_lengths_off, postings_off, directory_off) per field.
        let mut section_offs: Vec<(u64, u64, u64)> = Vec::with_capacity(self.fields.len());
        for fi in 0..self.fields.len() {
            let doc_lengths_off = cursor;
            let postings_off = doc_lengths_off + 4 * n_slots;
            let directory_off = postings_off + 4 + body_lens[fi];
            let directory_size = 4
                + 34 * directories[fi].len() as u64
                + directories[fi]
                    .iter()
                    .map(|(t, ..)| t.len() as u64)
                    .sum::<u64>();
            section_offs.push((doc_lengths_off, postings_off, directory_off));
            cursor = directory_off + directory_size;
        }
        // (dict_off, ords_off) per facet, then vals_off per numeric,
        // after the last field group — column-table order.
        let mut facet_offs: Vec<(u64, u64)> = Vec::with_capacity(self.facets.len());
        for facet in &self.facets {
            let dict_off = cursor;
            let dict_size: u64 = facet.dict.iter().map(|v| 2 + v.len() as u64).sum();
            let ords_off = dict_off + dict_size;
            facet_offs.push((dict_off, ords_off));
            cursor = ords_off + 4 * n_slots;
        }
        let mut numeric_offs: Vec<u64> = Vec::with_capacity(self.numerics.len());
        for _ in &self.numerics {
            numeric_offs.push(cursor);
            cursor += 8 * n_slots;
        }
        let mut map_facet_offs: Vec<(u64, u64, u64, u64)> =
            Vec::with_capacity(self.map_facets.len());
        for c in &self.map_facets {
            let keys_off = cursor;
            let keys_size: u64 = c.keys.iter().map(|k| 2 + k.len() as u64).sum();
            let values_off = keys_off + keys_size;
            let values_size: u64 = c.values.iter().map(|v| 2 + v.len() as u64).sum();
            let offsets_off = values_off + values_size;
            let pairs_off = offsets_off + 4 * (n_slots + 1);
            let total_pairs: u64 = c.pairs.iter().map(|l| l.len() as u64).sum();
            map_facet_offs.push((keys_off, values_off, offsets_off, pairs_off));
            cursor = pairs_off + 8 * total_pairs;
        }
        let mut map_numeric_offs: Vec<(u64, u64, u64)> =
            Vec::with_capacity(self.map_numerics.len());
        for c in &self.map_numerics {
            let keys_off = cursor;
            let keys_size: u64 = c.keys.iter().map(|k| 2 + k.len() as u64 + 16).sum();
            let offsets_off = keys_off + keys_size;
            let pairs_off = offsets_off + 4 * (n_slots + 1);
            let total_pairs: u64 = c.pairs.iter().map(|l| l.len() as u64).sum();
            map_numeric_offs.push((keys_off, offsets_off, pairs_off));
            cursor = pairs_off + 12 * total_pairs;
        }
        // vals_off per i64 column, last in table order (a new kind
        // appends; the earlier kinds' geometry must not shift).
        let mut integer_offs: Vec<u64> = Vec::with_capacity(self.integers.len());
        for _ in &self.integers {
            integer_offs.push(cursor);
            cursor += 8 * n_slots;
        }
        // vals_off per geo column, last in table order. Kind 5 appends
        // for the same reason kind 4 did: kinds 0 through 4 must keep
        // byte-for-byte the geometry they already have.
        let mut geo_offs: Vec<u64> = Vec::with_capacity(self.geos.len());
        for _ in &self.geos {
            geo_offs.push(cursor);
            cursor += 16 * n_slots;
        }

        let tmp = path.with_extension("bm25tmp");
        {
            let mut w = io::BufWriter::new(std::fs::File::create(&tmp)?);
            w.write_all(if has_columns { MAGIC_V7 } else { MAGIC_V6 })?;
            write_u32(&mut w, self.fields.len() as u32)?;
            write_u32(&mut w, n_slots as u32)?;
            write_u64(&mut w, texts_off)?;
            write_u64(&mut w, text_index_off)?;
            write_u64(&mut w, lineages_off)?;
            for (field, &(dl_off, p_off, d_off)) in self.fields.iter().zip(&section_offs) {
                write_u16(&mut w, field.name.len() as u16)?;
                w.write_all(field.name.as_bytes())?;
                write_u64(&mut w, field.analysis_fingerprint)?;
                write_u64(&mut w, field.total_length)?;
                write_u64(&mut w, dl_off)?;
                write_u64(&mut w, p_off)?;
                write_u64(&mut w, d_off)?;
            }
            if has_columns {
                write_u32(
                    &mut w,
                    (self.facets.len()
                        + self.numerics.len()
                        + self.map_facets.len()
                        + self.map_numerics.len()
                        + self.integers.len()
                        + self.geos.len()
                        + usize::from(self.binding.is_some())) as u32,
                )?;
                for (facet, &(dict_off, ords_off)) in self.facets.iter().zip(&facet_offs) {
                    write_u16(&mut w, facet.name.len() as u16)?;
                    w.write_all(facet.name.as_bytes())?;
                    w.write_all(&[COLUMN_KIND_FACET])?;
                    write_u32(&mut w, facet.dict.len() as u32)?;
                    write_u64(&mut w, dict_off)?;
                    write_u64(&mut w, ords_off)?;
                }
                for (numeric, &vals_off) in self.numerics.iter().zip(&numeric_offs) {
                    let (min, max) = numeric.min_max();
                    write_u16(&mut w, numeric.name.len() as u16)?;
                    w.write_all(numeric.name.as_bytes())?;
                    w.write_all(&[COLUMN_KIND_F64])?;
                    write_u64(&mut w, min.to_bits())?;
                    write_u64(&mut w, max.to_bits())?;
                    write_u64(&mut w, vals_off)?;
                }
                for (c, &(keys_off, values_off, offsets_off, pairs_off)) in
                    self.map_facets.iter().zip(&map_facet_offs)
                {
                    write_u16(&mut w, c.name.len() as u16)?;
                    w.write_all(c.name.as_bytes())?;
                    w.write_all(&[COLUMN_KIND_MAP_FACET])?;
                    write_u32(&mut w, c.keys.len() as u32)?;
                    write_u32(&mut w, c.values.len() as u32)?;
                    write_u64(&mut w, keys_off)?;
                    write_u64(&mut w, values_off)?;
                    write_u64(&mut w, offsets_off)?;
                    write_u64(&mut w, pairs_off)?;
                }
                for (c, &(keys_off, offsets_off, pairs_off)) in
                    self.map_numerics.iter().zip(&map_numeric_offs)
                {
                    write_u16(&mut w, c.name.len() as u16)?;
                    w.write_all(c.name.as_bytes())?;
                    w.write_all(&[COLUMN_KIND_MAP_F64])?;
                    write_u32(&mut w, c.keys.len() as u32)?;
                    write_u64(&mut w, keys_off)?;
                    write_u64(&mut w, offsets_off)?;
                    write_u64(&mut w, pairs_off)?;
                }
                for (c, &vals_off) in self.integers.iter().zip(&integer_offs) {
                    let (min, max) = c.min_max();
                    write_u16(&mut w, c.name.len() as u16)?;
                    w.write_all(c.name.as_bytes())?;
                    w.write_all(&[COLUMN_KIND_I64])?;
                    write_u64(&mut w, min as u64)?;
                    write_u64(&mut w, max as u64)?;
                    write_u64(&mut w, vals_off)?;
                }
                for (c, &vals_off) in self.geos.iter().zip(&geo_offs) {
                    let (min_lat, max_lat, min_lon, max_lon) = c.bbox();
                    write_u16(&mut w, c.name.len() as u16)?;
                    w.write_all(c.name.as_bytes())?;
                    w.write_all(&[COLUMN_KIND_GEO])?;
                    write_u64(&mut w, min_lat.to_bits())?;
                    write_u64(&mut w, max_lat.to_bits())?;
                    write_u64(&mut w, min_lon.to_bits())?;
                    write_u64(&mut w, max_lon.to_bits())?;
                    write_u64(&mut w, vals_off)?;
                }
                write_binding_entry(&mut w, self.binding.as_ref())?;
            }
            // texts: byte-copy of the spill (already section-encoded).
            let mut spill = std::fs::File::open(self.dir.join("texts.spill"))?;
            io::copy(&mut spill, &mut w)?;
            // text_index, from the in-memory length table; entries are
            // relative to the texts section start (v6).
            let mut tcur = 0u64;
            for &len in &self.text_lens {
                if len == u32::MAX {
                    write_u64(&mut w, 0)?;
                    write_u32(&mut w, u32::MAX)?;
                    tcur += 4;
                } else {
                    write_u64(&mut w, tcur + 4)?;
                    write_u32(&mut w, len)?;
                    tcur += 4 + len as u64;
                }
            }
            for lineage in &self.lineages {
                match lineage {
                    Some(l) => {
                        w.write_all(&[1u8])?;
                        write_u64(&mut w, l.parent_id)?;
                        write_u64(&mut w, l.group_id)?;
                        write_u32(&mut w, l.span_start)?;
                        write_u32(&mut w, l.span_end)?;
                    }
                    None => w.write_all(&[0u8])?,
                }
            }
            for (fi, field) in self.fields.iter().enumerate() {
                for &len in &field.doc_lengths {
                    write_u32(&mut w, len)?;
                }
                write_u32(&mut w, directories[fi].len() as u32)?;
                let mut body = std::fs::File::open(&body_paths[fi])?;
                io::copy(&mut body, &mut w)?;
                write_u32(&mut w, directories[fi].len() as u32)?;
                let mut blob_off = 0u64; // relative to the term blob start
                for (term, doc_rel, skip_rel, occ_rel, df) in &directories[fi] {
                    // Section-relative run offsets: past the section's
                    // leading u32 n_terms.
                    write_u64(&mut w, 4 + doc_rel)?;
                    write_u64(&mut w, 4 + skip_rel)?;
                    write_u64(&mut w, 4 + occ_rel)?;
                    write_u32(&mut w, *df)?;
                    write_u32(
                        &mut w,
                        u32::try_from(blob_off).expect("term blob exceeds u32"),
                    )?;
                    write_u16(&mut w, term.len() as u16)?;
                    blob_off += term.len() as u64;
                }
                for (term, ..) in &directories[fi] {
                    w.write_all(term.as_bytes())?;
                }
            }
            for facet in &self.facets {
                for value in &facet.dict {
                    write_u16(&mut w, value.len() as u16)?;
                    w.write_all(value.as_bytes())?;
                }
                for slot in 0..n_slots as usize {
                    write_u32(
                        &mut w,
                        facet.ords.get(slot).copied().unwrap_or(FACET_ABSENT),
                    )?;
                }
            }
            for numeric in &self.numerics {
                for slot in 0..n_slots as usize {
                    write_u64(
                        &mut w,
                        numeric
                            .vals
                            .get(slot)
                            .copied()
                            .unwrap_or(f64::NAN)
                            .to_bits(),
                    )?;
                }
            }
            for c in &self.map_facets {
                for key in &c.keys {
                    write_u16(&mut w, key.len() as u16)?;
                    w.write_all(key.as_bytes())?;
                }
                for value in &c.values {
                    write_u16(&mut w, value.len() as u16)?;
                    w.write_all(value.as_bytes())?;
                }
                write_map_offsets_and_pairs(&mut w, n_slots as usize, &c.pairs, |w, &(k, v)| {
                    write_u32(w, k)?;
                    write_u32(w, v)
                })?;
            }
            for c in &self.map_numerics {
                let mm = c.key_min_max();
                for (key, &(min, max)) in c.keys.iter().zip(&mm) {
                    write_u16(&mut w, key.len() as u16)?;
                    w.write_all(key.as_bytes())?;
                    write_u64(&mut w, min.to_bits())?;
                    write_u64(&mut w, max.to_bits())?;
                }
                write_map_offsets_and_pairs(&mut w, n_slots as usize, &c.pairs, |w, &(k, v)| {
                    write_u32(w, k)?;
                    write_u64(w, v.to_bits())
                })?;
            }
            for c in &self.integers {
                for slot in 0..n_slots as usize {
                    write_u64(
                        &mut w,
                        c.vals.get(slot).copied().unwrap_or(INTEGER_ABSENT) as u64,
                    )?;
                }
            }
            for c in &self.geos {
                for slot in 0..n_slots as usize {
                    let (lat, lon) = c.vals.get(slot).copied().unwrap_or(GEO_ABSENT);
                    write_u64(&mut w, lat.to_bits())?;
                    write_u64(&mut w, lon.to_bits())?;
                }
            }
            w.flush()?;
        }
        finalize_v8(&tmp)?;
        std::fs::rename(&tmp, path)?;
        fsync_parent(path)?;
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
            for run in &self.fields[0].runs {
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
        let field = &self.fields[0];
        let n_slots = field.doc_lengths.len() as u64;
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
            write_u64(&mut w, field.total_length)?;
            write_u64(&mut w, texts_off)?;
            write_u64(&mut w, lineages_off)?;
            write_u64(&mut w, postings_off)?;
            write_u64(&mut w, directory_off)?;
            write_u32(&mut w, field.doc_lengths.len() as u32)?;
            for &len in &field.doc_lengths {
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
                        write_u64(&mut w, l.parent_id)?;
                        write_u64(&mut w, l.group_id)?;
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
                write_u32(
                    &mut w,
                    u32::try_from(blob_off).expect("term blob exceeds u32"),
                )?;
                write_u16(&mut w, term.len() as u16)?;
                blob_off += term.len() as u64;
            }
            for (term, _, _) in &directory {
                w.write_all(term.as_bytes())?;
            }
            w.flush()?;
            w.get_ref().sync_all()?;
        }
        std::fs::rename(&tmp, path)?;
        fsync_parent(path)?;
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
        return Err(invalid(format!(
            "doc_lengths section [{doc_lengths_off}, {texts_off}) out of file ({file_len})"
        )));
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
    if postings_off < lineages_off
        || directory_off < postings_off + 4
        || directory_off + 4 > file_len
    {
        return Err(invalid("section offsets unordered or out of file".into()));
    }
    // Text index: each entry within the texts section (or the absent
    // marker).
    validate_text_index(map, texts_off, text_index_off, n_slots, 0)?;
    // Lineage walk: variable-stride entries must end exactly at the
    // postings section.
    if lineage_section_end(map, lineages_off, n_slots)? != postings_off {
        return Err(invalid(
            "lineage section does not end at the postings section".into(),
        ));
    }
    if v5 {
        return validate_v5_directory(map, postings_off, directory_off, file_len, 0);
    }
    // v3/v4: 18 B directory entries, interleaved postings records.
    let n_terms = u64::from(u32_at(directory_off)?);
    if u64::from(u32_at(postings_off)?) != n_terms {
        return Err(invalid("postings and directory term counts differ".into()));
    }
    let blob_start = directory_off + 4 + 18 * n_terms;
    if blob_start > file_len {
        return Err(invalid("directory overruns the file".into()));
    }
    let mut prev_term: Vec<u8> = Vec::new();
    for i in 0..n_terms {
        let e = directory_off + 4 + 18 * i;
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
            return Err(invalid(format!(
                "directory entry {i}: postings offset out of section"
            )));
        }
        // The postings entry's inline header must match the
        // directory entry exactly.
        let inline_len = u64::from(u32_at(postings_entry_off)?);
        if inline_len != term_len
            || bytes_at(postings_entry_off + 4, term_len)? != term
            || u32_at(postings_entry_off + 4 + term_len)? != df
        {
            return Err(invalid(format!(
                "directory entry {i}: postings header mismatch"
            )));
        }
        if !prev_term.is_empty() && term <= prev_term.as_slice() {
            return Err(invalid(format!(
                "directory entry {i}: terms not strictly ordered"
            )));
        }
        prev_term = term.to_vec();
    }
    Ok(())
}

/// Validate one v5-shaped postings/directory section pair (a v5 file's
/// only pair, or one field of a v6 file): term counts agree, entries in
/// bounds and strictly term-ordered, run offsets mutually consistent,
/// occurrence runs pair-aligned and sentinel-checked, skip runs walked
/// out exactly with block `last_doc_id`s cross-checked against the doc
/// run, and the term blob exactly filling the directory section to
/// `section_end`. `run_base` rebases the directory's run offsets: 0
/// when entries are absolute (v5), the field's postings section offset
/// when they are section-relative (v6).
fn validate_v5_directory(
    map: &[u8],
    postings_off: u64,
    directory_off: u64,
    section_end: u64,
    run_base: u64,
) -> io::Result<()> {
    let invalid = |msg: String| io::Error::new(io::ErrorKind::InvalidData, msg);
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
    let n_terms = u64::from(u32_at(directory_off)?);
    if u64::from(u32_at(postings_off)?) != n_terms {
        return Err(invalid("postings and directory term counts differ".into()));
    }
    let blob_start = directory_off + 4 + 34 * n_terms;
    if blob_start > section_end {
        return Err(invalid("directory overruns its section".into()));
    }
    let mut prev_term: Vec<u8> = Vec::new();
    let mut prev_skip_end: u64 = 0;
    let mut blob_used: u64 = 0;
    for i in 0..n_terms {
        let e = directory_off + 4 + 34 * i;
        let doc_run_off = u64_at(e)? + run_base;
        let skip_run_off = u64_at(e + 8)? + run_base;
        let occ_run_off = u64_at(e + 16)? + run_base;
        let df = u32_at(e + 24)?;
        let blob_off = u64::from(u32_at(e + 28)?);
        let term_len = u64::from(u16_at(bytes_at(e + 32, 2)?));
        if term_len == 0 {
            return Err(invalid(format!("directory entry {i}: empty term")));
        }
        if blob_start + blob_off + term_len > section_end {
            return Err(invalid(format!(
                "directory entry {i}: term out of the blob section"
            )));
        }
        let term = bytes_at(blob_start + blob_off, term_len)?;
        blob_used += term_len;
        // Run offsets: consistent with each other and with the
        // previous term's region.
        if doc_run_off < prev_skip_end || doc_run_off < postings_off + 4 {
            return Err(invalid(format!(
                "directory entry {i}: doc run overlaps previous regions"
            )));
        }
        if occ_run_off != doc_run_off + 12 * u64::from(df) + 4 || occ_run_off > skip_run_off {
            return Err(invalid(format!(
                "directory entry {i}: inconsistent run offsets"
            )));
        }
        let skip_end = if i + 1 < n_terms {
            u64_at(e + 34)? + run_base
        } else {
            directory_off
        };
        if skip_end < skip_run_off + 8 || skip_end > directory_off {
            return Err(invalid(format!(
                "directory entry {i}: skip run out of the postings section"
            )));
        }
        // Occurrence run: length divisible by 8 and equal to the
        // sentinel occ_start.
        if (skip_run_off - occ_run_off) % 8 != 0 {
            return Err(invalid(format!(
                "directory entry {i}: occurrence run not pair-aligned"
            )));
        }
        let sentinel = u32_at(doc_run_off + 12 * u64::from(df))?;
        if u64::from(sentinel) != (skip_run_off - occ_run_off) / 8 {
            return Err(invalid(format!(
                "directory entry {i}: sentinel occ_start mismatch"
            )));
        }
        if df > 0 && u32_at(doc_run_off + 8)? != 0 {
            return Err(invalid(format!(
                "directory entry {i}: first occ_start is not 0"
            )));
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
                return Err(invalid(format!(
                    "term {i} block {b}: n_pairs {n_pairs} out of range"
                )));
            }
            if last_doc < prev_last {
                return Err(invalid(format!(
                    "term {i} block {b}: last_doc_id goes backwards"
                )));
            }
            prev_last = last_doc;
            // Cross-check the block bound against the doc run.
            let last_posting = ((b + 1) * BLOCK as u64).min(u64::from(df)) - 1;
            if u32_at(doc_run_off + 12 * last_posting)? != last_doc {
                return Err(invalid(format!(
                    "term {i} block {b}: last_doc_id != doc run"
                )));
            }
            cur += 5 + 8 * n_pairs;
            if cur > region_end {
                return Err(invalid(format!(
                    "term {i}: level-0 records overrun the skip run"
                )));
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
                return Err(invalid(format!(
                    "term {i} group {g}: last_doc_id != doc run"
                )));
            }
            if l0_off != l0_record_offs[(g * LEVEL1_FACTOR as u64) as usize] {
                return Err(invalid(format!(
                    "term {i} group {g}: l0_off != group start"
                )));
            }
            cur += 13 + 8 * n_pairs;
        }
        if cur != region_end {
            return Err(invalid(format!(
                "term {i}: skip run does not end at the next region"
            )));
        }
        prev_skip_end = skip_end;
        if !prev_term.is_empty() && term <= prev_term.as_slice() {
            return Err(invalid(format!(
                "directory entry {i}: terms not strictly ordered"
            )));
        }
        prev_term = term.to_vec();
    }
    // The term blob exactly fills the directory section (terms are
    // laid out contiguously in entry order).
    if blob_start + blob_used != section_end {
        return Err(invalid(
            "term blob does not fill the directory section".into(),
        ));
    }
    Ok(())
}

/// Statistics from one [`transcode_to_v5`] run.
#[derive(Debug, Clone, Copy)]
pub struct TranscodeStats {
    /// Terms transcoded.
    pub n_terms: u32,
    /// Document slots carried over.
    pub n_slots: u32,
    /// Postings walked.
    pub postings: u64,
    /// Source file size in bytes.
    pub bytes_in: u64,
    /// Output file size in bytes.
    pub bytes_out: u64,
}

/// Rewrite a v3/v4 `.bm25` file as v5 (`TVBM2505`) without re-analysis:
/// the shared sections (doc_lengths, texts, text_index, lineages) are
/// byte-copied from the source map, and each term's interleaved v3/v4
/// postings are re-run into the v5 doc/occurrence/skip runs in two
/// map walks (doc run + skip state first, then the occurrence bytes,
/// which ARE the v5 occurrence-run encoding, copied map to file, never
/// staged). Heap per term is the skip builder's state plus its level-0
/// record bytes (~70 B per 128 postings, single-digit MB even for a
/// df-in-the-millions term), NOT the occurrence data — a hot term in a
/// 50 GB shard carries gigabytes of occurrence pairs, and staging
/// those was an OOM on real shards.
///
/// The output is byte-identical to what [`Bm25Store::save_v5`] would
/// write for the same corpus (pinned by test), so everything proven of
/// written-v5 files (dual-writer identity, pruned == exhaustive) holds
/// of transcoded ones. The source must be v3 or v4; the write is
/// atomic (tmp + rename) and the source is not modified.
pub fn transcode_to_v5(src: &Path, dst: &Path) -> io::Result<TranscodeStats> {
    use std::io::Seek;
    let invalid = |msg: &str| io::Error::new(io::ErrorKind::InvalidData, msg.to_string());
    let reader = Bm25Reader::open(src)?;
    if reader.v5_runs {
        return Err(invalid("source is already v5 or v6"));
    }
    let map: &[u8] = &reader.map;
    let u32_at = |off: usize| u32::from_le_bytes(map[off..off + 4].try_into().unwrap());
    let total_length = u64::from_le_bytes(map[8..16].try_into().unwrap());
    let texts_off = u64::from_le_bytes(map[16..24].try_into().unwrap());
    let lineages_off = u64::from_le_bytes(map[24..32].try_into().unwrap());
    let postings_off = u64::from_le_bytes(map[32..40].try_into().unwrap());
    let n_slots = u32_at(48);
    let field0 = &reader.fields[0];
    let n_terms = field0.n_terms;

    let tmp = dst.with_extension("bm25tmp");
    let mut w = io::BufWriter::new(std::fs::File::create(&tmp)?);
    w.write_all(MAGIC_V5)?;
    write_u64(&mut w, total_length)?;
    write_u64(&mut w, texts_off)?;
    write_u64(&mut w, lineages_off)?;
    write_u64(&mut w, postings_off)?;
    write_u64(&mut w, 0)?; // directory_off, patched after the postings pass
    write_u32(&mut w, n_slots)?;
    // Shared sections: identical bytes in v3/v4/v5, at identical
    // offsets (the header is the same 52 bytes), so the text index's
    // absolute offsets stay valid verbatim.
    w.write_all(&map[52..postings_off as usize])?;

    write_u32(&mut w, n_terms)?;
    let mut directory: Vec<(u64, u64, u64, u32)> = Vec::with_capacity(n_terms as usize);
    let mut cursor = postings_off + 4;
    let mut postings_walked = 0u64;
    for i in 0..n_terms {
        let (_, term_off, df) = reader.directory_entry(field0, i);
        // Step over the term's inline header: u32 len, term, u32 count.
        let term_len = u32_at(term_off as usize) as usize;
        let postings_start = term_off as usize + 4 + term_len + 4;
        let doc_run_off = cursor;
        // Pass 1: the doc run (doc_id, tf, occ_start) plus skip-run
        // state, straight off the map.
        let mut skip_l0: Vec<u8> = Vec::new();
        let mut skip = SkipRunBuilder::new();
        let mut occ_start = 0u32;
        let mut occ_bytes = 0u64;
        let mut p = postings_start;
        for _ in 0..df {
            let doc_id = u32_at(p);
            let tf = u32_at(p + 4);
            let n_offsets = u32_at(p + 8) as usize;
            write_u32(&mut w, doc_id)?;
            write_u32(&mut w, tf)?;
            write_u32(&mut w, occ_start)?;
            skip.push(tf, reader.doc_length(doc_id), doc_id, &mut skip_l0)?;
            occ_start = occ_start
                .checked_add(n_offsets as u32)
                .ok_or_else(|| invalid("occurrence run exceeds u32 pairs"))?;
            occ_bytes += 8 * n_offsets as u64;
            p += 12 + 8 * n_offsets;
        }
        write_u32(&mut w, occ_start)?; // sentinel
                                       // Pass 2: occurrence bytes, copied map to file per posting —
                                       // no staging, so the term's occurrence volume never touches
                                       // the heap. The re-walk revisits pages just read; the doc-run
                                       // fields it steps over are cheaper than staging gigabytes.
        let mut p = postings_start;
        for _ in 0..df {
            let n_offsets = u32_at(p + 8) as usize;
            w.write_all(&map[p + 12..p + 12 + 8 * n_offsets])?;
            p += 12 + 8 * n_offsets;
        }
        let (l0_bytes, l1) = skip.finish(&mut skip_l0)?;
        debug_assert_eq!(l0_bytes, skip_l0.len() as u64);
        let occ_run_off = doc_run_off + 12 * u64::from(df) + 4;
        let skip_run_off = occ_run_off + occ_bytes;
        write_skip_run(&mut w, &skip_l0, &l1)?;
        directory.push((doc_run_off, skip_run_off, occ_run_off, df));
        cursor = skip_run_off + skip_run_size(l0_bytes, &l1);
        postings_walked += u64::from(df);
    }
    let directory_off = cursor;
    write_u32(&mut w, n_terms)?;
    let mut blob_off = 0u64; // relative to the term blob start
    for (i, &(doc_off, skip_off, occ_off, df)) in directory.iter().enumerate() {
        let (term, _, _) = reader.directory_entry(field0, i as u32);
        write_u64(&mut w, doc_off)?;
        write_u64(&mut w, skip_off)?;
        write_u64(&mut w, occ_off)?;
        write_u32(&mut w, df)?;
        write_u32(
            &mut w,
            u32::try_from(blob_off).expect("term blob exceeds u32"),
        )?;
        write_u16(&mut w, term.len() as u16)?;
        blob_off += term.len() as u64;
    }
    for i in 0..n_terms {
        let (term, _, _) = reader.directory_entry(field0, i);
        w.write_all(term)?;
    }
    w.flush()?;
    let mut f = w
        .into_inner()
        .map_err(|e| io::Error::new(io::ErrorKind::Other, e.to_string()))?;
    let bytes_out = f.stream_position()?;
    f.seek(io::SeekFrom::Start(40))?;
    f.write_all(&directory_off.to_le_bytes())?;
    f.sync_all()?;
    drop(f);
    std::fs::rename(&tmp, dst)?;
    fsync_parent(dst)?;
    Ok(TranscodeStats {
        n_terms,
        n_slots,
        postings: postings_walked,
        bytes_in: map.len() as u64,
        bytes_out,
    })
}

/// Validate the text index: every present entry within the texts
/// section, absent markers untouched. `entry_base` rebases stored
/// offsets: the entries are absolute in v3/v4/v5 (base 0) and relative
/// to the texts section start in v6 (base `texts_off`).
fn validate_text_index(
    map: &[u8],
    texts_off: u64,
    text_index_off: u64,
    n_slots: u64,
    entry_base: u64,
) -> io::Result<()> {
    let invalid = |msg: String| io::Error::new(io::ErrorKind::InvalidData, msg);
    for slot in 0..n_slots {
        let e = (text_index_off + 12 * slot) as usize;
        let b = map
            .get(e..e + 12)
            .ok_or_else(|| invalid(format!("text index entry {slot} past end of file")))?;
        let offset = u64::from_le_bytes(b[..8].try_into().expect("8 bytes"));
        let len = u32::from_le_bytes(b[8..].try_into().expect("4 bytes"));
        if len != u32::MAX {
            let abs = entry_base + offset;
            if abs < texts_off + 4 || abs + u64::from(len) > text_index_off {
                return Err(invalid(format!(
                    "text index entry {slot} out of the texts section"
                )));
            }
        }
    }
    Ok(())
}

/// Walk the variable-stride lineage section (1 B absent, 25 B present)
/// and return the offset one past its last entry.
fn lineage_section_end(map: &[u8], lineages_off: u64, n_slots: u64) -> io::Result<u64> {
    let invalid = |msg: &str| io::Error::new(io::ErrorKind::InvalidData, msg.to_string());
    let mut cur = lineages_off;
    for _ in 0..n_slots {
        let flag = *map
            .get(cur as usize)
            .ok_or_else(|| invalid("lineage section overruns the file"))?;
        cur += if flag == 0 { 1 } else { 25 };
    }
    Ok(cur)
}

/// Full structural validation of a v6 or v7 file
/// (`docs/multi-field.md`; v7 adds facet columns), same
/// error-not-panic contract as [`validate_structure`]: the header,
/// field table, and (v7) facet table, shared-section geometry
/// (contiguous, in order), the text index and lineage walk, per field
/// the doc_lengths sum against the field table's total plus the full
/// v5-shaped directory and skip-run walk, and per facet the dict walk
/// and an ords scan (every ordinal in dictionary range or absent).
/// Field groups then facet groups must exactly tile the file from the
/// lineage section's end to EOF.
fn validate_structure_v6(map: &[u8], v7: bool) -> io::Result<()> {
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

    let n_fields = u64::from(u32_at(8)?);
    if n_fields == 0 {
        return Err(invalid("v6 file with zero fields".into()));
    }
    let n_slots = u64::from(u32_at(12)?);
    let texts_off = u64_at(16)?;
    let text_index_off = u64_at(24)?;
    let lineages_off = u64_at(32)?;
    // Field table walk: names non-empty and unique, offsets collected.
    let mut cursor = 40u64;
    let mut fields: Vec<(u64, u64, u64, u64)> = Vec::new(); // (total, dl, postings, dir)
    let mut names: Vec<Vec<u8>> = Vec::new();
    for i in 0..n_fields {
        let name_len = u64::from(u16_at(bytes_at(cursor, 2)?));
        if name_len == 0 {
            return Err(invalid(format!("field {i}: empty name")));
        }
        let name = bytes_at(cursor + 2, name_len)?.to_vec();
        if names.contains(&name) {
            return Err(invalid(format!("field {i}: duplicate name")));
        }
        names.push(name);
        let base = cursor + 2 + name_len;
        fields.push((
            u64_at(base + 8)?,  // total_length
            u64_at(base + 16)?, // doc_lengths_off
            u64_at(base + 24)?, // postings_off
            u64_at(base + 32)?, // directory_off
        ));
        cursor = base + 40;
    }
    // Column table (v7 only): kinded entries, names non-empty and
    // unique across the whole table, offsets collected per kind. A v7
    // file with zero columns is refused — the writers emit v6 for that
    // store, so the combination can only be corruption or a bug. An
    // unknown kind refuses by number rather than guessing a payload
    // width (parsing past it would misread every later entry).
    let mut facets: Vec<(u32, u64, u64)> = Vec::new(); // (n_values, dict, ords)
    let mut numerics: Vec<(u64, u64, u64)> = Vec::new(); // (min_bits, max_bits, vals)
                                                         // (n_keys, n_values, keys, values, offsets, pairs)
    let mut map_facets: Vec<(u32, u32, u64, u64, u64, u64)> = Vec::new();
    // (n_keys, keys, offsets, pairs)
    let mut map_numerics: Vec<(u32, u64, u64, u64)> = Vec::new();
    // (min_bits, max_bits, vals)
    let mut integers: Vec<(u64, u64, u64)> = Vec::new();
    // (min_lat, max_lat, min_lon, max_lon bits, vals)
    let mut geos: Vec<(u64, u64, u64, u64, u64)> = Vec::new();
    if v7 {
        let n_columns = u64::from(u32_at(cursor)?);
        if n_columns == 0 {
            return Err(invalid("v7 file with zero columns".into()));
        }
        cursor += 4;
        let mut column_names: Vec<Vec<u8>> = Vec::new();
        for i in 0..n_columns {
            let name_len = u64::from(u16_at(bytes_at(cursor, 2)?));
            if name_len == 0 {
                return Err(invalid(format!("column {i}: empty name")));
            }
            let name = bytes_at(cursor + 2, name_len)?.to_vec();
            if column_names.contains(&name) {
                return Err(invalid(format!("column {i}: duplicate name")));
            }
            column_names.push(name);
            let kind = bytes_at(cursor + 2 + name_len, 1)?[0];
            let base = cursor + 2 + name_len + 1;
            match kind {
                COLUMN_KIND_FACET => {
                    facets.push((
                        u32_at(base)?,      // n_values
                        u64_at(base + 4)?,  // dict_off
                        u64_at(base + 12)?, // ords_off
                    ));
                    cursor = base + 20;
                }
                COLUMN_KIND_F64 => {
                    numerics.push((u64_at(base)?, u64_at(base + 8)?, u64_at(base + 16)?));
                    cursor = base + 24;
                }
                COLUMN_KIND_MAP_FACET => {
                    map_facets.push((
                        u32_at(base)?,      // n_keys
                        u32_at(base + 4)?,  // n_values
                        u64_at(base + 8)?,  // keys_off
                        u64_at(base + 16)?, // values_off
                        u64_at(base + 24)?, // offsets_off
                        u64_at(base + 32)?, // pairs_off
                    ));
                    cursor = base + 40;
                }
                COLUMN_KIND_MAP_F64 => {
                    map_numerics.push((
                        u32_at(base)?,      // n_keys
                        u64_at(base + 4)?,  // keys_off
                        u64_at(base + 12)?, // offsets_off
                        u64_at(base + 20)?, // pairs_off
                    ));
                    cursor = base + 28;
                }
                COLUMN_KIND_I64 => {
                    integers.push((u64_at(base)?, u64_at(base + 8)?, u64_at(base + 16)?));
                    cursor = base + 24;
                }
                COLUMN_KIND_GEO => {
                    geos.push((
                        u64_at(base)?,      // min_lat bits
                        u64_at(base + 8)?,  // max_lat bits
                        u64_at(base + 16)?, // min_lon bits
                        u64_at(base + 24)?, // max_lon bits
                        u64_at(base + 32)?, // vals_off
                    ));
                    cursor = base + 40;
                }
                COLUMN_KIND_BINDING => {
                    // The reserved binding record: pinned name, inline
                    // payload, no sections. Duplicates fall to the
                    // table's name-uniqueness rule above.
                    if column_names.last().map(Vec::as_slice)
                        != Some(BINDING_ENTRY_NAME.as_bytes())
                    {
                        return Err(invalid(format!(
                            "column {i}: kind {COLUMN_KIND_BINDING} must be named \
                             {BINDING_ENTRY_NAME:?}"
                        )));
                    }
                    let mut cur = base;
                    for _ in 0..3 {
                        let len = u64::from(u16_at(bytes_at(cur, 2)?));
                        bytes_at(cur + 2, len)?;
                        cur += 2 + len;
                    }
                    cursor = cur;
                }
                k => {
                    return Err(invalid(format!(
                        "column {i}: kind {k} unknown to this binary"
                    )))
                }
            }
        }
    }
    let header_end = cursor;
    // Shared sections: contiguous from the header end, in order.
    if texts_off != header_end || texts_off > file_len {
        return Err(invalid(
            "texts section does not start at the header end".into(),
        ));
    }
    if text_index_off < texts_off
        || lineages_off != text_index_off + 12 * n_slots
        || lineages_off > file_len
    {
        return Err(invalid(
            "shared section offsets unordered or out of file".into(),
        ));
    }
    validate_text_index(map, texts_off, text_index_off, n_slots, texts_off)?;
    let lineage_end = lineage_section_end(map, lineages_off, n_slots)?;
    // Per-field groups: contiguous from the lineage end, tiling the
    // file exactly (each group's blob-fill check pins its end to the
    // next group's start, and the last to the first column section —
    // or EOF when there are none).
    let field_groups_end = facets
        .first()
        .map(|f| f.1)
        .or_else(|| numerics.first().map(|n| n.2))
        .or_else(|| map_facets.first().map(|c| c.2))
        .or_else(|| map_numerics.first().map(|c| c.1))
        .or_else(|| integers.first().map(|c| c.2))
        .or_else(|| geos.first().map(|c| c.4))
        .unwrap_or(file_len);
    let mut expected_start = lineage_end;
    for (i, &(total_length, dl_off, postings_off, directory_off)) in fields.iter().enumerate() {
        if dl_off != expected_start {
            return Err(invalid(format!(
                "field {i}: doc_lengths section does not start at the previous section's end"
            )));
        }
        if postings_off != dl_off + 4 * n_slots {
            return Err(invalid(format!(
                "field {i}: postings section does not follow doc_lengths"
            )));
        }
        let section_end = fields.get(i + 1).map_or(field_groups_end, |f| f.1);
        if directory_off < postings_off + 4 || directory_off + 4 > section_end {
            return Err(invalid(format!(
                "field {i}: directory offset out of the group"
            )));
        }
        // The field table total must agree with the doc-length table.
        let mut length_sum = 0u64;
        for slot in 0..n_slots {
            length_sum += u64::from(u32_at(dl_off + 4 * slot)?);
        }
        if length_sum != total_length {
            return Err(invalid(format!(
                "field {i}: total_length != sum of doc lengths"
            )));
        }
        validate_v5_directory(map, postings_off, directory_off, section_end, postings_off)?;
        expected_start = section_end;
    }
    // Facet groups: per facet a dict walk (n_values length-prefixed
    // entries ending exactly at the ords section) and an ords scan
    // (4 B per slot, every ordinal in range or absent), tiling from
    // the last field group to EOF.
    for (i, &(n_values, dict_off, ords_off)) in facets.iter().enumerate() {
        if dict_off != expected_start {
            return Err(invalid(format!(
                "facet field {i}: dict section does not start at the previous section's end"
            )));
        }
        let mut dcur = dict_off;
        for _ in 0..n_values {
            let len = u64::from(u16_at(bytes_at(dcur, 2)?));
            bytes_at(dcur + 2, len)?;
            dcur += 2 + len;
        }
        if dcur != ords_off {
            return Err(invalid(format!(
                "facet field {i}: dict section does not end at the ords section"
            )));
        }
        let group_end = ords_off + 4 * n_slots;
        let expected_end = facets
            .get(i + 1)
            .map(|f| f.1)
            .or_else(|| numerics.first().map(|n| n.2))
            .or_else(|| map_facets.first().map(|c| c.2))
            .or_else(|| map_numerics.first().map(|c| c.1))
            .or_else(|| integers.first().map(|c| c.2))
            .or_else(|| geos.first().map(|c| c.4))
            .unwrap_or(file_len);
        if group_end != expected_end {
            return Err(invalid(format!(
                "facet field {i}: ords section does not end at the next section's start"
            )));
        }
        for slot in 0..n_slots {
            let ord = u32_at(ords_off + 4 * slot)?;
            if ord != FACET_ABSENT && ord >= n_values {
                return Err(invalid(format!(
                    "facet field {i}: ordinal out of dictionary range at slot {slot}"
                )));
            }
        }
        expected_start = group_end;
    }
    // Numeric groups: one n_slots x f64 vals section each, tiling to
    // EOF. Every value is finite or NaN (NaN = absent; infinities
    // break the score-function bound algebra, so the writer refuses
    // them and the reader treats one as corruption), and the table's
    // min/max metadata must agree with a full scan — a bound computed
    // from stale metadata would prune true hits.
    for (i, &(min_bits, max_bits, vals_off)) in numerics.iter().enumerate() {
        if vals_off != expected_start {
            return Err(invalid(format!(
                "numeric field {i}: vals section does not start at the previous section's end"
            )));
        }
        let group_end = vals_off + 8 * n_slots;
        let expected_end = numerics
            .get(i + 1)
            .map(|n| n.2)
            .or_else(|| map_facets.first().map(|c| c.2))
            .or_else(|| map_numerics.first().map(|c| c.1))
            .or_else(|| integers.first().map(|c| c.2))
            .or_else(|| geos.first().map(|c| c.4))
            .unwrap_or(file_len);
        if group_end != expected_end {
            return Err(invalid(format!(
                "numeric field {i}: vals section does not end at the next section's start"
            )));
        }
        let mut min = f64::NAN;
        let mut max = f64::NAN;
        for slot in 0..n_slots {
            let v = f64::from_bits(u64_at(vals_off + 8 * slot)?);
            if v.is_infinite() {
                return Err(invalid(format!(
                    "numeric field {i}: non-finite value at slot {slot}"
                )));
            }
            if v.is_nan() {
                continue;
            }
            if min.is_nan() || v < min {
                min = v;
            }
            if max.is_nan() || v > max {
                max = v;
            }
        }
        if min.to_bits() != min_bits || max.to_bits() != max_bits {
            return Err(invalid(format!(
                "numeric field {i}: min/max metadata disagrees with the values"
            )));
        }
        expected_start = group_end;
    }
    // Map-facet groups (docs/map-columns.md): key dict, value dict,
    // offsets ((n_slots + 1) x u32 prefix sums, monotone from 0), and
    // pairs (per doc strictly key-ordered, ordinals in range), tiling
    // toward the map-numeric groups or EOF.
    for (i, &(n_keys, n_values, keys_off, values_off, offsets_off, pairs_off)) in
        map_facets.iter().enumerate()
    {
        if keys_off != expected_start {
            return Err(invalid(format!(
                "map-facet column {i}: keys section does not start at the previous section's end"
            )));
        }
        let mut cur = keys_off;
        for _ in 0..n_keys {
            let len = u64::from(u16_at(bytes_at(cur, 2)?));
            bytes_at(cur + 2, len)?;
            cur += 2 + len;
        }
        if cur != values_off {
            return Err(invalid(format!(
                "map-facet column {i}: keys section does not end at the values section"
            )));
        }
        for _ in 0..n_values {
            let len = u64::from(u16_at(bytes_at(cur, 2)?));
            bytes_at(cur + 2, len)?;
            cur += 2 + len;
        }
        if cur != offsets_off {
            return Err(invalid(format!(
                "map-facet column {i}: values section does not end at the offsets section"
            )));
        }
        if pairs_off != offsets_off + 4 * (n_slots + 1) || u32_at(offsets_off)? != 0 {
            return Err(invalid(format!(
                "map-facet column {i}: offsets section malformed"
            )));
        }
        let total_pairs = u64::from(u32_at(offsets_off + 4 * n_slots)?);
        let group_end = pairs_off + 8 * total_pairs;
        let expected_end = map_facets
            .get(i + 1)
            .map(|c| c.2)
            .or_else(|| map_numerics.first().map(|c| c.1))
            .or_else(|| integers.first().map(|c| c.2))
            .or_else(|| geos.first().map(|c| c.4))
            .unwrap_or(file_len);
        if group_end != expected_end {
            return Err(invalid(format!(
                "map-facet column {i}: pairs section does not end at the next section's start"
            )));
        }
        let mut prev_end = 0u64;
        for slot in 0..n_slots {
            let start = prev_end;
            let end = u64::from(u32_at(offsets_off + 4 * (slot + 1))?);
            if end < start || end > total_pairs {
                return Err(invalid(format!(
                    "map-facet column {i}: offsets not monotone at slot {slot}"
                )));
            }
            let mut prev_key: Option<u32> = None;
            for p in start..end {
                let key_ord = u32_at(pairs_off + 8 * p)?;
                let val_ord = u32_at(pairs_off + 8 * p + 4)?;
                if key_ord >= n_keys || val_ord >= n_values {
                    return Err(invalid(format!(
                        "map-facet column {i}: ordinal out of range at slot {slot}"
                    )));
                }
                if prev_key.is_some_and(|k| key_ord <= k) {
                    return Err(invalid(format!(
                        "map-facet column {i}: pairs not strictly key-ordered at slot {slot}"
                    )));
                }
                prev_key = Some(key_ord);
            }
            prev_end = end;
        }
        expected_start = group_end;
    }
    // Map-numeric groups: key dict with per-key min/max metadata,
    // offsets, and (key_ord, f64) pairs — finite values only, strictly
    // key-ordered per doc, per-key min/max re-derived and compared.
    for (i, &(n_keys, keys_off, offsets_off, pairs_off)) in map_numerics.iter().enumerate() {
        if keys_off != expected_start {
            return Err(invalid(format!(
                "map-numeric column {i}: keys section does not start at the previous \
                 section's end"
            )));
        }
        let mut key_mm: Vec<(u64, u64)> = Vec::with_capacity(n_keys as usize);
        let mut cur = keys_off;
        for _ in 0..n_keys {
            let len = u64::from(u16_at(bytes_at(cur, 2)?));
            bytes_at(cur + 2, len)?;
            key_mm.push((u64_at(cur + 2 + len)?, u64_at(cur + 2 + len + 8)?));
            cur += 2 + len + 16;
        }
        if cur != offsets_off {
            return Err(invalid(format!(
                "map-numeric column {i}: keys section does not end at the offsets section"
            )));
        }
        if pairs_off != offsets_off + 4 * (n_slots + 1) || u32_at(offsets_off)? != 0 {
            return Err(invalid(format!(
                "map-numeric column {i}: offsets section malformed"
            )));
        }
        let total_pairs = u64::from(u32_at(offsets_off + 4 * n_slots)?);
        let group_end = pairs_off + 12 * total_pairs;
        let expected_end = map_numerics
            .get(i + 1)
            .map(|c| c.1)
            .or_else(|| integers.first().map(|c| c.2))
            .or_else(|| geos.first().map(|c| c.4))
            .unwrap_or(file_len);
        if group_end != expected_end {
            return Err(invalid(format!(
                "map-numeric column {i}: pairs section does not end at the next section's start"
            )));
        }
        let mut scanned_mm: Vec<(f64, f64)> = vec![(f64::NAN, f64::NAN); n_keys as usize];
        let mut prev_end = 0u64;
        for slot in 0..n_slots {
            let start = prev_end;
            let end = u64::from(u32_at(offsets_off + 4 * (slot + 1))?);
            if end < start || end > total_pairs {
                return Err(invalid(format!(
                    "map-numeric column {i}: offsets not monotone at slot {slot}"
                )));
            }
            let mut prev_key: Option<u32> = None;
            for p in start..end {
                let key_ord = u32_at(pairs_off + 12 * p)?;
                let v = f64::from_bits(u64_at(pairs_off + 12 * p + 4)?);
                if key_ord >= n_keys {
                    return Err(invalid(format!(
                        "map-numeric column {i}: key ordinal out of range at slot {slot}"
                    )));
                }
                if !v.is_finite() {
                    return Err(invalid(format!(
                        "map-numeric column {i}: non-finite value at slot {slot}"
                    )));
                }
                if prev_key.is_some_and(|k| key_ord <= k) {
                    return Err(invalid(format!(
                        "map-numeric column {i}: pairs not strictly key-ordered at slot {slot}"
                    )));
                }
                prev_key = Some(key_ord);
                let (min, max) = &mut scanned_mm[key_ord as usize];
                if min.is_nan() || v < *min {
                    *min = v;
                }
                if max.is_nan() || v > *max {
                    *max = v;
                }
            }
            prev_end = end;
        }
        for (k, (&(min_bits, max_bits), &(min, max))) in key_mm.iter().zip(&scanned_mm).enumerate()
        {
            if min.to_bits() != min_bits || max.to_bits() != max_bits {
                return Err(invalid(format!(
                    "map-numeric column {i}: min/max metadata disagrees with the values \
                     for key {k}"
                )));
            }
        }
        expected_start = group_end;
    }
    // Integer groups (docs/range-facets.md): one n_slots x i64 vals
    // section each, tiling from the last map-numeric group to EOF. The
    // table's min/max must agree with a full scan over the NON-sentinel
    // values — i64::MIN is absence, so a column of nothing but absences
    // folds to the empty range (i64::MAX, i64::MIN), which is exactly
    // what the writer emits.
    for (i, &(min_bits, max_bits, vals_off)) in integers.iter().enumerate() {
        if vals_off != expected_start {
            return Err(invalid(format!(
                "integer field {i}: vals section does not start at the previous section's end"
            )));
        }
        let group_end = vals_off + 8 * n_slots;
        let expected_end = integers
            .get(i + 1)
            .map(|c| c.2)
            .or_else(|| geos.first().map(|c| c.4))
            .unwrap_or(file_len);
        if group_end != expected_end {
            return Err(invalid(format!(
                "integer field {i}: vals section does not end at the next section's start"
            )));
        }
        let mut min = i64::MAX;
        let mut max = i64::MIN;
        for slot in 0..n_slots {
            let v = u64_at(vals_off + 8 * slot)? as i64;
            if v == INTEGER_ABSENT {
                continue;
            }
            min = min.min(v);
            max = max.max(v);
        }
        if min as u64 != min_bits || max as u64 != max_bits {
            return Err(invalid(format!(
                "integer field {i}: min/max metadata disagrees with the values"
            )));
        }
        expected_start = group_end;
    }
    // Geo groups (docs/geo-columns.md): one n_slots x (f64 lat, f64 lon)
    // section each at a 16 B stride, tiling from the last integer group
    // to EOF. Three things are checked per slot, and each one is a lie
    // the reader must never repeat:
    //
    // - a HALF-NaN pair is corruption, not a sparser point. (NaN, NaN)
    //   is absence and a finite pair is a point; anything between is a
    //   value that lost half of itself, and guessing which half to
    //   believe is exactly the silent degradation this engine refuses.
    // - coordinates off the globe (or infinite) never survived ingest,
    //   so finding one means the bytes are not what the writer wrote.
    // - the table's bounding box must agree with a full scan, for the
    //   same reason kind 1's min/max must: metadata is re-derived, never
    //   trusted, and an empty column folds to four NaNs.
    for (i, &(min_lat_bits, max_lat_bits, min_lon_bits, max_lon_bits, vals_off)) in
        geos.iter().enumerate()
    {
        if vals_off != expected_start {
            return Err(invalid(format!(
                "geo field {i}: vals section does not start at the previous section's end"
            )));
        }
        let group_end = vals_off + 16 * n_slots;
        let expected_end = geos.get(i + 1).map_or(file_len, |c| c.4);
        if group_end != expected_end {
            return Err(invalid(format!(
                "geo field {i}: vals section does not end at the next section's start"
            )));
        }
        let (mut min_lat, mut max_lat) = (f64::NAN, f64::NAN);
        let (mut min_lon, mut max_lon) = (f64::NAN, f64::NAN);
        for slot in 0..n_slots {
            let lat = f64::from_bits(u64_at(vals_off + 16 * slot)?);
            let lon = f64::from_bits(u64_at(vals_off + 16 * slot + 8)?);
            if lat.is_nan() != lon.is_nan() {
                return Err(invalid(format!(
                    "geo field {i}: half-NaN coordinate pair at slot {slot} (absence is BOTH \
                     halves NaN; one half is a point that lost the other)"
                )));
            }
            if lat.is_nan() {
                continue;
            }
            if !(lat.is_finite() && (-90.0..=90.0).contains(&lat))
                || !(lon.is_finite() && (-180.0..=180.0).contains(&lon))
            {
                return Err(invalid(format!(
                    "geo field {i}: coordinate ({lat}, {lon}) at slot {slot} is not a finite \
                     degree pair on the globe"
                )));
            }
            if min_lat.is_nan() || lat < min_lat {
                min_lat = lat;
            }
            if max_lat.is_nan() || lat > max_lat {
                max_lat = lat;
            }
            if min_lon.is_nan() || lon < min_lon {
                min_lon = lon;
            }
            if max_lon.is_nan() || lon > max_lon {
                max_lon = lon;
            }
        }
        if min_lat.to_bits() != min_lat_bits
            || max_lat.to_bits() != max_lat_bits
            || min_lon.to_bits() != min_lon_bits
            || max_lon.to_bits() != max_lon_bits
        {
            return Err(invalid(format!(
                "geo field {i}: bounding-box metadata disagrees with the values"
            )));
        }
        expected_start = group_end;
    }
    Ok(())
}

fn u16_at(bytes: &[u8]) -> u16 {
    u16::from_le_bytes(bytes.try_into().expect("2 bytes"))
}

/// A memory-mapped, disk-resident view of a `.bm25` file (v3, v4, v5,
/// or single-field v6). Postings and document texts are read from the
/// map on demand; the only heap state is the per-document length table
/// and a term count.
/// Per-field read state of one open `.bm25` file: the field's decoded
/// doc-length table plus the map offsets of its directory and postings
/// sections. v3/v4/v5 files have exactly one ("body"); a v6 file one
/// per field-table entry.
struct FieldSlice {
    name: String,
    /// Hash of the field's AnalysisSpec, read back from the v6 field
    /// table. 0 for a shard written before fingerprints, which never
    /// enforces.
    analysis_fingerprint: u64,
    doc_lengths: Vec<u32>,
    total_length: u64,
    directory_off: u64,
    n_terms: u32,
    /// Base added to directory run offsets: 0 for v3/v4/v5 (absolute
    /// entries), the field's postings section offset for v6
    /// (section-relative entries).
    run_base: u64,
}

/// Per-facet read state of one open v7 file: the decoded value
/// dictionary (small — one entry per distinct value) plus the map
/// offset of the fixed-stride ords section, which stays on disk.
struct FacetSlice {
    name: String,
    /// Values in ordinal order, decoded eagerly at open.
    dict: Vec<String>,
    /// Absolute offset of the ords section (n_slots x u32).
    ords_off: u64,
}

/// Per-numeric-column read state of one open v7 file: the min/max
/// bound metadata from the column table plus the map offset of the
/// fixed-stride vals section, which stays on disk.
struct NumericSlice {
    name: String,
    /// Min over present values (NaN when the column holds none).
    min: f64,
    /// Max over present values (NaN when the column holds none).
    max: f64,
    /// Absolute offset of the vals section (n_slots x f64 bits).
    vals_off: u64,
}

/// Per-map-facet-column read state (`docs/map-columns.md`): both
/// dictionaries decoded eagerly (one entry per distinct key/value);
/// offsets and pairs sections stay in the map.
struct MapFacetSlice {
    name: String,
    /// Keys in ordinal order.
    keys: Vec<String>,
    /// Values in ordinal order.
    values: Vec<String>,
    /// Absolute offset of the offsets section ((n_slots + 1) x u32).
    offsets_off: u64,
    /// Absolute offset of the pairs section (8 B (key_ord, value_ord)
    /// entries, strictly key-ordered per document).
    pairs_off: u64,
}

/// Per-map-numeric-column read state: the key dictionary with its
/// per-key min/max bound metadata; offsets and pairs stay in the map.
struct MapNumericSlice {
    name: String,
    /// Keys in ordinal order.
    keys: Vec<String>,
    /// Per-key (min, max) over present values, parallel to `keys`.
    key_min_max: Vec<(f64, f64)>,
    /// Absolute offset of the offsets section ((n_slots + 1) x u32).
    offsets_off: u64,
    /// Absolute offset of the pairs section (12 B (key_ord, f64 bits)
    /// entries, strictly key-ordered per document).
    pairs_off: u64,
}

/// Per-integer-column read state of one open v7 file: the min/max
/// bound metadata from the column table plus the map offset of the
/// fixed-stride vals section, which stays on disk.
struct IntegerSlice {
    name: String,
    /// Min over present values (`i64::MAX` when the column holds none;
    /// see [`IntStore::min_max`] for why the empty range is inverted).
    min: i64,
    /// Max over present values (`i64::MIN` when the column holds none).
    max: i64,
    /// Absolute offset of the vals section (n_slots x i64).
    vals_off: u64,
}

/// Per-geo-column read state of one open v7 file: the bounding-box
/// metadata from the column table plus the map offset of the
/// fixed-stride vals section, which stays on disk.
struct GeoSlice {
    name: String,
    /// The column's bounding box over present points, all four NaN when
    /// there are none (the kind-1 empty convention). Validated against a
    /// full scan at open.
    bbox: (f64, f64, f64, f64),
    /// Absolute offset of the vals section (n_slots x (f64, f64)).
    vals_off: u64,
}

pub struct Bm25Reader {
    map: memmap2::Mmap,
    /// Per-field state, field-id order; never empty. Field 0 is the
    /// body.
    fields: Vec<FieldSlice>,
    /// Per-facet state, facet-id order (v7 files; empty otherwise).
    facets: Vec<FacetSlice>,
    /// Per-numeric-column state, numeric-id order (v7 files; empty
    /// otherwise).
    numerics: Vec<NumericSlice>,
    /// Per-map-facet-column state (v7 files; empty otherwise).
    map_facets: Vec<MapFacetSlice>,
    /// Per-map-numeric-column state (v7 files; empty otherwise).
    map_numerics: Vec<MapNumericSlice>,
    /// Per-integer-column state, integer-id order (v7 files; empty
    /// otherwise).
    integers: Vec<IntegerSlice>,
    /// Per-geo-column state, geo-id order (v7 files; empty otherwise).
    geos: Vec<GeoSlice>,
    /// Documents with postings in any field — the corpus-wide N (a
    /// document is a document; idf never uses a per-field count).
    doc_count: u64,
    lineages_off: u64,
    /// Start of the on-disk text index (explicit in the v6 header;
    /// derived as `lineages_off - 12 * n_slots` for v3/v4/v5).
    text_index_off: u64,
    /// Base added to text-index entry offsets: 0 for v3/v4/v5
    /// (absolute entries), the texts section offset for v6 (relative
    /// entries).
    text_base: u64,
    /// v5-shaped postings (v5 and v6 files): 34 B directory entries
    /// and the doc/occurrence/skip run layout; v3/v4: 18 B entries and
    /// interleaved postings.
    v5_runs: bool,
    /// v4+ directories store blob offsets relative to the blob start;
    /// v3 stored absolute file offsets.
    blob_relative: bool,
    /// Lazily built lineage-section index: per-slot byte offset relative
    /// to `lineages_off`. The section is variable stride (1 B absent, 25
    /// B present), so random access needs this — one O(n_slots) decode
    /// on the first `lineage()` call, ~4 B/slot of heap, cached.
    lineage_index: std::sync::OnceLock<Vec<u32>>,
    /// The v8 integrity table (None for pre-v8 files, which have
    /// nothing to verify). Open has already checked the table itself
    /// The mapped-plan binding read from the kind-6 table entry.
    binding: Option<StoredBinding>,
    /// and the eagerly-read sections; [`Self::verify_integrity`]
    /// checks everything.
    integrity: Option<IntegrityTable>,
}

impl Bm25Reader {
    /// The mapped-plan binding this file was written under, if any.
    pub fn binding(&self) -> Option<&StoredBinding> {
        self.binding.as_ref()
    }

    /// The next local doc id (number of document slots).
    pub fn next_doc_id(&self) -> u32 {
        self.n_slots() as u32
    }

    /// Verify EVERY recorded section CRC against the mapped bytes —
    /// including the big lazily-paged blobs open skips — and return
    /// `(sections, bytes)` verified. Reads the whole file. A mismatch
    /// is an error naming the section; a pre-v8 file is an error
    /// saying there is nothing to verify, so "unverifiable" can never
    /// be mistaken for "verified".
    pub fn verify_integrity(&self) -> io::Result<(usize, u64)> {
        let Some(table) = &self.integrity else {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "no integrity table: this file predates v8, rebuild to get checksums",
            ));
        };
        let mut bytes = 0u64;
        for e in &table.entries {
            let got = crate::wal::crc32(&self.map[e.off as usize..(e.off + e.len) as usize]);
            if got != e.crc {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "section {} ({} bytes at {}) CRC mismatch: stored {:08x}, computed {got:08x} — the bytes are not the ones the build wrote",
                        e.name, e.len, e.off, e.crc
                    ),
                ));
            }
            bytes += e.len;
        }
        Ok((table.entries.len(), bytes))
    }

    /// Whether this file carries a v8 integrity table at all.
    pub fn has_integrity(&self) -> bool {
        self.integrity.is_some()
    }

    /// The shared slot count (every field's doc-length table has it).
    fn n_slots(&self) -> usize {
        self.fields[0].doc_lengths.len()
    }

    /// Number of fields in the field table.
    pub fn field_count(&self) -> usize {
        self.fields.len()
    }

    /// The name of field `f`. Panics when out of range.
    pub fn field_name(&self, f: usize) -> &str {
        &self.fields[f].name
    }

    /// Field `f`'s analyzer fingerprint (0 = unknown, which never
    /// enforces).
    pub fn analysis_fingerprint(&self, f: usize) -> u64 {
        self.fields[f].analysis_fingerprint
    }

    /// The index of the field named `name`, if the table has it.
    pub fn field_index(&self, name: &str) -> Option<usize> {
        self.fields.iter().position(|f| f.name == name)
    }

    /// Field `f` as its own [`Bm25Index`]: directory lookups, postings
    /// walks, and impact cursors run against the field's sections;
    /// texts, lineages, and the document count are the file's shared
    /// ones (`docs/multi-field.md`). Panics when `f` is out of range.
    pub fn field(&self, f: usize) -> FieldView<'_> {
        FieldView {
            reader: self,
            field: &self.fields[f],
        }
    }

    /// Number of facet fields in the facet table (0 for pre-v7 files).
    pub fn facet_count(&self) -> usize {
        self.facets.len()
    }

    /// The name of facet field `fi`. Panics when out of range.
    pub fn facet_name(&self, fi: usize) -> &str {
        &self.facets[fi].name
    }

    /// The index of the facet field named `name`, if the table has it.
    pub fn facet_index(&self, name: &str) -> Option<usize> {
        self.facets.iter().position(|f| f.name == name)
    }

    /// Number of distinct values facet field `fi` holds.
    pub fn facet_value_count(&self, fi: usize) -> usize {
        self.facets[fi].dict.len()
    }

    /// The value of facet field `fi` at ordinal `ord`. Panics when out
    /// of range.
    pub fn facet_value(&self, fi: usize, ord: u32) -> &str {
        &self.facets[fi].dict[ord as usize]
    }

    /// The ordinal of `value` in facet field `fi`'s dictionary, `None`
    /// when this file never ingested it. A linear dictionary scan, the
    /// reader's [`Self::map_facet_key_ord`] pattern: resolution runs
    /// once per request, not per document, and the on-disk dictionary
    /// is in first-seen order (the sorted layout stays in the back
    /// pocket, `docs/map-columns.md`).
    pub fn facet_value_ord_of(&self, fi: usize, value: &str) -> Option<u32> {
        self.facets[fi]
            .dict
            .iter()
            .position(|v| v == value)
            .map(|p| p as u32)
    }

    /// The ordinal of `doc_id`'s value for facet field `fi`, `None`
    /// when the document has no value. One 4 B read of the mmapped
    /// fixed-stride ords section.
    pub fn facet_ord(&self, fi: usize, doc_id: u32) -> Option<u32> {
        let facet = &self.facets[fi];
        let slot = doc_id as usize;
        if slot >= self.n_slots() {
            return None;
        }
        let off = facet.ords_off as usize + 4 * slot;
        let ord = u32::from_le_bytes(self.map[off..off + 4].try_into().expect("4 bytes"));
        if ord == FACET_ABSENT {
            None
        } else {
            Some(ord)
        }
    }

    /// Number of numeric fields in the numeric table (0 pre-v7).
    pub fn numeric_count(&self) -> usize {
        self.numerics.len()
    }

    /// The name of numeric field `ni`. Panics when out of range.
    pub fn numeric_name(&self, ni: usize) -> &str {
        &self.numerics[ni].name
    }

    /// The index of the numeric field named `name`, if the table has it.
    pub fn numeric_index(&self, name: &str) -> Option<usize> {
        self.numerics.iter().position(|n| n.name == name)
    }

    /// (min, max) of numeric field `ni` over present values, from the
    /// column table's write-time metadata (validated against a full
    /// scan at open); (NaN, NaN) when no document has a value.
    pub fn numeric_min_max(&self, ni: usize) -> (f64, f64) {
        (self.numerics[ni].min, self.numerics[ni].max)
    }

    /// `doc_id`'s value for numeric field `ni`, `None` when absent.
    /// One 8 B read of the mmapped fixed-stride vals section.
    pub fn numeric_value(&self, ni: usize, doc_id: u32) -> Option<f64> {
        let numeric = &self.numerics[ni];
        let slot = doc_id as usize;
        if slot >= self.n_slots() {
            return None;
        }
        let off = numeric.vals_off as usize + 8 * slot;
        let v = f64::from_bits(u64::from_le_bytes(
            self.map[off..off + 8].try_into().expect("8 bytes"),
        ));
        if v.is_nan() {
            None
        } else {
            Some(v)
        }
    }

    /// Number of i64 fields in the integer table (0 pre-v7).
    pub fn integer_count(&self) -> usize {
        self.integers.len()
    }

    /// The name of integer field `ii`. Panics when out of range.
    pub fn integer_name(&self, ii: usize) -> &str {
        &self.integers[ii].name
    }

    /// The index of the integer field named `name`, if the table has it.
    pub fn integer_index(&self, name: &str) -> Option<usize> {
        self.integers.iter().position(|n| n.name == name)
    }

    /// (min, max) of integer field `ii` over present values, from the
    /// column table's write-time metadata (validated against a full
    /// scan at open); the empty range `(i64::MAX, i64::MIN)` when no
    /// document has a value.
    pub fn integer_min_max(&self, ii: usize) -> (i64, i64) {
        (self.integers[ii].min, self.integers[ii].max)
    }

    /// `doc_id`'s value for integer field `ii`, `None` when absent.
    /// One 8 B read of the mmapped fixed-stride vals section.
    pub fn integer_value(&self, ii: usize, doc_id: u32) -> Option<i64> {
        let integer = &self.integers[ii];
        let slot = doc_id as usize;
        if slot >= self.n_slots() {
            return None;
        }
        let off = integer.vals_off as usize + 8 * slot;
        let v = u64::from_le_bytes(self.map[off..off + 8].try_into().expect("8 bytes")) as i64;
        if v == INTEGER_ABSENT {
            None
        } else {
            Some(v)
        }
    }

    /// Number of geo fields in the geo table (0 pre-v7).
    pub fn geo_count(&self) -> usize {
        self.geos.len()
    }

    /// The name of geo field `gi`. Panics when out of range.
    pub fn geo_name(&self, gi: usize) -> &str {
        &self.geos[gi].name
    }

    /// The index of the geo field named `name`, if the table has it.
    pub fn geo_index(&self, name: &str) -> Option<usize> {
        self.geos.iter().position(|n| n.name == name)
    }

    /// Geo field `gi`'s bounding box `(min_lat, max_lat, min_lon,
    /// max_lon)` from the column table's write-time metadata (validated
    /// against a full scan at open); all four NaN when no document has a
    /// point.
    pub fn geo_bbox(&self, gi: usize) -> (f64, f64, f64, f64) {
        self.geos[gi].bbox
    }

    /// `doc_id`'s (lat, lon) for geo field `gi`, `None` when absent.
    /// One 16 B read of the mmapped fixed-stride vals section.
    pub fn geo_value(&self, gi: usize, doc_id: u32) -> Option<(f64, f64)> {
        let geo = &self.geos[gi];
        let slot = doc_id as usize;
        if slot >= self.n_slots() {
            return None;
        }
        let off = geo.vals_off as usize + 16 * slot;
        let lat = f64::from_bits(u64::from_le_bytes(
            self.map[off..off + 8].try_into().expect("8 bytes"),
        ));
        let lon = f64::from_bits(u64::from_le_bytes(
            self.map[off + 8..off + 16].try_into().expect("8 bytes"),
        ));
        // Validation refused half-NaN pairs at open, so testing one half
        // decides both.
        if lat.is_nan() {
            None
        } else {
            Some((lat, lon))
        }
    }

    /// A document's pair range [start, end) in a map column, from the
    /// offsets section's prefix sums.
    fn map_pair_range(&self, offsets_off: u64, doc_id: u32) -> Option<(usize, usize)> {
        let slot = doc_id as usize;
        if slot >= self.n_slots() {
            return None;
        }
        let at = |i: usize| {
            u32::from_le_bytes(
                self.map[offsets_off as usize + 4 * i..offsets_off as usize + 4 * i + 4]
                    .try_into()
                    .expect("4 bytes"),
            ) as usize
        };
        Some((at(slot), at(slot + 1)))
    }

    /// The index of the map-facet column named `name`.
    pub fn map_facet_index(&self, name: &str) -> Option<usize> {
        self.map_facets.iter().position(|c| c.name == name)
    }

    /// The key ordinal of `key` in map-facet column `ci`.
    pub fn map_facet_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        self.map_facets[ci]
            .keys
            .iter()
            .position(|k| k == key)
            .map(|p| p as u32)
    }

    /// Number of distinct values map-facet column `ci` holds.
    pub fn map_facet_value_count(&self, ci: usize) -> usize {
        self.map_facets[ci].values.len()
    }

    /// The value of map-facet column `ci` at ordinal `ord`.
    pub fn map_facet_value(&self, ci: usize, ord: u32) -> &str {
        &self.map_facets[ci].values[ord as usize]
    }

    /// The ordinal of `value` in map-facet column `ci`'s value
    /// dictionary, `None` when never ingested — the linear scan
    /// [`Self::facet_value_ord_of`] explains.
    pub fn map_facet_value_ord_of(&self, ci: usize, value: &str) -> Option<u32> {
        self.map_facets[ci]
            .values
            .iter()
            .position(|v| v == value)
            .map(|p| p as u32)
    }

    /// The value ordinal of `doc_id`'s entry under `key_ord` in
    /// map-facet column `ci`, `None` when absent: two offset reads and
    /// a binary search of the document's (strictly key-ordered) pairs.
    pub fn map_facet_value_ord(&self, ci: usize, key_ord: u32, doc_id: u32) -> Option<u32> {
        let c = &self.map_facets[ci];
        let (start, end) = self.map_pair_range(c.offsets_off, doc_id)?;
        let pair = |i: usize| {
            let off = c.pairs_off as usize + 8 * i;
            (
                u32::from_le_bytes(self.map[off..off + 4].try_into().expect("4 bytes")),
                u32::from_le_bytes(self.map[off + 4..off + 8].try_into().expect("4 bytes")),
            )
        };
        let mut lo = start;
        let mut hi = end;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let (k, v) = pair(mid);
            match k.cmp(&key_ord) {
                std::cmp::Ordering::Equal => return Some(v),
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        None
    }

    /// The index of the map-numeric column named `name`.
    pub fn map_numeric_index(&self, name: &str) -> Option<usize> {
        self.map_numerics.iter().position(|c| c.name == name)
    }

    /// The key ordinal of `key` in map-numeric column `ci`.
    pub fn map_numeric_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        self.map_numerics[ci]
            .keys
            .iter()
            .position(|k| k == key)
            .map(|p| p as u32)
    }

    /// (min, max) of map-numeric column `ci` under `key_ord`, from the
    /// key dictionary's write-time metadata (validated at open).
    pub fn map_numeric_key_min_max(&self, ci: usize, key_ord: u32) -> (f64, f64) {
        self.map_numerics[ci].key_min_max[key_ord as usize]
    }

    /// `doc_id`'s value under `key_ord` in map-numeric column `ci`,
    /// `None` when absent.
    pub fn map_numeric_value(&self, ci: usize, key_ord: u32, doc_id: u32) -> Option<f64> {
        let c = &self.map_numerics[ci];
        let (start, end) = self.map_pair_range(c.offsets_off, doc_id)?;
        let pair = |i: usize| {
            let off = c.pairs_off as usize + 12 * i;
            (
                u32::from_le_bytes(self.map[off..off + 4].try_into().expect("4 bytes")),
                f64::from_bits(u64::from_le_bytes(
                    self.map[off + 4..off + 12].try_into().expect("8 bytes"),
                )),
            )
        };
        let mut lo = start;
        let mut hi = end;
        while lo < hi {
            let mid = lo + (hi - lo) / 2;
            let (k, v) = pair(mid);
            match k.cmp(&key_ord) {
                std::cmp::Ordering::Equal => return Some(v),
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
            }
        }
        None
    }

    /// Number of terms in field 0's directory.
    pub fn term_count(&self) -> u32 {
        self.fields[0].n_terms
    }

    /// The `i`th term of field 0 in directory (sorted) order. Panics
    /// when out of range; verification tooling only.
    pub fn term_at(&self, i: u32) -> String {
        let field = &self.fields[0];
        assert!(i < field.n_terms, "term index {i} out of range");
        let bytes = if self.v5_runs {
            self.directory_entry_v5(field, i).0
        } else {
            self.directory_entry(field, i).0
        };
        String::from_utf8_lossy(bytes).into_owned()
    }

    /// Open a v3/v4/v5/v6 `.bm25` file read-only after full structural
    /// validation (see [`validate_structure`] / [`validate_structure_v6`]
    /// — malformed files error, never panic). Touches only the header,
    /// the doc-length table, the directory, and the skip runs — no
    /// postings or text pages beyond those are faulted in until queries
    /// ask for them.
    pub fn open(path: &Path) -> io::Result<Self> {
        let invalid = |msg: &str| io::Error::new(io::ErrorKind::InvalidData, msg.to_string());
        let file = std::fs::File::open(path)?;
        let map = unsafe { memmap2::MmapOptions::new().map(&file)? };
        if map.len() < 52 {
            return Err(invalid("not a v3/v4/v5/v6/v7/v8 .bm25 file"));
        }
        if &map[..8] == MAGIC_V8 {
            let table = parse_integrity(&map)?;
            // Verify every section open reads anyway; the lazily-paged
            // blobs (texts, per-field postings) wait for
            // `verify_integrity`, because reading 50 GB at open would
            // defeat the mmap paging model.
            for e in table.entries.iter().filter(|e| integrity_eager(&e.name)) {
                let got = crate::wal::crc32(&map[e.off as usize..(e.off + e.len) as usize]);
                if got != e.crc {
                    return Err(invalid(&format!(
                        "section {} CRC mismatch: stored {:08x}, computed {got:08x} — the bytes are not the ones the build wrote",
                        e.name, e.crc
                    )));
                }
            }
            let base_v7 = table.base_v7;
            let payload_len = table.payload_len as usize;
            let mut reader = Self::open_v6v7_bounded(map, base_v7, payload_len)?;
            reader.integrity = Some(table);
            return Ok(reader);
        }
        if &map[..8] == MAGIC_V7 {
            return Self::open_v6v7(map, true);
        }
        if &map[..8] == MAGIC_V6 {
            return Self::open_v6v7(map, false);
        }
        if &map[..8] != MAGIC_V5 && &map[..8] != MAGIC_V4 && &map[..8] != MAGIC_V3 {
            return Err(invalid("not a v3/v4/v5/v6/v7/v8 .bm25 file"));
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
            fields: vec![FieldSlice {
                name: "body".to_string(),
                // v3/v4/v5 carry no field table, so no fingerprint.
                analysis_fingerprint: 0,
                doc_lengths,
                total_length,
                directory_off,
                n_terms,
                run_base: 0,
            }],
            facets: Vec::new(),
            numerics: Vec::new(),
            binding: None,
            map_facets: Vec::new(),
            map_numerics: Vec::new(),
            integers: Vec::new(),
            geos: Vec::new(),
            doc_count,
            lineages_off,
            text_index_off: lineages_off - 12 * n_slots as u64,
            text_base: 0,
            v5_runs: v5,
            blob_relative,
            lineage_index: std::sync::OnceLock::new(),
            integrity: None,
        })
    }

    /// Open a validated v6 or v7 map, one [`FieldSlice`] per
    /// field-table entry (plus one [`FacetSlice`] per v7 facet-table
    /// entry). Every field's sections are v5-shaped, so the entire v5
    /// read machinery serves each field with run offsets rebased by
    /// that field's postings section offset.
    fn open_v6v7(map: memmap2::Mmap, v7: bool) -> io::Result<Self> {
        let len = map.len();
        Self::open_v6v7_bounded(map, v7, len)
    }

    /// [`Self::open_v6v7`] with the payload's byte length made
    /// explicit: a v8 file carries its integrity section and trailer
    /// AFTER the payload, and the structural validator derives the
    /// last section's extent from where the bytes end, so it must see
    /// the payload's end, not the file's.
    fn open_v6v7_bounded(map: memmap2::Mmap, v7: bool, payload_len: usize) -> io::Result<Self> {
        validate_structure_v6(&map[..payload_len], v7)?;
        let u32_at = |off: usize| u32::from_le_bytes(map[off..off + 4].try_into().unwrap());
        let u64_at = |off: usize| u64::from_le_bytes(map[off..off + 8].try_into().unwrap());
        let n_fields = u32_at(8) as usize;
        let n_slots = u32_at(12) as usize;
        let texts_off = u64_at(16);
        let text_index_off = u64_at(24);
        let lineages_off = u64_at(32);
        // Field table walk (validation already checked the geometry and
        // the names).
        let mut fields = Vec::with_capacity(n_fields);
        let mut cursor = 40usize;
        for _ in 0..n_fields {
            let name_len = u16::from_le_bytes(map[cursor..cursor + 2].try_into().unwrap()) as usize;
            let name =
                String::from_utf8_lossy(&map[cursor + 2..cursor + 2 + name_len]).into_owned();
            let base = cursor + 2 + name_len;
            let analysis_fingerprint = u64_at(base);
            let total_length = u64_at(base + 8);
            let doc_lengths_off = u64_at(base + 16) as usize;
            let postings_off = u64_at(base + 24);
            let directory_off = u64_at(base + 32);
            let mut doc_lengths = Vec::with_capacity(n_slots);
            for slot in 0..n_slots {
                doc_lengths.push(u32_at(doc_lengths_off + 4 * slot));
            }
            let n_terms = u32_at(directory_off as usize);
            fields.push(FieldSlice {
                name,
                analysis_fingerprint,
                doc_lengths,
                total_length,
                directory_off,
                n_terms,
                run_base: postings_off,
            });
            cursor = base + 40;
        }
        // Column table (v7 only): decode each facet dictionary eagerly
        // (one entry per distinct value — small) and each numeric
        // column's min/max metadata; ords and vals sections stay in the
        // map. Unknown kinds were already refused by validation.
        let mut facets = Vec::new();
        let mut numerics = Vec::new();
        let mut map_facets = Vec::new();
        let mut map_numerics = Vec::new();
        let mut integers = Vec::new();
        let mut geos = Vec::new();
        let mut binding = None;
        if v7 {
            // Decode a length-prefixed dictionary of `n` entries
            // starting at `off`, returning (entries, end offset).
            let read_dict = |off: usize, n: usize| -> (Vec<String>, usize) {
                let mut entries = Vec::with_capacity(n);
                let mut cur = off;
                for _ in 0..n {
                    let len = u16::from_le_bytes(map[cur..cur + 2].try_into().unwrap()) as usize;
                    entries
                        .push(String::from_utf8_lossy(&map[cur + 2..cur + 2 + len]).into_owned());
                    cur += 2 + len;
                }
                (entries, cur)
            };
            let n_columns = u32_at(cursor) as usize;
            cursor += 4;
            for _ in 0..n_columns {
                let name_len =
                    u16::from_le_bytes(map[cursor..cursor + 2].try_into().unwrap()) as usize;
                let name =
                    String::from_utf8_lossy(&map[cursor + 2..cursor + 2 + name_len]).into_owned();
                let kind = map[cursor + 2 + name_len];
                let base = cursor + 2 + name_len + 1;
                match kind {
                    COLUMN_KIND_FACET => {
                        let n_values = u32_at(base) as usize;
                        let dict_off = u64_at(base + 4) as usize;
                        let ords_off = u64_at(base + 12);
                        let (dict, _) = read_dict(dict_off, n_values);
                        facets.push(FacetSlice {
                            name,
                            dict,
                            ords_off,
                        });
                        cursor = base + 20;
                    }
                    COLUMN_KIND_F64 => {
                        numerics.push(NumericSlice {
                            name,
                            min: f64::from_bits(u64_at(base)),
                            max: f64::from_bits(u64_at(base + 8)),
                            vals_off: u64_at(base + 16),
                        });
                        cursor = base + 24;
                    }
                    COLUMN_KIND_MAP_FACET => {
                        let n_keys = u32_at(base) as usize;
                        let n_values = u32_at(base + 4) as usize;
                        let keys_off = u64_at(base + 8) as usize;
                        let values_off = u64_at(base + 16) as usize;
                        let offsets_off = u64_at(base + 24);
                        let pairs_off = u64_at(base + 32);
                        let (keys, _) = read_dict(keys_off, n_keys);
                        let (values, _) = read_dict(values_off, n_values);
                        map_facets.push(MapFacetSlice {
                            name,
                            keys,
                            values,
                            offsets_off,
                            pairs_off,
                        });
                        cursor = base + 40;
                    }
                    COLUMN_KIND_MAP_F64 => {
                        let n_keys = u32_at(base) as usize;
                        let keys_off = u64_at(base + 4) as usize;
                        let offsets_off = u64_at(base + 12);
                        let pairs_off = u64_at(base + 20);
                        // Key entries interleave name and min/max
                        // metadata, so read_dict does not apply.
                        let mut keys = Vec::with_capacity(n_keys);
                        let mut key_min_max = Vec::with_capacity(n_keys);
                        let mut cur = keys_off;
                        for _ in 0..n_keys {
                            let len =
                                u16::from_le_bytes(map[cur..cur + 2].try_into().unwrap()) as usize;
                            keys.push(
                                String::from_utf8_lossy(&map[cur + 2..cur + 2 + len]).into_owned(),
                            );
                            key_min_max.push((
                                f64::from_bits(u64_at(cur + 2 + len)),
                                f64::from_bits(u64_at(cur + 2 + len + 8)),
                            ));
                            cur += 2 + len + 16;
                        }
                        map_numerics.push(MapNumericSlice {
                            name,
                            keys,
                            key_min_max,
                            offsets_off,
                            pairs_off,
                        });
                        cursor = base + 28;
                    }
                    COLUMN_KIND_I64 => {
                        integers.push(IntegerSlice {
                            name,
                            min: u64_at(base) as i64,
                            max: u64_at(base + 8) as i64,
                            vals_off: u64_at(base + 16),
                        });
                        cursor = base + 24;
                    }
                    COLUMN_KIND_GEO => {
                        geos.push(GeoSlice {
                            name,
                            bbox: (
                                f64::from_bits(u64_at(base)),
                                f64::from_bits(u64_at(base + 8)),
                                f64::from_bits(u64_at(base + 16)),
                                f64::from_bits(u64_at(base + 24)),
                            ),
                            vals_off: u64_at(base + 32),
                        });
                        cursor = base + 40;
                    }
                    COLUMN_KIND_BINDING => {
                        let mut vals: Vec<String> = Vec::with_capacity(3);
                        let mut cur = base;
                        for _ in 0..3 {
                            let len =
                                u16::from_le_bytes(map[cur..cur + 2].try_into().unwrap()) as usize;
                            vals.push(
                                String::from_utf8_lossy(&map[cur + 2..cur + 2 + len]).into_owned(),
                            );
                            cur += 2 + len;
                        }
                        let mut it = vals.into_iter();
                        binding = Some(StoredBinding {
                            plan_fingerprint: it.next().expect("three strings"),
                            body_path: it.next().expect("three strings"),
                            materialize_sha: it.next().expect("three strings"),
                        });
                        cursor = cur;
                    }
                    k => unreachable!("validation refused unknown column kind {k}"),
                }
            }
        }
        let doc_count = (0..n_slots)
            .filter(|&slot| fields.iter().any(|f| f.doc_lengths[slot] > 0))
            .count() as u64;
        Ok(Self {
            map,
            fields,
            facets,
            numerics,
            map_facets,
            map_numerics,
            integers,
            geos,
            doc_count,
            lineages_off,
            text_index_off,
            text_base: texts_off,
            v5_runs: true,
            blob_relative: true,
            binding,
            lineage_index: std::sync::OnceLock::new(),
            integrity: None,
        })
    }

    fn directory_entry(&self, field: &FieldSlice, i: u32) -> (&[u8], u64, u32) {
        let e = field.directory_off as usize + 4 + 18 * i as usize;
        let postings_off = u64::from_le_bytes(self.map[e..e + 8].try_into().unwrap());
        let df = u32::from_le_bytes(self.map[e + 8..e + 12].try_into().unwrap());
        let stored = u32::from_le_bytes(self.map[e + 12..e + 16].try_into().unwrap()) as usize;
        let len = u16::from_le_bytes(self.map[e + 16..e + 18].try_into().unwrap()) as usize;
        // v4 stores offsets relative to the term blob; v3 stored absolute
        // file offsets (only valid below 4 GiB).
        let blob_off = if self.blob_relative {
            field.directory_off as usize + 4 + 18 * field.n_terms as usize + stored
        } else {
            stored
        };
        (&self.map[blob_off..blob_off + len], postings_off, df)
    }

    /// The 34 B v5-shaped directory entry: `(term bytes, doc_run_off,
    /// skip_run_off, occ_run_off, df)`. Run offsets are rebased by the
    /// field's `run_base` (0 on v5 files, where entries are absolute;
    /// the field's postings section offset on v6, where they are
    /// section-relative), so callers always see absolute offsets.
    fn directory_entry_v5(&self, field: &FieldSlice, i: u32) -> (&[u8], u64, u64, u64, u32) {
        let e = field.directory_off as usize + 4 + 34 * i as usize;
        let doc_run_off =
            u64::from_le_bytes(self.map[e..e + 8].try_into().unwrap()) + field.run_base;
        let skip_run_off =
            u64::from_le_bytes(self.map[e + 8..e + 16].try_into().unwrap()) + field.run_base;
        let occ_run_off =
            u64::from_le_bytes(self.map[e + 16..e + 24].try_into().unwrap()) + field.run_base;
        let df = u32::from_le_bytes(self.map[e + 24..e + 28].try_into().unwrap());
        let stored = u32::from_le_bytes(self.map[e + 28..e + 32].try_into().unwrap()) as usize;
        let len = u16::from_le_bytes(self.map[e + 32..e + 34].try_into().unwrap()) as usize;
        let blob_off = field.directory_off as usize + 4 + 34 * field.n_terms as usize + stored;
        (
            &self.map[blob_off..blob_off + len],
            doc_run_off,
            skip_run_off,
            occ_run_off,
            df,
        )
    }

    fn directory_lookup(&self, field: &FieldSlice, term: &str) -> Option<(u64, u32)> {
        let (mut lo, mut hi) = (0u32, field.n_terms);
        while lo < hi {
            let mid = (lo + hi) / 2;
            let (bytes, _, _) = self.directory_entry(field, mid);
            match bytes.cmp(term.as_bytes()) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => {
                    let (_, off, df) = self.directory_entry(field, mid);
                    return Some((off, df));
                }
            }
        }
        None
    }

    /// `(doc_run_off, skip_run_off, occ_run_off, df)` for `term` in
    /// `field`, or `None` when the term is absent. v5-shaped files only.
    fn directory_lookup_v5(&self, field: &FieldSlice, term: &str) -> Option<(u64, u64, u64, u32)> {
        let (mut lo, mut hi) = (0u32, field.n_terms);
        while lo < hi {
            let mid = (lo + hi) / 2;
            let (bytes, _, _, _, _) = self.directory_entry_v5(field, mid);
            match bytes.cmp(term.as_bytes()) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => {
                    let (_, doc, skip, occ, df) = self.directory_entry_v5(field, mid);
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
        (self.u32_at(e), self.u32_at(e + 4), self.u32_at(e + 8))
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

impl Bm25Reader {
    /// The shared stored text of a document, if present.
    fn read_text(&self, doc_id: u32) -> Option<String> {
        let slot = doc_id as usize;
        if slot >= self.n_slots() {
            return None;
        }
        // The on-disk text index: 12-byte entries; offsets absolute in
        // v3/v4/v5, texts-section-relative in v6 (text_base rebases).
        let e = (self.text_index_off + 12 * doc_id as u64) as usize;
        let offset = u64::from_le_bytes(self.map[e..e + 8].try_into().unwrap()) + self.text_base;
        let len = u32::from_le_bytes(self.map[e + 8..e + 12].try_into().unwrap());
        if len == u32::MAX {
            return None;
        }
        let bytes = &self.map[offset as usize..offset as usize + len as usize];
        String::from_utf8(bytes.to_vec()).ok()
    }

    /// The shared lineage of a document, if ingested with one.
    fn read_lineage(&self, doc_id: u32) -> Option<DocLineage> {
        let slot = doc_id as usize;
        if slot >= self.n_slots() {
            return None;
        }
        // The lineage section is variable stride (1 B absent marker,
        // 25 B present), so a fixed 25B-stride read lands anywhere —
        // correct only for dense all-present lineages. The lazily built
        // per-slot offset index makes random access exact.
        let index = self.lineage_index.get_or_init(|| {
            let base = self.lineages_off as usize;
            let mut offsets = Vec::with_capacity(self.n_slots());
            let mut cur = base;
            for _ in 0..self.n_slots() {
                offsets.push((cur - base) as u32);
                cur += if self.map[cur] == 0 { 1 } else { 25 };
            }
            offsets
        });
        let e = self.lineages_off as usize + index[slot] as usize;
        if self.map[e] == 0 {
            return None;
        }
        let parent_id = u64::from_le_bytes(self.map[e + 1..e + 9].try_into().unwrap());
        let group_id = u64::from_le_bytes(self.map[e + 9..e + 17].try_into().unwrap());
        let span_start = u32::from_le_bytes(self.map[e + 17..e + 21].try_into().unwrap());
        let span_end = u32::from_le_bytes(self.map[e + 21..e + 25].try_into().unwrap());
        Some(DocLineage {
            parent_id,
            group_id,
            span_start,
            span_end,
        })
    }
}

/// One field of an open `.bm25` file as its own [`Bm25Index`]
/// (`docs/multi-field.md`): directory lookups, postings walks, and
/// impact cursors run against the field's sections; texts, lineages,
/// and the document count are the file's shared ones.
#[derive(Clone, Copy)]
pub struct FieldView<'a> {
    reader: &'a Bm25Reader,
    field: &'a FieldSlice,
}

impl<'a> FieldView<'a> {
    /// [`Bm25Index::impacts`] with the cursor's borrow tied to the
    /// underlying reader rather than this view value, so a temporary
    /// view (`reader.field(0).impacts(..)`) can hand out a cursor.
    fn impacts_inner(self, term: &str) -> Option<ImpactCursor<'a>> {
        if !self.reader.v5_runs {
            return None;
        }
        let (doc_run_off, skip_run_off, occ_run_off, df) =
            self.reader.directory_lookup_v5(self.field, term)?;
        Some(ImpactCursor::new(
            &self.reader.map,
            doc_run_off as usize,
            occ_run_off as usize,
            skip_run_off as usize,
            df,
        ))
    }
}

impl Bm25Index for FieldView<'_> {
    fn doc_count(&self) -> u64 {
        self.reader.doc_count
    }
    fn total_doc_length(&self) -> u64 {
        self.field.total_length
    }
    fn doc_length(&self, doc_id: u32) -> u32 {
        self.field
            .doc_lengths
            .get(doc_id as usize)
            .copied()
            .unwrap_or(0)
    }
    fn df(&self, term: &str) -> u32 {
        if self.reader.v5_runs {
            self.reader
                .directory_lookup_v5(self.field, term)
                .map_or(0, |(_, _, _, df)| df)
        } else {
            self.reader
                .directory_lookup(self.field, term)
                .map_or(0, |(_, df)| df)
        }
    }
    fn for_each_posting(&self, term: &str, f: &mut PostingCallback) {
        let r = self.reader;
        if r.v5_runs {
            let Some((doc_run_off, _, occ_run_off, df)) = r.directory_lookup_v5(self.field, term)
            else {
                return;
            };
            let (doc_run_off, occ_run_off) = (doc_run_off as usize, occ_run_off as usize);
            for i in 0..df as usize {
                let (doc_id, tf, occ_start) = r.v5_doc_entry(doc_run_off, i);
                let occ_end = r.v5_occ_start(doc_run_off, df as usize, i + 1);
                let offsets = r.v5_occ_slice(occ_run_off, occ_start, occ_end);
                f(doc_id, tf, &offsets);
            }
        } else {
            let Some((off, df)) = r.directory_lookup(self.field, term) else {
                return;
            };
            r.v3_for_each_posting(off, df, f);
        }
    }
    fn for_each_doc_tf(&self, term: &str, f: &mut dyn FnMut(u32, u32)) {
        let r = self.reader;
        if r.v5_runs {
            let Some((doc_run_off, _, _, df)) = r.directory_lookup_v5(self.field, term) else {
                return;
            };
            r.v5_for_each_doc_tf(doc_run_off as usize, df, f);
        } else {
            self.for_each_posting(term, &mut |doc_id, tf, _offsets| {
                f(doc_id, tf);
            });
        }
    }
    fn posting_offsets(&self, term: &str, doc_id: u32) -> Vec<(u32, u32)> {
        let r = self.reader;
        if r.v5_runs {
            let Some((doc_run_off, _, occ_run_off, df)) = r.directory_lookup_v5(self.field, term)
            else {
                return Vec::new();
            };
            // Binary search the fixed-stride doc run (doc ids ascending).
            let doc_run_off = doc_run_off as usize;
            let (mut lo, mut hi) = (0usize, df as usize);
            while lo < hi {
                let mid = (lo + hi) / 2;
                if r.u32_at(doc_run_off + 12 * mid) < doc_id {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            let i = lo;
            if i >= df as usize || r.u32_at(doc_run_off + 12 * i) != doc_id {
                return Vec::new();
            }
            let occ_start = r.u32_at(doc_run_off + 12 * i + 8);
            let occ_end = r.v5_occ_start(doc_run_off, df as usize, i + 1);
            r.v5_occ_slice(occ_run_off as usize, occ_start, occ_end)
        } else {
            // v3/v4: sequential walk with early exit — postings are
            // doc-id-ordered by construction, so stop at the first doc
            // past the target; offset bytes before it are stepped over
            // undecoded. (The trait default walks the whole list, which
            // costs O(df) per survivor at k=1000.)
            let Some((off, df)) = r.directory_lookup(self.field, term) else {
                return Vec::new();
            };
            let mut cur = off as usize;
            let term_len = u32::from_le_bytes(r.map[cur..cur + 4].try_into().unwrap()) as usize;
            cur += 4 + term_len + 4; // term header + posting count
            for _ in 0..df {
                let doc = r.u32_at(cur);
                let n_offsets = r.u32_at(cur + 8) as usize;
                if doc == doc_id {
                    let mut offsets = Vec::with_capacity(n_offsets);
                    let mut o = cur + 12;
                    for _ in 0..n_offsets {
                        offsets.push((r.u32_at(o), r.u32_at(o + 4)));
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
        (*self).impacts_inner(term)
    }
    fn has_impacts(&self, term: &str) -> bool {
        self.reader.v5_runs && self.reader.directory_lookup_v5(self.field, term).is_some()
    }
    fn text(&self, doc_id: u32) -> Option<String> {
        self.reader.read_text(doc_id)
    }
    fn lineage(&self, doc_id: u32) -> Option<DocLineage> {
        self.reader.read_lineage(doc_id)
    }
}

/// The reader itself scores as its body field (field 0) — the surface
/// every single-field caller uses; multi-field scoring goes through
/// [`Bm25Reader::field`].
impl Bm25Index for Bm25Reader {
    fn doc_count(&self) -> u64 {
        self.doc_count
    }
    fn total_doc_length(&self) -> u64 {
        self.field(0).total_doc_length()
    }
    fn doc_length(&self, doc_id: u32) -> u32 {
        self.field(0).doc_length(doc_id)
    }
    fn df(&self, term: &str) -> u32 {
        self.field(0).df(term)
    }
    fn for_each_posting(&self, term: &str, f: &mut PostingCallback) {
        self.field(0).for_each_posting(term, f);
    }
    fn for_each_doc_tf(&self, term: &str, f: &mut dyn FnMut(u32, u32)) {
        self.field(0).for_each_doc_tf(term, f);
    }
    fn posting_offsets(&self, term: &str, doc_id: u32) -> Vec<(u32, u32)> {
        self.field(0).posting_offsets(term, doc_id)
    }
    fn impacts(&self, term: &str) -> Option<ImpactCursor<'_>> {
        self.field(0).impacts_inner(term)
    }
    fn has_impacts(&self, term: &str) -> bool {
        self.field(0).has_impacts(term)
    }
    fn text(&self, doc_id: u32) -> Option<String> {
        self.read_text(doc_id)
    }
    fn lineage(&self, doc_id: u32) -> Option<DocLineage> {
        self.read_lineage(doc_id)
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
                parent_id: u64::from(i) * 17,
                group_id: u64::from(i) * 31,
                span_start: i,
                span_end: i + 100,
            });
            docs.push((
                id,
                format!("document {i} body text"),
                AnalyzedDoc::body(terms, length),
                lineage,
            ));
        }
        docs
    }

    /// A small store for the exhaustive v8 damage sweeps: big enough
    /// to have every section populated, small enough that flipping
    /// every byte stays cheap.
    fn v8_store(columns: bool) -> Bm25Store {
        let mut store = if columns {
            Bm25Store::new()
                .with_facets(&["court"])
                .with_integers(&["cited"])
                .with_geos(&["place"])
        } else {
            Bm25Store::new()
        };
        for (id, text, doc, lineage) in synthetic_corpus().into_iter().take(20) {
            store.add_document_with_lineage(id, text, doc, lineage);
            if columns {
                store.set_facet(0, id, if id % 2 == 0 { "ca9" } else { "scotus" });
                store.set_integer(0, id, i64::from(id) * 3);
                store.set_geo(0, id, 40.0 + f64::from(id) * 0.1, -74.0);
            }
        }
        store
    }

    #[test]
    fn v8_roundtrip_serves_and_deep_verifies() {
        for columns in [false, true] {
            let dir = std::env::temp_dir().join(format!("v8-rt-{columns}-{}", std::process::id()));
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
            let path = dir.join("x.bm25");
            let store = v8_store(columns);
            store.save(&path).unwrap();
            assert_eq!(&std::fs::read(&path).unwrap()[..8], MAGIC_V8);

            let reader = Bm25Reader::open(&path).unwrap();
            assert!(reader.has_integrity());
            assert_eq!(Bm25Index::doc_count(&reader), store.doc_count());
            let (sections, bytes) = reader.verify_integrity().unwrap();
            let table = reader.integrity.as_ref().unwrap();
            assert_eq!(sections, table.entries.len());
            assert_eq!(
                bytes, table.payload_len,
                "entries must cover the whole payload"
            );
            assert_eq!(table.base_v7, columns);
            // The store loader takes the same file back.
            Bm25Store::load(&path).unwrap();
            std::fs::remove_dir_all(&dir).ok();
        }
    }

    #[test]
    fn v8_bit_flip_anywhere_is_caught_by_open_or_deep_verify() {
        let dir = std::env::temp_dir().join(format!("v8-flip-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("x.bm25");
        v8_store(true).save(&path).unwrap();
        let intact = std::fs::read(&path).unwrap();

        let victim = dir.join("flipped.bm25");
        for i in 0..intact.len() {
            let mut bytes = intact.clone();
            bytes[i] ^= 0x01;
            std::fs::write(&victim, &bytes).unwrap();
            match Bm25Reader::open(&victim) {
                Err(_) => {}
                Ok(reader) => {
                    let err = reader.verify_integrity().expect_err(&format!(
                        "flip at byte {i}: open accepted it and deep verify found nothing"
                    ));
                    assert!(
                        err.to_string().contains("CRC mismatch"),
                        "flip at byte {i}: unexpected deep-verify error: {err}"
                    );
                }
            }
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn v8_truncation_anywhere_refuses_open() {
        let dir = std::env::temp_dir().join(format!("v8-cut-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("x.bm25");
        v8_store(false).save(&path).unwrap();
        let intact = std::fs::read(&path).unwrap();

        let victim = dir.join("cut.bm25");
        for len in 0..intact.len() {
            std::fs::write(&victim, &intact[..len]).unwrap();
            assert!(
                Bm25Reader::open(&victim).is_err(),
                "a v8 file cut to {len} of {} bytes must refuse to open, \
                 never demote to an integrity-less file",
                intact.len()
            );
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn v8_deep_verify_names_the_rotted_section() {
        let dir = std::env::temp_dir().join(format!("v8-name-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("x.bm25");
        v8_store(false).save(&path).unwrap();

        let reader = Bm25Reader::open(&path).unwrap();
        let e = reader
            .integrity
            .as_ref()
            .unwrap()
            .entries
            .iter()
            .find(|e| e.name == "field:body:postings")
            .expect("body postings section exists")
            .clone();
        drop(reader);
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[(e.off + e.len / 2) as usize] ^= 0x01;
        std::fs::write(&path, &bytes).unwrap();

        // Postings are not eagerly verified, so open succeeds; the
        // deep verify must fail NAMING the section.
        let reader = Bm25Reader::open(&path).unwrap();
        let err = reader.verify_integrity().unwrap_err();
        assert!(
            err.to_string().contains("field:body:postings"),
            "error must name the rotted section: {err}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn pre_v8_files_open_but_report_nothing_to_verify() {
        let dir = std::env::temp_dir().join(format!("v8-old-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("x.bm25");
        // A raw v6 payload, no integrity pass: what every pre-v8 build
        // on disk looks like.
        let mut bytes = Vec::new();
        v8_store(false).write_v6_to(&mut bytes).unwrap();
        assert_eq!(&bytes[..8], MAGIC_V6);
        std::fs::write(&path, &bytes).unwrap();

        let reader = Bm25Reader::open(&path).unwrap();
        assert!(!reader.has_integrity());
        let err = reader.verify_integrity().unwrap_err();
        assert_eq!(err.kind(), io::ErrorKind::Unsupported);
        assert!(err.to_string().contains("predates v8"), "{err}");
        std::fs::remove_dir_all(&dir).ok();
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

    /// The dual-writer contract on a MULTI-FIELD corpus: the spill
    /// builder's v6 file is byte-identical to the store writer's, with
    /// a tiny buffer forcing per-field multi-run merges.
    #[test]
    fn spill_builder_v6_multi_field_byte_identical() {
        let base = std::env::temp_dir().join(format!("spill-eq-v6mf-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&base);
        std::fs::create_dir_all(&base).unwrap();

        let mut store = Bm25Store::with_fields(&["body", "case_name"]);
        let mut builder =
            SpillBuilder::create_with_fields(&base.join("build"), &["body", "case_name"])
                .unwrap()
                .with_buffer_bytes(128);
        for (id, text, doc, lineage) in two_field_corpus() {
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
        assert!(
            a == b,
            "multi-field spill output is not byte-identical to the store"
        );
        assert!(!base.join("build").exists());

        // And the reader serves both fields.
        let reader = Bm25Reader::open(&spill_path).unwrap();
        assert_eq!(reader.field_count(), 2);
        assert_eq!(reader.field(1).df("smith"), store.field(1).df("smith"));
        assert_eq!(reader.field(0).df("t1"), store.field(0).df("t1"));
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
        AnalyzedDoc::body(
            vec![
                ("rust".to_string(), 2, vec![(0, 4), (10, 14)]),
                ("search".to_string(), 1, vec![(5, 11)]),
            ],
            3,
        )
    }

    fn doc_b() -> AnalyzedDoc {
        AnalyzedDoc::body(
            vec![
                ("rust".to_string(), 1, vec![(0, 4)]),
                ("vector".to_string(), 2, vec![(5, 11), (12, 18)]),
            ],
            3,
        )
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
            AnalyzedDoc::body(vec![("hello".to_string(), 1, vec![(0, 5)])], 2),
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
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target/tmp")
            .join(format!("tvbm25_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("shard.tv.bm25");

        let mut store = Bm25Store::new();
        store.add_document(0, "rust search rust".to_string(), doc_a());
        store.add_document(2, "rust vector vector".to_string(), doc_b());
        store.save(&path).unwrap();

        let loaded = Bm25Store::load(&path).unwrap();
        assert_eq!(loaded.fields[0].doc_lengths, store.fields[0].doc_lengths);
        assert_eq!(loaded.fields[0].total_length, store.fields[0].total_length);
        assert_eq!(loaded.texts, store.texts);
        assert_eq!(loaded.fields[0].postings, store.fields[0].postings);

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
            parent_id: 1000 + u64::from(i),
            group_id: 2000 + u64::from(i),
            span_start: i * 7,
            span_end: i * 7 + 90,
        };
        // Gap slots at 0..1, then a mix of present and missing lineages.
        store.add_document_with_lineage(
            2,
            "a".to_string(),
            AnalyzedDoc::body(vec![("rust".into(), 1, vec![(0, 4)])], 1),
            Some(lineage(2)),
        );
        store.add_document_with_lineage(
            3,
            "b".to_string(),
            AnalyzedDoc::body(vec![("rust".into(), 2, vec![(0, 4), (6, 10)])], 2),
            None,
        );
        store.add_document_with_lineage(
            7,
            "c".to_string(),
            AnalyzedDoc::body(vec![("search".into(), 1, vec![(0, 6)])], 1),
            Some(lineage(7)),
        );
        store.save(&path).unwrap();
        let reader = Bm25Reader::open(&path).unwrap();
        for slot in 0..store.next_doc_id() {
            assert_eq!(reader.lineage(slot), store.lineage(slot), "lineage({slot})");
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
                AnalyzedDoc::body(vec![("court".to_string(), 1, vec![(i, i + 3)])], 5),
            );
        }
        {
            let mut w = io::BufWriter::new(std::fs::File::create(&path).unwrap());
            store.write_v4_for_bench(&mut w).unwrap();
            w.flush().unwrap();
        }
        let reader = Bm25Reader::open(&path).unwrap();
        assert!(!reader.v5_runs, "expected the v4 path");
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
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target/tmp")
            .join(format!("tvbm25_bad_{}", std::process::id()));
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
    fn random_corpus(
        rng: &mut Lcg,
        n_docs: u64,
        vocab: &[String],
    ) -> Vec<(u32, String, AnalyzedDoc)> {
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
            docs.push((id, format!("doc {id}"), AnalyzedDoc::body(terms, length)));
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
        (0..n.max(1))
            .map(|i| format!("t{}", i + rng.below(1)))
            .collect()
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
        store.save_v5(&path).unwrap();
        let reader = Bm25Reader::open(&path).unwrap();

        assert_eq!(Bm25Index::doc_count(&reader), store.doc_count());
        assert_eq!(reader.total_doc_length(), store.total_doc_length());
        assert_eq!(reader.next_doc_id(), store.next_doc_id());
        for term in [
            "court",
            "plaintiff",
            "rust",
            "search",
            "vector",
            "quant",
            "missing",
        ] {
            assert_eq!(reader.df(term), store.df(term), "df({term})");
            // for_each_posting: identical (doc, tf, offsets) stream.
            let mut got = Vec::new();
            reader.for_each_posting(term, &mut |d, tf, o| got.push((d, tf, o.to_vec())));
            let want: Vec<(u32, u32, Vec<(u32, u32)>)> = store
                .postings(term)
                .map(|ps| {
                    ps.iter()
                        .map(|p| (p.doc_id, p.tf, p.offsets.clone()))
                        .collect()
                })
                .unwrap_or_default();
            assert_eq!(got, want, "for_each_posting({term})");
            // for_each_doc_tf: identical (doc, tf) stream.
            let mut got_tf = Vec::new();
            reader.for_each_doc_tf(term, &mut |d, tf| got_tf.push((d, tf)));
            let want_tf: Vec<(u32, u32)> = want.iter().map(|(d, tf, _)| (*d, *tf)).collect();
            assert_eq!(got_tf, want_tf, "for_each_doc_tf({term})");
            // posting_offsets: exact per (term, doc).
            for (d, _, offs) in &want {
                assert_eq!(
                    &reader.posting_offsets(term, *d),
                    offs,
                    "posting_offsets({term}, {d})"
                );
            }
            assert!(reader.posting_offsets(term, u32::MAX).is_empty());
        }
        // text / lineage / doc_length (lineage exercises the lazily
        // built offset index: this corpus has gap slots and missing
        // lineages).
        for (id, text, _, lineage) in &corpus {
            assert_eq!(
                reader.text(*id).as_deref(),
                Some(text.as_str()),
                "text({id})"
            );
            assert_eq!(reader.lineage(*id), *lineage, "lineage({id})");
        }

        // Scoring surface: top_k and score_candidates identical to heap.
        let terms = vec![
            "court".to_string(),
            "rust".to_string(),
            "missing".to_string(),
        ];
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
        assert_eq!(loaded.fields[0].postings, store.fields[0].postings);
        assert_eq!(loaded.fields[0].doc_lengths, store.fields[0].doc_lengths);
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
            store.save_v5(&v5_path).unwrap();
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
                let terms = if terms.is_empty() {
                    vec![voc[0].clone()]
                } else {
                    terms
                };
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
                let oracle = sig(&crate::bm25::top_k_exhaustive(
                    &store, &terms, &stats, params, k,
                ));
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
                let Some((_, skip_off, _, df)) =
                    reader.directory_lookup_v5(&reader.fields[0], term)
                else {
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
                AnalyzedDoc::body(
                    vec![("stair".to_string(), 1 + i % 150, vec![(i, i + 2)])],
                    100 + i,
                ),
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
                AnalyzedDoc::body(
                    vec![(
                        "court".to_string(),
                        tf,
                        (0..tf).map(|o| (o * 10 + i, o * 10 + i + 4)).collect(),
                    )],
                    tf,
                ),
            );
        }
        store.save(&path).unwrap();
        let reader = Bm25Reader::open(&path).unwrap();
        let (_, skip_off, occ_off, _) = reader
            .directory_lookup_v5(&reader.fields[0], "court")
            .unwrap();

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

    // --- v6 (TVBM2506) tests -------------------------------------------

    fn synthetic_store() -> Bm25Store {
        let mut store = Bm25Store::new();
        for (id, text, doc, lineage) in synthetic_corpus() {
            store.add_document_with_lineage(id, text, doc, lineage);
        }
        store
    }

    /// The increment-1 parity contract (`docs/multi-field.md` build
    /// order step 1): a single-field v6 file carries the SAME section
    /// bytes as the v5 file of the same corpus. doc_lengths, texts,
    /// lineages, and postings are byte-identical; text_index entries
    /// and directory run offsets differ only by their documented
    /// rebasing (v6 stores them section-relative).
    #[test]
    fn v6_single_field_sections_match_v5() {
        let store = synthetic_store();
        let mut v5 = Vec::new();
        store.write_to(&mut v5).unwrap();
        let mut v6 = Vec::new();
        store.write_v6_to(&mut v6).unwrap();

        let u32le = |b: &[u8], o: usize| u32::from_le_bytes(b[o..o + 4].try_into().unwrap());
        let u64le = |b: &[u8], o: usize| u64::from_le_bytes(b[o..o + 8].try_into().unwrap());

        // v5 geometry from its fixed header.
        let v5_texts_off = u64le(&v5, 16) as usize;
        let v5_lineages_off = u64le(&v5, 24) as usize;
        let v5_postings_off = u64le(&v5, 32) as usize;
        let v5_directory_off = u64le(&v5, 40) as usize;
        let n_slots = u32le(&v5, 48) as usize;
        let v5_text_index_off = v5_lineages_off - 12 * n_slots;

        // v6 geometry from its section table.
        assert_eq!(&v6[..8], b"TVBM2506");
        assert_eq!(u32le(&v6, 8), 1, "n_fields");
        assert_eq!(u32le(&v6, 12) as usize, n_slots, "n_slots");
        let t_off = u64le(&v6, 16) as usize;
        let ti_off = u64le(&v6, 24) as usize;
        let l_off = u64le(&v6, 32) as usize;
        let name_len = u16::from_le_bytes(v6[40..42].try_into().unwrap()) as usize;
        assert_eq!(&v6[42..42 + name_len], b"body");
        let fe = 42 + name_len;
        assert_eq!(u64le(&v6, fe), 0, "analysis fingerprint placeholder");
        assert_eq!(u64le(&v6, fe + 8), u64le(&v5, 8), "total_length");
        let dl_off = u64le(&v6, fe + 16) as usize;
        let p_off = u64le(&v6, fe + 24) as usize;
        let d_off = u64le(&v6, fe + 32) as usize;

        // Byte-identical sections.
        assert_eq!(
            &v6[dl_off..dl_off + 4 * n_slots],
            &v5[52..v5_texts_off],
            "doc_lengths section"
        );
        assert_eq!(
            &v6[t_off..ti_off],
            &v5[v5_texts_off..v5_text_index_off],
            "texts section"
        );
        assert_eq!(
            &v6[l_off..dl_off],
            &v5[v5_lineages_off..v5_postings_off],
            "lineages section"
        );
        assert_eq!(
            &v6[p_off..d_off],
            &v5[v5_postings_off..v5_directory_off],
            "postings section"
        );

        // text_index: identical entries after the documented rebase.
        for slot in 0..n_slots {
            let a = v5_text_index_off + 12 * slot;
            let b = ti_off + 12 * slot;
            assert_eq!(
                u32le(&v5, a + 8),
                u32le(&v6, b + 8),
                "text len, slot {slot}"
            );
            if u32le(&v5, a + 8) != u32::MAX {
                assert_eq!(
                    u64le(&v5, a) - v5_texts_off as u64,
                    u64le(&v6, b),
                    "text offset, slot {slot}"
                );
            } else {
                assert_eq!(u64le(&v6, b), 0, "absent marker, slot {slot}");
            }
        }

        // directory: identical entries after the documented rebase,
        // identical term blob, both files fully tiled.
        let n_terms = u32le(&v5, v5_directory_off) as usize;
        assert_eq!(u32le(&v6, d_off) as usize, n_terms, "n_terms");
        for i in 0..n_terms {
            let a = v5_directory_off + 4 + 34 * i;
            let b = d_off + 4 + 34 * i;
            for run in 0..3 {
                assert_eq!(
                    u64le(&v5, a + 8 * run) - v5_postings_off as u64,
                    u64le(&v6, b + 8 * run),
                    "run offset {run}, term {i}"
                );
            }
            assert_eq!(
                &v5[a + 24..a + 34],
                &v6[b + 24..b + 34],
                "df/blob/len, term {i}"
            );
        }
        let blob_a = v5_directory_off + 4 + 34 * n_terms;
        let blob_b = d_off + 4 + 34 * n_terms;
        assert_eq!(v5.len() - blob_a, v6.len() - blob_b, "blob sizes");
        assert_eq!(&v5[blob_a..], &v6[blob_b..], "term blob");
    }

    /// v6 round trip: the reader serves exactly what the heap store
    /// holds through every trait method, the block-max surface is
    /// present, and `Bm25Store::load` (the shard append path)
    /// reproduces the store exactly.
    #[test]
    fn v6_round_trip_matches_heap_store() {
        let dir = test_dir("v6rt");
        let path = dir.join("shard.bm25");
        let store = synthetic_store();
        store.save(&path).unwrap();
        let reader = Bm25Reader::open(&path).unwrap();

        assert_eq!(Bm25Index::doc_count(&reader), store.doc_count());
        assert_eq!(reader.total_doc_length(), store.total_doc_length());
        assert_eq!(reader.next_doc_id(), store.next_doc_id());
        for term in [
            "court",
            "plaintiff",
            "rust",
            "search",
            "vector",
            "quant",
            "missing",
        ] {
            assert_eq!(reader.df(term), store.df(term), "df({term})");
            let mut got = Vec::new();
            reader.for_each_posting(term, &mut |d, tf, o| got.push((d, tf, o.to_vec())));
            let want: Vec<(u32, u32, Vec<(u32, u32)>)> = store
                .postings(term)
                .map(|ps| {
                    ps.iter()
                        .map(|p| (p.doc_id, p.tf, p.offsets.clone()))
                        .collect()
                })
                .unwrap_or_default();
            assert_eq!(got, want, "for_each_posting({term})");
            let mut got_tf = Vec::new();
            reader.for_each_doc_tf(term, &mut |d, tf| got_tf.push((d, tf)));
            let want_tf: Vec<(u32, u32)> = want.iter().map(|(d, tf, _)| (*d, *tf)).collect();
            assert_eq!(got_tf, want_tf, "for_each_doc_tf({term})");
            for (d, _, offs) in &want {
                assert_eq!(
                    &reader.posting_offsets(term, *d),
                    offs,
                    "posting_offsets({term}, {d})"
                );
            }
            assert_eq!(
                reader.has_impacts(term),
                store.df(term) > 0,
                "has_impacts({term})"
            );
        }
        for slot in 0..store.next_doc_id() {
            assert_eq!(
                reader.doc_length(slot),
                store.doc_length(slot),
                "doc_length({slot})"
            );
            assert_eq!(
                Bm25Index::text(&reader, slot),
                store.text(slot).map(str::to_string),
                "text({slot})"
            );
            assert_eq!(
                Bm25Index::lineage(&reader, slot),
                store.lineage(slot),
                "lineage({slot})"
            );
        }
        // Heap reload (the shard append path) parses v6.
        let loaded = Bm25Store::load(&path).unwrap();
        assert_eq!(loaded.fields[0].name, "body");
        assert_eq!(loaded.fields[0].postings, store.fields[0].postings);
        assert_eq!(loaded.fields[0].doc_lengths, store.fields[0].doc_lengths);
        assert_eq!(loaded.fields[0].total_length, store.fields[0].total_length);
        assert_eq!(loaded.texts, store.texts);
        assert_eq!(loaded.lineages, store.lineages);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A deterministic two-field corpus ("body", "case_name"): gappy
    /// ids, terms shared across fields with different postings, docs
    /// without a case name, a doc with an empty body but an indexed
    /// case name, and one slot with stored text but no postings at all.
    fn two_field_corpus() -> Vec<(u32, String, AnalyzedDoc, Option<DocLineage>)> {
        let mut docs = Vec::new();
        for i in 0..40u32 {
            let id = i * 3 + (i % 2);
            let mut body: DocTerms = vec![
                ("court".to_string(), 1 + i % 4, vec![(i, i + 5)]),
                (format!("t{}", i % 6), 1, vec![(i + 7, i + 9)]),
            ];
            if i % 8 == 0 {
                body.push(("smith".to_string(), 2, vec![(0, 5), (9, 14)]));
            }
            if i % 10 == 7 {
                body.clear();
            }
            let body_len: u32 = body.iter().map(|t| t.1).sum();
            let mut fields = vec![AnalyzedField {
                terms: body,
                length: body_len,
            }];
            if i % 3 != 2 {
                // "smith" and "court" appear in BOTH fields, with
                // different postings; "n*" terms only here.
                fields.push(AnalyzedField {
                    terms: vec![
                        ("smith".to_string(), 1, vec![(0, 5)]),
                        ("court".to_string(), 1, Vec::new()),
                        (format!("n{}", i % 4), 1, Vec::new()),
                    ],
                    length: 3,
                });
            }
            let lineage = (i % 4 == 0).then_some(DocLineage {
                parent_id: u64::from(i) * 11,
                group_id: u64::from(i) * 13,
                span_start: i,
                span_end: i + 40,
            });
            docs.push((
                id,
                format!("case {i} body"),
                AnalyzedDoc { fields, quality: None, geography: None },
                lineage,
            ));
        }
        docs
    }

    fn two_field_store() -> Bm25Store {
        let mut store = Bm25Store::with_fields(&["body", "case_name"]);
        for (id, text, doc, lineage) in two_field_corpus() {
            store.add_document_with_lineage(id, text, doc, lineage);
        }
        store
    }

    /// Increment 2 (`docs/multi-field.md` build order step 2): a
    /// two-field store round-trips through the v6 file and the reader
    /// serves BOTH fields via [`Bm25Reader::field`], each view
    /// answering exactly like the heap store's view of the same field,
    /// with shared texts, lineages, and document count.
    #[test]
    fn multi_field_v6_round_trip() {
        let dir = test_dir("v6multi");
        let path = dir.join("shard.bm25");
        let store = two_field_store();
        store.save(&path).unwrap();
        let reader = Bm25Reader::open(&path).unwrap();

        assert_eq!(reader.field_count(), 2);
        assert_eq!(reader.field_name(0), "body");
        assert_eq!(reader.field_name(1), "case_name");
        assert_eq!(Bm25Index::doc_count(&reader), store.doc_count());

        let terms = ["court", "smith", "t1", "n1", "missing"];
        for f in 0..2 {
            let rv = reader.field(f);
            let sv = store.field(f);
            assert_eq!(rv.doc_count(), sv.doc_count(), "field {f} doc_count");
            assert_eq!(
                rv.total_doc_length(),
                sv.total_doc_length(),
                "field {f} total_doc_length"
            );
            for term in terms {
                assert_eq!(rv.df(term), sv.df(term), "field {f} df({term})");
                let mut got = Vec::new();
                rv.for_each_posting(term, &mut |d, tf, o| got.push((d, tf, o.to_vec())));
                let mut want = Vec::new();
                sv.for_each_posting(term, &mut |d, tf, o| want.push((d, tf, o.to_vec())));
                assert_eq!(got, want, "field {f} postings({term})");
                assert_eq!(
                    rv.has_impacts(term),
                    sv.df(term) > 0,
                    "field {f} has_impacts({term})"
                );
                for (d, _, offs) in &want {
                    assert_eq!(
                        &rv.posting_offsets(term, *d),
                        offs,
                        "field {f} posting_offsets({term}, {d})"
                    );
                }
            }
            for slot in 0..store.next_doc_id() {
                assert_eq!(
                    rv.doc_length(slot),
                    sv.doc_length(slot),
                    "field {f} dl({slot})"
                );
            }
        }
        // The two fields are genuinely different indexes over the
        // shared slot space.
        assert_ne!(reader.field(0).df("smith"), reader.field(1).df("smith"));
        assert_eq!(reader.field(0).df("n1"), 0);
        assert_ne!(reader.field(1).df("n1"), 0);
        // Shared text/lineage through any view.
        for slot in (0..store.next_doc_id()).step_by(7) {
            assert_eq!(
                reader.field(1).text(slot),
                store.text(slot).map(str::to_string),
                "text({slot})"
            );
            assert_eq!(
                reader.field(1).lineage(slot),
                store.lineage(slot),
                "lineage({slot})"
            );
        }
        // Heap reload (the shard append path) reproduces both fields.
        let loaded = Bm25Store::load(&path).unwrap();
        assert_eq!(loaded.field_count(), 2);
        for f in 0..2 {
            assert_eq!(loaded.fields[f].name, store.fields[f].name);
            assert_eq!(loaded.fields[f].postings, store.fields[f].postings);
            assert_eq!(loaded.fields[f].doc_lengths, store.fields[f].doc_lengths);
            assert_eq!(loaded.fields[f].total_length, store.fields[f].total_length);
        }
        assert_eq!(loaded.texts, store.texts);
        assert_eq!(loaded.lineages, store.lineages);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Fused multi-field scoring through the v6 READER's field views
    /// is bit-identical to scoring the heap store's views — the mmap
    /// read path changes nothing about the scores.
    #[test]
    fn fused_scoring_v6_reader_matches_store() {
        use crate::bm25::{top_k_fused_exhaustive, Bm25Params, CorpusStats, FieldQuery};
        let dir = test_dir("v6fused");
        let path = dir.join("shard.bm25");
        let store = two_field_store();
        store.save(&path).unwrap();
        let reader = Bm25Reader::open(&path).unwrap();

        let body_terms = vec!["court".to_string(), "smith".to_string(), "t3".to_string()];
        let name_terms = vec!["smith".to_string(), "n2".to_string()];
        let stats_for = |f: usize, terms: &[String]| CorpusStats {
            doc_count: store.doc_count(),
            total_doc_length: store.field(f).total_doc_length(),
            dfs: terms.iter().map(|t| store.field(f).df(t)).collect(),
        };
        let body_stats = stats_for(0, &body_terms);
        let name_stats = stats_for(1, &name_terms);
        let queries = |v0: &dyn Bm25Index, v1: &dyn Bm25Index| {
            top_k_fused_exhaustive(
                &[
                    FieldQuery {
                        index: v0,
                        terms: &body_terms,
                        stats: body_stats.clone(),
                        params: Bm25Params::default(),
                        weight: 1.0,
                    },
                    FieldQuery {
                        index: v1,
                        terms: &name_terms,
                        stats: name_stats.clone(),
                        params: Bm25Params { k1: 0.6, b: 0.2 },
                        weight: 2.5,
                    },
                ],
                15,
            )
        };
        let want = queries(&store.field(0), &store.field(1));
        let got = queries(&reader.field(0), &reader.field(1));
        assert!(!want.is_empty());
        assert_eq!(want.len(), got.len());
        for (w, g) in want.iter().zip(&got) {
            assert_eq!(w.doc_id, g.doc_id);
            assert_eq!(w.score.to_bits(), g.score.to_bits(), "doc {}", w.doc_id);
            assert_eq!(w.term_offsets, g.term_offsets, "doc {}", w.doc_id);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The transcoder's contract: transcoding the v4 file of a corpus
    /// produces byte-for-byte the v5 file the direct writer emits, on
    /// a gappy/lineaged corpus and on one whose hot term spans
    /// multiple level-1 skip groups (df > 4096).
    #[test]
    fn transcode_v4_matches_direct_v5_bytes() {
        let dir = test_dir("transcode");
        let hot_store = {
            let mut store = Bm25Store::new();
            for i in 0..4300u32 {
                let tf = 1 + i % 3;
                let mut terms: DocTerms = vec![("hot".to_string(), tf, vec![(i, i + 2)])];
                if i % 5 == 0 {
                    terms.push((format!("t{}", i % 7), 1, Vec::new()));
                }
                let length = terms.iter().map(|t| t.1).sum();
                store.add_document(i * 2, format!("doc {i}"), AnalyzedDoc::body(terms, length));
            }
            store
        };
        for (tag, store) in [("synthetic", synthetic_store()), ("hot", hot_store)] {
            let v4_path = dir.join(format!("{tag}.v4.bm25"));
            {
                let mut w = io::BufWriter::new(std::fs::File::create(&v4_path).unwrap());
                store.write_v4_for_bench(&mut w).unwrap();
                w.flush().unwrap();
            }
            let v5_path = dir.join(format!("{tag}.v5.bm25"));
            let stats = transcode_to_v5(&v4_path, &v5_path).unwrap();
            assert_eq!(stats.n_slots, store.next_doc_id(), "{tag}: n_slots");
            let mut direct = Vec::new();
            store.write_to(&mut direct).unwrap();
            let transcoded = std::fs::read(&v5_path).unwrap();
            assert_eq!(transcoded.len(), direct.len(), "{tag}: sizes differ");
            assert!(
                transcoded == direct,
                "{tag}: transcoded v5 is not byte-identical to the direct writer"
            );
            // And it serves as v5, block-max surface included.
            let reader = Bm25Reader::open(&v5_path).unwrap();
            let probe = if tag == "hot" { "hot" } else { "court" };
            assert!(reader.has_impacts(probe), "{tag}: no impacts on {probe}");
            // Transcoding a v5 file is refused.
            assert!(transcode_to_v5(&v5_path, &dir.join("again.bm25")).is_err());
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// v6 validation rejects malformed headers and section geometry
    /// with errors, never panics.
    #[test]
    fn v6_open_rejects_malformed_files() {
        let dir = test_dir("v6bad");
        let path = dir.join("shard.bm25");
        let store = synthetic_store();
        store.save(&path).unwrap();
        let good = std::fs::read(&path).unwrap();
        assert!(Bm25Reader::open(&path).is_ok());

        let bad_path = dir.join("bad.bm25");
        let expect_err = |mutate: &dyn Fn(&mut Vec<u8>), what: &str| {
            let mut bytes = good.clone();
            mutate(&mut bytes);
            std::fs::write(&bad_path, &bytes).unwrap();
            assert!(Bm25Reader::open(&bad_path).is_err(), "{what} was accepted");
        };
        expect_err(
            &|b| b[8..12].copy_from_slice(&0u32.to_le_bytes()),
            "zero fields",
        );
        expect_err(
            &|b| {
                let v = u64::from_le_bytes(b[16..24].try_into().unwrap()) + 1;
                b[16..24].copy_from_slice(&v.to_le_bytes());
            },
            "texts section not at the header end",
        );
        expect_err(
            &|b| {
                let v = u64::from_le_bytes(b[32..40].try_into().unwrap()) + 1;
                b[32..40].copy_from_slice(&v.to_le_bytes());
            },
            "lineages offset off the text index end",
        );
        expect_err(
            &|b| {
                // Field total_length no longer matches the doc lengths.
                let name_len = u16::from_le_bytes(b[40..42].try_into().unwrap()) as usize;
                let fe = 42 + name_len + 8;
                let v = u64::from_le_bytes(b[fe..fe + 8].try_into().unwrap()) + 1;
                b[fe..fe + 8].copy_from_slice(&v.to_le_bytes());
            },
            "field total_length mismatch",
        );
        expect_err(
            &|b| {
                let n = b.len();
                b.truncate(n - 3);
            },
            "truncated term blob",
        );
        expect_err(&|b| b.truncate(30), "truncated header");
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
                parent_id: u64::from(i),
                group_id: u64::from(i) * 7,
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
                AnalyzedDoc::body(terms, length),
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
                parent_id: u64::from(i),
                group_id: u64::from(i) * 7,
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
                AnalyzedDoc::body(terms, length),
                lineage,
            );
        }
        store
    }

    /// Truncation at EVERY byte length must error, never panic — on a
    /// small file so the sweep stays fast. v6, v5, and v4.
    #[test]
    fn truncated_open_errors_never_panics() {
        let dir = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target/tmp")
            .join(format!("truncate_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let store = small_corpus_store();
        let v6_path = dir.join("small.v6.bm25");
        let v5_path = dir.join("small.v5.bm25");
        let v4_path = dir.join("small.v4.bm25");
        store.save(&v6_path).unwrap();
        store.save_v5(&v5_path).unwrap();
        write_v4(&store, &v4_path);
        for (tag, src) in [("v6", &v6_path), ("v5", &v5_path), ("v4", &v4_path)] {
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
        let v6_path = dir.join("corpus.v6.bm25");
        let v5_path = dir.join("corpus.v5.bm25");
        let v4_path = dir.join("corpus.v4.bm25");
        store.save(&v6_path).unwrap();
        store.save_v5(&v5_path).unwrap();
        write_v4(&store, &v4_path);

        for (tag, src) in [("v6", &v6_path), ("v5", &v5_path), ("v4", &v4_path)] {
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
            // v6: flips anywhere — the explicit section table makes
            // the whole file validated structure. v5/v4: header,
            // directory, and (v5) skip-run regions from the fixed
            // header: magic(8) total_length(8) texts(8) lineages(8)
            // postings(8) directory(8) n_slots(4).
            let regions: Vec<(usize, usize)> = if tag == "v6" {
                vec![(0, bytes.len())]
            } else {
                let directory_off = u64::from_le_bytes(bytes[40..48].try_into().unwrap()) as usize;
                let n_terms =
                    u32::from_le_bytes(bytes[directory_off..directory_off + 4].try_into().unwrap())
                        as usize;
                let mut regions: Vec<(usize, usize)> = vec![(0, 52), (directory_off, bytes.len())];
                if tag == "v5" {
                    for i in 0..n_terms {
                        let e = directory_off + 4 + 34 * i;
                        let skip_off =
                            u64::from_le_bytes(bytes[e + 8..e + 16].try_into().unwrap()) as usize;
                        let end = if i + 1 < n_terms {
                            u64::from_le_bytes(bytes[e + 34..e + 42].try_into().unwrap()) as usize
                        } else {
                            directory_off
                        };
                        regions.push((skip_off, end));
                    }
                }
                regions
            };
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
