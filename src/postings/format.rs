//! Binary format constants, section-geometry helpers, and v8 integrity framing.

use std::io::{self, Write};
use std::path::Path;

use super::columns::{decode_binding_entry, decode_derived_entry};
use super::reader::validate_structure_v6;
#[cfg(doc)]
use super::Bm25Reader;

/// v6 layout: multi-field. Variable-length
/// header holding an explicit section table (shared `texts` / `text_index` /
/// `lineages` offsets plus a field table locating each field's
/// `doc_lengths` / `postings` / `directory` sections); section-internal
/// pointers (`text_index` entries and directory run offsets) are relative
/// to their section's start so sections survive relocation.
pub(super) const MAGIC_V6: &[u8; 8] = b"TVBM2506";
/// v7 layout: v6 plus a kinded per-document column table
///. The header gains a
/// column table after the field table (u32 n_columns, then per column:
/// name, u8 kind, kind-specific payload); the column sections follow
/// the last field group in table order. Kind 0 is a dictionary-encoded
/// string facet column (u32 n_values, u64 dict_off, u64 ords_off →
/// dict + ords sections); kind 1 is an f64 numeric column (u64
/// min_bits, u64 max_bits, u64 vals_off → one n_slots x f64 section,
/// NaN = absent); kinds 2 and 3 are the map columns; kind 4 is an i64 column (same three-word
/// payload as kind 1, min/max as i64 bits → one n_slots x i64 section,
/// `i64::MIN` = absent); kind 5 is a geo-point
/// column (four bbox words + u64 vals_off → one n_slots x (f64, f64)
/// section, both NaN = absent). An unknown kind
/// refuses at open by number. A store with NO declared columns still
/// writes v6, byte-identical to every pre-facet build — the format
/// break is opt-in per shard.
pub(super) const MAGIC_V7: &[u8; 8] = b"TVBM2507";

/// v7 column-table kind: dictionary-encoded string facet column.
pub(super) const COLUMN_KIND_FACET: u8 = 0;
/// v7 column-table kind: f64 numeric column (NaN = absent).
pub(super) const COLUMN_KIND_F64: u8 = 1;
/// v7 column-table kind: map<string, string> column
///: key dict + value dict + per-doc
/// (key_ord, value_ord) pair lists behind a fixed-stride offsets
/// section. ONE column regardless of key cardinality — the structural
/// answer to the field-per-key explosion flattening causes.
pub(super) const COLUMN_KIND_MAP_FACET: u8 = 2;
/// v7 column-table kind: map<string, f64> column: key dict WITH
/// per-key min/max bound metadata + per-doc (key_ord, f64) pair lists.
pub(super) const COLUMN_KIND_MAP_F64: u8 = 3;
/// v7 column-table kind: i64 column, a
/// fixed-stride per-slot section like kind 1. Exists because f64 stops
/// being exact past 2^53 and this engine argues from exactness: an id,
/// a citation count, or an epoch-micros timestamp must come back the
/// integer it went in as.
pub(super) const COLUMN_KIND_I64: u8 = 4;
/// Full-domain i64: fixed-width values followed by an LSB-first presence bitmap.
/// Same table payload as kind 4; absent slots have canonical zero value bytes.
pub(super) const COLUMN_KIND_I64_PRESENT: u8 = 10;
/// Full-domain unsigned integer values with an explicit presence bitmap.
pub(super) const COLUMN_KIND_U64: u8 = 11;
/// v7 column-table kind: geo-point column, a
/// per-slot (lat, lon) f64 pair at a fixed 16 B stride. BOTH halves NaN
/// means absent; a half-NaN pair is refused at open as corruption,
/// because a point with one coordinate is not a point. The table entry
/// carries the column's bounding box (min/max lat and lon), validated
/// against a full scan at open like every other kind's metadata.
pub(super) const COLUMN_KIND_GEO: u8 = 5;
/// Kind 6 is the shard-level mapped-plan BINDING record
/// — not a column: at most
/// one entry, a pinned name, an inline payload (three length-prefixed
/// strings: plan fingerprint, bound body path, materialize-spec hash),
/// and NO sections. Riding the kinded table keeps the binding inside
/// the v8 integrity envelope with zero new format machinery, so it
/// lives and dies with the columns it describes — a binding that could
/// vanish separately from them would protect nothing. A user column
/// declared under the reserved name collides with the table's
/// name-uniqueness rule and refuses at save, loudly.
pub(super) const COLUMN_KIND_BINDING: u8 = 6;
/// Explicit analysis binding: the three legacy strings followed by analysis SHA-256.
/// A distinct kind makes old readers refuse rather than discard the contract.
pub(super) const COLUMN_KIND_ANALYSIS_BINDING: u8 = 12;
// Extends the explicit analysis payload with a canonical vector declaration.
pub(super) const COLUMN_KIND_VECTOR_BINDING: u8 = 13;
/// Explicit index policy, appended to the kind-13 binding payload.
pub(super) const COLUMN_KIND_INDEX_BINDING: u8 = 14;
pub(super) const COLUMN_KIND_MAP_I64: u8 = 15;
pub(super) const COLUMN_KIND_MAP_U64: u8 = 16;
/// Kind 17: the index's derived-column
/// declaration, one inline entry named `derived-columns`: a u16-length
/// fingerprint (lowercase hex), then a u32-length canonical
/// `DerivedColumns` protobuf. A file written under another declaration
/// is refused by name where it is attached. Kinds 15 and 16 contain exact integer maps.
pub(super) const COLUMN_KIND_DERIVED: u8 = 17;

pub(super) const COLUMN_KIND_SOURCES: u8 = 9;
pub(super) const SOURCES_ENTRY_NAME: &str = "protobuf-sources";
pub(super) const SOURCE_ENTRY_BYTES: u64 = 2 + SOURCES_ENTRY_NAME.len() as u64 + 1 + 16;

pub(super) fn write_source_entry(
    w: &mut impl Write,
    section: Option<(u64, u64)>,
) -> io::Result<()> {
    if let Some((offset, length)) = section {
        write_u16(w, SOURCES_ENTRY_NAME.len() as u16)?;
        w.write_all(SOURCES_ENTRY_NAME.as_bytes())?;
        w.write_all(&[COLUMN_KIND_SOURCES])?;
        write_u64(w, offset)?;
        write_u64(w, length)?;
    }
    Ok(())
}
/// The binding record's reserved entry name.
pub(super) const BINDING_ENTRY_NAME: &str = "plan-binding";
/// The derived-column declaration's reserved entry name (kind 17).
pub(super) const DERIVED_ENTRY_NAME: &str = "derived-columns";
/// Kind 7: one BM25 field's token
/// positions, the opt-in payload behind exact phrase and proximity
/// queries. The entry is named `positions:<field>` (the prefix keeps it
/// out of the column name space, where a facet may legitimately share
/// the field's name) and carries `u64 section_off | u64 total`, where
/// `total` is the field's occurrence count. Its section is
/// `u32 n_terms | (n_terms + 1) x u32 base | total x u32 ordinal`: `base[i]`
/// is the cumulative occurrence count before directory entry `i`, so a
/// posting's ordinals sit at `base[i] + occ_start .. base[i] + occ_end`,
/// parallel to its occurrence-run slice, and a lookup is the same
/// binary search the occurrence read already does plus one slice.
/// A file without the entry serves every old query and refuses
/// phrase/slop on that field by name; a binary predating kind 7 refuses
/// the file by number, as the kinded table intends.
pub(super) const COLUMN_KIND_POSITIONS: u8 = 7;
/// Name prefix of a kind-7 entry; the remainder is the field name.
pub(super) const POSITIONS_ENTRY_PREFIX: &str = "positions:";

/// Byte size of a kind-7 section for `n_terms` terms holding `total`
/// occurrences.
pub(super) fn positions_section_size(n_terms: u64, total: u64) -> u64 {
    4 + 4 * (n_terms + 1) + 4 * total
}

/// Kind 8: a field's sentence table, the
/// spans server-side snippets are cut at. Entry: the field's name under
/// [`SENTENCES_ENTRY_PREFIX`], then `u64 off | u64 total`. Section:
/// `u32 n_slots | (n_slots + 1) x u32 base | total x (u32 start, u32 end)`
/// — `base[d]..base[d + 1]` are document `d`'s sentences in text order,
/// UTF-8 byte offsets of the stored text, sorted and non-overlapping. Costs
/// 4 B per slot plus 8 B per sentence, read only for returned hits.
pub(super) const COLUMN_KIND_SENTENCES: u8 = 8;
/// Column-table name prefix of a kind-8 entry: `sentences:<field>`.
pub(super) const SENTENCES_ENTRY_PREFIX: &str = "sentences:";

/// Byte size of a kind-8 section for `n_slots` documents holding
/// `total` sentences.
pub(super) fn sentences_section_size(n_slots: u64, total: u64) -> u64 {
    4 + 4 * (n_slots + 1) + 8 * total
}

/// Kind 18: token-lattice position lengths (`pos_lengths:<field>`), parallel
/// to kind 7 (`positions:<field>`) with identical section geometry (`u64 off | u64 total`,
/// `u32 n_terms | (n_terms + 1) x u32 base | total x u32 pos_len`), emitted only
/// when a positional field records multi-token or parallel lattice arcs.
pub(super) const COLUMN_KIND_POS_LENGTHS: u8 = 18;
pub(super) const POS_LENGTHS_ENTRY_PREFIX: &str = "pos_lengths:";

/// Kind 19: calibrated quantized `u8`/`u16` SPLADE 128-doc Block-Max postings
/// (`splade:<field>`, `u64 off | u64 len`, `TVSPLD02` payload), emitted only
/// when a field records non-empty SPLADE impacts.
pub(super) const COLUMN_KIND_SPLADE: u8 = 19;
pub(super) const SPLADE_ENTRY_PREFIX: &str = "splade:";

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
pub(super) const MAGIC_V8: &[u8; 8] = b"TVBM2508";
/// The v8 trailer's closing magic; the trailer is `u64 integrity_off,
/// u64 base_version, TVBMINTG`.
pub(super) const TRAILER_MAGIC: &[u8; 8] = b"TVBMINTG";
/// Byte length of the v8 trailer.
pub(super) const TRAILER_LEN: u64 = 24;

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
pub(super) struct IntegrityEntry {
    pub name: String,
    pub off: u64,
    pub len: u64,
    pub crc: u32,
}

/// The parsed v8 integrity envelope.
#[derive(Debug)]
pub(super) struct IntegrityTable {
    pub entries: Vec<IntegrityEntry>,
    /// Where the payload ends and the integrity section begins.
    pub payload_len: u64,
    /// Whether the payload is v7-shaped (column table) or v6-shaped.
    pub base_v7: bool,
}

/// The big lazily-paged blobs are the only sections open does not
/// CRC-verify; everything else is read at open anyway, so checking it
/// there is nearly free. `verify_integrity` checks these too.
pub(super) fn integrity_eager(name: &str) -> bool {
    name != "texts" && !name.ends_with(":postings")
}

/// Parse and check the v8 envelope of `full` (a whole file, leading
/// magic already matched): trailer, table, table CRC, and that the
/// entries partition the payload exactly. Section CRCs are NOT
/// verified here — callers decide eager vs deep.
pub(super) fn parse_integrity(full: &[u8]) -> io::Result<IntegrityTable> {
    let invalid = |msg: String| io::Error::new(io::ErrorKind::InvalidData, msg);
    let (trailer, integrity_off, base_v7) = parse_integrity_trailer(full)?;
    let u64_at = |off: u64| -> u64 {
        u64::from_le_bytes(
            full[off as usize..off as usize + 8]
                .try_into()
                .expect("8 bytes"),
        )
    };
    let need = |cur: u64, n: u64| -> io::Result<()> {
        if cur + n > trailer {
            return Err(invalid(format!(
                "v8 integrity table runs past its section at {cur}"
            )));
        }
        Ok(())
    };
    let mut cur = integrity_off;
    need(cur, 4)?;
    let n_entries = u32::from_le_bytes(full[cur as usize..cur as usize + 4].try_into().unwrap());
    cur += 4;
    if n_entries == 0 {
        return Err(invalid("v8 integrity table with zero entries".into()));
    }
    let max_entries = (trailer - integrity_off).saturating_sub(8) / 23;
    if u64::from(n_entries) > max_entries {
        return Err(invalid(format!(
            "v8 integrity table declares {n_entries} entries, at most {max_entries} fit its section"
        )));
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
        let crc = u32::from_le_bytes(
            full[(base + 16) as usize..(base + 20) as usize]
                .try_into()
                .unwrap(),
        );
        entries.push(IntegrityEntry {
            name,
            off: u64_at(base),
            len: u64_at(base + 8),
            crc,
        });
        cur = base + 20;
    }
    need(cur, 4)?;
    verify_integrity_crc_and_partition(full, &entries, integrity_off, cur, trailer)?;
    Ok(IntegrityTable {
        entries,
        payload_len: integrity_off,
        base_v7,
    })
}

fn parse_integrity_trailer(full: &[u8]) -> io::Result<(u64, u64, bool)> {
    let invalid = |msg: String| io::Error::new(io::ErrorKind::InvalidData, msg);
    let file_len = full.len() as u64;
    if file_len < 52 + TRAILER_LEN {
        return Err(invalid("v8 file shorter than header plus trailer".into()));
    }
    let trailer = file_len - TRAILER_LEN;
    if &full[(trailer + 16) as usize..] != TRAILER_MAGIC {
        return Err(invalid(
            "v8 trailer magic missing: truncated or overwritten tail".into(),
        ));
    }
    let u64_at = |off: u64| -> u64 {
        u64::from_le_bytes(
            full[off as usize..off as usize + 8]
                .try_into()
                .expect("8 bytes"),
        )
    };
    let integrity_off = u64_at(trailer);
    let base_v7 = match u64_at(trailer + 8) {
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
    Ok((trailer, integrity_off, base_v7))
}

fn verify_integrity_crc_and_partition(
    full: &[u8],
    entries: &[IntegrityEntry],
    integrity_off: u64,
    crc_off: u64,
    trailer: u64,
) -> io::Result<()> {
    let invalid = |msg: String| io::Error::new(io::ErrorKind::InvalidData, msg);
    let stored_table_crc = u32::from_le_bytes(
        full[crc_off as usize..crc_off as usize + 4]
            .try_into()
            .unwrap(),
    );
    let computed = crate::wal::crc32(&full[integrity_off as usize..crc_off as usize]);
    if stored_table_crc != computed {
        return Err(invalid(format!(
            "v8 integrity table CRC mismatch: stored {stored_table_crc:08x}, computed {computed:08x}"
        )));
    }
    if crc_off + 4 != trailer {
        return Err(invalid(format!(
            "v8 integrity section ends at {} but the trailer starts at {trailer}",
            crc_off + 4
        )));
    }
    let mut expected = 0u64;
    for e in entries {
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
    Ok(())
}

/// Every section start of a STRUCTURALLY VALIDATED v6/v7 payload, in
/// file order with its integrity name. The walk mirrors
/// [`Bm25Reader::open_v6v7`]'s header parse; ranges are the gaps
/// between consecutive starts (the writer lays sections with one
/// cursor, so they are contiguous by construction).
pub(super) fn v6v7_section_starts(map: &[u8], v7: bool) -> io::Result<Vec<(String, u64)>> {
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
            push_column_section_start(&mut starts, map, &mut cursor)?;
        }
    }
    for pair in starts.windows(2) {
        if pair[1].1 < pair[0].1 {
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

fn push_column_section_start(
    starts: &mut Vec<(String, u64)>,
    map: &[u8],
    cursor: &mut usize,
) -> io::Result<()> {
    let u32_at = |off: usize| u32::from_le_bytes(map[off..off + 4].try_into().expect("4 bytes"));
    let u64_at = |off: usize| u64::from_le_bytes(map[off..off + 8].try_into().expect("8 bytes"));
    let name_len = u16::from_le_bytes(map[*cursor..*cursor + 2].try_into().unwrap()) as usize;
    let name = String::from_utf8_lossy(&map[*cursor + 2..*cursor + 2 + name_len]).into_owned();
    let kind = map[*cursor + 2 + name_len];
    let base = *cursor + 2 + name_len + 1;
    *cursor = match kind {
        COLUMN_KIND_FACET => {
            starts.push((format!("column:{name}:dict"), u64_at(base + 4)));
            starts.push((format!("column:{name}:ords"), u64_at(base + 12)));
            base + 20
        }
        COLUMN_KIND_F64 => {
            starts.push((format!("column:{name}:vals"), u64_at(base + 16)));
            base + 24
        }
        COLUMN_KIND_MAP_FACET => {
            starts.push((format!("column:{name}:keys"), u64_at(base + 8)));
            starts.push((format!("column:{name}:values"), u64_at(base + 16)));
            starts.push((format!("column:{name}:offsets"), u64_at(base + 24)));
            starts.push((format!("column:{name}:pairs"), u64_at(base + 32)));
            base + 40
        }
        COLUMN_KIND_MAP_F64 => {
            starts.push((format!("column:{name}:keys"), u64_at(base + 4)));
            starts.push((format!("column:{name}:offsets"), u64_at(base + 12)));
            starts.push((format!("column:{name}:pairs"), u64_at(base + 20)));
            base + 28
        }
        COLUMN_KIND_MAP_I64 | COLUMN_KIND_MAP_U64 => {
            starts.push((format!("column:{name}:integer_map"), u64_at(base)));
            base + 16
        }
        COLUMN_KIND_I64 | COLUMN_KIND_I64_PRESENT | COLUMN_KIND_U64 => {
            starts.push((format!("column:{name}:vals"), u64_at(base + 16)));
            if kind != COLUMN_KIND_I64 {
                starts.push((
                    format!("column:{name}:presence"),
                    u64_at(base + 16) + 8 * u64::from(u32_at(12)),
                ));
            }
            base + 24
        }
        COLUMN_KIND_GEO => {
            starts.push((format!("column:{name}:vals"), u64_at(base + 32)));
            base + 40
        }
        COLUMN_KIND_POSITIONS | COLUMN_KIND_SENTENCES | COLUMN_KIND_POS_LENGTHS => {
            starts.push((format!("column:{name}:vals"), u64_at(base)));
            base + 16
        }
        COLUMN_KIND_SOURCES => {
            starts.push((format!("column:{name}:sources"), u64_at(base)));
            base + 16
        }
        COLUMN_KIND_SPLADE => {
            starts.push((format!("column:{name}:splade"), u64_at(base)));
            base + 16
        }
        COLUMN_KIND_BINDING
        | COLUMN_KIND_ANALYSIS_BINDING
        | COLUMN_KIND_VECTOR_BINDING
        | COLUMN_KIND_INDEX_BINDING => base + decode_binding_entry(kind, &map[base..])?.1,
        COLUMN_KIND_DERIVED => base + decode_derived_entry(&map[base..])?.1,
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("column {name}: unknown kind {other}"),
            ))
        }
    };
    Ok(())
}

/// The v8 post-pass: turn a finished v6/v7 file into a v8 one in
/// place. Validates the payload (never stamp integrity onto malformed
/// bytes), CRCs every section, appends the integrity section and
/// trailer, and patches the leading magic LAST — a crash mid-pass
/// leaves a v6/v7 magic with a garbage tail, which the validator's
/// section-extent checks refuse loudly. Ends with `sync_all`; the
/// caller renames and fsyncs the parent.
pub(super) fn finalize_v8(path: &Path) -> io::Result<()> {
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

pub(super) fn write_u16<W: Write>(w: &mut W, v: u16) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}

pub(super) fn write_u32<W: Write>(w: &mut W, v: u32) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}

pub(super) fn write_u64<W: Write>(w: &mut W, v: u64) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}

pub(super) fn u16_at(bytes: &[u8]) -> u16 {
    u16::from_le_bytes(bytes.try_into().expect("2 bytes"))
}
