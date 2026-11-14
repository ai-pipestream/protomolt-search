//! Memory-mapped `.bm25` reader (`Bm25Reader`).

mod columns;
mod field_view;
mod transcode;
mod validate;
mod validate_v5;

pub use self::field_view::{FieldTranspose, FieldView};
pub use self::transcode::{transcode_to_v5, TranscodeStats};
pub(super) use self::validate::validate_structure_v6;

use std::io;
use std::path::Path;

use self::validate_v5::validate_structure;
use super::codec::ImpactCursor;
use super::columns::{
    decode_binding_entry, decode_derived_entry, dictionary_sorted, FacetSlice, GeoSlice,
    IntegerMapSlice, IntegerSlice, MapFacetSlice, MapNumericSlice, NumericSlice, StoredBinding,
    StoredDerived, UnsignedSlice,
};
use super::format::{
    compatible_column_kind, integrity_eager, parse_integrity, IntegrityTable,
    COLUMN_KIND_ANALYSIS_BINDING, COLUMN_KIND_BINDING, COLUMN_KIND_DERIVED, COLUMN_KIND_F64,
    COLUMN_KIND_FACET, COLUMN_KIND_GEO, COLUMN_KIND_I64, COLUMN_KIND_I64_PRESENT,
    COLUMN_KIND_INDEX_BINDING, COLUMN_KIND_MAP_F64, COLUMN_KIND_MAP_FACET, COLUMN_KIND_MAP_I64,
    COLUMN_KIND_MAP_U64, COLUMN_KIND_POSITIONS, COLUMN_KIND_POS_LENGTHS, COLUMN_KIND_SENTENCES,
    COLUMN_KIND_SOURCES, COLUMN_KIND_SPLADE, COLUMN_KIND_U64, COLUMN_KIND_VECTOR_BINDING, MAGIC_V3,
    MAGIC_V4, MAGIC_V5, MAGIC_V6, MAGIC_V7, MAGIC_V8, POSITIONS_ENTRY_PREFIX,
    POS_LENGTHS_ENTRY_PREFIX, SENTENCES_ENTRY_PREFIX, SPLADE_ENTRY_PREFIX,
};
#[cfg(test)]
use super::BLOCK;
use super::{Bm25Index, DocLineage, PostingCallback};

/// Per-field read state of one open `.bm25` file: the field's decoded
/// doc-length table plus the map offsets of its directory and postings
/// sections. v3/v4/v5 files have exactly one ("body"); a v6 file one
/// per field-table entry.
pub(in crate::postings) struct FieldSlice {
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
    /// Start of this field's kind-7 positions section, when the file
    /// carries one (`docs/phrase-proximity.md`); `None` is a field
    /// without token positions, which refuses phrase and slop by name.
    positions_off: Option<u64>,
    pos_lengths_off: Option<u64>,
    splade_range: Option<(u64, u64)>,
    /// Start of this field's kind-8 sentence section, when the file
    /// carries one (`docs/highlighting.md`); `None` refuses sentence-mode
    /// snippets by name.
    sentences_off: Option<u64>,
}

/// A memory-mapped, disk-resident view of a `.bm25` file (v3 through v8).
/// Postings and document texts are read from the map on demand; the only
/// heap state is the per-document length table and a term count.
pub struct Bm25Reader {
    map: memmap2::Mmap,
    /// Per-field state, field-id order; never empty. Field 0 is the
    /// body.
    pub(in crate::postings) fields: Vec<FieldSlice>,
    /// Per-facet state, facet-id order (v7 files; empty otherwise).
    facets: Vec<FacetSlice>,
    /// Per-numeric-column state, numeric-id order (v7 files; empty
    /// otherwise).
    numerics: Vec<NumericSlice>,
    /// Per-map-facet-column state (v7 files; empty otherwise).
    map_facets: Vec<MapFacetSlice>,
    /// Per-map-numeric-column state (v7 files; empty otherwise).
    map_numerics: Vec<MapNumericSlice>,
    map_integers: Vec<IntegerMapSlice<i64>>,
    map_unsigned_integers: Vec<IntegerMapSlice<u64>>,
    /// Per-integer-column state, integer-id order (v7 files; empty
    /// otherwise).
    integers: Vec<IntegerSlice>,
    unsigned_integers: Vec<UnsignedSlice>,
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
    pub(in crate::postings) v5_runs: bool,
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
    /// The derived-column declaration read from the kind-15 entry.
    derived: Option<StoredDerived>,
    source_section: Option<(u64, u64, crate::source_archive::SourceArchiveReader)>,
    /// and the eagerly-read sections; [`Self::verify_integrity`]
    /// checks everything.
    pub(in crate::postings) integrity: Option<IntegrityTable>,
    /// The facet columns inverted, each built the first time a filter
    /// names it (`crate::facet_rows`). The file never changes, so neither
    /// do they.
    facet_rows: std::sync::OnceLock<Box<[std::sync::OnceLock<crate::facet_rows::FacetRows>]>>,
    /// The number columns sorted by value, built and kept the same way
    /// (`crate::number_rows`).
    number_rows: SortedNumbers,
}

type SortedColumns<T> =
    std::sync::OnceLock<Box<[std::sync::OnceLock<crate::number_rows::NumberRows<T>>]>>;

#[derive(Default)]
struct SortedNumbers {
    numeric: SortedColumns<f64>,
    integer: SortedColumns<i64>,
    unsigned_integer: SortedColumns<u64>,
}

/// Column `column` of `columns` sorted, built on first use.
fn sorted_column<T: Copy>(
    cache: &SortedColumns<T>,
    columns: usize,
    column: usize,
    build: impl FnOnce() -> crate::number_rows::NumberRows<T>,
) -> &crate::number_rows::NumberRows<T> {
    let cells = cache.get_or_init(|| (0..columns).map(|_| std::sync::OnceLock::new()).collect());
    cells[column].get_or_init(build)
}

impl Bm25Reader {
    /// Visit stored rows for an exact key; does not apply visibility or grants.
    pub fn visit_document_rows(
        &self,
        key: &[u8],
        visitor: &mut dyn FnMut(u32, u64, Option<u32>) -> bool,
    ) -> bool {
        self.source_section
            .as_ref()
            .is_none_or(|(_, _, reader)| reader.visit_document_rows(key, visitor))
    }

    pub fn identity_snapshot(&self) -> crate::source_archive::IdentitySnapshot {
        self.source_section
            .as_ref()
            .map(|(_, _, reader)| reader.identity_snapshot())
            .unwrap_or_default()
    }

    pub fn document_identity(&self, row: u32) -> Option<crate::pb::DocumentIdentity> {
        self.source_section
            .as_ref()
            .and_then(|(_, _, reader)| reader.identity(row))
    }

    pub fn protobuf_source(
        &self,
        row: u32,
    ) -> io::Result<Option<(crate::pb::ProtobufSource, Option<u32>)>> {
        match &self.source_section {
            Some((off, len, reader)) => {
                reader.row(&self.map[*off as usize..(off + len) as usize], row)
            }
            None => Ok(None),
        }
    }

    /// The mapped-plan binding this file was written under, if any.
    pub fn binding(&self) -> Option<&StoredBinding> {
        self.binding.as_ref()
    }

    /// The derived-column declaration this file was written under, if any.
    pub fn derived(&self) -> Option<&StoredDerived> {
        self.derived.as_ref()
    }

    /// Drop this file's resident pages from the process. The mapping
    /// stays valid: a later read faults the page back in from the page
    /// cache or the file, so a reader is not changed by it. For a pass
    /// that reads a store once and moves on (the transplant over a
    /// catalog of many segments), so the resident set is the segment
    /// in hand and not every segment read so far.
    pub fn release_pages(&self) -> io::Result<()> {
        // A read-only file mapping: DONTNEED drops the pages and the
        // next access reads the file again; the advice is unchecked
        // in memmap2 because on an anonymous or writable private
        // mapping it would discard data, which this mapping has none of.
        unsafe {
            self.map
                .unchecked_advise(memmap2::UncheckedAdvice::DontNeed)
        }
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
    /// The names of the v8 integrity sections, in file order (empty
    /// for a pre-v8 file, which has no table).
    pub fn integrity_section_names(&self) -> Vec<String> {
        self.integrity_sections()
            .into_iter()
            .map(|(name, _)| name)
            .collect()
    }

    /// The v8 integrity sections as `(name, byte length)`, in file
    /// order — the per-section cost accounting a payload's price is
    /// measured from (empty for a pre-v8 file).
    pub fn integrity_sections(&self) -> Vec<(String, u64)> {
        self.integrity
            .as_ref()
            .map(|t| t.entries.iter().map(|e| (e.name.clone(), e.len)).collect())
            .unwrap_or_default()
    }

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
    /// validation (see `validate_structure` / `validate_structure_v6`
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
                positions_off: None,
                pos_lengths_off: None,
                splade_range: None,
                sentences_off: None,
            }],
            facets: Vec::new(),
            numerics: Vec::new(),
            binding: None,
            derived: None,
            source_section: None,
            map_facets: Vec::new(),
            map_numerics: Vec::new(),
            map_integers: Vec::new(),
            map_unsigned_integers: Vec::new(),
            integers: Vec::new(),
            unsigned_integers: Vec::new(),
            geos: Vec::new(),
            doc_count,
            lineages_off,
            text_index_off: lineages_off - 12 * n_slots as u64,
            text_base: 0,
            v5_runs: v5,
            blob_relative,
            lineage_index: std::sync::OnceLock::new(),
            integrity: None,
            facet_rows: std::sync::OnceLock::new(),
            number_rows: SortedNumbers::default(),
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
                positions_off: None,
                pos_lengths_off: None,
                splade_range: None,
                sentences_off: None,
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
        let mut map_integers = Vec::new();
        let mut map_unsigned_integers = Vec::new();
        let mut integers = Vec::new();
        let mut unsigned_integers = Vec::new();
        let mut geos = Vec::new();
        let mut binding = None;
        let mut derived = None;
        let mut source_section = None;
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
                let kind =
                    compatible_column_kind(kind, name.as_bytes(), &map[base..], map.len() as u64);
                match kind {
                    COLUMN_KIND_FACET => {
                        let n_values = u32_at(base) as usize;
                        let dict_off = u64_at(base + 4) as usize;
                        let ords_off = u64_at(base + 12);
                        let (dict, _) = read_dict(dict_off, n_values);
                        let sorted = dictionary_sorted(&dict);
                        facets.push(FacetSlice {
                            name,
                            dict,
                            sorted,
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
                        let keys_sorted = dictionary_sorted(&keys);
                        let values_sorted = dictionary_sorted(&values);
                        map_facets.push(MapFacetSlice {
                            name,
                            keys,
                            values,
                            keys_sorted,
                            values_sorted,
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
                    COLUMN_KIND_MAP_I64 | COLUMN_KIND_MAP_U64 => {
                        let off = u64_at(base) as usize;
                        let len = u64_at(base + 8) as usize;
                        if kind == COLUMN_KIND_MAP_I64 {
                            map_integers.push(IntegerMapSlice {
                                name,
                                off,
                                len,
                                reader: crate::integer_map::Reader::open(
                                    &map[off..off + len],
                                    n_slots as u32,
                                )?,
                            });
                        } else {
                            map_unsigned_integers.push(IntegerMapSlice {
                                name,
                                off,
                                len,
                                reader: crate::integer_map::Reader::open(
                                    &map[off..off + len],
                                    n_slots as u32,
                                )?,
                            });
                        }
                        cursor = base + 16;
                    }
                    COLUMN_KIND_I64 | COLUMN_KIND_I64_PRESENT => {
                        integers.push(IntegerSlice {
                            name,
                            min: u64_at(base) as i64,
                            max: u64_at(base + 8) as i64,
                            vals_off: u64_at(base + 16),
                            presence_off: (kind == COLUMN_KIND_I64_PRESENT)
                                .then(|| u64_at(base + 16) + 8 * n_slots as u64),
                        });
                        cursor = base + 24;
                    }
                    COLUMN_KIND_U64 => {
                        unsigned_integers.push(UnsignedSlice {
                            name,
                            min: u64_at(base),
                            max: u64_at(base + 8),
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
                    COLUMN_KIND_POSITIONS => {
                        field_by_prefixed_name(&mut fields, &name, POSITIONS_ENTRY_PREFIX, kind)
                            .positions_off = Some(u64_at(base));
                        cursor = base + 16;
                    }
                    COLUMN_KIND_SENTENCES => {
                        field_by_prefixed_name(&mut fields, &name, SENTENCES_ENTRY_PREFIX, kind)
                            .sentences_off = Some(u64_at(base));
                        cursor = base + 16;
                    }
                    COLUMN_KIND_SOURCES => {
                        let off = u64_at(base);
                        let len = u64_at(base + 8);
                        let reader = crate::source_archive::SourceArchiveReader::open(
                            &map[off as usize..(off + len) as usize],
                            n_slots as u32,
                        )?;
                        source_section = Some((off, len, reader));
                        cursor = base + 16;
                    }
                    COLUMN_KIND_POS_LENGTHS => {
                        field_by_prefixed_name(
                            &mut fields,
                            &name,
                            POS_LENGTHS_ENTRY_PREFIX,
                            kind,
                        )
                        .pos_lengths_off = Some(u64_at(base));
                        cursor = base + 16;
                    }
                    COLUMN_KIND_SPLADE => {
                        field_by_prefixed_name(&mut fields, &name, SPLADE_ENTRY_PREFIX, kind)
                            .splade_range = Some((u64_at(base), u64_at(base + 8)));
                        cursor = base + 16;
                    }
                    COLUMN_KIND_BINDING
                    | COLUMN_KIND_ANALYSIS_BINDING
                    | COLUMN_KIND_VECTOR_BINDING
                    | COLUMN_KIND_INDEX_BINDING => {
                        let (decoded, used) = decode_binding_entry(kind, &map[base..])?;
                        binding = Some(decoded);
                        cursor = base + used;
                    }
                    COLUMN_KIND_DERIVED => {
                        let (decoded, used) = decode_derived_entry(&map[base..])?;
                        derived = Some(decoded);
                        cursor = base + used;
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
            map_integers,
            map_unsigned_integers,
            integers,
            unsigned_integers,
            geos,
            doc_count,
            lineages_off,
            text_index_off,
            text_base: texts_off,
            v5_runs: true,
            blob_relative: true,
            binding,
            derived,
            source_section,
            lineage_index: std::sync::OnceLock::new(),
            integrity: None,
            facet_rows: std::sync::OnceLock::new(),
            number_rows: SortedNumbers::default(),
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
    pub(in crate::postings) fn directory_lookup_v5(
        &self,
        field: &FieldSlice,
        term: &str,
    ) -> Option<(u64, u64, u64, u32)> {
        let i = self.directory_index_v5(field, term)?;
        let (_, doc, skip, occ, df) = self.directory_entry_v5(field, i);
        Some((doc, skip, occ, df))
    }

    /// The directory entry index of `term` in `field` (v5-shaped files),
    /// by binary search over the byte-sorted term directory.
    fn directory_index_v5(&self, field: &FieldSlice, term: &str) -> Option<u32> {
        let (mut lo, mut hi) = (0u32, field.n_terms);
        while lo < hi {
            let mid = (lo + hi) / 2;
            let (bytes, _, _, _, _) = self.directory_entry_v5(field, mid);
            match bytes.cmp(term.as_bytes()) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(mid),
            }
        }
        None
    }

    /// Index of the posting for `doc_id` in a term's fixed-stride doc
    /// run (doc ids ascending), or `None` when absent.
    fn v5_posting_index(&self, doc_run_off: usize, df: u32, doc_id: u32) -> Option<usize> {
        let (mut lo, mut hi) = (0usize, df as usize);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.u32_at(doc_run_off + 12 * mid) < doc_id {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        (lo < df as usize && self.u32_at(doc_run_off + 12 * lo) == doc_id).then_some(lo)
    }

    /// Whether field `f` carries a kind-7 positions section.
    pub fn field_has_positions(&self, f: usize) -> bool {
        self.fields[f].positions_off.is_some()
    }

    /// Whether field `f` carries a kind-8 sentence section.
    pub fn field_has_sentences(&self, f: usize) -> bool {
        self.fields[f].sentences_off.is_some()
    }

    /// Field `f`'s sentence spans for `doc_id`, read from the map:
    /// `None` when the field has no section, an empty table for a slot
    /// past the section's count or without a document.
    pub fn field_doc_sentences(&self, f: usize, doc_id: u32) -> Option<Vec<(u32, u32)>> {
        let section = self.fields[f].sentences_off? as usize;
        let n_slots = self.u32_at(section) as usize;
        let slot = doc_id as usize;
        if slot >= n_slots {
            return Some(Vec::new());
        }
        let from = self.u32_at(section + 4 + 4 * slot) as usize;
        let to = self.u32_at(section + 4 + 4 * (slot + 1)) as usize;
        let data = section + 4 + 4 * (n_slots + 1);
        Some(
            (from..to)
                .map(|k| (self.u32_at(data + 8 * k), self.u32_at(data + 8 * k + 4)))
                .collect(),
        )
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
    #[cfg(test)]
    pub(in crate::postings) fn v5_l0_records(
        &self,
        skip_run_off: u64,
        df: u32,
    ) -> Vec<(u32, Vec<(u32, u32)>)> {
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
    fn has_positions(&self) -> bool {
        self.field(0).has_positions()
    }
    fn posting_positions(&self, term: &str, doc_id: u32) -> Option<Vec<u32>> {
        self.field(0).posting_positions(term, doc_id)
    }
    fn has_pos_lengths(&self) -> bool {
        self.field(0).has_pos_lengths()
    }
    fn posting_pos_lengths(&self, term: &str, doc_id: u32) -> Option<Vec<u32>> {
        self.field(0).posting_pos_lengths(term, doc_id)
    }
    fn has_splade(&self) -> bool {
        self.field(0).has_splade()
    }
    fn score_splade_with_floor(
        &self,
        query_weights: &[(String, f32)],
        query_l1_slack: f32,
        initial_floor: f32,
        k: usize,
        admitted: &dyn Fn(u32) -> bool,
    ) -> (
        Vec<(u32, f32)>,
        f32,
        crate::turbo_bridge::splade::SpladeTraversalStats,
    ) {
        self.field(0).score_splade_with_floor(
            query_weights,
            query_l1_slack,
            initial_floor,
            k,
            admitted,
        )
    }
    fn has_sentences(&self) -> bool {
        self.field(0).has_sentences()
    }
    fn doc_sentences(&self, doc_id: u32) -> Option<Vec<(u32, u32)>> {
        self.field(0).doc_sentences(doc_id)
    }
    fn prefix_terms(&self, prefix: &str) -> Box<dyn Iterator<Item = String> + '_> {
        self.field(0).prefix_iter(prefix)
    }
    fn expand_prefix(&self, prefix: &str, cap: usize) -> Result<Vec<String>, usize> {
        self.field(0).expand_prefix(prefix, cap)
    }
    fn suggest_prefix(&self, prefix: &str, max_scan: usize) -> Result<Vec<(String, u32)>, usize> {
        self.field(0).suggest_prefix(prefix, max_scan)
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

/// A sealed part answers a range from its column sorted by value.
impl crate::filter_bits::PartNumbers for Bm25Reader {
    fn int_value(&self, column: usize, row: u32) -> Option<i64> {
        (column < self.integer_count())
            .then(|| self.integer_value(column, row))
            .flatten()
    }
    fn uint_value(&self, column: usize, row: u32) -> Option<u64> {
        (column < self.unsigned_integer_count())
            .then(|| self.unsigned_integer_value(column, row))
            .flatten()
    }
    fn f64_value(&self, column: usize, row: u32) -> Option<f64> {
        (column < self.numeric_count())
            .then(|| self.numeric_value(column, row))
            .flatten()
    }
    fn int_sorted(&self, column: usize) -> Option<&crate::number_rows::NumberRows<i64>> {
        (column < self.integer_count()).then(|| self.integer_rows(column))
    }
    fn uint_sorted(&self, column: usize) -> Option<&crate::number_rows::NumberRows<u64>> {
        (column < self.unsigned_integer_count()).then(|| self.unsigned_integer_rows(column))
    }
    fn f64_sorted(&self, column: usize) -> Option<&crate::number_rows::NumberRows<f64>> {
        (column < self.numeric_count()).then(|| self.numeric_rows(column))
    }
}

fn field_by_prefixed_name<'a>(
    fields: &'a mut [FieldSlice],
    name: &str,
    prefix: &str,
    kind: u8,
) -> &'a mut FieldSlice {
    let field = name
        .strip_prefix(prefix)
        .unwrap_or_else(|| panic!("validation checked the kind-{kind} name"));
    fields
        .iter_mut()
        .find(|f| f.name == field)
        .unwrap_or_else(|| panic!("validation tied the kind-{kind} entry to a field"))
}
