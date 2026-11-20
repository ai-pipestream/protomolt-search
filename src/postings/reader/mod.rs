//! Memory-mapped `.bm25` reader (`Bm25Reader`).

mod columns;
mod field_view;
mod index_impl;
mod map_columns;
mod open_v6;
mod raw;
mod transpose;
mod validate;

pub use self::field_view::FieldView;
pub use self::transpose::FieldTranspose;
pub(super) use self::validate::validate_structure_v6;

use std::io;
use std::path::Path;

use super::columns::{
    FacetSlice, GeoSlice, IntegerMapSlice, IntegerSlice, MapFacetSlice, MapNumericSlice,
    NumericSlice, StoredBinding, StoredDerived, UnsignedSlice,
};
use super::format::{
    integrity_eager, parse_integrity, IntegrityTable, MAGIC_V6, MAGIC_V7, MAGIC_V8,
};
#[cfg(doc)]
use super::Bm25Index;

/// Per-field read state of one open `.bm25` file: the field's decoded
/// doc-length table plus the map offsets of its directory and postings
/// sections.
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
    /// Start of the field's postings section, added to section-relative
    /// directory run offsets.
    run_base: u64,
    /// Start of this field's kind-7 positions section, when the file
    /// carries one; `None` is a field
    /// without token positions, which refuses phrase and slop by name.
    positions_off: Option<u64>,
    pos_lengths_off: Option<u64>,
    splade_range: Option<(u64, u64)>,
    /// Start of this field's kind-8 sentence section, when the file
    /// carries one; `None` refuses sentence-mode
    /// snippets by name.
    sentences_off: Option<u64>,
}

/// A memory-mapped, disk-resident view of a `.bm25` file (v6, v7, or v8).
/// Postings and document texts are read from the map on demand; the only
/// heap state is the per-document length table and column metadata.
pub struct Bm25Reader {
    pub(in crate::postings) map: memmap2::Mmap,
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
    /// Start of the on-disk text index.
    text_index_off: u64,
    /// Start of the texts section, added to section-relative text-index offsets.
    text_base: u64,
    /// Lazily built lineage-section index: per-slot byte offset relative
    /// to `lineages_off`. The section is variable stride (1 B absent, 25
    /// B present), so random access needs this — one O(n_slots) decode
    /// on the first `lineage()` call, ~4 B/slot of heap, cached.
    lineage_index: std::sync::OnceLock<Vec<u32>>,
    /// The mapped-plan binding read from the kind-6/12/13/14 table entry.
    binding: Option<StoredBinding>,
    /// The derived-column declaration read from the kind-17 entry.
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
    /// ones. Panics when `f` is out of range.
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
        String::from_utf8_lossy(self.directory_entry(field, i).0).into_owned()
    }

    /// Open a `.bm25` file (v6, v7, or v8) read-only after full structural
    /// validation (see `validate_structure_v6` — malformed or unsupported
    /// files return `InvalidData`, never panic). Touches only the header,
    /// doc-length tables, directories, and skip runs; postings and stored
    /// texts remain unpaged until queries read them.
    pub fn open(path: &Path) -> io::Result<Self> {
        let invalid = |msg: &str| io::Error::new(io::ErrorKind::InvalidData, msg.to_string());
        let file = std::fs::File::open(path)?;
        let map = unsafe { memmap2::MmapOptions::new().map(&file)? };
        let Some(magic) = map.get(..8) else {
            return Err(invalid("unsupported or invalid .bm25 magic"));
        };
        match magic {
            m if m == MAGIC_V8 => Self::open_v8(map),
            m if m == MAGIC_V7 => Self::open_v6v7(map, true),
            m if m == MAGIC_V6 => Self::open_v6v7(map, false),
            _ => Err(invalid("unsupported or invalid .bm25 magic")),
        }
    }

    fn open_v8(map: memmap2::Mmap) -> io::Result<Self> {
        let table = parse_integrity(&map)?;
        for e in table.entries.iter().filter(|e| integrity_eager(&e.name)) {
            let got = crate::wal::crc32(&map[e.off as usize..(e.off + e.len) as usize]);
            if got != e.crc {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "section {} CRC mismatch: stored {:08x}, computed {got:08x} — the bytes are not the ones the build wrote",
                        e.name, e.crc
                    ),
                ));
            }
        }
        let base_v7 = table.base_v7;
        let payload_len = table.payload_len as usize;
        let mut reader = Self::open_v6v7_bounded(map, base_v7, payload_len)?;
        reader.integrity = Some(table);
        Ok(reader)
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
}
