//! The disk-resident `Bm25Reader`: the file is memory mapped, and the per-field
//! section slices plus the shared tables are validated at open — walking
//! message boundaries before allocating decoded records. v3 through v8 open
//! here; the v8 envelope's eager CRCs run during open, the lazy blobs during
//! `verify_integrity`.

use super::*;

use std::io::{self};
use std::path::Path;

pub(crate) fn u16_at(bytes: &[u8]) -> u16 {
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
pub(crate) struct FieldSlice {
    pub(crate) name: String,
    /// Hash of the field's AnalysisSpec, read back from the v6 field
    /// table. 0 for a shard written before fingerprints, which never
    /// enforces.
    pub(crate) analysis_fingerprint: u64,
    pub(crate) doc_lengths: Vec<u32>,
    pub(crate) total_length: u64,
    pub(crate) directory_off: u64,
    pub(crate) n_terms: u32,
    /// Base added to directory run offsets: 0 for v3/v4/v5 (absolute
    /// entries), the field's postings section offset for v6
    /// (section-relative entries).
    pub(crate) run_base: u64,
    /// Start of this field's kind-7 positions section, when the file
    /// carries one (`docs/phrase-proximity.md`); `None` is a field
    /// without token positions, which refuses phrase and slop by name.
    pub(crate) positions_off: Option<u64>,
    /// Start of this field's kind-8 sentence section, when the file
    /// carries one (`docs/highlighting.md`); `None` refuses sentence-mode
    /// snippets by name.
    pub(crate) sentences_off: Option<u64>,
}

/// Per-facet read state of one open v7 file: the decoded value
/// dictionary (small — one entry per distinct value) plus the map
/// offset of the fixed-stride ords section, which stays on disk.
pub(crate) struct FacetSlice {
    pub(crate) name: String,
    /// Values in ordinal order, decoded eagerly at open.
    pub(crate) dict: Vec<String>,
    /// Whether `dict` is in strict byte order, checked at open: true
    /// for every file this writer produces, and what makes an ordinal
    /// range an exact string range (`docs/prefix-terms.md`). A file
    /// written with first-seen ordinals refuses string ordering by name.
    pub(crate) sorted: bool,
    /// Absolute offset of the ords section (n_slots x u32).
    pub(crate) ords_off: u64,
}

/// Per-numeric-column read state of one open v7 file: the min/max
/// bound metadata from the column table plus the map offset of the
/// fixed-stride vals section, which stays on disk.
pub(crate) struct NumericSlice {
    pub(crate) name: String,
    /// Min over present values (NaN when the column holds none).
    pub(crate) min: f64,
    /// Max over present values (NaN when the column holds none).
    pub(crate) max: f64,
    /// Absolute offset of the vals section (n_slots x f64 bits).
    pub(crate) vals_off: u64,
}

/// Per-map-facet-column read state (`docs/map-columns.md`): both
/// dictionaries decoded eagerly (one entry per distinct key/value);
/// offsets and pairs sections stay in the map.
pub(crate) struct MapFacetSlice {
    pub(crate) name: String,
    /// Keys in ordinal order.
    pub(crate) keys: Vec<String>,
    /// Values in ordinal order.
    pub(crate) values: Vec<String>,
    /// Whether `keys` / `values` are in strict byte order (see
    /// `FacetSlice::sorted`).
    pub(crate) keys_sorted: bool,
    pub(crate) values_sorted: bool,
    /// Absolute offset of the offsets section ((n_slots + 1) x u32).
    pub(crate) offsets_off: u64,
    /// Absolute offset of the pairs section (8 B (key_ord, value_ord)
    /// entries, strictly key-ordered per document).
    pub(crate) pairs_off: u64,
}

/// Per-map-numeric-column read state: the key dictionary with its
/// per-key min/max bound metadata; offsets and pairs stay in the map.
pub(crate) struct IntegerMapSlice<T: crate::integer_map::Integer> {
    pub(crate) name: String,
    pub(crate) off: usize,
    pub(crate) len: usize,
    pub(crate) reader: crate::integer_map::Reader<T>,
}

pub(crate) struct MapNumericSlice {
    pub(crate) name: String,
    /// Keys in ordinal order.
    pub(crate) keys: Vec<String>,
    /// Per-key (min, max) over present values, parallel to `keys`.
    pub(crate) key_min_max: Vec<(f64, f64)>,
    /// Absolute offset of the offsets section ((n_slots + 1) x u32).
    pub(crate) offsets_off: u64,
    /// Absolute offset of the pairs section (12 B (key_ord, f64 bits)
    /// entries, strictly key-ordered per document).
    pub(crate) pairs_off: u64,
}

/// Per-integer-column read state of one open v7 file: the min/max
/// bound metadata from the column table plus the map offset of the
/// fixed-stride vals section, which stays on disk.
pub(crate) struct IntegerSlice {
    pub(crate) name: String,
    /// Min over present values (`i64::MAX` when the column holds none;
    /// see [`IntStore::min_max`] for why the empty range is inverted).
    pub(crate) min: i64,
    /// Max over present values (`i64::MIN` when the column holds none).
    pub(crate) max: i64,
    /// Absolute offset of the vals section (n_slots x i64).
    pub(crate) vals_off: u64,
    /// None decodes the legacy kind-4 sentinel.
    pub(crate) presence_off: Option<u64>,
}

pub(crate) struct UnsignedSlice {
    pub(crate) name: String,
    pub(crate) min: u64,
    pub(crate) max: u64,
    pub(crate) vals_off: u64,
}

/// Per-geo-column read state of one open v7 file: the bounding-box
/// metadata from the column table plus the map offset of the
/// fixed-stride vals section, which stays on disk.
pub(crate) struct GeoSlice {
    pub(crate) name: String,
    /// The column's bounding box over present points, all four NaN when
    /// there are none (the kind-1 empty convention). Validated against a
    /// full scan at open.
    pub(crate) bbox: (f64, f64, f64, f64),
    /// Absolute offset of the vals section (n_slots x (f64, f64)).
    pub(crate) vals_off: u64,
}

pub struct Bm25Reader {
    pub(crate) map: memmap2::Mmap,
    /// Per-field state, field-id order; never empty. Field 0 is the
    /// body.
    pub(crate) fields: Vec<FieldSlice>,
    /// Per-facet state, facet-id order (v7 files; empty otherwise).
    pub(crate) facets: Vec<FacetSlice>,
    /// Per-numeric-column state, numeric-id order (v7 files; empty
    /// otherwise).
    pub(crate) numerics: Vec<NumericSlice>,
    /// Per-map-facet-column state (v7 files; empty otherwise).
    pub(crate) map_facets: Vec<MapFacetSlice>,
    /// Per-map-numeric-column state (v7 files; empty otherwise).
    pub(crate) map_numerics: Vec<MapNumericSlice>,
    pub(crate) map_integers: Vec<IntegerMapSlice<i64>>,
    pub(crate) map_unsigned_integers: Vec<IntegerMapSlice<u64>>,
    /// Per-integer-column state, integer-id order (v7 files; empty
    /// otherwise).
    pub(crate) integers: Vec<IntegerSlice>,
    pub(crate) unsigned_integers: Vec<UnsignedSlice>,
    /// Per-geo-column state, geo-id order (v7 files; empty otherwise).
    pub(crate) geos: Vec<GeoSlice>,
    /// Documents with postings in any field — the corpus-wide N (a
    /// document is a document; idf never uses a per-field count).
    pub(crate) doc_count: u64,
    pub(crate) lineages_off: u64,
    /// Start of the on-disk text index (explicit in the v6 header;
    /// derived as `lineages_off - 12 * n_slots` for v3/v4/v5).
    pub(crate) text_index_off: u64,
    /// Base added to text-index entry offsets: 0 for v3/v4/v5
    /// (absolute entries), the texts section offset for v6 (relative
    /// entries).
    pub(crate) text_base: u64,
    /// v5-shaped postings (v5 and v6 files): 34 B directory entries
    /// and the doc/occurrence/skip run layout; v3/v4: 18 B entries and
    /// interleaved postings.
    pub(crate) v5_runs: bool,
    /// v4+ directories store blob offsets relative to the blob start;
    /// v3 stored absolute file offsets.
    pub(crate) blob_relative: bool,
    /// Lazily built lineage-section index: per-slot byte offset relative
    /// to `lineages_off`. The section is variable stride (1 B absent, 25
    /// B present), so random access needs this — one O(n_slots) decode
    /// on the first `lineage()` call, ~4 B/slot of heap, cached.
    pub(crate) lineage_index: std::sync::OnceLock<Vec<u32>>,
    /// The v8 integrity table (None for pre-v8 files, which have
    /// nothing to verify). Open has already checked the table itself
    /// The mapped-plan binding read from the kind-6 table entry.
    pub(crate) binding: Option<StoredBinding>,
    /// The derived-column declaration read from the kind-15 entry.
    pub(crate) derived: Option<StoredDerived>,
    pub(crate) source_section: Option<(u64, u64, crate::source_archive::SourceArchiveReader)>,
    /// and the eagerly-read sections; [`Self::verify_integrity`]
    /// checks everything.
    pub(crate) integrity: Option<IntegrityTable>,
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
    pub(crate) fn n_slots(&self) -> usize {
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
    /// when this file never ingested it. A binary search on a file
    /// whose dictionary is in byte order (every file this writer
    /// produces), a linear scan on an older first-seen file; either
    /// way resolution runs once per request, not per document.
    pub fn facet_value_ord_of(&self, fi: usize, value: &str) -> Option<u32> {
        dictionary_lookup(&self.facets[fi].dict, self.facets[fi].sorted, value)
    }

    /// Facet field `fi`'s dictionary, in ordinal order.
    pub fn facet_dictionary(&self, fi: usize) -> &[String] {
        &self.facets[fi].dict
    }

    /// Whether facet field `fi`'s dictionary is in byte order, so an
    /// ordinal range is a string range (`docs/prefix-terms.md`).
    pub fn facet_dictionary_sorted(&self, fi: usize) -> bool {
        self.facets[fi].sorted
    }

    /// Map-facet column `ci`'s value dictionary, in ordinal order.
    pub fn map_facet_values(&self, ci: usize) -> &[String] {
        &self.map_facets[ci].values
    }

    /// Whether map-facet column `ci`'s value dictionary is in byte order.
    pub fn map_facet_values_sorted(&self, ci: usize) -> bool {
        self.map_facets[ci].values_sorted
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
    /// One fixed-width value read and, for kind 10, one presence-bit read.
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
    /// One fixed-width value read and, for kind 10, one presence-bit read.
    pub fn integer_value(&self, ii: usize, doc_id: u32) -> Option<i64> {
        let integer = &self.integers[ii];
        let slot = doc_id as usize;
        if slot >= self.n_slots() {
            return None;
        }
        let off = integer.vals_off as usize + 8 * slot;
        let v = u64::from_le_bytes(self.map[off..off + 8].try_into().expect("8 bytes")) as i64;
        let present = integer.presence_off.map_or(v != INTEGER_ABSENT, |off| {
            integer_present(
                &self.map[off as usize..off as usize + self.n_slots().div_ceil(8)],
                slot,
            )
        });
        present.then_some(v)
    }

    /// Number of full-domain unsigned integer columns.
    pub fn unsigned_integer_count(&self) -> usize {
        self.unsigned_integers.len()
    }
    /// The name of one unsigned column.
    pub fn unsigned_integer_name(&self, column: usize) -> &str {
        &self.unsigned_integers[column].name
    }
    /// Resolve an unsigned column by name.
    pub fn unsigned_integer_index(&self, name: &str) -> Option<usize> {
        self.unsigned_integers
            .iter()
            .position(|column| column.name == name)
    }
    /// Bounds over present values. (MAX, 0) denotes an empty column.
    pub fn unsigned_integer_min_max(&self, column: usize) -> (u64, u64) {
        let column = &self.unsigned_integers[column];
        (column.min, column.max)
    }
    /// Read exact unsigned bits only when the row is present.
    pub fn unsigned_integer_value(&self, column: usize, row: u32) -> Option<u64> {
        let slot = row as usize;
        if slot >= self.n_slots() {
            return None;
        }
        let off = self.unsigned_integers[column].vals_off as usize;
        let bitmap = off + 8 * self.n_slots();
        if !integer_present(&self.map[bitmap..bitmap + self.n_slots().div_ceil(8)], slot) {
            return None;
        }
        Some(u64::from_le_bytes(
            self.map[off + 8 * slot..off + 8 * slot + 8]
                .try_into()
                .expect("8 bytes"),
        ))
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
    pub(crate) fn map_pair_range(&self, offsets_off: u64, doc_id: u32) -> Option<(usize, usize)> {
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
        dictionary_lookup(
            &self.map_facets[ci].keys,
            self.map_facets[ci].keys_sorted,
            key,
        )
    }

    /// The keys of map-facet column `ci`, in ordinal order.
    pub fn map_facet_keys(&self, ci: usize) -> &[String] {
        &self.map_facets[ci].keys
    }

    /// The keys of map-numeric column `ci`, in ordinal order.
    pub fn map_numeric_keys(&self, ci: usize) -> &[String] {
        &self.map_numerics[ci].keys
    }

    /// Number of map-facet columns.
    pub fn map_facet_count(&self) -> usize {
        self.map_facets.len()
    }

    /// The name of map-facet column `ci`.
    pub fn map_facet_name(&self, ci: usize) -> &str {
        &self.map_facets[ci].name
    }

    /// Number of exact i64 map columns.
    pub fn map_integer_count(&self) -> usize {
        self.map_integers.len()
    }
    /// Column name at `ci`; panics for an invalid column index.
    pub fn map_integer_name(&self, ci: usize) -> &str {
        &self.map_integers[ci].name
    }
    /// Look up an exact i64 map column by name.
    pub fn map_integer_index(&self, name: &str) -> Option<usize> {
        self.map_integers.iter().position(|c| c.name == name)
    }
    /// Map keys in this reader's ordinal order.
    pub fn map_integer_keys(&self, ci: usize) -> &[String] {
        &self.map_integers[ci].reader.keys
    }
    /// Find a key, including the empty string.
    pub fn map_integer_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        self.map_integers[ci].reader.key_ord(key)
    }
    /// Exact bounds, or `None` when the key has no entries.
    pub fn map_integer_key_min_max(&self, ci: usize, key: u32) -> Option<(i64, i64)> {
        self.map_integers[ci].reader.bounds(key)
    }
    /// Exact value; an absent entry or out-of-range key or row returns `None`.
    pub fn map_integer_value(&self, ci: usize, key: u32, row: u32) -> Option<i64> {
        let column = &self.map_integers[ci];
        column
            .reader
            .value(&self.map[column.off..column.off + column.len], key, row)
    }

    /// Number of exact u64 map columns.
    pub fn map_unsigned_integer_count(&self) -> usize {
        self.map_unsigned_integers.len()
    }
    /// Column name at `ci`; panics for an invalid column index.
    pub fn map_unsigned_integer_name(&self, ci: usize) -> &str {
        &self.map_unsigned_integers[ci].name
    }
    /// Look up an exact u64 map column by name.
    pub fn map_unsigned_integer_index(&self, name: &str) -> Option<usize> {
        self.map_unsigned_integers
            .iter()
            .position(|c| c.name == name)
    }
    /// Map keys in this reader's ordinal order.
    pub fn map_unsigned_integer_keys(&self, ci: usize) -> &[String] {
        &self.map_unsigned_integers[ci].reader.keys
    }
    /// Find a key, including the empty string.
    pub fn map_unsigned_integer_key_ord(&self, ci: usize, key: &str) -> Option<u32> {
        self.map_unsigned_integers[ci].reader.key_ord(key)
    }
    /// Exact bounds, or `None` when the key has no entries.
    pub fn map_unsigned_integer_key_min_max(&self, ci: usize, key: u32) -> Option<(u64, u64)> {
        self.map_unsigned_integers[ci].reader.bounds(key)
    }
    /// Exact value; an absent entry or out-of-range key or row returns `None`.
    pub fn map_unsigned_integer_value(&self, ci: usize, key: u32, row: u32) -> Option<u64> {
        let column = &self.map_unsigned_integers[ci];
        column
            .reader
            .value(&self.map[column.off..column.off + column.len], key, row)
    }

    /// Number of map-numeric columns.
    pub fn map_numeric_count(&self) -> usize {
        self.map_numerics.len()
    }

    /// The name of map-numeric column `ci`.
    pub fn map_numeric_name(&self, ci: usize) -> &str {
        &self.map_numerics[ci].name
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
        dictionary_lookup(
            &self.map_facets[ci].values,
            self.map_facets[ci].values_sorted,
            value,
        )
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
                positions_off: None,
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
        })
    }

    /// Open a validated v6 or v7 map, one [`FieldSlice`] per
    /// field-table entry (plus one [`FacetSlice`] per v7 facet-table
    /// entry). Every field's sections are v5-shaped, so the entire v5
    /// read machinery serves each field with run offsets rebased by
    /// that field's postings section offset.
    pub(crate) fn open_v6v7(map: memmap2::Mmap, v7: bool) -> io::Result<Self> {
        let len = map.len();
        Self::open_v6v7_bounded(map, v7, len)
    }

    /// [`Self::open_v6v7`] with the payload's byte length made
    /// explicit: a v8 file carries its integrity section and trailer
    /// AFTER the payload, and the structural validator derives the
    /// last section's extent from where the bytes end, so it must see
    /// the payload's end, not the file's.
    pub(crate) fn open_v6v7_bounded(
        map: memmap2::Mmap,
        v7: bool,
        payload_len: usize,
    ) -> io::Result<Self> {
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
                        // Validation tied the entry to a field of this
                        // table and checked the section's geometry.
                        let field = name
                            .strip_prefix(POSITIONS_ENTRY_PREFIX)
                            .expect("validation checked the kind-7 name");
                        let fi = fields
                            .iter()
                            .position(|f| f.name == field)
                            .expect("validation tied the kind-7 entry to a field");
                        fields[fi].positions_off = Some(u64_at(base));
                        cursor = base + 16;
                    }
                    COLUMN_KIND_SENTENCES => {
                        let field = name
                            .strip_prefix(SENTENCES_ENTRY_PREFIX)
                            .expect("validation checked the kind-8 name");
                        let fi = fields
                            .iter()
                            .position(|f| f.name == field)
                            .expect("validation tied the kind-8 entry to a field");
                        fields[fi].sentences_off = Some(u64_at(base));
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
                    COLUMN_KIND_BINDING
                    | COLUMN_KIND_ANALYSIS_BINDING
                    | COLUMN_KIND_VECTOR_BINDING
                    | COLUMN_KIND_INDEX_BINDING => {
                        let mut vals: Vec<String> = Vec::with_capacity(3);
                        let mut cur = base;
                        for _ in 0..if kind != COLUMN_KIND_BINDING { 4 } else { 3 } {
                            let len =
                                u16::from_le_bytes(map[cur..cur + 2].try_into().unwrap()) as usize;
                            vals.push(
                                String::from_utf8_lossy(&map[cur + 2..cur + 2 + len]).into_owned(),
                            );
                            cur += 2 + len;
                        }
                        let analysis_contract = if kind != COLUMN_KIND_BINDING {
                            let len =
                                u32::from_le_bytes(map[cur..cur + 4].try_into().unwrap()) as usize;
                            let bytes = map[cur + 4..cur + 4 + len].to_vec();
                            cur += 4 + len;
                            bytes
                        } else {
                            Vec::new()
                        };
                        let vector_binding = if matches!(
                            kind,
                            COLUMN_KIND_VECTOR_BINDING | COLUMN_KIND_INDEX_BINDING
                        ) {
                            let len =
                                u32::from_le_bytes(map[cur..cur + 4].try_into().unwrap()) as usize;
                            let bytes = map[cur + 4..cur + 4 + len].to_vec();
                            cur += 4 + len;
                            bytes
                        } else {
                            Vec::new()
                        };
                        let index_contract = if kind == COLUMN_KIND_INDEX_BINDING {
                            let len =
                                u32::from_le_bytes(map[cur..cur + 4].try_into().unwrap()) as usize;
                            let bytes = map[cur + 4..cur + 4 + len].to_vec();
                            cur += 4 + len;
                            bytes
                        } else {
                            Vec::new()
                        };
                        let mut it = vals.into_iter();
                        binding = Some(StoredBinding {
                            plan_fingerprint: it.next().expect("three strings"),
                            body_path: it.next().expect("three strings"),
                            materialize_sha: it.next().expect("three strings"),
                            analysis_sha: it.next().unwrap_or_default(),
                            analysis_contract,
                            vector_binding,
                            index_contract,
                        });
                        cursor = cur;
                    }
                    COLUMN_KIND_DERIVED => {
                        let mut cur = base;
                        let len =
                            u16::from_le_bytes(map[cur..cur + 2].try_into().unwrap()) as usize;
                        let fingerprint =
                            String::from_utf8_lossy(&map[cur + 2..cur + 2 + len]).into_owned();
                        cur += 2 + len;
                        let len =
                            u32::from_le_bytes(map[cur..cur + 4].try_into().unwrap()) as usize;
                        let declaration = map[cur + 4..cur + 4 + len].to_vec();
                        cur += 4 + len;
                        derived = Some(StoredDerived {
                            fingerprint,
                            declaration,
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
        })
    }

    pub(crate) fn directory_entry(&self, field: &FieldSlice, i: u32) -> (&[u8], u64, u32) {
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
    pub(crate) fn directory_entry_v5(
        &self,
        field: &FieldSlice,
        i: u32,
    ) -> (&[u8], u64, u64, u64, u32) {
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

    pub(crate) fn directory_lookup(&self, field: &FieldSlice, term: &str) -> Option<(u64, u32)> {
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
    pub(crate) fn directory_lookup_v5(
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
    pub(crate) fn directory_index_v5(&self, field: &FieldSlice, term: &str) -> Option<u32> {
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
    pub(crate) fn v5_posting_index(
        &self,
        doc_run_off: usize,
        df: u32,
        doc_id: u32,
    ) -> Option<usize> {
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

    pub(crate) fn u32_at(&self, off: usize) -> u32 {
        u32::from_le_bytes(self.map[off..off + 4].try_into().unwrap())
    }

    /// The v5 doc-run entry for posting `i`: `(doc_id, tf, occ_start)`.
    pub(crate) fn v5_doc_entry(&self, doc_run_off: usize, i: usize) -> (u32, u32, u32) {
        let e = doc_run_off + 12 * i;
        (self.u32_at(e), self.u32_at(e + 4), self.u32_at(e + 8))
    }

    /// The occ_start of posting `j`; `j == df` reads the trailing
    /// sentinel, so any posting's occurrence count is
    /// `v5_occ_start(j + 1) - v5_occ_start(j)` without a scan.
    pub(crate) fn v5_occ_start(&self, doc_run_off: usize, df: usize, j: usize) -> u32 {
        if j < df {
            self.u32_at(doc_run_off + 12 * j + 8)
        } else {
            self.u32_at(doc_run_off + 12 * df) // sentinel
        }
    }

    /// Decode the occurrence pairs `[occ_start, occ_end)` of a term's
    /// occurrence run.
    pub(crate) fn v5_occ_slice(
        &self,
        occ_run_off: usize,
        occ_start: u32,
        occ_end: u32,
    ) -> Vec<(u32, u32)> {
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
    pub(crate) fn v5_l0_records(&self, skip_run_off: u64, df: u32) -> Vec<(u32, Vec<(u32, u32)>)> {
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
    pub(crate) fn v5_for_each_doc_tf(
        &self,
        doc_run_off: usize,
        df: u32,
        f: &mut dyn FnMut(u32, u32),
    ) {
        for i in 0..df as usize {
            let (doc_id, tf, _) = self.v5_doc_entry(doc_run_off, i);
            f(doc_id, tf);
        }
    }

    pub(crate) fn v3_for_each_posting(&self, off: u64, df: u32, f: &mut PostingCallback) {
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

impl Bm25Reader {
    /// The shared stored text of a document, if present.
    pub(crate) fn read_text(&self, doc_id: u32) -> Option<String> {
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
    pub(crate) fn read_lineage(&self, doc_id: u32) -> Option<DocLineage> {
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
