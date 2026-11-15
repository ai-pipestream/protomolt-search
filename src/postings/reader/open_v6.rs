//! v6/v7 header and column-table decoding for `Bm25Reader`.

use std::io;

use super::super::columns::{
    decode_binding_entry, decode_derived_entry, dictionary_sorted, FacetSlice, GeoSlice,
    IntegerMapSlice, IntegerSlice, MapFacetSlice, MapNumericSlice, NumericSlice, StoredBinding,
    StoredDerived, UnsignedSlice,
};
use super::super::format::{
    COLUMN_KIND_ANALYSIS_BINDING, COLUMN_KIND_BINDING, COLUMN_KIND_DERIVED, COLUMN_KIND_F64,
    COLUMN_KIND_FACET, COLUMN_KIND_GEO, COLUMN_KIND_I64, COLUMN_KIND_I64_PRESENT,
    COLUMN_KIND_INDEX_BINDING, COLUMN_KIND_MAP_F64, COLUMN_KIND_MAP_FACET, COLUMN_KIND_MAP_I64,
    COLUMN_KIND_MAP_U64, COLUMN_KIND_POSITIONS, COLUMN_KIND_POS_LENGTHS, COLUMN_KIND_SENTENCES,
    COLUMN_KIND_SOURCES, COLUMN_KIND_SPLADE, COLUMN_KIND_U64, COLUMN_KIND_VECTOR_BINDING,
    POSITIONS_ENTRY_PREFIX, POS_LENGTHS_ENTRY_PREFIX, SENTENCES_ENTRY_PREFIX, SPLADE_ENTRY_PREFIX,
};
use super::{validate_structure_v6, Bm25Reader, FieldSlice, SortedNumbers};

impl Bm25Reader {
    /// Open a validated v6 or v7 map, one [`FieldSlice`] per field-table entry
    /// (plus typed and auxiliary column slices on v7 files). Run offsets are
    /// rebased by each field's postings section offset.
    pub(super) fn open_v6v7(map: memmap2::Mmap, v7: bool) -> io::Result<Self> {
        let len = map.len();
        Self::open_v6v7_bounded(map, v7, len)
    }

    /// [`Self::open_v6v7`] with the payload's byte length made
    /// explicit: a v8 file carries its integrity section and trailer
    /// AFTER the payload, and the structural validator derives the
    /// last section's extent from where the bytes end, so it must see
    /// the payload's end, not the file's.
    pub(super) fn open_v6v7_bounded(
        map: memmap2::Mmap,
        v7: bool,
        payload_len: usize,
    ) -> io::Result<Self> {
        validate_structure_v6(&map[..payload_len], v7)?;
        let n_fields = u32_at(&map, 8) as usize;
        let n_slots = u32_at(&map, 12) as usize;
        let texts_off = u64_at(&map, 16);
        let text_index_off = u64_at(&map, 24);
        let lineages_off = u64_at(&map, 32);
        let mut cursor = 40usize;
        let mut fields = decode_field_slices(&map, n_fields, n_slots, &mut cursor);
        let mut cols = DecodedV7Columns::default();
        if v7 {
            let n_columns = u32_at(&map, cursor) as usize;
            cursor += 4;
            for _ in 0..n_columns {
                cols.decode_next(&map, n_slots, &mut fields, &mut cursor)?;
            }
        }
        let doc_count = (0..n_slots)
            .filter(|&slot| fields.iter().any(|f| f.doc_lengths[slot] > 0))
            .count() as u64;
        Ok(Self {
            map,
            fields,
            facets: cols.facets,
            numerics: cols.numerics,
            map_facets: cols.map_facets,
            map_numerics: cols.map_numerics,
            map_integers: cols.map_integers,
            map_unsigned_integers: cols.map_unsigned_integers,
            integers: cols.integers,
            unsigned_integers: cols.unsigned_integers,
            geos: cols.geos,
            doc_count,
            lineages_off,
            text_index_off,
            text_base: texts_off,
            binding: cols.binding,
            derived: cols.derived,
            source_section: cols.source_section,
            lineage_index: std::sync::OnceLock::new(),
            integrity: None,
            facet_rows: std::sync::OnceLock::new(),
            number_rows: SortedNumbers::default(),
        })
    }
}

fn u32_at(map: &[u8], off: usize) -> u32 {
    u32::from_le_bytes(map[off..off + 4].try_into().unwrap())
}

fn u64_at(map: &[u8], off: usize) -> u64 {
    u64::from_le_bytes(map[off..off + 8].try_into().unwrap())
}

fn read_dict(map: &[u8], off: usize, n: usize) -> Vec<String> {
    let mut entries = Vec::with_capacity(n);
    let mut cur = off;
    for _ in 0..n {
        let len = u16::from_le_bytes(map[cur..cur + 2].try_into().unwrap()) as usize;
        entries.push(String::from_utf8_lossy(&map[cur + 2..cur + 2 + len]).into_owned());
        cur += 2 + len;
    }
    entries
}

fn decode_field_slices(
    map: &[u8],
    n_fields: usize,
    n_slots: usize,
    cursor: &mut usize,
) -> Vec<FieldSlice> {
    let mut fields = Vec::with_capacity(n_fields);
    for _ in 0..n_fields {
        let name_len = u16::from_le_bytes(map[*cursor..*cursor + 2].try_into().unwrap()) as usize;
        let name = String::from_utf8_lossy(&map[*cursor + 2..*cursor + 2 + name_len]).into_owned();
        let base = *cursor + 2 + name_len;
        let analysis_fingerprint = u64_at(map, base);
        let total_length = u64_at(map, base + 8);
        let doc_lengths_off = u64_at(map, base + 16) as usize;
        let postings_off = u64_at(map, base + 24);
        let directory_off = u64_at(map, base + 32);
        let mut doc_lengths = Vec::with_capacity(n_slots);
        for slot in 0..n_slots {
            doc_lengths.push(u32_at(map, doc_lengths_off + 4 * slot));
        }
        fields.push(FieldSlice {
            name,
            analysis_fingerprint,
            doc_lengths,
            total_length,
            directory_off,
            n_terms: u32_at(map, directory_off as usize),
            run_base: postings_off,
            positions_off: None,
            pos_lengths_off: None,
            splade_range: None,
            sentences_off: None,
        });
        *cursor = base + 40;
    }
    fields
}

#[derive(Default)]
struct DecodedV7Columns {
    facets: Vec<FacetSlice>,
    numerics: Vec<NumericSlice>,
    map_facets: Vec<MapFacetSlice>,
    map_numerics: Vec<MapNumericSlice>,
    map_integers: Vec<IntegerMapSlice<i64>>,
    map_unsigned_integers: Vec<IntegerMapSlice<u64>>,
    integers: Vec<IntegerSlice>,
    unsigned_integers: Vec<UnsignedSlice>,
    geos: Vec<GeoSlice>,
    binding: Option<StoredBinding>,
    derived: Option<StoredDerived>,
    source_section: Option<(u64, u64, crate::source_archive::SourceArchiveReader)>,
}

impl DecodedV7Columns {
    fn decode_next(
        &mut self,
        map: &[u8],
        n_slots: usize,
        fields: &mut [FieldSlice],
        cursor: &mut usize,
    ) -> io::Result<()> {
        let name_len = u16::from_le_bytes(map[*cursor..*cursor + 2].try_into().unwrap()) as usize;
        let name = String::from_utf8_lossy(&map[*cursor + 2..*cursor + 2 + name_len]).into_owned();
        let kind = map[*cursor + 2 + name_len];
        let base = *cursor + 2 + name_len + 1;
        if let Some(next) = self.decode_scalar_column(map, n_slots, &name, kind, base) {
            *cursor = next;
            return Ok(());
        }
        if let Some(next) = self.decode_map_column(map, n_slots, &name, kind, base)? {
            *cursor = next;
            return Ok(());
        }
        *cursor = self.decode_meta_column(map, n_slots, fields, (&name, kind, base))?;
        Ok(())
    }

    fn decode_scalar_column(
        &mut self,
        map: &[u8],
        n_slots: usize,
        name: &str,
        kind: u8,
        base: usize,
    ) -> Option<usize> {
        let name = name.to_string();
        let next = match kind {
            COLUMN_KIND_FACET => {
                let dict = read_dict(
                    map,
                    u64_at(map, base + 4) as usize,
                    u32_at(map, base) as usize,
                );
                let sorted = dictionary_sorted(&dict);
                self.facets.push(FacetSlice {
                    name,
                    dict,
                    sorted,
                    ords_off: u64_at(map, base + 12),
                });
                base + 20
            }
            COLUMN_KIND_F64 => {
                self.numerics.push(NumericSlice {
                    name,
                    min: f64::from_bits(u64_at(map, base)),
                    max: f64::from_bits(u64_at(map, base + 8)),
                    vals_off: u64_at(map, base + 16),
                });
                base + 24
            }
            COLUMN_KIND_I64 | COLUMN_KIND_I64_PRESENT => {
                self.integers.push(IntegerSlice {
                    name,
                    min: u64_at(map, base) as i64,
                    max: u64_at(map, base + 8) as i64,
                    vals_off: u64_at(map, base + 16),
                    presence_off: (kind == COLUMN_KIND_I64_PRESENT)
                        .then(|| u64_at(map, base + 16) + 8 * n_slots as u64),
                });
                base + 24
            }
            COLUMN_KIND_U64 => {
                self.unsigned_integers.push(UnsignedSlice {
                    name,
                    min: u64_at(map, base),
                    max: u64_at(map, base + 8),
                    vals_off: u64_at(map, base + 16),
                });
                base + 24
            }
            COLUMN_KIND_GEO => {
                self.geos.push(GeoSlice {
                    name,
                    bbox: (
                        f64::from_bits(u64_at(map, base)),
                        f64::from_bits(u64_at(map, base + 8)),
                        f64::from_bits(u64_at(map, base + 16)),
                        f64::from_bits(u64_at(map, base + 24)),
                    ),
                    vals_off: u64_at(map, base + 32),
                });
                base + 40
            }
            _ => return None,
        };
        Some(next)
    }

    fn decode_map_column(
        &mut self,
        map: &[u8],
        n_slots: usize,
        name: &str,
        kind: u8,
        base: usize,
    ) -> io::Result<Option<usize>> {
        let name = name.to_string();
        let next = match kind {
            COLUMN_KIND_MAP_FACET => {
                self.map_facets
                    .push(decode_map_facet_slice(map, name, base));
                base + 40
            }
            COLUMN_KIND_MAP_F64 => {
                self.map_numerics
                    .push(decode_map_numeric_slice(map, name, base));
                base + 28
            }
            COLUMN_KIND_MAP_I64 | COLUMN_KIND_MAP_U64 => {
                let off = u64_at(map, base) as usize;
                let len = u64_at(map, base + 8) as usize;
                let bytes = &map[off..off + len];
                if kind == COLUMN_KIND_MAP_I64 {
                    let reader = crate::integer_map::Reader::open(bytes, n_slots as u32)?;
                    self.map_integers.push(IntegerMapSlice {
                        name,
                        off,
                        len,
                        reader,
                    });
                } else {
                    let reader = crate::integer_map::Reader::open(bytes, n_slots as u32)?;
                    self.map_unsigned_integers.push(IntegerMapSlice {
                        name,
                        off,
                        len,
                        reader,
                    });
                }
                base + 16
            }
            _ => return Ok(None),
        };
        Ok(Some(next))
    }

    fn decode_meta_column(
        &mut self,
        map: &[u8],
        n_slots: usize,
        fields: &mut [FieldSlice],
        entry: (&str, u8, usize),
    ) -> io::Result<usize> {
        let (name, kind, base) = entry;
        match kind {
            COLUMN_KIND_POSITIONS => {
                field_by_prefixed_name(fields, name, POSITIONS_ENTRY_PREFIX, kind).positions_off =
                    Some(u64_at(map, base));
                Ok(base + 16)
            }
            COLUMN_KIND_SENTENCES => {
                field_by_prefixed_name(fields, name, SENTENCES_ENTRY_PREFIX, kind).sentences_off =
                    Some(u64_at(map, base));
                Ok(base + 16)
            }
            COLUMN_KIND_SOURCES => {
                let off = u64_at(map, base);
                let len = u64_at(map, base + 8);
                let reader = crate::source_archive::SourceArchiveReader::open(
                    &map[off as usize..(off + len) as usize],
                    n_slots as u32,
                )?;
                self.source_section = Some((off, len, reader));
                Ok(base + 16)
            }
            COLUMN_KIND_POS_LENGTHS => {
                field_by_prefixed_name(fields, name, POS_LENGTHS_ENTRY_PREFIX, kind)
                    .pos_lengths_off = Some(u64_at(map, base));
                Ok(base + 16)
            }
            COLUMN_KIND_SPLADE => {
                field_by_prefixed_name(fields, name, SPLADE_ENTRY_PREFIX, kind).splade_range =
                    Some((u64_at(map, base), u64_at(map, base + 8)));
                Ok(base + 16)
            }
            COLUMN_KIND_BINDING
            | COLUMN_KIND_ANALYSIS_BINDING
            | COLUMN_KIND_VECTOR_BINDING
            | COLUMN_KIND_INDEX_BINDING => {
                let (decoded, used) = decode_binding_entry(kind, &map[base..])?;
                self.binding = Some(decoded);
                Ok(base + used)
            }
            COLUMN_KIND_DERIVED => {
                let (decoded, used) = decode_derived_entry(&map[base..])?;
                self.derived = Some(decoded);
                Ok(base + used)
            }
            k => unreachable!("validation refused unknown column kind {k}"),
        }
    }
}

fn decode_map_facet_slice(map: &[u8], name: String, base: usize) -> MapFacetSlice {
    let keys = read_dict(
        map,
        u64_at(map, base + 8) as usize,
        u32_at(map, base) as usize,
    );
    let values = read_dict(
        map,
        u64_at(map, base + 16) as usize,
        u32_at(map, base + 4) as usize,
    );
    let keys_sorted = dictionary_sorted(&keys);
    let values_sorted = dictionary_sorted(&values);
    MapFacetSlice {
        name,
        keys,
        values,
        keys_sorted,
        values_sorted,
        offsets_off: u64_at(map, base + 24),
        pairs_off: u64_at(map, base + 32),
    }
}

fn decode_map_numeric_slice(map: &[u8], name: String, base: usize) -> MapNumericSlice {
    let n_keys = u32_at(map, base) as usize;
    let mut cur = u64_at(map, base + 4) as usize;
    let mut keys = Vec::with_capacity(n_keys);
    let mut key_min_max = Vec::with_capacity(n_keys);
    for _ in 0..n_keys {
        let len = u16::from_le_bytes(map[cur..cur + 2].try_into().unwrap()) as usize;
        keys.push(String::from_utf8_lossy(&map[cur + 2..cur + 2 + len]).into_owned());
        key_min_max.push((
            f64::from_bits(u64_at(map, cur + 2 + len)),
            f64::from_bits(u64_at(map, cur + 2 + len + 8)),
        ));
        cur += 2 + len + 16;
    }
    MapNumericSlice {
        name,
        keys,
        key_min_max,
        offsets_off: u64_at(map, base + 12),
        pairs_off: u64_at(map, base + 20),
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
