//! Structural and columnar validation for `.bm25` v6/v7 payloads.

use std::io;

use super::super::columns::{
    decode_binding_entry, decode_derived_entry, integer_present, FACET_ABSENT, INTEGER_ABSENT,
};
use super::super::format::{
    compatible_column_kind, positions_section_size, sentences_section_size, u16_at,
    BINDING_ENTRY_NAME, COLUMN_KIND_ANALYSIS_BINDING, COLUMN_KIND_BINDING, COLUMN_KIND_DERIVED,
    COLUMN_KIND_F64, COLUMN_KIND_FACET, COLUMN_KIND_GEO, COLUMN_KIND_I64, COLUMN_KIND_I64_PRESENT,
    COLUMN_KIND_INDEX_BINDING, COLUMN_KIND_MAP_F64, COLUMN_KIND_MAP_FACET, COLUMN_KIND_MAP_I64,
    COLUMN_KIND_MAP_U64, COLUMN_KIND_POSITIONS, COLUMN_KIND_POS_LENGTHS, COLUMN_KIND_SENTENCES,
    COLUMN_KIND_SOURCES, COLUMN_KIND_SPLADE, COLUMN_KIND_U64, COLUMN_KIND_VECTOR_BINDING,
    DERIVED_ENTRY_NAME, POSITIONS_ENTRY_PREFIX, POS_LENGTHS_ENTRY_PREFIX, SENTENCES_ENTRY_PREFIX,
    SOURCES_ENTRY_NAME, SPLADE_ENTRY_PREFIX,
};
#[cfg(doc)]
use super::validate_v5::validate_structure;
use super::validate_v5::validate_v5_directory;

/// Validate the text index: every present entry within the texts
/// section, absent markers untouched. `entry_base` rebases stored
/// offsets: the entries are absolute in v3/v4/v5 (base 0) and relative
/// to the texts section start in v6 (base `texts_off`).
pub(super) fn validate_text_index(
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
pub(super) fn lineage_section_end(map: &[u8], lineages_off: u64, n_slots: u64) -> io::Result<u64> {
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
pub(in crate::postings) fn validate_structure_v6(map: &[u8], v7: bool) -> io::Result<()> {
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
    let mut integers: Vec<(u64, u64, u64, bool)> = Vec::new();
    let mut unsigned_integers: Vec<(u64, u64, u64)> = Vec::new();
    // (min_lat, max_lat, min_lon, max_lon bits, vals)
    let mut geos: Vec<(u64, u64, u64, u64, u64)> = Vec::new();
    // (field name, section_off, total occurrences) per kind-7 entry.
    let mut positions: Vec<(Vec<u8>, u64, u64)> = Vec::new();
    let mut sentences: Vec<(Vec<u8>, u64, u64)> = Vec::new();
    let mut pos_lengths: Vec<(Vec<u8>, u64, u64)> = Vec::new();
    let mut splades: Vec<(Vec<u8>, u64, u64)> = Vec::new();
    let mut source_section = None;
    let mut integer_maps = Vec::new();
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
            let kind = compatible_column_kind(
                kind,
                column_names.last().unwrap(),
                &map[base as usize..],
                map.len() as u64,
            );
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
                COLUMN_KIND_MAP_I64 | COLUMN_KIND_MAP_U64 => {
                    integer_maps.push((kind, u64_at(base)?, u64_at(base + 8)?));
                    cursor = base + 16;
                }
                COLUMN_KIND_I64 | COLUMN_KIND_I64_PRESENT => {
                    integers.push((
                        u64_at(base)?,
                        u64_at(base + 8)?,
                        u64_at(base + 16)?,
                        kind == COLUMN_KIND_I64_PRESENT,
                    ));
                    cursor = base + 24;
                }
                COLUMN_KIND_U64 => {
                    unsigned_integers.push((u64_at(base)?, u64_at(base + 8)?, u64_at(base + 16)?));
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
                COLUMN_KIND_POSITIONS => {
                    push_prefixed_field_entry(
                        &mut positions,
                        &names,
                        column_names.last().expect("pushed above"),
                        (u64_at(base)?, u64_at(base + 8)?),
                        PrefixedEntrySpec {
                            col_idx: i,
                            kind,
                            prefix: POSITIONS_ENTRY_PREFIX,
                            label: "positions",
                        },
                    )?;
                    cursor = base + 16;
                }
                COLUMN_KIND_SENTENCES => {
                    push_prefixed_field_entry(
                        &mut sentences,
                        &names,
                        column_names.last().expect("pushed above"),
                        (u64_at(base)?, u64_at(base + 8)?),
                        PrefixedEntrySpec {
                            col_idx: i,
                            kind,
                            prefix: SENTENCES_ENTRY_PREFIX,
                            label: "sentences",
                        },
                    )?;
                    cursor = base + 16;
                }
                COLUMN_KIND_SOURCES => {
                    if column_names.last().map(Vec::as_slice) != Some(SOURCES_ENTRY_NAME.as_bytes())
                    {
                        return Err(invalid("invalid source entry name".into()));
                    }
                    source_section = Some((u64_at(base)?, u64_at(base + 8)?));
                    cursor = base + 16;
                }
                COLUMN_KIND_BINDING
                | COLUMN_KIND_ANALYSIS_BINDING
                | COLUMN_KIND_VECTOR_BINDING
                | COLUMN_KIND_INDEX_BINDING => {
                    if column_names.last().map(Vec::as_slice) != Some(BINDING_ENTRY_NAME.as_bytes())
                    {
                        return Err(invalid(format!(
                            "column {i}: kind {COLUMN_KIND_BINDING} must be named \
                             {BINDING_ENTRY_NAME:?}"
                        )));
                    }
                    let (_, used) = decode_binding_entry(kind, &map[base as usize..])?;
                    cursor = base + used as u64;
                }
                COLUMN_KIND_DERIVED => {
                    if column_names.last().map(Vec::as_slice) != Some(DERIVED_ENTRY_NAME.as_bytes())
                    {
                        return Err(invalid(format!(
                            "column {i}: kind {COLUMN_KIND_DERIVED} must be named \
                             {DERIVED_ENTRY_NAME:?}"
                        )));
                    }
                    let (_, used) = decode_derived_entry(&map[base as usize..])?;
                    cursor = base + used as u64;
                }
                COLUMN_KIND_POS_LENGTHS => {
                    push_prefixed_field_entry(
                        &mut pos_lengths,
                        &names,
                        column_names.last().expect("pushed above"),
                        (u64_at(base)?, u64_at(base + 8)?),
                        PrefixedEntrySpec {
                            col_idx: i,
                            kind,
                            prefix: POS_LENGTHS_ENTRY_PREFIX,
                            label: "pos_lengths",
                        },
                    )?;
                    cursor = base + 16;
                }
                COLUMN_KIND_SPLADE => {
                    push_prefixed_field_entry(
                        &mut splades,
                        &names,
                        column_names.last().expect("pushed above"),
                        (u64_at(base)?, u64_at(base + 8)?),
                        PrefixedEntrySpec {
                            col_idx: i,
                            kind,
                            prefix: SPLADE_ENTRY_PREFIX,
                            label: "splade",
                        },
                    )?;
                    cursor = base + 16;
                }
                k => {
                    return Err(invalid(format!(
                        "column {i}: kind {k} unknown to this binary"
                    )))
                }
            }
        }
    }
    let tail_sections_start = pos_lengths
        .first()
        .map(|p| p.1)
        .or_else(|| splades.first().map(|s| s.1))
        .unwrap_or(file_len);
    let integer_maps_end = source_section.map_or(tail_sections_start, |(off, _)| off);
    let unsigned_end = integer_maps
        .first()
        .map_or(integer_maps_end, |entry| entry.1);
    let columns_end = unsigned_integers
        .first()
        .map_or(unsigned_end, |entry| entry.2);
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
        .or_else(|| positions.first().map(|p| p.1))
        .or_else(|| sentences.first().map(|s| s.1))
        .unwrap_or(columns_end);
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
            .or_else(|| positions.first().map(|p| p.1))
            .unwrap_or(columns_end);
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
            .or_else(|| positions.first().map(|p| p.1))
            .unwrap_or(columns_end);
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
            .or_else(|| positions.first().map(|p| p.1))
            .unwrap_or(columns_end);
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
            .or_else(|| positions.first().map(|p| p.1))
            .unwrap_or(columns_end);
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
    // Integer groups: values plus optional presence, tiling without gaps.
    // Metadata is checked against present values. Legacy kind 4 keeps its
    // sentinel semantics; kind 10 validates the bitmap and canonical absences.
    for (i, &(min_bits, max_bits, vals_off, has_presence)) in integers.iter().enumerate() {
        if vals_off != expected_start {
            return Err(invalid(format!(
                "integer field {i}: vals section does not start at the previous section's end"
            )));
        }
        let bitmap_off = vals_off + 8 * n_slots;
        let group_end = bitmap_off + if has_presence { n_slots.div_ceil(8) } else { 0 };
        let expected_end = integers
            .get(i + 1)
            .map(|c| c.2)
            .or_else(|| geos.first().map(|c| c.4))
            .or_else(|| positions.first().map(|p| p.1))
            .unwrap_or(columns_end);
        if group_end != expected_end {
            return Err(invalid(format!(
                "integer field {i}: vals section does not end at the next section's start"
            )));
        }
        let bitmap = if has_presence {
            let bits = bytes_at(bitmap_off, n_slots.div_ceil(8))?;
            if n_slots % 8 != 0 && bits.last().is_some_and(|b| b >> (n_slots % 8) != 0) {
                return Err(invalid(format!(
                    "integer field {i}: nonzero presence padding"
                )));
            }
            Some(bits)
        } else {
            None
        };
        let mut min = i64::MAX;
        let mut max = i64::MIN;
        for slot in 0..n_slots {
            let v = u64_at(vals_off + 8 * slot)? as i64;
            if !bitmap.map_or(v != INTEGER_ABSENT, |bits| {
                integer_present(bits, slot as usize)
            }) {
                if has_presence && v != 0 {
                    return Err(invalid(format!(
                        "integer field {i}: nonzero absent value at slot {slot}"
                    )));
                }
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
        let expected_end = geos
            .get(i + 1)
            .map(|c| c.4)
            .or_else(|| positions.first().map(|p| p.1))
            .unwrap_or(columns_end);
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
    // Positions groups (docs/phrase-proximity.md), last, tiling to EOF.
    // Structure is checked here: the section belongs to a field, its
    // term count matches that field's directory, the base table is
    // monotone from 0 and ends at the declared total, and the section
    // is exactly the size that geometry implies. The ordinals themselves
    // are payload (CRC-covered, checked by the deep pass); reading all
    // of them at open would fault in the whole section, which is the
    // paging model this file exists to preserve.
    for (i, (field_name, off, total)) in positions.iter().enumerate() {
        let fi = names
            .iter()
            .position(|n| n == field_name)
            .expect("kind-7 entries were tied to a field above");
        let n_terms = u64::from(u32_at(fields[fi].3)?);
        let expected_end = positions
            .get(i + 1)
            .map(|p| p.1)
            .or_else(|| sentences.first().map(|s| s.1))
            .unwrap_or(columns_end);
        expected_start = validate_term_base_section(
            map,
            TermSectionCheck {
                label: "positions",
                index: i,
                field_name,
                off: *off,
                total: *total,
                n_terms,
                bounds: (expected_start, expected_end),
            },
        )?;
    }
    // Sentence groups (docs/highlighting.md) after the positions groups,
    // tiling to EOF: slot count equals the file's, base table monotone
    // from 0 to the declared total, size exactly the geometry's. The
    // spans are payload (CRC-covered), left on disk at open.
    let n_slots = u64::from(u32_at(12)?);
    for (i, (field_name, off, total)) in sentences.iter().enumerate() {
        if *off != expected_start {
            return Err(invalid(format!(
                "sentences {i}: section does not start at the previous section's end"
            )));
        }
        if u64::from(u32_at(*off)?) != n_slots {
            return Err(invalid(format!(
                "sentences {i}: slot count disagrees with the file's for field {:?}",
                String::from_utf8_lossy(field_name)
            )));
        }
        let mut prev = 0u64;
        for d in 0..=n_slots {
            let base = u64::from(u32_at(off + 4 + 4 * d)?);
            if base < prev {
                return Err(invalid(format!(
                    "sentences {i}: base table not monotone at slot {d}"
                )));
            }
            prev = base;
        }
        if prev != *total {
            return Err(invalid(format!(
                "sentences {i}: base table ends at {prev} but the entry declares {total}"
            )));
        }
        let group_end = off + sentences_section_size(n_slots, *total);
        let expected_end = sentences.get(i + 1).map_or(columns_end, |s| s.1);
        if group_end != expected_end {
            return Err(invalid(format!(
                "sentences {i}: section does not end at the next section's start"
            )));
        }
        expected_start = group_end;
    }
    for (i, &(stored_min, stored_max, off)) in unsigned_integers.iter().enumerate() {
        let bitmap_off = off + 8 * n_slots;
        let end = bitmap_off + n_slots.div_ceil(8);
        let expected_end = unsigned_integers
            .get(i + 1)
            .map_or(unsigned_end, |entry| entry.2);
        if off != expected_start || end != expected_end {
            return Err(invalid(format!(
                "unsigned integer field {i}: sections do not tile the file"
            )));
        }
        let bitmap = bytes_at(bitmap_off, n_slots.div_ceil(8))?;
        if n_slots % 8 != 0 && bitmap.last().is_some_and(|b| b >> (n_slots % 8) != 0) {
            return Err(invalid(format!(
                "unsigned integer field {i}: nonzero presence padding"
            )));
        }
        let (mut min, mut max) = (u64::MAX, u64::MIN);
        for slot in 0..n_slots {
            let value = u64_at(off + 8 * slot)?;
            if integer_present(bitmap, slot as usize) {
                min = min.min(value);
                max = max.max(value);
            } else if value != 0 {
                return Err(invalid(format!(
                    "unsigned integer field {i}: nonzero absent value"
                )));
            }
        }
        if (min, max) != (stored_min, stored_max) {
            return Err(invalid(format!(
                "unsigned integer field {i}: min/max metadata disagrees with values"
            )));
        }
        expected_start = end;
    }
    for &(kind, off, len) in &integer_maps {
        let end = off
            .checked_add(len)
            .ok_or_else(|| invalid("integer map section overflow".into()))?;
        if off != expected_start || end > integer_maps_end {
            return Err(invalid("integer map sections do not tile the file".into()));
        }
        if kind == COLUMN_KIND_MAP_I64 {
            crate::integer_map::Reader::<i64>::open(bytes_at(off, len)?, n_slots as u32)?;
        } else {
            crate::integer_map::Reader::<u64>::open(bytes_at(off, len)?, n_slots as u32)?;
        }
        expected_start = end;
    }
    if expected_start != integer_maps_end {
        return Err(invalid(
            "column sections leave a gap before sources or EOF".into(),
        ));
    }
    if let Some((off, len)) = source_section {
        let end = off
            .checked_add(len)
            .ok_or_else(|| invalid("source section overflow".into()))?;
        if off != expected_start || end != tail_sections_start {
            return Err(invalid("source section does not tile the file".into()));
        }
        crate::source_archive::SourceArchiveReader::open(bytes_at(off, len)?, n_slots as u32)?;
        expected_start = end;
    }
    for (i, (field_name, off, total)) in pos_lengths.iter().enumerate() {
        if !positions.iter().any(|(f, _, _)| f == field_name) {
            return Err(invalid(format!(
                "pos_lengths {i}: field {:?} lacks a positions section",
                String::from_utf8_lossy(field_name)
            )));
        }
        let fi = names
            .iter()
            .position(|n| n == field_name)
            .expect("kind-18 entries were tied to a field above");
        let n_terms = u64::from(u32_at(fields[fi].3)?);
        let expected_end = pos_lengths
            .get(i + 1)
            .map(|p| p.1)
            .or_else(|| splades.first().map(|s| s.1))
            .unwrap_or(file_len);
        expected_start = validate_term_base_section(
            map,
            TermSectionCheck {
                label: "pos_lengths",
                index: i,
                field_name,
                off: *off,
                total: *total,
                n_terms,
                bounds: (expected_start, expected_end),
            },
        )?;
    }
    for (i, (_field_name, off, len)) in splades.iter().enumerate() {
        let end = off
            .checked_add(*len)
            .ok_or_else(|| invalid("splade section overflow".into()))?;
        let expected_end = splades.get(i + 1).map_or(file_len, |s| s.1);
        if *off != expected_start || end != expected_end {
            return Err(invalid(format!(
                "splade {i}: section does not tile the file"
            )));
        }
        crate::turbo_bridge::splade::SpladeSectionReader::open(bytes_at(*off, *len)?)?;
        expected_start = end;
    }
    if expected_start != file_len {
        return Err(invalid("sections leave trailing bytes before EOF".into()));
    }
    Ok(())
}

struct PrefixedEntrySpec<'a> {
    col_idx: u64,
    kind: u8,
    prefix: &'a str,
    label: &'a str,
}

fn push_prefixed_field_entry(
    out: &mut Vec<(Vec<u8>, u64, u64)>,
    names: &[Vec<u8>],
    col_name: &[u8],
    payload: (u64, u64),
    spec: PrefixedEntrySpec<'_>,
) -> io::Result<()> {
    let invalid = |msg: String| io::Error::new(io::ErrorKind::InvalidData, msg);
    let field = col_name
        .strip_prefix(spec.prefix.as_bytes())
        .ok_or_else(|| {
            invalid(format!(
                "column {}: kind {} must be named {}<field>",
                spec.col_idx, spec.kind, spec.prefix
            ))
        })?;
    if !names.iter().any(|n| n == field) {
        return Err(invalid(format!(
            "column {}: {} for field {:?}, which the field table lacks",
            spec.col_idx,
            spec.label,
            String::from_utf8_lossy(field)
        )));
    }
    if out.iter().any(|(f, _, _)| f == field) {
        return Err(invalid(format!(
            "column {}: field {:?} has two {} entries",
            spec.col_idx,
            String::from_utf8_lossy(field),
            spec.label
        )));
    }
    out.push((field.to_vec(), payload.0, payload.1));
    Ok(())
}

struct TermSectionCheck<'a> {
    label: &'a str,
    index: usize,
    field_name: &'a [u8],
    off: u64,
    total: u64,
    n_terms: u64,
    bounds: (u64, u64),
}

fn validate_term_base_section(map: &[u8], check: TermSectionCheck<'_>) -> io::Result<u64> {
    let invalid = |msg: String| io::Error::new(io::ErrorKind::InvalidData, msg);
    let u32_at = |off: u64| -> io::Result<u32> {
        let b = map
            .get(off as usize..off as usize + 4)
            .ok_or_else(|| invalid(format!("read past end at {off}")))?;
        Ok(u32::from_le_bytes(b.try_into().expect("4 bytes")))
    };
    let (expected_start, expected_end) = check.bounds;
    let (label, i) = (check.label, check.index);
    if check.off != expected_start {
        return Err(invalid(format!(
            "{label} {i}: section does not start at the previous section's end"
        )));
    }
    if u64::from(u32_at(check.off)?) != check.n_terms {
        return Err(invalid(format!(
            "{label} {i}: term count disagrees with field {:?}'s directory",
            String::from_utf8_lossy(check.field_name)
        )));
    }
    let mut prev = 0u64;
    for t in 0..=check.n_terms {
        let base = u64::from(u32_at(check.off + 4 + 4 * t)?);
        if base < prev {
            return Err(invalid(format!(
                "{label} {i}: base table not monotone at term {t}"
            )));
        }
        prev = base;
    }
    if prev != check.total {
        return Err(invalid(format!(
            "{label} {i}: base table ends at {prev} but the entry declares {}",
            check.total
        )));
    }
    let group_end = check.off + positions_section_size(check.n_terms, check.total);
    if group_end != expected_end {
        return Err(invalid(format!(
            "{label} {i}: section does not end at the next section's start"
        )));
    }
    Ok(group_end)
}
