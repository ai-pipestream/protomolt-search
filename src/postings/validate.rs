use super::*;

use std::io::{self};

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
pub(crate) fn validate_structure(map: &[u8], v5: bool, blob_relative: bool) -> io::Result<()> {
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
pub(crate) fn validate_v5_directory(
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
        if !(skip_run_off - occ_run_off).is_multiple_of(8) {
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

/// Validate the text index: every present entry within the texts
/// section, absent markers untouched. `entry_base` rebases stored
/// offsets: the entries are absolute in v3/v4/v5 (base 0) and relative
/// to the texts section start in v6 (base `texts_off`).
pub(crate) fn validate_text_index(
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
pub(crate) fn lineage_section_end(map: &[u8], lineages_off: u64, n_slots: u64) -> io::Result<u64> {
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
pub(crate) fn validate_structure_v6(map: &[u8], v7: bool) -> io::Result<()> {
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
                    // The entry names a field of THIS file's table; a
                    // positions section for a field that does not exist
                    // is bytes nobody can read, refused here by name.
                    let name = column_names.last().expect("pushed above");
                    let field = name
                        .strip_prefix(POSITIONS_ENTRY_PREFIX.as_bytes())
                        .ok_or_else(|| {
                            invalid(format!(
                                "column {i}: kind {COLUMN_KIND_POSITIONS} must be named \
                                 {POSITIONS_ENTRY_PREFIX}<field>"
                            ))
                        })?;
                    if !names.iter().any(|n| n == field) {
                        return Err(invalid(format!(
                            "column {i}: positions for field {:?}, which the field table lacks",
                            String::from_utf8_lossy(field)
                        )));
                    }
                    if positions.iter().any(|(f, _, _)| f == field) {
                        return Err(invalid(format!(
                            "column {i}: field {:?} has two positions entries",
                            String::from_utf8_lossy(field)
                        )));
                    }
                    positions.push((field.to_vec(), u64_at(base)?, u64_at(base + 8)?));
                    cursor = base + 16;
                }
                COLUMN_KIND_SENTENCES => {
                    // Same rule as kind 7: the entry names a field of
                    // THIS file's table, once.
                    let name = column_names.last().expect("pushed above");
                    let field = name
                        .strip_prefix(SENTENCES_ENTRY_PREFIX.as_bytes())
                        .ok_or_else(|| {
                            invalid(format!(
                                "column {i}: kind {COLUMN_KIND_SENTENCES} must be named \
                                 {SENTENCES_ENTRY_PREFIX}<field>"
                            ))
                        })?;
                    if !names.iter().any(|n| n == field) {
                        return Err(invalid(format!(
                            "column {i}: sentences for field {:?}, which the field table lacks",
                            String::from_utf8_lossy(field)
                        )));
                    }
                    if sentences.iter().any(|(f, _, _)| f == field) {
                        return Err(invalid(format!(
                            "column {i}: field {:?} has two sentences entries",
                            String::from_utf8_lossy(field)
                        )));
                    }
                    sentences.push((field.to_vec(), u64_at(base)?, u64_at(base + 8)?));
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
                    // The reserved binding record: pinned name, inline
                    // payload, no sections. Duplicates fall to the
                    // table's name-uniqueness rule above.
                    if column_names.last().map(Vec::as_slice) != Some(BINDING_ENTRY_NAME.as_bytes())
                    {
                        return Err(invalid(format!(
                            "column {i}: kind {COLUMN_KIND_BINDING} must be named \
                             {BINDING_ENTRY_NAME:?}"
                        )));
                    }
                    let mut cur = base;
                    let mut values = Vec::new();
                    for part in 0..if kind != COLUMN_KIND_BINDING { 4 } else { 3 } {
                        let len = u64::from(u16_at(bytes_at(cur, 2)?));
                        let value = std::str::from_utf8(bytes_at(cur + 2, len)?)
                            .map_err(|_| invalid("invalid utf-8 in binding record".into()))?;
                        if part == 3
                            && (kind == COLUMN_KIND_ANALYSIS_BINDING || !value.is_empty())
                            && !crate::mapped_analysis::valid_digest(value)
                        {
                            return Err(invalid("invalid mapped analysis digest".into()));
                        }
                        values.push(value);
                        cur += 2 + len;
                    }
                    if kind != COLUMN_KIND_BINDING {
                        let len = u32::from_le_bytes(bytes_at(cur, 4)?.try_into().unwrap()) as u64;
                        crate::mapped_analysis::decode_contract(
                            values[3],
                            bytes_at(cur + 4, len)?,
                            values[1],
                        )
                        .map_err(invalid)?;
                        cur += 4 + len;
                    }
                    let mut vector_bytes = &[][..];
                    if matches!(kind, COLUMN_KIND_VECTOR_BINDING | COLUMN_KIND_INDEX_BINDING) {
                        let len = u32::from_le_bytes(bytes_at(cur, 4)?.try_into().unwrap()) as u64;
                        vector_bytes = bytes_at(cur + 4, len)?;
                        if crate::mapped_vector::decode(vector_bytes, values[0])
                            .map_err(|e| invalid(e.to_string()))?
                            .is_none()
                        {
                            return Err(invalid(
                                "empty vector binding in vector/index entry".into(),
                            ));
                        }
                        cur += 4 + len;
                    }
                    if kind == COLUMN_KIND_INDEX_BINDING {
                        let len = u32::from_le_bytes(bytes_at(cur, 4)?.try_into().unwrap()) as u64;
                        if crate::index_contract::validate_binding(
                            bytes_at(cur + 4, len)?,
                            values[0],
                            vector_bytes,
                        )
                        .map_err(|e| invalid(e.to_string()))?
                        .is_none()
                        {
                            return Err(invalid("empty index contract in kind-14 entry".into()));
                        }
                        cur += 4 + len;
                    }
                    cursor = cur;
                }
                COLUMN_KIND_DERIVED => {
                    if column_names.last().map(Vec::as_slice) != Some(DERIVED_ENTRY_NAME.as_bytes())
                    {
                        return Err(invalid(format!(
                            "column {i}: kind {COLUMN_KIND_DERIVED} must be named \
                             {DERIVED_ENTRY_NAME:?}"
                        )));
                    }
                    let mut cur = base;
                    let len = u64::from(u16_at(bytes_at(cur, 2)?));
                    let fingerprint = std::str::from_utf8(bytes_at(cur + 2, len)?)
                        .map_err(|_| invalid("invalid utf-8 in derived-columns record".into()))?
                        .to_string();
                    cur += 2 + len;
                    let len = u32::from_le_bytes(bytes_at(cur, 4)?.try_into().unwrap()) as u64;
                    StoredDerived {
                        fingerprint,
                        declaration: bytes_at(cur + 4, len)?.to_vec(),
                    }
                    .validate()
                    .map_err(invalid)?;
                    cur += 4 + len;
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
    let integer_maps_end = source_section.map_or(file_len, |(off, _)| off);
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
        if *off != expected_start {
            return Err(invalid(format!(
                "positions {i}: section does not start at the previous section's end"
            )));
        }
        let fi = names
            .iter()
            .position(|n| n == field_name)
            .expect("kind-7 entries were tied to a field above");
        let (_, _, _, directory_off) = fields[fi];
        let n_terms = u64::from(u32_at(directory_off)?);
        if u64::from(u32_at(*off)?) != n_terms {
            return Err(invalid(format!(
                "positions {i}: term count disagrees with field {:?}'s directory",
                String::from_utf8_lossy(field_name)
            )));
        }
        let mut prev = 0u64;
        for t in 0..=n_terms {
            let base = u64::from(u32_at(off + 4 + 4 * t)?);
            if base < prev {
                return Err(invalid(format!(
                    "positions {i}: base table not monotone at term {t}"
                )));
            }
            prev = base;
        }
        if prev != *total {
            return Err(invalid(format!(
                "positions {i}: base table ends at {prev} but the entry declares {total}"
            )));
        }
        let group_end = off + positions_section_size(n_terms, *total);
        let expected_end = positions
            .get(i + 1)
            .map(|p| p.1)
            .or_else(|| sentences.first().map(|s| s.1))
            .unwrap_or(columns_end);
        if group_end != expected_end {
            return Err(invalid(format!(
                "positions {i}: section does not end at the next section's start"
            )));
        }
        expected_start = group_end;
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
        if off != expected_start || off.checked_add(len) != Some(file_len) {
            return Err(invalid("source section does not tile the file".into()));
        }
        crate::source_archive::SourceArchiveReader::open(bytes_at(off, len)?, n_slots as u32)?;
    }
    Ok(())
}
