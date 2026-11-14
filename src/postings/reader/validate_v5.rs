//! Structural validation for `.bm25` v3/v4/v5 headers and v5 postings directories.

use std::io;

use super::super::format::u16_at;
use super::super::{BLOCK, LEVEL1_FACTOR, MAX_FRONTIER};
use super::validate::{lineage_section_end, validate_text_index};

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
pub(super) fn validate_structure(map: &[u8], v5: bool, blob_relative: bool) -> io::Result<()> {
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
pub(super) fn validate_v5_directory(
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
