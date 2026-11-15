//! Structural validation for block-max postings and 34-byte term directories.

use std::io;

use super::super::super::format::u16_at;
use super::super::super::{BLOCK, LEVEL1_FACTOR, MAX_FRONTIER};
use super::{invalid, MapView};

struct DirectoryValidateState {
    prev_term: Vec<u8>,
    prev_skip_end: u64,
    blob_used: u64,
}

struct TermRunBounds {
    term_idx: u64,
    doc_run_off: u64,
    skip_run_off: u64,
    skip_end: u64,
    df: u32,
}

/// Validate one field's postings and 34-byte directory sections: term counts
/// match, entries stay in bounds with strictly ascending UTF-8/byte terms,
/// doc/occurrence/skip run offsets tile monotonically, occurrence runs are
/// pair-aligned and sentinel-checked, skip runs walk out to exact region ends
/// with `last_doc_id` cross-checked against the doc run, and the term blob
/// fills the directory section to `section_end`.
pub(super) fn validate_block_max_directory(
    view: &MapView<'_>,
    postings_off: u64,
    directory_off: u64,
    section_end: u64,
) -> io::Result<()> {
    let n_terms = u64::from(view.u32_at(directory_off)?);
    if u64::from(view.u32_at(postings_off)?) != n_terms {
        return Err(invalid("postings and directory term counts differ"));
    }
    let table_bytes = 34u64
        .checked_mul(n_terms)
        .and_then(|n| n.checked_add(4))
        .ok_or_else(|| invalid("directory overruns its section"))?;
    let blob_start = directory_off
        .checked_add(table_bytes)
        .ok_or_else(|| invalid("directory overruns its section"))?;
    if blob_start > section_end {
        return Err(invalid("directory overruns its section"));
    }
    let mut state = DirectoryValidateState {
        prev_term: Vec::new(),
        prev_skip_end: 0,
        blob_used: 0,
    };
    for i in 0..n_terms {
        validate_term_entry(
            view,
            (postings_off, directory_off, blob_start, section_end),
            (i, n_terms),
            &mut state,
        )?;
    }
    if blob_start + state.blob_used != section_end {
        return Err(invalid("term blob does not fill the directory section"));
    }
    Ok(())
}

fn validate_term_entry(
    view: &MapView<'_>,
    section: (u64, u64, u64, u64),
    term_pos: (u64, u64),
    state: &mut DirectoryValidateState,
) -> io::Result<()> {
    let (postings_off, directory_off, blob_start, section_end) = section;
    let (i, n_terms) = term_pos;
    let e = directory_off + 4 + 34 * i;
    let rebase = |rel: u64| -> io::Result<u64> {
        rel.checked_add(postings_off)
            .ok_or_else(|| invalid(format!("directory entry {i}: run offset overflows")))
    };
    let doc_run_off = rebase(view.u64_at(e)?)?;
    let skip_run_off = rebase(view.u64_at(e + 8)?)?;
    let occ_run_off = rebase(view.u64_at(e + 16)?)?;
    let df = view.u32_at(e + 24)?;
    let blob_off = u64::from(view.u32_at(e + 28)?);
    let term_len = u64::from(u16_at(view.bytes_at(e + 32, 2)?));
    if term_len == 0 {
        return Err(invalid(format!("directory entry {i}: empty term")));
    }
    let term_start = blob_start
        .checked_add(blob_off)
        .ok_or_else(|| invalid(format!("directory entry {i}: term out of the blob section")))?;
    if term_start.saturating_add(term_len) > section_end {
        return Err(invalid(format!(
            "directory entry {i}: term out of the blob section"
        )));
    }
    let term = view.bytes_at(term_start, term_len)?;
    state.blob_used += term_len;
    if doc_run_off < state.prev_skip_end || doc_run_off < postings_off + 4 {
        return Err(invalid(format!(
            "directory entry {i}: doc run overlaps previous regions"
        )));
    }
    if Some(occ_run_off) != doc_run_off.checked_add(12 * u64::from(df) + 4)
        || occ_run_off > skip_run_off
    {
        return Err(invalid(format!(
            "directory entry {i}: inconsistent run offsets"
        )));
    }
    let skip_end = if i + 1 < n_terms {
        rebase(view.u64_at(e + 34)?)?
    } else {
        directory_off
    };
    if skip_end < skip_run_off.saturating_add(8) || skip_end > directory_off {
        return Err(invalid(format!(
            "directory entry {i}: skip run out of the postings section"
        )));
    }
    validate_occurrence_and_skip_runs(
        view,
        &TermRunBounds {
            term_idx: i,
            doc_run_off,
            skip_run_off,
            skip_end,
            df,
        },
        occ_run_off,
    )?;
    state.prev_skip_end = skip_end;
    if !state.prev_term.is_empty() && term <= state.prev_term.as_slice() {
        return Err(invalid(format!(
            "directory entry {i}: terms not strictly ordered"
        )));
    }
    state.prev_term = term.to_vec();
    Ok(())
}

fn validate_occurrence_and_skip_runs(
    view: &MapView<'_>,
    bounds: &TermRunBounds,
    occ_run_off: u64,
) -> io::Result<()> {
    let i = bounds.term_idx;
    if !(bounds.skip_run_off - occ_run_off).is_multiple_of(8) {
        return Err(invalid(format!(
            "directory entry {i}: occurrence run not pair-aligned"
        )));
    }
    let sentinel = view.u32_at(bounds.doc_run_off + 12 * u64::from(bounds.df))?;
    if u64::from(sentinel) != (bounds.skip_run_off - occ_run_off) / 8 {
        return Err(invalid(format!(
            "directory entry {i}: sentinel occ_start mismatch"
        )));
    }
    if bounds.df > 0 && view.u32_at(bounds.doc_run_off + 8)? != 0 {
        return Err(invalid(format!(
            "directory entry {i}: first occ_start is not 0"
        )));
    }
    validate_skip_run(view, bounds)
}

fn validate_skip_run(view: &MapView<'_>, bounds: &TermRunBounds) -> io::Result<()> {
    let i = bounds.term_idx;
    let region = bounds.skip_run_off;
    let region_end = bounds.skip_end;
    let l1_rel = view.u64_at(region)?;
    let n_l0 = u64::from(bounds.df).div_ceil(BLOCK as u64);
    let mut cur = region + 8;
    let mut prev_last = 0u32;
    let mut l0_record_offs: Vec<u64> = Vec::with_capacity(n_l0 as usize);
    for b in 0..n_l0 {
        l0_record_offs.push(cur - region);
        let last_doc = view.u32_at(cur)?;
        let n_pairs = u64::from(view.bytes_at(cur + 4, 1)?[0]);
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
        let last_posting = ((b + 1) * BLOCK as u64).min(u64::from(bounds.df)) - 1;
        if view.u32_at(bounds.doc_run_off + 12 * last_posting)? != last_doc {
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
    if Some(cur) != region.checked_add(l1_rel) {
        return Err(invalid(format!("term {i}: level-1 region offset mismatch")));
    }
    let n_l1 = n_l0.div_ceil(LEVEL1_FACTOR as u64);
    for g in 0..n_l1 {
        let last_doc = view.u32_at(cur)?;
        let l0_off = view.u64_at(cur + 4)?;
        let n_pairs = u64::from(view.bytes_at(cur + 12, 1)?[0]);
        if n_pairs == 0 || n_pairs > MAX_FRONTIER as u64 {
            return Err(invalid(format!("term {i} group {g}: n_pairs out of range")));
        }
        let last_block = ((g + 1) * LEVEL1_FACTOR as u64).min(n_l0) - 1;
        let last_posting = ((last_block + 1) * BLOCK as u64).min(u64::from(bounds.df)) - 1;
        if view.u32_at(bounds.doc_run_off + 12 * last_posting)? != last_doc {
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
    Ok(())
}
