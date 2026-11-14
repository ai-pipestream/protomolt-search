//! Auxiliary, unsigned/map/source, and tail section checks for `.bm25` v6/v7 payloads.

use std::io;

use super::super::super::columns::integer_present;
use super::super::super::format::{
    positions_section_size, sentences_section_size, COLUMN_KIND_MAP_I64,
};
use super::{invalid, MapView, SectionValidateCtx};

struct TermSectionCheck<'a> {
    label: &'a str,
    index: usize,
    field_name: &'a [u8],
    off: u64,
    total: u64,
    n_terms: u64,
    bounds: (u64, u64),
}

pub(super) fn validate_field_aux_sections(
    ctx: &SectionValidateCtx<'_>,
    mut expected_start: u64,
) -> io::Result<u64> {
    let (view, cols, names, fields) = (ctx.view, ctx.cols, ctx.names, ctx.fields);
    for (i, (field_name, off, total)) in cols.positions.iter().enumerate() {
        let fi = names
            .iter()
            .position(|n| n == field_name)
            .expect("kind-7 entries were tied to a field above");
        let n_terms = u64::from(view.u32_at(fields[fi].directory_off)?);
        let expected_end = cols
            .positions
            .get(i + 1)
            .map(|p| p.1)
            .or_else(|| cols.sentences.first().map(|s| s.1))
            .unwrap_or(cols.columns_end);
        expected_start = validate_term_base_section(
            view,
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
    validate_sentence_sections(ctx, expected_start)
}

fn validate_sentence_sections(
    ctx: &SectionValidateCtx<'_>,
    mut expected_start: u64,
) -> io::Result<u64> {
    let (view, cols, n_slots) = (ctx.view, ctx.cols, ctx.n_slots);
    for (i, (field_name, off, total)) in cols.sentences.iter().enumerate() {
        if *off != expected_start {
            return Err(invalid(format!(
                "sentences {i}: section does not start at the previous section's end"
            )));
        }
        if u64::from(view.u32_at(*off)?) != n_slots {
            return Err(invalid(format!(
                "sentences {i}: slot count disagrees with the file's for field {:?}",
                String::from_utf8_lossy(field_name)
            )));
        }
        let mut prev = 0u64;
        for d in 0..=n_slots {
            let base = u64::from(view.u32_at(off + 4 + 4 * d)?);
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
        let expected_end = cols.sentences.get(i + 1).map_or(cols.columns_end, |s| s.1);
        if group_end != expected_end {
            return Err(invalid(format!(
                "sentences {i}: section does not end at the next section's start"
            )));
        }
        expected_start = group_end;
    }
    Ok(expected_start)
}

pub(super) fn validate_late_column_sections(
    ctx: &SectionValidateCtx<'_>,
    mut expected_start: u64,
) -> io::Result<u64> {
    let (view, cols, n_slots) = (ctx.view, ctx.cols, ctx.n_slots);
    expected_start = validate_unsigned_integer_sections(ctx, expected_start)?;
    for &(kind, off, len) in &cols.integer_maps {
        let end = off
            .checked_add(len)
            .ok_or_else(|| invalid("integer map section overflow"))?;
        if off != expected_start || end > cols.integer_maps_end {
            return Err(invalid("integer map sections do not tile the file"));
        }
        if kind == COLUMN_KIND_MAP_I64 {
            crate::integer_map::Reader::<i64>::open(view.bytes_at(off, len)?, n_slots as u32)?;
        } else {
            crate::integer_map::Reader::<u64>::open(view.bytes_at(off, len)?, n_slots as u32)?;
        }
        expected_start = end;
    }
    if expected_start != cols.integer_maps_end {
        return Err(invalid("column sections leave a gap before sources or EOF"));
    }
    if let Some((off, len)) = cols.source_section {
        let end = off
            .checked_add(len)
            .ok_or_else(|| invalid("source section overflow"))?;
        if off != expected_start || end != cols.tail_sections_start {
            return Err(invalid("source section does not tile the file"));
        }
        crate::source_archive::SourceArchiveReader::open(view.bytes_at(off, len)?, n_slots as u32)?;
        expected_start = end;
    }
    Ok(expected_start)
}

fn validate_unsigned_integer_sections(
    ctx: &SectionValidateCtx<'_>,
    mut expected_start: u64,
) -> io::Result<u64> {
    let (view, cols, n_slots) = (ctx.view, ctx.cols, ctx.n_slots);
    for (i, &(stored_min, stored_max, off)) in cols.unsigned_integers.iter().enumerate() {
        let bitmap_off = off + 8 * n_slots;
        let end = bitmap_off + n_slots.div_ceil(8);
        let expected_end = cols
            .unsigned_integers
            .get(i + 1)
            .map_or(cols.unsigned_end, |entry| entry.2);
        if off != expected_start || end != expected_end {
            return Err(invalid(format!(
                "unsigned integer field {i}: sections do not tile the file"
            )));
        }
        let bitmap = view.bytes_at(bitmap_off, n_slots.div_ceil(8))?;
        if !n_slots.is_multiple_of(8) && bitmap.last().is_some_and(|b| b >> (n_slots % 8) != 0) {
            return Err(invalid(format!(
                "unsigned integer field {i}: nonzero presence padding"
            )));
        }
        let (mut min, mut max) = (u64::MAX, u64::MIN);
        for slot in 0..n_slots {
            let value = view.u64_at(off + 8 * slot)?;
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
    Ok(expected_start)
}

pub(super) fn validate_tail_sections(
    ctx: &SectionValidateCtx<'_>,
    mut expected_start: u64,
) -> io::Result<u64> {
    let (view, cols, names, fields) = (ctx.view, ctx.cols, ctx.names, ctx.fields);
    let file_len = view.len();
    for (i, (field_name, off, total)) in cols.pos_lengths.iter().enumerate() {
        if !cols.positions.iter().any(|(f, _, _)| f == field_name) {
            return Err(invalid(format!(
                "pos_lengths {i}: field {:?} lacks a positions section",
                String::from_utf8_lossy(field_name)
            )));
        }
        let fi = names
            .iter()
            .position(|n| n == field_name)
            .expect("kind-18 entries were tied to a field above");
        let n_terms = u64::from(view.u32_at(fields[fi].directory_off)?);
        let expected_end = cols
            .pos_lengths
            .get(i + 1)
            .map(|p| p.1)
            .or_else(|| cols.splades.first().map(|s| s.1))
            .unwrap_or(file_len);
        expected_start = validate_term_base_section(
            view,
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
    for (i, (_field_name, off, len)) in cols.splades.iter().enumerate() {
        let end = off
            .checked_add(*len)
            .ok_or_else(|| invalid("splade section overflow"))?;
        let expected_end = cols.splades.get(i + 1).map_or(file_len, |s| s.1);
        if *off != expected_start || end != expected_end {
            return Err(invalid(format!(
                "splade {i}: section does not tile the file"
            )));
        }
        crate::turbo_bridge::splade::SpladeSectionReader::open(view.bytes_at(*off, *len)?)?;
        expected_start = end;
    }
    Ok(expected_start)
}

fn validate_term_base_section(view: &MapView<'_>, check: TermSectionCheck<'_>) -> io::Result<u64> {
    let (expected_start, expected_end) = check.bounds;
    let (label, i) = (check.label, check.index);
    if check.off != expected_start {
        return Err(invalid(format!(
            "{label} {i}: section does not start at the previous section's end"
        )));
    }
    if u64::from(view.u32_at(check.off)?) != check.n_terms {
        return Err(invalid(format!(
            "{label} {i}: term count disagrees with field {:?}'s directory",
            String::from_utf8_lossy(check.field_name)
        )));
    }
    let mut prev = 0u64;
    for t in 0..=check.n_terms {
        let base = u64::from(view.u32_at(check.off + 4 + 4 * t)?);
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
