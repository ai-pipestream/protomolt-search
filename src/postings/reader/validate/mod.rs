//! Structural and columnar validation for `.bm25` v6/v7 payloads.

mod columns;
mod sections;
mod tail_sections;

use std::io;

use super::super::format::u16_at;
#[cfg(doc)]
use super::validate_v5::validate_structure;
use super::validate_v5::validate_v5_directory;

fn invalid(msg: impl Into<String>) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.into())
}

struct MapView<'a> {
    map: &'a [u8],
}

impl<'a> MapView<'a> {
    fn new(map: &'a [u8]) -> Self {
        Self { map }
    }

    fn len(&self) -> u64 {
        self.map.len() as u64
    }

    fn as_slice(&self) -> &'a [u8] {
        self.map
    }

    fn u32_at(&self, off: u64) -> io::Result<u32> {
        let end = (off as usize)
            .checked_add(4)
            .ok_or_else(|| invalid(format!("read past end at {off}")))?;
        let b = self
            .map
            .get(off as usize..end)
            .ok_or_else(|| invalid(format!("read past end at {off}")))?;
        Ok(u32::from_le_bytes(b.try_into().expect("4 bytes")))
    }

    fn u64_at(&self, off: u64) -> io::Result<u64> {
        let end = (off as usize)
            .checked_add(8)
            .ok_or_else(|| invalid(format!("read past end at {off}")))?;
        let b = self
            .map
            .get(off as usize..end)
            .ok_or_else(|| invalid(format!("read past end at {off}")))?;
        Ok(u64::from_le_bytes(b.try_into().expect("8 bytes")))
    }

    fn bytes_at(&self, off: u64, len: u64) -> io::Result<&'a [u8]> {
        let end = off
            .checked_add(len)
            .ok_or_else(|| invalid(format!("range [{off}, u64::MAX) out of file")))?;
        self.map
            .get(off as usize..end as usize)
            .ok_or_else(|| invalid(format!("range [{off}, {end}) out of file")))
    }

    fn walk_dict(&self, start: u64, count: u32) -> io::Result<u64> {
        let mut cur = start;
        for _ in 0..count {
            let len = u64::from(u16_at(self.bytes_at(cur, 2)?));
            self.bytes_at(cur + 2, len)?;
            cur += 2 + len;
        }
        Ok(cur)
    }
}

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
    let mut cur = lineages_off;
    for _ in 0..n_slots {
        let flag = *map
            .get(cur as usize)
            .ok_or_else(|| invalid("lineage section overruns the file"))?;
        cur += if flag == 0 { 1 } else { 25 };
    }
    Ok(cur)
}

struct V6Header {
    n_fields: u64,
    n_slots: u64,
    texts_off: u64,
    text_index_off: u64,
    lineages_off: u64,
}

struct FieldGroupEntry {
    total_length: u64,
    dl_off: u64,
    postings_off: u64,
    directory_off: u64,
}

struct SectionValidateCtx<'a> {
    view: &'a MapView<'a>,
    cols: &'a columns::V7Columns,
    names: &'a [Vec<u8>],
    fields: &'a [FieldGroupEntry],
    n_slots: u64,
}

fn parse_v6_header(view: &MapView<'_>) -> io::Result<V6Header> {
    let n_fields = u64::from(view.u32_at(8)?);
    if n_fields == 0 {
        return Err(invalid("v6 file with zero fields"));
    }
    Ok(V6Header {
        n_fields,
        n_slots: u64::from(view.u32_at(12)?),
        texts_off: view.u64_at(16)?,
        text_index_off: view.u64_at(24)?,
        lineages_off: view.u64_at(32)?,
    })
}

fn parse_field_table(
    view: &MapView<'_>,
    n_fields: u64,
) -> io::Result<(Vec<FieldGroupEntry>, Vec<Vec<u8>>, u64)> {
    let mut cursor = 40u64;
    let mut fields = Vec::new();
    let mut names: Vec<Vec<u8>> = Vec::new();
    for i in 0..n_fields {
        let name_len = u64::from(u16_at(view.bytes_at(cursor, 2)?));
        if name_len == 0 {
            return Err(invalid(format!("field {i}: empty name")));
        }
        let name = view.bytes_at(cursor + 2, name_len)?.to_vec();
        if names.contains(&name) {
            return Err(invalid(format!("field {i}: duplicate name")));
        }
        names.push(name);
        let base = cursor + 2 + name_len;
        fields.push(FieldGroupEntry {
            total_length: view.u64_at(base + 8)?,
            dl_off: view.u64_at(base + 16)?,
            postings_off: view.u64_at(base + 24)?,
            directory_off: view.u64_at(base + 32)?,
        });
        cursor = base + 40;
    }
    Ok((fields, names, cursor))
}

fn validate_shared_sections(
    view: &MapView<'_>,
    header: &V6Header,
    header_end: u64,
) -> io::Result<u64> {
    let file_len = view.len();
    if header.texts_off != header_end || header.texts_off > file_len {
        return Err(invalid("texts section does not start at the header end"));
    }
    if header.text_index_off < header.texts_off
        || header.lineages_off != header.text_index_off + 12 * header.n_slots
        || header.lineages_off > file_len
    {
        return Err(invalid("shared section offsets unordered or out of file"));
    }
    validate_text_index(
        view.as_slice(),
        header.texts_off,
        header.text_index_off,
        header.n_slots,
        header.texts_off,
    )?;
    lineage_section_end(view.as_slice(), header.lineages_off, header.n_slots)
}

fn validate_field_sections(
    view: &MapView<'_>,
    fields: &[FieldGroupEntry],
    n_slots: u64,
    lineage_end: u64,
    field_groups_end: u64,
) -> io::Result<u64> {
    let mut expected_start = lineage_end;
    for (i, field) in fields.iter().enumerate() {
        if field.dl_off != expected_start {
            return Err(invalid(format!(
                "field {i}: doc_lengths section does not start at the previous section's end"
            )));
        }
        if field.postings_off != field.dl_off + 4 * n_slots {
            return Err(invalid(format!(
                "field {i}: postings section does not follow doc_lengths"
            )));
        }
        let section_end = fields.get(i + 1).map_or(field_groups_end, |f| f.dl_off);
        if field.directory_off < field.postings_off + 4 || field.directory_off + 4 > section_end {
            return Err(invalid(format!(
                "field {i}: directory offset out of the group"
            )));
        }
        let mut length_sum = 0u64;
        for slot in 0..n_slots {
            length_sum += u64::from(view.u32_at(field.dl_off + 4 * slot)?);
        }
        if length_sum != field.total_length {
            return Err(invalid(format!(
                "field {i}: total_length != sum of doc lengths"
            )));
        }
        validate_v5_directory(
            view.as_slice(),
            field.postings_off,
            field.directory_off,
            section_end,
            field.postings_off,
        )?;
        expected_start = section_end;
    }
    Ok(expected_start)
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
    let view = MapView::new(map);
    let header = parse_v6_header(&view)?;
    let (fields, names, cursor) = parse_field_table(&view, header.n_fields)?;
    let (cols, header_end) = columns::parse_v7_columns(&view, v7, cursor, &names)?;
    let lineage_end = validate_shared_sections(&view, &header, header_end)?;
    let expected_start = validate_field_sections(
        &view,
        &fields,
        header.n_slots,
        lineage_end,
        cols.field_groups_end(),
    )?;
    let ctx = SectionValidateCtx {
        view: &view,
        cols: &cols,
        names: &names,
        fields: &fields,
        n_slots: header.n_slots,
    };
    let expected_start = sections::validate_early_column_sections(&ctx, expected_start)?;
    let expected_start = tail_sections::validate_field_aux_sections(&ctx, expected_start)?;
    let expected_start = tail_sections::validate_late_column_sections(&ctx, expected_start)?;
    let expected_start = tail_sections::validate_tail_sections(&ctx, expected_start)?;
    if expected_start != view.len() {
        return Err(invalid("sections leave trailing bytes before EOF"));
    }
    Ok(())
}
