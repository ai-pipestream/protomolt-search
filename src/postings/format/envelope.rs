//! Section-start walk over a v6/v7 payload and the v8 finalize post-pass.

use std::io::{self, Write};
use std::path::Path;

use super::{
    COLUMN_KIND_ANALYSIS_BINDING, COLUMN_KIND_BINDING, COLUMN_KIND_DERIVED, COLUMN_KIND_F64,
    COLUMN_KIND_FACET, COLUMN_KIND_GEO, COLUMN_KIND_I64, COLUMN_KIND_I64_PRESENT,
    COLUMN_KIND_INDEX_BINDING, COLUMN_KIND_MAP_F64, COLUMN_KIND_MAP_FACET, COLUMN_KIND_MAP_I64,
    COLUMN_KIND_MAP_U64, COLUMN_KIND_POSITIONS, COLUMN_KIND_POS_LENGTHS, COLUMN_KIND_SENTENCES,
    COLUMN_KIND_SOURCES, COLUMN_KIND_SPLADE, COLUMN_KIND_U64, COLUMN_KIND_VECTOR_BINDING, MAGIC_V6,
    MAGIC_V7, MAGIC_V8, TRAILER_MAGIC,
};
use crate::postings::columns::{decode_binding_entry, decode_derived_entry};
use crate::postings::reader::validate_structure_v6;
#[cfg(doc)]
use crate::postings::Bm25Reader;

/// Every section start of a STRUCTURALLY VALIDATED v6/v7 payload, in
/// file order with its integrity name. The walk mirrors
/// [`Bm25Reader::open_v6v7`]'s header parse; ranges are the gaps
/// between consecutive starts (the writer lays sections with one
/// cursor, so they are contiguous by construction).
pub(in crate::postings) fn v6v7_section_starts(
    map: &[u8],
    v7: bool,
) -> io::Result<Vec<(String, u64)>> {
    let u32_at = |off: usize| u32::from_le_bytes(map[off..off + 4].try_into().expect("4 bytes"));
    let u64_at = |off: usize| u64::from_le_bytes(map[off..off + 8].try_into().expect("8 bytes"));
    let mut starts: Vec<(String, u64)> = vec![("header".to_string(), 0)];
    let n_fields = u32_at(8) as usize;
    starts.push(("texts".to_string(), u64_at(16)));
    starts.push(("text_index".to_string(), u64_at(24)));
    starts.push(("lineages".to_string(), u64_at(32)));
    let mut cursor = 40usize;
    for _ in 0..n_fields {
        let name_len = u16::from_le_bytes(map[cursor..cursor + 2].try_into().unwrap()) as usize;
        let name = String::from_utf8_lossy(&map[cursor + 2..cursor + 2 + name_len]).into_owned();
        let base = cursor + 2 + name_len;
        starts.push((format!("field:{name}:doc_lengths"), u64_at(base + 16)));
        starts.push((format!("field:{name}:postings"), u64_at(base + 24)));
        starts.push((format!("field:{name}:directory"), u64_at(base + 32)));
        cursor = base + 40;
    }
    if v7 {
        let n_columns = u32_at(cursor) as usize;
        cursor += 4;
        for _ in 0..n_columns {
            push_column_section_start(&mut starts, map, &mut cursor)?;
        }
    }
    for pair in starts.windows(2) {
        if pair[1].1 < pair[0].1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "section {} at {} does not follow {} at {}",
                    pair[1].0, pair[1].1, pair[0].0, pair[0].1
                ),
            ));
        }
    }
    Ok(starts)
}

fn push_column_section_start(
    starts: &mut Vec<(String, u64)>,
    map: &[u8],
    cursor: &mut usize,
) -> io::Result<()> {
    let u32_at = |off: usize| u32::from_le_bytes(map[off..off + 4].try_into().expect("4 bytes"));
    let u64_at = |off: usize| u64::from_le_bytes(map[off..off + 8].try_into().expect("8 bytes"));
    let name_len = u16::from_le_bytes(map[*cursor..*cursor + 2].try_into().unwrap()) as usize;
    let name = String::from_utf8_lossy(&map[*cursor + 2..*cursor + 2 + name_len]).into_owned();
    let kind = map[*cursor + 2 + name_len];
    let base = *cursor + 2 + name_len + 1;
    *cursor = match kind {
        COLUMN_KIND_FACET => {
            starts.push((format!("column:{name}:dict"), u64_at(base + 4)));
            starts.push((format!("column:{name}:ords"), u64_at(base + 12)));
            base + 20
        }
        COLUMN_KIND_F64 => {
            starts.push((format!("column:{name}:vals"), u64_at(base + 16)));
            base + 24
        }
        COLUMN_KIND_MAP_FACET => {
            starts.push((format!("column:{name}:keys"), u64_at(base + 8)));
            starts.push((format!("column:{name}:values"), u64_at(base + 16)));
            starts.push((format!("column:{name}:offsets"), u64_at(base + 24)));
            starts.push((format!("column:{name}:pairs"), u64_at(base + 32)));
            base + 40
        }
        COLUMN_KIND_MAP_F64 => {
            starts.push((format!("column:{name}:keys"), u64_at(base + 4)));
            starts.push((format!("column:{name}:offsets"), u64_at(base + 12)));
            starts.push((format!("column:{name}:pairs"), u64_at(base + 20)));
            base + 28
        }
        COLUMN_KIND_MAP_I64 | COLUMN_KIND_MAP_U64 => {
            starts.push((format!("column:{name}:integer_map"), u64_at(base)));
            base + 16
        }
        COLUMN_KIND_I64 | COLUMN_KIND_I64_PRESENT | COLUMN_KIND_U64 => {
            starts.push((format!("column:{name}:vals"), u64_at(base + 16)));
            if kind != COLUMN_KIND_I64 {
                starts.push((
                    format!("column:{name}:presence"),
                    u64_at(base + 16) + 8 * u64::from(u32_at(12)),
                ));
            }
            base + 24
        }
        COLUMN_KIND_GEO => {
            starts.push((format!("column:{name}:vals"), u64_at(base + 32)));
            base + 40
        }
        COLUMN_KIND_POSITIONS | COLUMN_KIND_SENTENCES | COLUMN_KIND_POS_LENGTHS => {
            starts.push((format!("column:{name}:vals"), u64_at(base)));
            base + 16
        }
        COLUMN_KIND_SOURCES => {
            starts.push((format!("column:{name}:sources"), u64_at(base)));
            base + 16
        }
        COLUMN_KIND_SPLADE => {
            starts.push((format!("column:{name}:splade"), u64_at(base)));
            base + 16
        }
        COLUMN_KIND_BINDING
        | COLUMN_KIND_ANALYSIS_BINDING
        | COLUMN_KIND_VECTOR_BINDING
        | COLUMN_KIND_INDEX_BINDING => base + decode_binding_entry(kind, &map[base..])?.1,
        COLUMN_KIND_DERIVED => base + decode_derived_entry(&map[base..])?.1,
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("column {name}: unknown kind {other}"),
            ))
        }
    };
    Ok(())
}

/// The v8 post-pass: turn a finished v6/v7 file into a v8 one in
/// place. Validates the payload (never stamp integrity onto malformed
/// bytes), CRCs every section, appends the integrity section and
/// trailer, and patches the leading magic LAST — a crash mid-pass
/// leaves a v6/v7 magic with a garbage tail, which the validator's
/// section-extent checks refuse loudly. Ends with `sync_all`; the
/// caller renames and fsyncs the parent.
pub(in crate::postings) fn finalize_v8(path: &Path) -> io::Result<()> {
    use std::io::{Seek, SeekFrom};
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(path)?;
    let tail: Vec<u8> = {
        let map = unsafe { memmap2::MmapOptions::new().map(&file)? };
        let base_v7 = match map.get(..8) {
            Some(m) if m == MAGIC_V7 => true,
            Some(m) if m == MAGIC_V6 => false,
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "finalize_v8 expects a finished v6 or v7 file",
                ))
            }
        };
        validate_structure_v6(&map, base_v7)?;
        let starts = v6v7_section_starts(&map, base_v7)?;
        let integrity_off = map.len() as u64;
        let mut section: Vec<u8> = Vec::with_capacity(starts.len() * 48 + 32);
        section.extend_from_slice(&(starts.len() as u32).to_le_bytes());
        for (i, (name, off)) in starts.iter().enumerate() {
            let end = starts.get(i + 1).map_or(integrity_off, |s| s.1);
            // The header's CRC must describe the FINAL bytes, whose
            // magic this pass is about to patch to v8 — computing it
            // over the still-v6/v7 bytes would refuse every file at
            // open.
            let crc = if *off == 0 {
                let mut header = Vec::with_capacity(end as usize);
                header.extend_from_slice(MAGIC_V8);
                header.extend_from_slice(&map[8..end as usize]);
                crate::wal::crc32(&header)
            } else {
                crate::wal::crc32(&map[*off as usize..end as usize])
            };
            section.extend_from_slice(&(name.len() as u16).to_le_bytes());
            section.extend_from_slice(name.as_bytes());
            section.extend_from_slice(&off.to_le_bytes());
            section.extend_from_slice(&(end - off).to_le_bytes());
            section.extend_from_slice(&crc.to_le_bytes());
        }
        let table_crc = crate::wal::crc32(&section);
        section.extend_from_slice(&table_crc.to_le_bytes());
        section.extend_from_slice(&integrity_off.to_le_bytes());
        section.extend_from_slice(&if base_v7 { 7u64 } else { 6u64 }.to_le_bytes());
        section.extend_from_slice(TRAILER_MAGIC);
        section
    };
    file.seek(SeekFrom::End(0))?;
    file.write_all(&tail)?;
    file.seek(SeekFrom::Start(0))?;
    file.write_all(MAGIC_V8)?;
    file.sync_all()
}
