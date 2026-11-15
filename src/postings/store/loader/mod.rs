//! Deserialization of `.bm25` v6, v7, and v8 files back into `Bm25Store`.

mod v6;
mod v6_columns;

use std::collections::BTreeMap;
use std::io;
use std::path::Path;

use super::super::format::{parse_integrity, u16_at, MAGIC_V6, MAGIC_V7, MAGIC_V8};
use super::super::reader::validate_structure_v6;
use super::super::Posting;
use super::Bm25Store;

fn invalid(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
}

/// View over payload bytes starting at file offset 8 (past the 8-byte magic).
#[derive(Clone, Copy)]
struct PayloadView<'a> {
    all: &'a [u8],
}

impl<'a> PayloadView<'a> {
    fn at(&self, off: u64, len: u64) -> io::Result<&'a [u8]> {
        let i = (off as usize)
            .checked_sub(8)
            .ok_or_else(|| invalid("bad offset"))?;
        self.all
            .get(i..i + len as usize)
            .ok_or_else(|| invalid("offset out of range"))
    }

    fn u16_at(&self, off: u64) -> io::Result<u16> {
        Ok(u16_at(self.at(off, 2)?))
    }

    fn u32_at(&self, off: u64) -> io::Result<u32> {
        Ok(u32::from_le_bytes(
            self.at(off, 4)?.try_into().expect("4 bytes"),
        ))
    }

    fn u64_at(&self, off: u64) -> io::Result<u64> {
        Ok(u64::from_le_bytes(
            self.at(off, 8)?.try_into().expect("8 bytes"),
        ))
    }
}

impl Bm25Store {
    /// Load from `path`.
    pub fn load(path: &Path) -> io::Result<Self> {
        let bytes = std::fs::read(path)?;
        Self::read_from(&mut &bytes[..])
    }

    fn read_from(r: &mut &[u8]) -> io::Result<Self> {
        let original = *r;
        let Some(magic) = original.get(..8) else {
            return Err(invalid("unsupported or invalid .bm25 magic"));
        };
        *r = &original[8..];
        match magic {
            m if m == MAGIC_V8 => Self::read_v8_from(r),
            m if m == MAGIC_V7 => {
                validate_structure_v6(original, true)?;
                Self::read_v6v7_from(r, true)
            }
            m if m == MAGIC_V6 => {
                validate_structure_v6(original, false)?;
                Self::read_v6v7_from(r, false)
            }
            _ => Err(invalid("unsupported or invalid .bm25 magic")),
        }
    }

    fn read_v8_from(r: &mut &[u8]) -> io::Result<Self> {
        let mut full = Vec::with_capacity(8 + r.len());
        full.extend_from_slice(MAGIC_V8);
        full.extend_from_slice(r);
        let table = parse_integrity(&full)?;
        for e in &table.entries {
            let got = crate::wal::crc32(&full[e.off as usize..(e.off + e.len) as usize]);
            if got != e.crc {
                return Err(invalid(&format!(
                    "section {} CRC mismatch: stored {:08x}, computed {got:08x}",
                    e.name, e.crc
                )));
            }
        }
        validate_structure_v6(&full[..table.payload_len as usize], table.base_v7)?;
        let mut payload = &full[8..table.payload_len as usize];
        Self::read_v6v7_from(&mut payload, table.base_v7)
    }

    /// Decode one field's block-max postings/directory section pair back into a
    /// heap postings map. `all` starts at file offset 8 (past the magic);
    /// `run_base` rebases section-relative directory run offsets. The skip run
    /// is not needed in heap form and is skipped.
    pub(super) fn read_block_max_postings(
        all: &[u8],
        directory_off: u64,
        run_base: u64,
        positions_off: Option<u64>,
        pos_lengths_off: Option<u64>,
    ) -> io::Result<BTreeMap<String, Vec<Posting>>> {
        let view = PayloadView { all };
        let n_terms = view.u32_at(directory_off)? as usize;
        let blob_start = directory_off + 4 + 34 * n_terms as u64;
        let positions = check_ordinal_section(&view, positions_off, n_terms, "positions")?;
        let pos_lengths = check_ordinal_section(&view, pos_lengths_off, n_terms, "pos_lengths")?;
        let mut postings = BTreeMap::new();
        for i in 0..n_terms {
            let e = directory_off + 4 + 34 * i as u64;
            let doc_run_off = view.u64_at(e)? + run_base;
            let occ_run_off = view.u64_at(e + 16)? + run_base;
            let df = view.u32_at(e + 24)? as usize;
            let blob_off = u64::from(view.u32_at(e + 28)?);
            let term_len = u64::from(view.u16_at(e + 32)?);
            let term = String::from_utf8(view.at(blob_start + blob_off, term_len)?.to_vec())
                .map_err(|_| invalid("invalid utf-8 in term"))?;
            let term_base = term_ordinal_base(&view, positions, i)?;
            let pl_base = term_ordinal_base(&view, pos_lengths, i)?;
            let plist =
                decode_term_postings(&view, (doc_run_off, occ_run_off, df), term_base, pl_base)?;
            postings.insert(term, plist);
        }
        Ok(postings)
    }
}

fn check_ordinal_section(
    view: &PayloadView<'_>,
    section_off: Option<u64>,
    n_terms: usize,
    label: &str,
) -> io::Result<Option<(u64, u64)>> {
    let Some(off) = section_off else {
        return Ok(None);
    };
    if view.u32_at(off)? as usize != n_terms {
        return Err(invalid(&format!(
            "{label} section term count differs from directory"
        )));
    }
    Ok(Some((off + 4, off + 4 + 4 * (n_terms as u64 + 1))))
}

fn term_ordinal_base(
    view: &PayloadView<'_>,
    section: Option<(u64, u64)>,
    term_idx: usize,
) -> io::Result<Option<(u64, u64)>> {
    match section {
        Some((bases, data)) => Ok(Some((
            u64::from(view.u32_at(bases + 4 * term_idx as u64)?),
            data,
        ))),
        None => Ok(None),
    }
}

fn decode_term_postings(
    view: &PayloadView<'_>,
    runs: (u64, u64, usize),
    term_base: Option<(u64, u64)>,
    pl_base: Option<(u64, u64)>,
) -> io::Result<Vec<Posting>> {
    let (doc_run_off, occ_run_off, df) = runs;
    let mut plist = Vec::with_capacity(df);
    for j in 0..df {
        let p = doc_run_off + 12 * j as u64;
        let doc_id = view.u32_at(p)?;
        let tf = view.u32_at(p + 4)?;
        let occ_start = u64::from(view.u32_at(p + 8)?);
        let occ_end = u64::from(if j + 1 < df {
            view.u32_at(p + 12 + 8)?
        } else {
            view.u32_at(doc_run_off + 12 * df as u64)?
        });
        let mut offsets = Vec::with_capacity((occ_end - occ_start) as usize);
        for o in occ_start..occ_end {
            offsets.push((
                view.u32_at(occ_run_off + 8 * o)?,
                view.u32_at(occ_run_off + 8 * o + 4)?,
            ));
        }
        let positions = read_ordinal_slice(view, term_base, occ_start, occ_end)?;
        let pos_lengths = read_ordinal_slice(view, pl_base, occ_start, occ_end)?;
        plist.push(Posting {
            doc_id,
            tf,
            offsets,
            positions,
            pos_lengths,
        });
    }
    Ok(plist)
}

fn read_ordinal_slice(
    view: &PayloadView<'_>,
    base_and_data: Option<(u64, u64)>,
    occ_start: u64,
    occ_end: u64,
) -> io::Result<Vec<u32>> {
    let Some((base, data)) = base_and_data else {
        return Ok(Vec::new());
    };
    let mut out = Vec::with_capacity((occ_end - occ_start) as usize);
    for o in occ_start..occ_end {
        out.push(view.u32_at(data + 4 * (base + o))?);
    }
    Ok(out)
}
