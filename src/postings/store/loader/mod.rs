//! Deserialization of `.bm25` v1 through v8 files back into `Bm25Store`.

mod v6;
mod v6_columns;

use std::collections::BTreeMap;
use std::io;
use std::path::Path;

use super::super::columns::ColumnSet;
use super::super::format::{
    parse_integrity, read_u32, read_u64, take, u16_at, MAGIC_V1, MAGIC_V2, MAGIC_V3, MAGIC_V4,
    MAGIC_V5, MAGIC_V6, MAGIC_V7, MAGIC_V8,
};
use super::super::reader::validate_structure_v6;
use super::super::{DocLineage, Posting};
use super::{Bm25Store, FieldStore};

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

    /// A single-field ("body") store from already-decoded parts — the
    /// shape every pre-v6 read path produces.
    fn from_single_field(
        postings: BTreeMap<String, Vec<Posting>>,
        doc_lengths: Vec<u32>,
        total_length: u64,
        texts: Vec<Option<String>>,
        lineages: Vec<Option<DocLineage>>,
    ) -> Self {
        Self {
            fields: vec![FieldStore {
                name: "body".to_string(),
                analysis_fingerprint: 0,
                postings,
                doc_lengths,
                total_length,
                positions: false,
                has_lattice: false,
                sentences: false,
                doc_sentences: Vec::new(),
                splade: None,
            }],
            texts,
            lineages,
            columns: ColumnSet::default(),
            sources: Default::default(),
        }
    }

    fn read_from(r: &mut &[u8]) -> io::Result<Self> {
        let original = *r;
        let magic = take(r, 8)?;
        if magic == MAGIC_V8 {
            return Self::read_v8_from(r);
        }
        if magic == MAGIC_V7 {
            validate_structure_v6(original, true)?;
            return Self::read_v6v7_from(r, true);
        }
        if magic == MAGIC_V6 {
            validate_structure_v6(original, false)?;
            return Self::read_v6v7_from(r, false);
        }
        if magic == MAGIC_V5 {
            return Self::read_v5_from(r);
        }
        if magic == MAGIC_V4 || magic == MAGIC_V3 {
            return Self::read_v3_from(r);
        }
        let has_lineage = if magic == MAGIC_V2 {
            true
        } else if magic == MAGIC_V1 {
            false
        } else {
            return Err(invalid("bad magic"));
        };
        Self::read_v1v2_from(r, has_lineage)
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

    fn read_v1v2_from(r: &mut &[u8], has_lineage: bool) -> io::Result<Self> {
        let n_slots = read_u32(r)? as usize;
        let mut doc_lengths = Vec::with_capacity(n_slots);
        for _ in 0..n_slots {
            doc_lengths.push(read_u32(r)?);
        }
        let total_length = read_u64(r)?;
        let n_texts = read_u32(r)? as usize;
        if n_texts != n_slots {
            return Err(invalid("text slots != length slots"));
        }
        let texts = read_stream_texts(r, n_texts)?;
        let mut lineages = vec![None; n_texts];
        if has_lineage {
            read_stream_lineages(r, &mut lineages)?;
        }
        let postings = read_stream_postings(r)?;
        if !r.is_empty() {
            return Err(invalid("trailing bytes"));
        }
        Ok(Self::from_single_field(
            postings,
            doc_lengths,
            total_length,
            texts,
            lineages,
        ))
    }

    /// Parse the v3 sections back into a heap store (used when a
    /// disk-resident shard receives more documents: the append path is
    /// bulk-load, build-in-memory-then-flush again).
    fn read_v3_from(r: &mut &[u8]) -> io::Result<Self> {
        let total_length = read_u64(r)?;
        let _texts_off = read_u64(r)?;
        let _lineages_off = read_u64(r)?;
        let _postings_off = read_u64(r)?;
        let _directory_off = read_u64(r)?;
        let n_slots = read_u32(r)? as usize;
        let mut doc_lengths = Vec::with_capacity(n_slots);
        for _ in 0..n_slots {
            doc_lengths.push(read_u32(r)?);
        }
        let texts = read_stream_texts(r, n_slots)?;
        let mut lineages = Vec::with_capacity(n_slots);
        for _ in 0..n_slots {
            let _offset = read_u64(r)?;
            let _len = read_u32(r)?;
            lineages.push(None);
        }
        read_stream_lineages(r, &mut lineages)?;
        let postings = read_stream_postings(r)?;
        Ok(Self::from_single_field(
            postings,
            doc_lengths,
            total_length,
            texts,
            lineages,
        ))
    }

    /// Decode one v5-shaped postings/directory section pair back into a
    /// heap postings map (shared by the v5 reload path and every field
    /// of the v6 reload path). `all` starts at file offset 8 (the magic
    /// is consumed); `run_base` rebases the directory's run offsets — 0
    /// for v5 (absolute entries), the field's postings section offset
    /// for v6 (section-relative entries). The skip run is not needed in
    /// heap form and is skipped.
    pub(super) fn read_v5_shaped_postings(
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

    /// Parse a v5 file back into a heap store (same caller contract as
    /// [`Self::read_v3_from`]: a disk-resident shard about to receive more
    /// documents). The skip run is not needed in heap form and is skipped.
    fn read_v5_from(r: &mut &[u8]) -> io::Result<Self> {
        let all: &[u8] = r;
        let total_length = read_u64(r)?;
        let _texts_off = read_u64(r)?;
        let _lineages_off = read_u64(r)?;
        let _postings_off = read_u64(r)?;
        let directory_off = read_u64(r)?;
        let n_slots = read_u32(r)? as usize;
        let mut doc_lengths = Vec::with_capacity(n_slots);
        for _ in 0..n_slots {
            doc_lengths.push(read_u32(r)?);
        }
        let texts = read_stream_texts(r, n_slots)?;
        take(r, 12 * n_slots)?;
        let mut lineages = vec![None; n_slots];
        read_stream_lineages(r, &mut lineages)?;
        let postings = Self::read_v5_shaped_postings(all, directory_off, 0, None, None)?;
        Ok(Self::from_single_field(
            postings,
            doc_lengths,
            total_length,
            texts,
            lineages,
        ))
    }
}

fn read_stream_texts(r: &mut &[u8], n_slots: usize) -> io::Result<Vec<Option<String>>> {
    let mut texts = Vec::with_capacity(n_slots);
    for _ in 0..n_slots {
        let len = read_u32(r)?;
        if len == u32::MAX {
            texts.push(None);
        } else {
            let bytes = take(r, len as usize)?;
            texts.push(Some(
                String::from_utf8(bytes.to_vec())
                    .map_err(|_| invalid("invalid utf-8 in doc text"))?,
            ));
        }
    }
    Ok(texts)
}

fn read_stream_lineages(r: &mut &[u8], lineages: &mut [Option<DocLineage>]) -> io::Result<()> {
    for lineage in lineages.iter_mut() {
        if take(r, 1)?[0] != 0 {
            *lineage = Some(DocLineage {
                parent_id: read_u64(r)?,
                group_id: read_u64(r)?,
                span_start: read_u32(r)?,
                span_end: read_u32(r)?,
            });
        }
    }
    Ok(())
}

fn read_stream_postings(r: &mut &[u8]) -> io::Result<BTreeMap<String, Vec<Posting>>> {
    let n_terms = read_u32(r)? as usize;
    let mut postings = BTreeMap::new();
    for _ in 0..n_terms {
        let term_len = read_u32(r)? as usize;
        let term = String::from_utf8(take(r, term_len)?.to_vec())
            .map_err(|_| invalid("invalid utf-8 in term"))?;
        let n_postings = read_u32(r)? as usize;
        let mut plist = Vec::with_capacity(n_postings);
        for _ in 0..n_postings {
            let doc_id = read_u32(r)?;
            let tf = read_u32(r)?;
            let n_offsets = read_u32(r)? as usize;
            let mut offsets = Vec::with_capacity(n_offsets);
            for _ in 0..n_offsets {
                offsets.push((read_u32(r)?, read_u32(r)?));
            }
            plist.push(Posting {
                doc_id,
                tf,
                offsets,
                positions: Vec::new(),
                pos_lengths: Vec::new(),
            });
        }
        postings.insert(term, plist);
    }
    Ok(postings)
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
