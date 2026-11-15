//! Shared v6/v7 section-layout computation and v8 atomic framing for `Bm25Store` and `SpillBuilder`.
//!
//! Both builders produce byte-for-byte identical `.bm25` files over the same
//! corpus. This module computes the exact byte offsets of the v6/v7 header,
//! field table, column table, and section bodies; [`emit`] writes the payload
//! parameterized by [`V6SectionSource`].

mod emit;

pub(in crate::postings) use self::emit::write_v6v7_payload;

use std::io::{self, Write};
use std::path::{Path, PathBuf};

use super::columns::{integer_map_sections, ColumnSet};
use super::format::{
    finalize_v8, fsync_parent, positions_section_size, sentences_section_size, write_u16,
    write_u32, write_u64, POSITIONS_ENTRY_PREFIX, POS_LENGTHS_ENTRY_PREFIX, SENTENCES_ENTRY_PREFIX,
    SOURCE_ENTRY_BYTES, SPLADE_ENTRY_PREFIX,
};
use super::DocLineage;

/// Precomputed geometry and metadata of one text field for the v6/v7 writer.
pub(in crate::postings) struct FieldSectionDesc<'a> {
    pub(in crate::postings) name: &'a str,
    pub(in crate::postings) analysis_fingerprint: u64,
    pub(in crate::postings) total_length: u64,
    pub(in crate::postings) doc_lengths: &'a [u32],
    pub(in crate::postings) postings_size: u64,
    pub(in crate::postings) directory_size: u64,
    pub(in crate::postings) n_terms: u64,
    /// `Some(total_occurrences)` iff the field keeps token positions (kind 7).
    pub(in crate::postings) positions_total: Option<u64>,
    /// `Some(total_spans)` iff the field keeps sentence spans (kind 8).
    pub(in crate::postings) sentences_total: Option<u64>,
    /// `Some(total_occurrences)` iff the field keeps token-lattice lengths (kind 18).
    pub(in crate::postings) pos_lengths_total: Option<u64>,
    /// Non-empty calibrated quantized SPLADE index (kind 19), if present.
    pub(in crate::postings) splade: Option<&'a crate::turbo_bridge::splade::SpladeFieldIndex>,
}

/// Complete corpus and column description passed to [`write_v6v7_payload`].
pub(in crate::postings) struct V6PayloadParams<'a> {
    pub(in crate::postings) n_slots: u64,
    pub(in crate::postings) texts_size: u64,
    pub(in crate::postings) lineages: &'a [Option<DocLineage>],
    pub(in crate::postings) fields: &'a [FieldSectionDesc<'a>],
    pub(in crate::postings) columns: &'a ColumnSet,
    pub(in crate::postings) sources: &'a crate::source_archive::SourceArchive,
    pub(in crate::postings) byte_order: bool,
}

/// Streaming callbacks for sections whose backing store differs between
/// heap (`Bm25Store`) and merged spill runs (`SpillBuilder`).
pub(in crate::postings) trait V6SectionSource {
    fn write_texts<W: Write>(&mut self, w: &mut W) -> io::Result<()>;
    fn write_text_index<W: Write>(&mut self, w: &mut W) -> io::Result<()>;
    fn write_field_postings_and_directory<W: Write>(
        &mut self,
        w: &mut W,
        fi: usize,
    ) -> io::Result<()>;
    fn write_field_positions<W: Write>(
        &mut self,
        w: &mut W,
        fi: usize,
        total: u64,
    ) -> io::Result<()>;
    fn write_field_sentences<W: Write>(
        &mut self,
        w: &mut W,
        fi: usize,
        n_slots: u64,
        total: u64,
    ) -> io::Result<()>;
    fn write_field_pos_lengths<W: Write>(
        &mut self,
        w: &mut W,
        fi: usize,
        total: u64,
    ) -> io::Result<()>;
}

/// Write a v6/v7 payload to a temporary sibling file, stamp the v8 CRC
/// integrity table, rename atomically to `path`, and fsync the parent directory.
pub(in crate::postings) fn save_v8_atomic(
    path: &Path,
    write_payload: impl FnOnce(&mut io::BufWriter<std::fs::File>) -> io::Result<()>,
) -> io::Result<()> {
    let tmp: PathBuf = path.with_extension("bm25tmp");
    {
        let mut w = io::BufWriter::new(std::fs::File::create(&tmp)?);
        write_payload(&mut w)?;
        w.flush()?;
    }
    finalize_v8(&tmp)?;
    std::fs::rename(&tmp, path)?;
    fsync_parent(path)
}

/// Emit `text_index` entries (`n_slots x (u64 offset, u32 len)`) from per-slot
/// byte lengths (`u32::MAX` = absent), rebased by `index_base`.
pub(in crate::postings) fn write_text_index_from_lens<W: Write>(
    w: &mut W,
    index_base: u64,
    lens: impl IntoIterator<Item = u32>,
) -> io::Result<()> {
    let mut cursor = index_base;
    for len in lens {
        if len == u32::MAX {
            write_u64(w, 0)?;
            write_u32(w, u32::MAX)?;
            cursor += 4;
        } else {
            write_u64(w, cursor + 4)?;
            write_u32(w, len)?;
            cursor += 4 + u64::from(len);
        }
    }
    Ok(())
}

/// Emit the variable-stride `lineages` section (1 B absent, 25 B present).
fn write_lineages<W: Write>(w: &mut W, lineages: &[Option<DocLineage>]) -> io::Result<()> {
    for lineage in lineages {
        match lineage {
            Some(l) => {
                w.write_all(&[1u8])?;
                write_u64(w, l.parent_id)?;
                write_u64(w, l.group_id)?;
                write_u32(w, l.span_start)?;
                write_u32(w, l.span_end)?;
            }
            None => w.write_all(&[0u8])?,
        }
    }
    Ok(())
}

/// Emit a 34-byte block-max term directory: `u32 count`, fixed-stride 34 B
/// entries, then concatenated UTF-8 term bytes.
pub(in crate::postings) fn write_block_max_directory<'a, W: Write>(
    w: &mut W,
    count: usize,
    entries: impl IntoIterator<Item = (&'a str, u64, u64, u64, u32)> + Clone,
) -> io::Result<()> {
    write_u32(w, count as u32)?;
    let mut blob_off = 0u64;
    for (term, doc_off, skip_off, occ_off, df) in entries.clone() {
        write_u64(w, doc_off)?;
        write_u64(w, skip_off)?;
        write_u64(w, occ_off)?;
        write_u32(w, df)?;
        write_u32(w, u32::try_from(blob_off).expect("term blob exceeds u32"))?;
        write_u16(w, term.len() as u16)?;
        blob_off += term.len() as u64;
    }
    for (term, ..) in entries {
        w.write_all(term.as_bytes())?;
    }
    Ok(())
}

struct V6Layout {
    has_columns: bool,
    texts_off: u64,
    text_index_off: u64,
    lineages_off: u64,
    section_offs: Vec<(u64, u64, u64)>,
    facet_offs: Vec<(u64, u64)>,
    numeric_offs: Vec<u64>,
    map_facet_offs: Vec<(u64, u64, u64, u64)>,
    map_numeric_offs: Vec<(u64, u64, u64)>,
    integer_offs: Vec<u64>,
    geo_offs: Vec<u64>,
    positions_offs: Vec<(usize, u64, u64)>,
    sentences_offs: Vec<(usize, u64, u64)>,
    unsigned_offs: Vec<u64>,
    map_integer_sections: Vec<(u64, u64)>,
    map_unsigned_sections: Vec<(u64, u64)>,
    source_section: Option<(u64, u64)>,
    pos_lengths_offs: Vec<(usize, u64, u64)>,
    splade_offs: Vec<(usize, u64, u64)>,
}

fn compute_header_size(params: &V6PayloadParams<'_>, has_columns: bool) -> u64 {
    let column_table_size = if !has_columns {
        0
    } else {
        4 + params.columns.table_entries_size()
            + field_entry_bytes(params.fields, POSITIONS_ENTRY_PREFIX, |f| {
                f.positions_total.is_some()
            })
            + field_entry_bytes(params.fields, SENTENCES_ENTRY_PREFIX, |f| {
                f.sentences_total.is_some()
            })
            + if params.sources.is_empty() {
                0
            } else {
                SOURCE_ENTRY_BYTES
            }
            + field_entry_bytes(params.fields, POS_LENGTHS_ENTRY_PREFIX, |f| {
                f.pos_lengths_total.is_some()
            })
            + field_entry_bytes(params.fields, SPLADE_ENTRY_PREFIX, |f| f.splade.is_some())
    };
    8 + 4
        + 4
        + 8 * 3
        + params
            .fields
            .iter()
            .map(|f| 2 + f.name.len() as u64 + 8 * 5)
            .sum::<u64>()
        + column_table_size
}

fn compute_layout(params: &V6PayloadParams<'_>) -> io::Result<V6Layout> {
    let n_slots = params.n_slots;
    let cols = params.columns;
    let has_columns = !cols.is_empty()
        || !params.sources.is_empty()
        || params.fields.iter().any(|f| {
            f.positions_total.is_some() || f.sentences_total.is_some() || f.splade.is_some()
        });
    let texts_off = compute_header_size(params, has_columns);
    let text_index_off = texts_off + params.texts_size;
    let lineages_off = text_index_off + 12 * n_slots;
    let lineages_size: u64 = params
        .lineages
        .iter()
        .map(|l| if l.is_some() { 25 } else { 1 })
        .sum();
    let mut cursor = lineages_off + lineages_size;

    let mut section_offs = Vec::with_capacity(params.fields.len());
    for f in params.fields {
        let dl_off = cursor;
        let p_off = dl_off + 4 * n_slots;
        let d_off = p_off + f.postings_size;
        section_offs.push((dl_off, p_off, d_off));
        cursor = d_off + f.directory_size;
    }

    let (facet_offs, numeric_offs, map_facet_offs, map_numeric_offs, integer_offs, geo_offs) =
        compute_early_column_offs(cols, n_slots, &mut cursor);
    let tail = compute_tail_column_offs(params, &mut cursor)?;

    Ok(V6Layout {
        has_columns,
        texts_off,
        text_index_off,
        lineages_off,
        section_offs,
        facet_offs,
        numeric_offs,
        map_facet_offs,
        map_numeric_offs,
        integer_offs,
        geo_offs,
        positions_offs: tail.positions_offs,
        sentences_offs: tail.sentences_offs,
        unsigned_offs: tail.unsigned_offs,
        map_integer_sections: tail.map_integer_sections,
        map_unsigned_sections: tail.map_unsigned_sections,
        source_section: tail.source_section,
        pos_lengths_offs: tail.pos_lengths_offs,
        splade_offs: tail.splade_offs,
    })
}

struct TailColumnOffs {
    positions_offs: Vec<(usize, u64, u64)>,
    sentences_offs: Vec<(usize, u64, u64)>,
    unsigned_offs: Vec<u64>,
    map_integer_sections: Vec<(u64, u64)>,
    map_unsigned_sections: Vec<(u64, u64)>,
    source_section: Option<(u64, u64)>,
    pos_lengths_offs: Vec<(usize, u64, u64)>,
    splade_offs: Vec<(usize, u64, u64)>,
}

fn compute_tail_column_offs(
    params: &V6PayloadParams<'_>,
    cursor: &mut u64,
) -> io::Result<TailColumnOffs> {
    let n_slots = params.n_slots;
    let cols = params.columns;
    let mut positions_offs = Vec::new();
    for (fi, f) in params.fields.iter().enumerate() {
        if let Some(total) = f.positions_total {
            positions_offs.push((fi, *cursor, total));
            *cursor += positions_section_size(f.n_terms, total);
        }
    }
    let mut sentences_offs = Vec::new();
    for (fi, f) in params.fields.iter().enumerate() {
        if let Some(total) = f.sentences_total {
            sentences_offs.push((fi, *cursor, total));
            *cursor += sentences_section_size(n_slots, total);
        }
    }
    let mut unsigned_offs = Vec::with_capacity(cols.unsigned_integers.len());
    for _ in &cols.unsigned_integers {
        unsigned_offs.push(*cursor);
        *cursor += 8 * n_slots + n_slots.div_ceil(8);
    }
    let map_integer_sections = integer_map_sections(&cols.map_integers, n_slots as u32, cursor)?;
    let map_unsigned_sections =
        integer_map_sections(&cols.map_unsigned_integers, n_slots as u32, cursor)?;
    let source_section = if params.sources.is_empty() {
        None
    } else {
        let len = params.sources.encoded_len(n_slots as u32)?;
        let off = *cursor;
        *cursor += len;
        Some((off, len))
    };
    let mut pos_lengths_offs = Vec::new();
    for (fi, f) in params.fields.iter().enumerate() {
        if let Some(total) = f.pos_lengths_total {
            pos_lengths_offs.push((fi, *cursor, total));
            *cursor += positions_section_size(f.n_terms, total);
        }
    }
    let mut splade_offs = Vec::new();
    for (fi, f) in params.fields.iter().enumerate() {
        if let Some(splade) = f.splade {
            let len = splade.encoded_len();
            splade_offs.push((fi, *cursor, len));
            *cursor += len;
        }
    }
    Ok(TailColumnOffs {
        positions_offs,
        sentences_offs,
        unsigned_offs,
        map_integer_sections,
        map_unsigned_sections,
        source_section,
        pos_lengths_offs,
        splade_offs,
    })
}

type EarlyColumnOffs = (
    Vec<(u64, u64)>,
    Vec<u64>,
    Vec<(u64, u64, u64, u64)>,
    Vec<(u64, u64, u64)>,
    Vec<u64>,
    Vec<u64>,
);

fn compute_early_column_offs(cols: &ColumnSet, n_slots: u64, cursor: &mut u64) -> EarlyColumnOffs {
    let mut facet_offs = Vec::with_capacity(cols.facets.len());
    for facet in &cols.facets {
        let dict_off = *cursor;
        let dict_size: u64 = facet.dict.iter().map(|v| 2 + v.len() as u64).sum();
        let ords_off = dict_off + dict_size;
        facet_offs.push((dict_off, ords_off));
        *cursor = ords_off + 4 * n_slots;
    }
    let mut numeric_offs = Vec::with_capacity(cols.numerics.len());
    for _ in &cols.numerics {
        numeric_offs.push(*cursor);
        *cursor += 8 * n_slots;
    }
    let mut map_facet_offs = Vec::with_capacity(cols.map_facets.len());
    for c in &cols.map_facets {
        let keys_off = *cursor;
        let keys_size: u64 = c.keys.iter().map(|k| 2 + k.len() as u64).sum();
        let values_off = keys_off + keys_size;
        let values_size: u64 = c.values.iter().map(|v| 2 + v.len() as u64).sum();
        let offsets_off = values_off + values_size;
        let pairs_off = offsets_off + 4 * (n_slots + 1);
        let total_pairs: u64 = c.pairs.iter().map(|l| l.len() as u64).sum();
        map_facet_offs.push((keys_off, values_off, offsets_off, pairs_off));
        *cursor = pairs_off + 8 * total_pairs;
    }
    let mut map_numeric_offs = Vec::with_capacity(cols.map_numerics.len());
    for c in &cols.map_numerics {
        let keys_off = *cursor;
        let keys_size: u64 = c.keys.iter().map(|k| 2 + k.len() as u64 + 16).sum();
        let offsets_off = keys_off + keys_size;
        let pairs_off = offsets_off + 4 * (n_slots + 1);
        let total_pairs: u64 = c.pairs.iter().map(|l| l.len() as u64).sum();
        map_numeric_offs.push((keys_off, offsets_off, pairs_off));
        *cursor = pairs_off + 12 * total_pairs;
    }
    let mut integer_offs = Vec::with_capacity(cols.integers.len());
    for _ in &cols.integers {
        integer_offs.push(*cursor);
        *cursor += 8 * n_slots + n_slots.div_ceil(8);
    }
    let mut geo_offs = Vec::with_capacity(cols.geos.len());
    for _ in &cols.geos {
        geo_offs.push(*cursor);
        *cursor += 16 * n_slots;
    }
    (
        facet_offs,
        numeric_offs,
        map_facet_offs,
        map_numeric_offs,
        integer_offs,
        geo_offs,
    )
}

fn field_entry_bytes(
    fields: &[FieldSectionDesc<'_>],
    prefix: &str,
    pred: impl Fn(&FieldSectionDesc<'_>) -> bool,
) -> u64 {
    fields
        .iter()
        .filter(|f| pred(f))
        .map(|f| 2 + (prefix.len() + f.name.len()) as u64 + 1 + 8 * 2)
        .sum()
}
