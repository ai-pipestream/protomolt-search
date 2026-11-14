//! Header, column-table, and section-body serialization for v6/v7 `.bm25` payloads.

use std::io::{self, Write};

use super::super::columns::{
    write_binding_entry, write_derived_entry, write_facet_column, write_integer_map_entries,
    write_map_facet_column, write_map_numeric_column, ColumnSet, GEO_ABSENT,
};
use super::super::format::{
    write_source_entry, write_u16, write_u32, write_u64, COLUMN_KIND_F64, COLUMN_KIND_FACET,
    COLUMN_KIND_GEO, COLUMN_KIND_I64_PRESENT, COLUMN_KIND_MAP_F64, COLUMN_KIND_MAP_FACET,
    COLUMN_KIND_MAP_I64, COLUMN_KIND_MAP_U64, COLUMN_KIND_POSITIONS, COLUMN_KIND_POS_LENGTHS,
    COLUMN_KIND_SENTENCES, COLUMN_KIND_SPLADE, COLUMN_KIND_U64, MAGIC_V6, MAGIC_V7,
    POSITIONS_ENTRY_PREFIX, POS_LENGTHS_ENTRY_PREFIX, SENTENCES_ENTRY_PREFIX, SPLADE_ENTRY_PREFIX,
};
use super::{
    compute_layout, write_lineages, FieldSectionDesc, V6Layout, V6PayloadParams, V6SectionSource,
};

fn write_field_column_entries<W: Write>(
    w: &mut W,
    fields: &[FieldSectionDesc<'_>],
    prefix: &str,
    kind: u8,
    offs: &[(usize, u64, u64)],
) -> io::Result<()> {
    for &(fi, off, second) in offs {
        let name = format!("{prefix}{}", fields[fi].name);
        write_u16(w, name.len() as u16)?;
        w.write_all(name.as_bytes())?;
        w.write_all(&[kind])?;
        write_u64(w, off)?;
        write_u64(w, second)?;
    }
    Ok(())
}

fn write_header<W: Write>(
    w: &mut W,
    params: &V6PayloadParams<'_>,
    layout: &V6Layout,
) -> io::Result<()> {
    w.write_all(if layout.has_columns {
        MAGIC_V7
    } else {
        MAGIC_V6
    })?;
    write_u32(w, params.fields.len() as u32)?;
    write_u32(w, params.n_slots as u32)?;
    write_u64(w, layout.texts_off)?;
    write_u64(w, layout.text_index_off)?;
    write_u64(w, layout.lineages_off)?;
    for (field, &(dl_off, p_off, d_off)) in params.fields.iter().zip(&layout.section_offs) {
        write_u16(w, field.name.len() as u16)?;
        w.write_all(field.name.as_bytes())?;
        write_u64(w, field.analysis_fingerprint)?;
        write_u64(w, field.total_length)?;
        write_u64(w, dl_off)?;
        write_u64(w, p_off)?;
        write_u64(w, d_off)?;
    }
    if layout.has_columns {
        write_column_table(w, params, layout)?;
    }
    Ok(())
}

fn write_early_column_entries<W: Write>(
    w: &mut W,
    cols: &ColumnSet,
    layout: &V6Layout,
) -> io::Result<()> {
    for (facet, &(dict_off, ords_off)) in cols.facets.iter().zip(&layout.facet_offs) {
        write_u16(w, facet.name.len() as u16)?;
        w.write_all(facet.name.as_bytes())?;
        w.write_all(&[COLUMN_KIND_FACET])?;
        write_u32(w, facet.dict.len() as u32)?;
        write_u64(w, dict_off)?;
        write_u64(w, ords_off)?;
    }
    for (numeric, &vals_off) in cols.numerics.iter().zip(&layout.numeric_offs) {
        let (min, max) = numeric.min_max();
        write_u16(w, numeric.name.len() as u16)?;
        w.write_all(numeric.name.as_bytes())?;
        w.write_all(&[COLUMN_KIND_F64])?;
        write_u64(w, min.to_bits())?;
        write_u64(w, max.to_bits())?;
        write_u64(w, vals_off)?;
    }
    for (c, &(keys_off, values_off, offsets_off, pairs_off)) in
        cols.map_facets.iter().zip(&layout.map_facet_offs)
    {
        write_u16(w, c.name.len() as u16)?;
        w.write_all(c.name.as_bytes())?;
        w.write_all(&[COLUMN_KIND_MAP_FACET])?;
        write_u32(w, c.keys.len() as u32)?;
        write_u32(w, c.values.len() as u32)?;
        write_u64(w, keys_off)?;
        write_u64(w, values_off)?;
        write_u64(w, offsets_off)?;
        write_u64(w, pairs_off)?;
    }
    for (c, &(keys_off, offsets_off, pairs_off)) in
        cols.map_numerics.iter().zip(&layout.map_numeric_offs)
    {
        write_u16(w, c.name.len() as u16)?;
        w.write_all(c.name.as_bytes())?;
        w.write_all(&[COLUMN_KIND_MAP_F64])?;
        write_u32(w, c.keys.len() as u32)?;
        write_u64(w, keys_off)?;
        write_u64(w, offsets_off)?;
        write_u64(w, pairs_off)?;
    }
    for (c, &vals_off) in cols.integers.iter().zip(&layout.integer_offs) {
        let (min, max) = c.min_max();
        write_u16(w, c.name.len() as u16)?;
        w.write_all(c.name.as_bytes())?;
        w.write_all(&[COLUMN_KIND_I64_PRESENT])?;
        write_u64(w, min as u64)?;
        write_u64(w, max as u64)?;
        write_u64(w, vals_off)?;
    }
    for (c, &vals_off) in cols.geos.iter().zip(&layout.geo_offs) {
        let (min_lat, max_lat, min_lon, max_lon) = c.bbox();
        write_u16(w, c.name.len() as u16)?;
        w.write_all(c.name.as_bytes())?;
        w.write_all(&[COLUMN_KIND_GEO])?;
        write_u64(w, min_lat.to_bits())?;
        write_u64(w, max_lat.to_bits())?;
        write_u64(w, min_lon.to_bits())?;
        write_u64(w, max_lon.to_bits())?;
        write_u64(w, vals_off)?;
    }
    Ok(())
}

fn write_column_table<W: Write>(
    w: &mut W,
    params: &V6PayloadParams<'_>,
    layout: &V6Layout,
) -> io::Result<()> {
    let cols = params.columns;
    let total_entries = cols.table_entry_count()
        + layout.positions_offs.len()
        + layout.sentences_offs.len()
        + usize::from(!params.sources.is_empty())
        + layout.pos_lengths_offs.len()
        + layout.splade_offs.len();
    write_u32(w, total_entries as u32)?;
    write_early_column_entries(w, cols, layout)?;
    write_field_column_entries(
        w,
        params.fields,
        POSITIONS_ENTRY_PREFIX,
        COLUMN_KIND_POSITIONS,
        &layout.positions_offs,
    )?;
    write_field_column_entries(
        w,
        params.fields,
        SENTENCES_ENTRY_PREFIX,
        COLUMN_KIND_SENTENCES,
        &layout.sentences_offs,
    )?;
    for (column, &off) in cols.unsigned_integers.iter().zip(&layout.unsigned_offs) {
        let (min, max) = column.min_max();
        write_u16(w, column.name.len() as u16)?;
        w.write_all(column.name.as_bytes())?;
        w.write_all(&[COLUMN_KIND_U64])?;
        write_u64(w, min)?;
        write_u64(w, max)?;
        write_u64(w, off)?;
    }
    write_integer_map_entries(
        w,
        &cols.map_integers,
        &layout.map_integer_sections,
        COLUMN_KIND_MAP_I64,
    )?;
    write_integer_map_entries(
        w,
        &cols.map_unsigned_integers,
        &layout.map_unsigned_sections,
        COLUMN_KIND_MAP_U64,
    )?;
    write_binding_entry(w, cols.binding.as_ref())?;
    write_derived_entry(w, cols.derived.as_ref())?;
    write_source_entry(w, layout.source_section)?;
    write_field_column_entries(
        w,
        params.fields,
        POS_LENGTHS_ENTRY_PREFIX,
        COLUMN_KIND_POS_LENGTHS,
        &layout.pos_lengths_offs,
    )?;
    write_field_column_entries(
        w,
        params.fields,
        SPLADE_ENTRY_PREFIX,
        COLUMN_KIND_SPLADE,
        &layout.splade_offs,
    )
}

fn write_early_columns<W: Write>(
    w: &mut W,
    cols: &ColumnSet,
    n_slots: usize,
    byte_order: bool,
) -> io::Result<()> {
    for facet in &cols.facets {
        write_facet_column(w, facet, n_slots, byte_order)?;
    }
    for numeric in &cols.numerics {
        for slot in 0..n_slots {
            write_u64(
                w,
                numeric
                    .vals
                    .get(slot)
                    .copied()
                    .unwrap_or(f64::NAN)
                    .to_bits(),
            )?;
        }
    }
    for c in &cols.map_facets {
        write_map_facet_column(w, c, n_slots, byte_order)?;
    }
    for c in &cols.map_numerics {
        write_map_numeric_column(w, c, n_slots, byte_order)?;
    }
    for c in &cols.integers {
        c.write(w, n_slots)?;
    }
    for c in &cols.geos {
        for slot in 0..n_slots {
            let (lat, lon) = c.vals.get(slot).copied().unwrap_or(GEO_ABSENT);
            write_u64(w, lat.to_bits())?;
            write_u64(w, lon.to_bits())?;
        }
    }
    Ok(())
}

fn write_late_columns<W: Write>(
    w: &mut W,
    params: &V6PayloadParams<'_>,
    has_sources: bool,
) -> io::Result<()> {
    let n_slots = params.n_slots as usize;
    for column in &params.columns.unsigned_integers {
        column.write(w, n_slots)?;
    }
    for column in &params.columns.map_integers {
        column.write(w, params.n_slots as u32)?;
    }
    for column in &params.columns.map_unsigned_integers {
        column.write(w, params.n_slots as u32)?;
    }
    if has_sources {
        params.sources.write(w, params.n_slots as u32)?;
    }
    Ok(())
}

/// Serialize a complete v6 or v7 `.bm25` payload to `w`.
pub(in crate::postings) fn write_v6v7_payload<W: Write>(
    w: &mut W,
    params: &V6PayloadParams<'_>,
    source: &mut impl V6SectionSource,
) -> io::Result<()> {
    let layout = compute_layout(params)?;
    write_header(w, params, &layout)?;

    source.write_texts(w)?;
    source.write_text_index(w)?;
    write_lineages(w, params.lineages)?;

    for (fi, field) in params.fields.iter().enumerate() {
        debug_assert_eq!(field.doc_lengths.len() as u64, params.n_slots);
        for &len in field.doc_lengths {
            write_u32(w, len)?;
        }
        source.write_field_postings_and_directory(w, fi)?;
    }

    write_early_columns(
        w,
        params.columns,
        params.n_slots as usize,
        params.byte_order,
    )?;

    for &(fi, _, total) in &layout.positions_offs {
        source.write_field_positions(w, fi, total)?;
    }
    for &(fi, _, total) in &layout.sentences_offs {
        source.write_field_sentences(w, fi, params.n_slots, total)?;
    }

    write_late_columns(w, params, layout.source_section.is_some())?;

    for &(fi, _, total) in &layout.pos_lengths_offs {
        source.write_field_pos_lengths(w, fi, total)?;
    }
    for &(fi, _, _) in &layout.splade_offs {
        if let Some(splade) = params.fields[fi].splade {
            splade.write(w)?;
        }
    }
    Ok(())
}
