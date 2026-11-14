//! v7 column-table parsing and section-boundary helpers for structural validation.

use std::io;

use super::super::super::columns::{decode_binding_entry, decode_derived_entry};
use super::super::super::format::{
    compatible_column_kind, u16_at, BINDING_ENTRY_NAME, COLUMN_KIND_ANALYSIS_BINDING,
    COLUMN_KIND_BINDING, COLUMN_KIND_DERIVED, COLUMN_KIND_F64, COLUMN_KIND_FACET, COLUMN_KIND_GEO,
    COLUMN_KIND_I64, COLUMN_KIND_I64_PRESENT, COLUMN_KIND_INDEX_BINDING, COLUMN_KIND_MAP_F64,
    COLUMN_KIND_MAP_FACET, COLUMN_KIND_MAP_I64, COLUMN_KIND_MAP_U64, COLUMN_KIND_POSITIONS,
    COLUMN_KIND_POS_LENGTHS, COLUMN_KIND_SENTENCES, COLUMN_KIND_SOURCES, COLUMN_KIND_SPLADE,
    COLUMN_KIND_U64, COLUMN_KIND_VECTOR_BINDING, DERIVED_ENTRY_NAME, POSITIONS_ENTRY_PREFIX,
    POS_LENGTHS_ENTRY_PREFIX, SENTENCES_ENTRY_PREFIX, SOURCES_ENTRY_NAME, SPLADE_ENTRY_PREFIX,
};
use super::{invalid, MapView};

#[derive(Default)]
pub(super) struct V7Columns {
    pub(super) facets: Vec<(u32, u64, u64)>,
    pub(super) numerics: Vec<(u64, u64, u64)>,
    pub(super) map_facets: Vec<(u32, u32, u64, u64, u64, u64)>,
    pub(super) map_numerics: Vec<(u32, u64, u64, u64)>,
    pub(super) integers: Vec<(u64, u64, u64, bool)>,
    pub(super) unsigned_integers: Vec<(u64, u64, u64)>,
    pub(super) geos: Vec<(u64, u64, u64, u64, u64)>,
    pub(super) positions: Vec<(Vec<u8>, u64, u64)>,
    pub(super) sentences: Vec<(Vec<u8>, u64, u64)>,
    pub(super) pos_lengths: Vec<(Vec<u8>, u64, u64)>,
    pub(super) splades: Vec<(Vec<u8>, u64, u64)>,
    pub(super) source_section: Option<(u64, u64)>,
    pub(super) integer_maps: Vec<(u8, u64, u64)>,
    pub(super) tail_sections_start: u64,
    pub(super) integer_maps_end: u64,
    pub(super) unsigned_end: u64,
    pub(super) columns_end: u64,
}

struct ColEntryCtx<'a> {
    col_idx: u64,
    kind: u8,
    base: u64,
    col_name: &'a [u8],
    field_names: &'a [Vec<u8>],
}

struct PrefixedEntrySpec<'a> {
    col_idx: u64,
    kind: u8,
    prefix: &'a str,
    label: &'a str,
}

pub(super) fn parse_v7_columns(
    view: &MapView<'_>,
    v7: bool,
    mut cursor: u64,
    field_names: &[Vec<u8>],
) -> io::Result<(V7Columns, u64)> {
    let mut cols = V7Columns::default();
    if v7 {
        let n_columns = u64::from(view.u32_at(cursor)?);
        if n_columns == 0 {
            return Err(invalid("v7 file with zero columns"));
        }
        cursor += 4;
        let mut column_names: Vec<Vec<u8>> = Vec::new();
        for i in 0..n_columns {
            cursor = cols.parse_entry(view, cursor, i, field_names, &mut column_names)?;
        }
    }
    cols.finalize_boundaries(view.len());
    Ok((cols, cursor))
}

impl V7Columns {
    fn finalize_boundaries(&mut self, file_len: u64) {
        self.tail_sections_start = self
            .pos_lengths
            .first()
            .map(|p| p.1)
            .or_else(|| self.splades.first().map(|s| s.1))
            .unwrap_or(file_len);
        self.integer_maps_end = self
            .source_section
            .map_or(self.tail_sections_start, |(off, _)| off);
        self.unsigned_end = self
            .integer_maps
            .first()
            .map_or(self.integer_maps_end, |entry| entry.1);
        self.columns_end = self
            .unsigned_integers
            .first()
            .map_or(self.unsigned_end, |entry| entry.2);
    }

    pub(super) fn field_groups_end(&self) -> u64 {
        self.facets
            .first()
            .map(|f| f.1)
            .or_else(|| self.numerics.first().map(|n| n.2))
            .or_else(|| self.map_facets.first().map(|c| c.2))
            .or_else(|| self.map_numerics.first().map(|c| c.1))
            .or_else(|| self.integers.first().map(|c| c.2))
            .or_else(|| self.geos.first().map(|c| c.4))
            .or_else(|| self.positions.first().map(|p| p.1))
            .or_else(|| self.sentences.first().map(|s| s.1))
            .unwrap_or(self.columns_end)
    }

    pub(super) fn after_facet(&self, i: usize) -> u64 {
        self.facets
            .get(i + 1)
            .map(|f| f.1)
            .or_else(|| self.numerics.first().map(|n| n.2))
            .or_else(|| self.map_facets.first().map(|c| c.2))
            .or_else(|| self.map_numerics.first().map(|c| c.1))
            .or_else(|| self.integers.first().map(|c| c.2))
            .or_else(|| self.geos.first().map(|c| c.4))
            .or_else(|| self.positions.first().map(|p| p.1))
            .unwrap_or(self.columns_end)
    }

    pub(super) fn after_numeric(&self, i: usize) -> u64 {
        self.numerics
            .get(i + 1)
            .map(|n| n.2)
            .or_else(|| self.map_facets.first().map(|c| c.2))
            .or_else(|| self.map_numerics.first().map(|c| c.1))
            .or_else(|| self.integers.first().map(|c| c.2))
            .or_else(|| self.geos.first().map(|c| c.4))
            .or_else(|| self.positions.first().map(|p| p.1))
            .unwrap_or(self.columns_end)
    }

    pub(super) fn after_map_facet(&self, i: usize) -> u64 {
        self.map_facets
            .get(i + 1)
            .map(|c| c.2)
            .or_else(|| self.map_numerics.first().map(|c| c.1))
            .or_else(|| self.integers.first().map(|c| c.2))
            .or_else(|| self.geos.first().map(|c| c.4))
            .or_else(|| self.positions.first().map(|p| p.1))
            .unwrap_or(self.columns_end)
    }

    pub(super) fn after_map_numeric(&self, i: usize) -> u64 {
        self.map_numerics
            .get(i + 1)
            .map(|c| c.1)
            .or_else(|| self.integers.first().map(|c| c.2))
            .or_else(|| self.geos.first().map(|c| c.4))
            .or_else(|| self.positions.first().map(|p| p.1))
            .unwrap_or(self.columns_end)
    }

    pub(super) fn after_integer(&self, i: usize) -> u64 {
        self.integers
            .get(i + 1)
            .map(|c| c.2)
            .or_else(|| self.geos.first().map(|c| c.4))
            .or_else(|| self.positions.first().map(|p| p.1))
            .unwrap_or(self.columns_end)
    }

    pub(super) fn after_geo(&self, i: usize) -> u64 {
        self.geos
            .get(i + 1)
            .map(|c| c.4)
            .or_else(|| self.positions.first().map(|p| p.1))
            .unwrap_or(self.columns_end)
    }

    fn parse_entry(
        &mut self,
        view: &MapView<'_>,
        cursor: u64,
        col_idx: u64,
        field_names: &[Vec<u8>],
        column_names: &mut Vec<Vec<u8>>,
    ) -> io::Result<u64> {
        let name_len = u64::from(u16_at(view.bytes_at(cursor, 2)?));
        if name_len == 0 {
            return Err(invalid(format!("column {col_idx}: empty name")));
        }
        let name = view.bytes_at(cursor + 2, name_len)?.to_vec();
        if column_names.contains(&name) {
            return Err(invalid(format!("column {col_idx}: duplicate name")));
        }
        column_names.push(name);
        let raw_kind = view.bytes_at(cursor + 2 + name_len, 1)?[0];
        let base = cursor + 2 + name_len + 1;
        let col_name = column_names.last().expect("pushed above");
        let kind = compatible_column_kind(
            raw_kind,
            col_name,
            &view.as_slice()[base as usize..],
            view.len(),
        );
        if let Some(next) = self.parse_value_column_entry(view, kind, base)? {
            return Ok(next);
        }
        let ctx = ColEntryCtx {
            col_idx,
            kind,
            base,
            col_name,
            field_names,
        };
        if let Some(next) = self.parse_prefixed_entry(view, &ctx)? {
            return Ok(next);
        }
        self.parse_meta_entry(view, &ctx)
    }

    fn parse_value_column_entry(
        &mut self,
        view: &MapView<'_>,
        kind: u8,
        base: u64,
    ) -> io::Result<Option<u64>> {
        let next = match kind {
            COLUMN_KIND_FACET => {
                self.facets.push((
                    view.u32_at(base)?,
                    view.u64_at(base + 4)?,
                    view.u64_at(base + 12)?,
                ));
                base + 20
            }
            COLUMN_KIND_F64 => {
                self.numerics.push((
                    view.u64_at(base)?,
                    view.u64_at(base + 8)?,
                    view.u64_at(base + 16)?,
                ));
                base + 24
            }
            COLUMN_KIND_MAP_FACET => {
                self.map_facets.push((
                    view.u32_at(base)?,
                    view.u32_at(base + 4)?,
                    view.u64_at(base + 8)?,
                    view.u64_at(base + 16)?,
                    view.u64_at(base + 24)?,
                    view.u64_at(base + 32)?,
                ));
                base + 40
            }
            COLUMN_KIND_MAP_F64 => {
                self.map_numerics.push((
                    view.u32_at(base)?,
                    view.u64_at(base + 4)?,
                    view.u64_at(base + 12)?,
                    view.u64_at(base + 20)?,
                ));
                base + 28
            }
            COLUMN_KIND_MAP_I64 | COLUMN_KIND_MAP_U64 => {
                self.integer_maps
                    .push((kind, view.u64_at(base)?, view.u64_at(base + 8)?));
                base + 16
            }
            COLUMN_KIND_I64 | COLUMN_KIND_I64_PRESENT => {
                self.integers.push((
                    view.u64_at(base)?,
                    view.u64_at(base + 8)?,
                    view.u64_at(base + 16)?,
                    kind == COLUMN_KIND_I64_PRESENT,
                ));
                base + 24
            }
            COLUMN_KIND_U64 => {
                self.unsigned_integers.push((
                    view.u64_at(base)?,
                    view.u64_at(base + 8)?,
                    view.u64_at(base + 16)?,
                ));
                base + 24
            }
            COLUMN_KIND_GEO => {
                self.geos.push((
                    view.u64_at(base)?,
                    view.u64_at(base + 8)?,
                    view.u64_at(base + 16)?,
                    view.u64_at(base + 24)?,
                    view.u64_at(base + 32)?,
                ));
                base + 40
            }
            _ => return Ok(None),
        };
        Ok(Some(next))
    }

    fn parse_prefixed_entry(
        &mut self,
        view: &MapView<'_>,
        ctx: &ColEntryCtx<'_>,
    ) -> io::Result<Option<u64>> {
        let (target, prefix, label) = match ctx.kind {
            COLUMN_KIND_POSITIONS => (&mut self.positions, POSITIONS_ENTRY_PREFIX, "positions"),
            COLUMN_KIND_SENTENCES => (&mut self.sentences, SENTENCES_ENTRY_PREFIX, "sentences"),
            COLUMN_KIND_POS_LENGTHS => (
                &mut self.pos_lengths,
                POS_LENGTHS_ENTRY_PREFIX,
                "pos_lengths",
            ),
            COLUMN_KIND_SPLADE => (&mut self.splades, SPLADE_ENTRY_PREFIX, "splade"),
            _ => return Ok(None),
        };
        let payload = (view.u64_at(ctx.base)?, view.u64_at(ctx.base + 8)?);
        push_prefixed_field_entry(
            target,
            ctx.field_names,
            ctx.col_name,
            payload,
            PrefixedEntrySpec {
                col_idx: ctx.col_idx,
                kind: ctx.kind,
                prefix,
                label,
            },
        )?;
        Ok(Some(ctx.base + 16))
    }

    fn parse_meta_entry(&mut self, view: &MapView<'_>, ctx: &ColEntryCtx<'_>) -> io::Result<u64> {
        let i = ctx.col_idx;
        let base = ctx.base;
        match ctx.kind {
            COLUMN_KIND_SOURCES => {
                if ctx.col_name != SOURCES_ENTRY_NAME.as_bytes() {
                    return Err(invalid("invalid source entry name"));
                }
                self.source_section = Some((view.u64_at(base)?, view.u64_at(base + 8)?));
                Ok(base + 16)
            }
            COLUMN_KIND_BINDING
            | COLUMN_KIND_ANALYSIS_BINDING
            | COLUMN_KIND_VECTOR_BINDING
            | COLUMN_KIND_INDEX_BINDING => {
                if ctx.col_name != BINDING_ENTRY_NAME.as_bytes() {
                    return Err(invalid(format!(
                        "column {i}: kind {COLUMN_KIND_BINDING} must be named \
                         {BINDING_ENTRY_NAME:?}"
                    )));
                }
                let (_, used) = decode_binding_entry(ctx.kind, &view.as_slice()[base as usize..])?;
                Ok(base + used as u64)
            }
            COLUMN_KIND_DERIVED => {
                if ctx.col_name != DERIVED_ENTRY_NAME.as_bytes() {
                    return Err(invalid(format!(
                        "column {i}: kind {COLUMN_KIND_DERIVED} must be named \
                         {DERIVED_ENTRY_NAME:?}"
                    )));
                }
                let (_, used) = decode_derived_entry(&view.as_slice()[base as usize..])?;
                Ok(base + used as u64)
            }
            k => Err(invalid(format!(
                "column {i}: kind {k} unknown to this binary"
            ))),
        }
    }
}

fn push_prefixed_field_entry(
    out: &mut Vec<(Vec<u8>, u64, u64)>,
    names: &[Vec<u8>],
    col_name: &[u8],
    payload: (u64, u64),
    spec: PrefixedEntrySpec<'_>,
) -> io::Result<()> {
    let field = col_name
        .strip_prefix(spec.prefix.as_bytes())
        .ok_or_else(|| {
            invalid(format!(
                "column {}: kind {} must be named {}<field>",
                spec.col_idx, spec.kind, spec.prefix
            ))
        })?;
    if !names.iter().any(|n| n == field) {
        return Err(invalid(format!(
            "column {}: {} for field {:?}, which the field table lacks",
            spec.col_idx,
            spec.label,
            String::from_utf8_lossy(field)
        )));
    }
    if out.iter().any(|(f, _, _)| f == field) {
        return Err(invalid(format!(
            "column {}: field {:?} has two {} entries",
            spec.col_idx,
            String::from_utf8_lossy(field),
            spec.label
        )));
    }
    out.push((field.to_vec(), payload.0, payload.1));
    Ok(())
}
