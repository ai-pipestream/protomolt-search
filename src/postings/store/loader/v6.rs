//! v6/v7 header, shared-section, and per-field deserialization back into `Bm25Store`.

use std::io;

use super::super::super::columns::{
    decode_binding_entry, decode_derived_entry, StoredBinding, StoredDerived,
};
use super::super::super::format::{
    COLUMN_KIND_ANALYSIS_BINDING, COLUMN_KIND_BINDING, COLUMN_KIND_DERIVED, COLUMN_KIND_F64,
    COLUMN_KIND_FACET, COLUMN_KIND_GEO, COLUMN_KIND_I64, COLUMN_KIND_I64_PRESENT,
    COLUMN_KIND_INDEX_BINDING, COLUMN_KIND_MAP_F64, COLUMN_KIND_MAP_FACET, COLUMN_KIND_MAP_I64,
    COLUMN_KIND_MAP_U64, COLUMN_KIND_POSITIONS, COLUMN_KIND_POS_LENGTHS, COLUMN_KIND_SENTENCES,
    COLUMN_KIND_SOURCES, COLUMN_KIND_SPLADE, COLUMN_KIND_U64, COLUMN_KIND_VECTOR_BINDING,
    POSITIONS_ENTRY_PREFIX, POS_LENGTHS_ENTRY_PREFIX, SENTENCES_ENTRY_PREFIX, SPLADE_ENTRY_PREFIX,
};
use super::super::super::DocLineage;
use super::super::{Bm25Store, FieldStore};
use super::{invalid, PayloadView};

type FieldHeaderMeta = (String, u64, u64, u64, u64, u64);
type SharedSections = (Vec<Option<String>>, Vec<Option<DocLineage>>);

#[derive(Default)]
pub(super) struct V6ColumnMetas {
    pub(super) facets: Vec<(String, u32, u64, u64)>,
    pub(super) numerics: Vec<(String, u64)>,
    pub(super) map_facets: Vec<(String, u32, u32, u64, u64, u64, u64)>,
    pub(super) map_numerics: Vec<(String, u32, u64, u64, u64)>,
    pub(super) integer_maps: Vec<(u8, String, u64, u64)>,
    pub(super) integers: Vec<(String, u64, bool)>,
    pub(super) unsigned_integers: Vec<(String, u64)>,
    pub(super) geos: Vec<(String, u64)>,
    positions: Vec<(String, u64)>,
    sentences: Vec<(String, u64)>,
    pos_lengths: Vec<(String, u64)>,
    splades: Vec<(String, u64, u64)>,
    pub(super) binding: Option<StoredBinding>,
    pub(super) derived: Option<StoredDerived>,
    sources: Option<(u64, u64)>,
}

impl Bm25Store {
    /// Parse a v6/v7 file back into a heap store.
    pub(super) fn read_v6v7_from(r: &mut &[u8], v7: bool) -> io::Result<Self> {
        let view = PayloadView { all: r };
        let n_fields = view.u32_at(8)? as usize;
        if n_fields == 0 {
            return Err(invalid("v6 file with zero fields"));
        }
        let n_slots = view.u32_at(12)? as usize;
        let texts_off = view.u64_at(16)?;
        let lineages_off = view.u64_at(32)?;
        let mut cursor = 40u64;
        let field_metas = parse_field_metas(&view, n_fields, &mut cursor)?;
        let mut col_metas = V6ColumnMetas::default();
        if v7 {
            let n_columns = view.u32_at(cursor)? as usize;
            cursor += 4;
            for _ in 0..n_columns {
                col_metas.parse_entry(&view, &mut cursor)?;
            }
        }
        let (texts, lineages) = read_shared_sections(&view, n_slots, texts_off, lineages_off)?;
        let fields = Self::read_v6_fields(&view, n_slots, field_metas, &col_metas)?;
        let sources_meta = col_metas.sources;
        let columns = super::v6_columns::decode_v6_column_set(col_metas, &view, n_slots)?;
        let sources = match sources_meta {
            Some((offset, length)) => crate::source_archive::SourceArchive::read(
                view.at(offset, length)?,
                n_slots as u32,
            )?,
            None => Default::default(),
        };
        Ok(Self {
            fields,
            texts,
            lineages,
            columns,
            sources,
        })
    }

    fn read_v6_fields(
        view: &PayloadView<'_>,
        n_slots: usize,
        metas: Vec<FieldHeaderMeta>,
        col_metas: &V6ColumnMetas,
    ) -> io::Result<Vec<FieldStore>> {
        let mut fields = Vec::with_capacity(metas.len());
        for (name, fingerprint, total_length, dl_off, p_off, d_off) in metas {
            let mut doc_lengths = Vec::with_capacity(n_slots);
            for slot in 0..n_slots as u64 {
                doc_lengths.push(view.u32_at(dl_off + 4 * slot)?);
            }
            let positions_off = lookup_field_off(&col_metas.positions, &name);
            let pos_lengths_off = lookup_field_off(&col_metas.pos_lengths, &name);
            let postings = Self::read_block_max_postings(
                view.all,
                d_off,
                p_off,
                positions_off,
                pos_lengths_off,
            )?;
            let splade = match col_metas.splades.iter().find(|(f, _, _)| *f == name) {
                Some((_, off, len)) => Some(crate::turbo_bridge::splade::SpladeFieldIndex::decode(
                    view.at(*off, *len)?,
                )?),
                None => None,
            };
            let sentences_off = lookup_field_off(&col_metas.sentences, &name);
            let doc_sentences = match sentences_off {
                Some(off) => read_field_sentences(view, off)?,
                None => Vec::new(),
            };
            fields.push(FieldStore {
                name,
                analysis_fingerprint: fingerprint,
                postings,
                doc_lengths,
                total_length,
                positions: positions_off.is_some(),
                has_lattice: pos_lengths_off.is_some(),
                splade,
                sentences: sentences_off.is_some(),
                doc_sentences,
            });
        }
        Ok(fields)
    }
}

fn lookup_field_off(entries: &[(String, u64)], name: &str) -> Option<u64> {
    entries.iter().find(|(f, _)| f == name).map(|(_, off)| *off)
}

fn parse_field_metas(
    view: &PayloadView<'_>,
    n_fields: usize,
    cursor: &mut u64,
) -> io::Result<Vec<FieldHeaderMeta>> {
    let mut metas = Vec::with_capacity(n_fields);
    for _ in 0..n_fields {
        let name_len = u64::from(view.u16_at(*cursor)?);
        let name = String::from_utf8(view.at(*cursor + 2, name_len)?.to_vec())
            .map_err(|_| invalid("invalid utf-8 in field name"))?;
        let base = *cursor + 2 + name_len;
        metas.push((
            name,
            view.u64_at(base)?,
            view.u64_at(base + 8)?,
            view.u64_at(base + 16)?,
            view.u64_at(base + 24)?,
            view.u64_at(base + 32)?,
        ));
        *cursor = base + 40;
    }
    Ok(metas)
}

fn read_shared_sections(
    view: &PayloadView<'_>,
    n_slots: usize,
    texts_off: u64,
    lineages_off: u64,
) -> io::Result<SharedSections> {
    let mut texts = Vec::with_capacity(n_slots);
    let mut tcur = texts_off;
    for _ in 0..n_slots {
        let len = view.u32_at(tcur)?;
        if len == u32::MAX {
            texts.push(None);
            tcur += 4;
        } else {
            let bytes = view.at(tcur + 4, u64::from(len))?;
            texts.push(Some(
                String::from_utf8(bytes.to_vec())
                    .map_err(|_| invalid("invalid utf-8 in doc text"))?,
            ));
            tcur += 4 + u64::from(len);
        }
    }
    let mut lineages = Vec::with_capacity(n_slots);
    let mut lcur = lineages_off;
    for _ in 0..n_slots {
        if view.at(lcur, 1)?[0] == 0 {
            lineages.push(None);
            lcur += 1;
        } else {
            lineages.push(Some(DocLineage {
                parent_id: view.u64_at(lcur + 1)?,
                group_id: view.u64_at(lcur + 9)?,
                span_start: view.u32_at(lcur + 17)?,
                span_end: view.u32_at(lcur + 21)?,
            }));
            lcur += 25;
        }
    }
    Ok((texts, lineages))
}

fn read_field_sentences(view: &PayloadView<'_>, off: u64) -> io::Result<Vec<Vec<(u32, u32)>>> {
    let slots = u64::from(view.u32_at(off)?);
    let data = off + 4 + 4 * (slots + 1);
    let mut doc_sentences = Vec::with_capacity(slots as usize);
    for slot in 0..slots {
        let from = u64::from(view.u32_at(off + 4 + 4 * slot)?);
        let to = u64::from(view.u32_at(off + 4 + 4 * (slot + 1))?);
        doc_sentences.push(
            (from..to)
                .map(|k| Ok((view.u32_at(data + 8 * k)?, view.u32_at(data + 8 * k + 4)?)))
                .collect::<io::Result<Vec<_>>>()?,
        );
    }
    Ok(doc_sentences)
}

impl V6ColumnMetas {
    fn parse_entry(&mut self, view: &PayloadView<'_>, cursor: &mut u64) -> io::Result<()> {
        let name_len = u64::from(view.u16_at(*cursor)?);
        let name = String::from_utf8(view.at(*cursor + 2, name_len)?.to_vec())
            .map_err(|_| invalid("invalid utf-8 in column name"))?;
        let kind = view.at(*cursor + 2 + name_len, 1)?[0];
        let base = *cursor + 2 + name_len + 1;
        if let Some(next) = self.parse_value_entry(view, &name, kind, base)? {
            *cursor = next;
            return Ok(());
        }
        *cursor = self.parse_meta_entry(view, &name, kind, base)?;
        Ok(())
    }

    fn parse_value_entry(
        &mut self,
        view: &PayloadView<'_>,
        name: &str,
        kind: u8,
        base: u64,
    ) -> io::Result<Option<u64>> {
        let name = name.to_string();
        let next = match kind {
            COLUMN_KIND_FACET => {
                self.facets.push((
                    name,
                    view.u32_at(base)?,
                    view.u64_at(base + 4)?,
                    view.u64_at(base + 12)?,
                ));
                base + 20
            }
            COLUMN_KIND_F64 => {
                self.numerics.push((name, view.u64_at(base + 16)?));
                base + 24
            }
            COLUMN_KIND_MAP_FACET => {
                self.map_facets.push((
                    name,
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
                    name,
                    view.u32_at(base)?,
                    view.u64_at(base + 4)?,
                    view.u64_at(base + 12)?,
                    view.u64_at(base + 20)?,
                ));
                base + 28
            }
            COLUMN_KIND_MAP_I64 | COLUMN_KIND_MAP_U64 => {
                self.integer_maps
                    .push((kind, name, view.u64_at(base)?, view.u64_at(base + 8)?));
                base + 16
            }
            COLUMN_KIND_I64 | COLUMN_KIND_I64_PRESENT => {
                self.integers.push((
                    name,
                    view.u64_at(base + 16)?,
                    kind == COLUMN_KIND_I64_PRESENT,
                ));
                base + 24
            }
            COLUMN_KIND_U64 => {
                self.unsigned_integers.push((name, view.u64_at(base + 16)?));
                base + 24
            }
            COLUMN_KIND_GEO => {
                self.geos.push((name, view.u64_at(base + 32)?));
                base + 40
            }
            _ => return Ok(None),
        };
        Ok(Some(next))
    }

    fn parse_meta_entry(
        &mut self,
        view: &PayloadView<'_>,
        name: &str,
        kind: u8,
        base: u64,
    ) -> io::Result<u64> {
        match kind {
            COLUMN_KIND_POSITIONS => {
                let field = strip_field_prefix(
                    name,
                    POSITIONS_ENTRY_PREFIX,
                    "kind-7 entry name lacks the positions: prefix",
                )?;
                self.positions.push((field, view.u64_at(base)?));
                Ok(base + 16)
            }
            COLUMN_KIND_SENTENCES => {
                let field = strip_field_prefix(
                    name,
                    SENTENCES_ENTRY_PREFIX,
                    "kind-8 entry name lacks the sentences: prefix",
                )?;
                self.sentences.push((field, view.u64_at(base)?));
                Ok(base + 16)
            }
            COLUMN_KIND_SOURCES => {
                self.sources = Some((view.u64_at(base)?, view.u64_at(base + 8)?));
                Ok(base + 16)
            }
            COLUMN_KIND_POS_LENGTHS => {
                let field = strip_field_prefix(
                    name,
                    POS_LENGTHS_ENTRY_PREFIX,
                    "kind-18 entry name lacks the pos_lengths: prefix",
                )?;
                self.pos_lengths.push((field, view.u64_at(base)?));
                Ok(base + 16)
            }
            COLUMN_KIND_SPLADE => {
                let field = strip_field_prefix(
                    name,
                    SPLADE_ENTRY_PREFIX,
                    "kind-19 entry name lacks the splade: prefix",
                )?;
                self.splades
                    .push((field, view.u64_at(base)?, view.u64_at(base + 8)?));
                Ok(base + 16)
            }
            COLUMN_KIND_BINDING
            | COLUMN_KIND_ANALYSIS_BINDING
            | COLUMN_KIND_VECTOR_BINDING
            | COLUMN_KIND_INDEX_BINDING => {
                let (bound, consumed) = decode_binding_entry(kind, &view.all[base as usize - 8..])?;
                self.binding = Some(bound);
                Ok(base + consumed as u64)
            }
            COLUMN_KIND_DERIVED => {
                let (stored, consumed) = decode_derived_entry(&view.all[base as usize - 8..])?;
                self.derived = Some(stored);
                Ok(base + consumed as u64)
            }
            k => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("column kind {k} unknown to this binary"),
            )),
        }
    }
}

fn strip_field_prefix(name: &str, prefix: &str, err_msg: &str) -> io::Result<String> {
    name.strip_prefix(prefix)
        .map(str::to_string)
        .ok_or_else(|| invalid(err_msg))
}
