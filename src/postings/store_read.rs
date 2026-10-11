//! The heap store's v3-v7 decode paths: `read_v3_from`, `read_v5_from`, and
//! `read_v6v7_from`, each rebuilding a `Bm25Store` from already-validated
//! bytes, plus `from_single_field`, the shape every pre-v6 path produces.

use super::*;

use std::collections::HashMap;
use std::io::{self};

impl Bm25Store {
    /// Parse the v3 sections back into a heap store (used when a
    /// disk-resident shard receives more documents: the append path is
    /// bulk-load, build-in-memory-then-flush again).
    pub(crate) fn read_v3_from(r: &mut &[u8]) -> io::Result<Self> {
        let invalid = |msg: &str| io::Error::new(io::ErrorKind::InvalidData, msg.to_string());
        let total_length = read_u64(r)?;
        let texts_off = read_u64(r)?;
        let _lineages_off = read_u64(r)?;
        let _postings_off = read_u64(r)?;
        let _directory_off = read_u64(r)?;
        let n_slots = read_u32(r)? as usize;
        let mut doc_lengths = Vec::with_capacity(n_slots);
        for _ in 0..n_slots {
            doc_lengths.push(read_u32(r)?);
        }
        // texts (skip the on-disk index by tracking the cursor)
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
        let _ = texts_off;
        // text_index (on disk; skip — we just rebuilt texts in heap)
        let mut lineages = Vec::with_capacity(n_slots);
        for _ in 0..n_slots {
            let _offset = read_u64(r)?;
            let _len = read_u32(r)?;
            lineages.push(None);
        }
        for lineage in lineages.iter_mut() {
            let present = take(r, 1)?[0];
            if present != 0 {
                let parent_id = read_u64(r)?;
                let group_id = read_u64(r)?;
                let span_start = read_u32(r)?;
                let span_end = read_u32(r)?;
                *lineage = Some(DocLineage {
                    parent_id,
                    group_id,
                    span_start,
                    span_end,
                });
            }
        }
        let n_terms = read_u32(r)? as usize;
        let mut postings = std::collections::BTreeMap::new();
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
                });
            }
            postings.insert(term, plist);
        }
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
    pub(crate) fn read_v5_shaped_postings(
        all: &[u8],
        directory_off: u64,
        run_base: u64,
        positions_off: Option<u64>,
    ) -> io::Result<std::collections::BTreeMap<String, Vec<Posting>>> {
        let invalid = |msg: &str| io::Error::new(io::ErrorKind::InvalidData, msg.to_string());
        let at = |off: u64, len: u64| -> io::Result<&[u8]> {
            let i = (off as usize)
                .checked_sub(8)
                .ok_or_else(|| invalid("bad offset"))?;
            all.get(i..i + len as usize)
                .ok_or_else(|| invalid("offset out of range"))
        };
        let u32_at = |off: u64| -> io::Result<u32> {
            Ok(u32::from_le_bytes(at(off, 4)?.try_into().expect("4 bytes")))
        };
        let u64_at = |off: u64| -> io::Result<u64> {
            Ok(u64::from_le_bytes(at(off, 8)?.try_into().expect("8 bytes")))
        };
        let n_terms = u32_at(directory_off)? as usize;
        let blob_start = directory_off + 4 + 34 * n_terms as u64;
        // A kind-7 section: its base table starts past the term count,
        // its ordinals past the base table (see COLUMN_KIND_POSITIONS).
        let positions = match positions_off {
            Some(off) => {
                if u32_at(off)? as usize != n_terms {
                    return Err(invalid(
                        "positions section term count differs from directory",
                    ));
                }
                Some((off + 4, off + 4 + 4 * (n_terms as u64 + 1)))
            }
            None => None,
        };
        let mut postings = std::collections::BTreeMap::new();
        for i in 0..n_terms {
            let e = directory_off + 4 + 34 * i as u64;
            let doc_run_off = u64_at(e)? + run_base;
            let occ_run_off = u64_at(e + 16)? + run_base;
            let df = u32_at(e + 24)? as usize;
            let blob_off = u64::from(u32_at(e + 28)?);
            let term_len = u64::from(u16::from_le_bytes(
                at(e + 32, 2)?.try_into().expect("2 bytes"),
            ));
            let term = String::from_utf8(at(blob_start + blob_off, term_len)?.to_vec())
                .map_err(|_| invalid("invalid utf-8 in term"))?;
            let term_base = match positions {
                Some((bases, data)) => Some((u64::from(u32_at(bases + 4 * i as u64)?), data)),
                None => None,
            };
            let mut plist = Vec::with_capacity(df);
            for j in 0..df {
                let p = doc_run_off + 12 * j as u64;
                let doc_id = u32_at(p)?;
                let tf = u32_at(p + 4)?;
                let occ_start = u64::from(u32_at(p + 8)?);
                // The next entry's occ_start (or the trailing sentinel
                // for the last posting) bounds this occurrence slice.
                let occ_end = u64::from(if j + 1 < df {
                    u32_at(p + 12 + 8)?
                } else {
                    u32_at(doc_run_off + 12 * df as u64)?
                });
                let mut offsets = Vec::with_capacity((occ_end - occ_start) as usize);
                for o in occ_start..occ_end {
                    offsets.push((
                        u32_at(occ_run_off + 8 * o)?,
                        u32_at(occ_run_off + 8 * o + 4)?,
                    ));
                }
                let mut positions = Vec::new();
                if let Some((base, data)) = term_base {
                    positions.reserve((occ_end - occ_start) as usize);
                    for o in occ_start..occ_end {
                        positions.push(u32_at(data + 4 * (base + o))?);
                    }
                }
                plist.push(Posting {
                    doc_id,
                    tf,
                    offsets,
                    positions,
                });
            }
            postings.insert(term, plist);
        }
        Ok(postings)
    }

    /// Parse a v5 file back into a heap store (same caller contract as
    /// [`Self::read_v3_from`]: a disk-resident shard about to receive more
    /// documents). The skip run is not needed in heap form and is skipped.
    pub(crate) fn read_v5_from(r: &mut &[u8]) -> io::Result<Self> {
        let invalid = |msg: &str| io::Error::new(io::ErrorKind::InvalidData, msg.to_string());
        // `r` starts at file offset 8 (magic consumed); section offsets
        // are absolute file offsets.
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
        // text_index (on disk; skip — we just rebuilt texts in heap)
        take(r, 12 * n_slots)?;
        let mut lineages = vec![None; n_slots];
        for lineage in lineages.iter_mut() {
            let present = take(r, 1)?[0];
            if present != 0 {
                let parent_id = read_u64(r)?;
                let group_id = read_u64(r)?;
                let span_start = read_u32(r)?;
                let span_end = read_u32(r)?;
                *lineage = Some(DocLineage {
                    parent_id,
                    group_id,
                    span_start,
                    span_end,
                });
            }
        }
        // postings: locate each term's runs through the directory.
        let postings = Self::read_v5_shaped_postings(all, directory_off, 0, None)?;
        Ok(Self::from_single_field(
            postings,
            doc_lengths,
            total_length,
            texts,
            lineages,
        ))
    }

    /// Parse a v6 file back into a heap store (same caller contract as
    /// [`Self::read_v3_from`]). All fields are decoded; shared sections
    /// once, then one postings map per field.
    pub(crate) fn read_v6v7_from(r: &mut &[u8], v7: bool) -> io::Result<Self> {
        let invalid = |msg: &str| io::Error::new(io::ErrorKind::InvalidData, msg.to_string());
        // `r` starts at file offset 8 (magic consumed); header offsets
        // are absolute file offsets.
        let all: &[u8] = r;
        let at = |off: u64, len: u64| -> io::Result<&[u8]> {
            let i = (off as usize)
                .checked_sub(8)
                .ok_or_else(|| invalid("bad offset"))?;
            all.get(i..i + len as usize)
                .ok_or_else(|| invalid("offset out of range"))
        };
        let u32_at = |off: u64| -> io::Result<u32> {
            Ok(u32::from_le_bytes(at(off, 4)?.try_into().expect("4 bytes")))
        };
        let u64_at = |off: u64| -> io::Result<u64> {
            Ok(u64::from_le_bytes(at(off, 8)?.try_into().expect("8 bytes")))
        };
        let n_fields = u32_at(8)? as usize;
        if n_fields == 0 {
            return Err(invalid("v6 file with zero fields"));
        }
        let n_slots = u32_at(12)? as usize;
        let texts_off = u64_at(16)?;
        let lineages_off = u64_at(32)?;
        // Field table.
        let mut metas: Vec<(String, u64, u64, u64, u64, u64)> = Vec::with_capacity(n_fields);
        let mut cursor = 40u64;
        for _ in 0..n_fields {
            let name_len = u64::from(u16_at(at(cursor, 2)?));
            let name = String::from_utf8(at(cursor + 2, name_len)?.to_vec())
                .map_err(|_| invalid("invalid utf-8 in field name"))?;
            let base = cursor + 2 + name_len;
            metas.push((
                name,
                u64_at(base)?,      // analysis_fingerprint
                u64_at(base + 8)?,  // total_length
                u64_at(base + 16)?, // doc_lengths_off
                u64_at(base + 24)?, // postings_off
                u64_at(base + 32)?, // directory_off
            ));
            cursor = base + 40;
        }
        // Column table (v7 only): kinded entries. min/max metadata is
        // skipped here — the heap store recomputes it at the next
        // write. Unknown kinds refuse by number.
        let mut facet_metas: Vec<(String, u32, u64, u64)> = Vec::new();
        let mut numeric_metas: Vec<(String, u64)> = Vec::new();
        // (name, n_keys, n_values, keys, values, offsets, pairs)
        let mut map_facet_metas: Vec<(String, u32, u32, u64, u64, u64, u64)> = Vec::new();
        // (name, n_keys, keys, offsets, pairs)
        let mut map_numeric_metas: Vec<(String, u32, u64, u64, u64)> = Vec::new();
        let mut integer_map_metas = Vec::new();
        let mut integer_metas: Vec<(String, u64, bool)> = Vec::new();
        let mut unsigned_metas: Vec<(String, u64)> = Vec::new();
        let mut geo_metas: Vec<(String, u64)> = Vec::new();
        // (field name, section_off) per kind-7 entry.
        let mut positions_metas: Vec<(String, u64)> = Vec::new();
        let mut sentences_metas: Vec<(String, u64)> = Vec::new();
        let mut binding_meta: Option<StoredBinding> = None;
        let mut derived_meta: Option<StoredDerived> = None;
        let mut sources_meta = None;
        if v7 {
            let n_columns = u32_at(cursor)? as usize;
            cursor += 4;
            for _ in 0..n_columns {
                let name_len = u64::from(u16_at(at(cursor, 2)?));
                let name = String::from_utf8(at(cursor + 2, name_len)?.to_vec())
                    .map_err(|_| invalid("invalid utf-8 in column name"))?;
                let kind = at(cursor + 2 + name_len, 1)?[0];
                let base = cursor + 2 + name_len + 1;
                let kind = compatible_column_kind(
                    kind,
                    name.as_bytes(),
                    &all[base as usize - 8..],
                    (all.len() + 8) as u64,
                );
                match kind {
                    COLUMN_KIND_FACET => {
                        facet_metas.push((
                            name,
                            u32_at(base)?,      // n_values
                            u64_at(base + 4)?,  // dict_off
                            u64_at(base + 12)?, // ords_off
                        ));
                        cursor = base + 20;
                    }
                    COLUMN_KIND_F64 => {
                        numeric_metas.push((name, u64_at(base + 16)?));
                        cursor = base + 24;
                    }
                    COLUMN_KIND_MAP_FACET => {
                        map_facet_metas.push((
                            name,
                            u32_at(base)?,      // n_keys
                            u32_at(base + 4)?,  // n_values
                            u64_at(base + 8)?,  // keys_off
                            u64_at(base + 16)?, // values_off
                            u64_at(base + 24)?, // offsets_off
                            u64_at(base + 32)?, // pairs_off
                        ));
                        cursor = base + 40;
                    }
                    COLUMN_KIND_MAP_F64 => {
                        map_numeric_metas.push((
                            name,
                            u32_at(base)?,      // n_keys
                            u64_at(base + 4)?,  // keys_off
                            u64_at(base + 12)?, // offsets_off
                            u64_at(base + 20)?, // pairs_off
                        ));
                        cursor = base + 28;
                    }
                    COLUMN_KIND_MAP_I64 | COLUMN_KIND_MAP_U64 => {
                        integer_map_metas.push((kind, name, u64_at(base)?, u64_at(base + 8)?));
                        cursor = base + 16;
                    }
                    COLUMN_KIND_I64 | COLUMN_KIND_I64_PRESENT => {
                        integer_metas.push((
                            name,
                            u64_at(base + 16)?,
                            kind == COLUMN_KIND_I64_PRESENT,
                        ));
                        cursor = base + 24;
                    }
                    COLUMN_KIND_U64 => {
                        unsigned_metas.push((name, u64_at(base + 16)?));
                        cursor = base + 24;
                    }
                    COLUMN_KIND_GEO => {
                        geo_metas.push((name, u64_at(base + 32)?));
                        cursor = base + 40;
                    }
                    COLUMN_KIND_POSITIONS => {
                        let field = name
                            .strip_prefix(POSITIONS_ENTRY_PREFIX)
                            .ok_or_else(|| {
                                invalid("kind-7 entry name lacks the positions: prefix")
                            })?
                            .to_string();
                        positions_metas.push((field, u64_at(base)?));
                        cursor = base + 16;
                    }
                    COLUMN_KIND_SENTENCES => {
                        let field = name
                            .strip_prefix(SENTENCES_ENTRY_PREFIX)
                            .ok_or_else(|| {
                                invalid("kind-8 entry name lacks the sentences: prefix")
                            })?
                            .to_string();
                        sentences_metas.push((field, u64_at(base)?));
                        cursor = base + 16;
                    }
                    COLUMN_KIND_SOURCES => {
                        sources_meta = Some((u64_at(base)?, u64_at(base + 8)?));
                        cursor = base + 16;
                    }
                    COLUMN_KIND_BINDING
                    | COLUMN_KIND_ANALYSIS_BINDING
                    | COLUMN_KIND_VECTOR_BINDING
                    | COLUMN_KIND_INDEX_BINDING => {
                        let mut vals: Vec<String> = Vec::with_capacity(3);
                        let mut cur = base;
                        for _ in 0..if kind != COLUMN_KIND_BINDING { 4 } else { 3 } {
                            let len = u64::from(u16_at(at(cur, 2)?));
                            vals.push(
                                String::from_utf8(at(cur + 2, len)?.to_vec())
                                    .map_err(|_| invalid("invalid utf-8 in binding record"))?,
                            );
                            cur += 2 + len;
                        }
                        if kind != COLUMN_KIND_BINDING
                            && (kind == COLUMN_KIND_ANALYSIS_BINDING || !vals[3].is_empty())
                            && !crate::mapped_analysis::valid_digest(&vals[3])
                        {
                            return Err(invalid("invalid mapped analysis digest"));
                        }
                        let analysis_contract = if kind != COLUMN_KIND_BINDING {
                            let len = u32::from_le_bytes(at(cur, 4)?.try_into().unwrap()) as u64;
                            let bytes = at(cur + 4, len)?.to_vec();
                            cur += 4 + len;
                            bytes
                        } else {
                            Vec::new()
                        };
                        let vector_binding = if matches!(
                            kind,
                            COLUMN_KIND_VECTOR_BINDING | COLUMN_KIND_INDEX_BINDING
                        ) {
                            let len = u32::from_le_bytes(at(cur, 4)?.try_into().unwrap()) as u64;
                            let bytes = at(cur + 4, len)?.to_vec();
                            cur += 4 + len;
                            if crate::mapped_vector::decode(&bytes, &vals[0])
                                .map_err(|e| invalid(&e.to_string()))?
                                .is_none()
                            {
                                return Err(invalid("empty vector binding in vector/index entry"));
                            }
                            bytes
                        } else {
                            Vec::new()
                        };
                        let index_contract = if kind == COLUMN_KIND_INDEX_BINDING {
                            let len = u32::from_le_bytes(at(cur, 4)?.try_into().unwrap()) as u64;
                            let bytes = at(cur + 4, len)?.to_vec();
                            cur += 4 + len;
                            if crate::index_contract::validate_binding(
                                &bytes,
                                &vals[0],
                                &vector_binding,
                            )
                            .map_err(|e| invalid(&e.to_string()))?
                            .is_none()
                            {
                                return Err(invalid("empty index contract in kind-14 entry"));
                            }
                            bytes
                        } else {
                            Vec::new()
                        };
                        let mut it = vals.into_iter();
                        binding_meta = Some(StoredBinding {
                            plan_fingerprint: it.next().expect("three strings"),
                            body_path: it.next().expect("three strings"),
                            materialize_sha: it.next().expect("three strings"),
                            analysis_sha: it.next().unwrap_or_default(),
                            analysis_contract,
                            vector_binding,
                            index_contract,
                        });
                        let bound = binding_meta.as_ref().unwrap();
                        crate::mapped_analysis::decode_contract(
                            &bound.analysis_sha,
                            &bound.analysis_contract,
                            &bound.body_path,
                        )
                        .map_err(|e| invalid(&e))?;
                        cursor = cur;
                    }
                    COLUMN_KIND_DERIVED => {
                        let mut cur = base;
                        let len = u64::from(u16_at(at(cur, 2)?));
                        let fingerprint = String::from_utf8(at(cur + 2, len)?.to_vec())
                            .map_err(|_| invalid("invalid utf-8 in derived-columns record"))?;
                        cur += 2 + len;
                        let len = u32::from_le_bytes(at(cur, 4)?.try_into().unwrap()) as u64;
                        let declaration = at(cur + 4, len)?.to_vec();
                        cur += 4 + len;
                        let stored = StoredDerived {
                            fingerprint,
                            declaration,
                        };
                        stored.validate().map_err(|e| invalid(&e))?;
                        derived_meta = Some(stored);
                        cursor = cur;
                    }
                    k => {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("column kind {k} unknown to this binary"),
                        ))
                    }
                }
            }
        }
        // Shared sections.
        let mut texts = Vec::with_capacity(n_slots);
        let mut tcur = texts_off;
        for _ in 0..n_slots {
            let len = u32_at(tcur)?;
            if len == u32::MAX {
                texts.push(None);
                tcur += 4;
            } else {
                let bytes = at(tcur + 4, u64::from(len))?;
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
            if at(lcur, 1)?[0] == 0 {
                lineages.push(None);
                lcur += 1;
            } else {
                lineages.push(Some(DocLineage {
                    parent_id: u64_at(lcur + 1)?,
                    group_id: u64_at(lcur + 9)?,
                    span_start: u32_at(lcur + 17)?,
                    span_end: u32_at(lcur + 21)?,
                }));
                lcur += 25;
            }
        }
        // Per-field sections.
        let mut fields = Vec::with_capacity(n_fields);
        for (name, fingerprint, total_length, dl_off, p_off, d_off) in metas {
            let mut doc_lengths = Vec::with_capacity(n_slots);
            for slot in 0..n_slots as u64 {
                doc_lengths.push(u32_at(dl_off + 4 * slot)?);
            }
            // Validation already tied every kind-7 entry to a field in
            // the table; an entry naming no field cannot reach here.
            let positions_off = positions_metas
                .iter()
                .find(|(field, _)| *field == name)
                .map(|(_, off)| *off);
            let postings = Self::read_v5_shaped_postings(all, d_off, p_off, positions_off)?;
            // A kind-8 section (validated above) comes back into heap as
            // one table per slot.
            let sentences_off = sentences_metas
                .iter()
                .find(|(field, _)| *field == name)
                .map(|(_, off)| *off);
            let mut doc_sentences: Vec<Vec<(u32, u32)>> = Vec::new();
            if let Some(off) = sentences_off {
                let slots = u64::from(u32_at(off)?);
                let data = off + 4 + 4 * (slots + 1);
                doc_sentences.reserve(slots as usize);
                for slot in 0..slots {
                    let from = u64::from(u32_at(off + 4 + 4 * slot)?);
                    let to = u64::from(u32_at(off + 4 + 4 * (slot + 1))?);
                    doc_sentences.push(
                        (from..to)
                            .map(|k| Ok((u32_at(data + 8 * k)?, u32_at(data + 8 * k + 4)?)))
                            .collect::<io::Result<Vec<_>>>()?,
                    );
                }
            }
            fields.push(FieldStore {
                name,
                analysis_fingerprint: fingerprint,
                postings,
                doc_lengths,
                total_length,
                positions: positions_off.is_some(),
                sentences: sentences_off.is_some(),
                doc_sentences,
            });
        }
        // Facet columns (v7 only): dict then per-slot ordinals.
        let mut facets = Vec::with_capacity(facet_metas.len());
        for (name, n_values, dict_off, ords_off) in facet_metas {
            let mut dict = Vec::with_capacity(n_values as usize);
            let mut index = HashMap::with_capacity(n_values as usize);
            let mut dcur = dict_off;
            for ord in 0..n_values {
                let len = u64::from(u16_at(at(dcur, 2)?));
                let value = String::from_utf8(at(dcur + 2, len)?.to_vec())
                    .map_err(|_| invalid("invalid utf-8 in facet value"))?;
                index.insert(value.clone(), ord);
                dict.push(value);
                dcur = dcur + 2 + len;
            }
            let mut ords = Vec::with_capacity(n_slots);
            for slot in 0..n_slots as u64 {
                let ord = u32_at(ords_off + 4 * slot)?;
                if ord != FACET_ABSENT && ord >= n_values {
                    return Err(invalid("facet ordinal out of dictionary range"));
                }
                ords.push(ord);
            }
            facets.push(FacetStore {
                name,
                dict,
                index,
                ords,
            });
        }
        // Numeric columns (v7 only): n_slots x f64 bits, NaN = absent.
        let mut numerics = Vec::with_capacity(numeric_metas.len());
        for (name, vals_off) in numeric_metas {
            let mut vals = Vec::with_capacity(n_slots);
            for slot in 0..n_slots as u64 {
                vals.push(f64::from_bits(u64_at(vals_off + 8 * slot)?));
            }
            numerics.push(NumericStore { name, vals });
        }
        // Map columns (v7 only): decode dictionaries, then split the
        // pairs section back into per-slot lists via the offsets
        // section's prefix sums.
        let read_dict = |off: u64, n: u32| -> io::Result<(Vec<String>, HashMap<String, u32>)> {
            let mut dict = Vec::with_capacity(n as usize);
            let mut index = HashMap::with_capacity(n as usize);
            let mut cur = off;
            for ord in 0..n {
                let len = u64::from(u16_at(at(cur, 2)?));
                let entry = String::from_utf8(at(cur + 2, len)?.to_vec())
                    .map_err(|_| invalid("invalid utf-8 in map dictionary"))?;
                index.insert(entry.clone(), ord);
                dict.push(entry);
                cur += 2 + len;
            }
            Ok((dict, index))
        };
        let pair_range = |offsets_off: u64, slot: usize| -> io::Result<(u64, u64)> {
            Ok((
                u64::from(u32_at(offsets_off + 4 * slot as u64)?),
                u64::from(u32_at(offsets_off + 4 * (slot as u64 + 1))?),
            ))
        };
        let mut map_facets = Vec::with_capacity(map_facet_metas.len());
        for (name, n_keys, n_values, keys_off, values_off, offsets_off, pairs_off) in
            map_facet_metas
        {
            let (keys, key_index) = read_dict(keys_off, n_keys)?;
            let (values, value_index) = read_dict(values_off, n_values)?;
            let mut pairs = Vec::with_capacity(n_slots);
            for slot in 0..n_slots {
                let (start, end) = pair_range(offsets_off, slot)?;
                let mut list = Vec::with_capacity((end - start) as usize);
                for p in start..end {
                    list.push((u32_at(pairs_off + 8 * p)?, u32_at(pairs_off + 8 * p + 4)?));
                }
                pairs.push(list);
            }
            map_facets.push(MapFacetStore {
                name,
                keys,
                key_index,
                values,
                value_index,
                pairs,
            });
        }
        let mut map_numerics = Vec::with_capacity(map_numeric_metas.len());
        for (name, n_keys, keys_off, offsets_off, pairs_off) in map_numeric_metas {
            // Key entries interleave min/max metadata (recomputed at
            // the next write), so read_dict does not apply.
            let mut keys = Vec::with_capacity(n_keys as usize);
            let mut key_index = HashMap::with_capacity(n_keys as usize);
            let mut cur = keys_off;
            for ord in 0..n_keys {
                let len = u64::from(u16_at(at(cur, 2)?));
                let entry = String::from_utf8(at(cur + 2, len)?.to_vec())
                    .map_err(|_| invalid("invalid utf-8 in map dictionary"))?;
                key_index.insert(entry.clone(), ord);
                keys.push(entry);
                cur += 2 + len + 16;
            }
            let mut pairs = Vec::with_capacity(n_slots);
            for slot in 0..n_slots {
                let (start, end) = pair_range(offsets_off, slot)?;
                let mut list = Vec::with_capacity((end - start) as usize);
                for p in start..end {
                    list.push((
                        u32_at(pairs_off + 12 * p)?,
                        f64::from_bits(u64_at(pairs_off + 12 * p + 4)?),
                    ));
                }
                pairs.push(list);
            }
            map_numerics.push(MapNumericStore {
                name,
                keys,
                key_index,
                pairs,
            });
        }
        let mut map_integers = Vec::new();
        let mut map_unsigned_integers = Vec::new();
        for (kind, name, off, len) in integer_map_metas {
            if kind == COLUMN_KIND_MAP_I64 {
                map_integers.push(crate::integer_map::Store::load(
                    &name,
                    at(off, len)?,
                    n_slots as u32,
                )?);
            } else {
                map_unsigned_integers.push(crate::integer_map::Store::load(
                    &name,
                    at(off, len)?,
                    n_slots as u32,
                )?);
            }
        }
        // Translate legacy sentinel columns into explicit in-memory presence.
        let mut integers = Vec::with_capacity(integer_metas.len());
        for (name, vals_off, has_presence) in integer_metas {
            let mut column = IntStore::new(&name);
            let bitmap = if has_presence {
                Some(at(
                    vals_off + 8 * n_slots as u64,
                    (n_slots as u64).div_ceil(8),
                )?)
            } else {
                None
            };
            for slot in 0..n_slots {
                let value = u64_at(vals_off + 8 * slot as u64)? as i64;
                if bitmap.map_or(value != INTEGER_ABSENT, |bits| integer_present(bits, slot)) {
                    column.set(slot as u32, value);
                }
            }
            integers.push(column);
        }
        let mut unsigned_integers = Vec::with_capacity(unsigned_metas.len());
        for (name, vals_off) in unsigned_metas {
            let mut column = UintStore::new(&name);
            let bitmap = at(vals_off + 8 * n_slots as u64, (n_slots as u64).div_ceil(8))?;
            for slot in 0..n_slots {
                if integer_present(bitmap, slot) {
                    column.set(slot as u32, u64_at(vals_off + 8 * slot as u64)?);
                }
            }
            unsigned_integers.push(column);
        }
        // Geo columns (v7 only): n_slots x (f64 lat, f64 lon) at a
        // 16 B stride, (NaN, NaN) = absent. Validation already refused a
        // half-NaN pair, so the loader takes the bytes as they are.
        let mut geos = Vec::with_capacity(geo_metas.len());
        for (name, vals_off) in geo_metas {
            let mut vals = Vec::with_capacity(n_slots);
            for slot in 0..n_slots as u64 {
                vals.push((
                    f64::from_bits(u64_at(vals_off + 16 * slot)?),
                    f64::from_bits(u64_at(vals_off + 16 * slot + 8)?),
                ));
            }
            geos.push(GeoStore { name, vals });
        }
        Ok(Self {
            fields,
            texts,
            lineages,
            facets,
            numerics,
            map_facets,
            map_numerics,
            map_integers,
            map_unsigned_integers,
            integers,
            unsigned_integers,
            geos,
            binding: binding_meta,
            derived: derived_meta,
            sources: match sources_meta {
                Some((offset, length)) => {
                    crate::source_archive::SourceArchive::read(at(offset, length)?, n_slots as u32)?
                }
                None => Default::default(),
            },
        })
    }
}
