//! Serialization of `Bm25Store` into `.bm25` v4, v5, v6/v7, and v8 formats.

use std::io::{self, Write};
use std::path::{Path, PathBuf};

use super::super::codec::{push_posting_v5, skip_run_size, write_skip_run, SkipRunBuilder};
#[cfg(doc)]
use super::super::columns::FACET_ABSENT;
use super::super::format::{
    fsync_parent, write_str, write_u16, write_u32, write_u64, MAGIC_V4, MAGIC_V5,
};
use super::super::section_writer::{
    save_v8_atomic, write_lineages, write_text_index_from_lens, write_v5_directory,
    write_v6v7_payload, FieldSectionDesc, V6PayloadParams, V6SectionSource,
};
use super::{Bm25Store, FieldStore};

impl Bm25Store {
    /// Persist to `path` (atomically: write tmp, fsync, rename, fsync
    /// parent). Writes v8: the payload is [`Self::write_v6_to`]'s v6
    /// bytes when no columns are declared and v7 bytes when they are,
    /// followed by the v8 CRC integrity table and trailer.
    pub fn save(&self, path: &Path) -> io::Result<()> {
        save_v8_atomic(path, |w| self.write_v6_to(w))
    }

    /// Persist with first-seen dictionaries under the ordinary v8
    /// envelope: the exact file a pre-2026-09 build wrote. Legacy oracle
    /// for the tests only (see [`Self::write_v6_first_seen_dictionaries`]).
    pub fn save_first_seen_dictionaries(&self, path: &Path) -> io::Result<()> {
        save_v8_atomic(path, |w| self.write_v6_first_seen_dictionaries(w))
    }

    /// Persist in the v5 format. Correctness oracle only — the
    /// v5-vs-v6 section-parity and query-identity tests; production
    /// saves are v6 and multi-field stores are refused here.
    pub fn save_v5(&self, path: &Path) -> io::Result<()> {
        let tmp: PathBuf = path.with_extension("bm25tmp");
        {
            let mut w = io::BufWriter::new(std::fs::File::create(&tmp)?);
            self.write_to(&mut w)?;
            w.flush()?;
            w.get_ref().sync_all()?;
        }
        std::fs::rename(&tmp, path)?;
        fsync_parent(path)
    }

    /// Sizes of the shared sections (`texts`, `text_index`, `lineages`).
    fn shared_section_sizes(&self) -> (u64, u64, u64) {
        let texts_size: u64 = self
            .texts
            .iter()
            .map(|t| 4 + t.as_ref().map_or(0, |s| s.len() as u64))
            .sum();
        let lineages_size: u64 = self
            .lineages
            .iter()
            .map(|l| if l.is_some() { 25 } else { 1 })
            .sum();
        (texts_size, 12 * self.texts.len() as u64, lineages_size)
    }

    /// One field's term list in byte order — the on-disk directory order.
    fn sorted_terms(field: &FieldStore) -> Vec<&String> {
        field.postings.keys().collect()
    }

    /// Per-term `(occurrence, skip)` run byte sizes of one field.
    fn field_run_sizes(field: &FieldStore, terms: &[&String]) -> io::Result<Vec<(u64, u64)>> {
        let mut run_sizes: Vec<(u64, u64)> = Vec::with_capacity(terms.len());
        for term in terms {
            let postings = &field.postings[*term];
            let occ_bytes: u64 = postings.iter().map(|p| 8 * p.offsets.len() as u64).sum();
            let mut skip = SkipRunBuilder::new();
            let mut sink = io::sink();
            for p in postings {
                let dl = field.doc_lengths[p.doc_id as usize];
                skip.push(p.tf, dl, p.doc_id, &mut sink)?;
            }
            let (l0_bytes, l1) = skip.finish(&mut sink)?;
            run_sizes.push((occ_bytes, skip_run_size(l0_bytes, &l1)));
        }
        Ok(run_sizes)
    }

    /// Byte size of one field's postings section (v5 shape).
    fn field_postings_size(field: &FieldStore, terms: &[&String], run_sizes: &[(u64, u64)]) -> u64 {
        4 + terms
            .iter()
            .enumerate()
            .map(|(i, t)| {
                let df = field.postings[*t].len() as u64;
                12 * df + 4 + run_sizes[i].0 + run_sizes[i].1
            })
            .sum::<u64>()
    }

    /// Byte size of one field's directory section (v5 shape).
    fn field_directory_size(terms: &[&String]) -> u64 {
        4 + 34 * terms.len() as u64 + terms.iter().map(|t| t.len() as u64).sum::<u64>()
    }

    fn write_texts_section<W: Write>(&self, w: &mut W) -> io::Result<()> {
        for text in &self.texts {
            match text {
                Some(t) => {
                    write_u32(w, t.len() as u32)?;
                    w.write_all(t.as_bytes())?;
                }
                None => write_u32(w, u32::MAX)?,
            }
        }
        Ok(())
    }

    fn write_shared_sections<W: Write>(&self, w: &mut W, index_base: u64) -> io::Result<()> {
        self.write_texts_section(w)?;
        write_text_index_from_lens(
            w,
            index_base,
            self.texts
                .iter()
                .map(|t| t.as_ref().map_or(u32::MAX, |s| s.len() as u32)),
        )?;
        write_lineages(w, &self.lineages)
    }

    fn write_field_doc_lengths<W: Write>(w: &mut W, field: &FieldStore) -> io::Result<()> {
        for &len in &field.doc_lengths {
            write_u32(w, len)?;
        }
        Ok(())
    }

    fn write_field_postings<W: Write>(
        w: &mut W,
        field: &FieldStore,
        terms: &[&String],
        run_sizes: &[(u64, u64)],
        run_base: u64,
    ) -> io::Result<Vec<(u64, u64, u64, u32)>> {
        write_u32(w, terms.len() as u32)?;
        let mut directory: Vec<(u64, u64, u64, u32)> = Vec::with_capacity(terms.len());
        let mut cursor = run_base + 4;
        for (i, term) in terms.iter().enumerate() {
            let postings = &field.postings[*term];
            let df = postings.len() as u64;
            let (occ_bytes, skip_bytes) = run_sizes[i];
            let doc_run_off = cursor;
            let occ_run_off = doc_run_off + 12 * df + 4;
            let skip_run_off = occ_run_off + occ_bytes;
            directory.push((doc_run_off, skip_run_off, occ_run_off, df as u32));
            let mut occ_stage: Vec<u8> = Vec::with_capacity(occ_bytes as usize);
            let mut skip_l0: Vec<u8> = Vec::new();
            let mut skip = SkipRunBuilder::new();
            let mut occ_start = 0u32;
            for p in postings {
                let dl = field.doc_lengths[p.doc_id as usize];
                occ_start = push_posting_v5(
                    w,
                    &mut occ_stage,
                    &mut skip,
                    &mut skip_l0,
                    p.doc_id,
                    p.tf,
                    &p.offsets,
                    dl,
                    occ_start,
                )?;
            }
            write_u32(w, occ_start)?;
            w.write_all(&occ_stage)?;
            let (l0_bytes, l1) = skip.finish(&mut skip_l0)?;
            debug_assert_eq!(l0_bytes, skip_l0.len() as u64);
            debug_assert_eq!(skip_bytes, skip_run_size(l0_bytes, &l1));
            write_skip_run(w, &skip_l0, &l1)?;
            cursor = skip_run_off + skip_bytes;
        }
        Ok(directory)
    }

    fn write_field_directory<W: Write>(
        w: &mut W,
        terms: &[&String],
        directory: &[(u64, u64, u64, u32)],
    ) -> io::Result<()> {
        write_v5_directory(
            w,
            terms.len(),
            terms
                .iter()
                .zip(directory)
                .map(|(t, &(doc, skip, occ, df))| (t.as_str(), doc, skip, occ, df)),
        )
    }

    /// Write the v5 layout (`TVBM2505`, `docs/block-max.md`).
    pub(in crate::postings) fn write_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        if !self.columns.map_integers.is_empty() || !self.columns.map_unsigned_integers.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "legacy format cannot preserve integer map columns",
            ));
        }
        if !self.columns.unsigned_integers.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "legacy format cannot preserve unsigned integer columns",
            ));
        }
        if !self.sources.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "v5 cannot preserve protobuf sources",
            ));
        }
        if self.fields.len() > 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "v5 carries exactly one field; multi-field stores write v6",
            ));
        }
        if !self.columns.facets.is_empty() || !self.columns.numerics.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "v5 carries no columns; column-bearing stores write v7",
            ));
        }
        if self.fields[0].positions {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "v5 carries no token positions; positional stores write v7",
            ));
        }
        let field = &self.fields[0];
        let n_slots = self.texts.len() as u64;
        let header_size = 8 + 8 + 8 * 4 + 4;
        let (texts_size, text_index_size, lineages_size) = self.shared_section_sizes();
        let terms = Self::sorted_terms(field);
        let run_sizes = Self::field_run_sizes(field, &terms)?;
        let postings_size = Self::field_postings_size(field, &terms, &run_sizes);

        let doc_lengths_off = header_size as u64;
        let texts_off = doc_lengths_off + 4 * n_slots;
        let text_index_off = texts_off + texts_size;
        let lineages_off = text_index_off + text_index_size;
        let postings_off = lineages_off + lineages_size;
        let directory_off = postings_off + postings_size;

        w.write_all(MAGIC_V5)?;
        write_u64(w, field.total_length)?;
        write_u64(w, texts_off)?;
        write_u64(w, lineages_off)?;
        write_u64(w, postings_off)?;
        write_u64(w, directory_off)?;
        write_u32(w, n_slots as u32)?;
        Self::write_field_doc_lengths(w, field)?;
        self.write_shared_sections(w, texts_off)?;
        let directory = Self::write_field_postings(w, field, &terms, &run_sizes, postings_off)?;
        Self::write_field_directory(w, &terms, &directory)?;
        Ok(())
    }

    /// Write the v6 (`TVBM2506`) or v7 (`TVBM2507`) payload to `w`.
    ///
    /// Column-less stores emit v6; stores with declared columns, inline
    /// metadata, protobuf sources, or per-field positional/sentence/SPLADE
    /// sections emit v7 (with [`FACET_ABSENT`] marking absent facet slots).
    pub fn write_v6_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        self.write_v6_ordered(w, true)
    }

    /// [`Self::write_v6_to`] with dictionaries in first-seen order — a legacy
    /// oracle for tests verifying pre-2026-09-02 files (`docs/prefix-terms.md`).
    pub fn write_v6_first_seen_dictionaries<W: Write>(&self, w: &mut W) -> io::Result<()> {
        self.write_v6_ordered(w, false)
    }

    fn write_v6_ordered<W: Write>(&self, w: &mut W, byte_order: bool) -> io::Result<()> {
        let n_slots = self.texts.len() as u64;
        let (texts_size, _, _) = self.shared_section_sizes();

        let mut field_terms: Vec<Vec<&String>> = Vec::with_capacity(self.fields.len());
        let mut field_runs: Vec<Vec<(u64, u64)>> = Vec::with_capacity(self.fields.len());
        let mut field_descs: Vec<FieldSectionDesc<'_>> = Vec::with_capacity(self.fields.len());

        for field in &self.fields {
            let terms = Self::sorted_terms(field);
            let run_sizes = Self::field_run_sizes(field, &terms)?;
            let postings_size = Self::field_postings_size(field, &terms, &run_sizes);
            let directory_size = Self::field_directory_size(&terms);
            let occ_total = field.positions.then(|| {
                terms
                    .iter()
                    .map(|t| {
                        field.postings[*t]
                            .iter()
                            .map(|p| p.positions.len() as u64)
                            .sum::<u64>()
                    })
                    .sum::<u64>()
            });
            let sentences_total = field
                .sentences
                .then(|| field.doc_sentences.iter().map(|s| s.len() as u64).sum());
            let pos_lengths_total = occ_total.filter(|_| field.has_lattice);
            let splade = field.splade.as_ref().filter(|s| !s.is_empty());

            field_descs.push(FieldSectionDesc {
                name: &field.name,
                analysis_fingerprint: field.analysis_fingerprint,
                total_length: field.total_length,
                doc_lengths: &field.doc_lengths,
                postings_size,
                directory_size,
                n_terms: terms.len() as u64,
                positions_total: occ_total,
                sentences_total,
                pos_lengths_total,
                splade,
            });
            field_terms.push(terms);
            field_runs.push(run_sizes);
        }

        let params = V6PayloadParams {
            n_slots,
            texts_size,
            lineages: &self.lineages,
            fields: &field_descs,
            columns: &self.columns,
            sources: &self.sources,
            byte_order,
        };
        let mut source = HeapSectionSource {
            store: self,
            field_terms: &field_terms,
            field_runs: &field_runs,
        };
        write_v6v7_payload(w, &params, &mut source)
    }

    /// Write the v4 format. Exists for benchmarking (v4-vs-v5 scorer
    /// comparisons) and migration checks; new shards are always v5+.
    pub fn write_v4_for_bench<W: Write>(&self, w: &mut W) -> io::Result<()> {
        self.write_v4_to(w)
    }

    fn validate_v4_store(&self) -> io::Result<()> {
        if !self.columns.map_integers.is_empty() || !self.columns.map_unsigned_integers.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "legacy format cannot preserve integer map columns",
            ));
        }
        if !self.columns.unsigned_integers.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "legacy format cannot preserve unsigned integer columns",
            ));
        }
        if !self.sources.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "legacy writer cannot preserve protobuf sources",
            ));
        }
        if self.fields.len() > 1 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "v4 carries exactly one field",
            ));
        }
        if self.fields[0].positions {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "v4 carries no token positions; positional stores write v7",
            ));
        }
        if !self.columns.facets.is_empty() || !self.columns.numerics.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "v4 carries no columns; column-bearing stores write v7",
            ));
        }
        Ok(())
    }

    fn write_v4_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        self.validate_v4_store()?;
        let field = &self.fields[0];
        let n_slots = self.texts.len() as u64;
        let header_size = 8 + 8 + 8 * 4 + 4;
        let doc_lengths_size = 4 * n_slots;
        let (texts_size, text_index_size, lineages_size) = self.shared_section_sizes();
        let terms = Self::sorted_terms(field);
        let postings_size: u64 = 4 + terms
            .iter()
            .map(|t| {
                4 + t.len() as u64
                    + 4
                    + field.postings[*t]
                        .iter()
                        .map(|p| 12 + 8 * p.offsets.len() as u64)
                        .sum::<u64>()
            })
            .sum::<u64>();

        let doc_lengths_off = header_size as u64;
        let texts_off = doc_lengths_off + doc_lengths_size;
        let text_index_off = texts_off + texts_size;
        let lineages_off = text_index_off + text_index_size;
        let postings_off = lineages_off + lineages_size;
        let directory_off = postings_off + postings_size;

        w.write_all(MAGIC_V4)?;
        write_u64(w, field.total_length)?;
        write_u64(w, texts_off)?;
        write_u64(w, lineages_off)?;
        write_u64(w, postings_off)?;
        write_u64(w, directory_off)?;
        write_u32(w, n_slots as u32)?;
        Self::write_field_doc_lengths(w, field)?;
        self.write_shared_sections(w, texts_off)?;
        Self::write_v4_postings_and_directory(w, field, &terms, postings_off)
    }

    fn write_v4_postings_and_directory<W: Write>(
        w: &mut W,
        field: &FieldStore,
        terms: &[&String],
        postings_off: u64,
    ) -> io::Result<()> {
        write_u32(w, terms.len() as u32)?;
        let mut directory: Vec<(u64, u32)> = Vec::with_capacity(terms.len());
        let mut cursor = postings_off + 4;
        for term in terms {
            let postings = &field.postings[*term];
            directory.push((cursor, postings.len() as u32));
            write_str(w, term)?;
            write_u32(w, postings.len() as u32)?;
            cursor += 4 + term.len() as u64 + 4;
            for p in postings {
                write_u32(w, p.doc_id)?;
                write_u32(w, p.tf)?;
                write_u32(w, p.offsets.len() as u32)?;
                for &(start, end) in &p.offsets {
                    write_u32(w, start)?;
                    write_u32(w, end)?;
                }
                cursor += 12 + 8 * p.offsets.len() as u64;
            }
        }
        write_u32(w, terms.len() as u32)?;
        let mut blob_off = 0u64;
        for (term, &(offset, df)) in terms.iter().zip(directory.iter()) {
            write_u64(w, offset)?;
            write_u32(w, df)?;
            write_u32(w, u32::try_from(blob_off).expect("term blob exceeds u32"))?;
            write_u16(w, term.len() as u16)?;
            blob_off += term.len() as u64;
        }
        for term in terms {
            w.write_all(term.as_bytes())?;
        }
        Ok(())
    }
}

struct HeapSectionSource<'a> {
    store: &'a Bm25Store,
    field_terms: &'a [Vec<&'a String>],
    field_runs: &'a [Vec<(u64, u64)>],
}

impl V6SectionSource for HeapSectionSource<'_> {
    fn write_texts<W: Write>(&mut self, w: &mut W) -> io::Result<()> {
        self.store.write_texts_section(w)
    }

    fn write_text_index<W: Write>(&mut self, w: &mut W) -> io::Result<()> {
        write_text_index_from_lens(
            w,
            0,
            self.store
                .texts
                .iter()
                .map(|t| t.as_ref().map_or(u32::MAX, |s| s.len() as u32)),
        )
    }

    fn write_field_postings_and_directory<W: Write>(
        &mut self,
        w: &mut W,
        fi: usize,
    ) -> io::Result<()> {
        let field = &self.store.fields[fi];
        let terms = &self.field_terms[fi];
        let directory = Bm25Store::write_field_postings(w, field, terms, &self.field_runs[fi], 0)?;
        Bm25Store::write_field_directory(w, terms, &directory)
    }

    fn write_field_positions<W: Write>(
        &mut self,
        w: &mut W,
        fi: usize,
        total: u64,
    ) -> io::Result<()> {
        let field = &self.store.fields[fi];
        let terms = &self.field_terms[fi];
        write_term_occurrence_bases(w, field, terms, total, "positions")?;
        for term in terms {
            for p in &field.postings[*term] {
                for &ordinal in &p.positions {
                    write_u32(w, ordinal)?;
                }
            }
        }
        Ok(())
    }

    fn write_field_sentences<W: Write>(
        &mut self,
        w: &mut W,
        fi: usize,
        n_slots: u64,
        total: u64,
    ) -> io::Result<()> {
        let field = &self.store.fields[fi];
        write_u32(w, n_slots as u32)?;
        let mut base = 0u64;
        for slot in 0..n_slots as usize {
            write_u32(w, u32::try_from(base).expect("sentences exceed u32"))?;
            base += field.doc_sentences.get(slot).map_or(0, |s| s.len() as u64);
        }
        write_u32(w, u32::try_from(base).expect("sentences exceed u32"))?;
        debug_assert_eq!(base, total);
        for slot in 0..n_slots as usize {
            if let Some(spans) = field.doc_sentences.get(slot) {
                for &(start, end) in spans {
                    write_u32(w, start)?;
                    write_u32(w, end)?;
                }
            }
        }
        Ok(())
    }

    fn write_field_pos_lengths<W: Write>(
        &mut self,
        w: &mut W,
        fi: usize,
        total: u64,
    ) -> io::Result<()> {
        let field = &self.store.fields[fi];
        let terms = &self.field_terms[fi];
        write_term_occurrence_bases(w, field, terms, total, "pos_lengths")?;
        for term in terms {
            for p in &field.postings[*term] {
                if p.pos_lengths.is_empty() {
                    for _ in 0..p.positions.len() {
                        write_u32(w, 1)?;
                    }
                } else {
                    for &len in &p.pos_lengths {
                        write_u32(w, len)?;
                    }
                }
            }
        }
        Ok(())
    }
}

fn write_term_occurrence_bases<W: Write>(
    w: &mut W,
    field: &FieldStore,
    terms: &[&String],
    total: u64,
    label: &str,
) -> io::Result<()> {
    write_u32(w, terms.len() as u32)?;
    let mut base = 0u64;
    for term in terms {
        write_u32(
            w,
            u32::try_from(base).unwrap_or_else(|_| panic!("{label} exceed u32")),
        )?;
        base += field.postings[*term]
            .iter()
            .map(|p| p.positions.len() as u64)
            .sum::<u64>();
    }
    write_u32(
        w,
        u32::try_from(base).unwrap_or_else(|_| panic!("{label} exceed u32")),
    )?;
    debug_assert_eq!(base, total);
    Ok(())
}
