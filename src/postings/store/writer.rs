//! Serialization of `Bm25Store` into `.bm25` v6/v7 payloads and v8 envelopes.

use std::io::{self, Write};
use std::path::Path;

use super::super::codec::{push_block_max_posting, skip_run_size, write_skip_run, SkipRunBuilder};
#[cfg(doc)]
use super::super::columns::FACET_ABSENT;
use super::super::format::write_u32;
use super::super::section_writer::{
    save_v8_atomic, write_block_max_directory, write_text_index_from_lens, write_v6v7_payload,
    FieldSectionDesc, V6PayloadParams, V6SectionSource,
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
    /// envelope. A test-only oracle (see [`Self::write_v6_first_seen_dictionaries`]).
    pub fn save_first_seen_dictionaries(&self, path: &Path) -> io::Result<()> {
        save_v8_atomic(path, |w| self.write_v6_first_seen_dictionaries(w))
    }

    /// Byte size of the shared `texts` section.
    fn texts_section_size(&self) -> u64 {
        self.texts
            .iter()
            .map(|t| 4 + t.as_ref().map_or(0, |s| s.len() as u64))
            .sum()
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

    /// Byte size of one field's postings section.
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

    /// Byte size of one field's 34-byte directory section.
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
                occ_start = push_block_max_posting(
                    (w, &mut occ_stage),
                    (&mut skip, &mut skip_l0),
                    (p.doc_id, p.tf, &p.offsets),
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
        write_block_max_directory(
            w,
            terms.len(),
            terms
                .iter()
                .zip(directory)
                .map(|(t, &(doc, skip, occ, df))| (t.as_str(), doc, skip, occ, df)),
        )
    }

    /// Write the v6 (`TVBM2506`) or v7 (`TVBM2507`) payload to `w`.
    ///
    /// Column-less stores emit v6; stores with declared columns, inline
    /// metadata, protobuf sources, or per-field positional/sentence/SPLADE
    /// sections emit v7 (with [`FACET_ABSENT`] marking absent facet slots).
    pub fn write_v6_to<W: Write>(&self, w: &mut W) -> io::Result<()> {
        self.write_v6_ordered(w, true)
    }

    /// [`Self::write_v6_to`] with dictionaries in first-seen order: a test
    /// oracle for files whose dictionaries are not byte-ordered.
    pub fn write_v6_first_seen_dictionaries<W: Write>(&self, w: &mut W) -> io::Result<()> {
        self.write_v6_ordered(w, false)
    }

    fn write_v6_ordered<W: Write>(&self, w: &mut W, byte_order: bool) -> io::Result<()> {
        let n_slots = self.texts.len() as u64;
        let texts_size = self.texts_section_size();

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
