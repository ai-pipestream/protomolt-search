//! Sorted run merging and final `.bm25` assembly for `SpillBuilder`.

use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use super::super::codec::{skip_run_size, write_skip_run, SkipRunBuilder};
use super::super::columns::{
    binding_entry_size, derived_entry_size, integer_map_sections, write_binding_entry,
    write_derived_entry, write_facet_column, write_integer_map_entries, write_map_facet_column,
    write_map_numeric_column, GEO_ABSENT,
};
use super::super::format::{
    finalize_v8, fsync_parent, positions_section_size, sentences_section_size, write_source_entry,
    write_str, write_u16, write_u32, write_u64, COLUMN_KIND_F64, COLUMN_KIND_FACET,
    COLUMN_KIND_GEO, COLUMN_KIND_I64_PRESENT, COLUMN_KIND_MAP_F64, COLUMN_KIND_MAP_FACET,
    COLUMN_KIND_MAP_I64, COLUMN_KIND_MAP_U64, COLUMN_KIND_POSITIONS, COLUMN_KIND_POS_LENGTHS,
    COLUMN_KIND_SENTENCES, COLUMN_KIND_SPLADE, COLUMN_KIND_U64, MAGIC_V4, MAGIC_V6, MAGIC_V7,
    POSITIONS_ENTRY_PREFIX, POS_LENGTHS_ENTRY_PREFIX, SENTENCES_ENTRY_PREFIX, SOURCE_ENTRY_BYTES,
    SPLADE_ENTRY_PREFIX,
};
#[cfg(doc)]
use super::super::Bm25Store;
use super::{PostingDirectoryEntry, SpillBuilder};

impl SpillBuilder {
    /// Merge the runs and assemble the file at `path` (atomically:
    /// write tmp, rename). The spill directory is removed on success.
    /// Writes v6, byte-identical to [`Bm25Store::save`] on the same
    /// corpus, unless built with [`Self::create_v4_for_bench`].
    pub fn finish(&mut self, path: &Path) -> io::Result<()> {
        if self.v4_only {
            if self.binding.is_some() || self.derived.is_some() {
                return Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "the v4 oracle format cannot carry a mapped-plan binding or a derived-column declaration",
                ));
            }
            self.finish_v4(path)
        } else {
            self.finish_v6(path)
        }
    }

    /// Merge one field's runs into a postings-section body file at
    /// `body_path` (per-term doc/occurrence/skip runs, WITHOUT the
    /// section's leading u32 n_terms), returning the directory
    /// `(term, doc_rel, skip_rel, occ_rel, df)` with offsets relative
    /// to the body start. Merge side of the v5-shaped postings layout:
    /// the doc run streams into the body while occurrence bytes divert
    /// to a per-term stage file and the skip builder accumulates
    /// `(tf, dl)` per 128-posting block — all single-pass with O(1)
    /// state per term, so the sub-1 GB build memory the spill builder
    /// buys is untouched.
    fn merge_field_runs(
        runs: &[PathBuf],
        doc_lengths: &[u32],
        body_path: &Path,
        occ_stage_path: &Path,
        positions: Option<&mut PositionsStage>,
        pos_lengths: Option<&mut PositionsStage>,
    ) -> io::Result<Vec<PostingDirectoryEntry>> {
        let mut directory: Vec<PostingDirectoryEntry> = Vec::new();
        let mut out = io::BufWriter::new(std::fs::File::create(body_path)?);
        let mut heads: Vec<RunHead> = Vec::new();
        let positional = positions.is_some();
        let mut positions = positions;
        let mut pos_lengths = pos_lengths;
        for run in runs {
            if let Some(head) = RunHead::open(run, positional)? {
                heads.push(head);
            }
        }
        let mut cursor = 0u64;
        while !heads.is_empty() {
            // Smallest term across run heads; runs are time-ordered,
            // and ids only grow, so draining matching heads in run
            // order keeps postings doc-ascending.
            let term = heads
                .iter()
                .map(|h| h.term.clone())
                .min()
                .expect("nonempty heads");
            let doc_rel = cursor;
            let mut df = 0u64;
            let mut occ_bytes = 0u64;
            let mut occ_start = 0u32;
            let mut skip_l0: Vec<u8> = Vec::new();
            let mut skip = SkipRunBuilder::new();
            // The kind-7 base of this term: occurrences merged so far, in
            // the same (sorted) term order the directory will have.
            if let Some(stage) = positions.as_deref_mut() {
                stage
                    .bases
                    .push(u32::try_from(stage.total).expect("positions exceed u32"));
            }
            if let Some(stage) = pos_lengths.as_deref_mut() {
                stage
                    .bases
                    .push(u32::try_from(stage.total).expect("pos_lengths exceed u32"));
            }
            {
                let mut occ_stage = io::BufWriter::new(std::fs::File::create(occ_stage_path)?);
                let mut idx = 0;
                while idx < heads.len() {
                    if heads[idx].term == term {
                        for _ in 0..heads[idx].n_postings {
                            let (doc_id, tf, offsets, ordinals, pos_lens) =
                                heads[idx].next_posting_raw()?;
                            let dl = doc_lengths[doc_id as usize];
                            write_u32(&mut out, doc_id)?;
                            write_u32(&mut out, tf)?;
                            write_u32(&mut out, occ_start)?;
                            // The run encoding's offset bytes are
                            // exactly the occurrence-run pairs, and its
                            // ordinal bytes exactly the kind-7 entries.
                            occ_stage.write_all(&offsets)?;
                            if let Some(stage) = positions.as_deref_mut() {
                                stage.writer.write_all(&ordinals)?;
                                stage.total += ordinals.len() as u64 / 4;
                            }
                            if let Some(stage) = pos_lengths.as_deref_mut() {
                                let n_occ = offsets.len() / 8;
                                if pos_lens.is_empty() {
                                    for _ in 0..n_occ {
                                        write_u32(&mut stage.writer, 1)?;
                                    }
                                    stage.total += n_occ as u64;
                                } else {
                                    stage.writer.write_all(&pos_lens)?;
                                    stage.total += pos_lens.len() as u64 / 4;
                                }
                            }
                            skip.push(tf, dl, doc_id, &mut skip_l0)?;
                            occ_start += offsets.len() as u32 / 8;
                            occ_bytes += offsets.len() as u64;
                            df += 1;
                        }
                        if !heads[idx].advance()? {
                            heads.remove(idx);
                            continue;
                        }
                    }
                    idx += 1;
                }
                occ_stage.flush()?;
            }
            write_u32(&mut out, occ_start)?; // sentinel
            {
                let mut stage = std::fs::File::open(occ_stage_path)?;
                io::copy(&mut stage, &mut out)?;
            }
            let (l0_bytes, l1) = skip.finish(&mut skip_l0)?;
            debug_assert_eq!(l0_bytes, skip_l0.len() as u64);
            let occ_rel = doc_rel + 12 * df + 4;
            let skip_rel = occ_rel + occ_bytes;
            let skip_bytes = skip_run_size(l0_bytes, &l1);
            write_skip_run(&mut out, &skip_l0, &l1)?;
            directory.push((term, doc_rel, skip_rel, occ_rel, df as u32));
            cursor = skip_rel + skip_bytes;
        }
        out.flush()?;
        Ok(directory)
    }

    /// Merge every field's runs and assemble the v6 file at `path` —
    /// v7 when facet fields are declared — mirroring
    /// [`Bm25Store::write_v6_to`] section for section (the dual-writer
    /// byte-identity test pins the two).
    fn finish_v6(&mut self, path: &Path) -> io::Result<()> {
        self.spill_run()?;
        self.texts.flush()?;

        let occ_stage_path = self.dir.join("occ.stage");
        let mut directories: Vec<Vec<PostingDirectoryEntry>> =
            Vec::with_capacity(self.fields.len());
        let mut body_paths: Vec<PathBuf> = Vec::with_capacity(self.fields.len());
        let mut body_lens: Vec<u64> = Vec::with_capacity(self.fields.len());
        // Per positional field: its kind-7 stage (ordinals streamed to
        // a file during the merge, bases kept in heap at 4 B per term).
        let mut position_stages: Vec<Option<PositionsStage>> =
            Vec::with_capacity(self.fields.len());
        let mut pos_length_stages: Vec<Option<PositionsStage>> =
            Vec::with_capacity(self.fields.len());
        for (fi, field) in self.fields.iter().enumerate() {
            let body_path = self.dir.join(format!("postings-f{fi:03}.body"));
            let mut stage = if field.positions {
                let path = self.dir.join(format!("positions-f{fi:03}.stage"));
                Some(PositionsStage {
                    writer: io::BufWriter::new(std::fs::File::create(&path)?),
                    path,
                    bases: Vec::new(),
                    total: 0,
                })
            } else {
                None
            };
            let mut len_stage = if field.positions && field.has_lattice {
                let path = self.dir.join(format!("pos-lengths-f{fi:03}.stage"));
                Some(PositionsStage {
                    writer: io::BufWriter::new(std::fs::File::create(&path)?),
                    path,
                    bases: Vec::new(),
                    total: 0,
                })
            } else {
                None
            };
            let directory = Self::merge_field_runs(
                &field.runs,
                &field.doc_lengths,
                &body_path,
                &occ_stage_path,
                stage.as_mut(),
                len_stage.as_mut(),
            )?;
            if let Some(stage) = stage.as_mut() {
                stage.writer.flush()?;
            }
            if let Some(stage) = len_stage.as_mut() {
                stage.writer.flush()?;
            }
            body_lens.push(std::fs::metadata(&body_path)?.len());
            body_paths.push(body_path);
            directories.push(directory);
            position_stages.push(stage);
            pos_length_stages.push(len_stage);
        }

        // Section geometry, identical to Bm25Store::write_v6_to.
        let n_slots = self.fields[0].doc_lengths.len() as u64;
        let has_columns = !self.facets.is_empty()
            || !self.numerics.is_empty()
            || !self.map_facets.is_empty()
            || !self.map_numerics.is_empty()
            || !self.map_integers.is_empty()
            || !self.map_unsigned_integers.is_empty()
            || !self.integers.is_empty()
            || !self.unsigned_integers.is_empty()
            || !self.geos.is_empty()
            || self.binding.is_some()
            || self.derived.is_some()
            || !self.sources.is_empty()
            || self.fields.iter().any(|f| {
                f.positions || f.sentences || f.splade.as_ref().is_some_and(|s| !s.is_empty())
            });
        let column_table_size: u64 = if !has_columns {
            0
        } else {
            4 + self
                .facets
                .iter()
                .map(|f| 2 + f.name.len() as u64 + 1 + 4 + 8 + 8)
                .sum::<u64>()
                + self
                    .numerics
                    .iter()
                    .map(|n| 2 + n.name.len() as u64 + 1 + 8 + 8 + 8)
                    .sum::<u64>()
                + self
                    .map_facets
                    .iter()
                    .map(|c| 2 + c.name.len() as u64 + 1 + 4 + 4 + 8 * 4)
                    .sum::<u64>()
                + self
                    .map_numerics
                    .iter()
                    .map(|c| 2 + c.name.len() as u64 + 1 + 4 + 8 * 3)
                    .sum::<u64>()
                + self
                    .map_integers
                    .iter()
                    .map(|c| 2 + c.name.len() as u64 + 1 + 16)
                    .sum::<u64>()
                + self
                    .map_unsigned_integers
                    .iter()
                    .map(|c| 2 + c.name.len() as u64 + 1 + 16)
                    .sum::<u64>()
                + self
                    .integers
                    .iter()
                    .map(|c| 2 + c.name.len() as u64 + 1 + 8 * 3)
                    .sum::<u64>()
                + self
                    .unsigned_integers
                    .iter()
                    .map(|c| 2 + c.name.len() as u64 + 1 + 8 * 3)
                    .sum::<u64>()
                + self
                    .geos
                    .iter()
                    .map(|c| 2 + c.name.len() as u64 + 1 + 8 * 5)
                    .sum::<u64>()
                + self
                    .fields
                    .iter()
                    .filter(|f| f.positions)
                    .map(|f| 2 + (POSITIONS_ENTRY_PREFIX.len() + f.name.len()) as u64 + 1 + 8 * 2)
                    .sum::<u64>()
                + self
                    .fields
                    .iter()
                    .filter(|f| f.sentences)
                    .map(|f| 2 + (SENTENCES_ENTRY_PREFIX.len() + f.name.len()) as u64 + 1 + 8 * 2)
                    .sum::<u64>()
                + binding_entry_size(self.binding.as_ref())
                + derived_entry_size(self.derived.as_ref())
                + if self.sources.is_empty() {
                    0
                } else {
                    SOURCE_ENTRY_BYTES
                }
                + self
                    .fields
                    .iter()
                    .filter(|f| f.positions && f.has_lattice)
                    .map(|f| 2 + (POS_LENGTHS_ENTRY_PREFIX.len() + f.name.len()) as u64 + 1 + 8 * 2)
                    .sum::<u64>()
                + self
                    .fields
                    .iter()
                    .filter(|f| f.splade.as_ref().is_some_and(|s| !s.is_empty()))
                    .map(|f| 2 + (SPLADE_ENTRY_PREFIX.len() + f.name.len()) as u64 + 1 + 8 * 2)
                    .sum::<u64>()
        };
        let header_size: u64 = 8
            + 4
            + 4
            + 8 * 3
            + self
                .fields
                .iter()
                .map(|f| 2 + f.name.len() as u64 + 8 * 5)
                .sum::<u64>()
            + column_table_size;
        let texts_off = header_size;
        let text_index_off = texts_off + self.texts_bytes;
        let lineages_off = text_index_off + 12 * n_slots;
        let lineages_size: u64 = self
            .lineages
            .iter()
            .map(|l| if l.is_some() { 25 } else { 1 })
            .sum();
        let mut cursor = lineages_off + lineages_size;
        // (doc_lengths_off, postings_off, directory_off) per field.
        let mut section_offs: Vec<(u64, u64, u64)> = Vec::with_capacity(self.fields.len());
        for fi in 0..self.fields.len() {
            let doc_lengths_off = cursor;
            let postings_off = doc_lengths_off + 4 * n_slots;
            let directory_off = postings_off + 4 + body_lens[fi];
            let directory_size = 4
                + 34 * directories[fi].len() as u64
                + directories[fi]
                    .iter()
                    .map(|(t, ..)| t.len() as u64)
                    .sum::<u64>();
            section_offs.push((doc_lengths_off, postings_off, directory_off));
            cursor = directory_off + directory_size;
        }
        // (dict_off, ords_off) per facet, then vals_off per numeric,
        // after the last field group — column-table order.
        let mut facet_offs: Vec<(u64, u64)> = Vec::with_capacity(self.facets.len());
        for facet in &self.facets {
            let dict_off = cursor;
            let dict_size: u64 = facet.dict.iter().map(|v| 2 + v.len() as u64).sum();
            let ords_off = dict_off + dict_size;
            facet_offs.push((dict_off, ords_off));
            cursor = ords_off + 4 * n_slots;
        }
        let mut numeric_offs: Vec<u64> = Vec::with_capacity(self.numerics.len());
        for _ in &self.numerics {
            numeric_offs.push(cursor);
            cursor += 8 * n_slots;
        }
        let mut map_facet_offs: Vec<(u64, u64, u64, u64)> =
            Vec::with_capacity(self.map_facets.len());
        for c in &self.map_facets {
            let keys_off = cursor;
            let keys_size: u64 = c.keys.iter().map(|k| 2 + k.len() as u64).sum();
            let values_off = keys_off + keys_size;
            let values_size: u64 = c.values.iter().map(|v| 2 + v.len() as u64).sum();
            let offsets_off = values_off + values_size;
            let pairs_off = offsets_off + 4 * (n_slots + 1);
            let total_pairs: u64 = c.pairs.iter().map(|l| l.len() as u64).sum();
            map_facet_offs.push((keys_off, values_off, offsets_off, pairs_off));
            cursor = pairs_off + 8 * total_pairs;
        }
        let mut map_numeric_offs: Vec<(u64, u64, u64)> =
            Vec::with_capacity(self.map_numerics.len());
        for c in &self.map_numerics {
            let keys_off = cursor;
            let keys_size: u64 = c.keys.iter().map(|k| 2 + k.len() as u64 + 16).sum();
            let offsets_off = keys_off + keys_size;
            let pairs_off = offsets_off + 4 * (n_slots + 1);
            let total_pairs: u64 = c.pairs.iter().map(|l| l.len() as u64).sum();
            map_numeric_offs.push((keys_off, offsets_off, pairs_off));
            cursor = pairs_off + 12 * total_pairs;
        }
        // Integer values followed by their explicit presence bitmap.
        let mut integer_offs: Vec<u64> = Vec::with_capacity(self.integers.len());
        for _ in &self.integers {
            integer_offs.push(cursor);
            cursor += 8 * n_slots + n_slots.div_ceil(8);
        }
        // Geo sections follow the integer groups.
        let mut geo_offs: Vec<u64> = Vec::with_capacity(self.geos.len());
        for _ in &self.geos {
            geo_offs.push(cursor);
            cursor += 16 * n_slots;
        }
        // (field id, section_off, total) per positional field, last in
        // table order — identical geometry to Bm25Store::write_v6_to.
        let mut positions_offs: Vec<(usize, u64, u64)> = Vec::new();
        for (fi, stage) in position_stages.iter().enumerate() {
            let Some(stage) = stage else {
                continue;
            };
            positions_offs.push((fi, cursor, stage.total));
            cursor += positions_section_size(directories[fi].len() as u64, stage.total);
        }
        // Kind-8 sentence sections after the positions sections, from
        // the stages the ingest filled — same geometry as the heap
        // writer (docs/highlighting.md).
        let n_slots_u64 = self.fields[0].doc_lengths.len() as u64;
        let mut sentences_offs: Vec<(usize, u64, u64)> = Vec::new();
        for (fi, field) in self.fields.iter().enumerate() {
            if !field.sentences {
                continue;
            }
            let total = field.sentence_stage.as_ref().map_or(0, |s| s.total);
            sentences_offs.push((fi, cursor, total));
            cursor += sentences_section_size(n_slots_u64, total);
        }

        let mut unsigned_offs = Vec::with_capacity(self.unsigned_integers.len());
        for _ in &self.unsigned_integers {
            unsigned_offs.push(cursor);
            cursor += 8 * n_slots + n_slots.div_ceil(8);
        }
        let map_integer_sections =
            integer_map_sections(&self.map_integers, n_slots as u32, &mut cursor)?;
        let map_unsigned_sections =
            integer_map_sections(&self.map_unsigned_integers, n_slots as u32, &mut cursor)?;
        let source_section = if self.sources.is_empty() {
            None
        } else {
            let len = self.sources.encoded_len(n_slots as u32)?;
            let off = cursor;
            cursor += len;
            Some((off, len))
        };
        let mut pos_lengths_offs: Vec<(usize, u64, u64)> = Vec::new();
        for (fi, stage) in pos_length_stages.iter().enumerate() {
            let Some(stage) = stage else {
                continue;
            };
            pos_lengths_offs.push((fi, cursor, stage.total));
            cursor += positions_section_size(directories[fi].len() as u64, stage.total);
        }
        let mut splade_offs: Vec<(usize, u64, u64)> = Vec::new();
        for (fi, field) in self.fields.iter().enumerate() {
            let Some(splade) = field.splade.as_ref().filter(|s| !s.is_empty()) else {
                continue;
            };
            let len = splade.encoded_len();
            splade_offs.push((fi, cursor, len));
            cursor += len;
        }
        let tmp = path.with_extension("bm25tmp");
        {
            let mut w = io::BufWriter::new(std::fs::File::create(&tmp)?);
            w.write_all(if has_columns { MAGIC_V7 } else { MAGIC_V6 })?;
            write_u32(&mut w, self.fields.len() as u32)?;
            write_u32(&mut w, n_slots as u32)?;
            write_u64(&mut w, texts_off)?;
            write_u64(&mut w, text_index_off)?;
            write_u64(&mut w, lineages_off)?;
            for (field, &(dl_off, p_off, d_off)) in self.fields.iter().zip(&section_offs) {
                write_u16(&mut w, field.name.len() as u16)?;
                w.write_all(field.name.as_bytes())?;
                write_u64(&mut w, field.analysis_fingerprint)?;
                write_u64(&mut w, field.total_length)?;
                write_u64(&mut w, dl_off)?;
                write_u64(&mut w, p_off)?;
                write_u64(&mut w, d_off)?;
            }
            if has_columns {
                write_u32(
                    &mut w,
                    (self.facets.len()
                        + self.numerics.len()
                        + self.map_facets.len()
                        + self.map_numerics.len()
                        + self.map_integers.len()
                        + self.map_unsigned_integers.len()
                        + self.integers.len()
                        + self.unsigned_integers.len()
                        + self.geos.len()
                        + positions_offs.len()
                        + sentences_offs.len()
                        + usize::from(self.binding.is_some())
                        + usize::from(self.derived.is_some())
                        + usize::from(!self.sources.is_empty())
                        + pos_lengths_offs.len()
                        + splade_offs.len()) as u32,
                )?;
                for (facet, &(dict_off, ords_off)) in self.facets.iter().zip(&facet_offs) {
                    write_u16(&mut w, facet.name.len() as u16)?;
                    w.write_all(facet.name.as_bytes())?;
                    w.write_all(&[COLUMN_KIND_FACET])?;
                    write_u32(&mut w, facet.dict.len() as u32)?;
                    write_u64(&mut w, dict_off)?;
                    write_u64(&mut w, ords_off)?;
                }
                for (numeric, &vals_off) in self.numerics.iter().zip(&numeric_offs) {
                    let (min, max) = numeric.min_max();
                    write_u16(&mut w, numeric.name.len() as u16)?;
                    w.write_all(numeric.name.as_bytes())?;
                    w.write_all(&[COLUMN_KIND_F64])?;
                    write_u64(&mut w, min.to_bits())?;
                    write_u64(&mut w, max.to_bits())?;
                    write_u64(&mut w, vals_off)?;
                }
                for (c, &(keys_off, values_off, offsets_off, pairs_off)) in
                    self.map_facets.iter().zip(&map_facet_offs)
                {
                    write_u16(&mut w, c.name.len() as u16)?;
                    w.write_all(c.name.as_bytes())?;
                    w.write_all(&[COLUMN_KIND_MAP_FACET])?;
                    write_u32(&mut w, c.keys.len() as u32)?;
                    write_u32(&mut w, c.values.len() as u32)?;
                    write_u64(&mut w, keys_off)?;
                    write_u64(&mut w, values_off)?;
                    write_u64(&mut w, offsets_off)?;
                    write_u64(&mut w, pairs_off)?;
                }
                for (c, &(keys_off, offsets_off, pairs_off)) in
                    self.map_numerics.iter().zip(&map_numeric_offs)
                {
                    write_u16(&mut w, c.name.len() as u16)?;
                    w.write_all(c.name.as_bytes())?;
                    w.write_all(&[COLUMN_KIND_MAP_F64])?;
                    write_u32(&mut w, c.keys.len() as u32)?;
                    write_u64(&mut w, keys_off)?;
                    write_u64(&mut w, offsets_off)?;
                    write_u64(&mut w, pairs_off)?;
                }
                for (c, &vals_off) in self.integers.iter().zip(&integer_offs) {
                    let (min, max) = c.min_max();
                    write_u16(&mut w, c.name.len() as u16)?;
                    w.write_all(c.name.as_bytes())?;
                    w.write_all(&[COLUMN_KIND_I64_PRESENT])?;
                    write_u64(&mut w, min as u64)?;
                    write_u64(&mut w, max as u64)?;
                    write_u64(&mut w, vals_off)?;
                }
                for (c, &vals_off) in self.geos.iter().zip(&geo_offs) {
                    let (min_lat, max_lat, min_lon, max_lon) = c.bbox();
                    write_u16(&mut w, c.name.len() as u16)?;
                    w.write_all(c.name.as_bytes())?;
                    w.write_all(&[COLUMN_KIND_GEO])?;
                    write_u64(&mut w, min_lat.to_bits())?;
                    write_u64(&mut w, max_lat.to_bits())?;
                    write_u64(&mut w, min_lon.to_bits())?;
                    write_u64(&mut w, max_lon.to_bits())?;
                    write_u64(&mut w, vals_off)?;
                }
                for &(fi, off, total) in &positions_offs {
                    let name = format!("{POSITIONS_ENTRY_PREFIX}{}", self.fields[fi].name);
                    write_u16(&mut w, name.len() as u16)?;
                    w.write_all(name.as_bytes())?;
                    w.write_all(&[COLUMN_KIND_POSITIONS])?;
                    write_u64(&mut w, off)?;
                    write_u64(&mut w, total)?;
                }
                for &(fi, off, total) in &sentences_offs {
                    let name = format!("{SENTENCES_ENTRY_PREFIX}{}", self.fields[fi].name);
                    write_u16(&mut w, name.len() as u16)?;
                    w.write_all(name.as_bytes())?;
                    w.write_all(&[COLUMN_KIND_SENTENCES])?;
                    write_u64(&mut w, off)?;
                    write_u64(&mut w, total)?;
                }
                for (column, &off) in self.unsigned_integers.iter().zip(&unsigned_offs) {
                    let (min, max) = column.min_max();
                    write_u16(&mut w, column.name.len() as u16)?;
                    w.write_all(column.name.as_bytes())?;
                    w.write_all(&[COLUMN_KIND_U64])?;
                    write_u64(&mut w, min)?;
                    write_u64(&mut w, max)?;
                    write_u64(&mut w, off)?;
                }
                write_integer_map_entries(
                    &mut w,
                    &self.map_integers,
                    &map_integer_sections,
                    COLUMN_KIND_MAP_I64,
                )?;
                write_integer_map_entries(
                    &mut w,
                    &self.map_unsigned_integers,
                    &map_unsigned_sections,
                    COLUMN_KIND_MAP_U64,
                )?;
                write_binding_entry(&mut w, self.binding.as_ref())?;
                write_derived_entry(&mut w, self.derived.as_ref())?;
                write_source_entry(&mut w, source_section)?;
                for &(fi, off, total) in &pos_lengths_offs {
                    let name = format!("{POS_LENGTHS_ENTRY_PREFIX}{}", self.fields[fi].name);
                    write_u16(&mut w, name.len() as u16)?;
                    w.write_all(name.as_bytes())?;
                    w.write_all(&[COLUMN_KIND_POS_LENGTHS])?;
                    write_u64(&mut w, off)?;
                    write_u64(&mut w, total)?;
                }
                for &(fi, off, len) in &splade_offs {
                    let name = format!("{SPLADE_ENTRY_PREFIX}{}", self.fields[fi].name);
                    write_u16(&mut w, name.len() as u16)?;
                    w.write_all(name.as_bytes())?;
                    w.write_all(&[COLUMN_KIND_SPLADE])?;
                    write_u64(&mut w, off)?;
                    write_u64(&mut w, len)?;
                }
            }
            // texts: byte-copy of the spill (already section-encoded).
            let mut spill = std::fs::File::open(self.dir.join("texts.spill"))?;
            io::copy(&mut spill, &mut w)?;
            // text_index, from the in-memory length table; entries are
            // relative to the texts section start (v6).
            let mut tcur = 0u64;
            for &len in &self.text_lens {
                if len == u32::MAX {
                    write_u64(&mut w, 0)?;
                    write_u32(&mut w, u32::MAX)?;
                    tcur += 4;
                } else {
                    write_u64(&mut w, tcur + 4)?;
                    write_u32(&mut w, len)?;
                    tcur += 4 + len as u64;
                }
            }
            for lineage in &self.lineages {
                match lineage {
                    Some(l) => {
                        w.write_all(&[1u8])?;
                        write_u64(&mut w, l.parent_id)?;
                        write_u64(&mut w, l.group_id)?;
                        write_u32(&mut w, l.span_start)?;
                        write_u32(&mut w, l.span_end)?;
                    }
                    None => w.write_all(&[0u8])?,
                }
            }
            for (fi, field) in self.fields.iter().enumerate() {
                for &len in &field.doc_lengths {
                    write_u32(&mut w, len)?;
                }
                write_u32(&mut w, directories[fi].len() as u32)?;
                let mut body = std::fs::File::open(&body_paths[fi])?;
                io::copy(&mut body, &mut w)?;
                write_u32(&mut w, directories[fi].len() as u32)?;
                let mut blob_off = 0u64; // relative to the term blob start
                for (term, doc_rel, skip_rel, occ_rel, df) in &directories[fi] {
                    // Section-relative run offsets: past the section's
                    // leading u32 n_terms.
                    write_u64(&mut w, 4 + doc_rel)?;
                    write_u64(&mut w, 4 + skip_rel)?;
                    write_u64(&mut w, 4 + occ_rel)?;
                    write_u32(&mut w, *df)?;
                    write_u32(
                        &mut w,
                        u32::try_from(blob_off).expect("term blob exceeds u32"),
                    )?;
                    write_u16(&mut w, term.len() as u16)?;
                    blob_off += term.len() as u64;
                }
                for (term, ..) in &directories[fi] {
                    w.write_all(term.as_bytes())?;
                }
            }
            for facet in &self.facets {
                write_facet_column(&mut w, facet, n_slots as usize, true)?;
            }
            for numeric in &self.numerics {
                for slot in 0..n_slots as usize {
                    write_u64(
                        &mut w,
                        numeric
                            .vals
                            .get(slot)
                            .copied()
                            .unwrap_or(f64::NAN)
                            .to_bits(),
                    )?;
                }
            }
            for c in &self.map_facets {
                write_map_facet_column(&mut w, c, n_slots as usize, true)?;
            }
            for c in &self.map_numerics {
                write_map_numeric_column(&mut w, c, n_slots as usize, true)?;
            }
            for c in &self.integers {
                c.write(&mut w, n_slots as usize)?;
            }
            for c in &self.geos {
                for slot in 0..n_slots as usize {
                    let (lat, lon) = c.vals.get(slot).copied().unwrap_or(GEO_ABSENT);
                    write_u64(&mut w, lat.to_bits())?;
                    write_u64(&mut w, lon.to_bits())?;
                }
            }
            for &(fi, _, total) in &positions_offs {
                let stage = position_stages[fi]
                    .as_ref()
                    .expect("positional field has a stage");
                write_u32(&mut w, directories[fi].len() as u32)?;
                debug_assert_eq!(stage.bases.len(), directories[fi].len());
                for &base in &stage.bases {
                    write_u32(&mut w, base)?;
                }
                write_u32(&mut w, u32::try_from(total).expect("positions exceed u32"))?;
                let mut ordinals = std::fs::File::open(&stage.path)?;
                io::copy(&mut ordinals, &mut w)?;
            }
            for &(fi, _, total) in &sentences_offs {
                write_u32(&mut w, n_slots_u64 as u32)?;
                match self.fields[fi].sentence_stage.as_mut() {
                    Some(stage) => {
                        debug_assert_eq!(stage.bases.len() as u64, n_slots_u64);
                        for &base in &stage.bases {
                            write_u32(&mut w, base)?;
                        }
                        write_u32(&mut w, u32::try_from(total).expect("sentences exceed u32"))?;
                        stage.writer.flush()?;
                        let mut spans = std::fs::File::open(&stage.path)?;
                        io::copy(&mut spans, &mut w)?;
                    }
                    None => {
                        // Declared, no document arrived: an empty table.
                        for _ in 0..=n_slots_u64 {
                            write_u32(&mut w, 0)?;
                        }
                    }
                }
            }
            for column in &self.unsigned_integers {
                column.write(&mut w, n_slots as usize)?;
            }
            for column in &self.map_integers {
                column.write(&mut w, n_slots as u32)?;
            }
            for column in &self.map_unsigned_integers {
                column.write(&mut w, n_slots as u32)?;
            }
            if source_section.is_some() {
                self.sources.write(&mut w, n_slots as u32)?;
            }
            for &(fi, _, total) in &pos_lengths_offs {
                let stage = pos_length_stages[fi]
                    .as_ref()
                    .expect("lattice field has a stage");
                write_u32(&mut w, directories[fi].len() as u32)?;
                debug_assert_eq!(stage.bases.len(), directories[fi].len());
                for &base in &stage.bases {
                    write_u32(&mut w, base)?;
                }
                write_u32(
                    &mut w,
                    u32::try_from(total).expect("pos_lengths exceed u32"),
                )?;
                let mut lens = std::fs::File::open(&stage.path)?;
                io::copy(&mut lens, &mut w)?;
            }
            for &(fi, _, _) in &splade_offs {
                if let Some(splade) = self.fields[fi].splade.as_ref() {
                    splade.write(&mut w)?;
                }
            }
            w.flush()?;
        }
        finalize_v8(&tmp)?;
        std::fs::rename(&tmp, path)?;
        fsync_parent(path)?;
        std::fs::remove_dir_all(&self.dir)?;
        Ok(())
    }

    /// Merge the runs and assemble the v4 file at `path` (the pre-v5
    /// behavior, kept for benchmarking and migration checks).
    fn finish_v4(&mut self, path: &Path) -> io::Result<()> {
        if !self.sources.is_empty() {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "legacy writer cannot preserve protobuf sources",
            ));
        }
        if self.fields[0].positions {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "the v4 oracle format cannot carry token positions",
            ));
        }
        if self.fields[0].sentences {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "the v4 oracle format cannot carry sentence spans",
            ));
        }
        self.spill_run()?;
        self.texts.flush()?;

        // Pass 1: merge runs into the postings section body, collecting
        // the directory (term, absolute offset comes later, df).
        let postings_body = self.dir.join("postings.body");
        let mut directory: Vec<(String, u64, u32)> = Vec::new();
        {
            let mut out = io::BufWriter::new(std::fs::File::create(&postings_body)?);
            let mut heads: Vec<RunHead> = Vec::new();
            for run in &self.fields[0].runs {
                if let Some(head) = RunHead::open(run, false)? {
                    heads.push(head);
                }
            }
            let mut cursor = 0u64;
            while !heads.is_empty() {
                // Smallest term across run heads; runs are time-ordered,
                // and ids only grow, so draining matching heads in run
                // order keeps postings doc-ascending.
                let term = heads
                    .iter()
                    .map(|h| h.term.clone())
                    .min()
                    .expect("nonempty heads");
                let df: u32 = heads
                    .iter()
                    .filter(|h| h.term == term)
                    .map(|h| h.n_postings)
                    .sum();
                write_str(&mut out, &term)?;
                write_u32(&mut out, df)?;
                directory.push((term.clone(), cursor, df));
                cursor += 4 + term.len() as u64 + 4;
                let mut idx = 0;
                while idx < heads.len() {
                    if heads[idx].term == term {
                        cursor += heads[idx].copy_postings(&mut out)?;
                        if !heads[idx].advance()? {
                            heads.remove(idx);
                            continue;
                        }
                    }
                    idx += 1;
                }
            }
            out.flush()?;
        }
        let postings_body_len = std::fs::metadata(&postings_body)?.len();

        // Section geometry, identical to Bm25Store::write_to.
        let field = &self.fields[0];
        let n_slots = field.doc_lengths.len() as u64;
        let header_size = 8 + 8 + 8 * 4 + 4;
        let doc_lengths_off = header_size as u64;
        let texts_off = doc_lengths_off + 4 * n_slots;
        let text_index_off = texts_off + self.texts_bytes;
        let lineages_off = text_index_off + 12 * n_slots;
        let lineages_size: u64 = self
            .lineages
            .iter()
            .map(|l| if l.is_some() { 25 } else { 1 })
            .sum();
        let postings_off = lineages_off + lineages_size;
        let directory_off = postings_off + 4 + postings_body_len;

        let tmp = path.with_extension("bm25tmp");
        {
            let mut w = io::BufWriter::new(std::fs::File::create(&tmp)?);
            w.write_all(MAGIC_V4)?;
            write_u64(&mut w, field.total_length)?;
            write_u64(&mut w, texts_off)?;
            write_u64(&mut w, lineages_off)?;
            write_u64(&mut w, postings_off)?;
            write_u64(&mut w, directory_off)?;
            write_u32(&mut w, field.doc_lengths.len() as u32)?;
            for &len in &field.doc_lengths {
                write_u32(&mut w, len)?;
            }
            // texts: byte-copy of the spill (already section-encoded).
            let mut spill = std::fs::File::open(self.dir.join("texts.spill"))?;
            io::copy(&mut spill, &mut w)?;
            // text_index, from the in-memory length table.
            let mut cursor = texts_off;
            for &len in &self.text_lens {
                if len == u32::MAX {
                    write_u64(&mut w, 0)?;
                    write_u32(&mut w, u32::MAX)?;
                    cursor += 4;
                } else {
                    write_u64(&mut w, cursor + 4)?;
                    write_u32(&mut w, len)?;
                    cursor += 4 + len as u64;
                }
            }
            for lineage in &self.lineages {
                match lineage {
                    Some(l) => {
                        w.write_all(&[1u8])?;
                        write_u64(&mut w, l.parent_id)?;
                        write_u64(&mut w, l.group_id)?;
                        write_u32(&mut w, l.span_start)?;
                        write_u32(&mut w, l.span_end)?;
                    }
                    None => w.write_all(&[0u8])?,
                }
            }
            write_u32(&mut w, directory.len() as u32)?;
            let mut body = std::fs::File::open(&postings_body)?;
            io::copy(&mut body, &mut w)?;
            write_u32(&mut w, directory.len() as u32)?;
            let mut blob_off = 0u64; // relative to the term blob start (v4)
            for (term, rel_off, df) in &directory {
                write_u64(&mut w, postings_off + 4 + rel_off)?;
                write_u32(&mut w, *df)?;
                write_u32(
                    &mut w,
                    u32::try_from(blob_off).expect("term blob exceeds u32"),
                )?;
                write_u16(&mut w, term.len() as u16)?;
                blob_off += term.len() as u64;
            }
            for (term, _, _) in &directory {
                w.write_all(term.as_bytes())?;
            }
            w.flush()?;
            w.get_ref().sync_all()?;
        }
        std::fs::rename(&tmp, path)?;
        fsync_parent(path)?;
        std::fs::remove_dir_all(&self.dir)?;
        Ok(())
    }
}

/// The merge-time stage of one positional field's kind-7 section:
/// ordinals stream to a file in directory order as the merge emits
/// occurrences, and the per-term bases accumulate in heap (4 B per
/// term, never the memory ceiling the spill exists for).
struct PositionsStage {
    writer: io::BufWriter<std::fs::File>,
    path: PathBuf,
    bases: Vec<u32>,
    total: u64,
}

type RawSpillPosting = (u32, u32, Vec<u8>, Vec<u8>, Vec<u8>);

/// One run's read cursor for the merge: the current term group's header
/// plus a reader positioned at its postings.
struct RunHead {
    reader: io::BufReader<std::fs::File>,
    term: String,
    n_postings: u32,
    /// Whether each posting is followed by one ordinal per occurrence
    /// (a positional field's run encoding).
    positional: bool,
}

impl RunHead {
    fn open(path: &Path, positional: bool) -> io::Result<Option<Self>> {
        let reader = io::BufReader::new(std::fs::File::open(path)?);
        let mut head = Self {
            reader,
            term: String::new(),
            n_postings: 0,
            positional,
        };
        Ok(if head.advance()? { Some(head) } else { None })
    }

    /// Read the next term group header; false at end of run.
    fn advance(&mut self) -> io::Result<bool> {
        let mut len_buf = [0u8; 2];
        match self.reader.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => return Ok(false),
            Err(e) => return Err(e),
        }
        let term_len = u16::from_le_bytes(len_buf) as usize;
        let mut term = vec![0u8; term_len];
        self.reader.read_exact(&mut term)?;
        self.term = String::from_utf8(term)
            .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "invalid utf-8 in run"))?;
        let mut n = [0u8; 4];
        self.reader.read_exact(&mut n)?;
        self.n_postings = u32::from_le_bytes(n);
        Ok(true)
    }

    /// Copy this group's postings to `out` in the final v3 encoding
    /// (which is the run encoding), returning bytes written.
    fn copy_postings<W: Write>(&mut self, out: &mut W) -> io::Result<u64> {
        if self.positional {
            return Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "the v4 encoding cannot copy a positional run",
            ));
        }
        let mut written = 0u64;
        for _ in 0..self.n_postings {
            let mut fixed = [0u8; 12];
            self.reader.read_exact(&mut fixed)?;
            out.write_all(&fixed)?;
            let n_offsets = u32::from_le_bytes(fixed[8..12].try_into().expect("4 bytes"));
            let mut offsets = vec![0u8; 8 * n_offsets as usize];
            self.reader.read_exact(&mut offsets)?;
            out.write_all(&offsets)?;
            written += 12 + 8 * u64::from(n_offsets);
        }
        Ok(written)
    }

    /// Read one posting of the current group as `(doc_id, tf, raw offset
    /// bytes, raw ordinal bytes, raw pos_len bytes)` — the v5 merge splits the
    /// runs, and the run encoding's offset bytes are already the v5
    /// occurrence-run pairs, its ordinal bytes already the kind-7 entries
    /// (empty on a field without positions).
    fn next_posting_raw(&mut self) -> io::Result<RawSpillPosting> {
        let mut fixed = [0u8; 12];
        self.reader.read_exact(&mut fixed)?;
        let doc_id = u32::from_le_bytes(fixed[0..4].try_into().expect("4 bytes"));
        let tf = u32::from_le_bytes(fixed[4..8].try_into().expect("4 bytes"));
        let n_offsets = u32::from_le_bytes(fixed[8..12].try_into().expect("4 bytes")) as usize;
        let mut offsets = vec![0u8; 8 * n_offsets];
        self.reader.read_exact(&mut offsets)?;
        let mut ordinals = Vec::new();
        let mut pos_lengths = Vec::new();
        if self.positional {
            ordinals = vec![0u8; 4 * n_offsets];
            self.reader.read_exact(&mut ordinals)?;
            let mut flag = [0u8; 1];
            self.reader.read_exact(&mut flag)?;
            if flag[0] != 0 {
                pos_lengths = vec![0u8; 4 * n_offsets];
                self.reader.read_exact(&mut pos_lengths)?;
            }
        }
        Ok((doc_id, tf, offsets, ordinals, pos_lengths))
    }
}
