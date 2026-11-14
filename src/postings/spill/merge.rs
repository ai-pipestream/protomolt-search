//! Sorted run merging and final `.bm25` assembly for `SpillBuilder`.

use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use super::super::codec::{skip_run_size, write_skip_run, SkipRunBuilder};
use super::super::format::{fsync_parent, write_str, write_u16, write_u32, write_u64, MAGIC_V4};
use super::super::section_writer::{
    save_v8_atomic, write_lineages, write_text_index_from_lens, write_v5_directory,
    write_v6v7_payload, FieldSectionDesc, V6PayloadParams, V6SectionSource,
};
#[cfg(doc)]
use super::super::Bm25Store;
use super::{PostingDirectoryEntry, SpillBuilder};

impl SpillBuilder {
    /// Merge the runs and assemble the file at `path` (atomically:
    /// write tmp, rename). The spill directory is removed on success.
    /// Writes v8-framed v6/v7, byte-identical to [`Bm25Store::save`] on the
    /// same corpus, unless built with [`Self::create_v4_for_bench`].
    pub fn finish(&mut self, path: &Path) -> io::Result<()> {
        if self.v4_only {
            if self.columns.binding.is_some() || self.columns.derived.is_some() {
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

    /// Merge one field's runs into a postings-section body file at `body_path`
    /// (per-term doc/occurrence/skip runs, without the leading `u32 n_terms`),
    /// returning `(term, doc_rel, skip_rel, occ_rel, df)` relative to body start.
    fn merge_field_runs(
        runs: &[PathBuf],
        doc_lengths: &[u32],
        paths: (&Path, &Path),
        stages: (Option<&mut PositionsStage>, Option<&mut PositionsStage>),
    ) -> io::Result<Vec<PostingDirectoryEntry>> {
        let (body_path, occ_stage_path) = paths;
        let (mut positions, mut pos_lengths) = stages;
        let mut directory: Vec<PostingDirectoryEntry> = Vec::new();
        let mut out = io::BufWriter::new(std::fs::File::create(body_path)?);
        let mut heads: Vec<RunHead> = Vec::new();
        let positional = positions.is_some();
        for run in runs {
            if let Some(head) = RunHead::open(run, positional)? {
                heads.push(head);
            }
        }
        let mut cursor = 0u64;
        while !heads.is_empty() {
            let term = heads
                .iter()
                .map(|h| h.term.clone())
                .min()
                .expect("nonempty heads");
            let doc_rel = cursor;
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
            let mut skip_l0: Vec<u8> = Vec::new();
            let mut skip = SkipRunBuilder::new();
            let (df, occ_start, occ_bytes) = stage_term_postings(
                &mut heads,
                &term,
                doc_lengths,
                (&mut out, occ_stage_path),
                (&mut skip, &mut skip_l0, &mut positions, &mut pos_lengths),
            )?;
            write_u32(&mut out, occ_start)?;
            io::copy(&mut std::fs::File::open(occ_stage_path)?, &mut out)?;
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

    fn merge_all_field_stages(&self) -> io::Result<Vec<MergedFieldStage>> {
        let occ_stage_path = self.dir.join("occ.stage");
        let mut merged_fields = Vec::with_capacity(self.fields.len());
        for (fi, field) in self.fields.iter().enumerate() {
            let body_path = self.dir.join(format!("postings-f{fi:03}.body"));
            let mut pos_stage = field
                .positions
                .then(|| PositionsStage::create(self.dir.join(format!("positions-f{fi:03}.stage"))))
                .transpose()?;
            let mut len_stage = (field.positions && field.has_lattice)
                .then(|| {
                    PositionsStage::create(self.dir.join(format!("pos-lengths-f{fi:03}.stage")))
                })
                .transpose()?;
            let directory = Self::merge_field_runs(
                &field.runs,
                &field.doc_lengths,
                (&body_path, &occ_stage_path),
                (pos_stage.as_mut(), len_stage.as_mut()),
            )?;
            if let Some(stage) = pos_stage.as_mut() {
                stage.writer.flush()?;
            }
            if let Some(stage) = len_stage.as_mut() {
                stage.writer.flush()?;
            }
            let body_len = std::fs::metadata(&body_path)?.len();
            merged_fields.push(MergedFieldStage {
                body_path,
                body_len,
                directory,
                positions: pos_stage,
                pos_lengths: len_stage,
            });
        }
        Ok(merged_fields)
    }

    fn finish_v6(&mut self, path: &Path) -> io::Result<()> {
        self.spill_run()?;
        self.texts.flush()?;
        for field in &mut self.fields {
            if let Some(stage) = field.sentence_stage.as_mut() {
                stage.writer.flush()?;
            }
        }
        let merged_fields = self.merge_all_field_stages()?;
        let field_descs: Vec<FieldSectionDesc<'_>> = self
            .fields
            .iter()
            .zip(&merged_fields)
            .map(|(field, merged)| {
                let directory_size = 4
                    + 34 * merged.directory.len() as u64
                    + merged
                        .directory
                        .iter()
                        .map(|(t, ..)| t.len() as u64)
                        .sum::<u64>();
                FieldSectionDesc {
                    name: &field.name,
                    analysis_fingerprint: field.analysis_fingerprint,
                    total_length: field.total_length,
                    doc_lengths: &field.doc_lengths,
                    postings_size: 4 + merged.body_len,
                    directory_size,
                    n_terms: merged.directory.len() as u64,
                    positions_total: merged.positions.as_ref().map(|s| s.total),
                    sentences_total: field
                        .sentences
                        .then(|| field.sentence_stage.as_ref().map_or(0, |s| s.total)),
                    pos_lengths_total: merged.pos_lengths.as_ref().map(|s| s.total),
                    splade: field.splade.as_ref().filter(|s| !s.is_empty()),
                }
            })
            .collect();
        let params = V6PayloadParams {
            n_slots: self.fields[0].doc_lengths.len() as u64,
            texts_size: self.texts_bytes,
            lineages: &self.lineages,
            fields: &field_descs,
            columns: &self.columns,
            sources: &self.sources,
            byte_order: true,
        };
        let mut source = SpillSectionSource {
            builder: self,
            merged: &merged_fields,
        };
        save_v8_atomic(path, |w| write_v6v7_payload(w, &params, &mut source))?;
        std::fs::remove_dir_all(&self.dir)
    }

    fn merge_v4_runs(
        runs: &[PathBuf],
        postings_body: &Path,
    ) -> io::Result<Vec<(String, u64, u32)>> {
        let mut directory: Vec<(String, u64, u32)> = Vec::new();
        let mut out = io::BufWriter::new(std::fs::File::create(postings_body)?);
        let mut heads: Vec<RunHead> = Vec::new();
        for run in runs {
            if let Some(head) = RunHead::open(run, false)? {
                heads.push(head);
            }
        }
        let mut cursor = 0u64;
        while !heads.is_empty() {
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
        Ok(directory)
    }

    fn validate_v4_spill(&self) -> io::Result<()> {
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
        Ok(())
    }

    /// Merge the runs and assemble the v4 file at `path` (benchmarking/migration only).
    fn finish_v4(&mut self, path: &Path) -> io::Result<()> {
        self.validate_v4_spill()?;
        self.spill_run()?;
        self.texts.flush()?;

        let postings_body = self.dir.join("postings.body");
        let directory = Self::merge_v4_runs(&self.fields[0].runs, &postings_body)?;
        let postings_body_len = std::fs::metadata(&postings_body)?.len();

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
            io::copy(
                &mut std::fs::File::open(self.dir.join("texts.spill"))?,
                &mut w,
            )?;
            write_text_index_from_lens(&mut w, texts_off, self.text_lens.iter().copied())?;
            write_lineages(&mut w, &self.lineages)?;
            write_u32(&mut w, directory.len() as u32)?;
            io::copy(&mut std::fs::File::open(&postings_body)?, &mut w)?;
            write_u32(&mut w, directory.len() as u32)?;
            let mut blob_off = 0u64;
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

struct MergedFieldStage {
    body_path: PathBuf,
    body_len: u64,
    directory: Vec<PostingDirectoryEntry>,
    positions: Option<PositionsStage>,
    pos_lengths: Option<PositionsStage>,
}

struct SpillSectionSource<'a> {
    builder: &'a SpillBuilder,
    merged: &'a [MergedFieldStage],
}

impl V6SectionSource for SpillSectionSource<'_> {
    fn write_texts<W: Write>(&mut self, w: &mut W) -> io::Result<()> {
        let mut spill = std::fs::File::open(self.builder.dir.join("texts.spill"))?;
        io::copy(&mut spill, w)?;
        Ok(())
    }

    fn write_text_index<W: Write>(&mut self, w: &mut W) -> io::Result<()> {
        write_text_index_from_lens(w, 0, self.builder.text_lens.iter().copied())
    }

    fn write_field_postings_and_directory<W: Write>(
        &mut self,
        w: &mut W,
        fi: usize,
    ) -> io::Result<()> {
        let stage = &self.merged[fi];
        write_u32(w, stage.directory.len() as u32)?;
        let mut body = std::fs::File::open(&stage.body_path)?;
        io::copy(&mut body, w)?;
        write_v5_directory(
            w,
            stage.directory.len(),
            stage
                .directory
                .iter()
                .map(|(t, doc, skip, occ, df)| (t.as_str(), 4 + *doc, 4 + *skip, 4 + *occ, *df)),
        )
    }

    fn write_field_positions<W: Write>(
        &mut self,
        w: &mut W,
        fi: usize,
        total: u64,
    ) -> io::Result<()> {
        let stage = self.merged[fi]
            .positions
            .as_ref()
            .expect("positional field has a stage");
        write_positions_stage(
            w,
            stage,
            self.merged[fi].directory.len(),
            total,
            "positions",
        )
    }

    fn write_field_sentences<W: Write>(
        &mut self,
        w: &mut W,
        fi: usize,
        n_slots: u64,
        total: u64,
    ) -> io::Result<()> {
        write_u32(w, n_slots as u32)?;
        match self.builder.fields[fi].sentence_stage.as_ref() {
            Some(stage) => {
                debug_assert_eq!(stage.bases.len() as u64, n_slots);
                for &base in &stage.bases {
                    write_u32(w, base)?;
                }
                write_u32(w, u32::try_from(total).expect("sentences exceed u32"))?;
                let mut spans = std::fs::File::open(&stage.path)?;
                io::copy(&mut spans, w)?;
                Ok(())
            }
            None => {
                for _ in 0..=n_slots {
                    write_u32(w, 0)?;
                }
                Ok(())
            }
        }
    }

    fn write_field_pos_lengths<W: Write>(
        &mut self,
        w: &mut W,
        fi: usize,
        total: u64,
    ) -> io::Result<()> {
        let stage = self.merged[fi]
            .pos_lengths
            .as_ref()
            .expect("lattice field has a stage");
        write_positions_stage(
            w,
            stage,
            self.merged[fi].directory.len(),
            total,
            "pos_lengths",
        )
    }
}

fn write_positions_stage<W: Write>(
    w: &mut W,
    stage: &PositionsStage,
    n_terms: usize,
    total: u64,
    label: &str,
) -> io::Result<()> {
    write_u32(w, n_terms as u32)?;
    debug_assert_eq!(stage.bases.len(), n_terms);
    for &base in &stage.bases {
        write_u32(w, base)?;
    }
    write_u32(
        w,
        u32::try_from(total).unwrap_or_else(|_| panic!("{label} exceed u32")),
    )?;
    let mut file = std::fs::File::open(&stage.path)?;
    io::copy(&mut file, w)?;
    Ok(())
}

/// Merge-time stage of one positional field's kind-7 or kind-18 section.
struct PositionsStage {
    writer: io::BufWriter<std::fs::File>,
    path: PathBuf,
    bases: Vec<u32>,
    total: u64,
}

impl PositionsStage {
    fn create(path: PathBuf) -> io::Result<Self> {
        Ok(Self {
            writer: io::BufWriter::new(std::fs::File::create(&path)?),
            path,
            bases: Vec::new(),
            total: 0,
        })
    }
}

type RawSpillPosting = (u32, u32, Vec<u8>, Vec<u8>, Vec<u8>);

/// One run's read cursor for the merge: the current term group's header
/// plus a reader positioned at its postings.
struct RunHead {
    reader: io::BufReader<std::fs::File>,
    term: String,
    n_postings: u32,
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

type TermMergeState<'a, 'b> = (
    &'a mut SkipRunBuilder,
    &'a mut Vec<u8>,
    &'a mut Option<&'b mut PositionsStage>,
    &'a mut Option<&'b mut PositionsStage>,
);

fn stage_term_postings<W: Write>(
    heads: &mut Vec<RunHead>,
    term: &str,
    doc_lengths: &[u32],
    io_targets: (&mut W, &Path),
    state: TermMergeState<'_, '_>,
) -> io::Result<(u64, u32, u64)> {
    let (out, occ_stage_path) = io_targets;
    let (skip, skip_l0, positions, pos_lengths) = state;
    let mut occ_stage = io::BufWriter::new(std::fs::File::create(occ_stage_path)?);
    let mut df = 0u64;
    let mut occ_start = 0u32;
    let mut occ_bytes = 0u64;
    let mut idx = 0;
    while idx < heads.len() {
        if heads[idx].term == term {
            for _ in 0..heads[idx].n_postings {
                let (doc_id, tf, offsets, ordinals, pos_lens) = heads[idx].next_posting_raw()?;
                let dl = doc_lengths[doc_id as usize];
                write_u32(out, doc_id)?;
                write_u32(out, tf)?;
                write_u32(out, occ_start)?;
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
                skip.push(tf, dl, doc_id, skip_l0)?;
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
    Ok((df, occ_start, occ_bytes))
}
