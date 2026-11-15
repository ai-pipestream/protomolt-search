//! Sorted run merging and final `.bm25` assembly for `SpillBuilder`.

use std::io::{self, Read, Write};
use std::path::{Path, PathBuf};

use super::super::codec::{skip_run_size, write_skip_run, SkipRunBuilder};
use super::super::format::write_u32;
use super::super::section_writer::{
    save_v8_atomic, write_block_max_directory, write_text_index_from_lens, write_v6v7_payload,
    FieldSectionDesc, V6PayloadParams, V6SectionSource,
};
#[cfg(doc)]
use super::super::Bm25Store;
use super::{FieldSpill, PostingDirectoryEntry, SpillBuilder};

fn build_field_desc<'a>(field: &'a FieldSpill, merged: &MergedFieldStage) -> FieldSectionDesc<'a> {
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
}

impl SpillBuilder {
    /// Merge the runs and assemble the `.bm25` file at `path` (atomically:
    /// write tmp, finalize v8 CRC integrity table, rename). The spill
    /// directory is removed on success. Writes v8-framed v6/v7,
    /// byte-identical to [`Bm25Store::save`] on the same corpus.
    pub fn finish(&mut self, path: &Path) -> io::Result<()> {
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
            .map(|(field, merged)| build_field_desc(field, merged))
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
        write_block_max_directory(
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
