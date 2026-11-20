//! Document ingest and sorted-run spilling for [`SpillBuilder`].

use std::io::{self, Write};

use super::{FieldSpill, SentencesStage, SpillBuilder};
use crate::postings::format::{write_u16, write_u32};
#[cfg(doc)]
use crate::postings::Bm25Store;
use crate::postings::{clone_parallel_ordinals, AnalyzedDoc, AnalyzedField, DocLineage};

impl SpillBuilder {
    /// Append one analyzed document; same contract as
    /// [`Bm25Store::add_document_with_lineage`].
    pub fn add_document_with_lineage(
        &mut self,
        doc_id: u32,
        text: String,
        doc: AnalyzedDoc,
        lineage: Option<DocLineage>,
    ) -> io::Result<()> {
        assert!(
            doc.fields.len() <= self.fields.len(),
            "document carries {} fields, builder has {}",
            doc.fields.len(),
            self.fields.len()
        );
        let slot = doc_id as usize;
        assert!(
            slot >= self.fields[0].doc_lengths.len(),
            "doc id {doc_id} already used"
        );
        self.advance_to_slot(slot, &text, lineage)?;
        let mut lengths = vec![0u32; self.fields.len()];
        for (fi, analyzed) in doc.fields.into_iter().enumerate() {
            lengths[fi] = analyzed.length;
            self.buf_bytes += Self::append_analyzed_field(&mut self.fields[fi], doc_id, analyzed)?;
        }
        if lengths.iter().any(|&l| l > 0) {
            self.doc_count += 1;
        }
        for (field, &length) in self.fields.iter_mut().zip(&lengths) {
            field.doc_lengths.push(length);
        }
        if self.buf_bytes >= self.cap_bytes {
            self.spill_run()?;
        }
        Ok(())
    }

    /// Allocate empty slots up to `slots`, so the store covers rows that
    /// carry no document (a vector-only tail of a reshard child). Empty
    /// slots hold no text and count toward no statistic.
    pub fn reserve_slots(&mut self, slots: u32) -> io::Result<()> {
        self.fill_gaps_to(slots as usize)
    }

    fn advance_to_slot(
        &mut self,
        slot: usize,
        text: &str,
        lineage: Option<DocLineage>,
    ) -> io::Result<()> {
        self.fill_gaps_to(slot)?;
        write_u32(&mut self.texts, text.len() as u32)?;
        self.texts.write_all(text.as_bytes())?;
        self.texts_bytes += 4 + text.len() as u64;
        self.text_lens.push(text.len() as u32);
        self.lineages.push(lineage);
        for field in &mut self.fields {
            if let Some(stage) = field.sentence_stage.as_mut() {
                stage
                    .bases
                    .push(u32::try_from(stage.total).expect("sentences exceed u32"));
            }
        }
        Ok(())
    }

    /// Open the sentence stages, then push empty slots until `slot` is the
    /// next one.
    fn fill_gaps_to(&mut self, slot: usize) -> io::Result<()> {
        for (fi, field) in self.fields.iter_mut().enumerate() {
            if field.sentences && field.sentence_stage.is_none() {
                let path = self.dir.join(format!("sentences-f{fi:03}.stage"));
                field.sentence_stage = Some(SentencesStage {
                    writer: io::BufWriter::new(std::fs::File::create(&path)?),
                    path,
                    bases: Vec::new(),
                    total: 0,
                });
            }
        }
        while self.fields[0].doc_lengths.len() < slot {
            write_u32(&mut self.texts, u32::MAX)?;
            self.texts_bytes += 4;
            self.text_lens.push(u32::MAX);
            for field in &mut self.fields {
                field.doc_lengths.push(0);
                if let Some(stage) = field.sentence_stage.as_mut() {
                    stage
                        .bases
                        .push(u32::try_from(stage.total).expect("sentences exceed u32"));
                }
            }
            self.lineages.push(None);
        }
        Ok(())
    }

    fn append_analyzed_field(
        field: &mut FieldSpill,
        doc_id: u32,
        analyzed: AnalyzedField,
    ) -> io::Result<usize> {
        field.total_length += u64::from(analyzed.length);
        if let Some(stage) = field.sentence_stage.as_mut() {
            let spans = analyzed.sentences.as_deref().unwrap_or_else(|| {
                panic!(
                    "field {:?} keeps sentence spans but the document carried none",
                    field.name
                )
            });
            for &(start, end) in spans {
                write_u32(&mut stage.writer, start)?;
                write_u32(&mut stage.writer, end)?;
            }
            stage.total += spans.len() as u64;
        }
        if !analyzed.splade.is_empty() {
            field
                .splade
                .get_or_insert_with(crate::turbo_bridge::splade::SpladeFieldIndex::default)
                .insert_doc(doc_id, analyzed.splade_scale_bits, &analyzed.splade);
        }
        let positions = if field.positions {
            Some(analyzed.positions.unwrap_or_else(|| {
                panic!(
                    "field {:?} keeps positions but the document carried none",
                    field.name
                )
            }))
        } else {
            None
        };
        let pos_lengths = if field.positions {
            if analyzed.pos_lengths.is_some() {
                field.has_lattice = true;
            }
            analyzed.pos_lengths
        } else {
            None
        };
        let mut added_bytes = 0usize;
        for (ti, (term, tf, offsets)) in analyzed.terms.into_iter().enumerate() {
            let positions = clone_parallel_ordinals(&positions, ti, offsets.len(), "positions");
            let pos_lengths =
                clone_parallel_ordinals(&pos_lengths, ti, offsets.len(), "pos_lengths");
            added_bytes +=
                term.len() + 24 + 16 * offsets.len() + 4 * positions.len() + 4 * pos_lengths.len();
            field
                .buf
                .push((term, doc_id, tf, offsets, positions, pos_lengths));
        }
        Ok(added_bytes)
    }

    /// Sort every field's buffer by `(term, doc_id)` and write each as
    /// that field's next run.
    pub(super) fn spill_run(&mut self) -> io::Result<()> {
        for (fi, field) in self.fields.iter_mut().enumerate() {
            if field.buf.is_empty() {
                continue;
            }
            field
                .buf
                .sort_unstable_by(|a, b| a.0.cmp(&b.0).then(a.1.cmp(&b.1)));
            let path = self
                .dir
                .join(format!("run-f{fi:03}-{:06}", field.runs.len()));
            let mut w = io::BufWriter::new(std::fs::File::create(&path)?);
            let mut i = 0;
            while i < field.buf.len() {
                let term = &field.buf[i].0;
                let group_end = field.buf[i..]
                    .iter()
                    .position(|e| &e.0 != term)
                    .map_or(field.buf.len(), |p| i + p);
                write_u16(&mut w, term.len() as u16)?;
                w.write_all(term.as_bytes())?;
                write_u32(&mut w, (group_end - i) as u32)?;
                for (_, doc_id, tf, offsets, positions, pos_lengths) in &field.buf[i..group_end] {
                    write_u32(&mut w, *doc_id)?;
                    write_u32(&mut w, *tf)?;
                    write_u32(&mut w, offsets.len() as u32)?;
                    for &(start, end) in offsets {
                        write_u32(&mut w, start)?;
                        write_u32(&mut w, end)?;
                    }
                    if field.positions {
                        debug_assert_eq!(positions.len(), offsets.len());
                        for &ordinal in positions {
                            write_u32(&mut w, ordinal)?;
                        }
                        if pos_lengths.is_empty() {
                            w.write_all(&[0u8])?;
                        } else {
                            debug_assert_eq!(pos_lengths.len(), offsets.len());
                            w.write_all(&[1u8])?;
                            for &len in pos_lengths {
                                write_u32(&mut w, len)?;
                            }
                        }
                    }
                }
                i = group_end;
            }
            w.flush()?;
            field.runs.push(path);
            field.buf.clear();
        }
        self.buf_bytes = 0;
        Ok(())
    }
}
