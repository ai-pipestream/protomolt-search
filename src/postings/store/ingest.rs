//! Appending analyzed documents to the heap store.

use super::{Bm25Store, FieldStore};
use crate::postings::{clone_parallel_ordinals, AnalyzedDoc, AnalyzedField, DocLineage, Posting};

impl Bm25Store {
    /// Append one analyzed document with the given local doc id.
    ///
    /// `doc_id` must be `>= next_doc_id()` (append-only); ids above the
    /// current tip create sparse slots, which is how the vector and
    /// document sides share one positional id space.
    pub fn add_document(&mut self, doc_id: u32, text: String, doc: AnalyzedDoc) {
        self.add_document_with_lineage(doc_id, text, doc, None);
    }

    /// Like [`Self::add_document`], with corpus lineage attached.
    pub fn add_document_with_lineage(
        &mut self,
        doc_id: u32,
        text: String,
        doc: AnalyzedDoc,
        lineage: Option<DocLineage>,
    ) {
        let slot = doc_id as usize;
        assert!(slot >= self.texts.len(), "doc id {doc_id} already used");
        assert!(
            doc.fields.len() <= self.fields.len(),
            "document carries {} fields, store has {}",
            doc.fields.len(),
            self.fields.len()
        );
        for field in &mut self.fields {
            field.doc_lengths.resize(slot + 1, 0);
            if field.sentences {
                field.doc_sentences.resize_with(slot + 1, Vec::new);
            }
        }
        self.texts.resize_with(slot + 1, || None);
        self.lineages.resize_with(slot + 1, || None);
        self.texts[slot] = Some(text);
        self.lineages[slot] = lineage;
        for (fi, analyzed) in doc.fields.into_iter().enumerate() {
            self.fields[fi].record_analyzed(doc_id, analyzed);
        }
    }
}

impl FieldStore {
    fn record_analyzed(&mut self, doc_id: u32, analyzed: AnalyzedField) {
        let slot = doc_id as usize;
        self.doc_lengths[slot] = analyzed.length;
        self.total_length += u64::from(analyzed.length);
        if self.sentences {
            self.doc_sentences[slot] = analyzed.sentences.unwrap_or_else(|| {
                panic!(
                    "field {:?} keeps sentence spans but the document carried none",
                    self.name
                )
            });
        }
        let positions = if self.positions {
            Some(analyzed.positions.unwrap_or_else(|| {
                panic!(
                    "field {:?} keeps positions but the document carried none",
                    self.name
                )
            }))
        } else {
            None
        };
        let pos_lengths = if self.positions {
            if analyzed.pos_lengths.is_some() {
                self.has_lattice = true;
            }
            analyzed.pos_lengths
        } else {
            None
        };
        if !analyzed.splade.is_empty() {
            self.splade.get_or_insert_with(Default::default).insert_doc(
                doc_id,
                analyzed.splade_scale_bits,
                &analyzed.splade,
            );
        }
        for (ti, (term, tf, offsets)) in analyzed.terms.into_iter().enumerate() {
            let positions = clone_parallel_ordinals(&positions, ti, offsets.len(), "positions");
            let pos_lengths =
                clone_parallel_ordinals(&pos_lengths, ti, offsets.len(), "pos_lengths");
            self.postings.entry(term).or_default().push(Posting {
                doc_id,
                tf,
                offsets,
                positions,
                pos_lengths,
            });
        }
    }
}

/// A part still being written is read forward.
impl crate::filter_bits::PartNumbers for Bm25Store {
    fn int_value(&self, column: usize, row: u32) -> Option<i64> {
        (column < self.integer_count())
            .then(|| self.integer_value(column, row))
            .flatten()
    }
    fn uint_value(&self, column: usize, row: u32) -> Option<u64> {
        (column < self.unsigned_integer_count())
            .then(|| self.unsigned_integer_value(column, row))
            .flatten()
    }
    fn f64_value(&self, column: usize, row: u32) -> Option<f64> {
        (column < self.numeric_count())
            .then(|| self.numeric_value(column, row))
            .flatten()
    }
}
