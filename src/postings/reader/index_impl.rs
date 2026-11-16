//! [`Bm25Index`] and part-number trait surfaces of [`Bm25Reader`].

use super::Bm25Reader;
use crate::postings::codec::ImpactCursor;
use crate::postings::{Bm25Index, DocLineage, PostingCallback};

/// The reader itself scores as its body field (field 0) — the surface
/// every single-field caller uses; multi-field scoring goes through
/// [`Bm25Reader::field`].
impl Bm25Index for Bm25Reader {
    fn doc_count(&self) -> u64 {
        self.doc_count
    }
    fn total_doc_length(&self) -> u64 {
        self.field(0).total_doc_length()
    }
    fn doc_length(&self, doc_id: u32) -> u32 {
        self.field(0).doc_length(doc_id)
    }
    fn df(&self, term: &str) -> u32 {
        self.field(0).df(term)
    }
    fn for_each_posting(&self, term: &str, f: &mut PostingCallback) {
        self.field(0).for_each_posting(term, f);
    }
    fn for_each_doc_tf(&self, term: &str, f: &mut dyn FnMut(u32, u32)) {
        self.field(0).for_each_doc_tf(term, f);
    }
    fn posting_offsets(&self, term: &str, doc_id: u32) -> Vec<(u32, u32)> {
        self.field(0).posting_offsets(term, doc_id)
    }
    fn has_positions(&self) -> bool {
        self.field(0).has_positions()
    }
    fn posting_positions(&self, term: &str, doc_id: u32) -> Option<Vec<u32>> {
        self.field(0).posting_positions(term, doc_id)
    }
    fn has_pos_lengths(&self) -> bool {
        self.field(0).has_pos_lengths()
    }
    fn posting_pos_lengths(&self, term: &str, doc_id: u32) -> Option<Vec<u32>> {
        self.field(0).posting_pos_lengths(term, doc_id)
    }
    fn has_splade(&self) -> bool {
        self.field(0).has_splade()
    }
    fn score_splade_with_floor(
        &self,
        query_weights: &[(String, f32)],
        query_l1_slack: f32,
        initial_floor: f32,
        k: usize,
        admitted: &dyn Fn(u32) -> bool,
    ) -> (
        Vec<(u32, f32)>,
        f32,
        crate::turbo_bridge::splade::SpladeTraversalStats,
    ) {
        self.field(0).score_splade_with_floor(
            query_weights,
            query_l1_slack,
            initial_floor,
            k,
            admitted,
        )
    }
    fn has_sentences(&self) -> bool {
        self.field(0).has_sentences()
    }
    fn doc_sentences(&self, doc_id: u32) -> Option<Vec<(u32, u32)>> {
        self.field(0).doc_sentences(doc_id)
    }
    fn prefix_terms(&self, prefix: &str) -> Box<dyn Iterator<Item = String> + '_> {
        self.field(0).prefix_iter(prefix)
    }
    fn expand_prefix(&self, prefix: &str, cap: usize) -> Result<Vec<String>, usize> {
        self.field(0).expand_prefix(prefix, cap)
    }
    fn suggest_prefix(&self, prefix: &str, max_scan: usize) -> Result<Vec<(String, u32)>, usize> {
        self.field(0).suggest_prefix(prefix, max_scan)
    }
    fn impacts(&self, term: &str) -> Option<ImpactCursor<'_>> {
        self.field(0).impacts_inner(term)
    }
    fn has_impacts(&self, term: &str) -> bool {
        self.field(0).has_impacts(term)
    }
    fn text(&self, doc_id: u32) -> Option<String> {
        self.read_text(doc_id)
    }
    fn lineage(&self, doc_id: u32) -> Option<DocLineage> {
        self.read_lineage(doc_id)
    }
}

/// A sealed part answers a range from its column sorted by value.
impl crate::filter_bits::PartNumbers for Bm25Reader {
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
    fn int_sorted(&self, column: usize) -> Option<&crate::number_rows::NumberRows<i64>> {
        (column < self.integer_count()).then(|| self.integer_rows(column))
    }
    fn uint_sorted(&self, column: usize) -> Option<&crate::number_rows::NumberRows<u64>> {
        (column < self.unsigned_integer_count()).then(|| self.unsigned_integer_rows(column))
    }
    fn f64_sorted(&self, column: usize) -> Option<&crate::number_rows::NumberRows<f64>> {
        (column < self.numeric_count()).then(|| self.numeric_rows(column))
    }
}
