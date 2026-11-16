//! The shard scored as its body field, whole or over a subset of its parts.

use super::*;

/// The shard scores as its body field (field 0), like the other stores.
impl Bm25Index for SegmentedShard {
    fn doc_count(&self) -> u64 {
        self.field(0).doc_count()
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
        self.field(0).for_each_posting(term, f)
    }
    fn for_each_doc_tf(&self, term: &str, f: &mut dyn FnMut(u32, u32)) {
        self.field(0).for_each_doc_tf(term, f)
    }
    fn posting_offsets(&self, term: &str, doc_id: u32) -> Vec<(u32, u32)> {
        self.field(0).posting_offsets(term, doc_id)
    }
    fn impacts(&self, term: &str) -> Option<ImpactCursor<'_>> {
        // The chain borrows the sealed readers, which outlive the view.
        let parts: Vec<(u32, u32, FileImpacts<'_>)> = if self.field(0).heap_df(term) > 0 {
            Vec::new()
        } else {
            self.parts
                .iter()
                .enumerate()
                .filter_map(|(i, p)| {
                    self.reader(i)
                        .field(0)
                        .file_impacts(term)
                        .map(|c| (p.base, p.rows, c))
                })
                .collect()
        };
        if parts.is_empty() {
            None
        } else {
            Some(ImpactCursor::chain(parts))
        }
    }
    fn has_impacts(&self, term: &str) -> bool {
        self.field(0).has_impacts(term)
    }
    fn text(&self, doc_id: u32) -> Option<String> {
        SegmentedShard::text(self, doc_id)
    }
    fn lineage(&self, doc_id: u32) -> Option<DocLineage> {
        SegmentedShard::lineage(self, doc_id)
    }
    fn has_positions(&self) -> bool {
        self.field_has_positions(0)
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
        self.field_has_sentences(0)
    }
    fn doc_sentences(&self, doc_id: u32) -> Option<Vec<(u32, u32)>> {
        self.field_doc_sentences(0, doc_id)
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
}

/// A [`SegmentedShard`] scored as its body field over a subset of its
/// sealed parts. Every walk (postings,
/// document frequency, impacts, dictionary scans) covers the admitted
/// parts and the heaps; every read by document id covers the whole
/// shard, so a surviving hit resolves its text, offsets, and positions
/// as usual.
pub struct MaskedShard<'a> {
    pub(super) shard: &'a SegmentedShard,
    pub(super) mask: Arc<[bool]>,
}

impl MaskedShard<'_> {
    fn body(&self) -> UnionField<'_> {
        self.shard.field_masked(0, Arc::clone(&self.mask))
    }
}

impl Bm25Index for MaskedShard<'_> {
    fn doc_count(&self) -> u64 {
        Bm25Index::doc_count(self.shard)
    }
    fn total_doc_length(&self) -> u64 {
        Bm25Index::total_doc_length(self.shard)
    }
    fn doc_length(&self, doc_id: u32) -> u32 {
        Bm25Index::doc_length(self.shard, doc_id)
    }
    fn df(&self, term: &str) -> u32 {
        self.body().df(term)
    }
    fn for_each_posting(&self, term: &str, f: &mut PostingCallback) {
        self.body().for_each_posting(term, f)
    }
    fn for_each_doc_tf(&self, term: &str, f: &mut dyn FnMut(u32, u32)) {
        self.body().for_each_doc_tf(term, f)
    }
    fn posting_offsets(&self, term: &str, doc_id: u32) -> Vec<(u32, u32)> {
        Bm25Index::posting_offsets(self.shard, term, doc_id)
    }
    fn impacts(&self, term: &str) -> Option<ImpactCursor<'_>> {
        // The chain borrows the sealed readers, which outlive the view.
        if self.shard.field(0).heap_df(term) > 0 {
            return None;
        }
        let parts: Vec<(u32, u32, FileImpacts<'_>)> = self
            .shard
            .parts
            .iter()
            .enumerate()
            .filter(|(i, _)| self.mask[*i])
            .filter_map(|(i, p)| {
                self.shard
                    .reader(i)
                    .field(0)
                    .file_impacts(term)
                    .map(|c| (p.base, p.rows, c))
            })
            .collect();
        if parts.is_empty() {
            None
        } else {
            Some(ImpactCursor::chain(parts))
        }
    }
    fn has_impacts(&self, term: &str) -> bool {
        self.body().has_impacts(term)
    }
    fn text(&self, doc_id: u32) -> Option<String> {
        SegmentedShard::text(self.shard, doc_id)
    }
    fn lineage(&self, doc_id: u32) -> Option<DocLineage> {
        SegmentedShard::lineage(self.shard, doc_id)
    }
    fn has_positions(&self) -> bool {
        self.shard.field_has_positions(0)
    }
    fn posting_positions(&self, term: &str, doc_id: u32) -> Option<Vec<u32>> {
        Bm25Index::posting_positions(self.shard, term, doc_id)
    }
    fn has_pos_lengths(&self) -> bool {
        self.body().has_pos_lengths()
    }
    fn posting_pos_lengths(&self, term: &str, doc_id: u32) -> Option<Vec<u32>> {
        Bm25Index::posting_pos_lengths(self.shard, term, doc_id)
    }
    fn has_splade(&self) -> bool {
        self.body().has_splade()
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
        self.body().score_splade_with_floor(
            query_weights,
            query_l1_slack,
            initial_floor,
            k,
            admitted,
        )
    }
    fn has_sentences(&self) -> bool {
        self.shard.field_has_sentences(0)
    }
    fn doc_sentences(&self, doc_id: u32) -> Option<Vec<(u32, u32)>> {
        self.shard.field_doc_sentences(0, doc_id)
    }
    fn prefix_terms(&self, prefix: &str) -> Box<dyn Iterator<Item = String> + '_> {
        self.body().prefix_iter(prefix)
    }
    fn expand_prefix(&self, prefix: &str, cap: usize) -> Result<Vec<String>, usize> {
        self.body().expand_prefix(prefix, cap)
    }
    fn suggest_prefix(&self, prefix: &str, max_scan: usize) -> Result<Vec<(String, u32)>, usize> {
        self.body().suggest_prefix(prefix, max_scan)
    }
}
