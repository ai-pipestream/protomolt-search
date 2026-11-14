//! `Bm25Index` read views over heap-resident `Bm25Store` fields.

use super::super::{Bm25Index, DocLineage, PostingCallback};
use super::{Bm25Store, FieldStore};

/// One field of a [`Bm25Store`] as its own [`Bm25Index`]
/// (`docs/multi-field.md`): postings, document lengths, and total
/// length come from the field; texts, lineages, and the document count
/// are the store's shared ones (a document is a document — idf's N is
/// corpus-wide, never per field).
pub struct StoreFieldView<'a> {
    pub(super) store: &'a Bm25Store,
    pub(super) field: &'a FieldStore,
}

impl<'a> StoreFieldView<'a> {
    pub(crate) fn prefix_iter(&self, prefix: &str) -> Box<dyn Iterator<Item = String> + 'a> {
        let prefix = prefix.to_owned();
        Box::new(
            self.field
                .postings
                .range(prefix.clone()..)
                .take_while(move |(term, _)| term.starts_with(&prefix))
                .map(|(term, _)| term.clone()),
        )
    }
}

impl Bm25Index for StoreFieldView<'_> {
    fn doc_count(&self) -> u64 {
        self.store.doc_count()
    }
    fn total_doc_length(&self) -> u64 {
        self.field.total_length
    }
    fn doc_length(&self, doc_id: u32) -> u32 {
        self.field
            .doc_lengths
            .get(doc_id as usize)
            .copied()
            .unwrap_or(0)
    }
    fn df(&self, term: &str) -> u32 {
        self.field.postings.get(term).map_or(0, |p| p.len() as u32)
    }
    fn for_each_posting(&self, term: &str, f: &mut PostingCallback) {
        if let Some(postings) = self.field.postings.get(term) {
            for p in postings {
                f(p.doc_id, p.tf, &p.offsets);
            }
        }
    }
    fn for_each_doc_tf(&self, term: &str, f: &mut dyn FnMut(u32, u32)) {
        if let Some(postings) = self.field.postings.get(term) {
            for p in postings {
                f(p.doc_id, p.tf);
            }
        }
    }
    fn posting_offsets(&self, term: &str, doc_id: u32) -> Vec<(u32, u32)> {
        let Some(postings) = self.field.postings.get(term) else {
            return Vec::new();
        };
        match postings.binary_search_by_key(&doc_id, |p| p.doc_id) {
            Ok(i) => postings[i].offsets.clone(),
            Err(_) => Vec::new(),
        }
    }
    fn text(&self, doc_id: u32) -> Option<String> {
        self.store.text(doc_id).map(str::to_string)
    }
    fn lineage(&self, doc_id: u32) -> Option<DocLineage> {
        self.store.lineage(doc_id)
    }
    fn has_positions(&self) -> bool {
        self.field.positions
    }
    fn posting_positions(&self, term: &str, doc_id: u32) -> Option<Vec<u32>> {
        let postings = self.field.postings.get(term)?;
        postings
            .binary_search_by_key(&doc_id, |p| p.doc_id)
            .ok()
            .map(|i| postings[i].positions.clone())
    }
    fn has_pos_lengths(&self) -> bool {
        self.field.positions && self.field.has_lattice
    }
    fn posting_pos_lengths(&self, term: &str, doc_id: u32) -> Option<Vec<u32>> {
        if !(self.field.positions && self.field.has_lattice) {
            return None;
        }
        let postings = self.field.postings.get(term)?;
        postings
            .binary_search_by_key(&doc_id, |p| p.doc_id)
            .ok()
            .map(|i| {
                if postings[i].pos_lengths.is_empty() {
                    vec![1; postings[i].positions.len()]
                } else {
                    postings[i].pos_lengths.clone()
                }
            })
    }
    fn has_splade(&self) -> bool {
        self.field.splade.as_ref().is_some_and(|s| !s.is_empty())
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
        match self.field.splade.as_ref() {
            Some(splade) => splade.score_top_k_with_floor(
                query_weights,
                query_l1_slack,
                initial_floor,
                k,
                admitted,
            ),
            None => (Vec::new(), initial_floor.max(0.0), Default::default()),
        }
    }
    fn has_sentences(&self) -> bool {
        self.field.sentences
    }
    fn doc_sentences(&self, doc_id: u32) -> Option<Vec<(u32, u32)>> {
        if !self.field.sentences {
            return None;
        }
        Some(
            self.field
                .doc_sentences
                .get(doc_id as usize)
                .cloned()
                .unwrap_or_default(),
        )
    }
    fn prefix_terms(&self, prefix: &str) -> Box<dyn Iterator<Item = String> + '_> {
        self.prefix_iter(prefix)
    }
    fn expand_prefix(&self, prefix: &str, cap: usize) -> Result<Vec<String>, usize> {
        let matches: Vec<&String> = self
            .field
            .postings
            .range(prefix.to_string()..)
            .map(|(term, _)| term)
            .take_while(|term| term.starts_with(prefix))
            .collect();
        if matches.len() > cap {
            Err(matches.len())
        } else {
            Ok(matches.into_iter().cloned().collect())
        }
    }
    fn suggest_prefix(&self, prefix: &str, max_scan: usize) -> Result<Vec<(String, u32)>, usize> {
        // Borrow through the walk; clone the terms only once the count
        // is known to fit, so a scan past the bound allocates nothing.
        let matches: Vec<(&String, u32)> = self
            .field
            .postings
            .range(prefix.to_string()..)
            .take_while(|(term, _)| term.starts_with(prefix))
            .map(|(term, postings)| (term, postings.len() as u32))
            .collect();
        if matches.len() > max_scan {
            Err(matches.len())
        } else {
            Ok(matches
                .into_iter()
                .map(|(term, df)| (term.clone(), df))
                .collect())
        }
    }
}

/// The store itself scores as its body field (field 0) — the surface
/// every single-field caller uses; multi-field scoring goes through
/// [`Bm25Store::field`].
impl Bm25Index for Bm25Store {
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
        self.field(0).for_each_posting(term, f);
    }
    fn for_each_doc_tf(&self, term: &str, f: &mut dyn FnMut(u32, u32)) {
        self.field(0).for_each_doc_tf(term, f);
    }
    fn posting_offsets(&self, term: &str, doc_id: u32) -> Vec<(u32, u32)> {
        self.field(0).posting_offsets(term, doc_id)
    }
    fn text(&self, doc_id: u32) -> Option<String> {
        self.field(0).text(doc_id)
    }
    fn lineage(&self, doc_id: u32) -> Option<DocLineage> {
        self.field(0).lineage(doc_id)
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
}
