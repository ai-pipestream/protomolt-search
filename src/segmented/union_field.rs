//! One field's read surface across every part of a segmented shard.

use super::*;

/// The read surface of one field across every part of a segmented
/// shard, with document ids in the shard's positional space.
pub struct UnionField<'a> {
    pub(super) shard: &'a SegmentedShard,
    pub(super) fi: usize,
    /// Sealed parts to walk; `None` walks them all.
    pub(super) mask: Option<Arc<[bool]>>,
}

impl<'a> UnionField<'a> {
    pub(super) fn prefix_iter(&self, prefix: &str) -> Box<dyn Iterator<Item = String> + 'a> {
        use std::{cmp::Reverse, collections::BinaryHeap};
        let mut scans: Vec<_> = self
            .parts()
            .map(|(_, _, v)| v.prefix_iter(prefix))
            .chain(self.heaps().map(|(_, v)| v.prefix_iter(prefix)))
            .collect();
        let mut heap = BinaryHeap::new();
        for (i, scan) in scans.iter_mut().enumerate() {
            if let Some(term) = scan.next() {
                heap.push(Reverse((term, i)));
            }
        }
        Box::new(std::iter::from_fn(move || {
            let Reverse((term, i)) = heap.pop()?;
            if let Some(next) = scans[i].next() {
                heap.push(Reverse((next, i)));
            }
            while heap.peek().is_some_and(|Reverse((next, _))| next == &term) {
                let Reverse((_, i)) = heap.pop().unwrap();
                if let Some(next) = scans[i].next() {
                    heap.push(Reverse((next, i)));
                }
            }
            Some(term)
        }))
    }

    fn admits(&self, part: usize) -> bool {
        self.mask.as_ref().is_none_or(|mask| mask[part])
    }

    fn parts(&self) -> impl Iterator<Item = (u32, u32, crate::postings::FieldView<'a>)> + '_ {
        let shard = self.shard;
        let fi = self.fi;
        shard
            .parts
            .iter()
            .enumerate()
            .filter(move |(i, _)| self.admits(*i))
            .map(move |(i, p)| (p.base, p.rows, shard.reader(i).field(fi)))
    }

    /// The heap parts (frozen, then tail) with their bases.
    fn heaps(&self) -> impl Iterator<Item = (u32, crate::postings::StoreFieldView<'a>)> + '_ {
        let fi = self.fi;
        self.shard
            .heaps()
            .map(move |(base, store)| (base, store.field(fi)))
    }

    pub(super) fn heap_df(&self, term: &str) -> u32 {
        self.heaps().map(|(_, v)| v.df(term)).sum()
    }

    /// The heap view and local id for a heap placement; `None` for a
    /// sealed one.
    fn heap_of(&self, placement: Placement) -> Option<(crate::postings::StoreFieldView<'a>, u32)> {
        match placement {
            Placement::Sealed { .. } => None,
            Placement::Frozen { local } => Some((self.shard.frozen_store().field(self.fi), local)),
            Placement::Tail { local } => Some((self.shard.tail.field(self.fi), local)),
        }
    }
}

impl Bm25Index for UnionField<'_> {
    fn doc_count(&self) -> u64 {
        self.parts().map(|(_, _, v)| v.doc_count()).sum::<u64>()
            + self.heaps().map(|(_, v)| v.doc_count()).sum::<u64>()
    }
    fn total_doc_length(&self) -> u64 {
        self.parts()
            .map(|(_, _, v)| v.total_doc_length())
            .sum::<u64>()
            + self.heaps().map(|(_, v)| v.total_doc_length()).sum::<u64>()
    }
    fn doc_length(&self, doc_id: u32) -> u32 {
        let placement = self.shard.place(doc_id);
        match self.heap_of(placement) {
            Some((view, local)) => view.doc_length(local),
            None => {
                let Placement::Sealed { part, local } = placement else {
                    unreachable!("heap_of covers every heap placement")
                };
                self.shard.reader(part).field(self.fi).doc_length(local)
            }
        }
    }
    fn df(&self, term: &str) -> u32 {
        self.parts().map(|(_, _, v)| v.df(term)).sum::<u32>() + self.heap_df(term)
    }
    fn for_each_posting(&self, term: &str, f: &mut PostingCallback) {
        for (base, _, view) in self.parts() {
            view.for_each_posting(term, &mut |doc, tf, offsets| f(base + doc, tf, offsets));
        }
        for (base, view) in self.heaps() {
            view.for_each_posting(term, &mut |doc, tf, offsets| f(base + doc, tf, offsets));
        }
    }
    fn for_each_doc_tf(&self, term: &str, f: &mut dyn FnMut(u32, u32)) {
        for (base, _, view) in self.parts() {
            view.for_each_doc_tf(term, &mut |doc, tf| f(base + doc, tf));
        }
        for (base, view) in self.heaps() {
            view.for_each_doc_tf(term, &mut |doc, tf| f(base + doc, tf));
        }
    }
    fn posting_offsets(&self, term: &str, doc_id: u32) -> Vec<(u32, u32)> {
        let placement = self.shard.place(doc_id);
        match self.heap_of(placement) {
            Some((view, local)) => view.posting_offsets(term, local),
            None => {
                let Placement::Sealed { part, local } = placement else {
                    unreachable!("heap_of covers every heap placement")
                };
                self.shard
                    .reader(part)
                    .field(self.fi)
                    .posting_offsets(term, local)
            }
        }
    }
    fn impacts(&self, term: &str) -> Option<ImpactCursor<'_>> {
        // A term a heap part (frozen or tail) holds has no impacts until
        // the next seal.
        if self.heap_df(term) > 0 {
            return None;
        }
        let parts: Vec<(u32, u32, FileImpacts<'_>)> = self
            .shard
            .parts
            .iter()
            .enumerate()
            .filter(|(i, _)| self.admits(*i))
            .filter_map(|(i, p)| {
                self.shard
                    .reader(i)
                    .field(self.fi)
                    .file_impacts(term)
                    .map(|c| (p.base, p.rows, c))
            })
            .collect();
        if parts.is_empty() {
            return None;
        }
        Some(ImpactCursor::chain(parts))
    }
    fn has_impacts(&self, term: &str) -> bool {
        if self.heap_df(term) > 0 {
            return false;
        }
        let mut any = false;
        for (_, _, view) in self.parts() {
            if view.df(term) > 0 {
                if !view.has_impacts(term) {
                    return false;
                }
                any = true;
            }
        }
        any
    }
    fn text(&self, doc_id: u32) -> Option<String> {
        self.shard.text(doc_id)
    }
    fn lineage(&self, doc_id: u32) -> Option<DocLineage> {
        self.shard.lineage(doc_id)
    }
    fn has_positions(&self) -> bool {
        self.shard.field_has_positions(self.fi)
    }
    fn posting_positions(&self, term: &str, doc_id: u32) -> Option<Vec<u32>> {
        let placement = self.shard.place(doc_id);
        match self.heap_of(placement) {
            Some((view, local)) => view.posting_positions(term, local),
            None => {
                let Placement::Sealed { part, local } = placement else {
                    unreachable!("heap_of covers every heap placement")
                };
                self.shard
                    .reader(part)
                    .field(self.fi)
                    .posting_positions(term, local)
            }
        }
    }
    fn has_pos_lengths(&self) -> bool {
        self.has_positions()
            && (self.parts().any(|(_, _, v)| v.has_pos_lengths())
                || self.heaps().any(|(_, v)| v.has_pos_lengths()))
    }
    fn posting_pos_lengths(&self, term: &str, doc_id: u32) -> Option<Vec<u32>> {
        if !self.has_pos_lengths() {
            return None;
        }
        let placement = self.shard.place(doc_id);
        match self.heap_of(placement) {
            Some((view, local)) => {
                if view.has_pos_lengths() {
                    view.posting_pos_lengths(term, local)
                } else {
                    view.posting_positions(term, local)
                        .map(|pos| vec![1; pos.len()])
                }
            }
            None => {
                let Placement::Sealed { part, local } = placement else {
                    unreachable!("heap_of covers every heap placement")
                };
                let view = self.shard.reader(part).field(self.fi);
                if view.has_pos_lengths() {
                    view.posting_pos_lengths(term, local)
                } else {
                    view.posting_positions(term, local)
                        .map(|pos| vec![1; pos.len()])
                }
            }
        }
    }
    fn has_splade(&self) -> bool {
        self.parts().any(|(_, _, v)| v.has_splade()) || self.heaps().any(|(_, v)| v.has_splade())
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
        let mut floor = if initial_floor.is_finite() && initial_floor > 0.0 {
            initial_floor
        } else {
            0.0
        };
        if k == 0 {
            return (Vec::new(), floor, Default::default());
        }
        let mut merged: Vec<(u32, f32)> = Vec::new();
        let mut total_stats = crate::turbo_bridge::splade::SpladeTraversalStats::default();
        for (base, _, view) in self.parts() {
            if !view.has_splade() {
                continue;
            }
            let (hits, _, stats) =
                view.score_splade_with_floor(query_weights, query_l1_slack, floor, k, &|local| {
                    admitted(base + local)
                });
            total_stats.total_blocks += stats.total_blocks;
            total_stats.blocks_visited += stats.blocks_visited;
            total_stats.blocks_skipped += stats.blocks_skipped;
            total_stats.postings_scored += stats.postings_scored;
            merged.extend(hits.into_iter().map(|(local, s)| (base + local, s)));
            merged.sort_unstable_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            merged.truncate(k);
            if merged.len() == k {
                floor = floor.max(merged.last().unwrap().1);
            }
        }
        for (base, view) in self.heaps() {
            if !view.has_splade() {
                continue;
            }
            let (hits, _, stats) =
                view.score_splade_with_floor(query_weights, query_l1_slack, floor, k, &|local| {
                    admitted(base + local)
                });
            total_stats.total_blocks += stats.total_blocks;
            total_stats.blocks_visited += stats.blocks_visited;
            total_stats.blocks_skipped += stats.blocks_skipped;
            total_stats.postings_scored += stats.postings_scored;
            merged.extend(hits.into_iter().map(|(local, s)| (base + local, s)));
            merged.sort_unstable_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            merged.truncate(k);
            if merged.len() == k {
                floor = floor.max(merged.last().unwrap().1);
            }
        }
        (merged, floor, total_stats)
    }
    fn has_sentences(&self) -> bool {
        self.shard.field_has_sentences(self.fi)
    }
    fn doc_sentences(&self, doc_id: u32) -> Option<Vec<(u32, u32)>> {
        self.shard.field_doc_sentences(self.fi, doc_id)
    }
    fn prefix_terms(&self, prefix: &str) -> Box<dyn Iterator<Item = String> + '_> {
        self.prefix_iter(prefix)
    }
    fn expand_prefix(&self, prefix: &str, cap: usize) -> Result<Vec<String>, usize> {
        let mut union: Vec<String> = Vec::new();
        let mut over: Option<usize> = None;
        let mut fold = |result: Result<Vec<String>, usize>| match result {
            Ok(terms) => union.extend(terms),
            Err(count) => over = Some(over.unwrap_or(0).max(count)),
        };
        for (_, _, view) in self.parts() {
            fold(view.expand_prefix(prefix, cap));
        }
        for (_, view) in self.heaps() {
            fold(view.expand_prefix(prefix, cap));
        }
        if let Some(count) = over {
            return Err(count.max(union.len()));
        }
        union.sort_unstable();
        union.dedup();
        if union.len() > cap {
            Err(union.len())
        } else {
            Ok(union)
        }
    }
    fn suggest_prefix(&self, prefix: &str, max_scan: usize) -> Result<Vec<(String, u32)>, usize> {
        // Sum the posting df of each term across the sealed parts and
        // the heaps: the same term in two parts is one dictionary entry
        // whose df is the sum, exactly what one image of the rows holds.
        let mut union: std::collections::BTreeMap<String, u32> = std::collections::BTreeMap::new();
        let mut over: Option<usize> = None;
        let mut fold = |result: Result<Vec<(String, u32)>, usize>| match result {
            Ok(entries) => {
                for (term, df) in entries {
                    *union.entry(term).or_insert(0) += df;
                }
            }
            Err(count) => over = Some(over.unwrap_or(0).max(count)),
        };
        for (_, _, view) in self.parts() {
            fold(view.suggest_prefix(prefix, max_scan));
        }
        for (_, view) in self.heaps() {
            fold(view.suggest_prefix(prefix, max_scan));
        }
        if let Some(count) = over {
            return Err(count.max(union.len()));
        }
        if union.len() > max_scan {
            Err(union.len())
        } else {
            Ok(union.into_iter().collect())
        }
    }
}
