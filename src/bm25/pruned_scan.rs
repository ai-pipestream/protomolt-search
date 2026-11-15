//! Block-max traversal preserves term-order sums and rounded-score ties.

use super::pruned_cursor::Pair;
use super::ranking::HeapEntry;
use super::{ChainCtx, FieldQuery, PruneStats};
use std::collections::BinaryHeap;

pub(super) struct Cutoff<'a> {
    pub floor: f64,
    pub kth: Option<f32>,
    pub chain: ChainCtx<'a>,
}

impl Cutoff<'_> {
    // Rounding is monotone, so a rounded upper bound also bounds wire scores,
    // for the floor as for the k-th best.
    // Equal rounded scores cannot replace earlier, smaller document ids.
    pub fn inert(&self, bound: f64) -> bool {
        let bound = self.chain.map_or(bound, |(chain, _)| chain.bound(bound));
        !super::ranking::clears(bound, self.floor)
            || self.kth.is_some_and(|kth| {
                super::ranking::wire_score(bound)
                    .total_cmp(&(kth + 0.0))
                    .is_le()
            })
    }
}

pub(super) struct Scan<'a> {
    pub pairs: Vec<Pair<'a>>,
    pub heap: BinaryHeap<HeapEntry>,
    finished: (u64, u64),
    order: Vec<usize>,
    nonessential: Vec<bool>,
    prefix: Vec<usize>,
    lengths: Vec<u32>,
    contributions: Vec<f64>,
    touched: Vec<usize>,
    last_selected: Option<u32>,
}

impl<'a> Scan<'a> {
    pub fn new(pairs: Vec<Pair<'a>>, count: usize) -> Self {
        Self {
            pairs,
            heap: BinaryHeap::new(),
            finished: (0, 0),
            order: Vec::new(),
            nonessential: Vec::new(),
            prefix: Vec::new(),
            lengths: Vec::new(),
            contributions: vec![0.0; count],
            touched: Vec::new(),
            last_selected: None,
        }
    }

    pub fn retain_live(&mut self) {
        self.pairs.retain(|pair| {
            if pair.cursor.exhausted() {
                self.finished.0 += pair.cursor.blocks_skipped();
                self.finished.1 += pair.cursor.l1_groups_skipped();
                false
            } else {
                true
            }
        });
    }

    pub fn skip_bounds(&mut self, cutoff: &Cutoff<'_>) -> bool {
        let (mut groups, mut blocks) = (0.0, 0.0);
        for pair in &self.pairs {
            groups += pair.l1_max;
            blocks += pair.block_max;
        }
        let group = cutoff.inert(groups);
        if group || cutoff.inert(blocks) {
            let last = self
                .pairs
                .iter()
                .map(|pair| {
                    if group {
                        pair.cursor.l1_last_doc()
                    } else {
                        pair.cursor.block_last_doc()
                    }
                })
                .min()
                .expect("nonempty cursors");
            for pair in &mut self.pairs {
                pair.cursor.advance_shallow(last.saturating_add(1));
                pair.refresh();
            }
            self.retain_live();
            return true;
        }
        false
    }

    pub fn whole_inert(&self, cutoff: &Cutoff<'_>) -> bool {
        let mut bound = 0.0;
        for pair in &self.pairs {
            bound += pair.term_max;
        }
        cutoff.inert(bound)
    }

    fn partition(&mut self, cutoff: &Cutoff<'_>) {
        self.order.clear();
        self.order.extend(0..self.pairs.len());
        self.order.sort_by(|&a, &b| {
            self.pairs[a]
                .block_max
                .partial_cmp(&self.pairs[b].block_max)
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        self.nonessential.clear();
        self.nonessential.resize(self.pairs.len(), false);
        self.prefix.clear();
        for &next in &self.order {
            let mut bound = 0.0;
            for (position, pair) in self.pairs.iter().enumerate() {
                if position == next || self.prefix.contains(&position) {
                    bound += pair.block_max;
                }
            }
            if !cutoff.inert(bound) {
                break;
            }
            self.prefix.push(next);
        }
        for &position in &self.prefix {
            self.nonessential[position] = true;
        }
    }

    pub fn candidate(&mut self, cutoff: &Cutoff<'_>) -> Option<u32> {
        let end = self
            .pairs
            .iter()
            .map(|pair| pair.cursor.block_last_doc())
            .min()
            .expect("nonempty cursors");
        self.partition(cutoff);
        let doc = self
            .pairs
            .iter()
            .zip(&self.nonessential)
            .filter(|(_, nonessential)| !**nonessential)
            .map(|(pair, _)| pair.cursor.doc_id())
            .min()
            .expect("competitive block has an essential cursor");
        if doc > end {
            for pair in &mut self.pairs {
                if pair.cursor.block_last_doc() == end {
                    pair.cursor.advance_shallow(end.saturating_add(1));
                    pair.refresh();
                }
            }
            self.retain_live();
            return None;
        }
        debug_assert!(self.last_selected.is_none_or(|previous| doc > previous));
        self.last_selected = Some(doc);
        Some(doc)
    }

    pub fn bound(&mut self, fields: &[FieldQuery<'_>], doc: u32) -> f64 {
        self.lengths.clear();
        self.lengths
            .extend(fields.iter().map(|field| field.index.doc_length(doc)));
        let mut bound = 0.0;
        for (pair, nonessential) in self.pairs.iter().zip(&self.nonessential) {
            if *nonessential {
                bound += pair.block_max;
            } else if pair.cursor.doc_id() == doc {
                bound += pair.contribution(&self.lengths);
            }
        }
        bound
    }

    pub fn score(&mut self, doc: u32, chain: ChainCtx<'_>, prune: &mut PruneStats) -> HeapEntry {
        self.touched.clear();
        let mut mask = 0;
        for pair in &mut self.pairs {
            pair.cursor.advance_shallow(doc);
            if pair.cursor.doc_id() == doc {
                self.contributions[pair.order] = pair.contribution(&self.lengths);
                self.touched.push(pair.order);
                mask |= 1u128 << pair.order;
            }
        }
        self.touched.sort_unstable();
        let mut score = 0.0;
        for &position in &self.touched {
            score += self.contributions[position];
        }
        let score = chain.map_or(score, |(chain, columns)| chain.eval(score, doc, columns));
        prune.candidates_evaluated += 1;
        prune.postings_scored += self.touched.len() as u64;
        HeapEntry {
            doc_id: doc,
            score,
            mask,
        }
    }

    pub fn advance(&mut self, doc: u32, shallow: bool) {
        for pair in &mut self.pairs {
            if shallow {
                pair.cursor.advance_shallow(doc);
            }
            if pair.cursor.doc_id() == doc {
                pair.cursor.next_posting();
            }
            pair.refresh();
        }
        self.retain_live();
    }

    pub fn finish(self, prune: &mut PruneStats) -> Vec<HeapEntry> {
        prune.blocks_skipped = self.finished.0
            + self
                .pairs
                .iter()
                .map(|pair| pair.cursor.blocks_skipped())
                .sum::<u64>();
        prune.l1_groups_skipped = self.finished.1
            + self
                .pairs
                .iter()
                .map(|pair| pair.cursor.l1_groups_skipped())
                .sum::<u64>();
        let mut hits = self.heap.into_vec();
        hits.sort();
        hits
    }
}
