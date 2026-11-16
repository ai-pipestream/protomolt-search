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
    // Rounding is monotone, so a rounded upper bound also bounds wire scores.
    // Equal rounded scores cannot replace earlier, smaller document ids.
    pub fn inert(&self, bound: f64) -> bool {
        let bound = self.chain.map_or(bound, |(chain, _)| chain.bound(bound));
        bound < self.floor
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
    rank: Vec<usize>,
    nonessential: Vec<bool>,
    lengths: Vec<u32>,
    contributions: Vec<f64>,
    touched: Vec<usize>,
    last_selected: Option<u32>,
}

fn prefix_bound(len: usize, bound_at: &impl Fn(usize) -> f64, rank: &[usize], count: usize) -> f64 {
    let mut bound = 0.0;
    for (position, &item_rank) in rank.iter().take(len).enumerate() {
        if item_rank < count {
            bound += bound_at(position);
        }
    }
    bound
}

fn partition_by(
    len: usize,
    bound_at: impl Fn(usize) -> f64,
    cutoff: &Cutoff<'_>,
    order: &mut Vec<usize>,
    rank: &mut Vec<usize>,
    nonessential: &mut Vec<bool>,
) {
    order.clear();
    order.extend(0..len);
    order.sort_by(|&a, &b| {
        bound_at(a)
            .partial_cmp(&bound_at(b))
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    rank.resize(len, 0);
    for (item_rank, &position) in order.iter().enumerate() {
        rank[position] = item_rank;
    }
    let count = if (0..len).all(|i| bound_at(i) >= 0.0) {
        let (mut lo, mut hi) = (0, len);
        while lo < hi {
            let mid = lo + (hi - lo).div_ceil(2);
            if cutoff.inert(prefix_bound(len, &bound_at, rank, mid)) {
                lo = mid;
            } else {
                hi = mid - 1;
            }
        }
        lo
    } else {
        (1..=len)
            .take_while(|&n| cutoff.inert(prefix_bound(len, &bound_at, rank, n)))
            .last()
            .unwrap_or(0)
    };
    nonessential.clear();
    nonessential.extend(rank.iter().take(len).map(|&item_rank| item_rank < count));
}

impl<'a> Scan<'a> {
    pub fn new(pairs: Vec<Pair<'a>>, count: usize) -> Self {
        Self {
            pairs,
            heap: BinaryHeap::new(),
            finished: (0, 0),
            order: Vec::new(),
            rank: Vec::new(),
            nonessential: Vec::new(),
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
        let pairs = &self.pairs;
        partition_by(
            pairs.len(),
            |i| pairs[i].block_max,
            cutoff,
            &mut self.order,
            &mut self.rank,
            &mut self.nonessential,
        );
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

    pub fn score(&mut self, doc: u32, chain: ChainCtx<'_>, prune: &mut PruneStats) -> f64 {
        self.touched.clear();
        for pair in &mut self.pairs {
            pair.cursor.advance_shallow(doc);
            if pair.cursor.doc_id() == doc {
                self.contributions[pair.order] = pair.contribution(&self.lengths);
                self.touched.push(pair.order);
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
        score
    }

    pub fn admit(&mut self, doc: u32, score: f64, k: usize, include_offsets: bool) {
        let replace = if self.heap.len() < k {
            false
        } else if self.heap.peek().is_some_and(|worst| {
            super::ranking::compare((doc, score), (worst.doc_id, worst.score)).is_lt()
        }) {
            true
        } else {
            return;
        };
        if replace {
            self.heap.pop();
        }
        let matched = if include_offsets {
            Box::from(self.touched.as_slice())
        } else {
            Box::default()
        };
        self.heap.push(HeapEntry {
            doc_id: doc,
            score,
            matched,
        });
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

#[cfg(test)]
mod tests {
    use super::{partition_by, Cutoff};

    fn sequential_oracle(bounds: &[f64], cutoff: &Cutoff<'_>) -> Vec<bool> {
        let mut order: Vec<usize> = (0..bounds.len()).collect();
        order.sort_by(|&a, &b| {
            bounds[a]
                .partial_cmp(&bounds[b])
                .unwrap_or(std::cmp::Ordering::Equal)
        });
        let mut nonessential = vec![false; bounds.len()];
        for &next in &order {
            let mut bound = 0.0;
            for (position, &value) in bounds.iter().enumerate() {
                if position == next || nonessential[position] {
                    bound += value;
                }
            }
            if !cutoff.inert(bound) {
                break;
            }
            nonessential[next] = true;
        }
        nonessential
    }

    fn assert_partition_parity(bounds: &[f64], floor: f64, kth: Option<f32>) {
        let cutoff = Cutoff {
            floor,
            kth,
            chain: None,
        };
        let (mut order, mut rank, mut actual) = (Vec::new(), Vec::new(), Vec::new());
        partition_by(
            bounds.len(),
            |i| bounds[i],
            &cutoff,
            &mut order,
            &mut rank,
            &mut actual,
        );
        assert_eq!(actual, sequential_oracle(bounds, &cutoff));
    }

    #[test]
    fn partition_matches_sequential_oracle_across_edges_and_1024_terms() {
        for floor in [f64::NEG_INFINITY, 0.0, 0.5, 10.0] {
            assert_partition_parity(&[], floor, None);
            assert_partition_parity(&[0.0, 0.0, 0.0], floor, Some(0.0));
            assert_partition_parity(&[f64::NAN, 0.0, 0.5, 1.2], floor, None);
            assert_partition_parity(&[-0.25, 0.1, 0.4, 0.9], floor, None);
        }
        let wide: Vec<f64> = (0..1_024u64)
            .map(|i| {
                let bits = i.wrapping_mul(0x9E37_79B9_7F4A_7C15).rotate_left(17);
                if i % 37 == 0 {
                    0.0
                } else {
                    (bits % 10_000) as f64 * 1e-4 + (i as f64) * 1e-16
                }
            })
            .collect();
        let total: f64 = wide.iter().sum();
        for step in 0..=32 {
            let floor = total * (step as f64 / 32.0);
            assert_partition_parity(&wide, floor, None);
            assert_partition_parity(&wide, f64::NEG_INFINITY, Some(floor as f32));
        }
        assert_partition_parity(&wide, total + 1.0, None);
    }
}
