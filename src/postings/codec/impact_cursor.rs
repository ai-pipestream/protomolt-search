//! The block-max cursor the scorer drives across one or more files' skip runs.

use super::{pareto_frontier, FileImpacts};
use crate::postings::LEVEL1_FACTOR;

/// block-max surface. Level-0 impact records (one
/// per 128-posting block) are read straight from the map, and
/// `advance_shallow` first leaps whole level-1 groups (32 blocks = 4096
/// postings) by their `last_doc_id`, then skips level-0 blocks the same
/// way, never decoding doc-run entries it passes.
/// The block-max cursor the scorer drives (`src/bm25.rs`): one file's
/// skip runs, or a chain of them across the sealed segments of a
/// segmented shard. A chain presents the
/// parts as one term: document ids carry each part's base, block and
/// level-1 group numbers run on cumulatively so the scorer's "did the
/// block change" caches stay honest, per-block and per-group frontiers
/// are the part's own (an upper bound over a subset is an upper bound),
/// and the term frontier is the Pareto union of the parts'.
pub struct ImpactCursor<'a> {
    /// `(base doc id, physical rows, cursor)` in ascending base order,
    /// parts without the term omitted.
    parts: Vec<(u32, u32, FileImpacts<'a>)>,
    at: usize,
    blocks_before: u32,
    groups_before: u32,
    df: u32,
    n_blocks: u32,
    term_pairs: Vec<(u32, u32)>,
}

impl<'a> ImpactCursor<'a> {
    pub(in crate::postings) fn single(file: FileImpacts<'a>) -> Self {
        Self::chain(vec![(0, u32::MAX, file)])
    }

    /// Chain per-part cursors; `parts` must be in ascending base order
    /// and hold at least one part.
    pub(crate) fn chain(parts: Vec<(u32, u32, FileImpacts<'a>)>) -> Self {
        debug_assert!(!parts.is_empty());
        let df = parts.iter().map(|(_, _, c)| c.df()).sum();
        let n_blocks = parts.iter().map(|(_, _, c)| c.n_blocks()).sum();
        let mut pairs: Vec<(u32, u32)> = Vec::new();
        for (_, _, c) in &parts {
            pairs.extend_from_slice(c.term_frontier());
        }
        let term_pairs = pareto_frontier(&pairs);
        let mut cursor = Self {
            parts,
            at: 0,
            blocks_before: 0,
            groups_before: 0,
            df,
            n_blocks,
            term_pairs,
        };
        cursor.settle();
        cursor
    }

    /// Move past exhausted parts, keeping the cumulative counters.
    fn settle(&mut self) {
        while self.at < self.parts.len() && self.parts[self.at].2.exhausted() {
            let inner = &self.parts[self.at].2;
            self.blocks_before += inner.n_blocks();
            self.groups_before += inner.n_blocks().div_ceil(LEVEL1_FACTOR as u32);
            self.at += 1;
        }
    }

    fn part(&self) -> Option<&(u32, u32, FileImpacts<'a>)> {
        self.parts.get(self.at)
    }

    pub fn df(&self) -> u32 {
        self.df
    }

    pub fn n_blocks(&self) -> u32 {
        self.n_blocks
    }

    pub fn exhausted(&self) -> bool {
        self.at >= self.parts.len()
    }

    pub fn block(&self) -> u32 {
        match self.part() {
            Some((_, _, inner)) => self.blocks_before + inner.block(),
            None => self.n_blocks,
        }
    }

    pub fn block_frontier(&self) -> &[(u32, u32)] {
        self.part()
            .map_or(&[], |(_, _, inner)| inner.block_frontier())
    }

    pub fn term_frontier(&self) -> &[(u32, u32)] {
        &self.term_pairs
    }

    pub fn block_last_doc(&self) -> u32 {
        match self.part() {
            Some((base, _, inner)) => base.saturating_add(inner.block_last_doc()),
            None => u32::MAX,
        }
    }

    pub fn doc_id(&self) -> u32 {
        match self.part() {
            Some((base, _, inner)) if !inner.exhausted() => base + inner.doc_id(),
            _ => u32::MAX,
        }
    }

    pub fn tf(&self) -> u32 {
        self.part().map_or(0, |(_, _, inner)| inner.tf())
    }

    pub fn offsets(&self) -> Vec<(u32, u32)> {
        self.part()
            .map_or_else(Vec::new, |(_, _, inner)| inner.offsets())
    }

    pub fn next_posting(&mut self) -> bool {
        if self.exhausted() {
            return false;
        }
        let inner = &mut self.parts[self.at].2;
        if !inner.next_posting() {
            self.settle();
        }
        !self.exhausted()
    }

    pub fn advance_shallow(&mut self, target: u32) {
        loop {
            let Some((base, rows)) = self.part().map(|p| (p.0, p.1)) else {
                return;
            };
            // Every document of this part sits below `base + rows`: a
            // target at or past that skips the whole part.
            if base.checked_add(rows).is_some_and(|end| target >= end) {
                let inner = &self.parts[self.at].2;
                self.blocks_before += inner.n_blocks();
                self.groups_before += inner.n_blocks().div_ceil(LEVEL1_FACTOR as u32);
                self.at += 1;
                continue;
            }
            let inner = &mut self.parts[self.at].2;
            inner.advance_shallow(target.saturating_sub(base));
            if inner.exhausted() {
                self.settle();
                continue;
            }
            return;
        }
    }

    pub fn l1_group(&self) -> u32 {
        match self.part() {
            Some((_, _, inner)) => self.groups_before + inner.l1_group(),
            None => self
                .parts
                .iter()
                .map(|(_, _, c)| c.n_blocks().div_ceil(LEVEL1_FACTOR as u32))
                .sum(),
        }
    }

    pub fn l1_last_doc(&self) -> u32 {
        match self.part() {
            Some((base, _, inner)) => base.saturating_add(inner.l1_last_doc()),
            None => u32::MAX,
        }
    }

    pub fn l1_frontier(&self) -> &[(u32, u32)] {
        self.part().map_or(&[], |(_, _, inner)| inner.l1_frontier())
    }

    /// Level-0 blocks skipped so far, over every part.
    pub fn blocks_skipped(&self) -> u64 {
        self.parts.iter().map(|(_, _, c)| c.blocks_skipped).sum()
    }

    /// Level-1 groups skipped so far, over every part.
    pub fn l1_groups_skipped(&self) -> u64 {
        self.parts.iter().map(|(_, _, c)| c.l1_groups_skipped).sum()
    }
}
