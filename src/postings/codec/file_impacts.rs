//! One file's block-max skip-run cursor over the mapped postings section.

use super::pareto_frontier;
use crate::postings::{BLOCK, LEVEL1_FACTOR};

pub struct FileImpacts<'a> {
    map: &'a [u8],
    doc_run_off: usize,
    occ_run_off: usize,
    skip_run_off: usize,
    df: u32,
    n_blocks: u32,
    /// Current level-0 block index and its record's map offset.
    block: u32,
    block_rec_off: usize,
    block_last_doc: u32,
    block_pairs: Vec<(u32, u32)>,
    /// Level-1 state: the group covering `block` (invariant
    /// `l1_idx == block / LEVEL1_FACTOR`), its record's map offset,
    /// last doc id, merged frontier, and the level-0 offset its first
    /// record lives at (the leap target).
    l1_idx: u32,
    n_l1: u32,
    l1_rec_off: usize,
    l1_last_doc: u32,
    l1_l0_off: u64,
    l1_pairs: Vec<(u32, u32)>,
    /// Current posting index in `0..=df` (`df` = exhausted).
    pos: u32,
    /// The whole term's `(tf, dl)` frontier, merged from the level-1
    /// records: a static upper bound over every posting of the term.
    term_pairs: Vec<(u32, u32)>,
    /// Blocks bypassed by [`Self::advance_shallow`] without their
    /// postings being read (skip accounting for the pruned scorer).
    pub blocks_skipped: u64,
    /// Level-1 groups (32 blocks = 4096 postings) leapt by
    /// [`Self::advance_shallow`] without reading a single level-0
    /// record inside them.
    pub l1_groups_skipped: u64,
}

impl<'a> FileImpacts<'a> {
    pub(in crate::postings) fn new(
        map: &'a [u8],
        doc_run_off: usize,
        occ_run_off: usize,
        skip_run_off: usize,
        df: u32,
    ) -> Self {
        let n_blocks = df.div_ceil(BLOCK as u32);
        let n_l1 = n_blocks.div_ceil(LEVEL1_FACTOR as u32);
        // The whole-term frontier: merge every level-1 record's pairs
        // (each covers 32 blocks) and take the Pareto frontier of the
        // union — a static bound over every posting of the term.
        let l1_region = skip_run_off
            + u64::from_le_bytes(map[skip_run_off..skip_run_off + 8].try_into().unwrap()) as usize;
        let mut term_pairs: Vec<(u32, u32)> = Vec::new();
        let mut cur = l1_region;
        for _ in 0..n_l1 {
            // u32 last_doc_id, u64 l0_off, u8 n_pairs, pairs
            let n_pairs = map[cur + 12] as usize;
            for i in 0..n_pairs {
                let p = cur + 13 + 8 * i;
                term_pairs.push((
                    u32::from_le_bytes(map[p..p + 4].try_into().unwrap()),
                    u32::from_le_bytes(map[p + 4..p + 8].try_into().unwrap()),
                ));
            }
            cur += 13 + 8 * n_pairs;
        }
        let term_pairs = pareto_frontier(&term_pairs);
        let mut cur = Self {
            map,
            doc_run_off,
            occ_run_off,
            skip_run_off,
            df,
            n_blocks,
            block: 0,
            block_rec_off: skip_run_off + 8, // past the level-1 prefix
            block_last_doc: 0,
            block_pairs: Vec::new(),
            l1_idx: 0,
            n_l1,
            l1_rec_off: l1_region,
            l1_last_doc: u32::MAX,
            l1_l0_off: 0,
            l1_pairs: Vec::new(),
            pos: 0,
            term_pairs,
            blocks_skipped: 0,
            l1_groups_skipped: 0,
        };
        cur.load_l1();
        cur.load_block();
        cur
    }

    fn u32_at(&self, off: usize) -> u32 {
        u32::from_le_bytes(self.map[off..off + 4].try_into().unwrap())
    }

    /// Decode the level-1 record at `l1_rec_off` into the cursor.
    fn load_l1(&mut self) {
        if self.l1_idx >= self.n_l1 {
            self.l1_last_doc = u32::MAX;
            self.l1_pairs.clear();
            return;
        }
        let e = self.l1_rec_off;
        self.l1_last_doc = self.u32_at(e);
        self.l1_l0_off = u64::from_le_bytes(self.map[e + 4..e + 12].try_into().unwrap());
        let n_pairs = self.map[e + 12] as usize;
        self.l1_pairs.clear();
        for i in 0..n_pairs {
            self.l1_pairs
                .push((self.u32_at(e + 13 + 8 * i), self.u32_at(e + 13 + 8 * i + 4)));
        }
    }

    /// Advance to the next level-1 group (its record follows the
    /// current one contiguously).
    fn next_l1(&mut self) {
        if self.l1_idx >= self.n_l1 {
            return;
        }
        self.l1_rec_off += 13 + 8 * self.l1_pairs.len();
        self.l1_idx += 1;
        self.load_l1();
    }

    /// Decode the level-0 record at `block_rec_off` into the cursor.
    fn load_block(&mut self) {
        if self.block >= self.n_blocks {
            self.pos = self.df;
            self.block_pairs.clear();
            return;
        }
        let e = self.block_rec_off;
        self.block_last_doc = self.u32_at(e);
        let n_pairs = self.map[e + 4] as usize;
        self.block_pairs.clear();
        for i in 0..n_pairs {
            self.block_pairs
                .push((self.u32_at(e + 5 + 8 * i), self.u32_at(e + 5 + 8 * i + 4)));
        }
    }

    /// Advance to the next level-0 block (its record follows the current
    /// one contiguously), keeping the level-1 pointer in sync. False
    /// when the term is exhausted.
    fn next_block(&mut self) -> bool {
        if self.block >= self.n_blocks {
            return false;
        }
        self.block_rec_off += 5 + 8 * self.block_pairs.len();
        self.block += 1;
        if self.block.is_multiple_of(LEVEL1_FACTOR as u32) {
            self.next_l1();
        }
        self.pos = (self.block * BLOCK as u32).min(self.df);
        self.load_block();
        self.block < self.n_blocks
    }

    /// Postings in the term.
    pub fn df(&self) -> u32 {
        self.df
    }

    /// Blocks in the term.
    pub fn n_blocks(&self) -> u32 {
        self.n_blocks
    }

    /// True when every posting has been consumed.
    pub fn exhausted(&self) -> bool {
        self.pos >= self.df
    }

    /// The current level-0 block index.
    pub fn block(&self) -> u32 {
        self.block
    }

    /// The current block's `(tf, dl)` Pareto frontier (<= 8 pairs).
    pub fn block_frontier(&self) -> &[(u32, u32)] {
        &self.block_pairs
    }

    /// The whole term's `(tf, dl)` frontier from the level-1 records —
    /// a static bound over every posting, used for termination.
    pub fn term_frontier(&self) -> &[(u32, u32)] {
        &self.term_pairs
    }

    /// The last doc id in the current block — the shallow-advance bound.
    pub fn block_last_doc(&self) -> u32 {
        self.block_last_doc
    }

    /// The doc id at the cursor position (`u32::MAX` when exhausted).
    pub fn doc_id(&self) -> u32 {
        if self.exhausted() {
            u32::MAX
        } else {
            self.u32_at(self.doc_run_off + 12 * self.pos as usize)
        }
    }

    /// The term frequency at the cursor position.
    pub fn tf(&self) -> u32 {
        self.u32_at(self.doc_run_off + 12 * self.pos as usize + 4)
    }

    /// The occurrence spans of the posting at the cursor position.
    pub fn offsets(&self) -> Vec<(u32, u32)> {
        let e = self.doc_run_off + 12 * self.pos as usize;
        let occ_start = self.u32_at(e + 8);
        let occ_end = if self.pos + 1 < self.df {
            self.u32_at(e + 12 + 8)
        } else {
            self.u32_at(self.doc_run_off + 12 * self.df as usize) // sentinel
        };
        let mut out = Vec::with_capacity((occ_end - occ_start) as usize);
        for o in occ_start..occ_end {
            let p = self.occ_run_off + 8 * o as usize;
            out.push((self.u32_at(p), self.u32_at(p + 4)));
        }
        out
    }

    /// Advance one posting, crossing into the next block when the
    /// current one is consumed. False when the term is exhausted.
    pub fn next_posting(&mut self) -> bool {
        if self.exhausted() {
            return false;
        }
        self.pos += 1;
        if self.pos < self.df && self.pos.is_multiple_of(BLOCK as u32) {
            self.next_block();
        }
        !self.exhausted()
    }

    /// Position the cursor on the first posting with doc id >= `target`:
    /// first leap whole level-1 groups (32 blocks = 4096 postings) whose
    /// `last_doc_id` is below it without reading a level-0 record inside
    /// them, then bypass level-0 blocks the same way, then binary-search
    /// the landing block.
    pub fn advance_shallow(&mut self, target: u32) {
        while !self.exhausted() && self.l1_last_doc < target {
            self.l1_groups_skipped += 1;
            self.next_l1();
            if self.l1_idx >= self.n_l1 {
                // The last group's last doc is the term's last doc:
                // nothing at or after the target exists.
                self.block = self.n_blocks;
                self.block_pairs.clear();
                self.pos = self.df;
                return;
            }
            // Land directly on the group's first level-0 record.
            self.block = self.l1_idx * LEVEL1_FACTOR as u32;
            self.block_rec_off = self.skip_run_off + self.l1_l0_off as usize;
            self.pos = (self.block * BLOCK as u32).min(self.df);
            self.load_block();
        }
        while !self.exhausted() && self.block_last_doc < target {
            self.blocks_skipped += 1;
            self.next_block();
        }
        if self.exhausted() {
            return;
        }
        // Binary search within the block for the first doc >= target
        // (doc ids ascending; pos already points into this block).
        let block_end = ((self.block + 1) * BLOCK as u32).min(self.df);
        let (mut lo, mut hi) = (self.pos, block_end);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.u32_at(self.doc_run_off + 12 * mid as usize) < target {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        self.pos = lo;
    }

    /// The current level-1 group index.
    pub fn l1_group(&self) -> u32 {
        self.l1_idx
    }

    /// The last doc id in the current level-1 group.
    pub fn l1_last_doc(&self) -> u32 {
        self.l1_last_doc
    }

    /// The current level-1 group's merged `(tf, dl)` frontier.
    pub fn l1_frontier(&self) -> &[(u32, u32)] {
        &self.l1_pairs
    }
}
