use super::*;

/// Zero-copy cursor over one term's v5 doc run and skip run — the
/// block-max surface (`docs/block-max.md`). Level-0 impact records (one
/// per 128-posting block) are read straight from the map, and
/// `advance_shallow` first leaps whole level-1 groups (32 blocks = 4096
/// postings) by their `last_doc_id`, then skips level-0 blocks the same
/// way, never decoding doc-run entries it passes.
/// The block-max cursor the scorer drives (`src/bm25.rs`): one file's
/// skip runs, or a chain of them across the sealed segments of a
/// segmented shard (`docs/immutable-segments.md`). A chain presents the
/// parts as one term: document ids carry each part's base, block and
/// level-1 group numbers run on cumulatively so the scorer's "did the
/// block change" caches stay honest, per-block and per-group frontiers
/// are the part's own (an upper bound over a subset is an upper bound),
/// and the term frontier is the Pareto union of the parts'.
pub struct ImpactCursor<'a> {
    /// `(base doc id, physical rows, cursor)` in ascending base order,
    /// parts without the term omitted.
    pub(crate) parts: Vec<(u32, u32, FileImpacts<'a>)>,
    pub(crate) at: usize,
    pub(crate) blocks_before: u32,
    pub(crate) groups_before: u32,
    pub(crate) df: u32,
    pub(crate) n_blocks: u32,
    pub(crate) term_pairs: Vec<(u32, u32)>,
}

impl<'a> ImpactCursor<'a> {
    pub(crate) fn single(file: FileImpacts<'a>) -> Self {
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
    pub(crate) fn settle(&mut self) {
        while self.at < self.parts.len() && self.parts[self.at].2.exhausted() {
            let inner = &self.parts[self.at].2;
            self.blocks_before += inner.n_blocks();
            self.groups_before += inner.n_blocks().div_ceil(LEVEL1_FACTOR as u32);
            self.at += 1;
        }
    }

    pub(crate) fn part(&self) -> Option<&(u32, u32, FileImpacts<'a>)> {
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

pub struct FileImpacts<'a> {
    pub(crate) map: &'a [u8],
    pub(crate) doc_run_off: usize,
    pub(crate) occ_run_off: usize,
    pub(crate) skip_run_off: usize,
    pub(crate) df: u32,
    pub(crate) n_blocks: u32,
    /// Current level-0 block index and its record's map offset.
    pub(crate) block: u32,
    pub(crate) block_rec_off: usize,
    pub(crate) block_last_doc: u32,
    pub(crate) block_pairs: Vec<(u32, u32)>,
    /// Level-1 state: the group covering `block` (invariant
    /// `l1_idx == block / LEVEL1_FACTOR`), its record's map offset,
    /// last doc id, merged frontier, and the level-0 offset its first
    /// record lives at (the leap target).
    pub(crate) l1_idx: u32,
    pub(crate) n_l1: u32,
    pub(crate) l1_rec_off: usize,
    pub(crate) l1_last_doc: u32,
    pub(crate) l1_l0_off: u64,
    pub(crate) l1_pairs: Vec<(u32, u32)>,
    /// Current posting index in `0..=df` (`df` = exhausted).
    pub(crate) pos: u32,
    /// The whole term's `(tf, dl)` frontier, merged from the level-1
    /// records: a static upper bound over every posting of the term.
    pub(crate) term_pairs: Vec<(u32, u32)>,
    /// Blocks bypassed by [`Self::advance_shallow`] without their
    /// postings being read (skip accounting for the pruned scorer).
    pub blocks_skipped: u64,
    /// Level-1 groups (32 blocks = 4096 postings) leapt by
    /// [`Self::advance_shallow`] without reading a single level-0
    /// record inside them.
    pub l1_groups_skipped: u64,
}

impl<'a> FileImpacts<'a> {
    pub(crate) fn new(
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

    pub(crate) fn u32_at(&self, off: usize) -> u32 {
        u32::from_le_bytes(self.map[off..off + 4].try_into().unwrap())
    }

    /// Decode the level-1 record at `l1_rec_off` into the cursor.
    pub(crate) fn load_l1(&mut self) {
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
    pub(crate) fn next_l1(&mut self) {
        if self.l1_idx >= self.n_l1 {
            return;
        }
        self.l1_rec_off += 13 + 8 * self.l1_pairs.len();
        self.l1_idx += 1;
        self.load_l1();
    }

    /// Decode the level-0 record at `block_rec_off` into the cursor.
    pub(crate) fn load_block(&mut self) {
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
    pub(crate) fn next_block(&mut self) -> bool {
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
