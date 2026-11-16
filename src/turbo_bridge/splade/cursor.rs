//! Term metadata parsing, block-max cursors and the top-k collector.

use super::*;

#[derive(Clone, Debug)]
pub(super) struct SpladeTermMeta {
    pub(super) term_off: usize,
    pub(super) term_len: usize,
    pub(super) df: u32,
    pub(super) n_blocks: u32,
    pub(super) blocks_off: usize,
    pub(super) postings_off: usize,
    pub(super) global_max_impact: u16,
    pub(super) global_max_scale: f32,
}

pub(super) fn splade_invalid(msg: &str) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, msg.to_string())
}

pub(super) fn parse_splade_term<'a>(
    bytes: &'a [u8],
    cur: &mut usize,
    prev_term: &mut Option<&'a [u8]>,
    base_scale: f32,
) -> io::Result<SpladeTermMeta> {
    if *cur + 2 > bytes.len() {
        return Err(splade_invalid("truncated SPLADE term length"));
    }
    let term_len = u16::from_le_bytes(bytes[*cur..*cur + 2].try_into().unwrap()) as usize;
    *cur += 2;
    if *cur + term_len + 8 > bytes.len() {
        return Err(splade_invalid("truncated SPLADE term header"));
    }
    let term_slice = &bytes[*cur..*cur + term_len];
    std::str::from_utf8(term_slice).map_err(|_| splade_invalid("invalid utf-8 in SPLADE term"))?;
    if let Some(prev) = *prev_term {
        if term_slice <= prev {
            return Err(splade_invalid("SPLADE terms are not strictly byte-sorted"));
        }
    }
    *prev_term = Some(term_slice);
    let term_off = *cur;
    *cur += term_len;
    let df = u32::from_le_bytes(bytes[*cur..*cur + 4].try_into().unwrap());
    let n_blocks = u32::from_le_bytes(bytes[*cur + 4..*cur + 8].try_into().unwrap());
    *cur += 8;
    if n_blocks as usize != (df as usize).div_ceil(SPLADE_BLOCK_SIZE) {
        return Err(splade_invalid("SPLADE block count disagrees with df"));
    }
    let blocks_bytes = (n_blocks as usize)
        .checked_mul(SPLADE_BLOCK_STRIDE)
        .ok_or_else(|| splade_invalid("SPLADE blocks overflow"))?;
    let postings_bytes = (df as usize)
        .checked_mul(SPLADE_POSTING_STRIDE)
        .ok_or_else(|| splade_invalid("SPLADE postings overflow"))?;
    if *cur + blocks_bytes + postings_bytes > bytes.len() {
        return Err(splade_invalid("truncated SPLADE blocks or postings"));
    }
    let blocks_off = *cur;
    let mut global_max_impact = 0u16;
    let mut global_max_scale = base_scale;
    for b in 0..n_blocks as usize {
        let bo = blocks_off + b * SPLADE_BLOCK_STRIDE;
        let bmax = u16::from_le_bytes(bytes[bo + 4..bo + 6].try_into().unwrap());
        global_max_impact = global_max_impact.max(bmax);
        let blen = u16::from_le_bytes(bytes[bo + 6..bo + 8].try_into().unwrap()) as usize;
        if blen != (df as usize - b * SPLADE_BLOCK_SIZE).min(SPLADE_BLOCK_SIZE) {
            return Err(splade_invalid("SPLADE block length disagrees with df"));
        }
        let bs = f32::from_le_bytes(bytes[bo + 8..bo + 12].try_into().unwrap());
        if !bs.is_finite() || bs <= 0.0 {
            return Err(splade_invalid("invalid SPLADE block scale"));
        }
        global_max_scale = global_max_scale.max(bs);
    }
    let postings_off = blocks_off + blocks_bytes;
    *cur = postings_off + postings_bytes;
    Ok(SpladeTermMeta {
        term_off,
        term_len,
        df,
        n_blocks,
        blocks_off,
        postings_off,
        global_max_impact,
        global_max_scale,
    })
}

pub(super) struct SpladeCursor<'b> {
    pub(super) meta: &'b SpladeTermMeta,
    pub(super) q_weight: f32,
    pub(super) upper_bound: f32,
    pub(super) block_idx: usize,
    pub(super) in_block_idx: usize,
    pub(super) posting_abs_idx: usize,
    pub(super) cur_doc: u32,
    pub(super) block_entered: bool,
}

impl<'b> SpladeCursor<'b> {
    pub(super) fn new(bytes: &[u8], meta: &'b SpladeTermMeta, q_weight: f32) -> Self {
        let cur_doc = if meta.df > 0 {
            u32::from_le_bytes(
                bytes[meta.postings_off..meta.postings_off + 4]
                    .try_into()
                    .unwrap(),
            )
        } else {
            u32::MAX
        };
        Self {
            meta,
            q_weight,
            upper_bound: q_weight * f32::from(meta.global_max_impact) * meta.global_max_scale,
            block_idx: 0,
            in_block_idx: 0,
            posting_abs_idx: 0,
            cur_doc,
            block_entered: false,
        }
    }

    pub(super) fn mark_block_entered(&mut self, stats: &mut SpladeTraversalStats) {
        if !self.block_entered && self.block_idx < self.meta.n_blocks as usize {
            self.block_entered = true;
            stats.blocks_visited += 1;
        }
    }

    pub(super) fn finish_remaining_blocks(&mut self, stats: &mut SpladeTraversalStats) {
        if self.block_idx < self.meta.n_blocks as usize {
            if !self.block_entered {
                stats.blocks_skipped += 1;
            }
            let after = (self.meta.n_blocks as usize).saturating_sub(self.block_idx + 1);
            stats.blocks_skipped += after as u64;
            self.block_idx = self.meta.n_blocks as usize;
            self.block_entered = false;
        }
    }

    pub(super) fn block_header(&self, bytes: &[u8], b: usize) -> (u32, u16, u16, f32) {
        let bo = self.meta.blocks_off + b * SPLADE_BLOCK_STRIDE;
        let max_doc = u32::from_le_bytes(bytes[bo..bo + 4].try_into().unwrap());
        let max_q = u16::from_le_bytes(bytes[bo + 4..bo + 6].try_into().unwrap());
        let blen = u16::from_le_bytes(bytes[bo + 6..bo + 8].try_into().unwrap());
        let max_scale = f32::from_le_bytes(bytes[bo + 8..bo + 12].try_into().unwrap());
        (max_doc, max_q, blen, max_scale)
    }

    pub(super) fn block_upper_bound(&self, bytes: &[u8]) -> f32 {
        if self.block_idx >= self.meta.n_blocks as usize {
            return 0.0;
        }
        let (_, max_q, _, scale) = self.block_header(bytes, self.block_idx);
        self.q_weight * f32::from(max_q) * scale
    }

    pub(super) fn current_impact(&self, bytes: &[u8]) -> f32 {
        let p = self.meta.postings_off + self.posting_abs_idx * SPLADE_POSTING_STRIDE;
        let q = u16::from_le_bytes(bytes[p + 4..p + 6].try_into().unwrap());
        let scale = f32::from_le_bytes(bytes[p + 6..p + 10].try_into().unwrap());
        self.q_weight * f32::from(q) * scale
    }

    pub(super) fn advance_one(&mut self, bytes: &[u8], stats: &mut SpladeTraversalStats) {
        self.mark_block_entered(stats);
        self.posting_abs_idx += 1;
        if self.posting_abs_idx >= self.meta.df as usize {
            self.cur_doc = u32::MAX;
            self.block_idx = self.meta.n_blocks as usize;
            self.block_entered = false;
            return;
        }
        let (_, _, blen, _) = self.block_header(bytes, self.block_idx);
        self.in_block_idx += 1;
        if self.in_block_idx >= blen as usize {
            self.block_idx += 1;
            self.in_block_idx = 0;
            self.block_entered = false;
        }
        let p = self.meta.postings_off + self.posting_abs_idx * SPLADE_POSTING_STRIDE;
        self.cur_doc = u32::from_le_bytes(bytes[p..p + 4].try_into().unwrap());
    }

    pub(super) fn seek_ge(&mut self, bytes: &[u8], target: u32, stats: &mut SpladeTraversalStats) {
        if self.cur_doc >= target || self.cur_doc == u32::MAX {
            return;
        }
        let mut jumped = false;
        while self.block_idx < self.meta.n_blocks as usize {
            let (max_doc, _, _, _) = self.block_header(bytes, self.block_idx);
            if max_doc >= target {
                break;
            }
            if !self.block_entered {
                stats.blocks_skipped += 1;
            }
            self.block_idx += 1;
            self.in_block_idx = 0;
            self.block_entered = false;
            jumped = true;
        }
        if self.block_idx >= self.meta.n_blocks as usize {
            self.posting_abs_idx = self.meta.df as usize;
            self.cur_doc = u32::MAX;
            return;
        }
        if jumped {
            self.posting_abs_idx = self.block_idx * SPLADE_BLOCK_SIZE;
        }
        let (_, _, blen, _) = self.block_header(bytes, self.block_idx);
        let block_start = self.block_idx * SPLADE_BLOCK_SIZE;
        let block_end = block_start + blen as usize;
        while self.posting_abs_idx < block_end {
            let p = self.meta.postings_off + self.posting_abs_idx * SPLADE_POSTING_STRIDE;
            let d = u32::from_le_bytes(bytes[p..p + 4].try_into().unwrap());
            if d >= target {
                self.cur_doc = d;
                self.in_block_idx = self.posting_abs_idx - block_start;
                if self.in_block_idx > 0 {
                    self.mark_block_entered(stats);
                }
                return;
            }
            self.mark_block_entered(stats);
            self.posting_abs_idx += 1;
        }
        self.cur_doc = u32::MAX;
    }
}

pub(super) struct TopKCollector {
    pub(super) heap: Vec<(u32, f32)>,
    pub(super) k: usize,
    pub(super) floor: f32,
    pub(super) threshold: f32,
}

impl TopKCollector {
    pub(super) fn push(&mut self, pivot_doc: u32, score: f32) {
        if (self.heap.len() < self.k && score >= self.floor)
            || score > self.threshold
            || (self.heap.len() == self.k
                && score == self.threshold
                && pivot_doc < self.heap.last().unwrap().0)
        {
            let pos = self
                .heap
                .partition_point(|&(d, s)| s > score || (s == score && d < pivot_doc));
            self.heap.insert(pos, (pivot_doc, score));
            if self.heap.len() > self.k {
                self.heap.pop();
            }
            if self.heap.len() == self.k {
                self.threshold = self.threshold.max(self.heap.last().unwrap().1);
            }
        }
    }
}

pub(super) fn find_wand_pivot(cursors: &[SpladeCursor<'_>], effective_floor: f32) -> Option<usize> {
    let mut cum_ub = 0.0_f64;
    for (idx, c) in cursors.iter().enumerate() {
        if c.cur_doc == u32::MAX {
            break;
        }
        cum_ub += f64::from(c.upper_bound);
        if conservative_sum(cum_ub, idx + 1) > f64::from(effective_floor) {
            return Some(idx);
        }
    }
    None
}

pub(super) fn finish_all_cursors(
    cursors: &mut [SpladeCursor<'_>],
    stats: &mut SpladeTraversalStats,
) {
    for c in cursors {
        c.finish_remaining_blocks(stats);
    }
}
