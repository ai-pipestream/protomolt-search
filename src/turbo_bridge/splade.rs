//! Quantized `u8`/`u16` SPLADE 128-doc Block-Max postings and L1 rounding
//! slack traversal (`#104`, `#2343`).
//!
//! Stores calibrated integer term impacts (`u8` or `u16` widened to `u16` in
//! 128-posting blocks) inside `.bm25` `COLUMN_KIND_SPLADE` (`19`, named
//! `splade:<field>`), enabling zero-copy memory-mapped Block-Max MaxScore/WAND
//! retrieval with an exact L1 query rounding upper-bound certificate.

use std::collections::BTreeMap;
use std::io::{self, Write};

/// Postings per SPLADE skip block, matching BM25 v5 level-0 blocks (`128`).
pub const SPLADE_BLOCK_SIZE: usize = 128;

/// Magic bytes at the start of every `COLUMN_KIND_SPLADE` section.
pub const SPLADE_SECTION_MAGIC: &[u8; 8] = b"TVSPLD01";

/// Telemetry returned by a Block-Max SPLADE query evaluation.
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq)]
pub struct SpladeTraversalStats {
    /// Total 128-doc blocks across queried terms.
    pub total_blocks: u64,
    /// Blocks decoded and scored.
    pub blocks_visited: u64,
    /// Blocks skipped completely via their `block_max_impact` upper bound.
    pub blocks_skipped: u64,
    /// Individual `(doc_id, impact)` postings accumulated.
    pub postings_scored: u64,
}

/// Deterministically quantizes positive float SPLADE weights `(term, weight)`
/// into top-`max_terms` integer impacts (`bits = 8` -> `[1..=255]`, `bits = 16`
/// -> `[1..=65535]`), returning `(scale, quantized_terms, l1_rounding_bound)`.
///
/// The L1 rounding upper bound `sum_i |w_i - q_i * scale|` is exact for the
/// emitted vector and bounds the worst-case inner-product underestimation when
/// paired with unit-bounded or L_inf-bounded query weights.
pub fn quantize_splade_impacts(
    weights: &[(String, f32)],
    max_terms: usize,
    bits: u8,
) -> (f32, Vec<(String, u16)>, f32) {
    let mut positive: Vec<(String, f32)> = weights
        .iter()
        .filter(|(t, w)| !t.is_empty() && w.is_finite() && *w > 0.0)
        .cloned()
        .collect();
    if positive.is_empty() || max_terms == 0 {
        return (0.0, Vec::new(), 0.0);
    }
    positive.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    if positive.len() > max_terms {
        positive.truncate(max_terms);
    }
    let max_val = positive[0].1;
    let levels = if bits <= 8 { 255.0_f32 } else { 65535.0_f32 };
    let q_max = if bits <= 8 { 255u16 } else { u16::MAX };
    let scale = max_val / levels;
    if scale <= 0.0 || !scale.is_finite() {
        return (0.0, Vec::new(), 0.0);
    }

    let mut by_term: BTreeMap<String, (u16, f32)> = BTreeMap::new();
    for (term, w) in positive {
        let q = ((w / scale).round().clamp(1.0, f32::from(q_max))) as u16;
        by_term
            .entry(term)
            .and_modify(|e| {
                if q > e.0 {
                    *e = (q, w);
                }
            })
            .or_insert((q, w));
    }

    let mut l1_err = 0.0_f32;
    let mut out = Vec::with_capacity(by_term.len());
    for (term, (q, w)) in by_term {
        let recon = f32::from(q) * scale;
        l1_err += (w - recon).abs();
        out.push((term, q));
    }
    (scale, out, l1_err)
}

/// Heap-resident quantized SPLADE inverted index for one field on one shard.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SpladeFieldIndex {
    /// IEEE-754 `f32::to_bits()` of the impact quantization scale (`0` when empty).
    pub scale_bits: u32,
    /// Term -> doc-id-ordered `(doc_id, quantized_impact)` postings.
    pub postings: BTreeMap<String, Vec<(u32, u16)>>,
}

impl SpladeFieldIndex {
    /// Returns `true` when no SPLADE postings have been indexed.
    pub fn is_empty(&self) -> bool {
        self.postings.is_empty()
    }

    /// Appends one document's quantized SPLADE impacts (`doc_id` strictly ascending).
    pub fn insert_doc(&mut self, doc_id: u32, scale_bits: u32, impacts: &[(String, u16)]) {
        if impacts.is_empty() {
            return;
        }
        let doc_scale = f32::from_bits(scale_bits);
        if self.scale_bits == 0 && doc_scale > 0.0 && doc_scale.is_finite() {
            self.scale_bits = scale_bits;
        }
        let target_scale = f32::from_bits(self.scale_bits);
        for (term, raw_q) in impacts {
            if *raw_q == 0 || term.is_empty() {
                continue;
            }
            let q =
                if target_scale > 0.0 && doc_scale > 0.0 && (doc_scale - target_scale).abs() > 1e-9
                {
                    ((f32::from(*raw_q) * doc_scale / target_scale)
                        .round()
                        .clamp(1.0, 65535.0)) as u16
                } else {
                    *raw_q
                };
            let list = self.postings.entry(term.clone()).or_default();
            if let Some(last) = list.last_mut() {
                if last.0 == doc_id {
                    last.1 = last.1.max(q);
                    continue;
                }
            }
            list.push((doc_id, q));
        }
    }

    /// Exact serialized byte length of this section (`SPLADE_SECTION_MAGIC` layout).
    pub fn encoded_len(&self) -> u64 {
        if self.postings.is_empty() {
            return 0;
        }
        // magic (8) + scale_bits (4) + n_terms (4)
        let mut size = 16u64;
        for (term, plist) in &self.postings {
            let n_blocks = plist.len().div_ceil(SPLADE_BLOCK_SIZE) as u64;
            // term_len (2) + term + df (4) + n_blocks (4) + blocks (n_blocks * 8) + postings (df * 6)
            size += 2 + term.len() as u64 + 4 + 4 + n_blocks * 8 + (plist.len() as u64) * 6;
        }
        size
    }

    /// Writes the binary `TVSPLD01` section to `w`.
    pub fn write<W: Write>(&self, w: &mut W) -> io::Result<()> {
        if self.postings.is_empty() {
            return Ok(());
        }
        w.write_all(SPLADE_SECTION_MAGIC)?;
        w.write_all(&self.scale_bits.to_le_bytes())?;
        w.write_all(&(self.postings.len() as u32).to_le_bytes())?;
        for (term, plist) in &self.postings {
            let term_bytes = term.as_bytes();
            w.write_all(&(term_bytes.len() as u16).to_le_bytes())?;
            w.write_all(term_bytes)?;
            w.write_all(&(plist.len() as u32).to_le_bytes())?;
            let n_blocks = plist.len().div_ceil(SPLADE_BLOCK_SIZE);
            w.write_all(&(n_blocks as u32).to_le_bytes())?;
            for chunk in plist.chunks(SPLADE_BLOCK_SIZE) {
                let max_doc = chunk.last().map_or(0, |&(d, _)| d);
                let max_impact = chunk.iter().map(|&(_, q)| q).max().unwrap_or(0);
                let block_len = chunk.len() as u16;
                w.write_all(&max_doc.to_le_bytes())?;
                w.write_all(&max_impact.to_le_bytes())?;
                w.write_all(&block_len.to_le_bytes())?;
            }
            for &(doc_id, q) in plist {
                w.write_all(&doc_id.to_le_bytes())?;
                w.write_all(&q.to_le_bytes())?;
            }
        }
        Ok(())
    }

    /// Decodes a `TVSPLD01` section back into a [`SpladeFieldIndex`].
    pub fn decode(bytes: &[u8]) -> io::Result<Self> {
        let reader = SpladeSectionReader::open(bytes)?;
        let mut postings = BTreeMap::new();
        for entry in &reader.terms {
            let term = std::str::from_utf8(&bytes[entry.term_off..entry.term_off + entry.term_len])
                .map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "invalid utf-8 in splade term")
                })?
                .to_string();
            let mut plist = Vec::with_capacity(entry.df as usize);
            for j in 0..entry.df as usize {
                let p = entry.postings_off + j * 6;
                let doc_id = u32::from_le_bytes(bytes[p..p + 4].try_into().unwrap());
                let q = u16::from_le_bytes(bytes[p + 4..p + 6].try_into().unwrap());
                plist.push((doc_id, q));
            }
            postings.insert(term, plist);
        }
        Ok(Self {
            scale_bits: reader.scale_bits,
            postings,
        })
    }

    /// Evaluates top-`k` documents using 128-doc Block-Max upper bounds and
    /// `query_l1_slack` rounding slack so quantized pruning never drops a true
    /// top-`k` candidate within the calibration bound.
    pub fn score_top_k<F>(
        &self,
        query_weights: &[(String, f32)],
        query_l1_slack: f32,
        k: usize,
        admitted: F,
    ) -> (Vec<(u32, f32)>, SpladeTraversalStats)
    where
        F: Fn(u32) -> bool,
    {
        if k == 0 || self.postings.is_empty() || query_weights.is_empty() {
            return (Vec::new(), SpladeTraversalStats::default());
        }
        let mut buf = Vec::with_capacity(self.encoded_len() as usize);
        self.write(&mut buf)
            .expect("in-memory SpladeFieldIndex encode");
        let reader = SpladeSectionReader::open(&buf).expect("valid encoded SpladeFieldIndex");
        reader.score_top_k(query_weights, query_l1_slack, k, admitted)
    }
}

#[derive(Clone, Debug)]
struct SpladeTermMeta {
    term_off: usize,
    term_len: usize,
    df: u32,
    n_blocks: u32,
    blocks_off: usize,
    postings_off: usize,
    global_max_impact: u16,
}

/// Zero-copy memory-mapped reader over a `TVSPLD01` (`COLUMN_KIND_SPLADE`) section.
#[derive(Clone, Debug)]
pub struct SpladeSectionReader<'a> {
    bytes: &'a [u8],
    scale_bits: u32,
    terms: Vec<SpladeTermMeta>,
}

impl<'a> SpladeSectionReader<'a> {
    /// Validates and opens a `TVSPLD01` byte slice without copying posting runs.
    pub fn open(bytes: &'a [u8]) -> io::Result<Self> {
        let invalid = |msg: &str| io::Error::new(io::ErrorKind::InvalidData, msg.to_string());
        if bytes.len() < 16 || &bytes[..8] != SPLADE_SECTION_MAGIC {
            return Err(invalid("invalid SPLADE section magic"));
        }
        let scale_bits = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        let n_terms = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
        let mut cur = 16usize;
        let mut terms = Vec::with_capacity(n_terms);
        let mut prev_term: Option<&[u8]> = None;
        for _ in 0..n_terms {
            if cur + 2 > bytes.len() {
                return Err(invalid("truncated SPLADE term length"));
            }
            let term_len = u16::from_le_bytes(bytes[cur..cur + 2].try_into().unwrap()) as usize;
            cur += 2;
            if cur + term_len + 8 > bytes.len() {
                return Err(invalid("truncated SPLADE term header"));
            }
            let term_slice = &bytes[cur..cur + term_len];
            std::str::from_utf8(term_slice).map_err(|_| invalid("invalid utf-8 in SPLADE term"))?;
            if let Some(prev) = prev_term {
                if term_slice <= prev {
                    return Err(invalid("SPLADE terms are not strictly byte-sorted"));
                }
            }
            prev_term = Some(term_slice);
            let term_off = cur;
            cur += term_len;
            let df = u32::from_le_bytes(bytes[cur..cur + 4].try_into().unwrap());
            let n_blocks = u32::from_le_bytes(bytes[cur + 4..cur + 8].try_into().unwrap());
            cur += 8;
            if n_blocks as usize != (df as usize).div_ceil(SPLADE_BLOCK_SIZE) {
                return Err(invalid("SPLADE block count disagrees with df"));
            }
            let blocks_bytes = (n_blocks as usize)
                .checked_mul(8)
                .ok_or_else(|| invalid("SPLADE blocks overflow"))?;
            let postings_bytes = (df as usize)
                .checked_mul(6)
                .ok_or_else(|| invalid("SPLADE postings overflow"))?;
            if cur + blocks_bytes + postings_bytes > bytes.len() {
                return Err(invalid("truncated SPLADE blocks or postings"));
            }
            let blocks_off = cur;
            let mut global_max = 0u16;
            for b in 0..n_blocks as usize {
                let bo = blocks_off + b * 8;
                let bmax = u16::from_le_bytes(bytes[bo + 4..bo + 6].try_into().unwrap());
                global_max = global_max.max(bmax);
            }
            let postings_off = blocks_off + blocks_bytes;
            cur = postings_off + postings_bytes;
            terms.push(SpladeTermMeta {
                term_off,
                term_len,
                df,
                n_blocks,
                blocks_off,
                postings_off,
                global_max_impact: global_max,
            });
        }
        if cur != bytes.len() {
            return Err(invalid("trailing bytes in SPLADE section"));
        }
        Ok(Self {
            bytes,
            scale_bits,
            terms,
        })
    }

    fn lookup_term(&self, needle: &str) -> Option<&SpladeTermMeta> {
        let nb = needle.as_bytes();
        let idx = self
            .terms
            .binary_search_by(|m| self.bytes[m.term_off..m.term_off + m.term_len].cmp(nb))
            .ok()?;
        self.terms.get(idx)
    }

    /// Document-at-a-time Block-Max MaxScore / WAND top-`k` traversal over the
    /// memory-mapped section, subtracting `query_l1_slack` from the pruning
    /// threshold so L1 quantization rounding never causes a false-negative prune.
    pub fn score_top_k<F>(
        &self,
        query_weights: &[(String, f32)],
        query_l1_slack: f32,
        k: usize,
        admitted: F,
    ) -> (Vec<(u32, f32)>, SpladeTraversalStats)
    where
        F: Fn(u32) -> bool,
    {
        let mut stats = SpladeTraversalStats::default();
        let doc_scale = f32::from_bits(self.scale_bits);
        if k == 0 || doc_scale <= 0.0 || !doc_scale.is_finite() {
            return (Vec::new(), stats);
        }
        let slack = if query_l1_slack.is_finite() && query_l1_slack > 0.0 {
            query_l1_slack
        } else {
            0.0
        };

        struct Cursor<'b> {
            meta: &'b SpladeTermMeta,
            q_weight: f32,
            upper_bound: f32,
            block_idx: usize,
            in_block_idx: usize,
            posting_abs_idx: usize,
            cur_doc: u32,
            block_entered: bool,
        }

        impl<'b> Cursor<'b> {
            fn new(bytes: &[u8], meta: &'b SpladeTermMeta, q_weight: f32, doc_scale: f32) -> Self {
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
                    upper_bound: q_weight * f32::from(meta.global_max_impact) * doc_scale,
                    block_idx: 0,
                    in_block_idx: 0,
                    posting_abs_idx: 0,
                    cur_doc,
                    block_entered: false,
                }
            }

            fn mark_block_entered(&mut self, stats: &mut SpladeTraversalStats) {
                if !self.block_entered && self.block_idx < self.meta.n_blocks as usize {
                    self.block_entered = true;
                    stats.blocks_visited += 1;
                }
            }

            fn finish_remaining_blocks(&mut self, stats: &mut SpladeTraversalStats) {
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

            fn block_header(&self, bytes: &[u8], b: usize) -> (u32, u16, u16) {
                let bo = self.meta.blocks_off + b * 8;
                let max_doc = u32::from_le_bytes(bytes[bo..bo + 4].try_into().unwrap());
                let max_q = u16::from_le_bytes(bytes[bo + 4..bo + 6].try_into().unwrap());
                let blen = u16::from_le_bytes(bytes[bo + 6..bo + 8].try_into().unwrap());
                (max_doc, max_q, blen)
            }

            fn block_upper_bound(&self, bytes: &[u8], doc_scale: f32) -> f32 {
                if self.block_idx >= self.meta.n_blocks as usize {
                    return 0.0;
                }
                let (_, max_q, _) = self.block_header(bytes, self.block_idx);
                self.q_weight * f32::from(max_q) * doc_scale
            }

            fn current_impact(&self, bytes: &[u8], doc_scale: f32) -> f32 {
                let p = self.meta.postings_off + self.posting_abs_idx * 6;
                let q = u16::from_le_bytes(bytes[p + 4..p + 6].try_into().unwrap());
                self.q_weight * f32::from(q) * doc_scale
            }

            fn advance_one(&mut self, bytes: &[u8], stats: &mut SpladeTraversalStats) {
                self.mark_block_entered(stats);
                self.posting_abs_idx += 1;
                if self.posting_abs_idx >= self.meta.df as usize {
                    self.cur_doc = u32::MAX;
                    self.block_idx = self.meta.n_blocks as usize;
                    self.block_entered = false;
                    return;
                }
                let (_, _, blen) = self.block_header(bytes, self.block_idx);
                self.in_block_idx += 1;
                if self.in_block_idx >= blen as usize {
                    self.block_idx += 1;
                    self.in_block_idx = 0;
                    self.block_entered = false;
                }
                let p = self.meta.postings_off + self.posting_abs_idx * 6;
                self.cur_doc = u32::from_le_bytes(bytes[p..p + 4].try_into().unwrap());
            }

            fn seek_ge(&mut self, bytes: &[u8], target: u32, stats: &mut SpladeTraversalStats) {
                if self.cur_doc >= target || self.cur_doc == u32::MAX {
                    return;
                }
                // Skip whole 128-posting blocks whose max_doc < target!
                let mut jumped = false;
                while self.block_idx < self.meta.n_blocks as usize {
                    let (max_doc, _, _) = self.block_header(bytes, self.block_idx);
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
                let (_, _, blen) = self.block_header(bytes, self.block_idx);
                let block_start = self.block_idx * SPLADE_BLOCK_SIZE;
                let block_end = block_start + blen as usize;
                while self.posting_abs_idx < block_end {
                    let p = self.meta.postings_off + self.posting_abs_idx * 6;
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

        // Merge duplicate query terms cleanly.
        let mut qmap: BTreeMap<&str, f32> = BTreeMap::new();
        for (t, w) in query_weights {
            if w.is_finite() && *w > 0.0 {
                *qmap.entry(t.as_str()).or_insert(0.0) += *w;
            }
        }
        let mut cursors: Vec<Cursor<'_>> = Vec::with_capacity(qmap.len());
        for (term, qw) in qmap {
            if let Some(meta) = self.lookup_term(term) {
                if meta.df > 0 {
                    stats.total_blocks += u64::from(meta.n_blocks);
                    cursors.push(Cursor::new(self.bytes, meta, qw, doc_scale));
                }
            }
        }
        if cursors.is_empty() {
            return (Vec::new(), stats);
        }

        // Min-heap of top-k `(score_bits_ordered, doc_id)` kept in a small sorted Vec
        // (k is typically 10..1000; binary-search insert is cache-hot and exact).
        let mut heap: Vec<(u32, f32)> = Vec::with_capacity(k + 1);
        let mut threshold = 0.0_f32;

        loop {
            // Sort active cursors by current doc_id ascending, breaking ties by upper_bound desc.
            cursors.sort_by(|a, b| {
                a.cur_doc
                    .cmp(&b.cur_doc)
                    .then_with(|| b.upper_bound.total_cmp(&a.upper_bound))
            });
            if cursors[0].cur_doc == u32::MAX {
                for c in &mut cursors {
                    c.finish_remaining_blocks(&mut stats);
                }
                break;
            }

            let effective_floor = (threshold - slack).max(0.0);

            // WAND pivot selection using per-term global upper bounds.
            let mut cum_ub = 0.0_f32;
            let mut pivot_idx = None;
            for (idx, c) in cursors.iter().enumerate() {
                if c.cur_doc == u32::MAX {
                    break;
                }
                cum_ub += c.upper_bound;
                if cum_ub > effective_floor {
                    pivot_idx = Some(idx);
                    break;
                }
            }
            let Some(p_idx) = pivot_idx else {
                // Remaining upper bounds across all active cursors cannot beat threshold!
                for c in &mut cursors {
                    c.finish_remaining_blocks(&mut stats);
                }
                break;
            };

            let pivot_doc = cursors[p_idx].cur_doc;
            if pivot_doc == u32::MAX {
                for c in &mut cursors {
                    c.finish_remaining_blocks(&mut stats);
                }
                break;
            }

            if cursors[0].cur_doc == pivot_doc {
                // Refine with current 128-doc BLOCK upper bounds at `pivot_doc`!
                let mut block_ub = 0.0_f32;
                let mut end_same = 0usize;
                for c in &cursors {
                    if c.cur_doc != pivot_doc {
                        break;
                    }
                    block_ub += c.block_upper_bound(self.bytes, doc_scale);
                    end_same += 1;
                }
                if block_ub <= effective_floor {
                    // Block-Max bound rejects `pivot_doc` without scoring! Jump past the
                    // tightest block boundary among aligned cursors.
                    let mut next_target = pivot_doc.saturating_add(1);
                    let mut min_block_end = u32::MAX;
                    for c in &cursors[..end_same] {
                        let (b_max_doc, _, _) = c.block_header(self.bytes, c.block_idx);
                        min_block_end = min_block_end.min(b_max_doc);
                    }
                    if min_block_end < u32::MAX {
                        next_target = next_target.max(min_block_end.saturating_add(1));
                    }
                    for c in &mut cursors[..end_same] {
                        c.seek_ge(self.bytes, next_target, &mut stats);
                    }
                    continue;
                }

                // Candidate passes Block-Max gate: score if admitted!
                for c in &mut cursors[..end_same] {
                    c.mark_block_entered(&mut stats);
                }
                if admitted(pivot_doc) {
                    let mut score = 0.0_f32;
                    for c in &cursors[..end_same] {
                        score += c.current_impact(self.bytes, doc_scale);
                        stats.postings_scored += 1;
                    }
                    if heap.len() < k
                        || score > threshold
                        || (score == threshold && pivot_doc < heap.last().unwrap().0)
                    {
                        let pos = heap
                            .partition_point(|&(d, s)| s > score || (s == score && d < pivot_doc));
                        heap.insert(pos, (pivot_doc, score));
                        if heap.len() > k {
                            heap.pop();
                        }
                        if heap.len() == k {
                            threshold = heap.last().unwrap().1;
                        }
                    }
                }
                for c in &mut cursors[..end_same] {
                    c.advance_one(self.bytes, &mut stats);
                }
            } else {
                // Advance leading cursors (`< pivot_doc`) directly to `pivot_doc`, skipping blocks!
                for c in &mut cursors[..p_idx] {
                    c.seek_ge(self.bytes, pivot_doc, &mut stats);
                }
            }
        }

        (heap, stats)
    }
}
