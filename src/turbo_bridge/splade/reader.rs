//! Zero-copy reader over an encoded SPLADE section.

use super::cursor::*;
use super::*;

/// Zero-copy memory-mapped reader over a `TVSPLD02` (`COLUMN_KIND_SPLADE`) section.
#[derive(Clone, Debug)]
pub struct SpladeSectionReader<'a> {
    bytes: &'a [u8],
    pub(super) scale_bits: u32,
    pub(super) terms: Vec<SpladeTermMeta>,
}

impl<'a> SpladeSectionReader<'a> {
    /// Validates and opens a `TVSPLD02` SPLADE byte slice without copying posting runs.
    pub fn open(bytes: &'a [u8]) -> io::Result<Self> {
        if bytes.len() < 16 || &bytes[..8] != SPLADE_SECTION_MAGIC {
            return Err(splade_invalid("invalid SPLADE section magic"));
        }
        let scale_bits = u32::from_le_bytes(bytes[8..12].try_into().unwrap());
        let scale = f32::from_bits(scale_bits);
        if !scale.is_finite() || scale <= 0.0 {
            return Err(splade_invalid("invalid SPLADE scale"));
        }
        let n_terms = u32::from_le_bytes(bytes[12..16].try_into().unwrap()) as usize;
        if n_terms > bytes.len() / 10 {
            return Err(splade_invalid("excessive SPLADE term count"));
        }
        let mut cur = 16usize;
        let mut terms = Vec::with_capacity(n_terms);
        let mut prev_term: Option<&[u8]> = None;
        for _ in 0..n_terms {
            terms.push(parse_splade_term(bytes, &mut cur, &mut prev_term, scale)?);
        }
        if cur != bytes.len() {
            return Err(splade_invalid("trailing bytes in SPLADE section"));
        }
        Ok(Self {
            bytes,
            scale_bits,
            terms,
        })
    }

    /// Walk a section already checked by `open`, without allocating the term
    /// metadata vector. Source rewrites must stay bounded even for a field
    /// whose sparse vocabulary is much larger than a single output batch.
    pub(crate) fn visit_validated_range<E>(
        bytes: &[u8],
        start: u32,
        end: u32,
        mut visit: impl FnMut(&str, u32, u16, u32) -> Result<(), E>,
    ) -> Result<(), E> {
        let u32_at = |at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap());
        let mut at = 16;
        for _ in 0..u32_at(12) {
            let len = u16::from_le_bytes(bytes[at..at + 2].try_into().unwrap()) as usize;
            at += 2;
            let name = std::str::from_utf8(&bytes[at..at + len]).expect("validated term");
            at += len;
            let df = u32_at(at) as usize;
            let blocks = u32_at(at + 4) as usize;
            let postings = at + 8 + blocks * SPLADE_BLOCK_STRIDE;
            at = postings + df * SPLADE_POSTING_STRIDE;
            let doc_at = |i: usize| u32_at(postings + i * SPLADE_POSTING_STRIDE);
            let (mut lo, mut hi) = (0, df);
            while lo < hi {
                let mid = (lo + hi) / 2;
                if doc_at(mid) < start {
                    lo = mid + 1;
                } else {
                    hi = mid;
                }
            }
            for i in lo..df {
                let doc = doc_at(i);
                if doc >= end {
                    break;
                }
                let p = postings + i * SPLADE_POSTING_STRIDE;
                let q = u16::from_le_bytes(bytes[p + 4..p + 6].try_into().unwrap());
                let scale = u32_at(p + 6);
                visit(name, doc, q, scale)?;
            }
        }
        Ok(())
    }

    fn lookup_term(&self, needle: &str) -> Option<&SpladeTermMeta> {
        let nb = needle.as_bytes();
        let idx = self
            .terms
            .binary_search_by(|m| self.bytes[m.term_off..m.term_off + m.term_len].cmp(nb))
            .ok()?;
        self.terms.get(idx)
    }

    fn build_query_cursors(
        &self,
        query_weights: &[(String, f32)],
        stats: &mut SpladeTraversalStats,
    ) -> Vec<SpladeCursor<'_>> {
        let mut qmap: BTreeMap<&str, f32> = BTreeMap::new();
        for (t, w) in query_weights {
            if w.is_finite() && *w > 0.0 {
                *qmap.entry(t.as_str()).or_insert(0.0) += *w;
            }
        }
        let mut cursors = Vec::with_capacity(qmap.len());
        for (term, qw) in qmap {
            if let Some(meta) = self.lookup_term(term) {
                if meta.df > 0 {
                    stats.total_blocks += u64::from(meta.n_blocks);
                    cursors.push(SpladeCursor::new(self.bytes, meta, qw));
                }
            }
        }
        cursors
    }

    fn evaluate_aligned_pivot<F>(
        &self,
        cursors: &mut [SpladeCursor<'_>],
        pivot_doc: u32,
        effective_floor: f32,
        admitted: &F,
        collector: &mut TopKCollector,
        stats: &mut SpladeTraversalStats,
    ) where
        F: Fn(u32) -> bool,
    {
        // Every query term can contribute before the next block boundary,
        // including cursors not yet aligned at the candidate. Summing all
        // current blocks permits whole-block skips without losing those hits.
        let mut block_ub = 0.0_f64;
        let mut min_block_end = u32::MAX;
        let mut end_same = 0usize;
        for c in cursors.iter() {
            if c.cur_doc == u32::MAX {
                break;
            }
            block_ub += f64::from(c.block_upper_bound(self.bytes));
            let (last_doc, _, _, _) = c.block_header(self.bytes, c.block_idx);
            min_block_end = min_block_end.min(last_doc);
            if c.cur_doc == pivot_doc {
                end_same += 1;
            }
        }
        if conservative_sum(block_ub, cursors.len()) <= f64::from(effective_floor) {
            let next_target = min_block_end.saturating_add(1);
            for c in cursors.iter_mut() {
                c.seek_ge(self.bytes, next_target, stats);
            }
            return;
        }
        for c in &mut cursors[..end_same] {
            c.mark_block_entered(stats);
        }
        if admitted(pivot_doc) {
            let mut score = 0.0_f32;
            for c in &cursors[..end_same] {
                score += c.current_impact(self.bytes);
                stats.postings_scored += 1;
            }
            collector.push(pivot_doc, score);
        }
        for c in &mut cursors[..end_same] {
            c.advance_one(self.bytes, stats);
        }
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
        let (hits, _, stats) =
            self.score_top_k_with_floor(query_weights, query_l1_slack, 0.0, k, admitted);
        (hits, stats)
    }

    /// Document-at-a-time Block-Max MaxScore / WAND top-`k` traversal seeded with
    /// a monotonic cross-shard `initial_floor`.
    pub fn score_top_k_with_floor<F>(
        &self,
        query_weights: &[(String, f32)],
        query_l1_slack: f32,
        initial_floor: f32,
        k: usize,
        admitted: F,
    ) -> (Vec<(u32, f32)>, f32, SpladeTraversalStats)
    where
        F: Fn(u32) -> bool,
    {
        let mut stats = SpladeTraversalStats::default();
        let floor = if initial_floor.is_finite() && initial_floor > 0.0 {
            initial_floor
        } else {
            0.0
        };
        let doc_scale = f32::from_bits(self.scale_bits);
        if k == 0 || doc_scale <= 0.0 || !doc_scale.is_finite() {
            return (Vec::new(), floor, stats);
        }
        let slack = if query_l1_slack.is_finite() && query_l1_slack > 0.0 {
            query_l1_slack
        } else {
            0.0
        };
        let mut cursors = self.build_query_cursors(query_weights, &mut stats);
        if cursors.is_empty() {
            return (Vec::new(), floor, stats);
        }
        let possible_matches = cursors.iter().fold(0usize, |total, cursor| {
            total.saturating_add(cursor.meta.df as usize)
        });
        let mut collector = TopKCollector {
            heap: Vec::with_capacity(k.saturating_add(1).min(possible_matches)),
            k,
            floor,
            threshold: floor,
        };

        loop {
            cursors.sort_by(|a, b| {
                a.cur_doc
                    .cmp(&b.cur_doc)
                    .then_with(|| a.meta.term_off.cmp(&b.meta.term_off))
            });
            if cursors[0].cur_doc == u32::MAX {
                finish_all_cursors(&mut cursors, &mut stats);
                break;
            }
            let effective_floor = (collector.threshold - slack).max(0.0);
            let Some(p_idx) = find_wand_pivot(&cursors, effective_floor) else {
                finish_all_cursors(&mut cursors, &mut stats);
                break;
            };
            let pivot_doc = cursors[p_idx].cur_doc;
            if pivot_doc == u32::MAX {
                finish_all_cursors(&mut cursors, &mut stats);
                break;
            }
            if cursors[0].cur_doc == pivot_doc {
                self.evaluate_aligned_pivot(
                    &mut cursors,
                    pivot_doc,
                    effective_floor,
                    &admitted,
                    &mut collector,
                    &mut stats,
                );
            } else {
                for c in &mut cursors[..p_idx] {
                    c.seek_ge(self.bytes, pivot_doc, &mut stats);
                }
            }
        }

        (collector.heap, collector.threshold, stats)
    }
}
