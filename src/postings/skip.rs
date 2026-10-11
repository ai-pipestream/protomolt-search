//! v5 skip-run encoding (`docs/block-max.md`): per-block `(tf, dl)` Pareto
//! frontiers, collapsed never dropped, and the two-level `SkipRunBuilder` (one
//! level-0 record per 128 postings, one level-1 record per 32 blocks). Stage 1
//! writes skip runs no scorer consumes yet.

use super::*;

use std::io::{self, Write};

/// Postings per level-0 skip block (Lucene uses 128/256; 128 here).
pub(crate) const BLOCK: usize = 128;
/// Level-0 blocks per level-1 record.
pub(crate) const LEVEL1_FACTOR: usize = 32;
/// Max `(tf, dl)` pairs kept per skip record; larger frontiers are
/// COLLAPSED (never dropped) to their dominating corner, which only
/// raises the bound — see `docs/block-max.md`.
pub(crate) const MAX_FRONTIER: usize = 8;

/// The `(tf, dl)` Pareto frontier of one block of postings, sorted by tf
/// ascending (dl ascending with it, strictly), dominated pairs pruned,
/// collapsed to at most [`MAX_FRONTIER`] entries.
///
/// `tf_norm` is non-decreasing in `tf` and non-increasing in `dl` for
/// every `k1 >= 0`, `b >= 0`, so a dominated pair (`tf' >= tf` and
/// `dl' <= dl` for some other pair) can never be the block maximum under
/// any parameter choice and is pruned at build time. When the frontier
/// exceeds the cap, adjacent entries are COLLAPSED into their dominating
/// corner `(max tf, min dl)` — never dropped, because which entry
/// maximizes `tf_norm` depends on the query's `avgdl`; collapsing only
/// ever raises the bound, costing tightness and never exactness.
pub(crate) fn pareto_frontier(pairs: &[(u32, u32)]) -> Vec<(u32, u32)> {
    let mut sorted: Vec<(u32, u32)> = pairs.to_vec();
    // tf descending, dl ascending within equal tf: walking forward and
    // keeping only strictly decreasing dl drops every dominated pair
    // (an equal-tf higher-dl pair is seen after its dominator).
    sorted.sort_unstable_by(|a, b| b.0.cmp(&a.0).then(a.1.cmp(&b.1)));
    let mut kept: Vec<(u32, u32)> = Vec::new();
    let mut min_dl = u32::MAX;
    for &(tf, dl) in &sorted {
        if dl < min_dl {
            kept.push((tf, dl));
            min_dl = dl;
        }
    }
    kept.reverse();
    while kept.len() > MAX_FRONTIER {
        // Collapse the closest adjacent pair into its dominating corner
        // (max tf, min dl) = (kept[i+1].0, kept[i].1). The staircase
        // stays strictly increasing in both coordinates.
        let mut best = 0;
        let mut best_gap = u64::MAX;
        for i in 0..kept.len() - 1 {
            let gap = u64::from(kept[i + 1].0 - kept[i].0) + u64::from(kept[i + 1].1 - kept[i].1);
            if gap < best_gap {
                best_gap = gap;
                best = i;
            }
        }
        let corner = (kept[best + 1].0, kept[best].1);
        kept.splice(best..=best + 1, [corner]);
    }
    kept
}

/// One flushed level-1 skip record (covers up to [`LEVEL1_FACTOR`]
/// level-0 blocks, i.e. up to 4096 postings).
pub(crate) struct L1Record {
    pub(crate) last_doc_id: u32,
    /// Offset of the group's first level-0 record, relative to the start
    /// of the term's skip run.
    pub(crate) l0_off: u64,
    pub(crate) pairs: Vec<(u32, u32)>,
}

impl L1Record {
    pub(crate) fn encoded_len(&self) -> u64 {
        13 + 8 * self.pairs.len() as u64
    }
}

/// Streaming skip-run accumulator for ONE term: fed `(tf, dl, doc_id)`
/// per posting in doc-id order, it writes each flushed level-0 record to
/// `l0_out` and accumulates the (few: df/4096) level-1 records in heap.
/// State per term is the current 128-posting block plus the current
/// level-1 group's merged frontier entries (<= 32 * 8 pairs), so build
/// memory stays O(1) in the term's df.
///
/// Skip-run layout (all integers little-endian):
///
/// ```text
/// u64 l1_region_off          <- offset of the first level-1 record,
///                               relative to the skip run's start
/// level-0 records:           one per 128-posting block, doc-id order
///   u32 last_doc_id, u8 n_pairs, n_pairs x (u32 tf, u32 dl)
/// level-1 records:           one per 32 level-0 blocks
///   u32 last_doc_id, u64 l0_off, u8 n_pairs, n_pairs x (u32 tf, u32 dl)
/// ```
///
/// The level-1 region needs the explicit offset because level-0 records
/// are variable stride, so its start is not derivable from df alone.
pub(crate) struct SkipRunBuilder {
    /// `(tf, dl)` of the current level-0 block (<= BLOCK entries).
    pub(crate) block: Vec<(u32, u32)>,
    /// Merged frontier entries of the current level-1 group.
    pub(crate) l1_pairs: Vec<(u32, u32)>,
    pub(crate) l1_records: Vec<L1Record>,
    pub(crate) l0_in_group: usize,
    /// Bytes written to `l0_out` so far.
    pub(crate) l0_bytes: u64,
    pub(crate) last_doc_id: u32,
}

impl SkipRunBuilder {
    pub(crate) fn new() -> Self {
        Self {
            block: Vec::with_capacity(BLOCK),
            l1_pairs: Vec::new(),
            l1_records: Vec::new(),
            l0_in_group: 0,
            l0_bytes: 0,
            last_doc_id: 0,
        }
    }

    pub(crate) fn push<W: Write>(
        &mut self,
        tf: u32,
        dl: u32,
        doc_id: u32,
        l0_out: &mut W,
    ) -> io::Result<()> {
        self.block.push((tf, dl));
        self.last_doc_id = doc_id;
        if self.block.len() == BLOCK {
            self.flush_block(l0_out)?;
        }
        Ok(())
    }

    pub(crate) fn flush_block<W: Write>(&mut self, l0_out: &mut W) -> io::Result<()> {
        if self.block.is_empty() {
            return Ok(());
        }
        if self.l0_in_group == 0 {
            // First level-0 record of a new level-1 group: its offset is
            // 8 (the prefix) plus the level-0 bytes written so far.
            self.l1_records.push(L1Record {
                last_doc_id: 0, // patched when the group closes
                l0_off: 8 + self.l0_bytes,
                pairs: Vec::new(),
            });
        }
        let frontier = pareto_frontier(&self.block);
        write_u32(l0_out, self.last_doc_id)?;
        l0_out.write_all(&[frontier.len() as u8])?;
        for &(tf, dl) in &frontier {
            write_u32(l0_out, tf)?;
            write_u32(l0_out, dl)?;
        }
        self.l0_bytes += 5 + 8 * frontier.len() as u64;
        self.l1_pairs.extend_from_slice(&frontier);
        self.l0_in_group += 1;
        self.block.clear();
        if self.l0_in_group == LEVEL1_FACTOR {
            self.close_group();
        }
        Ok(())
    }

    /// Close the current level-1 group: frontier of the merged block
    /// frontiers (a bound over the whole group, by monotonicity).
    pub(crate) fn close_group(&mut self) {
        let group = self.l1_records.last_mut().expect("open group");
        group.last_doc_id = self.last_doc_id;
        group.pairs = pareto_frontier(&self.l1_pairs);
        self.l1_pairs.clear();
        self.l0_in_group = 0;
    }

    /// Flush any partial block/group at the end of a term; returns the
    /// level-0 bytes and the level-1 records.
    pub(crate) fn finish<W: Write>(mut self, l0_out: &mut W) -> io::Result<(u64, Vec<L1Record>)> {
        self.flush_block(l0_out)?;
        if self.l0_in_group > 0 {
            self.close_group();
        }
        Ok((self.l0_bytes, self.l1_records))
    }
}

/// Write a term's complete skip run: the level-1-region prefix, the
/// level-0 bytes (already encoded), then the level-1 records.
pub(crate) fn write_skip_run<W: Write>(w: &mut W, l0: &[u8], l1: &[L1Record]) -> io::Result<()> {
    write_u64(w, 8 + l0.len() as u64)?;
    w.write_all(l0)?;
    for rec in l1 {
        write_u32(w, rec.last_doc_id)?;
        write_u64(w, rec.l0_off)?;
        w.write_all(&[rec.pairs.len() as u8])?;
        for &(tf, dl) in &rec.pairs {
            write_u32(w, tf)?;
            write_u32(w, dl)?;
        }
    }
    Ok(())
}

/// Skip-run byte size without writing anything (the store's size pass).
pub(crate) fn skip_run_size(l0_bytes: u64, l1: &[L1Record]) -> u64 {
    8 + l0_bytes + l1.iter().map(L1Record::encoded_len).sum::<u64>()
}

/// Feed one posting into the three v5 runs: doc-run entry to `doc_out`,
/// occurrence pairs to `occ_out`, `(tf, dl)` to the skip builder (whose
/// level-0 records stream to `skip_l0_out`). `occ_start` is the running
/// occurrence count; returns the new count.
#[allow(clippy::too_many_arguments)]
pub(crate) fn push_posting_v5<D: Write, O: Write, S: Write>(
    doc_out: &mut D,
    occ_out: &mut O,
    skip: &mut SkipRunBuilder,
    skip_l0_out: &mut S,
    doc_id: u32,
    tf: u32,
    offsets: &[(u32, u32)],
    dl: u32,
    occ_start: u32,
) -> io::Result<u32> {
    write_u32(doc_out, doc_id)?;
    write_u32(doc_out, tf)?;
    write_u32(doc_out, occ_start)?;
    for &(start, end) in offsets {
        write_u32(occ_out, start)?;
        write_u32(occ_out, end)?;
    }
    skip.push(tf, dl, doc_id, skip_l0_out)?;
    Ok(occ_start + offsets.len() as u32)
}
