//! Collapse-by-parent chunked scan.

use super::batch::fill_mask;
use super::*;

/// One collapsed candidate: a parent document represented by its best
/// chunk.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct CollapsedHit {
    /// The parent id (`parents[slot]` of the best chunk).
    pub parent: u64,
    /// The best-scoring slot of this parent (the entry-point chunk c').
    pub slot: u32,
    /// That slot's score: the parent's score under max aggregation.
    pub score: f32,
}

/// Collapse-by-parent chunked scan: the top-`k` DISTINCT parents of
/// `query`, each represented by its best chunk. `parents[slot]` maps
/// every slot to its parent id (length must equal the index length).
///
/// Collection keeps ONE running entry per parent (max score, ties to the
/// lower slot), so a parent with 20,000 chunks costs a running max and
/// emits one candidate. The published floor is the k-th best PARENT
/// score — a valid lower bound on the global k-th best parent (the k-th
/// best of a subset never exceeds the union's) and always >= the plain
/// chunk floor, so collaborative pruning only strengthens. A chunk
/// scoring below the floor can never matter: it cannot beat its own
/// parent's best (which is >= any floor that parent ever defined) nor
/// introduce a new top-k parent.
///
/// Exactness under per-chunk truncation: the kernel returns its top
/// `chunk_k` slots per call. When that set is SATURATED (exactly
/// `chunk_k` real candidates came back, so slots above the floor may
/// have been dropped — possibly a hidden parent's best crowded out by a
/// many-chunk sibling), the chunk is rescanned with doubled `chunk_k`
/// until unsaturated; the final call provably returned every slot at or
/// above the floor in that chunk. Escalations count in
/// [`ScanStats::chunk_calls`].
#[allow(clippy::too_many_arguments)]
pub fn chunked_topk_collapsed(
    index: &VectorIndex,
    query: &[f32],
    k: usize,
    chunk_blocks: usize,
    parents: &[u64],
    external_floor: &mut dyn FnMut() -> Option<f32>,
    publish_floor: &mut dyn FnMut(f32) -> bool,
    allow: Option<&[bool]>,
) -> (Vec<CollapsedHit>, ScanStats) {
    let n = index.len();
    assert_eq!(parents.len(), n, "parents must map every slot of the index");
    if let Some(a) = allow {
        assert_eq!(
            a.len(),
            n,
            "an allowlist must cover every slot of the index"
        );
    }
    let mut scan = CollapsedScan {
        index,
        query,
        k,
        row_bytes: index_row_bytes(index),
        mask: vec![false; n],
        floor: f32::NEG_INFINITY,
        stats: ScanStats::default(),
        best: std::collections::HashMap::new(),
    };
    if n == 0 || k == 0 {
        return (Vec::new(), scan.stats);
    }
    let chunk_size = chunk_blocks.max(1) * BLOCK_VECTORS;
    let mut start = 0;
    while start < n {
        let end = (start + chunk_size).min(n);
        if let Some(f) = external_floor() {
            if !f.is_nan() && f > scan.floor {
                scan.floor = f;
                scan.stats.floor_updates_applied += 1;
            }
        }
        // Nothing in this chunk survives the filters: no kernel call,
        // no candidates, no parent to publish a floor for.
        if let Some(rows_streamed) = fill_mask(&mut scan.mask, allow, start, end) {
            let hits = scan.chunk_hits(start, end, rows_streamed);
            scan.mask[start..end].fill(false);
            scan.absorb(hits, parents);
            scan.publish(publish_floor);
        }
        start = end;
    }
    scan.finish()
}

/// The running state of one collapsed scan. `best` is the running best per
/// parent, pruned against the floor after each chunk, so it holds the
/// current top-k parents plus the latest chunk's newcomers.
struct CollapsedScan<'a> {
    index: &'a VectorIndex,
    query: &'a [f32],
    k: usize,
    row_bytes: u64,
    mask: Vec<bool>,
    floor: f32,
    stats: ScanStats,
    best: std::collections::HashMap<u64, ChunkHit>,
}

impl CollapsedScan<'_> {
    /// Every slot of `[start, end)` scoring at or above the floor. Escalate
    /// until the kernel provably returned everything at or above the floor
    /// in this chunk (an unsaturated call). The saturation test compares
    /// against the chunk's slot span, which an allowlist can only shrink:
    /// fewer allowed slots than `chunk_k` means the call came back short,
    /// which is exactly the unsaturated condition, so the test stays sound
    /// under filters.
    fn chunk_hits(&mut self, start: usize, end: usize, rows_streamed: usize) -> Vec<ChunkHit> {
        let mut chunk_k = self.k.max(1);
        loop {
            self.stats.chunk_calls += 1;
            let started = std::time::Instant::now();
            let results = self.index.search_with_options(
                self.query,
                chunk_k.min(end - start),
                VectorSearchOptions::new()
                    .with_mask(&self.mask)
                    .with_initial_threshold(self.floor),
            );
            self.stats.bytes_scanned += rows_streamed as u64 * self.row_bytes;
            self.stats.scan_nanos +=
                u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX);
            let hits: Vec<ChunkHit> = results
                .scores_for_query(0)
                .iter()
                .zip(results.indices_for_query(0))
                .filter(|&(&score, &slot)| slot >= 0 && score >= self.floor)
                .map(|(&score, &slot)| ChunkHit {
                    slot: slot as u32,
                    score,
                })
                .collect();
            if hits.len() < chunk_k.min(end - start) || chunk_k >= end - start {
                return hits;
            }
            chunk_k *= 2;
        }
    }

    /// Keep one running entry per parent: max score, ties to the lower slot.
    fn absorb(&mut self, hits: Vec<ChunkHit>, parents: &[u64]) {
        self.stats.candidates_collected += hits.len() as u64;
        for hit in hits {
            let parent = parents[hit.slot as usize];
            let entry = self.best.entry(parent).or_insert(hit);
            if ranks_before(hit, *entry) {
                *entry = hit;
            }
        }
    }

    /// Publish the k-th best parent score and prune the map to the parents
    /// still at or above it (ties survive, exactly like the plain scan's
    /// boundary handling).
    fn publish(&mut self, publish_floor: &mut dyn FnMut(f32) -> bool) {
        if self.best.len() < self.k {
            return;
        }
        let mut scores: Vec<f32> = self.best.values().map(|h| h.score).collect();
        scores.sort_by(|a, b| b.total_cmp(a));
        let kth = scores[self.k - 1];
        if kth > self.floor {
            self.floor = kth;
        }
        self.best.retain(|_, h| h.score >= kth);
        self.stats.floors_offered += 1;
        if publish_floor(kth) {
            self.stats.floors_published += 1;
        }
    }

    fn finish(self) -> (Vec<CollapsedHit>, ScanStats) {
        let mut out: Vec<CollapsedHit> = self
            .best
            .into_iter()
            .map(|(parent, h)| CollapsedHit {
                parent,
                slot: h.slot,
                score: h.score,
            })
            .collect();
        out.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.slot.cmp(&b.slot))
        });
        out.truncate(self.k);
        (out, self.stats)
    }
}
