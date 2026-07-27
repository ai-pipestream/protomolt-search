//! Chunked shard scan with mid-scan floor adoption.
//!
//! turbovec's `search_with_options` is a single synchronous scan whose floor
//! is fixed at call time, so reactive floor sharing is built *around* it:
//! the shard is scanned in chunks of `chunk_blocks` SIMD blocks, each chunk
//! a masked `search_with_options` call (an allowlist over a contiguous slot
//! range) seeded with the best floor known at that moment. Between chunks
//! the scan polls `external_floor` for coordinator-pushed floors and reports
//! its own k-th best through `publish_floor` once its heap is full.
//!
//! Losslessness: the union of the masked chunk ranges is the whole shard,
//! and every floor ever applied is a lower bound on the shard's final k-th
//! best — the local heap floor because it can only rise toward the true
//! k-th best, a coordinator floor because any shard's k-th best lower-bounds
//! the global k-th best, which lower-bounds... which is itself bounded below
//! by every shard's k-th best. turbovec keeps candidates scoring exactly at
//! the floor, so boundary ties survive. The merged heap therefore ends with
//! exactly the shard's unchunked top-k.

use turbovec::{SearchOptions, TurboQuantIndex};

/// Vectors per SIMD block in the turbovec scan kernel. Chunk ranges are
/// block-aligned so a chunk is always a whole number of kernel blocks.
pub const BLOCK_VECTORS: usize = 32;

/// Default chunk size in SIMD blocks (64 blocks = 2048 vectors per chunk).
pub const DEFAULT_CHUNK_BLOCKS: usize = 64;

/// One candidate collected during a chunked scan.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ChunkHit {
    /// Local slot within the shard index.
    pub slot: u32,
    /// turbovec score; higher is better.
    pub score: f32,
}

/// Instrumentation for one chunked scan.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ScanStats {
    /// Per-chunk `search_with_options` calls made.
    pub chunk_calls: u32,
    /// Real (non-sentinel) candidates collected across all chunks. This is
    /// exactly the set of vectors that survived every floor in effect when
    /// their chunk ran — the metric that shows floor sharing saving work.
    pub candidates_collected: u64,
    /// Floor values published via `publish_floor`.
    pub floors_published: u64,
    /// Chunks that ran with an external (coordinator-pushed) floor in effect.
    pub floor_updates_applied: u64,
}

/// `true` when `a` ranks ahead of `b` in top-k order: score descending,
/// ties broken by ascending slot (deterministic, matches the coordinator's
/// merge tie-break).
fn ranks_before(a: ChunkHit, b: ChunkHit) -> bool {
    a.score > b.score || (a.score == b.score && a.slot < b.slot)
}

/// Merge one chunk's hits into the running top-k `heap` (score desc, slot
/// asc, capped at `k` — or at `k` plus the boundary tie group when
/// `keep_ties` is set).
///
/// turbovec returns each row score-descending, so instead of inserting hit
/// by hit — O(k) per insert, which hurts at k=10000 — this sorts the (small)
/// chunk batch into the heap's total order and does a linear two-pointer
/// merge: O(chunk log chunk + k) per chunk.
///
/// With `keep_ties`, entries beyond `k` are kept while their score equals
/// the current k-th best: the cascade fusion mode's cutoff is
/// score-defined, so every doc tied at the boundary must survive on every
/// shard. Eviction still happens whenever the k-th best score RISES (the
/// old boundary group is then provably below any future cutoff), so the
/// retention is bounded by k plus the largest tie group at the CURRENT
/// boundary — worst case a shard of identical scores, same as any
/// tie-complete contract.
fn merge_chunk_hits(heap: &mut Vec<ChunkHit>, chunk: &mut [ChunkHit], k: usize, keep_ties: bool) {
    if chunk.is_empty() {
        return;
    }
    chunk.sort_by(|a, b| {
        if ranks_before(*a, *b) {
            std::cmp::Ordering::Less
        } else if ranks_before(*b, *a) {
            std::cmp::Ordering::Greater
        } else {
            std::cmp::Ordering::Equal
        }
    });
    if heap.len() >= k {
        // The chunk's best hit cannot displace the heap's k-th. With
        // keep_ties, a hit TIED at the boundary must still merge in.
        let skip = if keep_ties {
            chunk[0].score < heap[k - 1].score
        } else {
            heap.len() == k && !ranks_before(chunk[0], heap[k - 1])
        };
        if skip {
            return;
        }
    }
    let mut merged = Vec::with_capacity(heap.len() + chunk.len());
    let (mut i, mut j) = (0, 0);
    while merged.len() < k && (i < heap.len() || j < chunk.len()) {
        let take_heap = match (heap.get(i), chunk.get(j)) {
            (Some(&h), Some(&c)) => ranks_before(h, c),
            (Some(_), None) => true,
            (None, Some(_)) => false,
            (None, None) => break,
        };
        merged.push(if take_heap {
            i += 1;
            heap[i - 1]
        } else {
            j += 1;
            chunk[j - 1]
        });
    }
    if keep_ties && merged.len() == k {
        // Boundary tie group: every remaining entry scoring exactly the
        // k-th best rides along. Both sides are (score desc, slot asc), so
        // ties are contiguous.
        let boundary = merged[k - 1].score;
        while i < heap.len() && heap[i].score == boundary {
            merged.push(heap[i]);
            i += 1;
        }
        while j < chunk.len() && chunk[j].score == boundary {
            merged.push(chunk[j]);
            j += 1;
        }
        // Re-sort the tie tail into (score desc, slot asc) order: heap
        // ties and chunk ties interleave by slot.
        merged[k..].sort_by_key(|h| h.slot);
    }
    *heap = merged;
}

/// Scan `index` for the top-`k` of `query` in chunks, adopting external
/// floors between chunks and publishing the local k-th best once the heap
/// fills.
///
/// - `query`: a single query vector of the index's dimension.
/// - `chunk_blocks`: chunk size in SIMD blocks (values < 1 behave as 1).
/// - `external_floor`: polled before each chunk; `Some(f)` seeds that
///   chunk's threshold with `max(f, local heap floor)`. Return `None` when
///   no external floor is known (or floor sharing is disabled).
/// - `publish_floor`: called after each chunk in which the heap holds `k`
///   candidates, with the current k-th best score.
///
/// Returns the shard's local top-k sorted by [`ranks_before`] plus scan
/// statistics.
pub fn chunked_topk(
    index: &TurboQuantIndex,
    query: &[f32],
    k: usize,
    chunk_blocks: usize,
    external_floor: &mut dyn FnMut() -> Option<f32>,
    publish_floor: &mut dyn FnMut(f32),
    keep_ties: bool,
) -> (Vec<ChunkHit>, ScanStats) {
    let n = index.len();
    let mut stats = ScanStats::default();
    if k == 0 || n == 0 {
        return (Vec::new(), stats);
    }
    let chunk_size = chunk_blocks.max(1) * BLOCK_VECTORS;
    let mut heap: Vec<ChunkHit> = Vec::with_capacity(k);
    let mut mask = vec![false; n];

    let mut start = 0;
    while start < n {
        let end = (start + chunk_size).min(n);

        // Best known floor for this chunk: the external (coordinator) floor
        // and the local heap floor are each valid lower bounds on the final
        // k-th best, so their max is too.
        let mut floor = f32::NEG_INFINITY;
        if let Some(f) = external_floor() {
            if !f.is_nan() {
                floor = floor.max(f);
                if floor != f32::NEG_INFINITY {
                    stats.floor_updates_applied += 1;
                }
            }
        }
        if heap.len() >= k {
            floor = floor.max(heap[k - 1].score);
        }

        // In tie-complete mode the chunk search must NOT cap at k: a
        // boundary tie group larger than k would be truncated inside the
        // kernel before the merge could retain it. Collecting everything
        // at-or-above the floor keeps the whole group; the merge then
        // retains top-k plus the boundary ties. (The floor still does the
        // pruning — collection beyond it is what tie-completeness costs.)
        let chunk_k = if keep_ties { usize::MAX } else { k };
        for slot in mask.iter_mut().take(end).skip(start) {
            *slot = true;
        }
        let results = index.search_with_options(
            query,
            chunk_k,
            SearchOptions::new()
                .with_mask(&mask)
                .with_initial_threshold(floor),
        );
        stats.chunk_calls += 1;
        for slot in mask.iter_mut().take(end).skip(start) {
            *slot = false;
        }

        let mut chunk_hits = Vec::new();
        for (&score, &slot) in results
            .scores_for_query(0)
            .iter()
            .zip(results.indices_for_query(0))
        {
            // Floored searches pad short rows with (-inf, -1) sentinels.
            if slot < 0 {
                continue;
            }
            stats.candidates_collected += 1;
            chunk_hits.push(ChunkHit {
                slot: slot as u32,
                score,
            });
        }
        merge_chunk_hits(&mut heap, &mut chunk_hits, k, keep_ties);

        if heap.len() >= k {
            publish_floor(heap[k - 1].score);
            stats.floors_published += 1;
        }

        start = end;
    }

    (heap, stats)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn unit_vectors(n: usize, dim: usize, seed: u64) -> Vec<f32> {
        let mut s = seed.wrapping_add(0x9E3779B97F4A7C15);
        let mut out = vec![0.0f32; n * dim];
        for row in out.chunks_mut(dim) {
            let mut norm = 0.0f64;
            for x in row.iter_mut() {
                s = s
                    .wrapping_mul(6364136223846793005)
                    .wrapping_add(1442695040888963407);
                let v = ((s >> 33) as f64 / (1u64 << 31) as f64) - 1.0;
                *x = v as f32;
                norm += v * v;
            }
            let inv = 1.0 / (norm.sqrt() + 1e-9);
            for x in row.iter_mut() {
                *x = (*x as f64 * inv) as f32;
            }
        }
        out
    }

    fn build(n: usize, dim: usize) -> TurboQuantIndex {
        let mut idx = TurboQuantIndex::new(dim, 4).unwrap();
        idx.add(&unit_vectors(n, dim, 0xC40C_0001));
        idx
    }

    /// Plain scan (no external floors) must reproduce the index's own
    /// top-k exactly, for chunkings from one block up to the whole shard.
    #[test]
    fn chunked_matches_unchunked_for_all_chunk_sizes() {
        let (n, dim, k) = (5_000, 64, 10);
        let index = build(n, dim);
        let query = unit_vectors(1, dim, 0x0E50_0001);
        let expected = index.search(&query, k);

        for chunk_blocks in [1, 2, 7, 64, 10_000] {
            let (hits, stats) = chunked_topk(
                &index,
                &query,
                k,
                chunk_blocks,
                &mut || None,
                &mut |_| {},
                false,
            );
            let got: Vec<(i64, u32)> = hits
                .iter()
                .map(|h| (h.slot as i64, h.score.to_bits()))
                .collect();
            let mut want: Vec<(i64, u32)> = expected
                .indices_for_query(0)
                .iter()
                .zip(expected.scores_for_query(0))
                .map(|(&i, &s)| (i, s.to_bits()))
                .collect();
            // Same deterministic order the heap uses: score desc, slot asc.
            want.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
            assert_eq!(got, want, "chunk_blocks={chunk_blocks}");
            assert_eq!(
                stats.chunk_calls,
                n.div_ceil(chunk_blocks.max(1) * BLOCK_VECTORS) as u32
            );
        }
    }

    /// Large-k correctness: the merge-based heap must hold up at k=1000
    /// (where the per-chunk candidate batches are large and k exceeds the
    /// chunk size, so the heap fills across many chunks).
    #[test]
    fn chunked_matches_unchunked_at_large_k() {
        let (n, dim, k) = (6_000, 64, 1_000);
        let index = build(n, dim);
        let query = unit_vectors(1, dim, 0x0E50_0005);
        let expected = index.search(&query, k);

        for chunk_blocks in [1, 8] {
            let (hits, _) = chunked_topk(
                &index,
                &query,
                k,
                chunk_blocks,
                &mut || None,
                &mut |_| {},
                false,
            );
            assert_eq!(hits.len(), k);
            let got: Vec<(i64, u32)> = hits
                .iter()
                .map(|h| (h.slot as i64, h.score.to_bits()))
                .collect();
            let mut want: Vec<(i64, u32)> = expected
                .indices_for_query(0)
                .iter()
                .zip(expected.scores_for_query(0))
                .map(|(&i, &s)| (i, s.to_bits()))
                .collect();
            want.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
            assert_eq!(got, want, "chunk_blocks={chunk_blocks}");
        }
    }

    /// A floor injected mid-scan (here: the true k-th best, a lossless
    /// floor) must not change the result — and must shrink the candidate
    /// set, proving the floor actually prunes.
    #[test]
    fn mid_scan_floor_injection_is_lossless_and_prunes() {
        let (n, dim, k) = (8_192, 64, 10);
        let index = build(n, dim);
        let query = unit_vectors(1, dim, 0x0E50_0002);
        let true_kth = index.search(&query, k).scores_for_query(0)[k - 1];

        let (baseline, base_stats) =
            chunked_topk(&index, &query, k, 2, &mut || None, &mut |_| {}, false);

        // Floor becomes visible after the first chunk, like a coordinator
        // update arriving mid-scan.
        let mut polls = 0usize;
        let (floored, floored_stats) = chunked_topk(
            &index,
            &query,
            k,
            2,
            &mut || {
                polls += 1;
                (polls > 1).then_some(true_kth)
            },
            &mut |_| {},
            false,
        );

        assert_eq!(baseline, floored);
        assert!(
            floored_stats.candidates_collected < base_stats.candidates_collected,
            "floor should prune candidates: {} vs {}",
            floored_stats.candidates_collected,
            base_stats.candidates_collected
        );
        assert!(floored_stats.floor_updates_applied > 0);
    }

    /// The local heap floor alone (no external input) must engage the
    /// publisher once the heap fills.
    #[test]
    fn publishes_kth_best_once_heap_fills() {
        let (n, dim, k) = (2_048, 64, 5);
        let index = build(n, dim);
        let query = unit_vectors(1, dim, 0x0E50_0003);

        let mut published = Vec::new();
        let (hits, stats) = chunked_topk(
            &index,
            &query,
            k,
            1,
            &mut || None,
            &mut |f| published.push(f),
            false,
        );

        assert_eq!(hits.len(), k);
        assert!(!published.is_empty());
        // Published floors rise monotonically and end at the final k-th best.
        assert!(published.windows(2).all(|w| w[0] <= w[1]));
        assert_eq!(*published.last().unwrap(), hits[k - 1].score);
        assert_eq!(stats.floors_published as usize, published.len());
    }

    /// Boundary tie retention: with `keep_ties`, the whole tie group at
    /// the k-th score survives (the heap may exceed k); without it, the
    /// heap caps at exactly k. Duplicated vectors score identically, so
    /// copies past the boundary are the tie group.
    #[test]
    fn tie_complete_keeps_boundary_tie_group() {
        let (n, dim, k) = (512, 64, 3);
        let mut index = TurboQuantIndex::new(dim, 4).unwrap();
        index.add(&unit_vectors(n, dim, 0xC40C_0002));
        // Six copies of the query vector: identical codes, identical
        // (top) score. They ARE the boundary tie group at k=3.
        let query = unit_vectors(1, dim, 0x0E50_0006);
        for _ in 0..6 {
            index.add(&query);
        }

        let (capped, _) = chunked_topk(&index, &query, k, 1, &mut || None, &mut |_| {}, false);
        assert_eq!(capped.len(), k, "without keep_ties the heap caps at k");

        let (tied, _) = chunked_topk(&index, &query, k, 1, &mut || None, &mut |_| {}, true);
        assert_eq!(tied.len(), 6, "whole boundary tie group must survive");
        assert!(tied.iter().all(|h| h.score == tied[0].score));
        assert!(tied.windows(2).all(|w| w[0].slot < w[1].slot));
    }

    /// An over-high floor (above the true k-th best) is allowed by the
    /// turbovec contract to drop results; the scan must surface that
    /// honestly as fewer than k hits rather than fabricating candidates.
    #[test]
    fn overhigh_floor_shrinks_results() {
        let (n, dim, k) = (2_048, 64, 10);
        let index = build(n, dim);
        let query = unit_vectors(1, dim, 0x0E50_0004);
        let top = index.search(&query, k).scores_for_query(0)[0];

        let (hits, _) = chunked_topk(
            &index,
            &query,
            k,
            64,
            &mut || Some(top + 1.0),
            &mut |_| {},
            false,
        );
        assert!(hits.is_empty());
    }
}
