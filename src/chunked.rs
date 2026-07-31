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

/// One query of a batched chunked scan.
#[derive(Debug, Clone, Copy)]
pub struct BatchQuery<'a> {
    /// The query vector, of the index's dimension.
    pub vector: &'a [f32],
    /// Result count for this query.
    pub k: usize,
    /// Tie-complete collection for this query (see [`chunked_topk`]).
    pub keep_ties: bool,
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
    let queries = [BatchQuery {
        vector: query,
        k,
        keep_ties,
    }];
    chunked_topk_batch(
        index,
        &queries,
        chunk_blocks,
        &mut |_| external_floor(),
        &mut |_, f| publish_floor(f),
    )
    .pop()
    .expect("one query in, one result out")
}

/// Scan `index` for several queries' top-k in ONE pass: each chunk is a
/// single `search_with_options` call scoring every query, so the packed
/// codes are streamed from memory once per chunk for the whole batch
/// instead of once per query. turbovec's multi-query kernel scores up to
/// four queries per block pass; this is the scan-side amortization that
/// makes a bandwidth-bound shard serve concurrent queries at better than
/// one-full-sweep each.
///
/// Exactness: the kernel call shares one threshold, so it is seeded with
/// the MINIMUM of the per-query floors — a lower floor only collects
/// more, never less, so every query's result is exactly its solo result.
/// Each query's own floor (external max local k-th best) is re-applied
/// when its chunk candidates are filtered before the merge, keeping ties
/// at the floor, so per-query heaps and published floors are bitwise
/// identical to a solo [`chunked_topk`]. What coalescing costs is
/// collection: a query batched with a lower-floored neighbor sees more
/// candidates returned by the kernel than it would alone (they are
/// filtered before its merge), so `candidates_collected` can exceed the
/// solo count while results stay identical.
///
/// `external_floor` and `publish_floor` receive the query's position in
/// `queries` as their first argument.
pub fn chunked_topk_batch(
    index: &TurboQuantIndex,
    queries: &[BatchQuery<'_>],
    chunk_blocks: usize,
    external_floor: &mut dyn FnMut(usize) -> Option<f32>,
    publish_floor: &mut dyn FnMut(usize, f32),
) -> Vec<(Vec<ChunkHit>, ScanStats)> {
    let n = index.len();
    let nq = queries.len();
    let mut out: Vec<(Vec<ChunkHit>, ScanStats)> = queries
        .iter()
        .map(|_| (Vec::new(), ScanStats::default()))
        .collect();
    let max_k = queries.iter().map(|q| q.k).max().unwrap_or(0);
    if n == 0 || nq == 0 || max_k == 0 {
        return out;
    }
    let chunk_size = chunk_blocks.max(1) * BLOCK_VECTORS;
    let mut heaps: Vec<Vec<ChunkHit>> = queries.iter().map(|q| Vec::with_capacity(q.k)).collect();
    let mut mask = vec![false; n];
    let mut flat: Vec<f32> = Vec::with_capacity(queries.iter().map(|q| q.vector.len()).sum());
    for q in queries {
        flat.extend_from_slice(q.vector);
    }
    // Any tie-complete query forbids capping the kernel's collection at k
    // (its boundary tie group must survive whole); the cap costs only
    // collection, never correctness, so one such query widens the batch.
    let any_ties = queries.iter().any(|q| q.keep_ties);
    let chunk_k = if any_ties { usize::MAX } else { max_k };
    let mut query_floors = vec![f32::NEG_INFINITY; nq];

    let mut start = 0;
    while start < n {
        let end = (start + chunk_size).min(n);

        // Per-query floor: the external (coordinator) floor and the local
        // heap floor are each valid lower bounds on that query's final
        // k-th best, so their max is too. The kernel floor is the batch
        // minimum: valid for every query, prunes least.
        let mut kernel_floor = f32::INFINITY;
        for (qi, q) in queries.iter().enumerate() {
            let mut floor = f32::NEG_INFINITY;
            if let Some(f) = external_floor(qi) {
                if !f.is_nan() {
                    floor = floor.max(f);
                    if floor != f32::NEG_INFINITY {
                        out[qi].1.floor_updates_applied += 1;
                    }
                }
            }
            if q.k > 0 && heaps[qi].len() >= q.k {
                floor = floor.max(heaps[qi][q.k - 1].score);
            }
            query_floors[qi] = floor;
            kernel_floor = kernel_floor.min(floor);
        }

        for slot in mask.iter_mut().take(end).skip(start) {
            *slot = true;
        }
        let results = index.search_with_options(
            &flat,
            chunk_k,
            SearchOptions::new()
                .with_mask(&mask)
                .with_initial_threshold(kernel_floor),
        );
        for slot in mask.iter_mut().take(end).skip(start) {
            *slot = false;
        }

        for (qi, q) in queries.iter().enumerate() {
            out[qi].1.chunk_calls += 1;
            if q.k == 0 {
                continue;
            }
            let mut chunk_hits = Vec::new();
            for (&score, &slot) in results
                .scores_for_query(qi)
                .iter()
                .zip(results.indices_for_query(qi))
            {
                // Floored searches pad short rows with (-inf, -1)
                // sentinels; candidates below THIS query's floor were
                // collected only for a lower-floored neighbor (>= keeps
                // ties at the floor, the same boundary the kernel keeps).
                if slot < 0 || score < query_floors[qi] {
                    continue;
                }
                out[qi].1.candidates_collected += 1;
                chunk_hits.push(ChunkHit {
                    slot: slot as u32,
                    score,
                });
            }
            merge_chunk_hits(&mut heaps[qi], &mut chunk_hits, q.k, q.keep_ties);

            if heaps[qi].len() >= q.k {
                publish_floor(qi, heaps[qi][q.k - 1].score);
                out[qi].1.floors_published += 1;
            }
        }

        start = end;
    }

    for (qi, heap) in heaps.into_iter().enumerate() {
        out[qi].0 = heap;
    }
    out
}

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
pub fn chunked_topk_collapsed(
    index: &TurboQuantIndex,
    query: &[f32],
    k: usize,
    chunk_blocks: usize,
    parents: &[u64],
    external_floor: &mut dyn FnMut() -> Option<f32>,
    publish_floor: &mut dyn FnMut(f32),
) -> (Vec<CollapsedHit>, ScanStats) {
    let n = index.len();
    assert_eq!(
        parents.len(),
        n,
        "parents must map every slot of the index"
    );
    let mut stats = ScanStats::default();
    if n == 0 || k == 0 {
        return (Vec::new(), stats);
    }
    let chunk_size = chunk_blocks.max(1) * BLOCK_VECTORS;
    // Running best per parent. Pruned against the floor after each chunk,
    // so it holds the current top-k parents plus this chunk's newcomers.
    let mut best: std::collections::HashMap<u64, ChunkHit> = std::collections::HashMap::new();
    let mut floor = f32::NEG_INFINITY;
    let mut mask = vec![false; n];

    let mut start = 0;
    while start < n {
        let end = (start + chunk_size).min(n);
        if let Some(f) = external_floor() {
            if !f.is_nan() && f > floor {
                floor = f;
                stats.floor_updates_applied += 1;
            }
        }

        for slot in mask.iter_mut().take(end).skip(start) {
            *slot = true;
        }
        // Escalate until the kernel provably returned everything at or
        // above the floor in this chunk (an unsaturated call).
        let mut chunk_k = k.max(1);
        let hits: Vec<ChunkHit> = loop {
            stats.chunk_calls += 1;
            let results = index.search_with_options(
                query,
                chunk_k.min(end - start),
                SearchOptions::new()
                    .with_mask(&mask)
                    .with_initial_threshold(floor),
            );
            let mut hits = Vec::new();
            for (&score, &slot) in results
                .scores_for_query(0)
                .iter()
                .zip(results.indices_for_query(0))
            {
                if slot < 0 || score < floor {
                    continue;
                }
                hits.push(ChunkHit {
                    slot: slot as u32,
                    score,
                });
            }
            if hits.len() < chunk_k.min(end - start) || chunk_k >= end - start {
                break hits;
            }
            chunk_k *= 2;
        };
        for slot in mask.iter_mut().take(end).skip(start) {
            *slot = false;
        }

        stats.candidates_collected += hits.len() as u64;
        for hit in hits {
            let parent = parents[hit.slot as usize];
            let entry = best.entry(parent).or_insert(hit);
            if ranks_before(hit, *entry) {
                *entry = hit;
            }
        }

        // Publish the k-th best parent score and prune the map to the
        // parents still at or above it (ties survive, exactly like the
        // plain scan's boundary handling).
        if best.len() >= k {
            let mut scores: Vec<f32> = best.values().map(|h| h.score).collect();
            scores.sort_by(|a, b| b.total_cmp(a));
            let kth = scores[k - 1];
            if kth > floor {
                floor = kth;
            }
            best.retain(|_, h| h.score >= kth);
            publish_floor(kth);
            stats.floors_published += 1;
        }

        start = end;
    }

    let mut out: Vec<CollapsedHit> = best
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
    out.truncate(k);
    (out, stats)
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

    /// Every member of a mixed batch (small k, large k, tie-complete) must
    /// produce exactly the hits AND the published floor sequence of its
    /// solo chunked scan, across chunkings. This is the coalescing
    /// exactness contract: batching changes bandwidth, never results.
    #[test]
    fn batch_matches_solo_bitwise() {
        let (n, dim) = (6_000, 64);
        let index = build(n, dim);
        let specs = [(10usize, false), (100, false), (3, true), (1000, false)];
        let queries: Vec<Vec<f32>> = (0..specs.len())
            .map(|i| unit_vectors(1, dim, 0xBA7C_0000 + i as u64))
            .collect();
        for chunk_blocks in [1, 7, 64] {
            let mut solo = Vec::new();
            for (q, &(k, ties)) in queries.iter().zip(&specs) {
                let mut published = Vec::new();
                let (hits, _) = chunked_topk(
                    &index,
                    q,
                    k,
                    chunk_blocks,
                    &mut || None,
                    &mut |f| published.push(f),
                    ties,
                );
                solo.push((hits, published));
            }
            let batch: Vec<BatchQuery> = queries
                .iter()
                .zip(&specs)
                .map(|(q, &(k, ties))| BatchQuery {
                    vector: q,
                    k,
                    keep_ties: ties,
                })
                .collect();
            let mut published: Vec<Vec<f32>> = vec![Vec::new(); specs.len()];
            let results = chunked_topk_batch(&index, &batch, chunk_blocks, &mut |_| None, &mut |qi,
                                                                                              f| {
                published[qi].push(f)
            });
            for (qi, ((hits, _), (solo_hits, solo_published))) in
                results.iter().zip(&solo).enumerate()
            {
                assert_eq!(hits, solo_hits, "query {qi} chunk_blocks {chunk_blocks}");
                assert_eq!(
                    &published[qi], solo_published,
                    "query {qi} floor sequence, chunk_blocks {chunk_blocks}"
                );
            }
        }
    }

    /// An external floor on ONE member must prune that member's collection
    /// while leaving every member's results identical to solo — the
    /// batch-minimum kernel floor and the per-query re-filter in action.
    #[test]
    fn per_query_floor_stays_isolated() {
        let (n, dim, k) = (8_192, 64, 10);
        let index = build(n, dim);
        let q0 = unit_vectors(1, dim, 0xBA7C_1000);
        let q1 = unit_vectors(1, dim, 0xBA7C_1001);
        let kth0 = index.search(&q0, k).scores_for_query(0)[k - 1];

        let solo1 = chunked_topk(&index, &q1, k, 2, &mut || None, &mut |_| {}, false).0;
        let solo0 = chunked_topk(&index, &q0, k, 2, &mut || Some(kth0), &mut |_| {}, false).0;

        let batch = [
            BatchQuery {
                vector: &q0,
                k,
                keep_ties: false,
            },
            BatchQuery {
                vector: &q1,
                k,
                keep_ties: false,
            },
        ];
        // The true k-th best floors query 0 from the start; query 1 runs
        // unseeded.
        let results = chunked_topk_batch(
            &index,
            &batch,
            2,
            &mut |qi| (qi == 0).then_some(kth0),
            &mut |_, _| {},
        );
        assert_eq!(results[0].0, solo0, "floored member");
        assert_eq!(results[1].0, solo1, "unfloored member");
        assert!(
            results[0].1.candidates_collected < results[1].1.candidates_collected,
            "the floored member should collect less: {} vs {}",
            results[0].1.candidates_collected,
            results[1].1.candidates_collected
        );
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

    /// Deterministic parent layout: contiguous runs with lengths cycling
    /// 1..=max_run, mirroring opinions as contiguous chunk ranges.
    fn run_parents(n: usize, max_run: usize) -> Vec<u64> {
        let mut parents = Vec::with_capacity(n);
        let (mut parent, mut run, mut len) = (100u64, 0usize, 1usize);
        for _ in 0..n {
            parents.push(parent);
            run += 1;
            if run >= len {
                parent += 1;
                run = 0;
                len = len % max_run + 1;
            }
        }
        parents
    }

    /// Brute-force reference: exact scores for every slot, grouped by
    /// parent under max aggregation, top-k parents by (score desc, slot
    /// asc).
    fn collapse_reference(
        index: &TurboQuantIndex,
        query: &[f32],
        parents: &[u64],
        k: usize,
    ) -> Vec<CollapsedHit> {
        let n = index.len();
        let all = index.search(query, n);
        let mut best: std::collections::HashMap<u64, ChunkHit> = std::collections::HashMap::new();
        for (&score, &slot) in all
            .scores_for_query(0)
            .iter()
            .zip(all.indices_for_query(0))
        {
            let hit = ChunkHit {
                slot: slot as u32,
                score,
            };
            let entry = best.entry(parents[slot as usize]).or_insert(hit);
            if ranks_before(hit, *entry) {
                *entry = hit;
            }
        }
        let mut out: Vec<CollapsedHit> = best
            .into_iter()
            .map(|(parent, h)| CollapsedHit {
                parent,
                slot: h.slot,
                score: h.score,
            })
            .collect();
        out.sort_by(|a, b| b.score.total_cmp(&a.score).then_with(|| a.slot.cmp(&b.slot)));
        out.truncate(k);
        out
    }

    fn signature(hits: &[CollapsedHit]) -> Vec<(u64, u32, u32)> {
        hits.iter()
            .map(|h| (h.parent, h.slot, h.score.to_bits()))
            .collect()
    }

    /// Collapsed scan must equal the brute-force group-by reference
    /// bitwise, across chunkings from one block to whole-shard.
    #[test]
    fn collapsed_matches_brute_force_reference() {
        let (n, dim, k) = (5_000, 64, 10);
        let index = build(n, dim);
        let query = unit_vectors(1, dim, 0xC0AA_0001);
        for max_run in [1, 7, 40] {
            let parents = run_parents(n, max_run);
            let want = signature(&collapse_reference(&index, &query, &parents, k));
            for chunk_blocks in [1, 2, 7, 64, 10_000] {
                let (hits, stats) = chunked_topk_collapsed(
                    &index,
                    &query,
                    k,
                    chunk_blocks,
                    &parents,
                    &mut || None,
                    &mut |_| {},
                );
                assert_eq!(
                    signature(&hits),
                    want,
                    "max_run={max_run} chunk_blocks={chunk_blocks}"
                );
                assert!(stats.chunk_calls > 0);
            }
        }
    }

    /// A monster parent: hundreds of near-identical strong chunks in one
    /// contiguous run. Its siblings crowd every kernel top-k, forcing the
    /// saturation escalation; results must still match the reference and
    /// the monster must surface exactly once.
    #[test]
    fn collapsed_survives_a_monster_parent() {
        let (n, dim, k) = (4_096, 32, 5);
        let mut vectors = unit_vectors(n, dim, 0xC0AB_0001);
        let query = unit_vectors(1, dim, 0xC0AB_0009);
        // Slots 1024..1424: the monster, near-copies of the query with
        // tiny deterministic jitter, all scoring far above everything.
        for (i, row) in vectors[1024 * dim..1424 * dim].chunks_mut(dim).enumerate() {
            for (d, x) in row.iter_mut().enumerate() {
                *x = query[d] + ((i * 31 + d) % 17) as f32 * 1e-4;
            }
        }
        let mut index = TurboQuantIndex::new(dim, 4).unwrap();
        index.add(&vectors);
        let mut parents = run_parents(n, 5);
        for parent in parents.iter_mut().take(1424).skip(1024) {
            *parent = 7_777_777;
        }

        let want = signature(&collapse_reference(&index, &query, &parents, k));
        for chunk_blocks in [2, 8, 64, 10_000] {
            let (hits, stats) = chunked_topk_collapsed(
                &index,
                &query,
                k,
                chunk_blocks,
                &parents,
                &mut || None,
                &mut |_| {},
            );
            assert_eq!(signature(&hits), want, "chunk_blocks={chunk_blocks}");
            assert_eq!(
                hits.iter().filter(|h| h.parent == 7_777_777).count(),
                1,
                "monster surfaces exactly once"
            );
        }
    }

    /// Published parent floors are non-decreasing, become lower bounds on
    /// the final k-th parent score, and an adopted external floor prunes
    /// without changing results.
    #[test]
    fn collapsed_floors_are_sound_and_external_floor_is_lossless() {
        let (n, dim, k) = (5_000, 64, 10);
        let index = build(n, dim);
        let query = unit_vectors(1, dim, 0xC0AC_0001);
        let parents = run_parents(n, 9);

        let mut published = Vec::new();
        let (hits, _) = chunked_topk_collapsed(
            &index,
            &query,
            k,
            8,
            &parents,
            &mut || None,
            &mut |f| published.push(f),
        );
        assert!(!published.is_empty());
        assert!(
            published.windows(2).all(|w| w[0] <= w[1]),
            "floors must be monotone"
        );
        let kth = hits[k - 1].score;
        assert!(published.iter().all(|&f| f <= kth));

        // Seed the true k-th best as an external floor: results identical,
        // strictly fewer candidates collected.
        let (unseeded, base) = chunked_topk_collapsed(
            &index,
            &query,
            k,
            8,
            &parents,
            &mut || None,
            &mut |_| {},
        );
        let (seeded, pruned) = chunked_topk_collapsed(
            &index,
            &query,
            k,
            8,
            &parents,
            &mut || Some(kth),
            &mut |_| {},
        );
        assert_eq!(signature(&unseeded), signature(&seeded));
        assert!(pruned.candidates_collected < base.candidates_collected);
    }
}
