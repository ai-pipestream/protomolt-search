//! Plain chunked scans: one query, or a batch sharing kernel passes.

use super::groups::KernelGroup;
use super::*;

/// Scan the index for the query's top-`k` in chunks, adopting external
/// floors between chunks and publishing the local k-th best once the heap
/// fills.
///
/// - `keep_ties`: tie-complete collection (the boundary tie group survives
///   whole).
/// - `external_floor`: polled before each chunk; `Some(f)` seeds that
///   chunk's threshold with `max(f, local heap floor)`. Return `None` when
///   no external floor is known (or floor sharing is disabled).
/// - `publish_floor`: called after each chunk in which the heap holds `k`
///   candidates, with the current k-th best score. It returns whether the
///   floor was actually sent, so the scan can report offered and
///   published separately.
///
/// Returns the shard's local top-k, score descending with ties broken by
/// ascending slot, plus scan statistics.
pub fn chunked_topk(
    scan: ChunkedScan<'_>,
    keep_ties: bool,
    external_floor: &mut dyn FnMut() -> Option<f32>,
    publish_floor: &mut dyn FnMut(f32) -> bool,
) -> (Vec<ChunkHit>, ScanStats) {
    let queries = [BatchQuery {
        vector: scan.query,
        k: scan.k,
        keep_ties,
        allow: scan.allow,
        enumerate_max_rows: None,
        controls: Default::default(),
        patience: None,
        floor_prune: None,
    }];
    chunked_topk_batch(
        scan.index,
        &queries,
        scan.chunk_blocks,
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
    index: &VectorIndex,
    queries: &[BatchQuery<'_>],
    chunk_blocks: usize,
    external_floor: &mut dyn FnMut(usize) -> Option<f32>,
    publish_floor: &mut dyn FnMut(usize, f32) -> bool,
) -> Vec<(Vec<ChunkHit>, ScanStats)> {
    chunked_topk_batch_observed(
        index,
        queries,
        chunk_blocks,
        external_floor,
        publish_floor,
        &mut |_| {},
    )
}

/// [`chunked_topk_batch`] with an observer that sees every kernel call
/// once, with the bytes it streamed and its wall time, whatever the
/// batch's size. The node's scan-rate window and the process counters
/// take their samples here so a shared pass is counted once.
pub fn chunked_topk_batch_observed(
    index: &VectorIndex,
    queries: &[BatchQuery<'_>],
    chunk_blocks: usize,
    external_floor: &mut dyn FnMut(usize) -> Option<f32>,
    publish_floor: &mut dyn FnMut(usize, f32) -> bool,
    observe: &mut dyn FnMut(ScanPass),
) -> Vec<(Vec<ChunkHit>, ScanStats)> {
    let n = index.len();
    let mut state = BatchState::new(queries);
    let max_k = queries.iter().map(|q| q.k).max().unwrap_or(0);
    if n == 0 || queries.is_empty() || max_k == 0 {
        return state.out;
    }
    for q in queries {
        if let Some(allow) = q.allow {
            assert_eq!(
                allow.len(),
                n,
                "an allowlist must cover every slot of the index"
            );
        }
    }
    if !index
        .descriptor()
        .capabilities
        .contains(&VectorCapability::ExhaustiveCompletion)
    {
        return searched_batch(index, queries, external_floor, publish_floor, observe);
    }
    let chunk_size = chunk_blocks.max(1) * BLOCK_VECTORS;
    let row_bytes = index_row_bytes(index);
    let groups = KernelGroup::partition(queries);
    let mut mask = vec![false; n];
    let mut start = 0;
    while start < n {
        let end = (start + chunk_size).min(n);
        for group in &groups {
            let (kernel_floor, chunk_k) =
                state.seed_floors(queries, &group.members, external_floor);
            let Some(rows_streamed) = fill_mask(&mut mask, group.allow, start, end) else {
                continue;
            };
            let started = std::time::Instant::now();
            let results = index.search_with_options(
                &group.flat,
                chunk_k,
                VectorSearchOptions::new()
                    .with_mask(&mask)
                    .with_initial_threshold(kernel_floor),
            );
            let pass = ScanPass {
                bytes: rows_streamed as u64 * row_bytes,
                nanos: u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
            };
            observe(pass);
            mask[start..end].fill(false);
            state.absorb(queries, &group.members, &results, pass, publish_floor);
        }
        start = end;
    }
    state.finish(index, queries)
}

/// A non-scanning provider: one whole-index search per query, its k-th
/// best offered once.
fn searched_batch(
    index: &VectorIndex,
    queries: &[BatchQuery<'_>],
    external_floor: &mut dyn FnMut(usize) -> Option<f32>,
    publish_floor: &mut dyn FnMut(usize, f32) -> bool,
    observe: &mut dyn FnMut(ScanPass),
) -> Vec<(Vec<ChunkHit>, ScanStats)> {
    let mut out = Vec::with_capacity(queries.len());
    for (qi, q) in queries.iter().enumerate() {
        let (hits, mut stats) = searched_topk(index, q, external_floor(qi), observe);
        if q.k > 0 && hits.len() >= q.k {
            stats.floors_offered += 1;
            if publish_floor(qi, hits[q.k - 1].score) {
                stats.floors_published += 1;
            }
        }
        out.push((hits, stats));
    }
    out
}

/// Per-query heaps, statistics, and the floor each query applied in the
/// current chunk.
struct BatchState {
    heaps: Vec<Vec<ChunkHit>>,
    /// Each heap's back buffer: a competitive chunk merges into it and the
    /// two swap, so no merge allocates once both have grown to the heap.
    spares: Vec<Vec<ChunkHit>>,
    /// One kernel call's candidates for one query, reused across calls.
    chunk: Vec<ChunkHit>,
    out: Vec<(Vec<ChunkHit>, ScanStats)>,
    floors: Vec<f32>,
}

impl BatchState {
    fn new(queries: &[BatchQuery<'_>]) -> Self {
        Self {
            heaps: queries.iter().map(|q| Vec::with_capacity(q.k)).collect(),
            spares: queries.iter().map(|_| Vec::new()).collect(),
            chunk: Vec::new(),
            out: queries
                .iter()
                .map(|_| (Vec::new(), ScanStats::default()))
                .collect(),
            floors: vec![f32::NEG_INFINITY; queries.len()],
        }
    }

    /// Per-query floor: the external (coordinator) floor and the local heap
    /// floor are each valid lower bounds on that query's final k-th best,
    /// so their max is too. The kernel floor is the group minimum: valid
    /// for every query in it, prunes least. Any tie-complete query forbids
    /// capping the kernel's collection at k (its boundary tie group must
    /// survive whole); the cap costs only collection, never correctness, so
    /// one such query widens the group. Returns `(kernel_floor, chunk_k)`.
    fn seed_floors(
        &mut self,
        queries: &[BatchQuery<'_>],
        group: &[usize],
        external_floor: &mut dyn FnMut(usize) -> Option<f32>,
    ) -> (f32, usize) {
        let mut kernel_floor = f32::INFINITY;
        let mut group_k = 0usize;
        let mut any_ties = false;
        for &qi in group {
            let q = &queries[qi];
            let mut floor = f32::NEG_INFINITY;
            if let Some(f) = external_floor(qi) {
                if !f.is_nan() {
                    floor = floor.max(f);
                    if floor != f32::NEG_INFINITY {
                        self.out[qi].1.floor_updates_applied += 1;
                    }
                }
            }
            if q.k > 0 && self.heaps[qi].len() >= q.k {
                floor = floor.max(self.heaps[qi][q.k - 1].score);
            }
            self.floors[qi] = floor;
            kernel_floor = kernel_floor.min(floor);
            group_k = group_k.max(q.k);
            any_ties |= q.keep_ties;
        }
        let chunk_k = if any_ties { usize::MAX } else { group_k };
        (kernel_floor, chunk_k)
    }

    /// Merge one kernel call's candidates into each member query's heap and
    /// offer its k-th best once the heap is full.
    fn absorb(
        &mut self,
        queries: &[BatchQuery<'_>],
        group: &[usize],
        results: &crate::vector::VectorSearchResults,
        pass: ScanPass,
        publish_floor: &mut dyn FnMut(usize, f32) -> bool,
    ) {
        for (local, &qi) in group.iter().enumerate() {
            let q = &queries[qi];
            let stats = &mut self.out[qi].1;
            stats.chunk_calls += 1;
            stats.bytes_scanned += pass.bytes;
            stats.scan_nanos += pass.nanos;
            if q.k == 0 {
                continue;
            }
            self.chunk.clear();
            for (&score, &slot) in results
                .scores_for_query(local)
                .iter()
                .zip(results.indices_for_query(local))
            {
                // Floored searches pad short rows with (-inf, -1)
                // sentinels; candidates below THIS query's floor were
                // collected only for a lower-floored neighbor (>= keeps
                // ties at the floor, the same boundary the kernel keeps).
                if slot < 0 || score < self.floors[qi] {
                    continue;
                }
                stats.candidates_collected += 1;
                self.chunk.push(ChunkHit {
                    slot: slot as u32,
                    score,
                });
            }
            merge_chunk_hits(
                &mut self.heaps[qi],
                &mut self.spares[qi],
                &mut self.chunk,
                q.k,
                q.keep_ties,
            );
            if self.heaps[qi].len() >= q.k {
                self.out[qi].1.floors_offered += 1;
                if publish_floor(qi, self.heaps[qi][q.k - 1].score) {
                    self.out[qi].1.floors_published += 1;
                }
            }
        }
    }

    fn finish(
        mut self,
        index: &VectorIndex,
        queries: &[BatchQuery<'_>],
    ) -> Vec<(Vec<ChunkHit>, ScanStats)> {
        for (qi, heap) in self.heaps.into_iter().enumerate() {
            if queries[qi].k > 0 {
                let (total, skipped) = index.part_counts(queries[qi].allow);
                self.out[qi].1.parts = crate::vector::PartCounts {
                    total,
                    skipped,
                    scanned: total.saturating_sub(skipped),
                    ..Default::default()
                };
            }
            self.out[qi].0 = heap;
        }
        self.out
    }
}

/// One query on a provider that does not scan (a graph): a single search of
/// the whole index at a candidate depth of `k`, under the query's own
/// allowlist. Chunking is how a scan adopts floors midway; a graph search
/// has no chunks to cut, and a mask naming one chunk's rows would turn every
/// chunk into its own filtered search of the whole graph. The floor known
/// when the search starts filters its results; the k-th best it finds is
/// offered once, when it ends. The answer is the provider's (approximate
/// unless it enumerated the allowlist) and `keep_ties` cannot widen it: a
/// search that does not visit every row cannot vouch for a tie group.
fn searched_topk(
    index: &VectorIndex,
    query: &BatchQuery<'_>,
    floor: Option<f32>,
    observe: &mut dyn FnMut(ScanPass),
) -> (Vec<ChunkHit>, ScanStats) {
    let mut stats = ScanStats::default();
    if query.k == 0 {
        return (Vec::new(), stats);
    }
    let mut options = VectorSearchOptions::new().with_candidate_depth(query.k);
    options.allow = query.allow;
    options.enumerate_max_rows = query.enumerate_max_rows;
    options.filter_routing = query.controls.filter_routing;
    if query.controls.enumerate_all {
        // Pinned enumeration scores every admitted row; an unfiltered query
        // admits them all.
        options.allow = Some(query.allow.unwrap_or(Mask::All(index.len())));
        options.filter_regime = Some(crate::vector::FilterRegime::Enumerate);
        options.enumerate_max_rows = None;
    }
    options.patience = query.patience;
    options.floor_prune = query.floor_prune;
    if let Some(floor) = floor.filter(|f| !f.is_nan() && *f != f32::NEG_INFINITY) {
        options = options.with_minimum_score(floor);
        stats.floor_updates_applied = 1;
    }
    let started = std::time::Instant::now();
    let results = index.search_with_options(query.vector, query.k, options);
    // Nothing of the encoded image is streamed: the walk reads rows it
    // visits, which the provider counts in its own terms.
    let pass = ScanPass {
        bytes: 0,
        nanos: u64::try_from(started.elapsed().as_nanos()).unwrap_or(u64::MAX),
    };
    observe(pass);
    stats.chunk_calls = 1;
    stats.scan_nanos = pass.nanos;
    stats.parts = results.parts;
    stats.regimes = results.regimes;
    stats.tail_enumerated_rows = results.tail_enumerated_rows;
    stats.patience_stopped = results.patience_stopped;
    stats.floor_pruned = results.floor_pruned;
    let mut hits: Vec<ChunkHit> = results
        .scores_for_query(0)
        .iter()
        .zip(results.indices_for_query(0))
        .filter(|(_, &slot)| slot >= 0)
        .map(|(&score, &slot)| ChunkHit {
            slot: slot as u32,
            score,
        })
        .collect();
    stats.candidates_collected = hits.len() as u64;
    hits.sort_by(|a, b| b.score.total_cmp(&a.score).then(a.slot.cmp(&b.slot)));
    (hits, stats)
}
