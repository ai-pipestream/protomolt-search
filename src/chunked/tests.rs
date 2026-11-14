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

fn build(n: usize, dim: usize) -> VectorIndex {
    let mut idx = VectorIndex::create(crate::vector::EMBEDDED_TURBOVEC, dim, 4).unwrap();
    idx.add(&unit_vectors(n, dim, 0xC40C_0001), dim).unwrap();
    idx
}

/// The bytes a scan charges are the rows it streamed times the
/// image's row bytes, and a batched pass is observed once however
/// many queries rode it (`docs/bandwidth-budget.md`).
#[test]
fn scan_bytes_follow_the_geometry_and_a_shared_pass_counts_once() {
    let (n, dim) = (5_000, 64);
    let index = build(n, dim);
    let row_bytes = index_row_bytes(&index);
    assert_eq!(row_bytes, encoded_row_bytes(dim, 4));
    assert_eq!(
        row_bytes,
        (64 / 2 + 4) as u64,
        "4 bits: two codes a byte, plus the scale"
    );
    assert_eq!(encoded_row_bytes(0, 4), 0, "a lazy index counts nothing");
    assert_eq!(encoded_row_bytes(64, 0), 0);

    // One query, unfiltered: every row streamed exactly once.
    let query = unit_vectors(1, dim, 0x0E50_0002);
    let (_, stats) = chunked_topk(
        &index,
        &query,
        10,
        4,
        &mut || None,
        &mut |_| false,
        false,
        None,
    );
    assert_eq!(stats.bytes_scanned, n as u64 * row_bytes);
    assert!(stats.scan_nanos > 0, "active time is observed");

    // Four unfiltered queries in one batch: one pass, observed once
    // per kernel call, and each query charged the pass it rode.
    let vectors = unit_vectors(4, dim, 0x0E50_0003);
    let queries: Vec<BatchQuery<'_>> = vectors
        .chunks(dim)
        .map(|v| BatchQuery {
            vector: v,
            k: 10,
            keep_ties: false,
            allow: None,
        })
        .collect();
    let mut observed_bytes = 0u64;
    let mut observed_calls = 0u32;
    let results = chunked_topk_batch_observed(
        &index,
        &queries,
        4,
        &mut |_| None,
        &mut |_, _| false,
        &mut |pass| {
            observed_bytes += pass.bytes;
            observed_calls += 1;
        },
    );
    assert_eq!(
        observed_bytes,
        n as u64 * row_bytes,
        "the pass is counted once"
    );
    assert_eq!(
        observed_calls, results[0].1.chunk_calls,
        "one observation per kernel call"
    );
    for (_, stats) in &results {
        assert_eq!(stats.bytes_scanned, n as u64 * row_bytes);
    }

    // A filter that empties whole chunks and whole blocks: only the
    // blocks with an allowed slot are streamed.
    let mut allow = vec![false; n];
    allow[0] = true; // block 0 of chunk 0
    allow[4 * BLOCK_VECTORS + 3] = true; // block 0 of chunk 1 (chunk = 4 blocks)
    let (_, stats) = chunked_topk(
        &index,
        &query,
        10,
        4,
        &mut || None,
        &mut |_| false,
        false,
        Some(&allow),
    );
    assert_eq!(
        stats.bytes_scanned,
        2 * BLOCK_VECTORS as u64 * row_bytes,
        "two blocks kept a slot; the other chunks streamed nothing"
    );
    assert_eq!(stats.chunk_calls, 2);
    let empty = vec![false; n];
    let (_, stats) = chunked_topk(
        &index,
        &query,
        10,
        4,
        &mut || None,
        &mut |_| false,
        false,
        Some(&empty),
    );
    assert_eq!(stats.bytes_scanned, 0);
    assert_eq!(stats.scan_nanos, 0);
}

/// Plain scan (no external floors) must reproduce the index's own
/// top-k exactly, for chunkings from one block up to the whole shard.
#[test]
fn chunked_matches_unchunked_for_all_chunk_sizes() {
    let (n, dim, k) = (5_000, 64, 10);
    let index = build(n, dim);
    let query = unit_vectors(1, dim, 0x0E50_0001);
    let expected = index.search_unfiltered(&query, k);

    for chunk_blocks in [1, 2, 7, 64, 10_000] {
        let (hits, stats) = chunked_topk(
            &index,
            &query,
            k,
            chunk_blocks,
            &mut || None,
            &mut |_| false,
            false,
            None,
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
    let expected = index.search_unfiltered(&query, k);

    for chunk_blocks in [1, 8] {
        let (hits, _) = chunked_topk(
            &index,
            &query,
            k,
            chunk_blocks,
            &mut || None,
            &mut |_| false,
            false,
            None,
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
    let true_kth = index.search_unfiltered(&query, k).scores_for_query(0)[k - 1];

    let (baseline, base_stats) = chunked_topk(
        &index,
        &query,
        k,
        2,
        &mut || None,
        &mut |_| false,
        false,
        None,
    );

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
        &mut |_| false,
        false,
        None,
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
        &mut |f| {
            published.push(f);
            true
        },
        false,
        None,
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
    let mut index = VectorIndex::create(crate::vector::EMBEDDED_TURBOVEC, dim, 4).unwrap();
    index.add(&unit_vectors(n, dim, 0xC40C_0002), dim).unwrap();
    // Six copies of the query vector: identical codes, identical
    // (top) score. They ARE the boundary tie group at k=3.
    let query = unit_vectors(1, dim, 0x0E50_0006);
    for _ in 0..6 {
        index.add(&query, dim).unwrap();
    }

    let (capped, _) = chunked_topk(
        &index,
        &query,
        k,
        1,
        &mut || None,
        &mut |_| false,
        false,
        None,
    );
    assert_eq!(capped.len(), k, "without keep_ties the heap caps at k");

    let (tied, _) = chunked_topk(
        &index,
        &query,
        k,
        1,
        &mut || None,
        &mut |_| false,
        true,
        None,
    );
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
                &mut |f| {
                    published.push(f);
                    true
                },
                ties,
                None,
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
                allow: None,
            })
            .collect();
        let mut published: Vec<Vec<f32>> = vec![Vec::new(); specs.len()];
        let results =
            chunked_topk_batch(&index, &batch, chunk_blocks, &mut |_| None, &mut |qi, f| {
                published[qi].push(f);
                true
            });
        for (qi, ((hits, _), (solo_hits, solo_published))) in results.iter().zip(&solo).enumerate()
        {
            assert_eq!(hits, solo_hits, "query {qi} chunk_blocks {chunk_blocks}");
            assert_eq!(
                &published[qi], solo_published,
                "query {qi} floor sequence, chunk_blocks {chunk_blocks}"
            );
        }
    }
}

/// The exactness oracle for allowlists: a filtered scan must return
/// exactly the top-k OF THE SURVIVORS, which is the unfiltered scan
/// narrowed afterwards. Run for every chunking, because the chunk
/// boundary is where a mask could silently drop a candidate.
#[test]
fn an_allowlist_returns_the_top_k_of_the_survivors() {
    let (n, dim, k) = (5_000, 64, 10);
    let index = build(n, dim);
    let query = unit_vectors(1, dim, 0x0A11_0001);
    // Three selectivities: nearly everything, a third, and a handful.
    for keep in [1usize, 3, 977] {
        let allow: Vec<bool> = (0..n).map(|i| i % keep == 0).collect();
        let survivors = allow.iter().filter(|&&a| a).count();
        // The oracle: score everything, drop the removed, rank.
        let full = index.search_unfiltered(&query, n);
        let mut expected: Vec<ChunkHit> = full
            .indices_for_query(0)
            .iter()
            .zip(full.scores_for_query(0))
            .filter(|&(&slot, _)| slot >= 0 && allow[slot as usize])
            .map(|(&slot, &score)| ChunkHit {
                slot: slot as u32,
                score,
            })
            .collect();
        expected.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.slot.cmp(&b.slot))
        });
        expected.truncate(k.min(survivors));

        for chunk_blocks in [1, 2, 7, 64, 10_000] {
            let (hits, _) = chunked_topk(
                &index,
                &query,
                k,
                chunk_blocks,
                &mut || None,
                &mut |_| false,
                false,
                Some(&allow),
            );
            assert_eq!(hits, expected, "keep 1/{keep}, chunk_blocks {chunk_blocks}");
        }
    }
}

/// An all-true allowlist must produce the unfiltered result. It costs
/// a kernel mask and forfeits batching, which is why `None` exists,
/// but it may never change what comes back.
#[test]
fn an_all_true_allowlist_changes_nothing() {
    let (n, dim, k) = (4_096, 64, 10);
    let index = build(n, dim);
    let query = unit_vectors(1, dim, 0x0A11_0002);
    let allow = vec![true; n];
    let unfiltered = chunked_topk(
        &index,
        &query,
        k,
        3,
        &mut || None,
        &mut |_| false,
        false,
        None,
    )
    .0;
    let allowed = chunked_topk(
        &index,
        &query,
        k,
        3,
        &mut || None,
        &mut |_| false,
        false,
        Some(&allow),
    )
    .0;
    assert_eq!(unfiltered, allowed);
}

/// An allowlist that admits nothing returns nothing and never calls
/// the kernel: every chunk is skipped outright.
#[test]
fn an_empty_allowlist_scans_nothing() {
    let (n, dim, k) = (2_048, 64, 10);
    let index = build(n, dim);
    let query = unit_vectors(1, dim, 0x0A11_0003);
    let allow = vec![false; n];
    let (hits, stats) = chunked_topk(
        &index,
        &query,
        k,
        2,
        &mut || None,
        &mut |_| false,
        false,
        Some(&allow),
    );
    assert!(hits.is_empty());
    assert_eq!(stats.chunk_calls, 0);
    assert_eq!(stats.candidates_collected, 0);
}

/// Queries with DIFFERENT allowlists must not share a kernel call:
/// each batched result has to equal its solo result exactly. A shared
/// union mask would let one query's rejected documents fill another
/// query's chunk quota.
#[test]
fn mixed_allowlists_batch_without_disturbing_each_other() {
    let (n, dim, k) = (8_192, 64, 10);
    let index = build(n, dim);
    let q0 = unit_vectors(1, dim, 0x0A11_1000);
    let q1 = unit_vectors(1, dim, 0x0A11_1001);
    let q2 = unit_vectors(1, dim, 0x0A11_1002);
    // Disjoint halves plus one unfiltered query, so no two members
    // may share a mask.
    let even: Vec<bool> = (0..n).map(|i| i % 2 == 0).collect();
    let odd: Vec<bool> = (0..n).map(|i| i % 2 == 1).collect();

    for chunk_blocks in [1, 4, 64] {
        let solo: Vec<Vec<ChunkHit>> = [
            (&q0, Some(&even)),
            (&q1, Some(&odd)),
            (&q2, None::<&Vec<bool>>),
        ]
        .iter()
        .map(|(q, allow)| {
            chunked_topk(
                &index,
                q,
                k,
                chunk_blocks,
                &mut || None,
                &mut |_| false,
                false,
                allow.map(|a| a.as_slice()),
            )
            .0
        })
        .collect();

        let batch = [
            BatchQuery {
                vector: &q0,
                k,
                keep_ties: false,
                allow: Some(&even),
            },
            BatchQuery {
                vector: &q1,
                k,
                keep_ties: false,
                allow: Some(&odd),
            },
            BatchQuery {
                vector: &q2,
                k,
                keep_ties: false,
                allow: None,
            },
        ];
        let batched =
            chunked_topk_batch(&index, &batch, chunk_blocks, &mut |_| None, &mut |_, _| {
                false
            });
        for (qi, (batched, solo)) in batched.iter().zip(&solo).enumerate() {
            assert_eq!(&batched.0, solo, "query {qi}, chunk_blocks {chunk_blocks}");
        }
    }
}

/// Collapse-by-parent under an allowlist is the collapse of the
/// FILTERED corpus: a parent's representative is its best SURVIVING
/// chunk, and a parent all of whose chunks were removed is gone.
#[test]
fn collapse_under_an_allowlist_collapses_the_survivors() {
    let (n, dim, k) = (4_096, 64, 8);
    let index = build(n, dim);
    let query = unit_vectors(1, dim, 0x0A11_2000);
    let parents: Vec<u64> = (0..n).map(|i| (i / 7) as u64).collect();
    let allow: Vec<bool> = (0..n).map(|i| i % 3 != 0).collect();

    // Oracle: score everything, drop removed chunks, take each
    // parent's best, rank parents.
    let full = index.search_unfiltered(&query, n);
    let mut best: std::collections::HashMap<u64, ChunkHit> = std::collections::HashMap::new();
    for (&slot, &score) in full
        .indices_for_query(0)
        .iter()
        .zip(full.scores_for_query(0))
    {
        if slot < 0 || !allow[slot as usize] {
            continue;
        }
        let hit = ChunkHit {
            slot: slot as u32,
            score,
        };
        let entry = best.entry(parents[slot as usize]).or_insert(hit);
        if ranks_before(hit, *entry) {
            *entry = hit;
        }
    }
    let mut expected: Vec<CollapsedHit> = best
        .into_iter()
        .map(|(parent, h)| CollapsedHit {
            parent,
            slot: h.slot,
            score: h.score,
        })
        .collect();
    expected.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then_with(|| a.slot.cmp(&b.slot))
    });
    expected.truncate(k);

    let (hits, _) = chunked_topk_collapsed(
        &index,
        &query,
        k,
        4,
        &parents,
        &mut || None,
        &mut |_| false,
        Some(&allow),
    );
    assert_eq!(hits, expected);
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
    let kth0 = index.search_unfiltered(&q0, k).scores_for_query(0)[k - 1];

    let solo1 = chunked_topk(&index, &q1, k, 2, &mut || None, &mut |_| false, false, None).0;
    let solo0 = chunked_topk(
        &index,
        &q0,
        k,
        2,
        &mut || Some(kth0),
        &mut |_| false,
        false,
        None,
    )
    .0;

    let batch = [
        BatchQuery {
            vector: &q0,
            k,
            keep_ties: false,
            allow: None,
        },
        BatchQuery {
            vector: &q1,
            k,
            keep_ties: false,
            allow: None,
        },
    ];
    // The true k-th best floors query 0 from the start; query 1 runs
    // unseeded.
    let results = chunked_topk_batch(
        &index,
        &batch,
        2,
        &mut |qi| (qi == 0).then_some(kth0),
        &mut |_, _| false,
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
    let top = index.search_unfiltered(&query, k).scores_for_query(0)[0];

    let (hits, _) = chunked_topk(
        &index,
        &query,
        k,
        64,
        &mut || Some(top + 1.0),
        &mut |_| false,
        false,
        None,
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
    index: &VectorIndex,
    query: &[f32],
    parents: &[u64],
    k: usize,
) -> Vec<CollapsedHit> {
    let n = index.len();
    let all = index.search_unfiltered(query, n);
    let mut best: std::collections::HashMap<u64, ChunkHit> = std::collections::HashMap::new();
    for (&score, &slot) in all.scores_for_query(0).iter().zip(all.indices_for_query(0)) {
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
    out.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then_with(|| a.slot.cmp(&b.slot))
    });
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
                &mut |_| false,
                None,
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
    let mut index = VectorIndex::create(crate::vector::EMBEDDED_TURBOVEC, dim, 4).unwrap();
    index.add(&vectors, dim).unwrap();
    let mut parents = run_parents(n, 5);
    for parent in parents.iter_mut().take(1424).skip(1024) {
        *parent = 7_777_777;
    }

    let want = signature(&collapse_reference(&index, &query, &parents, k));
    for chunk_blocks in [2, 8, 64, 10_000] {
        let (hits, _stats) = chunked_topk_collapsed(
            &index,
            &query,
            k,
            chunk_blocks,
            &parents,
            &mut || None,
            &mut |_| false,
            None,
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
    let (hits, stats) = chunked_topk_collapsed(
        &index,
        &query,
        k,
        8,
        &parents,
        &mut || None,
        &mut |f| {
            published.push(f);
            true
        },
        None,
    );
    assert!(!published.is_empty());
    assert!(
        published.windows(2).all(|w| w[0] <= w[1]),
        "floors must be monotone"
    );
    let kth = hits[k - 1].score;
    assert!(published.iter().all(|&f| f <= kth));

    // A publisher that accepts everything makes the two counters
    // agree, and both must match what it actually saw.
    assert_eq!(stats.floors_offered, stats.floors_published);
    assert_eq!(stats.floors_published as usize, published.len());

    // A publisher that sends nothing is the shape a warmup window or
    // a debounce interval produces at the node. The scan still OFFERS
    // every floor; only the wire count drops. Reporting one number
    // for both is what made those knobs measure as inert.
    let (_, muted) = chunked_topk_collapsed(
        &index,
        &query,
        k,
        8,
        &parents,
        &mut || None,
        &mut |_| false,
        None,
    );
    assert_eq!(muted.floors_offered, stats.floors_offered);
    assert_eq!(muted.floors_published, 0);

    // Seed the true k-th best as an external floor: results identical,
    // strictly fewer candidates collected.
    let (unseeded, base) = chunked_topk_collapsed(
        &index,
        &query,
        k,
        8,
        &parents,
        &mut || None,
        &mut |_| false,
        None,
    );
    let (seeded, pruned) = chunked_topk_collapsed(
        &index,
        &query,
        k,
        8,
        &parents,
        &mut || Some(kth),
        &mut |_| false,
        None,
    );
    assert_eq!(signature(&unseeded), signature(&seeded));
    assert!(pruned.candidates_collected < base.candidates_collected);
}
