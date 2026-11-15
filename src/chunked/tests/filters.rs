use super::*;

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
                enumerate_max_rows: None,
                patience: None,
                floor_prune: None,
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
                allow: Some(Mask::Bools(&even)),
                enumerate_max_rows: None,
                patience: None,
                floor_prune: None,
            },
            BatchQuery {
                vector: &q1,
                k,
                keep_ties: false,
                allow: Some(Mask::Bools(&odd)),
                enumerate_max_rows: None,
                patience: None,
                floor_prune: None,
            },
            BatchQuery {
                vector: &q2,
                k,
                keep_ties: false,
                allow: None,
                enumerate_max_rows: None,
                patience: None,
                floor_prune: None,
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
            enumerate_max_rows: None,
            patience: None,
            floor_prune: None,
        },
        BatchQuery {
            vector: &q1,
            k,
            keep_ties: false,
            allow: None,
            enumerate_max_rows: None,
            patience: None,
            floor_prune: None,
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
