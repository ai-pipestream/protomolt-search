use super::*;

/// The bytes a scan charges are the rows it streamed times the
/// image's row bytes.
#[test]
fn scan_bytes_follow_the_geometry() {
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
}

/// A batched pass is observed once however many queries rode it.
#[test]
fn a_shared_pass_counts_once() {
    let (n, dim) = (5_000, 64);
    let index = build(n, dim);
    let row_bytes = index_row_bytes(&index);
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
            enumerate_max_rows: None,
            patience: None,
            floor_prune: None,
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
}

/// Only the blocks holding an allowed slot are streamed.
#[test]
fn filtered_scans_stream_only_blocks_with_an_allowed_slot() {
    let (n, dim) = (5_000, 64);
    let index = build(n, dim);
    let row_bytes = index_row_bytes(&index);
    let query = unit_vectors(1, dim, 0x0E50_0002);
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
