use super::*;

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
                ChunkedScan {
                    index: &index,
                    query: &query,
                    k,
                    chunk_blocks,
                    allow: None,
                },
                &parents,
                &mut || None,
                &mut |_| false,
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
            ChunkedScan {
                index: &index,
                query: &query,
                k,
                chunk_blocks,
                allow: None,
            },
            &parents,
            &mut || None,
            &mut |_| false,
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

    let scan = ChunkedScan {
        index: &index,
        query: &query,
        k,
        chunk_blocks: 8,
        allow: None,
    };

    let mut published = Vec::new();
    let (hits, stats) = chunked_topk_collapsed(scan, &parents, &mut || None, &mut |f| {
        published.push(f);
        true
    });
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
    let (_, muted) = chunked_topk_collapsed(scan, &parents, &mut || None, &mut |_| false);
    assert_eq!(muted.floors_offered, stats.floors_offered);
    assert_eq!(muted.floors_published, 0);

    // Seed the true k-th best as an external floor: results identical,
    // strictly fewer candidates collected.
    let (unseeded, base) = chunked_topk_collapsed(scan, &parents, &mut || None, &mut |_| false);
    let (seeded, pruned) =
        chunked_topk_collapsed(scan, &parents, &mut || Some(kth), &mut |_| false);
    assert_eq!(signature(&unseeded), signature(&seeded));
    assert!(pruned.candidates_collected < base.candidates_collected);
}
