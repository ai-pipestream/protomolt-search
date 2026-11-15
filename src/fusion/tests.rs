use super::*;

#[test]
fn merge_legs_reconstructs_global_ranking() {
    // Two interleaved shard lists (already score-descending locally).
    let merged = merge_legs_by_score(vec![
        (0, vec![(10, 0.9), (11, 0.5), (12, 0.1)]),
        (1, vec![(20, 0.8), (21, 0.6)]),
    ]);
    let ids: Vec<u64> = merged.iter().map(|h| h.0).collect();
    assert_eq!(ids, vec![10, 20, 21, 11, 12]);
    // Global ranks are 1-based positions in the merged list.
    assert_eq!(merged[0].2, 1);
    assert_eq!(merged[4].2, 5);
}

#[test]
fn merge_legs_ties_break_by_shard_then_doc_id() {
    let merged = merge_legs_by_score(vec![
        (2, vec![(7, 1.0)]),
        (0, vec![(9, 1.0)]),
        (1, vec![(3, 1.0), (1, 1.0)]),
    ]);
    // All scores tie at 1.0: shard 0's doc 9 first, then shard 1's
    // docs 1 and 3 (doc id ascending), then shard 2's doc 7.
    let order: Vec<u64> = merged.iter().map(|h| h.0).collect();
    assert_eq!(order, vec![9, 1, 3, 7]);
    // Same total order regardless of input shard order.
    let again = merge_legs_by_score(vec![
        (1, vec![(3, 1.0), (1, 1.0)]),
        (2, vec![(7, 1.0)]),
        (0, vec![(9, 1.0)]),
    ]);
    assert_eq!(merged, again);
}

#[test]
fn merge_legs_empty_inputs() {
    assert!(merge_legs_by_score(Vec::new()).is_empty());
    assert!(merge_legs_by_score(vec![(0, vec![]), (1, vec![])]).is_empty());
}

fn leg(hits: &[(u64, f64)], weight: f64) -> Leg {
    Leg {
        hits: hits.to_vec(),
        weight,
    }
}

#[test]
fn known_rankings_produce_expected_order_and_scores() {
    // Leg A: [1, 2, 3]; leg B: [3, 1, 9]; both weight 1, rrf_k=60.
    let fused = rrf_fuse(
        &[
            leg(&[(1, 0.9), (2, 0.8), (3, 0.7)], 1.0),
            leg(&[(3, 5.0), (1, 4.0), (9, 3.0)], 1.0),
        ],
        60.0,
        10,
    );
    // Doc 1: 1/61 + 1/62; doc 3: 1/63 + 1/61; doc 2: 1/62; doc 9: 1/63.
    let s1 = 1.0 / 61.0 + 1.0 / 62.0;
    let s3 = 1.0 / 63.0 + 1.0 / 61.0;
    assert_eq!(fused.len(), 4);
    assert_eq!(fused[0].doc_id, 1);
    assert!((fused[0].fused_score - s1).abs() < 1e-15);
    assert_eq!(fused[1].doc_id, 3);
    assert!((fused[1].fused_score - s3).abs() < 1e-15);
    assert_eq!(fused[2].doc_id, 2);
    assert_eq!(fused[3].doc_id, 9);
    // Provenance: doc 1 is rank 1 in leg A, rank 2 in leg B.
    assert_eq!(fused[0].leg_ranks, vec![Some(1), Some(2)]);
    assert_eq!(fused[0].leg_scores, vec![Some(0.9), Some(4.0)]);
}

#[test]
fn weights_shift_order() {
    // Doc 1 tops leg A, doc 2 tops leg B. Unweighted they tie (id
    // tie-break); upweighting leg B must put doc 2 first.
    let legs = || [leg(&[(1, 0.9)], 1.0), leg(&[(2, 5.0)], 1.0)];
    let fused = rrf_fuse(
        &[
            Leg {
                weight: 1.0,
                ..leg(&[(1, 0.9)], 1.0)
            },
            Leg {
                weight: 3.0,
                ..leg(&[(2, 5.0)], 1.0)
            },
        ],
        60.0,
        10,
    );
    assert_eq!(fused[0].doc_id, 2);
    assert!((fused[0].fused_score - 3.0 / 61.0).abs() < 1e-15);
    let _ = legs;
}

#[test]
fn rrf_k_constant_is_respected() {
    let fused60 = rrf_fuse(&[leg(&[(1, 1.0), (2, 0.5)], 1.0)], 60.0, 10);
    let fused10 = rrf_fuse(&[leg(&[(1, 1.0), (2, 0.5)], 1.0)], 10.0, 10);
    assert!((fused60[0].fused_score - 1.0 / 61.0).abs() < 1e-15);
    assert!((fused10[0].fused_score - 1.0 / 11.0).abs() < 1e-15);
    // Smaller constant: steeper rank discount, larger spread.
    let spread60 = fused60[0].fused_score - fused60[1].fused_score;
    let spread10 = fused10[0].fused_score - fused10[1].fused_score;
    assert!(spread10 > spread60);
}

#[test]
fn doc_in_one_gets_no_rank_for_the_other() {
    let fused = rrf_fuse(
        &[leg(&[(1, 0.9), (2, 0.8)], 1.0), leg(&[(2, 5.0)], 1.0)],
        60.0,
        10,
    );
    assert_eq!(fused[0].doc_id, 2, "two legs beat one");
    assert_eq!(fused[1].doc_id, 1);
    assert_eq!(fused[1].leg_ranks, vec![Some(1), None]);
    assert_eq!(fused[1].leg_scores, vec![Some(0.9), None]);
}

#[test]
fn empty_leg_preserves_the_other_legs_order() {
    let fused = rrf_fuse(&[leg(&[(5, 0.1), (3, 0.05)], 1.0), leg(&[], 1.0)], 60.0, 10);
    let ids: Vec<u64> = fused.iter().map(|h| h.doc_id).collect();
    assert_eq!(ids, vec![5, 3]);
    assert!(fused.iter().all(|h| h.leg_ranks[1].is_none()));
}

#[test]
fn empty_legs_and_zero_weights() {
    assert!(rrf_fuse(&[leg(&[], 1.0), leg(&[], 1.0)], 60.0, 10).is_empty());
    assert!(rrf_fuse(&[leg(&[(1, 0.5)], 0.0)], 60.0, 10).is_empty());
}

#[test]
fn tied_scores_share_a_competition_rank() {
    // Two docs tie at 0.5 in the leg: both get rank 2, the next doc
    // gets rank 4 (ranks skip). Fused scores of the tied docs are
    // identical — the layout-invariance property GLOBAL_RANK relies on.
    let fused = rrf_fuse(
        &[leg(&[(1, 0.9), (2, 0.5), (3, 0.5), (4, 0.1)], 1.0)],
        60.0,
        10,
    );
    let by_id = |id: u64| fused.iter().find(|h| h.doc_id == id).unwrap();
    assert_eq!(by_id(2).leg_ranks[0], Some(2));
    assert_eq!(by_id(3).leg_ranks[0], Some(2));
    assert_eq!(by_id(4).leg_ranks[0], Some(4));
    assert_eq!(by_id(2).fused_score, by_id(3).fused_score);
    assert_eq!(by_id(2).fused_score, 1.0 / 62.0);
    assert_eq!(by_id(4).fused_score, 1.0 / 64.0);
}

#[test]
fn merge_legs_assigns_competition_ranks() {
    let merged = merge_legs_by_score(vec![
        (0, vec![(10, 1.0), (11, 1.0)]),
        (1, vec![(20, 1.0), (21, 0.5)]),
    ]);
    let rank_of = |id: u64| merged.iter().find(|h| h.0 == id).unwrap().2;
    assert_eq!(rank_of(10), 1);
    assert_eq!(rank_of(11), 1);
    assert_eq!(rank_of(20), 1);
    assert_eq!(rank_of(21), 4);
}

#[test]
fn depth_truncates() {
    let fused = rrf_fuse(&[leg(&[(1, 0.9), (2, 0.8), (3, 0.7)], 1.0)], 60.0, 2);
    assert_eq!(fused.len(), 2);
    assert_eq!(fused[1].doc_id, 2);
}

#[test]
fn blend_min_max_arithmetic_known_values() {
    // Leg A spans [0, 10] -> normalized [1.0, 0.5, 0.0]; leg B spans
    // [2, 4] -> doc 2: 1.0, doc 1: 0.0. Equal weights:
    //   doc 2 = (0.5 + 1.0)/2 = 0.75
    //   doc 1 = (1.0 + 0.0)/2 = 0.50
    //   doc 3 = (0.0 + absent)/2 = 0.0
    let fused = blend_fuse(
        &[
            leg(&[(1, 10.0), (2, 5.0), (3, 0.0)], 1.0),
            leg(&[(2, 4.0), (1, 2.0)], 1.0),
        ],
        10,
        Normalization::MinMax,
        Combination::Arithmetic,
        10,
    );
    assert_eq!(fused.len(), 3);
    assert_eq!(fused[0].doc_id, 2);
    assert!((fused[0].fused_score - 0.75).abs() < 1e-12);
    assert_eq!(fused[1].doc_id, 1);
    assert!((fused[1].fused_score - 0.50).abs() < 1e-12);
    assert_eq!(fused[2].doc_id, 3);
    assert_eq!(fused[2].fused_score, 0.0);
    // Provenance carries competition ranks and RAW scores.
    assert_eq!(fused[0].leg_ranks, vec![Some(2), Some(1)]);
    assert_eq!(fused[0].leg_scores, vec![Some(5.0), Some(4.0)]);
    assert_eq!(fused[2].leg_ranks, vec![Some(3), None]);
}

#[test]
fn blend_weights_scale_arithmetic() {
    // Upweighting leg B (weight 3): doc 2 = (0.5 + 3*1.0)/4 = 0.875,
    // doc 1 = (1.0 + 3*0.0)/4 = 0.25.
    let fused = blend_fuse(
        &[
            leg(&[(1, 10.0), (2, 5.0), (3, 0.0)], 1.0),
            leg(&[(2, 4.0), (1, 2.0)], 3.0),
        ],
        10,
        Normalization::MinMax,
        Combination::Arithmetic,
        10,
    );
    assert_eq!(fused[0].doc_id, 2);
    assert!((fused[0].fused_score - 0.875).abs() < 1e-12);
    let doc1 = fused.iter().find(|h| h.doc_id == 1).unwrap();
    assert!((doc1.fused_score - 0.25).abs() < 1e-12);
}

#[test]
fn blend_degenerate_leg_normalizes_to_one() {
    // Every retained score equal (min == max): min-max maps all to
    // 1.0 rather than dividing by zero.
    let fused = blend_fuse(
        &[leg(&[(1, 7.0), (2, 7.0)], 1.0)],
        10,
        Normalization::MinMax,
        Combination::Arithmetic,
        10,
    );
    assert!(fused.iter().all(|h| (h.fused_score - 1.0).abs() < 1e-12));
    // Both docs tie: shared competition rank 1.
    assert!(fused.iter().all(|h| h.leg_ranks[0] == Some(1)));
}

#[test]
fn blend_z_score_centers_and_preserves_order() {
    // Scores [2, 4, 6]: mean 4, population sigma sqrt(8/3). One leg,
    // weight 1 -> fused = z directly (negative allowed).
    let fused = blend_fuse(
        &[leg(&[(3, 6.0), (2, 4.0), (1, 2.0)], 1.0)],
        10,
        Normalization::ZScore,
        Combination::Arithmetic,
        10,
    );
    let sigma = (8.0f64 / 3.0).sqrt();
    assert_eq!(fused[0].doc_id, 3);
    assert!((fused[0].fused_score - 2.0 / sigma).abs() < 1e-12);
    assert_eq!(fused[1].doc_id, 2);
    assert!(fused[1].fused_score.abs() < 1e-12);
    assert_eq!(fused[2].doc_id, 1);
    assert!((fused[2].fused_score + 2.0 / sigma).abs() < 1e-12);
}

#[test]
fn blend_geometric_and_harmonic_skip_nonpositive_legs() {
    // Doc 1: normalized 1.0 in A, 0.0 in B (boundary) -> B skipped,
    // geometric = 1.0. Doc 2: 0.5 in A, 1.0 in B -> geometric
    // sqrt(0.5), harmonic 2/(1/0.5 + 1/1.0) = 2/3. Doc 3: 0.0 in A
    // only -> no positive leg, fused 0.
    let legs_ab = [
        leg(&[(1, 10.0), (2, 5.0), (3, 0.0)], 1.0),
        leg(&[(2, 4.0), (1, 2.0)], 1.0),
    ];
    let geo = blend_fuse(
        &legs_ab,
        10,
        Normalization::MinMax,
        Combination::Geometric,
        10,
    );
    let by_id =
        |hits: &[FusedHit], id: u64| hits.iter().find(|h| h.doc_id == id).unwrap().fused_score;
    assert!((by_id(&geo, 1) - 1.0).abs() < 1e-12);
    assert!((by_id(&geo, 2) - 0.5f64.sqrt()).abs() < 1e-12);
    assert_eq!(by_id(&geo, 3), 0.0);
    let har = blend_fuse(
        &legs_ab,
        10,
        Normalization::MinMax,
        Combination::Harmonic,
        10,
    );
    assert!((by_id(&har, 1) - 1.0).abs() < 1e-12);
    assert!((by_id(&har, 2) - 2.0 / 3.0).abs() < 1e-12);
    assert_eq!(by_id(&har, 3), 0.0);
}

#[test]
fn blend_truncates_tie_complete_and_stats_follow() {
    // leg_k = 3 with scores [9, 8, 7, 7, 1]: boundary 7 keeps the
    // whole tie group (4 docs), drops the 1. Min-max stats over the
    // RETAINED set: min 7, max 9 -> doc 1: 1.0, doc 2: 0.5, boundary
    // docs 0.0. The dropped doc appears nowhere.
    let fused = blend_fuse(
        &[leg(
            &[(1, 9.0), (2, 8.0), (3, 7.0), (4, 7.0), (5, 1.0)],
            1.0,
        )],
        3,
        Normalization::MinMax,
        Combination::Arithmetic,
        10,
    );
    assert_eq!(fused.len(), 4);
    assert!(fused.iter().all(|h| h.doc_id != 5));
    assert!((fused[0].fused_score - 1.0).abs() < 1e-12);
    assert!((fused[1].fused_score - 0.5).abs() < 1e-12);
    assert_eq!(fused[2].fused_score, 0.0);
    assert_eq!(fused[3].fused_score, 0.0);
    // Boundary tie group shares competition rank 3.
    assert_eq!(fused[2].leg_ranks[0], Some(3));
    assert_eq!(fused[3].leg_ranks[0], Some(3));
}

#[test]
fn blend_none_passes_raw_scores_through() {
    let fused = blend_fuse(
        &[leg(&[(1, 0.8), (2, 0.2)], 1.0), leg(&[(1, 0.4)], 1.0)],
        10,
        Normalization::None,
        Combination::Arithmetic,
        10,
    );
    assert_eq!(fused[0].doc_id, 1);
    assert!((fused[0].fused_score - 0.6).abs() < 1e-12);
    assert!((fused[1].fused_score - 0.1).abs() < 1e-12);
}

#[test]
fn blend_empty_legs_and_zero_weights() {
    assert!(blend_fuse(
        &[leg(&[], 1.0)],
        10,
        Normalization::MinMax,
        Combination::Arithmetic,
        10
    )
    .is_empty());
    assert!(blend_fuse(
        &[leg(&[(1, 0.5)], 0.0)],
        10,
        Normalization::MinMax,
        Combination::Arithmetic,
        10
    )
    .is_empty());
}
