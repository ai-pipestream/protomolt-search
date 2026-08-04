//! Guard for the premise GLOBAL_RANK fusion relies on: with a shared
//! seeded TQ+ calibration, a vector's turbovec score is bit-identical no
//! matter which index layout it lives in (slot position, index size,
//! partition). If a future turbovec change ever breaks this, the
//! distributed == monolithic exactness guarantee breaks with it.

#[test]
fn same_vector_same_score_across_layouts() {
    let dim = 64;
    let corpus = turbovec_search::harness::unit_vectors(12, dim, 0x3333_0001);
    let (shift, scale) = turbovec_search::harness::fit_calibration(dim, 4, &corpus);
    let query = corpus[..dim].to_vec();

    let mut big = turbovec_search::harness::seeded_index(dim, 4, &shift, &scale);
    big.add(&corpus);
    let big_res = big.search(&query, 12);

    // A 4-vector shard holding docs 8..12 of the same corpus: doc 9 lives
    // at slot 9 in the big index and slot 1 here.
    let mut small =
        turbovec_search::harness::seeded_index(dim, 4, &shift, &scale);
    small.add(&corpus[8 * dim..12 * dim]);
    let small_res = small.search(&query, 4);

    let find = |res: &turbovec::SearchResults, slot: i64| {
        let pos = res
            .indices_for_query(0)
            .iter()
            .position(|&i| i == slot)
            .unwrap();
        res.scores_for_query(0)[pos]
    };
    assert_eq!(
        find(&big_res, 9).to_bits(),
        find(&small_res, 1).to_bits(),
        "same vector, same calibration, different layout: scores must be bit-identical"
    );
}

/// The CAVEAT behind the GLOBAL_RANK / cascade exactness claims: bitwise
/// score identity holds only within same-shape kernel paths. Across
/// differently-sized indexes the kernel's accumulation order can shift
/// scores by a couple of ULPs (the fork documents this for batch shapes).
/// Pinned here so the caveat is executable, not just prose: at dim=128,
/// the same vector in a 12-vector vs 4-vector index may differ — but
/// only within a few ULPs, never more.
#[test]
fn cross_size_scores_differ_only_within_a_few_ulps() {
    let dim = 128;
    let corpus = turbovec_search::harness::unit_vectors(12, dim, 0x3333_0002);
    let (shift, scale) = turbovec_search::harness::fit_calibration(dim, 4, &corpus);
    let query = corpus[..dim].to_vec();

    let score_in = |vecs: &[f32], slot: i64| {
        let mut idx =
            turbovec_search::harness::seeded_index(dim, 4, &shift, &scale);
        idx.add(vecs);
        let res = idx.search(&query, idx.len());
        let pos = res
            .indices_for_query(0)
            .iter()
            .position(|&i| i == slot)
            .unwrap();
        res.scores_for_query(0)[pos]
    };

    let big = score_in(&corpus, 9);
    let small = score_in(&corpus[8 * dim..12 * dim], 1);
    let key = |x: f32| -> i64 {
        let b = x.to_bits() as i32;
        if b < 0 {
            i64::from(i32::MIN) - i64::from(b)
        } else {
            i64::from(b)
        }
    };
    let drift = (key(big) - key(small)).unsigned_abs();
    assert!(
        drift <= 8,
        "cross-shape drift beyond a few ULPs: {:08x} vs {:08x} ({drift} ULPs)",
        big.to_bits(),
        small.to_bits()
    );
}
