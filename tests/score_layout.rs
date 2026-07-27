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

    let mut big = turbovec::TurboQuantIndex::new_with_calibration(dim, 4, &shift, &scale).unwrap();
    big.add(&corpus);
    let big_res = big.search(&query, 12);

    // A 4-vector shard holding docs 8..12 of the same corpus: doc 9 lives
    // at slot 9 in the big index and slot 1 here.
    let mut small =
        turbovec::TurboQuantIndex::new_with_calibration(dim, 4, &shift, &scale).unwrap();
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
