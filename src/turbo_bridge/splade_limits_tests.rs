use super::*;

fn tiny_field() -> SpladeFieldIndex {
    let mut field = SpladeFieldIndex::default();
    for (doc, impact) in [(2, 8), (5, 8), (7, 4)] {
        field.insert_doc(doc, 1.0_f32.to_bits(), &[("a".into(), impact)]);
    }
    field
}

#[test]
fn pathological_k_reserves_only_possible_matches_without_changing_floor() {
    let field = tiny_field();
    let query = [("a".into(), 1.0)];
    let mut bytes = Vec::new();
    field.write(&mut bytes).unwrap();
    let reader = SpladeSectionReader::open(&bytes).unwrap();
    // usize::MAX previously overflowed k+1 before allocation. Neither API
    // should reserve caller-sized storage for these three possible matches.
    for k in [u32::MAX as usize, usize::MAX] {
        let (hits, floor, _) = reader.score_top_k_with_floor(&query, 0.0, 3.0, k, |_| true);
        assert_eq!(hits, [(2, 8.0), (5, 8.0), (7, 4.0)]);
        assert!(hits.capacity() <= 3);
        assert_eq!(floor, 3.0, "unfilled top-k keeps the supplied floor");
        let (heap_hits, _, _) = field.score_top_k_with_floor(&query, 0.0, 3.0, k, |_| true);
        assert_eq!(heap_hits, hits);
    }
}

#[test]
fn bounded_reservation_preserves_admission_ties_and_top_k_boundaries() {
    let field = tiny_field();
    let query = [("a".into(), 1.0)];
    let expected = [
        Vec::new(),
        vec![(2, 8.0)],
        vec![(2, 8.0), (5, 8.0)],
        vec![(2, 8.0), (5, 8.0), (7, 4.0)],
    ];
    for (k, expected) in expected.into_iter().enumerate() {
        assert_eq!(field.score_top_k(&query, 0.0, k, |_| true).0, expected);
    }
    assert_eq!(
        field.score_top_k(&query, 0.0, 1, |doc| doc != 2).0,
        [(5, 8.0)]
    );
    assert_eq!(
        field.score_top_k(&query, 0.0, usize::MAX, |doc| doc == 7).0,
        [(7, 4.0)]
    );
    assert!(field
        .score_top_k(&query, 0.0, usize::MAX, |_| false)
        .0
        .is_empty());
}

#[test]
fn pathological_k_on_empty_or_nonmatching_fields_stays_empty() {
    let query = [("a".into(), 1.0)];
    assert!(SpladeFieldIndex::default()
        .score_top_k(&query, 0.0, usize::MAX, |_| true)
        .0
        .is_empty());
    let field = tiny_field();
    assert!(field
        .score_top_k(&[], 0.0, usize::MAX, |_| true)
        .0
        .is_empty());
    assert!(field
        .score_top_k(&[("missing".into(), 1.0)], 0.0, usize::MAX, |_| true)
        .0
        .is_empty());
}
