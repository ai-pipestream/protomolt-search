//! Conservative decomposed floors across seeding and later vector batches.

use super::*;

fn bounds() -> LowerBounds {
    LowerBounds {
        k: 2,
        vector_weight: 1.0,
        bm25_weight: 1.0,
        weighted_bm25_max: 10.0,
        minimum_vector: None,
        heap: std::collections::BinaryHeap::new(),
        lower: None,
        last_floor: f32::NEG_INFINITY,
    }
}

#[test]
fn decomposed_seed_and_later_bounds_preserve_boundary_candidates() {
    let mut bounds = bounds();
    bounds.push(-1.0, 10.0);
    bounds.push(2.0, 3.0);
    let first = bounds.initial_floor().unwrap();
    assert!(first < -5.0);
    bounds.push(3.0, 7.0);
    let second = bounds.raised_floor().unwrap();
    assert!(second > first && second < -1.0);
    // A candidate reaching the exact kth fused score must survive its vector floor.
    for lexical in [0.0f32, 1.0, 5.0, 10.0] {
        let vector = 9.0 - lexical;
        assert!(vector >= second);
        assert_eq!(bounds.fused(vector, lexical), 9.0);
    }
    bounds.push(-100.0, 0.0);
    assert!(bounds.raised_floor().is_none());
    assert_eq!(bounds.last_floor, second);
}

#[test]
fn vector_result_floor_excludes_seeds_without_becoming_a_fused_lower_bound() {
    let mut bounds = bounds();
    bounds.minimum_vector = Some(4.0);
    bounds.push(3.0, 10.0);
    assert!(bounds.heap.is_empty());
    assert_eq!(bounds.initial_floor(), Some(4.0));
    bounds.push(5.0, 0.0);
    assert!(bounds.raised_floor().is_none());
    bounds.push(6.0, 0.0);
    assert!(bounds.raised_floor().is_none());
    assert_eq!(bounds.lower, Some(5.0));
    assert_eq!(bounds.last_floor, 4.0);
}
