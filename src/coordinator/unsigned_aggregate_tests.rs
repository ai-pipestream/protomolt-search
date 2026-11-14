//! Unsigned aggregation and percentile precision checks.

use super::*;
use crate::pb::{AggregateOp as O, AggregateValueType as T};

#[test]
fn percentile_ranks_preserve_wide_counts_and_adjacent_ieee_percentiles() {
    for count in [0, 1, 2, 3, (1u64 << 53) - 1, (1u64 << 53) + 1, u64::MAX] {
        for (p, numerator, denominator) in [
            (0.0, 0, 1),
            (25.0, 1, 4),
            (50.0, 1, 2),
            (75.0, 3, 4),
            (100.0, 1, 1),
        ] {
            let expected = if count == 0 {
                0
            } else {
                ((u128::from(count) * numerator).div_ceil(denominator) as u64).max(1)
            };
            assert_eq!(
                nearest_percentile_rank(p, count),
                expected,
                "p={p} n={count}"
            );
        }
        assert_eq!(
            nearest_percentile_rank(f64::from_bits(1), count),
            u64::from(count > 0)
        );
    }
    assert_eq!(
        nearest_percentile_rank(f64::from_bits(50.0f64.to_bits() - 1), 2),
        1
    );
    assert_eq!(
        nearest_percentile_rank(f64::from_bits(50.0f64.to_bits() + 1), 2),
        2
    );
}

#[test]
fn unsigned_partials_merge_wide_before_checked_result_narrowing() {
    let agg = crate::pb::CompiledAggregation {
        name: "total".into(),
        op: O::Sum as i32,
        ..Default::default()
    };
    let mut merged = AggMerge::new();
    let partial = crate::pb::AggregatePartial {
        vtype: T::Uint as i32,
        present: 1,
        uint_sum_lo: u64::MAX,
        uint_min: u64::MAX,
        uint_max: u64::MAX,
        ..Default::default()
    };
    merged.fold(&partial, &agg).unwrap();
    assert_eq!(
        merged.result("total", O::Sum).unwrap().value,
        Some(crate::pb::aggregate_result::Value::UintValue(u64::MAX))
    );
    merged.fold(&partial, &agg).unwrap();
    assert_eq!(merged.uint_sum, 2 * u128::from(u64::MAX));
    assert!(merged
        .result("total", O::Sum)
        .unwrap_err()
        .message()
        .contains("does not fit u64"));
    assert_eq!(
        merged.result("total", O::Max).unwrap().value,
        Some(crate::pb::aggregate_result::Value::UintValue(u64::MAX))
    );
    let conflicting = crate::pb::AggregatePartial {
        vtype: T::Int as i32,
        present: 0,
        ..Default::default()
    };
    assert!(merged
        .fold(&conflicting, &agg)
        .unwrap_err()
        .message()
        .contains("shards disagree"));
    let mut pct = PctMerge::new();
    pct.fold(
        &crate::pb::PercentilePartial {
            vtype: T::Uint as i32,
            ..Default::default()
        },
        "p",
    )
    .unwrap();
    assert!(pct
        .fold(
            &crate::pb::PercentilePartial {
                vtype: T::Int as i32,
                ..Default::default()
            },
            "p"
        )
        .unwrap_err()
        .message()
        .contains("shards disagree"));
}

#[test]
fn aggregate_count_and_partial_overflows_refuse_without_wrapping() {
    let agg = crate::pb::CompiledAggregation {
        name: "total".into(),
        op: O::Sum as i32,
        ..Default::default()
    };
    let mut merged = AggMerge::new();
    let huge = crate::pb::AggregatePartial {
        vtype: T::Uint as i32,
        present: u64::MAX,
        ..Default::default()
    };
    merged.fold(&huge, &agg).unwrap();
    assert!(merged
        .result("total", O::Count)
        .unwrap_err()
        .message()
        .contains("does not fit"));
    let single = crate::pb::AggregatePartial {
        vtype: T::Uint as i32,
        present: 1,
        uint_sum_lo: 1,
        ..Default::default()
    };
    assert!(merged
        .fold(&single, &agg)
        .unwrap_err()
        .message()
        .contains("count overflows"));
    let mut merged = AggMerge::new();
    let bad_sum = crate::pb::AggregatePartial {
        vtype: T::Uint as i32,
        present: 1,
        uint_sum_hi: u64::MAX,
        uint_sum_lo: u64::MAX,
        ..Default::default()
    };
    merged.fold(&bad_sum, &agg).unwrap();
    assert!(merged
        .fold(&single, &agg)
        .unwrap_err()
        .message()
        .contains("sum overflows u128"));
}
