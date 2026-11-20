//! Normalization, missing-value policy, and the golden value of every
//! operation, recomputed from the reported provenance.

use super::*;

#[test]
fn min_max_normalizes_the_pool_and_degenerates_to_one() {
    let s = validate(&scorer(
        CompositeScoreOperation::WeightedSum,
        vec![base_dim("d")],
    ))
    .unwrap();
    let mut hits = vec![hit(0, 4.0, &[]), hit(1, 2.0, &[]), hit(2, 3.0, &[])];
    s.apply(&mut hits, &[]).unwrap();
    // (4-2)/(4-2)=1, (3-2)/2=0.5, (2-2)/2=0.
    assert_eq!(hits[0].score, 1.0);
    assert_eq!(hits[0].doc_id, 0);
    assert_eq!(hits[1].score, 0.5);
    assert_eq!(hits[2].score, 0.0);

    let mut equal = vec![hit(0, 7.0, &[]), hit(1, 7.0, &[])];
    s.apply(&mut equal, &[]).unwrap();
    assert!(equal.iter().all(|h| h.score == 1.0));
}

#[test]
fn z_score_normalizes_and_degenerates_to_zero() {
    let mut d = base_dim("d");
    d.normalization = ScoreNormalization::ZScore as i32;
    let s = validate(&scorer(CompositeScoreOperation::WeightedSum, vec![d])).unwrap();
    let mut hits = vec![hit(0, 1.0, &[]), hit(1, 3.0, &[])];
    s.apply(&mut hits, &[]).unwrap();
    // mean 2, population std 1: z = +1 and -1.
    assert_eq!(hits[0].score, 1.0);
    assert_eq!(hits[0].doc_id, 1);
    assert_eq!(hits[1].score, -1.0);

    let mut equal = vec![hit(0, 5.0, &[]), hit(1, 5.0, &[])];
    s.apply(&mut equal, &[]).unwrap();
    assert!(equal.iter().all(|h| h.score == 0.0));
}

#[test]
fn none_is_raw_identity() {
    let mut d = base_dim("d");
    d.normalization = ScoreNormalization::None as i32;
    let s = validate(&scorer(CompositeScoreOperation::WeightedSum, vec![d])).unwrap();
    let mut hits = vec![hit(0, 4.5, &[]), hit(1, 2.5, &[])];
    s.apply(&mut hits, &[]).unwrap();
    assert_eq!(hits[0].score, 4.5);
    assert_eq!(hits[1].score, 2.5);
}

// -- missing policies --

#[test]
fn zero_contributes_normalized_zero_with_weight_kept() {
    // Mean over (present base, missing signal): ZERO keeps the
    // weight in the denominator, halving the present dimension.
    let mut hits = vec![hit(0, 2.0, &[("lex", 3.0)]), hit(1, 1.0, &[])];
    let s = validate(&scorer(
        CompositeScoreOperation::WeightedMean,
        vec![base_dim("b"), query_dim("l", "lex")],
    ))
    .unwrap();
    s.apply(&mut hits, &[]).unwrap();
    // doc 0: base minmax -> 1, lex present alone -> degenerate 1;
    // mean = 1. doc 1: base -> 0, lex missing -> ZERO; mean = 0.
    assert_eq!(hits[0].score, 1.0);
    assert_eq!(hits[0].doc_id, 0);
    assert_eq!(hits[1].score, 0.0);
    assert!(!hits[1].dimensions[1].skipped);
    assert_eq!(hits[1].dimensions[1].raw, None);
}

#[test]
fn skip_drops_the_weight_from_the_mean() {
    let mut d = query_dim("l", "lex");
    d.missing = MissingScorePolicy::Skip as i32;
    let mut hits = vec![hit(0, 2.0, &[("lex", 3.0)]), hit(1, 1.0, &[])];
    let s = validate(&scorer(
        CompositeScoreOperation::WeightedMean,
        vec![base_dim("b"), d],
    ))
    .unwrap();
    s.apply(&mut hits, &[]).unwrap();
    // doc 1: lex SKIPPED, mean over base alone = 0 (its minmax is
    // 0) — same value as ZERO here, but the report says skipped.
    let doc1 = hits.iter().find(|h| h.doc_id == 1).unwrap();
    assert!(doc1.dimensions[1].skipped);
    assert_eq!(doc1.dimensions[1].contribution, 0.0);
}

#[test]
fn error_refuses_naming_document_and_dimension() {
    let mut d = query_dim("l", "lex");
    d.missing = MissingScorePolicy::Error as i32;
    let mut hits = vec![hit(0, 2.0, &[("lex", 3.0)]), hit(7, 1.0, &[])];
    let s = validate(&scorer(CompositeScoreOperation::WeightedSum, vec![d])).unwrap();
    let err = s.apply(&mut hits, &[]).unwrap_err();
    assert!(matches!(err, super::LtrError::FailedPrecondition(_)));
    assert!(err.message().contains("document 7"));
    assert!(err.message().contains("\"l\""));
}

// -- operations --

/// Two docs, two NONE-normalized signal dimensions with weights 2
/// and 3 — golden values per operation.
fn two_signal_fixture() -> (Vec<QueryHit>, Vec<ScoreDimension>) {
    let hits = vec![
        hit(0, 0.0, &[("lex", 4.0), ("vec", 0.5)]),
        hit(1, 0.0, &[("lex", 1.0), ("vec", 8.0)]),
    ];
    let mut d1 = query_dim("l", "lex");
    d1.weight = Some(2.0);
    d1.normalization = ScoreNormalization::None as i32;
    let mut d2 = query_dim("v", "vec");
    d2.weight = Some(3.0);
    d2.normalization = ScoreNormalization::None as i32;
    (hits, vec![d1, d2])
}

#[test]
fn weighted_sum_golden() {
    let (mut hits, dims) = two_signal_fixture();
    let s = validate(&scorer(CompositeScoreOperation::WeightedSum, dims)).unwrap();
    s.apply(&mut hits, &[]).unwrap();
    // doc 1: 2*1 + 3*8 = 26; doc 0: 2*4 + 3*0.5 = 9.5.
    assert_eq!(hits[0].doc_id, 1);
    assert_eq!(hits[0].score, 26.0);
    assert_eq!(hits[1].score, 9.5);
    assert_eq!(hits[0].dimensions[0].contribution, 2.0);
    assert_eq!(hits[0].dimensions[1].contribution, 24.0);
}

#[test]
fn weighted_mean_golden() {
    let (mut hits, dims) = two_signal_fixture();
    let s = validate(&scorer(CompositeScoreOperation::WeightedMean, dims)).unwrap();
    s.apply(&mut hits, &[]).unwrap();
    // doc 1: 26/5 = 5.2; doc 0: 9.5/5 = 1.9.
    assert_eq!(hits[0].score, 5.2);
    assert_eq!(hits[1].score, 1.9);
}

#[test]
fn maximum_golden() {
    let (mut hits, dims) = two_signal_fixture();
    let s = validate(&scorer(CompositeScoreOperation::Maximum, dims)).unwrap();
    s.apply(&mut hits, &[]).unwrap();
    // doc 1: max(2, 24) = 24; doc 0: max(8, 1.5) = 8.
    assert_eq!(hits[0].score, 24.0);
    assert_eq!(hits[1].score, 8.0);
}

#[test]
fn product_golden() {
    let (mut hits, dims) = two_signal_fixture();
    let s = validate(&scorer(CompositeScoreOperation::Product, dims)).unwrap();
    s.apply(&mut hits, &[]).unwrap();
    // doc 1: 2 * 24 = 48; doc 0: 8 * 1.5 = 12.
    assert_eq!(hits[0].score, 48.0);
    assert_eq!(hits[1].score, 12.0);
}

#[test]
fn geometric_mean_golden() {
    let (mut hits, dims) = two_signal_fixture();
    let s = validate(&scorer(CompositeScoreOperation::GeometricMean, dims)).unwrap();
    s.apply(&mut hits, &[]).unwrap();
    // doc 1: 1^(2/5) * 8^(3/5); doc 0: 4^(2/5) * 0.5^(3/5).
    let d1 = 8f64.powf(0.6);
    let d0 = 4f64.powf(0.4) * 0.5f64.powf(0.6);
    assert_eq!(hits[0].score, d1 as f32);
    assert_eq!(hits[1].score, d0 as f32);
}

#[test]
fn harmonic_mean_golden() {
    let (mut hits, dims) = two_signal_fixture();
    let s = validate(&scorer(CompositeScoreOperation::HarmonicMean, dims)).unwrap();
    s.apply(&mut hits, &[]).unwrap();
    // doc 1: 5 / (2/1 + 3/8) = 5/2.375; doc 0: 5 / (2/4 + 3/0.5).
    let d1 = 5.0 / (2.0 + 0.375);
    let d0 = 5.0 / (0.5 + 6.0);
    assert_eq!(hits[0].score, d1 as f32);
    assert_eq!(hits[1].score, d0 as f32);
}

#[test]
fn geometric_and_harmonic_skip_non_positive_values() {
    // NONE normalization keeps a raw zero: the geometric mean must
    // skip it (renormalizing onto the positive dimension), not
    // zero the product.
    let mut hits = vec![hit(0, 0.0, &[("lex", 0.0), ("vec", 8.0)])];
    let mut d1 = query_dim("l", "lex");
    d1.normalization = ScoreNormalization::None as i32;
    let mut d2 = query_dim("v", "vec");
    d2.normalization = ScoreNormalization::None as i32;
    let s = validate(&scorer(
        CompositeScoreOperation::GeometricMean,
        vec![d1, d2],
    ))
    .unwrap();
    s.apply(&mut hits, &[]).unwrap();
    assert_eq!(hits[0].score, 8.0);
    assert!(hits[0].dimensions[0].skipped);
    assert!(!hits[0].dimensions[1].skipped);
}

#[test]
fn no_positive_value_scores_zero() {
    let mut hits = vec![hit(0, 0.0, &[("lex", -1.0)])];
    let mut d = query_dim("l", "lex");
    d.normalization = ScoreNormalization::None as i32;
    let s = validate(&scorer(CompositeScoreOperation::HarmonicMean, vec![d])).unwrap();
    s.apply(&mut hits, &[]).unwrap();
    assert_eq!(hits[0].score, 0.0);
    assert!(hits[0].dimensions[0].skipped);
}

#[test]
fn zero_weight_is_reported_but_excluded() {
    let (mut hits, mut dims) = two_signal_fixture();
    dims[1].weight = Some(0.0);
    let s = validate(&scorer(CompositeScoreOperation::WeightedSum, dims)).unwrap();
    s.apply(&mut hits, &[]).unwrap();
    // Only the lex dimension counts: doc 0 wins on 2*4 = 8.
    assert_eq!(hits[0].doc_id, 0);
    assert_eq!(hits[0].score, 8.0);
    assert!(hits[0].dimensions[1].skipped);
    // Still evaluated and reported: the raw value is there.
    assert_eq!(hits[0].dimensions[1].raw, Some(0.5));
    assert_eq!(hits[0].dimensions[1].contribution, 0.0);
}

#[test]
fn negative_weight_penalizes_under_weighted_sum() {
    let (mut hits, mut dims) = two_signal_fixture();
    dims[1].weight = Some(-3.0);
    let s = validate(&scorer(CompositeScoreOperation::WeightedSum, dims)).unwrap();
    s.apply(&mut hits, &[]).unwrap();
    // doc 0: 8 - 1.5 = 6.5; doc 1: 2 - 24 = -22.
    assert_eq!(hits[0].doc_id, 0);
    assert_eq!(hits[0].score, 6.5);
    assert_eq!(hits[1].score, -22.0);
}

// -- determinism and ordering --

#[test]
fn ties_break_by_doc_id_on_the_wire_score() {
    let mut hits = vec![hit(9, 3.0, &[]), hit(2, 3.0, &[]), hit(5, 3.0, &[])];
    let s = validate(&scorer(
        CompositeScoreOperation::WeightedSum,
        vec![base_dim("d")],
    ))
    .unwrap();
    s.apply(&mut hits, &[]).unwrap();
    let ids: Vec<u64> = hits.iter().map(|h| h.doc_id).collect();
    assert_eq!(ids, vec![2, 5, 9]);
}

#[test]
fn client_can_recompute_every_operation_from_provenance() {
    // The contract's reconstruction guarantee, exercised per op:
    // the reported f64 contributions recombine to the f32 score.
    for op in [
        CompositeScoreOperation::WeightedSum,
        CompositeScoreOperation::WeightedMean,
        CompositeScoreOperation::Maximum,
        CompositeScoreOperation::Product,
        CompositeScoreOperation::GeometricMean,
        CompositeScoreOperation::HarmonicMean,
    ] {
        let (mut hits, dims) = two_signal_fixture();
        let weights: Vec<f64> = dims.iter().map(|d| d.weight.unwrap()).collect();
        let s = validate(&scorer(op, dims)).unwrap();
        s.apply(&mut hits, &[]).unwrap();
        for h in &hits {
            let active: Vec<(usize, &DimensionScore)> = h
                .dimensions
                .iter()
                .enumerate()
                .filter(|(_, d)| !d.skipped)
                .collect();
            let recomputed: f64 = match op {
                CompositeScoreOperation::WeightedSum | CompositeScoreOperation::WeightedMean => {
                    active.iter().map(|(_, d)| d.contribution).sum()
                }
                CompositeScoreOperation::Maximum => active
                    .iter()
                    .map(|(_, d)| d.contribution)
                    .fold(f64::NEG_INFINITY, f64::max),
                CompositeScoreOperation::Product | CompositeScoreOperation::GeometricMean => {
                    active.iter().map(|(_, d)| d.contribution).product()
                }
                CompositeScoreOperation::HarmonicMean => {
                    let w: f64 = active.iter().map(|(i, _)| weights[*i]).sum();
                    w / active.iter().map(|(_, d)| d.contribution).sum::<f64>()
                }
                CompositeScoreOperation::Unspecified => unreachable!(),
            };
            assert_eq!(h.score, recomputed as f32, "{op:?} doc {}", h.doc_id);
        }
    }
}
