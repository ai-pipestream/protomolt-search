//! Validation tests and the shared fixtures.

mod scoring;

use super::*;
use crate::pb::{score_signal, CompositeScorer, QuerySignal, ScoreDimension, ScoreSignal};

fn hit(doc_id: u64, score: f32, signals: &[(&str, f32)]) -> QueryHit {
    QueryHit {
        doc_id,
        score,
        signals: signals
            .iter()
            .map(|(id, s)| QuerySignal {
                id: (*id).to_string(),
                score: *s,
            })
            .collect(),
        ..Default::default()
    }
}

fn dim(id: &str, source: Option<score_signal::Source>) -> ScoreDimension {
    ScoreDimension {
        id: id.to_string(),
        weight: None,
        source: Some(ScoreSignal { source }),
        normalization: 0,
        missing: 0,
    }
}

fn base_dim(id: &str) -> ScoreDimension {
    dim(id, Some(score_signal::Source::Base(true)))
}

fn query_dim(id: &str, query_id: &str) -> ScoreDimension {
    dim(
        id,
        Some(score_signal::Source::QueryRelevanceId(query_id.to_string())),
    )
}

fn scorer(op: CompositeScoreOperation, dims: Vec<ScoreDimension>) -> CompositeScorer {
    CompositeScorer {
        operation: op as i32,
        dimensions: dims,
    }
}

const IDS: &[(&str, SignalKind)] = &[
    ("lex", SignalKind::Search),
    ("vec", SignalKind::Search),
    ("b", SignalKind::Boost),
    ("f", SignalKind::Filter),
];

fn validate(s: &CompositeScorer) -> Result<Scorer, LtrError> {
    Scorer::validate(s, IDS)
}

fn msg(err: LtrError) -> String {
    err.message().to_string()
}

// -- validation refusals, each named --

#[test]
fn refuses_unspecified_operation() {
    let s = scorer(CompositeScoreOperation::Unspecified, vec![base_dim("d")]);
    assert!(msg(validate(&s).unwrap_err()).contains("explicit operation"));
}

#[test]
fn refuses_empty_dimensions() {
    let s = scorer(CompositeScoreOperation::WeightedSum, vec![]);
    assert!(msg(validate(&s).unwrap_err()).contains("no dimensions"));
}

#[test]
fn refuses_empty_and_duplicate_ids() {
    let s = scorer(CompositeScoreOperation::WeightedSum, vec![base_dim("")]);
    assert!(msg(validate(&s).unwrap_err()).contains("non-empty id"));
    let s = scorer(
        CompositeScoreOperation::WeightedSum,
        vec![base_dim("d"), base_dim("d")],
    );
    assert!(msg(validate(&s).unwrap_err()).contains("duplicate scorer dimension id"));
}

#[test]
fn refuses_filter_and_unknown_sources() {
    let s = scorer(
        CompositeScoreOperation::WeightedSum,
        vec![query_dim("d", "f")],
    );
    assert!(msg(validate(&s).unwrap_err()).contains("never contributes"));
    let s = scorer(
        CompositeScoreOperation::WeightedSum,
        vec![query_dim("d", "nope")],
    );
    assert!(msg(validate(&s).unwrap_err()).contains("names no search or boost query"));
}

#[test]
fn refuses_missing_source_and_malformed_stage() {
    let s = scorer(CompositeScoreOperation::WeightedSum, vec![dim("d", None)]);
    assert!(msg(validate(&s).unwrap_err()).contains("no source"));
    // A stage under bounded_value obeys the stage admission rules:
    // the default stage names no column.
    let s = scorer(
        CompositeScoreOperation::WeightedSum,
        vec![dim(
            "d",
            Some(score_signal::Source::BoundedValue(Default::default())),
        )],
    );
    assert!(msg(validate(&s).unwrap_err()).contains("names the numeric column"));
}

#[test]
fn stored_dimensions_read_their_fetched_values() {
    use std::collections::HashMap;
    let stage = crate::pb::ScoreStage {
        operation: Some(crate::pb::score_stage::Operation::Op(
            crate::pb::ScoreOp::AddLinear as i32,
        )),
        column: "year".into(),
        weight: 1.0,
        ..Default::default()
    };
    let mut d = dim("recency", Some(score_signal::Source::BoundedValue(stage)));
    d.normalization = ScoreNormalization::None as i32;
    let s = validate(&scorer(CompositeScoreOperation::WeightedSum, vec![d])).unwrap();
    assert_eq!(s.stored_stages().len(), 1);
    let mut hits = vec![hit(0, 0.0, &[]), hit(1, 0.0, &[]), hit(2, 0.0, &[])];
    // Doc 2 has no value: the ZERO default applies.
    let stored: Vec<HashMap<u64, f64>> =
        vec![[(0u64, 1990.0), (1u64, 2020.0)].into_iter().collect()];
    s.apply(&mut hits, &stored).unwrap();
    assert_eq!(hits[0].doc_id, 1);
    assert_eq!(hits[0].score, 2020.0);
    assert_eq!(hits[1].score, 1990.0);
    assert_eq!(hits[2].doc_id, 2);
    assert_eq!(hits[2].score, 0.0);
    assert_eq!(hits[2].dimensions[0].raw, None);
}

#[test]
fn refuses_non_finite_weight() {
    let mut d = base_dim("d");
    d.weight = Some(f64::NAN);
    let s = scorer(CompositeScoreOperation::WeightedSum, vec![d]);
    assert!(msg(validate(&s).unwrap_err()).contains("non-finite weight"));
}

#[test]
fn negative_weight_only_under_weighted_sum() {
    let mut d = base_dim("d");
    d.weight = Some(-1.0);
    let s = scorer(CompositeScoreOperation::WeightedSum, vec![d.clone()]);
    assert!(validate(&s).is_ok());
    for op in [
        CompositeScoreOperation::WeightedMean,
        CompositeScoreOperation::Maximum,
        CompositeScoreOperation::Product,
        CompositeScoreOperation::GeometricMean,
        CompositeScoreOperation::HarmonicMean,
    ] {
        let s = scorer(op, vec![d.clone()]);
        assert!(
            msg(validate(&s).unwrap_err()).contains("negative weight"),
            "{op:?}"
        );
    }
}

#[test]
fn refuses_all_disabled() {
    let mut d = base_dim("d");
    d.weight = Some(0.0);
    let s = scorer(CompositeScoreOperation::WeightedSum, vec![d]);
    assert!(msg(validate(&s).unwrap_err()).contains("disabled"));
}

// -- normalization math --
