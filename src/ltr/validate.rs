//! Validation of the wire scorer against the request's id namespace.

use super::LtrError;

use crate::pb::{
    score_signal, CompositeScoreOperation, CompositeScorer, MissingScorePolicy, ScoreDimension,
    ScoreNormalization,
};

use super::{op_name, refuse, Dim, Scorer, SignalKind, Source};

impl Scorer {
    /// Validate the wire scorer against the request's id namespace.
    /// `ids` holds every (search, boost, filter) id in the request.
    pub fn validate(
        scorer: &CompositeScorer,
        ids: &[(&str, SignalKind)],
    ) -> Result<Self, LtrError> {
        let operation = CompositeScoreOperation::try_from(scorer.operation).map_err(|_| {
            refuse(format!(
                "unknown composite score operation {}",
                scorer.operation
            ))
        })?;
        if operation == CompositeScoreOperation::Unspecified {
            return Err(refuse(
                "the scorer needs an explicit operation (weighted_sum, weighted_mean, \
                 maximum, product, geometric_mean, or harmonic_mean); defaulting one \
                 silently would misreport what ran",
            ));
        }
        if scorer.dimensions.is_empty() {
            return Err(refuse("the scorer has no dimensions"));
        }
        let mut dims = Vec::with_capacity(scorer.dimensions.len());
        let mut stored: Vec<crate::pb::ScoreStage> = Vec::new();
        let mut seen: Vec<&str> = Vec::new();
        for d in &scorer.dimensions {
            if d.id.is_empty() {
                return Err(refuse(
                    "every scorer dimension needs a non-empty id; ids are what make \
                     per-dimension provenance unambiguous",
                ));
            }
            if seen.contains(&d.id.as_str()) {
                return Err(refuse(format!("duplicate scorer dimension id {:?}", d.id)));
            }
            seen.push(&d.id);
            dims.push(validate_dim(d, operation, ids, &mut stored)?);
        }
        if dims.iter().all(|d| d.disabled) {
            return Err(refuse(
                "every scorer dimension is disabled (explicit zero weight); the scorer \
                 would order every document at 0, silently",
            ));
        }
        Ok(Scorer {
            operation,
            dims,
            stored,
        })
    }
}

/// One dimension: its weight under the operation, its source, its
/// normalization, and its missing policy.
fn validate_dim(
    d: &ScoreDimension,
    operation: CompositeScoreOperation,
    ids: &[(&str, SignalKind)],
    stored: &mut Vec<crate::pb::ScoreStage>,
) -> Result<Dim, LtrError> {
    let weight = d.weight.unwrap_or(1.0);
    if !weight.is_finite() {
        return Err(refuse(format!(
            "dimension {:?} has a non-finite weight",
            d.id
        )));
    }
    if weight < 0.0 && operation != CompositeScoreOperation::WeightedSum {
        return Err(refuse(format!(
            "dimension {:?} has a negative weight; only weighted_sum admits one \
             (a penalty term) — under {} it would corrupt the denominator or the \
             mean",
            d.id,
            op_name(operation)
        )));
    }
    let source = dim_source(d, ids, stored)?;
    let normalization = match ScoreNormalization::try_from(d.normalization) {
        Ok(ScoreNormalization::Unspecified) => ScoreNormalization::MinMax,
        Ok(n) => n,
        Err(_) => {
            return Err(refuse(format!(
                "dimension {:?} names unknown normalization {}",
                d.id, d.normalization
            )))
        }
    };
    let missing = match MissingScorePolicy::try_from(d.missing) {
        Ok(MissingScorePolicy::Unspecified) => MissingScorePolicy::Zero,
        Ok(m) => m,
        Err(_) => {
            return Err(refuse(format!(
                "dimension {:?} names unknown missing policy {}",
                d.id, d.missing
            )))
        }
    };
    Ok(Dim {
        id: d.id.clone(),
        weight,
        disabled: weight == 0.0,
        source,
        normalization,
        missing,
    })
}

/// Where a dimension reads from: the base score, a search or boost
/// query by id (never a filter), or a stored-value stage.
fn dim_source(
    d: &ScoreDimension,
    ids: &[(&str, SignalKind)],
    stored: &mut Vec<crate::pb::ScoreStage>,
) -> Result<Source, LtrError> {
    match d.source.as_ref().and_then(|s| s.source.as_ref()) {
        Some(score_signal::Source::Base(_)) => Ok(Source::Base),
        Some(score_signal::Source::QueryRelevanceId(id)) => {
            match ids.iter().find(|(known, _)| known == id) {
                Some((_, SignalKind::Search | SignalKind::Boost)) => Ok(Source::Query(id.clone())),
                Some((_, SignalKind::Filter)) => Err(refuse(format!(
                    "dimension {:?} sources filter {id:?}; a filter never \
                     contributes a relevance score",
                    d.id
                ))),
                None => Err(refuse(format!(
                    "dimension {:?} sources {id:?}, which names no search or \
                     boost query in this request",
                    d.id
                ))),
            }
        }
        Some(score_signal::Source::BoundedValue(stage)) => {
            // The stage admission rules apply here exactly as on the
            // lexical route; a malformed stage refuses by name before
            // anything runs.
            crate::node::parse_score_stages(std::slice::from_ref(stage))
                .map_err(LtrError::foreign)?;
            stored.push(stage.clone());
            Ok(Source::Stored(stored.len() - 1))
        }
        None => Err(refuse(format!("dimension {:?} has no source", d.id))),
    }
}
