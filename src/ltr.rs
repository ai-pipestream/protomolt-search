//! The generic named-dimension composite scorer (`docs/query-api.md`).
//!
//! This is the engine's learning-to-rank surface: named signals in,
//! explicit weights and normalizations, one deterministic combination,
//! per-dimension provenance out. It runs on the coordinator AFTER
//! selection (and after boost signals are computed), over the fixed
//! `selection_k` candidate pool, so it can never invalidate a
//! first-stage pruning certificate — the same soundness argument that
//! lets rescoring engines run arbitrary formulas post-selection, kept
//! here with a structured vocabulary instead of an opaque expression
//! tree, because the response must let a client recompute the final
//! score without reimplementing server arithmetic.
//!
//! All arithmetic is f64 in dimension list order (IEEE addition and
//! multiplication are not associative across reorderings — the same
//! rule as fused field-leg order). The final order is the f32 wire
//! score descending, doc id ascending: ties are broken on exactly the
//! score the client sees, never on hidden f64 residue.

use tonic::Status;

use crate::pb::{
    score_signal, CompositeScoreOperation, CompositeScorer, DimensionScore, MissingScorePolicy,
    QueryHit, ScoreNormalization,
};

fn refuse(msg: impl Into<String>) -> Status {
    Status::invalid_argument(msg.into())
}

/// What a request id names, for source validation: a dimension may
/// source a search leaf or a boost query, never a filter.
#[derive(Clone, Copy, PartialEq, Eq)]
pub enum SignalKind {
    /// A selection search leaf.
    Search,
    /// A candidate-scoped boost query.
    Boost,
    /// A filter — never a relevance signal.
    Filter,
}

/// Where one dimension's raw value comes from.
#[derive(Debug)]
enum Source {
    /// The first-stage base score (the selection strategy's own order).
    Base,
    /// A search or boost query's raw relevance, by id.
    Query(String),
    /// A stored-value score function: index into `Scorer::stored`,
    /// whose values arrive per candidate through the FetchValues seam
    /// (each stage evaluated at its identity score; a document without
    /// the value is a missing signal).
    Stored(usize),
}

#[derive(Debug)]
struct Dim {
    id: String,
    /// Resolved weight (absent = 1.0). An explicit 0 is a disable.
    weight: f64,
    disabled: bool,
    source: Source,
    normalization: ScoreNormalization,
    missing: MissingScorePolicy,
}

/// A validated scorer, ready to apply to a candidate pool.
#[derive(Debug)]
pub struct Scorer {
    operation: CompositeScoreOperation,
    dims: Vec<Dim>,
    /// The stored-value stages, in dimension order; the adapter
    /// fetches their per-candidate contributions before `apply`.
    stored: Vec<crate::pb::ScoreStage>,
}

/// The `executed` suffix for one operation, e.g. `weighted_mean`.
fn op_name(op: CompositeScoreOperation) -> &'static str {
    match op {
        CompositeScoreOperation::Unspecified => "unspecified",
        CompositeScoreOperation::WeightedSum => "weighted_sum",
        CompositeScoreOperation::WeightedMean => "weighted_mean",
        CompositeScoreOperation::Maximum => "maximum",
        CompositeScoreOperation::Product => "product",
        CompositeScoreOperation::GeometricMean => "geometric_mean",
        CompositeScoreOperation::HarmonicMean => "harmonic_mean",
    }
}

impl Scorer {
    /// The operation's name for the response's `executed` echo.
    pub fn executed_suffix(&self) -> String {
        format!("+scorer:{}", op_name(self.operation))
    }

    /// The stored-value stages the dimensions reference, in dimension
    /// order. The caller fetches their per-candidate contributions
    /// (the FetchValues seam) and hands them to `apply` positionally.
    pub fn stored_stages(&self) -> &[crate::pb::ScoreStage] {
        &self.stored
    }

    /// Validate the wire scorer against the request's id namespace.
    /// `ids` holds every (search, boost, filter) id in the request.
    pub fn validate(scorer: &CompositeScorer, ids: &[(&str, SignalKind)]) -> Result<Self, Status> {
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
            let source = match d.source.as_ref().and_then(|s| s.source.as_ref()) {
                Some(score_signal::Source::Base(_)) => Source::Base,
                Some(score_signal::Source::QueryRelevanceId(id)) => {
                    match ids.iter().find(|(known, _)| known == id) {
                        Some((_, SignalKind::Search | SignalKind::Boost)) => {
                            Source::Query(id.clone())
                        }
                        Some((_, SignalKind::Filter)) => {
                            return Err(refuse(format!(
                                "dimension {:?} sources filter {id:?}; a filter never \
                                 contributes a relevance score",
                                d.id
                            )))
                        }
                        None => {
                            return Err(refuse(format!(
                                "dimension {:?} sources {id:?}, which names no search or \
                                 boost query in this request",
                                d.id
                            )))
                        }
                    }
                }
                Some(score_signal::Source::BoundedValue(stage)) => {
                    // The stage admission rules apply here exactly as
                    // on the lexical route; a malformed stage refuses
                    // by name before anything runs.
                    crate::node::parse_score_stages(std::slice::from_ref(stage))?;
                    stored.push(stage.clone());
                    Source::Stored(stored.len() - 1)
                }
                None => {
                    return Err(refuse(format!("dimension {:?} has no source", d.id)));
                }
            };
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
            dims.push(Dim {
                id: d.id.clone(),
                weight,
                disabled: weight == 0.0,
                source,
                normalization,
                missing,
            });
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

    /// Score the candidate pool: set every hit's final score and
    /// per-dimension provenance, then order by (f32 score desc, doc id
    /// asc) — ties break on exactly the score the client sees.
    pub fn apply(
        &self,
        hits: &mut [QueryHit],
        stored: &[std::collections::HashMap<u64, f64>],
    ) -> Result<(), Status> {
        debug_assert_eq!(stored.len(), self.stored.len());
        if hits.is_empty() {
            return Ok(());
        }
        // Per dimension: the raw value per hit (None = missing), the
        // ERROR-policy check, and the pool normalization parameters
        // over PRESENT values.
        let mut raws: Vec<Vec<Option<f64>>> = Vec::with_capacity(self.dims.len());
        let mut norms: Vec<Norm> = Vec::with_capacity(self.dims.len());
        for dim in &self.dims {
            let vals: Vec<Option<f64>> = hits.iter().map(|h| raw_of(dim, h, stored)).collect();
            if dim.missing == MissingScorePolicy::Error {
                if let Some(i) = vals.iter().position(Option::is_none) {
                    return Err(Status::failed_precondition(format!(
                        "document {} carries no {:?} signal and dimension {:?} names the \
                         ERROR missing policy; the request asserted every candidate has \
                         it",
                        hits[i].doc_id,
                        source_name(&dim.source),
                        dim.id
                    )));
                }
            }
            norms.push(Norm::fit(dim.normalization, &vals));
            raws.push(vals);
        }

        for (i, hit) in hits.iter_mut().enumerate() {
            let mut reports: Vec<DimensionScore> = Vec::with_capacity(self.dims.len());
            // (normalized, weight, active) per dimension, pre-op.
            let mut entries: Vec<(f64, f64, bool)> = Vec::with_capacity(self.dims.len());
            for (d, dim) in self.dims.iter().enumerate() {
                let raw = raws[d][i];
                let missing = raw.is_none();
                let normalized = match raw {
                    Some(v) => norms[d].apply(v),
                    // ZERO contributes a NORMALIZED zero; SKIP's value
                    // is reported as 0 but excluded below.
                    None => 0.0,
                };
                let active = !dim.disabled && !(missing && dim.missing == MissingScorePolicy::Skip);
                entries.push((normalized, dim.weight, active));
                reports.push(DimensionScore {
                    id: dim.id.clone(),
                    raw,
                    normalized,
                    contribution: 0.0,
                    skipped: !active,
                });
            }
            let score = self.combine(&mut entries, &mut reports);
            if !score.is_finite() {
                return Err(Status::failed_precondition(format!(
                    "the scorer produced a non-finite score for document {}; every \
                     produced score must be finite",
                    hit.doc_id
                )));
            }
            hit.score = score as f32;
            hit.dimensions = reports;
        }
        hits.sort_by(|a, b| {
            b.score
                .total_cmp(&a.score)
                .then_with(|| a.doc_id.cmp(&b.doc_id))
        });
        Ok(())
    }

    /// Combine one hit's per-dimension entries, filling each report's
    /// contribution (and the positive-value skips of the geometric and
    /// harmonic operations). A hit with no active dimension scores 0.
    fn combine(&self, entries: &mut [(f64, f64, bool)], reports: &mut [DimensionScore]) -> f64 {
        use CompositeScoreOperation as Op;
        match self.operation {
            Op::Unspecified => unreachable!("validated"),
            Op::WeightedSum => {
                let mut sum = 0.0;
                for ((n, w, active), report) in entries.iter().zip(reports.iter_mut()) {
                    if *active {
                        report.contribution = w * n;
                        sum += report.contribution;
                    }
                }
                sum
            }
            Op::WeightedMean => {
                let total: f64 = entries.iter().filter(|e| e.2).map(|e| e.1).sum();
                if total == 0.0 {
                    return 0.0;
                }
                let mut sum = 0.0;
                for ((n, w, active), report) in entries.iter().zip(reports.iter_mut()) {
                    if *active {
                        report.contribution = w * n / total;
                        sum += report.contribution;
                    }
                }
                sum
            }
            Op::Maximum => {
                let mut best: Option<f64> = None;
                for ((n, w, active), report) in entries.iter().zip(reports.iter_mut()) {
                    if *active {
                        report.contribution = w * n;
                        best = Some(
                            best.map_or(report.contribution, |b: f64| b.max(report.contribution)),
                        );
                    }
                }
                best.unwrap_or(0.0)
            }
            Op::Product => {
                let mut product = 1.0;
                let mut any = false;
                for ((n, w, active), report) in entries.iter().zip(reports.iter_mut()) {
                    if *active {
                        report.contribution = w * n;
                        product *= report.contribution;
                        any = true;
                    }
                }
                if any {
                    product
                } else {
                    0.0
                }
            }
            Op::GeometricMean | Op::HarmonicMean => {
                // The blend's positive-value rule: only dimensions with
                // a POSITIVE normalized value participate, weights
                // renormalized over those; none positive scores 0.
                for ((n, _, active), report) in entries.iter_mut().zip(reports.iter_mut()) {
                    if *active && *n <= 0.0 {
                        *active = false;
                        report.skipped = true;
                    }
                }
                let total: f64 = entries.iter().filter(|e| e.2).map(|e| e.1).sum();
                if total == 0.0 {
                    return 0.0;
                }
                if self.operation == Op::GeometricMean {
                    let mut product = 1.0;
                    for ((n, w, active), report) in entries.iter().zip(reports.iter_mut()) {
                        if *active {
                            report.contribution = n.powf(w / total);
                            product *= report.contribution;
                        }
                    }
                    product
                } else {
                    let mut denom = 0.0;
                    for ((n, w, active), report) in entries.iter().zip(reports.iter_mut()) {
                        if *active {
                            report.contribution = w / n;
                            denom += report.contribution;
                        }
                    }
                    total / denom
                }
            }
        }
    }
}

fn source_name(source: &Source) -> String {
    match source {
        Source::Base => "base".to_string(),
        Source::Query(id) => id.clone(),
        Source::Stored(i) => format!("stored value {i}"),
    }
}

/// One dimension's raw value on one hit. The base score is the hit's
/// pre-scorer score; a query signal is present exactly when the hit's
/// `signals` provenance carries the id — the provenance surface and
/// the scorer never disagree about what a document matched.
fn raw_of(
    dim: &Dim,
    hit: &QueryHit,
    stored: &[std::collections::HashMap<u64, f64>],
) -> Option<f64> {
    match &dim.source {
        Source::Base => Some(f64::from(hit.score)),
        Source::Query(id) => hit
            .signals
            .iter()
            .find(|s| &s.id == id)
            .map(|s| f64::from(s.score)),
        Source::Stored(i) => stored[*i].get(&hit.doc_id).copied(),
    }
}

/// Fitted normalization parameters for one dimension's pool.
enum Norm {
    /// Raw identity.
    None,
    /// (min, max) over present values; degenerate maps to 1.0.
    MinMax { min: f64, max: f64 },
    /// (mean, population stddev); degenerate maps to 0.0.
    ZScore { mean: f64, std: f64 },
}

impl Norm {
    fn fit(kind: ScoreNormalization, vals: &[Option<f64>]) -> Norm {
        let present: Vec<f64> = vals.iter().filter_map(|v| *v).collect();
        match kind {
            ScoreNormalization::None => Norm::None,
            ScoreNormalization::MinMax | ScoreNormalization::Unspecified => {
                let min = present.iter().copied().fold(f64::INFINITY, f64::min);
                let max = present.iter().copied().fold(f64::NEG_INFINITY, f64::max);
                Norm::MinMax { min, max }
            }
            ScoreNormalization::ZScore => {
                let n = present.len() as f64;
                if present.is_empty() {
                    return Norm::ZScore {
                        mean: 0.0,
                        std: 0.0,
                    };
                }
                let mean = present.iter().sum::<f64>() / n;
                let var = present.iter().map(|v| (v - mean) * (v - mean)).sum::<f64>() / n;
                Norm::ZScore {
                    mean,
                    std: var.sqrt(),
                }
            }
        }
    }

    fn apply(&self, v: f64) -> f64 {
        match self {
            Norm::None => v,
            Norm::MinMax { min, max } => {
                if max > min {
                    (v - min) / (max - min)
                } else {
                    // Degenerate pool (every present value equal, or a
                    // single value): 1.0, the blend rule.
                    1.0
                }
            }
            Norm::ZScore { mean, std } => {
                if *std > 0.0 {
                    (v - mean) / std
                } else {
                    0.0
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::{QuerySignal, ScoreDimension, ScoreSignal};

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

    fn validate(s: &CompositeScorer) -> Result<Scorer, Status> {
        Scorer::validate(s, IDS)
    }

    fn msg(err: Status) -> String {
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
            op: crate::pb::ScoreOp::AddLinear as i32,
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
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
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
                    CompositeScoreOperation::WeightedSum
                    | CompositeScoreOperation::WeightedMean => {
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
}
