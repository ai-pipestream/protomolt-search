//! The generic named-dimension composite scorer.
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
    CompositeScoreOperation, DimensionScore, MissingScorePolicy, QueryHit, ScoreNormalization,
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

mod combine;
#[cfg(test)]
mod tests;
mod validate;
