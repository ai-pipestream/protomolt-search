//! Exact-key qualification of a request against the measured points.

use crate::pb::{DenseExecutionShape, DensePolicyPoint, DenseScorePrecision};

use super::{DenseExecutionPolicy, UNFILTERED_PPM};

/// The request side of the key. `candidate_depth` is the request's
/// `selection_k`; 0 means the request named none, and the cheapest
/// qualifying depth is chosen.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RequestKey {
    pub k: u32,
    pub candidate_depth: u32,
    pub filter_selectivity_ppm: u32,
    pub output_precision: DenseScorePrecision,
    pub shape: DenseExecutionShape,
}

/// The policy and the point that admitted approximation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Qualified {
    pub policy_id: String,
    pub policy_fingerprint: String,
    pub embedding_model: String,
    pub measured_queries: u32,
    pub target_recall_ppm: u32,
    pub point: DensePolicyPoint,
}

/// A qualifying point, or why no point qualifies (the request then runs the
/// exhaustive technique and the reason is disclosed).
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Qualification {
    Qualified(Box<Qualified>),
    Unqualified(String),
}

impl DenseExecutionPolicy {
    /// The cheapest point the key matches exactly whose measured recall meets
    /// the target: fewest candidates, then FAST before BALANCED.
    pub fn qualify(&self, key: RequestKey) -> Qualification {
        let id = self.policy_id();
        if key.k == 0 {
            return Qualification::Unqualified(format!(
                "policy {id:?} qualifies only an explicit k"
            ));
        }
        if !(1..=UNFILTERED_PPM).contains(&key.filter_selectivity_ppm) {
            return Qualification::Unqualified(format!(
                "live filter selectivity {} ppm admits no measured band",
                key.filter_selectivity_ppm
            ));
        }
        let target = self.target_recall_ppm();
        let mut matching: Vec<&DensePolicyPoint> = self
            .points()
            .iter()
            .filter(|p| matches(p, &key) && p.measured_recall_ppm >= target)
            .collect();
        matching.sort_by_key(|p| (p.candidates, std::cmp::Reverse(p.mode)));
        match matching.first() {
            Some(point) => Qualification::Qualified(Box::new(Qualified {
                policy_id: id.to_string(),
                policy_fingerprint: self.fingerprint().to_string(),
                embedding_model: self.document().embedding_model.clone(),
                measured_queries: self.measured_queries(),
                target_recall_ppm: target,
                point: **point,
            })),
            None => Qualification::Unqualified(format!(
                "policy {id:?} measured no point meeting {target} ppm recall for k={} depth {} \
                 at selectivity {} ppm, {} precision, {} shape",
                key.k,
                key.candidate_depth,
                key.filter_selectivity_ppm,
                key.output_precision.as_str_name(),
                key.shape.as_str_name()
            )),
        }
    }
}

fn matches(point: &DensePolicyPoint, key: &RequestKey) -> bool {
    point.k == key.k
        && (point.filter_selectivity_ppm_min..=point.filter_selectivity_ppm_max)
            .contains(&key.filter_selectivity_ppm)
        && point.output_precision == key.output_precision as i32
        && point.shape == key.shape as i32
        && (key.candidate_depth == 0 || point.candidates == key.candidate_depth)
}
