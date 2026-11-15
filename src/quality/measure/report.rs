//! Small shared pieces: ladder argument checks, the FP32 query leaf,
//! exact medians, response checks, and the human-readable reports.

use crate::pb::{
    search_query, selection_query, DenseExecutionMode, DenseQuery, DenseScoreMode, QueryResponse,
    SearchQuery, SelectionQuery,
};

use super::super::UnmetTarget;
use super::MeasuredProfile;

pub(super) fn sorted_positive(name: &str, values: &[u32]) -> Result<Vec<u32>, String> {
    if values.is_empty() {
        return Err(format!("{name} must name at least one value"));
    }
    if values.contains(&0) {
        return Err(format!("{name} must be positive"));
    }
    let mut values = values.to_vec();
    values.sort_unstable();
    values.dedup();
    Ok(values)
}

pub(super) fn fp32_leaf(vector: &[f32]) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: "dense".into(),
            query: Some(search_query::Query::Dense(DenseQuery {
                vector: vector.to_vec(),
                score_mode: DenseScoreMode::Fp32Rerank as i32,
                // The profile describes an exhaustive selection reranked
                // in FP32; a provider that cannot prove one refuses here
                // rather than measuring a different thing.
                execution_mode: DenseExecutionMode::Exact as i32,
                ..Default::default()
            })),
        })),
    }
}

/// The lower median by rank: sorted samples, index `(n - 1) / 2`. Exact,
/// no interpolation between samples.
pub(super) fn p50(samples: &mut [f64]) -> f64 {
    samples.sort_by(f64::total_cmp);
    samples[(samples.len() - 1) / 2]
}

pub(super) fn check_exact(response: &QueryResponse, k: u32, index: usize) -> Result<(), String> {
    if response.hits.len() != k as usize {
        return Err(format!(
            "query {index} returned {} hits for k={k}",
            response.hits.len()
        ));
    }
    match &response.dense_execution {
        Some(outcome) if outcome.resolved_mode == DenseExecutionMode::Exact as i32 => Ok(()),
        Some(outcome) => Err(format!(
            "query {index} did not run an exhaustive selection: {}",
            outcome.planner_reason
        )),
        None => Err(format!(
            "query {index} carries no dense_execution provenance"
        )),
    }
}

pub fn describe_unmet(unmet: &[UnmetTarget]) -> String {
    unmet
        .iter()
        .map(|u| {
            format!(
                "k={} target={} (best worst-query recall {} at {} candidates)",
                u.k, u.target_recall_ppm, u.best_min_recall_ppm, u.best_candidates
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

/// The ladder as a fixed-width table: one line per rung with expansion
/// over `k`, mean and worst-query recall, and the p50 phases.
pub fn ladder_table(measured: &MeasuredProfile) -> String {
    let mut out = String::from(
        "k        candidates  expansion  mean_recall  min_recall  p50_total_ms  p50_selection_ms  p50_rerank_ms  p50_client_ms\n",
    );
    for row in &measured.ladder {
        let m = &row.measurement;
        out.push_str(&format!(
            "{:<8} {:>10}  {:>8.4}x  {:>11.6}  {:>10.6}  {:>12.3}  {:>16.3}  {:>13.3}  {:>13.3}\n",
            m.k,
            m.candidates,
            f64::from(m.candidates) / f64::from(m.k),
            f64::from(m.mean_recall_ppm) / 1e6,
            f64::from(m.min_recall_ppm) / 1e6,
            m.p50_total_ms,
            m.p50_selection_ms,
            m.p50_rerank_ms,
            row.p50_client_ms,
        ));
    }
    out
}
