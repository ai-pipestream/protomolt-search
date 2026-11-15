//! The pieces every route's answer is assembled from: hits, the
//! response envelope, profile timings, and the post-selection value
//! fetches.

use tonic::Status;

use crate::coordinator::CoordinatorServiceImpl;
use crate::pb::{QueryHit, QueryResponse, QuerySignal};

pub(super) fn ms(t: std::time::Instant) -> f32 {
    t.elapsed().as_secs_f32() * 1e3
}

pub(super) fn finish_prof(
    mut prof: Option<crate::pb::QueryProfile>,
    t_total: std::time::Instant,
) -> Option<crate::pb::QueryProfile> {
    if let Some(p) = prof.as_mut() {
        p.total_ms = ms(t_total);
    }
    prof
}

/// A hit as selection produced it: unranked, with its provenance and
/// nothing a later phase fills in.
pub(super) fn scored_hit(
    doc_id: u64,
    score: f32,
    signals: Vec<QuerySignal>,
    matched: Vec<String>,
) -> QueryHit {
    QueryHit {
        identity: None,
        snippets: Vec::new(),
        projected: Vec::new(),
        doc_id,
        score,
        rank: 0,
        signals,
        matched,
        sort_key: 0.0,
        sort_values: Vec::new(),
        dimensions: Vec::new(),
        explain: None,
    }
}

/// A single-leaf hit's provenance: the leaf's own score under its id.
pub(super) fn leaf_signal(id: &str, score: f32) -> Vec<QuerySignal> {
    vec![QuerySignal {
        id: id.to_string(),
        score,
    }]
}

pub(super) fn done(
    request_id: String,
    hits: Vec<QueryHit>,
    executed: &str,
    next_cursor: String,
    profile: Option<crate::pb::QueryProfile>,
) -> QueryResponse {
    QueryResponse {
        field_details_redacted: false,
        execution_details_redacted: false,
        request_id,
        hits,
        executed: executed.to_string(),
        next_cursor,
        profile,
        dense_quality: None,
        dense_execution: None,
        served_topology_generation: 0,
        aggregate: None,
        groups: Vec::new(),
        synonym_expansions: Vec::new(),
    }
}

pub(super) fn matched(mut search_ids: Vec<String>, filter_ids: &[&str]) -> Vec<String> {
    search_ids.extend(filter_ids.iter().map(|s| s.to_string()));
    search_ids
}

/// Fold a pool aggregation over the candidate pool the page was drawn
/// from: the same explicit-id fan-out the boolean root uses, so
/// `matched` is the pool's size and the folds are the exact ones the
/// Aggregate route computes over a filter.
pub(super) async fn aggregate_pool(
    coordinator: &CoordinatorServiceImpl,
    compiled: Option<&crate::coordinator::CompiledAggregate>,
    pool_ids: &[u64],
) -> Result<Option<crate::pb::AggregateResponse>, Status> {
    let Some(compiled) = compiled else {
        return Ok(None);
    };
    let empty = crate::coordinator::RequestFilters::compile(&[], "")?;
    coordinator
        .fanout_aggregate(&empty, compiled, Some(pool_ids))
        .await
        .map(Some)
}

/// Apply the composite scorer (when present) to the candidate pool and
/// return the `executed` echo: the route name, suffixed with the
/// operation that reordered it. Stored-value dimensions fetch their
/// per-candidate contributions first (the FetchValues seam) — over the
/// whole pool, because normalization statistics are pool statistics.
pub(super) async fn apply_scorer(
    coordinator: &CoordinatorServiceImpl,
    scorer: &Option<crate::ltr::Scorer>,
    hits: &mut [QueryHit],
    route: &str,
    prof: &mut Option<crate::pb::QueryProfile>,
) -> Result<String, Status> {
    match scorer {
        None => Ok(route.to_string()),
        Some(s) => {
            let stored = if s.stored_stages().is_empty() {
                Vec::new()
            } else {
                let t0 = std::time::Instant::now();
                let ids: Vec<u64> = hits.iter().map(|h| h.doc_id).collect();
                let rows = coordinator
                    .fetch_values(&ids, &[], s.stored_stages())
                    .await?
                    .stage_rows;
                if let Some(p) = prof.as_mut() {
                    p.values_ms = ms(t0);
                }
                rows
            };
            let t0 = std::time::Instant::now();
            s.apply(hits, &stored)?;
            if let Some(p) = prof.as_mut() {
                p.scorer_ms = ms(t0);
            }
            Ok(format!("{route}{}", s.executed_suffix()))
        }
    }
}

/// Fill the paged hits' projected values through the FetchValues seam.
/// `compiled` is empty, and this a no-op, on the relevance-ordered
/// lexical route, which carries projections natively. A hit whose shard
/// holds no column tables projects all-absent — exact, its document
/// holds no such values.
pub(super) async fn fill_projected(
    coordinator: &CoordinatorServiceImpl,
    compiled: &[crate::pb::CompiledProjection],
    hits: &mut [QueryHit],
    prof: &mut Option<crate::pb::QueryProfile>,
) -> Result<(), Status> {
    if compiled.is_empty() {
        return Ok(());
    }
    let t0 = std::time::Instant::now();
    let ids: Vec<u64> = hits.iter().map(|h| h.doc_id).collect();
    let fetched = coordinator.fetch_values(&ids, compiled, &[]).await?;
    for hit in hits.iter_mut() {
        hit.projected = fetched
            .rows
            .get(&hit.doc_id)
            .cloned()
            .unwrap_or_else(|| vec![crate::pb::ProjectedValue::default(); compiled.len()]);
    }
    if let Some(p) = prof.as_mut() {
        p.projection_ms = ms(t0);
    }
    Ok(())
}
