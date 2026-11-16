//! The phases every scored route shares once its candidate pool is
//! selected: boosts, the composite scorer, explain, paging or
//! collapse, projections, and the pool aggregation.

use tonic::Status;

use super::boosts::{apply_boosts, window_boosts};
use super::collapse::{fill_projected_groups, page_or_collapse, PageStep, Paged};
use super::response::{aggregate_pool, apply_scorer, done, fill_projected, finish_prof};
use super::run::Run;
use crate::pb::{QueryHit, QueryResponse};

/// How a pool was answered.
pub(super) enum Answer {
    /// The page cut from this run's pool; the route finishes it with
    /// what only it reports.
    Paged(Box<QueryResponse>),
    /// A single-leaf collapse found too few groups: the request must be
    /// re-run with a pool this deep.
    Deepen(u32),
}

/// What a route produced for the entry point.
pub(super) enum Routed {
    /// The finished response.
    Done(Box<QueryResponse>),
    /// The pool depth the entry point re-runs the request at.
    Deepen(u32),
}

/// Reorder the selected pool, cut the page (or the page of collapse
/// groups) and assemble the response. `route` is the `executed` echo so
/// far. The profile's total is taken before the pool aggregation folds.
pub(super) async fn answer_pool(
    run: &mut Run<'_>,
    mut hits: Vec<QueryHit>,
    route: &str,
    request_id: String,
) -> Result<Answer, Status> {
    let base = run.base;
    let (coordinator, req) = (base.coordinator, base.req);
    let scored = run.scorer.is_some();
    apply_boosts(coordinator, &run.boosts, &mut hits, scored, &mut run.prof).await?;
    let executed = apply_scorer(coordinator, &run.scorer, &mut hits, route, &mut run.prof).await?;
    let pool_ids: Vec<u64> = hits.iter().map(|h| h.doc_id).collect();
    if req.explain {
        let scorer_name: Option<String> = run.scorer.as_ref().map(|s| s.executed_suffix());
        crate::explain::finish(
            &mut hits,
            &window_boosts(&run.boosts),
            scorer_name.as_deref(),
        )?;
    }
    let step = PageStep {
        base: &base,
        cursor: run.cursor.as_ref(),
        pool_depth: run.depths.fetch_k,
        may_deepen: run.depths.collapse_may_deepen,
    };
    let (mut hits, mut groups, next, executed) =
        match page_or_collapse(&step, hits, executed, &mut run.prof).await? {
            Paged::Deepen(depth) => return Ok(Answer::Deepen(depth)),
            Paged::Done(hits, groups, next, executed) => (hits, groups, next, executed),
        };
    fill_projected(coordinator, &run.projections, &mut hits, &mut run.prof).await?;
    fill_projected_groups(coordinator, &run.projections, &mut groups, &mut run.prof).await?;
    let mut response = done(
        request_id,
        hits,
        &executed,
        next,
        finish_prof(run.prof.take(), base.t_total),
    );
    response.groups = groups;
    response.aggregate =
        aggregate_pool(coordinator, run.pool_aggregate.as_ref(), &pool_ids).await?;
    Ok(Answer::Paged(Box::new(response)))
}
