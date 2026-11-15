//! The depths a request runs at: the candidate set the page is drawn
//! from, and how deep the route is asked to answer.

use tonic::Status;

use super::cursor::Cursor;
use super::prepare_dense::DensePlan;
use super::refuse;
use super::run::{Base, Depths};
use super::selection::Shape;
use crate::pb::{DenseExecutionMode, QueryRequest};

/// selection_k: the candidate-set depth. 0 = k unless a measured
/// quality profile resolved it. The response is the best k of the
/// selection_k candidates under the final order.
pub(super) fn selection_depth(req: &QueryRequest, dense: &DensePlan) -> Result<u32, Status> {
    let selection_k = if let Some(resolution) = &dense.quality {
        resolution.selection_k
    } else if req.selection_k == 0 {
        dense.policy_depth.unwrap_or(req.k)
    } else {
        req.selection_k
    };
    if selection_k != 0 && req.k == 0 {
        return Err(refuse(
            "selection_k requires an explicit k: k = 0 selects the coordinator's max_k, \
             which cannot be compared against a fixed selection depth",
        ));
    }
    if selection_k < req.k {
        return Err(refuse(format!(
            "k ({}) must not exceed selection_k ({}): the final page is drawn FROM the \
             candidate set",
            req.k, selection_k
        )));
    }
    Ok(selection_k)
}

/// What besides the shape makes the candidate set a fixed pool.
pub(super) struct PoolUse {
    pub(super) scorer: bool,
    pub(super) aggregate: bool,
}

/// Fix the cursor and the depths, and report the fetch depth as the
/// depth an approximate traversal executes at (a policy point names the
/// same number, since the pool is fixed at it).
///
/// On a single-leaf shape without a scorer or boost no phase uses
/// extra selection depth; on a composite it is the leg/gate depth
/// (and the paging pool), with a scorer it is the pool the scorer
/// reorders, and with a boost it is the pool the boost rescores
/// (the honest form of the rescore window). An aggregation reads the
/// pool too: it is the set the fold runs over, so the depth is fixed
/// and paging moves inside it.
pub(super) fn plan_depths(
    base: &Base<'_>,
    dense: &mut DensePlan,
    selection_k: u32,
    uses: PoolUse,
) -> Result<(Option<Cursor>, Depths), Status> {
    let (req, shape) = (base.req, &base.plan.shape);
    let pooled = matches!(shape, Shape::Composite { .. })
        || uses.scorer
        || !req.boosts.is_empty()
        || dense.fp32_rerank
        || base.coordinator.rerank_spec().is_some()
        || req.collapse.is_some()
        || uses.aggregate;
    // A collapse over a single leaf deepens its pool itself, so the
    // leaf's own depth rules (paging by fetching deeper, a policy
    // depth) do not apply; every other collapse is a fixed pool.
    let collapse_may_deepen = req.collapse.is_some()
        && matches!(shape, Shape::Lexical { .. } | Shape::Dense { .. })
        && !uses.scorer
        && req.boosts.is_empty()
        && !dense.fp32_rerank
        && dense.policy_depth.is_none();
    if !pooled && dense.policy_depth.is_none() && selection_k != req.k {
        return Err(refuse(
            "selection_k differs from k but nothing on a single-leaf shape uses the \
             extra depth; naming it would be a silent no-op",
        ));
    }
    let cursor = Cursor::from_request(req)?;
    let pool = pooled.then_some(selection_k);
    let fetch_k = fetch_depth(base, cursor.as_ref(), pool, dense.policy_depth)?;
    if let Some(outcome) = dense.execution.as_mut() {
        if outcome.resolved_mode == DenseExecutionMode::Ann as i32 {
            outcome.candidate_depth = fetch_k;
        }
    }
    let depths = Depths {
        selection_k,
        fetch_k,
        collapse_may_deepen,
    };
    Ok((cursor, depths))
}

/// How deep the underlying route runs; `pool` is the fixed selection_k
/// pool of a pooled request. A single-leaf order is depth-independent
/// (exact top-k prefix property), so paging fetches past the boundary;
/// a POOLED order is NOT — a composite's strategy moves with depth (RRF
/// ranks, blend normalization, the cascade gate), and a scorer's
/// normalization statistics move with the pool — so paging stays inside
/// the fixed selection_k pool.
fn fetch_depth(
    base: &Base<'_>,
    cursor: Option<&Cursor>,
    pool: Option<u32>,
    policy_depth: Option<u32>,
) -> Result<u32, Status> {
    let req = base.req;
    match (&base.plan.shape, cursor, pool) {
        // Browse pages by an id floor (append-only ids are stable), so
        // every page fetches exactly k.
        (Shape::Browse, _, _) => Ok(req.k),
        (_, _, Some(selection_k)) => {
            // Under a collapse the cursor's rank counts groups, and the
            // group count is what decides exhaustion when the pool is
            // collapsed.
            if let (Some(c), None) = (cursor, &req.collapse) {
                if u64::from(c.rank) + u64::from(req.k) > u64::from(selection_k) {
                    return Err(Status::failed_precondition(format!(
                        "the selection_k = {selection_k} candidate set is exhausted at rank {}; \
                         deepening it would change the ranking under the cursor; \
                         re-run from the first page with a larger selection_k",
                        c.rank
                    )));
                }
            }
            Ok(selection_k)
        }
        (_, Some(c), None) => match policy_depth {
            Some(depth) => {
                if u64::from(c.rank) + u64::from(req.k) > u64::from(depth) {
                    return Err(Status::failed_precondition(format!(
                        "the policy candidate depth = {depth} is exhausted at rank {}; a deeper \
                         traversal is a different measured point; re-run from the first page \
                         with a selection_k the policy measured",
                        c.rank
                    )));
                }
                Ok(depth)
            }
            None => Ok(c.rank + req.k),
        },
        (_, None, None) => Ok(policy_depth.unwrap_or(req.k)),
    }
}
