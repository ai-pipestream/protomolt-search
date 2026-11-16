//! Request preparation: every rule that can refuse a non-boolean
//! request is checked here, in a fixed order, before a shard is asked.

use tonic::Status;

use super::boosts::parse_boosts;
use super::ids::{check_ids, signal_ids};
use super::prepare_dense::{fp32_rerank, resolve_dense, resolve_quality};
use super::prepare_depth::{plan_depths, selection_depth, PoolUse};
use super::refuse;
use super::run::{Base, Run};
use super::selection::{Plan, Shape};
use crate::coordinator::{CompiledAggregate, CoordinatorServiceImpl, RequestFilters};
use crate::pb::{CompositeScorer, LexicalQuery, QueryRequest};
use crate::turbo_bridge::query_plan::BoundOnePassQuery;

/// Validate the request against its selection and fix everything the
/// route runs under. The order of the checks is the order a request
/// with several faults is refused in.
pub(super) async fn prepare<'a>(
    base: Base<'a>,
    pool_aggregate: Option<CompiledAggregate>,
) -> Result<Run<'a>, Status> {
    let Base {
        coordinator,
        req,
        plan,
        ..
    } = base;
    let mut prof: Option<crate::pb::QueryProfile> = req.profile.then(Default::default);
    let (filter, one_pass) = bind_filter(coordinator, plan)?;
    if let Some(p) = prof.as_mut() {
        // The shards the plan's filter rules out before fan-out; a plan
        // without a filter skips none.
        let filters = RequestFilters::compile(&plan.geo_filters, &filter)?;
        let (total, mut skipped) = coordinator.shard_prune_counts(&filters);
        if coordinator.scoped_vector_scan() {
            skipped = 0;
        }
        p.shards_total = total;
        p.shards_skipped = skipped;
    }
    let mut dense = resolve_dense(&base, &filter, one_pass.as_ref()).await?;
    dense.fp32_rerank = fp32_rerank(&plan.shape)?;
    check_ids(plan, &req.boosts)?;
    let scorer = validate_scorer(req, plan)?;
    let projections = compile_projections(req, &plan.shape)?;
    check_sort(req, &plan.shape, scorer.is_some())?;
    check_collapse(req, &plan.shape)?;
    resolve_quality(&base, &mut dense).await?;
    let selection_k = selection_depth(req, &dense)?;
    let boosts = parse_boosts(&req.boosts, &plan.shape, scorer.is_some())?;
    check_explain(req, &plan.shape)?;
    let uses = PoolUse {
        scorer: scorer.is_some(),
        aggregate: pool_aggregate.is_some(),
    };
    let (cursor, depths) = plan_depths(&base, &mut dense, selection_k, uses)?;
    Ok(Run {
        base,
        prof,
        filter,
        one_pass,
        dense,
        scorer,
        boosts,
        projections,
        pool_aggregate,
        cursor,
        depths,
    })
}

/// The selection's CEL filters as one predicate. When the coordinator
/// runs the one-pass query analyzer, the query text is analyzed and
/// bound here, and the structured filter it synthesized is ANDed on.
fn bind_filter(
    coordinator: &CoordinatorServiceImpl,
    plan: &Plan<'_>,
) -> Result<(String, Option<BoundOnePassQuery>), Status> {
    let mut filter = plan
        .cel
        .iter()
        .map(|c| format!("({c})"))
        .collect::<Vec<_>>()
        .join(" && ");
    let Some((q_analyzer, q_cols)) = coordinator.one_pass_query() else {
        return Ok((filter, None));
    };
    let raw_q = match &plan.shape {
        Shape::Lexical { query, .. } | Shape::Composite { lexical: query, .. } => {
            Some(query.text.as_str())
        }
        Shape::Dense { .. } => coordinator
            .rerank_spec()
            .and_then(|s| s.query_text_override.as_deref()),
        Shape::Browse => None,
    };
    let Some(q_text) = raw_q.filter(|s| !s.trim().is_empty()) else {
        return Ok((filter, None));
    };
    let q_plan = q_analyzer
        .analyze(q_text)
        .map_err(|e| Status::invalid_argument(format!("one-pass query analysis: {e}")))?;
    let bound = crate::turbo_bridge::query_plan::bind_one_pass_query(
        &q_plan,
        q_cols,
        (!filter.is_empty()).then_some(filter.as_str()),
    )?;
    if !bound.synthesized_cel.is_empty() {
        filter = if filter.is_empty() {
            bound.synthesized_cel.clone()
        } else {
            format!("({filter}) && ({})", bound.synthesized_cel)
        };
    }
    Ok((filter, Some(bound)))
}

/// The request's composite scorer, when it names one. An empty scorer
/// message is treated as absent (a set-but-empty one names neither
/// operation nor dimensions).
pub(super) fn named_scorer(req: &QueryRequest) -> Option<&CompositeScorer> {
    req.scorer
        .as_ref()
        .filter(|s| s.operation != 0 || !s.dimensions.is_empty())
}

fn validate_scorer(
    req: &QueryRequest,
    plan: &Plan<'_>,
) -> Result<Option<crate::ltr::Scorer>, Status> {
    let Some(scorer) = named_scorer(req) else {
        return Ok(None);
    };
    if matches!(plan.shape, Shape::Browse) {
        return Err(refuse(
            "the composite scorer requires a SCORED selection: a browse has no \
             relevance signals to combine, and its deterministic id (or column) \
             order is the whole contract of the route",
        ));
    }
    Ok(Some(crate::ltr::Scorer::validate(
        scorer,
        &signal_ids(plan, &req.boosts),
    )?))
}

/// Projections: the lexical route carries them natively (bitwise the
/// path that always served them); a sorted lexical leaf and every other
/// shape — browse included — fetches them post-selection by id through
/// the FetchValues seam. The selection is already fixed when that runs,
/// so no pruning certificate is involved.
fn compile_projections(
    req: &QueryRequest,
    shape: &Shape<'_>,
) -> Result<Vec<crate::pb::CompiledProjection>, Status> {
    if req.projections.is_empty() || (req.sort.is_empty() && matches!(shape, Shape::Lexical { .. }))
    {
        return Ok(Vec::new());
    }
    crate::coordinator::compile_projections(&req.projections)
}

/// A column sort is served on a browse or a single lexical leaf that
/// carries no relevance shape, and never together with a collapse.
fn check_sort(req: &QueryRequest, shape: &Shape<'_>, scorer_present: bool) -> Result<(), Status> {
    if req.sort.is_empty() {
        return Ok(());
    }
    for sort in &req.sort {
        crate::sortkeys::target_field(&sort.column, sort.map.as_ref())?;
    }
    match shape {
        Shape::Browse => {}
        Shape::Lexical { query, .. } => {
            if let Some(what) = sorted_leaf_relevance(req, query, scorer_present) {
                return Err(refuse(format!(
                    "sort over a lexical leaf orders the leaf's exact term membership \
                     (the BM25 positive-score set) by the columns and computes no \
                     relevance; {what} on the request would be a silent no-op"
                )));
            }
        }
        Shape::Dense { .. } | Shape::Composite { .. } => {
            return Err(refuse(
                "sort by column is served on a browse or a single lexical leaf: a \
                 dense or composite selection has no membership to order (every \
                 document is a candidate), so a column order over it would be a \
                 relevance cut in disguise; name the cut as a filter, or collapse",
            ));
        }
    }
    if req.collapse.is_some() {
        return Err(refuse(
            "collapse and sort do not combine: collapse picks each group's \
             representative by relevance, and a sorted query computes none",
        ));
    }
    Ok(())
}

/// The first relevance shape a sorted lexical request names. A column
/// order over a lexical leaf walks the leaf's exact term membership
/// without scoring; every relevance shape on the leaf or the request
/// would be silently dropped.
fn sorted_leaf_relevance(
    req: &QueryRequest,
    query: &LexicalQuery,
    scorer_present: bool,
) -> Option<&'static str> {
    if query.phrase.is_some() {
        Some("a phrase")
    } else if !query.prefixes.is_empty() {
        Some("term prefixes")
    } else if !query.score_stages.is_empty() {
        Some("score stages")
    } else if !req.boosts.is_empty() {
        Some("a boost")
    } else if scorer_present {
        Some("the composite scorer")
    } else if req.highlight.is_some() {
        Some("highlighting")
    } else if !query.synonyms.is_empty() {
        Some("synonym rules")
    } else {
        None
    }
}

fn check_collapse(req: &QueryRequest, shape: &Shape<'_>) -> Result<(), Status> {
    let Some(collapse) = &req.collapse else {
        return Ok(());
    };
    crate::sortkeys::target_field(&collapse.column, collapse.map.as_ref())?;
    if matches!(shape, Shape::Browse) {
        return Err(refuse(
            "collapse needs a SCORED selection: a browse has no order to pick a \
             group's representative by",
        ));
    }
    Ok(())
}

/// The tree explains a score; a shape that computes none has no tree
/// to give and says so.
fn check_explain(req: &QueryRequest, shape: &Shape<'_>) -> Result<(), Status> {
    if !req.explain {
        return Ok(());
    }
    if matches!(shape, Shape::Browse) {
        return Err(refuse(
            "explain needs a SCORED selection: a browse computes no relevance, and its                  id (or column) order is the whole contract of the route",
        ));
    }
    if !req.sort.is_empty() {
        return Err(refuse(
            "explain is served on relevance-ordered results: a column sort over a                  lexical leaf walks its exact membership and computes no score to explain",
        ));
    }
    Ok(())
}
