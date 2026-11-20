//! The adapter's entry point: the request-level rules, then the route
//! the selection's shape maps onto.

use tonic::Status;

use super::boolean_execute::execute_recursive_boolean;
use super::browse::{execute_filter_browse, execute_sorted_lexical};
use super::composite::{execute_composite, Legs};
use super::dense::execute_dense;
use super::lexical::execute_lexical;
use super::pool::Routed;
use super::prepare::prepare;
use super::refuse;
use super::run::{Base, Run};
use super::selection::{parse_selection, Shape};
use crate::coordinator::{CompiledAggregate, CoordinatorServiceImpl};
use crate::pb::{
    search_query, selection_query, AggregateRequest, QueryRequest, QueryResponse, SearchQuery,
    SelectionQuery,
};

/// Execute one public query with an explicit Stage-2 deadline (epoch microseconds; `0` = unbounded).
///
/// A single-leaf collapse that finds too few groups asks for a deeper
/// pool; the request is then run again at that depth from here, so each
/// run's futures are dropped before the next begins.
pub(crate) async fn execute(
    coordinator: &CoordinatorServiceImpl,
    mut req: QueryRequest,
    finish_deadline_us: u64,
) -> Result<QueryResponse, Status> {
    let mut deepened_to: Option<u32> = None;
    loop {
        let depth = match execute_once(coordinator, &req, finish_deadline_us).await? {
            Routed::Done(response) => return Ok(*response),
            Routed::Deepen(depth) => depth,
        };
        if deepened_to.is_some_and(|last| depth <= last) {
            return Err(Status::internal(format!(
                "collapse asked to deepen its pool to {depth}, which is not past the {} it \
                 was just run at",
                req.selection_k
            )));
        }
        deepened_to = Some(depth);
        req.selection_k = depth;
    }
}

async fn execute_once(
    coordinator: &CoordinatorServiceImpl,
    req: &QueryRequest,
    finish_deadline_us: u64,
) -> Result<Routed, Status> {
    let t_total = std::time::Instant::now();
    let selection = req
        .selection
        .as_ref()
        .ok_or_else(|| refuse("a query needs a selection tree"))?;
    let pool_aggregate = compile_pool_aggregate(req, selection)?;
    check_highlight(coordinator, req, selection)?;
    if let Some(selection_query::Node::Boolean(boolean)) = selection.node.as_ref() {
        return execute_recursive_boolean(coordinator, req, boolean)
            .await
            .map(|response| Routed::Done(Box::new(response)));
    }
    let plan = parse_selection(selection)?;
    let scoped = match plan.dense_query() {
        Some(query) => Some(coordinator.for_vector_field(&query.field)?),
        None => None,
    };
    let base = Base {
        coordinator: scoped.as_ref().unwrap_or(coordinator),
        req,
        plan: &plan,
        finish_deadline_us,
        t_total,
    };
    let run = prepare(base, pool_aggregate).await?;
    dispatch(run).await
}

/// Whether an aggregation names a filter of its own. An aggregation
/// riding a query folds over the query's match set or pool instead.
pub(super) fn names_own_filter(aggregate: &AggregateRequest) -> bool {
    !aggregate.filter.trim().is_empty() || !aggregate.geo_filters.is_empty()
}

/// A pool aggregation compiles before selection, so a bad spec refuses
/// before searching shards; the public handler may already have probed
/// read versions. The fold itself runs once the pool is fixed.
fn compile_pool_aggregate(
    req: &QueryRequest,
    selection: &SelectionQuery,
) -> Result<Option<CompiledAggregate>, Status> {
    let Some(aggregate) = req.aggregate.as_ref() else {
        return Ok(None);
    };
    if matches!(selection.node, Some(selection_query::Node::Boolean(_))) {
        return Err(refuse(
            "a boolean root aggregates over its exact match set on \
             BooleanQuery.aggregate; QueryRequest.aggregate serves the pooled \
             shapes (a leaf, a composite, a scorer or boost pool) and a browse",
        ));
    }
    if names_own_filter(aggregate) {
        return Err(refuse(
            "QueryRequest.aggregate folds over the selection's candidate pool; its \
             own filter and geo_filters must be empty",
        ));
    }
    crate::coordinator::compile_aggregations(aggregate).map(Some)
}

/// Snippets are cut around the lexical leg's occurrence spans, which
/// only the single lexical selection carries to the client; every
/// other shape — the boolean planner included — refuses rather than
/// returning hits without them. A configured reranker cuts its own.
fn check_highlight(
    coordinator: &CoordinatorServiceImpl,
    req: &QueryRequest,
    selection: &SelectionQuery,
) -> Result<(), Status> {
    let single_lexical = matches!(
        selection.node.as_ref(),
        Some(selection_query::Node::Search(SearchQuery {
            query: Some(search_query::Query::Lexical(_)),
            ..
        }))
    );
    if req.highlight.is_some() && !single_lexical && coordinator.rerank_spec().is_none() {
        return Err(refuse(
            "highlight is served for the single lexical selection only: no other shape \
             carries the occurrence spans snippets are cut around",
        ));
    }
    Ok(())
}

async fn dispatch(run: Run<'_>) -> Result<Routed, Status> {
    let base = run.base;
    match &base.plan.shape {
        Shape::Browse => execute_filter_browse(run)
            .await
            .map(|response| Routed::Done(Box::new(response))),
        Shape::Lexical { id, query } if !base.req.sort.is_empty() => {
            execute_sorted_lexical(run, id, query)
                .await
                .map(|response| Routed::Done(Box::new(response)))
        }
        Shape::Lexical { id, query } => execute_lexical(run, id, query).await,
        Shape::Dense { id, query } => execute_dense(run, id, query).await,
        Shape::Composite {
            dense_id,
            dense,
            lexical_id,
            lexical,
            strategy,
        } => {
            let legs = Legs {
                dense_id,
                dense,
                lexical_id,
                lexical,
                strategy,
            };
            execute_composite(run, legs).await
        }
    }
}
