//! The request-level rules of a recursive `BooleanQuery` root: ids,
//! scorer, boosts, paging depth and the match-set aggregation.

use tonic::Status;

use super::boosts::{
    boost_leaf, boost_query, names_reorder_knobs, BoostKind, BoostPlan, ParsedBoost,
};
use super::cursor::Cursor;
use super::entry::names_own_filter;
use super::ids::check_unique;
use super::prepare::named_scorer;
use super::refuse;
use crate::coordinator::{CompiledAggregate, CoordinatorServiceImpl};
use crate::ltr::SignalKind;
use crate::pb::{
    search_query, selection_query, BooleanQuery, BooleanShardAggregate, BoostQuery, QueryRequest,
    SelectionQuery,
};

/// A validated boolean request: what runs above the shards' exact
/// membership and score order.
pub(super) struct BooleanRequest<'a> {
    /// The page size: the request's k, or the coordinator's max_k.
    pub(super) k: u32,
    pub(super) cursor: Option<Cursor>,
    pub(super) scorer: Option<crate::ltr::Scorer>,
    pub(super) boosts: BoostPlan<'a>,
    /// The depth each shard answers.
    pub(super) depth: u32,
    pub(super) aggregate: Option<(BooleanShardAggregate, CompiledAggregate)>,
}

/// Check the request around the boolean root, in the order a request
/// with several faults is refused in.
pub(super) fn validate<'a>(
    coordinator: &CoordinatorServiceImpl,
    req: &'a QueryRequest,
    boolean: &BooleanQuery,
) -> Result<BooleanRequest<'a>, Status> {
    if req.collapse.is_some() {
        return Err(refuse(
            "collapse over recursive boolean relevance is not served; collapse a single \
             leaf or a composite",
        ));
    }
    if !req.sort.is_empty() {
        return Err(refuse(
            "column sort over recursive boolean relevance is not served; page its exact score order",
        ));
    }
    let k = if req.k == 0 {
        coordinator.max_k()
    } else {
        req.k
    };
    if k > coordinator.max_k() {
        return Err(refuse(format!(
            "k ({k}) exceeds coordinator max_k ({})",
            coordinator.max_k()
        )));
    }
    let cursor = Cursor::from_request(req)?;
    let selection = req
        .selection
        .as_ref()
        .expect("caller found the root boolean");
    let mut namespace = Vec::new();
    let scored = boolean_ids(selection, &mut namespace, true)?;
    for boost in &req.boosts {
        namespace.push((boost_query(boost)?.id.as_str(), SignalKind::Boost));
    }
    check_unique(
        namespace.iter().map(|(id, _)| *id),
        "every boolean clause and boost needs a non-empty id",
    )?;
    let scorer = match named_scorer(req) {
        Some(scorer) => {
            if !scored {
                return Err(refuse(
                    "the composite scorer requires at least one positive scoring clause",
                ));
            }
            Some(crate::ltr::Scorer::validate(scorer, &namespace)?)
        }
        None => None,
    };
    let boosts = parse_boolean_boosts(&req.boosts, scorer.is_some(), scored)?;
    let cursor_rank = cursor.as_ref().map_or(0, |c| c.rank);
    let pooled = scorer.is_some() || !matches!(boosts, BoostPlan::None);
    let depth = shard_depth(coordinator, req, k, cursor_rank, pooled)?;
    let aggregate = aggregate_plan(boolean)?;
    Ok(BooleanRequest {
        k,
        cursor,
        scorer,
        boosts,
        depth,
        aggregate,
    })
}

/// Collect every clause id with what it names, and report whether a
/// MUST/SHOULD chain reaches a scoring clause. A search clause under a
/// MUST_NOT only excludes, so it names a filter.
fn boolean_ids<'a>(
    selection: &'a SelectionQuery,
    ids: &mut Vec<(&'a str, SignalKind)>,
    positive: bool,
) -> Result<bool, Status> {
    match selection.node.as_ref().ok_or_else(|| refuse("empty selection node"))? {
        selection_query::Node::Search(search) => {
            ids.push((
                &search.id,
                if positive {
                    SignalKind::Search
                } else {
                    SignalKind::Filter
                },
            ));
            Ok(positive)
        }
        selection_query::Node::Filter(filter) => {
            ids.push((&filter.id, SignalKind::Filter));
            Ok(false)
        }
        selection_query::Node::Boolean(boolean) => {
            let mut scored = false;
            for child in boolean.must.iter().chain(&boolean.should) {
                scored |= boolean_ids(child, ids, positive)?;
            }
            for child in &boolean.must_not {
                boolean_ids(child, ids, false)?;
            }
            Ok(scored)
        }
        selection_query::Node::Composite(_) => Err(refuse(
            "legacy CompositeSearchStrategy is not nested inside BooleanQuery; express its dense/lexical membership as MUST/SHOULD clauses",
        )),
    }
}

fn parse_boolean_boosts(
    boosts: &[BoostQuery],
    scorer_present: bool,
    scored: bool,
) -> Result<BoostPlan<'_>, Status> {
    if boosts.is_empty() {
        return Ok(BoostPlan::None);
    }
    if !scored {
        return Err(refuse(
            "a boost requires at least one positive scoring clause",
        ));
    }
    if boosts.len() > 1 && !scorer_present {
        return Err(refuse(
            "multiple boolean boost signals require a composite scorer",
        ));
    }
    let mut parsed = Vec::with_capacity(boosts.len());
    for boost in boosts {
        if scorer_present && names_reorder_knobs(boost) {
            return Err(refuse(
                "boolean boosts are signal-only with a composite scorer; leave window/base_weight/boost_weight unset",
            ));
        }
        let query = boost_query(boost)?;
        let kind = match boost_leaf(query)? {
            search_query::Query::Lexical(lexical) => {
                if lexical.text.is_empty()
                    || !lexical.score_stages.is_empty()
                    || lexical.phrase.is_some()
                    || !lexical.prefixes.is_empty()
                {
                    return Err(refuse(
                        "a boolean lexical boost needs text and carries neither score_stages \
                         nor a phrase or prefix constraint",
                    ));
                }
                BoostKind::Lexical {
                    text: &lexical.text,
                    analysis: lexical.analysis.clone(),
                }
            }
            search_query::Query::Dense(dense) => {
                if dense.vector.is_empty() {
                    return Err(refuse("a boolean dense boost needs a vector"));
                }
                super::boosts::native_boost_scoring(dense)?;
                BoostKind::Dense {
                    field: &dense.field,
                    vector: &dense.vector,
                }
            }
        };
        parsed.push(ParsedBoost::new(&query.id, kind, boost));
    }
    Ok(BoostPlan::Adapter(parsed))
}

/// The depth each shard answers: the page's absolute end, or, with a
/// scorer or a boost to reorder, the whole pool they reorder
/// (`selection_k`, the coordinator's max_k when zero).
fn shard_depth(
    coordinator: &CoordinatorServiceImpl,
    req: &QueryRequest,
    k: u32,
    cursor_rank: u32,
    pooled: bool,
) -> Result<u32, Status> {
    if !pooled {
        if req.selection_k != 0 {
            return Err(refuse(
                "selection_k names the pool a scorer or a boost reorders; without one, exact \
                 Boolean selection pages its score order and selection_k must stay zero",
            ));
        }
        return cursor_rank
            .checked_add(k)
            .ok_or_else(|| refuse("the cursor's rank plus k overflows the candidate depth"));
    }
    let pool = if req.selection_k == 0 {
        coordinator.max_k()
    } else {
        req.selection_k
    };
    if pool < k {
        return Err(refuse(format!(
            "k ({k}) must not exceed selection_k ({pool}), the pool the scorer or boost \
             reorders"
        )));
    }
    if pool > coordinator.max_k() {
        return Err(refuse(format!(
            "selection_k ({pool}) exceeds coordinator max_k ({})",
            coordinator.max_k()
        )));
    }
    if cursor_rank.saturating_add(k) > pool {
        return Err(refuse(format!(
            "the cursor at rank {cursor_rank} plus k ({k}) walks past the selection pool \
             ({pool}); raise selection_k or restart from the first page"
        )));
    }
    Ok(pool)
}

/// The root's aggregation over the boolean match set, as the shards
/// fold it and as the coordinator merges it.
fn aggregate_plan(
    boolean: &BooleanQuery,
) -> Result<Option<(BooleanShardAggregate, CompiledAggregate)>, Status> {
    let Some(aggregate) = &boolean.aggregate else {
        return Ok(None);
    };
    if names_own_filter(aggregate) {
        return Err(refuse(
            "BooleanQuery.aggregate uses the boolean match set; its own filter and geo_filters must be empty",
        ));
    }
    let compiled = crate::coordinator::compile_aggregations(aggregate)?;
    let spec = BooleanShardAggregate {
        aggregations: compiled.aggregations.clone(),
        group_by: compiled.group_by.clone(),
        max_groups: compiled.max_groups,
        histograms: compiled.histograms.clone(),
        percentiles: compiled.percentiles.clone(),
    };
    Ok(Some((spec, compiled)))
}
