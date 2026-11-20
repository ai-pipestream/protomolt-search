//! The selection tree validated into the one shape a route can serve.

use tonic::Status;

use super::composite_shape::composite_shape;
use super::refuse;
use crate::pb::{
    search_query, selection_query, BlendScore, CompositeSearchStrategy, DecomposedScore,
    DenseQuery, FilterQuery, GeoFilter, LexicalQuery, RrfScore, SearchQuery, SelectionOperator,
    SelectionQuery,
};

/// The composite strategies increment 1 executes, each carrying its
/// fusion-mode mapping.
pub(super) enum Strategy<'a> {
    Rrf(&'a RrfScore),
    Blend(&'a BlendScore),
    Decomposed(&'a DecomposedScore),
    Cascade,
}

/// One validated selection: either a single scoring leaf or the
/// two-leaf composite, plus the filters gathered from the AND wrapper.
pub(super) struct Plan<'a> {
    pub(super) shape: Shape<'a>,
    pub(super) geo_filters: Vec<GeoFilter>,
    /// CEL predicates, ANDed by parenthesized textual conjunction —
    /// each was written as its own predicate, so each binds as one.
    pub(super) cel: Vec<&'a str>,
    pub(super) filter_ids: Vec<&'a str>,
}

pub(super) enum Shape<'a> {
    /// Filter-only browse: no scoring leaf, deterministic id order.
    Browse,
    Lexical {
        id: &'a str,
        query: &'a LexicalQuery,
    },
    Dense {
        id: &'a str,
        query: &'a DenseQuery,
    },
    Composite {
        dense_id: &'a str,
        dense: &'a DenseQuery,
        lexical_id: &'a str,
        lexical: &'a LexicalQuery,
        strategy: Strategy<'a>,
    },
}

impl<'a> Plan<'a> {
    /// The selection's dense leaf: the single dense leaf or the
    /// composite's dense leg.
    pub(super) fn dense_query(&self) -> Option<&'a DenseQuery> {
        match &self.shape {
            Shape::Dense { query, .. } | Shape::Composite { dense: query, .. } => Some(query),
            Shape::Browse | Shape::Lexical { .. } => None,
        }
    }

    fn collect_filter(&mut self, filter: &'a FilterQuery) -> Result<(), Status> {
        self.filter_ids.push(filter.id.as_str());
        match filter_predicate(filter)? {
            Predicate::Cel(text) => self.cel.push(text),
            Predicate::Geo(geo) => self.geo_filters.push(geo.clone()),
        }
        Ok(())
    }
}

/// A filter's predicate, known to be present and non-empty.
pub(super) enum Predicate<'a> {
    Cel(&'a str),
    Geo(&'a GeoFilter),
}

pub(super) fn filter_predicate(filter: &FilterQuery) -> Result<Predicate<'_>, Status> {
    match filter.predicate.as_ref() {
        Some(crate::pb::filter_query::Predicate::Cel(text)) if text.is_empty() => Err(refuse(
            format!("filter {:?} has an empty CEL predicate", filter.id),
        )),
        Some(crate::pb::filter_query::Predicate::Cel(text)) => Ok(Predicate::Cel(text)),
        Some(crate::pb::filter_query::Predicate::Geo(geo)) => Ok(Predicate::Geo(geo)),
        None => Err(refuse(format!("filter {:?} has no predicate", filter.id))),
    }
}

/// Validate the selection tree into the one shape increment 1 can
/// certify: an optional AND wrapper holding filters plus exactly one
/// search structure (a scoring leaf, or the two-leaf strategy
/// composite).
pub(super) fn parse_selection(selection: &SelectionQuery) -> Result<Plan<'_>, Status> {
    let mut plan = Plan {
        shape: Shape::Browse,
        geo_filters: Vec::new(),
        cel: Vec::new(),
        filter_ids: Vec::new(),
    };
    let node = selection
        .node
        .as_ref()
        .ok_or_else(|| refuse("empty selection node"))?;
    plan.shape = match node {
        selection_query::Node::Search(leaf) => leaf_shape(leaf)?,
        selection_query::Node::Filter(f) => {
            plan.collect_filter(f)?;
            Shape::Browse
        }
        selection_query::Node::Composite(composite) => match composite.operator() {
            SelectionOperator::And => and_shape(composite, &mut plan)?,
            SelectionOperator::Or | SelectionOperator::Unspecified => composite_shape(composite)?,
        },
        selection_query::Node::Boolean(_) => {
            return Err(refuse(
                "BooleanQuery must be executed through the recursive boolean planner",
            ))
        }
    };
    Ok(plan)
}

/// The AND wrapper: its filters land on the plan, and what remains must
/// be at most one search structure.
fn and_shape<'a>(
    composite: &'a CompositeSearchStrategy,
    plan: &mut Plan<'a>,
) -> Result<Shape<'a>, Status> {
    let mut search_nodes = Vec::new();
    for clause in &composite.clauses {
        match clause.node.as_ref() {
            Some(selection_query::Node::Filter(f)) => plan.collect_filter(f)?,
            Some(other) => search_nodes.push(other),
            None => return Err(refuse("empty selection node")),
        }
    }
    if composite.scoring.is_some() {
        return Err(refuse(
            "the AND wrapper carries no scoring strategy; the strategy belongs \
             on the composite that holds the scoring leaves",
        ));
    }
    match search_nodes.len() {
        // No scoring leaf at all: filter-only browse, in
        // deterministic id order.
        0 => Ok(Shape::Browse),
        1 => match search_nodes[0] {
            selection_query::Node::Search(leaf) => leaf_shape(leaf),
            selection_query::Node::Composite(inner) => composite_shape(inner),
            selection_query::Node::Filter(_) => {
                unreachable!("filters collected above")
            }
            selection_query::Node::Boolean(_) => Err(refuse(
                "BooleanQuery is a root selection shape; do not wrap it in legacy CompositeSearchStrategy",
            )),
        },
        n => Err(refuse(format!(
            "AND over {n} scoring structures has no certifiable engine path \
             yet; increment 1 supports one search structure plus filters \
             under AND, or the OR composite of one dense and one lexical \
             leaf"
        ))),
    }
}

fn leaf_shape(leaf: &SearchQuery) -> Result<Shape<'_>, Status> {
    match leaf.query.as_ref() {
        Some(search_query::Query::Lexical(q)) => Ok(Shape::Lexical {
            id: &leaf.id,
            query: q,
        }),
        Some(search_query::Query::Dense(q)) => Ok(Shape::Dense {
            id: &leaf.id,
            query: q,
        }),
        None => Err(no_query(leaf)),
    }
}

/// The refusal for a search leaf that names neither a lexical nor a
/// dense query.
pub(super) fn no_query(leaf: &SearchQuery) -> Status {
    refuse(format!("search query {:?} has no query", leaf.id))
}
