//! A `BooleanQuery` compiled into the wire plan the shards evaluate.

use std::future::Future;
use std::pin::Pin;

use tonic::Status;

use super::refuse;
use super::selection::{filter_predicate, Predicate};
use crate::coordinator::{CoordinatorServiceImpl, RequestFilters};
use crate::pb::{
    search_query, selection_query, BooleanPlanGroup, BooleanPlanLeaf, BooleanPlanNode,
    BooleanQuery, DenseQuery, FilterQuery, LexicalQuery, SearchQuery, SelectionQuery,
};

/// One leaf of a compiled `BooleanQuery` as the coordinator names it:
/// the request id its signal and provenance carry, whether a MUST/SHOULD
/// chain reaches it, and the compiled filters of a filter leaf.
pub(super) struct BooleanLeafMeta {
    pub(super) id: String,
    pub(super) positive: bool,
    pub(super) filters: Option<RequestFilters>,
    /// The analyzed terms of a lexical leaf, the stats round's input.
    pub(super) terms: Option<Vec<String>>,
}

/// A `BooleanQuery` compiled once: the wire tree over indexed leaves
/// in traversal order (a group's MUST, SHOULD, then MUST_NOT clauses,
/// a nested group inline), which is the order signals sum in and
/// provenance lists.
pub(super) struct CompiledBoolean {
    pub(super) root: BooleanPlanGroup,
    pub(super) leaves: Vec<BooleanPlanLeaf>,
    pub(super) meta: Vec<BooleanLeafMeta>,
    /// The root group's MUST filter leaves: the AND spine placement
    /// prunes shards by.
    pub(super) root_must_filters: Vec<usize>,
}

impl CompiledBoolean {
    /// Compile the request's root group.
    pub(super) async fn compile(
        coordinator: &CoordinatorServiceImpl,
        boolean: &BooleanQuery,
    ) -> Result<Self, Status> {
        let mut compiled = CompiledBoolean {
            root: Default::default(),
            leaves: Vec::new(),
            meta: Vec::new(),
            root_must_filters: Vec::new(),
        };
        compiled.root =
            compile_boolean_group(coordinator, boolean, true, 1, &mut compiled, true).await?;
        Ok(compiled)
    }

    fn push_leaf(&mut self, leaf: crate::pb::boolean_plan_leaf::Leaf, meta: BooleanLeafMeta) {
        self.leaves.push(BooleanPlanLeaf { leaf: Some(leaf) });
        self.meta.push(meta);
    }
}

type Compiling<'a, T> = Pin<Box<dyn Future<Output = Result<T, Status>> + Send + 'a>>;

fn compile_boolean_filter(filter: &FilterQuery) -> Result<RequestFilters, Status> {
    match filter_predicate(filter)? {
        Predicate::Cel(cel) => RequestFilters::compile(&[], cel),
        Predicate::Geo(geo) => RequestFilters::compile(std::slice::from_ref(geo), ""),
    }
}

fn compile_boolean_node<'a>(
    coordinator: &'a CoordinatorServiceImpl,
    selection: &'a SelectionQuery,
    positive: bool,
    depth: usize,
    out: &'a mut CompiledBoolean,
) -> Compiling<'a, BooleanPlanNode> {
    Box::pin(async move {
        if depth > 64 {
            return Err(refuse(
                "boolean selection exceeds the 64-level recursion limit",
            ));
        }
        match selection.node.as_ref().ok_or_else(|| refuse("empty selection node"))? {
            selection_query::Node::Search(search) => {
                compile_search_leaf(coordinator, search, positive, out).await?;
            }
            selection_query::Node::Filter(filter) => compile_filter_leaf(filter, positive, out)?,
            selection_query::Node::Boolean(boolean) => {
                if boolean.aggregate.is_some() {
                    return Err(refuse(
                        "aggregate belongs on the root BooleanQuery, not a nested clause",
                    ));
                }
                let group =
                    compile_boolean_group(coordinator, boolean, positive, depth + 1, out, false)
                        .await?;
                return Ok(BooleanPlanNode {
                    node: Some(crate::pb::boolean_plan_node::Node::Group(group)),
                });
            }
            selection_query::Node::Composite(_) => {
                return Err(refuse(
                    "inside recursive boolean selection, express hybrid membership as dense and lexical MUST/SHOULD clauses; legacy CompositeSearchStrategy remains supported as the top-level compatibility shape",
                ))
            }
        };
        let index = (out.leaves.len() - 1) as u32;
        Ok(BooleanPlanNode {
            node: Some(crate::pb::boolean_plan_node::Node::Leaf(index)),
        })
    })
}

async fn compile_search_leaf(
    coordinator: &CoordinatorServiceImpl,
    search: &SearchQuery,
    positive: bool,
    out: &mut CompiledBoolean,
) -> Result<(), Status> {
    if search.id.is_empty() {
        return Err(refuse("a search clause needs a non-empty id"));
    }
    let mut meta = BooleanLeafMeta {
        id: search.id.clone(),
        positive,
        filters: None,
        terms: None,
    };
    let leaf = match search.query.as_ref() {
        Some(search_query::Query::Lexical(query)) => {
            let terms = lexical_leaf_terms(coordinator, &search.id, query).await?;
            let leaf = lexical_leaf(coordinator, query, terms.clone());
            meta.terms = Some(terms);
            leaf
        }
        Some(search_query::Query::Dense(query)) => dense_leaf(&search.id, query)?,
        None => {
            return Err(refuse(format!(
                "search clause {:?} has no query",
                search.id
            )))
        }
    };
    out.push_leaf(leaf, meta);
    Ok(())
}

/// Validate a lexical clause and analyze its text into the terms the
/// shards resolve membership from.
async fn lexical_leaf_terms(
    coordinator: &CoordinatorServiceImpl,
    id: &str,
    query: &LexicalQuery,
) -> Result<Vec<String>, Status> {
    if query.text.is_empty() {
        return Err(refuse(format!("lexical clause {id:?} has empty text")));
    }
    if query.phrase.is_some() || !query.prefixes.is_empty() {
        return Err(refuse(format!(
            "lexical clause {id:?}: a phrase or prefix constraint is not served inside \
             BooleanQuery yet (membership there is resolved from term \
             bitmaps, which carry no positions); use a single lexical leaf \
             selection for a phrase"
        )));
    }
    coordinator
        .analyze_terms(&query.text, query.analysis.as_ref())
        .await
}

/// The lexical wire leaf. Its global statistics are zero here; the
/// stats round fills them on every attempt.
fn lexical_leaf(
    coordinator: &CoordinatorServiceImpl,
    query: &LexicalQuery,
    terms: Vec<String>,
) -> crate::pb::boolean_plan_leaf::Leaf {
    let params = coordinator.bm25_params();
    crate::pb::boolean_plan_leaf::Leaf::Lexical(crate::pb::BooleanPlanLexical {
        analysis_fingerprint: crate::analyzer::analysis_fingerprint(query.analysis.as_ref()),
        terms,
        global_doc_count: 0,
        global_total_doc_length: 0,
        global_doc_frequencies: Vec::new(),
        k1: params.k1 as f32,
        b: params.b as f32,
        score_stages: query.score_stages.clone(),
    })
}

fn dense_leaf(id: &str, query: &DenseQuery) -> Result<crate::pb::boolean_plan_leaf::Leaf, Status> {
    if query.quality.is_some() {
        return Err(refuse(
            "DenseQualityPolicy is not used by exact bitmap Boolean planning",
        ));
    }
    let request =
        crate::dense_execution::parse_request(query).map_err(crate::coordinator::dense_status)?;
    if !matches!(
        request.mode,
        crate::pb::DenseExecutionMode::Exhaustive | crate::pb::DenseExecutionMode::Auto
    ) {
        return Err(refuse(
            "an approximate selection cannot establish recursive boolean membership; use \
             EXHAUSTIVE/AUTO or a top-level BALANCED/FAST selection",
        ));
    }
    if query.vector.is_empty() {
        return Err(refuse(format!("dense clause {id:?} has an empty vector")));
    }
    Ok(crate::pb::boolean_plan_leaf::Leaf::Dense(
        crate::pb::BooleanPlanDense {
            field: query.field.clone(),
            vector: query.vector.clone(),
            exact_fp32: request.precision == crate::pb::DensePrecisionRequest::Fp32,
        },
    ))
}

fn compile_filter_leaf(
    filter: &FilterQuery,
    positive: bool,
    out: &mut CompiledBoolean,
) -> Result<(), Status> {
    if filter.id.is_empty() {
        return Err(refuse("a filter clause needs a non-empty id"));
    }
    let filters = compile_boolean_filter(filter)?;
    let leaf = crate::pb::boolean_plan_leaf::Leaf::Filter(crate::pb::BooleanPlanFilter {
        filter: filters.tree.clone(),
        geo_filters: filters.geo.clone(),
    });
    let meta = BooleanLeafMeta {
        id: filter.id.clone(),
        positive,
        filters: Some(filters),
        terms: None,
    };
    out.push_leaf(leaf, meta);
    Ok(())
}

/// Compile one group: its clauses in MUST, SHOULD, MUST_NOT order, the
/// SHOULD minimum resolved (zero is one with only SHOULD clauses, else
/// zero). `root` marks the request's root group, whose MUST filter
/// leaves are the spine placement prunes shards by.
fn compile_boolean_group<'a>(
    coordinator: &'a CoordinatorServiceImpl,
    boolean: &'a BooleanQuery,
    positive: bool,
    depth: usize,
    out: &'a mut CompiledBoolean,
    root: bool,
) -> Compiling<'a, BooleanPlanGroup> {
    Box::pin(async move {
        if boolean.minimum_should_match as usize > boolean.should.len() {
            return Err(refuse(format!(
                "minimum_should_match {} exceeds {} SHOULD clauses",
                boolean.minimum_should_match,
                boolean.should.len()
            )));
        }
        if boolean.must.is_empty() && boolean.should.is_empty() && boolean.must_not.is_empty() {
            return Err(refuse("an empty BooleanQuery has no membership rule"));
        }
        let mut must = Vec::with_capacity(boolean.must.len());
        for clause in &boolean.must {
            let node = compile_boolean_node(coordinator, clause, positive, depth, out).await?;
            if root {
                if let (
                    Some(crate::pb::boolean_plan_node::Node::Leaf(index)),
                    Some(selection_query::Node::Filter(_)),
                ) = (node.node.as_ref(), clause.node.as_ref())
                {
                    out.root_must_filters.push(*index as usize);
                }
            }
            must.push(node);
        }
        let mut should = Vec::with_capacity(boolean.should.len());
        for clause in &boolean.should {
            should.push(compile_boolean_node(coordinator, clause, positive, depth, out).await?);
        }
        let mut must_not = Vec::with_capacity(boolean.must_not.len());
        for clause in &boolean.must_not {
            must_not.push(compile_boolean_node(coordinator, clause, false, depth, out).await?);
        }
        let minimum_should_match = if boolean.minimum_should_match == 0
            && boolean.must.is_empty()
            && !boolean.should.is_empty()
        {
            1
        } else {
            boolean.minimum_should_match
        };
        Ok(BooleanPlanGroup {
            must,
            should,
            must_not,
            minimum_should_match,
        })
    })
}
