//! The public `Query` adapter (`docs/query-api.md`), increment 1.
//!
//! This module is deliberately an ADAPTER and nothing more: it
//! validates the public shape, maps a supported request onto the
//! ordinary routes that already exist (`Search`, `Bm25Search`,
//! `HybridSearch` with its fusion modes and `BoostRescore`), executes
//! that route through the same handler every direct caller uses, and
//! translates the response into per-signal provenance. It never forks
//! scoring logic, and it refuses every shape outside the mapping table
//! BY NAME — compatibility never authorizes a heuristic substitute (no
//! `AND` reinterpreted as a union, no filter applied to one leg, no
//! silent strategy swap).
//!
//! The refusal messages are load-bearing surface: each one names the
//! unsupported construct and, where one exists, the supported way to
//! ask for the same thing.

use tonic::{Request, Status};

use crate::coordinator::CoordinatorServiceImpl;
use crate::pb::search_service_server::SearchService;
use crate::pb::{
    selection_query, selection_score_strategy, search_query, BlendScore, Bm25SearchRequest,
    DecomposedScore, DenseQuery, FilterQuery, FusionMode, GeoFilter,
    HybridLegOptions, HybridSearchRequest, LexicalQuery, QueryHit, QueryRequest, QueryResponse,
    QuerySignal, RrfScore, SearchQuery, SearchRequest, SelectionOperator, SelectionQuery,
    BoostRescore,
};

fn refuse(msg: impl Into<String>) -> Status {
    Status::invalid_argument(msg.into())
}

/// A search-after boundary: the last returned hit's absolute rank,
/// exact score bits, and doc id. The score-bits equality on resume is
/// the corpus-state check — search here is bitwise deterministic, so a
/// moved score means the corpus changed under the cursor.
struct Cursor {
    rank: u32,
    score_bits: u32,
    /// Sorted-browse boundary: the last hit's adjusted sort-key bits.
    /// `Some` exactly on tokens minted by a sorted browse; a cursor
    /// from one query shape refuses on another.
    key_bits: Option<u64>,
    doc_id: u64,
}

const CURSOR_PREFIX: &str = "tvq1";
const SORTED_CURSOR_PREFIX: &str = "tvqs1";

impl Cursor {
    fn encode(&self) -> String {
        match self.key_bits {
            Some(bits) => format!(
                "{SORTED_CURSOR_PREFIX}:{}:{bits:016x}:{}",
                self.rank, self.doc_id
            ),
            None => format!(
                "{CURSOR_PREFIX}:{}:{:08x}:{}",
                self.rank, self.score_bits, self.doc_id
            ),
        }
    }

    fn parse(token: &str) -> Result<Self, Status> {
        let bad = || refuse(format!("malformed cursor {token:?}"));
        let mut parts = token.split(':');
        let sorted = match parts.next() {
            Some(p) if p == CURSOR_PREFIX => false,
            Some(p) if p == SORTED_CURSOR_PREFIX => true,
            _ => return Err(bad()),
        };
        let rank: u32 = parts.next().and_then(|p| p.parse().ok()).ok_or_else(bad)?;
        let hex = parts.next().ok_or_else(bad)?;
        let doc_id: u64 = parts.next().and_then(|p| p.parse().ok()).ok_or_else(bad)?;
        if parts.next().is_some() || rank == 0 {
            return Err(bad());
        }
        let (score_bits, key_bits) = if sorted {
            (0, Some(u64::from_str_radix(hex, 16).map_err(|_| bad())?))
        } else {
            (u32::from_str_radix(hex, 16).map_err(|_| bad())?, None)
        };
        Ok(Cursor {
            rank,
            score_bits,
            key_bits,
            doc_id,
        })
    }
}

/// Cut the k-page after the cursor out of the full fetched list, with
/// absolute ranks, and mint the next cursor (empty when the page came
/// up short — nothing provably follows at the served depth).
fn page(
    mut hits: Vec<QueryHit>,
    k: u32,
    cursor: Option<&Cursor>,
) -> Result<(Vec<QueryHit>, String), Status> {
    for (i, hit) in hits.iter_mut().enumerate() {
        hit.rank = (i + 1) as u32;
    }
    let start = match cursor {
        None => 0,
        Some(c) => {
            if c.key_bits.is_some() {
                return Err(refuse(
                    "this cursor came from a sorted browse; the rest of the request must \
                     repeat the query that minted it",
                ));
            }
            let Some(i) = hits.iter().position(|h| h.doc_id == c.doc_id) else {
                return Err(Status::failed_precondition(format!(
                    "cursor boundary doc {} is no longer in the result set; the corpus changed under the cursor — restart from the first page",
                    c.doc_id
                )));
            };
            if hits[i].score.to_bits() != c.score_bits {
                return Err(Status::failed_precondition(format!(
                    "cursor boundary doc {}'s score moved; the corpus changed under the cursor — restart from the first page",
                    c.doc_id
                )));
            }
            i + 1
        }
    };
    let end = hits.len().min(start + k as usize);
    let page: Vec<QueryHit> = hits.drain(..end).skip(start).collect();
    let next = if page.len() == k as usize {
        page.last()
            .map(|h| {
                Cursor {
                    rank: h.rank,
                    score_bits: h.score.to_bits(),
                    key_bits: None,
                    doc_id: h.doc_id,
                }
                .encode()
            })
            .unwrap_or_default()
    } else {
        String::new()
    };
    Ok((page, next))
}

/// The composite strategies increment 1 executes, each carrying its
/// fusion-mode mapping.
enum Strategy<'a> {
    Rrf(&'a RrfScore),
    Blend(&'a BlendScore),
    Decomposed(&'a DecomposedScore),
    Cascade,
}

/// One validated selection: either a single scoring leaf or the
/// two-leaf composite, plus the filters gathered from the AND wrapper.
struct Plan<'a> {
    shape: Shape<'a>,
    geo_filters: Vec<GeoFilter>,
    /// CEL predicates, ANDed by parenthesized textual conjunction —
    /// each was written as its own predicate, so each binds as one.
    cel: Vec<&'a str>,
    filter_ids: Vec<&'a str>,
}

enum Shape<'a> {
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

/// Execute one public query by delegating to the ordinary routes.
pub async fn execute(
    coordinator: &CoordinatorServiceImpl,
    req: QueryRequest,
) -> Result<QueryResponse, Status> {
    if req.profile {
        return Err(refuse(
            "profile is not served yet: the response carries no profile surface, and \
             accepting the flag while ignoring it would misreport what ran",
        ));
    }
    if let Some(scorer) = &req.scorer {
        if scorer.operation != 0 || !scorer.dimensions.is_empty() {
            return Err(refuse(
                "the generic composite scorer is not served yet; the selection strategies \
                 (single, rrf, score_blend, decomposed, cascade) are the supported ways to \
                 combine signals",
            ));
        }
    }
    let selection = req
        .selection
        .as_ref()
        .ok_or_else(|| refuse("a query needs a selection tree"))?;
    let plan = parse_selection(selection)?;
    check_ids(&plan, &req.boosts)?;
    if !req.projections.is_empty() && !matches!(plan.shape, Shape::Lexical { .. }) {
        return Err(refuse(
            "projections are served on single-lexical-leaf selections only in this \
             increment (the Bm25Search delegate carries them); other shapes refuse \
             until their ordinary route does",
        ));
    }
    if let Some(sort) = &req.sort {
        if sort.column.is_empty() {
            return Err(refuse("sort names no column"));
        }
        if !matches!(plan.shape, Shape::Browse) {
            return Err(refuse(
                "sort by column is served on browse selections only: a column order over \
                 a SCORED selection must re-argue the pruning certificate (block-max \
                 bounds the score, not the column), and that path does not exist yet",
            ));
        }
    }

    // selection_k: the candidate-set depth. 0 = k. The response is the
    // best k of the selection_k candidates under the final order.
    let selection_k = if req.selection_k == 0 {
        req.k
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

    let boost = parse_boost(&req.boosts, &plan.shape)?;
    // On a single-leaf shape no phase uses extra selection depth; on a
    // composite it is the leg/gate depth (and the paging pool).
    if !matches!(plan.shape, Shape::Composite { .. }) && selection_k != req.k {
        return Err(refuse(
            "selection_k differs from k but nothing on a single-leaf shape uses the \
             extra depth; naming it would be a silent no-op",
        ));
    }
    let cursor = if req.cursor.is_empty() {
        None
    } else {
        if req.k == 0 {
            return Err(refuse("a cursor requires an explicit k"));
        }
        Some(Cursor::parse(&req.cursor)?)
    };
    // How deep the underlying route runs. A single-leaf order is
    // depth-independent (exact top-k prefix property), so paging
    // fetches past the boundary; a composite's order is NOT (RRF
    // ranks, blend normalization, the cascade gate all move with
    // depth), so paging stays inside the fixed selection_k pool.
    let fetch_k = match (&plan.shape, &cursor) {
        // Browse pages by an id floor (append-only ids are stable), so
        // every page fetches exactly k.
        (Shape::Browse, _) => req.k,
        (Shape::Composite { .. }, Some(c)) => {
            if u64::from(c.rank) + u64::from(req.k) > u64::from(selection_k) {
                return Err(Status::failed_precondition(format!(
                    "the selection_k = {selection_k} candidate set is exhausted at rank {}; \
                     deepening it would change the composite's ranking under the cursor; \
                     re-run from the first page with a larger selection_k",
                    c.rank
                )));
            }
            selection_k
        }
        (Shape::Composite { .. }, None) => selection_k,
        (_, Some(c)) => c.rank + req.k,
        (_, None) => req.k,
    };

    let filter = plan
        .cel
        .iter()
        .map(|c| format!("({c})"))
        .collect::<Vec<_>>()
        .join(" && ");

    match &plan.shape {
        Shape::Browse => {
            if plan.geo_filters.is_empty() && plan.cel.is_empty() {
                return Err(refuse(
                    "an empty browse (no filter at all) would page the whole corpus in id \
                     order; name at least one filter",
                ));
            }
            let compiled = crate::coordinator::RequestFilters::compile(&plan.geo_filters, &filter)?;
            let sort = req.sort.as_ref().map(|s| crate::pb::BrowseSort {
                column: s.column.clone(),
                descending: s.descending,
            });
            let after = match &cursor {
                None => None,
                Some(c) => {
                    // A cursor resumes the query that minted it: a
                    // sorted token carries the key boundary, a plain
                    // one only the id, and mixing the two shapes is a
                    // different query.
                    match (sort.is_some(), c.key_bits) {
                        (true, Some(key_bits)) => Some(crate::coordinator::BrowseAfter {
                            id: c.doc_id,
                            key_bits,
                        }),
                        (false, None) => Some(crate::coordinator::BrowseAfter {
                            id: c.doc_id,
                            key_bits: 0,
                        }),
                        _ => {
                            return Err(refuse(
                                "this cursor came from a browse with a different sort; the \
                                 rest of the request must repeat the query that minted it",
                            ))
                        }
                    }
                }
            };
            let base_rank = cursor.as_ref().map_or(0, |c| c.rank);
            let rows = coordinator
                .fanout_browse(req.k, after, sort.as_ref(), &compiled)
                .await?;
            let hits: Vec<QueryHit> = rows
                .ids
                .iter()
                .enumerate()
                .map(|(i, &doc_id)| QueryHit {
                    projected: Vec::new(),
                    doc_id,
                    // No relevance score exists on this route; the id
                    // (or column) order IS the order, and rank counts
                    // on across pages.
                    score: 0.0,
                    rank: base_rank + (i + 1) as u32,
                    signals: Vec::new(),
                    matched: matched(Vec::new(), &plan.filter_ids),
                    sort_key: if rows.sorted { rows.keys[i] } else { 0.0 },
                })
                .collect();
            let next = if req.k != 0 && hits.len() == req.k as usize {
                hits.last()
                    .map(|h| {
                        Cursor {
                            rank: h.rank,
                            score_bits: 0,
                            key_bits: rows
                                .sorted
                                .then(|| *rows.key_bits.last().expect("full page")),
                            doc_id: h.doc_id,
                        }
                        .encode()
                    })
                    .unwrap_or_default()
            } else {
                String::new()
            };
            Ok(done(req.request_id, hits, "browse", next))
        }
        Shape::Lexical { id, query } => {
            let response = coordinator
                .bm25_search(Request::new(Bm25SearchRequest {
                    text: query.text.clone(),
                    k: fetch_k,
                    analysis: query.analysis.clone(),
                    score_stages: query.score_stages.clone(),
                    geo_filters: plan.geo_filters.clone(),
                    filter,
                    projections: req.projections.clone(),
                    ..Default::default()
                }))
                .await?
                .into_inner();
            let hits = response
                .hits
                .iter()
                .map(|h| QueryHit {
                    projected: h.projected.clone(),
                    doc_id: h.doc_id,
                    score: h.score,
                    rank: 0,
                    signals: vec![QuerySignal {
                        id: id.to_string(),
                        score: h.score,
                    }],
                    matched: matched(vec![id.to_string()], &plan.filter_ids),
                    sort_key: 0.0,
                })
                .collect();
            let (hits, next) = page(hits, req.k, cursor.as_ref())?;
            Ok(done(req.request_id, hits, "bm25_search", next))
        }
        Shape::Dense { id, query } => {
            let response = coordinator
                .search(Request::new(SearchRequest {
                    request_id: req.request_id.clone(),
                    k: fetch_k,
                    vector: query.vector.clone(),
                    geo_filters: plan.geo_filters.clone(),
                    filter,
                    ..Default::default()
                }))
                .await?
                .into_inner();
            let hits = response
                .hits
                .iter()
                .map(|h| QueryHit {
                    projected: Vec::new(),
                    doc_id: h.vector_id,
                    score: h.score,
                    rank: 0,
                    signals: vec![QuerySignal {
                        id: id.to_string(),
                        score: h.score,
                    }],
                    matched: matched(vec![id.to_string()], &plan.filter_ids),
                    sort_key: 0.0,
                })
                .collect();
            let (hits, next) = page(hits, req.k, cursor.as_ref())?;
            Ok(done(response.request_id, hits, "search", next))
        }
        Shape::Composite {
            dense_id,
            dense,
            lexical_id,
            lexical,
            strategy,
        } => {
            let (mode, legs) = leg_options(strategy, selection_k);
            let boost_id = boost.as_ref().map(|(id, _)| id.to_string());
            let response = coordinator
                .hybrid_search(Request::new(HybridSearchRequest {
                    request_id: req.request_id.clone(),
                    text: lexical.text.clone(),
                    vector: dense.vector.clone(),
                    // The selection phase runs selection_k deep; the
                    // final page trims to k below. For the exact modes
                    // the trimmed prefix IS the top-k (prefix property
                    // of a total order).
                    k: selection_k,
                    analysis: lexical.analysis.clone(),
                    legs: Some(legs),
                    boost: boost.map(|(_, b)| b),
                    geo_filters: plan.geo_filters.clone(),
                    filter,
                    ..Default::default()
                }))
                .await?
                .into_inner();
            let hits = if matches!(strategy, Strategy::Cascade) {
                response
                    .cascade_hits
                    .iter()
                    .map(|h| {
                        let mut signals = vec![
                            QuerySignal {
                                id: dense_id.to_string(),
                                score: h.vector_score,
                            },
                            QuerySignal {
                                id: lexical_id.to_string(),
                                score: h.bm25_score,
                            },
                        ];
                        let mut m = vec![dense_id.to_string(), lexical_id.to_string()];
                        if let Some(bid) = &boost_id {
                            if h.boost_score != 0.0 {
                                signals.push(QuerySignal {
                                    id: bid.clone(),
                                    score: h.boost_score,
                                });
                                m.push(bid.clone());
                            }
                        }
                        QueryHit {
                            projected: Vec::new(),
                            doc_id: h.doc_id,
                            // Cascade's final order is the rerank leg's:
                            // score is the rerank leg's raw relevance
                            // (docs/query-api.md).
                            score: h.bm25_score,
                            rank: 0,
                            signals,
                            matched: matched(m, &plan.filter_ids),
                            sort_key: 0.0,
                        }
                    })
                    .collect()
            } else {
                response
                    .hits
                    .iter()
                    .map(|h| {
                        let mut signals = Vec::new();
                        let mut m = Vec::new();
                        // DECOMPOSED is rank-free (an exact full-corpus
                        // weighted sum): leg ranks are never set, but
                        // every hit's vector score is real, so the
                        // dense signal is always present under it.
                        if h.vector_rank.is_some() || mode == FusionMode::Decomposed {
                            signals.push(QuerySignal {
                                id: dense_id.to_string(),
                                score: h.vector_score,
                            });
                            m.push(dense_id.to_string());
                        }
                        if h.bm25_rank.is_some() {
                            signals.push(QuerySignal {
                                id: lexical_id.to_string(),
                                score: h.bm25_score,
                            });
                            m.push(lexical_id.to_string());
                        }
                        if let Some(bid) = &boost_id {
                            if h.boost_score != 0.0 {
                                signals.push(QuerySignal {
                                    id: bid.clone(),
                                    score: h.boost_score,
                                });
                                m.push(bid.clone());
                            }
                        }
                        QueryHit {
                            projected: Vec::new(),
                            doc_id: h.doc_id,
                            score: h.fused_score,
                            rank: 0,
                            signals,
                            matched: matched(m, &plan.filter_ids),
                            sort_key: 0.0,
                        }
                    })
                    .collect()
            };
            let executed = match mode {
                FusionMode::GlobalRank => "hybrid_search:global_rank",
                FusionMode::ScoreBlend => "hybrid_search:score_blend",
                FusionMode::Decomposed => "hybrid_search:decomposed",
                _ => "hybrid_search:cascade",
            };
            let (hits, next) = page(hits, req.k, cursor.as_ref())?;
            Ok(done(response.request_id, hits, executed, next))
        }
    }
}

fn done(
    request_id: String,
    hits: Vec<QueryHit>,
    executed: &str,
    next_cursor: String,
) -> QueryResponse {
    QueryResponse {
        request_id,
        hits,
        executed: executed.to_string(),
        next_cursor,
    }
}

fn matched(mut search_ids: Vec<String>, filter_ids: &[&str]) -> Vec<String> {
    search_ids.extend(filter_ids.iter().map(|s| s.to_string()));
    search_ids
}

/// Validate the selection tree into the one shape increment 1 can
/// certify: an optional AND wrapper holding filters plus exactly one
/// search structure (a scoring leaf, or the two-leaf strategy
/// composite).
fn parse_selection(selection: &SelectionQuery) -> Result<Plan<'_>, Status> {
    let mut geo_filters = Vec::new();
    let mut cel = Vec::new();
    let mut filter_ids = Vec::new();
    let node = selection
        .node
        .as_ref()
        .ok_or_else(|| refuse("empty selection node"))?;

    let structure = match node {
        selection_query::Node::Search(leaf) => leaf_shape(leaf)?,
        selection_query::Node::Filter(f) => {
            collect_filter(f, &mut geo_filters, &mut cel, &mut filter_ids)?;
            Shape::Browse
        }
        selection_query::Node::Composite(composite) => match composite.operator() {
            SelectionOperator::And => {
                let mut search_nodes = Vec::new();
                for clause in &composite.clauses {
                    match clause.node.as_ref() {
                        Some(selection_query::Node::Filter(f)) => {
                            collect_filter(f, &mut geo_filters, &mut cel, &mut filter_ids)?;
                        }
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
                    0 => Shape::Browse,
                    1 => match search_nodes[0] {
                        selection_query::Node::Search(leaf) => leaf_shape(leaf)?,
                        selection_query::Node::Composite(inner) => composite_shape(inner)?,
                        selection_query::Node::Filter(_) => {
                            unreachable!("filters collected above")
                        }
                    },
                    n => {
                        return Err(refuse(format!(
                            "AND over {n} scoring structures has no certifiable engine path \
                             yet; increment 1 supports one search structure plus filters \
                             under AND, or the OR composite of one dense and one lexical \
                             leaf"
                        )))
                    }
                }
            }
            SelectionOperator::Or | SelectionOperator::Unspecified => composite_shape(composite)?,
        },
    };
    Ok(Plan {
        shape: structure,
        geo_filters,
        cel,
        filter_ids,
    })
}

fn collect_filter<'a>(
    f: &'a FilterQuery,
    geo: &mut Vec<GeoFilter>,
    cel: &mut Vec<&'a str>,
    ids: &mut Vec<&'a str>,
) -> Result<(), Status> {
    ids.push(f.id.as_str());
    match f.predicate.as_ref() {
        Some(crate::pb::filter_query::Predicate::Cel(text)) => {
            if text.is_empty() {
                return Err(refuse(format!("filter {:?} has an empty CEL predicate", f.id)));
            }
            cel.push(text.as_str());
        }
        Some(crate::pb::filter_query::Predicate::Geo(g)) => geo.push(g.clone()),
        None => return Err(refuse(format!("filter {:?} has no predicate", f.id))),
    }
    Ok(())
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
        None => Err(refuse(format!("search query {:?} has no query", leaf.id))),
    }
}

/// The two-leaf strategy composite: exactly one dense and one lexical
/// scoring leaf, an operator the strategy can certify, and a strategy
/// that maps onto a fusion mode.
fn composite_shape(
    composite: &crate::pb::CompositeSearchStrategy,
) -> Result<Shape<'_>, Status> {
    let mut dense: Option<(&str, &DenseQuery)> = None;
    let mut lexical: Option<(&str, &LexicalQuery)> = None;
    for clause in &composite.clauses {
        match clause.node.as_ref() {
            Some(selection_query::Node::Search(leaf)) => match leaf.query.as_ref() {
                Some(search_query::Query::Dense(q)) => {
                    if dense.replace((&leaf.id, q)).is_some() {
                        return Err(refuse(
                            "two dense leaves in one composite have no engine path yet; \
                             increment 1 fuses exactly one dense and one lexical leaf",
                        ));
                    }
                }
                Some(search_query::Query::Lexical(q)) => {
                    if !q.score_stages.is_empty() {
                        return Err(refuse(
                            "score_stages on a composite's lexical leaf are not served: \
                             stages do not ride the hybrid legs today. Use a \
                             single-lexical-leaf selection for a staged query",
                        ));
                    }
                    if lexical.replace((&leaf.id, q)).is_some() {
                        return Err(refuse(
                            "two lexical leaves in one composite have no engine path yet; \
                             increment 1 fuses exactly one dense and one lexical leaf",
                        ));
                    }
                }
                None => return Err(refuse(format!("search query {:?} has no query", leaf.id))),
            },
            Some(selection_query::Node::Filter(f)) => {
                return Err(refuse(format!(
                    "filter {:?} sits inside the scoring composite; a filter belongs to an \
                     AND wrapper AROUND the composite (under OR it would admit documents \
                     with no relevance signal, which needs the match-all order that does \
                     not exist yet)",
                    f.id
                )));
            }
            Some(selection_query::Node::Composite(_)) => {
                return Err(refuse(
                    "nested scoring composites are not served yet; increment 1 supports \
                     one level: filters AND (one leaf | OR(dense, lexical))",
                ));
            }
            None => return Err(refuse("empty selection node")),
        }
    }
    let Some((dense_id, dense)) = dense else {
        return Err(refuse(
            "the composite needs a dense leaf; a lexical-only query is the \
             single-lexical-leaf selection",
        ));
    };
    let Some((lexical_id, lexical)) = lexical else {
        return Err(refuse(
            "the composite needs a lexical leaf; a dense-only query is the \
             single-dense-leaf selection",
        ));
    };
    let strategy = match composite.scoring.as_ref().and_then(|s| s.strategy.as_ref()) {
        Some(selection_score_strategy::Strategy::Rrf(s)) => Strategy::Rrf(s),
        Some(selection_score_strategy::Strategy::ScoreBlend(s)) => Strategy::Blend(s),
        Some(selection_score_strategy::Strategy::Decomposed(s)) => Strategy::Decomposed(s),
        Some(selection_score_strategy::Strategy::Cascade(s)) => {
            if s.gate_id != dense_id {
                return Err(refuse(format!(
                    "cascade gate {:?} is not the dense leaf {dense_id:?}: the engine's \
                     cascade is vector-gate, BM25-rerank; a lexical gate has no engine \
                     path yet",
                    s.gate_id
                )));
            }
            Strategy::Cascade
        }
        Some(selection_score_strategy::Strategy::Single(_)) => {
            return Err(refuse(
                "SingleScore names exactly one scoring leaf; this composite holds two. \
                 Pick rrf, score_blend, decomposed, or cascade",
            ));
        }
        None => {
            return Err(refuse(
                "a composite with two scoring leaves needs an explicit strategy (rrf, \
                 score_blend, decomposed, or cascade); defaulting one silently would \
                 misreport what ran",
            ));
        }
    };
    // The operator and the strategy must agree on membership.
    match (&strategy, composite.operator()) {
        (Strategy::Cascade, SelectionOperator::Unspecified) => {}
        (Strategy::Cascade, _) => {
            return Err(refuse(
                "cascade defines membership itself (the gate's admissions), so the \
                 boolean operator must be left unspecified; naming AND or OR would \
                 contradict the strategy",
            ));
        }
        (_, SelectionOperator::Or) => {}
        (_, SelectionOperator::And) => {
            return Err(refuse(
                "AND over the two scoring leaves has no certifiable engine path yet (the \
                 fusion modes are union-shaped); use OR, or cascade for gate semantics",
            ));
        }
        (_, SelectionOperator::Unspecified) => {
            return Err(refuse(
                "the composite needs an explicit operator: OR for the fusion strategies \
                 (their membership is the union of the retained legs)",
            ));
        }
    }
    Ok(Shape::Composite {
        dense_id,
        dense,
        lexical_id,
        lexical,
        strategy,
    })
}

/// Map a strategy onto the hybrid route's leg options.
fn leg_options(strategy: &Strategy<'_>, selection_k: u32) -> (FusionMode, HybridLegOptions) {
    let mut legs = HybridLegOptions {
        leg_k: selection_k,
        ..Default::default()
    };
    let mode = match strategy {
        Strategy::Rrf(s) => {
            legs.rrf_k = s.rrf_k;
            legs.vector_weight = s.dense_weight;
            legs.bm25_weight = s.lexical_weight;
            FusionMode::GlobalRank
        }
        Strategy::Blend(s) => {
            legs.normalization = s.normalization;
            legs.combination = s.combination;
            legs.vector_weight = s.dense_weight;
            legs.bm25_weight = s.lexical_weight;
            FusionMode::ScoreBlend
        }
        Strategy::Decomposed(s) => {
            legs.vector_weight = s.dense_weight;
            legs.bm25_weight = s.lexical_weight;
            FusionMode::Decomposed
        }
        Strategy::Cascade => FusionMode::Cascade,
    };
    legs.set_fusion_mode(mode);
    (mode, legs)
}

/// Boost validation: at most one, lexical, composite selections only
/// (it maps to the hybrid route's BoostRescore).
fn parse_boost<'a>(
    boosts: &'a [crate::pb::BoostQuery],
    shape: &Shape<'_>,
) -> Result<Option<(&'a str, BoostRescore)>, Status> {
    let boost = match boosts {
        [] => return Ok(None),
        [one] => one,
        many => {
            return Err(refuse(format!(
                "{} boost queries; increment 1 serves at most one (the hybrid route's \
                 single BoostRescore)",
                many.len()
            )))
        }
    };
    if !matches!(shape, Shape::Composite { .. }) {
        return Err(refuse(
            "a boost on a single-leaf selection is not served yet: the boost rescoring \
             rides the hybrid route. Use the composite selection, or wait for the \
             single-leaf boost increment",
        ));
    }
    let query = boost
        .query
        .as_ref()
        .ok_or_else(|| refuse("boost has no query"))?;
    let Some(search_query::Query::Lexical(lexical)) = query.query.as_ref() else {
        return Err(refuse(format!(
            "boost {:?} is not lexical; only a candidate-scoped LEXICAL boost has an \
             engine path (BoostRescore)",
            query.id
        )));
    };
    if !lexical.score_stages.is_empty() {
        return Err(refuse(
            "score_stages on a boost query are not served; stages ride the selection's \
             lexical leaf only",
        ));
    }
    if lexical.analysis.is_some() {
        return Err(refuse(
            "a boost query carries no analysis of its own: BoostRescore analyzes the \
             boost text under the REQUEST's analysis options, so a differing spec here \
             would be silently ignored",
        ));
    }
    Ok(Some((
        &query.id,
        BoostRescore {
            text: lexical.text.clone(),
            window: boost.window,
            base_weight: boost.base_weight,
            boost_weight: boost.boost_weight,
        },
    )))
}

/// Every id in the request — search leaves, filters, boosts — must be
/// non-empty and request-unique, which is what makes score provenance
/// unambiguous.
fn check_ids(plan: &Plan<'_>, boosts: &[crate::pb::BoostQuery]) -> Result<(), Status> {
    let mut ids: Vec<&str> = Vec::new();
    match &plan.shape {
        Shape::Browse => {}
        Shape::Lexical { id, .. } | Shape::Dense { id, .. } => ids.push(id),
        Shape::Composite {
            dense_id,
            lexical_id,
            ..
        } => {
            ids.push(dense_id);
            ids.push(lexical_id);
        }
    }
    ids.extend(plan.filter_ids.iter());
    for boost in boosts {
        if let Some(q) = &boost.query {
            ids.push(&q.id);
        }
    }
    let mut seen: Vec<&str> = Vec::new();
    for id in ids {
        if id.is_empty() {
            return Err(refuse(
                "every search, filter, and boost query needs a non-empty id; ids are what \
                 make score provenance unambiguous",
            ));
        }
        if seen.contains(&id) {
            return Err(refuse(format!("duplicate query id {id:?}")));
        }
        seen.push(id);
    }
    Ok(())
}
