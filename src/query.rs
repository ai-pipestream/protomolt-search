//! The public `Query` adapter (`docs/query-api.md`).
//!
//! This module is deliberately an ADAPTER and nothing more: it
//! validates the public shape, maps a supported request onto the
//! ordinary routes that already exist (`Search`, `Bm25Search`,
//! `HybridSearch` with its fusion modes and `BoostRescore`), executes
//! that route through the same handler every direct caller uses, and
//! translates the response into per-signal provenance. The one piece
//! of scoring that lives ABOVE the routes is the composite scorer
//! (`src/ltr.rs`), which reorders the already-selected candidate pool
//! and so never touches a pruning certificate. It never forks route
//! scoring logic, and it refuses every shape outside the mapping table
//! BY NAME — compatibility never authorizes a heuristic substitute (no
//! `AND` reinterpreted as a union, no filter applied to one leg, no
//! silent strategy swap).
//!
//! The refusal messages are load-bearing surface: each one names the
//! unsupported construct and, where one exists, the supported way to
//! ask for the same thing.

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::future::Future;
use std::pin::Pin;

use tonic::{Request, Status};

use crate::coordinator::CoordinatorServiceImpl;
use crate::pb::search_service_server::SearchService;
use crate::pb::{
    search_query, selection_query, selection_score_strategy, BlendScore, Bm25SearchRequest,
    BoostRescore, DecomposedScore, DenseExecutionMode, DenseQuery, DenseScoreMode, FilterQuery,
    FusionMode, GeoFilter, HybridLegOptions, HybridSearchRequest, LexicalQuery, QueryHit,
    QueryRequest, QueryResponse, QuerySignal, RrfScore, SearchQuery, SearchRequest,
    SelectionOperator, SelectionQuery,
};

#[derive(Clone, Default)]
struct BooleanHit {
    score: f32,
    signals: Vec<QuerySignal>,
    matched: Vec<String>,
}

enum PlannedSearchKind {
    Lexical {
        terms: Vec<String>,
        analysis_fingerprint: u64,
        epochs: Vec<crate::stats_identity::StatsClaim>,
        score_stages: Vec<crate::pb::ScoreStage>,
    },
    Dense {
        vector: Vec<f32>,
        exact_fp32: bool,
    },
}

struct PlannedSearchLeaf {
    id: String,
    membership: BTreeSet<u64>,
    kind: PlannedSearchKind,
}

struct PlannedMatcher {
    id: String,
    membership: BTreeSet<u64>,
}

struct PlannedBooleanNode {
    membership: BTreeSet<u64>,
    searches: Vec<PlannedSearchLeaf>,
    matchers: Vec<PlannedMatcher>,
    membership_wire_bytes: u64,
    /// Sealed segments the shards consulted and ruled out while
    /// resolving this node's membership (docs/segment-pruning.md).
    prune: crate::segment_prune::PruneStats,
}

fn compile_boolean_filter(
    filter: &FilterQuery,
) -> Result<crate::coordinator::RequestFilters, Status> {
    let (geo, cel) = match filter.predicate.as_ref() {
        Some(crate::pb::filter_query::Predicate::Cel(cel)) if !cel.is_empty() => {
            (Vec::new(), cel.as_str())
        }
        Some(crate::pb::filter_query::Predicate::Geo(geo)) => (vec![geo.clone()], ""),
        Some(crate::pb::filter_query::Predicate::Cel(_)) => {
            return Err(refuse(format!(
                "filter {:?} has an empty CEL predicate",
                filter.id
            )))
        }
        None => return Err(refuse(format!("filter {:?} has no predicate", filter.id))),
    };
    crate::coordinator::RequestFilters::compile(&geo, cel)
}

fn plan_boolean_selection<'a>(
    coordinator: &'a CoordinatorServiceImpl,
    selection: &'a SelectionQuery,
    depth: usize,
) -> Pin<Box<dyn Future<Output = Result<PlannedBooleanNode, Status>> + Send + 'a>> {
    Box::pin(async move {
        if depth > 64 {
            return Err(refuse(
                "boolean selection exceeds the 64-level recursion limit",
            ));
        }
        match selection.node.as_ref().ok_or_else(|| refuse("empty selection node"))? {
            selection_query::Node::Search(search) => {
                if search.id.is_empty() {
                    return Err(refuse("a search clause needs a non-empty id"));
                }
                let (membership, kind) = match search.query.as_ref() {
                    Some(search_query::Query::Lexical(query)) => {
                        if query.text.is_empty() {
                            return Err(refuse(format!(
                                "lexical clause {:?} has empty text",
                                search.id
                            )));
                        }
                        if query.phrase.is_some() || !query.prefixes.is_empty() {
                            return Err(refuse(format!(
                                "lexical clause {:?}: a phrase or prefix constraint is not served inside \
                                 BooleanQuery yet (membership there is resolved from term \
                                 bitmaps, which carry no positions); use a single lexical leaf \
                                 selection for a phrase",
                                search.id
                            )));
                        }
                        let membership = coordinator
                            .lexical_membership(&query.text, query.analysis.as_ref())
                            .await?;
                        let kind = PlannedSearchKind::Lexical {
                            terms: membership.terms.clone(),
                            analysis_fingerprint: crate::analyzer::analysis_fingerprint(query.analysis.as_ref()),
                            epochs: membership.epochs.clone(),
                            score_stages: query.score_stages.clone(),
                        };
                        (membership, kind)
                    }
                    Some(search_query::Query::Dense(query)) => {
                        if query.quality.is_some() {
                            return Err(refuse(
                                "DenseQualityPolicy is not used by exact bitmap Boolean planning",
                            ));
                        }
                        let execution = dense_execution_mode(query)?;
                        if execution == DenseExecutionMode::Ann {
                            return Err(refuse(
                                "ANN cannot establish recursive boolean membership exactly; use EXACT/AUTO or a top-level ANN selection",
                            ));
                        }
                        if query.vector.is_empty() {
                            return Err(refuse(format!(
                                "dense clause {:?} has an empty vector",
                                search.id
                            )));
                        }
                        let membership = coordinator.vector_membership("").await?;
                        let kind = PlannedSearchKind::Dense {
                            vector: query.vector.clone(),
                            exact_fp32: dense_score_mode(query)? == DenseScoreMode::Fp32Rerank,
                        };
                        (membership, kind)
                    }
                    None => {
                        return Err(refuse(format!(
                            "search clause {:?} has no query",
                            search.id
                        )))
                    }
                };
                let ids = membership.ids;
                Ok(PlannedBooleanNode {
                    membership: ids.clone(),
                    searches: vec![PlannedSearchLeaf {
                        id: search.id.clone(),
                        membership: ids.clone(),
                        kind,
                    }],
                    matchers: vec![PlannedMatcher {
                        id: search.id.clone(),
                        membership: ids,
                    }],
                    membership_wire_bytes: membership.wire_bytes,
                    prune: membership.prune,
                })
            }
            selection_query::Node::Filter(filter) => {
                if filter.id.is_empty() {
                    return Err(refuse("a filter clause needs a non-empty id"));
                }
                let membership = coordinator
                    .filter_membership(&compile_boolean_filter(filter)?)
                    .await?;
                let ids = membership.ids;
                Ok(PlannedBooleanNode {
                    membership: ids.clone(),
                    searches: Vec::new(),
                    matchers: vec![PlannedMatcher {
                        id: filter.id.clone(),
                        membership: ids,
                    }],
                    membership_wire_bytes: membership.wire_bytes,
                    prune: membership.prune,
                })
            }
            selection_query::Node::Boolean(boolean) => {
                if boolean.aggregate.is_some() {
                    return Err(refuse(
                        "aggregate belongs on the root BooleanQuery, not a nested clause",
                    ));
                }
                plan_boolean_group(coordinator, boolean, depth + 1).await
            }
            selection_query::Node::Composite(_) => Err(refuse(
                "inside recursive boolean selection, express hybrid membership as dense and lexical MUST/SHOULD clauses; legacy CompositeSearchStrategy remains supported as the top-level compatibility shape",
            )),
        }
    })
}

fn plan_boolean_group<'a>(
    coordinator: &'a CoordinatorServiceImpl,
    boolean: &'a crate::pb::BooleanQuery,
    depth: usize,
) -> Pin<Box<dyn Future<Output = Result<PlannedBooleanNode, Status>> + Send + 'a>> {
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
            must.push(plan_boolean_selection(coordinator, clause, depth).await?);
        }
        let mut should = Vec::with_capacity(boolean.should.len());
        for clause in &boolean.should {
            should.push(plan_boolean_selection(coordinator, clause, depth).await?);
        }
        let mut must_not = Vec::with_capacity(boolean.must_not.len());
        for clause in &boolean.must_not {
            must_not.push(plan_boolean_selection(coordinator, clause, depth).await?);
        }
        let minimum_should_match = if boolean.minimum_should_match == 0
            && boolean.must.is_empty()
            && !boolean.should.is_empty()
        {
            1
        } else {
            boolean.minimum_should_match as usize
        };
        // Seed MUST intersections from the cheapest bitmap. With no MUST,
        // count SHOULD memberships directly; a negative-only group starts
        // from the live-document bitmap rather than a paged browse.
        let mut membership =
            if let Some(seed) = must.iter().min_by_key(|clause| clause.membership.len()) {
                let mut ids = seed.membership.clone();
                for clause in &must {
                    if !std::ptr::eq(clause, seed) {
                        ids.retain(|id| clause.membership.contains(id));
                    }
                }
                ids
            } else if minimum_should_match > 0 {
                let mut counts = BTreeMap::<u64, usize>::new();
                for clause in &should {
                    for &id in &clause.membership {
                        *counts.entry(id).or_default() += 1;
                    }
                }
                counts
                    .into_iter()
                    .filter_map(|(id, count)| (count >= minimum_should_match).then_some(id))
                    .collect()
            } else {
                let empty = crate::coordinator::RequestFilters::compile(&[], "")?;
                coordinator.filter_membership(&empty).await?.ids
            };
        if !must.is_empty() && minimum_should_match > 0 {
            membership.retain(|id| {
                should
                    .iter()
                    .filter(|clause| clause.membership.contains(id))
                    .count()
                    >= minimum_should_match
            });
        }
        for clause in &must_not {
            membership.retain(|id| !clause.membership.contains(id));
        }

        let membership_wire_bytes =
            must.iter()
                .chain(&should)
                .chain(&must_not)
                .try_fold(0u64, |total, node| {
                    total
                        .checked_add(node.membership_wire_bytes)
                        .ok_or_else(|| {
                            Status::resource_exhausted("Boolean membership byte count overflow")
                        })
                })?;
        let mut prune = crate::segment_prune::PruneStats::default();
        for node in must.iter().chain(&should).chain(&must_not) {
            prune.add(node.prune);
        }
        let mut searches = Vec::new();
        let mut matchers = Vec::new();
        for mut node in must.into_iter().chain(should) {
            searches.append(&mut node.searches);
            matchers.append(&mut node.matchers);
        }
        Ok(PlannedBooleanNode {
            membership,
            searches,
            matchers,
            membership_wire_bytes,
            prune,
        })
    })
}

async fn score_boolean_plan(
    coordinator: &CoordinatorServiceImpl,
    plan: &PlannedBooleanNode,
) -> Result<BTreeMap<u64, BooleanHit>, Status> {
    let mut hits: BTreeMap<u64, BooleanHit> = plan
        .membership
        .iter()
        .copied()
        .map(|id| (id, BooleanHit::default()))
        .collect();
    for matcher in &plan.matchers {
        for &id in plan.membership.intersection(&matcher.membership) {
            hits.get_mut(&id)
                .expect("planned membership owns every matcher id")
                .matched
                .push(matcher.id.clone());
        }
    }
    for leaf in &plan.searches {
        let candidates: Vec<u64> = plan
            .membership
            .intersection(&leaf.membership)
            .copied()
            .collect();
        for chunk in candidates.chunks(coordinator.max_k() as usize) {
            let scores = match &leaf.kind {
                PlannedSearchKind::Lexical {
                    terms,
                    analysis_fingerprint,
                    epochs,
                    score_stages,
                } => {
                    coordinator
                        .lexical_signal_terms_with_stages(
                            terms,
                            *analysis_fingerprint,
                            chunk,
                            Some(epochs),
                            score_stages,
                        )
                        .await?
                }
                PlannedSearchKind::Dense { vector, exact_fp32 } => {
                    if *exact_fp32 {
                        coordinator
                            .exact_vector_scores(vector, chunk, "")
                            .await?
                            .scores
                    } else {
                        coordinator.dense_signal(vector, chunk, "").await?
                    }
                }
            };
            for &id in chunk {
                let Some(&score) = scores.get(&id) else {
                    return Err(Status::failed_precondition(format!(
                        "boolean membership selected doc {id} for scoring clause {:?}, but candidate rescore did not return it",
                        leaf.id
                    )));
                };
                let hit = hits
                    .get_mut(&id)
                    .expect("planned membership owns every scored id");
                hit.score += score;
                hit.signals.push(QuerySignal {
                    id: leaf.id.clone(),
                    score,
                });
            }
        }
    }
    Ok(hits)
}

fn boolean_ids<'a>(
    selection: &'a SelectionQuery,
    ids: &mut Vec<(&'a str, crate::ltr::SignalKind)>,
    positive: bool,
) -> Result<bool, Status> {
    use crate::ltr::SignalKind;
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

fn parse_boolean_boosts<'a>(
    boosts: &'a [crate::pb::BoostQuery],
    scorer_present: bool,
    scored: bool,
) -> Result<BoostPlan<'a>, Status> {
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
        if scorer_present
            && (boost.window != 0 || boost.base_weight != 0.0 || boost.boost_weight != 0.0)
        {
            return Err(refuse(
                "boolean boosts are signal-only with a composite scorer; leave window/base_weight/boost_weight unset",
            ));
        }
        let query = boost
            .query
            .as_ref()
            .ok_or_else(|| refuse("boost has no query"))?;
        let kind = match query.query.as_ref() {
            Some(search_query::Query::Lexical(lexical)) => {
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
            Some(search_query::Query::Dense(dense)) => {
                if dense.vector.is_empty() || dense_score_mode(dense)? == DenseScoreMode::Fp32Rerank
                {
                    return Err(refuse(
                        "a boolean dense boost needs a vector and uses provider-native candidate scoring",
                    ));
                }
                BoostKind::Dense {
                    vector: &dense.vector,
                }
            }
            None => return Err(refuse(format!("boost {:?} has no query", query.id))),
        };
        parsed.push(ParsedBoost {
            id: &query.id,
            kind,
            window: boost.window,
            base_weight: boost.base_weight,
            boost_weight: boost.boost_weight,
        });
    }
    Ok(BoostPlan::Adapter(parsed))
}

async fn execute_recursive_boolean(
    coordinator: &CoordinatorServiceImpl,
    req: QueryRequest,
    boolean: &crate::pb::BooleanQuery,
) -> Result<QueryResponse, Status> {
    let t_total = std::time::Instant::now();
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
    if req.selection_k != 0 {
        return Err(refuse(
            "selection_k is not used by exact bitmap Boolean selection; leave it zero",
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
    let selection = req
        .selection
        .as_ref()
        .expect("caller found the root boolean");
    let mut namespace = Vec::new();
    let scored = boolean_ids(selection, &mut namespace, true)?;
    for boost in &req.boosts {
        let query = boost
            .query
            .as_ref()
            .ok_or_else(|| refuse("boost has no query"))?;
        namespace.push((&query.id, crate::ltr::SignalKind::Boost));
    }
    let mut seen = HashSet::new();
    for (id, _) in &namespace {
        if id.is_empty() {
            return Err(refuse(
                "every boolean clause and boost needs a non-empty id",
            ));
        }
        if !seen.insert(*id) {
            return Err(refuse(format!("duplicate query id {id:?}")));
        }
    }
    let scorer = match &req.scorer {
        Some(scorer) if scorer.operation != 0 || !scorer.dimensions.is_empty() => {
            if !scored {
                return Err(refuse(
                    "the composite scorer requires at least one positive scoring clause",
                ));
            }
            Some(crate::ltr::Scorer::validate(scorer, &namespace)?)
        }
        _ => None,
    };
    let boosts = parse_boolean_boosts(&req.boosts, scorer.is_some(), scored)?;
    let empty = crate::coordinator::RequestFilters::compile(&[], "")?;
    let t_selection = std::time::Instant::now();
    let (evaluated, plan_prune) = {
        let mut attempt = 0;
        loop {
            let plan = plan_boolean_group(coordinator, boolean, 1).await?;
            let _membership_wire_bytes = plan.membership_wire_bytes;
            match score_boolean_plan(coordinator, &plan).await {
                Err(status) if status.code() == tonic::Code::Aborted && attempt == 0 => {
                    attempt += 1;
                }
                Err(status) if status.code() == tonic::Code::Aborted => {
                    return Err(Status::failed_precondition(format!(
                        "the lexical generation changed twice while planning this Boolean query; retry against a stable generation: {}",
                        status.message()
                    )));
                }
                Err(status) => return Err(status),
                Ok(hits) => break (hits, plan.prune),
            }
        }
    };
    let mut hits: Vec<QueryHit> = evaluated
        .into_iter()
        .map(|(doc_id, hit)| QueryHit {
            identity: None,
            snippets: Vec::new(),
            projected: Vec::new(),
            doc_id,
            score: hit.score,
            rank: 0,
            signals: hit.signals,
            matched: hit.matched,
            sort_key: 0.0,
            sort_values: Vec::new(),
            dimensions: Vec::new(),
            explain: None,
        })
        .collect();
    hits.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then_with(|| a.doc_id.cmp(&b.doc_id))
    });
    if req.explain {
        for hit in &mut hits {
            hit.explain = Some(crate::explain::boolean(hit));
        }
    }
    let mut profile: Option<crate::pb::QueryProfile> = req.profile.then(Default::default);
    if let Some(profile) = profile.as_mut() {
        profile.selection_ms = ms(t_selection);
        profile.segments_total = plan_prune.segments_total;
        profile.segments_skipped = plan_prune.segments_skipped;
        // A boolean root resolves each clause on its own shard set; the
        // profile counts the topology and reports no plan-level skip.
        let empty = crate::coordinator::RequestFilters::compile(&[], "")?;
        profile.shards_total = coordinator.shard_prune_counts(&empty).0;
    }
    apply_boosts(
        coordinator,
        &boosts,
        &mut hits,
        scorer.is_some(),
        &mut profile,
    )
    .await?;
    let executed = apply_scorer(
        coordinator,
        &scorer,
        &mut hits,
        "boolean:bitmap",
        &mut profile,
    )
    .await?;
    if req.explain {
        let scorer_name: Option<String> = scorer.as_ref().map(|s| s.executed_suffix());
        crate::explain::finish(&mut hits, &window_boosts(&boosts), scorer_name.as_deref())?;
    }
    let aggregate = if let Some(aggregate) = &boolean.aggregate {
        if !aggregate.filter.trim().is_empty() || !aggregate.geo_filters.is_empty() {
            return Err(refuse(
                "BooleanQuery.aggregate uses the boolean match set; its own filter and geo_filters must be empty",
            ));
        }
        let compiled = crate::coordinator::compile_aggregations(aggregate)?;
        let ids: Vec<u64> = hits.iter().map(|hit| hit.doc_id).collect();
        Some(
            coordinator
                .fanout_aggregate(&empty, &compiled, Some(&ids))
                .await?,
        )
    } else {
        None
    };
    let (mut hits, next_cursor) = page(hits, k, cursor.as_ref())?;
    let compiled_projections = crate::coordinator::compile_projections(&req.projections)?;
    fill_projected(coordinator, &compiled_projections, &mut hits, &mut profile).await?;
    let mut response = done(
        req.request_id,
        hits,
        &executed,
        next_cursor,
        finish_prof(profile, t_total),
    );
    response.aggregate = aggregate;
    Ok(response)
}

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
    /// Sorted boundary: the last hit's sort keys in merge form, one per
    /// sort column. `Some` exactly on tokens minted by a sorted query;
    /// a cursor from one query shape refuses on another.
    keys: Option<Vec<crate::sortkeys::Key>>,
    doc_id: u64,
}

const CURSOR_PREFIX: &str = "tvq1";
const SORTED_CURSOR_PREFIX: &str = "tvqs2";

impl Cursor {
    fn encode(&self) -> String {
        match &self.keys {
            Some(keys) => format!(
                "{SORTED_CURSOR_PREFIX}:{}:{}:{}",
                self.rank,
                crate::sortkeys::encode_keys(keys),
                self.doc_id
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
        let (score_bits, keys) = if sorted {
            (0, Some(crate::sortkeys::decode_keys(hex).ok_or_else(bad)?))
        } else {
            (u32::from_str_radix(hex, 16).map_err(|_| bad())?, None)
        };
        Ok(Cursor {
            rank,
            score_bits,
            keys,
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
            if c.keys.is_some() {
                return Err(refuse(
                    "this cursor came from a sorted query; the rest of the request must \
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
                    keys: None,
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
pub(crate) async fn execute(
    coordinator: &CoordinatorServiceImpl,
    req: QueryRequest,
) -> Result<QueryResponse, Status> {
    let t_total = std::time::Instant::now();
    // Purely observational: the same request with profile off returns
    // identical hits, bitwise.
    let mut prof: Option<crate::pb::QueryProfile> = req.profile.then(Default::default);
    let selection = req
        .selection
        .as_ref()
        .ok_or_else(|| refuse("a query needs a selection tree"))?;
    // A pool aggregation (docs/aggregations.md "Aggregating a query's
    // pool") compiles before selection, so a bad spec refuses before
    // searching shards; the public handler may already have probed read
    // versions. The fold itself runs once the pool is fixed.
    let pool_aggregate = match req.aggregate.as_ref() {
        None => None,
        Some(aggregate) => {
            if matches!(selection.node, Some(selection_query::Node::Boolean(_))) {
                return Err(refuse(
                    "a boolean root aggregates over its exact match set on \
                     BooleanQuery.aggregate; QueryRequest.aggregate serves the pooled \
                     shapes (a leaf, a composite, a scorer or boost pool) and a browse",
                ));
            }
            if !aggregate.filter.trim().is_empty() || !aggregate.geo_filters.is_empty() {
                return Err(refuse(
                    "QueryRequest.aggregate folds over the selection's candidate pool; its \
                     own filter and geo_filters must be empty",
                ));
            }
            Some(crate::coordinator::compile_aggregations(aggregate)?)
        }
    };
    // Snippets are cut around the lexical leg's occurrence spans, which
    // only the single lexical selection carries to the client; every
    // other shape — the boolean planner included — refuses rather than
    // returning hits without them (docs/highlighting.md).
    let single_lexical = matches!(
        selection.node.as_ref(),
        Some(selection_query::Node::Search(SearchQuery {
            query: Some(search_query::Query::Lexical(_)),
            ..
        }))
    );
    if req.highlight.is_some() && !single_lexical {
        return Err(refuse(
            "highlight is served for the single lexical selection only: no other shape \
             carries the occurrence spans snippets are cut around",
        ));
    }
    if let Some(selection_query::Node::Boolean(boolean)) = selection.node.as_ref() {
        let boolean = boolean.clone();
        return execute_recursive_boolean(coordinator, req, &boolean).await;
    }
    let plan = parse_selection(selection)?;
    let filter = plan
        .cel
        .iter()
        .map(|c| format!("({c})"))
        .collect::<Vec<_>>()
        .join(" && ");
    if let Some(p) = prof.as_mut() {
        // The shards the plan's filter rules out before fan-out
        // (docs/placement.md); a plan without a filter skips none.
        let filters = crate::coordinator::RequestFilters::compile(&plan.geo_filters, &filter)?;
        let (total, skipped) = coordinator.shard_prune_counts(&filters);
        p.shards_total = total;
        p.shards_skipped = skipped;
    }
    let mut dense_execution = match &plan.shape {
        Shape::Dense { query, .. } | Shape::Composite { dense: query, .. } => {
            let requested = dense_execution_mode(query)?;
            // The policy key AUTO is judged on: k as sent, the named
            // candidate depth, and the request's filters (their live
            // selectivity is measured only when a policy is consulted).
            let filters = (!plan.geo_filters.is_empty() || !plan.cel.is_empty())
                .then(|| crate::coordinator::RequestFilters::compile(&plan.geo_filters, &filter))
                .transpose()?;
            Some(
                coordinator
                    .resolve_dense_execution(
                        requested,
                        query.vector.len(),
                        crate::coordinator::DenseRequestKey {
                            k: req.k,
                            candidate_depth: req.selection_k,
                            filters: filters.as_ref(),
                        },
                    )
                    .await?,
            )
        }
        _ => None,
    };
    // A policy point fixes the candidate depth AUTO runs at; it is the
    // depth that was measured, reported back, and never widened.
    let policy_depth: Option<u32> = dense_execution
        .as_ref()
        .filter(|o| o.resolved_mode == DenseExecutionMode::Ann as i32 && o.policy_point.is_some())
        .map(|o| o.candidate_depth);
    let fp32_rerank = match &plan.shape {
        Shape::Dense { query, .. } => {
            let mode = dense_score_mode(query)?;
            if query.quality.is_some() && mode != DenseScoreMode::Fp32Rerank {
                return Err(refuse(
                    "DenseQualityPolicy is served only with DENSE_SCORE_MODE_FP32_RERANK",
                ));
            }
            mode == DenseScoreMode::Fp32Rerank
        }
        Shape::Composite { dense, .. } => {
            if dense.quality.is_some() {
                return Err(refuse(
                    "DenseQualityPolicy is currently served on a single dense selection only",
                ));
            }
            if dense_score_mode(dense)? == DenseScoreMode::Fp32Rerank {
                return Err(refuse(
                    "FP32 rerank is currently served on a single dense selection only; \
                     composite fusion still consumes provider-native dense scores",
                ));
            }
            false
        }
        _ => false,
    };
    check_ids(&plan, &req.boosts)?;
    // An empty scorer message is treated as absent (a set-but-empty
    // one names neither operation nor dimensions).
    let scorer = match &req.scorer {
        Some(s) if s.operation != 0 || !s.dimensions.is_empty() => {
            if matches!(plan.shape, Shape::Browse) {
                return Err(refuse(
                    "the composite scorer requires a SCORED selection: a browse has no \
                     relevance signals to combine, and its deterministic id (or column) \
                     order is the whole contract of the route",
                ));
            }
            Some(crate::ltr::Scorer::validate(
                s,
                &signal_ids(&plan, &req.boosts),
            )?)
        }
        _ => None,
    };
    // Projections: the lexical route carries them natively (bitwise the
    // path that always served them); a sorted lexical leaf and every other shape — browse
    // included — fetches them post-selection by id through the
    // FetchValues seam. The selection is already fixed when that runs,
    // so no pruning certificate is involved.
    let compiled_projections = if req.projections.is_empty()
        || (req.sort.is_empty() && matches!(plan.shape, Shape::Lexical { .. }))
    {
        Vec::new()
    } else {
        crate::coordinator::compile_projections(&req.projections)?
    };
    if !req.sort.is_empty() {
        for sort in &req.sort {
            if sort.column.is_empty() {
                return Err(refuse("sort names no column"));
            }
        }
        match &plan.shape {
            Shape::Browse => {}
            Shape::Lexical { query, .. } => {
                // A column order over a lexical leaf walks the leaf's exact
                // term membership without scoring; every relevance shape on
                // the leaf or the request would be silently dropped.
                let relevance = if query.phrase.is_some() {
                    Some("a phrase")
                } else if !query.prefixes.is_empty() {
                    Some("term prefixes")
                } else if !query.score_stages.is_empty() {
                    Some("score stages")
                } else if !req.boosts.is_empty() {
                    Some("a boost")
                } else if scorer.is_some() {
                    Some("the composite scorer")
                } else if req.highlight.is_some() {
                    Some("highlighting")
                } else if !query.synonyms.is_empty() {
                    Some("synonym rules")
                } else {
                    None
                };
                if let Some(what) = relevance {
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
    }
    if let Some(collapse) = &req.collapse {
        if collapse.column.is_empty() {
            return Err(refuse("collapse names no column"));
        }
        if matches!(plan.shape, Shape::Browse) {
            return Err(refuse(
                "collapse needs a SCORED selection: a browse has no order to pick a \
                 group's representative by",
            ));
        }
    }

    // AUTO + FP32 rerank with neither a policy nor a selection_k names no
    // depth; running at selection_k = k would silently serve the raw
    // quantized top-k (0.544 recall@10 on the challenge fixture). AUTO on
    // an exhaustive provider therefore resolves the depth through the
    // installed profile's default target, exactly as an explicit policy
    // would (docs/dense-quality-profile.md); AUTO through an ANN policy
    // already fixed its depth and is left alone. EXACT/UNSPECIFIED keep
    // the pool at k: that is the caller's explicit choice.
    let auto_default_depth = match &plan.shape {
        Shape::Dense { query, .. } => {
            fp32_rerank
                && query.quality.is_none()
                && req.selection_k == 0
                && dense_execution_mode(query)? == DenseExecutionMode::Auto
                && dense_execution
                    .as_ref()
                    .is_some_and(|o| o.resolved_mode == DenseExecutionMode::Exact as i32)
        }
        _ => false,
    };
    let quality_resolution = match &plan.shape {
        Shape::Dense { query, .. } => match &query.quality {
            Some(policy) => {
                if req.selection_k != 0 {
                    return Err(refuse(
                        "DenseQualityPolicy and selection_k are competing depth authorities; leave selection_k zero",
                    ));
                }
                Some(
                    coordinator
                        .resolve_dense_quality(req.k, query.vector.len(), policy)
                        .await?,
                )
            }
            None if auto_default_depth => Some(
                coordinator
                    .resolve_dense_quality_default(req.k, query.vector.len())
                    .await?,
            ),
            None => None,
        },
        _ => None,
    };

    // selection_k: the candidate-set depth. 0 = k unless a measured
    // quality profile resolved it. The response is the best k of the
    // selection_k candidates under the final order.
    let selection_k = if let Some(resolution) = &quality_resolution {
        resolution.selection_k
    } else if req.selection_k == 0 {
        policy_depth.unwrap_or(req.k)
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

    let boosts = parse_boosts(&req.boosts, &plan.shape, scorer.is_some())?;
    if req.explain {
        // The tree explains a score; a shape that computes none has
        // no tree to give and says so (docs/explain.md).
        if matches!(plan.shape, Shape::Browse) {
            return Err(refuse(
                "explain needs a SCORED selection: a browse computes no relevance, and its                  id (or column) order is the whole contract of the route",
            ));
        }
        if !req.sort.is_empty() {
            return Err(refuse(
                "explain is served on relevance-ordered results: a column sort over a                  lexical leaf walks its exact membership and computes no score to explain",
            ));
        }
    }
    let window_boosts = window_boosts(&boosts);
    let scorer_name: Option<String> = scorer.as_ref().map(|s| s.executed_suffix());
    // On a single-leaf shape without a scorer or boost no phase uses
    // extra selection depth; on a composite it is the leg/gate depth
    // (and the paging pool), with a scorer it is the pool the scorer
    // reorders, and with a boost it is the pool the boost rescores
    // (the honest form of the rescore window).
    // An aggregation reads the pool too: it is the set the fold runs
    // over, so the depth is fixed and paging moves inside it.
    let pooled = matches!(plan.shape, Shape::Composite { .. })
        || scorer.is_some()
        || !req.boosts.is_empty()
        || fp32_rerank
        || req.collapse.is_some()
        || pool_aggregate.is_some();
    // A collapse over a single leaf deepens its pool itself (below), so
    // the leaf's own depth rules (paging by fetching deeper, a policy
    // depth) do not apply; every other collapse is a fixed pool.
    let collapse_may_deepen = req.collapse.is_some()
        && matches!(plan.shape, Shape::Lexical { .. } | Shape::Dense { .. })
        && scorer.is_none()
        && req.boosts.is_empty()
        && !fp32_rerank
        && policy_depth.is_none();
    if !pooled && policy_depth.is_none() && selection_k != req.k {
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
    // fetches past the boundary; a POOLED order is NOT — a composite's
    // strategy moves with depth (RRF ranks, blend normalization, the
    // cascade gate), and a scorer's normalization statistics move with
    // the pool — so paging stays inside the fixed selection_k pool.
    let fetch_k = match (&plan.shape, &cursor) {
        // Browse pages by an id floor (append-only ids are stable), so
        // every page fetches exactly k.
        (Shape::Browse, _) => req.k,
        _ if pooled => {
            // Under a collapse the cursor's rank counts groups, and the
            // group count is what decides exhaustion (below).
            if let (Some(c), None) = (&cursor, &req.collapse) {
                if u64::from(c.rank) + u64::from(req.k) > u64::from(selection_k) {
                    return Err(Status::failed_precondition(format!(
                        "the selection_k = {selection_k} candidate set is exhausted at rank {}; \
                         deepening it would change the ranking under the cursor; \
                         re-run from the first page with a larger selection_k",
                        c.rank
                    )));
                }
            }
            selection_k
        }
        (_, Some(c)) => match policy_depth {
            Some(depth) => {
                if u64::from(c.rank) + u64::from(req.k) > u64::from(depth) {
                    return Err(Status::failed_precondition(format!(
                        "the policy candidate depth = {depth} is exhausted at rank {}; a deeper \
                         traversal is a different measured point; re-run from the first page \
                         with a selection_k the policy measured",
                        c.rank
                    )));
                }
                depth
            }
            None => c.rank + req.k,
        },
        (_, None) => policy_depth.unwrap_or(req.k),
    };

    match &plan.shape {
        Shape::Browse => {
            let mut response = execute_browse(
                coordinator,
                &req,
                &plan,
                &filter,
                cursor.as_ref(),
                &compiled_projections,
                Vec::new(),
                0,
                None,
                "browse",
                prof,
                t_total,
            )
            .await?;
            if let Some(compiled) = &pool_aggregate {
                // A browse has no pool: its membership is the filter's
                // exact match set, which the aggregation fan-out
                // evaluates on the shards directly; `matched` is that
                // set's size, whatever page was asked for.
                let filters =
                    crate::coordinator::RequestFilters::compile(&plan.geo_filters, &filter)?;
                response.aggregate = Some(
                    coordinator
                        .fanout_aggregate(&filters, compiled, None)
                        .await?,
                );
            }
            Ok(response)
        }
        Shape::Lexical { id, query } if !req.sort.is_empty() => {
            if pool_aggregate.is_some() {
                return Err(refuse(
                    "aggregate over a sorted lexical leaf is not served: the leaf's term \
                     membership is walked page by page and never held as a set; sort by \
                     relevance (a pool) or name the membership as a filter (a browse)",
                ));
            }
            // A sorted lexical leaf: its exact term membership, walked in
            // column order (validated above: no relevance shape rides
            // along). Analysis happens once here; the shards read the
            // same postings the bitmap route would.
            let terms = coordinator
                .analyze_terms(&query.text, query.analysis.as_ref())
                .await?;
            if terms.is_empty() {
                let mut response = done(
                    req.request_id.clone(),
                    Vec::new(),
                    "browse_shard:lexical",
                    String::new(),
                    finish_prof(prof, t_total),
                );
                response.executed.push_str("(no terms)");
                return Ok(response);
            }
            execute_browse(
                coordinator,
                &req,
                &plan,
                &filter,
                cursor.as_ref(),
                &compiled_projections,
                terms,
                crate::analyzer::analysis_fingerprint(query.analysis.as_ref()),
                Some(id),
                "browse_shard:lexical",
                prof,
                t_total,
            )
            .await
        }
        Shape::Lexical { id, query } => {
            let t_sel = std::time::Instant::now();
            let response = coordinator
                .bm25_search(crate::metrics::nested(Request::new(Bm25SearchRequest {
                    text: query.text.clone(),
                    k: fetch_k,
                    analysis: query.analysis.clone(),
                    score_stages: query.score_stages.clone(),
                    phrase: query.phrase,
                    prefixes: query.prefixes.clone(),
                    geo_filters: plan.geo_filters.clone(),
                    filter,
                    projections: req.projections.clone(),
                    highlight: req.highlight.clone(),
                    synonyms: query.synonyms.clone(),
                    synonyms_off: query.synonyms_off,
                    explain: req.explain,
                    ..Default::default()
                })))
                .await?
                .into_inner();
            if let Some(p) = prof.as_mut() {
                p.selection_ms = ms(t_sel);
                p.segments_total = response.segments_total;
                p.segments_skipped = response.segments_skipped;
            }
            let synonym_expansions = response.synonym_expansions.clone();
            let mut hits: Vec<QueryHit> = response
                .hits
                .iter()
                .map(|h| QueryHit {
                    identity: h.identity.clone(),
                    snippets: h.snippets.clone(),
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
                    sort_values: Vec::new(),
                    dimensions: Vec::new(),
                    explain: None,
                })
                .collect();
            if req.explain {
                for (hit, source) in hits.iter_mut().zip(&response.hits) {
                    hit.explain = Some(crate::explain::lexical(
                        id,
                        source,
                        &response.prefix_expansions,
                        &response.synonym_expansions,
                    )?);
                }
            }
            apply_boosts(coordinator, &boosts, &mut hits, scorer.is_some(), &mut prof).await?;
            let executed =
                apply_scorer(coordinator, &scorer, &mut hits, "bm25_search", &mut prof).await?;
            let pool_ids: Vec<u64> = hits.iter().map(|h| h.doc_id).collect();
            if req.explain {
                crate::explain::finish(&mut hits, &window_boosts, scorer_name.as_deref())?;
            }
            let (hits, mut groups, next, executed) = match page_or_collapse(
                coordinator,
                hits,
                &req,
                cursor.as_ref(),
                fetch_k,
                collapse_may_deepen,
                executed,
                &mut prof,
            )
            .await?
            {
                Paged::Deepen(depth) => return deepen(coordinator, &req, depth).await,
                Paged::Done(hits, groups, next, executed) => (hits, groups, next, executed),
            };
            fill_projected_groups(coordinator, &compiled_projections, &mut groups, &mut prof)
                .await?;
            let mut response = done(
                req.request_id,
                hits,
                &executed,
                next,
                finish_prof(prof, t_total),
            );
            response.groups = groups;
            response.synonym_expansions = synonym_expansions;
            response.aggregate =
                aggregate_pool(coordinator, pool_aggregate.as_ref(), &pool_ids).await?;
            Ok(response)
        }
        Shape::Dense { id, query } => {
            let t_sel = std::time::Instant::now();
            let response = coordinator
                .search(crate::metrics::nested(Request::new(SearchRequest {
                    request_id: req.request_id.clone(),
                    k: fetch_k,
                    vector: query.vector.clone(),
                    geo_filters: plan.geo_filters.clone(),
                    filter,
                    ..Default::default()
                })))
                .await?
                .into_inner();
            if let Some(p) = prof.as_mut() {
                p.selection_ms = ms(t_sel);
                p.segments_total = response.segments_total;
                p.segments_skipped = response.segments_skipped;
            }
            let mut hits: Vec<QueryHit> = response
                .hits
                .iter()
                .map(|h| QueryHit {
                    identity: h.identity.clone(),
                    snippets: Vec::new(),
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
                    sort_values: Vec::new(),
                    dimensions: Vec::new(),
                    explain: None,
                })
                .collect();
            let dense_mode = dense_score_mode(query)?;
            let route = if fp32_rerank {
                let t0 = std::time::Instant::now();
                let ids: Vec<u64> = hits.iter().map(|hit| hit.doc_id).collect();
                let reranked = coordinator
                    .exact_vector_scores(&query.vector, &ids, "")
                    .await?;
                for hit in &mut hits {
                    let score = *reranked.scores.get(&hit.doc_id).ok_or_else(|| {
                        Status::failed_precondition(format!(
                            "FP32 rerank candidate {} has no exact score",
                            hit.doc_id
                        ))
                    })?;
                    if req.explain {
                        hit.explain = Some(crate::explain::dense(
                            id,
                            score,
                            dense_mode,
                            Some(hit.score),
                        ));
                    }
                    hit.score = score;
                    hit.signals[0].score = score;
                }
                hits.sort_by(|a, b| {
                    b.score
                        .total_cmp(&a.score)
                        .then_with(|| a.doc_id.cmp(&b.doc_id))
                });
                if let Some(p) = prof.as_mut() {
                    p.rerank_ms = ms(t0);
                    p.rerank_rows = reranked.rows;
                    p.rerank_logical_bytes = reranked.logical_bytes;
                    p.rerank_pages = reranked.pages_touched;
                    p.rerank_tasks = reranked.tasks;
                }
                "search:fp32_rerank"
            } else {
                if req.explain {
                    for hit in &mut hits {
                        hit.explain = Some(crate::explain::dense(id, hit.score, dense_mode, None));
                    }
                }
                "search"
            };
            apply_boosts(coordinator, &boosts, &mut hits, scorer.is_some(), &mut prof).await?;
            let executed = apply_scorer(coordinator, &scorer, &mut hits, route, &mut prof).await?;
            let pool_ids: Vec<u64> = hits.iter().map(|h| h.doc_id).collect();
            if req.explain {
                crate::explain::finish(&mut hits, &window_boosts, scorer_name.as_deref())?;
            }
            let (mut hits, mut groups, next, executed) = match page_or_collapse(
                coordinator,
                hits,
                &req,
                cursor.as_ref(),
                fetch_k,
                collapse_may_deepen,
                executed,
                &mut prof,
            )
            .await?
            {
                Paged::Deepen(depth) => return deepen(coordinator, &req, depth).await,
                Paged::Done(hits, groups, next, executed) => (hits, groups, next, executed),
            };
            fill_projected(coordinator, &compiled_projections, &mut hits, &mut prof).await?;
            fill_projected_groups(coordinator, &compiled_projections, &mut groups, &mut prof)
                .await?;
            let mut response = done(
                response.request_id,
                hits,
                &executed,
                next,
                finish_prof(prof, t_total),
            );
            response.groups = groups;
            response.aggregate =
                aggregate_pool(coordinator, pool_aggregate.as_ref(), &pool_ids).await?;
            if auto_default_depth {
                if let (Some(outcome), Some(resolution)) =
                    (dense_execution.as_mut(), quality_resolution.as_ref())
                {
                    outcome.planner_reason.push_str(&format!(
                        "; FP32 rerank depth selection_k={} resolved through quality profile \
                         {:?} default_target_recall_ppm={} (as an explicit DenseQualityPolicy would)",
                        resolution.selection_k, resolution.profile_id, resolution.target_recall_ppm
                    ));
                }
            }
            response.dense_quality =
                quality_resolution.map(|resolution| crate::pb::DenseQualityOutcome {
                    target_recall_ppm: resolution.target_recall_ppm,
                    selection_k: resolution.selection_k,
                    profile_fingerprint: resolution.profile_fingerprint,
                    profile_id: resolution.profile_id,
                    embedding_model: resolution.embedding_model,
                    corpus_generation: resolution.corpus_generation,
                    corpus_rows: resolution.corpus_rows,
                    provider_backend: resolution.provider_backend,
                    scoring_fingerprint: resolution.scoring_fingerprint,
                });
            if fp32_rerank {
                if let Some(outcome) = dense_execution.as_mut() {
                    if outcome.resolved_mode == DenseExecutionMode::Ann as i32 {
                        outcome.planner_reason.push_str(
                            "; FP32 rerank rescored that candidate pool without widening it, so \
                             the traversal stays approximate",
                        );
                    }
                }
            }
            response.dense_execution = dense_execution;
            Ok(response)
        }
        Shape::Composite {
            dense_id,
            dense,
            lexical_id,
            lexical,
            strategy,
        } => {
            let (mode, legs) = leg_options(strategy, selection_k);
            let legacy = match &boosts {
                BoostPlan::LegacyHybrid(id, b) => Some((id.clone(), b.clone())),
                _ => None,
            };
            let boost_id = legacy.as_ref().map(|(id, _)| id.clone());
            let t_sel = std::time::Instant::now();
            let response = coordinator
                .hybrid_search(crate::metrics::nested(Request::new(HybridSearchRequest {
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
                    boost: legacy.map(|(_, b)| b),
                    geo_filters: plan.geo_filters.clone(),
                    filter,
                    ..Default::default()
                })))
                .await?
                .into_inner();
            if let Some(p) = prof.as_mut() {
                p.selection_ms = ms(t_sel);
            }
            let mut hits: Vec<QueryHit> = if matches!(strategy, Strategy::Cascade) {
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
                            identity: None,
                            snippets: Vec::new(),
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
                            sort_values: Vec::new(),
                            dimensions: Vec::new(),
                            explain: None,
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
                            identity: None,
                            snippets: Vec::new(),
                            projected: Vec::new(),
                            doc_id: h.doc_id,
                            score: h.fused_score,
                            rank: 0,
                            signals,
                            matched: matched(m, &plan.filter_ids),
                            sort_key: 0.0,
                            sort_values: Vec::new(),
                            dimensions: Vec::new(),
                            explain: None,
                        }
                    })
                    .collect()
            };
            let route = match mode {
                FusionMode::GlobalRank => "hybrid_search:global_rank",
                FusionMode::ScoreBlend => "hybrid_search:score_blend",
                FusionMode::Decomposed => "hybrid_search:decomposed",
                _ => "hybrid_search:cascade",
            };
            if req.explain {
                let fusion = crate::explain::Fusion::resolve(mode, &legs);
                let legacy_boost = match &boosts {
                    BoostPlan::LegacyHybrid(id, b) => Some(crate::explain::LegacyBoost {
                        id: id.clone(),
                        base_weight: if b.base_weight == 0.0 {
                            1.0
                        } else {
                            f64::from(b.base_weight)
                        },
                        boost_weight: if b.boost_weight == 0.0 {
                            1.0
                        } else {
                            f64::from(b.boost_weight)
                        },
                    }),
                    _ => None,
                };
                if matches!(strategy, Strategy::Cascade) {
                    for (hit, source) in hits.iter_mut().zip(&response.cascade_hits) {
                        hit.explain = Some(crate::explain::cascade(
                            source,
                            dense_id,
                            lexical_id,
                            legacy_boost.as_ref(),
                        ));
                    }
                } else {
                    for (hit, source) in hits.iter_mut().zip(&response.hits) {
                        hit.explain = Some(crate::explain::composite(
                            source,
                            &fusion,
                            dense_id,
                            lexical_id,
                            legacy_boost.as_ref(),
                        )?);
                    }
                }
            }
            apply_boosts(coordinator, &boosts, &mut hits, scorer.is_some(), &mut prof).await?;
            let executed = apply_scorer(coordinator, &scorer, &mut hits, route, &mut prof).await?;
            let pool_ids: Vec<u64> = hits.iter().map(|h| h.doc_id).collect();
            if req.explain {
                crate::explain::finish(&mut hits, &window_boosts, scorer_name.as_deref())?;
            }
            let (mut hits, mut groups, next, executed) = match page_or_collapse(
                coordinator,
                hits,
                &req,
                cursor.as_ref(),
                fetch_k,
                collapse_may_deepen,
                executed,
                &mut prof,
            )
            .await?
            {
                Paged::Deepen(depth) => return deepen(coordinator, &req, depth).await,
                Paged::Done(hits, groups, next, executed) => (hits, groups, next, executed),
            };
            fill_projected(coordinator, &compiled_projections, &mut hits, &mut prof).await?;
            fill_projected_groups(coordinator, &compiled_projections, &mut groups, &mut prof)
                .await?;
            let mut response = done(
                response.request_id,
                hits,
                &executed,
                next,
                finish_prof(prof, t_total),
            );
            response.groups = groups;
            response.dense_execution = dense_execution;
            response.aggregate =
                aggregate_pool(coordinator, pool_aggregate.as_ref(), &pool_ids).await?;
            Ok(response)
        }
    }
}

/// Fold a pool aggregation over the candidate pool the page was drawn
/// from (docs/aggregations.md "Aggregating a query's pool"): the same
/// explicit-id fan-out the boolean root uses, so `matched` is the
/// pool's size and the folds are the exact ones the Aggregate route
/// computes over a filter.
async fn aggregate_pool(
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
async fn apply_scorer(
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

/// Fill the paged hits' projected values through the FetchValues seam
/// (non-lexical shapes; the lexical route carries projections
/// natively). A hit whose shard holds no column tables projects
/// all-absent — exact, its document holds no such values.
async fn fill_projected(
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

/// Every request id and what it names, for scorer source validation.
fn signal_ids<'a>(
    plan: &'a Plan<'a>,
    boosts: &'a [crate::pb::BoostQuery],
) -> Vec<(&'a str, crate::ltr::SignalKind)> {
    use crate::ltr::SignalKind;
    let mut ids: Vec<(&str, SignalKind)> = Vec::new();
    match &plan.shape {
        Shape::Browse => {}
        Shape::Lexical { id, .. } | Shape::Dense { id, .. } => ids.push((id, SignalKind::Search)),
        Shape::Composite {
            dense_id,
            lexical_id,
            ..
        } => {
            ids.push((dense_id, SignalKind::Search));
            ids.push((lexical_id, SignalKind::Search));
        }
    }
    for f in &plan.filter_ids {
        ids.push((f, SignalKind::Filter));
    }
    for b in boosts {
        if let Some(q) = &b.query {
            ids.push((&q.id, SignalKind::Boost));
        }
    }
    ids
}

fn ms(t: std::time::Instant) -> f32 {
    t.elapsed().as_secs_f32() * 1e3
}

fn finish_prof(
    mut prof: Option<crate::pb::QueryProfile>,
    t_total: std::time::Instant,
) -> Option<crate::pb::QueryProfile> {
    if let Some(p) = prof.as_mut() {
        p.total_ms = ms(t_total);
    }
    prof
}

#[allow(clippy::too_many_arguments)]
async fn execute_browse(
    coordinator: &CoordinatorServiceImpl,
    req: &QueryRequest,
    plan: &Plan<'_>,
    filter: &str,
    cursor: Option<&Cursor>,
    compiled_projections: &[crate::pb::CompiledProjection],
    lexical_terms: Vec<String>,
    analysis_fingerprint: u64,
    leaf_id: Option<&str>,
    executed: &str,
    mut prof: Option<crate::pb::QueryProfile>,
    t_total: std::time::Instant,
) -> Result<QueryResponse, Status> {
    {
        if plan.geo_filters.is_empty() && plan.cel.is_empty() && lexical_terms.is_empty() {
            return Err(refuse(
                "an empty browse (no filter at all) would page the whole corpus in id \
                     order; name at least one filter",
            ));
        }
        let compiled = crate::coordinator::RequestFilters::compile(&plan.geo_filters, filter)?;
        let sort: Vec<crate::pb::BrowseSort> = req
            .sort
            .iter()
            .map(|s| crate::pb::BrowseSort {
                column: s.column.clone(),
                descending: s.descending,
            })
            .collect();
        let after = match cursor {
            None => None,
            Some(c) => {
                // A cursor resumes the query that minted it: a
                // sorted token carries the key boundary, a plain
                // one only the id, and mixing the two shapes is a
                // different query.
                match (sort.is_empty(), &c.keys) {
                    (false, Some(keys)) if keys.len() == sort.len() => {
                        Some(crate::coordinator::BrowseAfter {
                            id: c.doc_id,
                            keys: keys.clone(),
                        })
                    }
                    (true, None) => Some(crate::coordinator::BrowseAfter {
                        id: c.doc_id,
                        keys: Vec::new(),
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
        let base_rank = cursor.map_or(0, |c| c.rank);
        let t_sel = std::time::Instant::now();
        let rows = coordinator
            .fanout_browse(
                req.k,
                after,
                &sort,
                &lexical_terms,
                analysis_fingerprint,
                &compiled,
            )
            .await?;
        if let Some(p) = prof.as_mut() {
            p.selection_ms = ms(t_sel);
            p.segments_total = rows.prune.segments_total;
            p.segments_skipped = rows.prune.segments_skipped;
        }
        let hits: Vec<QueryHit> = rows
            .ids
            .iter()
            .enumerate()
            .map(|(i, &doc_id)| QueryHit {
                identity: None,
                snippets: Vec::new(),
                projected: Vec::new(),
                doc_id,
                // No relevance score exists on this route; the id
                // (or column) order IS the order, and rank counts
                // on across pages.
                score: 0.0,
                rank: base_rank + (i + 1) as u32,
                signals: Vec::new(),
                matched: matched(
                    leaf_id.map(str::to_string).into_iter().collect(),
                    &plan.filter_ids,
                ),
                sort_key: rows
                    .values
                    .get(i)
                    .and_then(|v| v.first())
                    .map_or(0.0, crate::sortkeys::Value::as_f64),
                sort_values: rows
                    .values
                    .get(i)
                    .map(|v| v.iter().map(crate::sortkeys::Value::to_pb).collect())
                    .unwrap_or_default(),
                dimensions: Vec::new(),
                explain: None,
            })
            .collect();
        let next = if req.k != 0 && hits.len() == req.k as usize {
            hits.last()
                .map(|h| {
                    Cursor {
                        rank: h.rank,
                        score_bits: 0,
                        keys: rows
                            .sorted
                            .then(|| rows.keys.last().expect("full page").clone()),
                        doc_id: h.doc_id,
                    }
                    .encode()
                })
                .unwrap_or_default()
        } else {
            String::new()
        };
        let mut hits = hits;
        fill_projected(coordinator, compiled_projections, &mut hits, &mut prof).await?;
        Ok(done(
            req.request_id.clone(),
            hits,
            executed,
            next,
            finish_prof(prof, t_total),
        ))
    }
}

/// What a page step produced: the page, or the depth a single-leaf
/// collapse must re-run at to find its groups.
enum Paged {
    Done(Vec<QueryHit>, Vec<crate::pb::QueryGroup>, String, String),
    Deepen(u32),
}

/// Re-run the request with a deeper collapse pool (docs/query-api.md
/// "Collapse"): the leaf's order is depth-independent, so a deeper pool
/// only adds groups after the ones already found.
async fn deepen(
    coordinator: &CoordinatorServiceImpl,
    req: &QueryRequest,
    depth: u32,
) -> Result<QueryResponse, Status> {
    let mut again = req.clone();
    again.selection_k = depth;
    Box::pin(execute(coordinator, again)).await
}

/// A collapse group's identity: an integer or a facet term.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
enum GroupKey {
    Integer(i64),
    UnsignedInteger(u64),
    Text(String),
}

impl GroupKey {
    fn to_value(&self) -> crate::pb::SortValue {
        match self {
            GroupKey::Integer(i) => crate::sortkeys::Value::Integer(*i).to_pb(),
            GroupKey::UnsignedInteger(i) => crate::sortkeys::Value::UnsignedInteger(*i).to_pb(),
            GroupKey::Text(t) => crate::sortkeys::Value::Text(t.clone()).to_pb(),
        }
    }
}

/// The collapse key of every candidate: the lineage keys through the
/// parent resolver, a column through the value seam (an i64 or facet
/// column; a double or bool is not a group identity). A candidate
/// without a value is absent.
async fn collapse_keys(
    coordinator: &CoordinatorServiceImpl,
    column: &str,
    ids: &[u64],
) -> Result<std::collections::HashMap<u64, GroupKey>, Status> {
    let mut keys = std::collections::HashMap::with_capacity(ids.len());
    if column == "parent_id" || column == "group_id" {
        for (id, v) in coordinator.lineage_key(ids, column).await? {
            keys.insert(id, GroupKey::UnsignedInteger(v));
        }
        return Ok(keys);
    }
    let compiled = crate::coordinator::compile_projections(&[crate::pb::NamedProjection {
        name: column.to_string(),
        expression: column.to_string(),
    }])?;
    let fetched = coordinator.fetch_values(ids, &compiled, &[]).await?;
    for (id, values) in fetched.rows {
        let Some(value) = values.first().and_then(|v| v.value.as_ref()) else {
            continue;
        };
        let key = match value {
            crate::pb::projected_value::Value::UintValue(v) => GroupKey::UnsignedInteger(*v),
            crate::pb::projected_value::Value::IntValue(i) => GroupKey::Integer(*i),
            crate::pb::projected_value::Value::StringValue(t) => GroupKey::Text(t.clone()),
            crate::pb::projected_value::Value::DoubleValue(_) => {
                return Err(refuse(format!(
                    "collapse by {column:?}: a double column is not a group identity; \
                     collapse by an i64 or facet column, or by parent_id / group_id"
                )))
            }
            crate::pb::projected_value::Value::BoolValue(_) => {
                return Err(refuse(format!(
                    "collapse by {column:?}: a bool is not a group identity; collapse by an \
                     i64 or facet column, or by parent_id / group_id"
                )))
            }
        };
        keys.insert(id, key);
    }
    Ok(keys)
}

/// Cut the page, or collapse the candidate pool into groups and cut
/// the page of groups (docs/query-api.md "Collapse"). `pool_depth` is
/// how deep the pool was fetched; a full pool that holds too few
/// groups either deepens (a single leaf) or refuses naming
/// selection_k (a fixed pool).
#[allow(clippy::too_many_arguments)]
async fn page_or_collapse(
    coordinator: &CoordinatorServiceImpl,
    hits: Vec<QueryHit>,
    req: &QueryRequest,
    cursor: Option<&Cursor>,
    pool_depth: u32,
    may_deepen: bool,
    executed: String,
    prof: &mut Option<crate::pb::QueryProfile>,
) -> Result<Paged, Status> {
    let Some(collapse) = req.collapse.as_ref() else {
        let (hits, next) = page(hits, req.k, cursor)?;
        return Ok(Paged::Done(hits, Vec::new(), next, executed));
    };
    let t0 = std::time::Instant::now();
    let pool_full = hits.len() as u64 >= u64::from(pool_depth);
    let ids: Vec<u64> = hits.iter().map(|h| h.doc_id).collect();
    let keys = collapse_keys(coordinator, &collapse.column, &ids).await?;
    // Groups in first-appearance order; the pool's order is the
    // selection's order, so the first hit of a group is its best.
    let mut index: std::collections::HashMap<GroupKey, usize> = std::collections::HashMap::new();
    let mut groups: Vec<(GroupKey, Vec<QueryHit>)> = Vec::new();
    for hit in hits {
        let Some(key) = keys.get(&hit.doc_id) else {
            continue;
        };
        match index.get(key) {
            Some(&g) => groups[g].1.push(hit),
            None => {
                index.insert(key.clone(), groups.len());
                groups.push((key.clone(), vec![hit]));
            }
        }
    }
    let needed = u64::from(cursor.map_or(0, |c| c.rank)) + u64::from(req.k);
    if (groups.len() as u64) < needed && pool_full {
        let max_k = coordinator.max_k();
        if may_deepen && pool_depth < max_k {
            return Ok(Paged::Deepen(pool_depth.saturating_mul(2).min(max_k)));
        }
        if !may_deepen {
            return Err(Status::failed_precondition(format!(
                "the selection_k = {pool_depth} candidate pool holds {} groups by {:?}, \
                 fewer than the {needed} the page needs; deepening it would change the \
                 ranking under the cursor; re-run from the first page with a larger \
                 selection_k",
                groups.len(),
                collapse.column
            )));
        }
        // At max_k with too few groups: what the pool holds is served,
        // and the short page says nothing follows at the served depth.
    }
    let reps: Vec<QueryHit> = groups.iter().map(|(_, hits)| hits[0].clone()).collect();
    let (page_reps, next) = page(reps, req.k, cursor)?;
    let listed = collapse.inner_hits.max(1) as usize;
    let mut out_groups = Vec::with_capacity(page_reps.len());
    for rep in &page_reps {
        let g = index[keys.get(&rep.doc_id).expect("a representative has a key")];
        let (key, members) = &groups[g];
        let pool_hits = members.len();
        let mut listed_hits: Vec<QueryHit> = members.iter().take(listed).cloned().collect();
        for (i, hit) in listed_hits.iter_mut().enumerate() {
            hit.rank = (i + 1) as u32;
        }
        out_groups.push(crate::pb::QueryGroup {
            key: Some(key.to_value()),
            hits: listed_hits,
            complete: pool_hits >= listed || !pool_full,
            pool_hits: pool_hits as u32,
        });
    }
    if let Some(p) = prof.as_mut() {
        p.collapse_ms = ms(t0);
    }
    Ok(Paged::Done(
        page_reps,
        out_groups,
        next,
        format!("{executed}+collapse"),
    ))
}

/// Projections on the listed inner hits, the same seam the page's hits
/// use.
async fn fill_projected_groups(
    coordinator: &CoordinatorServiceImpl,
    compiled: &[crate::pb::CompiledProjection],
    groups: &mut [crate::pb::QueryGroup],
    prof: &mut Option<crate::pb::QueryProfile>,
) -> Result<(), Status> {
    if compiled.is_empty() || groups.is_empty() {
        return Ok(());
    }
    let mut all: Vec<QueryHit> = groups.iter().flat_map(|g| g.hits.iter().cloned()).collect();
    fill_projected(coordinator, compiled, &mut all, prof).await?;
    let mut filled = all.into_iter();
    for group in groups.iter_mut() {
        for hit in &mut group.hits {
            if let Some(f) = filled.next() {
                hit.projected = f.projected;
            }
        }
    }
    Ok(())
}

fn done(
    request_id: String,
    hits: Vec<QueryHit>,
    executed: &str,
    next_cursor: String,
    profile: Option<crate::pb::QueryProfile>,
) -> QueryResponse {
    QueryResponse {
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
                        selection_query::Node::Boolean(_) => {
                            return Err(refuse(
                                "BooleanQuery is a root selection shape; do not wrap it in legacy CompositeSearchStrategy",
                            ))
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
        selection_query::Node::Boolean(_) => {
            return Err(refuse(
                "BooleanQuery must be executed through the recursive boolean planner",
            ))
        }
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
                return Err(refuse(format!(
                    "filter {:?} has an empty CEL predicate",
                    f.id
                )));
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

fn dense_score_mode(query: &DenseQuery) -> Result<DenseScoreMode, Status> {
    DenseScoreMode::try_from(query.score_mode)
        .map_err(|_| refuse(format!("unknown dense score_mode {}", query.score_mode)))
}

fn dense_execution_mode(query: &DenseQuery) -> Result<DenseExecutionMode, Status> {
    DenseExecutionMode::try_from(query.execution_mode).map_err(|_| {
        refuse(format!(
            "unknown dense execution_mode {}",
            query.execution_mode
        ))
    })
}

/// The two-leaf strategy composite: exactly one dense and one lexical
/// scoring leaf, an operator the strategy can certify, and a strategy
/// that maps onto a fusion mode.
fn composite_shape(composite: &crate::pb::CompositeSearchStrategy) -> Result<Shape<'_>, Status> {
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
                    if q.phrase.is_some() || !q.prefixes.is_empty() {
                        return Err(refuse(
                            "a phrase or prefix constraint on a composite's lexical leaf is not served: \
                             the hybrid legs carry no phrase gate yet. Use a \
                             single-lexical-leaf selection for a phrase",
                        ));
                    }
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
            Some(selection_query::Node::Boolean(_)) => {
                return Err(refuse(
                    "BooleanQuery is not nested inside legacy CompositeSearchStrategy; use it as the root selection shape",
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

/// One validated boost query, ready to score adapter-side through the
/// candidate-scoped rescore seams.
struct ParsedBoost<'a> {
    id: &'a str,
    kind: BoostKind<'a>,
    window: u32,
    base_weight: f32,
    boost_weight: f32,
}

enum BoostKind<'a> {
    /// A lexical boost with the analysis it resolves to: the
    /// selection's lexical leaf's spec when one exists (term identity
    /// must match the index the leaf searched), the boost's own on a
    /// dense-only selection (there is nothing to inherit).
    Lexical {
        text: &'a str,
        analysis: Option<crate::pb::AnalysisSpec>,
    },
    Dense {
        vector: &'a [f32],
    },
}

/// How the request's boosts execute.
enum BoostPlan<'a> {
    None,
    /// The composite + single lexical boost without a scorer keeps its
    /// original engine path (`BoostRescore` on the hybrid route),
    /// bitwise.
    LegacyHybrid(String, BoostRescore),
    /// Everything else scores adapter-side through the rescore seams:
    /// signal-only under a scorer, the weighted window reorder alone.
    Adapter(Vec<ParsedBoost<'a>>),
}

/// Boost validation. A boost never admits a document; it scores the
/// fixed candidate pool. Without a scorer exactly one boost is served
/// (its base_weight/boost_weight reorder is the combination); multiple
/// boosts need the composite scorer, which owns combination. With a
/// scorer present every boost is signal-only: the reorder knobs
/// (window, base_weight, boost_weight) are refused — each would be a
/// silent no-op — and the whole candidate set is scored.
fn parse_boosts<'a>(
    boosts: &'a [crate::pb::BoostQuery],
    shape: &Shape<'a>,
    scorer_present: bool,
) -> Result<BoostPlan<'a>, Status> {
    if boosts.is_empty() {
        return Ok(BoostPlan::None);
    }
    if matches!(shape, Shape::Browse) {
        return Err(refuse(
            "a boost needs a SCORED selection: a browse has no base order to boost, and \
             reordering its deterministic id order would make it a scored query in \
             disguise",
        ));
    }
    if boosts.len() > 1 && !scorer_present {
        return Err(refuse(format!(
            "{} boost queries with no composite scorer; the scorer is what defines how \
             multiple boost signals combine — name one, or send a single boost with \
             base_weight/boost_weight",
            boosts.len()
        )));
    }
    let has_lexical_leaf = matches!(shape, Shape::Lexical { .. } | Shape::Composite { .. });
    let mut parsed = Vec::with_capacity(boosts.len());
    for boost in boosts {
        let query = boost
            .query
            .as_ref()
            .ok_or_else(|| refuse("boost has no query"))?;
        if scorer_present
            && (boost.window != 0 || boost.base_weight != 0.0 || boost.boost_weight != 0.0)
        {
            return Err(refuse(
                "window, base_weight, and boost_weight belong to the boost's own reorder; \
                 with a composite scorer present the boost is signal-only (the scorer owns \
                 combination and the whole candidate set is scored), so naming them would \
                 be a silent no-op",
            ));
        }
        let kind = match query.query.as_ref() {
            Some(search_query::Query::Lexical(lexical)) => {
                if lexical.phrase.is_some() || !lexical.prefixes.is_empty() {
                    return Err(refuse(
                        "a phrase or prefix constraint on a boost query is not served; a boost scores \
                         a fixed candidate set, and the phrase gate belongs to selection",
                    ));
                }
                if !lexical.score_stages.is_empty() {
                    return Err(refuse(
                        "score_stages on a boost query are not served; stages ride the \
                         selection's lexical leaf only",
                    ));
                }
                if lexical.analysis.is_some() && has_lexical_leaf {
                    return Err(refuse(
                        "a boost query carries no analysis of its own when the selection \
                         has a lexical leaf: the boost text is analyzed under that leaf's \
                         analysis options (term identity must match the index it \
                         searched), so a differing spec here would be silently ignored",
                    ));
                }
                if lexical.text.is_empty() {
                    return Err(refuse(format!("boost {:?} has empty text", query.id)));
                }
                let analysis = match shape {
                    Shape::Lexical { query: leaf, .. } => leaf.analysis.clone(),
                    Shape::Composite { lexical: leaf, .. } => leaf.analysis.clone(),
                    Shape::Dense { .. } => lexical.analysis.clone(),
                    Shape::Browse => unreachable!("refused above"),
                };
                BoostKind::Lexical {
                    text: &lexical.text,
                    analysis,
                }
            }
            Some(search_query::Query::Dense(dense)) => {
                if dense_score_mode(dense)? == DenseScoreMode::Fp32Rerank {
                    return Err(refuse(
                        "FP32 rerank is a selection mode, not a dense boost mode; dense boosts \
                         continue to use provider-native candidate scoring",
                    ));
                }
                if dense.vector.is_empty() {
                    return Err(refuse(format!(
                        "dense boost {:?} has an empty vector",
                        query.id
                    )));
                }
                BoostKind::Dense {
                    vector: &dense.vector,
                }
            }
            None => return Err(refuse(format!("boost {:?} has no query", query.id))),
        };
        parsed.push(ParsedBoost {
            id: &query.id,
            kind,
            window: boost.window,
            base_weight: boost.base_weight,
            boost_weight: boost.boost_weight,
        });
    }
    if !scorer_present && parsed.len() == 1 && matches!(shape, Shape::Composite { .. }) {
        if let BoostKind::Lexical { text, .. } = &parsed[0].kind {
            return Ok(BoostPlan::LegacyHybrid(
                parsed[0].id.to_string(),
                BoostRescore {
                    text: (*text).to_string(),
                    window: parsed[0].window,
                    base_weight: parsed[0].base_weight,
                    boost_weight: parsed[0].boost_weight,
                },
            ));
        }
    }
    Ok(BoostPlan::Adapter(parsed))
}

/// Score the adapter-side boosts over the candidate pool: each boost
/// scores the top-`window` of the CURRENT order through its rescore
/// seam and lands its raw relevance as a named signal on the hits it
/// scored. Without a scorer (a single boost, by validation) the window
/// is then reordered by `base_weight * base + boost_weight * boost`
/// (0 = 1.0, the BoostRescore convention), ties by doc id; hits beyond
/// the window keep their order after it. The reported score stays the
/// BASE score — exactly the legacy boost surface: the boost's own
/// relevance rides provenance, never the score field.
async fn apply_boosts(
    coordinator: &CoordinatorServiceImpl,
    plan: &BoostPlan<'_>,
    hits: &mut [QueryHit],
    scorer_present: bool,
    prof: &mut Option<crate::pb::QueryProfile>,
) -> Result<(), Status> {
    let BoostPlan::Adapter(list) = plan else {
        return Ok(());
    };
    let t0 = std::time::Instant::now();
    for b in list {
        let window = if b.window == 0 {
            hits.len()
        } else {
            (b.window as usize).min(hits.len())
        };
        let ids: Vec<u64> = hits[..window].iter().map(|h| h.doc_id).collect();
        let scores = match &b.kind {
            BoostKind::Lexical { text, analysis } => {
                coordinator
                    .lexical_signal(text, analysis.as_ref(), &ids)
                    .await?
            }
            BoostKind::Dense { vector } => coordinator.dense_signal(vector, &ids, "").await?,
        };
        for hit in hits[..window].iter_mut() {
            if let Some(score) = scores.get(&hit.doc_id) {
                hit.signals.push(QuerySignal {
                    id: b.id.to_string(),
                    score: *score,
                });
                hit.matched.push(b.id.to_string());
            }
        }
        if !scorer_present {
            let base_w = if b.base_weight == 0.0 {
                1.0
            } else {
                f64::from(b.base_weight)
            };
            let boost_w = if b.boost_weight == 0.0 {
                1.0
            } else {
                f64::from(b.boost_weight)
            };
            let combined = |h: &QueryHit| {
                let boost = h
                    .signals
                    .iter()
                    .find(|s| s.id == b.id)
                    .map_or(0.0, |s| f64::from(s.score));
                base_w * f64::from(h.score) + boost_w * boost
            };
            hits[..window].sort_by(|a, c| {
                combined(c)
                    .total_cmp(&combined(a))
                    .then_with(|| a.doc_id.cmp(&c.doc_id))
            });
        }
    }
    if let Some(p) = prof.as_mut() {
        p.boost_ms = ms(t0);
    }
    Ok(())
}

/// The window boosts the explain tree reports (docs/explain.md): the
/// adapter-planned boosts with their reorder weights defaulted the way
/// `apply_boosts` defaults them. A legacy hybrid boost is reported by
/// the composite builder instead; a scorer-owned boost is a plain
/// signal and appears as a dimension.
fn window_boosts(plan: &BoostPlan<'_>) -> Vec<crate::explain::WindowBoost> {
    match plan {
        BoostPlan::Adapter(list) => list
            .iter()
            .map(|b| crate::explain::WindowBoost {
                id: b.id.to_string(),
                base_weight: if b.base_weight == 0.0 {
                    1.0
                } else {
                    f64::from(b.base_weight)
                },
                boost_weight: if b.boost_weight == 0.0 {
                    1.0
                } else {
                    f64::from(b.boost_weight)
                },
            })
            .collect(),
        _ => Vec::new(),
    }
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
