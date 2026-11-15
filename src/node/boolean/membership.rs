//! Boolean query tree evaluation, filter domain narrowing, and short-circuiting.

use crate::node::*;

// ---------------------------------------------------------------------
// Shard-side recursive Boolean evaluation (docs/query-api.md,
// "Recursive boolean execution"): the planned tree arrives, every leaf
// resolves over this shard's bitmaps, the set algebra runs on the words,
// the members are scored for each scoring leaf, and the best `depth`
// go back. No membership crosses the wire.
// ---------------------------------------------------------------------

/// One planned leaf resolved on this shard.
pub(in crate::node) struct EvaluatedLeaf {
    /// The leaf's live members; a dense leaf's are the live rows that
    /// hold a vector.
    pub(in crate::node) membership: crate::boolean_bits::Bits,
    /// Whether a MUST/SHOULD chain reaches the leaf; MUST_NOT leaves
    /// score nothing. The wire matched list still describes all matching leaves.
    pub(in crate::node) positive: bool,
    pub(in crate::node) filter_known: Option<crate::pb::BooleanFilterKnown>,
    /// A narrowed filter still needs its complete verdict on returned rows
    /// admitted through other branches. Reuse the compiled predicate on those
    /// candidates instead of expanding its whole-shard membership bitmap.
    pub(in crate::node) filter_recheck: Option<crate::filter::DocFilter<'static>>,
}

/// The tree resolved on one shard.
pub(crate) struct EvaluatedBoolean {
    /// The root's members under the group rule of `docs/query-api.md`.
    pub(crate) members: crate::boolean_bits::Bits,
    pub(in crate::node) leaves: Vec<EvaluatedLeaf>,
    /// Sealed segments consulted and ruled out, summed over the leaves,
    /// plus the live-document seed where the group rule consulted it.
    pub(in crate::node) prune: crate::segment_prune::PruneStats,
}

fn boolean_leaf(
    req: &crate::pb::BooleanShardRequest,
    index: u32,
) -> Result<&crate::pb::boolean_plan_leaf::Leaf, Status> {
    req.leaves
        .get(index as usize)
        .and_then(|leaf| leaf.leaf.as_ref())
        .ok_or_else(|| {
            Status::invalid_argument(format!(
                "Boolean plan names leaf {index} and the request holds {} leaves",
                req.leaves.len()
            ))
        })
}

/// Mark the leaves a MUST/SHOULD chain reaches.
fn mark_positive(
    req: &crate::pb::BooleanShardRequest,
    group: &crate::pb::BooleanPlanGroup,
    positive: bool,
    depth: usize,
    out: &mut [bool],
) -> Result<(), Status> {
    if depth > 64 {
        return Err(Status::invalid_argument(
            "Boolean plan exceeds the 64-level recursion limit",
        ));
    }
    if group.must.is_empty() && group.should.is_empty() && group.must_not.is_empty() {
        return Err(Status::invalid_argument(
            "an empty Boolean group has no membership rule",
        ));
    }
    mark_nodes(req, &group.must, positive, depth, out)?;
    mark_nodes(req, &group.should, positive, depth, out)?;
    mark_nodes(req, &group.must_not, false, depth, out)
}

fn mark_nodes(
    req: &crate::pb::BooleanShardRequest,
    nodes: &[crate::pb::BooleanPlanNode],
    positive: bool,
    depth: usize,
    out: &mut [bool],
) -> Result<(), Status> {
    for node in nodes {
        match node.node.as_ref() {
            Some(crate::pb::boolean_plan_node::Node::Group(child)) => {
                mark_positive(req, child, positive, depth + 1, out)?;
            }
            Some(crate::pb::boolean_plan_node::Node::Leaf(index)) => {
                boolean_leaf(req, *index)?;
                if positive {
                    out[*index as usize] = true;
                }
            }
            None => {
                return Err(Status::invalid_argument(
                    "Boolean plan node is neither a group nor a leaf",
                ))
            }
        }
    }
    Ok(())
}

fn sealed_total(bm25: Option<&Bm25Shard>) -> u32 {
    match bm25 {
        Some(Bm25Shard::Segmented(shard)) => shard.sealed_parts() as u32,
        _ => 0,
    }
}

/// Filter-leaf domain narrowing is the default. Setting
/// `PIPESTREAM_SEARCH_BOOLEAN_FILTER_FULL_SCAN` forces the
/// pre-narrowing whole-shard column scan — the A/B baseline. The answer is the same either way; only the cost moves.
/// The MUST short-circuit is orthogonal: an empty group skips its
/// remaining siblings in both modes.
fn boolean_filter_domain_enabled() -> bool {
    std::env::var_os("PIPESTREAM_SEARCH_BOOLEAN_FILTER_FULL_SCAN").is_none()
}

/// The slots a filter leaf's verdict can still decide, per leaf: a row
/// is out of a group the moment one MUST clause misses it, so a filter
/// leaf that appears under MUST beside a lexical sibling (the lexical
/// leaves resolve in the pre-pass, over the whole shard) is consulted
/// on that intersection and nowhere else. A dense sibling narrows no
/// filter: dense leaves resolve after the filters, so a filter that
/// empties its group never pays a dense membership. The bound narrows
/// down a MUST chain of groups; a SHOULD or MUST_NOT position keeps its
/// incoming bound — a minimum-should-match count reads every SHOULD
/// clause wherever the group can still admit, so no sibling narrows a
/// filter there. A leaf the tree reaches under MUST with no lexical
/// sibling and no ancestor bound stays whole-shard (`unbounded`), as
/// does one shared between a bounded and an unbounded position.
pub(in crate::node) fn filter_leaf_domains(
    req: &crate::pb::BooleanShardRequest,
    group: &crate::pb::BooleanPlanGroup,
    bound: Option<&crate::boolean_bits::Bits>,
    leaves: &[LeafSlot],
    bounded: &mut [Option<crate::boolean_bits::Bits>],
    unbounded: &mut [bool],
    depth: usize,
) -> Result<(), Status> {
    use crate::pb::boolean_plan_node::Node;
    if depth > 64 {
        return Err(Status::invalid_argument(
            "Boolean plan exceeds the 64-level recursion limit",
        ));
    }
    let mut narrowed = bound.cloned();
    for node in &group.must {
        if let Some(Node::Leaf(index)) = node.node.as_ref() {
            if !matches!(
                boolean_leaf(req, *index)?,
                crate::pb::boolean_plan_leaf::Leaf::Lexical(_)
            ) {
                continue;
            }
            let membership = &leaves[*index as usize]
                .done()
                .ok_or_else(|| {
                    Status::internal(format!(
                        "boolean lexical leaf {index} has no membership before the filter domains"
                    ))
                })?
                .membership;
            match &mut narrowed {
                Some(bits) => bits.and_with(membership),
                None => narrowed = Some(membership.clone()),
            }
        }
    }
    for node in &group.must {
        match node.node.as_ref() {
            Some(Node::Group(child)) => {
                filter_leaf_domains(
                    req,
                    child,
                    narrowed.as_ref(),
                    leaves,
                    bounded,
                    unbounded,
                    depth + 1,
                )?;
            }
            Some(Node::Leaf(index)) => {
                if matches!(
                    boolean_leaf(req, *index)?,
                    crate::pb::boolean_plan_leaf::Leaf::Filter(_)
                ) {
                    match (&narrowed, &mut bounded[*index as usize]) {
                        (None, _) => unbounded[*index as usize] = true,
                        (Some(domain), slot @ None) => *slot = Some(domain.clone()),
                        (Some(domain), Some(bits)) => bits.or_with(domain),
                    }
                }
            }
            None => {
                return Err(Status::invalid_argument(
                    "Boolean plan node is neither a group nor a leaf",
                ))
            }
        }
    }
    for node in group.should.iter().chain(&group.must_not) {
        match node.node.as_ref() {
            Some(Node::Group(child)) => {
                filter_leaf_domains(req, child, bound, leaves, bounded, unbounded, depth + 1)?;
            }
            Some(Node::Leaf(index)) => {
                if matches!(
                    boolean_leaf(req, *index)?,
                    crate::pb::boolean_plan_leaf::Leaf::Filter(_)
                ) {
                    // A shared leaf must cover this direct occurrence too.
                    // Sibling MUST clauses cannot narrow SHOULD/MUST_NOT here.
                    match (bound, &mut bounded[*index as usize]) {
                        (None, _) => unbounded[*index as usize] = true,
                        (Some(domain), slot @ None) => *slot = Some(domain.clone()),
                        (Some(domain), Some(bits)) => bits.or_with(domain),
                    }
                }
            }
            None => {
                return Err(Status::invalid_argument(
                    "Boolean plan node is neither a group nor a leaf",
                ));
            }
        }
    }
    Ok(())
}

/// Leaf resolutions a MUST short-circuit replaced with an empty set
/// since process start: dense leaves never built, filters resolved
/// over an empty domain. Diagnostics and the short-circuit tests.
pub static BOOLEAN_MUST_SHORT_CIRCUIT_SKIPS: AtomicU64 = AtomicU64::new(0);

/// One leaf's slot in a resolving evaluation.
#[allow(clippy::large_enum_variant)]
pub(in crate::node) enum LeafSlot {
    Todo,
    Done(EvaluatedLeaf),
}

impl LeafSlot {
    pub(in crate::node) fn done(&self) -> Option<&EvaluatedLeaf> {
        match self {
            LeafSlot::Done(leaf) => Some(leaf),
            LeafSlot::Todo => None,
        }
    }
}

/// The working state of one [`evaluate_boolean_membership`] call: the
/// shard, the request, the shared bitmaps, the per-leaf domains, and
/// the slots the group walk fills on demand. Lexical leaves resolve in
/// the pre-pass (they are the filters' narrowing source); dense leaves
/// and filter leaves resolve when the group walk reaches them, so a
/// group whose MUST intersection is already empty never pays them.
struct BooleanMembership<'a> {
    pub(in crate::node) guard: &'a ShardState,
    pub(in crate::node) req: &'a crate::pb::BooleanShardRequest,
    pub(in crate::node) n: usize,
    pub(in crate::node) prune_enabled: bool,
    pub(in crate::node) positive: &'a [bool],
    pub(in crate::node) domains: &'a [Option<crate::boolean_bits::Bits>],
    pub(in crate::node) deleted: &'a Option<Arc<Vec<u64>>>,
    pub(in crate::node) authority: &'a crate::boolean_bits::Bits,
    pub(in crate::node) live: &'a crate::boolean_bits::Bits,
    pub(in crate::node) prune: crate::segment_prune::PruneStats,
    pub(in crate::node) seeded: u32,
    pub(in crate::node) leaves: Vec<LeafSlot>,
}

impl BooleanMembership<'_> {
    /// The filter leaf's membership over its domain — over an empty
    /// domain when its group is already empty (`short`), so the
    /// validation, the known-column handshake, and the prune counts
    /// still run and no row is read.
    pub(in crate::node) fn resolve_filter(
        &mut self,
        index: usize,
        short: bool,
    ) -> Result<(), Status> {
        use crate::boolean_bits::Bits;
        if matches!(self.leaves[index], LeafSlot::Done(_)) {
            return Ok(());
        }
        let filter = match boolean_leaf(self.req, index as u32)? {
            crate::pb::boolean_plan_leaf::Leaf::Filter(filter) => filter,
            _ => {
                return Err(Status::internal(format!(
                    "boolean leaf {index} is not a filter"
                )))
            }
        };
        let geo_regions = validate_geo_filters(&filter.geo_filters)?;
        let bm25 = self.guard.bm25.as_ref();
        let (geo_columns_known, filter_columns_known) =
            filter_known_flags(bm25, &filter.geo_filters, filter.filter.as_ref());
        let empty;
        let domain = if short {
            BOOLEAN_MUST_SHORT_CIRCUIT_SKIPS.fetch_add(1, AtomicOrdering::Relaxed);
            empty = Bits::empty(self.n);
            Some(&empty)
        } else {
            self.domains[index].as_ref()
        };
        let (predicate, allow, stats) = resolve_shard_filters(
            bm25,
            self.deleted.clone(),
            self.n,
            &filter.geo_filters,
            &geo_regions,
            filter.filter.as_ref(),
            self.prune_enabled,
            domain,
        )?;
        self.prune.add(stats);
        let mut membership = match allow {
            None => Bits::full(self.n),
            Some(allow) => {
                if allow.len() != self.n {
                    return Err(Status::internal(format!(
                        "Boolean filter leaf {index} resolved {} slots over a universe \
                         of {}",
                        allow.len(),
                        self.n
                    )));
                }
                allow
            }
        };
        // The early shapes of resolve_shard_filters (no predicate, no
        // store) ignore the domain; narrow here so every shape is
        // bounded the same way.
        if let Some(domain) = domain {
            membership.and_with(domain);
        }
        membership.and_with(self.authority);
        membership.and_with(self.live);
        self.leaves[index] = LeafSlot::Done(EvaluatedLeaf {
            membership,
            positive: self.positive[index],
            filter_known: Some(crate::pb::BooleanFilterKnown {
                geo_columns_known,
                filter_columns_known,
            }),
            filter_recheck: (domain.is_some() && bm25.is_some())
                .then(|| predicate.unwrap_or_default()),
        });
        Ok(())
    }

    /// The dense leaf's membership: the live rows that hold a vector,
    /// resolved on first use. Scoring mode does not change clause
    /// membership: an absent FP32 sidecar must not turn a native vector
    /// into a non-member and silently skip its rescore.
    pub(in crate::node) fn resolve_dense(&mut self, index: usize) -> Result<(), Status> {
        use crate::boolean_bits::Bits;
        if matches!(self.leaves[index], LeafSlot::Done(_)) {
            return Ok(());
        }
        let dense = match boolean_leaf(self.req, index as u32)? {
            crate::pb::boolean_plan_leaf::Leaf::Dense(dense) => dense,
            _ => {
                return Err(Status::internal(format!(
                    "boolean leaf {index} is not dense"
                )))
            }
        };
        let guard = self.guard;
        let ranges = guard.index.as_ref().map_or_else(
            || {
                if dense.exact_fp32 {
                    vec![(
                        0,
                        guard
                            .exact_vectors
                            .as_ref()
                            .map_or(0, ExactVectorStore::len),
                    )]
                } else {
                    Vec::new()
                }
            },
            VectorIndex::vector_rows,
        );
        let mut membership = Bits::ranges(self.n, &ranges);
        membership.and_with(self.authority);
        self.leaves[index] = LeafSlot::Done(EvaluatedLeaf {
            membership,
            positive: self.positive[index],
            filter_known: None,
            filter_recheck: None,
        });
        Ok(())
    }

    /// A dense leaf under a short-circuited group: the empty set,
    /// unpaid. The bits are invisible — the emptiness reaches the root
    /// through the MUST chain, so no scoring, provenance, or aggregate
    /// row can consult them.
    pub(in crate::node) fn skip_dense(&mut self, index: usize) {
        if matches!(self.leaves[index], LeafSlot::Todo) {
            BOOLEAN_MUST_SHORT_CIRCUIT_SKIPS.fetch_add(1, AtomicOrdering::Relaxed);
            self.leaves[index] = LeafSlot::Done(EvaluatedLeaf {
                membership: crate::boolean_bits::Bits::empty(self.n),
                positive: self.positive[index],
                filter_known: None,
                filter_recheck: None,
            });
        }
    }

    pub(in crate::node) fn short_leaf(&mut self, index: u32) -> Result<(), Status> {
        match boolean_leaf(self.req, index)? {
            crate::pb::boolean_plan_leaf::Leaf::Lexical(_) => Ok(()),
            crate::pb::boolean_plan_leaf::Leaf::Dense(_) => {
                self.skip_dense(index as usize);
                Ok(())
            }
            crate::pb::boolean_plan_leaf::Leaf::Filter(_) => {
                self.resolve_filter(index as usize, true)
            }
        }
    }

    /// Resolve every leaf under an already-empty group without paying
    /// for it: filters over the empty domain, dense leaves as the
    /// empty set.
    pub(in crate::node) fn short_group(
        &mut self,
        group: &crate::pb::BooleanPlanGroup,
        depth: usize,
    ) -> Result<(), Status> {
        if depth > 64 {
            return Err(Status::invalid_argument(
                "Boolean plan exceeds the 64-level recursion limit",
            ));
        }
        for node in group
            .must
            .iter()
            .chain(&group.should)
            .chain(&group.must_not)
        {
            self.short_node(node, depth)?;
        }
        Ok(())
    }

    pub(in crate::node) fn short_node(
        &mut self,
        node: &crate::pb::BooleanPlanNode,
        depth: usize,
    ) -> Result<(), Status> {
        use crate::pb::boolean_plan_node::Node;
        match node.node.as_ref() {
            Some(Node::Group(child)) => self.short_group(child, depth + 1),
            Some(Node::Leaf(index)) => self.short_leaf(*index),
            None => Err(Status::invalid_argument(
                "Boolean plan node is neither a group nor a leaf",
            )),
        }
    }

    /// One resolved node's membership bits.
    pub(in crate::node) fn resolve_node(
        &mut self,
        node: &crate::pb::BooleanPlanNode,
        on_must_path: bool,
        depth: usize,
    ) -> Result<crate::boolean_bits::Bits, Status> {
        use crate::pb::boolean_plan_node::Node;
        match node.node.as_ref() {
            Some(Node::Group(child)) => self.resolve_group(child, on_must_path, depth + 1),
            Some(Node::Leaf(index)) => {
                let index = *index as usize;
                match boolean_leaf(self.req, index as u32)? {
                    crate::pb::boolean_plan_leaf::Leaf::Lexical(_) => {}
                    crate::pb::boolean_plan_leaf::Leaf::Filter(_) => {
                        self.resolve_filter(index, false)?
                    }
                    crate::pb::boolean_plan_leaf::Leaf::Dense(_) => self.resolve_dense(index)?,
                }
                match &self.leaves[index] {
                    LeafSlot::Done(leaf) => Ok(leaf.membership.clone()),
                    LeafSlot::Todo => Err(Status::internal(format!(
                        "boolean leaf {index} was never evaluated"
                    ))),
                }
            }
            None => Err(Status::invalid_argument(
                "Boolean plan node is neither a group nor a leaf",
            )),
        }
    }

    /// The group's members under the group rule of
    /// `docs/query-api.md`. MUST children resolve cheapest first —
    /// lexical bits come from the pre-pass, then filters, then nested
    /// groups, then dense leaves — folding the running intersection.
    /// Once the intersection is empty on a MUST-only path from the
    /// root the group admits no row and neither does the root, so the
    /// remaining children resolve in short mode: their verdicts are
    /// unread by the group rule, the scorers, and provenance. SHOULD
    /// and MUST_NOT subtrees never short-circuit: off the MUST path
    /// their verdicts still count toward the minimum or the
    /// subtraction, and a returned candidate's matched list reads
    /// every leaf.
    pub(in crate::node) fn resolve_group(
        &mut self,
        group: &crate::pb::BooleanPlanGroup,
        on_must_path: bool,
        depth: usize,
    ) -> Result<crate::boolean_bits::Bits, Status> {
        use crate::boolean_bits::Bits;
        use crate::pb::boolean_plan_node::Node;
        if depth > 64 {
            return Err(Status::invalid_argument(
                "Boolean plan exceeds the 64-level recursion limit",
            ));
        }
        // Cheapest first, stable inside a class: lexical (pre-resolved),
        // filter, nested group, dense.
        let mut order: Vec<usize> = (0..group.must.len()).collect();
        let mut classes = Vec::with_capacity(order.len());
        for &c in &order {
            classes.push(match group.must[c].node.as_ref() {
                Some(Node::Group(_)) => 2,
                Some(Node::Leaf(index)) => match boolean_leaf(self.req, *index)? {
                    crate::pb::boolean_plan_leaf::Leaf::Lexical(_) => 0,
                    crate::pb::boolean_plan_leaf::Leaf::Filter(_) => 1,
                    crate::pb::boolean_plan_leaf::Leaf::Dense(_) => 3,
                },
                None => {
                    return Err(Status::invalid_argument(
                        "Boolean plan node is neither a group nor a leaf",
                    ))
                }
            });
        }
        order.sort_by_key(|&c| classes[c]);
        let mut must: Vec<Option<Bits>> = (0..group.must.len()).map(|_| None).collect();
        let mut acc: Option<Bits> = None;
        let mut short = false;
        for &c in &order {
            if short {
                continue;
            }
            let bits = self.resolve_node(&group.must[c], on_must_path, depth)?;
            match &mut acc {
                Some(acc) => acc.and_with(&bits),
                None => acc = Some(bits.clone()),
            }
            must[c] = Some(bits);
            // The restricted bits of a filter are exact wherever the
            // exact conjuncts hold, so the running intersection equals
            // the true intersection of the children resolved so far.
            if on_must_path && acc.as_ref().is_some_and(|acc| acc.count() == 0) {
                short = true;
            }
        }
        if short {
            for &c in &order {
                if must[c].is_none() {
                    self.short_node(&group.must[c], depth)?;
                }
            }
            for node in group.should.iter().chain(&group.must_not) {
                self.short_node(node, depth)?;
            }
            return Ok(Bits::empty(self.n));
        }
        let mut should = Vec::with_capacity(group.should.len());
        for node in &group.should {
            should.push(self.resolve_node(node, false, depth)?);
        }
        let mut must_not = Vec::with_capacity(group.must_not.len());
        for node in &group.must_not {
            must_not.push(self.resolve_node(node, false, depth)?);
        }
        let must: Vec<Bits> = must
            .into_iter()
            .map(|bits| bits.expect("every MUST child resolved above"))
            .collect();
        let (members, seeded_live) = crate::boolean_bits::group_members(
            &must,
            &should,
            &must_not,
            group.minimum_should_match as usize,
            self.live,
        );
        if seeded_live {
            self.seeded += 1;
        }
        Ok(members)
    }
}

/// Resolve the planned tree's membership on this shard under `guard`.
pub(crate) fn evaluate_boolean_membership(
    guard: &ShardState,
    req: &crate::pb::BooleanShardRequest,
    prune_enabled: bool,
) -> Result<EvaluatedBoolean, Status> {
    use crate::boolean_bits::Bits;
    guard.check_stats_epoch(req.expected_stats_epoch, &req.expected_stats_incarnation)?;
    crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
    for leaf in &req.leaves {
        match leaf.leaf.as_ref() {
            Some(crate::pb::boolean_plan_leaf::Leaf::Lexical(leaf)) => {
                guard.check_query_analysis("body", leaf.analysis_fingerprint)?
            }
            Some(crate::pb::boolean_plan_leaf::Leaf::Dense(leaf)) => {
                guard.vector_binding(&leaf.field)?;
            }
            _ => {}
        }
    }
    let root = req
        .root
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("Boolean plan has no root group"))?;
    let mut positive = vec![false; req.leaves.len()];
    mark_positive(req, root, true, 1, &mut positive)?;
    let bm25 = guard.bm25.as_ref();
    let docs = bm25.map_or(0, |store| store.next_doc_id() as usize);
    // The universe is the shard's rows: the lexical store's documents
    // (some without a vector) and the provider's vectors (some without a
    // document yet), whichever reaches further.
    let n = bm25
        .map_or(0, |store| store.next_doc_id() as usize)
        .max(guard.index.as_ref().map_or(0, VectorIndex::len))
        .max(
            guard
                .exact_vectors
                .as_ref()
                .map_or(0, ExactVectorStore::len),
        );
    let deleted = guard.live_docs.words();
    let mut authority = Bits::live(deleted.as_deref().map(Vec::as_slice), n);
    if let Some(view) = &req.visibility {
        let (_, allowed, _) = resolve_shard_filters(
            bm25,
            deleted.clone(),
            n,
            &[],
            &[],
            view.filter.as_ref(),
            prune_enabled,
            None,
        )?;
        if let Some(allowed) = allowed {
            authority.and_with(&allowed);
        }
        authority.and_with(&Bits::prefix(n, docs));
    }
    let mut live = Bits::prefix(n, docs);
    live.and_with(&authority);
    let mut prune = crate::segment_prune::PruneStats::default();
    // Lexical leaves resolve in the pre-pass: the cheapest membership
    // (a postings walk) and the filters' narrowing source. Dense and
    // filter leaves resolve when the group walk reaches them, so a
    // group whose MUST intersection is already empty never pays them.
    let mut leaves: Vec<LeafSlot> = Vec::with_capacity(req.leaves.len());
    for (index, leaf) in req.leaves.iter().enumerate() {
        use crate::pb::boolean_plan_leaf::Leaf as L;
        match leaf.leaf.as_ref() {
            Some(L::Filter(_)) | Some(L::Dense(_)) => leaves.push(LeafSlot::Todo),
            Some(L::Lexical(lexical)) => {
                let mut bits = Bits::empty(n);
                if !lexical.terms.is_empty() {
                    if let Some(store) = bm25 {
                        let index = store.as_index().ok_or_else(|| {
                            Status::failed_precondition("bm25 bulk build in progress; Flush first")
                        })?;
                        // A sealed segment none of the terms occur in
                        // contributes no member (docs/segment-pruning.md).
                        let (masked, stats) = match store {
                            Bm25Shard::Segmented(segmented) => {
                                let lacking = segmented.parts_lacking_terms(0, &lexical.terms);
                                let skipped = lacking.iter().filter(|&&lacks| lacks).count() as u32;
                                let stats = crate::segment_prune::PruneStats {
                                    segments_total: lacking.len() as u32,
                                    segments_skipped: if prune_enabled { skipped } else { 0 },
                                };
                                let masked = (prune_enabled && skipped > 0).then(|| {
                                    segmented.masked(
                                        lacking
                                            .iter()
                                            .map(|&lacks| !lacks)
                                            .collect::<Arc<[bool]>>(),
                                    )
                                });
                                (masked, stats)
                            }
                            _ => (None, crate::segment_prune::PruneStats::default()),
                        };
                        prune.add(stats);
                        let index: &dyn Bm25Index = match masked.as_ref() {
                            Some(masked) => masked,
                            None => index,
                        };
                        for term in &lexical.terms {
                            index.for_each_doc_tf(term, &mut |doc_id, _tf| {
                                let slot = doc_id as usize;
                                if slot < n && !guard.live_docs.is_deleted(slot) {
                                    bits.set(slot);
                                }
                            });
                        }
                    }
                }
                bits.and_with(&authority);
                leaves.push(LeafSlot::Done(EvaluatedLeaf {
                    membership: bits,
                    positive: positive[index],
                    filter_known: None,
                    filter_recheck: None,
                }));
            }
            None => {
                return Err(Status::invalid_argument(format!(
                    "Boolean plan leaf {index} has no kind"
                )))
            }
        }
    }
    let mut domains: Vec<Option<Bits>> = vec![None; req.leaves.len()];
    if boolean_filter_domain_enabled() {
        let mut bounded: Vec<Option<Bits>> = vec![None; req.leaves.len()];
        let mut unbounded = vec![false; req.leaves.len()];
        filter_leaf_domains(req, root, None, &leaves, &mut bounded, &mut unbounded, 1)?;
        for (index, domain) in bounded.into_iter().enumerate() {
            if !unbounded[index] {
                domains[index] = domain;
            }
        }
    }
    let mut eval = BooleanMembership {
        guard,
        req,
        n,
        prune_enabled,
        positive: &positive,
        domains: &domains,
        deleted: &deleted,
        authority: &authority,
        live: &live,
        prune,
        seeded: 0,
        leaves,
    };
    let members = eval.resolve_group(root, true, 1)?;
    // A leaf the tree never references still resolves: its handshakes
    // ride the response.
    for index in 0..req.leaves.len() {
        if matches!(eval.leaves[index], LeafSlot::Done(_)) {
            continue;
        }
        match boolean_leaf(req, index as u32)? {
            crate::pb::boolean_plan_leaf::Leaf::Filter(_) => eval.resolve_filter(index, false)?,
            crate::pb::boolean_plan_leaf::Leaf::Dense(_) => eval.resolve_dense(index)?,
            crate::pb::boolean_plan_leaf::Leaf::Lexical(_) => {
                return Err(Status::internal(format!(
                    "boolean lexical leaf {index} survived the pre-pass unresolved"
                )))
            }
        }
    }
    let BooleanMembership {
        mut prune,
        seeded,
        leaves,
        ..
    } = eval;
    let leaves: Vec<EvaluatedLeaf> = leaves
        .into_iter()
        .enumerate()
        .map(|(index, leaf)| match leaf {
            LeafSlot::Done(leaf) => Ok(leaf),
            LeafSlot::Todo => Err(Status::internal(format!(
                "boolean leaf {index} was never evaluated"
            ))),
        })
        .collect::<Result<_, _>>()?;
    for _ in 0..seeded {
        prune.add(crate::segment_prune::PruneStats {
            segments_total: sealed_total(bm25),
            segments_skipped: 0,
        });
    }
    Ok(EvaluatedBoolean {
        members,
        leaves,
        prune,
    })
}
