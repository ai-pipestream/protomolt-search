//! Boolean query tree evaluation, filter domain narrowing, and short-circuiting.

use crate::node::*;
mod domains;
mod prepass;
mod resolve;

pub(in crate::node) use domains::filter_leaf_domains;
use prepass::{check_leaf_bindings, filter_domains, shard_authority, universe, LexicalPrepass};
use resolve::BooleanMembership;

// Shard-side recursive Boolean evaluation: the planned tree arrives, every
// leaf resolves over this shard's bitmaps, the set algebra runs on the words,
// the members are scored for each scoring leaf, and the best `depth` go back.
// No membership crosses the wire.

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
    /// The root's members under the Boolean group rule.
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
/// `PIPESTREAM_SEARCH_BOOLEAN_FILTER_FULL_SCAN` forces the whole-shard
/// column scan — the A/B baseline. The answer is the same either way; only the cost moves.
/// The MUST short-circuit is orthogonal: an empty group skips its
/// remaining siblings in both modes.
fn boolean_filter_domain_enabled() -> bool {
    std::env::var_os("PIPESTREAM_SEARCH_BOOLEAN_FILTER_FULL_SCAN").is_none()
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
/// Resolve the planned tree's membership on this shard under `guard`.
pub(crate) fn evaluate_boolean_membership(
    guard: &ShardState,
    req: &crate::pb::BooleanShardRequest,
    prune_enabled: bool,
) -> Result<EvaluatedBoolean, Status> {
    use crate::boolean_bits::Bits;
    guard.check_stats_epoch(req.expected_stats_epoch, &req.expected_stats_incarnation)?;
    crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
    check_leaf_bindings(guard, req)?;
    let root = req
        .root
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("Boolean plan has no root group"))?;
    let mut positive = vec![false; req.leaves.len()];
    mark_positive(req, root, true, 1, &mut positive)?;
    let docs = guard
        .bm25
        .as_ref()
        .map_or(0, |store| store.next_doc_id() as usize);
    let n = universe(guard);
    let deleted = guard.live_docs.words();
    let authority = shard_authority(guard, req, &deleted, (n, docs), prune_enabled)?;
    let mut live = Bits::prefix(n, docs);
    live.and_with(&authority);
    let mut prune = crate::segment_prune::PruneStats::default();
    let leaves = LexicalPrepass {
        guard,
        n,
        prune_enabled,
        authority: &authority,
        positive: &positive,
    }
    .resolve(req, &mut prune)?;
    let domains = filter_domains(req, root, &leaves)?;
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
    eval.resolve_unreferenced()?;
    eval.finish(members)
}
