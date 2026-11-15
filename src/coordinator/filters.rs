//! Compiled filters and shard capability checks.

use super::*;

/// Request filters compiled once at the coordinator into geo predicates and a
/// CEL predicate IR tree shared across lexical and vector legs.
#[derive(Debug, Clone, Default)]
pub struct RequestFilters {
    /// `geo_filters` verbatim.
    pub geo: Vec<crate::pb::GeoFilter>,
    /// The compiled tree; `None` when the request sent no CEL.
    pub tree: Option<crate::pb::FilterExpr>,
    /// The enumerate threshold the shards answer the mask under
    /// (`StartShardSearch.enumerate_max_rows`); `None` leaves each
    /// provider its own.
    pub enumerate_max_rows: Option<u32>,
    /// Optional hop-to-hop beam saturation early-stop parameters forwarded
    /// to the shards (`StartShardSearch.patience_saturation_ppm` /
    /// `patience_hops`).
    pub patience_saturation_ppm: Option<u32>,
    pub patience_hops: Option<u32>,
    /// Optional coordinator streaming settle and deadline windows in microseconds.
    pub finish_settle_us: Option<u32>,
    pub finish_deadline_us: Option<u32>,
    /// Optional shared-floor candidate pruning parameters forwarded to shards.
    pub floor_prune: Option<bool>,
    pub floor_k: Option<u32>,
    pub parent_floor_share_ppm: Option<u32>,
    pub floor_greediness_ppm: Option<u32>,
    pub floor_clamp_min: Option<u32>,
    pub include_rerank_candidate: bool,
}

/// One exact distributed membership bitmap decoded into generation-local row IDs.
/// `epochs` is parallel to the coordinator's node order on every route. A
/// pruned filter shard (or a lexical request with no analyzed terms and no
/// mandatory view) has no read claim. Query-bound reads must match the admitted
/// read set before any bitmap enters the plan.
#[derive(Debug, Clone, Default)]
pub struct MembershipSet {
    pub ids: BTreeSet<u64>,
    pub epochs: Vec<StatsClaim>,
    pub wire_bytes: u64,
    pub terms: Vec<String>,
    /// Sealed segments the shards consulted and ruled out while resolving this set.
    pub prune: crate::segment_prune::PruneStats,
    pub(crate) ranges: Vec<(u64, u64)>,
}

/// Match-set scope for percentile rounds: an explicit ID allowlist or a
/// planned Boolean tree re-evaluated per round on each shard.
#[derive(Clone, Copy)]
pub(crate) enum PercentileScope<'a> {
    Ids(Option<&'a [u64]>),
    Boolean(&'a [Option<crate::pb::BooleanShardRequest>]),
}

/// Planned `BooleanQuery` prepared for shard fan-out, pairing the wire plan
/// with compiled filter leaves for per-shard pruning and unknown-column checks.
pub(crate) struct BooleanFanoutPlan {
    pub root: crate::pb::BooleanPlanGroup,
    pub leaves: Vec<crate::pb::BooleanPlanLeaf>,
    /// Parallel to `leaves`: the compiled filters of a filter leaf.
    pub filters: Vec<Option<RequestFilters>>,
    /// Leaf indices of the root group's MUST filter leaves, in order:
    /// the AND spine the placement tree prunes shards by.
    pub root_must_filters: Vec<usize>,
    /// Leaf indices of the lexical leaves a MUST/SHOULD chain reaches.
    pub positive_lexical: Vec<usize>,
    pub depth: u32,
    pub aggregate: Option<(crate::pb::BooleanShardAggregate, CompiledAggregate)>,
}

/// The merged answer of one Boolean fan-out.
pub(crate) struct BooleanFanout {
    /// Score descending, doc id ascending, at most `depth`.
    pub candidates: Vec<crate::pb::BooleanCandidate>,
    pub prune: crate::segment_prune::PruneStats,
    pub shards_total: u32,
    pub shards_skipped: u32,
    pub aggregate: Option<crate::pb::AggregateResponse>,
}

impl RequestFilters {
    /// Compile a request's filter surface, validating both families.
    pub(crate) fn compile(geo: &[crate::pb::GeoFilter], cel: &str) -> Result<Self, Status> {
        crate::node::validate_geo_filters(geo)?;
        let tree = crate::cel::compile_filter(cel)?;
        if let Some(f) = tree.as_ref() {
            crate::filter::validate_filter(f)?;
        }
        Ok(Self {
            geo: geo.to_vec(),
            tree,
            enumerate_max_rows: None,
            patience_saturation_ppm: None,
            patience_hops: None,
            finish_settle_us: None,
            finish_deadline_us: None,
            floor_prune: None,
            floor_k: None,
            parent_floor_share_ppm: None,
            floor_greediness_ppm: None,
            floor_clamp_min: None,
            include_rerank_candidate: false,
        })
    }
}

/// Accumulates the per-shard known-column handshakes for one request:
/// a column or leaf counts as known when ANY shard resolves it, and
/// the request is refused when NO shard does. A shard that answers the
/// wrong number of flags is a protocol break, not a partial answer.
pub(super) struct FilterKnown {
    pub(super) geo: Vec<bool>,
    pub(super) tree: Vec<bool>,
    pub(super) leaves: usize,
    /// Per shard, the request-tree indices of the leaves that shard's
    /// tree keeps, when its placement leaf implied some
    /// (`ShardMask::implied`); the shard's flags come back in that
    /// shorter order and are mapped through it. `None`: the full tree.
    pub(super) kept: Vec<Option<Vec<usize>>>,
}

impl FilterKnown {
    pub(super) fn new(filters: &RequestFilters) -> Self {
        let leaves = filters.tree.as_ref().map_or(0, crate::filter::leaf_count);
        Self {
            geo: vec![false; filters.geo.len()],
            tree: vec![false; leaves],
            leaves,
            kept: Vec::new(),
        }
    }

    /// Fold one shard's answer in, flags over the full request tree.
    pub(super) fn merge(&mut self, geo: &[bool], tree: &[bool]) -> Result<(), Status> {
        self.merge_kept(geo, tree, None)
    }

    /// Fold `shard`'s answer in; its tree flags are over the tree that
    /// shard received, which [`Self::merge_pruned`] recorded.
    pub(super) fn merge_shard(
        &mut self,
        shard: usize,
        geo: &[bool],
        tree: &[bool],
    ) -> Result<(), Status> {
        let kept = self.kept.get(shard).and_then(|k| k.clone());
        self.merge_kept(geo, tree, kept.as_deref())
    }

    pub(super) fn merge_kept(
        &mut self,
        geo: &[bool],
        tree: &[bool],
        kept: Option<&[usize]>,
    ) -> Result<(), Status> {
        if geo.len() != self.geo.len() {
            return Err(Status::internal(format!(
                "shard answered {} geo-column flags for {} filters",
                geo.len(),
                self.geo.len()
            )));
        }
        for (acc, k) in self.geo.iter_mut().zip(geo) {
            *acc |= *k;
        }
        match kept {
            Some(kept) if tree.len() == kept.len() => {
                for (&index, k) in kept.iter().zip(tree) {
                    if let Some(acc) = self.tree.get_mut(index) {
                        *acc |= *k;
                    }
                }
                Ok(())
            }
            Some(kept) => Err(Status::internal(format!(
                "shard answered {} filter-leaf flags for the {} leaves it was sent (of {})",
                tree.len(),
                kept.len(),
                self.leaves
            ))),
            None => {
                if tree.len() != self.leaves {
                    return Err(Status::internal(format!(
                        "shard answered {} filter-leaf flags for {} leaves",
                        tree.len(),
                        self.leaves
                    )));
                }
                for (acc, k) in self.tree.iter_mut().zip(tree) {
                    *acc |= *k;
                }
                Ok(())
            }
        }
    }

    /// Mark filter leaves that excluded a shard via placement pruning as known.
    pub(super) fn merge_pruned(&mut self, mask: Option<&crate::placement::ShardMask>) {
        if let Some(mask) = mask {
            mark_known(&mut self.tree, &mask.known);
            self.kept = mask
                .implied
                .iter()
                .map(|dropped| {
                    (!dropped.is_empty()).then(|| {
                        (0..self.leaves)
                            .filter(|index| !dropped.contains(index))
                            .collect()
                    })
                })
                .collect();
        }
    }

    /// Refuse a name NO shard resolved.
    pub(super) fn refuse_unknown(&self, filters: &RequestFilters) -> Result<(), Status> {
        refuse_unknown_geo_columns(&filters.geo, &self.geo)?;
        refuse_unknown_filter_leaves(filters.tree.as_ref(), &self.tree)
    }
}

/// Mark filter leaves (indices in walk order) as resolved.
pub(super) fn mark_known(flags: &mut [bool], leaves: &[usize]) {
    for &leaf in leaves {
        if let Some(flag) = flags.get_mut(leaf) {
            *flag = true;
        }
    }
}
