//! Validated routing maps and topology cutover barriers.

use super::*;

/// One immutable product-shard route in a topology generation.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TopologyRoute {
    pub addr: String,
    pub replica: Option<String>,
    /// Inclusive stable-key hash range. Every route must either provide a
    /// range or omit one; mixed/ragged maps are refused. Under a
    /// placement tree the ranges tile the hash space per leaf.
    pub hash_range: Option<(u64, u64)>,
    /// Placement leaf code served by this shard (required when the topology has
    /// a placement tree and refused otherwise).
    pub placement: Option<i64>,
}

pub(super) struct CoordinatorTopology {
    pub(super) generation: u64,
    pub(super) routes: Vec<TopologyRoute>,
    pub(super) stats_cache: Arc<crate::stats_cache::StatsCache>,
    /// The validated placement tree of this generation, if any.
    pub(super) placement: Option<Arc<crate::placement::Placement>>,
}

/// One generation's routing inputs: generation, hash range per shard,
/// placement code per shard, and whether the map has a tree.
pub(super) type RoutingView = (u64, Vec<Option<(u64, u64)>>, Vec<Option<i64>>, bool);

/// Inclusive hash ranges `(lo, hi, shard)` must tile `0..=u64::MAX`
/// with no gap and no overlap.
pub(super) fn check_hash_tiling(
    mut ranges: Vec<(u64, u64, usize)>,
    scope: &str,
) -> Result<(), String> {
    ranges.sort_by_key(|range| range.0);
    let mut expected = 0u64;
    for (position, (lo, hi, shard)) in ranges.iter().copied().enumerate() {
        if lo != expected {
            return Err(format!(
                "topology hash space{scope} has a gap or overlap before shard {shard}: expected {expected}, got {lo}"
            ));
        }
        if position + 1 == ranges.len() {
            if hi != u64::MAX {
                return Err(format!(
                    "topology hash space{scope} ends at {hi}, not {}",
                    u64::MAX
                ));
            }
        } else {
            expected = hi.checked_add(1).ok_or_else(|| {
                format!("topology shard {shard} reaches the hash-space end too early")
            })?;
        }
    }
    Ok(())
}

pub(super) fn build_topology(
    generation: u64,
    routes: Vec<TopologyRoute>,
    tree: Option<&crate::placement::PlacementTreeConfig>,
) -> Result<CoordinatorTopology, String> {
    if routes.is_empty() {
        return Err("topology requires at least one primary shard".to_string());
    }
    let placement = match tree {
        Some(tree) => Some(Arc::new(crate::placement::Placement::validate(tree)?)),
        None => None,
    };
    match placement.as_ref() {
        None => {
            if let Some(shard) = routes.iter().position(|route| route.placement.is_some()) {
                return Err(format!(
                    "topology shard {shard} carries a placement code but the map has no \
                     placement tree"
                ));
            }
        }
        Some(placement) => {
            let mut served = vec![0usize; placement.leaves().len()];
            for (shard, route) in routes.iter().enumerate() {
                let Some(code) = route.placement else {
                    return Err(format!(
                        "topology shard {shard} has no placement code; the map has a placement \
                         tree, so every shard names the leaf it serves"
                    ));
                };
                let Some(index) = placement.leaves().iter().position(|leaf| leaf.code == code)
                else {
                    return Err(format!(
                        "topology shard {shard} names placement code {code}, which is no leaf \
                         of the tree"
                    ));
                };
                served[index] += 1;
            }
            for (leaf, count) in placement.leaves().iter().zip(&served) {
                if *count == 0 {
                    return Err(format!(
                        "placement leaf {:?} (code {}) has no shard",
                        leaf.name, leaf.code
                    ));
                }
            }
        }
    }
    let mut addresses = std::collections::HashSet::new();
    for (shard, route) in routes.iter().enumerate() {
        if route.addr.is_empty() {
            return Err(format!(
                "topology shard {shard} has an empty primary address"
            ));
        }
        if !addresses.insert(route.addr.as_str()) {
            return Err(format!("duplicate topology endpoint {:?}", route.addr));
        }
        if let Some(replica) = route.replica.as_deref() {
            if replica.is_empty() {
                return Err(format!(
                    "topology shard {shard} has an empty replica address"
                ));
            }
            if !addresses.insert(replica) {
                return Err(format!("duplicate topology endpoint {replica:?}"));
            }
        }
        if let Some((lo, hi)) = route.hash_range {
            if lo > hi {
                return Err(format!(
                    "topology shard {shard} has inverted hash range {lo}..={hi}"
                ));
            }
        }
    }
    let ranged = routes
        .iter()
        .filter(|route| route.hash_range.is_some())
        .count();
    if ranged != 0 && ranged != routes.len() {
        return Err("topology must provide hash ranges for every shard or none".to_string());
    }
    if ranged != 0 {
        let ranges = |code: Option<i64>| -> Vec<(u64, u64, usize)> {
            routes
                .iter()
                .enumerate()
                .filter(|(_, route)| code.is_none() || route.placement == code)
                .map(|(shard, route)| {
                    let (lo, hi) = route.hash_range.expect("all routes ranged");
                    (lo, hi, shard)
                })
                .collect()
        };
        match placement.as_ref() {
            None => check_hash_tiling(ranges(None), "")?,
            Some(placement) => {
                for leaf in placement.leaves() {
                    check_hash_tiling(
                        ranges(Some(leaf.code)),
                        &format!(" of placement leaf {:?}", leaf.name),
                    )?;
                }
            }
        }
    }
    Ok(CoordinatorTopology {
        generation,
        stats_cache: Arc::new(crate::stats_cache::StatsCache::new(routes.len())),
        routes,
        placement,
    })
}

/// Stable FNV-1a over opaque product identity bytes. Unlike the historical
/// WAL bucket hash, this never depends on generation-local numeric slots.
pub fn stable_routing_hash(key: &[u8]) -> u64 {
    crate::values::fnv1a64_bytes(key)
}

impl CoordinatorServiceImpl {
    /// Enable atomic topology replacement. The current fields become the
    /// configured generation's immutable request snapshot.
    pub fn with_hot_topology(self, hash_ranges: Vec<Option<(u64, u64)>>) -> Result<Self, String> {
        self.with_hot_topology_placed(hash_ranges, None)
    }

    /// [`Self::with_hot_topology`] under a placement tree, pairing one placement
    /// code per shard parallel to `node_addrs`.
    pub fn with_hot_topology_placed(
        mut self,
        hash_ranges: Vec<Option<(u64, u64)>>,
        placement: Option<(crate::placement::PlacementTreeConfig, Vec<Option<i64>>)>,
    ) -> Result<Self, String> {
        if hash_ranges.len() != self.node_addrs.len() {
            return Err(format!(
                "topology has {} shard addresses but {} hash ranges",
                self.node_addrs.len(),
                hash_ranges.len()
            ));
        }
        let (tree, codes) = match placement {
            Some((tree, codes)) => {
                if codes.len() != self.node_addrs.len() {
                    return Err(format!(
                        "topology has {} shard addresses but {} placement codes",
                        self.node_addrs.len(),
                        codes.len()
                    ));
                }
                (Some(tree), codes)
            }
            None => (None, vec![None; self.node_addrs.len()]),
        };
        let routes = self
            .node_addrs
            .iter()
            .enumerate()
            .map(|(shard, addr)| TopologyRoute {
                addr: addr.clone(),
                replica: self.replica_addrs.get(shard).cloned().flatten(),
                hash_range: hash_ranges.get(shard).copied().flatten(),
                placement: codes.get(shard).copied().flatten(),
            })
            .collect();
        let topology = build_topology(self.topology_generation, routes, tree.as_ref())?;
        self.hash_ranges = hash_ranges;
        self.placement_codes = codes;
        self.placement = topology.placement.clone();
        self.live_topology = Some(Arc::new(RwLock::new(Arc::new(topology))));
        self.topology_watch.send_replace(self.topology_generation);
        Ok(self)
    }

    /// Atomically publish a strictly newer topology generation. Existing
    /// requests retain their prior `Arc`; later requests snapshot this map.
    pub fn reload_topology(
        &self,
        generation: u64,
        routes: Vec<TopologyRoute>,
        placement: Option<&crate::placement::PlacementTreeConfig>,
    ) -> Result<(), String> {
        if self.cutover_pending.load(AtomicOrdering::Acquire) {
            return Err(
                "topology cutover has frozen writes; publish or abort it first".to_string(),
            );
        }
        self.publish_topology_inner(generation, routes, placement)
    }

    pub(super) fn publish_topology_inner(
        &self,
        generation: u64,
        routes: Vec<TopologyRoute>,
        placement: Option<&crate::placement::PlacementTreeConfig>,
    ) -> Result<(), String> {
        let authority = self
            .live_topology
            .as_ref()
            .ok_or_else(|| "hot topology is not enabled".to_string())?;
        let replacement = Arc::new(build_topology(generation, routes, placement)?);
        let mut current = authority
            .write()
            .map_err(|_| "topology authority lock is poisoned".to_string())?;
        if generation <= current.generation {
            return Err(format!(
                "topology generation must increase: current {}, proposed {generation}",
                current.generation
            ));
        }
        *current = replacement;
        drop(current);
        self.topology_watch.send_replace(generation);
        Ok(())
    }

    /// A receiver that changes whenever a new topology generation is
    /// published on this coordinator; its value is that generation.
    pub fn topology_changes(&self) -> watch::Receiver<u64> {
        self.topology_watch.subscribe()
    }

    /// A frozen clone of this coordinator over exactly `routes`: the
    /// links, keys, limits, and TLS of this one, the shard set of the
    /// caller's map snapshot, no live topology. A relay fans out through
    /// this so the children it talks to are the children its pinned map
    /// revision names, whatever source that map came from.
    pub(crate) fn frozen_over(
        &self,
        generation: u64,
        routes: &[TopologyRoute],
        placement: Option<Arc<crate::placement::Placement>>,
    ) -> Self {
        let mut frozen = self.clone();
        frozen.node_addrs = routes.iter().map(|route| route.addr.clone()).collect();
        frozen.replica_addrs = routes.iter().map(|route| route.replica.clone()).collect();
        frozen.hash_ranges = routes.iter().map(|route| route.hash_range).collect();
        frozen.placement_codes = routes.iter().map(|route| route.placement).collect();
        frozen.topology_generation = generation;
        frozen.placement = placement;
        frozen.stats_cache = Arc::new(crate::stats_cache::StatsCache::new(routes.len()));
        frozen.live_topology = None;
        frozen
    }

    pub fn current_topology_generation(&self) -> u64 {
        self.live_topology
            .as_ref()
            .and_then(|authority| authority.read().ok().map(|topology| topology.generation))
            .unwrap_or(self.topology_generation)
    }

    /// Current routes for control-plane workers. The returned vector is one
    /// immutable generation snapshot; callers never borrow the live lock.
    pub fn current_topology_routes(&self) -> Vec<TopologyRoute> {
        if let Some(authority) = &self.live_topology {
            return authority
                .read()
                .expect("topology authority lock poisoned")
                .routes
                .clone();
        }
        self.node_addrs
            .iter()
            .enumerate()
            .map(|(shard, addr)| TopologyRoute {
                addr: addr.clone(),
                replica: self.replica_addrs.get(shard).cloned().flatten(),
                hash_range: self.hash_ranges.get(shard).copied().flatten(),
                placement: self.placement_codes.get(shard).copied().flatten(),
            })
            .collect()
    }

    /// The placement tree of the current generation, if the map has one.
    pub fn current_placement(&self) -> Option<Arc<crate::placement::Placement>> {
        if let Some(authority) = &self.live_topology {
            return authority
                .read()
                .expect("topology authority lock poisoned")
                .placement
                .clone();
        }
        self.placement.clone()
    }

    /// Resolve an opaque stable product identity under one immutable map.
    /// Returns `(generation, shard_index)` for ingest stamping.
    pub fn route_stable_key(&self, key: &[u8]) -> Result<(u64, usize), String> {
        self.route_stable_key_in(key, None)
    }

    /// [`Self::route_stable_key`] inside one placement leaf: under a
    /// tree the hash ranges tile the space per leaf, so the leaf's code
    /// is part of the route. A placed topology refuses a leafless route
    /// and an unplaced one refuses a leaf.
    pub fn route_stable_key_in(
        &self,
        key: &[u8],
        leaf: Option<i64>,
    ) -> Result<(u64, usize), String> {
        if key.is_empty() {
            return Err("stable routing key is empty".to_string());
        }
        let hash = stable_routing_hash(key);
        let (generation, ranges, codes, placed): RoutingView =
            if let Some(authority) = &self.live_topology {
                let topology = authority
                    .read()
                    .map_err(|_| "topology authority lock is poisoned".to_string())?
                    .clone();
                (
                    topology.generation,
                    topology
                        .routes
                        .iter()
                        .map(|route| route.hash_range)
                        .collect(),
                    topology
                        .routes
                        .iter()
                        .map(|route| route.placement)
                        .collect(),
                    topology.placement.is_some(),
                )
            } else {
                (
                    self.topology_generation,
                    self.hash_ranges.clone(),
                    self.placement_codes.clone(),
                    self.placement.is_some(),
                )
            };
        if ranges.is_empty() || ranges.iter().any(Option::is_none) {
            return Err("topology has no complete stable hash ranges".to_string());
        }
        match (placed, leaf) {
            (true, None) => {
                return Err(
                    "the topology has a placement tree; a stable key routes inside a leaf"
                        .to_string(),
                )
            }
            (false, Some(code)) => {
                return Err(format!(
                    "placement code {code} given, but the topology has no placement tree"
                ))
            }
            _ => {}
        }
        let shard = ranges
            .iter()
            .enumerate()
            .position(|(shard, range)| {
                leaf.is_none_or(|code| codes.get(shard).copied().flatten() == Some(code))
                    && range.is_some_and(|(lo, hi)| hash >= lo && hash <= hi)
            })
            .ok_or_else(|| match leaf {
                Some(code) => {
                    format!("stable hash {hash} is not covered inside placement leaf {code}")
                }
                None => format!("stable hash {hash} is not covered by the topology"),
            })?;
        Ok((generation, shard))
    }

    /// Compute the shards ruled out by `filter` under this generation's
    /// placement tree, or `None` when unplaced, unfiltered, or `shard_pruning` is off.
    pub(crate) fn shard_mask(
        &self,
        filter: Option<&crate::pb::FilterExpr>,
    ) -> Option<crate::placement::ShardMask> {
        let placement = self.placement.as_ref()?;
        let filter = filter?;
        if !self.knobs.shard_pruning() {
            return None;
        }
        Some(crate::placement::ShardMask::compute(
            placement,
            &self.placement_codes,
            filter,
        ))
    }

    /// Return `shard`'s filter tree with clauses implied by its placement leaf
    /// removed, or the unmodified tree when nothing is implied.
    pub(crate) fn shard_filter_tree(
        filters: &RequestFilters,
        mask: Option<&crate::placement::ShardMask>,
        shard: usize,
    ) -> Option<crate::pb::FilterExpr> {
        match (filters.tree.as_ref(), mask) {
            (Some(tree), Some(mask)) => mask.filter_for(shard, tree),
            (tree, _) => tree.cloned(),
        }
    }

    /// [`Self::shard_filter_tree`] for every shard of the topology, for
    /// a context that outlives the mask (a stream and its hedge leg must
    /// send one shard the identical tree).
    pub(super) fn shard_filter_trees(
        &self,
        filters: &RequestFilters,
        mask: Option<&crate::placement::ShardMask>,
    ) -> Vec<Option<crate::pb::FilterExpr>> {
        (0..self.node_addrs.len())
            .map(|shard| Self::shard_filter_tree(filters, mask, shard))
            .collect()
    }

    /// `(shards in the topology, shards the filter skips)` for a profile.
    pub fn shard_prune_counts(&self, filters: &RequestFilters) -> (u32, u32) {
        let total = self.node_addrs.len() as u32;
        let skipped = self
            .shard_mask(filters.tree.as_ref())
            .map_or(0, |mask| mask.skipped_count());
        (total, skipped)
    }

    /// The known-column accumulator for one fan-out, with the leaves a
    /// mask already resolved marked.
    pub(super) fn filter_known(
        filters: &RequestFilters,
        mask: Option<&crate::placement::ShardMask>,
    ) -> FilterKnown {
        let mut known = FilterKnown::new(filters);
        known.merge_pruned(mask);
        known
    }

    pub(crate) fn request_snapshot(&self) -> Option<Self> {
        let authority = self.live_topology.as_ref()?;
        let topology = authority
            .read()
            .expect("topology authority lock poisoned")
            .clone();
        let mut frozen = self.clone();
        frozen.node_addrs = topology
            .routes
            .iter()
            .map(|route| route.addr.clone())
            .collect();
        frozen.replica_addrs = topology
            .routes
            .iter()
            .map(|route| route.replica.clone())
            .collect();
        frozen.topology_generation = topology.generation;
        frozen.hash_ranges = topology
            .routes
            .iter()
            .map(|route| route.hash_range)
            .collect();
        frozen.placement_codes = topology
            .routes
            .iter()
            .map(|route| route.placement)
            .collect();
        frozen.placement = topology.placement.clone();
        frozen.stats_cache = topology.stats_cache.clone();
        frozen.live_topology = None;
        Some(frozen)
    }

    pub(super) fn require_topology_generation(&self, requested: u64) -> Result<(), Status> {
        if requested != 0 && requested != self.topology_generation {
            return Err(Status::failed_precondition(format!(
                "request requires topology generation {requested}, but this request was assigned generation {}",
                self.topology_generation
            )));
        }
        Ok(())
    }
}
