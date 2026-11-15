//! The coordinator's shard map file and the placement tree it may carry.

use serde::{Deserialize, Serialize};

/// One shard entry of a coordinator shard map (`--shard-map`): which node
/// owns which id range, plus the hash range it covers when the map was
/// produced by a hash-partitioned split (see `examples/reshard.rs`).
/// `hash_lo`/`hash_hi` are inclusive and default to the full range.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ShardMapShard {
    /// Node address (`host:port` or `http://host:port`).
    pub addr: String,
    /// Optional replica serving the same data — the target for the
    /// coordinator's hedged retries and failover. Exact search over
    /// identical data returns identical results from either copy.
    pub replica: Option<String>,
    /// The shard's global id base.
    #[serde(default)]
    pub slot_offset: u64,
    /// Inclusive hash-range bounds (`fnv1a64(vector_id)` space).
    pub hash_lo: Option<u64>,
    pub hash_hi: Option<u64>,
    /// The placement code this shard serves, when
    /// the map has a `[placement]` tree. Required on every shard then.
    pub placement: Option<u64>,
}

/// The id-to-shard authority for one cluster generation
/// (`--shard-map=<file>`). Replaces `--nodes`; bumping `generation` is how
/// a split/merge rollout distinguishes old topology from new.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ShardMap {
    /// Topology generation (0 for the implicit `--nodes` topology).
    #[serde(default)]
    pub generation: u64,
    /// One entry per shard, in fan-out order (= merge tie-break order).
    pub shards: Vec<ShardMapShard>,
    /// The placement tree: which leaf each shard
    /// serves and the predicates that route a document there.
    #[serde(default)]
    pub placement: Option<crate::placement::PlacementTreeConfig>,
    /// The index's derived columns, the
    /// same declaration every shard runs with; the coordinator
    /// evaluates them to route under the tree and to scope field
    /// permissions.
    #[serde(default)]
    pub derived: Vec<crate::derived::DerivedColumnConfig>,
}

/// Read one complete shard-map candidate. Callers validate its routing
/// geometry before publishing it; a parse failure never changes live state.
pub fn load_shard_map(path: &std::path::Path) -> Result<ShardMap, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("read shard map {}: {e}", path.display()))?;
    toml::from_str(&text).map_err(|e| format!("parse shard map {}: {e}", path.display()))
}

/// Read a placement tree for `--placement-tree`: the coordinator's shard
/// map (its `[placement]` table) or a file holding just that table.
/// A shard map without a tree, and a file that is neither, refuse by
/// name.
pub fn load_placement_tree(
    path: &std::path::Path,
) -> Result<crate::placement::PlacementTreeConfig, String> {
    let text = std::fs::read_to_string(path)
        .map_err(|e| format!("read placement tree {}: {e}", path.display()))?;
    if let Ok(map) = toml::from_str::<ShardMap>(&text) {
        return map.placement.ok_or_else(|| {
            format!(
                "placement tree {}: the shard map has no [placement] table",
                path.display()
            )
        });
    }
    toml::from_str::<crate::placement::PlacementTreeConfig>(&text).map_err(|e| {
        format!(
            "parse placement tree {}: neither a shard map with a [placement] table nor a \
             placement table on its own: {e}",
            path.display()
        )
    })
}
