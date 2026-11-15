//! Where the process sits: its role and listeners, the shards it reaches,
//! their replicas, and the control plane over them.

use std::net::SocketAddr;
use std::path::PathBuf;

use super::args::normalize_addrs;
use super::file::FileConfig;
use super::option::Options;
use super::shard_map::load_shard_map;
use super::{Role, ShardMap};

/// The role, the listeners, and the coordinator's fan-out list.
pub(super) struct Topology {
    pub(super) role: Role,
    pub(super) coord_listen: SocketAddr,
    pub(super) metrics_listen: Option<SocketAddr>,
    pub(super) shard_map_path: Option<PathBuf>,
    pub(super) shard_map: Option<ShardMap>,
    pub(super) shard_map_reload_ms: u64,
    pub(super) node_addrs: Vec<String>,
    pub(super) replica_addrs: Vec<Option<String>>,
}

pub(super) fn resolve_topology(o: Options<'_>, file: &FileConfig) -> Result<Topology, String> {
    let role = match o.text("role", file.role.as_deref()).as_deref() {
        None | Some("node") => Role::Node,
        Some("coordinator") => Role::Coordinator,
        Some("both") => Role::Both,
        Some(other) => return Err(format!("unknown role {other:?} (node|coordinator|both)")),
    };
    let coord_listen = o
        .parsed::<SocketAddr>("coord-listen", file.coord_listen.as_deref())?
        .unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], 50050)));
    let metrics_listen = o.parsed("metrics-listen", file.metrics_listen.as_deref())?;
    // A shard map replaces --nodes: it carries the same addresses plus
    // topology metadata.
    let shard_map_path = o
        .text("shard-map", file.shard_map.as_deref())
        .map(PathBuf::from);
    let shard_map = shard_map_path.as_deref().map(load_shard_map).transpose()?;
    let shard_map_reload_ms = o
        .value("shard-map-reload-ms", file.shard_map_reload_ms)?
        .unwrap_or_else(|| u64::from(shard_map_path.is_some()) * 1_000);
    let nodes_given = o.given("nodes").is_some() || file.nodes.is_some();
    if shard_map.is_some() && nodes_given {
        return Err("--shard-map replaces --nodes; pass exactly one".to_string());
    }
    let (node_addrs, replica_addrs) = match &shard_map {
        Some(map) => (
            normalize_addrs(map.shards.iter().map(|s| s.addr.clone()).collect()),
            replicas_of(map),
        ),
        None => (
            normalize_addrs(o.list("nodes", file.nodes.as_ref())),
            Vec::new(),
        ),
    };
    Ok(Topology {
        role,
        coord_listen,
        metrics_listen,
        shard_map_path,
        shard_map,
        shard_map_reload_ms,
        node_addrs,
        replica_addrs,
    })
}

/// Each shard's replica address in shard order, normalized.
pub(super) fn replicas_of(map: &ShardMap) -> Vec<Option<String>> {
    map.shards
        .iter()
        .map(|shard| {
            shard
                .replica
                .clone()
                .map(|addr| normalize_addrs(vec![addr]).remove(0))
        })
        .collect()
}

/// Replica catch-up and the durable control plane.
pub(super) struct ControlPlane {
    pub(super) replica_sync_ms: u64,
    pub(super) replica_state_path: Option<PathBuf>,
    pub(super) state_path: Option<PathBuf>,
    pub(super) reconcile_ms: u64,
    pub(super) lease_ms: u64,
    pub(super) replication_factor: usize,
    pub(super) split_rows: u64,
    pub(super) merge_rows: u64,
    pub(super) compact_segments: u32,
    pub(super) compact_tombstone_ppm: u32,
}

pub(super) fn resolve_control(
    o: Options<'_>,
    file: &FileConfig,
    topology: &Topology,
) -> Result<ControlPlane, String> {
    let has_replicas = topology.replica_addrs.iter().any(Option::is_some);
    let replica_state_path = o
        .text("replica-state", file.replica_state.as_deref())
        .map(PathBuf::from)
        .or_else(|| {
            topology.shard_map_path.as_ref().map(|path| {
                let mut name = path.as_os_str().to_owned();
                name.push(".replica-sync.toml");
                PathBuf::from(name)
            })
        });
    let state_path = o
        .text("control-state", file.control_state.as_deref())
        .map(PathBuf::from);
    Ok(ControlPlane {
        replica_sync_ms: o
            .value("replica-sync-ms", file.replica_sync_ms)?
            .unwrap_or(u64::from(has_replicas) * 1_000),
        replica_state_path,
        reconcile_ms: o
            .value("control-reconcile-ms", file.control_reconcile_ms)?
            .unwrap_or(u64::from(state_path.is_some()) * 1_000),
        state_path,
        lease_ms: o
            .value("control-lease-ms", file.control_lease_ms)?
            .unwrap_or(15_000),
        replication_factor: o
            .value(
                "control-replication-factor",
                file.control_replication_factor,
            )?
            .unwrap_or(2),
        split_rows: o
            .value("control-split-rows", file.control_split_rows)?
            .unwrap_or(25_000_000),
        merge_rows: o
            .value("control-merge-rows", file.control_merge_rows)?
            .unwrap_or(2_000_000),
        compact_segments: o
            .value("control-compact-segments", file.control_compact_segments)?
            .unwrap_or(8),
        compact_tombstone_ppm: o
            .value(
                "control-compact-tombstone-ppm",
                file.control_compact_tombstone_ppm,
            )?
            .unwrap_or(100_000),
    })
}
