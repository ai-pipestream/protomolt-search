//! The process role, its listeners, the coordinator's fan-out set, and the
//! durable control plane over that set.

use std::net::SocketAddr;
use std::path::{Path, PathBuf};

use super::args::normalize_addrs;
use super::file::FileConfig;
use super::option::{split_list, Options};
use super::shard_map::{load_shard_map, ShardMap};
use super::Role;

/// Role, listeners, and the shard set a coordinator fans out to.
pub(super) struct Topology {
    pub(super) role: Role,
    pub(super) coord_listen: SocketAddr,
    pub(super) metrics_listen: Option<SocketAddr>,
    pub(super) node_addrs: Vec<String>,
    pub(super) replica_addrs: Vec<Option<String>>,
    pub(super) shard_map: Option<ShardMap>,
    pub(super) shard_map_path: Option<PathBuf>,
    pub(super) shard_map_reload_ms: u64,
}

/// Replica catch-up and the durable control plane.
pub(super) struct Control {
    pub(super) replica_sync_ms: u64,
    pub(super) replica_state_path: Option<PathBuf>,
    pub(super) control_state_path: Option<PathBuf>,
    pub(super) control_reconcile_ms: u64,
    pub(super) control_lease_ms: u64,
    pub(super) control_replication_factor: usize,
    pub(super) control_split_rows: u64,
    pub(super) control_merge_rows: u64,
    pub(super) control_compact_segments: u32,
    pub(super) control_compact_tombstone_ppm: u32,
}

fn parse_role(text: &str) -> Result<Role, String> {
    match text {
        "node" => Ok(Role::Node),
        "coordinator" => Ok(Role::Coordinator),
        "both" => Ok(Role::Both),
        other => Err(format!("unknown role {other:?} (node|coordinator|both)")),
    }
}

/// The node addresses a shard map lists, in fan-out order.
pub(super) fn map_node_addrs(map: &ShardMap) -> Vec<String> {
    normalize_addrs(map.shards.iter().map(|s| s.addr.clone()).collect())
}

/// One optional replica per shard map entry, aligned with its node.
pub(super) fn map_replica_addrs(map: &ShardMap) -> Vec<Option<String>> {
    map.shards
        .iter()
        .map(|s| s.replica.clone().map(super::args::normalize_addr))
        .collect()
}

/// Role, listeners, and the fan-out set. A shard map replaces `--nodes`: it
/// carries the same addresses plus topology metadata.
pub(super) fn resolve_topology(o: Options, file: &FileConfig) -> Result<Topology, String> {
    let role = parse_role(
        &o.text("role", file.role.as_deref())
            .unwrap_or_else(|| "node".into()),
    )?;
    let coord_listen = o
        .parsed::<SocketAddr>("coord-listen", file.coord_listen.as_deref())?
        .unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], 50050)));
    let metrics_listen =
        o.parsed::<SocketAddr>("metrics-listen", file.metrics_listen.as_deref())?;
    let shard_map_path = o
        .text("shard-map", file.shard_map.as_deref())
        .map(PathBuf::from);
    let shard_map = shard_map_path.as_deref().map(load_shard_map).transpose()?;
    let reload_default = u64::from(shard_map_path.is_some()) * 1_000;
    let shard_map_reload_ms = o.value_or(
        "shard-map-reload-ms",
        file.shard_map_reload_ms,
        reload_default,
    )?;
    if shard_map_reload_ms > 0 && shard_map_path.is_none() {
        return Err("--shard-map-reload-ms requires --shard-map".to_string());
    }
    let cli_nodes = o.given("nodes");
    if shard_map.is_some() && (cli_nodes.is_some() || file.nodes.is_some()) {
        return Err("--shard-map replaces --nodes; pass exactly one".to_string());
    }
    let (node_addrs, replica_addrs) = match &shard_map {
        Some(map) => (map_node_addrs(map), map_replica_addrs(map)),
        None => {
            let nodes = cli_nodes.map_or_else(
                || file.nodes.clone().unwrap_or_default(),
                |s| split_list(&s),
            );
            (normalize_addrs(nodes), Vec::new())
        }
    };
    Ok(Topology {
        role,
        coord_listen,
        metrics_listen,
        node_addrs,
        replica_addrs,
        shard_map,
        shard_map_path,
        shard_map_reload_ms,
    })
}

/// The replica cursor file defaults to one beside the shard map.
fn default_replica_state(shard_map_path: Option<&Path>) -> Option<PathBuf> {
    shard_map_path.map(|path| {
        let mut name = path.as_os_str().to_owned();
        name.push(".replica-sync.toml");
        PathBuf::from(name)
    })
}

/// A control knob narrowed to its field's type, refused by name when it does not fit.
fn narrow<T: TryFrom<u64>>(flag: &str, value: u64) -> Result<T, String> {
    T::try_from(value).map_err(|_| format!("--{flag} {value} does not fit its type"))
}

/// Replica catch-up and the control plane's policy knobs.
pub(super) fn resolve_control(
    o: Options,
    file: &FileConfig,
    topology: &Topology,
) -> Result<Control, String> {
    let any_replica = topology.replica_addrs.iter().any(Option::is_some);
    let replica_sync_ms = o.value_or(
        "replica-sync-ms",
        file.replica_sync_ms,
        u64::from(any_replica) * 1_000,
    )?;
    let replica_state_path = o
        .text("replica-state", file.replica_state.as_deref())
        .map(PathBuf::from)
        .or_else(|| default_replica_state(topology.shard_map_path.as_deref()));
    if replica_sync_ms > 0 && replica_state_path.is_none() {
        return Err("automatic replica sync requires --replica-state or --shard-map".to_string());
    }
    let control_state_path = o
        .text("control-state", file.control_state.as_deref())
        .map(PathBuf::from);
    if control_state_path.is_some() && topology.shard_map.is_none() {
        return Err("--control-state requires a generation-stamped --shard-map".to_string());
    }
    let reconcile_default = u64::from(control_state_path.is_some()) * 1_000;
    let factor = file.control_replication_factor.map(|v| v as u64);
    let segments = file.control_compact_segments.map(u64::from);
    let ppm = file.control_compact_tombstone_ppm.map(u64::from);
    Ok(Control {
        replica_sync_ms,
        replica_state_path,
        control_state_path,
        control_reconcile_ms: o.value_or(
            "control-reconcile-ms",
            file.control_reconcile_ms,
            reconcile_default,
        )?,
        control_lease_ms: o.value_or("control-lease-ms", file.control_lease_ms, 15_000)?,
        control_replication_factor: narrow(
            "control-replication-factor",
            o.value_or("control-replication-factor", factor, 2)?,
        )?,
        control_split_rows: o.value_or(
            "control-split-rows",
            file.control_split_rows,
            25_000_000,
        )?,
        control_merge_rows: o.value_or("control-merge-rows", file.control_merge_rows, 2_000_000)?,
        control_compact_segments: narrow(
            "control-compact-segments",
            o.value_or("control-compact-segments", segments, 8)?,
        )?,
        control_compact_tombstone_ppm: narrow(
            "control-compact-tombstone-ppm",
            o.value_or("control-compact-tombstone-ppm", ppm, 100_000)?,
        )?,
    })
}
