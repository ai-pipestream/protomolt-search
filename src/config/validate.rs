//! Cross-field rules, checked once every option is resolved.

use std::net::SocketAddr;

use super::{Config, Role};

/// Refuse a configuration whose options contradict each other.
pub(super) fn validate(config: &Config) -> Result<(), String> {
    check_role(config)?;
    check_topology(config)?;
    check_control_plane(config)?;
    check_transport(config)?;
    check_collections(config)
}

fn serves_nodes(role: Role) -> bool {
    matches!(role, Role::Node | Role::Both)
}

fn coordinates(role: Role) -> bool {
    matches!(role, Role::Coordinator | Role::Both)
}

fn check_role(config: &Config) -> Result<(), String> {
    if serves_nodes(config.role) && config.shards.is_empty() {
        return Err(
            "node/both role requires at least one shard (--index/--demo-vectors or [[shards]])"
                .to_string(),
        );
    }
    if coordinates(config.role) && config.node_addrs.is_empty() && config.collections.is_empty() {
        return Err(
            "coordinator role requires --nodes or --shard-map (or `nodes` or `collections` in \
             the config file)"
                .to_string(),
        );
    }
    if config.role == Role::Node && config.clustered_turbovec.is_some() {
        return Err(
            "clustered TurboVec is a product-coordinator backend and cannot be configured on a node-only process"
                .to_string(),
        );
    }
    if config.demo_query && config.role == Role::Node {
        return Err("--demo-query requires the coordinator or both role".to_string());
    }
    if config.relay && config.role != Role::Coordinator {
        return Err(
            "--relay serves the node-facing surface over a coordinator's shard set and \
             takes --role=coordinator only"
                .to_string(),
        );
    }
    if config.relay && !config.collections.is_empty() {
        return Err(
            "--relay serves one unnamed collection on a dedicated endpoint; named \
             [[collections]] are not multiplexed through a relay"
                .to_string(),
        );
    }
    Ok(())
}

fn check_topology(config: &Config) -> Result<(), String> {
    if config.shard_map_reload_ms > 0 && config.shard_map_path.is_none() {
        return Err("--shard-map-reload-ms requires --shard-map".to_string());
    }
    if config.placement_leaf.is_some() && config.placement_column.is_none() {
        return Err(
            "--placement-leaf needs --placement-column, the column that holds the code".to_string(),
        );
    }
    if config.rerank_parallel > crate::node::MAX_RERANK_PARALLEL {
        return Err(format!(
            "rerank parallel must be <= {}, got {}",
            crate::node::MAX_RERANK_PARALLEL,
            config.rerank_parallel
        ));
    }
    if config.replica_sync_ms > 0 && config.replica_state_path.is_none() {
        return Err("automatic replica sync requires --replica-state or --shard-map".to_string());
    }
    Ok(())
}

fn check_control_plane(config: &Config) -> Result<(), String> {
    if config.control_state_path.is_none() {
        return Ok(());
    }
    if config.shard_map.is_none() {
        return Err("--control-state requires a generation-stamped --shard-map".to_string());
    }
    if !coordinates(config.role)
        || config.control_replication_factor == 0
        || config.control_lease_ms < 1_000
        || config.control_split_rows == 0
        || config.control_merge_rows == 0
        || config.control_compact_segments == 0
        || config.control_compact_tombstone_ppm == 0
        || config.control_compact_tombstone_ppm > 1_000_000
    {
        return Err("control plane requires coordinator/both role, positive thresholds and replication factor, lease >= 1000ms, and tombstone ppm <= 1000000".to_string());
    }
    Ok(())
}

/// TLS or an explicit plaintext choice on every listener that is not loopback.
fn check_transport(config: &Config) -> Result<(), String> {
    if config.tls.is_some() && config.allow_plaintext {
        return Err(
            "--allow-plaintext contradicts --tls-cert: once TLS is set, plaintext is refused"
                .to_string(),
        );
    }
    if config.tls.is_none() && !config.allow_plaintext {
        let mut listeners: Vec<SocketAddr> = config.shards.iter().map(|s| s.listen).collect();
        if coordinates(config.role) {
            listeners.push(config.coord_listen);
        }
        if let Some(addr) = listeners
            .iter()
            .find(|addr| !crate::security::is_loopback(addr))
        {
            return Err(format!(
                "listener {addr} is not loopback and no TLS is configured; pass --tls-cert and \
                 --tls-key, or --allow-plaintext to serve plaintext gRPC there on purpose"
            ));
        }
    }
    if serves_nodes(config.role)
        && config
            .tls
            .as_ref()
            .is_some_and(|t| t.client_ca_pem.is_none())
    {
        return Err(
            "node listeners run mTLS: --tls-client-ca (the cluster CA) is required with --tls-cert"
                .to_string(),
        );
    }
    if coordinates(config.role) && config.tls.is_some() && config.client_tls.is_none() {
        return Err(
            "a TLS coordinator needs --tls-ca (and --tls-client-cert / --tls-client-key, its \
             membership) to reach its nodes"
                .to_string(),
        );
    }
    Ok(())
}

fn check_collections(config: &Config) -> Result<(), String> {
    let declared = || -> Vec<&str> { config.collections.iter().map(|c| c.name.as_str()).collect() };
    if let Some(name) = &config.default_collection {
        if !config.collections.iter().any(|c| &c.name == name) {
            return Err(format!(
                "default_collection {name:?} is not a declared collection ({:?})",
                declared()
            ));
        }
    }
    if config.collections.is_empty() || !serves_nodes(config.role) {
        return Ok(());
    }
    for (i, shard) in config.shards.iter().enumerate() {
        if !config
            .collections
            .iter()
            .any(|c| c.name == shard.collection)
        {
            return Err(format!(
                "shards[{i}] serves collection {:?}, which this process's `collections` \
                 does not declare ({:?})",
                shard.collection,
                declared()
            ));
        }
    }
    Ok(())
}
