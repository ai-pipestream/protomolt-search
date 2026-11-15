//! The rules that span option sections, checked once on the assembled
//! configuration.

use super::{Config, Role};

fn serves_nodes(config: &Config) -> bool {
    matches!(config.role, Role::Node | Role::Both)
}

fn coordinates(config: &Config) -> bool {
    matches!(config.role, Role::Coordinator | Role::Both)
}

/// The durable control plane runs on a coordinator with workable thresholds.
fn check_control(config: &Config) -> Result<(), String> {
    if config.control_state_path.is_none() {
        return Ok(());
    }
    let ppm = config.control_compact_tombstone_ppm;
    if !coordinates(config)
        || config.control_replication_factor == 0
        || config.control_lease_ms < 1_000
        || config.control_split_rows == 0
        || config.control_merge_rows == 0
        || config.control_compact_segments == 0
        || ppm == 0
        || ppm > 1_000_000
    {
        return Err(
            "control plane requires coordinator/both role, positive thresholds and replication \
             factor, lease >= 1000ms, and tombstone ppm <= 1000000"
                .to_string(),
        );
    }
    Ok(())
}

/// Each role has something to serve, and role-bound options sit on their role.
fn check_roles(config: &Config) -> Result<(), String> {
    if serves_nodes(config) && config.shards.is_empty() {
        return Err(
            "node/both role requires at least one shard (--index/--demo-vectors or [[shards]])"
                .to_string(),
        );
    }
    if coordinates(config) && config.node_addrs.is_empty() && config.collections.is_empty() {
        return Err(
            "coordinator role requires --nodes or --shard-map (or `nodes` or `collections` in \
             the config file)"
                .to_string(),
        );
    }
    if config.role == Role::Node && config.clustered_turbovec.is_some() {
        return Err(
            "clustered TurboVec is a product-coordinator backend and cannot be configured on a \
             node-only process"
                .to_string(),
        );
    }
    if config.demo_query && config.role == Role::Node {
        return Err("--demo-query requires the coordinator or both role".to_string());
    }
    Ok(())
}

/// A relay presents one unnamed collection to a parent coordinator.
fn check_relay(config: &Config) -> Result<(), String> {
    if !config.relay {
        return Ok(());
    }
    if config.role != Role::Coordinator {
        return Err(
            "--relay serves the node-facing surface over a coordinator's shard set and takes \
             --role=coordinator only"
                .to_string(),
        );
    }
    if !config.collections.is_empty() {
        return Err(
            "--relay serves one unnamed collection on a dedicated endpoint; named \
             [[collections]] are not multiplexed through a relay"
                .to_string(),
        );
    }
    Ok(())
}

/// Plaintext only on loopback or on purpose; TLS nodes run mTLS; a TLS
/// coordinator can reach its nodes.
fn check_transport(config: &Config) -> Result<(), String> {
    if config.tls.is_some() && config.allow_plaintext {
        return Err(
            "--allow-plaintext contradicts --tls-cert: once TLS is set, plaintext is refused"
                .to_string(),
        );
    }
    if config.tls.is_none() && !config.allow_plaintext {
        let coordinator = coordinates(config).then_some(config.coord_listen);
        let mut listeners = config.shards.iter().map(|s| s.listen).chain(coordinator);
        if let Some(addr) = listeners.find(|addr| !crate::security::is_loopback(addr)) {
            return Err(format!(
                "listener {addr} is not loopback and no TLS is configured; pass --tls-cert and \
                 --tls-key, or --allow-plaintext to serve plaintext gRPC there on purpose"
            ));
        }
    }
    let tls = config.tls.as_ref();
    if serves_nodes(config) && tls.is_some_and(|t| t.client_ca_pem.is_none()) {
        return Err(
            "node listeners run mTLS: --tls-client-ca (the cluster CA) is required with --tls-cert"
                .to_string(),
        );
    }
    if coordinates(config) && tls.is_some() && config.client_tls.is_none() {
        return Err(
            "a TLS coordinator needs --tls-ca (and --tls-client-cert / --tls-client-key, its \
             membership) to reach its nodes"
                .to_string(),
        );
    }
    Ok(())
}

/// The default collection and every shard's collection are declared ones.
fn check_collection_names(config: &Config) -> Result<(), String> {
    let declared = || {
        config
            .collections
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>()
    };
    if let Some(name) = &config.default_collection {
        if !config.collections.iter().any(|c| &c.name == name) {
            return Err(format!(
                "default_collection {name:?} is not a declared collection ({:?})",
                declared()
            ));
        }
    }
    if config.collections.is_empty() || !serves_nodes(config) {
        return Ok(());
    }
    for (i, shard) in config.shards.iter().enumerate() {
        if !config
            .collections
            .iter()
            .any(|c| c.name == shard.collection)
        {
            return Err(format!(
                "shards[{i}] serves collection {:?}, which this process's `collections` does \
                 not declare ({:?})",
                shard.collection,
                declared()
            ));
        }
    }
    Ok(())
}

/// Every rule that spans option sections.
pub(super) fn validate(config: &Config) -> Result<(), String> {
    check_control(config)?;
    check_roles(config)?;
    check_relay(config)?;
    check_transport(config)?;
    check_collection_names(config)
}
