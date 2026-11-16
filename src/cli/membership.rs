//! Cluster membership: one agent per collection the configured shards
//! name, each reporting its shards under their own listener addresses.

use std::collections::BTreeMap;
use std::net::SocketAddr;
use std::sync::Arc;

use pipestream_search::config::{Config, NodeMembershipConfig};
use pipestream_search::node::NodeConfig;
use pipestream_search::node_agent::{NodeAgent, NodeAgentConfig, ServedShard};

use super::{Startup, TaskHandle};

/// The `(host, port)` a registered node advertises: `--advertise-addr`,
/// else the first shard listener when it binds a concrete interface.
pub(super) fn resolve_advertise(
    cfg: &Config,
    membership: &NodeMembershipConfig,
) -> Result<(String, u16), String> {
    if let Some(addr) = &membership.advertise_addr {
        let (host, port) = addr
            .rsplit_once(':')
            .ok_or_else(|| format!("--advertise-addr={addr:?} is not host:port"))?;
        let port = port
            .parse::<u16>()
            .map_err(|e| format!("--advertise-addr={addr:?}: {e}"))?;
        return Ok((
            host.trim_matches(|c| c == '[' || c == ']').to_string(),
            port,
        ));
    }
    let first = cfg
        .shards
        .first()
        .ok_or_else(|| "--node-id with no shards needs --advertise-addr".to_string())?;
    if first.listen.ip().is_unspecified() {
        return Err(format!(
            "shard listener {} binds every interface; pass --advertise-addr=host:port so the \
             control plane and other nodes can reach this node",
            first.listen
        ));
    }
    Ok((first.listen.ip().to_string(), first.listen.port()))
}

/// The node configuration an agent opens assigned shards with: the
/// process-wide settings, with the per-shard ones taken from the first
/// configured shard.
fn agent_template(
    cfg: &Config,
    turbo_handshake: Option<&turbo_client::IndexGenerationHandshake>,
) -> NodeConfig {
    NodeConfig {
        max_k: cfg.max_k,
        udp_hmac_key: cfg.udp_hmac_key.clone(),
        layout: cfg.layout,
        vector_mmap: cfg.vector_mmap,
        seal_tail_docs: cfg.seal_tail_docs,
        chunk_blocks: cfg.chunk_blocks,
        share_floors: cfg.share_floors,
        block_max: cfg.block_max,
        segment_pruning: cfg.segment_pruning,
        coalesce: cfg.coalesce,
        scan_parallel: cfg.scan_parallel,
        rerank_parallel: cfg.rerank_parallel,
        floor_delta: cfg.floor_delta,
        floor_warmup_chunks: cfg.floor_warmup_chunks,
        floor_min_interval_ms: cfg.floor_min_interval_ms,
        bit_width: cfg.bit_width,
        analysis_addr: cfg
            .shards
            .first()
            .and_then(|s| s.analysis_addr.clone())
            .or_else(|| cfg.analysis_addr.clone()),
        bm25_fields: cfg.bm25_fields.clone(),
        facet_fields: cfg.facet_fields.clone(),
        numeric_fields: cfg.numeric_fields.clone(),
        map_facet_fields: cfg.map_facet_fields.clone(),
        map_numeric_fields: cfg.map_numeric_fields.clone(),
        map_integer_fields: cfg.map_integer_fields.clone(),
        map_unsigned_integer_fields: cfg.map_unsigned_integer_fields.clone(),
        integer_fields: cfg.integer_fields.clone(),
        unsigned_integer_fields: cfg.unsigned_integer_fields.clone(),
        placement_column: cfg.placement_column.clone(),
        placement_leaf: cfg.placement_leaf,
        placement_tree: cfg.placement_tree.clone(),
        derived: cfg.derived.clone(),
        geo_fields: cfg.geo_fields.clone(),
        position_fields: cfg.position_fields.clone(),
        bigram_fields: cfg.bigram_fields.clone(),
        sentence_fields: cfg.sentence_fields.clone(),
        vector_backend: cfg
            .shards
            .first()
            .map(|s| s.vector_backend.clone())
            .unwrap_or_else(|| pipestream_search::vector::EMBEDDED_TURBOVEC.to_string()),
        wal_buckets: cfg.shards.first().map_or(64, |s| s.wal_buckets),
        vocab_window_docs: cfg.vocab_window_docs,
        vocab_top_k: cfg.vocab_top_k,
        turbo_handshake: turbo_handshake.cloned(),
        matryoshka_prefix_dims: cfg
            .turbo_matryoshka_dims
            .iter()
            .map(|&d| d as u32)
            .collect(),
        matryoshka_active_dim: cfg.turbo_matryoshka_active_dim.map(|d| d as u32),
        vocab_kserve_endpoint: cfg.turbo_vocab_endpoint.clone(),
        ..NodeConfig::default()
    }
}

/// The agent settings of one collection on this node.
fn agent_config(
    startup: &Startup<'_>,
    membership: &NodeMembershipConfig,
    (host, port): &(String, u16),
    collection: &str,
    template: &NodeConfig,
) -> NodeAgentConfig {
    let cfg = startup.cfg;
    let scheme = if cfg.tls.is_some() { "https" } else { "http" };
    let replica_listen = membership.replica_listen.unwrap_or_else(|| {
        SocketAddr::new(
            cfg.shards
                .first()
                .map_or(std::net::IpAddr::from([0, 0, 0, 0]), |s| s.listen.ip()),
            0,
        )
    });
    NodeAgentConfig {
        node_id: membership.node_id.clone(),
        control_addr: membership.control_addr.clone(),
        collection: collection.to_string(),
        failure_domain: membership.failure_domain.clone(),
        data_dir: membership.data_dir.clone(),
        node_addr: format!("{scheme}://{host}:{port}"),
        advertise_host: host.clone(),
        replica_listen,
        lease_ms: membership.lease_ms,
        report_ms: membership.report_ms,
        reconcile_ms: membership.reconcile_ms,
        lag_bound: membership.lag_bound,
        scan_parallel: cfg.scan_parallel,
        template: NodeConfig {
            collection: collection.to_string(),
            ..template.clone()
        },
        phrase_index: startup.phrase_index.clone(),
        allow_missing_bm25: cfg.allow_missing_bm25,
        tls: cfg.tls.clone(),
        max_message_bytes: cfg.max_message_bytes,
    }
}

/// Start one agent per collection when the node is a registered member;
/// a member with no configured shard still registers under the unnamed
/// collection.
pub(super) fn start_agents(
    startup: &Startup<'_>,
    served: Vec<(String, ServedShard)>,
    handles: &mut Vec<TaskHandle>,
) {
    let (Some(membership), Some(advertise)) = (&startup.cfg.membership, &startup.advertise) else {
        return;
    };
    let mut by_collection: BTreeMap<String, Vec<ServedShard>> = BTreeMap::new();
    for (collection, shard) in served {
        by_collection.entry(collection).or_default().push(shard);
    }
    if by_collection.is_empty() {
        by_collection.insert(String::new(), Vec::new());
    }
    let template = agent_template(startup.cfg, startup.turbo_handshake.as_ref());
    for (collection, shards) in by_collection {
        let agent = NodeAgent::new(
            agent_config(startup, membership, advertise, &collection, &template),
            shards,
        );
        // The configured shards' flushes wake this agent's reporter.
        let notify = agent.flush_notify();
        let wake = Arc::clone(&startup.flush_notify);
        tokio::spawn(async move {
            loop {
                wake.notified().await;
                notify.notify_one();
            }
        });
        eprintln!(
            "node {:?}: membership at {} (collection {:?}, data dir {})",
            membership.node_id,
            membership.control_addr,
            collection,
            membership.data_dir.display()
        );
        for handle in agent.start(startup.shutdown_rx.clone()) {
            handles.push(tokio::spawn(async move {
                let _ = handle.await;
                Ok(())
            }));
        }
    }
}
