//! The node role: one `NodeService` listener per configured shard.

use std::net::SocketAddr;
use std::sync::Arc;

use pipestream_search::config::{Config, ShardConfig};
use pipestream_search::node::{NodeConfig, NodeServiceImpl};
use pipestream_search::node_agent::ServedShard;
use tokio::net::TcpListener;

use super::demo::load_shard_index;
use super::raft_member::RaftSurfaces;
use super::transport::{spawn_listener, windowed_server};
use super::{Startup, TaskHandle};

/// The shards this process serves from its own configuration.
#[derive(Default)]
pub(super) struct NodeShards {
    /// Every configured shard's service, in configuration order.
    pub(super) services: Vec<NodeServiceImpl>,
    /// The shards a membership agent reports, each under its collection.
    pub(super) served: Vec<(String, ServedShard)>,
}

/// The node configuration of one configured shard.
fn shard_node_config(
    cfg: &Config,
    shard: &ShardConfig,
    turbo_handshake: Option<&turbo_client::IndexGenerationHandshake>,
) -> NodeConfig {
    NodeConfig {
        max_k: cfg.max_k,
        collection: shard.collection.clone(),
        udp_hmac_key: cfg.udp_hmac_key.clone(),
        layout: cfg.layout,
        vector_mmap: cfg.vector_mmap,
        seal_tail_docs: cfg.seal_tail_docs,
        vector_backend: shard.vector_backend.clone(),
        slot_offset: shard.slot_offset,
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
        index_path: shard.index_path.clone(),
        analysis_addr: shard.analysis_addr.clone(),
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
        wal: shard.wal,
        wal_buckets: shard.wal_buckets,
        vocab: shard.vocab,
        vocab_window_docs: cfg.vocab_window_docs,
        vocab_top_k: cfg.vocab_top_k,
        position_fields: cfg.position_fields.clone(),
        bigram_fields: cfg.bigram_fields.clone(),
        sentence_fields: cfg.sentence_fields.clone(),
        turbo_handshake: turbo_handshake.cloned(),
        matryoshka_prefix_dims: cfg
            .turbo_matryoshka_dims
            .iter()
            .map(|&d| d as u32)
            .collect(),
        matryoshka_active_dim: cfg.turbo_matryoshka_active_dim.map(|d| d as u32),
        vocab_kserve_endpoint: cfg.turbo_vocab_endpoint.clone(),
    }
}

/// Open one shard's service over its demo corpus or its stored state.
fn open_shard_node(
    startup: &Startup<'_>,
    shard: &ShardConfig,
) -> Result<NodeServiceImpl, Box<dyn std::error::Error>> {
    let cfg = startup.cfg;
    let node_config = shard_node_config(cfg, shard, startup.turbo_handshake.as_ref());
    let mut node = if shard.demo.is_some() {
        NodeServiceImpl::new(load_shard_index(shard, None)?, node_config)
            .with_phrase_index(startup.phrase_index.clone())
    } else {
        NodeServiceImpl::open(
            node_config,
            startup.phrase_index.clone(),
            cfg.allow_missing_bm25,
        )?
    }
    .with_flush_notify(Arc::clone(&startup.flush_notify));
    if let Some(ref nlp_spec) = cfg.turbo_nlp_columns {
        node = node.with_nlp_columns(nlp_spec.clone());
    }
    if let Some(us) = cfg.stream_poll_delay_us.filter(|&us| us > 0) {
        node = node.with_stream_poll_delay(std::time::Duration::from_micros(us));
    }
    Ok(node)
}

/// The shard as a membership agent reports it, under the advertised host
/// and the listener's bound port.
fn served_shard(
    cfg: &Config,
    shard: &ShardConfig,
    node: &NodeServiceImpl,
    host: &str,
    addr: SocketAddr,
) -> (String, ServedShard) {
    let scheme = if cfg.tls.is_some() { "https" } else { "http" };
    let shard_id = shard
        .shard_id
        .clone()
        .unwrap_or_else(|| format!("slot-{}", shard.slot_offset));
    (
        shard.collection.clone(),
        ServedShard::configured(
            shard_id,
            node.clone(),
            format!("{scheme}://{host}:{}", addr.port()),
            shard.hash_range,
        ),
    )
}

/// Open one configured shard and serve it on its own listener.
pub(super) async fn serve_shard(
    startup: &Startup<'_>,
    raft: &RaftSurfaces,
    shard: &ShardConfig,
    shards: &mut NodeShards,
    handles: &mut Vec<TaskHandle>,
) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = startup.cfg;
    let node = open_shard_node(startup, shard)?;
    let listener = TcpListener::bind(shard.listen).await?;
    let addr: SocketAddr = listener.local_addr()?;
    // The UDP stream-signal lane shares the gRPC listener's host:port.
    node.spawn_floor_listener(addr);
    shards.services.push(node.clone());
    eprintln!("NodeService listening on {addr}");
    if let Some((host, _)) = &startup.advertise {
        shards
            .served
            .push(served_shard(cfg, shard, &node, host, addr));
    }
    let max = cfg.max_message_bytes;
    let diagnostics = node.diagnostics_server(max);
    let node_base = windowed_server(cfg.tls.as_ref(), true)?
        .add_service(NodeServiceImpl::into_server(node, max))
        .add_service(diagnostics);
    let node_server = raft.on_node_listener(node_base);
    handles.push(spawn_listener(node_server, listener, &startup.shutdown_rx));
    Ok(())
}
