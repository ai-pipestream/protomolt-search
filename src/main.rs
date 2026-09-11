//! pipestream-search binary: one process, one or both roles, one or more
//! shards.
//!
//! Configuration: `--config cluster.toml` (TOML file) with `PIPESTREAM_SEARCH_*`
//! env overrides and `--key=value` flags on top (see `src/config.rs`).
//!
//! Subcommand: `pipestream-search configure-backend --fit-from=host:port
//! --apply-to=h1:p1,h2:p2` copies opaque provider construction state from
//! one configured node to empty peers.
//!
//! Examples:
//!
//! ```text
//! # Single-process demo: coordinator + node + random demo corpus,
//! # then one self-issued search against itself.
//! pipestream-search --role=both --demo-vectors=20000 --allow-plaintext \
//!     --nodes=127.0.0.1:50051 --demo-query
//!
//! # Static two-machine cluster (see README "Two-machine runbook").
//! # host-a:    pipestream-search --config /etc/turbovec/host-a.toml
//! # host-b:   pipestream-search --config /etc/turbovec/host-b.toml
//! ```

use std::net::SocketAddr;
use std::path::Path;

use pipestream_search::clustered_turbovec::ClusteredTurboVecBackend;
use pipestream_search::collections::{ClusterControlSet, CollectionSet};
use pipestream_search::config::{
    load_shard_map, normalize_addr, parse, ClusteredTurboVecConfig, Config, DemoConfig, Role,
    ShardConfig, ShardMap,
};
use pipestream_search::control_plane::{ClusterControlService, ControlPolicy, DurableControlPlane};
use pipestream_search::coordinator::{CoordinatorServiceImpl, TopologyRoute};
use pipestream_search::harness;
use pipestream_search::node::{NodeConfig, NodeServiceImpl};
use pipestream_search::node_agent::{NodeAgent, NodeAgentConfig, ServedShard};
use pipestream_search::pb::cluster_control_server::ClusterControl;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_client::SearchServiceClient;
use pipestream_search::pb::{
    ConfigureVectorBackendRequest, GetVectorBackendRequest, ReconcileClusterRequest, SearchRequest,
};
use pipestream_search::vector::VectorIndex;
use tokio::net::TcpListener;
use tonic::transport::Server;

fn topology_routes(map: &ShardMap) -> Result<Vec<TopologyRoute>, String> {
    map.shards
        .iter()
        .enumerate()
        .map(|(shard, entry)| {
            let hash_range = match (entry.hash_lo, entry.hash_hi) {
                (Some(lo), Some(hi)) => Some((lo, hi)),
                (None, None) => None,
                _ => {
                    return Err(format!(
                        "shard map entry {shard} must provide both hash_lo and hash_hi or neither"
                    ))
                }
            };
            Ok(TopologyRoute {
                addr: normalize_addr(entry.addr.clone()),
                replica: entry.replica.clone().map(normalize_addr),
                hash_range,
                placement: entry.placement.map(|code| code as i64),
            })
        })
        .collect()
}

/// Build a demo index by fitting provider-owned state on a sample and then
/// constructing the served generation from that state.
fn build_demo_index(demo: DemoConfig, backend_kind: &str) -> Result<VectorIndex, String> {
    let corpus = harness::unit_vectors(demo.vectors, demo.dim, 0xDE10_0001);
    let sample_n = (demo.vectors / 5).max(1).min(demo.vectors);
    let backend_config = VectorIndex::fit_backend_config(
        backend_kind,
        demo.dim,
        demo.bit_width,
        &corpus[..sample_n * demo.dim],
    )
    .map_err(|e| format!("demo calibration fit: {e}"))?;
    let mut index = VectorIndex::from_backend_config(demo.dim, &backend_config)
        .map_err(|e| format!("demo index construct: {e}"))?;
    index
        .add(&corpus, demo.dim)
        .map_err(|e| format!("demo add: {e}"))?;
    index.prepare().map_err(|e| format!("demo prepare: {e}"))?;
    Ok(index)
}

/// Load a shard's index, or `None` when the shard starts empty: its index
/// path does not exist yet (a from-scratch shard awaiting
/// ConfigureVectorBackend + AddVectors; Flush later writes that path). When a snapshot
/// generation is active, the index loads from INSIDE it — the generation
/// always reflects the newest installed or flushed image.
fn load_shard_index(
    shard: &ShardConfig,
    generation: Option<&Path>,
) -> Result<Option<VectorIndex>, String> {
    if let Some(demo) = shard.demo {
        eprintln!(
            "building demo index: {} vectors x dim {} @ {} bits",
            demo.vectors, demo.dim, demo.bit_width
        );
        return build_demo_index(demo, &shard.vector_backend).map(Some);
    }
    let path = match generation {
        Some(dir) => pipestream_search::node::generation_vector(dir),
        None => shard
            .index_path
            .as_ref()
            .expect("config validated an index source")
            .clone(),
    };
    if !path.exists() {
        return Ok(None);
    }
    let mut index = VectorIndex::load(&shard.vector_backend, &path)
        .map_err(|e| format!("load {}: {e}", path.display()))?;
    index
        .prepare()
        .map_err(|e| format!("prepare {}: {e}", path.display()))?;
    Ok(Some(index))
}

/// The `(host, port)` a registered node advertises: `--advertise-addr`,
/// else the first shard listener when it binds a concrete interface.
fn resolve_advertise(
    cfg: &Config,
    membership: &pipestream_search::config::NodeMembershipConfig,
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

/// Wait for SIGINT or SIGTERM (whichever comes first).
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{signal, SignalKind};
        let mut term = signal(SignalKind::terminate()).expect("install SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {},
            _ = term.recv() => {},
        }
    }
    #[cfg(not(unix))]
    {
        let _ = tokio::signal::ctrl_c().await;
    }
}

async fn run(cfg: Config) -> Result<(), Box<dyn std::error::Error>> {
    pipestream_search::security::install_client_tls(cfg.client_tls.clone());
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        shutdown_signal().await;
        let _ = shutdown_tx.send(true);
    });

    let mut handles = Vec::new();
    // A Raft member starts from its existing durable state before any
    // listener that routes on its committed map (docs/raft-hosting.md).
    let raft_host = start_raft_member(&cfg).await?.map(std::sync::Arc::new);
    // Managed catalogs are served on the coordinator or relay listener; a
    // node-only process has neither, so it is rejected before any catalog
    // is opened. Then the catalogs recover, before any listener serves
    // them: a rejection stops the process with its cause, no partial
    // service.
    #[cfg(all(feature = "raft", feature = "tls"))]
    let hosted_writes = {
        let names_catalogs = cfg
            .raft
            .as_ref()
            .is_some_and(|member| !member.managed_catalogs.is_empty());
        if names_catalogs && !cfg.relay && !matches!(cfg.role, Role::Coordinator | Role::Both) {
            return Err(
                "raft managed catalogs need the coordinator or relay listener; a node-only process serves no write surface"
                    .into(),
            );
        }
        hosted_write_service(&cfg, raft_host.clone())?
    };
    // The operator service rides every listener this process opens when
    // it is a Raft member, node listeners included: unlike the hosted
    // writes, it needs no catalog and no coordinator surface.
    #[cfg(all(feature = "raft", feature = "tls"))]
    let raft_operator = raft_host.clone().map(|host| {
        pipestream_search::raft::operator_service::RaftOperatorServiceImpl::new(host)
            .with_client_cert_required(
                cfg.tls
                    .as_ref()
                    .is_some_and(|tls| tls.client_ca_pem.is_some()),
            )
    });
    let mut node_services = Vec::new();
    // Membership (docs/cluster-control.md): one agent per collection the
    // configured shards name, each reporting its shards under their own
    // listener addresses.
    let flush_notify = std::sync::Arc::new(tokio::sync::Notify::new());
    let mut served: Vec<(String, ServedShard)> = Vec::new();
    let advertise = match &cfg.membership {
        Some(membership) => Some(resolve_advertise(&cfg, membership)?),
        None => None,
    };
    let phrase_index = cfg
        .phrase_glossary
        .as_ref()
        .map(|path| {
            pipestream_search::phrases::PhraseIndex::load_tsv(
                path,
                cfg.phrase_field.clone(),
                cfg.entity_map_field.clone(),
                cfg.phrase_ignore_case,
                cfg.phrase_ner,
            )
            .map(std::sync::Arc::new)
        })
        .transpose()?;
    if let Some(index) = &phrase_index {
        eprintln!(
            "phrase vocabulary: field {:?}, fingerprint {:016x}, entity map {:?}, NER {}",
            index.phrase_field(),
            index.fingerprint(),
            index.entity_map_field(),
            index.include_ner()
        );
    }

    if matches!(cfg.role, Role::Node | Role::Both) {
        for shard in &cfg.shards {
            let node_config = NodeConfig {
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
            };
            let node = if shard.demo.is_some() {
                NodeServiceImpl::new(load_shard_index(shard, None)?, node_config)
                    .with_phrase_index(phrase_index.clone())
            } else {
                NodeServiceImpl::open(node_config, phrase_index.clone(), cfg.allow_missing_bm25)?
            }
            .with_flush_notify(std::sync::Arc::clone(&flush_notify));
            let listener = TcpListener::bind(shard.listen).await?;
            let addr: SocketAddr = listener.local_addr()?;
            // The UDP stream-signal lane shares the gRPC listener's host:port.
            node.spawn_floor_listener(addr);
            node_services.push(node.clone());
            eprintln!("NodeService listening on {addr}");
            if let Some((host, _)) = &advertise {
                let scheme = if cfg.tls.is_some() { "https" } else { "http" };
                let shard_id = shard
                    .shard_id
                    .clone()
                    .unwrap_or_else(|| format!("slot-{}", shard.slot_offset));
                served.push((
                    shard.collection.clone(),
                    ServedShard::configured(
                        shard_id,
                        node.clone(),
                        format!("{scheme}://{host}:{}", addr.port()),
                        shard.hash_range,
                    ),
                ));
            }
            let max = cfg.max_message_bytes;
            let mut shutdown = shutdown_rx.clone();
            let diagnostics = node.diagnostics_server(max);
            let node_base = secured_server(cfg.tls.as_ref(), true)?
                .initial_stream_window_size(pipestream_search::H2_STREAM_WINDOW)
                .initial_connection_window_size(pipestream_search::H2_CONN_WINDOW)
                .add_service(NodeServiceImpl::into_server(node, max))
                .add_service(diagnostics);
            #[cfg(all(feature = "raft", feature = "tls"))]
            let node_server = node_base
                .add_optional_service(raft_operator.clone().map(|service| service.into_server()));
            #[cfg(not(all(feature = "raft", feature = "tls")))]
            let node_server = node_base;
            handles.push(tokio::spawn(node_server.serve_with_incoming_shutdown(
                harness::nodelay_incoming(listener),
                async move {
                    let _ = shutdown.wait_for(|v| *v).await;
                },
            )));
        }
    }

    if let (Some(membership), Some((host, port))) = (&cfg.membership, &advertise) {
        let scheme = if cfg.tls.is_some() { "https" } else { "http" };
        let mut by_collection: std::collections::BTreeMap<String, Vec<ServedShard>> =
            std::collections::BTreeMap::new();
        for (collection, shard) in served {
            by_collection.entry(collection).or_default().push(shard);
        }
        if by_collection.is_empty() {
            by_collection.insert(String::new(), Vec::new());
        }
        let template = NodeConfig {
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
            ..NodeConfig::default()
        };
        let replica_listen = membership.replica_listen.unwrap_or_else(|| {
            SocketAddr::new(
                cfg.shards
                    .first()
                    .map_or(std::net::IpAddr::from([0, 0, 0, 0]), |s| s.listen.ip()),
                0,
            )
        });
        for (collection, shards) in by_collection {
            let agent = NodeAgent::new(
                NodeAgentConfig {
                    node_id: membership.node_id.clone(),
                    control_addr: membership.control_addr.clone(),
                    collection: collection.clone(),
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
                        collection: collection.clone(),
                        ..template.clone()
                    },
                    phrase_index: phrase_index.clone(),
                    allow_missing_bm25: cfg.allow_missing_bm25,
                    tls: cfg.tls.clone(),
                    max_message_bytes: cfg.max_message_bytes,
                },
                shards,
            );
            // The configured shards' flushes wake this agent's reporter.
            let notify = agent.flush_notify();
            let wake = std::sync::Arc::clone(&flush_notify);
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
            for handle in agent.start(shutdown_rx.clone()) {
                handles.push(tokio::spawn(async move {
                    let _ = handle.await;
                    Ok(())
                }));
            }
        }
    }

    if let Some(metrics_addr) = cfg.metrics_listen {
        let listener = TcpListener::bind(metrics_addr).await?;
        let bound = listener.local_addr()?;
        let gauges: Vec<pipestream_search::metrics::GaugeProvider> = node_services
            .iter()
            .map(|node| node.metrics_provider())
            .collect();
        #[cfg(all(feature = "raft", feature = "tls"))]
        let member_gauges: Vec<pipestream_search::metrics::MemberGaugeProvider> = raft_host
            .clone()
            .map(|host| pipestream_search::raft::RaftHost::member_gauge_provider(host))
            .into_iter()
            .collect();
        #[cfg(not(all(feature = "raft", feature = "tls")))]
        let member_gauges: Vec<pipestream_search::metrics::MemberGaugeProvider> = Vec::new();
        eprintln!(
            "metrics on http://{bound}/metrics ({} shard gauges)",
            gauges.len()
        );
        handles.push(tokio::spawn(async move {
            pipestream_search::metrics::serve_with_member(listener, gauges, member_gauges).await;
            Ok(())
        }));
    }

    if cfg.relay {
        // A relay (docs/relay-coordinators.md): this coordinator's shard
        // set behind the node-facing surface, on the coordinator listener,
        // with the parent-facing UDP lane on the same port. The startup
        // check refuses children whose slot ranges are not contiguous.
        let listener = TcpListener::bind(cfg.coord_listen).await?;
        let addr: SocketAddr = listener.local_addr()?;
        let (coordinator, _control) = build_corpus(
            &cfg,
            CorpusSpec {
                name: "",
                node_addrs: &cfg.node_addrs,
                replica_addrs: &cfg.replica_addrs,
                shard_map: cfg.shard_map.as_ref(),
                shard_map_path: cfg.shard_map_path.as_deref(),
                analysis_addr: cfg.analysis_addr.as_ref(),
                bm25_k1: cfg.bm25_k1,
                bm25_b: cfg.bm25_b,
                dense_quality_profile: cfg.dense_quality_profile.as_deref(),
                synonyms: cfg.synonyms.as_deref(),
                dense_execution_policy: cfg.dense_execution_policy.as_deref(),
                replica_state_path: cfg.replica_state_path.as_deref(),
                control_state_path: None,
                clustered_turbovec: cfg.clustered_turbovec.as_ref(),
            },
            &phrase_index,
            &shutdown_rx,
            &mut handles,
        )
        .await?;
        let relay = relay_service(&cfg, raft_host.as_deref(), coordinator)?;
        let health = relay
            .check_children()
            .await
            .map_err(|status| format!("relay startup: {}", status.message()))?;
        relay.spawn_floor_listener(addr);
        eprintln!(
            "relay NodeService listening on {addr} over {} children, slots {}..{} ({} vectors, \
             {} documents)",
            relay.children().len(),
            health.slot_offset,
            health.slot_offset + health.num_vectors.max(health.bm25_docs),
            health.num_vectors,
            health.bm25_docs
        );
        let max = cfg.max_message_bytes;
        let mut shutdown = shutdown_rx.clone();
        let relay_base = secured_server(cfg.tls.as_ref(), true)?
            .initial_stream_window_size(pipestream_search::H2_STREAM_WINDOW)
            .initial_connection_window_size(pipestream_search::H2_CONN_WINDOW)
            .add_service(relay.clone().into_server(max))
            .add_service(relay.diagnostics_server(max));
        #[cfg(all(feature = "raft", feature = "tls"))]
        let relay_server = relay_base
            .add_optional_service(hosted_writes.clone().map(|service| service.into_server()))
            .add_optional_service(raft_operator.clone().map(|service| service.into_server()));
        #[cfg(not(all(feature = "raft", feature = "tls")))]
        let relay_server = relay_base;
        handles.push(tokio::spawn(relay_server.serve_with_incoming_shutdown(
            harness::nodelay_incoming(listener),
            async move {
                let _ = shutdown.wait_for(|v| *v).await;
            },
        )));
    } else if matches!(cfg.role, Role::Coordinator | Role::Both) {
        let listener = TcpListener::bind(cfg.coord_listen).await?;
        let addr: SocketAddr = listener.local_addr()?;
        let (search_set, control_set) =
            build_collections(&cfg, &phrase_index, &shutdown_rx, &mut handles, addr).await?;
        let max = cfg.max_message_bytes;
        let mut shutdown = shutdown_rx.clone();
        let gauges: Vec<pipestream_search::metrics::GaugeProvider> = node_services
            .iter()
            .map(|node| node.metrics_provider())
            .collect();
        #[cfg(all(feature = "raft", feature = "tls"))]
        let member_gauges: Vec<pipestream_search::metrics::MemberGaugeProvider> = raft_host
            .clone()
            .map(|host| pipestream_search::raft::RaftHost::member_gauge_provider(host))
            .into_iter()
            .collect();
        #[cfg(not(all(feature = "raft", feature = "tls")))]
        let member_gauges: Vec<pipestream_search::metrics::MemberGaugeProvider> = Vec::new();
        let diagnostics = search_set
            .diagnostics()
            .with_gauges(gauges)
            .with_member_gauges(member_gauges)
            .into_server(max);
        let coord_base = secured_server(cfg.tls.as_ref(), false)?
            .initial_stream_window_size(pipestream_search::H2_STREAM_WINDOW)
            .initial_connection_window_size(pipestream_search::H2_CONN_WINDOW)
            .add_optional_service(control_set.map(|set| set.into_server(max)))
            .add_service(search_set.into_server(max))
            .add_service(diagnostics);
        #[cfg(all(feature = "raft", feature = "tls"))]
        let coord_server = coord_base
            .add_optional_service(hosted_writes.clone().map(|service| service.into_server()))
            .add_optional_service(raft_operator.clone().map(|service| service.into_server()));
        #[cfg(not(all(feature = "raft", feature = "tls")))]
        let coord_server = coord_base;
        handles.push(tokio::spawn(coord_server.serve_with_incoming_shutdown(
            harness::nodelay_incoming(listener),
            async move {
                let _ = shutdown.wait_for(|v| *v).await;
            },
        )));
    }
    // The listeners own their copies now; releasing these lets the
    // member shut down on its last handle once they drain.
    #[cfg(all(feature = "raft", feature = "tls"))]
    drop(hosted_writes);
    #[cfg(all(feature = "raft", feature = "tls"))]
    drop(raft_operator);

    if cfg.demo_query {
        let query = harness::unit_vectors(1, cfg.query_dim, 0x0E0E_0001);
        let endpoint = format!("http://127.0.0.1:{}", cfg.coord_listen.port());
        let mut client = SearchServiceClient::connect(endpoint)
            .await?
            .max_decoding_message_size(cfg.max_message_bytes)
            .max_encoding_message_size(cfg.max_message_bytes);
        let response = client
            .search(SearchRequest {
                field: String::new(),
                collection: String::new(),
                request_id: String::new(),
                k: 10,
                vector: query,
                collapse_parents: false,
                geo_filters: Vec::new(),
                filter: String::new(),
            })
            .await?
            .into_inner();
        println!(
            "demo search ({}): top {} hits",
            response.request_id,
            response.hits.len()
        );
        for hit in &response.hits {
            println!("  id={:<8} score={:.6}", hit.vector_id, hit.score);
        }
    }

    #[cfg(all(feature = "raft", feature = "tls"))]
    if let Some(host) = raft_host {
        let mut shutdown = shutdown_rx.clone();
        handles.push(tokio::spawn(async move {
            let _ = shutdown.wait_for(|v| *v).await;
            // The hosted write service holds the member's other handle
            // while its listener drains; the member shuts down on the
            // last one, once every listener below has released it.
            let mut shared = host;
            let owned = loop {
                match std::sync::Arc::try_unwrap(shared) {
                    Ok(host) => break host,
                    Err(back) => {
                        shared = back;
                        tokio::time::sleep(std::time::Duration::from_millis(10)).await;
                    }
                }
            };
            if let Err(error) = owned.shutdown().await {
                eprintln!("raft member shutdown: {}", error.message());
            }
            Ok(())
        }));
    }
    #[cfg(not(all(feature = "raft", feature = "tls")))]
    let _ = raft_host;

    for handle in handles {
        handle.await??;
    }

    if cfg.save_on_shutdown {
        for node in &node_services {
            match node.flush_index() {
                Ok(resp) if resp.written => eprintln!(
                    "shutdown: flushed {} vectors to {}",
                    resp.num_vectors, resp.path
                ),
                Ok(_) => {}
                Err(e) => eprintln!("shutdown: flush failed: {e}"),
            }
        }
    }
    // Vocabulary windows seal on shutdown too, independent of the index
    // flush (analytics, not a ledger — an empty window writes nothing).
    for node in &node_services {
        node.snapshot_vocab_on_shutdown();
    }
    Ok(())
}

/// The Raft member this process serves as, from `--raft-*`; `None` when
/// none is configured. Serving opens existing state and creates nothing.
#[cfg(all(feature = "raft", feature = "tls"))]
async fn start_raft_member(
    cfg: &Config,
) -> Result<Option<pipestream_search::raft::RaftHost>, Box<dyn std::error::Error>> {
    use pipestream_search::raft::{operator, RaftHost};
    let Some(member) = &cfg.raft else {
        return Ok(None);
    };
    let identity = operator::identity(member);
    let host_config = operator::host_config(member)?;
    let transport = operator::cluster_transport(member, cfg.tls.as_ref(), cfg.client_tls.as_ref())?;
    let host = RaftHost::start_member(
        &member.dir,
        &identity,
        member.node_id,
        &host_config,
        transport,
    )
    .await
    .map_err(|status| format!("raft member: {}", status.message()))?;
    eprintln!(
        "raft member {} of group {} listening on {} (peers dial {}), timing {}",
        member.node_id,
        hex(&member.group_id),
        host.listen_addr()
            .map_or_else(String::new, |a| a.to_string()),
        host.advertised_addr().unwrap_or(""),
        host_config.timing_agreement()
    );
    Ok(Some(host))
}

#[cfg(not(all(feature = "raft", feature = "tls")))]
async fn start_raft_member(cfg: &Config) -> Result<Option<()>, Box<dyn std::error::Error>> {
    match &cfg.raft {
        Some(_) => {
            Err("this build has no Raft support (features `raft` and `tls` are needed)".into())
        }
        None => Ok(None),
    }
}

/// The relay over its coordinator's shard set, routing on the committed map
/// of the Raft member when `--raft-map-*` names one, and on the file-polled
/// map otherwise.
#[cfg(all(feature = "raft", feature = "tls"))]
fn relay_service(
    cfg: &Config,
    host: Option<&pipestream_search::raft::RaftHost>,
    coordinator: pipestream_search::coordinator::CoordinatorServiceImpl,
) -> Result<pipestream_search::relay::RelayService, Box<dyn std::error::Error>> {
    use pipestream_search::relay::RelayService;
    let base = std::sync::Arc::new(coordinator);
    let map = cfg.raft.as_ref().and_then(|member| member.map.as_ref());
    match (host, map) {
        (Some(host), Some(map)) => {
            let key = pipestream_search::pb::storage::LogicalSourceOwner {
                workspace: map.workspace.clone(),
                collection: map.collection.clone(),
                owner_id: Vec::new(),
            };
            let source = pipestream_search::source_authority::AuthorityMapSource::attach(
                host.store_handle(),
                &map.principal,
                &key,
            )
            .map_err(|status| format!("raft map source: {}", status.message()))?;
            eprintln!(
                "relay routes on the committed map of {}/{} (revision {})",
                map.workspace,
                map.collection,
                pipestream_search::relay::MapSource::current(&source).control_revision
            );
            Ok(RelayService::with_map(base, std::sync::Arc::new(source)))
        }
        _ => Ok(RelayService::new(base)),
    }
}

#[cfg(not(all(feature = "raft", feature = "tls")))]
fn relay_service(
    _cfg: &Config,
    _host: Option<&()>,
    coordinator: pipestream_search::coordinator::CoordinatorServiceImpl,
) -> Result<pipestream_search::relay::RelayService, Box<dyn std::error::Error>> {
    Ok(pipestream_search::relay::RelayService::new(
        std::sync::Arc::new(coordinator),
    ))
}

/// The hosted owner-write service of a member whose operator configuration
/// names managed catalogs, recovered at start under the configured actor
/// on the store's applied view (no leader is needed to start). `None` when
/// no catalog is named. Every admission the service later grants comes
/// from the host's lease.
#[cfg(all(feature = "raft", feature = "tls"))]
fn hosted_write_service(
    cfg: &Config,
    host: Option<std::sync::Arc<pipestream_search::raft::RaftHost>>,
) -> Result<
    Option<pipestream_search::document_write_service::hosted::HostedDocumentWriteService>,
    Box<dyn std::error::Error>,
> {
    use pipestream_search::document_write_service::hosted::{
        recover_catalogs, HostedDocumentWriteService,
    };
    use pipestream_search::pb::DocumentWriteServiceLimits;
    let Some(member) = &cfg.raft else {
        return Ok(None);
    };
    if member.managed_catalogs.is_empty() {
        return Ok(None);
    }
    let Some(host) = host else {
        return Err("raft managed catalogs need the member's host".into());
    };
    let actor = member
        .managed_principal
        .as_deref()
        .ok_or("--raft-managed-catalog needs --raft-managed-principal")?;
    let Some(principals) = &cfg.principals else {
        return Err(
            "--raft-managed-catalog needs --bearer-tokens, the transport principals".into(),
        );
    };
    let catalogs = recover_catalogs(&host, actor, &member.managed_catalogs)
        .map_err(|status| format!("raft managed catalogs: {}", status.message()))?;
    let count = catalogs.len();
    let limits = DocumentWriteServiceLimits {
        max_request_bytes: member.managed_max_request_bytes,
        max_pending_bytes: member.managed_max_pending_bytes,
        max_in_flight: u32::try_from(member.managed_max_in_flight)
            .map_err(|_| "--raft-managed-max-in-flight must be 1..1024")?,
    };
    let service = HostedDocumentWriteService::new((**principals).clone(), host, catalogs, limits)
        .map_err(|status| format!("hosted write service: {}", status.message()))?;
    let collections: Vec<&str> = member
        .managed_catalogs
        .iter()
        .map(|entry| entry.collection.as_str())
        .collect();
    eprintln!(
        "hosted owner writes for collections [{collections}] as actor {actor} ({count} catalogs)",
        collections = collections.join(", "),
    );
    Ok(Some(service))
}

fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(char::from(b"0123456789abcdef"[usize::from(byte >> 4)]));
        out.push(char::from(b"0123456789abcdef"[usize::from(byte & 15)]));
    }
    out
}

/// `raft-prepare --raft-dir=... --raft-node-id=... --raft-group-id=...
/// --raft-authority-incarnation=... --raft-listen=... --raft-peers=...`:
/// create the durable state of a member that will join by snapshot. The
/// genesis policy and limits are placeholders the group's image replaces;
/// no entry applies before that (docs/raft-hosting.md, "Membership").
fn raft_prepare(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = parse(args)?;
    let Some(member) = &cfg.raft else {
        return Err("raft-prepare needs --raft-dir and the other --raft-* options".into());
    };
    #[cfg(all(feature = "raft", feature = "tls"))]
    {
        use pipestream_search::raft::{operator, RaftHost};
        let identity = operator::identity(member);
        let policy = pipestream_search::pb::AccessPolicy {
            format_version: 1,
            revision: 1,
            resources: Vec::new(),
            grants: Vec::new(),
        };
        let limits = pipestream_search::pb::storage::SourceAuthorityLimits {
            max_owners: 16,
            max_decisions: 256,
            max_payload_bytes: 16 << 20,
            max_command_bytes: 64 << 10,
        };
        RaftHost::prepare_member(&member.dir, &identity, member.node_id, &policy, &limits)
            .map_err(|status| format!("raft-prepare: {}", status.message()))?;
        println!(
            "prepared raft member {} of group {} in {}; add it as a learner from the leader",
            member.node_id,
            hex(&member.group_id),
            member.dir.display()
        );
        Ok(())
    }
    #[cfg(not(all(feature = "raft", feature = "tls")))]
    {
        let _ = member;
        Err("this build has no Raft support (features `raft` and `tls` are needed)".into())
    }
}

#[cfg(all(feature = "raft", feature = "tls"))]
/// The `raft-*` operator subcommands (docs/raft-hosting.md, "Operator
/// surface"): thin clients of the operator RPCs next to `raft-prepare`,
/// plus the local `raft-bootstrap`. Exit codes: 0 on success, 2 on a
/// usage error (the invocation names no valid call, so nothing is
/// attempted), 1 on a rejected call with the status code and message on
/// stderr.
fn cli_usage(command: &str, detail: &str) -> ! {
    eprintln!("{command}: usage error: {detail}");
    std::process::exit(2);
}

#[cfg(all(feature = "raft", feature = "tls"))]
/// The value of `--{key}=...` among a subcommand's args.
fn cli_flag(args: &[String], key: &str) -> Option<String> {
    let prefix = format!("--{key}=");
    args.iter()
        .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
}

#[cfg(all(feature = "raft", feature = "tls"))]
/// `raft-status --addr=<host:port>`: the member's address.
fn parse_raft_status(args: &[String]) -> Result<String, String> {
    cli_flag(args, "addr").ok_or_else(|| "raft-status --addr=<host:port>".to_string())
}

#[cfg(all(feature = "raft", feature = "tls"))]
/// `raft-add-learner --addr=<leader> --node-id=<n> --node-addr=<host:port>`:
/// the leader to ask, the learner's id, the address peers dial for it.
fn parse_raft_add_learner(args: &[String]) -> Result<(String, u64, String), String> {
    const USAGE: &str = "raft-add-learner --addr=<leader> --node-id=<n> --node-addr=<host:port>";
    let addr = cli_flag(args, "addr").ok_or(USAGE)?;
    let raw = cli_flag(args, "node-id").ok_or(USAGE)?;
    let node_id = raw
        .parse::<u64>()
        .map_err(|_| format!("{USAGE}: --node-id={raw:?} is not a node id"))?;
    let node_addr = cli_flag(args, "node-addr").ok_or(USAGE)?;
    Ok((addr, node_id, node_addr))
}

#[cfg(all(feature = "raft", feature = "tls"))]
/// `raft-promote --addr=<leader> --node-id=<n>[,<n>...]`: the leader to
/// ask and the learners to promote.
fn parse_raft_promote(args: &[String]) -> Result<(String, Vec<u64>), String> {
    const USAGE: &str = "raft-promote --addr=<leader> --node-id=<n>[,<n>...]";
    let addr = cli_flag(args, "addr").ok_or(USAGE)?;
    let raw = cli_flag(args, "node-id").ok_or(USAGE)?;
    let mut ids = Vec::new();
    for part in raw.split(',') {
        ids.push(
            part.trim()
                .parse::<u64>()
                .map_err(|_| format!("{USAGE}: --node-id={raw:?} is not a learner list"))?,
        );
    }
    Ok((addr, ids))
}

#[cfg(all(feature = "raft", feature = "tls"))]
/// `raft-remove-member --addr=<leader> --node-id=<n>`: the leader to ask
/// and the member to remove.
fn parse_raft_remove_member(args: &[String]) -> Result<(String, u64), String> {
    const USAGE: &str = "raft-remove-member --addr=<leader> --node-id=<n>";
    let addr = cli_flag(args, "addr").ok_or(USAGE)?;
    let raw = cli_flag(args, "node-id").ok_or(USAGE)?;
    let node_id = raw
        .parse::<u64>()
        .map_err(|_| format!("{USAGE}: --node-id={raw:?} is not a node id"))?;
    Ok((addr, node_id))
}

#[cfg(all(feature = "raft", feature = "tls"))]
/// `raft-verify-image --addr=<member>`: the member to ask.
fn parse_raft_verify_image(args: &[String]) -> Result<String, String> {
    cli_flag(args, "addr").ok_or_else(|| "raft-verify-image --addr=<member>".to_string())
}

#[cfg(all(feature = "raft", feature = "tls"))]
/// `raft-bootstrap`'s two JSON files: the policy and the limits the
/// first member is created with.
fn parse_raft_bootstrap_files(args: &[String]) -> Result<(String, String), String> {
    const USAGE: &str = "raft-bootstrap --raft-dir=... --raft-node-id=... --raft-group-id=... \
        --raft-authority-incarnation=... --raft-listen=... --raft-peers=... \
        --policy=<file> --limits=<file>";
    let policy = cli_flag(args, "policy").ok_or(USAGE)?;
    let limits = cli_flag(args, "limits").ok_or(USAGE)?;
    Ok((policy, limits))
}

/// The operator RPC client over the member's listener, with the
/// process-wide client material, the way the console dials.
#[cfg(all(feature = "raft", feature = "tls"))]
async fn operator_client(
    addr: &str,
) -> Result<
    pipestream_search::pb::raft_operator_service_client::RaftOperatorServiceClient<
        tonic::transport::Channel,
    >,
    Box<dyn std::error::Error>,
> {
    let channel = secure_channel(addr).await?;
    Ok(
        pipestream_search::pb::raft_operator_service_client::RaftOperatorServiceClient::new(
            channel,
        )
        .max_decoding_message_size(pipestream_search::MAX_MESSAGE_BYTES)
        .max_encoding_message_size(pipestream_search::MAX_MESSAGE_BYTES),
    )
}

/// Print one proto message as proto3 JSON: the console's rendering
/// (every field, defaults included), one document, no other output on
/// stdout.
#[cfg(all(feature = "raft", feature = "tls"))]
fn print_message_json<M: prost::Message>(
    message_name: &str,
    message: &M,
) -> Result<(), Box<dyn std::error::Error>> {
    let pool = pipestream_search::console::descriptor_pool();
    let descriptor = pool
        .get_message_by_name(message_name)
        .ok_or_else(|| format!("{message_name} is not in the compiled descriptor set"))?;
    let json =
        pipestream_search::console::message_bytes_to_json(&descriptor, &message.encode_to_vec())?;
    println!(
        "{}",
        String::from_utf8(json).map_err(|e| format!("response JSON is not UTF-8: {e}"))?
    );
    Ok(())
}

#[cfg(all(feature = "raft", feature = "tls"))]
async fn raft_status(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    use pipestream_search::pb::GetMemberStatusRequest;
    let addr = parse_raft_status(args).unwrap_or_else(|e| cli_usage("raft-status", &e));
    let status = operator_client(&addr)
        .await?
        .get_member_status(tonic::Request::new(GetMemberStatusRequest {}))
        .await
        .map_err(|status| format!("raft-status: {}: {}", status.code(), status.message()))?
        .into_inner();
    print_message_json("ai.protomolt.search.v1.MemberStatus", &status)
}

#[cfg(all(feature = "raft", feature = "tls"))]
async fn raft_add_learner(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    use pipestream_search::pb::AddLearnerRequest;
    let (addr, node_id, node_addr) =
        parse_raft_add_learner(args).unwrap_or_else(|e| cli_usage("raft-add-learner", &e));
    let change = operator_client(&addr)
        .await?
        .add_learner(tonic::Request::new(AddLearnerRequest {
            node_id,
            addr: node_addr,
        }))
        .await
        .map_err(|status| format!("raft-add-learner: {}: {}", status.code(), status.message()))?
        .into_inner();
    print_message_json("ai.protomolt.search.v1.MembershipChange", &change)
}

#[cfg(all(feature = "raft", feature = "tls"))]
async fn raft_promote(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    use pipestream_search::pb::PromoteLearnersRequest;
    let (addr, learner_ids) =
        parse_raft_promote(args).unwrap_or_else(|e| cli_usage("raft-promote", &e));
    let change = operator_client(&addr)
        .await?
        .promote_learners(tonic::Request::new(PromoteLearnersRequest { learner_ids }))
        .await
        .map_err(|status| format!("raft-promote: {}: {}", status.code(), status.message()))?
        .into_inner();
    print_message_json("ai.protomolt.search.v1.MembershipChange", &change)
}

#[cfg(all(feature = "raft", feature = "tls"))]
async fn raft_remove_member(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    use pipestream_search::pb::RemoveMemberRequest;
    let (addr, node_id) =
        parse_raft_remove_member(args).unwrap_or_else(|e| cli_usage("raft-remove-member", &e));
    let change = operator_client(&addr)
        .await?
        .remove_member(tonic::Request::new(RemoveMemberRequest { node_id }))
        .await
        .map_err(|status| {
            format!(
                "raft-remove-member: {}: {}",
                status.code(),
                status.message()
            )
        })?
        .into_inner();
    print_message_json("ai.protomolt.search.v1.MembershipChange", &change)
}

#[cfg(all(feature = "raft", feature = "tls"))]
async fn raft_verify_image(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    use pipestream_search::pb::VerifyPublishedImageRequest;
    let addr = parse_raft_verify_image(args).unwrap_or_else(|e| cli_usage("raft-verify-image", &e));
    let verification = operator_client(&addr)
        .await?
        .verify_published_image(tonic::Request::new(VerifyPublishedImageRequest {}))
        .await
        .map_err(|status| format!("raft-verify-image: {}: {}", status.code(), status.message()))?
        .into_inner();
    print_message_json(
        "ai.protomolt.search.v1.PublishedImageVerification",
        &verification,
    )
}

/// Read one proto3 JSON file into its typed message through the shared
/// decoder, so the CLI rejects what the console rejects. A file that
/// does not decode is rejected naming the file and the decoder's
/// message.
#[cfg(all(feature = "raft", feature = "tls"))]
fn read_message_json<M: prost::Message + Default>(
    path: &str,
    message_name: &str,
) -> Result<M, Box<dyn std::error::Error>> {
    let pool = pipestream_search::console::descriptor_pool();
    let descriptor = pool
        .get_message_by_name(message_name)
        .ok_or_else(|| format!("{message_name} is not in the compiled descriptor set"))?;
    let json = std::fs::read(path).map_err(|e| format!("raft-bootstrap: {path}: {e}"))?;
    let bytes = pipestream_search::console::json_to_message_bytes(&descriptor, &json)
        .map_err(|e| format!("raft-bootstrap: {path}: {e}"))?;
    let message = M::decode(bytes.as_slice())
        .map_err(|e| format!("raft-bootstrap: {path}: response bytes for {message_name}: {e}"))?;
    Ok(message)
}

/// `raft-bootstrap ... --policy=<file> --limits=<file>`: the one-time
/// creation of the first member. Local, not an RPC: it decodes the
/// policy and limits, refuses a directory that already has a store or a
/// log (each named with its own create's wording), bootstraps the
/// group, waits until the member leads, prints the status JSON, and
/// shuts the host down cleanly.
#[cfg(all(feature = "raft", feature = "tls"))]
async fn raft_bootstrap(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    use pipestream_search::raft::host::{LOG_FILE, STORE_FILE};
    use pipestream_search::raft::{operator, RaftHost};
    let (member, tls, client_tls) = pipestream_search::config::parse_raft_local(args)
        .unwrap_or_else(|e| cli_usage("raft-bootstrap", &e));
    let Some(member) = &member else {
        cli_usage(
            "raft-bootstrap",
            "raft-bootstrap needs --raft-dir and the other --raft-* options",
        );
    };
    let (policy_path, limits_path) =
        parse_raft_bootstrap_files(args).unwrap_or_else(|e| cli_usage("raft-bootstrap", &e));
    if member.dir.join(STORE_FILE).exists() {
        return Err("raft-bootstrap: source authority already exists".into());
    }
    if member.dir.join(LOG_FILE).exists() {
        return Err("raft-bootstrap: raft log store already exists".into());
    }
    std::fs::create_dir_all(&member.dir)
        .map_err(|e| format!("raft-bootstrap: member directory: {e}"))?;
    let policy = read_message_json::<pipestream_search::pb::AccessPolicy>(
        &policy_path,
        "ai.protomolt.search.v1.AccessPolicy",
    )?;
    let limits = read_message_json::<pipestream_search::pb::storage::SourceAuthorityLimits>(
        &limits_path,
        "ai.protomolt.search.storage.v1.SourceAuthorityLimits",
    )?;
    let identity = operator::identity(member);
    let host_config = operator::host_config(member).map_err(|e| format!("raft-bootstrap: {e}"))?;
    let transport = operator::cluster_transport(member, tls.as_ref(), client_tls.as_ref())
        .map_err(|e| format!("raft-bootstrap: {e}"))?;
    let host = RaftHost::bootstrap_cluster(
        &member.dir,
        &identity,
        member.node_id,
        &policy,
        &limits,
        &host_config,
        transport,
    )
    .await
    .map_err(|status| format!("raft-bootstrap: {}: {}", status.code(), status.message()))?;
    host.wait(Some(std::time::Duration::from_secs(30)))
        .state(openraft::ServerState::Leader, "raft-bootstrap leader")
        .await
        .map_err(|e| format!("raft-bootstrap: the member never led: {e}"))?;
    let status = pipestream_search::raft::operator_service::member_status(&host)
        .map_err(|status| format!("raft-bootstrap: {}: {}", status.code(), status.message()))?;
    print_message_json("ai.protomolt.search.v1.MemberStatus", &status)?;
    host.shutdown()
        .await
        .map_err(|status| format!("raft-bootstrap: {}: {}", status.code(), status.message()))?;
    Ok(())
}

/// The `raft-*` operator subcommands on a build without Raft: parsed
/// nowhere, attempted nowhere.
#[cfg(not(all(feature = "raft", feature = "tls")))]
async fn raft_operator_cli(
    command: &str,
    _args: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    Err(format!("{command} needs Raft support (features `raft` and `tls` are needed)").into())
}

/// The `raft-*` operator subcommands: status and membership over the
/// member's listener, bootstrap local.
#[cfg(all(feature = "raft", feature = "tls"))]
async fn raft_operator_cli(
    command: &str,
    args: &[String],
) -> Result<(), Box<dyn std::error::Error>> {
    match command {
        "raft-status" => raft_status(args).await,
        "raft-add-learner" => raft_add_learner(args).await,
        "raft-promote" => raft_promote(args).await,
        "raft-remove-member" => raft_remove_member(args).await,
        "raft-verify-image" => raft_verify_image(args).await,
        "raft-bootstrap" => raft_bootstrap(args).await,
        _ => cli_usage(command, "unknown raft subcommand"),
    }
}

/// Copy provider-owned construction state from one node to empty peers.
async fn configure_backend(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let get = |key: &str| {
        let prefix = format!("--{key}=");
        args.iter()
            .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
            .ok_or_else(|| format!("configure-backend requires --{key}"))
    };
    let normalize = |s: &str| {
        if s.starts_with("http://") || s.starts_with("https://") {
            s.to_string()
        } else {
            format!("http://{s}")
        }
    };
    let fit_from = normalize(&get("fit-from")?);
    let apply_to: Vec<String> = get("apply-to")?
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(normalize)
        .collect();
    if apply_to.is_empty() {
        return Err("configure-backend requires at least one --apply-to node".into());
    }

    let mut source = NodeServiceClient::new(secure_channel(&fit_from).await?);
    let backend = source
        .get_vector_backend(GetVectorBackendRequest {})
        .await?
        .into_inner();
    let descriptor = backend
        .descriptor
        .ok_or_else(|| format!("{fit_from} reports no vector backend"))?;
    let config = backend
        .config
        .ok_or_else(|| format!("{fit_from} reports no vector backend config"))?;
    println!(
        "fit-from {fit_from}: {} dim {} fingerprint {}, {} vectors",
        descriptor.backend_kind,
        descriptor.dim,
        descriptor.scoring_fingerprint,
        backend.num_vectors
    );

    for addr in &apply_to {
        let mut client = NodeServiceClient::new(secure_channel(addr).await?);
        let resp = client
            .configure_vector_backend(ConfigureVectorBackendRequest {
                dim: descriptor.dim,
                config: Some(config.clone()),
            })
            .await?
            .into_inner();
        println!(
            "apply-to {addr}: {}",
            if resp.already_configured {
                "already configured (idempotent)"
            } else {
                "vector backend configured"
            }
        );
    }
    Ok(())
}

/// A server builder under the configured listener TLS (`docs/security.md`):
/// node listeners demand a client certificate, the coordinator listener
/// accepts one and lets cluster control demand it per call.
fn secured_server(
    tls: Option<&pipestream_search::security::ServerTls>,
    require_client: bool,
) -> Result<Server, Box<dyn std::error::Error>> {
    match tls {
        None => Ok(Server::builder()),
        #[cfg(feature = "tls")]
        Some(tls) => Ok(Server::builder().tls_config(tls.server_config(require_client))?),
        #[cfg(not(feature = "tls"))]
        Some(_) => {
            let _ = require_client;
            Err("this build has no TLS support (feature `tls` is off)".into())
        }
    }
}

/// A channel to a cluster member under the process-wide client TLS
/// material when installed (`docs/security.md`).
async fn secure_channel(
    addr: &str,
) -> Result<tonic::transport::Channel, Box<dyn std::error::Error>> {
    let endpoint = tonic::transport::Endpoint::from_shared(
        pipestream_search::security::process_secure_url(addr),
    )?;
    let endpoint = pipestream_search::security::secure_endpoint(endpoint)?;
    Ok(endpoint.connect().await?)
}

/// One dataset's coordinator settings (`docs/collections.md`): the
/// unnamed dataset of a pre-collection configuration, or one named
/// collection.
struct CorpusSpec<'a> {
    name: &'a str,
    node_addrs: &'a [String],
    replica_addrs: &'a [Option<String>],
    shard_map: Option<&'a ShardMap>,
    shard_map_path: Option<&'a Path>,
    analysis_addr: Option<&'a String>,
    bm25_k1: f32,
    bm25_b: f32,
    dense_quality_profile: Option<&'a Path>,
    synonyms: Option<&'a Path>,
    dense_execution_policy: Option<&'a Path>,
    replica_state_path: Option<&'a Path>,
    control_state_path: Option<&'a Path>,
    clustered_turbovec: Option<&'a ClusteredTurboVecConfig>,
}

type TaskHandle = tokio::task::JoinHandle<Result<(), tonic::transport::Error>>;

/// Build one dataset's coordinator, its background tasks (shard-map
/// reload, replica sync, control reconciliation), and its control plane
/// when configured. A collection set contains one of these per collection.
async fn build_corpus(
    cfg: &Config,
    dataset: CorpusSpec<'_>,
    phrase_index: &Option<std::sync::Arc<pipestream_search::phrases::PhraseIndex>>,
    shutdown_rx: &tokio::sync::watch::Receiver<bool>,
    handles: &mut Vec<TaskHandle>,
) -> Result<(CoordinatorServiceImpl, Option<ClusterControlService>), Box<dyn std::error::Error>> {
    let to_duration = |ms: u64| (ms > 0).then(|| std::time::Duration::from_millis(ms));
    let mut coordinator = CoordinatorServiceImpl::new(dataset.node_addrs.to_vec())
        .with_bm25(
            dataset.analysis_addr.cloned(),
            pipestream_search::bm25::Bm25Params {
                k1: f64::from(dataset.bm25_k1),
                b: f64::from(dataset.bm25_b),
            },
        )
        .with_phrase_index(phrase_index.clone())
        .with_limits(pipestream_search::coordinator::FanoutLimits {
            shard_deadline: to_duration(cfg.shard_deadline_ms),
            hedge_delay: to_duration(cfg.hedge_delay_ms),
        })
        .with_replicas(dataset.replica_addrs.to_vec())
        .with_stream_search(cfg.stream_search)
        .with_bm25_stream(cfg.bm25_stream)
        .with_max_k(cfg.max_k)
        .with_signal_batch(cfg.signal_batch)
        .with_shard_pruning(cfg.shard_pruning)
        .with_max_rerank_bytes(cfg.max_rerank_bytes)
        .with_topology_generation(dataset.shard_map.map_or(0, |map| map.generation))
        .with_collection(dataset.name);
    if let Some(tls) = &cfg.client_tls {
        coordinator = coordinator.with_client_tls(tls.clone());
    }
    if let Some(key) = &cfg.udp_hmac_key {
        coordinator = coordinator.with_udp_hmac_key(key.clone());
    }
    if let Some(path) = dataset.dense_quality_profile {
        let profile = pipestream_search::quality::DenseQualityProfile::load(path)?;
        eprintln!(
            "dense quality profile: {} ({} measured queries)",
            path.display(),
            profile.measured_queries()
        );
        coordinator = coordinator.with_dense_quality_profile(profile);
    }
    if let Some(path) = dataset.synonyms {
        let table = pipestream_search::synonyms::SynonymTable::load(path)?;
        eprintln!("synonyms: {} ({} rules)", path.display(), table.len());
        coordinator = coordinator.with_synonyms(table);
    }
    if let Some(path) = dataset.dense_execution_policy {
        let policy = pipestream_search::dense_policy::DenseExecutionPolicy::load(path)?;
        eprintln!(
            "dense execution policy: {} ({}, {} measured queries, {} points)",
            path.display(),
            policy.policy_id(),
            policy.measured_queries(),
            policy.points().len()
        );
        coordinator = coordinator.with_dense_execution_policy(policy);
    }
    if let Some(clustered) = dataset.clustered_turbovec {
        let backend = match clustered {
            ClusteredTurboVecConfig::InProcess {
                nodes,
                state,
                allow_ephemeral,
            } => {
                let table = turbovec_grpc::NodeTable::parse(&nodes.join("\n"))?;
                let limits = turbovec_grpc::CoordinatorLimits {
                    max_k: cfg.max_k as usize,
                    ..Default::default()
                };
                let service = match state {
                    Some(path) => turbovec_grpc::CoordinatorService::with_state_file_and_limits(
                        table, path, limits,
                    )?,
                    None if *allow_ephemeral => {
                        turbovec_grpc::CoordinatorService::with_limits(table, limits)
                    }
                    None => unreachable!(
                        "configuration requires durable state or explicit ephemeral mode"
                    ),
                };
                ClusteredTurboVecBackend::in_process(service)
            }
            ClusteredTurboVecConfig::External { endpoint } => {
                ClusteredTurboVecBackend::external(endpoint, cfg.max_message_bytes)?
            }
        };
        eprintln!(
            "vector backend: clustered TurboVec ({} coordinator)",
            backend.transport_name()
        );
        coordinator = coordinator.with_clustered_turbovec(backend);
    }
    if let Some(map) = dataset.shard_map {
        let routes = topology_routes(map)?;
        let ranges = routes.iter().map(|route| route.hash_range).collect();
        let placement = map
            .placement
            .clone()
            .map(|tree| (tree, routes.iter().map(|route| route.placement).collect()));
        coordinator = coordinator.with_hot_topology_placed(ranges, placement)?;
        // The map's [[derived]] table is the index's declaration
        // (docs/derived-columns.md); the coordinator routes and scopes
        // with it for the life of the process.
        if !map.derived.is_empty() {
            let declaration = pipestream_search::derived::spec_from_config(&map.derived)
                .and_then(|spec| pipestream_search::derived::Declaration::compile(&spec))
                .map_err(|e| format!("shard map [[derived]]: {e}"))?;
            eprintln!("derived columns: declaration {}", declaration.fingerprint());
            coordinator = coordinator.with_derived(Some(std::sync::Arc::new(declaration)));
        }
        eprintln!(
            "shard map generation {} ({} shards)",
            map.generation,
            dataset.node_addrs.len()
        );
    }
    if cfg.shard_map_reload_ms > 0 {
        let path = dataset
            .shard_map_path
            .map(Path::to_path_buf)
            .expect("configuration validated a shard-map path");
        let reload = coordinator.clone();
        let reload_ms = cfg.shard_map_reload_ms;
        let mut shutdown = shutdown_rx.clone();
        handles.push(tokio::spawn(async move {
                let mut interval =
                    tokio::time::interval(std::time::Duration::from_millis(reload_ms));
                interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                loop {
                    tokio::select! {
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                break;
                            }
                        }
                        _ = interval.tick() => {
                            let candidate = match load_shard_map(&path) {
                                Ok(map) => map,
                                Err(error) => {
                                    eprintln!("shard-map reload refused: {error}");
                                    continue;
                                }
                            };
                            if candidate.generation <= reload.current_topology_generation() {
                                continue;
                            }
                            // A map that changes the derived-column declaration
                            // describes another index: a rebuild, not a reload.
                            let running: Vec<pipestream_search::derived::DerivedColumnConfig> = reload
                                .derived()
                                .map(|declaration| declaration.config())
                                .unwrap_or_default();
                            if candidate.derived != running {
                                eprintln!(
                                    "shard-map reload refused: generation {} changes the [[derived]] \
                                     declaration; a changed declaration is a rebuild through the \
                                     reshard tool and a restart (docs/derived-columns.md)",
                                    candidate.generation
                                );
                                continue;
                            }
                            let generation = candidate.generation;
                            match topology_routes(&candidate).and_then(|routes| {
                                reload.reload_topology(
                                    generation,
                                    routes,
                                    candidate.placement.as_ref(),
                                )
                            }) {
                                Ok(()) => eprintln!(
                                    "shard-map generation {generation} published atomically ({} shards)",
                                    candidate.shards.len()
                                ),
                                Err(error) => eprintln!("shard-map reload refused: {error}"),
                            }
                        }
                    }
                }
                Ok::<(), tonic::transport::Error>(())
            }));
    }
    if cfg.replica_sync_ms > 0 {
        let path = dataset
            .replica_state_path
            .map(Path::to_path_buf)
            .expect("configuration validated a replica-state path");
        let mut state = pipestream_search::replication::ReplicaState::load(&path)?;
        let topology = coordinator.clone();
        let sync_ms = cfg.replica_sync_ms;
        let mut shutdown = shutdown_rx.clone();
        handles.push(tokio::spawn(async move {
            let mut interval = tokio::time::interval(std::time::Duration::from_millis(sync_ms));
            interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            break;
                        }
                    }
                    _ = interval.tick() => {
                        for route in topology.current_topology_routes() {
                            let Some(replica) = route.replica else {
                                continue;
                            };
                            let prior = state.cursor_mut(&route.addr, &replica).clone();
                            match pipestream_search::replication::sync_once(&prior).await {
                                Ok(updated) => {
                                    *state.cursor_mut(&route.addr, &replica) = updated;
                                    if let Err(error) = state.write(&path) {
                                        eprintln!("replica cursor persistence failed: {error}");
                                    }
                                }
                                Err(error) => eprintln!(
                                    "replica catch-up {} -> {} refused: {error}",
                                    route.addr, replica
                                ),
                            }
                        }
                    }
                }
            }
            Ok::<(), tonic::transport::Error>(())
        }));
    }
    let control_service = if let Some(path) = dataset.control_state_path {
        let plane = DurableControlPlane::open(
            path,
            ControlPolicy {
                lease_ms: cfg.control_lease_ms,
                replication_factor: cfg.control_replication_factor,
                split_rows: cfg.control_split_rows,
                merge_rows: cfg.control_merge_rows,
                compact_segments: cfg.control_compact_segments,
                compact_tombstone_ppm: cfg.control_compact_tombstone_ppm,
                history_limit: 32,
            },
        )?
        .with_collection(dataset.name)?;
        plane.bootstrap_topology(
            coordinator.current_topology_generation(),
            &coordinator.current_topology_routes(),
        )?;
        let control = ClusterControlService::new(plane)
            .with_coordinator(coordinator.clone())
            .with_client_cert_required(cfg.tls.as_ref().is_some_and(|t| t.client_ca_pem.is_some()));
        control.publish_current_topology()?;
        if cfg.control_reconcile_ms > 0 {
            let reconcile = control.clone();
            let interval_ms = cfg.control_reconcile_ms;
            let mut shutdown = shutdown_rx.clone();
            handles.push(tokio::spawn(async move {
                    let mut interval =
                        tokio::time::interval(std::time::Duration::from_millis(interval_ms));
                    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                    loop {
                        tokio::select! {
                            changed = shutdown.changed() => {
                                if changed.is_err() || *shutdown.borrow() {
                                    break;
                                }
                            }
                            _ = interval.tick() => {
                                if let Err(error) = ClusterControl::reconcile_cluster(
                                    &reconcile,
                                    tonic::Request::new(ReconcileClusterRequest { collection: String::new(), dry_run: false }),
                                ).await {
                                    eprintln!("cluster reconciliation refused: {error}");
                                }
                            }
                        }
                    }
                    Ok::<(), tonic::transport::Error>(())
                }));
        }
        eprintln!("cluster control state: {}", path.display());
        Some(control)
    } else {
        None
    };
    Ok((coordinator, control_service))
}

/// The collection set this coordinator serves: the one unnamed dataset of
/// a pre-collection configuration, or every named collection, each with
/// its own coordinator and control plane (`docs/collections.md`).
/// Membership is verified against the nodes that answer; a node that
/// serves another collection refuses startup, an unreachable one is
/// reported and re-checked by cluster health.
async fn build_collections(
    cfg: &Config,
    phrase_index: &Option<std::sync::Arc<pipestream_search::phrases::PhraseIndex>>,
    shutdown_rx: &tokio::sync::watch::Receiver<bool>,
    handles: &mut Vec<TaskHandle>,
    addr: SocketAddr,
) -> Result<(CollectionSet, Option<ClusterControlSet>), Box<dyn std::error::Error>> {
    let (search_set, control_set) = if cfg.collections.is_empty() {
        let (coordinator, control) = build_corpus(
            cfg,
            CorpusSpec {
                name: "",
                node_addrs: &cfg.node_addrs,
                replica_addrs: &cfg.replica_addrs,
                shard_map: cfg.shard_map.as_ref(),
                shard_map_path: cfg.shard_map_path.as_deref(),
                analysis_addr: cfg.analysis_addr.as_ref(),
                bm25_k1: cfg.bm25_k1,
                bm25_b: cfg.bm25_b,
                dense_quality_profile: cfg.dense_quality_profile.as_deref(),
                synonyms: cfg.synonyms.as_deref(),
                dense_execution_policy: cfg.dense_execution_policy.as_deref(),
                replica_state_path: cfg.replica_state_path.as_deref(),
                control_state_path: cfg.control_state_path.as_deref(),
                clustered_turbovec: cfg.clustered_turbovec.as_ref(),
            },
            phrase_index,
            shutdown_rx,
            handles,
        )
        .await?;
        eprintln!(
            "SearchService listening on {addr} ({} shard nodes)",
            cfg.node_addrs.len()
        );
        let set = CollectionSet::single(coordinator);
        let set = match &cfg.principals {
            Some(p) => set.with_principals(p.clone()),
            None => set,
        };
        (set, control.map(ClusterControlSet::single))
    } else {
        let mut members = Vec::with_capacity(cfg.collections.len());
        let mut controls = Vec::new();
        for c in &cfg.collections {
            let (coordinator, control) = build_corpus(
                cfg,
                CorpusSpec {
                    name: &c.name,
                    node_addrs: &c.node_addrs,
                    replica_addrs: &c.replica_addrs,
                    shard_map: c.shard_map.as_ref(),
                    shard_map_path: c.shard_map_path.as_deref(),
                    analysis_addr: c.analysis_addr.as_ref(),
                    bm25_k1: c.bm25_k1,
                    bm25_b: c.bm25_b,
                    dense_quality_profile: c.dense_quality_profile.as_deref(),
                    synonyms: c.synonyms.as_deref(),
                    dense_execution_policy: c.dense_execution_policy.as_deref(),
                    replica_state_path: c.replica_state_path.as_deref(),
                    control_state_path: c.control_state_path.as_deref(),
                    clustered_turbovec: None,
                },
                phrase_index,
                shutdown_rx,
                handles,
            )
            .await?;
            eprintln!(
                "collection {:?}: {} shard nodes{}",
                c.name,
                c.node_addrs.len(),
                if control.is_some() {
                    ", durable control"
                } else {
                    ""
                }
            );
            members.push((c.name.clone(), coordinator));
            if let Some(control) = control {
                controls.push((c.name.clone(), control));
            }
        }
        let set = CollectionSet::named(members, cfg.default_collection.clone())?;
        let set = match &cfg.principals {
            Some(p) => set.with_principals(p.clone()),
            None => set,
        };
        let control = if controls.is_empty() {
            None
        } else {
            Some(ClusterControlSet::named(
                controls,
                cfg.default_collection.clone(),
            )?)
        };
        eprintln!(
            "SearchService listening on {addr} (collections {:?}, default {:?})",
            set.names(),
            cfg.default_collection
        );
        (set, control)
    };
    match search_set.verify_membership().await {
        Ok(()) => {}
        Err(status) if status.code() == tonic::Code::Unavailable => {
            eprintln!(
                "collection membership not verified at start ({}); cluster health re-checks it",
                status.message()
            );
        }
        Err(status) => return Err(format!("collection membership: {}", status.message()).into()),
    }
    Ok((search_set, control_set))
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    if matches!(
        argv.first().map(String::as_str),
        Some("configure-backend" | "calibrate")
    ) {
        return configure_backend(&argv[1..]).await;
    }
    if argv.first().map(String::as_str) == Some("raft-prepare") {
        return raft_prepare(&argv[1..]);
    }
    if matches!(
        argv.first().map(String::as_str),
        Some(
            "raft-status"
                | "raft-add-learner"
                | "raft-promote"
                | "raft-remove-member"
                | "raft-verify-image"
                | "raft-bootstrap"
        )
    ) {
        let command = argv[0].clone();
        return raft_operator_cli(&command, &argv[1..]).await;
    }
    let cfg = parse(&argv)?;
    run(cfg).await
}

#[cfg(all(test, feature = "raft", feature = "tls"))]
mod cli_tests {
    use super::*;

    fn args(words: &[&str]) -> Vec<String> {
        words.iter().map(|w| (*w).to_string()).collect()
    }

    #[test]
    fn status_parses_its_addr() {
        assert_eq!(
            parse_raft_status(&args(&["--addr=127.0.0.1:50051"])),
            Ok("127.0.0.1:50051".to_string())
        );
        assert!(parse_raft_status(&args(&[])).is_err());
    }

    #[test]
    fn add_learner_parses_its_target() {
        assert_eq!(
            parse_raft_add_learner(&args(&[
                "--addr=127.0.0.1:50051",
                "--node-id=4",
                "--node-addr=127.0.0.1:50151"
            ])),
            Ok((
                "127.0.0.1:50051".to_string(),
                4,
                "127.0.0.1:50151".to_string()
            ))
        );
        assert!(parse_raft_add_learner(&args(&["--addr=127.0.0.1:50051", "--node-id=4"])).is_err());
        assert!(parse_raft_add_learner(&args(&[
            "--addr=127.0.0.1:50051",
            "--node-id=four",
            "--node-addr=127.0.0.1:50151"
        ]))
        .is_err());
    }

    #[test]
    fn promote_parses_a_learner_list() {
        assert_eq!(
            parse_raft_promote(&args(&["--addr=127.0.0.1:50051", "--node-id=4,5"])),
            Ok(("127.0.0.1:50051".to_string(), vec![4, 5]))
        );
        assert!(parse_raft_promote(&args(&["--addr=127.0.0.1:50051"])).is_err());
        assert!(parse_raft_promote(&args(&["--addr=127.0.0.1:50051", "--node-id=4,x"])).is_err());
    }

    #[test]
    fn remove_member_parses_its_target() {
        assert_eq!(
            parse_raft_remove_member(&args(&["--addr=127.0.0.1:50051", "--node-id=4"])),
            Ok(("127.0.0.1:50051".to_string(), 4))
        );
        assert!(parse_raft_remove_member(&args(&["--addr=127.0.0.1:50051"])).is_err());
    }

    #[test]
    fn verify_image_parses_its_member() {
        assert_eq!(
            parse_raft_verify_image(&args(&["--addr=127.0.0.1:50051"])),
            Ok("127.0.0.1:50051".to_string())
        );
        assert!(parse_raft_verify_image(&args(&[])).is_err());
    }

    #[test]
    fn bootstrap_parses_its_two_files() {
        assert_eq!(
            parse_raft_bootstrap_files(&args(&[
                "--raft-dir=/tmp/raft0",
                "--policy=/tmp/policy.json",
                "--limits=/tmp/limits.json"
            ])),
            Ok((
                "/tmp/policy.json".to_string(),
                "/tmp/limits.json".to_string()
            ))
        );
        assert!(parse_raft_bootstrap_files(&args(&["--policy=/tmp/policy.json"])).is_err());
    }
}
