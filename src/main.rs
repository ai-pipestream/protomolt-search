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
                integer_fields: cfg.integer_fields.clone(),
                placement_column: cfg.placement_column.clone(),
                placement_leaf: cfg.placement_leaf,
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
            handles.push(tokio::spawn(
                secured_server(cfg.tls.as_ref(), true)?
                    .initial_stream_window_size(pipestream_search::H2_STREAM_WINDOW)
                    .initial_connection_window_size(pipestream_search::H2_CONN_WINDOW)
                    .add_service(NodeServiceImpl::into_server(node, max))
                    .add_service(diagnostics)
                    .serve_with_incoming_shutdown(
                        harness::nodelay_incoming(listener),
                        async move {
                            let _ = shutdown.wait_for(|v| *v).await;
                        },
                    ),
            ));
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
            integer_fields: cfg.integer_fields.clone(),
            placement_column: cfg.placement_column.clone(),
            placement_leaf: cfg.placement_leaf,
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
        eprintln!(
            "metrics on http://{bound}/metrics ({} shard gauges)",
            gauges.len()
        );
        handles.push(tokio::spawn(async move {
            pipestream_search::metrics::serve(listener, gauges).await;
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
        let relay = pipestream_search::relay::RelayService::new(std::sync::Arc::new(coordinator));
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
        handles.push(tokio::spawn(
            secured_server(cfg.tls.as_ref(), true)?
                .initial_stream_window_size(pipestream_search::H2_STREAM_WINDOW)
                .initial_connection_window_size(pipestream_search::H2_CONN_WINDOW)
                .add_service(relay.into_server(max))
                .serve_with_incoming_shutdown(harness::nodelay_incoming(listener), async move {
                    let _ = shutdown.wait_for(|v| *v).await;
                }),
        ));
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
        let diagnostics = search_set
            .diagnostics()
            .with_gauges(gauges)
            .into_server(max);
        handles.push(tokio::spawn(
            secured_server(cfg.tls.as_ref(), false)?
                .initial_stream_window_size(pipestream_search::H2_STREAM_WINDOW)
                .initial_connection_window_size(pipestream_search::H2_CONN_WINDOW)
                .add_optional_service(control_set.map(|set| set.into_server(max)))
                .add_service(search_set.into_server(max))
                .add_service(diagnostics)
                .serve_with_incoming_shutdown(harness::nodelay_incoming(listener), async move {
                    let _ = shutdown.wait_for(|v| *v).await;
                }),
        ));
    }

    if cfg.demo_query {
        let query = harness::unit_vectors(1, cfg.query_dim, 0x0E0E_0001);
        let endpoint = format!("http://127.0.0.1:{}", cfg.coord_listen.port());
        let mut client = SearchServiceClient::connect(endpoint)
            .await?
            .max_decoding_message_size(cfg.max_message_bytes)
            .max_encoding_message_size(cfg.max_message_bytes);
        let response = client
            .search(SearchRequest {
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
    let cfg = parse(&argv)?;
    run(cfg).await
}
