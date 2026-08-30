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
//! pipestream-search --role=both --demo-vectors=20000 \
//!     --nodes=127.0.0.1:50051 --demo-query
//!
//! # Static two-machine cluster (see README "Two-machine runbook").
//! # host-a:    pipestream-search --config /etc/turbovec/host-a.toml
//! # host-b:   pipestream-search --config /etc/turbovec/host-b.toml
//! ```

use std::net::SocketAddr;
use std::path::Path;

use pipestream_search::clustered_turbovec::ClusteredTurboVecBackend;
use pipestream_search::config::{
    parse, ClusteredTurboVecConfig, Config, DemoConfig, Role, ShardConfig,
};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::harness;
use pipestream_search::node::{NodeConfig, NodeServiceImpl};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_client::SearchServiceClient;
use pipestream_search::pb::{
    ConfigureVectorBackendRequest, GetVectorBackendRequest, SearchRequest,
};
use pipestream_search::vector::VectorIndex;
use tokio::net::TcpListener;
use tonic::transport::Server;

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
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    tokio::spawn(async move {
        shutdown_signal().await;
        let _ = shutdown_tx.send(true);
    });

    let mut handles = Vec::new();
    let mut node_services = Vec::new();

    if matches!(cfg.role, Role::Node | Role::Both) {
        for shard in &cfg.shards {
            // Snapshot generations take precedence over the legacy layout
            // (and recover any interrupted swap) before anything loads.
            let generation = shard
                .index_path
                .as_ref()
                .and_then(|p| pipestream_search::node::recover_generation(p));
            let index = load_shard_index(shard, generation.as_deref())?;
            match &index {
                Some(index) => eprintln!(
                    "shard @{}: {} vectors, dim {:?}, {} bits, slot offset {}",
                    shard.listen,
                    index.len(),
                    index.dim_opt(),
                    index.bits_per_dimension().unwrap_or_default(),
                    shard.slot_offset
                ),
                None => eprintln!(
                    "shard @{}: no index at {}; starting empty (awaiting ConfigureVectorBackend/AddVectors)",
                    shard.listen,
                    shard.index_path.as_ref().map(|p| p.display().to_string()).unwrap_or_default()
                ),
            }
            let listener = TcpListener::bind(shard.listen).await?;
            let addr: SocketAddr = listener.local_addr()?;
            let mut bm25_store = None;
            if let Some(p) = shard.index_path.as_ref() {
                let bm25_path = match &generation {
                    Some(dir) => pipestream_search::node::generation_bm25(dir),
                    None => pipestream_search::node::bm25_sidecar_path(p),
                };
                if bm25_path.exists() {
                    eprintln!(
                        "shard @{}: loading BM25 store from {}",
                        shard.listen,
                        bm25_path.display()
                    );
                    bm25_store = Some(
                        pipestream_search::node::Bm25Shard::open(&bm25_path)
                            .unwrap_or_else(|e| panic!("load {}: {e}", bm25_path.display())),
                    );
                } else if pipestream_search::node::bm25_build_dir(&bm25_path).exists()
                    && !cfg.allow_missing_bm25
                {
                    // A spill directory with no .bm25 beside it means a
                    // bulk build was interrupted: Flush removes the
                    // directory on success, so this state cannot be
                    // reached by a shard that finished. Serving it anyway
                    // is the bad kind of quiet -- the node reports
                    // healthy, answers vector queries normally, and
                    // contributes nothing to every lexical query, so the
                    // fleet ranks against a corpus short one shard's
                    // share with nothing anywhere saying so.
                    //
                    // A shard with no build directory and no .bm25 is NOT
                    // refused: that is exactly what a vector-only
                    // deployment looks like, and it is a real one.
                    return Err(format!(
                        "shard @{}: BM25 build directory {} exists but {} does not. \
                         A bulk build was interrupted; this shard would answer lexical \
                         queries with silence, which is indistinguishable from a corpus \
                         that genuinely lacks those terms. Re-run the ingest for this \
                         shard, or pass --allow-missing-bm25 to serve it vector-only \
                         on purpose.",
                        shard.listen,
                        pipestream_search::node::bm25_build_dir(&bm25_path).display(),
                        bm25_path.display()
                    )
                    .into());
                }
            }
            let node = NodeServiceImpl::new(
                index,
                NodeConfig {
                    vector_backend: shard.vector_backend.clone(),
                    slot_offset: shard.slot_offset,
                    chunk_blocks: cfg.chunk_blocks,
                    share_floors: cfg.share_floors,
                    block_max: cfg.block_max,
                    coalesce: cfg.coalesce,
                    scan_parallel: cfg.scan_parallel,
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
                    geo_fields: cfg.geo_fields.clone(),
                    wal: shard.wal,
                    wal_buckets: shard.wal_buckets,
                    vocab: shard.vocab,
                    vocab_window_docs: cfg.vocab_window_docs,
                    vocab_top_k: cfg.vocab_top_k,
                },
            )
            .with_bm25(bm25_store)
            .with_generation(generation);
            // The UDP stream-signal lane shares the gRPC listener's host:port.
            node.spawn_floor_listener(addr);
            node_services.push(node.clone());
            eprintln!("NodeService listening on {addr}");
            let max = cfg.max_message_bytes;
            let mut shutdown = shutdown_rx.clone();
            handles.push(tokio::spawn(
                Server::builder()
                    .initial_stream_window_size(pipestream_search::H2_STREAM_WINDOW)
                    .initial_connection_window_size(pipestream_search::H2_CONN_WINDOW)
                    .add_service(NodeServiceImpl::into_server(node, max))
                    .serve_with_incoming_shutdown(
                        harness::nodelay_incoming(listener),
                        async move {
                            let _ = shutdown.wait_for(|v| *v).await;
                        },
                    ),
            ));
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

    if matches!(cfg.role, Role::Coordinator | Role::Both) {
        let listener = TcpListener::bind(cfg.coord_listen).await?;
        let addr: SocketAddr = listener.local_addr()?;
        let to_duration = |ms: u64| (ms > 0).then(|| std::time::Duration::from_millis(ms));
        let mut coordinator = CoordinatorServiceImpl::new(cfg.node_addrs.clone())
            .with_bm25(
                cfg.analysis_addr.clone(),
                pipestream_search::bm25::Bm25Params {
                    k1: f64::from(cfg.bm25_k1),
                    b: f64::from(cfg.bm25_b),
                },
            )
            .with_limits(pipestream_search::coordinator::FanoutLimits {
                shard_deadline: to_duration(cfg.shard_deadline_ms),
                hedge_delay: to_duration(cfg.hedge_delay_ms),
            })
            .with_replicas(cfg.replica_addrs.clone())
            .with_stream_search(cfg.stream_search)
            .with_bm25_stream(cfg.bm25_stream)
            .with_max_k(cfg.max_k);
        if let Some(clustered) = &cfg.clustered_turbovec {
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
                        Some(path) => {
                            turbovec_grpc::CoordinatorService::with_state_file_and_limits(
                                table, path, limits,
                            )?
                        }
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
        if let Some(map) = &cfg.shard_map {
            eprintln!(
                "shard map generation {} ({} shards)",
                map.generation,
                cfg.node_addrs.len()
            );
        }
        eprintln!(
            "SearchService listening on {addr} ({} shard nodes)",
            cfg.node_addrs.len()
        );
        let max = cfg.max_message_bytes;
        let mut shutdown = shutdown_rx.clone();
        handles.push(tokio::spawn(
            Server::builder()
                .initial_stream_window_size(pipestream_search::H2_STREAM_WINDOW)
                .initial_connection_window_size(pipestream_search::H2_CONN_WINDOW)
                .add_service(CoordinatorServiceImpl::into_server(coordinator, max))
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

    let mut source = NodeServiceClient::connect(fit_from.clone()).await?;
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
        let mut client = NodeServiceClient::connect(addr.clone()).await?;
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
