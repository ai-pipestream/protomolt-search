//! turbovec-search binary: one process, one or both roles, one or more
//! shards.
//!
//! Configuration: `--config cluster.toml` (TOML file) with `TURBOVEC_*`
//! env overrides and `--key=value` flags on top (see `src/config.rs`).
//!
//! Subcommand: `turbovec-search calibrate --fit-from=host:port
//! --apply-to=h1:p1,h2:p2` reads the calibration from one seeded node and
//! pushes it to the others (see the README "ingest flow").
//!
//! Examples:
//!
//! ```text
//! # Single-process demo: coordinator + node + random demo corpus,
//! # then one self-issued search against itself.
//! turbovec-search --role=both --demo-vectors=20000 \
//!     --nodes=127.0.0.1:50051 --demo-query
//!
//! # Static two-machine cluster (see README "Two-machine runbook").
//! # host-a:    turbovec-search --config /etc/turbovec/host-a.toml
//! # krick-1:   turbovec-search --config /etc/turbovec/krick-1.toml
//! ```

use std::net::SocketAddr;
use std::path::Path;

use tokio::net::TcpListener;
use tonic::transport::Server;
use turbovec::TurboQuantIndex;
use turbovec_search::config::{parse, Config, DemoConfig, Role, ShardConfig};
use turbovec_search::coordinator::CoordinatorServiceImpl;
use turbovec_search::harness;
use turbovec_search::node::{NodeConfig, NodeServiceImpl};
use turbovec_search::pb::node_service_client::NodeServiceClient;
use turbovec_search::pb::search_service_client::SearchServiceClient;
use turbovec_search::pb::{GetCalibrationRequest, SearchRequest, SetCalibrationRequest};

/// Build a demo index: fit TQ+ calibration on a 20% sample (min 1024
/// vectors), then construct the real index seeded with it — the same flow a
/// multi-shard deployment uses to keep shard scores comparable.
fn build_demo_index(demo: DemoConfig) -> Result<TurboQuantIndex, String> {
    let corpus = harness::unit_vectors(demo.vectors, demo.dim, 0xDE10_0001);
    let sample_n = (demo.vectors / 5).max(1).min(demo.vectors);
    let mut fitting = TurboQuantIndex::new(demo.dim, demo.bit_width)
        .map_err(|e| format!("demo index construct: {e}"))?;
    fitting.add(&corpus[..sample_n * demo.dim]);
    let (shift, scale) = fitting
        .calibration()
        .ok_or_else(|| "calibration fitting produced nothing".to_string())?;
    let mut index = TurboQuantIndex::new_with_calibration(demo.dim, demo.bit_width, shift, scale)
        .map_err(|e| format!("seeded index construct: {e}"))?;
    index.add(&corpus);
    index.prepare();
    Ok(index)
}

/// Load a shard's index, or `None` when the shard starts empty: its index
/// path does not exist yet (a from-scratch shard awaiting SetCalibration +
/// AddVectors; Flush later writes exactly that path).
fn load_shard_index(shard: &ShardConfig) -> Result<Option<TurboQuantIndex>, String> {
    if let Some(demo) = shard.demo {
        eprintln!(
            "building demo index: {} vectors x dim {} @ {} bits",
            demo.vectors, demo.dim, demo.bit_width
        );
        return build_demo_index(demo).map(Some);
    }
    let path = shard
        .index_path
        .as_ref()
        .expect("config validated an index source");
    if !Path::new(path).exists() {
        return Ok(None);
    }
    let index = TurboQuantIndex::load(path).map_err(|e| format!("load {}: {e}", path.display()))?;
    index.prepare();
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
            let index = load_shard_index(shard)?;
            match &index {
                Some(index) => eprintln!(
                    "shard @{}: {} vectors, dim {:?}, {} bits, slot offset {}",
                    shard.listen,
                    index.len(),
                    index.dim_opt(),
                    index.bit_width(),
                    shard.slot_offset
                ),
                None => eprintln!(
                    "shard @{}: no index at {}; starting empty (awaiting SetCalibration/AddVectors)",
                    shard.listen,
                    shard.index_path.as_ref().map(|p| p.display().to_string()).unwrap_or_default()
                ),
            }
            let listener = TcpListener::bind(shard.listen).await?;
            let addr: SocketAddr = listener.local_addr()?;
            let bm25_store = shard.index_path.as_ref().and_then(|p| {
                let bm25_path = turbovec_search::node::bm25_sidecar_path(p);
                if bm25_path.exists() {
                    eprintln!(
                        "shard @{}: loading BM25 store from {}",
                        shard.listen,
                        bm25_path.display()
                    );
                    Some(
                        turbovec_search::node::Bm25Shard::open(&bm25_path)
                            .unwrap_or_else(|e| panic!("load {}: {e}", bm25_path.display())),
                    )
                } else {
                    None
                }
            });
            let node = NodeServiceImpl::new(
                index,
                NodeConfig {
                    slot_offset: shard.slot_offset,
                    chunk_blocks: cfg.chunk_blocks,
                    share_floors: cfg.share_floors,
                    bit_width: cfg.bit_width,
                    index_path: shard.index_path.clone(),
                    analysis_addr: shard.analysis_addr.clone(),
                },
            )
            .with_bm25(bm25_store);
            node_services.push(node.clone());
            eprintln!("NodeService listening on {addr}");
            let max = cfg.max_message_bytes;
            let mut shutdown = shutdown_rx.clone();
            handles.push(tokio::spawn(
                Server::builder()
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

    if matches!(cfg.role, Role::Coordinator | Role::Both) {
        let listener = TcpListener::bind(cfg.coord_listen).await?;
        let addr: SocketAddr = listener.local_addr()?;
        let coordinator = CoordinatorServiceImpl::new(cfg.node_addrs.clone()).with_bm25(
            cfg.analysis_addr.clone(),
            turbovec_search::bm25::Bm25Params {
                k1: f64::from(cfg.bm25_k1),
                b: f64::from(cfg.bm25_b),
            },
        );
        eprintln!(
            "SearchService listening on {addr} ({} shard nodes)",
            cfg.node_addrs.len()
        );
        let max = cfg.max_message_bytes;
        let mut shutdown = shutdown_rx.clone();
        handles.push(tokio::spawn(
            Server::builder()
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
    Ok(())
}

/// `turbovec-search calibrate --fit-from=host:port --apply-to=a,b`:
/// read the calibration from one seeded node and push it to the others.
async fn calibrate(args: &[String]) -> Result<(), Box<dyn std::error::Error>> {
    let get = |key: &str| {
        let prefix = format!("--{key}=");
        args.iter()
            .find_map(|a| a.strip_prefix(&prefix).map(str::to_string))
            .ok_or_else(|| format!("calibrate requires --{key}"))
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
        return Err("calibrate requires at least one --apply-to node".into());
    }

    let mut source = NodeServiceClient::connect(fit_from.clone()).await?;
    let cal = source
        .get_calibration(GetCalibrationRequest {})
        .await?
        .into_inner();
    if cal.shift.is_empty() {
        return Err(format!("{fit_from} reports no locked calibration").into());
    }
    println!(
        "fit-from {fit_from}: dim {} @ {} bits, {} vectors",
        cal.dim, cal.bit_width, cal.num_vectors
    );

    for addr in &apply_to {
        let mut client = NodeServiceClient::connect(addr.clone()).await?;
        let resp = client
            .set_calibration(SetCalibrationRequest {
                dim: cal.dim,
                bit_width: cal.bit_width,
                shift: cal.shift.clone(),
                scale: cal.scale.clone(),
            })
            .await?
            .into_inner();
        println!(
            "apply-to {addr}: {}",
            if resp.already_seeded {
                "already seeded (idempotent)"
            } else {
                "calibration locked"
            }
        );
    }
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    if argv.first().map(String::as_str) == Some("calibrate") {
        return calibrate(&argv[1..]).await;
    }
    let cfg = parse(&argv)?;
    run(cfg).await
}
