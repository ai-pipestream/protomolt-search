//! turbovec-search binary: one process, one or both roles, one or more
//! shards.
//!
//! Configuration: `--config cluster.toml` (TOML file) with `TURBOVEC_*`
//! env overrides and `--key=value` flags on top (see `src/config.rs`).
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
use std::sync::Arc;

use tokio::net::TcpListener;
use tonic::transport::Server;
use turbovec::TurboQuantIndex;
use turbovec_search::config::{parse, Config, DemoConfig, Role, ShardConfig};
use turbovec_search::coordinator::CoordinatorServiceImpl;
use turbovec_search::harness;
use turbovec_search::node::{NodeConfig, NodeServiceImpl};
use turbovec_search::pb::search_service_client::SearchServiceClient;
use turbovec_search::pb::SearchRequest;

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

fn load_shard_index(shard: &ShardConfig) -> Result<Arc<TurboQuantIndex>, String> {
    if let Some(demo) = shard.demo {
        eprintln!(
            "building demo index: {} vectors x dim {} @ {} bits",
            demo.vectors, demo.dim, demo.bit_width
        );
        return build_demo_index(demo).map(Arc::new);
    }
    let path = shard
        .index_path
        .as_ref()
        .expect("config validated an index source");
    let index = TurboQuantIndex::load(path).map_err(|e| format!("load {}: {e}", path.display()))?;
    index.prepare();
    Ok(Arc::new(index))
}

async fn run(cfg: Config) -> Result<(), Box<dyn std::error::Error>> {
    let mut handles = Vec::new();

    if matches!(cfg.role, Role::Node | Role::Both) {
        for shard in &cfg.shards {
            let index = load_shard_index(shard)?;
            eprintln!(
                "shard @{}: {} vectors, dim {:?}, {} bits, slot offset {}",
                shard.listen,
                index.len(),
                index.dim_opt(),
                index.bit_width(),
                shard.slot_offset
            );
            let listener = TcpListener::bind(shard.listen).await?;
            let addr: SocketAddr = listener.local_addr()?;
            let node = NodeServiceImpl::new(
                index,
                NodeConfig {
                    slot_offset: shard.slot_offset,
                    chunk_blocks: cfg.chunk_blocks,
                    share_floors: cfg.share_floors,
                },
            );
            eprintln!("NodeService listening on {addr}");
            let max = cfg.max_message_bytes;
            handles.push(tokio::spawn(
                Server::builder()
                    .add_service(NodeServiceImpl::into_server(node, max))
                    .serve_with_incoming(harness::nodelay_incoming(listener)),
            ));
        }
    }

    if matches!(cfg.role, Role::Coordinator | Role::Both) {
        let listener = TcpListener::bind(cfg.coord_listen).await?;
        let addr: SocketAddr = listener.local_addr()?;
        let coordinator = CoordinatorServiceImpl::new(cfg.node_addrs.clone());
        eprintln!(
            "SearchService listening on {addr} ({} shard nodes)",
            cfg.node_addrs.len()
        );
        let max = cfg.max_message_bytes;
        handles.push(tokio::spawn(
            Server::builder()
                .add_service(CoordinatorServiceImpl::into_server(coordinator, max))
                .serve_with_incoming(harness::nodelay_incoming(listener)),
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
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let cfg = parse(&argv)?;
    run(cfg).await
}
