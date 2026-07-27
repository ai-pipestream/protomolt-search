//! turbovec-search binary: one process, one or both roles.
//!
//! Examples:
//!
//! ```text
//! # Single-process demo: coordinator + node + random demo corpus,
//! # then one self-issued search against itself.
//! turbovec-search --role=both --demo-vectors=20000 \
//!     --nodes=127.0.0.1:50051 --demo-query
//!
//! # A real shard node over a persisted .tv index.
//! turbovec-search --role=node --index=/data/shard-0.tv \
//!     --slot-offset=0 --node-listen=0.0.0.0:50051
//!
//! # A coordinator over three nodes.
//! turbovec-search --role=coordinator --coord-listen=0.0.0.0:50050 \
//!     --nodes=node0:50051,node1:50051,node2:50051
//! ```

use std::net::SocketAddr;
use std::sync::Arc;

use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use turbovec::TurboQuantIndex;
use turbovec_search::config::{parse, Config, DemoConfig, Role};
use turbovec_search::coordinator::CoordinatorServiceImpl;
use turbovec_search::node::{NodeConfig, NodeServiceImpl};
use turbovec_search::pb::node_service_server::NodeServiceServer;
use turbovec_search::pb::search_service_client::SearchServiceClient;
use turbovec_search::pb::search_service_server::SearchServiceServer;
use turbovec_search::pb::SearchRequest;

/// Deterministic pseudo-random unit vectors (LCG + normalize) for the demo
/// corpus. Fixed seed so demo runs are reproducible.
fn unit_vectors(n: usize, dim: usize, seed: u64) -> Vec<f32> {
    let mut s = seed.wrapping_add(0x9E3779B97F4A7C15);
    let mut out = vec![0.0f32; n * dim];
    for row in out.chunks_mut(dim) {
        let mut norm = 0.0f64;
        for x in row.iter_mut() {
            s = s
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            let v = ((s >> 33) as f64 / (1u64 << 31) as f64) - 1.0;
            *x = v as f32;
            norm += v * v;
        }
        let inv = 1.0 / (norm.sqrt() + 1e-9);
        for x in row.iter_mut() {
            *x = (*x as f64 * inv) as f32;
        }
    }
    out
}

/// Build a demo index: fit TQ+ calibration on a 20% sample (min 1024
/// vectors), then construct the real index seeded with it — the same flow a
/// multi-shard deployment uses to keep shard scores comparable.
fn build_demo_index(demo: DemoConfig) -> Result<TurboQuantIndex, String> {
    let corpus = unit_vectors(demo.vectors, demo.dim, 0xDE10_0001);
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

fn load_index(cfg: &Config) -> Result<Arc<TurboQuantIndex>, String> {
    if let Some(demo) = cfg.demo {
        eprintln!(
            "building demo index: {} vectors x dim {} @ {} bits",
            demo.vectors, demo.dim, demo.bit_width
        );
        return build_demo_index(demo).map(Arc::new);
    }
    let path = cfg
        .index_path
        .as_ref()
        .expect("config validated an index source");
    let index = TurboQuantIndex::load(path).map_err(|e| format!("load {}: {e}", path.display()))?;
    index.prepare();
    Ok(Arc::new(index))
}

async fn serve(
    listener: TcpListener,
    server: tonic::transport::server::Router,
) -> Result<(), tonic::transport::Error> {
    server
        .serve_with_incoming(TcpListenerStream::new(listener))
        .await
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let demo_query = argv.iter().any(|a| a == "--demo-query");
    let cfg = parse(&argv)?;

    let mut handles = Vec::new();

    if matches!(cfg.role, Role::Node | Role::Both) {
        let index = load_index(&cfg)?;
        eprintln!(
            "node: {} vectors, dim {:?}, {} bits, slot offset {}",
            index.len(),
            index.dim_opt(),
            index.bit_width(),
            cfg.slot_offset
        );
        let listener = TcpListener::bind(cfg.node_listen).await?;
        let addr: SocketAddr = listener.local_addr()?;
        let node = NodeServiceImpl::new(
            index,
            NodeConfig {
                slot_offset: cfg.slot_offset,
                chunk_blocks: cfg.chunk_blocks,
                share_floors: cfg.share_floors,
            },
        );
        eprintln!("NodeService listening on {addr}");
        handles.push(tokio::spawn(serve(
            listener,
            Server::builder().add_service(NodeServiceServer::new(node)),
        )));
    }

    if matches!(cfg.role, Role::Coordinator | Role::Both) {
        let listener = TcpListener::bind(cfg.coord_listen).await?;
        let addr: SocketAddr = listener.local_addr()?;
        let coordinator = CoordinatorServiceImpl::new(cfg.node_addrs.clone());
        eprintln!(
            "SearchService listening on {addr} ({} shard nodes)",
            cfg.node_addrs.len()
        );
        handles.push(tokio::spawn(serve(
            listener,
            Server::builder().add_service(SearchServiceServer::new(coordinator)),
        )));
    }

    if demo_query {
        let query = unit_vectors(1, cfg.demo.map(|d| d.dim).unwrap_or(128), 0x0E0E_0001);
        let endpoint = format!("http://127.0.0.1:{}", cfg.coord_listen.port());
        let mut client = SearchServiceClient::connect(endpoint).await?;
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
