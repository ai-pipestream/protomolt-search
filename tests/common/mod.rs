//! Shared harness for the integration tests: deterministic corpora,
//! calibration fitting, shard partitioning, and in-process gRPC servers on
//! loopback (port 0).
//!
//! Everything is seeded: corpora, queries, and shard builds are fully
//! deterministic, so assertions are exact (bitwise score equality, exact id
//! sequences) rather than probabilistic.
#![allow(dead_code)]

use std::sync::Arc;

use tokio::net::TcpListener;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Error as TransportError, Server};
use turbovec::TurboQuantIndex;
use turbovec_search::coordinator::CoordinatorServiceImpl;
use turbovec_search::node::{NodeConfig, NodeServiceImpl};
use turbovec_search::pb::node_service_server::NodeServiceServer;
use turbovec_search::pb::search_service_server::SearchServiceServer;

pub const DIM: usize = 128;
pub const BIT_WIDTH: usize = 4;

/// Deterministic pseudo-random unit vectors (LCG + L2 normalize), same
/// generator style as the turbovec test suite.
pub fn unit_vectors(n: usize, dim: usize, seed: u64) -> Vec<f32> {
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

/// Fit a TQ+ calibration on a representative sample: build a throwaway
/// index from the sample and read out its locked (shift, scale).
pub fn fit_calibration(dim: usize, bit_width: usize, sample: &[f32]) -> (Vec<f32>, Vec<f32>) {
    let mut fitting = TurboQuantIndex::new(dim, bit_width).unwrap();
    fitting.add(sample);
    let (shift, scale) = fitting.calibration().expect("first add fits calibration");
    (shift.to_vec(), scale.to_vec())
}

/// One shard's index plus its global id base (the corpus offset of its
/// first vector; partitions are contiguous ranges).
pub struct Shard {
    pub index: Arc<TurboQuantIndex>,
    pub slot_offset: u64,
}

/// Build `n_shards` indexes over contiguous, disjoint partitions of
/// `corpus`, all seeded with the same calibration — the property that makes
/// their scores mutually comparable.
pub fn build_shards(
    corpus: &[f32],
    dim: usize,
    bit_width: usize,
    n_shards: usize,
    shift: &[f32],
    scale: &[f32],
) -> Vec<Shard> {
    let n = corpus.len() / dim;
    (0..n_shards)
        .map(|i| {
            let start = i * n / n_shards;
            let end = (i + 1) * n / n_shards;
            let mut index =
                TurboQuantIndex::new_with_calibration(dim, bit_width, shift, scale).unwrap();
            index.add(&corpus[start * dim..end * dim]);
            index.prepare();
            Shard {
                index: Arc::new(index),
                slot_offset: start as u64,
            }
        })
        .collect()
}

/// The single-index reference: one index over the whole corpus, same
/// calibration.
pub fn build_monolithic(
    corpus: &[f32],
    dim: usize,
    bit_width: usize,
    shift: &[f32],
    scale: &[f32],
) -> TurboQuantIndex {
    let mut index = TurboQuantIndex::new_with_calibration(dim, bit_width, shift, scale).unwrap();
    index.add(corpus);
    index.prepare();
    index
}

/// Start a node server on 127.0.0.1:0. Returns its `http://` address and
/// the server task (abort to stop).
pub async fn start_node(
    index: Arc<TurboQuantIndex>,
    config: NodeConfig,
) -> (String, JoinHandle<Result<(), TransportError>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(
        Server::builder()
            .add_service(NodeServiceServer::new(NodeServiceImpl::new(index, config)))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    (format!("http://{addr}"), handle)
}

/// Start a coordinator server on 127.0.0.1:0.
pub async fn start_coordinator(
    node_addrs: Vec<String>,
) -> (String, JoinHandle<Result<(), TransportError>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(
        Server::builder()
            .add_service(SearchServiceServer::new(CoordinatorServiceImpl::new(
                node_addrs,
            )))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    (format!("http://{addr}"), handle)
}

/// Reference top-k from the monolithic index as `(global_id, score_bits)`,
/// re-sorted into the coordinator's total order (score desc, id asc) so the
/// comparison is exact, tie-break included.
pub fn monolithic_topk(index: &TurboQuantIndex, query: &[f32], k: usize) -> Vec<(u64, u32)> {
    let results = index.search(query, k);
    let mut hits: Vec<(u64, u32)> = results
        .indices_for_query(0)
        .iter()
        .zip(results.scores_for_query(0))
        .map(|(&i, &s)| (i as u64, s.to_bits()))
        .collect();
    hits.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    hits
}

/// A served 3-shard cluster over a seeded-calibration corpus plus the
/// monolithic reference index.
pub struct Cluster {
    pub node_addrs: Vec<String>,
    pub monolithic: TurboQuantIndex,
    pub n: usize,
    handles: Vec<JoinHandle<Result<(), TransportError>>>,
}

impl Cluster {
    /// Corpus of `n` vectors partitioned into 3 shards; nodes scan with
    /// `chunk_blocks` and optionally share floors.
    pub async fn start(n: usize, chunk_blocks: usize, share_floors: bool) -> Self {
        let corpus = unit_vectors(n, DIM, 0x5EED_CA11);
        let sample_n = 2_000.min(n);
        let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &corpus[..sample_n * DIM]);
        let shards = build_shards(&corpus, DIM, BIT_WIDTH, 3, &shift, &scale);
        let monolithic = build_monolithic(&corpus, DIM, BIT_WIDTH, &shift, &scale);

        let mut node_addrs = Vec::new();
        let mut handles = Vec::new();
        for shard in shards {
            let (addr, handle) = start_node(
                shard.index,
                NodeConfig {
                    slot_offset: shard.slot_offset,
                    chunk_blocks,
                    share_floors,
                },
            )
            .await;
            node_addrs.push(addr);
            handles.push(handle);
        }
        Cluster {
            node_addrs,
            monolithic,
            n,
            handles,
        }
    }

    pub async fn shutdown(self) {
        for handle in self.handles {
            handle.abort();
        }
    }
}
