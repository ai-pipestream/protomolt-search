//! Shared fixtures for the integration tests. The heavy lifting (corpus
//! generation, shard building, server startup) lives in
//! [`pipestream_search::harness`] so the `sweep` binary uses the same code;
//! this module adds the test-only `Cluster` wrapper and the monolithic
//! reference comparator.
//!
//! Everything is seeded: corpora, queries, and shard builds are fully
//! deterministic, so assertions are exact (bitwise score equality, exact id
//! sequences) rather than probabilistic.
#![allow(dead_code, unused_imports)]

pub mod files;
pub mod mock;

pub fn protobuf_source(text: &str, key: &str) -> pipestream_search::pb::ProtobufSource {
    let mut payload = Vec::new();
    prost::encoding::string::encode(2, &text.to_string(), &mut payload);
    payload.extend([122, 0]); // Required bytes field, explicitly present empty.
    prost::encoding::string::encode(99, &key.to_string(), &mut payload); // Unknown field.
    pipestream_search::pb::ProtobufSource {
        descriptor_set: include_bytes!("../fixtures/protobuf-semantics/descriptor.bin").to_vec(),
        message_type: "semantics.Doc".into(),
        payload,
    }
}

use pipestream_search::harness::{self, build_monolithic, build_shards};
use pipestream_search::node::NodeConfig;
use pipestream_search::vector::VectorIndex;
use tokio::task::JoinHandle;
use tonic::transport::Error as TransportError;

pub use harness::{
    embedded_backend_request, fit_calibration, start_coordinator, start_empty_node, start_node,
    start_opened_node, unit_vectors,
};

pub const DIM: usize = 128;
pub const BIT_WIDTH: usize = 4;

/// Reference top-k from the monolithic index as `(global_id, score_bits)`,
/// re-sorted into the coordinator's total order (score desc, id asc) so the
/// comparison is exact, tie-break included.
pub fn monolithic_topk(index: &VectorIndex, query: &[f32], k: usize) -> Vec<(u64, u32)> {
    let results = index.search_unfiltered(query, k);
    let mut hits: Vec<(u64, u32)> = results
        .indices_for_query(0)
        .iter()
        .zip(results.scores_for_query(0))
        .map(|(&i, &s)| (i as u64, s.to_bits()))
        .collect();
    hits.sort_by(|a, b| b.1.cmp(&a.1).then(a.0.cmp(&b.0)));
    hits
}

/// A served multi-shard cluster over a seeded-calibration corpus plus the
/// monolithic reference index.
pub struct Cluster {
    pub node_addrs: Vec<String>,
    pub monolithic: VectorIndex,
    pub n: usize,
    handles: Vec<JoinHandle<Result<(), TransportError>>>,
}

impl Cluster {
    /// Corpus of `n` vectors partitioned into 3 shards; nodes scan with
    /// `chunk_blocks` and optionally share floors.
    pub async fn start(n: usize, chunk_blocks: usize, share_floors: bool) -> Self {
        Self::start_sharded(n, 3, chunk_blocks, share_floors).await
    }

    /// Like [`Cluster::start`] with an explicit shard count.
    pub async fn start_sharded(
        n: usize,
        n_shards: usize,
        chunk_blocks: usize,
        share_floors: bool,
    ) -> Self {
        let corpus = unit_vectors(n, DIM, 0x5EED_CA11);
        let sample_n = 2_000.min(n);
        let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &corpus[..sample_n * DIM]);
        let shards = build_shards(&corpus, DIM, BIT_WIDTH, n_shards, &shift, &scale);
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
                    ..Default::default()
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

/// Explicit capabilities for isolated test collections; no wildcard grants.
pub fn access_policy(
    principals: &[&str],
    collections: &[&str],
    actions: &[pipestream_search::pb::AccessAction],
) -> pipestream_search::pb::AccessPolicy {
    use pipestream_search::pb::{AccessPolicy, CollectionGrant, CollectionResource};
    AccessPolicy {
        format_version: 1,
        revision: 1,
        resources: collections
            .iter()
            .map(|c| CollectionResource {
                workspace: "test".into(),
                collection: (*c).into(),
            })
            .collect(),
        grants: principals
            .iter()
            .flat_map(|p| {
                collections.iter().map(move |c| CollectionGrant {
                    document_visibility: None,
                    principal: (*p).into(),
                    workspace: "test".into(),
                    collection: (*c).into(),
                    actions: actions.iter().map(|a| *a as i32).collect(),
                })
            })
            .collect(),
    }
}
