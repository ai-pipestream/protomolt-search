//! Hybrid (RRF-fused vector + BM25) integration tests over the mock
//! analyzer and seeded shards.
//!
//! Two properties are under test:
//!
//! 1. Determinism and provenance: identical results across runs; hits
//!    carry per-leg ranks, and a doc in both legs outranks single-leg
//!    docs at its shard.
//! 2. Distributed exactness on a partition-stable corpus: 3 shards fused
//!    == monolithic fused, exactly (id sequence). Two-level RRF is NOT
//!    partition-independent in general (see the README's counterexample),
//!    so the corpus is constructed partition-stable: docs are assigned to
//!    shards round-robin by the monolithic fused order, which makes the
//!    coordinator's (shard-rank, shard-index) ordering coincide with the
//!    monolithic order by construction. The test is deterministic — fixed
//!    seeds, fixed corpus — and proves the distribution machinery (id
//!    mapping, both RPC levels, fusion application) is lossless on it.

mod common;

use pipestream_search::coordinator::{CoordinatorServiceImpl, HybridLegs};
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::{AddDocumentsRequest, AddVectorsRequest};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use common::{fit_calibration, mock::start_mock_analysis, start_empty_node, unit_vectors};
use pipestream_search::fusion::{Combination, Normalization};

const DIM: usize = 64;
const SHARD_DOCS: usize = 4;
const N_SHARDS: usize = 3;
const N_DOCS: usize = SHARD_DOCS * N_SHARDS;

async fn configure_backend(addr: &str, shift: &[f32], scale: &[f32]) {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    client
        .configure_vector_backend(pipestream_search::harness::embedded_backend_request(
            DIM, 4, shift, scale,
        ))
        .await
        .unwrap();
}

async fn add_documents(addr: &str, texts: &[String]) {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(8);
    // Feed the stream from a task: the RPC only starts reading once it is
    // initiated, so sending more messages than the channel capacity before
    // the call would deadlock.
    let texts = texts.to_vec();
    let feeder = tokio::spawn(async move {
        for text in texts {
            tx.send(AddDocumentsRequest {
                nlp_materialization: String::new(),
                unsigned_integers: Vec::new(),
                map_integers: Vec::new(),
                map_unsigned_integers: Vec::new(),
                derived_fingerprint: String::new(),
                original_source: None,
                source_chunk_ordinal: None,
                identity: None,
                collection: String::new(),
                cased_field: String::new(),
                sentence_fields: Vec::new(),
                materialize: None,
                map_numerics: Vec::new(),
                map_facets: Vec::new(),
                numerics: Vec::new(),
                facets: Vec::new(),
                text,
                analysis: None,
                lineage: None,
                fields: Vec::new(),
                integers: Vec::new(),
                timestamps: Vec::new(),
                geo_points: Vec::new(),
                quality: None,
                geography: None,
                phrases: Vec::new(),
                phrase_fingerprint: 0,
                phrase_field: String::new(),
                position_fields: Vec::new(),
                bigram_fields: Vec::new(),
            })
            .await
            .unwrap();
        }
    });
    client.add_documents(ReceiverStream::new(rx)).await.unwrap();
    feeder.await.unwrap();
}

async fn add_vectors(addr: &str, vectors: Vec<f32>) {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(4);
    // dim is sent explicitly: harmless when the shard already knows it
    // (validated then), required for a from-scratch shard.
    tx.send(AddVectorsRequest {
        materialized_rows: Vec::new(),
        vectors,
        dim: DIM as u32,
    })
    .await
    .unwrap();
    drop(tx);
    client.add_vectors(ReceiverStream::new(rx)).await.unwrap();
}

fn legs_default() -> HybridLegs {
    HybridLegs {
        leg_k: 60,
        vector_weight: 1.0,
        bm25_weight: 1.0,
        rrf_k: 60.0,
        fusion_mode: pipestream_search::pb::FusionMode::GlobalRank,
        normalization: Normalization::MinMax,
        combination: Combination::Arithmetic,
        min_vector_score: 0.0,
    }
}

fn legs_two_level() -> HybridLegs {
    HybridLegs {
        fusion_mode: pipestream_search::pb::FusionMode::TwoLevel,
        ..legs_default()
    }
}

fn legs_blend(normalization: Normalization, combination: Combination) -> HybridLegs {
    HybridLegs {
        fusion_mode: pipestream_search::pb::FusionMode::ScoreBlend,
        normalization,
        combination,
        ..legs_default()
    }
}

fn ids(hits: &[pipestream_search::pb::HybridHit]) -> Vec<u64> {
    hits.iter().map(|h| h.doc_id).collect()
}

/// Start a shard, seed it, ingest docs then vectors (in that order so doc
/// ids and vector slots align 1:1 in the shared positional id space).
async fn start_hybrid_shard(
    analysis: &str,
    slot_offset: u64,
    texts: &[String],
    vectors: Vec<f32>,
    shift: &[f32],
    scale: &[f32],
) -> (
    String,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
) {
    let (addr, handle) = start_empty_node(NodeConfig {
        slot_offset,
        analysis_addr: Some(analysis.to_string()),
        ..Default::default()
    })
    .await;
    configure_backend(&addr, shift, scale).await;
    add_documents(&addr, texts).await;
    add_vectors(&addr, vectors).await;
    (addr, handle)
}

#[path = "hybrid/backend_and_diagnostics.rs"]
mod backend_and_diagnostics;
#[path = "hybrid/determinism_and_parity.rs"]
mod determinism_and_parity;
#[path = "hybrid/leg_gating_and_fallback.rs"]
mod leg_gating_and_fallback;
#[path = "hybrid/score_blend_and_boost.rs"]
mod score_blend_and_boost;
