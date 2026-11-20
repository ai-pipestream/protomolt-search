//! Relay coordinators : the exactness gate
//! and the refusals of the restricted, read-only relay.
//!
//! Flat, one-level, and two-level execution over the same leaf shards
//! must return the same `StreamSearch` answer bit for bit (ids, scores,
//! order, completion), under permuted child order and grouping, with
//! ties across relays and with an initial floor. Statistics through a
//! relay equal the flat sum and carry a token the relay translates into
//! each child's epoch claim, which the child enforces. Every route
//! outside the scope refuses by name, a child error fails the attempt,
//! and a map move refuses the decisions pinned under the older map.

#[path = "relay/capability_probe.rs"]
mod capability_probe;
mod common;

use common::mock::start_mock_analysis;
use common::{
    fit_calibration, monolithic_topk, start_empty_node, start_node, unit_vectors, BIT_WIDTH, DIM,
};
use pipestream_search::coordinator::{CoordinatorServiceImpl, HybridLegs, TopologyRoute};
use pipestream_search::fusion::{Combination, Normalization};
use pipestream_search::harness::{start_relay, start_relay_over};
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    bm25_query_stream_request, bm25_query_stream_response, stream_search_request,
    stream_search_response, AddDocumentsRequest, AddVectorsRequest, Bm25QueryRequest,
    Bm25QueryStreamRequest, Bm25SearchRequest, FacetValue, FieldTerms, FusionMode, HealthRequest,
    PhraseMatch, ScoredHit, StartStreamSearch, StopBm25Query, StopStreamSearch,
    StreamSearchRequest, TermStatsRequest, TermStatsResponse,
};
use pipestream_search::pb::{Bm25Hit, HybridHit};
use pipestream_search::vector::{VectorIndex, EMBEDDED_TURBOVEC};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

/// The engine's emission-chunk cadence: two chunks per leaf so floors
/// can bind between batches.
const CHUNK: usize = 8192;
const LEAF_ROWS: usize = 2 * CHUNK;
const N_LEAVES: usize = 4;
const N: usize = N_LEAVES * LEAF_ROWS;
const K: u32 = 10;
/// A row copied into another leaf, under another relay, so the top-k
/// holds a tie at equal score across the tree.
const TIE_SOURCE: usize = 7;
const TIE_COPY: usize = 3 * LEAF_ROWS + 3;

struct Leaves {
    addrs: Vec<String>,
    monolithic: VectorIndex,
    corpus: Vec<f32>,
}

/// Four unseeded leaves with contiguous slot ranges, one duplicated row
/// across the relay boundary, and the monolithic reference built from
/// the same corpus.
async fn leaves() -> Leaves {
    let mut corpus = unit_vectors(N, DIM, 0x5EED_2E1A);
    let (src, dst) = (TIE_SOURCE * DIM, TIE_COPY * DIM);
    let row: Vec<f32> = corpus[src..src + DIM].to_vec();
    corpus[dst..dst + DIM].copy_from_slice(&row);
    let mut addrs = Vec::new();
    for leaf in 0..N_LEAVES {
        let mut index = VectorIndex::create(EMBEDDED_TURBOVEC, DIM, BIT_WIDTH).unwrap();
        index
            .add(
                &corpus[leaf * LEAF_ROWS * DIM..(leaf + 1) * LEAF_ROWS * DIM],
                DIM,
            )
            .unwrap();
        index.prepare().unwrap();
        let (addr, _handle) = start_node(
            index,
            NodeConfig {
                slot_offset: (leaf * LEAF_ROWS) as u64,
                ..Default::default()
            },
        )
        .await;
        addrs.push(addr);
    }
    let mut monolithic = VectorIndex::create(EMBEDDED_TURBOVEC, DIM, BIT_WIDTH).unwrap();
    monolithic.add(&corpus, DIM).unwrap();
    monolithic.prepare().unwrap();
    Leaves {
        addrs,
        monolithic,
        corpus,
    }
}

fn bits(hits: &[ScoredHit]) -> Vec<(u64, u32)> {
    hits.iter()
        .map(|h| (h.vector_id, h.score.to_bits()))
        .collect()
}

async fn relay_over(children: &[&str]) -> String {
    let (addr, _relay, _handle) =
        start_relay(children.iter().map(|c| c.to_string()).collect()).await;
    addr
}

use pipestream_search::pb::{
    filter_query, search_query, selection_query, BooleanQuery, CompositeSearchStrategy,
    DenseExecutionMode, FilterQuery, LexicalQuery, QueryHit, QueryRequest, SearchQuery,
    SelectionOperator, SelectionQuery,
};

#[path = "relay/fixtures.rs"]
mod fixtures;
pub(crate) use fixtures::*;
#[path = "relay/aggregates_and_browse.rs"]
mod aggregates_and_browse;
#[path = "relay/dictionaries_and_diagnostics.rs"]
mod dictionaries_and_diagnostics;
#[path = "relay/fetches_and_bitmaps.rs"]
mod fetches_and_bitmaps;
#[path = "relay/lexical_and_hybrid_fanout.rs"]
mod lexical_and_hybrid_fanout;
#[path = "relay/map_moves_and_route_health.rs"]
mod map_moves_and_route_health;
#[path = "relay/unary_cascade_and_boolean.rs"]
mod unary_cascade_and_boolean;
#[path = "relay/vector_streaming_and_stats.rs"]
mod vector_streaming_and_stats;
