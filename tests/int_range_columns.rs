//! i64-column and range-facet acceptance tests
//! : kind 4 through the v7 column table with
//! integers that survive past 2^53, count-then-rank range buckets that
//! are exact and additive over the full match set, score chains that
//! read i64 columns with the same bitwise gates as f64 ones, and
//! Timestamp ingest as sugar that lands as epoch micros.

mod common;

use pipestream_search::bm25::{self, Bm25Params, CorpusStats};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    filter_bound::Value as BoundValue, AddDocumentsRequest, Bm25Hit, Bm25SearchRequest,
    FilterBound, IntegerValue, RangeFacetField, ScoreOp, ScoreStage, TimestampValue,
};
use pipestream_search::postings::{
    AnalyzedDoc, Bm25Reader, Bm25Store, ColumnKind::*, SpillBuilder,
};
use pipestream_search::scorefn::{ColumnRef, NumericRead, ScoreChain, Stage, StageOp};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

use common::{mock::start_mock_analysis, start_empty_node};

/// A document's integer values at ingest: (field, value) pairs.
type Integers = &'static [(&'static str, i64)];

/// The controlled corpus: eight documents over three shards with a
/// "citations" i64 column. Shard 2 declares NO integer field — the
/// heterogeneous-fleet case, whose documents hold no value and are
/// therefore counted in no bucket, exactly and not by degradation.
///
/// df("rust") = 6 (d0, d1, d2, d3, d5, d6); df("vector") = 2 (d1, d4).
/// Against edges [0, 10, 20, 30] the "rust" match set exercises every
/// boundary rule at once: d0 (0) and d2 (5) fall in [0, 10), d1 sits
/// exactly ON the interior edge 10 and must land in the UPPER bucket,
/// d3 (20) falls in [20, 30), d5 (-1) is below the first edge and lands
/// nowhere, and d6 has no value at all.
const SHARD_DOCS: [&[(&str, Integers)]; 3] = [
    &[
        ("rust search rust fast", &[("citations", 0)]),
        ("vector search rust", &[("citations", 10)]),
        ("rust rust", &[("citations", 5)]),
    ],
    &[
        ("search engines love rust", &[("citations", 20)]),
        ("vector vector vector", &[("citations", 30)]),
        ("rust fast", &[("citations", -1)]),
    ],
    &[("rust", &[]), ("nothing relevant here", &[])],
];

/// The bucket edges every distributed assertion below uses.
const EDGES: [f64; 4] = [0.0, 10.0, 20.0, 30.0];

async fn add_documents_integer(
    addr: &str,
    docs: &[(&str, Integers)],
) -> Result<pipestream_search::pb::AddDocumentsResponse, tonic::Status> {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(8);
    for (text, integers) in docs {
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
            text: text.to_string(),
            analysis: None,
            lineage: None,
            fields: Vec::new(),
            facets: Vec::new(),
            numerics: Vec::new(),
            map_facets: Vec::new(),
            map_numerics: Vec::new(),
            timestamps: Vec::new(),
            geo_points: Vec::new(),
            integers: integers
                .iter()
                .map(|(field, value)| IntegerValue {
                    field: field.to_string(),
                    value: *value,
                })
                .collect(),
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
    drop(tx);
    client
        .add_documents(ReceiverStream::new(rx))
        .await
        .map(|r| r.into_inner())
}

/// Three shards, the integer table declared on shards 0 and 1 only.
async fn start_integer_shards(
    analysis: &str,
) -> (
    Vec<String>,
    Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
) {
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for (i, _) in SHARD_DOCS.iter().enumerate() {
        let integer_fields = if i < 2 {
            vec!["citations".to_string()]
        } else {
            Vec::new()
        };
        let (addr, handle) = start_empty_node(NodeConfig {
            slot_offset: [0u64, 3, 6][i],
            analysis_addr: Some(analysis.to_string()),
            integer_fields,
            ..Default::default()
        })
        .await;
        addrs.push(addr);
        handles.push(handle);
    }
    for (i, docs) in SHARD_DOCS.iter().enumerate() {
        add_documents_integer(&addrs[i], docs).await.unwrap();
    }
    (addrs, handles)
}

fn range_field(column: &str, edges: &[f64]) -> RangeFacetField {
    RangeFacetField {
        column: column.to_string(),
        map_key: None,
        edges: edges.iter().copied().map(FilterBound::num).collect(),
    }
}

/// A range facet's buckets as `(from, to, count)`, for hand-computed
/// comparisons.
fn buckets_of(rf: &pipestream_search::pb::RangeFacetCounts) -> Vec<(f64, f64, u64)> {
    let edge = |b: &Option<FilterBound>| match b.as_ref().and_then(|x| x.value) {
        Some(BoundValue::Num(v)) => v,
        other => panic!("double edge expected, got {other:?}"),
    };
    rf.buckets
        .iter()
        .map(|b| (edge(&b.from), edge(&b.to), b.count))
        .collect()
}

fn hit_signature(hits: &[Bm25Hit]) -> Vec<(u64, u32)> {
    hits.iter().map(|h| (h.doc_id, h.score.to_bits())).collect()
}

#[path = "int_range_columns/range_facets_and_chains.rs"]
mod range_facets_and_chains;
#[path = "int_range_columns/storage_and_tiling.rs"]
mod storage_and_tiling;
#[path = "int_range_columns/timestamps_and_refusals.rs"]
mod timestamps_and_refusals;
