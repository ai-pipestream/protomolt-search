//! A boolean group's dense clause over a wide membership
//! (`docs/query-api.md`, "Recursive boolean execution").
//!
//! The coordinator scores the dense clause of a boolean group through
//! `VectorRescore`, and a node answers each call with one masked scan of
//! its index. The `signal_batch` knob is the ids per call; the
//! answer must not depend on it, since a row's product does not depend
//! on which other rows share the call. One call per shard is the default
//! and the cheap case; a batch of one id is the slow extreme, and an odd
//! batch cuts the membership at places no shard boundary sits.

mod common;

use std::path::PathBuf;

use common::{fit_calibration, start_empty_node, unit_vectors};
use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::{Layout, NodeConfig};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    selection_query, AddDocumentsRequest, AddVectorsRequest, BooleanQuery, DenseQuery,
    FlushRequest, LexicalQuery, QueryHit, QueryRequest, QueryResponse, SearchQuery, SelectionQuery,
    SetCalibrationRequest,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

const DIM: usize = 16;
const BIT_WIDTH: usize = 4;
const ROWS: usize = 3_000;
/// Rows per sealed segment: six segments plus no tail.
const SEAL: usize = 500;
const BLOCK: usize = 250;

fn tempdir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("boolmask-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn config(index_path: PathBuf) -> NodeConfig {
    NodeConfig {
        index_path: Some(index_path),
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        layout: Layout::Segments,
        seal_tail_docs: SEAL as u32,
        wal: false,
        ..Default::default()
    }
}

/// Every row says "search"; every second row says "zebra"; every fifth
/// says "quagga". The memberships are 100%, 50%, and 20% of the rows.
fn text(i: usize) -> String {
    let mut t = format!("opinion {i} about search");
    if i % 2 == 1 {
        t.push_str(" zebra");
    }
    if i.is_multiple_of(5) {
        t.push_str(" quagga");
    }
    t
}

fn corpus() -> Vec<f32> {
    unit_vectors(ROWS, DIM, 0xB00_1EA4)
}

async fn ingest(addr: &str) {
    let vectors = corpus();
    let sample = &vectors[..vectors.len().min(64 * DIM)];
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, sample);
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    client
        .set_calibration(SetCalibrationRequest {
            dim: DIM as u32,
            bit_width: BIT_WIDTH as u32,
            shift,
            scale,
        })
        .await
        .unwrap();
    for block in 0..ROWS.div_ceil(BLOCK) {
        let start = block * BLOCK;
        let end = (start + BLOCK).min(ROWS);
        let (tx, rx) = mpsc::channel(BLOCK);
        for i in start..end {
            tx.send(AddDocumentsRequest {
                text: text(i),
                analysis: Some(body_spec()),
                ..Default::default()
            })
            .await
            .unwrap();
        }
        drop(tx);
        client.add_documents(ReceiverStream::new(rx)).await.unwrap();
        let (tx, rx) = mpsc::channel(2);
        tx.send(AddVectorsRequest {
            vectors: vectors[start * DIM..end * DIM].to_vec(),
            dim: DIM as u32,
        })
        .await
        .unwrap();
        drop(tx);
        client.add_vectors(ReceiverStream::new(rx)).await.unwrap();
    }
    client.flush(FlushRequest {}).await.unwrap();
}

fn coordinator(addr: &str) -> CoordinatorServiceImpl {
    CoordinatorServiceImpl::new(vec![addr.to_string()]).with_bm25(
        Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        Default::default(),
    )
}

fn lexical(id: &str, text: &str) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: id.to_string(),
            query: Some(pipestream_search::pb::search_query::Query::Lexical(
                LexicalQuery {
                    text: text.to_string(),
                    analysis: Some(body_spec()),
                    ..Default::default()
                },
            )),
        })),
    }
}

fn dense(id: &str, q: usize) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: id.to_string(),
            query: Some(pipestream_search::pb::search_query::Query::Dense(
                DenseQuery {
                    vector: corpus()[q * DIM..(q + 1) * DIM].to_vec(),
                    ..Default::default()
                },
            )),
        })),
    }
}

fn boolean(must: Vec<SelectionQuery>) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Boolean(BooleanQuery {
            must,
            should: Vec::new(),
            must_not: Vec::new(),
            minimum_should_match: 0,
            aggregate: None,
        })),
    }
}

async fn query(c: &CoordinatorServiceImpl, selection: SelectionQuery) -> QueryResponse {
    SearchService::query(
        c,
        Request::new(QueryRequest {
            request_id: "boolmask".into(),
            k: 50,
            selection: Some(selection),
            profile: true,
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner()
}

fn bits(hits: &[QueryHit]) -> Vec<(u64, u32, u32)> {
    hits.iter()
        .map(|h| (h.doc_id, h.score.to_bits(), h.rank))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_signal_batch_moves_no_answer() {
    let dir = tempdir("batch");
    let (addr, handle) = start_empty_node(config(dir.join("bool.tv"))).await;
    ingest(&addr).await;
    let c = coordinator(&addr);
    assert_eq!(
        c.signal_batch(),
        pipestream_search::coordinator::DEFAULT_SIGNAL_BATCH as usize
    );

    let shapes: Vec<(&str, Vec<SelectionQuery>)> = vec![
        ("all rows", vec![lexical("l", "search"), dense("v", 3)]),
        ("half", vec![lexical("l", "zebra"), dense("v", 7)]),
        ("fifth", vec![lexical("l", "quagga"), dense("v", 11)]),
        (
            "half and fifth",
            vec![
                lexical("a", "zebra"),
                lexical("b", "quagga"),
                dense("v", 13),
            ],
        ),
    ];
    let mut reference = Vec::new();
    for (name, must) in &shapes {
        let r = query(&c, boolean(must.clone())).await;
        assert_eq!(r.hits.len(), 50, "{name}: a full page at the default batch");
        reference.push(bits(&r.hits));
    }

    // The slow extreme and an odd cut: identical ids, scores, and ranks.
    for batch in ["1", "7", "333"] {
        c.knobs().set("signal_batch", batch).unwrap();
        assert_eq!(c.signal_batch().to_string(), batch);
        for ((name, must), expected) in shapes.iter().zip(&reference) {
            let r = query(&c, boolean(must.clone())).await;
            assert_eq!(&bits(&r.hits), expected, "{name} at batch {batch}");
        }
    }

    // Zero is not a batch.
    let err = c.knobs().set("signal_batch", "0").unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::InvalidArgument,
        "{}",
        err.message()
    );
    assert_eq!(c.signal_batch(), 333, "a refused set keeps the value");

    // The knob is listed with its startup value.
    let listed = c.knobs().list();
    let knob = listed
        .knobs
        .iter()
        .find(|k| k.name == "signal_batch")
        .expect("listed");
    assert_eq!(knob.value, "333");
    assert_eq!(
        knob.startup_value,
        pipestream_search::coordinator::DEFAULT_SIGNAL_BATCH.to_string()
    );

    handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
