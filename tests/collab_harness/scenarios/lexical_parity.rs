//! Unary and streaming lexical parity over a traced three-shard cluster.
//!
//! Every shard holds the same four documents, so equal documents on
//! different shards score identically under the coordinator's global
//! BM25 statistics and only the tie-break orders them. The checks:
//!
//! - unary `Query`, streaming `QueryStream`, and unary `Query` over the
//!   `Bm25QueryStream` floor relay return the same hits, bitwise;
//! - ties are broken by ascending global document id across shards;
//! - cursor pages, unary and streaming interleaved, stitch to the full
//!   result with absolute ranks;
//! - each query fans out exactly one `TermStats` and one shard query per
//!   node, read from the recorded spans, and every span finishes `OK`.

use std::sync::Arc;

use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::{NodeConfig, NodeServiceImpl};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    query_stream_response, search_query, selection_query, AddDocumentsRequest, LexicalQuery,
    QueryRequest, QueryResponse, QueryStreamRequest, SearchQuery, SelectionQuery, TermStatsRequest,
};
use prost::Message;
use tokio_stream::StreamExt;
use tonic::Request;

use crate::collab_harness::trace_proxy::{
    serve_traced, CallSpan, Outcome, TraceRecorder, TracedNode,
};

const SHARDS: u64 = 3;
const SLOT_STRIDE: u64 = 100;
/// The documents of every shard; three of the four match `alpha`.
const TEXTS: [&str; 4] = ["alpha beta", "omega", "alpha beta", "alpha gamma delta"];

struct Cluster {
    recorder: Arc<TraceRecorder>,
    nodes: Vec<TracedNode>,
}

impl Cluster {
    async fn start() -> Self {
        let recorder = TraceRecorder::new();
        let mut nodes = Vec::new();
        for shard in 0..SHARDS {
            let node = NodeServiceImpl::new(
                None,
                NodeConfig {
                    slot_offset: shard * SLOT_STRIDE,
                    analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
                    ..Default::default()
                },
            );
            let traced = serve_traced(node, &format!("shard{shard}"), recorder.clone()).await;
            let mut client = NodeServiceClient::connect(traced.addr.clone())
                .await
                .unwrap();
            let docs = TEXTS.map(|text| AddDocumentsRequest {
                text: text.into(),
                analysis: Some(body_spec()),
                ..Default::default()
            });
            client
                .add_documents(tokio_stream::iter(docs))
                .await
                .unwrap();
            nodes.push(traced);
        }
        Self { recorder, nodes }
    }

    fn coordinator(&self, bm25_stream: bool) -> CoordinatorServiceImpl {
        let addrs = self.nodes.iter().map(|n| n.addr.clone()).collect();
        CoordinatorServiceImpl::new(addrs)
            .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default())
            .with_bm25_stream(bm25_stream)
    }

    /// Run `query` and return its response with the calls it made. Every
    /// call has arrived before the response returns; the wait only lets
    /// the servers finish closing the last response bodies.
    async fn traced<F>(&self, query: F) -> (QueryResponse, Vec<CallSpan>)
    where
        F: std::future::Future<Output = QueryResponse>,
    {
        let checkpoint = self.recorder.checkpoint();
        let response = query.await;
        let done = |spans: &[CallSpan]| {
            !spans.is_empty() && spans.iter().all(|s| s.outcome != Outcome::Open)
        };
        let timeout = std::time::Duration::from_secs(5);
        let spans = self.recorder.wait_for(checkpoint, timeout, done).await;
        (
            response,
            spans.unwrap_or_else(|seen| panic!("calls did not settle: {seen:?}")),
        )
    }
}

fn lexical(k: u32, cursor: String) -> QueryRequest {
    QueryRequest {
        k,
        cursor,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Search(SearchQuery {
                id: "text".into(),
                query: Some(search_query::Query::Lexical(LexicalQuery {
                    text: "alpha".into(),
                    analysis: Some(body_spec()),
                    ..Default::default()
                })),
            })),
        }),
        ..Default::default()
    }
}

async fn unary(coordinator: &CoordinatorServiceImpl, query: QueryRequest) -> QueryResponse {
    coordinator
        .query(Request::new(query))
        .await
        .unwrap()
        .into_inner()
}

async fn streamed(coordinator: &CoordinatorServiceImpl, query: QueryRequest) -> QueryResponse {
    let request = QueryStreamRequest {
        query: Some(query),
        ..Default::default()
    };
    let mut events = coordinator
        .query_stream(Request::new(request))
        .await
        .unwrap()
        .into_inner();
    while let Some(event) = events.next().await {
        if let Some(query_stream_response::Payload::Completion(done)) = event.unwrap().payload {
            assert!(done.completed, "stream incomplete: {}", done.error_message);
            return done
                .response
                .expect("a completed stream carries its response");
        }
    }
    panic!("stream ended without a completion");
}

/// `(doc_id, score bits, rank)` of every hit.
fn hits(response: &QueryResponse) -> Vec<(u64, u32, u32)> {
    let hits = response.hits.iter();
    hits.map(|h| (h.doc_id, h.score.to_bits(), h.rank))
        .collect()
}

/// A span's role: `TermStats` is a version probe or a statistics read.
fn role(span: &CallSpan) -> String {
    if span.method != "TermStats" {
        return span.method.clone();
    }
    let request = TermStatsRequest::decode(&span.first_request[..]).expect("TermStats request");
    if request.version_only {
        assert!(request.terms.is_empty() && request.fields.is_empty());
        return "version".into();
    }
    assert_eq!(
        request.terms,
        ["alpha"],
        "statistics are read for the query terms"
    );
    "stats".into()
}

/// Each node's `(span id, role)` list in arrival order, with every span
/// checked to have finished `OK` with at least one message each way.
fn per_node(spans: &[CallSpan]) -> Vec<(String, Vec<(usize, String)>)> {
    for span in spans {
        assert_eq!(span.outcome, Outcome::Finished, "{span:?}");
        assert_eq!(span.grpc_status, Some(0), "{span:?}");
        assert!(span.request_messages >= 1 && span.response_messages >= 1);
        assert!(span.latency().is_some());
    }
    let nodes = (0..SHARDS).map(|shard| format!("shard{shard}"));
    nodes
        .map(|node| {
            let mine = spans.iter().filter(|s| s.node == node);
            let roles = mine.map(|s| (s.id, role(s))).collect();
            (node, roles)
        })
        .collect()
}

/// Unary `Query`: every node sees, in order, a version probe pinning the
/// read set, a statistics read when the coordinator's cache is `cold`,
/// exactly one `shard_rpc`, one `FetchValues` for hit identities, and a
/// closing version probe validating the pinned set. Each phase arrives in
/// order and completes on every node before any node sees the next.
fn assert_unary_fanout(spans: &[CallSpan], shard_rpc: &str, cold: bool) {
    let mut want = vec!["version", "stats", shard_rpc, "FetchValues", "version"];
    if !cold {
        want.remove(1);
    }
    let mut phase_of = Vec::new();
    for (node, seen) in per_node(spans) {
        let roles: Vec<&str> = seen.iter().map(|(_, r)| r.as_str()).collect();
        assert_eq!(roles, want, "calls on {node}");
        phase_of.extend(seen.iter().enumerate().map(|(phase, (id, _))| (*id, phase)));
    }
    phase_of.sort();
    let phases: Vec<usize> = phase_of.iter().map(|(_, phase)| *phase).collect();
    assert!(
        phases.is_sorted(),
        "a phase arrived before the last one did: {phases:?}"
    );
    let phased: Vec<(usize, &CallSpan)> = phase_of
        .iter()
        .map(|(id, phase)| (*phase, spans.iter().find(|s| s.id == *id).unwrap()))
        .collect();
    assert_phase_barrier(&phased);
}

/// Every call of phase `p` ended no later than the first call of phase
/// `p + 1` started.
fn assert_phase_barrier(phased: &[(usize, &CallSpan)]) {
    let last = phased.iter().map(|(phase, _)| *phase).max().unwrap_or(0);
    for p in 0..last {
        let of = |q: usize| phased.iter().filter(move |(phase, _)| *phase == q);
        let ended = of(p).map(|(_, s)| s.end_ns.expect("closed")).max();
        let next = of(p + 1).map(|(_, s)| s.start_ns).min();
        if let (Some(ended), Some(next)) = (ended, next) {
            assert!(
                ended <= next,
                "phase {p} ended at {ended} ns, after phase {} started at {next} ns",
                p + 1
            );
        }
    }
}

/// `QueryStream`: the unary execution runs over `Bm25QueryStream`, and
/// each progress snapshot the stream publishes adds an identity
/// `FetchValues` and a validating version probe while the execution is
/// still running. How many snapshots are published depends on timing, so
/// per node this checks the fixed frame: a version probe first and last,
/// statistics only when `cold`, exactly one `Bm25QueryStream`, nothing
/// but `FetchValues` and probes otherwise, and at least one probe per
/// identity read beyond the opening pin.
fn assert_streaming_fanout(spans: &[CallSpan], cold: bool) {
    for (node, seen) in per_node(spans) {
        let roles: Vec<&str> = seen.iter().map(|(_, r)| r.as_str()).collect();
        let count = |role: &str| roles.iter().filter(|r| **r == role).count();
        assert_eq!(roles.first(), Some(&"version"), "{node}: {roles:?}");
        assert_eq!(roles.last(), Some(&"version"), "{node}: {roles:?}");
        assert_eq!(count("Bm25QueryStream"), 1, "{node}: {roles:?}");
        assert_eq!(count("stats"), usize::from(cold), "{node}: {roles:?}");
        let fetches = count("FetchValues");
        assert!(
            fetches >= 1 && count("version") > fetches,
            "{node}: {roles:?}"
        );
        assert_eq!(
            roles.len(),
            1 + usize::from(cold) + fetches + count("version")
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unary_and_streaming_lexical_results_agree_bitwise() {
    let cluster = Cluster::start().await;
    let (plain, relay) = (cluster.coordinator(false), cluster.coordinator(true));
    let (full, spans) = cluster
        .traced(unary(&plain, lexical(32, String::new())))
        .await;
    assert_unary_fanout(&spans, "Bm25Query", true);
    // A second coordinator has its own statistics cache.
    let (via_relay, spans) = cluster
        .traced(unary(&relay, lexical(32, String::new())))
        .await;
    assert_unary_fanout(&spans, "Bm25QueryStream", true);
    let (via_stream, spans) = cluster
        .traced(streamed(&plain, lexical(32, String::new())))
        .await;
    assert_streaming_fanout(&spans, false);

    let full = hits(&full);
    assert_eq!(full.len(), 3 * SHARDS as usize);
    assert_eq!(hits(&via_relay), full);
    assert_eq!(hits(&via_stream), full);

    // The two "alpha beta" rows of every shard tie: one score, ordered by
    // global id across the shards.
    let tied: Vec<u64> = full.iter().take(6).map(|h| h.0).collect();
    assert_eq!(tied, [0, 2, 100, 102, 200, 202]);
    assert!(full[..6].iter().all(|h| h.1 == full[0].1));
    let tail: Vec<u64> = full[6..].iter().map(|h| h.0).collect();
    assert_eq!(tail, [3, 103, 203]);
    assert!(f32::from_bits(full[6].1) < f32::from_bits(full[0].1));
    let ranks: Vec<u32> = full.iter().map(|h| h.2).collect();
    assert_eq!(ranks, (1..=9).collect::<Vec<u32>>());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn cursor_pages_stitch_across_unary_and_streaming_calls() {
    let cluster = Cluster::start().await;
    let plain = cluster.coordinator(false);
    let full = hits(&unary(&plain, lexical(32, String::new())).await);

    let (mut stitched, mut cursor, mut pages) = (Vec::new(), String::new(), 0);
    loop {
        let page = if pages % 2 == 0 {
            let (page, spans) = cluster.traced(unary(&plain, lexical(4, cursor))).await;
            assert_unary_fanout(&spans, "Bm25Query", false);
            page
        } else {
            let (page, spans) = cluster.traced(streamed(&plain, lexical(4, cursor))).await;
            assert_streaming_fanout(&spans, false);
            page
        };
        stitched.extend(hits(&page));
        pages += 1;
        if page.next_cursor.is_empty() {
            assert!(page.hits.len() < 4, "a full page always mints a cursor");
            break;
        }
        cursor = page.next_cursor;
    }
    assert_eq!(pages, 3, "9 hits in pages of 4");
    assert_eq!(stitched, full, "pages stitch bitwise with absolute ranks");
}

fn synthetic(id: usize, start_ns: u64, end_ns: u64) -> CallSpan {
    CallSpan {
        id,
        node: "shard0".into(),
        method: "TermStats".into(),
        start_ns,
        end_ns: Some(end_ns),
        request_bytes: 7,
        request_messages: 1,
        response_bytes: 7,
        response_messages: 1,
        grpc_status: Some(0),
        outcome: Outcome::Finished,
        first_request: Vec::new(),
    }
}

#[test]
fn the_phase_barrier_accepts_completed_phases() {
    let (a, b, c) = (synthetic(0, 1, 5), synthetic(1, 2, 6), synthetic(2, 6, 9));
    assert_phase_barrier(&[(0, &a), (0, &b), (1, &c)]);
}

#[test]
#[should_panic(expected = "phase 0 ended at 10000 ns, after phase 1 started at 3 ns")]
fn the_phase_barrier_rejects_a_phase_that_overlaps_the_next() {
    let (a, b, c) = (
        synthetic(0, 1, 2),
        synthetic(1, 2, 10_000),
        synthetic(2, 3, 4),
    );
    assert_phase_barrier(&[(0, &a), (0, &b), (1, &c)]);
}
