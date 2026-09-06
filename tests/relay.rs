//! Relay coordinators (`docs/relay-coordinators.md`): the exactness gate
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
use pipestream_search::pb::search_service_server::SearchService as _;
use pipestream_search::pb::{
    bm25_query_stream_request, bm25_query_stream_response, stream_search_request,
    stream_search_response, AddDocumentsRequest, AddVectorsRequest, Bm25QueryRequest,
    Bm25QueryStreamRequest, Bm25SearchRequest, FacetValue, FieldTerms, FusionMode, HealthRequest,
    PhraseMatch, ScoredHit, SetCalibrationRequest, StartStreamSearch, StopBm25Query,
    StopStreamSearch, StreamSearchRequest, TermStatsRequest, TermStatsResponse,
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

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_and_two_relay_levels_equal_the_flat_fanout_bitwise() {
    let leaves = leaves().await;
    let l = &leaves.addrs;
    let flat = CoordinatorServiceImpl::new(l.clone());

    // One level, in order: A over leaves 0 and 1, B over 2 and 3.
    let a = relay_over(&[&l[0], &l[1]]).await;
    let b = relay_over(&[&l[2], &l[3]]).await;
    let one_level = CoordinatorServiceImpl::new(vec![a.clone(), b.clone()]);
    // One level, permuted and regrouped: children out of slot order,
    // groups that cross the halves, relays out of order at the root.
    let p = relay_over(&[&l[3], &l[1]]).await;
    let q = relay_over(&[&l[2], &l[0]]).await;
    let permuted = CoordinatorServiceImpl::new(vec![q, p]);
    // Two levels: a relay over the two relays, alone under the root.
    let top = relay_over(&[&a, &b]).await;
    let two_level = CoordinatorServiceImpl::new(vec![top]);

    let mut queries: Vec<(String, Vec<f32>)> = (0..6u64)
        .map(|qi| (format!("q{qi}"), unit_vectors(1, DIM, 0x57AE_2E1A + qi)))
        .collect();
    let tie = leaves.corpus[TIE_SOURCE * DIM..(TIE_SOURCE + 1) * DIM].to_vec();
    queries.push(("tie".to_string(), tie));

    for (name, query) in &queries {
        let want = monolithic_topk(&leaves.monolithic, query, K as usize);
        let flat_result = flat
            .fanout_stream_search(name, query, K, None, &Default::default())
            .await
            .expect("flat fan-out");
        assert_eq!(bits(&flat_result.hits), want, "{name}: flat != monolithic");
        if name == "tie" {
            let ids: Vec<u64> = flat_result.hits.iter().map(|h| h.vector_id).collect();
            assert!(
                ids.contains(&(TIE_SOURCE as u64)) && ids.contains(&(TIE_COPY as u64)),
                "{name}: both copies of the tied row are in the top-k: {ids:?}"
            );
            assert_eq!(
                flat_result.hits[0].score.to_bits(),
                flat_result.hits[1].score.to_bits(),
                "{name}: the copies tie at equal score"
            );
        }

        for (label, tree, relays) in [
            ("one level", &one_level, 2usize),
            ("permuted", &permuted, 2),
            ("two levels", &two_level, 1),
        ] {
            let got = tree
                .fanout_stream_search(name, query, K, None, &Default::default())
                .await
                .unwrap_or_else(|e| panic!("{name}: {label}: {e}"));
            assert_eq!(bits(&got.hits), want, "{name}: {label} != monolithic");
            assert_eq!(
                got.summaries.len(),
                relays,
                "{name}: {label}: one summary per relay"
            );
            let mut blocks = 0;
            for (shard, summary) in got.summaries.iter().enumerate() {
                assert!(
                    summary.completed,
                    "{name}: {label}: relay {shard} not completed"
                );
                assert!(
                    !summary.scoring_fingerprint.is_empty(),
                    "{name}: {label}: relay {shard} carries the fingerprint"
                );
                blocks += summary.blocks_scanned;
            }
            assert_eq!(
                blocks,
                (N_LEAVES * LEAF_ROWS / CHUNK) as u64,
                "{name}: {label}: the relays' block counts sum to the leaves'"
            );
        }

        // An initial floor from the flat answer's k-th best binds every
        // leaf from its first chunk, through the relays, and changes
        // no bit of the answer.
        let kth = flat_result.hits.last().expect("k hits").score;
        let floor = Some(kth.next_down());
        let floored_flat = flat
            .fanout_stream_search(name, query, K, floor, &Default::default())
            .await
            .expect("floored flat");
        assert_eq!(bits(&floored_flat.hits), want, "{name}: floored flat");
        let floored = one_level
            .fanout_stream_search(name, query, K, floor, &Default::default())
            .await
            .expect("floored one level");
        assert_eq!(bits(&floored.hits), want, "{name}: floored one level");
        let emitted_flat: u64 = floored_flat.summaries.iter().map(|s| s.emitted).sum();
        let emitted_relayed: u64 = floored.summaries.iter().map(|s| s.emitted).sum();
        assert_eq!(
            emitted_relayed, emitted_flat,
            "{name}: under one initial floor the relays forward exactly what the leaves emit"
        );
        let unfloored: u64 = flat_result.summaries.iter().map(|s| s.emitted).sum();
        assert!(
            emitted_flat < unfloored,
            "{name}: the initial floor cut emissions ({emitted_flat} < {unfloored})"
        );
    }
}

async fn add_documents(addr: &str, texts: &[&str]) {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(texts.len().max(1));
    for text in texts {
        tx.send(AddDocumentsRequest {
            text: text.to_string(),
            ..Default::default()
        })
        .await
        .unwrap();
    }
    drop(tx);
    let added = client
        .add_documents(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner()
        .added;
    assert_eq!(added as usize, texts.len());
}

fn stats_request() -> TermStatsRequest {
    TermStatsRequest {
        version_only: false,
        visibility: None,
        terms: vec!["court".into(), "opinion".into(), "absent".into()],
        fields: vec![FieldTerms {
            field: "body".into(),
            terms: vec!["court".into(), "absent".into()],
        }],
    }
}

async fn direct_stats(addr: &str) -> TermStatsResponse {
    NodeServiceClient::connect(addr.to_string())
        .await
        .unwrap()
        .term_stats(stats_request())
        .await
        .unwrap()
        .into_inner()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn term_stats_through_a_relay_equal_the_flat_sum_and_the_token_translates() {
    let (analysis, _mock) = start_mock_analysis().await;
    let mut children = Vec::new();
    let corpora: [&[&str]; 3] = [
        &["the court held", "an opinion of the court"],
        &["opinion", "court court court", "no match here"],
        &["a lone court"],
    ];
    for (i, docs) in corpora.iter().enumerate() {
        let (addr, _handle) = start_empty_node(NodeConfig {
            analysis_addr: Some(analysis.clone()),
            slot_offset: (i * 100) as u64,
            ..Default::default()
        })
        .await;
        add_documents(&addr, docs).await;
        children.push(addr);
    }
    let (relay_addr, relay, _handle) = start_relay(children.clone()).await;
    let mut client = NodeServiceClient::connect(relay_addr).await.unwrap();

    let merged = client
        .term_stats(stats_request())
        .await
        .unwrap()
        .into_inner();
    let mut shares = Vec::new();
    for child in &children {
        shares.push(direct_stats(child).await);
    }
    assert_eq!(
        merged.doc_count,
        shares.iter().map(|s| s.doc_count).sum::<u64>()
    );
    assert_eq!(
        merged.total_doc_length,
        shares.iter().map(|s| s.total_doc_length).sum::<u64>()
    );
    for ti in 0..3 {
        assert_eq!(
            merged.doc_frequencies[ti],
            shares.iter().map(|s| s.doc_frequencies[ti]).sum::<u32>(),
            "term {ti}"
        );
    }
    assert!(merged.doc_frequencies[0] > 0 && merged.doc_frequencies[2] == 0);
    let field = &merged.field_stats[0];
    assert!(field.known, "the body field is known on every child");
    assert_eq!(
        field.doc_frequencies[0],
        shares
            .iter()
            .map(|s| s.field_stats[0].doc_frequencies[0])
            .sum::<u32>()
    );

    // The token: nonzero, stable while nothing moves, and a translation
    // to each child's epoch that the child itself enforces.
    let t1 = merged.stats_epoch;
    assert_ne!(t1, 0);
    let again = client
        .term_stats(stats_request())
        .await
        .unwrap()
        .into_inner();
    assert_eq!(
        again.stats_epoch, t1,
        "same children, same epochs, same token"
    );
    let claims = relay.translate_epoch(t1).unwrap();
    assert_eq!(claims.len(), 3);
    for (share, claim) in shares.iter().zip(&claims) {
        assert_eq!(share.stats_epoch, claim.epoch);
    }
    assert_eq!(
        relay.translate_epoch(0).unwrap(),
        vec![pipestream_search::stats_identity::StatsClaim::default(); 3],
        "no claim stays no claim"
    );
    let unknown = relay.translate_epoch(t1 ^ 0x5555).unwrap_err();
    assert_eq!(unknown.code(), tonic::Code::FailedPrecondition);
    assert!(
        unknown.message().starts_with("stale stats epoch"),
        "{}",
        unknown.message()
    );

    let bm25 = |epoch: pipestream_search::stats_identity::StatsClaim| Bm25QueryRequest {
        terms: vec!["court".into()],
        k: 5,
        global_doc_count: merged.doc_count,
        global_total_doc_length: merged.total_doc_length,
        global_doc_frequencies: vec![merged.doc_frequencies[0]],
        k1: 1.2,
        b: 0.75,
        expected_stats_epoch: epoch.epoch,
        expected_stats_incarnation: epoch.incarnation(),
        ..Default::default()
    };
    let mut child1 = NodeServiceClient::connect(children[1].clone())
        .await
        .unwrap();
    child1
        .bm25_query(bm25(claims[1]))
        .await
        .expect("the translated claim matches the child's epoch");

    // A child moves: the old claim is refused by the child, the relay
    // issues a new token, and the new claim passes.
    add_documents(&children[1], &["one more court"]).await;
    let stale = child1.bm25_query(bm25(claims[1])).await.unwrap_err();
    assert_eq!(stale.code(), tonic::Code::FailedPrecondition);
    assert!(
        stale.message().starts_with("stale stats epoch"),
        "{}",
        stale.message()
    );
    let refreshed = client
        .term_stats(stats_request())
        .await
        .unwrap()
        .into_inner();
    let t2 = refreshed.stats_epoch;
    assert_ne!(t2, t1, "a moved child is a new token");
    let claims2 = relay.translate_epoch(t2).unwrap();
    assert_ne!(claims2[1], claims[1]);
    assert_eq!(claims2[0], claims[0]);
    child1
        .bm25_query(bm25(claims2[1]))
        .await
        .expect("the refreshed claim passes");
    assert_eq!(
        relay.translate_epoch(t1).unwrap(),
        claims,
        "the older token is still translated while retained; the child is the judge"
    );
}

/// A child address that accepts connections and never speaks: the
/// relay's stream to it stays open until something ends the attempt.
async fn holding_child() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut held = Vec::new();
        loop {
            if let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        }
    });
    format!("http://{addr}")
}

async fn read_until_summary_or_error(
    inbound: &mut tonic::Streaming<pipestream_search::pb::StreamSearchResponse>,
) -> Result<pipestream_search::pb::StreamSearchSummary, tonic::Status> {
    loop {
        match inbound.message().await? {
            Some(pipestream_search::pb::StreamSearchResponse {
                payload: Some(stream_search_response::Payload::Summary(summary)),
            }) => return Ok(summary),
            Some(_) => continue,
            None => panic!("the stream closed without a summary"),
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_map_move_refuses_the_decisions_pinned_under_the_older_map() {
    let leaves = leaves().await;
    let l0 = leaves.addrs[0].clone();

    // The token is pinned to the map revision it was issued under.
    let hot = CoordinatorServiceImpl::new(vec![l0.clone()])
        .with_hot_topology(vec![None])
        .unwrap();
    let (relay_addr, relay, _handle) = start_relay_over(hot).await;
    let mut client = NodeServiceClient::connect(relay_addr).await.unwrap();
    let token = client
        .term_stats(stats_request())
        .await
        .unwrap()
        .into_inner()
        .stats_epoch;
    assert_eq!(relay.map().control_revision, 0);
    assert!(relay.token_tuple(token).is_ok());
    relay
        .base()
        .reload_topology(
            1,
            vec![TopologyRoute {
                addr: l0.clone(),
                replica: None,
                hash_range: None,
                placement: None,
            }],
            None,
        )
        .unwrap();
    assert_eq!(relay.map().control_revision, 1);
    let refused = relay.token_tuple(token).unwrap_err();
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
    assert!(
        refused.message().starts_with("stale stats epoch")
            && refused.message().contains("revision"),
        "{}",
        refused.message()
    );
    let fresh = client
        .term_stats(stats_request())
        .await
        .unwrap()
        .into_inner()
        .stats_epoch;
    assert_ne!(fresh, token);
    assert_eq!(relay.token_tuple(fresh).unwrap().control_revision, 1);

    // A stream opened under one map and still waiting on a child when
    // the map moves is refused by name, not completed under the old map.
    let hold = holding_child().await;
    let hot = CoordinatorServiceImpl::new(vec![l0.clone(), hold])
        .with_hot_topology(vec![None, None])
        .unwrap();
    let (relay_addr, relay, _handle) = start_relay_over(hot).await;
    let mut client = NodeServiceClient::connect(relay_addr).await.unwrap();
    let (tx, rx) = mpsc::channel(4);
    tx.send(StreamSearchRequest {
        payload: Some(stream_search_request::Payload::Start(StartStreamSearch {
            request_id: "moved".into(),
            vector: unit_vectors(1, DIM, 0x0C0D_E001),
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    let mut inbound = client
        .stream_search(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    // The live leaf's first batch arrives through the relay while the
    // holding child keeps the attempt open.
    let first = inbound.message().await.unwrap().expect("a batch");
    assert!(matches!(
        first.payload,
        Some(stream_search_response::Payload::Batch(_))
    ));
    relay
        .base()
        .reload_topology(
            1,
            vec![TopologyRoute {
                addr: l0.clone(),
                replica: None,
                hash_range: None,
                placement: None,
            }],
            None,
        )
        .unwrap();
    let refused = read_until_summary_or_error(&mut inbound).await.unwrap_err();
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
    assert!(
        refused.message().contains("moved from revision 0"),
        "{}",
        refused.message()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_child_error_fails_the_attempt_and_a_stop_yields_an_incomplete_summary() {
    let leaves = leaves().await;
    let l0 = leaves.addrs[0].clone();
    let relay_addr = relay_over(&[&l0, "http://127.0.0.1:1"]).await;
    let root = CoordinatorServiceImpl::new(vec![relay_addr.clone()]);
    let query = unit_vectors(1, DIM, 0x0C0D_E002);
    let err = match root
        .fanout_stream_search("dead-child", &query, K, None, &Default::default())
        .await
    {
        Ok(_) => panic!("a dead child completed"),
        Err(status) => status,
    };
    assert!(
        err.message().contains("failed") || err.message().contains("child"),
        "{}",
        err.message()
    );

    let relay_addr = relay_over(&[&l0]).await;
    let mut client = NodeServiceClient::connect(relay_addr).await.unwrap();
    let (tx, rx) = mpsc::channel(4);
    tx.send(StreamSearchRequest {
        payload: Some(stream_search_request::Payload::Start(StartStreamSearch {
            request_id: "stopped".into(),
            vector: query.clone(),
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    tx.send(StreamSearchRequest {
        payload: Some(stream_search_request::Payload::Stop(StopStreamSearch {})),
    })
    .await
    .unwrap();
    let mut inbound = client
        .stream_search(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    let summary = read_until_summary_or_error(&mut inbound).await.unwrap();
    assert!(!summary.completed, "a parent's Stop is never a completion");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unsupported_routes_refuse_by_name() {
    let leaves = leaves().await;
    let relay_addr = relay_over(&[&leaves.addrs[0]]).await;
    let mut client = NodeServiceClient::connect(relay_addr).await.unwrap();
    let err = client
        .get_documents(pipestream_search::pb::GetDocumentsRequest::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unimplemented);
    assert!(
        err.message().contains("relay") && err.message().contains("GetDocuments"),
        "{}",
        err.message()
    );
    // Aggregates the relay cannot merge without changing bits refuse by
    // name on the routes it does serve.
    let err = client
        .bm25_query(Bm25QueryRequest {
            terms: vec!["court".into()],
            k: 3,
            stats_fields: vec!["score".into()],
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unimplemented);
    assert!(
        err.message().contains("stats_fields") && err.message().contains("relay"),
        "{}",
        err.message()
    );
    let (tx, rx) = mpsc::channel::<AddDocumentsRequest>(1);
    drop(tx);
    let err = client
        .add_documents(ReceiverStream::new(rx))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unimplemented);
    assert!(err.message().contains("AddDocuments"), "{}", err.message());
    let err = client
        .compact_shard(pipestream_search::pb::CompactShardRequest::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unimplemented);
    assert!(err.message().contains("CompactShard"), "{}", err.message());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn health_merges_contiguous_children_and_refuses_a_gap() {
    let leaves = leaves().await;
    let (relay_addr, relay, _handle) =
        start_relay(vec![leaves.addrs[1].clone(), leaves.addrs[0].clone()]).await;
    let merged = relay.check_children().await.expect("contiguous children");
    assert_eq!(merged.slot_offset, 0);
    assert_eq!(merged.num_vectors, (2 * LEAF_ROWS) as u64);
    assert!(!merged.scoring_fingerprint.is_empty());
    assert!(!merged.wal_clocked && merged.wal_generation == 0);
    let over_the_wire = NodeServiceClient::connect(relay_addr)
        .await
        .unwrap()
        .health(HealthRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(over_the_wire.num_vectors, merged.num_vectors);
    assert_eq!(over_the_wire.slot_offset, 0);

    // A leaf placed past a gap.
    let mut index = VectorIndex::create(EMBEDDED_TURBOVEC, DIM, BIT_WIDTH).unwrap();
    index.add(&unit_vectors(100, DIM, 0x6A9), DIM).unwrap();
    index.prepare().unwrap();
    let (gapped, _handle) = start_node(
        index,
        NodeConfig {
            slot_offset: (LEAF_ROWS + 7) as u64,
            ..Default::default()
        },
    )
    .await;
    let (relay_addr, relay, _handle) = start_relay(vec![leaves.addrs[0].clone(), gapped]).await;
    let err = relay.check_children().await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("gap"), "{}", err.message());
    let err = NodeServiceClient::connect(relay_addr)
        .await
        .unwrap()
        .health(HealthRequest {})
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("contiguous"), "{}", err.message());
}

// --- The keyword leg ------------------------------------------------------

const LEX_LEAVES: usize = 4;
const LEX_ROWS: usize = 8;
const LEX_K: u32 = 6;

/// One leaf's texts. Each leaf pads its documents by a different amount,
/// so a document with the same words in another leaf scores differently:
/// the flat unary fan-out orders equal scores by shard index and a relay
/// by id, and only equal scores could tell the two apart.
fn lexical_texts(leaf: usize) -> Vec<String> {
    let base = [
        "the court held",
        "an opinion of the court",
        "zebra crossing ahead",
        "the court opinion on the zebra",
        "a lone court",
        "no match in here",
        "opinion opinion",
        "zebra zebra court",
    ];
    base.iter()
        .map(|words| format!("{words}{}", " pad".repeat(leaf + 1)))
        .collect()
}

struct LexicalLeaves {
    addrs: Vec<String>,
    analysis: String,
    corpus: Vec<f32>,
    _mock: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

async fn set_calibration(addr: &str, shift: &[f32], scale: &[f32]) {
    NodeServiceClient::connect(addr.to_string())
        .await
        .unwrap()
        .set_calibration(SetCalibrationRequest {
            dim: DIM as u32,
            bit_width: BIT_WIDTH as u32,
            shift: shift.to_vec(),
            scale: scale.to_vec(),
        })
        .await
        .unwrap();
}

async fn add_faceted_documents(addr: &str, texts: &[String]) {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(4);
    let texts = texts.to_vec();
    let feeder = tokio::spawn(async move {
        for (i, text) in texts.into_iter().enumerate() {
            tx.send(AddDocumentsRequest {
                text,
                facets: vec![FacetValue {
                    field: "court".into(),
                    value: if i % 2 == 0 {
                        "scotus".into()
                    } else {
                        "ca9".into()
                    },
                }],
                ..Default::default()
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
    let (tx, rx) = mpsc::channel(1);
    tx.send(AddVectorsRequest {
        vectors,
        dim: DIM as u32,
    })
    .await
    .unwrap();
    drop(tx);
    client.add_vectors(ReceiverStream::new(rx)).await.unwrap();
}

/// Four leaves with documents, a facet, and vectors aligned by id, on
/// contiguous slot ranges; `positions[leaf]` gives the leaf positions on
/// the body field.
async fn lexical_leaves(positions: &[bool; LEX_LEAVES]) -> LexicalLeaves {
    let (analysis, mock) = start_mock_analysis().await;
    let corpus = unit_vectors(LEX_LEAVES * LEX_ROWS, DIM, 0x1E7A_2E1A);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &corpus);
    let mut addrs = Vec::new();
    for leaf in 0..LEX_LEAVES {
        let (addr, _handle) = start_empty_node(NodeConfig {
            slot_offset: (leaf * LEX_ROWS) as u64,
            analysis_addr: Some(analysis.clone()),
            facet_fields: vec!["court".into()],
            position_fields: if positions[leaf] {
                vec!["body".into()]
            } else {
                Vec::new()
            },
            ..Default::default()
        })
        .await;
        set_calibration(&addr, &shift, &scale).await;
        add_faceted_documents(&addr, &lexical_texts(leaf)).await;
        add_vectors(
            &addr,
            corpus[leaf * LEX_ROWS * DIM..(leaf + 1) * LEX_ROWS * DIM].to_vec(),
        )
        .await;
        addrs.push(addr);
    }
    LexicalLeaves {
        addrs,
        analysis,
        corpus,
        _mock: mock,
    }
}

fn lexical_request(text: &str) -> Bm25SearchRequest {
    Bm25SearchRequest {
        text: text.into(),
        k: LEX_K,
        facet_fields: vec!["court".into()],
        explain: true,
        ..Default::default()
    }
}

fn rrf_legs() -> HybridLegs {
    HybridLegs {
        leg_k: 12,
        vector_weight: 1.0,
        bm25_weight: 1.0,
        rrf_k: 60.0,
        fusion_mode: FusionMode::GlobalRank,
        normalization: Normalization::MinMax,
        combination: Combination::Arithmetic,
        min_vector_score: 0.0,
    }
}

/// A weighted, normalized sum of the two legs. (The decomposed mode
/// rescores vector candidates by id through `VectorRescore`, a
/// vector-side follow-up that is outside the relay's scope.)
fn weighted_legs() -> HybridLegs {
    HybridLegs {
        vector_weight: 0.7,
        bm25_weight: 0.3,
        fusion_mode: FusionMode::ScoreBlend,
        ..rrf_legs()
    }
}

/// (doc id, fused score bits, vector rank, vector score bits, bm25 rank,
/// bm25 score bits).
type HybridBits = (u64, u32, Option<u32>, u32, Option<u32>, u32);

/// A hybrid hit without the shard index the parent names, which is the
/// relay's index through a relay and the leaf's on the flat path.
fn hybrid_bits(hits: &[HybridHit]) -> Vec<HybridBits> {
    hits.iter()
        .map(|h| {
            (
                h.doc_id,
                h.fused_score.to_bits(),
                h.vector_rank,
                h.vector_score.to_bits(),
                h.bm25_rank,
                h.bm25_score.to_bits(),
            )
        })
        .collect()
}

async fn lexical(
    coordinator: &CoordinatorServiceImpl,
    text: &str,
) -> (
    Vec<Bm25Hit>,
    Vec<pipestream_search::pb::FacetFieldCounts>,
    u32,
) {
    let response = coordinator
        .bm25_search(tonic::Request::new(lexical_request(text)))
        .await
        .expect("lexical search")
        .into_inner();
    (response.hits, response.facets, response.segments_total)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lexical_and_hybrid_queries_through_relays_equal_the_flat_fanout() {
    let leaves = lexical_leaves(&[true; LEX_LEAVES]).await;
    let l = &leaves.addrs;
    // Stream search on: a relay serves the streaming vector route, and
    // the cascade's gate takes the unary one otherwise.
    let with_bm25 = |addrs: Vec<String>, stream: bool| {
        CoordinatorServiceImpl::new(addrs)
            .with_bm25(Some(leaves.analysis.clone()), Default::default())
            .with_bm25_stream(stream)
            .with_stream_search(true)
    };
    let a = relay_over(&[&l[0], &l[1]]).await;
    let b = relay_over(&[&l[2], &l[3]]).await;
    let p = relay_over(&[&l[3], &l[1]]).await;
    let q = relay_over(&[&l[2], &l[0]]).await;
    let top = relay_over(&[&a, &b]).await;
    let trees: Vec<(&str, Vec<String>)> = vec![
        ("one level", vec![a.clone(), b.clone()]),
        ("permuted", vec![q, p]),
        ("two levels", vec![top]),
    ];
    let queries = ["court", "zebra court", "opinion", "held"];
    for stream in [false, true] {
        let flat = with_bm25(l.clone(), stream);
        for (name, roots) in &trees {
            let relayed = with_bm25(roots.clone(), stream);
            for text in queries {
                let (want_hits, want_facets, want_segments) = lexical(&flat, text).await;
                let (hits, facets, segments) = lexical(&relayed, text).await;
                assert!(!want_hits.is_empty(), "{text}: the flat query matched");
                assert_eq!(
                    hits, want_hits,
                    "{name} stream={stream} {text:?}: lexical hits"
                );
                assert!(
                    hits.iter().all(|h| h.explain.is_some()),
                    "{name} stream={stream} {text:?}: explain carried through"
                );
                assert_eq!(
                    facets, want_facets,
                    "{name} stream={stream} {text:?}: facets"
                );
                assert_eq!(
                    segments, want_segments,
                    "{name} stream={stream} {text:?}: segment count"
                );
            }
        }
    }
    // Hybrid: the fusions whose answer does not depend on how shards are
    // grouped (global ranks, a normalized weighted sum). The cascade's
    // vector gate takes `SearchShard`, a vector route this relay does not
    // serve; its rescoring half is exercised on its own below.
    let flat = with_bm25(l.clone(), true);
    for (qi, text) in ["court", "zebra court"].into_iter().enumerate() {
        let query = leaves.corpus[qi * DIM..(qi + 1) * DIM].to_vec();
        let filters = Default::default();
        let (want_rrf, _) = flat
            .fanout_hybrid("h", text, &query, LEX_K, None, rrf_legs(), false, &filters)
            .await
            .expect("flat rrf");
        let (want_weighted, _) = flat
            .fanout_hybrid(
                "w",
                text,
                &query,
                LEX_K,
                None,
                weighted_legs(),
                false,
                &filters,
            )
            .await
            .expect("flat weighted");
        assert!(!want_rrf.is_empty(), "{text}: hybrid matched");
        for (name, roots) in &trees {
            let relayed = with_bm25(roots.clone(), true);
            let (rrf, _) = relayed
                .fanout_hybrid("h", text, &query, LEX_K, None, rrf_legs(), false, &filters)
                .await
                .expect("relayed rrf");
            assert_eq!(
                hybrid_bits(&rrf),
                hybrid_bits(&want_rrf),
                "{name} {text:?}: rrf"
            );
            let (weighted, _) = relayed
                .fanout_hybrid(
                    "w",
                    text,
                    &query,
                    LEX_K,
                    None,
                    weighted_legs(),
                    false,
                    &filters,
                )
                .await
                .expect("relayed weighted");
            assert_eq!(
                hybrid_bits(&weighted),
                hybrid_bits(&want_weighted),
                "{name} {text:?}: weighted sum"
            );
        }
    }
}

fn scoring_request(stats: &TermStatsResponse, token: u64) -> Bm25QueryRequest {
    Bm25QueryRequest {
        terms: vec!["court".into()],
        k: 5,
        global_doc_count: stats.doc_count,
        global_total_doc_length: stats.total_doc_length,
        global_doc_frequencies: stats.doc_frequencies.clone(),
        k1: 1.2,
        b: 0.75,
        expected_stats_epoch: token,
        expected_stats_incarnation: if token == 0 {
            Vec::new()
        } else {
            stats.stats_incarnation.clone()
        },
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_moved_child_refuses_the_relayed_claim_and_a_refetch_restores_it() {
    let leaves = lexical_leaves(&[true; LEX_LEAVES]).await;
    let l = &leaves.addrs;
    let relay_addr = relay_over(&[&l[0], &l[1]]).await;
    let root = CoordinatorServiceImpl::new(vec![relay_addr.clone()])
        .with_bm25(Some(leaves.analysis.clone()), Default::default());
    let (before, _, _) = lexical(&root, "court").await;
    assert!(!before.is_empty());

    let mut relay = NodeServiceClient::connect(relay_addr.clone())
        .await
        .unwrap();
    let request = TermStatsRequest {
        version_only: false,
        visibility: None,
        terms: vec!["court".into()],
        fields: Vec::new(),
    };
    let stats = relay
        .term_stats(request.clone())
        .await
        .unwrap()
        .into_inner();
    let token = stats.stats_epoch;
    assert_ne!(token, 0);
    let scored = relay
        .bm25_query(scoring_request(&stats, token))
        .await
        .expect("the claim translates to each child")
        .into_inner();
    assert!(!scored.hits.is_empty());

    // Child 0 moves: the token's claim on it is stale, and the child says
    // so through the relay with the prefix the root's retry rule reads.
    add_faceted_documents(&l[0], &["a new court opinion".to_string()]).await;
    let err = relay
        .bm25_query(scoring_request(&stats, token))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(
        err.message().starts_with("stale stats epoch"),
        "{}",
        err.message()
    );
    assert!(err.message().contains("relay child 0"), "{}", err.message());

    // The root refetches on its own and answers with the new document.
    let (after, _, _) = lexical(&root, "court").await;
    assert_eq!(after.len(), before.len().max(1));
    assert!(
        after
            .iter()
            .any(|h| !before.iter().any(|b| b.doc_id == h.doc_id))
            || after.len() > before.len()
            || after != before,
        "the moved child's document reached the root's answer"
    );

    let fresh = relay.term_stats(request).await.unwrap().into_inner();
    assert_ne!(fresh.stats_epoch, token, "a moved child is a new token");
    let scored = relay
        .bm25_query(scoring_request(&fresh, fresh.stats_epoch))
        .await
        .expect("the fresh claim translates")
        .into_inner();
    assert!(!scored.hits.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_phrase_refuses_under_mixed_position_capabilities() {
    let leaves = lexical_leaves(&[true, false, true, true]).await;
    let l = &leaves.addrs;
    let mixed = relay_over(&[&l[0], &l[1]]).await;
    let uniform = relay_over(&[&l[2], &l[3]]).await;
    let root = CoordinatorServiceImpl::new(vec![mixed, uniform])
        .with_bm25(Some(leaves.analysis.clone()), Default::default());
    let err = root
        .bm25_search(tonic::Request::new(Bm25SearchRequest {
            text: "court opinion".into(),
            k: 4,
            phrase: Some(PhraseMatch { slop: 0 }),
            ..Default::default()
        }))
        .await
        .expect_err("a phrase over a relay whose children disagree on positions is refused");
    assert!(
        err.message().contains("positions") && err.message().contains("relay"),
        "{}",
        err.message()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_parent_stop_mid_stream_yields_an_incomplete_bm25_certificate() {
    let leaves = lexical_leaves(&[true; LEX_LEAVES]).await;
    let holding = holding_child().await;
    let relay_addr = relay_over(&[&leaves.addrs[0], &holding]).await;
    let stats = NodeServiceClient::connect(leaves.addrs[0].clone())
        .await
        .unwrap()
        .term_stats(TermStatsRequest {
            version_only: false,
            visibility: None,
            terms: vec!["court".into()],
            fields: Vec::new(),
        })
        .await
        .unwrap()
        .into_inner();
    let mut relay = NodeServiceClient::connect(relay_addr).await.unwrap();
    let (tx, rx) = mpsc::channel::<Bm25QueryStreamRequest>(4);
    tx.send(Bm25QueryStreamRequest {
        payload: Some(bm25_query_stream_request::Payload::Start(scoring_request(
            &stats, 0,
        ))),
    })
    .await
    .unwrap();
    let mut inbound = relay
        .bm25_query_stream(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    tx.send(Bm25QueryStreamRequest {
        payload: Some(bm25_query_stream_request::Payload::Stop(StopBm25Query {})),
    })
    .await
    .unwrap();
    let completion = loop {
        match inbound
            .message()
            .await
            .expect("the relay answers, not errors")
        {
            Some(pipestream_search::pb::Bm25QueryStreamResponse {
                payload: Some(bm25_query_stream_response::Payload::Completion(c)),
            }) => break c,
            Some(_) => continue,
            None => panic!("the stream closed without a completion"),
        }
    };
    assert!(
        !completion.completed,
        "a stopped scan is not certified complete"
    );
    assert!(completion.response.is_none());
    drop(tx);
}

/// The cascade's rescoring half: candidate ids routed to the child whose
/// slot range holds them, each child under its own translated claim,
/// the hits the same as the children's own answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rescore_through_a_relay_routes_each_id_to_its_child() {
    let leaves = lexical_leaves(&[true; LEX_LEAVES]).await;
    let l = &leaves.addrs;
    let relay_addr = relay_over(&[&l[0], &l[1]]).await;
    let mut relay = NodeServiceClient::connect(relay_addr).await.unwrap();
    let stats = relay
        .term_stats(TermStatsRequest {
            version_only: false,
            visibility: None,
            terms: vec!["court".into()],
            fields: Vec::new(),
        })
        .await
        .unwrap()
        .into_inner();
    let claims = stats.stats_epoch;
    // Ids from both children, out of order.
    let ids: Vec<u64> = vec![LEX_ROWS as u64 + 3, 0, 4, LEX_ROWS as u64, 5];
    let rescore = |ids: Vec<u64>, claim: u64| pipestream_search::pb::Bm25RescoreRequest {
        analysis_fingerprint: 0,
        terms: vec!["court".into()],
        global_doc_count: stats.doc_count,
        global_total_doc_length: stats.total_doc_length,
        global_doc_frequencies: stats.doc_frequencies.clone(),
        candidate_ids: ids,
        k1: 1.2,
        b: 0.75,
        expected_stats_epoch: claim,
        expected_stats_incarnation: if claim == 0 {
            Vec::new()
        } else {
            stats.stats_incarnation.clone()
        },
        score_stages: Vec::new(),
    };
    let through = relay
        .bm25_rescore(rescore(ids.clone(), claims))
        .await
        .expect("routed rescore")
        .into_inner();
    let mut want = Vec::new();
    for (child, addr) in l.iter().take(2).enumerate() {
        let mine: Vec<u64> = ids
            .iter()
            .copied()
            .filter(|&id| (id as usize) / LEX_ROWS == child)
            .collect();
        let direct = NodeServiceClient::connect(addr.clone())
            .await
            .unwrap()
            .bm25_rescore(rescore(mine, 0))
            .await
            .unwrap()
            .into_inner();
        want.extend(direct.hits);
    }
    let key = |h: &Bm25Hit| (h.doc_id, h.score.to_bits());
    let mut got: Vec<_> = through.hits.iter().map(key).collect();
    let mut expected: Vec<_> = want.iter().map(key).collect();
    got.sort_unstable();
    expected.sort_unstable();
    assert_eq!(
        got, expected,
        "the routed rescore equals the children's own answers"
    );
    assert!(!got.is_empty());
    let err = relay
        .bm25_rescore(rescore(vec![(LEX_LEAVES * LEX_ROWS + 10) as u64], claims))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(
        err.message().contains("no child's slot range"),
        "{}",
        err.message()
    );
}
