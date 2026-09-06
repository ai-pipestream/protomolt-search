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
        assert_eq!(share.stats_epoch, *claim);
    }
    assert_eq!(
        relay.translate_epoch(0).unwrap(),
        vec![0, 0, 0],
        "no claim stays no claim"
    );
    let unknown = relay.translate_epoch(t1 ^ 0x5555).unwrap_err();
    assert_eq!(unknown.code(), tonic::Code::FailedPrecondition);
    assert!(
        unknown.message().starts_with("stale stats epoch"),
        "{}",
        unknown.message()
    );

    let bm25 = |epoch: u64| Bm25QueryRequest {
        terms: vec!["court".into()],
        k: 5,
        global_doc_count: merged.doc_count,
        global_total_doc_length: merged.total_doc_length,
        global_doc_frequencies: vec![merged.doc_frequencies[0]],
        k1: 1.2,
        b: 0.75,
        expected_stats_epoch: epoch,
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
async fn vector_backend_merges_the_children_and_refuses_a_mismatch() {
    let leaves = leaves().await;
    let (relay_addr, _relay, _handle) =
        start_relay(vec![leaves.addrs[0].clone(), leaves.addrs[1].clone()]).await;
    let leaf = NodeServiceClient::connect(leaves.addrs[0].clone())
        .await
        .unwrap()
        .get_vector_backend(pipestream_search::pb::GetVectorBackendRequest {})
        .await
        .unwrap()
        .into_inner();
    let merged = NodeServiceClient::connect(relay_addr)
        .await
        .unwrap()
        .get_vector_backend(pipestream_search::pb::GetVectorBackendRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(leaf.descriptor.is_some());
    assert_eq!(merged.descriptor, leaf.descriptor);
    assert_eq!(merged.config, leaf.config);
    assert_eq!(merged.num_vectors, (2 * LEAF_ROWS) as u64);

    // A child of another dimension is contiguous in slots and healthy,
    // yet a different provider identity: the relay refuses by name.
    let wide = DIM * 2;
    let mut index = VectorIndex::create(EMBEDDED_TURBOVEC, wide, BIT_WIDTH).unwrap();
    index.add(&unit_vectors(100, wide, 0x6A9), wide).unwrap();
    index.prepare().unwrap();
    let (other, _handle) = start_node(
        index,
        NodeConfig {
            slot_offset: LEAF_ROWS as u64,
            ..Default::default()
        },
    )
    .await;
    let (relay_addr, _relay, _handle) = start_relay(vec![leaves.addrs[0].clone(), other]).await;
    let err = NodeServiceClient::connect(relay_addr)
        .await
        .unwrap()
        .get_vector_backend(pipestream_search::pb::GetVectorBackendRequest {})
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(
        err.message().contains("child 1") && err.message().contains("descriptor"),
        "{}",
        err.message()
    );
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
        terms: vec!["court".into()],
        global_doc_count: stats.doc_count,
        global_total_doc_length: stats.total_doc_length,
        global_doc_frequencies: stats.doc_frequencies.clone(),
        candidate_ids: ids,
        k1: 1.2,
        b: 0.75,
        expected_stats_epoch: claim,
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
    // An id in no child's range is another shard's: ignored, as a node
    // ignores an id outside its own range (the boolean planner sends
    // every shard every candidate).
    let foreign = relay
        .bm25_rescore(rescore(vec![(LEX_LEAVES * LEX_ROWS + 10) as u64], claims))
        .await
        .expect("a foreign id is not this shard's to refuse")
        .into_inner();
    assert!(foreign.hits.is_empty());
}

// --- The vector-side routes, the bitmaps, the dictionaries ----------------

/// `(vector id, score bits, parent id)` of a unary fan-out's hits.
fn unary_bits(hits: &[ScoredHit]) -> Vec<(u64, u32, u64)> {
    hits.iter()
        .map(|h| (h.vector_id, h.score.to_bits(), h.parent_id))
        .collect()
}

/// The cascade's gate over a fan-out's raw lists: every candidate at or
/// above the k-th best score, shard assignments dropped. The raw lists
/// themselves depend on which floors reached each leaf when; the gate
/// is score-defined and must not depend on grouping.
fn pool(result: &pipestream_search::coordinator::FanoutResult, k: usize) -> Vec<(u64, u32)> {
    let mut all: Vec<(u64, f32)> = result
        .shard_hits
        .iter()
        .flat_map(|(_, hits)| hits.iter().copied())
        .collect();
    all.sort_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    let boundary = all.get(k - 1).map_or(f32::NEG_INFINITY, |h| h.1);
    let mut gate: Vec<(u64, u32)> = all
        .into_iter()
        .filter(|h| h.1 >= boundary)
        .map(|(id, score)| (id, score.to_bits()))
        .collect();
    gate.sort_unstable();
    gate
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unary_vector_search_through_relays_equals_the_flat_fanout() {
    let leaves = leaves().await;
    let l = &leaves.addrs;
    let flat = CoordinatorServiceImpl::new(l.clone());
    let a = relay_over(&[&l[0], &l[1]]).await;
    let b = relay_over(&[&l[2], &l[3]]).await;
    // Children out of slot order, relays out of order at the root; the
    // groups are contiguous, as the routes routed by slot range require.
    let p = relay_over(&[&l[3], &l[2]]).await;
    let q = relay_over(&[&l[1], &l[0]]).await;
    let top = relay_over(&[&a, &b]).await;
    let trees = [
        (
            "one level",
            CoordinatorServiceImpl::new(vec![a.clone(), b.clone()]),
        ),
        ("permuted", CoordinatorServiceImpl::new(vec![q, p])),
        ("two levels", CoordinatorServiceImpl::new(vec![top])),
    ];
    let mut queries: Vec<(String, Vec<f32>)> = (0..4u64)
        .map(|qi| (format!("q{qi}"), unit_vectors(1, DIM, 0x57AE_2E1A + qi)))
        .collect();
    let tie = leaves.corpus[TIE_SOURCE * DIM..(TIE_SOURCE + 1) * DIM].to_vec();
    queries.push(("tie".to_string(), tie));
    let filters = Default::default();
    for (name, query) in &queries {
        let want = monolithic_topk(&leaves.monolithic, query, K as usize);
        for tie_complete in [false, true] {
            let flat_result = flat
                .fanout_search(name, query, K, tie_complete, &filters)
                .await
                .expect("flat unary fan-out");
            assert_eq!(bits(&flat_result.hits), want, "{name}: flat != monolithic");
            let flat_pool = pool(&flat_result, K as usize);
            let flat_scanned: u64 = flat_result
                .shard_stats
                .iter()
                .flatten()
                .map(|s| s.candidates_collected)
                .sum();
            for (label, tree) in &trees {
                let got = tree
                    .fanout_search(name, query, K, tie_complete, &filters)
                    .await
                    .unwrap_or_else(|e| panic!("{name}: {label}: {e}"));
                assert_eq!(
                    bits(&got.hits),
                    want,
                    "{name}: {label} tie_complete={tie_complete}"
                );
                assert_eq!(
                    pool(&got, K as usize),
                    flat_pool,
                    "{name}: {label} tie_complete={tie_complete}: the gate is the same set"
                );
                let scanned: u64 = got
                    .shard_stats
                    .iter()
                    .flatten()
                    .map(|s| s.candidates_collected)
                    .sum();
                assert!(
                    scanned > 0 && scanned <= flat_scanned.max(scanned),
                    "{name}: {label}: the relays' scan counters sum the leaves'"
                );
            }
        }
        // Collapse by parent: leaves without a document store parent
        // themselves, and the relay concatenates the representatives.
        let flat_collapse = flat
            .fanout_search_collapse(name, query, K, &filters)
            .await
            .expect("flat collapse");
        for (label, tree) in &trees {
            let got = tree
                .fanout_search_collapse(name, query, K, &filters)
                .await
                .unwrap_or_else(|e| panic!("{name}: {label}: {e}"));
            assert_eq!(
                unary_bits(&got.hits),
                unary_bits(&flat_collapse.hits),
                "{name}: {label}: collapse"
            );
        }
    }
}

/// `(doc id, rank, vector score bits, bm25 score bits)`: a cascade hit
/// without the shard index, which names the relay through a relay.
fn cascade_bits(hits: &[pipestream_search::pb::CascadeHit]) -> Vec<(u64, u32, u32, u32)> {
    hits.iter()
        .map(|h| {
            (
                h.doc_id,
                h.rank,
                h.vector_score.to_bits(),
                h.bm25_score.to_bits(),
            )
        })
        .collect()
}

fn decomposed_legs() -> HybridLegs {
    HybridLegs {
        vector_weight: 0.6,
        bm25_weight: 0.4,
        fusion_mode: FusionMode::Decomposed,
        ..rrf_legs()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cascade_and_decomposed_fusion_through_relays_equal_the_flat_fanout() {
    let leaves = lexical_leaves(&[true; LEX_LEAVES]).await;
    let l = &leaves.addrs;
    let with_bm25 = |addrs: Vec<String>| {
        CoordinatorServiceImpl::new(addrs)
            .with_bm25(Some(leaves.analysis.clone()), Default::default())
            .with_stream_search(true)
    };
    let a = relay_over(&[&l[0], &l[1]]).await;
    let b = relay_over(&[&l[2], &l[3]]).await;
    // Children out of slot order, relays out of order at the root; the
    // groups are contiguous, as the routes routed by slot range require.
    let p = relay_over(&[&l[3], &l[2]]).await;
    let q = relay_over(&[&l[1], &l[0]]).await;
    let top = relay_over(&[&a, &b]).await;
    let trees: Vec<(&str, Vec<String>)> = vec![
        ("one level", vec![a.clone(), b.clone()]),
        ("permuted", vec![q, p]),
        ("two levels", vec![top]),
    ];
    let flat = with_bm25(l.clone());
    let filters = Default::default();
    for (qi, text) in ["court", "zebra court", "opinion"].into_iter().enumerate() {
        let query = leaves.corpus[qi * DIM..(qi + 1) * DIM].to_vec();
        let (want_cascade, _) = flat
            .fanout_cascade("c", text, &query, LEX_K, None, 0.0, false, &filters)
            .await
            .expect("flat cascade");
        assert!(!want_cascade.is_empty(), "{text}: the cascade matched");
        let (want_decomposed, _) = flat
            .fanout_hybrid(
                "d",
                text,
                &query,
                LEX_K,
                None,
                decomposed_legs(),
                false,
                &filters,
            )
            .await
            .expect("flat decomposed");
        assert!(!want_decomposed.is_empty(), "{text}: decomposed matched");
        for (name, roots) in &trees {
            let relayed = with_bm25(roots.clone());
            let (cascade, _) = relayed
                .fanout_cascade("c", text, &query, LEX_K, None, 0.0, false, &filters)
                .await
                .unwrap_or_else(|e| panic!("{name} {text:?}: cascade: {e}"));
            assert_eq!(
                cascade_bits(&cascade),
                cascade_bits(&want_cascade),
                "{name} {text:?}: cascade"
            );
            let (decomposed, _) = relayed
                .fanout_hybrid(
                    "d",
                    text,
                    &query,
                    LEX_K,
                    None,
                    decomposed_legs(),
                    false,
                    &filters,
                )
                .await
                .unwrap_or_else(|e| panic!("{name} {text:?}: decomposed: {e}"));
            assert_eq!(
                hybrid_bits(&decomposed),
                hybrid_bits(&want_decomposed),
                "{name} {text:?}: decomposed fusion"
            );
        }
    }
}

use pipestream_search::pb::{
    filter_query, search_query, selection_query, BooleanQuery, CompositeSearchStrategy, DenseQuery,
    FilterQuery, LexicalQuery, QueryHit, QueryRequest, SearchQuery, SelectionOperator,
    SelectionQuery,
};

fn cel_clause(id: &str, cel: &str) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Filter(FilterQuery {
            id: id.to_string(),
            predicate: Some(filter_query::Predicate::Cel(cel.to_string())),
        })),
    }
}

fn lexical_clause(id: &str, text: &str) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: id.to_string(),
            query: Some(search_query::Query::Lexical(LexicalQuery {
                text: text.to_string(),
                ..Default::default()
            })),
        })),
    }
}

fn dense_clause(id: &str, vector: &[f32]) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: id.to_string(),
            query: Some(search_query::Query::Dense(DenseQuery {
                vector: vector.to_vec(),
                ..Default::default()
            })),
        })),
    }
}

/// A dense clause reranked on the original vectors (`ExactVectorRescore`).
fn fp32_clause(id: &str, vector: &[f32]) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: id.to_string(),
            query: Some(search_query::Query::Dense(DenseQuery {
                vector: vector.to_vec(),
                score_mode: pipestream_search::pb::DenseScoreMode::Fp32Rerank as i32,
                ..Default::default()
            })),
        })),
    }
}

fn and_filtered(filter: SelectionQuery, leaf: SelectionQuery) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Composite(CompositeSearchStrategy {
            operator: SelectionOperator::And as i32,
            clauses: vec![filter, leaf],
            scoring: None,
        })),
    }
}

fn boolean(
    must: Vec<SelectionQuery>,
    should: Vec<SelectionQuery>,
    must_not: Vec<SelectionQuery>,
) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Boolean(BooleanQuery {
            must,
            should,
            must_not,
            minimum_should_match: 0,
            aggregate: None,
        })),
    }
}

/// `(doc id, score bits, rank, matched clauses)`.
fn query_bits(hits: &[QueryHit]) -> Vec<(u64, u32, u32, Vec<String>)> {
    hits.iter()
        .map(|h| (h.doc_id, h.score.to_bits(), h.rank, h.matched.clone()))
        .collect()
}

async fn run_query(
    coordinator: &CoordinatorServiceImpl,
    selection: SelectionQuery,
) -> Result<Vec<QueryHit>, tonic::Status> {
    coordinator
        .query(tonic::Request::new(QueryRequest {
            request_id: "relayed".into(),
            k: LEX_K,
            selection: Some(selection),
            ..Default::default()
        }))
        .await
        .map(|r| r.into_inner().hits)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn filtered_and_boolean_queries_through_relays_equal_the_flat_fanout() {
    let leaves = lexical_leaves(&[true; LEX_LEAVES]).await;
    let l = &leaves.addrs;
    let with_bm25 = |addrs: Vec<String>| {
        CoordinatorServiceImpl::new(addrs)
            .with_bm25(Some(leaves.analysis.clone()), Default::default())
            .with_stream_search(true)
    };
    let a = relay_over(&[&l[0], &l[1]]).await;
    let b = relay_over(&[&l[2], &l[3]]).await;
    // Children out of slot order, relays out of order at the root; the
    // groups are contiguous, as the routes routed by slot range require.
    let p = relay_over(&[&l[3], &l[2]]).await;
    let q = relay_over(&[&l[1], &l[0]]).await;
    let top = relay_over(&[&a, &b]).await;
    let trees: Vec<(&str, Vec<String>)> = vec![
        ("one level", vec![a.clone(), b.clone()]),
        ("permuted", vec![q, p]),
        ("two levels", vec![top]),
    ];
    let flat = with_bm25(l.clone());
    let vector = leaves.corpus[..DIM].to_vec();
    let shapes: Vec<(&str, SelectionQuery)> = vec![
        (
            "filtered lexical",
            and_filtered(
                cel_clause("f", r#"court == "scotus""#),
                lexical_clause("lex", "court"),
            ),
        ),
        (
            "filtered dense",
            and_filtered(
                cel_clause("f", r#"court == "ca9""#),
                dense_clause("vec", &vector),
            ),
        ),
        (
            "boolean lexical must filter",
            boolean(
                vec![
                    lexical_clause("lex", "court"),
                    cel_clause("f", r#"court == "scotus""#),
                ],
                Vec::new(),
                Vec::new(),
            ),
        ),
        (
            "boolean dense must filter",
            boolean(
                vec![
                    dense_clause("vec", &vector),
                    cel_clause("f", r#"court == "ca9""#),
                ],
                Vec::new(),
                Vec::new(),
            ),
        ),
        ("fp32 rerank", fp32_clause("vec", &vector)),
        (
            "boolean fp32 must filter",
            boolean(
                vec![
                    fp32_clause("vec", &vector),
                    cel_clause("f", r#"court == "scotus""#),
                ],
                Vec::new(),
                Vec::new(),
            ),
        ),
        (
            "boolean should and must_not",
            boolean(
                vec![lexical_clause("lex", "court")],
                vec![lexical_clause("z", "zebra"), dense_clause("vec", &vector)],
                vec![cel_clause("f", r#"court == "ca9""#)],
            ),
        ),
    ];
    for (shape, selection) in &shapes {
        let want = run_query(&flat, selection.clone())
            .await
            .unwrap_or_else(|e| panic!("{shape}: flat: {e}"));
        assert!(!want.is_empty(), "{shape}: the flat query matched");
        for (name, roots) in &trees {
            let relayed = with_bm25(roots.clone());
            let got = run_query(&relayed, selection.clone())
                .await
                .unwrap_or_else(|e| panic!("{shape}: {name}: {e}"));
            assert_eq!(query_bits(&got), query_bits(&want), "{shape}: {name}");
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bitmaps_through_a_relay_lie_over_the_children_and_refuse_a_gap() {
    let leaves = lexical_leaves(&[true; LEX_LEAVES]).await;
    let l = &leaves.addrs;
    let relay_addr = relay_over(&[&l[1], &l[0]]).await;
    let mut relay = NodeServiceClient::connect(relay_addr).await.unwrap();
    let request = pipestream_search::pb::LexicalBitmapRequest {
        terms: vec!["court".into()],
    };
    let through = relay
        .resolve_lexical_bitmap(request.clone())
        .await
        .expect("relayed lexical bitmap")
        .into_inner();
    assert_eq!(through.base_label, 0);
    assert_eq!(through.label_count, 2 * LEX_ROWS as u64);
    assert_ne!(through.stats_epoch, 0, "the epoch is a relay token");
    let mut want = vec![false; 2 * LEX_ROWS];
    for (child, addr) in l.iter().take(2).enumerate() {
        let direct = NodeServiceClient::connect(addr.clone())
            .await
            .unwrap()
            .resolve_lexical_bitmap(request.clone())
            .await
            .unwrap()
            .into_inner();
        assert_eq!(direct.base_label, (child * LEX_ROWS) as u64);
        for position in 0..direct.label_count as usize {
            want[child * LEX_ROWS + position] =
                direct.bits[position / 8] & (1 << (position % 8)) != 0;
        }
    }
    let got: Vec<bool> = (0..2 * LEX_ROWS)
        .map(|position| through.bits[position / 8] & (1 << (position % 8)) != 0)
        .collect();
    assert_eq!(
        got, want,
        "the relayed bitmap is the children's laid side by side"
    );
    assert!(got.iter().any(|&b| b), "the term matched");
    // The token translates on a rescore that echoes it.
    let stats = relay
        .term_stats(TermStatsRequest {
            terms: vec!["court".into()],
            fields: Vec::new(),
        })
        .await
        .unwrap()
        .into_inner();
    let members: Vec<u64> = got
        .iter()
        .enumerate()
        .filter_map(|(id, &member)| member.then_some(id as u64))
        .collect();
    let rescored = relay
        .bm25_rescore(pipestream_search::pb::Bm25RescoreRequest {
            terms: vec!["court".into()],
            global_doc_count: stats.doc_count,
            global_total_doc_length: stats.total_doc_length,
            global_doc_frequencies: stats.doc_frequencies.clone(),
            candidate_ids: members.clone(),
            k1: 1.2,
            b: 0.75,
            expected_stats_epoch: through.stats_epoch,
            score_stages: Vec::new(),
        })
        .await
        .expect("the bitmap's token is a claim the relay translates")
        .into_inner();
    assert_eq!(rescored.hits.len(), members.len());
    // The filter and vector bitmaps share the layout.
    let filtered = relay
        .resolve_filter_bitmap(pipestream_search::pb::FilterBitmapRequest {
            geo_filters: Vec::new(),
            filter: pipestream_search::cel::compile_filter(r#"court == "scotus""#)
                .expect("compiles"),
        })
        .await
        .expect("relayed filter bitmap")
        .into_inner();
    assert_eq!(filtered.base_label, 0);
    assert_eq!(filtered.label_count, 2 * LEX_ROWS as u64);
    assert_eq!(filtered.filter_columns_known, vec![true]);
    let scotus = (0..2 * LEX_ROWS)
        .filter(|position| filtered.bits[position / 8] & (1 << (position % 8)) != 0)
        .count();
    assert_eq!(scotus, LEX_ROWS, "every other document is scotus");
    let vectors = relay
        .resolve_vector_bitmap(pipestream_search::pb::VectorBitmapRequest {})
        .await
        .expect("relayed vector bitmap")
        .into_inner();
    assert_eq!(vectors.label_count, 2 * LEX_ROWS as u64);
    assert_eq!(vectors.stats_epoch, 0);

    // A child past a gap: the layout the parent would derive is a lie,
    // so the routes refuse by name.
    let (gapped, _handle) = start_empty_node(NodeConfig {
        slot_offset: (LEX_ROWS + 3) as u64,
        analysis_addr: Some(leaves.analysis.clone()),
        ..Default::default()
    })
    .await;
    let relay_addr = relay_over(&[&l[0], &gapped]).await;
    let mut relay = NodeServiceClient::connect(relay_addr).await.unwrap();
    let err = relay
        .resolve_vector_bitmap(pipestream_search::pb::VectorBitmapRequest {})
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("gap"), "{}", err.message());
    let err = relay
        .vector_rescore(pipestream_search::pb::VectorRescoreRequest {
            vector: leaves.corpus[..DIM].to_vec(),
            candidate_ids: vec![0],
        })
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("gap"), "{}", err.message());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dictionaries_through_a_relay_are_the_union_of_the_children() {
    let leaves = lexical_leaves(&[true; LEX_LEAVES]).await;
    let l = &leaves.addrs;
    let relay_addr = relay_over(&[&l[0], &l[1]]).await;
    let mut relay = NodeServiceClient::connect(relay_addr).await.unwrap();
    let mut children = Vec::new();
    for addr in l.iter().take(2) {
        children.push(NodeServiceClient::connect(addr.clone()).await.unwrap());
    }
    let expand = |cap: u32| pipestream_search::pb::ExpandTermPrefixRequest {
        field: "body".into(),
        prefix: "o".into(),
        cap,
    };
    let mut union = std::collections::BTreeSet::new();
    for child in children.iter_mut() {
        let direct = child
            .expand_term_prefix(expand(64))
            .await
            .unwrap()
            .into_inner();
        assert!(direct.known);
        union.extend(direct.terms);
    }
    assert!(
        union.len() >= 2,
        "the prefix expands to several terms: {union:?}"
    );
    let through = relay
        .expand_term_prefix(expand(64))
        .await
        .unwrap()
        .into_inner();
    assert!(through.known);
    assert_eq!(through.count, union.len() as u64);
    assert_eq!(through.terms, union.iter().cloned().collect::<Vec<_>>());
    // Past the cap on the union: the count, no terms.
    let past = relay
        .expand_term_prefix(expand(union.len() as u32 - 1))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(past.count, union.len() as u64);
    assert!(past.terms.is_empty());

    let suggest = |max_scan: u64| pipestream_search::pb::SuggestTermsRequest {
        field: "body".into(),
        prefix: "o".into(),
        max_scan,
    };
    let mut dfs: std::collections::BTreeMap<String, u64> = std::collections::BTreeMap::new();
    for child in children.iter_mut() {
        let direct = child.suggest_terms(suggest(64)).await.unwrap().into_inner();
        for entry in direct.entries {
            *dfs.entry(entry.term).or_default() += entry.df;
        }
    }
    let through = relay.suggest_terms(suggest(64)).await.unwrap().into_inner();
    assert!(through.known);
    assert_eq!(through.count, dfs.len() as u64);
    let got: Vec<(String, u64)> = through
        .entries
        .iter()
        .map(|e| (e.term.clone(), e.df))
        .collect();
    let want: Vec<(String, u64)> = dfs.into_iter().collect();
    assert_eq!(got, want, "entries in byte order with the df summed");
    let err = relay
        .expand_term_prefix(pipestream_search::pb::ExpandTermPrefixRequest {
            field: "nope".into(),
            prefix: "o".into(),
            cap: 8,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!err.known, "a field no child indexes stays unknown");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn diagnostics_through_the_root_merge_the_relay_children() {
    use pipestream_search::pb::diagnostics_service_client::DiagnosticsServiceClient;
    let leaves = lexical_leaves(&[true; LEX_LEAVES]).await;
    let l = &leaves.addrs;
    let relay_addr = relay_over(&[&l[0], &l[1]]).await;
    let mut direct_rows = 0;
    let mut direct_segments = 0;
    for addr in l.iter().take(2) {
        let layout = DiagnosticsServiceClient::connect(addr.clone())
            .await
            .unwrap()
            .get_shard_diagnostics(pipestream_search::pb::ShardDiagnosticsRequest { shard: None })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(layout.shards.len(), 1);
        direct_rows += layout.shards[0].rows;
        direct_segments += layout.shards[0].segments.len();
    }
    assert!(direct_rows > 0, "the leaves report their rows");
    let relayed = DiagnosticsServiceClient::connect(relay_addr.clone())
        .await
        .unwrap()
        .get_shard_diagnostics(pipestream_search::pb::ShardDiagnosticsRequest { shard: None })
        .await
        .expect("the relay serves the diagnostics service")
        .into_inner();
    assert_eq!(relayed.shards.len(), 1, "one shard: the relay's");
    let merged = &relayed.shards[0];
    assert!(
        merged.layout.starts_with("relay over 2 children"),
        "{}",
        merged.layout
    );
    assert_eq!(merged.rows, direct_rows);
    assert_eq!(merged.segments.len(), direct_segments);
    // Through the root: the relay's shard is the merged view, the leaf
    // beside it is itself.
    let root = CoordinatorServiceImpl::new(vec![relay_addr, l[2].clone()]);
    let layouts = root.shard_diagnostics(None).await;
    assert_eq!(layouts.len(), 2);
    assert_eq!(layouts[0].shard, 0);
    assert!(layouts[0].layout.starts_with("relay over 2 children"));
    assert_eq!(layouts[0].rows, direct_rows);
    assert_eq!(layouts[1].shard, 1);
    assert!(layouts[1].rows > 0 && !layouts[1].layout.contains("relay"));
}
