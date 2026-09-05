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
use common::{monolithic_topk, start_empty_node, start_node, unit_vectors, BIT_WIDTH, DIM};
use pipestream_search::coordinator::{CoordinatorServiceImpl, TopologyRoute};
use pipestream_search::harness::{start_relay, start_relay_over};
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::{
    stream_search_request, stream_search_response, AddDocumentsRequest, Bm25QueryRequest,
    FieldTerms, HealthRequest, ScoredHit, StartStreamSearch, StopStreamSearch, StreamSearchRequest,
    TermStatsRequest, TermStatsResponse,
};
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
        .bm25_query(Bm25QueryRequest::default())
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::Unimplemented);
    assert!(
        err.message().contains("relay") && err.message().contains("Bm25Query"),
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
