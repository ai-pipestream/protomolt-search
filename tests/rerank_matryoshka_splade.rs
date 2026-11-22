#![cfg(feature = "net")]

//! Regression coverage for per-request dense Stage-2 `query_text`, missing
//! shortlist document refusal, lexical owner-routed reranking, multi-node
//! Matryoshka rescoring, and concurrent read-version-pinned SPLADE fan-out.

mod common;
#[path = "rerank_matryoshka_splade/support.rs"]
mod support;

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use common::unit_vectors;
use pipestream_search::analyzer::body_spec;
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::harness::mock_analysis::start_mock_analysis_recording_rerank;
use pipestream_search::link::NodeLink;
use pipestream_search::node::{NodeConfig, NodeServiceImpl};
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    search_query, selection_query, AddDocumentsRequest, DeleteDocumentsRequest, LexicalQuery,
    QueryRequest, SearchQuery, SelectionQuery,
};
use pipestream_search::stats_identity::StatsClaim;
use pipestream_search::turbo_bridge::rerank::{rerank_and_highlight_shortlist, RerankCandidate};
use support::{
    build_splade_node, dense_req, hit_bits, matryoshka_shard, remote_rerank_spec, seed_rerank_node,
    ProxyNode, DIM,
};
use tokio::sync::{mpsc, Barrier};
use tonic::{Code, Request};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dense_query_skips_reranker_without_query_text_and_never_uses_request_id() {
    let (analysis_addr, analysis_srv, rerank_log) = start_mock_analysis_recording_rerank().await;
    let vecs = unit_vectors(2, DIM, 0x2470_0001);
    let docs = [
        "Boiler valve pressure inspection log for regional plant operations.",
        "Preamble sentence. The appellate court affirmed constitutional jurisdiction.",
    ];
    let node = seed_rerank_node(&analysis_addr, &docs, &vecs).await;
    let coord = CoordinatorServiceImpl::with_local_nodes(vec![node])
        .with_bm25(Some(analysis_addr.clone()), Default::default())
        .with_rerank_spec(remote_rerank_spec(&analysis_addr));
    let q_vec = &vecs[..DIM];

    // Without `query_text`, varying `request_id` (even when `request_id` matches Doc 1's words!)
    // never invokes the reranker and yields bit-identical hits and scores.
    let resp_a = SearchService::query(
        &coord,
        Request::new(dense_req(
            "appellate constitutional jurisdiction",
            q_vec,
            "",
            false,
        )),
    )
    .await
    .unwrap()
    .into_inner();
    let resp_b = SearchService::query(
        &coord,
        Request::new(dense_req("boiler valve pressure", q_vec, "", false)),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(resp_a.executed, "search");
    assert_eq!(resp_b.executed, "search");
    assert_eq!(resp_a.hits[0].doc_id, 0);
    assert_eq!(hit_bits(&resp_a.hits), hit_bits(&resp_b.hits));
    assert!(rerank_log.lock().unwrap().is_empty());

    // Explicitly demanding `highlight` on a pure dense selection without `query_text` refuses.
    let missing_qtext_hl = SearchService::query(
        &coord,
        Request::new(dense_req("req-hl", q_vec, "   ", true)),
    )
    .await
    .unwrap_err();
    assert_eq!(missing_qtext_hl.code(), Code::InvalidArgument);

    // Supplying per-request `query_text` passes `query_text` (never `request_id`) to the reranker.
    let q_text = "appellate constitutional jurisdiction";
    let reranked_1 = SearchService::query(
        &coord,
        Request::new(dense_req("req-alpha", q_vec, q_text, true)),
    )
    .await
    .unwrap()
    .into_inner();
    let reranked_2 = SearchService::query(
        &coord,
        Request::new(dense_req("req-omega", q_vec, q_text, true)),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(reranked_1.executed, "search:turbo_rerank");
    assert_eq!(reranked_1.hits[0].doc_id, 1);
    assert!(reranked_1.hits[0].snippets[0].start >= 300);
    assert_eq!(hit_bits(&reranked_1.hits), hit_bits(&reranked_2.hits));
    let logged = rerank_log.lock().unwrap().clone();
    assert_eq!(logged.len(), 2);
    assert!(logged.iter().all(|(q, _)| q == q_text));
    analysis_srv.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn missing_shortlist_document_refuses_before_any_empty_string_reaches_backend() {
    let (analysis_addr, analysis_srv, rerank_log) = start_mock_analysis_recording_rerank().await;
    let vecs = unit_vectors(2, DIM, 0x2470_0002);
    // Only Doc 0 has stored text; Doc 1 has a vector row with no stored document text.
    let node = seed_rerank_node(&analysis_addr, &["Stored text for doc zero only."], &vecs).await;
    let spec = remote_rerank_spec(&analysis_addr);
    let coord = CoordinatorServiceImpl::with_local_nodes(vec![node])
        .with_bm25(Some(analysis_addr.clone()), Default::default())
        .with_rerank_spec(spec.clone());

    let err = SearchService::query(
        &coord,
        Request::new(dense_req("req-missing", &vecs[..DIM], "stored text", false)),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
    assert!(err.message().contains("1"));

    let direct_err = rerank_and_highlight_shortlist(
        &spec,
        "stored text",
        &[RerankCandidate {
            doc_id: 77,
            prior_score: 1.0,
            text: String::new(),
            sentence_spans: Vec::new(),
            lineage_span_start: 0,
        }],
        0,
    )
    .await
    .unwrap_err();
    assert_eq!(direct_err.code(), Code::FailedPrecondition);
    assert!(rerank_log.lock().unwrap().is_empty());
    analysis_srv.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lexical_rerank_routes_candidate_reads_only_to_owning_shards() {
    let (analysis_addr, analysis_srv, rerank_log) = start_mock_analysis_recording_rerank().await;
    let mut handles = Vec::new();
    let mut addrs = Vec::new();
    let mut get_doc_counters = Vec::new();
    for (shard, text) in [
        (0u64, "Constitutional jurisdiction over maritime appeals."),
        (100u64, "Boiler valve maintenance inspection log."),
    ] {
        let node = NodeServiceImpl::new(
            None,
            NodeConfig {
                slot_offset: shard,
                analysis_addr: Some(analysis_addr.clone()),
                sentence_fields: vec!["body".into()],
                ..Default::default()
            },
        );
        NodeLink::local(Arc::new(node.clone()))
            .add_documents(tokio_stream::iter([AddDocumentsRequest {
                text: text.into(),
                analysis: Some(body_spec()),
                ..Default::default()
            }]))
            .await
            .unwrap();
        let proxy = ProxyNode::new(node);
        get_doc_counters.push(Arc::clone(&proxy.get_docs_calls));
        let (addr, h) = proxy.serve().await;
        addrs.push(addr);
        handles.push(h);
    }
    let coord = CoordinatorServiceImpl::new(addrs)
        .with_bm25(Some(analysis_addr.clone()), Default::default())
        .with_rerank_spec(remote_rerank_spec(&analysis_addr));
    let resp = SearchService::query(
        &coord,
        Request::new(QueryRequest {
            k: 4,
            selection: Some(SelectionQuery {
                node: Some(selection_query::Node::Search(SearchQuery {
                    id: "lex".into(),
                    query: Some(search_query::Query::Lexical(LexicalQuery {
                        text: "maritime jurisdiction".into(),
                        analysis: Some(body_spec()),
                        ..Default::default()
                    })),
                })),
            }),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(resp.executed, "bm25_search:turbo_rerank");
    assert_eq!(resp.hits.len(), 1);
    assert_eq!(resp.hits[0].doc_id, 0);
    assert_eq!(get_doc_counters[0].load(Ordering::Acquire), 1);
    assert_eq!(get_doc_counters[1].load(Ordering::Acquire), 0);
    assert_eq!(rerank_log.lock().unwrap().len(), 1);
    for h in handles {
        h.abort();
    }
    analysis_srv.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_and_relay_matryoshka_shortlist_match_local_bit_for_bit_and_pin_versions() {
    let mut shard0_vecs = vec![0.0_f32; 16];
    shard0_vecs[0] = 1.0; // doc 0: wins 8-dim prefix (1.0), loses 16-dim (0.5)
    let mut shard1_vecs = vec![0.0_f32; 32];
    shard1_vecs[0] = 0.6;
    shard1_vecs[8] = 0.8; // doc 1: wins 16-dim (0.6*0.5 + 0.8*0.8660254 = 0.9928)
    shard1_vecs[16] = 1.0; // doc 2: tombstoned
    let shard0 = matryoshka_shard(0, shard0_vecs, None).await;
    let shard1 = matryoshka_shard(1, shard1_vecs, Some(1)).await;

    let (addr0, srv0) = pipestream_search::harness::serve_node((*shard0).clone()).await;
    let (addr1, srv1) = pipestream_search::harness::serve_node((*shard1).clone()).await;
    let (relay_addr, _relay, relay_srv) =
        pipestream_search::harness::start_relay(vec![addr0.clone(), addr1.clone()]).await;

    let local_coord =
        CoordinatorServiceImpl::with_local_nodes(vec![Arc::clone(&shard0), Arc::clone(&shard1)]);
    let remote_coord = CoordinatorServiceImpl::new(vec![addr0, addr1]);
    let relay_coord = CoordinatorServiceImpl::new(vec![relay_addr]);

    let mut query = vec![0.0_f32; 16];
    query[0] = 0.5;
    query[8] = 0.8660254;
    let shortlist = [(0, 0.95), (1, 0.40), (2, 0.99)];

    let local_out = local_coord
        .rescore_matryoshka_shortlist(&shortlist, &query, 16)
        .await
        .unwrap();
    let remote_out = remote_coord
        .rescore_matryoshka_shortlist(&shortlist, &query, 16)
        .await
        .unwrap();
    let relay_out = relay_coord
        .rescore_matryoshka_shortlist(&shortlist, &query, 16)
        .await
        .unwrap();

    assert_eq!(local_out.len(), 2, "tombstoned doc 2 must be omitted");
    assert_eq!(
        local_out[0].0, 1,
        "remote shard 1 doc 1 must win 16-dim rescore"
    );
    let to_bits = |v: &[(u64, f32)]| {
        v.iter()
            .map(|&(id, s)| (id, s.to_bits()))
            .collect::<Vec<_>>()
    };
    assert_eq!(to_bits(&local_out), to_bits(&remote_out));
    assert_eq!(to_bits(&local_out), to_bits(&relay_out));

    let pinned = remote_coord.with_pinned_read_versions().await.unwrap();
    assert_eq!(
        to_bits(
            &pinned
                .rescore_matryoshka_shortlist(&shortlist, &query, 16)
                .await
                .unwrap()
        ),
        to_bits(&local_out)
    );
    NodeLink::local(shard1)
        .delete_documents(DeleteDocumentsRequest {
            doc_ids: vec![1],
            ..Default::default()
        })
        .await
        .unwrap();
    let stale = pinned
        .rescore_matryoshka_shortlist(&shortlist, &query, 16)
        .await
        .unwrap_err();
    assert_eq!(stale.code(), Code::FailedPrecondition);

    relay_srv.abort();
    srv0.abort();
    srv1.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn concurrent_splade_enforces_pinned_read_versions_and_aborts_on_drop_without_sleep() {
    let (node0, _dir0) = build_splade_node(0, 3, 0.5);
    let (node1, _dir1) = build_splade_node(100, 8, 0.5);

    // 1. Barrier of size 2 proves both shards run concurrently inside `JoinSet`.
    let barrier = Arc::new(Barrier::new(2));
    let mut proxy0 = ProxyNode::new(node0.clone());
    proxy0.splade_barrier = Some(Arc::clone(&barrier));
    let mut proxy1 = ProxyNode::new(node1.clone());
    proxy1.splade_barrier = Some(Arc::clone(&barrier));
    let (addr0, srv0) = proxy0.serve().await;
    let (addr1, srv1) = proxy1.serve().await;

    let coord = CoordinatorServiceImpl::new(vec![addr0, addr1]);
    let pinned = coord.with_pinned_read_versions().await.unwrap();
    let q_weights = [("lithium".to_string(), 1.0_f32)];
    let hits = tokio::time::timeout(
        Duration::from_secs(5),
        pinned.score_splade("body", &q_weights, 0.0, 2, ""),
    )
    .await
    .expect("concurrent SPLADE barrier deadlocked")
    .unwrap();
    assert_eq!(
        hits.iter().map(|&(id, _)| id).collect::<Vec<_>>(),
        vec![101, 100]
    );

    // 2. Advancing a shard's epoch or supplying a mismatched incarnation refuses with FailedPrecondition.
    NodeLink::local(Arc::new(node1))
        .delete_documents(DeleteDocumentsRequest {
            doc_ids: vec![101],
            ..Default::default()
        })
        .await
        .unwrap();
    let epoch_err = pinned
        .score_splade("body", &q_weights, 0.0, 2, "")
        .await
        .unwrap_err();
    assert_eq!(epoch_err.code(), Code::FailedPrecondition);

    let wrong_inc = coord
        .with_read_claims(vec![
            StatsClaim::required(1, &[0xAA; 32]).unwrap(),
            StatsClaim::required(1, &[0xBB; 32]).unwrap(),
        ])
        .unwrap();
    assert_eq!(
        wrong_inc
            .score_splade("body", &q_weights, 0.0, 2, "")
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    srv0.abort();
    srv1.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn dropping_score_splade_future_aborts_inflight_shard_rpcs_without_sleep() {
    let (hold_tx, mut hold_rx) = mpsc::unbounded_channel();
    let (hold_node0, _hd0) = build_splade_node(0, 3, 0.5);
    let (hold_node1, _hd1) = build_splade_node(100, 8, 0.5);
    let mut hproxy0 = ProxyNode::new(hold_node0);
    hproxy0.splade_hold = Some(hold_tx.clone());
    let mut hproxy1 = ProxyNode::new(hold_node1);
    hproxy1.splade_hold = Some(hold_tx);
    let (haddr0, hsrv0) = hproxy0.serve().await;
    let (haddr1, hsrv1) = hproxy1.serve().await;
    let hcoord = CoordinatorServiceImpl::new(vec![haddr0, haddr1]);

    let inflight = tokio::spawn(async move {
        hcoord
            .score_splade("body", &[("lithium".into(), 1.0)], 0.0, 2, "")
            .await
    });
    let drop_rx0 = tokio::time::timeout(Duration::from_secs(5), hold_rx.recv())
        .await
        .unwrap()
        .unwrap();
    let drop_rx1 = tokio::time::timeout(Duration::from_secs(5), hold_rx.recv())
        .await
        .unwrap()
        .unwrap();
    inflight.abort();
    let _ = inflight.await;
    tokio::time::timeout(Duration::from_secs(5), drop_rx0)
        .await
        .expect("shard 0 handler was not cancelled on drop")
        .unwrap_err();
    tokio::time::timeout(Duration::from_secs(5), drop_rx1)
        .await
        .expect("shard 1 handler was not cancelled on drop")
        .unwrap_err();
    hsrv0.abort();
    hsrv1.abort();
}
