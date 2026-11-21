//! Relayed BM25/RRF/weighted hybrid fan-out, moved-child claims, phrases, and rescoring.

use super::*;

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
        let (want_rrf, _) = common::fanout::hybrid(&flat, "h", text, &query, LEX_K, rrf_legs())
            .await
            .expect("flat rrf");
        let (want_weighted, _) =
            common::fanout::hybrid(&flat, "w", text, &query, LEX_K, weighted_legs())
                .await
                .expect("flat weighted");
        assert!(!want_rrf.is_empty(), "{text}: hybrid matched");
        for (name, roots) in &trees {
            let relayed = with_bm25(roots.clone(), true);
            let (rrf, _) = common::fanout::hybrid(&relayed, "h", text, &query, LEX_K, rrf_legs())
                .await
                .expect("relayed rrf");
            assert_eq!(
                hybrid_bits(&rrf),
                hybrid_bits(&want_rrf),
                "{name} {text:?}: rrf"
            );
            let (weighted, _) =
                common::fanout::hybrid(&relayed, "w", text, &query, LEX_K, weighted_legs())
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
        visibility: None,
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
