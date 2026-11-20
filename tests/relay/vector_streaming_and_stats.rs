//! Multi-level vector StreamSearch bitwise parity and relayed TermStats epoch translation.

use super::*;

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
