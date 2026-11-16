//! KServe call policy, deadlines, pool eviction and the vocabulary
//! feedback loop.

use super::vocab_mock::*;
use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn kserve_security_priority_deadlines_pool_eviction_and_vocabulary_feedback_loop() {
    let dir = tempdir("vocab_feedback");
    let recorded = Arc::new(Mutex::new(RecordedCallMeta::default()));
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://{}", listener.local_addr().unwrap());
    let recorded_srv = Arc::clone(&recorded);
    let srv_handle = tokio::spawn(async move {
        Server::builder()
            .add_service(TurboVocabularyServiceServer::new(LoopbackVocabServer {
                expected_bearer: "secret-kserve-token".into(),
                recorded: recorded_srv,
            }))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
    });

    let future_deadline_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
        + 15_000_000;

    // 1. Verify `MtlsClientMaterial` + `KserveCallPolicy` construction and unauthenticated refusal!
    let tls_policy = KserveCallPolicy {
        bearer_token: Some("secret-kserve-token".into()),
        mtls: Some(MtlsClientMaterial {
            server_ca_pem: b"-----BEGIN CERTIFICATE-----\n-----END CERTIFICATE-----\n".to_vec(),
            client_identity_pem: Some((b"cert".to_vec(), b"key".to_vec())),
            domain_name: Some("kserve.internal".into()),
        }),
    };
    let tls_ep = tls_policy.endpoint_config(
        "https://127.0.0.1:8443",
        RemotePriority::Interactive,
        Some(Duration::from_secs(3)),
    );
    assert_eq!(tls_ep.default_priority, RemotePriority::Interactive);
    assert_eq!(tls_ep.bearer_token.as_deref(), Some("secret-kserve-token"));

    let unauth_client = RemoteClient::new(RemoteEndpointConfig::new(&addr));
    let unauth_err = export_corpus_statistics_to_kserve(
        &unauth_client,
        "en",
        "col-unauth",
        10,
        &[("court".into(), 5)],
        future_deadline_us,
    )
    .await
    .unwrap_err();
    assert_eq!(unauth_err.code(), Code::Unauthenticated);

    // 2. Verify expired `finish_deadline_us` (`1` us since epoch) aborts BEFORE network dispatch
    //    with `Code::DeadlineExceeded` across corpus export, Voronoi training, and Stage-2 rerank!
    let auth_policy = KserveCallPolicy {
        bearer_token: Some("secret-kserve-token".into()),
        mtls: None,
    };
    let auth_client = RemoteClient::new(auth_policy.endpoint_config(
        &addr,
        RemotePriority::Interactive,
        Some(Duration::from_secs(5)),
    ));

    assert_eq!(
        export_corpus_statistics_to_kserve(
            &auth_client,
            "en",
            "expired",
            10,
            &[("court".into(), 5)],
            1,
        )
        .await
        .unwrap_err()
        .code(),
        Code::DeadlineExceeded
    );

    let voronoi_spec = VoronoiTrainingSpec {
        model_id: "bge-small-en-v1.5".into(),
        model_revision: "rev1".into(),
        model_sha256: "deadbeef".into(),
        dim: 16,
        num_centroids: 4,
        max_iters: 6,
        seed: 42,
        tag_set_name: "shard-voronoi".into(),
        temperature: 1.0,
    };
    let sample_vecs = unit_vectors(64, 16, 0x2346_BEEF);
    assert_eq!(
        train_voronoi_centroids_on_kserve(&auth_client, &voronoi_spec, &sample_vecs, 1)
            .await
            .unwrap_err()
            .code(),
        Code::DeadlineExceeded
    );

    let rerank_expired = rerank_and_highlight_shortlist(
        &RerankSpec::new(RerankBackend::DeterministicCalibrated {
            platt_slope: 1.5,
            platt_intercept: 0.0,
        }),
        "query",
        &[RerankCandidate {
            doc_id: 1,
            prior_score: 1.0,
            text: "sample passage".into(),
            sentence_spans: Vec::new(),
            lineage_span_start: 0,
        }],
        1,
    )
    .await
    .unwrap_err();
    assert_eq!(rerank_expired.code(), Code::DeadlineExceeded);

    // 3. Feed documents into `VocabularyListener`, export live heavy hitters via
    //    `export_corpus_statistics_to_kserve`, and verify `TCIDF001` round-trip + Bulk priority!
    let vocab_listener = VocabularyListener::create(&dir, 1_000, 32).unwrap();
    for i in 0..20 {
        if i == 0 {
            vocab_listener.feed(
                [("common_term", 1), ("rare_jurisdiction", 1)].into_iter(),
                ["common_term", "rare_jurisdiction"].into_iter(),
            );
        } else {
            vocab_listener.feed(
                [("common_term", 1)].into_iter(),
                ["common_term"].into_iter(),
            );
        }
    }
    let terms_stats = vocab_listener
        .live_stats()
        .into_iter()
        .find(|s| s.channel == VocabChannel::Terms as i32)
        .unwrap();
    assert_eq!(terms_stats.documents, 20);
    let term_dfs: Vec<(String, u64)> = terms_stats
        .heavy_hitters
        .iter()
        .map(|h| (h.term.clone(), h.count.min(terms_stats.documents)))
        .collect();

    let idf_desc = export_corpus_statistics_to_kserve(
        &auth_client,
        "en",
        "legal-corpus-v1",
        terms_stats.documents,
        &term_dfs,
        future_deadline_us,
    )
    .await
    .unwrap();
    assert_eq!(idf_desc.doc_count, 20);
    assert_eq!(idf_desc.term_count, 2);

    // Decode the `TCIDF001` bytes compiled by the server and verify smooth BM25 IDF monotonicity!
    let idf_bytes = recorded.lock().unwrap().compiled_idf_bytes.clone();
    assert_eq!(&idf_bytes[..8], b"TCIDF001");
    let decoded_idf = VocabularyCorpusIdf::from_bytes(&idf_bytes).unwrap();
    assert!(decoded_idf.idf("rare_jurisdiction") > decoded_idf.idf("common_term"));
    assert!(decoded_idf.idf("common_term") > 0.0);

    // 4. Stream sample embeddings via `train_voronoi_centroids_on_kserve` (`TCENT001`)!
    let voronoi_desc = train_voronoi_centroids_on_kserve(
        &auth_client,
        &voronoi_spec,
        &sample_vecs,
        future_deadline_us,
    )
    .await
    .unwrap();
    assert_eq!(voronoi_desc.dim, 16);
    assert_eq!(voronoi_desc.num_centroids, 4);
    assert_eq!(voronoi_desc.sample_count, 64);
    assert_eq!(voronoi_desc.cluster_counts.iter().sum::<u64>(), 64);
    assert!(voronoi_desc.mean_cosine > 0.15);

    // Verify both background feedback RPCs overrode the client's `Interactive` default to `Bulk`
    // and propagated `grpc-timeout`!
    {
        let meta = recorded.lock().unwrap();
        assert_eq!(
            meta.priorities,
            vec!["bulk".to_string(), "bulk".to_string()]
        );
        assert_eq!(meta.has_timeout, vec![true, true]);
    }

    // 5. Verify `RemoteChannelPool` runtime-scoped eviction: opening a channel on a dedicated
    //    Tokio runtime caches 1 entry, and dropping that runtime evicts it automatically!
    let shared_pool = RemoteChannelPool::new();
    assert_eq!(shared_pool.active_channel_count(), 0);
    let pool_clone = shared_pool.clone();
    let addr_clone = addr.clone();
    std::thread::spawn(move || {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        rt.block_on(async {
            let _ch = pool_clone
                .channel(&RemoteEndpointConfig::new(addr_clone))
                .unwrap();
            assert_eq!(pool_clone.active_channel_count(), 1);
        });
        drop(rt);
    })
    .join()
    .unwrap();
    assert_eq!(shared_pool.active_channel_count(), 0);

    srv_handle.abort();
}
