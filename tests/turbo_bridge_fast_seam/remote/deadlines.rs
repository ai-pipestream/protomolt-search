use super::*;

async fn reached(counter: &AtomicUsize, count: usize) {
    for _ in 0..100_000 {
        if counter.load(Ordering::SeqCst) >= count {
            return;
        }
        tokio::task::yield_now().await;
    }
    panic!("RPC did not reach controlled stage");
}

fn options() -> EmbedOptions {
    EmbedOptions {
        call: RemoteCallOptions {
            timeout: Some(Duration::from_secs(10)),
            bearer_token: Some("deadline-token".into()),
            interactive: true,
            ..Default::default()
        },
        ..Default::default()
    }
}

fn wire_timeout(value: &str) -> Duration {
    let (digits, unit) = value.split_at(value.len() - 1);
    let value: u64 = digits.parse().unwrap();
    match unit {
        "H" => Duration::from_secs(value * 3600),
        "M" => Duration::from_secs(value * 60),
        "S" => Duration::from_secs(value),
        "m" => Duration::from_millis(value),
        "u" => Duration::from_micros(value),
        "n" => Duration::from_nanos(value),
        _ => panic!("bad grpc-timeout"),
    }
}

async fn cumulative_budget(mode: u8) {
    let mock = Arc::new(Mock::default());
    mock.hold_metadata.store(true, Ordering::SeqCst);
    mock.hold_inference.store(true, Ordering::SeqCst);
    let (addr, server) = server(mock.clone()).await;
    let addr = if mode == 1 {
        format!("{addr}&sha256={}&dim=2&revision=0", "a".repeat(64))
    } else {
        addr
    };
    let mut call_options = options();
    if mode == 2 {
        call_options.call.expected_handshake = Some(turbo_client::IndexGenerationHandshake {
            analysis: None,
            bundle: Some(
                turbo_client::BundleScoringSpec {
                    model_id: "fast-seam".into(),
                    revision: "0".into(),
                    manifest_sha256: "a".repeat(64),
                    task: "embed".into(),
                    dim: 2,
                    pooling: "mean".into(),
                    normalize: "l2".into(),
                }
                .fingerprint(),
            ),
        });
    }
    let caller = tokio::spawn(async move {
        RemoteBatchEmbedder::new(addr)
            .embed_batch(&["query"], &call_options)
            .await
    });
    reached(&mock.metadata, 1).await;
    tokio::time::advance(Duration::from_secs(6)).await;
    mock.metadata_gate.notify_one();
    reached(&mock.inference, 1).await;
    {
        let metadata = mock.infer_meta.lock().unwrap();
        let metadata = metadata.as_ref().unwrap();
        assert_eq!(
            metadata.get("authorization").unwrap(),
            "Bearer deadline-token"
        );
        assert_eq!(metadata.get("x-turbo-priority").unwrap(), "interactive");
        let remaining = wire_timeout(metadata.get("grpc-timeout").unwrap().to_str().unwrap());
        assert!(remaining <= Duration::from_secs(4), "{remaining:?}");
        assert!(remaining > Duration::from_secs(3), "{remaining:?}");
    }
    tokio::time::advance(Duration::from_secs(4)).await;
    assert_eq!(
        caller.await.unwrap().unwrap_err().code(),
        tonic::Code::DeadlineExceeded
    );
    server.abort();
}

#[tokio::test(start_paused = true)]
async fn tofu_handshake_and_inference_share_budget_and_options() {
    let _guard = RPC_TEST.lock().await;
    cumulative_budget(0).await;
}

#[tokio::test(start_paused = true)]
async fn explicit_handshake_and_inference_share_budget_and_options() {
    let _guard = RPC_TEST.lock().await;
    cumulative_budget(1).await;
}

#[tokio::test(start_paused = true)]
async fn no_deadline_waits_for_handshake_and_inference() {
    let _guard = RPC_TEST.lock().await;
    let mock = Arc::new(Mock::default());
    mock.hold_metadata.store(true, Ordering::SeqCst);
    mock.hold_inference.store(true, Ordering::SeqCst);
    let (addr, server) = server(mock.clone()).await;
    let caller = tokio::spawn(async move {
        embed_text_with_options(&addr, "query", &RemoteCallOptions::default()).await
    });
    reached(&mock.metadata, 1).await;
    tokio::time::advance(Duration::from_secs(60)).await;
    assert!(!caller.is_finished());
    mock.metadata_gate.notify_one();
    reached(&mock.inference, 1).await;
    tokio::time::advance(Duration::from_secs(60)).await;
    assert!(!caller.is_finished());
    mock.inference_gate.notify_one();
    assert_eq!(caller.await.unwrap().unwrap(), vec![0.6, 0.8]);
    server.abort();
}

#[tokio::test(start_paused = true)]
async fn cold_handshake_expiration_never_dispatches_inference_and_can_recover() {
    let _guard = RPC_TEST.lock().await;
    let mock = Arc::new(Mock::default());
    mock.hold_metadata.store(true, Ordering::SeqCst);
    let (addr, server) = server(mock.clone()).await;
    let endpoint = addr.clone();
    let caller =
        tokio::spawn(
            async move { embed_texts_with_options(&endpoint, &["query"], &options()).await },
        );
    reached(&mock.metadata, 1).await;
    tokio::time::advance(Duration::from_secs(10)).await;
    assert_eq!(
        caller.await.unwrap().unwrap_err().code(),
        tonic::Code::DeadlineExceeded
    );
    assert_eq!(mock.inference.load(Ordering::SeqCst), 0);
    mock.hold_metadata.store(false, Ordering::SeqCst);
    let caller = tokio::spawn(async move {
        embed_texts_with_options(&addr, &["query"], &EmbedOptions::default()).await
    });
    reached(&mock.inference, 1).await;
    assert_eq!(caller.await.unwrap().unwrap().data(), &[0.6, 0.8]);
    server.abort();
}

#[tokio::test(start_paused = true)]
async fn options_handshake_and_inference_share_budget() {
    let _guard = RPC_TEST.lock().await;
    cumulative_budget(2).await;
}

#[tokio::test(start_paused = true)]
async fn verification_lock_wait_reduces_outgoing_handshake_budget() {
    let _guard = RPC_TEST.lock().await;
    let mock = Arc::new(Mock::default());
    mock.hold_metadata.store(true, Ordering::SeqCst);
    let (addr, server) = server(mock.clone()).await;
    let owner_addr = addr.clone();
    let owner = tokio::spawn(async move {
        embed_texts_with_options(&owner_addr, &["query"], &EmbedOptions::default()).await
    });
    reached(&mock.metadata, 1).await;
    let waiter =
        tokio::spawn(async move { embed_texts_with_options(&addr, &["query"], &options()).await });
    tokio::task::yield_now().await;
    tokio::time::advance(Duration::from_secs(6)).await;
    mock.hold_metadata.store(false, Ordering::SeqCst);
    owner.abort();
    assert!(owner.await.unwrap_err().is_cancelled());
    reached(&mock.inference, 1).await;
    {
        let calls = mock.metadata_calls.lock().unwrap();
        assert_eq!(calls.len(), 2);
        let timeout = wire_timeout(calls[1].get("grpc-timeout").unwrap().to_str().unwrap());
        assert!(timeout <= Duration::from_secs(4), "{timeout:?}");
        assert!(timeout > Duration::from_secs(3), "{timeout:?}");
    }
    assert_eq!(waiter.await.unwrap().unwrap().data(), &[0.6, 0.8]);
    server.abort();
}

#[tokio::test(start_paused = true)]
async fn timed_out_explicit_probe_invalidates_cached_tofu_verification() {
    let _guard = RPC_TEST.lock().await;
    let mock = Arc::new(Mock::default());
    let (addr, server) = server(mock.clone()).await;
    let endpoint = addr.clone();
    let prime = tokio::spawn(async move {
        embed_text_with_options(&endpoint, "query", &RemoteCallOptions::default()).await
    });
    reached(&mock.inference, 1).await;
    prime.await.unwrap().unwrap();
    mock.revision.store(1, Ordering::SeqCst);
    mock.hold_metadata.store(true, Ordering::SeqCst);
    let explicit = format!("{addr}&sha256={}&dim=2&revision=1", "a".repeat(64));
    let probe =
        tokio::spawn(
            async move { embed_texts_with_options(&explicit, &["query"], &options()).await },
        );
    reached(&mock.metadata, 2).await;
    tokio::time::advance(Duration::from_secs(10)).await;
    assert_eq!(
        probe.await.unwrap().unwrap_err().code(),
        tonic::Code::DeadlineExceeded
    );
    mock.hold_metadata.store(false, Ordering::SeqCst);
    let retry = tokio::spawn(async move {
        embed_text_with_options(&addr, "query", &RemoteCallOptions::default()).await
    });
    reached(&mock.metadata, 3).await;
    assert_eq!(
        retry.await.unwrap().unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(mock.inference.load(Ordering::SeqCst), 1);
    server.abort();
}

#[tokio::test]
async fn repeated_explicit_handshakes_reuse_the_same_channel() {
    let _guard = RPC_TEST.lock().await;
    let mock = Arc::new(Mock::default());
    let (addr, server) = server(mock.clone()).await;
    let endpoint = format!("{addr}&sha256={}&dim=2&revision=0", "a".repeat(64));
    let options = options();
    pipestream_search::analyzer::evict_unhealthy_turbo_channel(addr.split_once('?').unwrap().0);
    let remote = pipestream_search::analyzer::remote_client_for(
        addr.split_once('?').unwrap().0,
        &options.call,
    )
    .unwrap();
    embed_texts_with_options(&endpoint, &["query"], &options)
        .await
        .unwrap();
    let count = remote.pool().active_channel_count();
    for _ in 0..8 {
        embed_texts_with_options(&endpoint, &["query"], &options)
            .await
            .unwrap();
        assert_eq!(remote.pool().active_channel_count(), count);
    }
    assert_eq!(mock.metadata.load(Ordering::SeqCst), 9);
    assert_eq!(mock.inference.load(Ordering::SeqCst), 9);
    server.abort();
}
