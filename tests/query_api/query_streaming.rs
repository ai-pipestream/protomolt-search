//! Public QueryStream exactness, completion certificates, shard errors, and cancellation.

use super::*;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_query_stream_is_exact_certified_and_retry_stable() {
    let (coordinator, qvec, _handles) = start_fleet().await;

    for (name, request, provisional_phase) in [
        (
            "lexical",
            QueryRequest {
                request_id: "stream-lexical".into(),
                k: 5,
                selection: Some(lexical_leaf("lex", "zebra")),
                ..Default::default()
            },
            QueryStreamPhase::Lexical,
        ),
        (
            "dense",
            QueryRequest {
                request_id: "stream-dense".into(),
                k: 5,
                selection: Some(dense_leaf("vec", &qvec)),
                ..Default::default()
            },
            QueryStreamPhase::Dense,
        ),
    ] {
        let unary = query(&coordinator, request.clone()).await.unwrap();
        let events = streamed_query(&coordinator, Some(request.clone()), 0).await;
        let (revisions, completion) = stream_parts(&events);
        assert!(
            completion.completed,
            "{name}: terminal failure: {completion:?}"
        );
        assert_eq!(completion.error_code, 0);
        assert!(completion.error_message.is_empty());
        assert_eq!(
            completion.response.as_ref().unwrap().encode_to_vec(),
            unary.encode_to_vec(),
            "{name}: streamed terminal response differs from unary Query"
        );
        assert_eq!(
            completion.final_revision,
            revisions.last().unwrap().revision
        );
        assert!(!completion.scoring_fingerprints.is_empty());
        assert!(completion
            .scoring_fingerprints
            .iter()
            .all(|fingerprint| !fingerprint.is_empty()));

        assert_eq!(
            QueryStreamPhase::try_from(revisions[0].phase).unwrap(),
            QueryStreamPhase::Accepted
        );
        assert!(
            revisions.iter().any(|revision| {
                QueryStreamPhase::try_from(revision.phase).unwrap() == provisional_phase
            }),
            "{name}: collector never published a provisional revision"
        );
        let final_revision = revisions.last().unwrap();
        assert_eq!(
            QueryStreamPhase::try_from(final_revision.phase).unwrap(),
            QueryStreamPhase::Final
        );
        let final_rows: Vec<(u64, u32, u32)> = final_revision
            .hits
            .iter()
            .map(|hit| (hit.doc_id, hit.score.to_bits(), hit.rank))
            .collect();
        let expected_rows: Vec<(u64, u32, u32)> = unary
            .hits
            .iter()
            .map(|hit| (hit.doc_id, hit.score.to_bits(), hit.rank))
            .collect();
        assert_eq!(final_rows, expected_rows, "{name}: FINAL order");

        let mut fingerprints = std::collections::HashSet::new();
        for (position, revision) in revisions.iter().enumerate() {
            assert_eq!(revision.revision, position as u64 + 1, "{name}: gap");
            assert!(
                fingerprints.insert(revision.content_fingerprint.as_str()),
                "{name}: duplicate replacement snapshot"
            );
            for (rank, hit) in revision.hits.iter().enumerate() {
                assert_eq!(hit.rank, rank as u32 + 1, "{name}: rank column");
            }
        }

        // Timing may conflate intermediate replacement snapshots, but the
        // authoritative content and score-space identities are retry-stable.
        let retry = streamed_query(&coordinator, Some(request), 0).await;
        let (retry_revisions, retry_completion) = stream_parts(&retry);
        assert!(retry_completion.completed);
        assert_eq!(
            retry_revisions.last().unwrap().content_fingerprint,
            final_revision.content_fingerprint,
            "{name}: FINAL content fingerprint changed on retry"
        );
        assert_eq!(
            retry_completion.scoring_fingerprints, completion.scoring_fingerprints,
            "{name}: score-space identities changed on retry"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_query_stream_errors_are_terminal_not_partial_success() {
    let (coordinator, _qvec, _handles) = start_fleet().await;
    let events = streamed_query(&coordinator, None, 0).await;
    let (revisions, completion) = stream_parts(&events);
    assert_eq!(revisions.len(), 1);
    assert!(!completion.completed);
    assert!(completion.response.is_none());
    assert_eq!(completion.final_revision, 1);
    assert_eq!(completion.error_code, tonic::Code::InvalidArgument as u32);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_query_stream_missing_shard_cannot_certify_completion() {
    let ((_, qvec, _handles), mut addrs) = start_fleet_with_addrs().await;
    addrs.push("http://127.0.0.1:1".into());
    let coordinator = CoordinatorServiceImpl::new(addrs);
    let events = streamed_query(
        &coordinator,
        Some(QueryRequest {
            request_id: "stream-missing-shard".into(),
            k: 5,
            selection: Some(dense_leaf("vec", &qvec)),
            ..Default::default()
        }),
        1_000,
    )
    .await;
    let (_, completion) = stream_parts(&events);
    assert!(!completion.completed);
    assert!(completion.response.is_none());
    assert_ne!(completion.error_code, 0);
    assert!(!completion.error_message.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn public_query_stream_deadline_and_client_drop_cancel_analysis() {
    use common::mock::start_mock_analysis_delayed;
    use std::time::{Duration, Instant};

    let ((_, _qvec, _handles), addrs) = start_fleet_with_addrs().await;
    let (analysis, _slow_handle, probe) = start_mock_analysis_delayed(Duration::from_secs(5)).await;
    let coordinator =
        CoordinatorServiceImpl::new(addrs).with_bm25(Some(analysis), Default::default());
    let request = || QueryRequest {
        k: 5,
        selection: Some(lexical_leaf("lex", "zebra")),
        ..Default::default()
    };

    let events = streamed_query(&coordinator, Some(request()), 25).await;
    let (_, completion) = stream_parts(&events);
    assert!(!completion.completed);
    assert!(completion.response.is_none());
    assert_eq!(completion.error_code, tonic::Code::DeadlineExceeded as u32);

    let cancelled_after_deadline = Instant::now() + Duration::from_secs(2);
    while probe.cancelled() < 1 && Instant::now() < cancelled_after_deadline {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(probe.started(), 1, "deadline must start one analysis call");
    assert_eq!(
        probe.completed(),
        0,
        "deadline must not let analysis finish"
    );
    assert_eq!(probe.cancelled(), 1, "deadline must drop analysis");

    let mut stream = coordinator
        .query_stream(Request::new(QueryStreamRequest {
            collection: String::new(),
            query: Some(request()),
            timeout_ms: 0,
        }))
        .await
        .unwrap()
        .into_inner();
    let accepted = stream.next().await.unwrap().unwrap();
    assert!(matches!(
        accepted.payload,
        Some(query_stream_response::Payload::Revision(_))
    ));
    let started = Instant::now() + Duration::from_secs(2);
    while probe.started() < 2 && Instant::now() < started {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(probe.started(), 2, "cancel test never reached analysis");
    drop(stream);

    let cancelled_after_drop = Instant::now() + Duration::from_secs(2);
    while probe.cancelled() < 2 && Instant::now() < cancelled_after_drop {
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    assert_eq!(
        probe.completed(),
        0,
        "dropped client must not finish analysis"
    );
    assert_eq!(probe.cancelled(), 2, "dropped client must cancel execution");
}
