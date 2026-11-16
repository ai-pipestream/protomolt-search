use super::*;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

fn setup() -> (Arc<VectorReadBarrier>, Vec<VectorReadReceipt>) {
    let binding = crate::mapping::derive_plan(
        include_bytes!("../../tests/fixtures/vector-binding/descriptor.bin"),
        "vector_binding.Named",
    )
    .unwrap()
    .vector_binding
    .unwrap();
    let view = Some(DocumentVisibility {
        filter: crate::cel::compile_filter("audience == 'public'").unwrap(),
    });
    let scope = VisibilityScope::new(view.as_ref()).unwrap();
    let claims: Vec<_> = (1..=2)
        .map(|n| StatsClaim::required(4, &[n; 32]).unwrap())
        .collect();
    let receipts = claims
        .iter()
        .map(|claim| VectorReadReceipt {
            vector_binding: Some(binding.clone()),
            stats_epoch: claim.epoch,
            stats_incarnation: claim.incarnation(),
            visibility_fingerprint: scope.fingerprint().to_vec(),
            visibility_columns_known: vec![true],
        })
        .collect();
    (
        VectorReadBarrier::new("semantic".into(), view, claims).unwrap(),
        receipts,
    )
}
fn frame(receipt: VectorReadReceipt) -> StreamSearchResponse {
    StreamSearchResponse {
        payload: Some(stream_search_response::Payload::ReadReady(receipt)),
    }
}

#[tokio::test]
async fn fast_peer_stays_under_backpressure_until_the_whole_read_set_is_admitted() {
    let (barrier, receipts) = setup();
    let (tx, rx) = mpsc::channel(1);
    let consumer = barrier.clone();
    let task = tokio::spawn(async move {
        let mut stream = ReceiverStream::new(rx);
        consumer.admit(0, &mut stream).await?;
        next(&mut stream).await
    });
    tx.send(Ok(frame(receipts[0].clone()))).await.unwrap();
    let batch = StreamSearchResponse {
        payload: Some(stream_search_response::Payload::Batch(StreamSearchBatch {
            hits: vec![7; 12],
        })),
    };
    tx.send(Ok(batch.clone())).await.unwrap();
    // The receipt has been consumed (capacity is one), but no candidate
    // may pass while the other peer is absent. Bounded buffering stays real.
    assert!(tokio::time::timeout(
        std::time::Duration::from_millis(30),
        tx.send(Ok(batch.clone()))
    )
    .await
    .is_err());
    barrier
        .admit(1, &mut tokio_stream::iter([Ok(frame(receipts[1].clone()))]))
        .await
        .unwrap();
    let found = tokio::time::timeout(std::time::Duration::from_secs(2), task)
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(found, batch);
}

#[tokio::test]
async fn incompatible_or_missing_receipts_cancel_waiting_peers_before_data() {
    for case in 0..10 {
        let (barrier, mut receipts) = setup();
        let consumer = barrier.clone();
        let first = receipts[0].clone();
        let task = tokio::spawn(async move {
            consumer
                .admit(0, &mut tokio_stream::iter([Ok(frame(first))]))
                .await
        });
        let bad = &mut receipts[1];
        match case {
            0 => bad.vector_binding = None,
            1 => bad.vector_binding.as_mut().unwrap().field = "signal".into(),
            2 => bad.vector_binding.as_mut().unwrap().plan_fingerprint = "b".repeat(64),
            3 => bad.stats_epoch += 1,
            4 => bad.stats_incarnation.clear(),
            5 => bad.stats_incarnation = vec![1; 32],
            6 => bad.visibility_fingerprint.clear(),
            7 => bad.visibility_fingerprint = vec![9; 32],
            8 => bad.visibility_columns_known.clear(),
            _ => bad.vector_binding.as_mut().unwrap().format_version += 1,
        }
        assert!(barrier
            .admit(1, &mut tokio_stream::iter([Ok(frame(receipts[1].clone()))]))
            .await
            .is_err());
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(2), task)
                .await
                .unwrap()
                .unwrap()
                .is_err()
        );
    }
}

#[tokio::test]
async fn no_floor_candidate_or_completion_can_substitute_for_read_ready() {
    for frame in [
        SearchShardResponse { payload: None },
        SearchShardResponse {
            payload: Some(search_shard_response::Payload::FloorUpdate(FloorUpdate {
                floor: 10.0,
            })),
        },
        SearchShardResponse {
            payload: Some(search_shard_response::Payload::Done(
                SearchShardDone::default(),
            )),
        },
    ] {
        let (barrier, _) = setup();
        assert!(barrier
            .admit(0, &mut tokio_stream::iter([Ok(frame)]))
            .await
            .is_err());
        assert_eq!(barrier.receipts.lock().unwrap().seen, vec![false, false]);
    }
    for frame in [
        StreamSearchResponse { payload: None },
        StreamSearchResponse {
            payload: Some(stream_search_response::Payload::Batch(StreamSearchBatch {
                hits: vec![0; 12],
            })),
        },
        StreamSearchResponse {
            payload: Some(stream_search_response::Payload::Summary(
                StreamSearchSummary {
                    completed: true,
                    ..Default::default()
                },
            )),
        },
        StreamSearchResponse {
            payload: Some(stream_search_response::Payload::IdentityReady(
                StreamIdentityReady::default(),
            )),
        },
        StreamSearchResponse {
            payload: Some(stream_search_response::Payload::Identities(
                StreamIdentities::default(),
            )),
        },
    ] {
        let (barrier, _) = setup();
        assert!(barrier
            .admit(0, &mut tokio_stream::iter([Ok(frame)]))
            .await
            .is_err());
        assert_eq!(barrier.receipts.lock().unwrap().seen, vec![false, false]);
    }
    let (barrier, _) = setup();
    assert!(barrier
        .admit(
            0,
            &mut tokio_stream::empty::<Result<StreamSearchResponse, Status>>()
        )
        .await
        .is_err());
}

#[tokio::test]
async fn duplicates_unknown_authority_columns_and_transport_errors_stay_terminal() {
    let (barrier, receipts) = setup();
    barrier.accept(0, &receipts[0]).unwrap();
    assert!(barrier.accept(0, &receipts[0]).is_err());
    barrier.accept(1, &receipts[1]).unwrap();
    assert!(barrier.wait().await.is_err()); // a late valid peer cannot reopen failure
    assert!(
        next(&mut tokio_stream::iter([Ok(frame(receipts[0].clone()))]))
            .await
            .is_err()
    );
    assert!(next(&mut tokio_stream::iter([Ok(SearchShardResponse {
        payload: Some(search_shard_response::Payload::ReadReady(
            receipts[0].clone()
        )),
    })]))
    .await
    .is_err());
    let (barrier, mut receipts) = setup();
    for receipt in &mut receipts {
        receipt.visibility_columns_known = vec![false];
    }
    barrier.accept(0, &receipts[0]).unwrap();
    assert!(barrier.accept(1, &receipts[1]).is_err());
    assert!(barrier.wait().await.is_err());
    let (barrier, receipts) = setup();
    barrier.accept(0, &receipts[0]).unwrap();
    assert!(barrier
        .admit(
            1,
            &mut tokio_stream::iter([Err::<StreamSearchResponse, _>(Status::unavailable(
                "peer lost"
            ))])
        )
        .await
        .is_err());
    assert_eq!(
        barrier.wait().await.unwrap_err().code(),
        tonic::Code::Unavailable
    );
}
