//! Collector and stream completion invariants.

use super::*;

fn row_bytes(rows: &[(u64, f32)]) -> Vec<u8> {
    rows.iter()
        .flat_map(|(id, score)| id.to_le_bytes().into_iter().chain(score.to_le_bytes()))
        .collect()
}

fn parent_bytes(rows: &[(u64, f32, u64)]) -> Vec<u8> {
    rows.iter()
        .flat_map(|(id, score, parent)| {
            id.to_le_bytes()
                .into_iter()
                .chain(score.to_le_bytes())
                .chain(parent.to_le_bytes())
        })
        .collect()
}

fn certificate() -> StreamSearchSummary {
    StreamSearchSummary {
        completed: true,
        ..Default::default()
    }
}

#[test]
fn row_collector_keeps_negative_boundary_ties_and_deduplicates_per_shard() {
    let mut rows = RowCollector::new(2);
    let update = rows
        .batch(1, &row_bytes(&[(8, -2.0), (9, -3.0)]), 2)
        .unwrap();
    assert!(update.full && update.changed);
    assert_eq!(update.kth, Some(-3.0));
    rows.batch(0, &row_bytes(&[(4, -3.0)]), 2).unwrap();
    // A duplicate cannot rewrite the score or evict a different row.
    let duplicate = rows.batch(0, &row_bytes(&[(4, 100.0)]), 2).unwrap();
    assert!(!duplicate.changed);
    let hits = rows.finish();
    assert_eq!(
        hits.iter()
            .map(|h| (h.shard, h.vector_id, h.score))
            .collect::<Vec<_>>(),
        vec![(1, 8, -2.0), (0, 4, -3.0)]
    );
}

#[test]
fn parent_collector_preserves_cross_shard_chunks_and_final_floor_ties() {
    let mut parents = ParentCollector::new(2);
    let first = parents
        .batch(
            0,
            &parent_bytes(&[(5, -2.0, 100), (9, -5.0, 100), (6, -3.0, 200)]),
            2,
        )
        .unwrap();
    assert_eq!(first.kth, Some(-3.0));
    let second = parents
        .batch(1, &parent_bytes(&[(2, -2.0, 100), (3, -1.0, 300)]), 2)
        .unwrap();
    assert_eq!(second.kth, Some(-2.0));
    assert!(
        !parents
            .batch(1, &parent_bytes(&[(3, 99.0, 300)]), 2)
            .unwrap()
            .changed
    );
    let (hits, groups, floor) = parents.finish(2);
    assert_eq!(floor.to_bits(), (-2.0_f32).next_down().to_bits());
    assert_eq!(
        hits.iter()
            .map(|h| (h.parent_id, h.vector_id, h.score))
            .collect::<Vec<_>>(),
        vec![(300, 3, -1.0), (100, 2, -2.0)]
    );
    assert_eq!(
        groups[1]
            .chunks
            .iter()
            .map(|h| h.vector_id)
            .collect::<Vec<_>>(),
        vec![2, 5]
    );
}

#[test]
fn shared_floor_never_rises_above_a_signed_boundary_or_moves_backwards() {
    let mut completion = StreamCompletion::new(2, None, (None, None), None);
    let first = completion
        .batch(BatchUpdate {
            full: true,
            changed: true,
            kth: Some(-8.0),
        })
        .unwrap();
    assert_eq!(first.to_bits(), (-8.0_f32).next_down().to_bits());
    assert!(first < -8.0);
    assert!(completion
        .batch(BatchUpdate {
            full: true,
            changed: true,
            kth: Some(-9.0)
        })
        .is_none());
    assert!(completion
        .batch(BatchUpdate {
            full: true,
            changed: false,
            kth: Some(-8.0)
        })
        .is_none());
    assert!(completion
        .batch(BatchUpdate {
            full: false,
            changed: true,
            kth: Some(1.0)
        })
        .is_none());
    let second = completion
        .batch(BatchUpdate {
            full: true,
            changed: true,
            kth: Some(-2.0),
        })
        .unwrap();
    assert_eq!(second.to_bits(), (-2.0_f32).next_down().to_bits());
    assert_eq!(completion.floors_sent, 2);
    let mut seeded = StreamCompletion::new(2, None, (None, None), Some(-1.0));
    assert!(seeded
        .batch(BatchUpdate {
            full: true,
            changed: true,
            kth: Some(-2.0)
        })
        .is_none());
    assert_eq!(seeded.last_floor, -1.0);
}

#[test]
fn settle_requires_active_quorum_and_only_changed_survivors_reset_it() {
    let window = Duration::from_secs(10);
    let mut completion = StreamCompletion::new(
        5,
        Some(&[true, false, false, false, false]),
        (Some(window), None),
        None,
    );
    assert_eq!(completion.quorum, 3);
    assert_eq!(completion.remaining, 4);
    completion.completed(1, certificate(), true);
    completion.completed(2, certificate(), true);
    assert!(completion.settle_at.is_none());
    completion.completed(3, certificate(), true);
    assert!(completion.settle_at.is_some());
    assert!(completion.terminal[3].is_none());
    assert!(completion.summaries[3].is_some());
    let old = tokio::time::Instant::now() - Duration::from_secs(1);
    completion.settle_at = Some(old);
    completion.batch(BatchUpdate {
        full: true,
        changed: false,
        kth: None,
    });
    assert_eq!(completion.settle_at, Some(old));
    completion.batch(BatchUpdate {
        full: true,
        changed: true,
        kth: None,
    });
    assert!(completion.settle_at.unwrap() > old);
    completion.finish_sent = true;
    completion.settle_at = None;
    completion.batch(BatchUpdate {
        full: true,
        changed: true,
        kth: Some(2.0),
    });
    assert!(completion.settle_at.is_none());
}

#[tokio::test]
async fn deadline_precedes_settle_and_both_precede_an_already_queued_batch() {
    let mut completion = StreamCompletion::new(1, None, (None, None), None);
    let past = tokio::time::Instant::now() - Duration::from_secs(1);
    completion.deadline_at = Some(past);
    completion.settle_at = Some(past);
    let (tx, rx) = mpsc::channel(1);
    tx.send((
        0,
        Ok(Some(StreamSearchResponse {
            payload: Some(stream_search_response::Payload::Batch(
                crate::pb::StreamSearchBatch {
                    hits: row_bytes(&[(1, -1.0)]),
                },
            )),
        })),
    ))
    .await
    .unwrap();
    let mut fanout = StreamFanout {
        scoped: false,
        readers: tokio::task::JoinSet::new(),
        merged_rx: rx,
        floor_txs: vec![None],
        udp_lanes: vec![None],
        udp_socket: None,
        udp_key: None,
        udp_seq: Arc::new(std::sync::atomic::AtomicU32::new(0)),
    };
    assert!(matches!(
        completion.next_step(&mut fanout, false).await,
        StreamLoopStep::Deadline
    ));
    completion.deadline_at = None;
    assert!(matches!(
        completion.next_step(&mut fanout, false).await,
        StreamLoopStep::Settle
    ));
    completion.settle_at = None;
    assert!(matches!(
        completion.next_step(&mut fanout, false).await,
        StreamLoopStep::Message(Ok(Some((0, _))))
    ));
}

#[test]
fn collectors_reject_the_other_wire_record_width() {
    assert!(RowCollector::new(1)
        .batch(0, &parent_bytes(&[(1, 2.0, 3)]), 1)
        .is_err());
    assert!(ParentCollector::new(1)
        .batch(0, &row_bytes(&[(1, 2.0)]), 1)
        .is_err());
}
