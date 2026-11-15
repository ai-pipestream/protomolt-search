use super::service::snapshot_stream;
use super::*;

#[tokio::test]
async fn closing_an_idle_snapshot_stream_drops_its_producer_immediately() {
    struct Released(Option<tokio::sync::oneshot::Sender<()>>);
    impl Drop for Released {
        fn drop(&mut self) {
            let _ = self.0.take().unwrap().send(());
        }
    }
    let (sender, receiver) = tokio::sync::oneshot::channel();
    let released = Released(Some(sender));
    let mut stream = snapshot_stream(Duration::from_secs(60), move || {
        let _ = &released;
        MetricsSnapshot::default()
    });
    use tokio_stream::StreamExt;
    stream.next().await.unwrap().unwrap();
    drop(stream);
    tokio::time::timeout(Duration::from_secs(1), receiver)
        .await
        .expect("dropping a revoked stream must release the producer before its next tick")
        .unwrap();
}

fn node_knobs() -> Knobs {
    Knobs::node(
        "node-test",
        NodeKnobValues {
            share_floors: true,
            segment_pruning: true,
            floor_delta: 0.0,
            floor_warmup_chunks: 0,
            floor_min_interval_ms: 0,
        },
        vec![FixedKnob {
            name: "chunk_blocks",
            kind: KnobKind::Int,
            value: "8192".to_string(),
            description: "SIMD blocks per scan chunk.",
        }],
    )
}

#[test]
fn a_live_knob_flips_and_lists_its_startup_value() {
    let knobs = node_knobs();
    knobs.set("segment_pruning", "false").unwrap();
    assert!(!knobs.segment_pruning());
    let listed = knobs.list();
    let entry = listed
        .knobs
        .iter()
        .find(|k| k.name == "segment_pruning")
        .unwrap();
    assert_eq!(entry.value, "false");
    assert_eq!(entry.startup_value, "true");
    assert!(entry.mutable);
    knobs.set("floor_delta", "0.25").unwrap();
    assert_eq!(knobs.floor_delta(), 0.25);
    knobs.set("floor_min_interval_ms", "40").unwrap();
    assert_eq!(knobs.floor_min_interval_ms(), 40);
}

#[test]
fn immutable_unknown_and_malformed_are_rejected_by_name() {
    let knobs = node_knobs();
    let fixed = knobs.set("chunk_blocks", "4096").unwrap_err();
    assert_eq!(fixed.code(), tonic::Code::FailedPrecondition);
    assert!(fixed.message().contains("chunk_blocks"));
    let unknown = knobs.set("max_k", "5").unwrap_err();
    assert_eq!(unknown.code(), tonic::Code::InvalidArgument);
    assert!(unknown.message().contains("segment_pruning"));
    let bad = knobs.set("segment_pruning", "maybe").unwrap_err();
    assert_eq!(bad.code(), tonic::Code::InvalidArgument);
    assert!(knobs.segment_pruning());
    let negative = knobs.set("floor_delta", "-1").unwrap_err();
    assert_eq!(negative.code(), tonic::Code::InvalidArgument);
}

#[test]
fn coordinator_knobs_cap_k_and_hedge() {
    let knobs = Knobs::coordinator(
        "coord-test",
        CoordinatorKnobValues {
            max_k: 100,
            hedge_delay_ms: 0,
            shard_pruning: true,
            signal_batch: 10_000,
        },
        Vec::new(),
    );
    assert_eq!(knobs.max_k(), 100);
    assert!(knobs.hedge_delay().is_none());
    knobs.set("max_k", "250").unwrap();
    knobs.set("hedge_delay_ms", "30").unwrap();
    assert_eq!(knobs.max_k(), 250);
    assert_eq!(knobs.hedge_delay(), Some(Duration::from_millis(30)));
    assert_eq!(
        knobs.set("max_k", "0").unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
    assert_eq!(
        knobs.set("segment_pruning", "false").unwrap_err().code(),
        tonic::Code::InvalidArgument
    );
}

#[test]
fn the_ring_is_bounded_and_newest_first() {
    let ring = RecentRing::default();
    for i in 0..(RECENT_RING as u64 + 10) {
        ring.push(RecentQuery {
            unix_ms: i,
            ..Default::default()
        });
    }
    assert_eq!(ring.total_seen(), RECENT_RING as u64 + 10);
    let recent = ring.recent(3);
    let stamps: Vec<u64> = recent.iter().map(|q| q.unix_ms).collect();
    assert_eq!(
        stamps,
        vec![
            RECENT_RING as u64 + 9,
            RECENT_RING as u64 + 8,
            RECENT_RING as u64 + 7
        ]
    );
    assert_eq!(ring.recent(1000).len(), RECENT_RING);
    assert_eq!(ring.recent(1000).last().unwrap().unix_ms, 10);
}
