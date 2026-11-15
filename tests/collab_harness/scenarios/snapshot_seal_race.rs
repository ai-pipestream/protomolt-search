//! `Flush` racing a paused `InstallSnapshot` on one traced node.
//!
//! `NodeService` has no `Seal` RPC; `Flush` is the seal step, persisting
//! the shard's current index. The install stream is held at a fixed protocol
//! boundary, after the manifest and the first half of the image have
//! reached the node, by a [`GateHook`] stage in the stream producer. With
//! the install parked there, `Flush` runs to completion, and only then is
//! the stream released. Two schedules run back to back on one node:
//!
//! 1. Complete install: the flush persists the pre-install shard (64
//!    vectors at the legacy path), the install then swaps in the 512-row
//!    generation, and the spans show the flush nested inside the install.
//! 2. Truncated install: the stream ends after the first half, the node
//!    refuses it with `INVALID_ARGUMENT`, and the generation installed by
//!    schedule 1 keeps serving and keeps receiving flushes.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use pipestream_search::harness::unit_vectors;
use pipestream_search::harness::{embedded_backend_request, fit_calibration, seeded_index};
use pipestream_search::node::{
    generation_dir, generation_vector, recover_generation, NodeConfig, NodeServiceImpl,
};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::{
    snapshot_chunk, AddVectorsRequest, FlushRequest, HealthRequest, SnapshotChunk, SnapshotManifest,
};
use pipestream_search::vector::{legacy_calibration_config, VectorIndex, EMBEDDED_TURBOVEC};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::Code;

use crate::collab_harness::gate::GateHook;
use crate::collab_harness::trace_proxy::{
    counts, serve_traced, CallSpan, Outcome, TraceRecorder, TracedNode,
};

const DIM: usize = 64;
const BITS: usize = 4;
const IMAGE_ROWS: usize = 512;
const LIVE_ROWS: usize = 64;
const MID_STREAM: &str = "install.after-first-half";
const WAIT: Duration = Duration::from_secs(4);

struct Fixture {
    dir: PathBuf,
    node_tv: PathBuf,
    image: Vec<u8>,
    recorder: Arc<TraceRecorder>,
    client: NodeServiceClient<Channel>,
    _node: TracedNode,
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

/// A seeded 512-row image on disk and a node holding 64 rows under the
/// image's calibration.
async fn fixture(tag: &str) -> Fixture {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("collab_seal_race_{tag}_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let corpus = unit_vectors(IMAGE_ROWS, DIM, 0x5EA1_0001);
    let (shift, scale) = fit_calibration(DIM, BITS, &corpus);
    let mut index = seeded_index(DIM, BITS, &shift, &scale);
    index.add(&corpus, DIM).unwrap();
    index.prepare().unwrap();
    let image_path = dir.join("image.tv");
    index.write(&image_path).unwrap();
    let legacy = legacy_calibration_config(&index.backend_config().unwrap())
        .unwrap()
        .unwrap();

    let node_tv = dir.join("shard.tv");
    let config = NodeConfig {
        index_path: Some(node_tv.clone()),
        ..Default::default()
    };
    let recorder = TraceRecorder::new();
    let node = serve_traced(
        NodeServiceImpl::new(None, config),
        "shard0",
        recorder.clone(),
    )
    .await;
    let mut client = NodeServiceClient::connect(node.addr.clone()).await.unwrap();
    let configure = embedded_backend_request(DIM, BITS, &legacy.shift, &legacy.scale);
    client.configure_vector_backend(configure).await.unwrap();
    let batch = AddVectorsRequest {
        vectors: corpus[..LIVE_ROWS * DIM].to_vec(),
        dim: DIM as u32,
        ..Default::default()
    };
    client
        .add_vectors(tokio_stream::iter([batch]))
        .await
        .unwrap();
    Fixture {
        image: std::fs::read(&image_path).unwrap(),
        dir,
        node_tv,
        recorder,
        client,
        _node: node,
    }
}

/// The manifest and the first half of `image`, a park at [`MID_STREAM`],
/// then the second half unless `truncate`.
fn install_stream(
    image: Vec<u8>,
    gate: Arc<GateHook>,
    truncate: bool,
) -> ReceiverStream<SnapshotChunk> {
    let (tx, rx) = mpsc::channel(4);
    tokio::spawn(async move {
        let chunk = |payload| SnapshotChunk {
            payload: Some(payload),
        };
        let manifest = SnapshotManifest {
            vector_bytes: image.len() as u64,
            ..Default::default()
        };
        let half = image.len() / 2;
        let _ = tx
            .send(chunk(snapshot_chunk::Payload::Manifest(manifest)))
            .await;
        let _ = tx
            .send(chunk(snapshot_chunk::Payload::Data(image[..half].to_vec())))
            .await;
        if gate.enter(MID_STREAM).await.is_ok() && !truncate {
            let _ = tx
                .send(chunk(snapshot_chunk::Payload::Data(image[half..].to_vec())))
                .await;
        }
    });
    ReceiverStream::new(rx)
}

fn rows_at(path: &Path) -> usize {
    VectorIndex::load(EMBEDDED_TURBOVEC, path).unwrap().len()
}

fn span<'a>(spans: &'a [CallSpan], method: &str) -> &'a CallSpan {
    spans.iter().find(|s| s.method == method).unwrap()
}

impl Fixture {
    /// Start an install, wait until it is parked mid-stream with both of
    /// its first messages delivered, flush, and return the flushed path
    /// with the still-running install.
    async fn flush_during_install(
        &mut self,
        truncate: bool,
    ) -> (
        String,
        tokio::task::JoinHandle<Result<u64, tonic::Status>>,
        Arc<GateHook>,
    ) {
        let gate = GateHook::new(WAIT);
        let checkpoint = self.recorder.checkpoint();
        let stream = install_stream(self.image.clone(), gate.clone(), truncate);
        let mut client = self.client.clone();
        let install = tokio::spawn(async move {
            let response = client.install_snapshot(stream).await?;
            Ok(response.into_inner().num_vectors)
        });
        gate.wait_until_entered(MID_STREAM).await.unwrap();
        let delivered = |spans: &[CallSpan]| spans.iter().any(|s| s.request_messages >= 2);
        self.recorder
            .wait_for(checkpoint, WAIT, delivered)
            .await
            .unwrap();

        let flushed = tokio::time::timeout(WAIT, self.client.flush(FlushRequest {}))
            .await
            .expect("flush must not wait on a parked install")
            .unwrap()
            .into_inner();
        assert!(flushed.written);
        let spans = self.recorder.since(checkpoint);
        let methods: Vec<&str> = spans.iter().map(|s| s.method.as_str()).collect();
        assert_eq!(methods, ["InstallSnapshot", "Flush"]);
        assert_eq!(span(&spans, "InstallSnapshot").outcome, Outcome::Open);
        (flushed.path, install, gate)
    }

    /// Wait for the calls of the schedule started at `checkpoint` to
    /// close, check they are exactly `methods` in arrival order, and check
    /// the flush ran entirely inside the install.
    async fn settled(&self, checkpoint: usize, methods: &[&str]) -> CallSpan {
        let closed = |spans: &[CallSpan]| {
            spans.len() == methods.len() && spans.iter().all(|s| s.outcome == Outcome::Finished)
        };
        let spans = self
            .recorder
            .wait_for(checkpoint, WAIT, closed)
            .await
            .unwrap();
        let arrived: Vec<&str> = spans.iter().map(|s| s.method.as_str()).collect();
        assert_eq!(arrived, methods);
        let shard0 = |method: &str| (("shard0".to_string(), method.to_string()), 1);
        assert_eq!(counts(&spans), methods.iter().map(|m| shard0(m)).collect());
        let (install, flush) = (span(&spans, "InstallSnapshot"), span(&spans, "Flush"));
        assert_eq!(flush.grpc_status, Some(0));
        assert!(install.id < flush.id && install.start_ns < flush.start_ns);
        assert!(flush.end_ns.unwrap() < install.end_ns.unwrap());
        assert!(install.request_bytes >= self.image.len() as u64 / 2);
        install.clone()
    }

    async fn rows_served(&mut self) -> u64 {
        let health = self.client.health(HealthRequest {}).await.unwrap();
        health.into_inner().num_vectors
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn flush_inside_a_parked_install_persists_the_pre_install_shard() {
    let mut fx = fixture("flush").await;
    let gen_tv = generation_vector(&generation_dir(&fx.node_tv));

    // Schedule 1: complete install.
    let checkpoint = fx.recorder.checkpoint();
    let (path, install, gate) = fx.flush_during_install(false).await;
    assert_eq!(PathBuf::from(path), fx.node_tv, "no generation exists yet");
    assert_eq!(rows_at(&fx.node_tv), LIVE_ROWS);
    assert_eq!(fx.rows_served().await, LIVE_ROWS as u64);
    gate.release(MID_STREAM);
    assert_eq!(install.await.unwrap().unwrap(), IMAGE_ROWS as u64);
    let installed = fx
        .settled(checkpoint, &["InstallSnapshot", "Flush", "Health"])
        .await;
    assert_eq!(installed.grpc_status, Some(0));
    assert_eq!(installed.request_messages, 3);
    assert_eq!(fx.rows_served().await, IMAGE_ROWS as u64);
    assert_eq!(rows_at(&gen_tv), IMAGE_ROWS);

    // Schedule 2: truncated install over the installed generation.
    let checkpoint = fx.recorder.checkpoint();
    let (path, install, gate) = fx.flush_during_install(true).await;
    assert_eq!(
        PathBuf::from(path),
        gen_tv,
        "flushes land in the generation"
    );
    gate.release(MID_STREAM);
    let refused = install.await.unwrap().unwrap_err();
    assert_eq!(refused.code(), Code::InvalidArgument, "{refused}");
    let truncated = fx.settled(checkpoint, &["InstallSnapshot", "Flush"]).await;
    assert_eq!(truncated.grpc_status, Some(Code::InvalidArgument as i32));
    assert_eq!(truncated.request_messages, 2);
    assert_eq!(fx.rows_served().await, IMAGE_ROWS as u64);
    assert_eq!(rows_at(&gen_tv), IMAGE_ROWS);
    assert_eq!(
        recover_generation(&fx.node_tv),
        Some(generation_dir(&fx.node_tv))
    );
}

/// An install aborted by its client mid-stream, before any response
/// headers, closes its span with an end time and leaves the shard as it
/// was. Two transport orders are legitimate: the node sees the reset
/// stream fail and answers with an error status (`Finished`, non-zero
/// status), or the server drops the call first (`Dropped`, no status);
/// `cancelled_call` pins the second path down deterministically.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_install_aborted_mid_stream_closes_its_span_and_keeps_the_shard() {
    let mut fx = fixture("abort").await;
    let gate = GateHook::new(WAIT);
    let checkpoint = fx.recorder.checkpoint();
    let stream = install_stream(fx.image.clone(), gate.clone(), false);
    let mut client = fx.client.clone();
    let call = tokio::spawn(async move { client.install_snapshot(stream).await });
    gate.wait_until_entered(MID_STREAM).await.unwrap();
    let parked = |spans: &[CallSpan]| spans.iter().any(|s| s.request_messages >= 2);
    let spans = fx
        .recorder
        .wait_for(checkpoint, WAIT, parked)
        .await
        .unwrap();
    assert_eq!(spans[0].outcome, Outcome::Open);
    assert_eq!(spans[0].response_bytes, 0, "no response has started");

    call.abort();
    let closed = |spans: &[CallSpan]| spans.iter().all(|s| s.outcome != Outcome::Open);
    let spans = fx
        .recorder
        .wait_for(checkpoint, WAIT, closed)
        .await
        .unwrap();
    assert_eq!(spans.len(), 1);
    let aborted = &spans[0];
    assert_eq!(aborted.method, "InstallSnapshot");
    assert!(aborted.end_ns.is_some_and(|end| end >= aborted.start_ns));
    match aborted.outcome {
        Outcome::Dropped => assert_eq!(aborted.grpc_status, None),
        Outcome::Finished => assert!(aborted.grpc_status.is_some_and(|s| s != 0)),
        Outcome::Open => unreachable!("waited for the span to close"),
    }
    gate.release(MID_STREAM);
    assert_eq!(fx.rows_served().await, LIVE_ROWS as u64);
}
