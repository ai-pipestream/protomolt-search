use super::*;
use pipestream_search::pb::StreamSearchResponse;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tonic::codegen::{http, Service};

#[derive(Default)]
struct ProbeGate {
    entered: tokio::sync::Notify,
    release: tokio::sync::Notify,
    finished: tokio::sync::Notify,
    active: AtomicUsize,
    stream_calls: AtomicUsize,
    backend_calls: AtomicUsize,
}
struct ActiveProbe(Arc<ProbeGate>);
impl Drop for ActiveProbe {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::SeqCst);
        self.0.finished.notify_one();
    }
}
#[derive(Clone)]
struct HeldRoute<S> {
    inner: S,
    route: &'static str,
    gate: Arc<ProbeGate>,
}
impl<S: tonic::server::NamedService> tonic::server::NamedService for HeldRoute<S> {
    const NAME: &'static str = S::NAME;
}
impl<S, B> Service<http::Request<B>> for HeldRoute<S>
where
    S: Service<http::Request<B>> + Clone,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;
    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let method = request.uri().path().rsplit('/').next().unwrap_or_default();
        if method == "StreamSearch" {
            self.gate.stream_calls.fetch_add(1, Ordering::SeqCst);
        }
        if method == "GetVectorBackend" {
            self.gate.backend_calls.fetch_add(1, Ordering::SeqCst);
        }
        let held = method == self.route;
        let gate = self.gate.clone();
        let future = self.inner.clone().call(request);
        Box::pin(async move {
            if held {
                gate.active.fetch_add(1, Ordering::SeqCst);
                let _active = ActiveProbe(gate.clone());
                gate.entered.notify_one();
                gate.release.notified().await;
            }
            future.await
        })
    }
}

pub(super) struct Peer {
    pub(super) addr: String,
    gate: Arc<ProbeGate>,
    server: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}
impl Peer {
    pub(super) async fn start(route: &'static str) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}", listener.local_addr().unwrap());
        let gate = Arc::new(ProbeGate::default());
        let index = VectorIndex::create(EMBEDDED_TURBOVEC, DIM, BIT_WIDTH).unwrap();
        let node =
            pipestream_search::node::NodeServiceImpl::new(Some(index), NodeConfig::default());
        let service = HeldRoute {
            inner: pipestream_search::pb::node_service_server::NodeServiceServer::new(node),
            route,
            gate: gate.clone(),
        };
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(service)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        Self { addr, gate, server }
    }
}
impl Drop for Peer {
    fn drop(&mut self) {
        self.gate.release.notify_waiters();
        self.server.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_map_move_during_capability_probe_refuses_before_any_child_stream() {
    let peer = Peer::start("GetVectorBackend").await;
    let hot = CoordinatorServiceImpl::new(vec![peer.addr.clone()])
        .with_hot_topology(vec![None])
        .unwrap();
    let (relay_addr, relay, server) = start_relay_over(hot).await;
    let mut client = NodeServiceClient::connect(relay_addr).await.unwrap();
    let (tx, rx) = mpsc::channel(4);
    tx.send(StreamSearchRequest {
        payload: Some(stream_search_request::Payload::Start(StartStreamSearch {
            request_id: "capability-map-move".into(),
            vector: unit_vectors(1, DIM, 122),
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    let mut inbound = client
        .stream_search(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    tokio::time::timeout(Duration::from_secs(2), peer.gate.entered.notified())
        .await
        .expect("GetVectorBackend must be entered");
    assert_eq!(peer.gate.active.load(Ordering::SeqCst), 1);
    assert_eq!(
        peer.gate.stream_calls.load(Ordering::SeqCst),
        0,
        "capability probing precedes child StreamSearch"
    );
    assert!(
        tokio::time::timeout(Duration::from_millis(50), inbound.message())
            .await
            .is_err(),
        "the first response is blocked before fanout"
    );
    eprintln!("phase proof: GetVectorBackend entered; child StreamSearch calls=0; first response still pending before reload");
    relay
        .base()
        .reload_topology(
            1,
            vec![TopologyRoute {
                addr: peer.addr.clone(),
                replica: None,
                hash_range: None,
                placement: None,
            }],
            None,
        )
        .unwrap();
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        read_until_summary_or_error(&mut inbound),
    )
    .await;
    let refused = result
        .expect("map movement must interrupt capability probing within the test bound")
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
    assert!(
        refused.message().contains("moved from revision 0"),
        "{}",
        refused.message()
    );
    peer.wait_idle().await;
    server.abort();
}

impl Peer {
    async fn entered(&self) {
        tokio::time::timeout(Duration::from_secs(2), self.gate.entered.notified())
            .await
            .expect("capability RPC must start");
    }

    async fn wait_idle(&self) {
        tokio::time::timeout(Duration::from_secs(2), async {
            while self.gate.active.load(Ordering::SeqCst) != 0 {
                self.gate.finished.notified().await;
            }
        })
        .await
        .expect("cancelled capability RPC must be released");
        assert_eq!(self.gate.stream_calls.load(Ordering::SeqCst), 0);
    }
}

async fn pending_stream(
    peer: &Peer,
    timeout: Option<Duration>,
) -> (
    mpsc::Sender<StreamSearchRequest>,
    tonic::Streaming<StreamSearchResponse>,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
) {
    let hot = CoordinatorServiceImpl::new(vec![peer.addr.clone()]);
    let (addr, _, server) = start_relay_over(hot).await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    let (tx, rx) = mpsc::channel(4);
    tx.send(StreamSearchRequest {
        payload: Some(stream_search_request::Payload::Start(StartStreamSearch {
            request_id: "capability-lifecycle".into(),
            vector: unit_vectors(1, DIM, 123),
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    let mut request = tonic::Request::new(ReceiverStream::new(rx));
    if let Some(timeout) = timeout {
        request.set_timeout(timeout);
    }
    let response = client.stream_search(request).await.unwrap().into_inner();
    peer.entered().await;
    (tx, response, server)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stop_during_capability_probe_returns_zero_work_abandoned_summary() {
    let peer = Peer::start("GetVectorBackend").await;
    let (tx, mut inbound, server) = pending_stream(&peer, None).await;
    tx.send(StreamSearchRequest {
        payload: Some(stream_search_request::Payload::Stop(StopStreamSearch {})),
    })
    .await
    .unwrap();
    let summary = tokio::time::timeout(
        Duration::from_secs(2),
        read_until_summary_or_error(&mut inbound),
    )
    .await
    .expect("Stop must interrupt probing")
    .unwrap();
    assert!(!summary.completed);
    assert_eq!(
        summary.end_reason,
        pipestream_search::pb::StreamEndReason::Abandoned as i32
    );
    assert_eq!(summary.emitted, 0);
    assert_eq!(summary.blocks_scanned, 0);
    assert_eq!(summary.floor_raises_applied, 0);
    assert_eq!(summary.segments_total, 0);
    peer.wait_idle().await;
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deadline_during_capability_probe_releases_the_rpc() {
    let peer = Peer::start("GetVectorBackend").await;
    let (_tx, mut inbound, server) = pending_stream(&peer, Some(Duration::from_secs(1))).await;
    let refused = tokio::time::timeout(Duration::from_secs(3), inbound.message())
        .await
        .expect("deadline must interrupt probing")
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::DeadlineExceeded);
    peer.wait_idle().await;
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn response_drop_during_capability_probe_releases_the_rpc() {
    let peer = Peer::start("GetVectorBackend").await;
    let (tx, inbound, server) = pending_stream(&peer, None).await;
    // h2 deliberately does not reset a dropped receive half while the
    // request body remains live. Abandon both halves of the client attempt.
    drop(tx);
    drop(inbound);
    peer.wait_idle().await;
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn request_without_deadline_waits_for_capability_then_completes() {
    let peer = Peer::start("GetVectorBackend").await;
    let (_tx, mut inbound, server) = pending_stream(&peer, None).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), inbound.message())
            .await
            .is_err()
    );
    assert_eq!(peer.gate.active.load(Ordering::SeqCst), 1);
    peer.gate.release.notify_one();
    let summary = tokio::time::timeout(
        Duration::from_secs(2),
        read_until_summary_or_error(&mut inbound),
    )
    .await
    .expect("released probe must permit fanout")
    .unwrap();
    assert!(summary.completed);
    assert_eq!(peer.gate.backend_calls.load(Ordering::SeqCst), 1);
    assert_eq!(peer.gate.stream_calls.load(Ordering::SeqCst), 1);
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn search_shard_map_movement_interrupts_capability_probe() {
    use pipestream_search::pb::{search_shard_request, SearchShardRequest, StartShardSearch};
    let peer = Peer::start("GetVectorBackend").await;
    let hot = CoordinatorServiceImpl::new(vec![peer.addr.clone()])
        .with_hot_topology(vec![None])
        .unwrap();
    let (addr, relay, server) = start_relay_over(hot).await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    let (tx, rx) = mpsc::channel(4);
    tx.send(SearchShardRequest {
        payload: Some(search_shard_request::Payload::Start(StartShardSearch {
            vector: unit_vectors(1, DIM, 124),
            k: 1,
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    let mut inbound = client
        .search_shard(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    peer.entered().await;
    relay
        .base()
        .reload_topology(
            1,
            vec![TopologyRoute {
                addr: peer.addr.clone(),
                replica: None,
                hash_range: None,
                placement: None,
            }],
            None,
        )
        .unwrap();
    let refused = tokio::time::timeout(Duration::from_secs(2), inbound.message())
        .await
        .expect("SearchShard map move must interrupt probing")
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
    assert!(refused.message().contains("moved from revision 0"));
    peer.wait_idle().await;
    server.abort();
}

async fn pending_shard(
    peer: &Peer,
    timeout: Option<Duration>,
) -> (
    mpsc::Sender<pipestream_search::pb::SearchShardRequest>,
    tonic::Streaming<pipestream_search::pb::SearchShardResponse>,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
) {
    use pipestream_search::pb::{search_shard_request, SearchShardRequest, StartShardSearch};
    let (addr, _, server) =
        start_relay_over(CoordinatorServiceImpl::new(vec![peer.addr.clone()])).await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    let (tx, rx) = mpsc::channel(4);
    tx.send(SearchShardRequest {
        payload: Some(search_shard_request::Payload::Start(StartShardSearch {
            vector: unit_vectors(1, DIM, 125),
            k: 1,
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    let mut request = tonic::Request::new(ReceiverStream::new(rx));
    if let Some(timeout) = timeout {
        request.set_timeout(timeout);
    }
    let inbound = client.search_shard(request).await.unwrap().into_inner();
    peer.entered().await;
    (tx, inbound, server)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn search_shard_deadline_during_capability_probe_releases_the_rpc() {
    let peer = Peer::start("GetVectorBackend").await;
    let (_tx, mut inbound, server) = pending_shard(&peer, Some(Duration::from_secs(1))).await;
    let refused = tokio::time::timeout(Duration::from_secs(3), inbound.message())
        .await
        .expect("SearchShard deadline must interrupt probing")
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::DeadlineExceeded);
    peer.wait_idle().await;
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn search_shard_response_drop_during_capability_probe_releases_the_rpc() {
    let peer = Peer::start("GetVectorBackend").await;
    let (tx, inbound, server) = pending_shard(&peer, None).await;
    drop(tx);
    drop(inbound);
    peer.wait_idle().await;
    server.abort();
}

async fn finish_during_probe(deadline: bool) {
    let peer = Peer::start("GetVectorBackend").await;
    let (tx, mut inbound, server) = pending_stream(&peer, None).await;
    tx.send(StreamSearchRequest {
        payload: Some(stream_search_request::Payload::Finish(
            pipestream_search::pb::FinishStreamSearch { deadline },
        )),
    })
    .await
    .unwrap();
    let summary = tokio::time::timeout(
        Duration::from_secs(2),
        read_until_summary_or_error(&mut inbound),
    )
    .await
    .expect("Finish must interrupt capability discovery")
    .unwrap();
    assert!(
        !summary.completed,
        "no child scan ran or exhausted its rows"
    );
    let expected = if deadline {
        pipestream_search::pb::StreamEndReason::Deadline
    } else {
        pipestream_search::pb::StreamEndReason::FinishRequested
    };
    assert_eq!(summary.end_reason, expected as i32);
    assert_eq!(summary.emitted, 0);
    assert_eq!(summary.blocks_scanned, 0);
    assert_eq!(summary.floor_raises_applied, 0);
    assert_eq!(summary.segments_total, 0);
    assert_eq!(summary.segments_skipped, 0);
    assert_eq!(summary.tail_enumerated_rows, 0);
    assert_eq!(summary.floor_pruned, 0);
    assert_eq!(summary.patience_stopped, 0);
    peer.wait_idle().await;
    server.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn finish_request_during_capability_probe_returns_incomplete_summary() {
    finish_during_probe(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn finish_deadline_during_capability_probe_returns_incomplete_summary() {
    finish_during_probe(true).await;
}
