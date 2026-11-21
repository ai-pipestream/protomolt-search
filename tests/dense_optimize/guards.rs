//! Runtime bounds, single-flight locking, corpus mutation aborts, and explicit
//! post-sweep host-profile verification without masking sweep RPC errors.

use std::future::Future;
use std::ops::Range;
use std::path::Path;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::dense_policy::local_host_profile;
use pipestream_search::harness::nodelay_incoming;
use pipestream_search::hnsw::{HnswConfig, HnswIndex};
use pipestream_search::node::{NodeConfig, NodeServiceImpl};
use pipestream_search::pb::node_service_server::{NodeService, NodeServiceServer};
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{DenseHostProfile, GetVectorBackendRequest, GetVectorBackendResponse};
use pipestream_search::vector::*;
use pipestream_search::MAX_MESSAGE_BYTES;
use tokio::sync::Notify;
use tonic::codegen::{http, Service};
use tonic::{Code, Request, Response, Status};

use super::{optimize_request, send_vectors, unit_vectors, NodeTask, DIM};

struct DelayedProvider {
    inner: HnswIndex,
    delay_ms: Arc<AtomicU64>,
}

impl DelayedProvider {
    fn pause(&self) {
        let ms = self.delay_ms.load(Ordering::Relaxed);
        if ms > 0 {
            std::thread::sleep(Duration::from_millis(ms));
        }
    }
}

impl VectorProvider for DelayedProvider {
    fn descriptor(&self) -> VectorBackendDescriptor {
        self.inner.descriptor()
    }
    fn backend_config(&self) -> Result<VectorBackendConfig, VectorError> {
        self.inner.backend_config()
    }
    fn len(&self) -> usize {
        self.inner.len()
    }
    fn dimension(&self) -> Option<usize> {
        self.inner.dimension()
    }
    fn is_graph_linked(&self) -> bool {
        self.inner.is_graph_linked()
    }
    fn add(&mut self, vectors: &[f32], dimension: usize) -> Result<(), VectorError> {
        self.inner.add(vectors, dimension)
    }
    fn prepare(&mut self) -> Result<(), VectorError> {
        self.inner.prepare()
    }
    fn write(&self, path: &Path) -> Result<(), VectorError> {
        self.inner.write(path)
    }
    fn search(
        &self,
        queries: &[f32],
        k: usize,
        options: VectorSearchOptions<'_>,
    ) -> Result<VectorSearchResults, VectorError> {
        self.pause();
        self.inner.search(queries, k, options)
    }
    fn search_streaming_controlled(
        &self,
        queries: &[f32],
        options: VectorSearchOptions<'_>,
        sink: &mut dyn FnMut(&VectorStreamBatch<'_>) -> VectorStreamControl,
        control: &mut dyn FnMut() -> VectorStreamControl,
    ) -> Result<VectorStreamSummary, VectorError> {
        self.pause();
        self.inner
            .search_streaming_controlled(queries, options, sink, control)
    }
    fn row_transcripts(
        &self,
        rows: Range<usize>,
        sink: &mut dyn FnMut(usize, &[u8]),
    ) -> Result<(), VectorError> {
        self.inner.row_transcripts(rows, sink)
    }
}

#[derive(Default)]
struct TermStatsGate {
    calls: AtomicUsize,
    hold_on_call: AtomicUsize,
    entered: Notify,
    release: Notify,
}

#[derive(Clone)]
struct InterceptedNode {
    node: NodeServiceImpl,
    host_override: Arc<Mutex<Option<DenseHostProfile>>>,
    fail_search: Arc<AtomicBool>,
    term_gate: Arc<TermStatsGate>,
}

impl tonic::server::NamedService for InterceptedNode {
    const NAME: &'static str =
        <NodeServiceServer<NodeServiceImpl> as tonic::server::NamedService>::NAME;
}

impl<B> Service<http::Request<B>> for InterceptedNode
where
    B: http_body::Body + Send + 'static,
    B::Data: Send,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = http::Response<tonic::body::Body>;
    type Error = std::convert::Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }

    fn call(&mut self, request: http::Request<B>) -> Self::Future {
        let node = self.node.clone();
        let path = request.uri().path().to_string();
        if path.ends_with("/SearchShard") && self.fail_search.load(Ordering::SeqCst) {
            let mut host = local_host_profile();
            host.logical_cores += 16;
            *self.host_override.lock().unwrap() = Some(host);
            return Box::pin(async move {
                Ok(Status::unavailable("injected shard search failure").into_http())
            });
        }
        let host_override = self.host_override.lock().unwrap().clone();
        if path.ends_with("/GetVectorBackend") && host_override.is_some() {
            return Box::pin(async move {
                struct Backend(NodeServiceImpl, Option<DenseHostProfile>);
                impl tonic::server::UnaryService<GetVectorBackendRequest> for Backend {
                    type Response = GetVectorBackendResponse;
                    type Future = Pin<
                        Box<dyn Future<Output = Result<Response<Self::Response>, Status>> + Send>,
                    >;
                    fn call(&mut self, req: Request<GetVectorBackendRequest>) -> Self::Future {
                        let node = self.0.clone();
                        let host = self.1.clone();
                        Box::pin(async move {
                            let mut resp = node.get_vector_backend(req).await?;
                            resp.get_mut().host = host;
                            Ok(resp)
                        })
                    }
                }
                Ok(tonic::server::Grpc::new(tonic_prost::ProstCodec::default())
                    .unary(Backend(node, host_override), request)
                    .await)
            });
        }
        let gate = Arc::clone(&self.term_gate);
        let hold = path.ends_with("/TermStats") && {
            let n = gate.calls.fetch_add(1, Ordering::SeqCst) + 1;
            let target = gate.hold_on_call.load(Ordering::SeqCst);
            target > 0 && n == target
        };
        let fut = NodeServiceServer::new(node)
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES)
            .call(request);
        Box::pin(async move {
            if hold {
                gate.entered.notify_one();
                gate.release.notified().await;
            }
            fut.await
        })
    }
}

struct ControlledShard {
    addr: String,
    delay: Arc<AtomicU64>,
    host_override: Arc<Mutex<Option<DenseHostProfile>>>,
    fail_search: Arc<AtomicBool>,
    term_gate: Arc<TermStatsGate>,
    handle: NodeTask,
}

async fn start_controlled_shard(delay_ms: u64, seed: u64) -> ControlledShard {
    let inner = HnswIndex::new(DIM, HnswConfig::default()).unwrap();
    let delay = Arc::new(AtomicU64::new(0));
    let index = VectorIndex::from_provider(DelayedProvider {
        inner,
        delay_ms: Arc::clone(&delay),
    });
    let node = NodeServiceImpl::new(Some(index), NodeConfig::default());
    let host_override = Arc::new(Mutex::new(None));
    let fail_search = Arc::new(AtomicBool::new(false));
    let term_gate = Arc::new(TermStatsGate::default());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://{}", listener.local_addr().unwrap());
    let svc = InterceptedNode {
        node,
        host_override: Arc::clone(&host_override),
        fail_search: Arc::clone(&fail_search),
        term_gate: Arc::clone(&term_gate),
    };
    let handle = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(svc)
            .serve_with_incoming(nodelay_incoming(listener)),
    );
    send_vectors(&addr, unit_vectors(1_200, DIM, seed), DIM, true).await;
    delay.store(delay_ms, Ordering::Relaxed);
    ControlledShard {
        addr,
        delay,
        host_override,
        fail_search,
        term_gate,
        handle,
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn delayed_provider_respects_max_runtime_ms_single_flight_and_mid_sweep_mutation() {
    let shard = start_controlled_shard(220, 0x0971_0042).await;
    let dir = std::env::temp_dir().join(format!("dense-opt-abort-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("policy.binpb");
    let coordinator = CoordinatorServiceImpl::new(vec![shard.addr.clone()])
        .with_dense_execution_policy_at(path.clone(), None);

    let mut bad_k = optimize_request(false);
    bad_k.k = vec![99_999];
    let err = SearchService::optimize_dense_policy(&coordinator, Request::new(bad_k))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);

    let mut short = optimize_request(false);
    short.max_runtime_ms = 60;
    let t0 = Instant::now();
    let err = SearchService::optimize_dense_policy(&coordinator, Request::new(short))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::DeadlineExceeded);
    assert!(
        t0.elapsed() < Duration::from_millis(210),
        "in-flight search exceeded deadline: {:?}",
        t0.elapsed()
    );

    // Query 1 issues 2 TermStats probes (pin + validate); hold right before Query 2's pin (#3).
    shard.term_gate.calls.store(0, Ordering::SeqCst);
    shard.term_gate.hold_on_call.store(3, Ordering::SeqCst);
    let c1 = coordinator.clone();
    let mut slow_req = optimize_request(true);
    slow_req.k = vec![10];
    slow_req.query_vectors = unit_vectors(1, DIM, 0x0971_0055);
    let slow_clone = slow_req.clone();
    let first = tokio::spawn(async move {
        SearchService::optimize_dense_policy(&c1, Request::new(slow_clone)).await
    });
    tokio::time::sleep(Duration::from_millis(40)).await;
    let second = SearchService::optimize_dense_policy(&coordinator, Request::new(slow_req))
        .await
        .unwrap_err();
    assert_eq!(second.code(), Code::Aborted);

    shard.term_gate.entered.notified().await;
    send_vectors(&shard.addr, unit_vectors(1, DIM, 0x0971_0077), DIM, true).await;
    shard.delay.store(0, Ordering::Relaxed);
    shard.term_gate.release.notify_one();

    let mutated_err = first.await.unwrap().unwrap_err();
    assert_eq!(mutated_err.code(), Code::Aborted, "{mutated_err:?}");
    assert!(mutated_err
        .message()
        .contains("corpus mutated during sweep"));
    assert!(!path.exists(), "aborted sweep must never install policy");
    let _ = std::fs::remove_dir_all(dir);
    shard.handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mid_sweep_host_profile_drift_aborts_and_sweep_errors_are_not_masked() {
    let shard = start_controlled_shard(160, 0x0971_0099).await;
    let dir = std::env::temp_dir().join(format!("dense-opt-host-drift-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("policy.binpb");
    let coordinator = CoordinatorServiceImpl::new(vec![shard.addr.clone()])
        .with_dense_execution_policy_at(path.clone(), None);
    let pre_identity = coordinator.live_dense_identity().await.unwrap();

    let c1 = coordinator.clone();
    let mut req = optimize_request(true);
    req.k = vec![10];
    req.query_vectors = unit_vectors(1, DIM, 0x0971_00AA);
    let in_flight =
        tokio::spawn(
            async move { SearchService::optimize_dense_policy(&c1, Request::new(req)).await },
        );
    tokio::time::sleep(Duration::from_millis(40)).await;
    let mut drifted_host = local_host_profile();
    drifted_host.logical_cores += 8;
    *shard.host_override.lock().unwrap() = Some(drifted_host);
    shard.delay.store(0, Ordering::Relaxed);

    let err = in_flight.await.unwrap().unwrap_err();
    assert_eq!(err.code(), Code::Aborted);
    assert!(err.message().contains("host profile"), "{err}");
    assert_eq!(
        coordinator.live_dense_identity().await.unwrap(),
        pre_identity,
        "LiveIdentity stayed unchanged so only explicit host recheck caught drift"
    );
    assert!(!path.exists(), "host drift must discard sweep evidence");

    *shard.host_override.lock().unwrap() = None;
    shard.fail_search.store(true, Ordering::SeqCst);
    let mut fail_req = optimize_request(false);
    fail_req.k = vec![10];
    fail_req.query_vectors = unit_vectors(1, DIM, 0x0971_00BB);
    let err = SearchService::optimize_dense_policy(&coordinator, Request::new(fail_req))
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        Code::Internal,
        "sweep RPC failure must propagate directly without post-sweep ABORTED masking it"
    );
    let _ = std::fs::remove_dir_all(dir);
    shard.handle.abort();
}
