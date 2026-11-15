//! A call its client cancels while the server holds it, before any
//! response headers exist.
//!
//! The served [`Parked`] service accepts every call and never answers, so
//! the only way such a call ends is the transport dropping it when the
//! client resets the stream. Its span must close as `Dropped` with an end
//! time rather than stay open.

use std::convert::Infallible;
use std::task::{Context, Poll};
use std::time::Duration;

use pipestream_search::harness::nodelay_incoming;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::node_service_server::SERVICE_NAME;
use pipestream_search::pb::HealthRequest;
use tonic::body::Body;
use tonic::codegen::{http, BoxFuture, Service};
use tonic::server::NamedService;
use tonic::transport::Server;

use crate::collab_harness::trace_proxy::{CallSpan, Outcome, TraceRecorder, Traced};

const WAIT: Duration = Duration::from_secs(4);

/// A `NodeService` stand-in whose calls never complete.
#[derive(Clone)]
struct Parked;

impl NamedService for Parked {
    const NAME: &'static str = SERVICE_NAME;
}

impl Service<http::Request<Body>> for Parked {
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Infallible>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, _request: http::Request<Body>) -> Self::Future {
        Box::pin(std::future::pending())
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_parked_call_cancelled_before_response_headers_closes_as_dropped() {
    let recorder = TraceRecorder::new();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(
        Server::builder()
            .add_service(Traced::new(Parked, "parked", recorder.clone()))
            .serve_with_incoming(nodelay_incoming(listener)),
    );
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    let checkpoint = recorder.checkpoint();
    let call = tokio::spawn(async move { client.health(HealthRequest::default()).await });
    let arrived = |spans: &[CallSpan]| !spans.is_empty();
    let spans = recorder.wait_for(checkpoint, WAIT, arrived).await.unwrap();
    assert_eq!(
        (spans[0].method.as_str(), spans[0].outcome),
        ("Health", Outcome::Open)
    );

    call.abort();
    let closed = |spans: &[CallSpan]| spans.iter().all(|s| s.outcome != Outcome::Open);
    let spans = recorder.wait_for(checkpoint, WAIT, closed).await.unwrap();
    server.abort();
    assert_eq!(spans.len(), 1);
    let cancelled = &spans[0];
    assert_eq!(cancelled.outcome, Outcome::Dropped);
    assert!(cancelled
        .end_ns
        .is_some_and(|end| end >= cancelled.start_ns));
    assert_eq!((cancelled.grpc_status, cancelled.response_bytes), (None, 0));
    assert_eq!(cancelled.request_messages, 0, "the request was never read");
}
