//! Transparent call tracing for a served gRPC service.
//!
//! [`Traced`] wraps any tonic server service (the generated
//! `NodeServiceServer` in the scenarios) as a tower service with the same
//! route name, so it drops into `Server::add_service` unchanged. Each
//! call opens a [`CallSpan`] in a shared [`TraceRecorder`] in arrival
//! order and the request and response bodies are wrapped to count wire
//! bytes and gRPC messages as they pass. The span closes as
//! [`Outcome::Finished`] when the `grpc-status` trailer is handed to the
//! transport, before the client can see it, so a later call that depends
//! on the response always starts after this one ended. It closes as
//! [`Outcome::Dropped`] when the response body, or the call itself before
//! any response headers, is dropped first, as on client cancellation.
//! Nothing in `src/` is touched: the
//! node is served by [`serve_traced`], which mirrors
//! `harness::serve_node` with the wrapper added.
//!
//! Byte counts are HTTP/2 DATA payload bytes, which include the 5-byte
//! gRPC message prefix; message counts come from parsing that prefix. The
//! first request message is kept (up to [`CAPTURE_LIMIT`] bytes) so a test
//! can decode it and tell apart calls that share a method name.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use bytes::{Buf, Bytes};
use http_body::{Body as HttpBody, Frame, SizeHint};
use pipestream_search::harness::nodelay_incoming;
use pipestream_search::node::NodeServiceImpl;
use pipestream_search::{H2_CONN_WINDOW, H2_STREAM_WINDOW, MAX_MESSAGE_BYTES};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tonic::body::Body;
use tonic::codegen::{http, BoxFuture, Service};
use tonic::server::NamedService;
use tonic::transport::{Error as TransportError, Server};

/// How a call ended, as seen by the server.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Open,
    Finished,
    Dropped,
}

/// One recorded call. Times are nanoseconds since the recorder started.
#[derive(Clone, Debug)]
pub struct CallSpan {
    pub id: usize,
    pub node: String,
    pub method: String,
    pub start_ns: u64,
    pub end_ns: Option<u64>,
    pub request_bytes: u64,
    pub request_messages: u64,
    pub response_bytes: u64,
    pub response_messages: u64,
    pub grpc_status: Option<i32>,
    pub outcome: Outcome,
    /// The first request message's encoded protobuf, up to
    /// [`CAPTURE_LIMIT`] bytes, for decoding with `prost::Message`.
    pub first_request: Vec<u8>,
}

impl CallSpan {
    /// Wall latency from arrival to the end of the response body.
    pub fn latency(&self) -> Option<Duration> {
        self.end_ns
            .map(|end| Duration::from_nanos(end - self.start_ns))
    }
}

/// Shared, ordered log of every traced call.
pub struct TraceRecorder {
    epoch: Instant,
    spans: Mutex<Vec<CallSpan>>,
    changed: Notify,
}

impl TraceRecorder {
    pub fn new() -> Arc<Self> {
        Arc::new(Self {
            epoch: Instant::now(),
            spans: Mutex::new(Vec::new()),
            changed: Notify::new(),
        })
    }

    fn now_ns(&self) -> u64 {
        self.epoch.elapsed().as_nanos() as u64
    }

    fn open(&self, node: &str, method: &str) -> usize {
        let mut spans = self.spans.lock().unwrap();
        let id = spans.len();
        spans.push(CallSpan {
            id,
            node: node.to_string(),
            method: method.to_string(),
            start_ns: self.now_ns(),
            end_ns: None,
            request_bytes: 0,
            request_messages: 0,
            response_bytes: 0,
            response_messages: 0,
            grpc_status: None,
            outcome: Outcome::Open,
            first_request: Vec::new(),
        });
        drop(spans);
        self.changed.notify_waiters();
        id
    }

    fn update(&self, id: usize, change: impl FnOnce(&mut CallSpan, u64)) {
        let now = self.now_ns();
        change(&mut self.spans.lock().unwrap()[id], now);
        self.changed.notify_waiters();
    }

    /// The number of calls so far: pass it to [`TraceRecorder::since`] to
    /// isolate the calls of one phase.
    pub fn checkpoint(&self) -> usize {
        self.spans.lock().unwrap().len()
    }

    /// Every call that arrived after `checkpoint`, in arrival order.
    pub fn since(&self, checkpoint: usize) -> Vec<CallSpan> {
        self.spans.lock().unwrap()[checkpoint..].to_vec()
    }

    /// Wait, at most `timeout`, until `ready` holds over the calls since
    /// `checkpoint`; the error carries the calls seen at the deadline.
    pub async fn wait_for(
        &self,
        checkpoint: usize,
        timeout: Duration,
        ready: impl Fn(&[CallSpan]) -> bool,
    ) -> Result<Vec<CallSpan>, Vec<CallSpan>> {
        let deadline = tokio::time::Instant::now() + timeout;
        loop {
            let notified = self.changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            let spans = self.since(checkpoint);
            if ready(&spans) {
                return Ok(spans);
            }
            if tokio::time::timeout_at(deadline, notified).await.is_err() {
                return Err(spans);
            }
        }
    }
}

/// Calls per `(node, method)`.
pub fn counts(spans: &[CallSpan]) -> BTreeMap<(String, String), usize> {
    let mut out = BTreeMap::new();
    for span in spans {
        *out.entry((span.node.clone(), span.method.clone()))
            .or_default() += 1;
    }
    out
}

/// Most bytes of a request's first message kept in [`CallSpan::first_request`].
pub const CAPTURE_LIMIT: usize = 4096;

/// Counts the 5-byte gRPC message prefixes in a byte stream split at
/// arbitrary frame boundaries, and keeps the start of the first message.
#[derive(Default)]
struct GrpcFramer {
    header: [u8; 5],
    have: usize,
    remaining: u64,
    seen: u64,
    first: Vec<u8>,
}

impl GrpcFramer {
    fn feed(&mut self, mut data: &[u8]) -> u64 {
        let mut messages = 0;
        while !data.is_empty() {
            if self.remaining > 0 {
                let take = self.remaining.min(data.len() as u64) as usize;
                if self.seen == 1 {
                    let room = CAPTURE_LIMIT - self.first.len().min(CAPTURE_LIMIT);
                    self.first.extend_from_slice(&data[..take.min(room)]);
                }
                self.remaining -= take as u64;
                data = &data[take..];
                continue;
            }
            let take = (5 - self.have).min(data.len());
            self.header[self.have..self.have + take].copy_from_slice(&data[..take]);
            self.have += take;
            data = &data[take..];
            if self.have == 5 {
                let len = [
                    self.header[1],
                    self.header[2],
                    self.header[3],
                    self.header[4],
                ];
                self.remaining = u32::from_be_bytes(len) as u64;
                self.have = 0;
                self.seen += 1;
                messages += 1;
            }
        }
        messages
    }
}

fn status_of(headers: &http::HeaderMap) -> Option<i32> {
    headers.get("grpc-status")?.to_str().ok()?.parse().ok()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Side {
    Request,
    Response,
}

/// A body that reports its frames to the recorder and passes them on.
struct CountingBody {
    inner: Body,
    recorder: Arc<TraceRecorder>,
    id: usize,
    side: Side,
    framer: GrpcFramer,
    closed: bool,
}

impl CountingBody {
    fn observe(&mut self, frame: &Frame<Bytes>) {
        if let Some(data) = frame.data_ref() {
            let (bytes, messages) = (data.remaining() as u64, self.framer.feed(data.chunk()));
            let (side, first) = (self.side, &self.framer.first);
            self.recorder.update(self.id, |span, _| match side {
                Side::Request => {
                    span.request_bytes += bytes;
                    span.request_messages += messages;
                    if span.first_request.len() < first.len() {
                        span.first_request.clone_from(first);
                    }
                }
                Side::Response => {
                    span.response_bytes += bytes;
                    span.response_messages += messages;
                }
            });
        } else if let Some(trailers) = frame.trailers_ref() {
            let status = status_of(trailers);
            self.recorder
                .update(self.id, |span, _| span.grpc_status = status);
            self.close(Outcome::Finished);
        }
    }

    fn close(&mut self, outcome: Outcome) {
        if self.side == Side::Response && !self.closed {
            self.closed = true;
            self.recorder.update(self.id, |span, now| {
                span.end_ns = Some(now);
                span.outcome = outcome;
            });
        }
    }
}

impl HttpBody for CountingBody {
    type Data = Bytes;
    type Error = tonic::Status;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, tonic::Status>>> {
        let polled = Pin::new(&mut self.inner).poll_frame(cx);
        match &polled {
            Poll::Ready(Some(Ok(frame))) => self.observe(frame),
            Poll::Ready(Some(Err(_))) => self.close(Outcome::Dropped),
            Poll::Ready(None) => self.close(Outcome::Finished),
            Poll::Pending => {}
        }
        polled
    }

    fn is_end_stream(&self) -> bool {
        self.inner.is_end_stream()
    }

    fn size_hint(&self) -> SizeHint {
        self.inner.size_hint()
    }
}

impl Drop for CountingBody {
    fn drop(&mut self) {
        // A body dropped once it has nothing left to yield still finished.
        let outcome = if self.inner.is_end_stream() {
            Outcome::Finished
        } else {
            Outcome::Dropped
        };
        self.close(outcome);
    }
}

/// Closes a call's span as [`Outcome::Dropped`] if the call is dropped
/// before it has response headers, unless disarmed first.
struct PendingSpan {
    recorder: Arc<TraceRecorder>,
    id: usize,
    armed: bool,
}

impl PendingSpan {
    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for PendingSpan {
    fn drop(&mut self) {
        if self.armed {
            self.recorder.update(self.id, |span, now| {
                if span.outcome == Outcome::Open {
                    span.end_ns = Some(now);
                    span.outcome = Outcome::Dropped;
                }
            });
        }
    }
}

/// A traced tonic server service. `node` labels every span it records.
#[derive(Clone)]
pub struct Traced<S> {
    inner: S,
    node: Arc<str>,
    recorder: Arc<TraceRecorder>,
}

impl<S> Traced<S> {
    pub fn new(inner: S, node: &str, recorder: Arc<TraceRecorder>) -> Self {
        Self {
            inner,
            node: node.into(),
            recorder,
        }
    }
}

impl<S: NamedService> NamedService for Traced<S> {
    const NAME: &'static str = S::NAME;
}

impl<S> Service<http::Request<Body>> for Traced<S>
where
    S: Service<http::Request<Body>, Response = http::Response<Body>, Error = Infallible>,
    S::Future: Send + 'static,
{
    type Response = http::Response<Body>;
    type Error = Infallible;
    type Future = BoxFuture<Self::Response, Infallible>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<Body>) -> Self::Future {
        let method = request
            .uri()
            .path()
            .rsplit('/')
            .next()
            .unwrap_or("")
            .to_string();
        let recorder = self.recorder.clone();
        let id = recorder.open(&self.node, &method);
        let counting = {
            let recorder = recorder.clone();
            move |inner: Body, side: Side| CountingBody {
                inner,
                recorder: recorder.clone(),
                id,
                side,
                framer: GrpcFramer::default(),
                closed: false,
            }
        };
        let request = request.map(|body| Body::new(counting(body, Side::Request)));
        let response = self.inner.call(request);
        let pending = PendingSpan {
            recorder: recorder.clone(),
            id,
            armed: true,
        };
        Box::pin(async move {
            let response = response.await?;
            if let Some(status) = status_of(response.headers()) {
                // Trailers-only response: the status is in the headers and
                // no body follows.
                recorder.update(id, |span, now| {
                    span.grpc_status = Some(status);
                    span.end_ns = Some(now);
                    span.outcome = Outcome::Finished;
                });
                pending.disarm();
                return Ok(response);
            }
            let response = response.map(|body| Body::new(counting(body, Side::Response)));
            // The response body owns the span from here.
            pending.disarm();
            Ok(response)
        })
    }
}

/// A node served behind [`Traced`] on 127.0.0.1:0.
pub struct TracedNode {
    pub addr: String,
    task: JoinHandle<Result<(), TransportError>>,
}

impl Drop for TracedNode {
    fn drop(&mut self) {
        self.task.abort();
    }
}

/// Serve `node` with its `NodeService` traced as `label`, exactly as
/// `harness::serve_node` serves it otherwise.
pub async fn serve_traced(
    node: NodeServiceImpl,
    label: &str,
    recorder: Arc<TraceRecorder>,
) -> TracedNode {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    node.spawn_floor_listener(addr);
    let diagnostics = node.diagnostics_server(MAX_MESSAGE_BYTES);
    let traced = Traced::new(
        NodeServiceImpl::into_server(node, MAX_MESSAGE_BYTES),
        label,
        recorder,
    );
    let task = tokio::spawn(
        Server::builder()
            .initial_stream_window_size(H2_STREAM_WINDOW)
            .initial_connection_window_size(H2_CONN_WINDOW)
            .add_service(traced)
            .add_service(diagnostics)
            .serve_with_incoming(nodelay_incoming(listener)),
    );
    TracedNode {
        addr: format!("http://{addr}"),
        task,
    }
}

#[test]
fn the_framer_counts_messages_split_across_frames() {
    let mut framer = GrpcFramer::default();
    let mut wire = vec![0, 0, 0, 0, 3, b'a', b'b', b'c'];
    wire.extend([0, 0, 0, 0, 0]);
    wire.extend([0, 0, 0, 0, 1, b'z']);
    let counted: u64 = wire.chunks(2).map(|chunk| framer.feed(chunk)).sum();
    assert_eq!(counted, 3);
    assert_eq!((framer.have, framer.remaining), (0, 0));
    assert_eq!(framer.first, b"abc", "only the first message is kept");
}
