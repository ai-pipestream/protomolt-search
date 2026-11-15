//! The instrumentation seam every handler passes through.

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::task::{Context, Poll};
use std::time::Instant;

use tonic::{Code, Request, Response, Status};

use super::registry::{code_index, COMPLETE, ERRORS, FIRST_RESPONSE, IN_FLIGHT, REQUESTS};
use super::routes::{Route, REQUEST_ROUTES};

/// Count one served request on `route`, at the top of its handler —
/// arrivals, not successes, so a shard erroring under load is visible
/// as traffic rather than invisible as silence. [`timed`] calls this;
/// it stays public for the one-off counting a caller without a
/// request body may need.
pub fn inc_request(route: Route) {
    REQUESTS[route as usize].fetch_add(1, Ordering::Relaxed);
}

/// Marker on a request that is a re-dispatch of one already counted —
/// the coordinator delegating to its topology snapshot, a variant
/// search running its arms through the ordinary routes. [`timed`]
/// runs a marked request's body without counting it, so one arrival
/// is one count however many trait methods it passes through.
#[derive(Debug, Clone, Copy)]
struct Nested;

/// Mark `request` as already counted (see [`Nested`]).
pub fn nested<T>(mut request: Request<T>) -> Request<T> {
    request.extensions_mut().insert(Nested);
    request
}

/// The in-flight bookkeeping for one counted request: arrival counted
/// and the clock started on construction, the terminal outcome
/// recorded exactly once by `finish` or — if the future or the stream
/// is dropped first, the client having gone away — by `Drop`, as
/// `CANCELLED`, so the in-flight gauge always returns to zero.
struct Ticket {
    idx: usize,
    start: Instant,
    first_done: bool,
    done: bool,
}

impl Ticket {
    fn start(route: Route) -> Self {
        let idx = route as usize;
        REQUESTS[idx].fetch_add(1, Ordering::Relaxed);
        IN_FLIGHT[idx].fetch_add(1, Ordering::Relaxed);
        Ticket {
            idx,
            start: Instant::now(),
            first_done: false,
            done: false,
        }
    }

    fn elapsed_ns(&self) -> u64 {
        u64::try_from(self.start.elapsed().as_nanos()).unwrap_or(u64::MAX)
    }

    /// The first message of a response stream reached the transport.
    fn first_response(&mut self) {
        if !self.first_done {
            self.first_done = true;
            FIRST_RESPONSE[self.idx].observe(self.elapsed_ns());
        }
    }

    /// The terminal outcome. A streaming route that never produced a
    /// first message (refused before the stream opened, or an empty
    /// stream) records its first-response phase here too, so both
    /// phases count every request.
    fn finish(&mut self, code: Code) {
        if self.done {
            return;
        }
        self.done = true;
        let ns = self.elapsed_ns();
        if REQUEST_ROUTES[self.idx].2 && !self.first_done {
            self.first_done = true;
            FIRST_RESPONSE[self.idx].observe(ns);
        }
        COMPLETE[self.idx].observe(ns);
        if let Some(code) = code_index(code) {
            ERRORS[self.idx][code].fetch_add(1, Ordering::Relaxed);
        }
        IN_FLIGHT[self.idx].fetch_sub(1, Ordering::Relaxed);
    }
}

impl Drop for Ticket {
    fn drop(&mut self) {
        self.finish(Code::Cancelled);
    }
}

fn is_nested<T>(request: &Request<T>) -> bool {
    request.extensions().get::<Nested>().is_some()
}

/// The instrumentation seam for a unary-response handler: count the
/// arrival, time `body`, and record the outcome by the gRPC code that
/// left it. A [`nested`] request runs uncounted. The handler keeps its
/// own shape — `timed(route, request, |request| async move { .. })`.
pub async fn timed<R, T, F, Fut>(route: Route, request: Request<R>, body: F) -> Result<T, Status>
where
    F: FnOnce(Request<R>) -> Fut,
    Fut: Future<Output = Result<T, Status>>,
{
    if is_nested(&request) {
        return body(request).await;
    }
    let mut ticket = Ticket::start(route);
    let out = body(request).await;
    ticket.finish(out.as_ref().err().map_or(Code::Ok, Status::code));
    out
}

/// The seam for a handler that answers with a response stream. The
/// stream comes back wrapped in [`Timed`], which records the
/// `first_response` phase when the first message is handed to the
/// transport and the `complete` phase (plus the error row and the
/// in-flight departure) at the stream's terminal event: exhaustion, a
/// terminal `Err`, or the stream being dropped unread. A refusal
/// before the stream opens records both phases at once.
pub async fn timed_stream<R, S, F, Fut>(
    route: Route,
    request: Request<R>,
    body: F,
) -> Result<Response<Timed<S>>, Status>
where
    F: FnOnce(Request<R>) -> Fut,
    Fut: Future<Output = Result<Response<S>, Status>>,
{
    if is_nested(&request) {
        return body(request).await.map(|response| {
            response.map(|inner| Timed {
                inner,
                ticket: None,
            })
        });
    }
    let mut ticket = Ticket::start(route);
    match body(request).await {
        Ok(response) => Ok(response.map(|inner| Timed {
            inner,
            ticket: Some(ticket),
        })),
        Err(status) => {
            ticket.finish(status.code());
            Err(status)
        }
    }
}

/// A response stream carrying its request's [`Ticket`]; see
/// [`timed_stream`]. A nested request's stream carries none.
pub struct Timed<S> {
    inner: S,
    ticket: Option<Ticket>,
}

impl<S> Timed<S> {
    /// Unwrap a NESTED re-dispatch's stream so the outer handler can
    /// hand it to its own seam. Only a nested stream carries no
    /// ticket; unwrapping a counted one would drop its ticket and
    /// record a cancellation for a stream that is still running.
    pub fn into_inner(self) -> S {
        debug_assert!(
            self.ticket.is_none(),
            "Timed::into_inner is for nested re-dispatch streams only"
        );
        self.inner
    }
}

impl<S, M> tokio_stream::Stream for Timed<S>
where
    S: tokio_stream::Stream<Item = Result<M, Status>> + Unpin,
{
    type Item = S::Item;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        let next = Pin::new(&mut this.inner).poll_next(cx);
        if let Some(ticket) = this.ticket.as_mut() {
            match &next {
                Poll::Ready(Some(Ok(_))) => ticket.first_response(),
                Poll::Ready(Some(Err(status))) => ticket.finish(status.code()),
                Poll::Ready(None) => ticket.finish(Code::Ok),
                Poll::Pending => {}
            }
        }
        next
    }
}
