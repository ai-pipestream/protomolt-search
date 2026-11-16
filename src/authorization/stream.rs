//! Streams that stop disclosing as soon as a permit is revoked.

use super::AccessPermit;
use std::pin::Pin;
use std::task::{Context, Poll};
use tokio_stream::{wrappers::WatchStream, Stream};
use tonic::Status;

/// Rejects a revoked stream even while its producer is pending, then drops the
/// producer. A policy revision is checked before each disclosed item.
pub struct AuthorizedStream<S> {
    inner: Option<S>,
    permits: Vec<AccessPermit>,
    revisions: Vec<WatchStream<u64>>,
}
impl<S> AuthorizedStream<S> {
    pub fn new(inner: S, permit: Option<AccessPermit>) -> Self {
        Self::with_permits(inner, permit.into_iter().collect())
    }

    /// A stream whose disclosure requires every resource decision to remain
    /// valid. Subscribe to each authority so revocation wakes an idle producer.
    pub fn with_permits(inner: S, permits: Vec<AccessPermit>) -> Self {
        let revisions = permits
            .iter()
            .map(|p| WatchStream::new(p.revisions.clone()))
            .collect();
        Self {
            inner: Some(inner),
            permits,
            revisions,
        }
    }
}
impl<S, T> Stream for AuthorizedStream<S>
where
    S: Stream<Item = Result<T, Status>> + Unpin,
{
    type Item = Result<T, Status>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.inner.is_none() {
            return Poll::Ready(None);
        }
        for revisions in &mut self.revisions {
            while let Poll::Ready(Some(_)) = Pin::new(&mut *revisions).poll_next(cx) {}
        }
        for permit in &self.permits {
            if let Err(error) = permit.check() {
                self.inner = None;
                return Poll::Ready(Some(Err(error)));
            }
        }
        let result = Pin::new(self.inner.as_mut().expect("checked above")).poll_next(cx);
        // A producer may yield after doing work that spans a policy replacement.
        if matches!(result, Poll::Ready(Some(_))) {
            for permit in &self.permits {
                if let Err(error) = permit.check() {
                    self.inner = None;
                    return Poll::Ready(Some(Err(error)));
                }
            }
        }
        if matches!(result, Poll::Ready(None)) {
            self.inner = None;
        }
        result
    }
}
