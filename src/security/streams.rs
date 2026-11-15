//! Stream wrappers that hold a principal's quota for as long as the
//! stream runs.

use std::sync::Arc;

use tonic::Status;

use super::quota::{Permit, Principal};

/// A response stream that keeps its principal's in-flight slot until
/// the client is done reading it, so a streaming query counts against
/// the concurrency quota for as long as it runs.
pub struct Guarded<S> {
    inner: S,
    _permit: Option<Permit>,
}

impl<S> Guarded<S> {
    pub fn new(inner: S, permit: Option<Permit>) -> Self {
        Guarded {
            inner,
            _permit: permit,
        }
    }
}

impl<S: tokio_stream::Stream + Unpin> tokio_stream::Stream for Guarded<S> {
    type Item = S::Item;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        std::pin::Pin::new(&mut self.inner).poll_next(cx)
    }
}

/// A routed-ingest request stream that charges each document against
/// its principal's ingest rate and ends the stream with the named
/// refusal when the rate is exceeded, never trimming the batch.
pub struct MeteredIngest<S> {
    inner: S,
    principal: Option<Arc<Principal>>,
    refused: bool,
}

impl<S> MeteredIngest<S> {
    pub fn new(inner: S, principal: Option<Arc<Principal>>) -> Self {
        MeteredIngest {
            inner,
            principal,
            refused: false,
        }
    }
}

impl<S> tokio_stream::Stream for MeteredIngest<S>
where
    S: tokio_stream::Stream<Item = Result<crate::pb::RoutedIngestMappedRequest, Status>> + Unpin,
{
    type Item = Result<crate::pb::RoutedIngestMappedRequest, Status>;
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        if self.refused {
            return std::task::Poll::Ready(None);
        }
        let next = std::pin::Pin::new(&mut self.inner).poll_next(cx);
        if let std::task::Poll::Ready(Some(Ok(message))) = &next {
            let is_document = matches!(
                message.payload,
                Some(crate::pb::routed_ingest_mapped_request::Payload::Document(
                    _
                ))
            );
            if is_document {
                if let Some(principal) = &self.principal {
                    if let Err(status) = principal.admit_ingest(1) {
                        self.refused = true;
                        return std::task::Poll::Ready(Some(Err(status)));
                    }
                }
            }
        }
        next
    }
}
