//! Error disclosure at the public collection boundary. Raw handler errors are
//! diagnostic data, not automatically authorized search results.
use crate::pb::{self, AccessDecision, ErrorDisclosure, SearchErrorReason};
use prost::Message;
use std::{
    pin::Pin,
    task::{Context, Poll},
};
use tokio_stream::Stream;
use tonic::{Code, Status};

const DETAIL_TYPE: &str = "type.googleapis.com/ai.protomolt.search.v1.ErrorDisclosure";
const MAX_REASON_DETAILS_BYTES: usize = 1024;

#[derive(Clone, Copy)]
pub(crate) struct ErrorScope(bool);

impl ErrorScope {
    pub(crate) fn new(decision: Option<&AccessDecision>) -> Self {
        Self(decision.is_some_and(|decision| {
            decision.action == pb::AccessAction::Search as i32
                && (decision.document_visibility.is_some() || decision.field_permissions.is_some())
        }))
    }

    pub(crate) fn status(self, status: Status) -> Status {
        if !self.0 {
            return status;
        }
        let code = if status.code() == Code::Ok {
            Code::Internal
        } else {
            status.code()
        };
        let reason = reason(&status);
        let message = if reason == SearchErrorReason::AccessPolicyChanged {
            "access policy changed; start a new operation"
        } else {
            match code {
                Code::Cancelled => "operation cancelled; details withheld",
                Code::InvalidArgument => "invalid request; details withheld",
                Code::DeadlineExceeded => "operation deadline exceeded; details withheld",
                Code::NotFound => "requested item not found; details withheld",
                Code::AlreadyExists => "requested item already exists; details withheld",
                Code::PermissionDenied => "operation is not authorized; details withheld",
                Code::ResourceExhausted => "operation capacity exceeded; details withheld",
                Code::FailedPrecondition => {
                    "operation cannot run in the current state; details withheld"
                }
                Code::Aborted => "operation aborted; details withheld",
                Code::OutOfRange => "request is out of range; details withheld",
                Code::Unimplemented => "operation is unsupported; details withheld",
                Code::Unavailable => "service is unavailable; details withheld",
                Code::DataLoss => "data integrity failure; details withheld",
                Code::Unauthenticated => "authentication required; details withheld",
                Code::Unknown | Code::Internal | Code::Ok => "operation failed; details withheld",
            }
        };
        // Construct a fresh status: metadata, source chains and opaque rich
        // details can contain the same private data as the original message.
        disclosed_status(
            code,
            message,
            ErrorDisclosure {
                details_redacted: true,
                reason: reason as i32,
            },
        )
    }

    fn completion(self, completion: &mut pb::QueryStreamCompletion) -> Result<(), Status> {
        let pb::QueryStreamCompletion {
            completed,
            response,
            final_revision: _,
            scoring_fingerprints,
            error_code,
            error_message,
            error_disclosure,
        } = completion;
        if *completed {
            if response.is_none()
                || *error_code != 0
                || !error_message.is_empty()
                || error_disclosure.is_some()
            {
                return Err(self.status(Status::internal("malformed successful query completion")));
            }
            return Ok(());
        }
        if !self.0 {
            return Ok(());
        }
        let reason = error_disclosure
            .as_ref()
            .filter(|_| *error_code == Code::PermissionDenied as u32)
            .and_then(|detail| SearchErrorReason::try_from(detail.reason).ok())
            .unwrap_or(SearchErrorReason::Unspecified);
        let code = i32::try_from(*error_code)
            .map(Code::from_i32)
            .unwrap_or(Code::Unknown);
        let status = self.status(disclosed_status(
            code,
            "",
            ErrorDisclosure {
                details_redacted: false,
                reason: reason as i32,
            },
        ));
        *error_code = status.code() as u32;
        *error_message = status.message().to_string();
        *error_disclosure = Some(ErrorDisclosure {
            details_redacted: true,
            reason: reason as i32,
        });
        *response = None;
        scoring_fingerprints.clear();
        Ok(())
    }
}

fn disclosed_status(code: Code, message: &str, detail: ErrorDisclosure) -> Status {
    let envelope = pb::google_rpc::Status {
        code: code as i32,
        message: message.into(),
        details: vec![prost_types::Any {
            type_url: DETAIL_TYPE.into(),
            value: detail.encode_to_vec(),
        }],
    };
    Status::with_details(code, message, envelope.encode_to_vec().into())
}

/// Decode this service's disclosure detail from a matching Google RPC status.
/// Invalid, oversized, or ambiguous envelopes have no disclosure claim.
pub fn status_detail(status: &Status) -> Option<ErrorDisclosure> {
    if status.details().len() > MAX_REASON_DETAILS_BYTES {
        return None;
    }
    let envelope = pb::google_rpc::Status::decode(status.details()).ok()?;
    if envelope.code != status.code() as i32 || envelope.message != status.message() {
        return None;
    }
    let mut matching = envelope
        .details
        .iter()
        .filter(|detail| detail.type_url == DETAIL_TYPE);
    let detail = matching.next()?;
    if matching.next().is_some() {
        return None;
    }
    ErrorDisclosure::decode(detail.value.as_slice()).ok()
}

fn reason(status: &Status) -> SearchErrorReason {
    if status.code() == Code::PermissionDenied
        && status_detail(status)
            .is_some_and(|detail| detail.reason == SearchErrorReason::AccessPolicyChanged as i32)
    {
        SearchErrorReason::AccessPolicyChanged
    } else {
        SearchErrorReason::Unspecified
    }
}

pub(crate) fn policy_changed() -> Status {
    disclosed_status(
        Code::PermissionDenied,
        "access policy changed; start a new operation",
        ErrorDisclosure {
            details_redacted: false,
            reason: SearchErrorReason::AccessPolicyChanged as i32,
        },
    )
}

/// No resource decision exists yet, so an authority's diagnostics are never
/// evidence that the caller may see them.
pub(crate) fn authorization_status(status: Status) -> Status {
    ErrorScope(true).status(status)
}

/// Applies error disclosure after the authority's per-item revocation check.
/// Provisional and successful items retain their separately enforced contracts.
pub struct DisclosedQueryStream<S> {
    inner: Option<S>,
    scope: ErrorScope,
}
impl<S> DisclosedQueryStream<S> {
    pub(crate) fn new(inner: S, scope: ErrorScope) -> Self {
        Self {
            inner: Some(inner),
            scope,
        }
    }
}
impl<S> Stream for DisclosedQueryStream<S>
where
    S: Stream<Item = Result<pb::QueryStreamResponse, Status>> + Unpin,
{
    type Item = Result<pb::QueryStreamResponse, Status>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let Some(inner) = self.inner.as_mut() else {
            return Poll::Ready(None);
        };
        match Pin::new(inner).poll_next(cx) {
            Poll::Ready(Some(Err(status))) => {
                self.inner = None;
                Poll::Ready(Some(Err(self.scope.status(status))))
            }
            Poll::Ready(Some(Ok(mut response))) => {
                if let Some(pb::query_stream_response::Payload::Completion(completion)) =
                    response.payload.as_mut()
                {
                    let result = self.scope.completion(completion);
                    self.inner = None;
                    if let Err(status) = result {
                        return Poll::Ready(Some(Err(status)));
                    }
                }
                Poll::Ready(Some(Ok(response)))
            }
            Poll::Ready(None) => {
                self.inner = None;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

#[cfg(test)]
mod tests;
