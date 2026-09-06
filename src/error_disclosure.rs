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
mod tests {
    use super::*;
    use std::{error::Error, sync::Arc};
    use tokio_stream::StreamExt;

    fn scope() -> ErrorScope {
        ErrorScope::new(Some(&AccessDecision {
            action: pb::AccessAction::Search as i32,
            field_permissions: Some(pb::FieldPermissions::default()),
            ..Default::default()
        }))
    }

    fn private_error(code: Code) -> Status {
        let mut error = Status::with_details(
            code,
            "SECRET message",
            b"SECRET rich details".to_vec().into(),
        );
        error
            .metadata_mut()
            .insert("x-internal", "SECRET-header".parse().unwrap());
        error.metadata_mut().insert_bin(
            "x-internal-bin",
            tonic::metadata::MetadataValue::from_bytes(b"SECRET binary"),
        );
        error.set_source(Arc::new(std::io::Error::other("SECRET source")));
        error
    }

    fn detail(status: &Status) -> ErrorDisclosure {
        let envelope = pb::google_rpc::Status::decode(status.details()).unwrap();
        assert_eq!(envelope.code, status.code() as i32);
        assert_eq!(envelope.message, status.message());
        assert_eq!(envelope.details.len(), 1);
        assert_eq!(envelope.details[0].type_url, DETAIL_TYPE);
        ErrorDisclosure::decode(envelope.details[0].value.as_slice()).unwrap()
    }

    #[test]
    fn restricted_errors_preserve_codes_but_not_private_payloads() {
        for value in 0..=16 {
            let code = Code::from_i32(value);
            let error = scope().status(private_error(code));
            assert_eq!(
                error.code(),
                if code == Code::Ok {
                    Code::Internal
                } else {
                    code
                }
            );
            assert!(!error.message().contains("SECRET"));
            assert!(!error.details().windows(6).any(|part| part == b"SECRET"));
            assert!(error.metadata().is_empty());
            assert!(error.source().is_none());
            assert_eq!(
                detail(&error),
                ErrorDisclosure {
                    details_redacted: true,
                    reason: SearchErrorReason::Unspecified as i32,
                }
            );
            let different = scope().status(Status::new(code, "different hidden corpus"));
            assert_eq!(error.message(), different.message());
            assert_eq!(error.details(), different.details());
        }
    }

    #[test]
    fn unrestricted_errors_and_structured_policy_changes_keep_their_contract() {
        let error = ErrorScope::new(None).status(private_error(Code::Unavailable));
        assert_eq!(error.message(), "SECRET message");
        assert_eq!(error.details(), b"SECRET rich details");
        assert!(!error.metadata().is_empty());
        assert!(error.source().is_some());
        let changed = scope().status(policy_changed());
        assert_eq!(changed.code(), Code::PermissionDenied);
        assert!(changed.message().contains("policy changed"));
        assert_eq!(
            detail(&changed).reason,
            SearchErrorReason::AccessPolicyChanged as i32
        );
        assert!(detail(&changed).details_redacted);
        let repeated = scope().status(changed.clone());
        assert_eq!(changed.details(), repeated.details());
        // An arbitrary rich detail cannot change the meaning of another code.
        let forged =
            Status::with_details(Code::Internal, "SECRET", changed.details().to_vec().into());
        assert_eq!(
            detail(&scope().status(forged)).reason,
            SearchErrorReason::Unspecified as i32
        );
    }

    #[test]
    fn rich_details_require_a_bounded_unambiguous_matching_envelope() {
        let valid = policy_changed();
        assert_eq!(status_detail(&valid), Some(detail(&valid)));
        for case in 0..5 {
            let mut envelope = pb::google_rpc::Status::decode(valid.details()).unwrap();
            match case {
                0 => envelope.code = Code::Internal as i32,
                1 => envelope.message = "different".into(),
                2 => envelope.details.push(envelope.details[0].clone()),
                3 => envelope.details[0].value = vec![0xff],
                _ => envelope.details.push(prost_types::Any {
                    type_url: "unrelated".into(),
                    value: vec![0; MAX_REASON_DETAILS_BYTES],
                }),
            }
            let invalid = Status::with_details(
                valid.code(),
                valid.message(),
                envelope.encode_to_vec().into(),
            );
            assert!(status_detail(&invalid).is_none(), "case {case}");
            assert_eq!(
                detail(&scope().status(invalid)).reason,
                SearchErrorReason::Unspecified as i32
            );
        }
        let unknown = disclosed_status(
            Code::PermissionDenied,
            "",
            ErrorDisclosure {
                details_redacted: false,
                reason: 999,
            },
        );
        assert_eq!(status_detail(&unknown).unwrap().reason, 999);
        assert_eq!(
            detail(&scope().status(unknown)).reason,
            SearchErrorReason::Unspecified as i32
        );
    }

    #[test]
    fn failed_completion_codes_and_policy_changes_are_explicit() {
        for code in [
            0,
            Code::PermissionDenied as u32,
            Code::Unavailable as u32,
            u32::MAX,
        ] {
            let mut completion = pb::QueryStreamCompletion {
                error_code: code,
                error_message: "SECRET".into(),
                error_disclosure: status_detail(&policy_changed()),
                ..Default::default()
            };
            scope().completion(&mut completion).unwrap();
            assert_eq!(
                completion.error_code,
                match code {
                    0 => Code::Internal as u32,
                    u32::MAX => Code::Unknown as u32,
                    _ => code,
                }
            );
            let disclosed = completion.error_disclosure.as_ref().unwrap();
            assert!(disclosed.details_redacted);
            assert_eq!(
                disclosed.reason,
                if code == Code::PermissionDenied as u32 {
                    SearchErrorReason::AccessPolicyChanged as i32
                } else {
                    SearchErrorReason::Unspecified as i32
                }
            );
            assert!(!completion.error_message.contains("SECRET"));
            let once = completion.clone();
            scope().completion(&mut completion).unwrap();
            assert_eq!(once, completion);
        }
    }

    #[tokio::test]
    async fn a_failed_certificate_cannot_carry_results_or_be_followed_by_hits() {
        let failed = pb::QueryStreamResponse {
            payload: Some(pb::query_stream_response::Payload::Completion(
                pb::QueryStreamCompletion {
                    completed: false,
                    response: Some(pb::QueryResponse {
                        executed: "SECRET".into(),
                        ..Default::default()
                    }),
                    final_revision: 4,
                    scoring_fingerprints: vec!["SECRET".into()],
                    error_code: Code::FailedPrecondition as u32,
                    error_message: "SECRET".into(),
                    error_disclosure: None,
                },
            )),
        };
        let followup = pb::QueryStreamResponse {
            payload: Some(pb::query_stream_response::Payload::Revision(
                pb::QueryStreamRevision {
                    hits: vec![pb::QueryStreamHit {
                        doc_id: 42,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            )),
        };
        let mut stream =
            DisclosedQueryStream::new(tokio_stream::iter([Ok(failed), Ok(followup)]), scope());
        let event = stream.next().await.unwrap().unwrap();
        assert!(!event
            .encode_to_vec()
            .windows(6)
            .any(|part| part == b"SECRET"));
        let Some(pb::query_stream_response::Payload::Completion(end)) = event.payload else {
            panic!("completion")
        };
        assert!(!end.completed);
        assert!(end.response.is_none() && end.scoring_fingerprints.is_empty());
        assert_eq!(end.final_revision, 4);
        assert_eq!(end.error_code, Code::FailedPrecondition as u32);
        assert!(end.error_disclosure.unwrap().details_redacted);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn successful_certificates_cannot_smuggle_error_details() {
        let mut completion = pb::QueryStreamCompletion {
            completed: true,
            response: Some(pb::QueryResponse::default()),
            ..Default::default()
        };
        for malformed in 0..4 {
            match malformed {
                0 => completion.error_message = "SECRET".into(),
                1 => completion.error_code = Code::Internal as u32,
                2 => completion.error_disclosure = Some(ErrorDisclosure::default()),
                _ => completion.response = None,
            }
            let event = pb::QueryStreamResponse {
                payload: Some(pb::query_stream_response::Payload::Completion(
                    completion.clone(),
                )),
            };
            let mut stream = DisclosedQueryStream::new(tokio_stream::iter([Ok(event)]), scope());
            let error = stream.next().await.unwrap().unwrap_err();
            assert_eq!(error.code(), Code::Internal);
            assert!(!error.message().contains("SECRET"));
            assert!(detail(&error).details_redacted);
            assert!(stream.next().await.is_none());
            completion.error_message.clear();
            completion.error_code = 0;
            completion.error_disclosure = None;
        }
    }

    #[tokio::test]
    async fn revocation_wakes_a_pending_stream_and_passes_through_error_disclosure() {
        use crate::authorization::{AccessPermit, AuthorizedStream, PolicyAuthority};
        let mut policy = pb::AccessPolicy {
            format_version: 3,
            revision: 1,
            resources: vec![pb::CollectionResource {
                workspace: "w".into(),
                collection: "c".into(),
            }],
            grants: vec![pb::CollectionGrant {
                principal: "reader".into(),
                workspace: "w".into(),
                collection: "c".into(),
                actions: vec![pb::AccessAction::Search as i32],
                field_permissions: Some(pb::FieldPermissions::default()),
                ..Default::default()
            }],
        };
        let authority = Arc::new(PolicyAuthority::new(policy.clone()).unwrap());
        let permit =
            AccessPermit::acquire(authority.clone(), "reader", "c", pb::AccessAction::Search)
                .unwrap();
        let pending = tokio_stream::pending::<Result<pb::QueryStreamResponse, Status>>();
        let mut stream =
            DisclosedQueryStream::new(AuthorizedStream::new(pending, Some(permit)), scope());
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(10), stream.next())
                .await
                .is_err()
        );
        policy.revision = 2;
        policy.grants.clear();
        authority.replace(policy).unwrap();
        let error = tokio::time::timeout(std::time::Duration::from_secs(1), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap_err();
        assert_eq!(error.code(), Code::PermissionDenied);
        assert_eq!(
            detail(&error).reason,
            SearchErrorReason::AccessPolicyChanged as i32
        );
        assert!(detail(&error).details_redacted);
        assert!(stream.next().await.is_none());
    }
}
