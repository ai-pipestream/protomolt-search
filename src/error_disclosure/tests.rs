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
    let forged = Status::with_details(Code::Internal, "SECRET", changed.details().to_vec().into());
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
        AccessPermit::acquire(authority.clone(), "reader", "c", pb::AccessAction::Search).unwrap();
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
