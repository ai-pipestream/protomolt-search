use super::*;
use crate::pb::storage::{
    SourceHistorySeal, SourceRetirementIntent, SourceRetirementRequest, SourceSealRequest,
};
use std::sync::{Arc, Barrier};
use tonic::Code;

pub(super) fn request(f: &Fixture, operation: &[u8]) -> SourceRetirementRequest {
    SourceRetirementRequest {
        history_id: f.history_id(),
        operation_id: operation.to_vec(),
    }
}

pub(super) fn seal_request(intent: &SourceRetirementIntent) -> SourceSealRequest {
    SourceSealRequest {
        history_id: intent.history_id.clone(),
        expected_accepted_sequence: intent.accepted_sequence,
        operation_id: intent.operation_id.clone(),
    }
}

/// Malformed requests and a request for another history refuse without
/// starting a retirement.
fn assert_retirement_refuses_malformed(f: &Fixture, request: &SourceRetirementRequest) {
    for malformed in [
        SourceRetirementRequest {
            history_id: vec![],
            ..request.clone()
        },
        SourceRetirementRequest {
            operation_id: vec![],
            ..request.clone()
        },
    ] {
        assert_eq!(
            f.source.begin_retirement(&malformed).unwrap_err().code(),
            Code::InvalidArgument
        );
    }
    let mut wrong = request.clone();
    wrong.history_id[0] ^= 1;
    assert_eq!(
        f.source.begin_retirement(&wrong).unwrap_err().code(),
        Code::FailedPrecondition
    );
}

/// A retiring source replays the first write's exact retry, refuses the
/// same operation carrying another mutation, and refuses a new write by
/// name while keeping the accepted version.
fn assert_retiring_source_only_replays(f: &Fixture, original: ProtobufSource) {
    let retry = first_write_retry(original);
    let replayed = f.source.accept(&retry).unwrap();
    assert!(replayed.replayed);
    assert_eq!((replayed.version, replayed.accepted_sequence), (1, 1));
    let mut altered = retry.clone();
    altered.mutation = Some(Mutation::Delete(true));
    assert_eq!(
        f.source.accept(&altered).unwrap_err().code(),
        Code::AlreadyExists
    );
    let write = fresh_delete(b"new-during-retirement");
    let error = f.source.accept(&write).unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("retiring"), "{error}");
    assert!(f.source.get(KEY, Some(1)).unwrap().is_some());
}

/// A seal for another operation and a seal at a stale watermark both
/// refuse and leave the retirement unsealed; the retirement's own seal
/// then closes the source to new writes.
fn seal_after_refused_mismatches(
    f: &Fixture,
    intent: &SourceRetirementIntent,
) -> SourceHistorySeal {
    let mut mismatched_seal = seal_request(intent);
    mismatched_seal.operation_id = b"another-seal".to_vec();
    assert_eq!(
        f.source.seal_history(&mismatched_seal).unwrap_err().code(),
        Code::AlreadyExists
    );
    let mut stale_seal = seal_request(intent);
    stale_seal.expected_accepted_sequence -= 1;
    assert_eq!(
        f.source.seal_history(&stale_seal).unwrap_err().code(),
        Code::FailedPrecondition
    );
    assert_eq!(f.source.retirement_intent().unwrap(), Some(intent.clone()));
    assert!(f.source.history_seal().unwrap().is_none());
    let seal = f.source.seal_history(&seal_request(intent)).unwrap();
    let error = f
        .source
        .accept(&fresh_delete(b"new-after-seal"))
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("sealed"), "{error}");
    seal
}

#[tokio::test]
async fn retirement_is_persistent_idempotent_and_closes_source_writes() {
    let mut f = Fixture::new();
    let (_, candidate) = f.stage(KEY, 1, Some(0)).await;
    let original = f.source.get(KEY, Some(1)).unwrap().unwrap().1.unwrap();
    let request = request(&f, b"move-books");
    assert_retirement_refuses_malformed(&f, &request);
    let intent = f.source.begin_retirement(&request).unwrap();
    assert_eq!(intent.format_version, 1);
    assert_eq!(intent.accepted_sequence, 1);
    assert_eq!(intent.history_id, request.history_id);
    assert_eq!(intent.operation_id, request.operation_id);
    assert_eq!(f.source.begin_retirement(&request).unwrap(), intent);
    assert_eq!(f.source.retirement_intent().unwrap(), Some(intent.clone()));
    let error = f.source.enable_index_maintenance().unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("retiring"), "{error}");
    let mut changed = request.clone();
    changed.operation_id = b"another-move".to_vec();
    assert_eq!(
        f.source.begin_retirement(&changed).unwrap_err().code(),
        Code::AlreadyExists
    );
    assert_retiring_source_only_replays(&f, original);
    let node = f.node.clone();
    let source = f.source.clone();
    let error = tokio::task::spawn_blocking(move || {
        node.publish_document_projection_blocking(&source, INDEX, &candidate)
    })
    .await
    .unwrap()
    .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("retiring"), "{error}");
    let seal = seal_after_refused_mismatches(&f, &intent);

    f.close_source();
    let reopened = DocumentCatalog::open(&f.root.join("source.redb"), "books").unwrap();
    assert_eq!(reopened.retirement_intent().unwrap(), Some(intent.clone()));
    assert_eq!(reopened.begin_retirement(&request).unwrap(), intent);
    assert_eq!(reopened.history_seal().unwrap(), Some(seal));
    assert_eq!(
        DocumentCatalog::in_memory("books")
            .unwrap()
            .begin_retirement(&SourceRetirementRequest {
                history_id: vec![1; 16],
                operation_id: b"move".to_vec(),
            })
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
}

#[tokio::test]
async fn retirement_allows_pending_publication_recovery_before_matching_seal() {
    for committed in [false, true] {
        let f = Fixture::new();
        let (_, candidate) = f.stage(KEY, 1, Some(1)).await;
        let retirement = request(&f, if committed { b"commit" } else { b"abort" });
        f.bind(&candidate);
        let result = f.publish(&candidate, None, !committed);
        assert_eq!(result.is_ok(), committed);
        let intent = f.source.begin_retirement(&retirement).unwrap();
        let recovery = f
            .source
            .recover_index_publication(INDEX, &f.target)
            .unwrap();
        assert_eq!(
            matches!(&recovery, ProjectionRecovery::Committed(_)),
            committed
        );
        assert_eq!(
            matches!(&recovery, ProjectionRecovery::Aborted(_)),
            !committed
        );
        let seal = f.source.seal_history(&seal_request(&intent)).unwrap();
        assert_eq!(seal.accepted_sequence, intent.accepted_sequence);
        assert_eq!(f.source.begin_retirement(&retirement).unwrap(), intent);
    }
}

#[tokio::test]
async fn retirement_allows_pending_maintenance_recovery_before_matching_seal() {
    for phase in [1, 2] {
        let f = Fixture::new();
        let active = f.publish_through_node(KEY, 1, Some(2)).await;
        let retirement = request(
            &f,
            if phase == 1 {
                b"maint-abort"
            } else {
                b"maint-commit"
            },
        );
        let root = f.node_segments_root();
        let before = Arc::new(OpenedSegmentSet::open(&root).unwrap());
        let scratch = format!("maintenance-proof-{phase}");
        let rewrite = Rewrite {
            segment: 0,
            output: "retirement-maintenance-output",
            scratch: &scratch,
        };
        f.rewrite_segment(&active, &before, rewrite, move || {
            crate::node::INTERRUPT_MAINTENANCE.with(|point| point.set(phase));
        })
        .await
        .unwrap_err();
        let intent = f.source.begin_retirement(&retirement).unwrap();
        let catalog = SegmentCatalog::open(root).unwrap();
        let recovery = f.source.recover_index_maintenance(INDEX, &catalog).unwrap();
        assert_eq!(
            matches!(&recovery, MaintenanceRecovery::Committed(_)),
            phase == 2
        );
        assert_eq!(
            matches!(&recovery, MaintenanceRecovery::Aborted(_)),
            phase == 1
        );
        f.source.seal_history(&seal_request(&intent)).unwrap();
    }
}

#[test]
fn acceptance_racing_retirement_is_serialized_at_one_captured_watermark() {
    let f = Fixture::new();
    let history = f.history_id();
    let barrier = Arc::new(Barrier::new(3));
    let accepting = {
        let source = f.source.clone();
        let barrier = barrier.clone();
        let history = history.clone();
        std::thread::spawn(move || {
            barrier.wait();
            source.accept(&AcceptDocumentRequest {
                contract_version: 2,
                history_id: history,
                document_key: KEY.to_vec(),
                operation_id: b"racing-accept".to_vec(),
                expected_version: Some(0),
                mutation: Some(Mutation::Delete(true)),
            })
        })
    };
    let retiring = {
        let source = f.source.clone();
        let barrier = barrier.clone();
        std::thread::spawn(move || {
            barrier.wait();
            source.begin_retirement(&SourceRetirementRequest {
                history_id: history,
                operation_id: b"racing-retirement".to_vec(),
            })
        })
    };
    barrier.wait();
    let accepted = accepting.join().unwrap();
    let intent = retiring.join().unwrap().unwrap();
    match accepted {
        Ok(receipt) => {
            assert_eq!(receipt.accepted_sequence, 1);
            assert_eq!(intent.accepted_sequence, 1);
        }
        Err(error) => {
            assert_eq!(error.code(), Code::FailedPrecondition);
            assert!(error.message().contains("retiring"), "{error}");
            assert_eq!(intent.accepted_sequence, 0);
        }
    }
}

#[test]
fn reopen_refuses_incoherent_retirement_header_states() {
    for (case, format_version, retirement, seal) in [
        ("active-with-intent", 3, true, false),
        ("retiring-without-intent", 5, false, false),
        ("retiring-with-seal", 5, true, true),
        ("retired-without-intent", 6, false, true),
        ("retired-without-seal", 6, true, false),
        ("direct-seal-with-intent", 4, true, true),
        ("retired-operation-mismatch", 6, true, true),
        ("retiring-wrong-history", 5, true, false),
        ("retiring-wrong-watermark", 5, true, false),
        ("retiring-wrong-inner-format", 5, true, false),
        ("retiring-empty-operation", 5, true, false),
    ] {
        assert_reopen_refuses_header("retirement-header", case, |header, history| {
            header.format_version = format_version;
            header.retirement_intent = retirement.then(|| SourceRetirementIntent {
                format_version: 1,
                history_id: history.to_vec(),
                accepted_sequence: 0,
                operation_id: b"transfer".to_vec(),
            });
            header.history_seal = seal.then(|| SourceHistorySeal {
                format_version: 1,
                history_id: history.to_vec(),
                accepted_sequence: 0,
                operation_id: b"transfer".to_vec(),
            });
            match case {
                "retired-operation-mismatch" => {
                    header.history_seal.as_mut().unwrap().operation_id = b"other".to_vec()
                }
                "retiring-wrong-history" => {
                    header.retirement_intent.as_mut().unwrap().history_id[0] ^= 1
                }
                "retiring-wrong-watermark" => {
                    header.retirement_intent.as_mut().unwrap().accepted_sequence = 1
                }
                "retiring-wrong-inner-format" => {
                    header.retirement_intent.as_mut().unwrap().format_version = 2
                }
                "retiring-empty-operation" => header
                    .retirement_intent
                    .as_mut()
                    .unwrap()
                    .operation_id
                    .clear(),
                _ => {}
            }
        });
    }
}

#[tokio::test]
async fn accepted_operation_retries_survive_retirement_and_seal_fences() {
    let f = Fixture::new();
    f.publish_through_node(KEY, 1, Some(1)).await;
    let source = f.source.get(KEY, Some(1)).unwrap().unwrap().1.unwrap();
    let exact = first_write_retry(source);
    let expected = f.source.accept(&exact).unwrap();
    assert!(expected.replayed);
    let retirement = f
        .source
        .begin_retirement(&request(&f, b"move-retries"))
        .unwrap();
    assert_eq!(f.source.accept(&exact).unwrap(), expected);

    let mut altered = exact.clone();
    altered.mutation = Some(Mutation::Delete(true));
    assert_eq!(
        f.source.accept(&altered).unwrap_err().code(),
        Code::AlreadyExists
    );
    let new = fresh_delete(b"new-during-retirement");
    let error = f.source.accept(&new).unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("retiring"), "{error}");

    f.source.seal_history(&seal_request(&retirement)).unwrap();
    assert_eq!(f.source.accept(&exact).unwrap(), expected);
    assert_eq!(
        f.source.accept(&altered).unwrap_err().code(),
        Code::AlreadyExists
    );
    let mut after_seal = new;
    after_seal.operation_id = b"new-after-seal".to_vec();
    after_seal.document_key = b"new-after-seal".to_vec();
    let error = f.source.accept(&after_seal).unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("sealed"), "{error}");
}
