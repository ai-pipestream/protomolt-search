//! The accepted-document catalog through the byte ABI: retries,
//! conflicts, history cursors, and durable reopen.

use super::*;
use pipestream_search::pb::mobile::MobileDocumentCatalogConfig;
use pipestream_search::pb::{
    accept_document_request::Mutation, DocumentWriteReceipt, ProtobufSource,
    ReadAcceptedDocumentsResponse,
};

/// A source write of the fixture record under `key` and `operation`.
fn source_write(key: &[u8], operation: &[u8]) -> AcceptDocumentRequest {
    AcceptDocumentRequest {
        contract_version: 1,
        document_key: key.to_vec(),
        operation_id: operation.to_vec(),
        expected_version: Some(0),
        mutation: Some(Mutation::Source(ProtobufSource {
            descriptor_set: record_descriptor(),
            message_type: "private.v1.Record".into(),
            payload: vec![],
        })),
        ..Default::default()
    }
}

fn open_with_catalog(catalog: MobileDocumentCatalogConfig) -> MobileOpenRequest {
    MobileOpenRequest {
        shards: vec![in_memory_shard()],
        document_catalog: Some(catalog),
        ..Default::default()
    }
}

fn read_history(handle: u64, request: &ReadAcceptedDocumentsRequest) -> Vec<u8> {
    read_accepted_documents_bytes(handle, &request.encode_to_vec())
}

#[test]
fn mobile_document_acceptance_preserves_retry_and_conflict_codes() {
    let open = open_with_catalog(MobileDocumentCatalogConfig {
        collection: "private".into(),
        in_memory: true,
        ..Default::default()
    });
    let opened: MobileOpenResponse = payload(&open_bytes(&open.encode_to_vec(), true));
    let handle = opened.handle;
    let mut request = source_write(b"source", b"create");
    let bytes = request.encode_to_vec();
    let buffer = protomolt_search_accept_document(handle, bytes.as_ptr(), bytes.len());
    let receipt: DocumentWriteReceipt = ffi_payload(buffer);
    assert!(receipt.accepted && !receipt.durable && !receipt.searchable && !receipt.replayed);
    let retry: DocumentWriteReceipt = payload(&accept_document_bytes(handle, &bytes));
    assert!(retry.replayed);
    assert_eq!(retry.version, receipt.version);
    assert_eq!(receipt.history_id.len(), 16);
    assert_eq!(retry.history_id, receipt.history_id);
    let mut history_request = ReadAcceptedDocumentsRequest {
        limit: 10,
        max_bytes: 1024 * 1024,
        ..Default::default()
    };
    let history: ReadAcceptedDocumentsResponse = payload(&read_history(handle, &history_request));
    assert!(history.complete);
    assert_eq!(history.documents.len(), 1);
    assert_eq!(history.documents[0].document_key, request.document_key);
    history_request.max_bytes = 1;
    let code = expect_error(
        &read_history(handle, &history_request),
        "oversized source must fail",
    );
    assert_eq!(code, MobileErrorCode::ResourceExhausted);
    assert_eq!(history.history_id, receipt.history_id);
    history_request.after_sequence = 1;
    history_request.max_bytes = 1024 * 1024;
    history_request.history_id = receipt.history_id.clone();
    history_request.history_id[0] ^= 1;
    let code = expect_error(
        &read_history(handle, &history_request),
        "foreign history cursor must fail",
    );
    assert_eq!(code, MobileErrorCode::FailedPrecondition);
    request.contract_version = 2;
    request.history_id = receipt.history_id;
    request.expected_version = Some(1);
    request.operation_id = b"pinned".to_vec();
    let pinned: DocumentWriteReceipt =
        payload(&accept_document_bytes(handle, &request.encode_to_vec()));
    assert_eq!(pinned.version, 2);
    request.history_id[0] ^= 1;
    let code = expect_error(
        &accept_document_bytes(handle, &request.encode_to_vec()),
        "foreign history write must fail before retry lookup",
    );
    assert_eq!(code, MobileErrorCode::FailedPrecondition);
    request.history_id = pinned.history_id;
    request.operation_id = b"stale".to_vec();
    let code = expect_error(
        &accept_document_bytes(handle, &request.encode_to_vec()),
        "stale version must fail",
    );
    assert_eq!(code, MobileErrorCode::Aborted);
    let closed: MobileCloseResponse = payload(&close_bytes(handle));
    assert!(closed.closed);
}

#[test]
fn mobile_reopen_never_recreates_missing_durable_document_history() {
    let dir = std::env::temp_dir().join(format!(
        "mobile-catalog-reopen-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("documents.redb");
    let saved = dir.join("saved.redb");
    let request = open_with_catalog(MobileDocumentCatalogConfig {
        collection: "private".into(),
        path: path.to_str().unwrap().into(),
        in_memory: false,
    })
    .encode_to_vec();
    // Exercise the ABI entry point, including its typed error envelope.
    let response = ffi_outcome(protomolt_search_open(request.as_ptr(), request.len(), 0));
    let mobile_response::Outcome::Error(error) = response else {
        panic!("missing catalog must fail");
    };
    assert_eq!(error.code(), MobileErrorCode::NotFound);
    assert!(!path.exists());
    let opened: MobileOpenResponse = payload(&open_bytes(&request, true));
    let write = source_write(b"original-key", b"original-operation").encode_to_vec();
    let receipt: DocumentWriteReceipt = payload(&accept_document_bytes(opened.handle, &write));
    assert!(receipt.durable);
    let closed: MobileCloseResponse = payload(&close_bytes(opened.handle));
    assert!(closed.closed);
    std::fs::rename(&path, &saved).unwrap();
    let code = expect_error(
        &open_bytes(&request, false),
        "missing accepted history must fail",
    );
    assert_eq!(code, MobileErrorCode::NotFound);
    assert!(!path.exists());
    std::fs::rename(&saved, &path).unwrap();
    let reopened: MobileOpenResponse = payload(&open_bytes(&request, false));
    let retry: DocumentWriteReceipt = payload(&accept_document_bytes(reopened.handle, &write));
    let replayed = DocumentWriteReceipt {
        replayed: true,
        ..receipt
    };
    assert_eq!(retry, replayed);
    let closed: MobileCloseResponse = payload(&close_bytes(reopened.handle));
    assert!(closed.closed);
    std::fs::remove_dir_all(dir).unwrap();
}
