use super::*;
use pipestream_search::analyzer::body_spec;
use pipestream_search::pb::{
    ingest_mapped_request, query_stream_response, search_query, selection_query,
    IngestMappedRequest, LexicalQuery, MappedBind, SearchQuery, SelectionQuery,
};
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet};
use std::time::{SystemTime, UNIX_EPOCH};

fn outcome(bytes: &[u8]) -> mobile_response::Outcome {
    MobileResponse::decode(bytes)
        .expect("response envelope")
        .outcome
        .expect("response outcome")
}

#[test]
fn mobile_unsigned_column_declarations_survive_the_wire_bridge() {
    let request = MobileShardConfig {
        in_memory: true,
        integer_fields: vec!["signed".into()],
        unsigned_integer_fields: vec!["counter".into()],
        ..Default::default()
    };
    let decoded = MobileShardConfig::decode(request.encode_to_vec().as_slice()).unwrap();
    let configured = node_config(decoded).unwrap();
    assert_eq!(configured.node.integer_fields, ["signed"]);
    assert_eq!(configured.node.unsigned_integer_fields, ["counter"]);
    let old = MobileShardConfig::decode([0x10u8, 0x01].as_slice()).unwrap();
    assert!(node_config(old)
        .unwrap()
        .node
        .unsigned_integer_fields
        .is_empty());
}

#[test]
fn mobile_schema_description_needs_no_index_rows_or_source_catalog() {
    let opened: MobileOpenResponse = payload(&open_bytes(
        &MobileOpenRequest {
            shards: vec![MobileShardConfig {
                in_memory: true,
                ..Default::default()
            }],
            ..Default::default()
        }
        .encode_to_vec(),
        true,
    ));
    assert!(opened.no_egress);
    let request = DescribeSchemaRequest {
        descriptor_set: include_bytes!("../../../../tests/fixtures/schema-report/source-only.bin")
            .to_vec(),
        message_type: "source_report.AllShapes".into(),
        ..Default::default()
    };
    let expected =
        pipestream_search::mapping::describe_schema(&request.descriptor_set, &request.message_type)
            .unwrap();
    let bytes = request.encode_to_vec();
    let buffer = protomolt_search_describe_schema(opened.handle, bytes.as_ptr(), bytes.len());
    let actual: pipestream_search::pb::DescribeSchemaResponse =
        payload(unsafe { std::slice::from_raw_parts(buffer.data, buffer.len) });
    unsafe { protomolt_search_buffer_free(buffer) };
    assert_eq!(actual, expected);
    let _: MobileCloseResponse = payload(&close_bytes(opened.handle));
    let mobile_response::Outcome::Error(error) =
        outcome(&describe_schema_bytes(opened.handle, &bytes))
    else {
        panic!("closed schema handle remained accessible");
    };
    assert_eq!(error.code, MobileErrorCode::NotFound as i32);
}

#[test]
fn mobile_document_acceptance_preserves_retry_and_conflict_codes() {
    use pipestream_search::pb::mobile::MobileDocumentCatalogConfig;
    use pipestream_search::pb::{
        accept_document_request::Mutation, DocumentWriteReceipt, ProtobufSource,
    };
    let opened: MobileOpenResponse = payload(&open_bytes(
        &MobileOpenRequest {
            shards: vec![MobileShardConfig {
                in_memory: true,
                ..Default::default()
            }],
            document_catalog: Some(MobileDocumentCatalogConfig {
                collection: "private".into(),
                in_memory: true,
                ..Default::default()
            }),
            ..Default::default()
        }
        .encode_to_vec(),
        true,
    ));
    let mut request = AcceptDocumentRequest {
        contract_version: 1,
        document_key: b"source".to_vec(),
        operation_id: b"create".to_vec(),
        expected_version: Some(0),
        mutation: Some(Mutation::Source(ProtobufSource {
            descriptor_set: record_descriptor(),
            message_type: "private.v1.Record".into(),
            payload: vec![],
        })),
        ..Default::default()
    };
    let bytes = request.encode_to_vec();
    let buffer = protomolt_search_accept_document(opened.handle, bytes.as_ptr(), bytes.len());
    let receipt: DocumentWriteReceipt =
        payload(unsafe { std::slice::from_raw_parts(buffer.data, buffer.len) });
    // The buffer was returned by the C ABI above and is freed exactly once.
    unsafe { protomolt_search_buffer_free(buffer) };
    assert!(receipt.accepted && !receipt.durable && !receipt.searchable && !receipt.replayed);
    let retry: DocumentWriteReceipt = payload(&accept_document_bytes(opened.handle, &bytes));
    assert!(retry.replayed);
    assert_eq!(retry.version, receipt.version);
    assert_eq!(receipt.history_id.len(), 16);
    assert_eq!(retry.history_id, receipt.history_id);
    let mut history_request = ReadAcceptedDocumentsRequest {
        limit: 10,
        max_bytes: 1024 * 1024,
        ..Default::default()
    };
    let history: pipestream_search::pb::ReadAcceptedDocumentsResponse = payload(
        &read_accepted_documents_bytes(opened.handle, &history_request.encode_to_vec()),
    );
    assert!(history.complete);
    assert_eq!(history.documents.len(), 1);
    assert_eq!(history.documents[0].document_key, request.document_key);
    history_request.max_bytes = 1;
    let mobile_response::Outcome::Error(error) = outcome(&read_accepted_documents_bytes(
        opened.handle,
        &history_request.encode_to_vec(),
    )) else {
        panic!("oversized source must fail");
    };
    assert_eq!(error.code(), MobileErrorCode::ResourceExhausted);
    assert_eq!(history.history_id, receipt.history_id);
    history_request.after_sequence = 1;
    history_request.max_bytes = 1024 * 1024;
    history_request.history_id = receipt.history_id.clone();
    history_request.history_id[0] ^= 1;
    let mobile_response::Outcome::Error(error) = outcome(&read_accepted_documents_bytes(
        opened.handle,
        &history_request.encode_to_vec(),
    )) else {
        panic!("foreign history cursor must fail");
    };
    assert_eq!(error.code(), MobileErrorCode::FailedPrecondition);
    request.contract_version = 2;
    request.history_id = receipt.history_id;
    request.expected_version = Some(1);
    request.operation_id = b"pinned".to_vec();
    let pinned: DocumentWriteReceipt = payload(&accept_document_bytes(
        opened.handle,
        &request.encode_to_vec(),
    ));
    assert_eq!(pinned.version, 2);
    request.history_id[0] ^= 1;
    let mobile_response::Outcome::Error(error) = outcome(&accept_document_bytes(
        opened.handle,
        &request.encode_to_vec(),
    )) else {
        panic!("foreign history write must fail before retry lookup");
    };
    assert_eq!(error.code(), MobileErrorCode::FailedPrecondition);
    request.history_id = pinned.history_id;
    request.operation_id = b"stale".to_vec();
    let mobile_response::Outcome::Error(error) = outcome(&accept_document_bytes(
        opened.handle,
        &request.encode_to_vec(),
    )) else {
        panic!("stale version must fail");
    };
    assert_eq!(error.code(), MobileErrorCode::Aborted);
    let closed: MobileCloseResponse = payload(&close_bytes(opened.handle));
    assert!(closed.closed);
}

#[test]
fn mobile_reopen_never_recreates_missing_durable_document_history() {
    use pipestream_search::pb::mobile::MobileDocumentCatalogConfig;
    use pipestream_search::pb::{
        accept_document_request::Mutation, DocumentWriteReceipt, ProtobufSource,
    };
    let dir = std::env::temp_dir().join(format!(
        "mobile-catalog-reopen-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("documents.redb");
    let saved = dir.join("saved.redb");
    let request = MobileOpenRequest {
        shards: vec![MobileShardConfig {
            in_memory: true,
            ..Default::default()
        }],
        document_catalog: Some(MobileDocumentCatalogConfig {
            collection: "private".into(),
            path: path.to_str().unwrap().into(),
            in_memory: false,
        }),
        ..Default::default()
    }
    .encode_to_vec();
    // Exercise the ABI entry point, including its typed error envelope.
    let buffer = protomolt_search_open(request.as_ptr(), request.len(), 0);
    let response = outcome(unsafe { std::slice::from_raw_parts(buffer.data, buffer.len) });
    unsafe {
        protomolt_search_buffer_free(buffer);
    }
    let mobile_response::Outcome::Error(error) = response else {
        panic!("missing catalog must fail");
    };
    assert_eq!(error.code(), MobileErrorCode::NotFound);
    assert!(!path.exists());
    let opened: MobileOpenResponse = payload(&open_bytes(&request, true));
    let write = AcceptDocumentRequest {
        contract_version: 1,
        document_key: b"original-key".to_vec(),
        operation_id: b"original-operation".to_vec(),
        expected_version: Some(0),
        mutation: Some(Mutation::Source(ProtobufSource {
            descriptor_set: record_descriptor(),
            message_type: "private.v1.Record".into(),
            payload: vec![],
        })),
        ..Default::default()
    }
    .encode_to_vec();
    let receipt: DocumentWriteReceipt = payload(&accept_document_bytes(opened.handle, &write));
    assert!(receipt.durable);
    let closed: MobileCloseResponse = payload(&close_bytes(opened.handle));
    assert!(closed.closed);
    std::fs::rename(&path, &saved).unwrap();
    let mobile_response::Outcome::Error(error) = outcome(&open_bytes(&request, false)) else {
        panic!("missing accepted history must fail");
    };
    assert_eq!(error.code(), MobileErrorCode::NotFound);
    assert!(!path.exists());
    std::fs::rename(&saved, &path).unwrap();
    let reopened: MobileOpenResponse = payload(&open_bytes(&request, false));
    let retry: DocumentWriteReceipt = payload(&accept_document_bytes(reopened.handle, &write));
    assert_eq!(
        retry,
        DocumentWriteReceipt {
            replayed: true,
            ..receipt
        }
    );
    let closed: MobileCloseResponse = payload(&close_bytes(reopened.handle));
    assert!(closed.closed);
    std::fs::remove_dir_all(dir).unwrap();
}

fn payload<M: Message + Default>(bytes: &[u8]) -> M {
    let payload = match outcome(bytes) {
        mobile_response::Outcome::Payload(payload) => payload,
        mobile_response::Outcome::Error(error) => {
            panic!(
                "mobile operation failed: {:?}: {}",
                error.code(),
                error.message
            )
        }
    };
    M::decode(payload.as_slice()).expect("operation payload")
}

fn lexical_request() -> QueryRequest {
    QueryRequest {
        request_id: "mobile-abi".into(),
        k: 10,
        selection_k: 10,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Search(SearchQuery {
                id: "lexical".into(),
                query: Some(search_query::Query::Lexical(LexicalQuery {
                    text: "zebra".into(),
                    analysis: Some(body_spec()),
                    ..Default::default()
                })),
            })),
        }),
        ..Default::default()
    }
}

fn scalar(name: &str, number: i32, typ: Type, label: Label) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.into()),
        number: Some(number),
        label: Some(label as i32),
        r#type: Some(typ as i32),
        ..Default::default()
    }
}

fn record_descriptor() -> Vec<u8> {
    FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("private.proto".into()),
            package: Some("private.v1".into()),
            message_type: vec![DescriptorProto {
                name: Some("Record".into()),
                field: vec![
                    scalar("id", 1, Type::String, Label::Optional),
                    scalar("body", 2, Type::String, Label::Optional),
                    scalar("embedding", 3, Type::Float, Label::Repeated),
                ],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

fn varint(out: &mut Vec<u8>, mut value: u64) {
    loop {
        let byte = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn string_field(out: &mut Vec<u8>, field: u64, value: &str) {
    varint(out, field << 3 | 2);
    varint(out, value.len() as u64);
    out.extend_from_slice(value.as_bytes());
}

fn record_document() -> Vec<u8> {
    let mut out = Vec::new();
    string_field(&mut out, 1, "mobile-1");
    string_field(&mut out, 2, "private mobile zebra");
    varint(&mut out, 3 << 3 | 2);
    let embedding = [0.125_f32; 32];
    varint(&mut out, (embedding.len() * 4) as u64);
    for value in embedding {
        out.extend_from_slice(&value.to_le_bytes());
    }
    out
}

fn tree_bytes(path: &std::path::Path) -> u64 {
    let Ok(metadata) = std::fs::metadata(path) else {
        return 0;
    };
    if metadata.is_file() {
        return metadata.len();
    }
    std::fs::read_dir(path)
        .expect("read test index tree")
        .map(|entry| tree_bytes(&entry.expect("test index entry").path()))
        .sum()
}

fn blocked_read_is_cancelled(close_owner: bool) {
    let (sender, receiver) = tokio::sync::mpsc::channel(1);
    let receiver = runtime()
        .unwrap()
        .block_on(pipestream_search::metrics::timed_stream(
            pipestream_search::metrics::Route::QueryStream,
            tonic::Request::new(()),
            |_| async { Ok(tonic::Response::new(ReceiverStream::new(receiver))) },
        ))
        .unwrap()
        .into_inner();
    let opened: MobileOpenResponse = payload(&open_bytes(
        &MobileOpenRequest {
            shards: vec![MobileShardConfig {
                in_memory: true,
                ..Default::default()
            }],
            ..Default::default()
        }
        .encode_to_vec(),
        false,
    ));
    let owner = opened.handle;
    let stream_handle = {
        let mut registry = lock_registry().unwrap();
        let stream_handle = registry.allocate().unwrap();
        registry.streams.insert(
            stream_handle,
            OpenStream {
                owner,
                receiver: Some(receiver),
                cancel: tokio::sync::watch::channel(false).0,
            },
        );
        stream_handle
    };
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        done_tx
            .send(query_stream_next_bytes(stream_handle))
            .unwrap();
    });
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        if lock_registry().unwrap().streams[&stream_handle]
            .receiver
            .is_none()
        {
            break;
        }
        assert!(std::time::Instant::now() < deadline, "reader did not start");
        std::thread::yield_now();
    }
    let mobile_response::Outcome::Error(error) = outcome(&query_stream_next_bytes(stream_handle))
    else {
        panic!("concurrent read unexpectedly succeeded");
    };
    assert_eq!(error.code(), MobileErrorCode::FailedPrecondition);
    if close_owner {
        let closed: MobileCloseResponse = payload(&close_bytes(owner));
        assert!(closed.closed);
    } else {
        let closed: MobileCloseResponse = payload(&query_stream_close_bytes(stream_handle));
        assert!(closed.closed);
    }
    let bytes = done_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("close must wake a blocked read");
    reader.join().unwrap();
    let mobile_response::Outcome::Error(error) = outcome(&bytes) else {
        panic!("closed read unexpectedly succeeded");
    };
    assert_eq!(error.code(), MobileErrorCode::Cancelled);
    assert!(!lock_registry()
        .unwrap()
        .streams
        .contains_key(&stream_handle));
    assert!(sender.is_closed(), "close must drop the query receiver");
    let closed: MobileCloseResponse = payload(&query_stream_close_bytes(stream_handle));
    assert!(!closed.closed);
    let owner_closed: MobileCloseResponse = payload(&close_bytes(owner));
    assert_eq!(owner_closed.closed, !close_owner);
}

#[test]
fn closing_stream_wakes_pending_read_without_restoring_handle() {
    blocked_read_is_cancelled(false);
}

#[test]
fn closing_owner_wakes_pending_read_without_restoring_handle() {
    blocked_read_is_cancelled(true);
}

#[test]
fn malformed_request_is_an_encoded_error() {
    let mobile_response::Outcome::Error(error) = outcome(&open_bytes(&[0xff], false)) else {
        panic!("malformed input unexpectedly succeeded");
    };
    assert_eq!(error.code(), MobileErrorCode::InvalidArgument);
    assert!(error.message.contains("decode MobileOpenRequest"));
}

#[test]
fn empty_open_request_is_refused() {
    let bytes = MobileOpenRequest::default().encode_to_vec();
    let mobile_response::Outcome::Error(error) = outcome(&open_bytes(&bytes, false)) else {
        panic!("empty open unexpectedly succeeded");
    };
    assert_eq!(error.code(), MobileErrorCode::InvalidArgument);
    assert!(error.message.contains("at least one shard"));
}

#[test]
fn unknown_handle_is_not_found() {
    let request = QueryRequest::default().encode_to_vec();
    let mobile_response::Outcome::Error(error) = outcome(&query_bytes(u64::MAX, &request)) else {
        panic!("unknown handle unexpectedly succeeded");
    };
    assert_eq!(error.code(), MobileErrorCode::NotFound);
}

#[test]
fn c_buffer_round_trip() {
    let expected = open_bytes(&[0xff], false);
    let buffer = protomolt_search_open([0xff].as_ptr(), 1, 0);
    // SAFETY: the returned buffer remains owned until the matching free.
    let actual = unsafe { std::slice::from_raw_parts(buffer.data, buffer.len) };
    assert_eq!(actual, expected);
    // SAFETY: this buffer came from the bridge and has not been freed.
    unsafe { protomolt_search_buffer_free(buffer) };
}

#[test]
fn lifecycle_stream_persistence_and_no_egress_cross_the_byte_abi() {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("clock")
        .as_nanos();
    let root = std::env::temp_dir().join(format!(
        "protomolt-mobile-abi-{}-{nonce}",
        std::process::id()
    ));
    let index = root.join("private.tv");
    let request = MobileOpenRequest {
        shards: vec![MobileShardConfig {
            index_path: index.to_string_lossy().into_owned(),
            facet_fields: vec!["id".into()],
            ..Default::default()
        }],
        ..Default::default()
    }
    .encode_to_vec();

    let opened: MobileOpenResponse = payload(&open_bytes(&request, true));
    assert_eq!(opened.shard_count, 1);
    assert!(opened.no_egress);

    let descriptor_set = record_descriptor();
    let definition = pipestream_search::pb::IndexDefinition {
        projections: vec![
            pipestream_search::pb::IndexProjection {
                field_numbers: vec![1],
                kind: pipestream_search::pb::MappedKind::Keyword as i32,
                column_name: "id".into(),
                role: pipestream_search::pb::MappedRole::DocId as i32,
                ..Default::default()
            },
            pipestream_search::pb::IndexProjection {
                field_numbers: vec![2],
                kind: pipestream_search::pb::MappedKind::Text as i32,
                column_name: "body".into(),
                ..Default::default()
            },
            pipestream_search::pb::IndexProjection {
                field_numbers: vec![3],
                kind: pipestream_search::pb::MappedKind::Vector as i32,
                column_name: "semantic".into(),
                vector_dims: 32,
                ..Default::default()
            },
        ],
    };
    let planning = PlanIndexRequest {
        descriptor_set: descriptor_set.clone(),
        message_type: "private.v1.Record".into(),
        index_definition: Some(definition.clone()),
        ..Default::default()
    }
    .encode_to_vec();
    let buffer = protomolt_search_plan_index(opened.handle, planning.as_ptr(), planning.len());
    let planned: pipestream_search::pb::PlanIndexResponse =
        payload(unsafe { std::slice::from_raw_parts(buffer.data, buffer.len) });
    unsafe { protomolt_search_buffer_free(buffer) };
    let plan = planned.plan.expect("mobile fixture plan");
    assert_eq!(plan.index_definition, Some(definition.clone()));
    let ingest = MobileIngestMappedBatch {
        shard: 0,
        requests: vec![
            IngestMappedRequest {
                payload: Some(ingest_mapped_request::Payload::Bind(MappedBind {
                    collection: String::new(),
                    descriptor_set,
                    message_type: "private.v1.Record".into(),
                    expected_fingerprint: plan.fingerprint,
                    body_path: "body".into(),
                    analysis: Some(body_spec()),
                    materialize: None,
                    field_analysis: Vec::new(),
                    index_definition: Some(definition),
                })),
            },
            IngestMappedRequest {
                payload: Some(ingest_mapped_request::Payload::Document(record_document())),
            },
        ],
    };
    let ingested: pipestream_search::pb::IngestMappedResponse =
        payload(&ingest_mapped_bytes(opened.handle, &ingest.encode_to_vec()));
    assert_eq!(ingested.added, 1);

    let flushed: MobileFlushResponse = payload(&flush_bytes(opened.handle));
    assert_eq!(flushed.shards.len(), 1);

    let query = lexical_request();
    let queried: QueryResponse = payload(&query_bytes(opened.handle, &query.encode_to_vec()));
    assert_eq!(queried.hits.len(), 1);

    let stream_request = QueryStreamRequest {
        collection: String::new(),
        query: Some(query),
        timeout_ms: 0,
    };
    let stream: MobileQueryStreamOpenResponse = payload(&query_stream_open_bytes(
        opened.handle,
        &stream_request.encode_to_vec(),
    ));
    let mut completions = 0;
    loop {
        let next: MobileQueryStreamNextResponse =
            payload(&query_stream_next_bytes(stream.stream_handle));
        if next.end {
            break;
        }
        if matches!(
            next.response.and_then(|response| response.payload),
            Some(query_stream_response::Payload::Completion(_))
        ) {
            completions += 1;
        }
    }
    assert_eq!(completions, 1);

    let flushed: MobileFlushResponse = payload(&flush_bytes(opened.handle));
    assert_eq!(flushed.shards.len(), 1);
    let closed: MobileCloseResponse = payload(&close_bytes(opened.handle));
    assert!(closed.closed);

    let disk_bytes = tree_bytes(&root);
    assert!(
        disk_bytes > 0,
        "persistent open must create durable WAL state"
    );
    assert!(
        disk_bytes < 1024 * 1024,
        "empty mobile generation exceeded the 1 MiB lifecycle-test budget: {disk_bytes}"
    );

    let reopened: MobileOpenResponse = payload(&open_bytes(&request, false));
    assert!(reopened.no_egress);
    let closed: MobileCloseResponse = payload(&close_bytes(reopened.handle));
    assert!(closed.closed);

    let mobile_response::Outcome::Error(error) = outcome(&open_bytes(&request, true)) else {
        panic!("create overwrote an existing mobile generation");
    };
    assert_eq!(error.code(), MobileErrorCode::AlreadyExists);
    std::fs::remove_dir_all(root).expect("remove mobile ABI fixture");
}
