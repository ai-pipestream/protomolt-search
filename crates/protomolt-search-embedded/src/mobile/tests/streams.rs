//! Query streams and the full persisted lifecycle through the byte ABI.

use super::*;
use pipestream_search::pb::{
    ingest_mapped_request, query_stream_response, IndexDefinition, IndexProjection,
    IngestMappedRequest, MappedBind, MappedKind, MappedRole,
};

type QuerySender = tokio::sync::mpsc::Sender<Result<QueryStreamResponse, tonic::Status>>;

/// A query stream whose sender the test holds, so a read on it blocks.
fn stalled_stream() -> (QuerySender, QueryReceiver) {
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
    (sender, receiver)
}

fn register_stream(owner: u64, receiver: QueryReceiver) -> u64 {
    let mut registry = lock_registry().unwrap();
    let stream_handle = registry.allocate().unwrap();
    let stream = OpenStream {
        owner,
        receiver: Some(receiver),
        cancel: tokio::sync::watch::channel(false).0,
    };
    registry.streams.insert(stream_handle, stream);
    stream_handle
}

/// Wait until a reader has taken the stream's receiver.
fn wait_for_pending_read(stream_handle: u64) {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    while lock_registry().unwrap().streams[&stream_handle]
        .receiver
        .is_some()
    {
        assert!(std::time::Instant::now() < deadline, "reader did not start");
        std::thread::yield_now();
    }
}

fn blocked_read_is_cancelled(close_owner: bool) {
    let (sender, receiver) = stalled_stream();
    let owner = open_in_memory(false).handle;
    let stream_handle = register_stream(owner, receiver);
    let (done_tx, done_rx) = std::sync::mpsc::channel();
    let reader = std::thread::spawn(move || {
        done_tx
            .send(query_stream_next_bytes(stream_handle))
            .unwrap();
    });
    wait_for_pending_read(stream_handle);
    let code = expect_error(
        &query_stream_next_bytes(stream_handle),
        "concurrent read unexpectedly succeeded",
    );
    assert_eq!(code, MobileErrorCode::FailedPrecondition);
    let closed: MobileCloseResponse = if close_owner {
        payload(&close_bytes(owner))
    } else {
        payload(&query_stream_close_bytes(stream_handle))
    };
    assert!(closed.closed);
    let bytes = done_rx
        .recv_timeout(std::time::Duration::from_secs(5))
        .expect("close must wake a blocked read");
    reader.join().unwrap();
    let code = expect_error(&bytes, "closed read unexpectedly succeeded");
    assert_eq!(code, MobileErrorCode::Cancelled);
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

fn projection(field: u32, kind: MappedKind, column: &str) -> IndexProjection {
    IndexProjection {
        field_numbers: vec![field],
        kind: kind as i32,
        column_name: column.into(),
        ..Default::default()
    }
}

/// The fixture record's index: id as the doc id, body as text, and a
/// 32-dimension vector.
fn fixture_definition() -> IndexDefinition {
    IndexDefinition {
        projections: vec![
            IndexProjection {
                role: MappedRole::DocId as i32,
                ..projection(1, MappedKind::Keyword, "id")
            },
            projection(2, MappedKind::Text, "body"),
            IndexProjection {
                vector_dims: 32,
                ..projection(3, MappedKind::Vector, "semantic")
            },
        ],
    }
}

/// Plan the fixture index through the C ABI and bind and ingest the one
/// fixture record under that plan.
fn plan_and_ingest(handle: u64) {
    let descriptor_set = record_descriptor();
    let definition = fixture_definition();
    let planning = PlanIndexRequest {
        descriptor_set: descriptor_set.clone(),
        message_type: "private.v1.Record".into(),
        index_definition: Some(definition.clone()),
        ..Default::default()
    }
    .encode_to_vec();
    let buffer = protomolt_search_plan_index(handle, planning.as_ptr(), planning.len());
    let planned: pipestream_search::pb::PlanIndexResponse = ffi_payload(buffer);
    let plan = planned.plan.expect("mobile fixture plan");
    assert_eq!(plan.index_definition, Some(definition.clone()));
    let bind = MappedBind {
        collection: String::new(),
        descriptor_set,
        message_type: "private.v1.Record".into(),
        expected_fingerprint: plan.fingerprint,
        body_path: "body".into(),
        analysis: Some(body_spec()),
        materialize: None,
        field_analysis: Vec::new(),
        index_definition: Some(definition),
    };
    let ingest = MobileIngestMappedBatch {
        shard: 0,
        requests: vec![
            IngestMappedRequest {
                payload: Some(ingest_mapped_request::Payload::Bind(bind)),
            },
            IngestMappedRequest {
                payload: Some(ingest_mapped_request::Payload::Document(record_document())),
            },
        ],
    };
    let ingested: pipestream_search::pb::IngestMappedResponse =
        payload(&ingest_mapped_bytes(handle, &ingest.encode_to_vec()));
    assert_eq!(ingested.added, 1);
}

/// Read a query stream to its end, counting completion frames.
fn stream_completions(handle: u64, query: QueryRequest) -> usize {
    let stream_request = QueryStreamRequest {
        collection: String::new(),
        query: Some(query),
        timeout_ms: 0,
    };
    let stream: MobileQueryStreamOpenResponse = payload(&query_stream_open_bytes(
        handle,
        &stream_request.encode_to_vec(),
    ));
    let mut completions = 0;
    loop {
        let next: MobileQueryStreamNextResponse =
            payload(&query_stream_next_bytes(stream.stream_handle));
        if next.end {
            return completions;
        }
        if matches!(
            next.response.and_then(|response| response.payload),
            Some(query_stream_response::Payload::Completion(_))
        ) {
            completions += 1;
        }
    }
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
    let request = MobileOpenRequest {
        shards: vec![MobileShardConfig {
            index_path: root.join("private.tv").to_string_lossy().into_owned(),
            facet_fields: vec!["id".into()],
            ..Default::default()
        }],
        ..Default::default()
    }
    .encode_to_vec();
    let opened: MobileOpenResponse = payload(&open_bytes(&request, true));
    assert_eq!(opened.shard_count, 1);
    assert!(opened.no_egress);
    plan_and_ingest(opened.handle);
    let flushed: MobileFlushResponse = payload(&flush_bytes(opened.handle));
    assert_eq!(flushed.shards.len(), 1);
    let query = lexical_request();
    let queried: QueryResponse = payload(&query_bytes(opened.handle, &query.encode_to_vec()));
    assert_eq!(queried.hits.len(), 1);
    assert_eq!(stream_completions(opened.handle, query), 1);
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
    let code = expect_error(
        &open_bytes(&request, true),
        "create overwrote an existing mobile generation",
    );
    assert_eq!(code, MobileErrorCode::AlreadyExists);
    std::fs::remove_dir_all(root).expect("remove mobile ABI fixture");
}
