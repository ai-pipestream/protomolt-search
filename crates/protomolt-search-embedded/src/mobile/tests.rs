//! The byte ABI's tests: shared fixtures, the small envelope checks, and
//! the document and stream suites.

mod documents;
mod streams;

use super::android;
use super::bridge::{lock_registry, runtime, OpenStream, QueryReceiver};
use super::config::node_config;
use super::ffi::*;
use super::ops::*;
use pipestream_search::analyzer::body_spec;
use pipestream_search::pb::mobile::{
    mobile_response, MobileCloseResponse, MobileErrorCode, MobileFlushResponse,
    MobileIngestMappedBatch, MobileOpenRequest, MobileOpenResponse, MobileQueryStreamNextResponse,
    MobileQueryStreamOpenResponse, MobileResponse, MobileShardConfig,
};
use pipestream_search::pb::{
    search_query, selection_query, LexicalQuery, SearchQuery, SelectionQuery,
};
use pipestream_search::pb::{
    AcceptDocumentRequest, DescribeSchemaRequest, PlanIndexRequest, QueryRequest, QueryResponse,
    QueryStreamRequest, QueryStreamResponse, ReadAcceptedDocumentsRequest,
};
use prost::Message;
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet};
use std::time::{SystemTime, UNIX_EPOCH};
use tokio_stream::wrappers::ReceiverStream;

fn outcome(bytes: &[u8]) -> mobile_response::Outcome {
    MobileResponse::decode(bytes)
        .expect("response envelope")
        .outcome
        .expect("response outcome")
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

/// The error code of a response that must fail, panicking with `what`
/// when it succeeded.
fn expect_error(bytes: &[u8], what: &str) -> MobileErrorCode {
    let mobile_response::Outcome::Error(error) = outcome(bytes) else {
        panic!("{what}");
    };
    error.code()
}

/// The outcome of a buffer returned through the C ABI, freed once.
fn ffi_outcome(buffer: MobileBuffer) -> mobile_response::Outcome {
    // SAFETY: the buffer came from the bridge and stays owned until the
    // matching free below.
    let decoded = outcome(unsafe { std::slice::from_raw_parts(buffer.data, buffer.len) });
    // SAFETY: this buffer came from the bridge and has not been freed.
    unsafe { protomolt_search_buffer_free(buffer) };
    decoded
}

/// The payload of a buffer returned through the C ABI, freed once.
fn ffi_payload<M: Message + Default>(buffer: MobileBuffer) -> M {
    // SAFETY: the buffer came from the bridge and stays owned until the
    // matching free below.
    let decoded = payload(unsafe { std::slice::from_raw_parts(buffer.data, buffer.len) });
    // SAFETY: this buffer came from the bridge and has not been freed.
    unsafe { protomolt_search_buffer_free(buffer) };
    decoded
}

fn in_memory_shard() -> MobileShardConfig {
    MobileShardConfig {
        in_memory: true,
        ..Default::default()
    }
}

/// Open one in-memory shard through the byte ABI.
fn open_in_memory(create: bool) -> MobileOpenResponse {
    let request = MobileOpenRequest {
        shards: vec![in_memory_shard()],
        ..Default::default()
    };
    payload(&open_bytes(&request.encode_to_vec(), create))
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
    let opened = open_in_memory(true);
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
    let actual: pipestream_search::pb::DescribeSchemaResponse = ffi_payload(buffer);
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

/// Every JNI entry point the Android AAR binds to exists, compiles on
/// the host, and is a distinct symbol.
#[test]
fn every_jni_entry_point_is_a_distinct_symbol() {
    let symbols = [
        android::Java_ai_pipestream_search_mobile_ProtomoltSearch_nativeOpen as *const () as usize,
        android::Java_ai_pipestream_search_mobile_ProtomoltSearch_nativeIngestMapped as *const ()
            as usize,
        android::Java_ai_pipestream_search_mobile_ProtomoltSearch_nativeAcceptDocument as *const ()
            as usize,
        android::Java_ai_pipestream_search_mobile_ProtomoltSearch_nativeDescribeSchema as *const ()
            as usize,
        android::Java_ai_pipestream_search_mobile_ProtomoltSearch_nativePlanIndex as *const ()
            as usize,
        android::Java_ai_pipestream_search_mobile_ProtomoltSearch_nativeReadAcceptedDocuments
            as *const () as usize,
        android::Java_ai_pipestream_search_mobile_ProtomoltSearch_nativeQuery as *const () as usize,
        android::Java_ai_pipestream_search_mobile_ProtomoltSearch_nativeQueryStreamOpen as *const ()
            as usize,
        android::Java_ai_pipestream_search_mobile_ProtomoltSearch_nativeQueryStreamNext as *const ()
            as usize,
        android::Java_ai_pipestream_search_mobile_ProtomoltSearch_nativeQueryStreamClose
            as *const () as usize,
        android::Java_ai_pipestream_search_mobile_ProtomoltSearch_nativeFlush as *const () as usize,
        android::Java_ai_pipestream_search_mobile_ProtomoltSearch_nativeClose as *const () as usize,
    ];
    assert_eq!(symbols.len(), 12);
    assert!(symbols.iter().all(|&address| address != 0));
    let distinct: std::collections::HashSet<usize> = symbols.iter().copied().collect();
    assert_eq!(distinct.len(), symbols.len());
}
