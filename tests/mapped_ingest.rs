//! Descriptor-mapped protobuf-native ingest : bind a derived plan by fingerprint, stream serialized
//! protobuf documents, and land them on the ordinary column planes and
//! both legs — no JSON, no intermediate document model, ids in lockstep
//! across BM25 and the vector index.
//!
//! Descriptor sets are built with prost-types (hint-free inference is
//! enough here); DOCUMENTS are hand-encoded at the wire level, because
//! that is exactly what the extractor consumes in production.

mod common;
use pipestream_search::pb::ProtobufSource;

use common::mock::start_mock_analysis;
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::harness::{
    fit_calibration, start_empty_node, start_opened_node, unit_vectors,
};
use pipestream_search::mapping::{derive_plan, MappingError};
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_client::SearchServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    ingest_mapped_request, projected_value, routed_ingest_mapped_request, AddDocumentsRequest,
    Bm25SearchRequest, FlushRequest, FreezeTopologyWritesRequest, HealthRequest,
    IngestMappedRequest, IngestMappedResponse, MappedBind, MaterializeKind, MaterializeSpec,
    MaterializedColumn, NamedProjection, PublishTopologyRequest, PublishedTopologyShard,
    RoutedIngestMappedRequest, RoutedMappedBind, RoutedMappedDocument, SearchRequest,
};
use prost::Message;
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{
    DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
    FileDescriptorProto, FileDescriptorSet,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

const DIM: usize = 8;
const BIT_WIDTH: usize = 4;

// The corpus type: law.v1.Case

fn scalar(name: &str, number: i32, typ: Type, label: Label) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.to_string()),
        number: Some(number),
        label: Some(label as i32),
        r#type: Some(typ as i32),
        ..Default::default()
    }
}

fn message_field(name: &str, number: i32, type_name: &str) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.to_string()),
        number: Some(number),
        label: Some(Label::Optional as i32),
        r#type: Some(Type::Message as i32),
        type_name: Some(type_name.to_string()),
        ..Default::default()
    }
}

fn timestamp_file() -> FileDescriptorProto {
    FileDescriptorProto {
        name: Some("google/protobuf/timestamp.proto".to_string()),
        package: Some("google.protobuf".to_string()),
        message_type: vec![DescriptorProto {
            name: Some("Timestamp".to_string()),
            field: vec![
                scalar("seconds", 1, Type::Int64, Label::Optional),
                scalar("nanos", 2, Type::Int32, Label::Optional),
            ],
            ..Default::default()
        }],
        ..Default::default()
    }
}

use pipestream_search::config::load_shard_map;
use pipestream_search::replication::{
    atomic_live_cutover, catch_up_children_once, initialize_live_reshard, LiveChild,
    LiveReshardState,
};
use pipestream_search::reshard::{
    resolve_gen, split_stable_logs_with, ChildImage, SplitRequest, StableReshardOutput,
};

#[path = "mapped_ingest/fixtures.rs"]
mod fixtures;
pub(crate) use fixtures::*;
#[path = "mapped_ingest/chunked_opinions.rs"]
mod chunked_opinions;
#[path = "mapped_ingest/columns_restart_and_timestamps.rs"]
mod columns_restart_and_timestamps;
#[path = "mapped_ingest/live_and_replay_reshard.rs"]
mod live_and_replay_reshard;
#[path = "mapped_ingest/planes_and_refusals.rs"]
mod planes_and_refusals;
#[path = "mapped_ingest/replication_and_routing.rs"]
mod replication_and_routing;
#[path = "mapped_ingest/vector_bindings_and_compaction.rs"]
mod vector_bindings_and_compaction;
#[path = "mapped_ingest/wire_boundaries_and_utf8.rs"]
mod wire_boundaries_and_utf8;
