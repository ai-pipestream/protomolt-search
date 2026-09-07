//! Descriptor-mapped protobuf-native ingest (`docs/descriptor-mappings.md`
//! increment 2): bind a derived plan by fingerprint, stream serialized
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
use pipestream_search::harness::{fit_calibration, start_empty_node, unit_vectors};
use pipestream_search::mapping::derive_plan;
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_client::SearchServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    ingest_mapped_request, projected_value, routed_ingest_mapped_request, AddDocumentsRequest,
    Bm25SearchRequest, FreezeTopologyWritesRequest, HealthRequest, IngestMappedRequest,
    IngestMappedResponse, MappedBind, MaterializeKind, MaterializeSpec, MaterializedColumn,
    NamedProjection, PublishTopologyRequest, PublishedTopologyShard, RoutedIngestMappedRequest,
    RoutedMappedBind, RoutedMappedDocument, SearchRequest, SetCalibrationRequest,
};
use prost::Message as _;
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

// ---------------------------------------------------------------------
// The corpus type: law.v1.Case
// ---------------------------------------------------------------------

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

/// law.v1.Case:
///   id=1 string (KEYWORD, the doc id, facet "id")
///   title=2 string (TEXT, bound as the body)
///   price=3 double (f64 "price")
///   embedding=4 repeated float (the vector)
///   status=5 enum law.v1.Status (facet "status", value NAMES)
///   created_at=6 Timestamp (DATE, i64 "created_at" epoch micros)
///   meta=7 law.v1.Meta { author=1 string TEXT "meta_author",
///                        page_count=2 int32 i64 "meta_page_count" }
///   year=8 int64 (i64 "year")
///   tags=9 repeated string (FAMILY_NONE, visibly skipped)
///   published=10 bool (facet "published", "true"/"false")
fn case_set() -> Vec<u8> {
    let meta = DescriptorProto {
        name: Some("Meta".to_string()),
        field: vec![
            scalar("author", 1, Type::String, Label::Optional),
            scalar("page_count", 2, Type::Int32, Label::Optional),
        ],
        ..Default::default()
    };
    let status = EnumDescriptorProto {
        name: Some("Status".to_string()),
        value: [("STATUS_UNSPECIFIED", 0), ("OPEN", 1), ("CLOSED", 2)]
            .iter()
            .map(|(name, number)| EnumValueDescriptorProto {
                name: Some((*name).to_string()),
                number: Some(*number),
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    };
    let case = DescriptorProto {
        name: Some("Case".to_string()),
        field: vec![
            scalar("id", 1, Type::String, Label::Optional),
            scalar("title", 2, Type::String, Label::Optional),
            scalar("price", 3, Type::Double, Label::Optional),
            scalar("embedding", 4, Type::Float, Label::Repeated),
            FieldDescriptorProto {
                name: Some("status".to_string()),
                number: Some(5),
                label: Some(Label::Optional as i32),
                r#type: Some(Type::Enum as i32),
                type_name: Some(".law.v1.Status".to_string()),
                ..Default::default()
            },
            message_field("created_at", 6, ".google.protobuf.Timestamp"),
            message_field("meta", 7, ".law.v1.Meta"),
            scalar("year", 8, Type::Int64, Label::Optional),
            scalar("tags", 9, Type::String, Label::Repeated),
            scalar("published", 10, Type::Bool, Label::Optional),
        ],
        ..Default::default()
    };
    FileDescriptorSet {
        file: vec![
            timestamp_file(),
            FileDescriptorProto {
                name: Some("case.proto".to_string()),
                package: Some("law.v1".to_string()),
                message_type: vec![case, meta],
                enum_type: vec![status],
                dependency: vec!["google/protobuf/timestamp.proto".to_string()],
                ..Default::default()
            },
        ],
    }
    .encode_to_vec()
}

// ---------------------------------------------------------------------
// Hand-encoded documents
// ---------------------------------------------------------------------

fn vint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
}

fn w_str(out: &mut Vec<u8>, field: u64, value: &str) {
    vint(out, field << 3 | 2);
    vint(out, value.len() as u64);
    out.extend_from_slice(value.as_bytes());
}

fn w_varint(out: &mut Vec<u8>, field: u64, value: u64) {
    vint(out, field << 3);
    vint(out, value);
}

fn w_double(out: &mut Vec<u8>, field: u64, value: f64) {
    vint(out, field << 3 | 1);
    out.extend_from_slice(&value.to_le_bytes());
}

fn w_packed_floats(out: &mut Vec<u8>, field: u64, values: &[f32]) {
    vint(out, field << 3 | 2);
    vint(out, (values.len() * 4) as u64);
    for v in values {
        out.extend_from_slice(&v.to_le_bytes());
    }
}

fn w_msg(out: &mut Vec<u8>, field: u64, body: &[u8]) {
    vint(out, field << 3 | 2);
    vint(out, body.len() as u64);
    out.extend_from_slice(body);
}

/// One Case document, absent-able per field. Everything mirrors the
/// deterministic value tables the assertions below recompute.
#[derive(Default, Clone)]
struct CaseDoc {
    id: Option<String>,
    title: Option<String>,
    price: Option<f64>,
    embedding: Vec<f32>,
    status: Option<u64>,
    created_at: Option<(i64, i32)>,
    author: Option<String>,
    page_count: Option<u64>,
    year: Option<i64>,
    published: bool,
}

impl CaseDoc {
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        if let Some(id) = &self.id {
            w_str(&mut out, 1, id);
        }
        if let Some(title) = &self.title {
            w_str(&mut out, 2, title);
        }
        if let Some(price) = self.price {
            w_double(&mut out, 3, price);
        }
        if !self.embedding.is_empty() {
            w_packed_floats(&mut out, 4, &self.embedding);
        }
        if let Some(status) = self.status {
            w_varint(&mut out, 5, status);
        }
        if let Some((seconds, nanos)) = self.created_at {
            let mut ts = Vec::new();
            w_varint(&mut ts, 1, seconds as u64);
            w_varint(&mut ts, 2, nanos as u64);
            w_msg(&mut out, 6, &ts);
        }
        if self.author.is_some() || self.page_count.is_some() {
            let mut meta = Vec::new();
            if let Some(author) = &self.author {
                w_str(&mut meta, 1, author);
            }
            if let Some(pages) = self.page_count {
                w_varint(&mut meta, 2, pages);
            }
            w_msg(&mut out, 7, &meta);
        }
        if let Some(year) = self.year {
            w_varint(&mut out, 8, year as u64);
        }
        if self.published {
            w_varint(&mut out, 10, 1);
        }
        out
    }
}

const N_DOCS: usize = 6;

fn embedding_of(i: usize) -> Vec<f32> {
    unit_vectors(N_DOCS, DIM, 42)[i * DIM..(i + 1) * DIM].to_vec()
}

fn doc(i: usize) -> CaseDoc {
    CaseDoc {
        id: Some(format!("case-{i}")),
        title: Some(format!("case number {i}")),
        price: (i != 3).then_some(1.5 * i as f64 + 0.5),
        embedding: embedding_of(i),
        status: (i != 4).then_some(if i.is_multiple_of(2) { 1 } else { 2 }),
        created_at: Some((1_600_000_000 + i as i64, 2_500)),
        author: i.is_multiple_of(2).then(|| format!("author {i}")),
        page_count: Some(10 * i as u64 + 5),
        year: (i != 2).then_some(1990 + i as i64),
        published: i.is_multiple_of(2),
    }
}

// ---------------------------------------------------------------------
// Node plumbing
// ---------------------------------------------------------------------

fn case_node_config(analysis: String) -> NodeConfig {
    NodeConfig {
        analysis_addr: Some(analysis),
        bm25_fields: vec!["body".into(), "meta_author".into()],
        facet_fields: vec!["id".into(), "status".into(), "published".into()],
        integer_fields: vec!["created_at".into(), "meta_page_count".into(), "year".into()],
        numeric_fields: vec!["price".into(), "price2".into()],
        ..Default::default()
    }
}

async fn seed_calibration(addr: &str) {
    let sample = unit_vectors(64, DIM, 7);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &sample);
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    client
        .set_calibration(SetCalibrationRequest {
            dim: DIM as u32,
            bit_width: BIT_WIDTH as u32,
            shift,
            scale,
        })
        .await
        .unwrap();
}

fn bind() -> MappedBind {
    let plan = derive_plan(&case_set(), "law.v1.Case").expect("the Case plan derives");
    MappedBind {
        collection: String::new(),
        descriptor_set: case_set(),
        message_type: "law.v1.Case".into(),
        expected_fingerprint: plan.fingerprint,
        body_path: "title".into(),
        analysis: None,
        materialize: None,
        field_analysis: Vec::new(),
        index_definition: None,
    }
}

fn explicit_bind() -> MappedBind {
    let mut binding = bind();
    binding.field_analysis = derive_plan(&case_set(), "law.v1.Case")
        .unwrap()
        .fields
        .into_iter()
        .filter(|field| field.family == pipestream_search::pb::ColumnFamily::TextField as i32)
        .map(|field| pipestream_search::pb::MappedFieldAnalysis {
            path: field.path,
            analysis: Some(pipestream_search::analyzer::body_spec()),
        })
        .collect();
    binding
}

async fn ingest(
    addr: &str,
    bind: MappedBind,
    docs: Vec<Vec<u8>>,
) -> Result<IngestMappedResponse, tonic::Status> {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(16);
    let feeder = tokio::spawn(async move {
        let _ = tx
            .send(IngestMappedRequest {
                payload: Some(ingest_mapped_request::Payload::Bind(bind)),
            })
            .await;
        for d in docs {
            if tx
                .send(IngestMappedRequest {
                    payload: Some(ingest_mapped_request::Payload::Document(d)),
                })
                .await
                .is_err()
            {
                break;
            }
        }
    });
    let out = client
        .ingest_mapped(ReceiverStream::new(rx))
        .await
        .map(|r| r.into_inner());
    let _ = feeder.await;
    out
}

fn expect_refusal(result: Result<IngestMappedResponse, tonic::Status>, needle: &str) {
    let status = result.expect_err(&format!("expected a refusal mentioning {needle:?}"));
    assert!(
        status.message().contains(needle),
        "refusal {:?} does not mention {needle:?}",
        status.message()
    );
}

#[tokio::test]
async fn clocked_wal_catches_a_replica_up_idempotently() {
    replicated_binding(false).await;
}
#[tokio::test]
async fn explicit_analysis_replication_preserves_the_binding_and_is_idempotent() {
    replicated_binding(true).await;
}
async fn replicated_binding(explicit: bool) {
    let (analysis, _mock) = start_mock_analysis().await;
    let root = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "tvmapped_replication_{explicit}_{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let (primary, primary_handle) = start_empty_node(NodeConfig {
        index_path: Some(root.join("primary.tv")),
        wal: true,
        ..case_node_config(analysis.clone())
    })
    .await;
    let (replica, replica_handle) = start_empty_node(NodeConfig {
        index_path: Some(root.join("replica.tv")),
        layout: pipestream_search::node::Layout::SingleImage,
        wal: true,
        ..case_node_config(analysis)
    })
    .await;
    seed_calibration(&primary).await;
    seed_calibration(&replica).await;

    let key = b"law.v1.Case/case-0".to_vec();
    let mut client = NodeServiceClient::connect(primary.clone()).await.unwrap();
    let response = client
        .ingest_mapped(tokio_stream::iter([
            IngestMappedRequest {
                payload: Some(ingest_mapped_request::Payload::Bind(if explicit {
                    explicit_bind()
                } else {
                    bind()
                })),
            },
            IngestMappedRequest {
                payload: Some(ingest_mapped_request::Payload::RoutedDocument(
                    RoutedMappedDocument {
                        stable_key: key.clone(),
                        document: doc(0).encode(),
                    },
                )),
            },
        ]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.added, 1);

    let first =
        pipestream_search::replication::sync_once(&pipestream_search::replication::ReplicaCursor {
            primary: primary.clone(),
            replica: replica.clone(),
            ..Default::default()
        })
        .await
        .expect("first catch-up");
    assert!(first.clock > 0);
    let replica_clock_after_first = NodeServiceClient::connect(replica.clone())
        .await
        .unwrap()
        .health(HealthRequest {})
        .await
        .unwrap()
        .into_inner()
        .wal_high_watermark;
    let second = pipestream_search::replication::sync_once(&first)
        .await
        .expect("idempotent empty catch-up");
    assert!(second.clock >= first.clock);

    if explicit {
        let mut replica_client = NodeServiceClient::connect(replica.clone()).await.unwrap();
        for extra in [false, true] {
            let error = replica_client
                .add_documents(tokio_stream::iter([
                    pipestream_search::pb::AddDocumentsRequest {
                        text: "rogue".into(),
                        analysis: extra.then(pipestream_search::analyzer::body_spec),
                        fields: if extra {
                            vec![pipestream_search::pb::DocumentField {
                                field: "meta_author".into(),
                                text: "rogue".into(),
                                analysis: None,
                            }]
                        } else {
                            Vec::new()
                        },
                        ..Default::default()
                    },
                ]))
                .await
                .unwrap_err();
            assert_eq!(error.code(), tonic::Code::FailedPrecondition);
            assert!(
                error.message().contains("explicit mapped binding"),
                "{error}"
            );
        }
    }

    let source = NodeServiceClient::connect(primary)
        .await
        .unwrap()
        .health(HealthRequest {})
        .await
        .unwrap()
        .into_inner();
    let target = NodeServiceClient::connect(replica)
        .await
        .unwrap()
        .health(HealthRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(source.num_vectors, target.num_vectors);
    assert_eq!(source.document_slots, target.document_slots);
    assert_eq!(source.scoring_fingerprint, target.scoring_fingerprint);
    assert_eq!(target.wal_high_watermark, replica_clock_after_first);
    let replica_store = pipestream_search::postings::Bm25Store::load(
        &pipestream_search::node::bm25_sidecar_path(&root.join("replica.tv")),
    )
    .unwrap();
    if explicit {
        let generation = pipestream_search::reshard::resolve_gen(&pipestream_search::wal::wal_dir(
            &root.join("primary.tv"),
        ))
        .unwrap();
        let source_binding = pipestream_search::reshard::read_generation_binding(&generation)
            .unwrap()
            .unwrap();
        assert_eq!(source_binding.analysis_sha.len(), 64);
        assert_eq!(replica_store.binding(), Some(&source_binding));
    } else {
        assert_eq!(replica_store.binding(), Some(&expected_binding()));
    }
    assert_eq!(
        replica_store.protobuf_source(0).unwrap(),
        Some((
            ProtobufSource {
                descriptor_set: case_set(),
                message_type: "law.v1.Case".into(),
                payload: doc(0).encode(),
            },
            None
        ))
    );

    primary_handle.abort();
    replica_handle.abort();
    std::fs::remove_dir_all(&root).ok();
}

#[tokio::test]
async fn routed_mapped_ingest_uses_stable_keys_and_requires_one_generation() {
    let (analysis, _mock) = start_mock_analysis().await;
    let split = u64::MAX / 2;
    let mut nodes = Vec::new();
    let mut handles = Vec::new();
    for offset in [0, 1_000] {
        let (addr, handle) = start_empty_node(NodeConfig {
            slot_offset: offset,
            ..case_node_config(analysis.clone())
        })
        .await;
        seed_calibration(&addr).await;
        nodes.push(addr);
        handles.push(handle);
    }
    let coordinator = CoordinatorServiceImpl::new(nodes.clone())
        .with_topology_generation(10)
        .with_bm25(Some(analysis), Default::default())
        .with_hot_topology(vec![Some((0, split)), Some((split + 1, u64::MAX))])
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let coordinator_addr = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(CoordinatorServiceImpl::into_server(
                coordinator.clone(),
                pipestream_search::MAX_MESSAGE_BYTES,
            ))
            .serve_with_incoming(pipestream_search::harness::nodelay_incoming(listener)),
    );

    let mut keys: [Option<Vec<u8>>; 2] = [None, None];
    for candidate in 0..10_000u64 {
        let key = format!("law.v1.Case/case-{candidate}").into_bytes();
        let shard = usize::from(pipestream_search::coordinator::stable_routing_hash(&key) > split);
        keys[shard].get_or_insert(key);
        if keys.iter().all(Option::is_some) {
            break;
        }
    }
    let mut client = SearchServiceClient::connect(coordinator_addr)
        .await
        .unwrap();
    let stream = vec![
        RoutedIngestMappedRequest {
            payload: Some(routed_ingest_mapped_request::Payload::Bind(
                RoutedMappedBind {
                    collection: String::new(),
                    required_topology_generation: 10,
                    bind: Some(bind()),
                },
            )),
        },
        RoutedIngestMappedRequest {
            payload: Some(routed_ingest_mapped_request::Payload::Document(
                RoutedMappedDocument {
                    stable_key: keys[0].clone().unwrap(),
                    document: doc(0).encode(),
                },
            )),
        },
        RoutedIngestMappedRequest {
            payload: Some(routed_ingest_mapped_request::Payload::Document(
                RoutedMappedDocument {
                    stable_key: keys[1].clone().unwrap(),
                    document: doc(1).encode(),
                },
            )),
        },
    ];
    let response = client
        .routed_ingest_mapped(tokio_stream::iter(stream))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.served_topology_generation, 10);
    assert_eq!(response.added, 2);
    assert_eq!(response.shards.len(), 2);
    assert!(response.shards.iter().all(|shard| shard.added == 1));
    for addr in &nodes {
        let health = NodeServiceClient::connect(addr.clone())
            .await
            .unwrap()
            .health(HealthRequest {})
            .await
            .unwrap()
            .into_inner();
        assert_eq!(health.num_vectors, 1);
        assert_eq!(health.document_slots, 1);
    }

    let error = client
        .routed_ingest_mapped(tokio_stream::iter([RoutedIngestMappedRequest {
            payload: Some(routed_ingest_mapped_request::Payload::Bind(
                RoutedMappedBind {
                    collection: String::new(),
                    required_topology_generation: 9,
                    bind: Some(bind()),
                },
            )),
        }]))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);

    let frozen = SearchService::freeze_topology_writes(
        &coordinator,
        Request::new(FreezeTopologyWritesRequest {
            collection: String::new(),
            required_topology_generation: 10,
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let mut waiting_client = client.clone();
    let waiting = tokio::spawn(async move {
        waiting_client
            .routed_ingest_mapped(tokio_stream::iter([RoutedIngestMappedRequest {
                payload: Some(routed_ingest_mapped_request::Payload::Bind(
                    RoutedMappedBind {
                        collection: String::new(),
                        required_topology_generation: 10,
                        bind: Some(bind()),
                    },
                )),
            }]))
            .await
    });
    tokio::time::sleep(std::time::Duration::from_millis(20)).await;
    assert!(!waiting.is_finished(), "routed write crossed the freeze");
    SearchService::publish_topology(
        &coordinator,
        Request::new(PublishTopologyRequest {
            collection: String::new(),
            placement: None,
            cutover_token: frozen.cutover_token,
            generation: 11,
            shards: vec![
                PublishedTopologyShard {
                    addr: nodes[0].clone(),
                    replica: String::new(),
                    hash_lo: 0,
                    hash_hi: split,
                    has_placement: false,
                    placement: 0,
                },
                PublishedTopologyShard {
                    addr: nodes[1].clone(),
                    replica: String::new(),
                    hash_lo: split + 1,
                    hash_hi: u64::MAX,
                    has_placement: false,
                    placement: 0,
                },
            ],
        }),
    )
    .await
    .unwrap();
    let error = waiting.await.unwrap().unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("generation 11"));

    server.abort();
    for handle in handles {
        handle.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_reshard_catches_the_tail_and_cuts_over_without_stopping_queries() {
    let (analysis, _mock) = start_mock_analysis().await;
    let root = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("tvmapped_live_reshard_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&root);
    std::fs::create_dir_all(&root).unwrap();
    let source_path = root.join("source.tv");
    let (source, source_handle) = start_empty_node(NodeConfig {
        index_path: Some(source_path.clone()),
        wal: true,
        wal_buckets: 8,
        ..case_node_config(analysis.clone())
    })
    .await;
    seed_calibration(&source).await;
    let mut source_client = NodeServiceClient::connect(source.clone()).await.unwrap();
    let routed = |start: usize, end: usize| {
        std::iter::once(IngestMappedRequest {
            payload: Some(ingest_mapped_request::Payload::Bind(bind())),
        })
        .chain((start..end).map(|i| IngestMappedRequest {
            payload: Some(ingest_mapped_request::Payload::RoutedDocument(
                RoutedMappedDocument {
                    stable_key: format!("law.v1.Case/case-{i}").into_bytes(),
                    document: doc(i).encode(),
                },
            )),
        }))
        .collect::<Vec<_>>()
    };
    source_client
        .ingest_mapped(tokio_stream::iter(routed(0, 4)))
        .await
        .unwrap();
    source_client
        .flush(pipestream_search::pb::FlushRequest {})
        .await
        .unwrap();

    let generation =
        pipestream_search::reshard::resolve_gen(&pipestream_search::wal::wal_dir(&source_path))
            .unwrap();
    let handle = tokio::runtime::Handle::current();
    let analyze_addr = analysis.clone();
    let mut analyze = move |docs: &[(
        &str,
        Option<&pipestream_search::pb::AnalysisSpec>,
        pipestream_search::analyzer::SessionLayers,
    )]| {
        tokio::task::block_in_place(|| {
            handle
                .block_on(pipestream_search::analyzer::analyze_batch_streams(
                    &analyze_addr,
                    docs,
                    1,
                ))
                .map_err(|error| error.to_string())
        })
    };
    let baseline = pipestream_search::reshard::split_stable_logs(
        &[generation],
        2,
        &root.join("children"),
        10_000,
        10_000,
        false,
        Some(&["body".to_string(), "meta_author".to_string()]),
        &mut analyze,
    )
    .unwrap();
    assert_eq!(baseline.source_cutoffs.len(), 1);

    let mut child_addrs = Vec::new();
    let mut child_handles = Vec::new();
    let mut children = Vec::new();
    for child in &baseline.images.children {
        let config = NodeConfig {
            index_path: Some(child.vector_path.clone()),
            slot_offset: child.slot_offset,
            wal: true,
            ..case_node_config(analysis.clone())
        };
        let service = pipestream_search::node::NodeServiceImpl::open(config, None, false).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}", listener.local_addr().unwrap());
        let task = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(service.into_server(pipestream_search::MAX_MESSAGE_BYTES))
                .serve_with_incoming(pipestream_search::harness::nodelay_incoming(listener)),
        );
        children.push(pipestream_search::replication::LiveChild {
            addr: addr.clone(),
            replica: None,
            hash_lo: child.hash_lo,
            hash_hi: child.hash_hi,
            slot_offset: child.slot_offset,
            base_vectors: 0,
            base_document_slots: 0,
            applied_vectors: 0,
            applied_documents: 0,
        });
        child_addrs.push(addr);
        child_handles.push(task);
    }

    let state = pipestream_search::replication::initialize_live_reshard(
        source.clone(),
        baseline.source_cutoffs[0],
        1,
        2,
        children,
    )
    .await
    .unwrap();
    source_client
        .ingest_mapped(tokio_stream::iter(routed(4, 5)))
        .await
        .unwrap();
    let state = pipestream_search::replication::catch_up_children_once(&state)
        .await
        .unwrap();
    assert!(state.source_clock > baseline.source_cutoffs[0].high_watermark);

    let coordinator = CoordinatorServiceImpl::new(vec![source.clone()])
        .with_topology_generation(1)
        .with_bm25(Some(analysis), Default::default())
        .with_hot_topology(vec![Some((0, u64::MAX))])
        .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let coordinator_addr = format!("http://{}", listener.local_addr().unwrap());
    let coordinator_handle = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(CoordinatorServiceImpl::into_server(
                coordinator,
                pipestream_search::MAX_MESSAGE_BYTES,
            ))
            .serve_with_incoming(pipestream_search::harness::nodelay_incoming(listener)),
    );
    source_client
        .ingest_mapped(tokio_stream::iter(routed(5, 6)))
        .await
        .unwrap();
    let state_path = root.join("live-state.toml");
    let map_path = root.join("shard-map.toml");
    let final_state = pipestream_search::replication::atomic_live_cutover(
        &coordinator_addr,
        &state,
        &state_path,
        &map_path,
    )
    .await
    .unwrap();
    assert!(final_state.source_clock > state.source_clock);
    assert_eq!(
        pipestream_search::config::load_shard_map(&map_path)
            .unwrap()
            .generation,
        2
    );
    let total_rows = final_state
        .children
        .iter()
        .map(|child| child.base_vectors + child.applied_vectors)
        .sum::<u64>();
    assert_eq!(total_rows, 6);

    let mut coordinator_client = SearchServiceClient::connect(coordinator_addr)
        .await
        .unwrap();
    let response = coordinator_client
        .routed_ingest_mapped(tokio_stream::iter([RoutedIngestMappedRequest {
            payload: Some(routed_ingest_mapped_request::Payload::Bind(
                RoutedMappedBind {
                    collection: String::new(),
                    required_topology_generation: 2,
                    bind: Some(bind()),
                },
            )),
        }]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(response.served_topology_generation, 2);

    coordinator_handle.abort();
    source_handle.abort();
    for handle in child_handles {
        handle.abort();
    }
    std::fs::remove_dir_all(root).ok();
}

// ---------------------------------------------------------------------
// The tests
// ---------------------------------------------------------------------

/// Every plane at once: text body, multi-field text, string / enum /
/// bool facets, i64 (plain, timestamp-sourced, and nested), f64, and
/// the vector leg at the SAME ids — through one mapped stream.
#[tokio::test]
async fn mapped_ingest_lands_on_every_plane() {
    let (analysis, _mock) = start_mock_analysis().await;
    let (addr, _node) = start_empty_node(case_node_config(analysis.clone())).await;
    seed_calibration(&addr).await;

    let response = ingest(
        &addr,
        bind(),
        (0..N_DOCS).map(|i| doc(i).encode()).collect(),
    )
    .await
    .expect("mapped ingest succeeds");
    assert_eq!(response.added, N_DOCS as u64);
    assert_eq!(response.total, N_DOCS as u64);
    assert_eq!(response.first_id, 0);
    assert_eq!(
        response.fingerprint,
        derive_plan(&case_set(), "law.v1.Case").unwrap().fingerprint
    );

    let coordinator =
        CoordinatorServiceImpl::new(vec![addr]).with_bm25(Some(analysis), Default::default());

    // Projections read back every column, absences included.
    let hits = coordinator
        .bm25_search(Request::new(Bm25SearchRequest {
            text: "case".into(),
            k: N_DOCS as u32,
            projections: [
                "price",
                "year",
                "created_at",
                "meta_page_count",
                "id",
                "status",
                "published",
            ]
            .iter()
            .map(|name| NamedProjection {
                name: (*name).to_string(),
                expression: (*name).to_string(),
            })
            .collect(),
            ..Default::default()
        }))
        .await
        .expect("projections over mapped columns")
        .into_inner()
        .hits;
    assert_eq!(hits.len(), N_DOCS);
    for hit in &hits {
        let i = hit.doc_id as usize;
        let d = doc(i);
        let values: Vec<Option<projected_value::Value>> =
            hit.projected.iter().map(|p| p.value.clone()).collect();
        use projected_value::Value::{DoubleValue, IntValue, StringValue};
        assert_eq!(values[0], d.price.map(DoubleValue), "price of doc {i}");
        assert_eq!(values[1], d.year.map(IntValue), "year of doc {i}");
        let (seconds, nanos) = d.created_at.unwrap();
        assert_eq!(
            values[2],
            Some(IntValue(seconds * 1_000_000 + i64::from(nanos) / 1_000)),
            "created_at of doc {i}"
        );
        assert_eq!(
            values[3],
            Some(IntValue(10 * i as i64 + 5)),
            "meta_page_count of doc {i}"
        );
        assert_eq!(
            values[4],
            Some(StringValue(format!("case-{i}"))),
            "id of doc {i}"
        );
        assert_eq!(
            values[5],
            d.status
                .map(|s| StringValue(if s == 1 { "OPEN" } else { "CLOSED" }.to_string())),
            "status of doc {i}"
        );
        assert_eq!(
            values[6],
            d.published.then(|| StringValue("true".to_string())),
            "published of doc {i}"
        );
    }

    // CEL selects over the mapped columns with the documented absence
    // rule: doc 2 (no year) and doc 3 (no price) never match.
    let filtered: Vec<u64> = coordinator
        .bm25_search(Request::new(Bm25SearchRequest {
            text: "case".into(),
            k: N_DOCS as u32,
            filter: "year >= 1992 && price > 0.0".into(),
            ..Default::default()
        }))
        .await
        .expect("CEL filter over mapped columns")
        .into_inner()
        .hits
        .iter()
        .map(|h| h.doc_id)
        .collect();
    let mut filtered = filtered;
    filtered.sort_unstable();
    assert_eq!(filtered, vec![4, 5], "1994 and 1995, priced");

    // The vector leg landed at the SAME ids: querying doc 2's own
    // embedding returns doc 2 first.
    let top = coordinator
        .search(Request::new(SearchRequest {
            k: 1,
            vector: embedding_of(2),
            ..Default::default()
        }))
        .await
        .expect("vector search over mapped ingest")
        .into_inner()
        .hits;
    assert_eq!(top.len(), 1);
    assert_eq!(top[0].vector_id, 2, "the vector leg shares the doc's id");
}

/// The bind stands or nothing streams: fingerprint discipline, declared
/// columns, body selection, protocol order.
#[tokio::test]
async fn bind_refusals_name_the_gap() {
    let (analysis, _mock) = start_mock_analysis().await;
    let (addr, _node) = start_empty_node(case_node_config(analysis.clone())).await;
    seed_calibration(&addr).await;

    // Fingerprint required: dry-run first, then bind what you saw.
    let mut unbound = bind();
    unbound.expected_fingerprint = String::new();
    expect_refusal(
        ingest(&addr, unbound, vec![]).await,
        "expected_fingerprint is required",
    );

    // A stale fingerprint names both sides.
    let real = derive_plan(&case_set(), "law.v1.Case").unwrap().fingerprint;
    let mut stale = bind();
    stale.expected_fingerprint = "deadbeef".into();
    expect_refusal(ingest(&addr, stale, vec![]).await, &real);

    // Two TEXT fields and no body_path: refused naming both.
    let mut ambiguous = bind();
    ambiguous.body_path = String::new();
    expect_refusal(ingest(&addr, ambiguous, vec![]).await, "meta.author");

    // body_path must be one of the plan's TEXT fields.
    let mut wrong_body = bind();
    wrong_body.body_path = "price".into();
    expect_refusal(ingest(&addr, wrong_body, vec![]).await, "TEXT fields");

    // A shard that does not declare the landing columns refuses at
    // bind, listing every gap with its flag.
    let (bare_addr, _bare) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis.clone()),
        facet_fields: vec!["id".into(), "status".into(), "published".into()],
        integer_fields: vec!["created_at".into(), "meta_page_count".into(), "year".into()],
        ..Default::default()
    })
    .await;
    expect_refusal(
        ingest(&bare_addr, bind(), vec![]).await,
        "\"price\" (--numeric-fields)",
    );

    // Protocol: the bind comes first, exactly once.
    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    let (tx, rx) = mpsc::channel(4);
    tx.send(IngestMappedRequest {
        payload: Some(ingest_mapped_request::Payload::Document(doc(0).encode())),
    })
    .await
    .unwrap();
    drop(tx);
    let status = client
        .ingest_mapped(ReceiverStream::new(rx))
        .await
        .expect_err("a document before the bind is refused");
    assert!(status.message().contains("must be a MappedBind"));

    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    let (tx, rx) = mpsc::channel(4);
    for payload in [
        ingest_mapped_request::Payload::Bind(bind()),
        ingest_mapped_request::Payload::Bind(bind()),
    ] {
        tx.send(IngestMappedRequest {
            payload: Some(payload),
        })
        .await
        .unwrap();
    }
    drop(tx);
    let status = client
        .ingest_mapped(ReceiverStream::new(rx))
        .await
        .expect_err("a second bind is refused");
    assert!(status.message().contains("bind repeats"));
}

/// Documents that cannot land refuse by position and by field; the
/// shard stays usable afterwards.
#[tokio::test]
async fn document_refusals_name_position_and_field() {
    let (analysis, _mock) = start_mock_analysis().await;
    let (addr, _node) = start_empty_node(case_node_config(analysis)).await;
    seed_calibration(&addr).await;

    let mut no_vector = doc(0);
    no_vector.embedding = Vec::new();
    expect_refusal(
        ingest(&addr, bind(), vec![no_vector.encode()]).await,
        "document 0",
    );
    expect_refusal(
        ingest(&addr, bind(), vec![no_vector.encode()]).await,
        "has no vector",
    );

    let mut no_id = doc(0);
    no_id.id = None;
    expect_refusal(
        ingest(&addr, bind(), vec![no_id.encode()]).await,
        "has no id",
    );

    let mut no_body = doc(0);
    no_body.title = None;
    expect_refusal(
        ingest(&addr, bind(), vec![no_body.encode()]).await,
        "no body text",
    );

    let mut truncated = doc(0).encode();
    truncated.truncate(truncated.len() - 3);
    expect_refusal(ingest(&addr, bind(), vec![truncated]).await, "document 0");

    let mut short = doc(1);
    short.embedding = embedding_of(1)[..4].to_vec();
    expect_refusal(
        ingest(&addr, bind(), vec![doc(0).encode(), short.encode()]).await,
        "4 floats",
    );

    // Nothing above poisoned the shard: a clean stream still lands.
    // (The dim-mismatch stream above applied its first document before
    // refusing the second, exactly like ordinary ingest.)
    let response = ingest(&addr, bind(), vec![doc(2).encode()])
        .await
        .expect("the shard ingests after refusals");
    assert_eq!(response.added, 1);
}

#[tokio::test]
async fn closed_enum_unknowns_preserve_presence_and_prior_values_in_query() {
    let (analysis, _mock) = start_mock_analysis().await;
    let (addr, _node) = start_empty_node(case_node_config(analysis.clone())).await;
    seed_calibration(&addr).await;

    let mut unknown = doc(0);
    unknown.status = Some(99);
    let mut known_then_unknown = doc(1).encode();
    known_then_unknown.extend([40, 99]);
    let mut unknown_then_known = unknown.encode();
    unknown_then_known.extend([40, 1]);
    let response = ingest(
        &addr,
        bind(),
        vec![unknown.encode(), known_then_unknown, unknown_then_known],
    )
    .await
    .unwrap();
    assert_eq!(response.added, 3);
    let coordinator =
        CoordinatorServiceImpl::new(vec![addr]).with_bm25(Some(analysis), Default::default());
    let hits = coordinator
        .bm25_search(Request::new(Bm25SearchRequest {
            text: "case".into(),
            k: 3,
            projections: vec![NamedProjection {
                name: "status".into(),
                expression: "status".into(),
            }],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner()
        .hits;
    assert_eq!(hits.len(), 3);
    for hit in hits {
        let expected = match hit.doc_id {
            0 => None,
            1 => Some(projected_value::Value::StringValue("CLOSED".into())),
            2 => Some(projected_value::Value::StringValue("OPEN".into())),
            other => panic!("unexpected row {other}"),
        };
        assert_eq!(hit.projected[0].value, expected);
    }
}

/// A shard whose document leg ran ahead cannot take mapped documents:
/// the vector would land below its document. Refused by name.
#[tokio::test]
async fn lockstep_refusal_when_document_leg_is_ahead() {
    let (analysis, _mock) = start_mock_analysis().await;
    let (addr, _node) = start_empty_node(case_node_config(analysis)).await;
    seed_calibration(&addr).await;

    // Bind while empty; this test targets alignment after an established bind.
    ingest(&addr, bind(), vec![]).await.unwrap();

    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    let (tx, rx) = mpsc::channel(4);
    tx.send(AddDocumentsRequest {
        text: "a plain document with no vector".into(),
        ..Default::default()
    })
    .await
    .unwrap();
    drop(tx);
    client.add_documents(ReceiverStream::new(rx)).await.unwrap();

    expect_refusal(
        ingest(&addr, bind(), vec![doc(0).encode()]).await,
        "lockstep",
    );
}

/// CEL materialization is a bind property: derived columns compute from
/// the document's own mapped values and become ordinary columns.
#[tokio::test]
async fn materialized_columns_ride_the_bind() {
    let (analysis, _mock) = start_mock_analysis().await;
    let (addr, _node) = start_empty_node(case_node_config(analysis.clone())).await;
    seed_calibration(&addr).await;

    let mut with_spec = bind();
    with_spec.materialize = Some(MaterializeSpec {
        columns: vec![MaterializedColumn {
            name: "price2".into(),
            expression: "price * 2.0".into(),
            kind: MaterializeKind::F64 as i32,
        }],
    });
    ingest(
        &addr,
        with_spec,
        (0..N_DOCS).map(|i| doc(i).encode()).collect(),
    )
    .await
    .expect("mapped ingest with materialization");

    let coordinator =
        CoordinatorServiceImpl::new(vec![addr]).with_bm25(Some(analysis), Default::default());
    let hits = coordinator
        .bm25_search(Request::new(Bm25SearchRequest {
            text: "case".into(),
            k: N_DOCS as u32,
            filter: "price2 >= 3.0".into(),
            projections: vec![NamedProjection {
                name: "price2".into(),
                expression: "price2".into(),
            }],
            ..Default::default()
        }))
        .await
        .expect("filter and project the materialized column")
        .into_inner()
        .hits;
    let mut selected: Vec<(u64, Option<projected_value::Value>)> = hits
        .iter()
        .map(|h| (h.doc_id, h.projected[0].value.clone()))
        .collect();
    selected.sort_unstable_by_key(|(id, _)| *id);
    // price2 = 2 * (1.5 i + 0.5) >= 3.0 selects i >= 1; doc 3 has no
    // price, so no price2 — absence propagates, it never matches.
    let expected: Vec<(u64, Option<projected_value::Value>)> = [1usize, 2, 4, 5]
        .iter()
        .map(|&i| {
            (
                i as u64,
                Some(projected_value::Value::DoubleValue(
                    2.0 * (1.5 * i as f64 + 0.5),
                )),
            )
        })
        .collect();
    assert_eq!(selected, expected);
}

// ---------------------------------------------------------------------
// The durable shard-level binding
// ---------------------------------------------------------------------

/// The same schema minus the (FAMILY_NONE) `tags` field: lands on the
/// same declared columns but derives a different plan fingerprint.
fn case_set_without_tags() -> Vec<u8> {
    let mut set = FileDescriptorSet::decode(&case_set()[..]).unwrap();
    set.file[1].message_type[0]
        .field
        .retain(|f| f.name() != "tags");
    set.encode_to_vec()
}

fn expected_binding() -> pipestream_search::postings::StoredBinding {
    pipestream_search::postings::StoredBinding {
        index_contract: Vec::new(),
        plan_fingerprint: derive_plan(&case_set(), "law.v1.Case").unwrap().fingerprint,
        body_path: "title".into(),
        materialize_sha: String::new(),
        analysis_sha: String::new(),
        analysis_contract: Vec::new(),
        vector_binding: derive_plan(&case_set(), "law.v1.Case")
            .unwrap()
            .vector_binding
            .unwrap()
            .encode_to_vec(),
    }
}

/// The first bind pins the shard to its plan durably: the flushed store
/// carries the binding (the kind-13 column-table entry inside the v8
/// integrity envelope), a restarted node adopts it from the file, and a
/// bind under a different plan, a different body, or a different
/// materialize spec refuses by name. Same-plan binds keep ingesting.
#[tokio::test]
async fn binding_survives_restart_and_refuses_a_different_plan() {
    let (analysis, _mock) = start_mock_analysis().await;
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("tvmapped_bind_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let index_path = dir.join("shard.tv");

    let (addr, node) = start_empty_node(NodeConfig {
        index_path: Some(index_path.clone()),
        layout: pipestream_search::node::Layout::SingleImage,
        ..case_node_config(analysis.clone())
    })
    .await;
    seed_calibration(&addr).await;
    let response = ingest(&addr, bind(), (0..3).map(|i| doc(i).encode()).collect())
        .await
        .expect("mapped ingest succeeds");
    assert_eq!(response.added, 3);
    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    client
        .flush(pipestream_search::pb::FlushRequest {})
        .await
        .unwrap();
    node.abort();

    // The flushed file itself carries the binding.
    let bm25_path = pipestream_search::node::bm25_sidecar_path(&index_path);
    let store = pipestream_search::postings::Bm25Store::load(&bm25_path).unwrap();
    assert_eq!(store.binding(), Some(&expected_binding()));
    for row in 0..3 {
        assert_eq!(
            store.protobuf_source(row).unwrap(),
            Some((
                ProtobufSource {
                    descriptor_set: case_set(),
                    message_type: "law.v1.Case".into(),
                    payload: doc(row as usize).encode(),
                },
                None
            ))
        );
    }

    // Restart from disk, no in-memory state carried over. The wider
    // bm25_fields table (title added) is only there to let the
    // body-path probe below reach the BINDING check instead of the
    // declared-columns check.
    let mut index = pipestream_search::vector::VectorIndex::load(
        pipestream_search::vector::EMBEDDED_TURBOVEC,
        &index_path,
    )
    .unwrap();
    index.prepare().unwrap();
    let bm25 = pipestream_search::node::Bm25Shard::open(&bm25_path).unwrap();
    let exact = pipestream_search::exact_vectors::ExactVectorStore::open(
        &pipestream_search::node::exact_vector_sidecar_path(&index_path),
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = format!("http://{}", listener.local_addr().unwrap());
    let mut config = case_node_config(analysis);
    config.index_path = Some(index_path.clone());
    config.bm25_fields.push("title".into());
    let service = pipestream_search::node::NodeServiceImpl::new(Some(index), config)
        .with_bm25(Some(bm25))
        .with_exact_vectors(Some(exact))
        .unwrap();
    let _node2 = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(service.into_server(pipestream_search::MAX_MESSAGE_BYTES))
            .serve_with_incoming(pipestream_search::harness::nodelay_incoming(listener)),
    );

    // Same plan: still bound, ids continue.
    let more = ingest(&addr2, bind(), vec![doc(3).encode()])
        .await
        .expect("the same plan keeps ingesting after restart");
    assert_eq!(more.added, 1);
    assert_eq!(more.first_id, 3);

    // A different plan (same landing columns, different fingerprint).
    let variant = derive_plan(&case_set_without_tags(), "law.v1.Case").unwrap();
    assert_ne!(variant.fingerprint, expected_binding().plan_fingerprint);
    let mut other_plan = bind();
    other_plan.descriptor_set = case_set_without_tags();
    other_plan.expected_fingerprint = variant.fingerprint;
    expect_refusal(ingest(&addr2, other_plan, vec![]).await, "durably bound");

    // Same plan, different body.
    let mut other_body = bind();
    other_body.body_path = "meta.author".into();
    expect_refusal(ingest(&addr2, other_body, vec![]).await, "the body");

    // Same plan, a materialize spec the binding does not carry.
    let mut other_spec = bind();
    other_spec.materialize = Some(MaterializeSpec {
        columns: vec![MaterializedColumn {
            name: "price2".into(),
            expression: "price * 2.0".into(),
            kind: MaterializeKind::F64 as i32,
        }],
    });
    expect_refusal(ingest(&addr2, other_spec, vec![]).await, "materialize spec");

    let _ = std::fs::remove_dir_all(&dir);
}

/// Replay must not launder a binding away: the bind rides the WAL
/// (markers), and resharded children come out bound to the same plan
/// their parent was written under.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reshard_replay_carries_the_binding() {
    reshard_binding(false).await;
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn explicit_analysis_reshard_replay_keeps_the_complete_binding() {
    reshard_binding(true).await;
}
async fn reshard_binding(explicit: bool) {
    let (analysis, _mock) = start_mock_analysis().await;
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "tvmapped_reshard_{explicit}_{}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let index_path = dir.join("shard.tv");

    let (addr, _node) = start_empty_node(NodeConfig {
        index_path: Some(index_path.clone()),
        wal: true,
        wal_buckets: 8,
        ..case_node_config(analysis.clone())
    })
    .await;
    seed_calibration(&addr).await;
    ingest(
        &addr,
        if explicit { explicit_bind() } else { bind() },
        (0..4).map(|i| doc(i).encode()).collect(),
    )
    .await
    .expect("mapped ingest succeeds");
    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    client
        .flush(pipestream_search::pb::FlushRequest {})
        .await
        .unwrap();

    let handle = tokio::runtime::Handle::current();
    let analysis_addr = analysis.clone();
    let mut analyze = move |docs: &[(
        &str,
        Option<&pipestream_search::pb::AnalysisSpec>,
        pipestream_search::analyzer::SessionLayers,
    )]| {
        tokio::task::block_in_place(|| {
            handle.block_on(async {
                let mut out = Vec::with_capacity(docs.len());
                for (text, spec, layers) in docs {
                    let analyzed = if *layers != Default::default() {
                        pipestream_search::analyzer::analyze_batch(
                            &analysis_addr,
                            &[(*text, *spec, *layers)],
                        )
                        .await
                        .map(|mut batch| batch.remove(0))
                    } else {
                        pipestream_search::analyzer::analyze_document(&analysis_addr, text, *spec)
                            .await
                    };
                    out.push(analyzed.map_err(|e| e.to_string())?);
                }
                Ok(out)
            })
        })
    };
    let fields: Vec<String> = vec!["body".into(), "meta_author".into()];
    let gen =
        pipestream_search::reshard::resolve_gen(&pipestream_search::wal::wal_dir(&index_path))
            .unwrap();
    let source_binding = pipestream_search::reshard::read_generation_binding(&gen)
        .unwrap()
        .unwrap();
    assert_eq!(
        source_binding.analysis_sha.len(),
        if explicit { 64 } else { 0 }
    );
    let out_dir = dir.join("out");
    let output = pipestream_search::reshard::merge(
        &[gen],
        &out_dir,
        None,
        false,
        Some(&fields),
        &mut analyze,
    )
    .unwrap();
    assert_eq!(output.children.len(), 1);
    let child_bm25 = output.children[0]
        .bm25_path
        .as_ref()
        .expect("documents were replayed");
    let child = pipestream_search::postings::Bm25Store::load(child_bm25).unwrap();
    assert_eq!(child.doc_count(), 4);
    assert_eq!(child.binding(), Some(&source_binding));
    for row in 0..4 {
        let original = (0..4)
            .find(|i| child.text(row) == doc(*i).title.as_deref())
            .unwrap();
        assert_eq!(
            child.protobuf_source(row).unwrap(),
            Some((
                ProtobufSource {
                    descriptor_set: case_set(),
                    message_type: "law.v1.Case".into(),
                    payload: doc(original).encode(),
                },
                None
            ))
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}

// ---------------------------------------------------------------------
// Chunk-scope ingest
// ---------------------------------------------------------------------

// Descriptor wire helpers for HINT-BEARING sets (prost drops extension
// fields, so these are hand-encoded like production reads them).
fn d_field(
    name: &str,
    number: u64,
    label: prost_types::field_descriptor_proto::Label,
    typ: Type,
    type_name: Option<&str>,
    hint: Option<&pipestream_search::pb::hints::FieldIndexHint>,
) -> Vec<u8> {
    let mut f = Vec::new();
    w_str(&mut f, 1, name);
    w_varint(&mut f, 3, number);
    w_varint(&mut f, 4, label as u64);
    w_varint(&mut f, 5, typ as u64);
    if let Some(tn) = type_name {
        w_str(&mut f, 6, tn);
    }
    if let Some(hint) = hint {
        let mut options = Vec::new();
        w_msg(&mut options, 59_100_471, &hint.encode_to_vec());
        w_msg(&mut f, 8, &options);
    }
    f
}

fn d_message(name: &str, fields: &[Vec<u8>]) -> Vec<u8> {
    let mut m = Vec::new();
    w_str(&mut m, 1, name);
    for f in fields {
        w_msg(&mut m, 2, f);
    }
    m
}

fn d_set(package: &str, file: &str, messages: &[Vec<u8>]) -> Vec<u8> {
    let mut f = Vec::new();
    w_str(&mut f, 1, file);
    w_str(&mut f, 2, package);
    for m in messages {
        w_msg(&mut f, 4, m);
    }
    let mut s = Vec::new();
    w_msg(&mut s, 1, &f);
    s
}

fn role(
    role: pipestream_search::pb::hints::BlockRole,
) -> pipestream_search::pb::hints::FieldIndexHint {
    pipestream_search::pb::hints::FieldIndexHint {
        block_role: role as i32,
        ..Default::default()
    }
}

/// law2.v1.Opinion { id, case_name (parent TEXT), court_code, year,
/// chunks[] } with law2.v1.Chunk { cid (CHUNK_ID), text (the body),
/// embedding, page }.
fn opinion_set() -> Vec<u8> {
    use prost_types::field_descriptor_proto::Label;
    let chunk = d_message(
        "Chunk",
        &[
            d_field(
                "cid",
                1,
                Label::Optional,
                Type::String,
                None,
                Some(&role(pipestream_search::pb::hints::BlockRole::ChunkId)),
            ),
            d_field("text", 2, Label::Optional, Type::String, None, None),
            d_field("embedding", 3, Label::Repeated, Type::Float, None, None),
            d_field("page", 4, Label::Optional, Type::Int32, None, None),
        ],
    );
    let opinion = d_message(
        "Opinion",
        &[
            d_field("id", 1, Label::Optional, Type::String, None, None),
            d_field("case_name", 2, Label::Optional, Type::String, None, None),
            d_field("court_code", 3, Label::Optional, Type::String, None, None),
            d_field("year", 4, Label::Optional, Type::Int64, None, None),
            d_field(
                "chunks",
                5,
                Label::Repeated,
                Type::Message,
                Some(".law2.v1.Chunk"),
                Some(&role(pipestream_search::pb::hints::BlockRole::Chunks)),
            ),
        ],
    );
    d_set("law2.v1", "opinion.proto", &[opinion, chunk])
}

struct ChunkDoc {
    cid: Option<String>,
    text: Option<String>,
    embedding: Vec<f32>,
    page: Option<u64>,
}

fn opinion_doc(
    id: Option<&str>,
    case_name: &str,
    court: &str,
    year: i64,
    chunks: &[ChunkDoc],
) -> Vec<u8> {
    let mut out = Vec::new();
    if let Some(id) = id {
        w_str(&mut out, 1, id);
    }
    w_str(&mut out, 2, case_name);
    w_str(&mut out, 3, court);
    w_varint(&mut out, 4, year as u64);
    for chunk in chunks {
        let mut c = Vec::new();
        if let Some(cid) = &chunk.cid {
            w_str(&mut c, 1, cid);
        }
        if let Some(text) = &chunk.text {
            w_str(&mut c, 2, text);
        }
        if !chunk.embedding.is_empty() {
            w_packed_floats(&mut c, 3, &chunk.embedding);
        }
        if let Some(page) = chunk.page {
            w_varint(&mut c, 4, page);
        }
        w_msg(&mut out, 5, &c);
    }
    out
}

fn chunk_of(parent: usize, ordinal: usize, row: usize) -> ChunkDoc {
    ChunkDoc {
        cid: Some(format!("op-{parent}-c{ordinal}")),
        text: Some(format!("chunk alpha {parent} {ordinal}")),
        embedding: unit_vectors(16, DIM, 99)[row * DIM..(row + 1) * DIM].to_vec(),
        page: Some(10 * parent as u64 + ordinal as u64),
    }
}

fn opinion_bind() -> MappedBind {
    let plan = derive_plan(&opinion_set(), "law2.v1.Opinion").expect("the Opinion plan derives");
    MappedBind {
        collection: String::new(),
        descriptor_set: opinion_set(),
        message_type: "law2.v1.Opinion".into(),
        expected_fingerprint: plan.fingerprint,
        // Empty: the scope's only TEXT field is the body by default.
        body_path: String::new(),
        analysis: None,
        materialize: None,
        field_analysis: Vec::new(),
        index_definition: None,
    }
}

fn opinion_node_config(analysis: String) -> NodeConfig {
    NodeConfig {
        analysis_addr: Some(analysis),
        bm25_fields: vec!["body".into(), "case_name".into()],
        facet_fields: vec!["id".into(), "court_code".into(), "cid".into()],
        integer_fields: vec!["year".into(), "page".into()],
        ..Default::default()
    }
}

fn reduced(id: &str) -> u64 {
    u64::from_be_bytes(
        pipestream_search::sha256::digest(id.as_bytes())[..8]
            .try_into()
            .unwrap(),
    )
}

/// Chunk rows are the searchable rows: parent scalars and parent TEXT
/// denormalize onto every chunk, filters see parent and chunk fields
/// together with no join, the vector leg lands per chunk at the same
/// ids, and lineage carries the reduced parent id — so the engine's
/// existing parent-collapse groups mapped chunks with no new machinery.
#[tokio::test]
async fn chunked_ingest_denormalizes_and_collapses() {
    let (analysis, _mock) = start_mock_analysis().await;
    let (addr, _node) = start_empty_node(opinion_node_config(analysis.clone())).await;
    seed_calibration(&addr).await;

    // op-0: 2 chunks (rows 0, 1); op-1: 3 chunks (rows 2, 3, 4);
    // op-2: ZERO chunks — a legitimate empty document, zero rows.
    let mut row = 0;
    let mut docs = Vec::new();
    for (parent, n_chunks) in [(0usize, 2usize), (1, 3), (2, 0)] {
        let chunks: Vec<ChunkDoc> = (0..n_chunks)
            .map(|ordinal| {
                let c = chunk_of(parent, ordinal, row);
                row += 1;
                c
            })
            .collect();
        docs.push(opinion_doc(
            Some(&format!("op-{parent}")),
            &format!("case name {parent}"),
            if parent % 2 == 0 { "ca9" } else { "scotus" },
            1990 + parent as i64,
            &chunks,
        ));
    }
    let response = ingest(&addr, opinion_bind(), docs)
        .await
        .expect("chunked ingest");
    assert_eq!(response.added, 5, "rows = chunks");
    assert_eq!(
        response.parents, 3,
        "source documents, the chunkless one included"
    );
    assert_eq!(response.total, 5);

    let coordinator =
        CoordinatorServiceImpl::new(vec![addr]).with_bm25(Some(analysis), Default::default());

    // A filter mixing PARENT and CHUNK fields selects exact rows, and
    // projections read the denormalized parent id next to per-chunk
    // values.
    let hits = coordinator
        .bm25_search(Request::new(Bm25SearchRequest {
            text: "alpha".into(),
            k: 10,
            filter: "year >= 1991 && page >= 11".into(),
            projections: ["id", "cid", "year", "page"]
                .iter()
                .map(|name| NamedProjection {
                    name: (*name).to_string(),
                    expression: (*name).to_string(),
                })
                .collect(),
            ..Default::default()
        }))
        .await
        .expect("mixed parent/chunk filter")
        .into_inner()
        .hits;
    let mut selected: Vec<(u64, Vec<Option<projected_value::Value>>)> = hits
        .iter()
        .map(|h| {
            (
                h.doc_id,
                h.projected.iter().map(|p| p.value.clone()).collect(),
            )
        })
        .collect();
    selected.sort_unstable_by_key(|(id, _)| *id);
    use projected_value::Value::{IntValue, StringValue};
    assert_eq!(
        selected,
        vec![
            (
                3,
                vec![
                    Some(StringValue("op-1".into())),
                    Some(StringValue("op-1-c1".into())),
                    Some(IntValue(1991)),
                    Some(IntValue(11)),
                ]
            ),
            (
                4,
                vec![
                    Some(StringValue("op-1".into())),
                    Some(StringValue("op-1-c2".into())),
                    Some(IntValue(1991)),
                    Some(IntValue(12)),
                ]
            ),
        ]
    );

    // Parent collapse over the vector leg: one hit per parent, keyed by
    // the reduced parent id the lineage carries.
    let hits = coordinator
        .search(Request::new(SearchRequest {
            k: 5,
            vector: unit_vectors(16, DIM, 99)[..DIM].to_vec(),
            collapse_parents: true,
            ..Default::default()
        }))
        .await
        .expect("collapsed vector search")
        .into_inner()
        .hits;
    assert_eq!(hits.len(), 2, "one hit per parent that has chunks");
    assert_eq!(hits[0].vector_id, 0, "the queried chunk wins its parent");
    assert_eq!(hits[0].parent_id, reduced("op-0"));
    let parents: std::collections::HashSet<u64> = hits.iter().map(|h| h.parent_id).collect();
    assert_eq!(
        parents,
        [reduced("op-0"), reduced("op-1")].into_iter().collect()
    );
}

/// Chunk refusals name the document, the chunk ordinal, and the field.
#[tokio::test]
async fn chunked_refusals_name_document_chunk_and_field() {
    let (analysis, _mock) = start_mock_analysis().await;
    let (addr, _node) = start_empty_node(opinion_node_config(analysis)).await;
    seed_calibration(&addr).await;

    // The body must live inside the CHUNKS scope.
    let mut parent_body = opinion_bind();
    parent_body.body_path = "case_name".into();
    expect_refusal(ingest(&addr, parent_body, vec![]).await, "CHUNKS-scope");

    let good = |row: usize| chunk_of(0, row, row);
    let doc_with =
        |chunks: &[ChunkDoc]| opinion_doc(Some("op-0"), "case name 0", "ca9", 1990, chunks);

    let mut no_vector = good(1);
    no_vector.embedding = Vec::new();
    expect_refusal(
        ingest(&addr, opinion_bind(), vec![doc_with(&[good(0), no_vector])]).await,
        "document 0: chunk 1:",
    );

    let mut no_cid = good(0);
    no_cid.cid = None;
    expect_refusal(
        ingest(&addr, opinion_bind(), vec![doc_with(&[no_cid])]).await,
        "the chunk has no id",
    );

    let mut no_text = good(0);
    no_text.text = None;
    expect_refusal(
        ingest(&addr, opinion_bind(), vec![doc_with(&[no_text])]).await,
        "no body text",
    );

    let orphan = opinion_doc(None, "case name 0", "ca9", 1990, &[good(0)]);
    expect_refusal(
        ingest(&addr, opinion_bind(), vec![orphan]).await,
        "identity is required",
    );

    // Nothing above poisoned the shard.
    let response = ingest(&addr, opinion_bind(), vec![doc_with(&[good(0), good(1)])])
        .await
        .expect("the shard ingests after refusals");
    assert_eq!(response.added, 2);
    assert_eq!(response.parents, 1);
}

#[test]
fn timestamp_projection_rejects_incompatible_named_descriptors() {
    use pipestream_search::mapping::{derive_plan, describe_schema};
    for change in 0..9 {
        let mut set = FileDescriptorSet::decode(case_set().as_slice()).unwrap();
        let timestamp = &mut set.file[0].message_type[0];
        match change {
            0 => {
                timestamp.field.remove(0);
            }
            1 => timestamp.field[0].r#type = Some(Type::String as i32),
            2 => timestamp.field[0].number = Some(3),
            3 => timestamp.field[0].name = Some("renamed".into()),
            4 => timestamp.field[0].label = Some(Label::Repeated as i32),
            5 => timestamp.field[0].default_value = Some("9".into()),
            6 => timestamp.field[0].r#type = Some(Type::Sint64 as i32),
            7 => timestamp.field[1].r#type = Some(Type::Int64 as i32),
            8 => {
                timestamp
                    .oneof_decl
                    .push(prost_types::OneofDescriptorProto {
                        name: Some("parts".into()),
                        ..Default::default()
                    });
                timestamp.field[0].oneof_index = Some(0);
                timestamp.field[1].oneof_index = Some(0);
            }
            _ => unreachable!(),
        }
        let bytes = set.encode_to_vec();
        // These are valid user-defined protobuf schemas. They can be retained
        // and described, but their name must not confer Timestamp projection.
        describe_schema(&bytes, "law.v1.Case").unwrap();
        let error = derive_plan(&bytes, "law.v1.Case")
            .err()
            .expect("invalid Timestamp shape");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(error.message().contains("created_at"), "{change}: {error}");
        assert!(error.message().contains("Timestamp"), "{change}: {error}");
    }
}

#[test]
fn timestamp_projection_validates_instants_and_preserves_message_presence() {
    use pipestream_search::mapping::Extractor;
    let extractor = Extractor::new(&case_set(), "law.v1.Case", "title").unwrap();
    let base = CaseDoc {
        id: Some("instant".into()),
        title: Some("body".into()),
        embedding: vec![1.0, 0.0],
        ..Default::default()
    };
    let absent = extractor.extract(&base.encode()).unwrap();
    assert!(absent[0].request.timestamps.is_empty());
    for (seconds, nanos) in [
        (-62_135_596_800, 0),
        (253_402_300_799, 999_999_999),
        (-1, 999_999_999),
        (0, 0),
        (0, 1),
    ] {
        // Use generated Timestamp encoding, including omitted scalar defaults,
        // and compare the extracted value with the original typed instant.
        let timestamp = prost_types::Timestamp { seconds, nanos };
        let mut wire = base.encode();
        w_msg(&mut wire, 6, &timestamp.encode_to_vec());
        let rows = extractor.extract(&wire).unwrap();
        assert_eq!(rows[0].request.timestamps[0].value, Some(timestamp));
    }
    let mut merged = base.encode();
    w_msg(
        &mut merged,
        6,
        &prost_types::Timestamp {
            seconds: -1,
            nanos: 0,
        }
        .encode_to_vec(),
    );
    w_msg(
        &mut merged,
        6,
        &prost_types::Timestamp {
            seconds: 0,
            nanos: 999_999_999,
        }
        .encode_to_vec(),
    );
    let rows = extractor.extract(&merged).unwrap();
    assert_eq!(
        rows[0].request.timestamps[0].value,
        Some(prost_types::Timestamp {
            seconds: -1,
            nanos: 999_999_999
        })
    );

    for (seconds, nanos) in [
        (-62_135_596_801, 0),
        (253_402_300_800, 0),
        (i64::MIN, 0),
        (i64::MAX, 0),
        (0, -1),
        (0, 1_000_000_000),
    ] {
        let mut wire = base.encode();
        w_msg(
            &mut wire,
            6,
            &prost_types::Timestamp { seconds, nanos }.encode_to_vec(),
        );
        let error = extractor
            .extract(&wire)
            .err()
            .expect("invalid Timestamp instant");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(error.message().contains("created_at"), "{error}");
    }
}

#[tokio::test]
async fn vector_names_cannot_shadow_node_columns_outside_the_mapped_plan() {
    let (analysis, mock) = start_mock_analysis().await;
    for family in 0..8 {
        let mut config = case_node_config(analysis.clone());
        let columns = match family {
            0 => &mut config.bm25_fields,
            1 => &mut config.facet_fields,
            2 => &mut config.numeric_fields,
            3 => &mut config.integer_fields,
            4 => &mut config.unsigned_integer_fields,
            5 => &mut config.map_facet_fields,
            6 => &mut config.map_numeric_fields,
            _ => &mut config.geo_fields,
        };
        columns.push("embedding".into());
        let (address, node) = start_empty_node(config).await;
        let error = ingest(&address, bind(), vec![]).await.unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(
            error.message().contains("vector column"),
            "family {family}: {error}"
        );
        node.abort();
        let _ = node.await;
    }
    mock.abort();
    let _ = mock.await;
}

#[tokio::test]
async fn named_vector_binding_cannot_relabel_legacy_or_populated_shards() {
    let (analysis, mock) = start_mock_analysis().await;
    for legacy in [false, true] {
        let (address, node) = start_empty_node(case_node_config(analysis.clone())).await;
        let mut client = NodeServiceClient::connect(address.clone()).await.unwrap();
        if legacy {
            let expected = expected_binding();
            client
                .apply_wal_binding(pipestream_search::pb::ApplyWalBindingRequest {
                    plan_fingerprint: expected.plan_fingerprint,
                    body_path: expected.body_path,
                    ..Default::default()
                })
                .await
                .unwrap();
        } else {
            client
                .add_documents(tokio_stream::iter([AddDocumentsRequest {
                    text: "unmapped document".into(),
                    ..Default::default()
                }]))
                .await
                .unwrap();
        }
        expect_refusal(
            ingest(&address, bind(), vec![]).await,
            if legacy {
                "vector field binding"
            } else {
                "populated unbound"
            },
        );
        node.abort();
        let _ = node.await;
    }
    mock.abort();
    let _ = mock.await;
}

#[tokio::test]
async fn empty_generations_recover_and_transfer_bindings_without_source_rows() {
    use pipestream_search::{
        node::{Layout, NodeServiceImpl},
        pb::node_service_server::NodeService,
    };
    let (analysis, mock) = start_mock_analysis().await;
    let root = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("mapped-vector-empty-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    for layout in [Layout::SingleImage, Layout::Segments] {
        for wal in [false, true] {
            let path = root.join(format!("{layout:?}-{wal}.tv"));
            let config = NodeConfig {
                index_path: Some(path.clone()),
                layout,
                wal,
                ..case_node_config(analysis.clone())
            };
            let (address, node) = start_empty_node(config.clone()).await;
            ingest(&address, bind(), vec![]).await.unwrap();
            let mut client = NodeServiceClient::connect(address.clone()).await.unwrap();
            let flushed = client
                .flush(pipestream_search::pb::FlushRequest {})
                .await
                .unwrap()
                .into_inner();
            assert!(flushed.written);
            assert_eq!(flushed.num_documents, 0);
            assert_eq!(flushed.num_vectors, 0);
            let before = pipestream_search::segments::SegmentCatalog::read_manifest(
                &pipestream_search::node::segments_root(&path),
            )
            .unwrap();
            client
                .flush(pipestream_search::pb::FlushRequest {})
                .await
                .unwrap();
            assert_eq!(
                before,
                pipestream_search::segments::SegmentCatalog::read_manifest(
                    &pipestream_search::node::segments_root(&path)
                )
                .unwrap()
            );

            let target_config = NodeConfig {
                index_path: Some(root.join(format!("target-{layout:?}-{wal}.tv"))),
                wal: false,
                ..config.clone()
            };
            let (target, receiver) = start_empty_node(target_config.clone()).await;
            pipestream_search::snapshot::install_snapshot_from(
                &target,
                pipestream_search::pb::InstallSnapshotFromRequest {
                    source: Some(
                        pipestream_search::pb::install_snapshot_from_request::Source::PeerAddr(
                            address,
                        ),
                    ),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            node.abort();
            let _ = node.await;
            receiver.abort();
            let _ = receiver.await;

            // Neither source rows nor a provider image exist. The snapshot
            // receiver also has no WAL from which to recover the declaration.
            for recovered in [config, target_config.clone()] {
                let reopened = NodeServiceImpl::open(recovered, None, false).unwrap();
                let expected = expected_binding();
                let request = pipestream_search::pb::ApplyWalBindingRequest {
                    plan_fingerprint: expected.plan_fingerprint,
                    body_path: expected.body_path,
                    vector_binding: expected.vector_binding,
                    ..Default::default()
                };
                let response = reopened
                    .apply_wal_binding(Request::new(request.clone()))
                    .await
                    .unwrap()
                    .into_inner();
                assert!(response.already_bound);
                assert_eq!(response.vector_binding, request.vector_binding);
                let mut missing = request.clone();
                missing.vector_binding.clear();
                assert_eq!(
                    reopened
                        .apply_wal_binding(Request::new(missing))
                        .await
                        .unwrap_err()
                        .code(),
                    tonic::Code::FailedPrecondition
                );
                let mut invalid = request;
                invalid.vector_binding.extend([8, 1]);
                assert_eq!(
                    reopened
                        .apply_wal_binding(Request::new(invalid))
                        .await
                        .unwrap_err()
                        .code(),
                    tonic::Code::InvalidArgument
                );
            }
            // Metadata publication must not turn a segment-layout shard into
            // a single image or prevent its first real document from landing.
            let (target, receiver) = common::start_opened_node(target_config).await;
            seed_calibration(&target).await;
            let inserted = ingest(&target, bind(), vec![doc(0).encode()])
                .await
                .unwrap();
            assert_eq!(inserted.first_id, 0);
            assert_eq!(inserted.added, 1);
            let mut client = NodeServiceClient::connect(target).await.unwrap();
            client
                .flush(pipestream_search::pb::FlushRequest {})
                .await
                .unwrap();
            let health = client.health(HealthRequest {}).await.unwrap().into_inner();
            assert_eq!(health.num_vectors, 1);
            assert_eq!(health.document_slots, 1);
            receiver.abort();
            let _ = receiver.await;
        }
    }
    mock.abort();
    let _ = mock.await;
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn compaction_keeps_vector_bindings_in_images_and_rewritten_logs() {
    use pipestream_search::node::{generation_bm25, generation_dir, segments_root, Layout};
    let (analysis, mock) = start_mock_analysis().await;
    let root = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("mapped-vector-compaction-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    for layout in [Layout::SingleImage, Layout::Segments] {
        for (explicit, erase_all) in [(false, false), (true, false), (false, true), (true, true)] {
            let path = root.join(format!("{layout:?}-{explicit}-{erase_all}.tv"));
            let (address, node) = start_empty_node(NodeConfig {
                index_path: Some(path.clone()),
                layout,
                wal: true,
                ..case_node_config(analysis.clone())
            })
            .await;
            seed_calibration(&address).await;
            ingest(
                &address,
                if explicit { explicit_bind() } else { bind() },
                (0..4).map(|i| doc(i).encode()).collect(),
            )
            .await
            .unwrap();
            let mut client = NodeServiceClient::connect(address.clone()).await.unwrap();
            client
                .flush(pipestream_search::pb::FlushRequest {})
                .await
                .unwrap();
            if layout == Layout::Segments {
                // Older vector-first generations can retain this whole image
                // after rows move into segments. Compaction makes it stale.
                let set = pipestream_search::segments::OpenedSegmentSet::open(segments_root(&path))
                    .unwrap();
                let first = set.metadata(0);
                std::fs::copy(
                    pipestream_search::segments::SegmentCatalog::segment_dir(
                        &segments_root(&path),
                        &first.segment_id,
                    )
                    .join(&first.vector.file),
                    &path,
                )
                .unwrap();
            }
            let generation =
                pipestream_search::reshard::resolve_gen(&pipestream_search::wal::wal_dir(&path))
                    .unwrap();
            let expected = pipestream_search::reshard::read_generation_binding(&generation)
                .unwrap()
                .unwrap();
            assert!(!expected.vector_binding.is_empty());
            client
                .delete_documents(pipestream_search::pb::DeleteDocumentsRequest {
                    doc_ids: if erase_all { vec![0, 1, 2, 3] } else { vec![0] },
                    ..Default::default()
                })
                .await
                .unwrap();
            let backend_before = client
                .get_vector_backend(pipestream_search::pb::GetVectorBackendRequest {})
                .await
                .unwrap()
                .into_inner();
            let compacted = client
                .compact_shard(pipestream_search::pb::CompactShardRequest::default())
                .await
                .unwrap()
                .into_inner();
            assert_eq!(compacted.rows_after, if erase_all { 0 } else { 3 });
            assert_eq!(
                compacted.tombstones_reclaimed,
                if erase_all { 4 } else { 1 }
            );
            let rewritten =
                pipestream_search::reshard::resolve_gen(&pipestream_search::wal::wal_dir(&path))
                    .unwrap();
            assert_eq!(
                pipestream_search::reshard::read_generation_binding(&rewritten).unwrap(),
                Some(expected.clone())
            );
            match layout {
                Layout::SingleImage => {
                    let image = pipestream_search::postings::Bm25Reader::open(&generation_bm25(
                        &generation_dir(&path),
                    ))
                    .unwrap();
                    assert_eq!(image.binding(), Some(&expected));
                }
                Layout::Segments => {
                    let set =
                        pipestream_search::segments::OpenedSegmentSet::open(segments_root(&path))
                            .unwrap();
                    assert_eq!(set.is_empty(), erase_all);
                    assert_eq!(set.binding(), Some(&expected));
                    for part in 0..set.len() {
                        assert_eq!(set.bm25(part).binding(), Some(&expected));
                    }
                }
            }
            // Install over the real snapshot stream, then reopen without WAL:
            // only the transferred image can supply the binding on this receiver.
            let target_config = NodeConfig {
                index_path: Some(
                    root.join(format!("snapshot-{layout:?}-{explicit}-{erase_all}.tv")),
                ),
                layout,
                wal: false,
                ..case_node_config(analysis.clone())
            };
            let (target, receiver) = start_empty_node(target_config.clone()).await;
            pipestream_search::snapshot::install_snapshot_from(
                &target,
                pipestream_search::pb::InstallSnapshotFromRequest {
                    source: Some(
                        pipestream_search::pb::install_snapshot_from_request::Source::PeerAddr(
                            address.clone(),
                        ),
                    ),
                    ..Default::default()
                },
            )
            .await
            .unwrap();
            receiver.abort();
            let _ = receiver.await;
            let reopened =
                pipestream_search::node::NodeServiceImpl::open(target_config.clone(), None, false)
                    .unwrap();
            let backend_after =
                pipestream_search::pb::node_service_server::NodeService::get_vector_backend(
                    &reopened,
                    Request::new(pipestream_search::pb::GetVectorBackendRequest {}),
                )
                .await
                .unwrap()
                .into_inner();
            assert_eq!(backend_after.descriptor, backend_before.descriptor);
            assert_eq!(backend_after.config, backend_before.config);
            assert_eq!(backend_after.num_vectors, if erase_all { 0 } else { 3 });
            let acknowledged =
                pipestream_search::pb::node_service_server::NodeService::apply_wal_binding(
                    &reopened,
                    Request::new(pipestream_search::pb::ApplyWalBindingRequest {
                        plan_fingerprint: expected.plan_fingerprint,
                        body_path: expected.body_path,
                        materialize_sha: expected.materialize_sha,
                        analysis_sha: expected.analysis_sha,
                        analysis_contract: expected.analysis_contract,
                        vector_binding: expected.vector_binding.clone(),
                        ..Default::default()
                    }),
                )
                .await
                .unwrap()
                .into_inner();
            assert!(acknowledged.already_bound);
            assert_eq!(acknowledged.vector_binding, expected.vector_binding);
            drop(reopened);
            if erase_all {
                // The restored provider must accept new rows without refitting.
                // Its earlier empty image must not hide newly sealed segments
                // on the next peer snapshot install.
                let (restored, restored_handle) =
                    common::start_opened_node(target_config.clone()).await;
                let inserted = ingest(
                    &restored,
                    if explicit { explicit_bind() } else { bind() },
                    vec![doc(0).encode()],
                )
                .await
                .unwrap();
                assert_eq!(inserted.first_id, 0);
                let mut restored_client =
                    NodeServiceClient::connect(restored.clone()).await.unwrap();
                restored_client
                    .flush(pipestream_search::pb::FlushRequest {})
                    .await
                    .unwrap();
                let mut second_config = target_config;
                second_config.index_path =
                    Some(root.join(format!("second-{layout:?}-{explicit}.tv")));
                let (second, second_handle) = start_empty_node(second_config).await;
                let installed = pipestream_search::snapshot::install_snapshot_from(
                    &second,
                    pipestream_search::pb::InstallSnapshotFromRequest {
                        source: Some(
                            pipestream_search::pb::install_snapshot_from_request::Source::PeerAddr(
                                restored,
                            ),
                        ),
                        ..Default::default()
                    },
                )
                .await
                .unwrap();
                assert_eq!(installed.num_vectors, 1);
                assert_eq!(installed.num_documents, 1);
                let mut second_client = NodeServiceClient::connect(second).await.unwrap();
                let backend = second_client
                    .get_vector_backend(pipestream_search::pb::GetVectorBackendRequest {})
                    .await
                    .unwrap()
                    .into_inner();
                assert_eq!(backend.num_vectors, 1);
                assert_eq!(backend.config, backend_before.config);
                second_handle.abort();
                restored_handle.abort();
                let _ = second_handle.await;
                let _ = restored_handle.await;
            }
            node.abort();
            let _ = node.await;
        }
    }
    mock.abort();
    let _ = mock.await;
    std::fs::remove_dir_all(root).unwrap();
}
