mod common;

use pipestream_search::{
    mapping::{derive_plan, Extractor},
    pb,
};
use prost::Message;

const DESCRIPTOR: &[u8] = include_bytes!("fixtures/unsigned-mapping/descriptor.bin");
const TYPE: &str = "unsigned_mapping.Record";

#[derive(Clone, PartialEq, Message)]
struct Detail {
    #[prost(fixed64, optional, tag = "1")]
    capacity: Option<u64>,
    #[prost(uint32, tag = "2")]
    defaulted: u32,
}
#[derive(Clone, PartialEq, prost::Oneof)]
enum Choice {
    #[prost(uint64, tag = "8")]
    Chosen(u64),
    #[prost(bytes, tag = "9")]
    Opaque(Vec<u8>),
}
#[derive(Clone, PartialEq, Message)]
struct Record {
    #[prost(uint64, tag = "1")]
    id: u64,
    #[prost(string, tag = "2")]
    body: String,
    #[prost(float, repeated, tag = "3")]
    embedding: Vec<f32>,
    #[prost(uint64, optional, tag = "4")]
    counter: Option<u64>,
    #[prost(fixed64, tag = "5")]
    fixed: u64,
    #[prost(uint32, tag = "6")]
    small: u32,
    #[prost(fixed32, tag = "7")]
    small_fixed: u32,
    #[prost(oneof = "Choice", tags = "8,9")]
    choice: Option<Choice>,
    #[prost(message, optional, tag = "10")]
    detail: Option<Detail>,
    #[prost(uint64, repeated, tag = "11")]
    many: Vec<u64>,
    #[prost(map = "string, uint64", tag = "12")]
    counters: std::collections::HashMap<String, u64>,
    #[prost(uint64, tag = "13")]
    signed_hint: u64,
}
fn record(id: u64, counter: Option<u64>) -> Record {
    Record {
        id,
        counter,
        body: "word".into(),
        embedding: vec![0.25; 8],
        ..Default::default()
    }
}
fn values(row: &pb::AddDocumentsRequest) -> std::collections::BTreeMap<&str, u64> {
    row.unsigned_integers
        .iter()
        .map(|v| (v.field.as_str(), v.value))
        .collect()
}

#[test]
fn unsigned_descriptors_preserve_types_presence_and_oneof_selection() {
    let extractor = Extractor::new(DESCRIPTOR, TYPE, "body").unwrap();
    let plan = extractor.plan();
    for (path, kind) in [
        ("id", pb::MappedKind::Uint64),
        ("counter", pb::MappedKind::Uint64),
        ("fixed", pb::MappedKind::Uint64),
        ("small", pb::MappedKind::Uint32),
        ("small_fixed", pb::MappedKind::Uint32),
        ("detail.capacity", pb::MappedKind::Uint64),
        ("detail.defaulted", pb::MappedKind::Uint32),
    ] {
        let field = plan.fields.iter().find(|f| f.path == path).unwrap();
        assert_eq!(field.kind, kind as i32);
        assert_eq!(field.family, pb::ColumnFamily::U64 as i32);
    }
    for path in ["many", "counters", "opaque"] {
        let field = plan.fields.iter().find(|f| f.path == path).unwrap();
        assert_eq!(field.family, pb::ColumnFamily::None as i32);
    }
    let report = plan.schema_report.as_ref().unwrap();
    for field in report.messages.iter().flat_map(|m| &m.fields) {
        for projection in &field.projections {
            let Some(planned) = plan.fields.iter().find(|f| f.path == projection.path) else {
                assert_eq!(projection.r#use, pb::ProjectionUse::Container as i32);
                continue;
            };
            if planned.family == pb::ColumnFamily::U64 as i32 {
                assert_eq!(
                    projection.query_representation,
                    pb::MappedQueryRepresentation::UnsignedInteger as i32
                );
                let capabilities = projection.constraints.join(" ");
                for supported in [
                    "value projections",
                    "COUNT, SUM, MIN, MAX, CARDINALITY",
                    "exact percentiles",
                ] {
                    assert!(capabilities.contains(supported), "{capabilities}");
                }
                assert!(capabilities.contains("statistical folds require explicit double()"));
                assert!(
                    capabilities.contains("unsigned range facets and scoring remain unavailable")
                );
            }
        }
    }
    for value in [0, 1, (1u64 << 53) + 1, 1u64 << 63, u64::MAX] {
        let mut doc = record(value, Some(value));
        doc.fixed = value;
        doc.small = u32::MAX;
        doc.small_fixed = u32::MAX;
        doc.choice = Some(Choice::Chosen(value));
        doc.detail = Some(Detail {
            capacity: Some(value),
            defaulted: 0,
        });
        let row = extractor.extract(&doc.encode_to_vec()).unwrap().remove(0);
        let columns = values(&row.request);
        for name in ["id", "counter", "fixed", "chosen", "detail_capacity"] {
            assert_eq!(columns[name], value, "{name}");
        }
        assert_eq!(columns["small"], u64::from(u32::MAX));
        assert_eq!(columns["small_fixed"], u64::from(u32::MAX));
        assert_eq!(columns["detail_defaulted"], 0);
    }
    let absent = extractor.extract(&record(0, None).encode_to_vec()).unwrap();
    let columns = values(&absent[0].request);
    assert!(!columns.contains_key("counter"));
    assert!(!columns.contains_key("chosen"));
    assert!(!columns.contains_key("detail_defaulted"));
    for name in ["id", "fixed", "small", "small_fixed"] {
        assert_eq!(columns[name], 0);
    }
    // Concatenated protobuf messages merge. A later unindexed oneof alternative
    // clears the earlier indexed member, while nested message fragments merge.
    let mut first = record(u64::MAX, Some(0));
    first.choice = Some(Choice::Chosen(u64::MAX));
    first.detail = Some(Detail {
        capacity: Some(u64::MAX),
        defaulted: 0,
    });
    let last = Record {
        choice: Some(Choice::Opaque(vec![1])),
        detail: Some(Detail {
            capacity: None,
            defaulted: 7,
        }),
        ..Default::default()
    };
    let mut wire = first.encode_to_vec();
    wire.extend(last.encode_to_vec());
    let decoded = Record::decode(wire.as_slice()).unwrap();
    assert!(matches!(decoded.choice, Some(Choice::Opaque(_))));
    let rows = extractor.extract(&wire).unwrap();
    let columns = values(&rows[0].request);
    assert!(!columns.contains_key("chosen"));
    assert_eq!(
        columns["detail_capacity"],
        decoded.detail.as_ref().unwrap().capacity.unwrap()
    );
    assert_eq!(
        columns["detail_defaulted"],
        u64::from(decoded.detail.unwrap().defaulted)
    );
    let mut coerced = record(1, None);
    coerced.signed_hint = i64::MAX as u64;
    let rows = extractor.extract(&coerced.encode_to_vec()).unwrap();
    assert_eq!(rows[0].request.integers[0].value, i64::MAX);
    coerced.signed_hint = 1u64 << 63;
    let error = extractor.extract(&coerced.encode_to_vec()).err().unwrap();
    assert!(
        error.message().contains("signed_hint") && error.message().contains("overflows the i64")
    );
}

#[test]
fn unsigned_parent_and_chunk_ids_retain_all_bits() {
    use prost_reflect::{DescriptorPool, DynamicMessage, Value};
    let pool = DescriptorPool::decode(DESCRIPTOR).unwrap();
    let extractor = Extractor::new(DESCRIPTOR, "unsigned_mapping.Parent", "").unwrap();
    for id in [0, 1u64 << 63, u64::MAX] {
        let mut chunk =
            DynamicMessage::new(pool.get_message_by_name("unsigned_mapping.Chunk").unwrap());
        chunk.set_field_by_name("id", Value::U64(u64::MAX));
        chunk.set_field_by_name("body", Value::String("word".into()));
        chunk.set_field_by_name("embedding", Value::List(vec![Value::F32(0.25); 8]));
        let mut parent =
            DynamicMessage::new(pool.get_message_by_name("unsigned_mapping.Parent").unwrap());
        parent.set_field_by_name("id", Value::U64(id));
        parent.set_field_by_name("chunks", Value::List(vec![Value::Message(chunk)]));
        let rows = extractor.extract(&parent.encode_to_vec()).unwrap();
        assert_eq!(rows[0].request.lineage.as_ref().unwrap().parent_id, id);
        let columns = values(&rows[0].request);
        // The parent and child both have id fields; the plan provides distinct names.
        for field in extractor.plan().fields.iter().filter(|f| {
            f.role == pb::MappedRole::DocId as i32 || f.role == pb::MappedRole::ChunkId as i32
        }) {
            assert_eq!(
                columns[field.name.as_str()],
                if field.role == pb::MappedRole::DocId as i32 {
                    id
                } else {
                    u64::MAX
                }
            );
        }
    }
}

#[test]
fn ambiguous_column_names_refuse_during_planning() {
    for (kind, left, right) in [
        ("unsigned_mapping.CollidingParent", "id", "chunks.id"),
        (
            "unsigned_mapping.FlatCollision",
            "detail.capacity",
            "detail_capacity",
        ),
    ] {
        let error = derive_plan(DESCRIPTOR, kind).unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(
            error.message().contains(left)
                && error.message().contains(right)
                && error.message().contains("both land column"),
            "{error}"
        );
        assert!(pipestream_search::mapping::describe_schema(DESCRIPTOR, kind).is_ok());
    }
}

fn mapped_bind() -> pb::MappedBind {
    pb::MappedBind {
        descriptor_set: DESCRIPTOR.to_vec(),
        message_type: TYPE.into(),
        expected_fingerprint: derive_plan(DESCRIPTOR, TYPE).unwrap().fingerprint,
        body_path: "body".into(),
        analysis: Some(pipestream_search::analyzer::body_spec()),
        ..Default::default()
    }
}

fn inspect_disk(
    path: &std::path::Path,
    layout: pipestream_search::node::Layout,
    expected: &std::collections::BTreeMap<u64, Vec<u8>>,
) {
    use pipestream_search::{node, postings::Bm25Reader, segments::OpenedSegmentSet};
    let mut found = std::collections::BTreeMap::new();
    let mut inspect = |reader: &Bm25Reader| {
        reader.verify_integrity().unwrap();
        assert_eq!(
            reader.binding().unwrap().plan_fingerprint,
            mapped_bind().expected_fingerprint
        );
        let id = reader.unsigned_integer_index("id").unwrap();
        let counter = reader.unsigned_integer_index("counter").unwrap();
        let derived = reader.unsigned_integer_index("derived_uint").unwrap();
        for row in 0..reader.next_doc_id() {
            let key = reader.unsigned_integer_value(id, row).unwrap();
            let source = reader.protobuf_source(row).unwrap().unwrap().0;
            assert_eq!(source.descriptor_set, DESCRIPTOR);
            assert_eq!(source.message_type, TYPE);
            let decoded = Record::decode(source.payload.as_slice()).unwrap();
            assert_eq!(reader.unsigned_integer_value(counter, row), decoded.counter);
            assert_eq!(reader.unsigned_integer_value(derived, row), decoded.counter);
            assert!(found.insert(key, source.payload).is_none());
        }
    };
    match layout {
        node::Layout::SingleImage => {
            let generation = node::generation_dir(path);
            let image = if generation.exists() {
                node::generation_bm25(&generation)
            } else {
                node::bm25_sidecar_path(path)
            };
            inspect(&Bm25Reader::open(&image).unwrap())
        }
        node::Layout::Segments => {
            let segments = OpenedSegmentSet::open(node::segments_root(path)).unwrap();
            for part in 0..segments.len() {
                inspect(segments.bm25(part));
            }
        }
    }
    assert_eq!(&found, expected);
}

#[tokio::test]
async fn unsigned_mapped_queries_sources_and_keys_survive_reopen_and_compaction() {
    use pipestream_search::{
        analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND},
        coordinator::CoordinatorServiceImpl,
        node::{Layout, NodeConfig},
        pb::{node_service_client::NodeServiceClient, search_service_server::SearchService},
    };
    let records = [
        (0, None),
        (1, Some(0)),
        ((1u64 << 53) + 1, Some((1u64 << 53) + 1)),
        (1u64 << 63, Some(1u64 << 63)),
        (u64::MAX, Some(u64::MAX)),
    ];
    let (shift, scale) = common::fit_calibration(8, 4, &common::unit_vectors(64, 8, 81568));
    for (label, layout) in [
        ("mapped", Layout::SingleImage),
        ("segmented", Layout::Segments),
    ] {
        let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("unsigned_mapping_{label}_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("shard.tv");
        let plan = derive_plan(DESCRIPTOR, TYPE).unwrap();
        let config = NodeConfig {
            index_path: Some(path.clone()),
            layout,
            wal: true,
            wal_buckets: 2,
            seal_tail_docs: 2,
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
            unsigned_integer_fields: plan
                .fields
                .iter()
                .filter(|f| f.family == pb::ColumnFamily::U64 as i32)
                .map(|f| f.name.clone())
                .chain(["derived_uint".into()])
                .collect(),
            integer_fields: vec!["signed_hint".into()],
            ..Default::default()
        };
        let (mut addr, mut server) = common::start_empty_node(config.clone()).await;
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        client
            .set_calibration(pb::SetCalibrationRequest {
                dim: 8,
                bit_width: 4,
                shift: shift.clone(),
                scale: scale.clone(),
            })
            .await
            .unwrap();
        let mut expected = std::collections::BTreeMap::new();
        let mut bind = mapped_bind();
        bind.materialize = Some(pb::MaterializeSpec {
            columns: vec![pb::MaterializedColumn {
                name: "derived_uint".into(),
                expression: "counter + 0u".into(),
                kind: pb::MaterializeKind::U64 as i32,
            }],
        });
        let mut requests = vec![pb::IngestMappedRequest {
            payload: Some(pb::ingest_mapped_request::Payload::Bind(bind)),
        }];
        for (id, counter) in records {
            let mut doc = record(id, counter);
            doc.many = vec![u64::MAX, 0];
            doc.counters.insert("max".into(), u64::MAX);
            let mut bytes = doc.encode_to_vec();
            bytes.extend([0xa0, 0x06, 0x7b]); // Unknown field 100.
            expected.insert(id, bytes.clone());
            requests.push(pb::IngestMappedRequest {
                payload: Some(pb::ingest_mapped_request::Payload::Document(bytes)),
            });
        }
        assert_eq!(
            client
                .ingest_mapped(tokio_stream::iter(requests))
                .await
                .unwrap()
                .into_inner()
                .added,
            5
        );
        client.flush(pb::FlushRequest {}).await.unwrap();
        inspect_disk(&path, layout, &expected);
        for pass in 0..3 {
            drop(client);
            server.abort();
            let _ = server.await;
            (addr, server) = common::start_opened_node(config.clone()).await;
            client = NodeServiceClient::connect(addr.clone()).await.unwrap();
            let coordinator = CoordinatorServiceImpl::new(vec![addr.clone()])
                .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default());
            for (&id, bytes) in &expected {
                let counter = Record::decode(bytes.as_slice()).unwrap().counter;
                let filter = match counter {
                    Some(v) => format!("id == {id}u && counter == {v}u"),
                    None => format!("id == {id}u && !has(counter)"),
                };
                let response = coordinator
                    .bm25_search(tonic::Request::new(pb::Bm25SearchRequest {
                        text: "word".into(),
                        analysis: Some(body_spec()),
                        filter,
                        k: 5,
                        projections: vec![pb::NamedProjection {
                            name: "copy".into(),
                            expression: "derived_uint".into(),
                        }],
                        ..Default::default()
                    }))
                    .await
                    .unwrap()
                    .into_inner();
                assert_eq!(response.hits.len(), 1);
                assert_eq!(
                    response.hits[0].projected[0].value,
                    counter.map(pb::projected_value::Value::UintValue)
                );
            }
            if pass == 0 {
                client
                    .delete_documents(pb::DeleteDocumentsRequest {
                        doc_ids: vec![1],
                        ..Default::default()
                    })
                    .await
                    .unwrap();
                expected.remove(&1);
            }
            if pass < 2 {
                client
                    .compact_shard(pb::CompactShardRequest {
                        work_dir: dir.join(format!("compact-{pass}")).display().to_string(),
                        ..Default::default()
                    })
                    .await
                    .unwrap();
            }
            inspect_disk(&path, layout, &expected);
        }
        server.abort();
        let _ = server.await;
        drop(client);
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[tokio::test]
async fn mapped_binding_refuses_legacy_fingerprints_and_signed_column_tables() {
    use pipestream_search::{
        analyzer::NATIVE_ANALYSIS_BACKEND, node::NodeConfig,
        pb::node_service_client::NodeServiceClient,
    };
    let plan = derive_plan(DESCRIPTOR, TYPE).unwrap();
    let legacy = include_str!("fixtures/unsigned-mapping/legacy-fingerprint.txt").trim();
    assert_eq!(
        plan.fingerprint,
        "bd9afb8a87c5fcce4a1161f1087c52f330afddbe4c980c594bf834955ff959ff"
    );
    assert_ne!(plan.fingerprint, legacy);
    let unsigned: Vec<_> = plan
        .fields
        .iter()
        .filter(|f| f.family == pb::ColumnFamily::U64 as i32)
        .map(|f| f.name.clone())
        .collect();
    for wrong_columns in [false, true] {
        let config = NodeConfig {
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
            integer_fields: if wrong_columns {
                unsigned
                    .iter()
                    .cloned()
                    .chain(["signed_hint".into()])
                    .collect()
            } else {
                vec!["signed_hint".into()]
            },
            unsigned_integer_fields: if wrong_columns {
                vec![]
            } else {
                unsigned.clone()
            },
            ..Default::default()
        };
        let (addr, server) = common::start_empty_node(config).await;
        let mut client = NodeServiceClient::connect(addr).await.unwrap();
        let mut bind = mapped_bind();
        if !wrong_columns {
            bind.expected_fingerprint = legacy.into();
        }
        let error = client
            .ingest_mapped(tokio_stream::iter([pb::IngestMappedRequest {
                payload: Some(pb::ingest_mapped_request::Payload::Bind(bind)),
            }]))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.message().contains(if wrong_columns {
            "--unsigned-integer-fields"
        } else {
            "fingerprint mismatch"
        }));
        assert_eq!(
            client
                .health(pb::HealthRequest {})
                .await
                .unwrap()
                .into_inner()
                .document_slots,
            0
        );
        server.abort();
        let _ = server.await;
    }
}

#[tokio::test]
async fn unsigned_materialization_rejects_wrong_target_even_when_input_is_absent() {
    use pipestream_search::{
        analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND},
        node::NodeConfig,
        pb::node_service_client::NodeServiceClient,
    };
    let plan = derive_plan(DESCRIPTOR, TYPE).unwrap();
    let config = NodeConfig {
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
        integer_fields: vec!["signed_hint".into(), "derived".into()],
        unsigned_integer_fields: plan
            .fields
            .iter()
            .filter(|f| f.family == pb::ColumnFamily::U64 as i32)
            .map(|f| f.name.clone())
            .collect(),
        ..Default::default()
    };
    let (addr, server) = common::start_empty_node(config).await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    let (shift, scale) = common::fit_calibration(8, 4, &common::unit_vectors(64, 8, 81569));
    client
        .set_calibration(pb::SetCalibrationRequest {
            dim: 8,
            bit_width: 4,
            shift,
            scale,
        })
        .await
        .unwrap();
    let spec = pb::MaterializeSpec {
        columns: vec![pb::MaterializedColumn {
            name: "derived".into(),
            expression: "counter".into(),
            kind: pb::MaterializeKind::I64 as i32,
        }],
    };
    for mapped in [true, false] {
        for counter in [Some(u64::MAX), None] {
            let error = if mapped {
                let mut bind = mapped_bind();
                bind.materialize = Some(spec.clone());
                client
                    .ingest_mapped(tokio_stream::iter([
                        pb::IngestMappedRequest {
                            payload: Some(pb::ingest_mapped_request::Payload::Bind(bind)),
                        },
                        pb::IngestMappedRequest {
                            payload: Some(pb::ingest_mapped_request::Payload::Document(
                                record(1, counter).encode_to_vec(),
                            )),
                        },
                    ]))
                    .await
                    .unwrap_err()
            } else {
                client
                    .add_documents(tokio_stream::iter([pb::AddDocumentsRequest {
                        text: "word".into(),
                        analysis: Some(body_spec()),
                        unsigned_integers: counter
                            .map(|value| pb::UnsignedIntegerValue {
                                field: "counter".into(),
                                value,
                            })
                            .into_iter()
                            .collect(),
                        materialize: Some(spec.clone()),
                        ..Default::default()
                    }]))
                    .await
                    .unwrap_err()
            };
            assert_eq!(error.code(), tonic::Code::InvalidArgument);
            assert!(
                error.message().contains("Uint")
                    && error.message().contains("counter")
                    && error.message().contains("materializ"),
                "{error}"
            );
            assert_eq!(
                client
                    .health(pb::HealthRequest {})
                    .await
                    .unwrap()
                    .into_inner()
                    .document_slots,
                0
            );
        }
    }
    let mut valid = spec;
    valid.columns[0].expression = "signed_hint + 1".into();
    client
        .add_documents(tokio_stream::iter([pb::AddDocumentsRequest {
            text: "word".into(),
            analysis: Some(body_spec()),
            unsigned_integers: vec![pb::UnsignedIntegerValue {
                field: "counter".into(),
                value: u64::MAX,
            }],
            integers: vec![pb::IntegerValue {
                field: "signed_hint".into(),
                value: 2,
            }],
            materialize: Some(valid),
            ..Default::default()
        }]))
        .await
        .unwrap();
    assert_eq!(
        client
            .health(pb::HealthRequest {})
            .await
            .unwrap()
            .into_inner()
            .document_slots,
        1
    );
    server.abort();
    let _ = server.await;
}
