mod common;
use pipestream_search::{
    mapping::{derive_plan, Extractor},
    pb,
};
const DESCRIPTOR: &[u8] = include_bytes!("fixtures/wrapper-mapping/descriptor.bin");
#[test]
fn wrapper_fields_use_the_declared_path_and_hints() {
    let legacy = derive_plan(
        include_bytes!("fixtures/schema-report/descriptor.bin"),
        "report_fixture.Record",
    )
    .unwrap();
    assert_ne!(
        legacy.fingerprint,
        "769f4edf25a69d85ddfbd1a740f94c17466432def38ada264903e7269276d0ef"
    );
    let plan = derive_plan(DESCRIPTOR, "wrapper_fixture.Record").unwrap();
    assert_eq!(plan.doc_id_path, "id");
    for (path, kind, family) in [
        ("id", pb::MappedKind::Uint64, pb::ColumnFamily::U64),
        ("body", pb::MappedKind::Text, pb::ColumnFamily::TextField),
        ("signed", pb::MappedKind::Int64, pb::ColumnFamily::I64),
        ("unsigned", pb::MappedKind::Uint64, pb::ColumnFamily::U64),
        ("small_signed", pb::MappedKind::Int32, pb::ColumnFamily::I64),
        (
            "small_unsigned",
            pb::MappedKind::Uint32,
            pb::ColumnFamily::U64,
        ),
        ("enabled", pb::MappedKind::Boolean, pb::ColumnFamily::Facet),
        ("ratio", pb::MappedKind::Float, pb::ColumnFamily::F64),
        ("weight", pb::MappedKind::Double, pb::ColumnFamily::F64),
        ("status", pb::MappedKind::Keyword, pb::ColumnFamily::Facet),
        ("payload", pb::MappedKind::Binary, pb::ColumnFamily::None),
    ] {
        let field = plan.fields.iter().find(|field| field.path == path).unwrap();
        assert_eq!(
            (field.kind, field.family),
            (kind as i32, family as i32),
            "{path}"
        );
    }

    assert!(plan.fields.iter().any(|field| field.path == "renamed"
        && field.name == "total"
        && field.family == pb::ColumnFamily::I64 as i32));
    assert!(plan
        .fields
        .iter()
        .all(|field| !field.path.ends_with(".value")));
    assert!(derive_plan(DESCRIPTOR, "wrapper_fixture.BadText").is_err());
    Extractor::new(DESCRIPTOR, "wrapper_fixture.Record", "body").unwrap();
}

use prost::Message;
#[derive(Clone, PartialEq, Message)]
struct Nested {
    #[prost(message, optional, tag = "1")]
    caption: Option<String>,
    #[prost(message, optional, tag = "2")]
    number: Option<i64>,
}
#[derive(Clone, PartialEq, prost::Oneof)]
enum Selection {
    #[prost(message, tag = "13")]
    Selected(i64),
    #[prost(message, tag = "14")]
    Ignored(String),
}
#[derive(Clone, PartialEq, Message)]
struct Record {
    #[prost(message, optional, tag = "1")]
    id: Option<u64>,
    #[prost(message, optional, tag = "2")]
    body: Option<String>,
    #[prost(float, repeated, tag = "3")]
    embedding: Vec<f32>,
    #[prost(message, optional, tag = "4")]
    signed: Option<i64>,
    #[prost(message, optional, tag = "5")]
    unsigned: Option<u64>,
    #[prost(message, optional, tag = "6")]
    small_signed: Option<i32>,
    #[prost(message, optional, tag = "7")]
    small_unsigned: Option<u32>,
    #[prost(message, optional, tag = "8")]
    enabled: Option<bool>,
    #[prost(message, optional, tag = "9")]
    ratio: Option<f32>,
    #[prost(message, optional, tag = "10")]
    weight: Option<f64>,
    #[prost(message, optional, tag = "11")]
    status: Option<String>,
    #[prost(message, optional, tag = "12")]
    payload: Option<Vec<u8>>,
    #[prost(oneof = "Selection", tags = "13,14")]
    selection: Option<Selection>,
    #[prost(message, optional, tag = "15")]
    keyword_id: Option<u64>,
    #[prost(message, optional, tag = "16")]
    renamed: Option<i64>,
    #[prost(message, repeated, tag = "17")]
    many: Vec<i64>,
    #[prost(map = "string, message", tag = "18")]
    by_key: std::collections::HashMap<String, i64>,
    #[prost(message, optional, tag = "19")]
    nested: Option<Nested>,
    #[prost(message, optional, tag = "20")]
    opaque: Option<i64>,
}
fn record(id: u64) -> Record {
    Record {
        id: Some(id),
        body: Some("word".into()),
        embedding: vec![0.25; 8],
        ..Default::default()
    }
}
fn verify_projection(bytes: &[u8]) {
    let decoded = Record::decode(bytes).unwrap();
    let rows = Extractor::new(DESCRIPTOR, "wrapper_fixture.Record", "body")
        .unwrap()
        .extract(bytes)
        .unwrap();
    assert_eq!(rows.len(), 1);
    let row = &rows[0].request;
    assert_eq!(row.text, decoded.body.unwrap());
    assert_eq!(rows[0].vector, decoded.embedding);
    assert!(
        row.lineage.is_none(),
        "flat mapped rows do not invent chunk lineage"
    );
    let ints: std::collections::BTreeMap<_, _> = row
        .integers
        .iter()
        .map(|v| (v.field.as_str(), v.value))
        .collect();
    let mut expected = std::collections::BTreeMap::new();
    for (name, value) in [
        ("signed", decoded.signed),
        ("small_signed", decoded.small_signed.map(i64::from)),
        (
            "selected",
            match decoded.selection {
                Some(Selection::Selected(v)) => Some(v),
                _ => None,
            },
        ),
        ("total", decoded.renamed),
        (
            "nested_number",
            decoded.nested.as_ref().and_then(|v| v.number),
        ),
    ] {
        if let Some(v) = value {
            expected.insert(name, v);
        }
    }
    assert_eq!(ints, expected);
    let uints: std::collections::BTreeMap<_, _> = row
        .unsigned_integers
        .iter()
        .map(|v| (v.field.as_str(), v.value))
        .collect();
    let expected: std::collections::BTreeMap<_, _> = [
        ("id", decoded.id),
        ("unsigned", decoded.unsigned),
        ("small_unsigned", decoded.small_unsigned.map(u64::from)),
    ]
    .into_iter()
    .filter_map(|(name, value)| value.map(|v| (name, v)))
    .collect();
    assert_eq!(uints, expected);
    let facets: std::collections::BTreeMap<_, _> = row
        .facets
        .iter()
        .map(|v| (v.field.as_str(), v.value.clone()))
        .collect();
    let expected: std::collections::BTreeMap<_, _> = [
        ("enabled", decoded.enabled.map(|v| v.to_string())),
        ("status", decoded.status),
        ("keyword_id", decoded.keyword_id.map(|v| v.to_string())),
    ]
    .into_iter()
    .filter_map(|(name, value)| value.map(|v| (name, v)))
    .collect();
    assert_eq!(facets, expected);
    let numbers: std::collections::BTreeMap<_, _> = row
        .numerics
        .iter()
        .map(|v| (v.field.as_str(), v.value.to_bits()))
        .collect();
    let expected: std::collections::BTreeMap<_, _> = [
        ("ratio", decoded.ratio.map(f64::from)),
        ("weight", decoded.weight),
    ]
    .into_iter()
    .filter_map(|(name, value)| value.map(|v| (name, v.to_bits())))
    .collect();
    assert_eq!(numbers, expected);
    let fields: std::collections::BTreeMap<_, _> = row
        .fields
        .iter()
        .map(|v| (v.field.as_str(), v.text.clone()))
        .collect();
    let expected: std::collections::BTreeMap<_, _> = decoded
        .nested
        .and_then(|n| n.caption)
        .map(|v| ("nested_caption", v))
        .into_iter()
        .collect();
    assert_eq!(fields, expected);
}
#[test]
fn wrapper_projection_matches_generated_message_semantics() {
    for row in [
        record(u64::MAX),
        Record {
            signed: Some(0),
            unsigned: Some(0),
            small_signed: Some(0),
            small_unsigned: Some(0),
            enabled: Some(false),
            ratio: Some(0.0),
            weight: Some(0.0),
            status: Some(String::new()),
            payload: Some(vec![]),
            selection: Some(Selection::Selected(0)),
            keyword_id: Some(0),
            renamed: Some(0),
            nested: Some(Nested {
                caption: Some(String::new()),
                number: Some(0),
            }),
            ..record(0)
        },
        Record {
            signed: Some(i64::MIN),
            unsigned: Some(u64::MAX),
            small_signed: Some(i32::MIN),
            small_unsigned: Some(u32::MAX),
            enabled: Some(true),
            ratio: Some(f32::MAX),
            weight: Some(f64::MAX),
            status: Some("OPEN".into()),
            payload: Some(vec![0, 255]),
            selection: Some(Selection::Selected(i64::MAX)),
            keyword_id: Some(u64::MAX),
            renamed: Some(i64::MAX),
            many: vec![i64::MIN, 0],
            by_key: [("a".into(), i64::MAX)].into_iter().collect(),
            nested: Some(Nested {
                caption: Some("nested".into()),
                number: Some(i64::MIN),
            }),
            opaque: Some(i64::MAX),
            ..record(1 << 63)
        },
    ] {
        verify_projection(&row.encode_to_vec());
    }
    // Message merging retains earlier scalar values when a later wrapper is
    // empty, while the skipped oneof alternative still deselects its sibling.
    let mut bytes = Record {
        signed: Some(91),
        selection: Some(Selection::Selected(42)),
        ..record(9)
    }
    .encode_to_vec();
    bytes.extend([0x22, 0]); // Empty signed wrapper.
    bytes.extend([0x72, 0]); // Present empty ignored alternative.
    verify_projection(&bytes);
    for invalid in [
        Record {
            id: None,
            ..record(1)
        },
        Record {
            body: None,
            ..record(1)
        },
    ] {
        assert_eq!(
            Extractor::new(DESCRIPTOR, "wrapper_fixture.Record", "body")
                .unwrap()
                .extract(&invalid.encode_to_vec())
                .err()
                .unwrap()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
}

#[test]
fn reports_distinguish_wrapper_and_timestamp_inputs_from_query_values() {
    let plan = derive_plan(DESCRIPTOR, "wrapper_fixture.Record").unwrap();
    let report = plan.schema_report.as_ref().unwrap();
    assert_eq!(report.report_version, 2);
    let field = |name: &str| {
        report
            .messages
            .iter()
            .flat_map(|m| &m.fields)
            .find(|f| f.full_name == name)
            .unwrap()
    };
    let inputs = &field("google.protobuf.Int64Value.value").projections;
    let expected = [
        ("signed.value", "signed", vec![4, 1]),
        ("selected.value", "selected", vec![13, 1]),
        ("renamed.value", "total", vec![16, 1]),
        ("nested.number.value", "nested_number", vec![19, 2, 1]),
    ];
    assert_eq!(inputs.len(), expected.len());
    for (path, column, numbers) in expected {
        let input = inputs.iter().find(|p| p.path == path).unwrap();
        assert_eq!(input.r#use, pb::ProjectionUse::Input as i32);
        assert_eq!(
            input.query_representation,
            pb::MappedQueryRepresentation::None as i32
        );
        assert_eq!(input.column_name, column);
        assert_eq!(input.field_numbers, numbers);
        assert_eq!(input.value_path, path.strip_suffix(".value").unwrap());
        assert!(plan
            .fields
            .iter()
            .any(|f| f.path == input.value_path && f.name == column));
    }
    assert!(field("google.protobuf.BytesValue.value")
        .projections
        .is_empty());
    for path in ["payload", "many", "by_key", "opaque"] {
        let entry = &field(&format!("wrapper_fixture.Record.{path}")).projections[0];
        assert_eq!(entry.r#use, pb::ProjectionUse::SourceOnly as i32);
        assert!(entry.value_path.is_empty());
    }
    let id = &field("wrapper_fixture.Record.id").projections[0];
    assert_eq!(
        id.query_representation,
        pb::MappedQueryRepresentation::UnsignedInteger as i32
    );
    assert!(id
        .constraints
        .iter()
        .any(|c| c.contains("present empty wrapper")));
    assert!(id.value_path.is_empty());
    assert_eq!(
        pb::SchemaReport::decode(report.encode_to_vec().as_slice()).unwrap(),
        *report
    );
    let timestamp = derive_plan(
        include_bytes!("fixtures/schema-report/descriptor.bin"),
        "report_fixture.Record",
    )
    .unwrap()
    .schema_report
    .unwrap();
    for (name, number) in [("seconds", 1), ("nanos", 2)] {
        let input = &timestamp
            .messages
            .iter()
            .flat_map(|m| &m.fields)
            .find(|f| f.full_name == format!("google.protobuf.Timestamp.{name}"))
            .unwrap()
            .projections[0];
        assert_eq!(input.path, format!("created.{name}"));
        assert_eq!(input.field_numbers, [14, number]);
        assert_eq!(input.value_path, "created");
        assert_eq!(input.r#use, pb::ProjectionUse::Input as i32);
    }
}

#[test]
fn incompatible_wrapper_descriptors_refuse_projection_but_remain_describable() {
    use prost_types::{
        field_descriptor_proto::{Label, Type},
        FileDescriptorSet,
    };
    for change in 0..8 {
        let mut set = FileDescriptorSet::decode(DESCRIPTOR).unwrap();
        let file = set
            .file
            .iter_mut()
            .find(|f| f.name() == "google/protobuf/wrappers.proto")
            .unwrap();
        file.syntax = Some("proto2".into());
        let wrapper = file
            .message_type
            .iter_mut()
            .find(|m| m.name() == "Int64Value")
            .unwrap();
        match change {
            0 => wrapper.field.clear(),
            1 => wrapper.field[0].number = Some(2),
            2 => wrapper.field[0].name = Some("other".into()),
            3 => wrapper.field[0].r#type = Some(Type::Uint64 as i32),
            4 => wrapper.field[0].label = Some(Label::Repeated as i32),
            5 => wrapper.field[0].default_value = Some("7".into()),
            6 => wrapper.field[0].label = Some(Label::Required as i32),
            7 => {
                wrapper.oneof_decl.push(prost_types::OneofDescriptorProto {
                    name: Some("choice".into()),
                    ..Default::default()
                });
                wrapper.field[0].oneof_index = Some(0);
            }
            _ => unreachable!(),
        }
        let bytes = set.encode_to_vec();
        pipestream_search::mapping::describe_schema(&bytes, "wrapper_fixture.Record").unwrap();
        let error = derive_plan(&bytes, "wrapper_fixture.Record")
            .err()
            .expect("invalid wrapper shape");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(
            error.message().contains("signed") && error.message().contains("wrapper"),
            "{change}: {error}"
        );
    }
}

#[derive(Clone, PartialEq, Message)]
struct Chunk {
    #[prost(message, optional, tag = "1")]
    id: Option<u64>,
    #[prost(message, optional, tag = "2")]
    body: Option<String>,
    #[prost(float, repeated, tag = "3")]
    embedding: Vec<f32>,
}
#[derive(Clone, PartialEq, Message)]
struct Chunked {
    #[prost(message, optional, tag = "1")]
    id: Option<String>,
    #[prost(message, repeated, tag = "2")]
    chunks: Vec<Chunk>,
}
#[test]
fn wrapped_ids_and_bodies_work_in_chunk_scopes() {
    let extractor = Extractor::new(DESCRIPTOR, "wrapper_fixture.Chunked", "chunks.body").unwrap();
    let mut document = Chunked {
        id: Some(String::new()),
        chunks: vec![
            Chunk {
                id: Some(0),
                body: Some("first".into()),
                embedding: vec![0.25; 8],
            },
            Chunk {
                id: Some(u64::MAX),
                body: Some(String::new()),
                embedding: vec![0.25; 8],
            },
        ],
    };
    let rows = extractor.extract(&document.encode_to_vec()).unwrap();
    assert_eq!(rows.len(), 2);
    for (row, chunk) in rows.iter().zip(&document.chunks) {
        // SHA-256 of the explicitly present empty string, not absent identity.
        assert_eq!(
            row.request.lineage.as_ref().unwrap().parent_id,
            0xe3b0c44298fc1c14
        );
        assert_eq!(row.request.text, *chunk.body.as_ref().unwrap());
        assert_eq!(
            row.request
                .unsigned_integers
                .iter()
                .find(|v| v.field == "chunk_key")
                .unwrap()
                .value,
            chunk.id.unwrap()
        );
    }
    document.chunks[1].id = None;
    let error = extractor.extract(&document.encode_to_vec()).err().unwrap();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("chunks.id"), "{error}");
}

fn bind() -> pb::MappedBind {
    pb::MappedBind {
        descriptor_set: DESCRIPTOR.to_vec(),
        message_type: "wrapper_fixture.Record".into(),
        expected_fingerprint: derive_plan(DESCRIPTOR, "wrapper_fixture.Record")
            .unwrap()
            .fingerprint,
        body_path: "body".into(),
        analysis: Some(pipestream_search::analyzer::body_spec()),
        ..Default::default()
    }
}
fn explicit_bind() -> pb::MappedBind {
    pb::MappedBind {
        analysis: None,
        field_analysis: vec![
            pb::MappedFieldAnalysis {
                path: "body".into(),
                analysis: Some(pipestream_search::analyzer::body_spec()),
            },
            pb::MappedFieldAnalysis {
                path: "nested.caption".into(),
                analysis: Some(pipestream_search::analyzer::cased_body_spec()),
            },
        ],
        ..bind()
    }
}
fn inspect_sources(
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
            bind().expected_fingerprint
        );
        for row in 0..reader.next_doc_id() {
            let source = reader.protobuf_source(row).unwrap().unwrap().0;
            assert_eq!(source.descriptor_set, DESCRIPTOR);
            assert_eq!(source.message_type, "wrapper_fixture.Record");
            let original = Record::decode(source.payload.as_slice()).unwrap();
            let id = reader
                .unsigned_integer_value(reader.unsigned_integer_index("id").unwrap(), row)
                .unwrap();
            assert_eq!(Some(id), original.id);
            for (column, value) in [("signed", original.signed), ("total", original.renamed)] {
                assert_eq!(
                    reader.integer_value(reader.integer_index(column).unwrap(), row),
                    value
                );
            }
            assert_eq!(
                reader.unsigned_integer_value(
                    reader.unsigned_integer_index("unsigned").unwrap(),
                    row
                ),
                original.unsigned
            );
            assert!(found.insert(id, source.payload).is_none());
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
            inspect(&Bm25Reader::open(&image).unwrap());
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
async fn wrapper_queries_and_originals_survive_flush_reopen_and_compaction() {
    wrapper_lifecycle(false).await;
}
#[tokio::test]
async fn explicit_native_fields_survive_flush_reopen_and_compaction() {
    wrapper_lifecycle(true).await;
}
async fn wrapper_lifecycle(explicit: bool) {
    let (sidecar, mock) = common::mock::start_mock_analysis().await;
    let analysis = if explicit {
        "native".to_owned()
    } else {
        sidecar
    };
    let binding = if explicit { explicit_bind() } else { bind() };
    use pipestream_search::{
        analyzer::body_spec,
        coordinator::CoordinatorServiceImpl,
        node::{Layout, NodeConfig},
        pb::{
            node_service_client::NodeServiceClient, projected_value::Value,
            search_service_server::SearchService,
        },
    };
    let documents = [
        record(0),
        Record {
            signed: Some(0),
            unsigned: Some(0),
            enabled: Some(false),
            status: Some(String::new()),
            renamed: Some(0),
            ratio: Some(0.0),
            ..record(1)
        },
        Record {
            signed: Some(i64::MIN),
            unsigned: Some(u64::MAX),
            enabled: Some(true),
            status: Some("OPEN".into()),
            renamed: Some(i64::MAX),
            ratio: Some(0.5),
            nested: Some(Nested {
                caption: Some("NESTED".into()),
                number: Some(i64::MIN),
            }),
            payload: Some(vec![0, 255]),
            many: vec![0, i64::MIN],
            by_key: [("retained".into(), i64::MAX)].into_iter().collect(),
            ..record(u64::MAX)
        },
    ];
    let (shift, scale) = common::fit_calibration(8, 4, &common::unit_vectors(64, 8, 94217));
    for (label, layout) in [
        ("single", Layout::SingleImage),
        ("segments", Layout::Segments),
    ] {
        let directory = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "wrapper_mapping_{explicit}_{label}_{}",
            std::process::id()
        ));
        std::fs::create_dir_all(&directory).unwrap();
        let path = directory.join("shard.tv");
        let config = NodeConfig {
            index_path: Some(path.clone()),
            layout,
            wal: true,
            wal_buckets: 2,
            seal_tail_docs: 2,
            analysis_addr: Some(analysis.clone()),
            bm25_fields: vec!["body".into(), "nested_caption".into()],
            facet_fields: ["enabled", "status", "keyword_id"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            integer_fields: [
                "signed",
                "small_signed",
                "selected",
                "total",
                "nested_number",
            ]
            .into_iter()
            .map(str::to_owned)
            .collect(),
            unsigned_integer_fields: ["id", "unsigned", "small_unsigned"]
                .into_iter()
                .map(str::to_owned)
                .collect(),
            numeric_fields: vec!["ratio".into(), "weight".into()],
            ..Default::default()
        };
        let (mut address, mut server) = common::start_empty_node(config.clone()).await;
        let mut client = NodeServiceClient::connect(address.clone()).await.unwrap();
        client
            .set_calibration(pb::SetCalibrationRequest {
                dim: 8,
                bit_width: 4,
                shift: shift.clone(),
                scale: scale.clone(),
            })
            .await
            .unwrap();
        if explicit {
            let mut invalid = binding.clone();
            invalid.field_analysis[1]
                .analysis
                .as_mut()
                .unwrap()
                .tokenizer = 2;
            let error = client
                .ingest_mapped(tokio_stream::iter(vec![pb::IngestMappedRequest {
                    payload: Some(pb::ingest_mapped_request::Payload::Bind(invalid)),
                }]))
                .await
                .unwrap_err();
            assert_eq!(error.code(), tonic::Code::InvalidArgument);
            client
                .ingest_mapped(tokio_stream::iter(vec![pb::IngestMappedRequest {
                    payload: Some(pb::ingest_mapped_request::Payload::Bind(binding.clone())),
                }]))
                .await
                .unwrap();
            client.flush(pb::FlushRequest {}).await.unwrap();
            drop(client);
            server.abort();
            let _ = server.await;
            (address, server) = common::start_opened_node(config.clone()).await;
            client = NodeServiceClient::connect(address.clone()).await.unwrap();
            let mut changed = binding.clone();
            changed.field_analysis[1].analysis = Some(body_spec());
            let error = client
                .ingest_mapped(tokio_stream::iter(vec![pb::IngestMappedRequest {
                    payload: Some(pb::ingest_mapped_request::Payload::Bind(changed)),
                }]))
                .await
                .unwrap_err();
            assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        }
        let mut expected = std::collections::BTreeMap::new();
        let mut requests = vec![pb::IngestMappedRequest {
            payload: Some(pb::ingest_mapped_request::Payload::Bind(binding.clone())),
        }];
        for doc in &documents {
            let mut bytes = doc.encode_to_vec();
            bytes.extend([0xa0, 0x06, 0x7b]); // Unknown field 100.
            expected.insert(doc.id.unwrap(), bytes.clone());
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
            3
        );
        client.flush(pb::FlushRequest {}).await.unwrap();
        inspect_sources(&path, layout, &expected);
        for pass in 0..3 {
            drop(client);
            server.abort();
            let _ = server.await;
            (address, server) = common::start_opened_node(config.clone()).await;
            client = NodeServiceClient::connect(address.clone()).await.unwrap();
            let coordinator = CoordinatorServiceImpl::new(vec![address.clone()])
                .with_bm25(Some(analysis.clone()), Default::default());
            for (&id, bytes) in &expected {
                let original = Record::decode(bytes.as_slice()).unwrap();
                let filter = match original.unsigned {
                    Some(v) => format!("id == {id}u && unsigned == {v}u"),
                    None => format!("id == {id}u && !has(unsigned)"),
                };
                let filter = match &original.status {
                    Some(value) => format!(
                        "{filter} && has(status) && status == {}",
                        serde_json::to_string(value).unwrap()
                    ),
                    None => format!("{filter} && !has(status)"),
                };
                let result = coordinator
                    .bm25_search(tonic::Request::new(pb::Bm25SearchRequest {
                        text: "word".into(),
                        analysis: Some(body_spec()),
                        k: 3,
                        filter,
                        projections: [
                            "signed",
                            "unsigned",
                            "total",
                            "enabled",
                            "status",
                            "ratio",
                            "nested_number",
                        ]
                        .into_iter()
                        .map(|name| pb::NamedProjection {
                            name: name.into(),
                            expression: name.into(),
                        })
                        .collect(),
                        ..Default::default()
                    }))
                    .await
                    .unwrap()
                    .into_inner();
                assert_eq!(result.hits.len(), 1);
                let values: Vec<_> = result.hits[0]
                    .projected
                    .iter()
                    .map(|v| v.value.clone())
                    .collect();
                assert_eq!(
                    values,
                    vec![
                        original.signed.map(Value::IntValue),
                        original.unsigned.map(Value::UintValue),
                        original.renamed.map(Value::IntValue),
                        original.enabled.map(|v| Value::StringValue(v.to_string())),
                        original.status.map(Value::StringValue),
                        original.ratio.map(|v| Value::DoubleValue(f64::from(v))),
                        original.nested.and_then(|v| v.number).map(Value::IntValue)
                    ]
                );
            }
            if explicit {
                for (text, count) in [("NESTED", 1), ("nested", 0)] {
                    let result = coordinator
                        .bm25_search(tonic::Request::new(pb::Bm25SearchRequest {
                            text: text.into(),
                            k: 3,
                            fields: vec![pb::QueryField {
                                field: "nested_caption".into(),
                                weight: 1.0,
                                analysis: Some(pipestream_search::analyzer::cased_body_spec()),
                                ..Default::default()
                            }],
                            ..Default::default()
                        }))
                        .await
                        .unwrap()
                        .into_inner();
                    assert_eq!(result.hits.len(), count);
                }
                let mut changed = binding.clone();
                changed.field_analysis[1].analysis = Some(body_spec());
                let error = client
                    .ingest_mapped(tokio_stream::iter(vec![pb::IngestMappedRequest {
                        payload: Some(pb::ingest_mapped_request::Payload::Bind(changed)),
                    }]))
                    .await
                    .unwrap_err();
                assert_eq!(error.code(), tonic::Code::FailedPrecondition);
                let generation = pipestream_search::reshard::resolve_gen(
                    &pipestream_search::wal::wal_dir(&path),
                )
                .unwrap();
                let bound = pipestream_search::reshard::read_generation_binding(&generation)
                    .unwrap()
                    .unwrap();
                assert_eq!(bound.analysis_sha.len(), 64);
                assert_eq!(
                    pipestream_search::wal::read_manifest(&generation)
                        .unwrap()
                        .format_version,
                    4
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
                        work_dir: directory
                            .join(format!("compact-{pass}"))
                            .display()
                            .to_string(),
                        ..Default::default()
                    })
                    .await
                    .unwrap();
            }
            inspect_sources(&path, layout, &expected);
        }
        server.abort();
        let _ = server.await;
        drop(client);
        std::fs::remove_dir_all(directory).unwrap();
    }
    mock.abort();
    let _ = mock.await;
}

#[test]
fn identity_roles_require_an_exact_value_projection_during_planning() {
    for name in ["BadId", "BadTextId", "BadChunked"] {
        let error = derive_plan(DESCRIPTOR, &format!("wrapper_fixture.{name}"))
            .err()
            .expect("unusable identity plan");
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(
            error.message().contains("id") || error.message().contains("ID"),
            "{name}: {error}"
        );
    }
}

#[derive(Clone, PartialEq, Message)]
struct NumericParent {
    #[prost(message, optional, tag = "1")]
    id: Option<u64>,
    #[prost(message, repeated, tag = "2")]
    chunks: Vec<Chunk>,
}
#[test]
fn wrapped_numeric_parent_keys_keep_bits_under_keyword_hints() {
    for root in [
        "wrapper_fixture.NumericParent",
        "wrapper_fixture.NumericKeywordParent",
    ] {
        let extractor = Extractor::new(DESCRIPTOR, root, "chunks.body").unwrap();
        for id in [0, 1 << 63, u64::MAX] {
            let source = NumericParent {
                id: Some(id),
                chunks: vec![Chunk {
                    id: Some(u64::MAX),
                    body: Some("word".into()),
                    embedding: vec![0.25; 8],
                }],
            };
            let rows = extractor.extract(&source.encode_to_vec()).unwrap();
            assert_eq!(rows[0].request.lineage.as_ref().unwrap().parent_id, id);
            if root.ends_with("KeywordParent") {
                assert_eq!(
                    rows[0]
                        .request
                        .facets
                        .iter()
                        .find(|f| f.field == "id")
                        .unwrap()
                        .value,
                    id.to_string()
                );
            } else {
                assert_eq!(
                    rows[0]
                        .request
                        .unsigned_integers
                        .iter()
                        .find(|f| f.field == "id")
                        .unwrap()
                        .value,
                    id
                );
            }
        }
    }
}

#[test]
fn well_known_component_hints_are_not_silently_discarded() {
    use prost_reflect::{DescriptorPool, DynamicMessage, Value};
    for (bytes, root, hint_field, component_type) in [
        (
            DESCRIPTOR,
            "wrapper_fixture.Record",
            "keyword_id",
            "Int64Value",
        ),
        (
            include_bytes!("fixtures/schema-report/descriptor.bin").as_slice(),
            "report_fixture.Record",
            "secret",
            "Timestamp",
        ),
    ] {
        let pool = DescriptorPool::decode(bytes).unwrap();
        let descriptor = pool
            .get_message_by_name("google.protobuf.FileDescriptorSet")
            .unwrap();
        let mut set = DynamicMessage::decode(descriptor, bytes).unwrap();
        let Value::List(files) = set.get_field_by_name_mut("file").unwrap() else {
            panic!()
        };
        let mut hint = None;
        for file in files.iter().filter_map(Value::as_message) {
            let messages = file.get_field_by_name("message_type").unwrap();
            for message in messages
                .as_list()
                .unwrap()
                .iter()
                .filter_map(Value::as_message)
            {
                if message.get_field_by_name("name").unwrap().as_str() != Some("Record") {
                    continue;
                }
                let fields = message.get_field_by_name("field").unwrap();
                let field = fields
                    .as_list()
                    .unwrap()
                    .iter()
                    .filter_map(Value::as_message)
                    .find(|field| {
                        field.get_field_by_name("name").unwrap().as_str() == Some(hint_field)
                    })
                    .unwrap();
                hint = Some(field.get_field_by_name("options").unwrap().into_owned());
            }
        }
        let hint = hint.unwrap();
        let mut edited = false;
        for file in files {
            let Value::Message(file) = file else { panic!() };
            let Value::List(messages) = file.get_field_by_name_mut("message_type").unwrap() else {
                panic!()
            };
            for message in messages {
                let Value::Message(message) = message else {
                    panic!()
                };
                if message.get_field_by_name("name").unwrap().as_str() != Some(component_type) {
                    continue;
                }
                let Value::List(fields) = message.get_field_by_name_mut("field").unwrap() else {
                    panic!()
                };
                let Value::Message(value) = &mut fields[0] else {
                    panic!()
                };
                value.set_field_by_name("options", hint.clone());
                edited = true;
            }
        }
        assert!(edited);
        let error = derive_plan(&set.encode_to_vec(), root).err().unwrap();
        assert!(
            error
                .message()
                .contains("hints on well-known value components"),
            "{error}"
        );
    }
}

#[tokio::test]
async fn wrapped_body_and_scalar_defaults_work_in_the_embedded_native_runtime() {
    embedded_wrappers(false).await;
}
#[tokio::test]
async fn native_embedded_mapped_non_body_text_has_its_own_analysis() {
    embedded_wrappers(true).await;
}
async fn embedded_wrappers(explicit: bool) {
    use pipestream_search::embedded::{EmbeddedSearch, EmbeddedSearchConfig, EmbeddedShardConfig};
    let mut shard = EmbeddedShardConfig::in_memory(0);
    shard.node.bm25_fields = vec!["body".into(), "nested_caption".into()];
    shard.node.facet_fields = ["enabled", "status", "keyword_id"]
        .into_iter()
        .map(str::to_owned)
        .collect();
    shard.node.integer_fields = [
        "signed",
        "small_signed",
        "selected",
        "total",
        "nested_number",
    ]
    .into_iter()
    .map(str::to_owned)
    .collect();
    shard.node.unsigned_integer_fields = ["id", "unsigned", "small_unsigned"]
        .into_iter()
        .map(str::to_owned)
        .collect();
    shard.node.numeric_fields = vec!["ratio".into(), "weight".into()];
    let runtime = EmbeddedSearch::open(EmbeddedSearchConfig::single(shard))
        .await
        .unwrap();
    let (shift, scale) = common::fit_calibration(8, 4, &common::unit_vectors(64, 8, 94218));
    runtime
        .set_calibration(
            0,
            pb::SetCalibrationRequest {
                dim: 8,
                bit_width: 4,
                shift,
                scale,
            },
        )
        .await
        .unwrap();
    let source = Record {
        signed: Some(i64::MIN),
        unsigned: Some(0),
        status: Some(String::new()),
        nested: explicit.then(|| Nested {
            caption: Some("NESTED".into()),
            number: None,
        }),
        ..record(u64::MAX)
    };
    runtime
        .ingest_mapped(
            0,
            vec![
                pb::IngestMappedRequest {
                    payload: Some(pb::ingest_mapped_request::Payload::Bind(if explicit {
                        explicit_bind()
                    } else {
                        bind()
                    })),
                },
                pb::IngestMappedRequest {
                    payload: Some(pb::ingest_mapped_request::Payload::Document(
                        source.encode_to_vec(),
                    )),
                },
            ],
        )
        .await
        .unwrap();
    let result = runtime
        .query(pb::QueryRequest {
            k: 3,
            selection_k: 3,
            selection: Some(pb::SelectionQuery {
                node: Some(pb::selection_query::Node::Search(pb::SearchQuery {
                    id: "lexical".into(),
                    query: Some(pb::search_query::Query::Lexical(pb::LexicalQuery {
                        text: "word".into(),
                        analysis: Some(pipestream_search::analyzer::body_spec()),
                        ..Default::default()
                    })),
                })),
            }),
            ..Default::default()
        })
        .await
        .unwrap();
    assert_eq!(result.hits.len(), 1);
    if explicit {
        let result = runtime
            .bm25_search(pb::Bm25SearchRequest {
                text: "NESTED".into(),
                k: 3,
                fields: vec![pb::QueryField {
                    field: "nested_caption".into(),
                    weight: 1.0,
                    analysis: Some(pipestream_search::analyzer::cased_body_spec()),
                    ..Default::default()
                }],
                ..Default::default()
            })
            .await
            .unwrap();
        assert_eq!(result.hits.len(), 1);
    }
}
