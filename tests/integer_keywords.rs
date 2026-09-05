mod common;

use pipestream_search::mapping::{derive_plan, Extractor};
use pipestream_search::pb;
use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, Value};

const DESCRIPTOR: &[u8] = include_bytes!("fixtures/integer-keywords/descriptor.bin");

fn message(name: &str) -> DynamicMessage {
    let pool = DescriptorPool::decode(DESCRIPTOR).unwrap();
    DynamicMessage::new(
        pool.get_message_by_name(&format!("integer_keywords.{name}"))
            .unwrap(),
    )
}

fn record(id: u64) -> DynamicMessage {
    let mut doc = message("Record");
    doc.set_field_by_name("id", Value::U64(id));
    doc.set_field_by_name("body", Value::String("word".into()));
    doc.set_field_by_name("embedding", Value::List(vec![Value::F32(0.25); 8]));
    doc
}

#[test]
fn keyword_projection_preserves_all_integer_encodings_and_optional_zero() {
    let extractor = Extractor::new(DESCRIPTOR, "integer_keywords.Record", "body").unwrap();
    let mut doc = record(u64::MAX);
    let values = [
        ("unsigned_value", Value::U64(u64::MAX), u64::MAX.to_string()),
        (
            "unsigned_fixed",
            Value::U64(1u64 << 63),
            (1u64 << 63).to_string(),
        ),
        ("unsigned_small", Value::U32(u32::MAX), u32::MAX.to_string()),
        ("unsigned_small_fixed", Value::U32(0), "0".into()),
        ("signed_value", Value::I64(i64::MIN), i64::MIN.to_string()),
        ("signed_zigzag", Value::I64(i64::MAX), i64::MAX.to_string()),
        ("signed_fixed", Value::I64(-1), "-1".into()),
        ("signed_small", Value::I32(i32::MIN), i32::MIN.to_string()),
        (
            "signed_small_zigzag",
            Value::I32(i32::MAX),
            i32::MAX.to_string(),
        ),
        ("signed_small_fixed", Value::I32(0), "0".into()),
    ];
    for (name, value, _) in &values {
        doc.set_field_by_name(name, value.clone());
    }
    let rows = extractor.extract(&doc.encode_to_vec()).unwrap();
    for (name, _, expected) in values {
        assert_eq!(
            rows[0]
                .request
                .facets
                .iter()
                .find(|f| f.field == name)
                .unwrap()
                .value,
            expected
        );
    }
    assert_eq!(
        rows[0]
            .request
            .facets
            .iter()
            .find(|f| f.field == "id")
            .unwrap()
            .value,
        u64::MAX.to_string()
    );
    let absent = extractor.extract(&record(0).encode_to_vec()).unwrap();
    assert!(!absent[0]
        .request
        .facets
        .iter()
        .any(|f| f.field == "unsigned_small_fixed"));

    let plan = derive_plan(DESCRIPTOR, "integer_keywords.Record").unwrap();
    let report = plan.schema_report.unwrap();
    let fields = &report
        .messages
        .iter()
        .find(|m| m.full_name == "integer_keywords.Record")
        .unwrap()
        .fields;
    let keyword = fields
        .iter()
        .find(|f| f.full_name.ends_with(".unsigned_value"))
        .unwrap();
    assert!(keyword
        .projections
        .iter()
        .all(|p| !p.constraints.iter().any(|c| c.contains("i64::MAX"))));
    let numeric = fields
        .iter()
        .find(|f| f.full_name.ends_with(".numeric_value"))
        .unwrap();
    assert_eq!(numeric.projections.len(), 1);
    assert!(numeric.projections.iter().all(|p| p.query_representation
        == pb::MappedQueryRepresentation::UnsignedInteger as i32
        && !p.constraints.iter().any(|c| c.contains("i64::MAX"))));
}

#[test]
fn keyword_parent_ids_preserve_unsigned_bits_and_signed_twos_complement() {
    for (name, id, expected) in [
        ("UnsignedParent", Value::U64(u64::MAX), u64::MAX),
        ("UnsignedParent", Value::U64(1u64 << 63), 1u64 << 63),
        ("UnsignedParent", Value::U64(0), 0),
        ("SignedParent", Value::I64(i64::MIN), i64::MIN as u64),
        ("SignedParent", Value::I64(-1), u64::MAX),
        ("SignedParent", Value::I64(i64::MAX), i64::MAX as u64),
    ] {
        let mut chunk = message("Chunk");
        chunk.set_field_by_name("id", Value::U64(u64::MAX));
        chunk.set_field_by_name("body", Value::String("word".into()));
        chunk.set_field_by_name("embedding", Value::List(vec![Value::F32(0.25); 8]));
        let mut parent = message(name);
        parent.set_field_by_name("id", id);
        parent.set_field_by_name("chunks", Value::List(vec![Value::Message(chunk)]));
        let extractor =
            Extractor::new(DESCRIPTOR, &format!("integer_keywords.{name}"), "").unwrap();
        let rows = extractor.extract(&parent.encode_to_vec()).unwrap();
        assert_eq!(
            rows[0].request.lineage.as_ref().unwrap().parent_id,
            expected
        );
    }
}

#[tokio::test]
async fn large_keyword_values_survive_mapped_ingest_queries_and_persistence() {
    use pipestream_search::pb::node_service_client::NodeServiceClient;
    use pipestream_search::pb::search_service_server::SearchService;
    let (analysis, mock) = common::mock::start_mock_analysis().await;
    let plan = derive_plan(DESCRIPTOR, "integer_keywords.Record").unwrap();
    let root = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "integer_keywords_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("shard.tv");
    let config = pipestream_search::node::NodeConfig {
        index_path: Some(path.clone()),
        layout: pipestream_search::node::Layout::SingleImage,
        analysis_addr: Some(analysis.clone()),
        facet_fields: plan
            .fields
            .iter()
            .filter(|f| f.family == pb::ColumnFamily::Facet as i32)
            .map(|f| f.name.clone())
            .collect(),
        unsigned_integer_fields: vec!["numeric_value".into()],
        ..Default::default()
    };
    let (addr, server) = common::start_empty_node(config).await;
    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    let (shift, scale) = common::fit_calibration(8, 4, &common::unit_vectors(64, 8, 911));
    client
        .set_calibration(pb::SetCalibrationRequest {
            dim: 8,
            bit_width: 4,
            shift,
            scale,
        })
        .await
        .unwrap();
    let values = [0, (1u64 << 53) + 1, i64::MAX as u64, 1u64 << 63, u64::MAX];
    let mut requests = vec![pb::IngestMappedRequest {
        payload: Some(pb::ingest_mapped_request::Payload::Bind(pb::MappedBind {
            descriptor_set: DESCRIPTOR.to_vec(),
            message_type: "integer_keywords.Record".into(),
            expected_fingerprint: plan.fingerprint,
            body_path: "body".into(),
            ..Default::default()
        })),
    }];
    let documents: Vec<_> = values
        .iter()
        .map(|&value| {
            let mut doc = record(value);
            doc.set_field_by_name("unsigned_value", Value::U64(value));
            doc.encode_to_vec()
        })
        .collect();
    requests.extend(documents.iter().map(|doc| pb::IngestMappedRequest {
        payload: Some(pb::ingest_mapped_request::Payload::Document(doc.clone())),
    }));
    assert_eq!(
        client
            .ingest_mapped(tokio_stream::iter(requests))
            .await
            .unwrap()
            .into_inner()
            .added,
        values.len() as u64
    );
    client.flush(pb::FlushRequest {}).await.unwrap();
    let coordinator = pipestream_search::coordinator::CoordinatorServiceImpl::new(vec![addr])
        .with_bm25(Some(analysis), Default::default());
    for (row, value) in values.iter().enumerate() {
        let response = coordinator
            .bm25_search(tonic::Request::new(pb::Bm25SearchRequest {
                text: "word".into(),
                k: 10,
                filter: format!("unsigned_value == \"{value}\""),
                projections: vec![pb::NamedProjection {
                    name: "value".into(),
                    expression: "unsigned_value".into(),
                }],
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.hits.len(), 1);
        assert_eq!(response.hits[0].doc_id, row as u64);
        assert_eq!(
            response.hits[0].projected[0].value,
            Some(pb::projected_value::Value::StringValue(value.to_string()))
        );
    }
    server.abort();
    let _ = server.await;
    let stored = pipestream_search::postings::Bm25Store::load(
        &pipestream_search::node::bm25_sidecar_path(&path),
    )
    .unwrap();
    let column = stored.facet_index("unsigned_value").unwrap();
    for (row, value) in values.iter().enumerate() {
        let ordinal = stored.facet_ord(column, row as u32).unwrap();
        assert_eq!(stored.facet_value(column, ordinal), value.to_string());
        assert_eq!(
            stored
                .protobuf_source(row as u32)
                .unwrap()
                .unwrap()
                .0
                .payload,
            documents[row]
        );
    }
    mock.abort();
    std::fs::remove_dir_all(root).unwrap();
}
