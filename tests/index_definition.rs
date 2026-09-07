mod common;

use pipestream_search::mapping::{derive_plan, derive_plan_with_definition, Extractor};
use pipestream_search::pb::{self, MappedKind as K, MappedRole as R};
use prost::Message;
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet};

fn field(name: &str, number: i32, kind: Type) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.into()),
        number: Some(number),
        r#type: Some(kind as i32),
        label: Some(Label::Optional as i32),
        ..Default::default()
    }
}

fn schema() -> Vec<u8> {
    FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("explicit.proto".into()),
            package: Some("explicit".into()),
            syntax: Some("proto3".into()),
            message_type: vec![
                DescriptorProto {
                    name: Some("Record".into()),
                    field: vec![
                        field("id", 1, Type::Uint64),
                        field("body", 2, Type::String),
                        FieldDescriptorProto {
                            label: Some(Label::Repeated as i32),
                            ..field("embedding", 3, Type::Float)
                        },
                        field("private_notes", 4, Type::String),
                        FieldDescriptorProto {
                            type_name: Some(".explicit.Meta".into()),
                            ..field("left", 5, Type::Message)
                        },
                        FieldDescriptorProto {
                            type_name: Some(".explicit.Meta".into()),
                            ..field("right", 6, Type::Message)
                        },
                    ],
                    ..Default::default()
                },
                DescriptorProto {
                    name: Some("Meta".into()),
                    field: vec![field("value", 1, Type::Uint64)],
                    ..Default::default()
                },
            ],
            ..Default::default()
        }],
    }
    .encode_to_vec()
}

fn projection(path: &[u32], kind: K, name: &str, role: R) -> pb::IndexProjection {
    pb::IndexProjection {
        field_numbers: path.into(),
        kind: kind as i32,
        column_name: name.into(),
        role: role as i32,
        vector_dims: if kind == K::Vector { 8 } else { 0 },
    }
}

fn definition() -> pb::IndexDefinition {
    pb::IndexDefinition {
        projections: vec![
            projection(&[1], K::Uint64, "key", R::DocId),
            projection(&[2], K::Text, "body", R::None),
            projection(&[3], K::Vector, "semantic", R::None),
            projection(&[5, 1], K::Keyword, "left_value", R::None),
            projection(&[6, 1], K::Uint64, "right_value", R::None),
        ],
    }
}

#[test]
fn explicit_policy_selects_occurrences_without_inferred_columns() {
    let bytes = schema();
    let definition = definition();
    let plan = derive_plan_with_definition(&bytes, "explicit.Record", Some(&definition)).unwrap();
    assert_eq!(plan.index_definition, Some(definition.clone()));
    assert_eq!(plan.fields.len(), 5);
    assert!(!plan.fields.iter().any(|f| f.path == "private_notes"));
    assert_eq!(
        plan.fields
            .iter()
            .find(|f| f.path == "left.value")
            .unwrap()
            .family,
        pb::ColumnFamily::Facet as i32
    );
    assert_eq!(
        plan.fields
            .iter()
            .find(|f| f.path == "right.value")
            .unwrap()
            .family,
        pb::ColumnFamily::U64 as i32
    );
    assert_ne!(
        plan.fingerprint,
        derive_plan(&bytes, "explicit.Record").unwrap().fingerprint
    );
    let mut reordered = definition.clone();
    reordered.projections.reverse();
    assert_eq!(
        plan,
        derive_plan_with_definition(&bytes, "explicit.Record", Some(&reordered)).unwrap()
    );
    let report = plan.schema_report.unwrap();
    let private = report
        .messages
        .iter()
        .flat_map(|m| &m.fields)
        .find(|f| f.full_name == "explicit.Record.private_notes")
        .unwrap();
    assert!(private.projections.is_empty());
}

#[test]
fn definition_refuses_ambiguous_paths_and_unimplemented_value_shapes() {
    let bytes = schema();
    let base = definition();
    let mut invalid = vec![pb::IndexDefinition::default()];
    let mut duplicate = base.clone();
    duplicate.projections.push(duplicate.projections[0].clone());
    invalid.push(duplicate);
    for changed in [
        projection(&[], K::Text, "x", R::None),
        projection(&[99], K::Text, "x", R::None),
        projection(&[5, 99], K::Text, "x", R::None),
        projection(&[1, 1], K::Uint64, "x", R::None),
        projection(&[5], K::Object, "x", R::None),
        projection(&[3], K::Float, "x", R::None),
        projection(&[2], K::Uint64, "x", R::None),
        projection(&[4], K::Text, "key", R::None),
        projection(&[4], K::Text, "", R::None),
        projection(&[4], K::Unspecified, "x", R::None),
        pb::IndexProjection {
            kind: 999,
            ..projection(&[4], K::Text, "x", R::None)
        },
        pb::IndexProjection {
            role: 999,
            ..projection(&[4], K::Text, "x", R::None)
        },
        pb::IndexProjection {
            vector_dims: 1,
            ..projection(&[4], K::Text, "x", R::None)
        },
    ] {
        let mut bad = base.clone();
        bad.projections.push(changed);
        invalid.push(bad);
    }
    for index in 0..3 {
        let mut bad = base.clone();
        match index {
            0 => bad.projections[0].role = R::None as i32,
            1 => bad.projections[2].vector_dims = 0,
            _ => bad.projections.remove(2).vector_dims = 0,
        }
        invalid.push(bad);
    }
    for bad in invalid {
        let error = derive_plan_with_definition(&bytes, "explicit.Record", Some(&bad)).unwrap_err();
        assert_eq!(
            error.code(),
            tonic::Code::InvalidArgument,
            "{bad:?}: {error}"
        );
    }
}

fn document(bytes: &[u8], id: u64) -> Vec<u8> {
    use prost_reflect::{DescriptorPool, DynamicMessage, Value};
    let pool = DescriptorPool::decode(bytes).unwrap();
    let mut doc = DynamicMessage::new(pool.get_message_by_name("explicit.Record").unwrap());
    doc.set_field_by_name("id", Value::U64(id));
    doc.set_field_by_name("body", Value::String("private zebra".into()));
    doc.set_field_by_name("private_notes", Value::String("unindexed secret".into()));
    doc.set_field_by_name("embedding", Value::List(vec![Value::F32(0.25); 8]));
    let mut meta = DynamicMessage::new(pool.get_message_by_name("explicit.Meta").unwrap());
    meta.set_field_by_name("value", Value::U64(u64::MAX));
    doc.set_field_by_name("left", Value::Message(meta.clone()));
    doc.set_field_by_name("right", Value::Message(meta));
    let mut encoded = doc.encode_to_vec();
    // An unknown length-delimited field must survive source retention.
    encoded.extend_from_slice(&[0xba, 0x06, 0x03, b'r', b'a', b'w']);
    encoded
}

#[test]
fn extraction_applies_occurrence_policy_and_ignores_source_only_values() {
    let bytes = schema();
    let extractor =
        Extractor::with_definition(&bytes, "explicit.Record", "body", Some(&definition())).unwrap();
    let rows = extractor.extract(&document(&bytes, 7)).unwrap();
    assert_eq!(rows.len(), 1);
    let row = &rows[0].request;
    assert!(row.fields.is_empty());
    assert_eq!(row.text, "private zebra");
    assert_eq!(
        row.facets,
        vec![pb::FacetValue {
            field: "left_value".into(),
            value: u64::MAX.to_string()
        }]
    );
    assert_eq!(
        row.unsigned_integers,
        vec![
            pb::UnsignedIntegerValue {
                field: "key".into(),
                value: 7
            },
            pb::UnsignedIntegerValue {
                field: "right_value".into(),
                value: u64::MAX
            },
        ]
    );
}

#[test]
fn explicit_and_inferred_equal_columns_have_distinct_binding_domains() {
    let bytes = include_bytes!("fixtures/vector-binding/descriptor.bin");
    let legacy = derive_plan(bytes, "vector_binding.Named").unwrap();
    let mut vector = projection(&[3], K::Vector, "semantic", R::None);
    vector.vector_dims = 16;
    let definition = pb::IndexDefinition {
        projections: vec![
            projection(&[1], K::Int64, "id", R::DocId),
            projection(&[2], K::Text, "body", R::None),
            vector,
        ],
    };
    let explicit =
        derive_plan_with_definition(bytes, "vector_binding.Named", Some(&definition)).unwrap();
    assert_eq!(legacy.fields, explicit.fields);
    assert_ne!(legacy.fingerprint, explicit.fingerprint);
    assert_eq!(legacy.descriptor_sha256, explicit.descriptor_sha256);
}

#[test]
fn chunks_are_explicit_and_do_not_enable_arbitrary_repeated_traversal() {
    let bytes = include_bytes!("fixtures/integer-keywords/descriptor.bin");
    let mut definition = pb::IndexDefinition {
        projections: vec![
            projection(&[1], K::Uint64, "key", R::DocId),
            projection(&[2], K::Nested, "", R::Chunks),
            projection(&[2, 1], K::Uint64, "chunk_key", R::ChunkId),
            projection(&[2, 2], K::Text, "body", R::None),
            projection(&[2, 3], K::Vector, "semantic", R::None),
        ],
    };
    let plan =
        derive_plan_with_definition(bytes, "integer_keywords.UnsignedParent", Some(&definition))
            .unwrap();
    assert_eq!(plan.chunks_path, "chunks");
    assert_eq!(plan.chunk_id_path, "chunks.id");
    let extractor = Extractor::with_definition(
        bytes,
        "integer_keywords.UnsignedParent",
        "",
        Some(&definition),
    )
    .unwrap();
    use prost_reflect::{DescriptorPool, DynamicMessage, Value};
    let pool = DescriptorPool::decode(bytes.as_slice()).unwrap();
    let mut parent = DynamicMessage::new(
        pool.get_message_by_name("integer_keywords.UnsignedParent")
            .unwrap(),
    );
    parent.set_field_by_name("id", Value::U64(u64::MAX));
    let mut chunk =
        DynamicMessage::new(pool.get_message_by_name("integer_keywords.Chunk").unwrap());
    chunk.set_field_by_name("id", Value::U64(3));
    chunk.set_field_by_name("body", Value::String("chunk word".into()));
    chunk.set_field_by_name("embedding", Value::List(vec![Value::F32(0.25); 8]));
    parent.set_field_by_name("chunks", Value::List(vec![Value::Message(chunk)]));
    let rows = extractor.extract(&parent.encode_to_vec()).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].request.lineage.as_ref().unwrap().parent_id,
        u64::MAX
    );
    assert!(
        rows[0].request.facets.is_empty(),
        "descriptor keyword hints must not override explicit unsigned projections"
    );
    definition.projections.remove(1);
    let error =
        derive_plan_with_definition(bytes, "integer_keywords.UnsignedParent", Some(&definition))
            .unwrap_err();
    assert!(error.message().contains("CHUNKS"));
}

#[tokio::test]
async fn explicit_policy_crosses_rpc_bind_and_persistent_source_boundaries() {
    use pipestream_search::node::{Layout, NodeConfig};
    use pipestream_search::pb::node_service_client::NodeServiceClient;
    use pipestream_search::pb::search_service_server::SearchService;
    let bytes = schema();
    let definition = definition();
    let (planning_addr, planning_server) = common::start_coordinator(Vec::new()).await;
    let mut planner = pb::search_service_client::SearchServiceClient::connect(planning_addr)
        .await
        .unwrap();
    let plan = planner
        .plan_index(pb::PlanIndexRequest {
            descriptor_set: bytes.clone(),
            message_type: "explicit.Record".into(),
            index_definition: Some(definition.clone()),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .plan
        .unwrap();
    assert_eq!(plan.index_definition, Some(definition.clone()));
    let root = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
        "index_definition_{}_{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&root).unwrap();
    let path = root.join("shard.tv");
    let config = NodeConfig {
        index_path: Some(path.clone()),
        layout: Layout::SingleImage,
        wal: true,
        analysis_addr: Some(pipestream_search::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
        facet_fields: vec!["left_value".into()],
        unsigned_integer_fields: vec!["key".into(), "right_value".into()],
        ..Default::default()
    };
    let bind = pb::MappedBind {
        descriptor_set: bytes.clone(),
        message_type: "explicit.Record".into(),
        body_path: "body".into(),
        expected_fingerprint: plan.fingerprint.clone(),
        index_definition: Some(definition.clone()),
        analysis: Some(pipestream_search::analyzer::body_spec()),
        ..Default::default()
    };
    let data = document(&bytes, 7);
    let retained = document(&bytes, 8);
    let (mut address, mut server) = common::start_empty_node(config.clone()).await;
    let mut client = NodeServiceClient::connect(address.clone()).await.unwrap();
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
    let frames = |bind| {
        vec![
            pb::IngestMappedRequest {
                payload: Some(pb::ingest_mapped_request::Payload::Bind(bind)),
            },
            pb::IngestMappedRequest {
                payload: Some(pb::ingest_mapped_request::Payload::Document(data.clone())),
            },
            pb::IngestMappedRequest {
                payload: Some(pb::ingest_mapped_request::Payload::Document(
                    retained.clone(),
                )),
            },
        ]
    };
    assert_eq!(
        client
            .ingest_mapped(tokio_stream::iter(frames(bind.clone())))
            .await
            .unwrap()
            .into_inner()
            .added,
        2
    );
    let mut changed = bind.clone();
    changed.index_definition.as_mut().unwrap().projections.pop();
    assert_eq!(
        client
            .ingest_mapped(tokio_stream::iter(frames(changed)))
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    client.flush(pb::FlushRequest {}).await.unwrap();
    drop(client);
    server.abort();
    let _ = server.await;
    (address, server) = common::start_opened_node(config.clone()).await;
    client = NodeServiceClient::connect(address.clone()).await.unwrap();
    // A correctly fingerprinted, changed policy must also refuse against the
    // persisted binding, not merely against the client's old fingerprint.
    let mut changed = bind.clone();
    changed.index_definition.as_mut().unwrap().projections.pop();
    changed.expected_fingerprint =
        derive_plan_with_definition(&bytes, "explicit.Record", changed.index_definition.as_ref())
            .unwrap()
            .fingerprint;
    let error = client
        .ingest_mapped(tokio_stream::iter(frames(changed)))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("durably bound"), "{error}");
    let coordinator = pipestream_search::coordinator::CoordinatorServiceImpl::new(vec![address])
        .with_bm25(
            Some(pipestream_search::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
            Default::default(),
        );
    let found = coordinator
        .bm25_search(tonic::Request::new(pb::Bm25SearchRequest {
            text: "zebra".into(),
            k: 5,
            analysis: Some(pipestream_search::analyzer::body_spec()),
            filter: format!(
                "left_value == \"{}\" && right_value == {}u",
                u64::MAX,
                u64::MAX
            ),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(found.hits.len(), 2);
    client.flush(pb::FlushRequest {}).await.unwrap();
    let before = pipestream_search::postings::Bm25Store::load(
        &pipestream_search::node::bm25_sidecar_path(&path),
    )
    .unwrap();
    assert!(
        before.document_identity(1).is_none(),
        "mapping a source id does not invent catalog identity"
    );
    drop(before);
    client
        .delete_documents(pb::DeleteDocumentsRequest {
            doc_ids: vec![0],
            ..Default::default()
        })
        .await
        .unwrap();
    client
        .compact_shard(pb::CompactShardRequest {
            work_dir: root.join("compact").display().to_string(),
            ..Default::default()
        })
        .await
        .unwrap();
    drop(client);
    drop(coordinator);
    server.abort();
    let _ = server.await;
    let stored = pipestream_search::postings::Bm25Store::load(
        &pipestream_search::node::generation_bm25(&pipestream_search::node::generation_dir(&path)),
    )
    .unwrap();
    assert_eq!(
        stored.protobuf_source(0).unwrap().unwrap().0.payload,
        retained
    );
    assert!(stored.document_identity(0).is_none());
    assert_eq!(
        stored.unsigned_integer_value(stored.unsigned_integer_index("key").unwrap(), 0),
        Some(8)
    );
    assert_eq!(stored.binding().unwrap().plan_fingerprint, plan.fingerprint);
    let contract = pipestream_search::index_contract::decode(
        &stored.binding().unwrap().index_contract,
        &plan.fingerprint,
    )
    .unwrap()
    .expect("compaction must retain the explicit policy");
    assert_eq!(contract.index_definition, plan.index_definition);
    assert_eq!(contract.message_type, "explicit.Record");
    let generation =
        pipestream_search::reshard::resolve_gen(&pipestream_search::wal::wal_dir(&path)).unwrap();
    assert_eq!(
        pipestream_search::reshard::read_generation_binding(&generation)
            .unwrap()
            .as_ref(),
        stored.binding(),
    );
    assert!(stored.field_index("private_notes").is_none());
    let output_dir = root.join("replayed");
    let handle = tokio::runtime::Handle::current();
    let output = tokio::task::spawn_blocking(move || {
        let mut analyze = |docs: &[(
            &str,
            Option<&pb::AnalysisSpec>,
            pipestream_search::analyzer::SessionLayers,
        )]| {
            handle
                .block_on(pipestream_search::analyzer::analyze_batch(
                    pipestream_search::analyzer::NATIVE_ANALYSIS_BACKEND,
                    docs,
                ))
                .map_err(|error| error.to_string())
        };
        pipestream_search::reshard::merge(
            &[generation],
            &output_dir,
            None,
            false,
            Some(&["body".into()]),
            &mut analyze,
        )
    })
    .await
    .unwrap()
    .unwrap();
    assert_eq!(output.children.len(), 1);
    let child = pipestream_search::postings::Bm25Store::load(
        output.children[0].bm25_path.as_ref().unwrap(),
    )
    .unwrap();
    assert_eq!(child.binding(), stored.binding());
    assert_eq!(
        child.protobuf_source(0).unwrap(),
        stored.protobuf_source(0).unwrap()
    );
    assert_eq!(child.doc_count(), 1);
    drop(child);
    planning_server.abort();
    let _ = planning_server.await;
    drop(stored);
    std::fs::remove_dir_all(root).unwrap();
}
