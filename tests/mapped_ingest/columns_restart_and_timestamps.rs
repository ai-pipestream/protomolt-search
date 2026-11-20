//! Materialized columns, restart binding persistence, and WKT Timestamp projection.

use super::*;

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
    send(
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

// The durable shard-level binding

/// The same schema minus the (FAMILY_NONE) `tags` field: lands on the
/// same declared columns but derives a different plan fingerprint.
fn case_set_without_tags() -> Vec<u8> {
    let mut set = FileDescriptorSet::decode(&case_set()[..]).unwrap();
    set.file[1].message_type[0]
        .field
        .retain(|f| f.name() != "tags");
    set.encode_to_vec()
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

    // The wider bm25_fields table (title added) is only there to let the
    // body-path probe after the restart reach the BINDING check instead of
    // the declared-columns check; the catalog keeps the same table.
    let mut config = case_node_config(analysis.clone());
    config.index_path = Some(index_path.clone());
    config.bm25_fields.push("title".into());
    let (addr, node) = start_empty_node(config).await;
    seed_calibration(&addr).await;
    let mut wrong_wire_title = doc(2).encode();
    wrong_wire_title.extend([0x10, 0x01]);
    let original_documents = vec![doc(0).encode(), doc(1).encode(), wrong_wire_title];
    let response = send(&addr, bind(), original_documents.clone())
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
    let bm25_path = common::catalog::segment_bm25(&index_path, 0);
    let store = pipestream_search::postings::Bm25Store::load(&bm25_path).unwrap();
    assert_eq!(store.binding(), Some(&expected_binding()));
    for row in 0..3 {
        assert_eq!(
            store.protobuf_source(row).unwrap(),
            Some((
                ProtobufSource {
                    descriptor_set: case_set(),
                    message_type: "law.v1.Case".into(),
                    payload: original_documents[row as usize].clone(),
                },
                None
            ))
        );
    }

    // Restart from disk, no in-memory state carried over.
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = format!("http://{}", listener.local_addr().unwrap());
    let mut config = case_node_config(analysis);
    config.index_path = Some(index_path.clone());
    config.bm25_fields.push("title".into());
    let service = pipestream_search::node::NodeServiceImpl::open(config, None).unwrap();
    let _node2 = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(service.into_server(pipestream_search::MAX_MESSAGE_BYTES))
            .serve_with_incoming(pipestream_search::harness::nodelay_incoming(listener)),
    );

    // Same plan: still bound, ids continue.
    let more = send(&addr2, bind(), vec![doc(3).encode()])
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
    expect_refusal(send(&addr2, other_plan, vec![]).await, "durably bound");

    // Same plan, different body.
    let mut other_body = bind();
    other_body.body_path = "meta.author".into();
    expect_refusal(send(&addr2, other_body, vec![]).await, "the body");

    // Same plan, a materialize spec the binding does not carry.
    let mut other_spec = bind();
    other_spec.materialize = Some(MaterializeSpec {
        columns: vec![MaterializedColumn {
            name: "price2".into(),
            expression: "price * 2.0".into(),
            kind: MaterializeKind::F64 as i32,
        }],
    });
    expect_refusal(send(&addr2, other_spec, vec![]).await, "materialize spec");

    let _ = std::fs::remove_dir_all(&dir);
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
        let error = derive_plan(&bytes, "law.v1.Case").expect_err("invalid Timestamp shape");
        assert!(matches!(error, MappingError::InvalidArgument(_)));
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
        assert!(matches!(error, MappingError::InvalidArgument(_)));
        assert!(error.message().contains("created_at"), "{error}");
    }
}
