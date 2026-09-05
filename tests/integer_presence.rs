mod common;

use pipestream_search::postings::{AnalyzedDoc, Bm25Reader, Bm25Store, SpillBuilder};

fn directory(name: &str) -> std::path::PathBuf {
    let path = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("integer_presence_{name}_{}", std::process::id()));
    std::fs::create_dir_all(&path).unwrap();
    path
}

fn analyzed() -> AnalyzedDoc {
    AnalyzedDoc::body(vec![("word".into(), 1, vec![(0, 4)])], 1)
}

#[test]
fn full_signed_domain_and_missing_values_survive_both_writers_and_readers() {
    let dir = directory("roundtrip");
    for slots in [0, 1, 7, 8, 9, 65] {
        let expected: Vec<_> = (0..slots)
            .map(|row| match row % 6 {
                0 => Some(i64::MIN),
                1 => None,
                2 => Some(0),
                3 => Some(i64::MAX),
                4 => Some(-1),
                _ => Some((1i64 << 53) + 1),
            })
            .collect();
        let mut heap = Bm25Store::with_fields(&["body"]).with_integers(&["value", "absent"]);
        let mut spill =
            SpillBuilder::create_with_fields(&dir.join(format!("spill-{slots}.build")), &["body"])
                .unwrap()
                .with_integer_fields(&["value", "absent"])
                .with_buffer_bytes(32);
        for (row, value) in expected.iter().enumerate() {
            heap.add_document(row as u32, "word".into(), analyzed());
            spill
                .add_document_with_lineage(row as u32, "word".into(), analyzed(), None)
                .unwrap();
            if let Some(value) = value {
                heap.set_integer(0, row as u32, *value);
                spill.set_integer(0, row as u32, *value);
            }
        }
        let heap_path = dir.join(format!("heap-{slots}.bm25"));
        let spill_path = dir.join(format!("spill-{slots}.bm25"));
        heap.save(&heap_path).unwrap();
        spill.finish(&spill_path).unwrap();
        assert_eq!(
            std::fs::read(&heap_path).unwrap(),
            std::fs::read(&spill_path).unwrap()
        );
        let loaded = Bm25Store::load(&heap_path).unwrap();
        let mapped = Bm25Reader::open(&heap_path).unwrap();
        for (row, expected) in expected.iter().enumerate() {
            assert_eq!(heap.integer_value(0, row as u32), *expected);
            assert_eq!(loaded.integer_value(0, row as u32), *expected);
            assert_eq!(mapped.integer_value(0, row as u32), *expected);
            assert_eq!(mapped.integer_value(1, row as u32), None);
        }
        let range = (
            expected.iter().flatten().min().copied().unwrap_or(i64::MAX),
            expected.iter().flatten().max().copied().unwrap_or(i64::MIN),
        );
        assert_eq!(mapped.integer_min_max(0), range);
        assert_eq!(loaded.integer_min_max(0), range);
        assert_eq!(mapped.integer_value(0, slots as u32), None);
        assert_eq!(mapped.integer_min_max(1), (i64::MAX, i64::MIN));
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[test]
fn legacy_sentinel_values_keep_their_original_meaning_when_rewritten() {
    let dir = directory("legacy");
    let legacy = dir.join("legacy.bm25");
    std::fs::write(
        &legacy,
        include_bytes!("fixtures/integer-presence/legacy-kind4.bm25"),
    )
    .unwrap();
    let mut loaded = Bm25Store::load(&legacy).unwrap();
    let mapped = Bm25Reader::open(&legacy).unwrap();
    for (row, value) in [Some(-7), None, Some(0), Some(i64::MAX)].iter().enumerate() {
        assert_eq!(loaded.integer_value(0, row as u32), *value);
        assert_eq!(mapped.integer_value(0, row as u32), *value);
    }
    loaded.add_document(4, "word".into(), analyzed());
    loaded.set_integer(0, 4, i64::MIN);
    let rewritten = dir.join("rewritten.bm25");
    loaded.save(&rewritten).unwrap();
    let mapped = Bm25Reader::open(&rewritten).unwrap();
    assert_eq!(mapped.integer_value(0, 1), None);
    assert_eq!(mapped.integer_value(0, 4), Some(i64::MIN));
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn mapped_numeric_presence_survives_reopen_and_compaction_replay() {
    use pb::{node_service_client::NodeServiceClient, search_service_server::SearchService};
    use pipestream_search::{
        analyzer::NATIVE_ANALYSIS_BACKEND,
        coordinator::CoordinatorServiceImpl,
        mapping::derive_plan,
        node::{Layout, NodeConfig},
        pb,
    };
    use prost::Message;
    use prost_reflect::{DescriptorPool, DynamicMessage, Value};
    const DESCRIPTOR: &[u8] = include_bytes!("fixtures/integer-presence/descriptor.bin");
    let plan = derive_plan(DESCRIPTOR, "integer_presence.Record").unwrap();
    assert!(!plan
        .schema_report
        .as_ref()
        .unwrap()
        .messages
        .iter()
        .flat_map(|m| &m.fields)
        .flat_map(|f| &f.projections)
        .flat_map(|p| &p.constraints)
        .any(|c| c.contains("reserved for absence")));
    let pool = DescriptorPool::decode(DESCRIPTOR).unwrap();
    let descriptor = pool.get_message_by_name("integer_presence.Record").unwrap();
    let values = [
        Some(i64::MIN),
        None,
        Some(0),
        Some(i64::MAX),
        Some(-1),
        Some(i64::MIN + 1),
    ];
    let vectors = common::unit_vectors(values.len(), 8, 6633);
    let (shift, scale) = common::fit_calibration(8, 4, &common::unit_vectors(64, 8, 6634));
    for (label, layout) in [
        ("single", Layout::SingleImage),
        ("segments", Layout::Segments),
    ] {
        let dir = directory(label);
        let config = NodeConfig {
            index_path: Some(dir.join("shard.tv")),
            layout,
            wal: true,
            wal_buckets: 2,
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
            facet_fields: vec!["id".into()],
            integer_fields: vec!["value".into(), "derived".into()],
            ..Default::default()
        };
        let (addr, server) = common::start_empty_node(config.clone()).await;
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
        let bind = pb::MappedBind {
            descriptor_set: DESCRIPTOR.to_vec(),
            message_type: "integer_presence.Record".into(),
            expected_fingerprint: plan.fingerprint.clone(),
            body_path: "body".into(),
            analysis: Some(pipestream_search::analyzer::body_spec()),
            materialize: Some(pb::MaterializeSpec {
                columns: vec![pb::MaterializedColumn {
                    name: "derived".into(),
                    expression: "value".into(),
                    kind: pb::MaterializeKind::I64 as i32,
                }],
            }),
            ..Default::default()
        };
        for range in [0..5, 5..6] {
            let mut messages = vec![pb::IngestMappedRequest {
                payload: Some(pb::ingest_mapped_request::Payload::Bind(bind.clone())),
            }];
            for row in range.clone() {
                let mut doc = DynamicMessage::new(descriptor.clone());
                doc.set_field_by_name("id", Value::String(format!("doc{row}")));
                doc.set_field_by_name("body", Value::String("word".into()));
                doc.set_field_by_name(
                    "embedding",
                    Value::List(
                        vectors[row * 8..row * 8 + 8]
                            .iter()
                            .copied()
                            .map(Value::F32)
                            .collect(),
                    ),
                );
                if let Some(value) = values[row] {
                    doc.set_field_by_name("value", Value::I64(value));
                }
                messages.push(pb::IngestMappedRequest {
                    payload: Some(pb::ingest_mapped_request::Payload::Document(
                        doc.encode_to_vec(),
                    )),
                });
            }
            client
                .ingest_mapped(tokio_stream::iter(messages))
                .await
                .unwrap();
            client.flush(pb::FlushRequest {}).await.unwrap();
        }
        // Flush is the current shard durability boundary; compaction below
        // rebuilds these rows from the WAL into a renumbered generation.
        server.abort();
        let _ = server.await;
        drop(client);
        let (addr, server) = common::start_opened_node(config).await;
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        for compacted in [false, true] {
            if compacted {
                client
                    .compact_shard(pb::CompactShardRequest {
                        work_dir: dir.join("compact-work").display().to_string(),
                        ..Default::default()
                    })
                    .await
                    .unwrap();
            }
            let coordinator = CoordinatorServiceImpl::new(vec![addr.clone()])
                .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default());
            for (row, value) in values.iter().enumerate() {
                let filter = value.map_or("!has(value)".into(), |v| format!("value == {v}"));
                let response = coordinator
                    .bm25_search(tonic::Request::new(pb::Bm25SearchRequest {
                        text: "word".into(),
                        analysis: Some(pipestream_search::analyzer::body_spec()),
                        k: 10,
                        filter,
                        projections: vec![
                            pb::NamedProjection {
                                name: "id".into(),
                                expression: "id".into(),
                            },
                            pb::NamedProjection {
                                name: "copy".into(),
                                expression: "derived".into(),
                            },
                        ],
                        ..Default::default()
                    }))
                    .await
                    .unwrap()
                    .into_inner();
                assert_eq!(
                    response.hits.len(),
                    1,
                    "{label} row {row}, compacted {compacted}"
                );
                assert_eq!(
                    response.hits[0].projected[0].value,
                    Some(pb::projected_value::Value::StringValue(format!("doc{row}")))
                );
                assert_eq!(
                    response.hits[0].projected[1].value,
                    value.map(pb::projected_value::Value::IntValue)
                );
            }
        }
        server.abort();
        let _ = server.await;
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[test]
fn malformed_presence_and_value_bytes_refuse_before_serving() {
    let dir = directory("corrupt");
    let mut store = Bm25Store::with_fields(&["body"]).with_integers(&["value"]);
    for row in 0..9 {
        store.add_document(row, "word".into(), analyzed());
        if row != 1 {
            store.set_integer(0, row, if row == 0 { i64::MIN } else { 0 });
        }
    }
    // Use the unwrapped payload so structural validation is tested independently
    // of CRC rejection. One field table, one integer column, no trailing kinds.
    let mut bytes = Vec::new();
    store.write_v6_to(&mut bytes).unwrap();
    let field_name_len = u16::from_le_bytes(bytes[40..42].try_into().unwrap()) as usize;
    let column_table = 40 + 2 + field_name_len + 40;
    let column = column_table + 4;
    let name_len = u16::from_le_bytes(bytes[column..column + 2].try_into().unwrap()) as usize;
    let kind = column + 2 + name_len;
    assert_eq!(bytes[kind], 10);
    let base = kind + 1;
    let values = u64::from_le_bytes(bytes[base + 16..base + 24].try_into().unwrap()) as usize;
    let bitmap = values + 9 * 8;
    assert_eq!(bytes.len(), bitmap + 2);
    let mut invalid = Vec::new();
    let mut padding = bytes.clone();
    padding[bitmap + 1] |= 0x80;
    invalid.push(("padding", padding));
    let mut absent = bytes.clone();
    absent[values + 8] = 1;
    invalid.push(("absent-value", absent));
    let mut metadata = bytes.clone();
    metadata[base] ^= 1;
    invalid.push(("metadata", metadata));
    for cut in values..bytes.len() {
        invalid.push(("truncated", bytes[..cut].to_vec()));
    }
    for (label, bytes) in invalid {
        let path = dir.join("bad.bm25");
        std::fs::write(&path, bytes).unwrap();
        assert!(Bm25Store::load(&path).is_err(), "heap accepted {label}");
        assert!(Bm25Reader::open(&path).is_err(), "mapped accepted {label}");
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[tokio::test]
async fn old_integer_materialization_bindings_refuse_but_float_bindings_still_match() {
    use pb::node_service_client::NodeServiceClient;
    use pipestream_search::{
        analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND},
        mapping::derive_plan,
        node::NodeConfig,
        pb,
    };
    const DESCRIPTOR: &[u8] = include_bytes!("fixtures/integer-presence/descriptor.bin");
    let plan = derive_plan(DESCRIPTOR, "integer_presence.Record").unwrap();
    // Content hashes produced by materialize_sha at main 816c279, before
    // computed MIN stopped disappearing. Float-only semantics are unchanged.
    for (kind, expression, legacy_hash) in [
        (
            pb::MaterializeKind::I64,
            "value",
            "1bf6cd7a9e13c5aa9f93c2fb2ccc0b563e8311570fb42f231d235f5d641861ea",
        ),
        (
            pb::MaterializeKind::F64,
            "double(value)",
            "73c963afc0751a0bc9433a5b514e9665eabcca488923a941fccdb0e34d46f38f",
        ),
    ] {
        let (integers, numerics) = if kind == pb::MaterializeKind::I64 {
            (vec!["value".into(), "derived".into()], vec![])
        } else {
            (vec!["value".into()], vec!["derived".into()])
        };
        let (addr, node) = common::start_empty_node(NodeConfig {
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
            facet_fields: vec!["id".into()],
            integer_fields: integers,
            numeric_fields: numerics,
            ..Default::default()
        })
        .await;
        let mut client = NodeServiceClient::connect(addr).await.unwrap();
        let (shift, scale) = common::fit_calibration(8, 4, &common::unit_vectors(64, 8, 13533));
        client
            .set_calibration(pb::SetCalibrationRequest {
                dim: 8,
                bit_width: 4,
                shift,
                scale,
            })
            .await
            .unwrap();
        client
            .apply_wal_binding(pb::ApplyWalBindingRequest {
                plan_fingerprint: plan.fingerprint.clone(),
                body_path: "body".into(),
                materialize_sha: legacy_hash.into(),
                ..Default::default()
            })
            .await
            .unwrap();
        let offered = client
            .ingest_mapped(tokio_stream::iter(vec![pb::IngestMappedRequest {
                payload: Some(pb::ingest_mapped_request::Payload::Bind(pb::MappedBind {
                    descriptor_set: DESCRIPTOR.to_vec(),
                    message_type: "integer_presence.Record".into(),
                    expected_fingerprint: plan.fingerprint.clone(),
                    body_path: "body".into(),
                    analysis: Some(body_spec()),
                    materialize: Some(pb::MaterializeSpec {
                        columns: vec![pb::MaterializedColumn {
                            name: "derived".into(),
                            expression: expression.into(),
                            kind: kind as i32,
                        }],
                    }),
                    ..Default::default()
                })),
            }]))
            .await;
        if kind == pb::MaterializeKind::I64 {
            let status = offered.unwrap_err();
            assert_eq!(status.code(), tonic::Code::FailedPrecondition);
            assert!(status.message().contains("materialize spec"));
            assert!(status.message().contains("original documents"));
        } else {
            offered.unwrap();
        }
        node.abort();
        let _ = node.await;
    }
}
