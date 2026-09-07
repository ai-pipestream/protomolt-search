mod common;

use pipestream_search::{
    analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND},
    node::{self, Layout, NodeConfig},
    pb::{self, node_service_client::NodeServiceClient},
    postings::{Bm25Index, Bm25Reader},
    segments::OpenedSegmentSet,
};
use std::collections::BTreeMap;
use std::path::Path;

fn verify_disk(index: &Path, layout: Layout, expected: &BTreeMap<String, Option<u64>>) {
    let mut actual = BTreeMap::new();
    let mut inspect = |reader: &Bm25Reader| {
        reader.verify_integrity().unwrap();
        let column = reader.unsigned_integer_index("value").unwrap();
        let missing = reader
            .unsigned_integer_index("missing")
            .expect("declared absent column retained");
        let signed_missing = reader.integer_index("signed_missing").unwrap();
        let facet_missing = reader.facet_index("facet_missing").unwrap();
        let numeric_missing = reader.numeric_index("numeric_missing").unwrap();
        let geo_missing = reader.geo_index("geo_missing").unwrap();
        assert!(reader.map_facet_index("map_facet_missing").is_some());
        assert!(reader.map_numeric_index("map_numeric_missing").is_some());
        let signed_map = reader.map_integer_index("signed_map").unwrap();
        let unsigned_map = reader.map_unsigned_integer_index("unsigned_map").unwrap();
        assert!(reader
            .map_integer_keys(reader.map_integer_index("signed_map_missing").unwrap())
            .is_empty());
        assert!(reader
            .map_unsigned_integer_keys(
                reader
                    .map_unsigned_integer_index("unsigned_map_missing")
                    .unwrap()
            )
            .is_empty());
        for row in 0..reader.next_doc_id() {
            let text = reader.text(row).unwrap();
            let value = expected[&text];
            assert_eq!(
                reader
                    .map_integer_key_ord(signed_map, "")
                    .and_then(|key| reader.map_integer_value(signed_map, key, row)),
                value.map(|v| v as i64)
            );
            assert_eq!(
                reader
                    .map_unsigned_integer_key_ord(unsigned_map, "")
                    .and_then(|key| reader.map_unsigned_integer_value(unsigned_map, key, row)),
                value
            );
            assert_eq!(
                reader.document_identity(row).unwrap().document_key,
                text.as_bytes()
            );
            assert_eq!(
                reader.protobuf_source(row).unwrap(),
                Some((common::protobuf_source(&text, "original"), None))
            );
            assert_eq!(reader.integer_value(signed_missing, row), None);
            assert_eq!(reader.facet_ord(facet_missing, row), None);
            assert_eq!(reader.numeric_value(numeric_missing, row), None);
            assert_eq!(reader.geo_value(geo_missing, row), None);
            assert_eq!(reader.unsigned_integer_value(missing, row), None);
            assert_eq!(
                reader.integer_value(reader.integer_index("signed").unwrap(), row),
                Some(i64::MIN)
            );
            assert!(actual
                .insert(
                    reader.text(row).unwrap(),
                    reader.unsigned_integer_value(column, row)
                )
                .is_none());
        }
    };
    match layout {
        Layout::SingleImage => {
            let generation = node::generation_dir(index);
            let path = if generation.exists() {
                node::generation_bm25(&generation)
            } else {
                node::bm25_sidecar_path(index)
            };
            inspect(&Bm25Reader::open(&path).unwrap());
        }
        Layout::Segments => {
            let set = OpenedSegmentSet::open(node::segments_root(index)).unwrap();
            for part in 0..set.len() {
                inspect(set.bm25(part));
            }
        }
    }
    assert_eq!(&actual, expected);
}

#[tokio::test]
async fn exact_integer_grpc_values_survive_flush_reopen_and_compaction() {
    let values = [
        Some(0),
        None,
        Some(1),
        Some((1u64 << 53) + 1),
        Some(i64::MAX as u64),
        Some(1u64 << 63),
        Some(u64::MAX),
        Some(u64::MAX - 1),
    ];
    let vectors = common::unit_vectors(values.len(), 8, 99441);
    let (shift, scale) = common::fit_calibration(8, 4, &common::unit_vectors(64, 8, 99442));
    for (label, layout) in [
        ("single", Layout::SingleImage),
        ("segments", Layout::Segments),
    ] {
        let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("unsigned_ingest_{label}_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let index = dir.join("shard.tv");
        let config = NodeConfig {
            index_path: Some(index.clone()),
            layout,
            wal: true,
            wal_buckets: 2,
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
            unsigned_integer_fields: vec!["value".into(), "missing".into()],
            map_integer_fields: vec!["signed_map".into(), "signed_map_missing".into()],
            map_unsigned_integer_fields: vec!["unsigned_map".into(), "unsigned_map_missing".into()],
            integer_fields: vec!["signed".into(), "signed_missing".into()],
            facet_fields: vec!["facet_missing".into()],
            numeric_fields: vec!["numeric_missing".into()],
            geo_fields: vec!["geo_missing".into()],
            map_facet_fields: vec!["map_facet_missing".into()],
            map_numeric_fields: vec!["map_numeric_missing".into()],
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
        assert_eq!(
            client
                .health(pb::HealthRequest {})
                .await
                .unwrap()
                .into_inner()
                .document_contract_version,
            1
        );
        let mut expected = BTreeMap::new();
        for range in [0..4, 4..values.len()] {
            for fields in [vec!["undeclared"], vec!["value", "value"], vec!["signed"]] {
                let before = client
                    .health(pb::HealthRequest {})
                    .await
                    .unwrap()
                    .into_inner()
                    .document_slots;
                let bad = pb::AddDocumentsRequest {
                    text: "rejected".into(),
                    analysis: Some(body_spec()),
                    unsigned_integers: fields
                        .into_iter()
                        .map(|field| pb::UnsignedIntegerValue {
                            field: field.into(),
                            value: u64::MAX,
                        })
                        .collect(),
                    ..Default::default()
                };
                assert_eq!(
                    client
                        .add_documents(tokio_stream::iter([bad]))
                        .await
                        .unwrap_err()
                        .code(),
                    tonic::Code::InvalidArgument
                );
                assert_eq!(
                    client
                        .health(pb::HealthRequest {})
                        .await
                        .unwrap()
                        .into_inner()
                        .document_slots,
                    before
                );
            }
            for unsigned in [false, true] {
                let own = if unsigned {
                    "unsigned_map"
                } else {
                    "signed_map"
                };
                let other = if unsigned {
                    "signed_map"
                } else {
                    "unsigned_map"
                };
                for fields in [vec!["undeclared"], vec![own, own], vec![other]] {
                    let before = client
                        .health(pb::HealthRequest {})
                        .await
                        .unwrap()
                        .into_inner()
                        .document_slots;
                    let mut bad = pb::AddDocumentsRequest {
                        text: "rejected typed map".into(),
                        analysis: Some(body_spec()),
                        ..Default::default()
                    };
                    for field in fields {
                        if unsigned {
                            bad.map_unsigned_integers.push(pb::MapUnsignedIntegerEntry {
                                field: field.into(),
                                key: "".into(),
                                value: u64::MAX,
                            });
                        } else {
                            bad.map_integers.push(pb::MapIntegerEntry {
                                field: field.into(),
                                key: "".into(),
                                value: i64::MIN,
                            });
                        }
                    }
                    assert_eq!(
                        client
                            .add_documents(tokio_stream::iter([bad]))
                            .await
                            .unwrap_err()
                            .code(),
                        tonic::Code::InvalidArgument
                    );
                    assert_eq!(
                        client
                            .health(pb::HealthRequest {})
                            .await
                            .unwrap()
                            .into_inner()
                            .document_slots,
                        before
                    );
                }
            }
            let docs: Vec<_> = range
                .clone()
                .map(|row| {
                    let text = format!("word row{row}");
                    expected.insert(text.clone(), values[row]);
                    pb::AddDocumentsRequest {
                        original_source: Some(common::protobuf_source(&text, "original")),
                        identity: Some(pb::DocumentIdentity {
                            document_key: text.as_bytes().to_vec(),
                            version: 1,
                            chunk_ordinal: None,
                        }),
                        map_integers: values[row]
                            .map(|value| pb::MapIntegerEntry {
                                field: "signed_map".into(),
                                key: String::new(),
                                value: value as i64,
                            })
                            .into_iter()
                            .collect(),
                        map_unsigned_integers: values[row]
                            .map(|value| pb::MapUnsignedIntegerEntry {
                                field: "unsigned_map".into(),
                                key: String::new(),
                                value,
                            })
                            .into_iter()
                            .collect(),
                        text,
                        analysis: Some(body_spec()),
                        integers: vec![pb::IntegerValue {
                            field: "signed".into(),
                            value: i64::MIN,
                        }],
                        unsigned_integers: values[row]
                            .map(|value| pb::UnsignedIntegerValue {
                                field: "value".into(),
                                value,
                            })
                            .into_iter()
                            .collect(),
                        ..Default::default()
                    }
                })
                .collect();
            let response = client
                .add_documents(tokio_stream::iter(docs))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(response.document_contract_version, 1);
            client
                .add_vectors(tokio_stream::iter([pb::AddVectorsRequest {
                    vectors: vectors[range.start * 8..range.end * 8].to_vec(),
                    dim: 8,
                }]))
                .await
                .unwrap();
            client.flush(pb::FlushRequest {}).await.unwrap();
            verify_disk(&index, layout, &expected);
        }
        server.abort();
        let _ = server.await;
        drop(client);
        let (addr, server) = common::start_opened_node(config.clone()).await;
        let mut client = NodeServiceClient::connect(addr).await.unwrap();
        verify_disk(&index, layout, &expected);
        client
            .delete_documents(pb::DeleteDocumentsRequest {
                doc_ids: vec![1],
                ..Default::default()
            })
            .await
            .unwrap();
        expected.remove("word row1");
        for turn in 0..2 {
            client
                .compact_shard(pb::CompactShardRequest {
                    work_dir: dir.join(format!("compact-{turn}")).display().to_string(),
                    ..Default::default()
                })
                .await
                .unwrap();
            verify_disk(&index, layout, &expected);
        }
        let generation = client
            .health(pb::HealthRequest {})
            .await
            .unwrap()
            .into_inner()
            .wal_generation;
        let gen_dir =
            pipestream_search::wal::gen_dir(&pipestream_search::wal::wal_dir(&index), generation);
        let mut analyze = |docs: &[(
            &str,
            Option<&pb::AnalysisSpec>,
            pipestream_search::analyzer::SessionLayers,
        )]| {
            docs.iter()
                .map(|(text, spec, _)| {
                    pipestream_search::analyzer::analyze_document_native(text, *spec)
                        .map_err(|e| e.to_string())
                })
                .collect()
        };
        let split = pipestream_search::reshard::split_logs(
            &[gen_dir],
            2,
            &dir.join("split"),
            0,
            1000,
            false,
            Some(&["body".into()]),
            &mut analyze,
        )
        .unwrap();
        let mut replayed = BTreeMap::new();
        for child in split.children {
            if let Some(path) = child.bm25_path {
                let reader = Bm25Reader::open(&path).unwrap();
                let column = reader.unsigned_integer_index("value");
                for row in 0..reader.next_doc_id() {
                    let text = reader.text(row).unwrap();
                    let value = expected[&text];
                    assert_eq!(
                        reader.map_integer_index("signed_map").and_then(|ci| reader
                            .map_integer_key_ord(ci, "")
                            .and_then(|key| reader.map_integer_value(ci, key, row))),
                        value.map(|v| v as i64)
                    );
                    assert_eq!(
                        reader
                            .map_unsigned_integer_index("unsigned_map")
                            .and_then(|ci| reader
                                .map_unsigned_integer_key_ord(ci, "")
                                .and_then(|key| reader.map_unsigned_integer_value(ci, key, row))),
                        value
                    );
                    assert_eq!(
                        reader.document_identity(row).unwrap().document_key,
                        text.as_bytes()
                    );
                    assert_eq!(
                        reader.protobuf_source(row).unwrap(),
                        Some((common::protobuf_source(&text, "original"), None))
                    );
                    assert!(replayed
                        .insert(
                            reader.text(row).unwrap(),
                            column.and_then(|column| reader.unsigned_integer_value(column, row))
                        )
                        .is_none());
                }
            }
        }
        assert_eq!(replayed, expected);
        server.abort();
        let _ = server.await;
        drop(client);
        let (_, server) = common::start_opened_node(config).await;
        verify_disk(&index, layout, &expected);
        server.abort();
        let _ = server.await;
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[tokio::test]
async fn direct_node_configuration_refuses_column_type_aliases() {
    for mut config in [
        NodeConfig {
            integer_fields: vec!["value".into()],
            ..Default::default()
        },
        NodeConfig {
            numeric_fields: vec!["value".into()],
            ..Default::default()
        },
        NodeConfig {
            map_integer_fields: vec!["value".into()],
            ..Default::default()
        },
        NodeConfig {
            map_unsigned_integer_fields: vec!["value".into()],
            ..Default::default()
        },
        NodeConfig {
            placement_column: Some("value".into()),
            ..Default::default()
        },
        NodeConfig {
            unsigned_integer_fields: vec!["value".into()],
            ..Default::default()
        },
    ] {
        config.unsigned_integer_fields.push("value".into());
        config.analysis_addr = Some(NATIVE_ANALYSIS_BACKEND.into());
        assert!(node::NodeServiceImpl::open(config.clone(), None, false)
            .err()
            .unwrap()
            .contains("declared more than once"));
        let (addr, server) = common::start_empty_node(config).await;
        let mut client = NodeServiceClient::connect(addr).await.unwrap();
        let doc = pb::AddDocumentsRequest {
            text: "word".into(),
            analysis: Some(body_spec()),
            unsigned_integers: vec![pb::UnsignedIntegerValue {
                field: "value".into(),
                value: u64::MAX,
            }],
            ..Default::default()
        };
        let error = client
            .add_documents(tokio_stream::iter([doc]))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.message().contains("declared more than once"));
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
