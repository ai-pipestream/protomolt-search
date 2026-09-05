mod common;

use pipestream_search::{
    analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND},
    coordinator::CoordinatorServiceImpl,
    node::{Layout, NodeConfig},
    pb::{self, node_service_client::NodeServiceClient, search_service_server::SearchService},
};
use prost::Message;

const VALUES: [Option<u64>; 10] = [
    Some(u64::MAX),
    Some(0),
    Some(1 << 63),
    Some((1 << 53) + 1),
    None,
    Some(u64::MAX - 1),
    Some(0),
    Some(u64::MAX),
    Some(1),
    Some((1 << 53) + 2),
];
fn lexical() -> pb::SelectionQuery {
    pb::SelectionQuery {
        node: Some(pb::selection_query::Node::Search(pb::SearchQuery {
            id: "words".into(),
            query: Some(pb::search_query::Query::Lexical(pb::LexicalQuery {
                text: "word".into(),
                analysis: Some(body_spec()),
                ..Default::default()
            })),
        })),
    }
}
fn browse() -> pb::SelectionQuery {
    pb::SelectionQuery {
        node: Some(pb::selection_query::Node::Filter(pb::FilterQuery {
            id: "all".into(),
            predicate: Some(pb::filter_query::Predicate::Cel(
                "has(value) || !has(value)".into(),
            )),
        })),
    }
}
fn projection() -> pb::NamedProjection {
    pb::NamedProjection {
        name: "key".into(),
        expression: "key".into(),
    }
}
fn row_of(hit: &pb::QueryHit) -> usize {
    let Some(pb::projected_value::Value::StringValue(key)) = &hit.projected[0].value else {
        panic!("missing key")
    };
    key.parse().unwrap()
}
fn request(selection: pb::SelectionQuery) -> pb::QueryRequest {
    pb::QueryRequest {
        k: 100,
        selection: Some(selection),
        projections: vec![projection()],
        ..Default::default()
    }
}
async fn verify(coordinator: &CoordinatorServiceImpl) {
    let baseline = coordinator
        .query(tonic::Request::new(request(lexical())))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(baseline.hits.len(), VALUES.len());
    for column in ["value", "parent_id", "group_id"] {
        for desc in [false, true] {
            for selection in [browse(), lexical()] {
                let mut req = request(selection);
                req.sort = vec![
                    pb::QuerySort {
                        column: column.into(),
                        descending: desc,
                    },
                    pb::QuerySort {
                        column: "key".into(),
                        descending: !desc,
                    },
                ];
                let mut expected: Vec<_> = baseline
                    .hits
                    .iter()
                    .filter_map(|h| {
                        let row = row_of(h);
                        let value = if column == "value" {
                            VALUES[row]?
                        } else {
                            VALUES[row].unwrap_or(42)
                        };
                        Some((row, value, h.doc_id))
                    })
                    .collect();
                expected.sort_by(|a, b| {
                    let primary = if desc { b.1.cmp(&a.1) } else { a.1.cmp(&b.1) };
                    primary
                        .then_with(|| {
                            if !desc {
                                b.0.to_string().cmp(&a.0.to_string())
                            } else {
                                a.0.to_string().cmp(&b.0.to_string())
                            }
                        })
                        .then(a.2.cmp(&b.2))
                });
                let full = coordinator
                    .query(tonic::Request::new(req.clone()))
                    .await
                    .unwrap()
                    .into_inner();
                assert_eq!(
                    full.hits.iter().map(row_of).collect::<Vec<_>>(),
                    expected.iter().map(|v| v.0).collect::<Vec<_>>()
                );
                for (hit, (_, value, _)) in full.hits.iter().zip(&expected) {
                    assert_eq!(
                        hit.sort_values[0].value,
                        Some(pb::sort_value::Value::UnsignedInteger(*value))
                    );
                    assert_eq!(hit.sort_key, *value as f64); // Legacy display only, never the order oracle.
                }
                req.k = 3;
                let mut paged = Vec::new();
                loop {
                    let response = coordinator
                        .query(tonic::Request::new(req.clone()))
                        .await
                        .unwrap()
                        .into_inner();
                    paged.extend(response.hits.iter().map(|h| (row_of(h), h.rank)));
                    if response.next_cursor.is_empty() {
                        break;
                    }
                    assert!(
                        response.next_cursor.contains(":u"),
                        "unsigned cursor encoding"
                    );
                    req.cursor = response.next_cursor;
                    assert!(paged.len() <= VALUES.len());
                }
                assert_eq!(
                    paged,
                    expected
                        .iter()
                        .enumerate()
                        .map(|(i, v)| (v.0, i as u32 + 1))
                        .collect::<Vec<_>>()
                );
            }
        }
        let mut expected: Vec<(u64, Vec<u64>)> = Vec::new();
        for hit in &baseline.hits {
            let row = row_of(hit);
            let value = if column == "value" {
                let Some(v) = VALUES[row] else { continue };
                v
            } else {
                VALUES[row].unwrap_or(42)
            };
            if let Some((_, ids)) = expected.iter_mut().find(|(v, _)| *v == value) {
                ids.push(hit.doc_id);
            } else {
                expected.push((value, vec![hit.doc_id]));
            }
        }
        let mut req = request(lexical());
        req.selection_k = 100;
        req.collapse = Some(pb::CollapseSpec {
            column: column.into(),
            inner_hits: 100,
        });
        let response = coordinator
            .query(tonic::Request::new(req.clone()))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(response.groups.len(), expected.len());
        for (group, (value, ids)) in response.groups.iter().zip(&expected) {
            assert_eq!(
                group.key.as_ref().unwrap().value,
                Some(pb::sort_value::Value::UnsignedInteger(*value))
            );
            assert_eq!(
                group.hits.iter().map(|h| h.doc_id).collect::<Vec<_>>(),
                *ids
            );
            assert!(group.complete);
        }
        req.k = 2;
        let mut paged = Vec::new();
        loop {
            let response = coordinator
                .query(tonic::Request::new(req.clone()))
                .await
                .unwrap()
                .into_inner();
            paged.extend(
                response
                    .groups
                    .into_iter()
                    .map(|g| g.key.unwrap().value.unwrap()),
            );
            if response.next_cursor.is_empty() {
                break;
            }
            req.cursor = response.next_cursor;
            assert!(paged.len() <= expected.len());
        }
        assert_eq!(
            paged,
            expected
                .iter()
                .map(|(v, _)| pb::sort_value::Value::UnsignedInteger(*v))
                .collect::<Vec<_>>()
        );
    }
}

#[tokio::test]
async fn unsigned_sort_collapse_and_cursors_survive_reopen_and_compaction() {
    let (shift, scale) = common::fit_calibration(8, 4, &common::unit_vectors(64, 8, 37195));
    for layout in [Layout::SingleImage, Layout::Segments] {
        let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("unsigned_order_{layout:?}_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let mut configs = Vec::new();
        let mut addresses = Vec::new();
        let mut servers = Vec::new();
        for range in [0..5, 5..VALUES.len()] {
            let config = NodeConfig {
                index_path: Some(dir.join(format!("{}.tv", range.start))),
                layout,
                wal: true,
                wal_buckets: 2,
                seal_tail_docs: 2,
                slot_offset: range.start as u64,
                analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
                unsigned_integer_fields: vec!["value".into()],
                facet_fields: vec!["key".into()],
                ..Default::default()
            };
            let (address, server) = common::start_empty_node(config.clone()).await;
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
            for row in range {
                client
                    .add_documents(tokio_stream::iter([pb::AddDocumentsRequest {
                        text: "word".into(),
                        analysis: Some(body_spec()),
                        facets: vec![pb::FacetValue {
                            field: "key".into(),
                            value: row.to_string(),
                        }],
                        unsigned_integers: VALUES[row]
                            .map(|value| pb::UnsignedIntegerValue {
                                field: "value".into(),
                                value,
                            })
                            .into_iter()
                            .collect(),
                        lineage: Some(pb::DocLineage {
                            parent_id: VALUES[row].unwrap_or(42),
                            group_id: VALUES[row].unwrap_or(42),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }]))
                    .await
                    .unwrap();
                client
                    .add_vectors(tokio_stream::iter([pb::AddVectorsRequest {
                        dim: 8,
                        vectors: common::unit_vectors(1, 8, 57890 + row as u64),
                    }]))
                    .await
                    .unwrap();
            }
            client.flush(pb::FlushRequest {}).await.unwrap();
            configs.push(config);
            addresses.push(address);
            servers.push(server);
        }
        verify(
            &CoordinatorServiceImpl::new(addresses)
                .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default()),
        )
        .await;
        for server in servers.drain(..) {
            server.abort();
            let _ = server.await;
        }
        for round in 0..2 {
            let mut addresses = Vec::new();
            for (index, config) in configs.iter().enumerate() {
                let (address, server) = common::start_opened_node(config.clone()).await;
                let mut client = NodeServiceClient::connect(address.clone()).await.unwrap();
                if round == 0 {
                    client
                        .compact_shard(pb::CompactShardRequest {
                            work_dir: dir.join(format!("compact-{index}")).display().to_string(),
                            ..Default::default()
                        })
                        .await
                        .unwrap();
                }
                addresses.push(address);
                servers.push(server);
            }
            verify(
                &CoordinatorServiceImpl::new(addresses)
                    .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default()),
            )
            .await;
            for server in servers.drain(..) {
                server.abort();
                let _ = server.await;
            }
        }
        std::fs::remove_dir_all(dir).unwrap();
    }
}

#[test]
fn unsigned_keys_and_reported_values_round_trip_without_narrowing() {
    use pipestream_search::sortkeys::{self, Key, Value};
    for malformed in ["t€0", "tzz", "u18446744073709551616", "u", "u-1"] {
        assert!(sortkeys::decode_keys(malformed).is_none(), "{malformed}");
    }
    for n in [0, (1 << 53) + 1, 1 << 63, u64::MAX] {
        let key = Key::UnsignedBits(n);
        let wire = key.to_pb();
        let decoded = pb::SortKey::decode(wire.encode_to_vec().as_slice()).unwrap();
        assert_eq!(Key::from_pb(&decoded), Some(key.clone()));
        assert_eq!(
            sortkeys::decode_keys(&sortkeys::encode_keys(&[key.clone()])),
            Some(vec![key])
        );
        let wire = Value::UnsignedInteger(n).to_pb();
        let decoded = pb::SortValue::decode(wire.encode_to_vec().as_slice()).unwrap();
        assert_eq!(
            sortkeys::value_from_pb(&decoded),
            Some(Value::UnsignedInteger(n))
        );
    }
}

#[tokio::test]
async fn incompatible_shard_types_refuse_even_when_no_rows_match() {
    let mut addresses = Vec::new();
    let mut servers = Vec::new();
    for unsigned in [false, true] {
        let config = NodeConfig {
            slot_offset: if unsigned { 10 } else { 0 },
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
            integer_fields: if unsigned {
                vec![]
            } else {
                vec!["value".into()]
            },
            unsigned_integer_fields: if unsigned {
                vec!["value".into()]
            } else {
                vec![]
            },
            facet_fields: vec!["key".into()],
            ..Default::default()
        };
        let (address, server) = common::start_empty_node(config).await;
        let mut client = NodeServiceClient::connect(address.clone()).await.unwrap();
        client
            .add_documents(tokio_stream::iter([pb::AddDocumentsRequest {
                text: "word".into(),
                analysis: Some(body_spec()),
                facets: vec![pb::FacetValue {
                    field: "key".into(),
                    value: "found".into(),
                }],
                integers: if unsigned {
                    vec![]
                } else {
                    vec![pb::IntegerValue {
                        field: "value".into(),
                        value: 1,
                    }]
                },
                unsigned_integers: if unsigned {
                    vec![pb::UnsignedIntegerValue {
                        field: "value".into(),
                        value: u64::MAX,
                    }]
                } else {
                    vec![]
                },
                ..Default::default()
            }]))
            .await
            .unwrap();
        addresses.push(address);
        servers.push(server);
    }
    let coordinator = CoordinatorServiceImpl::new(addresses.clone())
        .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default());
    for filter in ["key == 'found'", "key == 'missing'"] {
        let mut req = request(pb::SelectionQuery {
            node: Some(pb::selection_query::Node::Filter(pb::FilterQuery {
                id: "match".into(),
                predicate: Some(pb::filter_query::Predicate::Cel(filter.into())),
            })),
        });
        req.sort = vec![pb::QuerySort {
            column: "value".into(),
            descending: false,
        }];
        let error = coordinator
            .query(tonic::Request::new(req))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(
            error.message().contains("incompatible types across shards"),
            "{error}"
        );
    }
    let compiled = vec![pb::CompiledProjection {
        name: "value".into(),
        expr: Some(pipestream_search::cel::compile_value("value").unwrap()),
    }];
    for ids in [&[][..], &[0, 10][..]] {
        let error = coordinator
            .fetch_values(ids, &compiled, &[])
            .await
            .err()
            .unwrap();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(
            error.message().contains("incompatible types across shards"),
            "{error}"
        );
    }
    for filter in ["key == 'found'", "key == 'missing'"] {
        for percentile in [false, true] {
            let req = pb::AggregateRequest {
                filter: filter.into(),
                aggregations: if percentile {
                    vec![]
                } else {
                    vec![pb::Aggregation {
                        name: "count".into(),
                        expression: "value".into(),
                        op: pb::AggregateOp::Count as i32,
                        ..Default::default()
                    }]
                },
                percentiles: if percentile {
                    vec![pb::PercentileSpec {
                        name: "p".into(),
                        expression: "value".into(),
                        percentiles: vec![50.0],
                    }]
                } else {
                    vec![]
                },
                ..Default::default()
            };
            let error = coordinator
                .aggregate(tonic::Request::new(req))
                .await
                .unwrap_err();
            assert_eq!(error.code(), tonic::Code::FailedPrecondition);
            assert!(error.message().contains("shards disagree"), "{error}");
        }
    }
    let mut req = request(lexical());
    req.collapse = Some(pb::CollapseSpec {
        column: "value".into(),
        inner_hits: 2,
    });
    let error = coordinator
        .query(tonic::Request::new(req))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(
        error.message().contains("incompatible types across shards"),
        "{error}"
    );

    // A legacy signed/double cursor key cannot resume an unsigned column.
    let mut client = NodeServiceClient::connect(addresses[1].clone())
        .await
        .unwrap();
    let error = client
        .browse_shard(pb::BrowseShardRequest {
            k: 1,
            first_page: false,
            after: 10,
            sort: vec![pb::BrowseSort {
                column: "value".into(),
                descending: false,
            }],
            after_keys: vec![pb::SortKey {
                key: Some(pb::sort_key::Key::Bits(0)),
            }],
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("boundary key type"), "{error}");
    for server in servers {
        server.abort();
        let _ = server.await;
    }
}
