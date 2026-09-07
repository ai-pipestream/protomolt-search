mod common;

use pipestream_search::{
    analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND},
    coordinator::CoordinatorServiceImpl,
    node::{Layout, NodeConfig},
    pb::{self, node_service_client::NodeServiceClient, search_service_server::SearchService},
};
use tonic::Request;

const SIGNED: [Option<i64>; 6] = [
    Some(i64::MIN),
    Some(1 << 53),
    Some((1 << 53) + 1),
    Some(0),
    None,
    None,
];
const UNSIGNED: [Option<u64>; 6] = [
    Some(u64::MAX),
    Some(1 << 53),
    Some((1 << 53) + 1),
    Some(0),
    None,
    None,
];

fn docs() -> Vec<pb::AddDocumentsRequest> {
    SIGNED
        .iter()
        .zip(UNSIGNED)
        .enumerate()
        .map(|(row, (&s, u))| pb::AddDocumentsRequest {
            text: format!("word row{row}"),
            analysis: Some(body_spec()),
            facets: vec![pb::FacetValue {
                field: "row".into(),
                value: row.to_string(),
            }],
            map_integers: s
                .into_iter()
                .map(|value| pb::MapIntegerEntry {
                    field: "signed".into(),
                    key: "".into(),
                    value,
                })
                .collect(),
            map_unsigned_integers: u
                .into_iter()
                .map(|value| pb::MapUnsignedIntegerEntry {
                    field: "unsigned".into(),
                    key: "".into(),
                    value,
                })
                .collect(),
            ..Default::default()
        })
        .enumerate()
        .map(|(row, mut doc)| {
            // The second ingest batch adds a key that sorts before the sealed
            // segment's key, so query ordinals must translate across segments.
            if row == 0 || row == 2 {
                let key = if row == 0 { "z" } else { "a" };
                doc.map_integers.push(pb::MapIntegerEntry {
                    field: "signed".into(),
                    key: key.into(),
                    value: i64::MIN,
                });
                doc.map_unsigned_integers.push(pb::MapUnsignedIntegerEntry {
                    field: "unsigned".into(),
                    key: key.into(),
                    value: u64::MAX,
                });
            }
            doc
        })
        .collect()
}
fn materialized_docs() -> Vec<pb::AddDocumentsRequest> {
    let mut input = docs();
    for doc in &mut input {
        doc.materialize = Some(pb::MaterializeSpec {
            columns: [
                ("signed_copy", "signed['']", pb::MaterializeKind::I64),
                ("unsigned_copy", "unsigned['']", pb::MaterializeKind::U64),
                (
                    "unsigned_overflow",
                    "unsigned[''] + 1u",
                    pb::MaterializeKind::U64,
                ),
            ]
            .into_iter()
            .map(|(name, expression, kind)| pb::MaterializedColumn {
                name: name.into(),
                expression: expression.into(),
                kind: kind as i32,
            })
            .collect(),
        });
    }
    input
}

async fn query(
    coordinator: &CoordinatorServiceImpl,
    filter: &str,
    expressions: &[&str],
) -> Result<pb::Bm25SearchResponse, tonic::Status> {
    coordinator
        .bm25_search(Request::new(pb::Bm25SearchRequest {
            text: "word".into(),
            k: 20,
            analysis: Some(body_spec()),
            filter: filter.into(),
            projections: expressions
                .iter()
                .enumerate()
                .map(|(i, e)| pb::NamedProjection {
                    name: format!("p{i}"),
                    expression: (*e).into(),
                })
                .collect(),
            ..Default::default()
        }))
        .await
        .map(|r| r.into_inner())
}
fn coordinator(address: String) -> CoordinatorServiceImpl {
    CoordinatorServiceImpl::new(vec![address])
        .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default())
}

#[tokio::test]
async fn exact_map_filters_and_values_distinguish_adjacent_large_integers() {
    let (address, server) = common::start_empty_node(NodeConfig {
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
        facet_fields: vec!["row".into()],
        map_integer_fields: vec!["signed".into()],
        map_unsigned_integer_fields: vec!["unsigned".into()],
        ..Default::default()
    })
    .await;
    NodeServiceClient::connect(address.clone())
        .await
        .unwrap()
        .add_documents(tokio_stream::iter(docs()))
        .await
        .unwrap();
    let coordinator = coordinator(address);
    for (filter, expected) in [
        ("signed[''] == 9007199254740993", vec![2]),
        ("unsigned[''] == 18446744073709551615u", vec![0]),
        ("unsigned[''] > 9007199254740992u", vec![0, 2]),
        ("!(signed[''] == 0)", vec![0, 1, 2]),
        ("'' in signed", vec![0, 1, 2, 3]),
        ("!('' in unsigned)", vec![4, 5]),
        ("!('never' in unsigned)", vec![0, 1, 2, 3, 4, 5]),
    ] {
        let response = query(&coordinator, filter, &["signed['']", "unsigned['']"])
            .await
            .unwrap();
        let mut ids: Vec<_> = response.hits.iter().map(|h| h.doc_id).collect();
        ids.sort();
        assert_eq!(ids, expected, "{filter}");
        for hit in response.hits {
            assert_eq!(
                hit.projected[0].value,
                SIGNED[hit.doc_id as usize].map(pb::projected_value::Value::IntValue)
            );
            assert_eq!(
                hit.projected[1].value,
                UNSIGNED[hit.doc_id as usize].map(pb::projected_value::Value::UintValue)
            );
        }
    }
    server.abort();
    let _ = server.await;
}

fn row(hit: &pb::Bm25Hit) -> usize {
    let Some(pb::projected_value::Value::StringValue(row)) = &hit.projected[0].value else {
        panic!("missing row identity")
    };
    row.parse().unwrap()
}

async fn verify_queries(coordinator: &CoordinatorServiceImpl) {
    for (filter, expected) in [
        ("signed[''] > 9007199254740992", vec![2]),
        ("unsigned[''] == 9007199254740993u", vec![2]),
        ("unsigned[''] > 18446744073709551614u", vec![0]),
        ("signed[''] < -9223372036854775807", vec![0]),
        ("unsigned[''] < -1", vec![]),
        ("signed[''] >= 18446744073709551615u", vec![]),
        ("unsigned[''] > 9007199254740992.0", vec![0, 2]),
        ("unsigned[''] < 18446744073709551616.0", vec![0, 1, 2, 3]),
        (
            "signed[''] >= 9007199254740992 && signed[''] <= 9007199254740993",
            vec![1, 2],
        ),
        ("!(unsigned[''] == 0u)", vec![0, 1, 2]),
        ("unsigned[''] == 0u || !('' in unsigned)", vec![3, 4, 5]),
        ("'' in signed", vec![0, 1, 2, 3]),
        ("!('' in unsigned)", vec![4, 5]),
        ("!('never' in unsigned)", vec![0, 1, 2, 3, 4, 5]),
        ("unsigned['z'] == 18446744073709551615u", vec![0]),
        ("signed['a'] < -9223372036854775807", vec![2]),
        ("'z' in unsigned", vec![0]),
    ] {
        // The placement evaluator must agree with the serving filter, including
        // exact cross-domain comparisons and total presence on missing columns.
        let compiled = pipestream_search::cel::compile_filter(filter)
            .unwrap()
            .unwrap();
        let placed: Vec<_> = docs()
            .iter()
            .enumerate()
            .filter_map(|(row, doc)| {
                let columns = pipestream_search::placement::DocColumns::of(doc).unwrap();
                (pipestream_search::placement::eval_document(&compiled, &columns)
                    == pipestream_search::filter::Tri::True)
                    .then_some(row)
            })
            .collect();
        assert_eq!(placed, expected, "placement {filter}");
        let response = query(coordinator, filter, &["row", "signed['']", "unsigned['']"])
            .await
            .unwrap();
        let mut ids: Vec<_> = response.hits.iter().map(row).collect();
        ids.sort();
        assert_eq!(ids, expected, "{filter}");
        for hit in response.hits {
            let index = row(&hit);
            assert_eq!(
                hit.projected[1].value,
                SIGNED[index].map(pb::projected_value::Value::IntValue)
            );
            assert_eq!(
                hit.projected[2].value,
                UNSIGNED[index].map(pb::projected_value::Value::UintValue)
            );
        }
    }
    let response = query(
        coordinator,
        "",
        &[
            "row",
            "unsigned[''] + 1u",
            "signed[''] - 1",
            "double(unsigned[''])",
            "unsigned_copy",
            "unsigned_overflow",
            "signed_copy",
        ],
    )
    .await
    .unwrap();
    assert_eq!(response.hits.len(), SIGNED.len());
    for hit in response.hits {
        let index = row(&hit);
        use pb::projected_value::Value as V;
        assert_eq!(
            hit.projected[1].value,
            UNSIGNED[index]
                .and_then(|v| v.checked_add(1))
                .map(V::UintValue)
        );
        assert_eq!(
            hit.projected[2].value,
            SIGNED[index]
                .and_then(|v| v.checked_sub(1))
                .map(V::IntValue)
        );
        assert_eq!(
            hit.projected[3].value,
            UNSIGNED[index].map(|v| V::DoubleValue(v as f64))
        );
        assert_eq!(hit.projected[4].value, UNSIGNED[index].map(V::UintValue));
        assert_eq!(
            hit.projected[5].value,
            UNSIGNED[index]
                .and_then(|v| v.checked_add(1))
                .map(V::UintValue)
        );
        assert_eq!(hit.projected[6].value, SIGNED[index].map(V::IntValue));
    }
    for expression in [
        "unsigned[''] + 1",
        "signed[''] + 1u",
        "unsigned[''] + signed['']",
    ] {
        assert_eq!(
            query(coordinator, "", &[expression])
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
    for (filter, expression) in [("", "unsigned['never']"), ("unsigned['never'] > 0u", "row")] {
        assert_eq!(
            query(coordinator, filter, &[expression])
                .await
                .unwrap_err()
                .code(),
            tonic::Code::InvalidArgument
        );
    }
    use pb::{aggregate_result::Value as A, AggregateOp as O};
    let specs = [
        ("signed['']", O::Min, A::IntValue(i64::MIN)),
        ("signed['']", O::Max, A::IntValue((1 << 53) + 1)),
        ("unsigned['']", O::Max, A::UintValue(u64::MAX)),
        ("unsigned['']", O::Min, A::UintValue(0)),
        ("unsigned['']", O::Count, A::IntValue(4)),
        ("unsigned['']", O::Cardinality, A::IntValue(4)),
        (
            "unsigned[''] % 1000u",
            O::Sum,
            A::UintValue(UNSIGNED.iter().flatten().map(|v| v % 1000).sum()),
        ),
    ];
    let response = coordinator
        .aggregate(Request::new(pb::AggregateRequest {
            aggregations: specs
                .iter()
                .enumerate()
                .map(|(i, (expression, op, _))| pb::Aggregation {
                    name: format!("a{i}"),
                    expression: (*expression).into(),
                    op: *op as i32,
                    ..Default::default()
                })
                .collect(),
            percentiles: vec![pb::PercentileSpec {
                name: "upper".into(),
                expression: "unsigned['']".into(),
                percentiles: vec![100.0],
            }],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    for (actual, (_, _, expected)) in response.results.iter().zip(specs) {
        assert_eq!(actual.value, Some(expected));
    }
    assert_eq!(
        response.percentiles[0].values[0].value,
        Some(pb::percentile_value::Value::UintValue(u64::MAX))
    );
}

#[tokio::test]
async fn exact_map_queries_survive_sealing_reopen_and_nested_relays() {
    let root = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("integer-map-queries-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let vectors = common::unit_vectors(SIGNED.len(), 8, 23411);
    let (shift, scale) = common::fit_calibration(8, 4, &common::unit_vectors(64, 8, 23412));
    for layout in [Layout::SingleImage, Layout::Segments] {
        let config = NodeConfig {
            index_path: Some(root.join(format!("{layout:?}.tv"))),
            layout,
            wal: true,
            wal_buckets: 2,
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
            facet_fields: vec!["row".into()],
            map_integer_fields: vec!["signed".into()],
            map_unsigned_integer_fields: vec!["unsigned".into()],
            integer_fields: vec!["signed_copy".into()],
            unsigned_integer_fields: vec!["unsigned_copy".into(), "unsigned_overflow".into()],
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
        let input = materialized_docs();
        for range in [0..2, 2..6] {
            client
                .add_documents(tokio_stream::iter(input[range.clone()].to_vec()))
                .await
                .unwrap();
            client
                .add_vectors(tokio_stream::iter([pb::AddVectorsRequest {
                    dim: 8,
                    vectors: vectors[range.start * 8..range.end * 8].to_vec(),
                }]))
                .await
                .unwrap();
            if range.end == 2 {
                client.flush(pb::FlushRequest {}).await.unwrap();
            }
        }
        for phase in 0..3 {
            if phase == 1 {
                client.flush(pb::FlushRequest {}).await.unwrap();
            }
            if phase == 2 {
                server.abort();
                let _ = server.await;
                (address, server) = common::start_opened_node(config.clone()).await;
                client = NodeServiceClient::connect(address.clone()).await.unwrap();
            }
            verify_queries(&coordinator(address.clone())).await;
            let (relay, _, relay_task) =
                pipestream_search::harness::start_relay(vec![address.clone()]).await;
            let (top, _, top_task) = pipestream_search::harness::start_relay(vec![relay]).await;
            verify_queries(&coordinator(top)).await;
            top_task.abort();
            relay_task.abort();
            let _ = top_task.await;
            let _ = relay_task.await;
        }
        server.abort();
        let _ = server.await;
    }
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn empty_map_columns_retain_types_across_relay_projection_checks() {
    let mut addresses = Vec::new();
    let mut servers = Vec::new();
    for signed in [false, true] {
        let (address, server) = common::start_empty_node(NodeConfig {
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
            map_integer_fields: if signed { vec!["value".into()] } else { vec![] },
            map_unsigned_integer_fields: if signed { vec![] } else { vec!["value".into()] },
            ..Default::default()
        })
        .await;
        NodeServiceClient::connect(address.clone())
            .await
            .unwrap()
            .add_documents(tokio_stream::iter([pb::AddDocumentsRequest {
                text: "word".into(),
                analysis: Some(body_spec()),
                map_unsigned_integers: if signed {
                    vec![]
                } else {
                    vec![pb::MapUnsignedIntegerEntry {
                        field: "value".into(),
                        key: "".into(),
                        value: u64::MAX,
                    }]
                },
                ..Default::default()
            }]))
            .await
            .unwrap();
        addresses.push(address);
        servers.push(server);
    }
    let direct = CoordinatorServiceImpl::new(addresses.clone())
        .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default());
    assert_eq!(
        query(&direct, "", &["value['']"]).await.unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    let (relay, _, relay_task) = pipestream_search::harness::start_relay(addresses).await;
    let error = query(&coordinator(relay), "", &["value['']"])
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("incompatible types"), "{error}");
    relay_task.abort();
    let _ = relay_task.await;
    for task in servers {
        task.abort();
        let _ = task.await;
    }
}

#[tokio::test]
async fn typed_map_queries_compose_across_children_with_missing_columns() {
    let input = materialized_docs();
    let mut addresses = Vec::new();
    let mut servers = Vec::new();
    for child in 0..3 {
        let mut config = NodeConfig {
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
            facet_fields: vec!["row".into()],
            ..Default::default()
        };
        if child < 2 {
            config.map_integer_fields = vec!["signed".into()];
            config.map_unsigned_integer_fields = vec!["unsigned".into()];
            config.integer_fields = vec!["signed_copy".into()];
            config.unsigned_integer_fields =
                vec!["unsigned_copy".into(), "unsigned_overflow".into()];
        }
        let (address, server) = common::start_empty_node(config).await;
        let mut chunk = input[child * 2..child * 2 + 2].to_vec();
        if child == 2 {
            for doc in &mut chunk {
                doc.materialize = None;
            }
        }
        NodeServiceClient::connect(address.clone())
            .await
            .unwrap()
            .add_documents(tokio_stream::iter(chunk))
            .await
            .unwrap();
        addresses.push(address);
        servers.push(server);
    }
    let direct = CoordinatorServiceImpl::new(addresses.clone())
        .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default());
    verify_queries(&direct).await;
    let (left, _, left_task) =
        pipestream_search::harness::start_relay(addresses[..2].to_vec()).await;
    let (right, _, right_task) =
        pipestream_search::harness::start_relay(addresses[2..].to_vec()).await;
    let (root, _, root_task) = pipestream_search::harness::start_relay(vec![left, right]).await;
    verify_queries(&coordinator(root)).await;
    for task in [left_task, right_task, root_task] {
        task.abort();
        let _ = task.await;
    }
    for task in servers {
        task.abort();
        let _ = task.await;
    }
}
