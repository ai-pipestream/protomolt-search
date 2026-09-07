mod common;

use pipestream_search::{
    analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND},
    coordinator::CoordinatorServiceImpl,
    node::{Layout, NodeConfig},
    pb::{self, node_service_client::NodeServiceClient, search_service_server::SearchService},
};

const UNSIGNED: [Option<u64>; 10] = [
    Some(u64::MAX),
    Some(0),
    Some(1 << 63),
    Some((1 << 53) + 1),
    None,
    Some(u64::MAX - 1),
    Some(0),
    Some(u64::MAX),
    Some(1),
    Some(1 << 53),
];
const SIGNED: [Option<i64>; 10] = [
    Some(i64::MIN),
    Some(0),
    Some(i64::MAX),
    Some((1 << 53) + 1),
    None,
    Some(i64::MIN + 1),
    Some(0),
    Some(i64::MIN),
    Some(-1),
    Some(1 << 53),
];
const QUOTED_KEY: &str = "zz'\\\"[empty]雪";

fn request(lexical: bool) -> pb::QueryRequest {
    let node = if lexical {
        pb::selection_query::Node::Search(pb::SearchQuery {
            id: "words".into(),
            query: Some(pb::search_query::Query::Lexical(pb::LexicalQuery {
                text: "word".into(),
                analysis: Some(body_spec()),
                ..Default::default()
            })),
        })
    } else {
        pb::selection_query::Node::Filter(pb::FilterQuery {
            id: "all".into(),
            predicate: Some(pb::filter_query::Predicate::Cel(
                "has(row) || !has(row)".into(),
            )),
        })
    };
    pb::QueryRequest {
        k: 100,
        selection: Some(pb::SelectionQuery { node: Some(node) }),
        projections: vec![pb::NamedProjection {
            name: "row".into(),
            expression: "row".into(),
        }],
        ..Default::default()
    }
}
fn row(hit: &pb::QueryHit) -> usize {
    match &hit.projected[0].value {
        Some(pb::projected_value::Value::StringValue(s)) => s.parse().unwrap(),
        other => panic!("missing row: {other:?}"),
    }
}
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum Value {
    I(i64),
    U(u64),
}
impl Value {
    fn pb(&self) -> pb::sort_value::Value {
        match self {
            Self::I(v) => pb::sort_value::Value::Integer(*v),
            Self::U(v) => pb::sort_value::Value::UnsignedInteger(*v),
        }
    }
}
fn value(column: &str, row: usize) -> Option<Value> {
    if column == "signed" {
        SIGNED[row].map(Value::I)
    } else {
        UNSIGNED[row].map(Value::U)
    }
}
fn coordinator(addresses: Vec<String>) -> CoordinatorServiceImpl {
    CoordinatorServiceImpl::new(addresses)
        .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default())
}
async fn verify(coordinator: &CoordinatorServiceImpl, expected_count: usize) {
    let baseline = coordinator
        .query(tonic::Request::new(request(true)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(baseline.hits.len(), expected_count);
    for column in ["signed", "unsigned"] {
        for key in ["", QUOTED_KEY] {
            let selector = pb::MapRead {
                column: column.into(),
                key: key.into(),
            };
            for descending in [false, true] {
                for lexical in [false, true] {
                    let mut req = request(lexical);
                    req.sort = vec![
                        pb::QuerySort {
                            map: Some(selector.clone()),
                            descending,
                            ..Default::default()
                        },
                        pb::QuerySort {
                            column: "row".into(),
                            descending: !descending,
                            ..Default::default()
                        },
                    ];
                    let mut expected: Vec<_> = baseline
                        .hits
                        .iter()
                        .filter_map(|h| value(column, row(h)).map(|v| (v, row(h), h.doc_id)))
                        .collect();
                    expected.sort_by(|a, b| {
                        (if descending {
                            b.0.cmp(&a.0)
                        } else {
                            a.0.cmp(&b.0)
                        })
                        .then_with(|| {
                            if descending {
                                a.1.to_string().cmp(&b.1.to_string())
                            } else {
                                b.1.to_string().cmp(&a.1.to_string())
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
                        full.hits.iter().map(row).collect::<Vec<_>>(),
                        expected.iter().map(|v| v.1).collect::<Vec<_>>()
                    );
                    for (hit, (value, _, _)) in full.hits.iter().zip(&expected) {
                        assert_eq!(hit.sort_values[0].value, Some(value.pb()));
                    }
                    req.k = 3;
                    let mut paged = Vec::new();
                    loop {
                        let response = coordinator
                            .query(tonic::Request::new(req.clone()))
                            .await
                            .unwrap()
                            .into_inner();
                        paged.extend(response.hits.iter().map(|h| (row(h), h.rank)));
                        if response.next_cursor.is_empty() {
                            break;
                        }
                        // A changed map key cannot reuse this signed cursor.
                        let mut changed = req.clone();
                        changed.cursor = response.next_cursor.clone();
                        changed.sort[0].map.as_mut().unwrap().key.push('x');
                        assert!(coordinator
                            .query(tonic::Request::new(changed))
                            .await
                            .is_err());
                        req.cursor = response.next_cursor;
                        assert!(paged.len() <= expected.len());
                    }
                    assert_eq!(
                        paged,
                        expected
                            .iter()
                            .enumerate()
                            .map(|(i, v)| (v.1, i as u32 + 1))
                            .collect::<Vec<_>>()
                    );
                }
            }
            let mut expected: Vec<(Value, Vec<usize>)> = Vec::new();
            for hit in &baseline.hits {
                let Some(v) = value(column, row(hit)) else {
                    continue;
                };
                if let Some((_, rows)) = expected.iter_mut().find(|(key, _)| *key == v) {
                    rows.push(row(hit));
                } else {
                    expected.push((v, vec![row(hit)]));
                }
            }
            let mut req = request(true);
            req.collapse = Some(pb::CollapseSpec {
                map: Some(selector),
                inner_hits: 100,
                ..Default::default()
            });
            let full = coordinator
                .query(tonic::Request::new(req.clone()))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(full.groups.len(), expected.len());
            for (group, (value, rows)) in full.groups.iter().zip(&expected) {
                assert_eq!(group.key.as_ref().unwrap().value, Some(value.pb()));
                assert_eq!(group.hits.iter().map(row).collect::<Vec<_>>(), *rows);
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
                        .iter()
                        .map(|g| g.key.as_ref().unwrap().value.clone().unwrap()),
                );
                if response.next_cursor.is_empty() {
                    break;
                }
                req.cursor = response.next_cursor;
                assert!(paged.len() <= expected.len());
            }
            assert_eq!(
                paged,
                expected.iter().map(|(v, _)| v.pb()).collect::<Vec<_>>()
            );
        }
    }
}
async fn verify_relays(addresses: Vec<String>, expected_count: usize) {
    verify(&coordinator(addresses.clone()), expected_count).await;
    let mut relays = Vec::new();
    let mut tasks = Vec::new();
    for address in addresses {
        let (relay, _, task) = pipestream_search::harness::start_relay(vec![address]).await;
        relays.push(relay);
        tasks.push(task);
    }
    let (root, _, task) = pipestream_search::harness::start_relay(relays).await;
    tasks.push(task);
    verify(&coordinator(vec![root]), expected_count).await;
    for task in tasks {
        task.abort();
        let _ = task.await;
    }
}
#[tokio::test]
async fn integer_map_order_survives_tail_sealing_reopen_compaction_and_relay_grouping() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("map-order-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (shift, scale) = common::fit_calibration(8, 4, &common::unit_vectors(64, 8, 62871));
    for layout in [Layout::SingleImage, Layout::Segments] {
        let mut configs = Vec::new();
        let mut addresses = Vec::new();
        let mut servers = Vec::new();
        for range in [0..5, 5..10] {
            let config = NodeConfig {
                index_path: Some(dir.join(format!("{layout:?}-{}.tv", range.start))),
                layout,
                wal: true,
                wal_buckets: 2,
                slot_offset: range.start as u64,
                analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
                facet_fields: vec!["row".into()],
                map_integer_fields: vec!["signed".into()],
                map_unsigned_integer_fields: vec!["unsigned".into()],
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
            let first = range.start;
            for row in range {
                let mut keys = vec!["", QUOTED_KEY, "z"];
                if row > first + 1 {
                    keys.push("a");
                }
                client
                    .add_documents(tokio_stream::iter([pb::AddDocumentsRequest {
                        text: "word".into(),
                        analysis: Some(body_spec()),
                        facets: vec![pb::FacetValue {
                            field: "row".into(),
                            value: row.to_string(),
                        }],
                        map_integers: keys
                            .iter()
                            .flat_map(|key| {
                                SIGNED[row].map(|value| pb::MapIntegerEntry {
                                    field: "signed".into(),
                                    key: (*key).into(),
                                    value,
                                })
                            })
                            .collect(),
                        map_unsigned_integers: keys
                            .iter()
                            .flat_map(|key| {
                                UNSIGNED[row].map(|value| pb::MapUnsignedIntegerEntry {
                                    field: "unsigned".into(),
                                    key: (*key).into(),
                                    value,
                                })
                            })
                            .collect(),
                        ..Default::default()
                    }]))
                    .await
                    .unwrap();
                client
                    .add_vectors(tokio_stream::iter([pb::AddVectorsRequest {
                        dim: 8,
                        vectors: common::unit_vectors(1, 8, 62880 + row as u64),
                    }]))
                    .await
                    .unwrap();
                if row == first + 1 {
                    client.flush(pb::FlushRequest {}).await.unwrap();
                }
            }
            configs.push(config);
            addresses.push(address);
            servers.push(server);
        }
        // Frozen segment dictionaries and the unsealed tail must agree.
        verify_relays(addresses.clone(), 10).await;
        for address in &addresses {
            NodeServiceClient::connect(address.clone())
                .await
                .unwrap()
                .flush(pb::FlushRequest {})
                .await
                .unwrap();
        }
        for server in servers.drain(..) {
            server.abort();
            let _ = server.await;
        }
        for phase in 0..2 {
            addresses.clear();
            for (i, config) in configs.iter().enumerate() {
                let (address, server) = common::start_opened_node(config.clone()).await;
                if phase == 0 {
                    let mut client = NodeServiceClient::connect(address.clone()).await.unwrap();
                    if i == 1 {
                        let deleted = client
                            .delete_documents(pb::DeleteDocumentsRequest {
                                doc_ids: vec![5],
                                expected_wal_generation: Some(0),
                            })
                            .await
                            .unwrap()
                            .into_inner();
                        assert_eq!(deleted.deleted, 1);
                    }
                    client
                        .compact_shard(pb::CompactShardRequest {
                            work_dir: dir
                                .join(format!("compact-{layout:?}-{i}"))
                                .display()
                                .to_string(),
                            ..Default::default()
                        })
                        .await
                        .unwrap();
                }
                addresses.push(address);
                servers.push(server);
            }
            verify_relays(addresses.clone(), 9).await;
            for server in servers.drain(..) {
                server.abort();
                let _ = server.await;
            }
        }
    }
    std::fs::remove_dir_all(dir).unwrap();
}
