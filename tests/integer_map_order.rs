mod common;

use pipestream_search::{
    analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND},
    coordinator::CoordinatorServiceImpl,
    node::NodeConfig,
    pb::{self, node_service_client::NodeServiceClient, search_service_server::SearchService},
};

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
                "'' in values || !('' in values)".into(),
            )),
        })
    };
    pb::QueryRequest {
        k: 100,
        selection: Some(pb::SelectionQuery { node: Some(node) }),
        ..Default::default()
    }
}
fn selector() -> pb::MapRead {
    pb::MapRead {
        column: "values".into(),
        key: "".into(),
    }
}

#[tokio::test]
async fn unsigned_map_sort_and_collapse_do_not_round_large_neighbors() {
    let (address, server) = common::start_empty_node(NodeConfig {
        map_unsigned_integer_fields: vec!["values".into()],
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
        ..Default::default()
    })
    .await;
    let mut client = NodeServiceClient::connect(address.clone()).await.unwrap();
    let values = [u64::MAX, (1 << 53) + 1, 1 << 53, 0, u64::MAX - 1, u64::MAX];
    for value in values {
        client
            .add_documents(tokio_stream::iter(vec![pb::AddDocumentsRequest {
                text: "word".into(),
                analysis: Some(body_spec()),
                map_unsigned_integers: vec![pb::MapUnsignedIntegerEntry {
                    field: "values".into(),
                    key: "".into(),
                    value,
                }],
                ..Default::default()
            }]))
            .await
            .unwrap();
    }
    let coordinator = CoordinatorServiceImpl::new(vec![address])
        .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default());
    let mut req = request(false);
    req.sort = vec![pb::QuerySort {
        map: Some(selector()),
        ..Default::default()
    }];
    let result = coordinator
        .query(tonic::Request::new(req))
        .await
        .unwrap()
        .into_inner();
    let mut expected = values.to_vec();
    expected.sort();
    assert_eq!(
        result
            .hits
            .iter()
            .map(|h| h.sort_values[0].value.clone().unwrap())
            .collect::<Vec<_>>(),
        expected
            .into_iter()
            .map(pb::sort_value::Value::UnsignedInteger)
            .collect::<Vec<_>>()
    );
    let mut req = request(true);
    req.collapse = Some(pb::CollapseSpec {
        map: Some(selector()),
        inner_hits: 100,
        ..Default::default()
    });
    let result = coordinator
        .query(tonic::Request::new(req))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(result.groups.len(), 5);
    assert_eq!(result.groups.iter().map(|g| g.hits.len()).sum::<usize>(), 6);
    server.abort();
}

#[tokio::test]
async fn string_and_float_map_selectors_preserve_empty_values_and_missing_entries() {
    let (address, task) = common::start_empty_node(NodeConfig {
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
        map_facet_fields: vec!["labels".into()],
        map_numeric_fields: vec!["weights".into()],
        ..Default::default()
    })
    .await;
    let mut client = NodeServiceClient::connect(address.clone()).await.unwrap();
    for (text, number) in [
        (Some(""), Some(-1.0)),
        (Some("雪"), Some(0.0)),
        (Some(""), Some(2.0)),
        (None, None),
    ] {
        client
            .add_documents(tokio_stream::iter([pb::AddDocumentsRequest {
                text: "word".into(),
                analysis: Some(body_spec()),
                map_facets: text
                    .map(|value| pb::MapFacetEntry {
                        field: "labels".into(),
                        key: "".into(),
                        value: value.into(),
                    })
                    .into_iter()
                    .collect(),
                map_numerics: number
                    .map(|value| pb::MapNumericEntry {
                        field: "weights".into(),
                        key: "".into(),
                        value,
                    })
                    .into_iter()
                    .collect(),
                ..Default::default()
            }]))
            .await
            .unwrap();
    }
    let (relay, _, relay_task) =
        pipestream_search::harness::start_relay(vec![address.clone()]).await;
    for nodes in [vec![address], vec![relay]] {
        let coordinator = CoordinatorServiceImpl::new(nodes)
            .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default());
        for (column, expected) in [
            (
                "labels",
                vec![
                    pb::sort_value::Value::Text("".into()),
                    pb::sort_value::Value::Text("".into()),
                    pb::sort_value::Value::Text("雪".into()),
                ],
            ),
            (
                "weights",
                vec![
                    pb::sort_value::Value::Number(-1.0),
                    pb::sort_value::Value::Number(0.0),
                    pb::sort_value::Value::Number(2.0),
                ],
            ),
        ] {
            let map = pb::MapRead {
                column: column.into(),
                key: "".into(),
            };
            for descending in [false, true] {
                let mut req = request(true);
                req.sort = vec![pb::QuerySort {
                    map: Some(map.clone()),
                    descending,
                    ..Default::default()
                }];
                let response = coordinator
                    .query(tonic::Request::new(req))
                    .await
                    .unwrap()
                    .into_inner();
                let mut expected = expected.clone();
                if descending {
                    expected.reverse();
                }
                assert_eq!(
                    response
                        .hits
                        .iter()
                        .map(|h| h.sort_values[0].value.clone().unwrap())
                        .collect::<Vec<_>>(),
                    expected
                );
            }
            let mut req = request(true);
            req.collapse = Some(pb::CollapseSpec {
                map: Some(map),
                inner_hits: 100,
                ..Default::default()
            });
            let response = coordinator.query(tonic::Request::new(req)).await;
            if column == "weights" {
                assert_eq!(response.unwrap_err().code(), tonic::Code::InvalidArgument);
            } else {
                let response = response.unwrap().into_inner();
                assert_eq!(response.groups.len(), 2);
                assert_eq!(
                    response.groups.iter().map(|g| g.hits.len()).sum::<usize>(),
                    3
                );
            }
        }
    }
    for task in [relay_task, task] {
        task.abort();
        let _ = task.await;
    }
}
