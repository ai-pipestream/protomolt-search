mod common;

use pipestream_search::{pb, relay::merge_browse_pages, sortkeys};
use prost::Message;

fn sort() -> pb::BrowseSort {
    pb::BrowseSort {
        map: Some(pb::MapRead {
            column: "values".into(),
            key: "".into(),
        }),
        ..Default::default()
    }
}
fn share(kind: pb::ScalarValueType, known: bool) -> pb::BrowseShardResponse {
    pb::BrowseShardResponse {
        sort_contract_version: 1,
        sort_columns_known: vec![known],
        sort_column_types: vec![kind as i32],
        ..Default::default()
    }
}
#[derive(Clone, PartialEq, Message)]
struct LegacySort {
    #[prost(string, tag = "1")]
    column: String,
    #[prost(bool, tag = "2")]
    descending: bool,
}

#[test]
fn map_selectors_have_no_legacy_column_fallback() {
    let modern = pb::QuerySort {
        map: sort().map,
        ..Default::default()
    };
    let old = LegacySort::decode(modern.encode_to_vec().as_slice()).unwrap();
    assert!(old.column.is_empty()); // The old public Query validator rejects this.
    assert!(sortkeys::target_field(&old.column, None).is_err());
    let map = pb::MapRead {
        column: "values".into(),
        key: "".into(),
    };
    assert_eq!(sortkeys::target_field("", Some(&map)).unwrap(), "values");
    assert!(sortkeys::target_field("fallback", Some(&map)).is_err());
    assert!(sortkeys::target_field("", Some(&pb::MapRead::default())).is_err());
    let collapse = pb::CollapseSpec {
        map: Some(map),
        ..Default::default()
    };
    let old = LegacySort::decode(collapse.encode_to_vec().as_slice()).unwrap();
    assert!(old.column.is_empty());
}

#[test]
fn every_child_must_acknowledge_map_sort_even_when_it_returns_no_rows() {
    let req = pb::BrowseShardRequest {
        k: 1,
        sort: vec![sort()],
        ..Default::default()
    };
    let current = share(pb::ScalarValueType::UnsignedInteger, true);
    let mut legacy = share(pb::ScalarValueType::Unspecified, false);
    legacy.sort_contract_version = 0;
    assert!(sortkeys::validate_browse_metadata(&req.sort, &legacy)
        .unwrap_err()
        .message()
        .contains("acknowledgement"));
    for shares in [
        vec![current.clone(), legacy.clone()],
        vec![legacy.clone(), current.clone()],
    ] {
        assert!(merge_browse_pages(&req, &shares)
            .unwrap_err()
            .message()
            .contains("acknowledgement"));
    }
    let merged = merge_browse_pages(
        &req,
        &[current, share(pb::ScalarValueType::UnsignedInteger, false)],
    )
    .unwrap();
    assert_eq!(merged.sort_contract_version, 1);
    assert!(merge_browse_pages(&req, &[merged, legacy]).is_err());
    let scalar = vec![pb::BrowseSort {
        column: "values".into(),
        ..Default::default()
    }];
    let mut legacy_scalar = share(pb::ScalarValueType::UnsignedInteger, true);
    legacy_scalar.sort_contract_version = 0;
    sortkeys::validate_browse_metadata(&scalar, &legacy_scalar).unwrap();
}

#[test]
fn relay_checks_empty_column_types_and_rows_before_truncating() {
    let req = pb::BrowseShardRequest {
        k: 1,
        sort: vec![sort()],
        ..Default::default()
    };
    let unsigned = share(pb::ScalarValueType::UnsignedInteger, true);
    assert!(merge_browse_pages(
        &req,
        &[unsigned.clone(), share(pb::ScalarValueType::Integer, false)]
    )
    .is_err());
    let absent = share(pb::ScalarValueType::Unspecified, false);
    let merged = merge_browse_pages(&req, &[unsigned.clone(), absent]).unwrap();
    assert_eq!(
        merged.sort_column_types,
        vec![pb::ScalarValueType::UnsignedInteger as i32]
    );
    let mut unresolved = share(pb::ScalarValueType::UnsignedInteger, false);
    unresolved.doc_ids = vec![42];
    assert!(sortkeys::validate_browse_metadata(&req.sort, &unresolved)
        .unwrap_err()
        .message()
        .contains("unresolved sort key"));
    let mut malformed = unsigned;
    malformed.doc_ids = vec![999];
    malformed.sort_rows = vec![pb::SortKeyRow {
        keys: vec![pb::SortKey {
            key: Some(pb::sort_key::Key::UnsignedBits(u64::MAX)),
        }],
        values: vec![pb::SortValue {
            value: Some(pb::sort_value::Value::Integer(-1)),
        }],
    }];
    assert!(merge_browse_pages(&req, &[merged, malformed])
        .unwrap_err()
        .message()
        .contains("declared column type"));
}

#[tokio::test]
async fn empty_node_map_types_are_checked_through_nested_relays() {
    use pipestream_search::{
        analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND},
        coordinator::CoordinatorServiceImpl,
        node::NodeConfig,
        pb::{node_service_client::NodeServiceClient, search_service_server::SearchService},
    };
    let (populated, populated_task) = common::start_empty_node(NodeConfig {
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
        map_unsigned_integer_fields: vec!["values".into()],
        ..Default::default()
    })
    .await;
    let mut client = NodeServiceClient::connect(populated.clone()).await.unwrap();
    client
        .add_documents(tokio_stream::iter([pb::AddDocumentsRequest {
            text: "word".into(),
            analysis: Some(body_spec()),
            map_unsigned_integers: vec![pb::MapUnsignedIntegerEntry {
                field: "values".into(),
                key: "".into(),
                value: u64::MAX,
            }],
            ..Default::default()
        }]))
        .await
        .unwrap();
    let req = pb::QueryRequest {
        k: 10,
        sort: vec![pb::QuerySort {
            map: sort().map,
            ..Default::default()
        }],
        selection: Some(pb::SelectionQuery {
            node: Some(pb::selection_query::Node::Search(pb::SearchQuery {
                id: "word".into(),
                query: Some(pb::search_query::Query::Lexical(pb::LexicalQuery {
                    text: "word".into(),
                    analysis: Some(body_spec()),
                    ..Default::default()
                })),
            })),
        }),
        ..Default::default()
    };
    for conflict in [false, true] {
        let config = NodeConfig {
            slot_offset: 1,
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
            map_unsigned_integer_fields: if conflict {
                vec![]
            } else {
                vec!["values".into()]
            },
            map_integer_fields: if conflict {
                vec!["values".into()]
            } else {
                vec![]
            },
            ..Default::default()
        };
        let (empty, empty_task) = common::start_empty_node(config).await;
        let mut empty_client = NodeServiceClient::connect(empty.clone()).await.unwrap();
        let invalid = pb::FetchValuesRequest {
            projections: vec![pb::CompiledProjection {
                name: "invalid".into(),
                expr: Some(pb::ValueExpr::default()),
            }],
            ..Default::default()
        };
        assert_eq!(
            empty_client.fetch_values(invalid).await.unwrap_err().code(),
            tonic::Code::InvalidArgument
        );
        let addresses = vec![populated.clone(), empty];
        let (relay, _, relay_task) =
            pipestream_search::harness::start_relay(addresses.clone()).await;
        let (top, _, top_task) = pipestream_search::harness::start_relay(vec![relay]).await;
        for nodes in [addresses, vec![top]] {
            let coordinator = CoordinatorServiceImpl::new(nodes)
                .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default());
            let mut collapse = req.clone();
            collapse.sort.clear();
            collapse.collapse = Some(pb::CollapseSpec {
                map: sort().map,
                ..Default::default()
            });
            let collapsed = coordinator.query(tonic::Request::new(collapse)).await;
            if conflict {
                assert_eq!(
                    collapsed.unwrap_err().code(),
                    tonic::Code::FailedPrecondition
                );
            } else {
                assert_eq!(collapsed.unwrap().into_inner().groups.len(), 1);
            }
            let result = coordinator.query(tonic::Request::new(req.clone())).await;
            if conflict {
                assert_eq!(result.unwrap_err().code(), tonic::Code::FailedPrecondition);
            } else {
                let response = result.unwrap().into_inner();
                assert_eq!(response.hits.len(), 1);
                assert_eq!(
                    response.hits[0].sort_values[0].value,
                    Some(pb::sort_value::Value::UnsignedInteger(u64::MAX))
                );
                let mut missing = req.clone();
                missing.sort[0].map.as_mut().unwrap().key = "unseen".into();
                assert_eq!(
                    coordinator
                        .query(tonic::Request::new(missing))
                        .await
                        .unwrap_err()
                        .code(),
                    tonic::Code::InvalidArgument
                );
            }
        }
        for task in [top_task, relay_task, empty_task] {
            task.abort();
            let _ = task.await;
        }
    }
    populated_task.abort();
    let _ = populated_task.await;
}
