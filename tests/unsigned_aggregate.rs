mod common;

use pipestream_search::{
    analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND},
    coordinator::CoordinatorServiceImpl,
    node::{Layout, NodeConfig},
    pb::{
        self, aggregate_result::Value as A, node_service_client::NodeServiceClient,
        percentile_value::Value as P, search_service_server::SearchService,
    },
};
use tonic::{Code, Request};

const VALUES: [Option<u64>; 18] = [
    None,
    Some(0),
    Some(1),
    Some((1 << 53) + 1),
    Some(1 << 63),
    Some(u64::MAX),
    Some(u64::MAX - 1),
    Some(u64::MAX),
    Some(0),
    Some((1 << 53) + 2),
    Some(12),
    None,
    None,
    None,
    None,
    None,
    None,
    None,
];
const PERCENTILES: [f64; 5] = [0.0, 25.0, 50.0, 75.0, 100.0];
fn aggregation(name: &str, expression: &str, op: pb::AggregateOp) -> pb::Aggregation {
    pb::Aggregation {
        name: name.into(),
        expression: expression.into(),
        op: op as i32,
        ..Default::default()
    }
}
fn request() -> pb::AggregateRequest {
    use pb::AggregateOp as O;
    pb::AggregateRequest {
        aggregations: vec![
            aggregation("count", "counter", O::Count),
            aggregation("distinct", "counter", O::Cardinality),
            aggregation("min", "counter", O::Min),
            aggregation("max", "counter", O::Max),
            aggregation("sum", "counter % 1000u", O::Sum),
        ],
        group_by: "group".into(),
        percentiles: vec![pb::PercentileSpec {
            name: "percentile".into(),
            expression: "counter".into(),
            percentiles: PERCENTILES.to_vec(),
        }],
        ..Default::default()
    }
}
fn expected(rows: &[Option<u64>]) -> Vec<(u64, Option<A>)> {
    let values: Vec<_> = rows.iter().flatten().copied().collect();
    let count = values.len() as u64;
    let sum: u128 = values.iter().map(|v| u128::from(v % 1000)).sum();
    vec![
        (count, Some(A::IntValue(count as i64))),
        (
            count,
            Some(A::IntValue(
                values
                    .iter()
                    .collect::<std::collections::BTreeSet<_>>()
                    .len() as i64,
            )),
        ),
        (count, values.iter().min().copied().map(A::UintValue)),
        (count, values.iter().max().copied().map(A::UintValue)),
        (
            count,
            (count > 0).then_some(A::UintValue(u64::try_from(sum).unwrap())),
        ),
    ]
}
fn check_results(actual: &[pb::AggregateResult], rows: &[Option<u64>]) {
    let actual: Vec<_> = actual
        .iter()
        .map(|r| (r.present, r.value.clone()))
        .collect();
    assert_eq!(actual, expected(rows));
}
fn check_response(response: &pb::AggregateResponse, rows: &[Option<u64>]) {
    assert_eq!(response.matched, rows.len() as u64);
    check_results(&response.results, rows);
    let mut ordered: Vec<u64> = rows.iter().flatten().copied().collect();
    ordered.sort_unstable();
    let p = &response.percentiles[0];
    assert_eq!(p.present, ordered.len() as u64);
    assert_eq!(p.unrankable, 0);
    for (result, pct) in p.values.iter().zip(PERCENTILES) {
        let rank = if ordered.is_empty() {
            0
        } else {
            ((pct as u128 * ordered.len() as u128).div_ceil(100) as usize).max(1)
        };
        assert_eq!(result.rank, rank as u64);
        assert_eq!(
            result.value,
            rank.checked_sub(1).map(|i| P::UintValue(ordered[i]))
        );
    }
}

async fn verify(addresses: Vec<String>) {
    let coordinator = CoordinatorServiceImpl::new(addresses.clone())
        .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default());
    let response = coordinator
        .aggregate(Request::new(request()))
        .await
        .unwrap()
        .into_inner();
    check_response(&response, &VALUES);
    assert_eq!(response.groups.len(), 2);
    for group in &response.groups {
        let group_rows: Vec<_> = VALUES
            .iter()
            .enumerate()
            .filter(|(i, _)| {
                if group.value == "even" {
                    i % 2 == 0
                } else {
                    i % 2 == 1
                }
            })
            .map(|(_, v)| *v)
            .collect();
        check_results(&group.results, &group_rows);
        assert_eq!(group.matched, group_rows.len() as u64);
    }
    let mut reversed = addresses.clone();
    reversed.reverse();
    let other = CoordinatorServiceImpl::new(reversed)
        .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default());
    assert_eq!(
        other
            .aggregate(Request::new(request()))
            .await
            .unwrap()
            .into_inner(),
        response
    );
    let mut empty = request();
    empty.filter = "row == 'not-a-row'".into();
    check_response(
        &coordinator
            .aggregate(Request::new(empty))
            .await
            .unwrap()
            .into_inner(),
        &[],
    );
    let zeros = coordinator
        .aggregate(Request::new(pb::AggregateRequest {
            filter: "counter == 0u".into(),
            aggregations: vec![aggregation("sum", "counter", pb::AggregateOp::Sum)],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(zeros.results[0].present, 2);
    assert_eq!(zeros.results[0].value, Some(A::UintValue(0)));
    let exact_sum: u128 = VALUES.iter().flatten().map(|v| u128::from(*v)).sum();
    let error = coordinator
        .aggregate(Request::new(pb::AggregateRequest {
            aggregations: vec![aggregation("sum", "counter", pb::AggregateOp::Sum)],
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains(&exact_sum.to_string()), "{error}");
    assert!(error.message().contains("does not fit u64"), "{error}");
    for op in [
        pb::AggregateOp::Mean,
        pb::AggregateOp::Variance,
        pb::AggregateOp::Stddev,
    ] {
        let error = coordinator
            .aggregate(Request::new(pb::AggregateRequest {
                aggregations: vec![aggregation("stat", "counter", op)],
                ..Default::default()
            }))
            .await
            .unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument);
        assert!(error.message().contains("double()"), "{error}");
    }
    // Explicit conversion is a separate, accepted floating-point calculation.
    coordinator
        .aggregate(Request::new(pb::AggregateRequest {
            aggregations: vec![aggregation(
                "mean",
                "double(counter)",
                pb::AggregateOp::Mean,
            )],
            ..Default::default()
        }))
        .await
        .unwrap();
    let mut capped = aggregation("distinct", "counter", pb::AggregateOp::Cardinality);
    capped.max_distinct = 2;
    let error = coordinator
        .aggregate(Request::new(pb::AggregateRequest {
            aggregations: vec![capped],
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("distinct values"), "{error}");
    let mut capped = aggregation("distinct", "counter", pb::AggregateOp::Cardinality);
    capped.max_distinct = 5; // Each leaf fits, but the distinct union does not.
    let error = coordinator
        .aggregate(Request::new(pb::AggregateRequest {
            aggregations: vec![capped],
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(error.message().contains("across the fleet"), "{error}");
    // The public lexical pool folds the complete selection, despite a one-hit page.
    let result = coordinator
        .query(Request::new(pb::QueryRequest {
            k: 1,
            selection_k: 100,
            selection: Some(pb::SelectionQuery {
                node: Some(pb::selection_query::Node::Search(pb::SearchQuery {
                    id: "lex".into(),
                    query: Some(pb::search_query::Query::Lexical(pb::LexicalQuery {
                        text: "word".into(),
                        analysis: Some(body_spec()),
                        ..Default::default()
                    })),
                })),
            }),
            aggregate: Some(request()),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(result.hits.len(), 1);
    check_response(result.aggregate.as_ref().unwrap(), &VALUES);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unsigned_folds_and_percentiles_survive_reopen_compaction_and_query_pools() {
    let (shift, scale) = common::fit_calibration(8, 4, &common::unit_vectors(64, 8, 21572));
    for layout in [Layout::SingleImage, Layout::Segments] {
        let temp = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("uint_aggregate_{layout:?}_{}", std::process::id()));
        std::fs::create_dir_all(&temp).unwrap();
        let mut configs = Vec::new();
        let mut addresses = Vec::new();
        let mut servers = Vec::new();
        for shard in 0..3 {
            let config = NodeConfig {
                index_path: Some(temp.join(format!("{shard}.tv"))),
                layout,
                wal: true,
                wal_buckets: 2,
                seal_tail_docs: 2,
                slot_offset: shard as u64 * 100,
                analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
                facet_fields: vec!["row".into(), "group".into()],
                unsigned_integer_fields: if shard < 2 {
                    vec!["counter".into()]
                } else {
                    vec![]
                },
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
            for (row, value) in VALUES.iter().enumerate().filter(|(i, _)| i / 6 == shard) {
                client
                    .add_documents(tokio_stream::iter([pb::AddDocumentsRequest {
                        text: "word".into(),
                        analysis: Some(body_spec()),
                        facets: vec![
                            pb::FacetValue {
                                field: "row".into(),
                                value: row.to_string(),
                            },
                            pb::FacetValue {
                                field: "group".into(),
                                value: if row % 2 == 0 { "even" } else { "odd" }.into(),
                            },
                        ],
                        unsigned_integers: value
                            .map(|v| pb::UnsignedIntegerValue {
                                field: "counter".into(),
                                value: v,
                            })
                            .into_iter()
                            .collect(),
                        ..Default::default()
                    }]))
                    .await
                    .unwrap();
                client
                    .add_vectors(tokio_stream::iter([pb::AddVectorsRequest {
                        dim: 8,
                        vectors: common::unit_vectors(1, 8, 816 + row as u64),
                    }]))
                    .await
                    .unwrap();
            }
            client.flush(pb::FlushRequest {}).await.unwrap();
            configs.push(config);
            addresses.push(addr);
            servers.push(server);
        }
        verify(addresses).await;
        for server in servers.drain(..) {
            server.abort();
            let _ = server.await;
        }
        for round in 0..2 {
            let mut addresses = Vec::new();
            for (shard, config) in configs.iter().enumerate() {
                let (addr, server) = common::start_opened_node(config.clone()).await;
                if round == 0 {
                    NodeServiceClient::connect(addr.clone())
                        .await
                        .unwrap()
                        .compact_shard(pb::CompactShardRequest {
                            work_dir: temp.join(format!("compact-{shard}")).display().to_string(),
                            ..Default::default()
                        })
                        .await
                        .unwrap();
                }
                addresses.push(addr);
                servers.push(server);
            }
            verify(addresses).await;
            for server in servers.drain(..) {
                server.abort();
                let _ = server.await;
            }
        }
        std::fs::remove_dir_all(temp).unwrap();
    }
}
