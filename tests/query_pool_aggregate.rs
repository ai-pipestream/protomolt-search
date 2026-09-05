//! `QueryRequest.aggregate` (`docs/aggregations.md`, "Aggregating a
//! query's pool"): an exact fold over the candidate pool a page was
//! drawn from, on the pooled shapes and on a browse. The page itself is
//! bitwise the page the same request returns without the aggregation;
//! `matched` is the pool's size; a boolean root, a foreign filter, and a
//! sorted lexical leaf refuse by name.

mod common;

use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    aggregate_result, search_query, selection_query, selection_score_strategy, AddDocumentsRequest,
    AddVectorsRequest, AggregateOp, AggregateRequest, AggregateResponse, Aggregation, BooleanQuery,
    CollapseSpec, CompositeSearchStrategy, DenseQuery, DocLineage, FacetValue, FilterQuery,
    IntegerValue, LexicalQuery, QueryHit, QueryRequest, QueryResponse, QuerySort, RrfScore,
    SearchQuery, SelectionOperator, SelectionQuery, SelectionScoreStrategy, SetCalibrationRequest,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

use common::{fit_calibration, mock::start_mock_analysis, start_empty_node, unit_vectors};

const DIM: usize = 64;
const SHARD_DOCS: usize = 4;
const N_DOCS: usize = 2 * SHARD_DOCS;
const COURTS: [&str; 3] = ["ca9", "ca2", "scotus"];

/// The `tests/query_api.rs` corpus: even ids say "zebra", odd ids say
/// "plain"; year = id; court round-robin over three values.
async fn start_cluster() -> (
    CoordinatorServiceImpl,
    Vec<f32>,
    Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
) {
    let (analysis, mock) = start_mock_analysis().await;
    let corpus = unit_vectors(N_DOCS, DIM, 0xC0FE_0001);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);
    let mut handles = vec![mock];
    let mut addrs = Vec::new();
    for shard in 0..2usize {
        let (addr, handle) = start_empty_node(NodeConfig {
            slot_offset: (shard * SHARD_DOCS) as u64,
            analysis_addr: Some(analysis.clone()),
            integer_fields: vec!["year".into()],
            facet_fields: vec!["court".into()],
            ..Default::default()
        })
        .await;
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        client
            .set_calibration(SetCalibrationRequest {
                dim: DIM as u32,
                bit_width: 4,
                shift: shift.clone(),
                scale: scale.clone(),
            })
            .await
            .unwrap();
        let (tx, rx) = mpsc::channel(16);
        for i in 0..SHARD_DOCS {
            let id = shard * SHARD_DOCS + i;
            let text = if id.is_multiple_of(2) {
                format!("zebra document {id}")
            } else {
                format!("plain document {id}")
            };
            tx.send(AddDocumentsRequest {
                text,
                integers: vec![IntegerValue {
                    field: "year".into(),
                    value: id as i64,
                }],
                facets: vec![FacetValue {
                    field: "court".into(),
                    value: COURTS[id % 3].into(),
                }],
                lineage: Some(DocLineage {
                    parent_id: (id / 2) as u64,
                    group_id: (id / 4) as u64,
                    span_start: 0,
                    span_end: 0,
                }),
                ..Default::default()
            })
            .await
            .unwrap();
        }
        drop(tx);
        client.add_documents(ReceiverStream::new(rx)).await.unwrap();
        let start = shard * SHARD_DOCS;
        let (vtx, vrx) = mpsc::channel(4);
        vtx.send(AddVectorsRequest {
            vectors: corpus[start * DIM..(start + SHARD_DOCS) * DIM].to_vec(),
            dim: DIM as u32,
        })
        .await
        .unwrap();
        drop(vtx);
        client.add_vectors(ReceiverStream::new(vrx)).await.unwrap();
        addrs.push(addr);
        handles.push(handle);
    }
    let coordinator =
        CoordinatorServiceImpl::new(addrs).with_bm25(Some(analysis), Default::default());
    (coordinator, corpus[..DIM].to_vec(), handles)
}

fn lexical_leaf(id: &str, text: &str) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: id.to_string(),
            query: Some(search_query::Query::Lexical(LexicalQuery {
                text: text.to_string(),
                ..Default::default()
            })),
        })),
    }
}

fn dense_leaf(id: &str, vector: &[f32]) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: id.to_string(),
            query: Some(search_query::Query::Dense(DenseQuery {
                vector: vector.to_vec(),
                ..Default::default()
            })),
        })),
    }
}

fn cel_filter(id: &str, cel: &str) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Filter(FilterQuery {
            id: id.to_string(),
            predicate: Some(pipestream_search::pb::filter_query::Predicate::Cel(
                cel.to_string(),
            )),
        })),
    }
}

fn rrf(clauses: Vec<SelectionQuery>) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Composite(CompositeSearchStrategy {
            operator: SelectionOperator::Or as i32,
            clauses,
            scoring: Some(SelectionScoreStrategy {
                strategy: Some(selection_score_strategy::Strategy::Rrf(RrfScore::default())),
            }),
        })),
    }
}

fn agg(name: &str, expression: &str, op: AggregateOp) -> Aggregation {
    Aggregation {
        name: name.into(),
        expression: expression.into(),
        op: op as i32,
        max_distinct: 0,
    }
}

/// COUNT and SUM of `year` plus a group-by over `court`.
fn year_by_court() -> AggregateRequest {
    AggregateRequest {
        aggregations: vec![
            agg("n", "year", AggregateOp::Count),
            agg("years", "year", AggregateOp::Sum),
        ],
        group_by: "court".into(),
        ..Default::default()
    }
}

async fn query(
    coordinator: &CoordinatorServiceImpl,
    req: QueryRequest,
) -> Result<QueryResponse, tonic::Status> {
    coordinator
        .query(Request::new(req))
        .await
        .map(|r| r.into_inner())
}

fn ids(hits: &[QueryHit]) -> Vec<u64> {
    hits.iter().map(|h| h.doc_id).collect()
}

fn int_result(response: &AggregateResponse, name: &str) -> i64 {
    let r = response
        .results
        .iter()
        .find(|r| r.name == name)
        .unwrap_or_else(|| panic!("aggregation {name} answered"));
    match r.value {
        Some(aggregate_result::Value::IntValue(v)) => v,
        other => panic!("{name}: expected an int, got {other:?}"),
    }
}

/// The reference: the pool's `year` values are its doc ids, so the
/// expected folds follow from the ids alone.
fn assert_pool(response: &AggregateResponse, pool: &[u64]) {
    assert_eq!(
        response.matched,
        pool.len() as u64,
        "matched is the pool size"
    );
    assert_eq!(int_result(response, "n"), pool.len() as i64);
    assert_eq!(
        int_result(response, "years"),
        pool.iter().map(|&id| id as i64).sum::<i64>()
    );
    let mut expected: std::collections::BTreeMap<&str, (u64, i64)> = Default::default();
    for &id in pool {
        let e = expected.entry(COURTS[id as usize % 3]).or_default();
        e.0 += 1;
        e.1 += id as i64;
    }
    let got: Vec<(&str, u64, i64)> = response
        .groups
        .iter()
        .map(|g| {
            let years = g
                .results
                .iter()
                .find(|r| r.name == "years")
                .and_then(|r| match r.value {
                    Some(aggregate_result::Value::IntValue(v)) => Some(v),
                    _ => None,
                })
                .unwrap();
            (g.value.as_str(), g.matched, years)
        })
        .collect();
    let want: Vec<(&str, u64, i64)> = expected
        .iter()
        .map(|(court, (n, years))| (*court, *n, *years))
        .collect();
    assert_eq!(got, want, "groups over the pool");
}

/// The page with and without the aggregation, bitwise.
fn assert_same_page(with: &QueryResponse, without: &QueryResponse) {
    assert_eq!(ids(&with.hits), ids(&without.hits));
    for (a, b) in with.hits.iter().zip(&without.hits) {
        assert_eq!(
            a.score.to_bits(),
            b.score.to_bits(),
            "score bits for {}",
            a.doc_id
        );
        assert_eq!(a.rank, b.rank);
    }
    assert_eq!(with.executed, without.executed);
    assert!(without.aggregate.is_none());
}

#[tokio::test]
async fn a_lexical_leaf_aggregates_its_selection_k_pool_and_pages_inside_it() {
    let (coordinator, _qvec, _handles) = start_cluster().await;
    // "zebra" matches the four even ids; the pool is all of them and
    // the page is the best two.
    let plain = query(
        &coordinator,
        QueryRequest {
            k: 2,
            selection: Some(lexical_leaf("z", "zebra")),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let first = query(
        &coordinator,
        QueryRequest {
            k: 2,
            selection_k: 4,
            selection: Some(lexical_leaf("z", "zebra")),
            aggregate: Some(year_by_court()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_same_page(&first, &plain);
    let aggregate = first.aggregate.as_ref().expect("the pool aggregate");
    let mut pool: Vec<u64> = (0..N_DOCS as u64).filter(|id| id % 2 == 0).collect();
    pool.sort_unstable();
    assert_pool(aggregate, &pool);
    // The second page draws from the same pool and reports the same
    // fold, bitwise.
    assert!(!first.next_cursor.is_empty());
    let second = query(
        &coordinator,
        QueryRequest {
            k: 2,
            selection_k: 4,
            selection: Some(lexical_leaf("z", "zebra")),
            aggregate: Some(year_by_court()),
            cursor: first.next_cursor.clone(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(second.aggregate, first.aggregate);
    assert_eq!(second.hits.len(), 2);
    let paged: std::collections::BTreeSet<u64> = ids(&first.hits)
        .into_iter()
        .chain(ids(&second.hits))
        .collect();
    assert_eq!(paged.into_iter().collect::<Vec<_>>(), pool);
    // Paging past the pool refuses rather than deepening it.
    let past = query(
        &coordinator,
        QueryRequest {
            k: 2,
            selection_k: 4,
            selection: Some(lexical_leaf("z", "zebra")),
            aggregate: Some(year_by_court()),
            cursor: second.next_cursor.clone(),
            ..Default::default()
        },
    )
    .await;
    match past {
        Err(status) => {
            assert_eq!(status.code(), tonic::Code::FailedPrecondition);
            assert!(status.message().contains("selection_k = 4"), "{status}");
        }
        Ok(response) => assert!(
            response.hits.is_empty() && second.next_cursor.is_empty(),
            "the pool was exhausted on the second page"
        ),
    }
}

#[tokio::test]
async fn a_dense_leaf_and_a_composite_aggregate_their_pools() {
    let (coordinator, qvec, _handles) = start_cluster().await;
    for (label, selection) in [
        ("dense", dense_leaf("vec", &qvec)),
        (
            "rrf",
            rrf(vec![dense_leaf("vec", &qvec), lexical_leaf("lex", "zebra")]),
        ),
    ] {
        let plain = query(
            &coordinator,
            QueryRequest {
                k: 2,
                selection_k: if label == "rrf" { 5 } else { 0 },
                selection: Some(selection.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap_or_else(|e| panic!("{label}: {e}"));
        let with = query(
            &coordinator,
            QueryRequest {
                k: 2,
                selection_k: 5,
                selection: Some(selection.clone()),
                aggregate: Some(year_by_court()),
                ..Default::default()
            },
        )
        .await
        .unwrap_or_else(|e| panic!("{label}: {e}"));
        // A single dense leaf's top-2 is depth-independent, so the
        // plain page ran at depth 2; the composite's page is already a
        // pooled page at depth 5.
        assert_same_page(&with, &plain);
        let aggregate = with.aggregate.as_ref().expect("the pool aggregate");
        // The pool is the top 5 of the same selection, which the
        // aggregation-free request at k = 5 names.
        let full = query(
            &coordinator,
            QueryRequest {
                k: 5,
                selection_k: 5,
                selection: Some(selection.clone()),
                ..Default::default()
            },
        )
        .await
        .unwrap_or_else(|e| panic!("{label}: {e}"));
        let mut pool = ids(&full.hits);
        pool.sort_unstable();
        assert_eq!(pool.len(), 5, "{label}: five candidates");
        assert_pool(aggregate, &pool);
        assert_eq!(&ids(&with.hits)[..], &ids(&full.hits)[..2], "{label}");
    }
}

#[tokio::test]
async fn a_collapse_aggregates_the_pool_before_grouping() {
    let (coordinator, _qvec, _handles) = start_cluster().await;
    // Four zebra docs over three courts: two groups asked for, the
    // fold covers the four documents, not the groups.
    let response = query(
        &coordinator,
        QueryRequest {
            k: 2,
            selection_k: 4,
            selection: Some(lexical_leaf("z", "zebra")),
            collapse: Some(CollapseSpec {
                column: "court".into(),
                inner_hits: 2,
            }),
            aggregate: Some(year_by_court()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(response.groups.len(), 2);
    let aggregate = response.aggregate.as_ref().expect("the pool aggregate");
    assert_pool(aggregate, &[0, 2, 4, 6]);
}

#[tokio::test]
async fn a_browse_aggregates_the_exact_filter_match_set() {
    let (coordinator, _qvec, _handles) = start_cluster().await;
    // A page of two over the five documents with year >= 3: the fold
    // covers the five.
    let response = query(
        &coordinator,
        QueryRequest {
            k: 2,
            selection: Some(cel_filter("late", "year >= 3")),
            aggregate: Some(year_by_court()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(response.hits.len(), 2);
    let aggregate = response.aggregate.as_ref().expect("the browse aggregate");
    assert_pool(aggregate, &[3, 4, 5, 6, 7]);
    // Sorted browse: the same set.
    let sorted = query(
        &coordinator,
        QueryRequest {
            k: 2,
            selection: Some(cel_filter("late", "year >= 3")),
            sort: vec![QuerySort {
                column: "year".into(),
                descending: true,
            }],
            aggregate: Some(year_by_court()),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(sorted.aggregate, response.aggregate);
}

#[tokio::test]
async fn pool_aggregation_refuses_by_name() {
    let (coordinator, qvec, _handles) = start_cluster().await;
    let cases: Vec<(&str, QueryRequest, &str)> = vec![
        (
            "boolean root",
            QueryRequest {
                k: 2,
                selection: Some(SelectionQuery {
                    node: Some(selection_query::Node::Boolean(BooleanQuery {
                        must: vec![lexical_leaf("z", "zebra")],
                        ..Default::default()
                    })),
                }),
                aggregate: Some(year_by_court()),
                ..Default::default()
            },
            "BooleanQuery.aggregate",
        ),
        (
            "foreign filter",
            QueryRequest {
                k: 2,
                selection: Some(dense_leaf("vec", &qvec)),
                aggregate: Some(AggregateRequest {
                    filter: "year > 1".into(),
                    ..year_by_court()
                }),
                ..Default::default()
            },
            "filter and geo_filters must be empty",
        ),
        (
            "sorted lexical leaf",
            QueryRequest {
                k: 2,
                selection: Some(lexical_leaf("z", "zebra")),
                sort: vec![QuerySort {
                    column: "year".into(),
                    descending: false,
                }],
                aggregate: Some(year_by_court()),
                ..Default::default()
            },
            "sorted lexical leaf",
        ),
        (
            "bad spec",
            QueryRequest {
                k: 2,
                selection: Some(dense_leaf("vec", &qvec)),
                aggregate: Some(AggregateRequest {
                    aggregations: vec![agg("n", "nonsense_column", AggregateOp::Count)],
                    ..Default::default()
                }),
                ..Default::default()
            },
            "nonsense_column",
        ),
    ];
    for (label, req, needle) in cases {
        let status = query(&coordinator, req)
            .await
            .err()
            .unwrap_or_else(|| panic!("{label}: refused"));
        assert_eq!(
            status.code(),
            tonic::Code::InvalidArgument,
            "{label}: {status}"
        );
        assert!(
            status.message().contains(needle),
            "{label}: {} lacks {needle:?}",
            status.message()
        );
    }
}
