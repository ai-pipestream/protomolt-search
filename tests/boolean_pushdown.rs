//! The shard-side Boolean planner (`docs/query-api.md`, "Recursive
//! boolean execution"): membership and set algebra run on the shards
//! over their bitmaps, the coordinator merges ranked candidates, and no
//! membership crosses the wire. Two shards, so the merge, the paging
//! depth, and the fold are exercised across a shard boundary.

mod common;

use std::path::PathBuf;

use common::{fit_calibration, start_empty_node, unit_vectors};
use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::{Layout, NodeConfig};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    aggregate_result, selection_query, AddDocumentsRequest, AddVectorsRequest, AggregateOp,
    AggregateRequest, Aggregation, BooleanQuery, BoostQuery, CompositeSearchStrategy, DenseQuery,
    FacetValue, FilterQuery, FlushRequest, HistogramSpec, IntegerValue, LexicalQuery,
    NumericValue, PercentileSpec, QueryHit, QueryRequest, QueryResponse, SearchQuery,
    SelectionOperator, SelectionQuery, SetCalibrationRequest,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

const DIM: usize = 16;
const BIT_WIDTH: usize = 4;
const ROWS: usize = 3_000;
const SHARD_ROWS: usize = ROWS / 2;
/// Rows per sealed segment: three segments per shard.
const SEAL: usize = 500;
const BLOCK: usize = 250;

fn tempdir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("boolpush-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn config(index_path: PathBuf, slot_offset: u64) -> NodeConfig {
    NodeConfig {
        index_path: Some(index_path),
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        layout: Layout::Segments,
        seal_tail_docs: SEAL as u32,
        wal: false,
        slot_offset,
        facet_fields: vec!["court".into()],
        integer_fields: vec!["year".into()],
        numeric_fields: vec!["pages".into()],
        ..Default::default()
    }
}

/// Every row says "search"; every odd row says "zebra"; every fifth
/// says "quagga". Year cycles through 25 values, court through three,
/// pages through 97.
fn text(i: usize) -> String {
    let mut t = format!("opinion {i} about search");
    if i % 2 == 1 {
        t.push_str(" zebra");
    }
    if i.is_multiple_of(5) {
        t.push_str(" quagga");
    }
    t
}

fn year(i: usize) -> i64 {
    2000 + (i % 25) as i64
}

fn court(i: usize) -> &'static str {
    ["scotus", "ca9", "ca2"][i % 3]
}

fn pages(i: usize) -> f64 {
    (i % 97) as f64
}

fn corpus() -> Vec<f32> {
    unit_vectors(ROWS, DIM, 0xB00_1EA5)
}

async fn ingest(addr: &str, shard: usize) {
    let vectors = corpus();
    let sample = &vectors[..vectors.len().min(64 * DIM)];
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, sample);
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    client
        .set_calibration(SetCalibrationRequest {
            dim: DIM as u32,
            bit_width: BIT_WIDTH as u32,
            shift,
            scale,
        })
        .await
        .unwrap();
    let first = shard * SHARD_ROWS;
    for block in 0..SHARD_ROWS.div_ceil(BLOCK) {
        let start = first + block * BLOCK;
        let end = (start + BLOCK).min(first + SHARD_ROWS);
        let (tx, rx) = mpsc::channel(BLOCK);
        for i in start..end {
            tx.send(AddDocumentsRequest {
                text: text(i),
                analysis: Some(body_spec()),
                integers: vec![IntegerValue {
                    field: "year".into(),
                    value: year(i),
                }],
                facets: vec![FacetValue {
                    field: "court".into(),
                    value: court(i).into(),
                }],
                numerics: vec![NumericValue {
                    field: "pages".into(),
                    value: pages(i),
                }],
                ..Default::default()
            })
            .await
            .unwrap();
        }
        drop(tx);
        client.add_documents(ReceiverStream::new(rx)).await.unwrap();
        let (tx, rx) = mpsc::channel(2);
        tx.send(AddVectorsRequest {
            vectors: vectors[start * DIM..end * DIM].to_vec(),
            dim: DIM as u32,
        })
        .await
        .unwrap();
        drop(tx);
        client.add_vectors(ReceiverStream::new(rx)).await.unwrap();
    }
    client.flush(FlushRequest {}).await.unwrap();
}

struct Fleet {
    coordinator: CoordinatorServiceImpl,
    dir: PathBuf,
    handles: Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
}

async fn fleet(tag: &str) -> Fleet {
    let dir = tempdir(tag);
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for shard in 0..2 {
        let (addr, handle) = start_empty_node(config(
            dir.join(format!("shard-{shard}.tv")),
            (shard * SHARD_ROWS) as u64,
        ))
        .await;
        ingest(&addr, shard).await;
        addrs.push(addr);
        handles.push(handle);
    }
    let coordinator = CoordinatorServiceImpl::new(addrs).with_bm25(
        Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        Default::default(),
    );
    Fleet {
        coordinator,
        dir,
        handles,
    }
}

impl Fleet {
    fn stop(self) {
        for handle in self.handles {
            handle.abort();
        }
        let _ = std::fs::remove_dir_all(&self.dir);
    }
}

fn lexical(id: &str, text: &str) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: id.to_string(),
            query: Some(pipestream_search::pb::search_query::Query::Lexical(
                LexicalQuery {
                    text: text.to_string(),
                    analysis: Some(body_spec()),
                    ..Default::default()
                },
            )),
        })),
    }
}

fn dense(id: &str, q: usize) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: id.to_string(),
            query: Some(pipestream_search::pb::search_query::Query::Dense(
                DenseQuery {
                    vector: corpus()[q * DIM..(q + 1) * DIM].to_vec(),
                    ..Default::default()
                },
            )),
        })),
    }
}

fn cel(id: &str, cel: &str) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Filter(FilterQuery {
            id: id.to_string(),
            predicate: Some(pipestream_search::pb::filter_query::Predicate::Cel(
                cel.to_string(),
            )),
        })),
    }
}

fn and(clauses: Vec<SelectionQuery>) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Composite(CompositeSearchStrategy {
            operator: SelectionOperator::And as i32,
            clauses,
            scoring: None,
        })),
    }
}

fn boolean(
    must: Vec<SelectionQuery>,
    should: Vec<SelectionQuery>,
    must_not: Vec<SelectionQuery>,
    minimum_should_match: u32,
    aggregate: Option<AggregateRequest>,
) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Boolean(BooleanQuery {
            must,
            should,
            must_not,
            minimum_should_match,
            aggregate,
        })),
    }
}

fn request(selection: SelectionQuery, k: u32) -> QueryRequest {
    QueryRequest {
        request_id: "boolpush".into(),
        k,
        selection: Some(selection),
        profile: true,
        ..Default::default()
    }
}

async fn query(c: &CoordinatorServiceImpl, req: QueryRequest) -> QueryResponse {
    SearchService::query(c, Request::new(req))
        .await
        .unwrap()
        .into_inner()
}

fn bits(hits: &[QueryHit]) -> Vec<(u64, u32)> {
    hits.iter().map(|h| (h.doc_id, h.score.to_bits())).collect()
}

fn ids(hits: &[QueryHit]) -> Vec<u64> {
    hits.iter().map(|h| h.doc_id).collect()
}

/// The process-wide request count of one route.
fn route_count(rpc: &str) -> u64 {
    let snapshot = pipestream_search::metrics::snapshot("test", &[]);
    snapshot
        .samples
        .iter()
        .find(|s| {
            s.name == "turbovec_requests_total"
                && s.labels.iter().any(|l| l.name == "rpc" && l.value == rpc)
        })
        .map(|s| s.value as u64)
        .expect("the route is exported")
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_filter_and_a_search_equal_the_and_composite_without_a_bitmap_route() {
    let fleet = fleet("composite").await;
    let c = &fleet.coordinator;
    let filter_bitmaps = route_count("resolve_filter_bitmap");
    let evaluations = route_count("evaluate_boolean");
    // Lexical: the same BM25 under the same global statistics.
    let planned = query(
        c,
        request(
            boolean(
                vec![lexical("l", "zebra"), cel("f", "year >= 2010")],
                vec![],
                vec![],
                0,
                None,
            ),
            50,
        ),
    )
    .await;
    let ordinary = query(
        c,
        request(and(vec![cel("f", "year >= 2010"), lexical("l", "zebra")]), 50),
    )
    .await;
    assert_eq!(planned.hits.len(), 50);
    assert_eq!(bits(&planned.hits), bits(&ordinary.hits), "lexical");
    assert_eq!(planned.executed, "boolean:bitmap");
    for hit in &planned.hits {
        assert_eq!(hit.matched, vec!["l".to_string(), "f".to_string()]);
        assert_eq!(hit.signals.len(), 1);
        assert_eq!(hit.signals[0].id, "l");
    }
    // Dense: the same calibrated products.
    let planned = query(
        c,
        request(
            boolean(
                vec![dense("v", 7), cel("f", "year >= 2010")],
                vec![],
                vec![],
                0,
                None,
            ),
            50,
        ),
    )
    .await;
    let ordinary = query(
        c,
        request(and(vec![cel("f", "year >= 2010"), dense("v", 7)]), 50),
    )
    .await;
    assert_eq!(planned.hits.len(), 50);
    assert_eq!(bits(&planned.hits), bits(&ordinary.hits), "dense");
    assert!(planned.hits.iter().all(|h| ids(&[h.clone()])[0] % 25 >= 10));
    // The profile counts the segments each leaf consulted, the filter
    // and the dense leaf apart: the filter rules out the years below
    // 2010 on no segment (every segment holds every year).
    let profile = planned.profile.as_ref().unwrap();
    assert_eq!((profile.shards_total, profile.shards_skipped), (2, 0));
    assert_eq!(profile.segments_total, 6);
    assert_eq!(profile.segments_skipped, 0);
    // Two queries over two shards: two evaluations each (the counters
    // are process-wide, so other tests in this binary may add more),
    // and no membership bitmap fetched.
    assert_eq!(route_count("resolve_filter_bitmap"), filter_bitmaps);
    assert!(route_count("evaluate_boolean") >= evaluations + 4);
    fleet.stop();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn pages_walk_the_order_one_deep_page_has() {
    let fleet = fleet("pages").await;
    let c = &fleet.coordinator;
    let selection = || {
        boolean(
            vec![lexical("l", "search"), dense("v", 3)],
            vec![],
            vec![cel("f", "year == 2003")],
            0,
            None,
        )
    };
    let deep = query(c, request(selection(), 35)).await;
    assert_eq!(deep.hits.len(), 35);
    assert!(deep.hits.iter().all(|h| h.doc_id % 25 != 3));
    let mut walked = Vec::new();
    let mut cursor = String::new();
    for _ in 0..5 {
        let page = query(
            c,
            QueryRequest {
                cursor: cursor.clone(),
                ..request(selection(), 7)
            },
        )
        .await;
        assert_eq!(page.hits.len(), 7);
        cursor = page.next_cursor.clone();
        assert!(!cursor.is_empty());
        walked.extend(page.hits);
    }
    assert_eq!(bits(&walked), bits(&deep.hits));
    assert_eq!(
        walked.iter().map(|h| h.rank).collect::<Vec<_>>(),
        (1..=35).collect::<Vec<_>>()
    );
    fleet.stop();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_group_rule_holds_across_shards() {
    let fleet = fleet("rule").await;
    let c = &fleet.coordinator;
    // At least two of {odd, fifth, every row} hold, minus year 2005.
    let r = query(
        c,
        request(
            boolean(
                vec![],
                vec![lexical("a", "zebra"), lexical("b", "quagga"), dense("v", 11)],
                vec![cel("n", "year == 2005")],
                2,
                None,
            ),
            ROWS as u32,
        ),
    )
    .await;
    let mut got = ids(&r.hits);
    got.sort_unstable();
    let want: Vec<u64> = (0..ROWS as u64)
        .filter(|i| (i % 2 == 1 || i % 5 == 0) && i % 25 != 5)
        .collect();
    assert_eq!(got, want);
    for hit in &r.hits {
        let odd = hit.doc_id % 2 == 1;
        let fifth = hit.doc_id % 5 == 0;
        let mut matched = Vec::new();
        if odd {
            matched.push("a".to_string());
        }
        if fifth {
            matched.push("b".to_string());
        }
        matched.push("v".to_string());
        assert_eq!(hit.matched, matched, "doc {}", hit.doc_id);
        let signals: Vec<&str> = hit.signals.iter().map(|s| s.id.as_str()).collect();
        assert_eq!(signals, matched, "doc {}", hit.doc_id);
        let sum = hit.signals.iter().fold(0.0f32, |acc, s| acc + s.score);
        assert_eq!(sum.to_bits(), hit.score.to_bits(), "doc {}", hit.doc_id);
    }
    // A MUST filter with a SHOULD that scores some members: the scored
    // rows first, then the rest in id order with no signal.
    let r = query(
        c,
        request(
            boolean(
                vec![cel("f", "year >= 2020")],
                vec![lexical("q", "quagga")],
                vec![],
                0,
                None,
            ),
            ROWS as u32,
        ),
    )
    .await;
    let members = (0..ROWS as u64).filter(|i| i % 25 >= 20).count();
    assert_eq!(r.hits.len(), members);
    let scored = r.hits.iter().take_while(|h| h.score > 0.0).count();
    assert_eq!(scored, (0..ROWS as u64).filter(|i| i % 25 == 20).count());
    let tail: Vec<u64> = r.hits[scored..].iter().map(|h| h.doc_id).collect();
    let mut sorted = tail.clone();
    sorted.sort_unstable();
    assert_eq!(tail, sorted, "zero-score members in id order");
    assert!(r.hits[scored..].iter().all(|h| h.signals.is_empty()));
    assert!(r.hits[scored..]
        .iter()
        .all(|h| h.matched == vec!["f".to_string()]));
    fleet.stop();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_boost_reorders_the_selection_pool_named_by_selection_k() {
    let fleet = fleet("pool").await;
    let c = &fleet.coordinator;
    let boost = || BoostQuery {
        query: Some(SearchQuery {
            id: "boost".into(),
            query: Some(pipestream_search::pb::search_query::Query::Lexical(
                LexicalQuery {
                    text: "quagga".into(),
                    analysis: Some(body_spec()),
                    ..Default::default()
                },
            )),
        }),
        ..Default::default()
    };
    let selection = || boolean(vec![lexical("l", "zebra")], vec![], vec![], 0, None);
    let base = query(c, request(selection(), 20)).await;
    let boosted = query(
        c,
        QueryRequest {
            selection_k: 20,
            boosts: vec![boost()],
            ..request(selection(), 10)
        },
    )
    .await;
    assert_eq!(boosted.hits.len(), 10);
    let mut pool = ids(&base.hits);
    pool.sort_unstable();
    for hit in &boosted.hits {
        assert!(pool.binary_search(&hit.doc_id).is_ok(), "from the pool");
        if hit.doc_id % 5 == 0 {
            assert!(hit.matched.contains(&"boost".to_string()));
        }
    }
    let err = SearchService::query(
        c,
        Request::new(QueryRequest {
            selection_k: 5,
            boosts: vec![boost()],
            ..request(selection(), 10)
        }),
    )
    .await
    .unwrap_err();
    assert!(
        err.message().contains("selection_k"),
        "{}",
        err.message()
    );
    let err = SearchService::query(
        c,
        Request::new(QueryRequest {
            selection_k: 20,
            ..request(selection(), 10)
        }),
    )
    .await
    .unwrap_err();
    assert!(
        err.message().contains("selection_k"),
        "{}",
        err.message()
    );
    fleet.stop();
}

fn aggregate_spec() -> AggregateRequest {
    AggregateRequest {
        aggregations: vec![Aggregation {
            name: "pages_sum".into(),
            expression: "pages".into(),
            op: AggregateOp::Sum as i32,
            max_distinct: 0,
        }],
        group_by: "court".into(),
        histograms: vec![HistogramSpec {
            name: "pages_hist".into(),
            expression: "pages".into(),
            interval: 10.0,
            ..Default::default()
        }],
        percentiles: vec![PercentileSpec {
            name: "pages_pct".into(),
            expression: "pages".into(),
            percentiles: vec![50.0, 90.0],
            ..Default::default()
        }],
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_root_aggregate_folds_on_the_shards() {
    let fleet = fleet("fold").await;
    let c = &fleet.coordinator;
    // A filter-only root against the Aggregate route under the same
    // predicate: results, groups, histogram, and percentiles agree.
    let r = query(
        c,
        request(
            boolean(
                vec![cel("y", "year >= 2010"), cel("p", "pages < 50")],
                vec![],
                vec![],
                0,
                Some(aggregate_spec()),
            ),
            5,
        ),
    )
    .await;
    let want = SearchService::aggregate(
        c,
        Request::new(AggregateRequest {
            filter: "year >= 2010 && pages < 50".into(),
            ..aggregate_spec()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let got = r.aggregate.expect("root aggregate");
    assert_eq!(got, want);
    assert!(got.matched > 0);
    assert_eq!(got.percentiles[0].values.len(), 2);
    // A lexical clause in the tree: the fold covers the match set the
    // shards resolved, counted here from the corpus rule.
    let r = query(
        c,
        request(
            boolean(
                vec![lexical("l", "zebra"), cel("y", "year >= 2010")],
                vec![],
                vec![],
                0,
                Some(aggregate_spec()),
            ),
            5,
        ),
    )
    .await;
    let got = r.aggregate.expect("root aggregate");
    let members: Vec<usize> = (0..ROWS).filter(|i| i % 2 == 1 && i % 25 >= 10).collect();
    assert_eq!(got.matched, members.len() as u64);
    let sum: f64 = members.iter().map(|&i| pages(i)).sum();
    match got.results[0].value {
        Some(aggregate_result::Value::DoubleValue(folded)) => {
            assert!((folded - sum).abs() < 1e-6, "{folded} vs {sum}")
        }
        other => panic!("sum over pages is a double: {other:?}"),
    }
    assert_eq!(got.percentiles[0].present, members.len() as u64);
    let mut sorted: Vec<f64> = members.iter().map(|&i| pages(i)).collect();
    sorted.sort_by(f64::total_cmp);
    let nearest = |p: f64| {
        let k = ((p / 100.0 * sorted.len() as f64).ceil() as usize).clamp(1, sorted.len());
        sorted[k - 1]
    };
    for value in &got.percentiles[0].values {
        assert_eq!(
            value.value,
            Some(pipestream_search::pb::percentile_value::Value::DoubleValue(
                nearest(value.percentile)
            )),
            "p{}",
            value.percentile
        );
    }
    fleet.stop();
}
