//! The aggregation surface (`docs/aggregations.md`): exact folds of
//! CEL value expressions over the filtered corpus. The reference
//! results are computed HERE with the same documented algorithms
//! (Neumaier summation and Welford/Chan moments, per shard in doc
//! order, merged in shard order), so every assertion is bitwise; the
//! determinism contract is asserted by running twice.

mod common;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use turbovec_search::coordinator::CoordinatorServiceImpl;
use turbovec_search::node::NodeConfig;
use turbovec_search::pb::node_service_client::NodeServiceClient;
use turbovec_search::pb::search_service_server::SearchService;
use turbovec_search::pb::{
    aggregate_result::Value as W, AddDocumentsRequest, AggregateOp, AggregateRequest,
    AggregateResponse, Aggregation, FacetValue, IntegerValue, NumericValue,
};

use common::{mock::start_mock_analysis, start_empty_node};

const SHARD_DOCS: usize = 4;
const N_DOCS: usize = 2 * SHARD_DOCS;

/// The same corpus `tests/cel_values.rs` uses: price = id * 1.25 + 0.5
/// (absent for id 3), year = 1990 + id (absent for id 5), court one of
/// three strings.
fn price_of(id: usize) -> Option<f64> {
    (id != 3).then_some(id as f64 * 1.25 + 0.5)
}

fn year_of(id: usize) -> Option<i64> {
    (id != 5).then_some(1990 + id as i64)
}

fn court_of(id: usize) -> &'static str {
    ["scotus", "ca9", "ca2"][id % 3]
}

async fn start_cluster() -> (
    CoordinatorServiceImpl,
    Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
) {
    let (analysis, mock) = start_mock_analysis().await;
    let mut handles = vec![mock];
    let mut addrs = Vec::new();
    for shard in 0..2usize {
        let (addr, handle) = start_empty_node(NodeConfig {
            slot_offset: (shard * SHARD_DOCS) as u64,
            analysis_addr: Some(analysis.clone()),
            numeric_fields: vec!["price".into()],
            integer_fields: vec!["year".into()],
            facet_fields: vec!["court".into()],
            ..Default::default()
        })
        .await;
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        let (tx, rx) = mpsc::channel(16);
        for i in 0..SHARD_DOCS {
            let id = shard * SHARD_DOCS + i;
            tx.send(AddDocumentsRequest {
                text: format!("document number {id}"),
                numerics: price_of(id)
                    .map(|v| {
                        vec![NumericValue {
                            field: "price".into(),
                            value: v,
                        }]
                    })
                    .unwrap_or_default(),
                integers: year_of(id)
                    .map(|v| {
                        vec![IntegerValue {
                            field: "year".into(),
                            value: v,
                        }]
                    })
                    .unwrap_or_default(),
                facets: vec![FacetValue {
                    field: "court".into(),
                    value: court_of(id).into(),
                }],
                ..Default::default()
            })
            .await
            .unwrap();
        }
        drop(tx);
        client.add_documents(ReceiverStream::new(rx)).await.unwrap();
        addrs.push(addr);
        handles.push(handle);
    }
    let coordinator =
        CoordinatorServiceImpl::new(addrs).with_bm25(Some(analysis), Default::default());
    (coordinator, handles)
}

fn agg(name: &str, expression: &str, op: AggregateOp) -> Aggregation {
    Aggregation {
        name: name.into(),
        expression: expression.into(),
        op: op as i32,
    }
}

async fn run(
    coordinator: &CoordinatorServiceImpl,
    filter: &str,
    aggregations: Vec<Aggregation>,
) -> Result<AggregateResponse, tonic::Status> {
    coordinator
        .aggregate(Request::new(AggregateRequest {
            filter: filter.into(),
            geo_filters: Vec::new(),
            aggregations,
        }))
        .await
        .map(|r| r.into_inner())
}

// ---------------------------------------------------------------------
// The reference folds, exactly the engine's documented algorithms.
// ---------------------------------------------------------------------

fn neumaier_step(acc: &mut (f64, f64), x: f64) {
    let t = acc.0 + x;
    acc.1 += if acc.0.abs() >= x.abs() {
        (acc.0 - t) + x
    } else {
        (x - t) + acc.0
    };
    acc.0 = t;
}

/// Per-shard Neumaier in doc order, shard partials folded in shard
/// order (sum then compensation), final sum + compensation.
fn reference_sum(shards: &[Vec<f64>]) -> f64 {
    let mut acc = (0.0, 0.0);
    for docs in shards {
        let mut partial = (0.0, 0.0);
        for &x in docs {
            neumaier_step(&mut partial, x);
        }
        neumaier_step(&mut acc, partial.0);
        neumaier_step(&mut acc, partial.1);
    }
    acc.0 + acc.1
}

fn welford(values: &[f64]) -> (u64, f64, f64) {
    let (mut n, mut mean, mut m2) = (0u64, 0.0f64, 0.0f64);
    for &x in values {
        n += 1;
        let delta = x - mean;
        mean += delta / n as f64;
        m2 += delta * (x - mean);
    }
    (n, mean, m2)
}

fn chan(a: (u64, f64, f64), b: (u64, f64, f64)) -> (u64, f64, f64) {
    if b.0 == 0 {
        return a;
    }
    if a.0 == 0 {
        return b;
    }
    let n = a.0 + b.0;
    let delta = b.1 - a.1;
    let mean = a.1 + delta * (b.0 as f64 / n as f64);
    let m2 = a.2 + b.2 + delta * delta * (a.0 as f64 * b.0 as f64 / n as f64);
    (n, mean, m2)
}

/// Present price values per shard, in doc order.
fn price_shards() -> Vec<Vec<f64>> {
    (0..2)
        .map(|shard| {
            (0..SHARD_DOCS)
                .filter_map(|i| price_of(shard * SHARD_DOCS + i))
                .collect()
        })
        .collect()
}

/// Every op against the reference folds, bitwise, and the determinism
/// contract: the same request answers the same bits twice.
#[tokio::test]
async fn exact_aggregates_match_the_reference_folds() {
    let (coordinator, handles) = start_cluster().await;
    let aggregations = vec![
        agg("n_price", "price", AggregateOp::Count),
        agg("sum_price", "price", AggregateOp::Sum),
        agg("min_price", "price", AggregateOp::Min),
        agg("max_price", "price", AggregateOp::Max),
        agg("mean_price", "price", AggregateOp::Mean),
        agg("var_price", "price", AggregateOp::Variance),
        agg("sd_price", "price", AggregateOp::Stddev),
        agg("sum_year", "year", AggregateOp::Sum),
        agg("min_year", "year", AggregateOp::Min),
        agg("max_year", "year", AggregateOp::Max),
        agg("mean_year", "double(year)", AggregateOp::Mean),
    ];
    let first = run(&coordinator, "", aggregations.clone()).await.unwrap();
    let second = run(&coordinator, "", aggregations).await.unwrap();
    assert_eq!(first, second, "the same request answers the same bits");

    assert_eq!(first.matched, N_DOCS as u64, "no filter admits every doc");
    let shards = price_shards();
    let all: Vec<f64> = shards.iter().flatten().copied().collect();
    let (n, mean, m2) = chan(welford(&shards[0]), welford(&shards[1]));
    assert_eq!(n, 7, "price is present on 7 documents");
    let by_name = |name: &str| {
        first
            .results
            .iter()
            .find(|r| r.name == name)
            .unwrap_or_else(|| panic!("{name} missing"))
    };
    assert_eq!(by_name("n_price").present, 7);
    assert_eq!(by_name("n_price").value, Some(W::IntValue(7)));
    assert_eq!(
        by_name("sum_price").value,
        Some(W::DoubleValue(reference_sum(&shards)))
    );
    let min = all.iter().copied().fold(f64::INFINITY, f64::min);
    let max = all.iter().copied().fold(f64::NEG_INFINITY, f64::max);
    assert_eq!(by_name("min_price").value, Some(W::DoubleValue(min)));
    assert_eq!(by_name("max_price").value, Some(W::DoubleValue(max)));
    assert_eq!(by_name("mean_price").value, Some(W::DoubleValue(mean)));
    assert_eq!(
        by_name("var_price").value,
        Some(W::DoubleValue(m2 / n as f64))
    );
    assert_eq!(
        by_name("sd_price").value,
        Some(W::DoubleValue((m2 / n as f64).sqrt()))
    );
    // Sanity beside the bitwise pins: the folds agree with the naive
    // formulas to double precision on this tiny set.
    let naive_mean = all.iter().sum::<f64>() / all.len() as f64;
    assert!((mean - naive_mean).abs() < 1e-12);

    let years: Vec<i64> = (0..N_DOCS).filter_map(year_of).collect();
    assert_eq!(
        by_name("sum_year").value,
        Some(W::IntValue(years.iter().sum())),
        "int sums are exact"
    );
    assert_eq!(
        by_name("min_year").value,
        Some(W::IntValue(*years.iter().min().unwrap()))
    );
    assert_eq!(
        by_name("max_year").value,
        Some(W::IntValue(*years.iter().max().unwrap()))
    );
    assert_eq!(by_name("sum_year").present, 7, "year absent on doc 5");
    for h in handles {
        h.abort();
    }
}

/// The filter scopes the fold: only admitted documents feed any
/// aggregate, and `matched` is the admitted count.
#[tokio::test]
async fn the_filter_scopes_the_fold() {
    let (coordinator, handles) = start_cluster().await;
    let response = run(
        &coordinator,
        "court == \"ca9\"",
        vec![
            agg("n", "price", AggregateOp::Count),
            agg("total", "price", AggregateOp::Sum),
            agg("courts", "court", AggregateOp::Count),
            agg("scaled", "price * 2.0 + double(year)", AggregateOp::Sum),
        ],
    )
    .await
    .unwrap();
    let admitted: Vec<usize> = (0..N_DOCS).filter(|id| court_of(*id) == "ca9").collect();
    assert_eq!(response.matched, admitted.len() as u64);
    let prices: Vec<Vec<f64>> = (0..2)
        .map(|shard| {
            (0..SHARD_DOCS)
                .map(|i| shard * SHARD_DOCS + i)
                .filter(|id| court_of(*id) == "ca9")
                .filter_map(price_of)
                .collect()
        })
        .collect();
    let n: u64 = prices.iter().map(|s| s.len() as u64).sum();
    assert_eq!(response.results[0].present, n);
    assert_eq!(response.results[0].value, Some(W::IntValue(n as i64)));
    assert_eq!(
        response.results[1].value,
        Some(W::DoubleValue(reference_sum(&prices)))
    );
    assert_eq!(
        response.results[2].value,
        Some(W::IntValue(admitted.len() as i64)),
        "a string expression counts its present values"
    );
    let scaled: Vec<Vec<f64>> = (0..2)
        .map(|shard| {
            (0..SHARD_DOCS)
                .map(|i| shard * SHARD_DOCS + i)
                .filter(|id| court_of(*id) == "ca9")
                .filter_map(|id| Some(price_of(id)? * 2.0 + year_of(id)? as f64))
                .collect()
        })
        .collect();
    assert_eq!(
        response.results[3].value,
        Some(W::DoubleValue(reference_sum(&scaled))),
        "expressions with two absentable inputs skip a doc when either is absent"
    );
    for h in handles {
        h.abort();
    }
}

/// A selection admitting nothing reports matched 0, COUNT 0, and no
/// value anywhere else, never a fabricated zero.
#[tokio::test]
async fn an_empty_selection_reports_no_values() {
    let (coordinator, handles) = start_cluster().await;
    let response = run(
        &coordinator,
        "court == \"nonexistent\"",
        vec![
            agg("n", "price", AggregateOp::Count),
            agg("total", "price", AggregateOp::Sum),
            agg("mean", "price", AggregateOp::Mean),
        ],
    )
    .await
    .unwrap();
    assert_eq!(response.matched, 0);
    assert_eq!(response.results[0].value, Some(W::IntValue(0)));
    assert_eq!(response.results[1].value, None, "no values, no sum");
    assert_eq!(response.results[2].value, None, "no values, no mean");
    for h in handles {
        h.abort();
    }
}

/// An int total outside i64 refuses naming double() as the fix — the
/// per-document values fit, only the exact total overflows.
#[tokio::test]
async fn int_sum_overflow_refuses_naming_the_fix() {
    let (coordinator, handles) = start_cluster().await;
    let status = run(
        &coordinator,
        "",
        vec![agg(
            "big",
            "9223372036854775807 / 4",
            AggregateOp::Sum,
        )],
    )
    .await
    .expect_err("an out-of-i64 exact total must refuse");
    assert_eq!(status.code(), tonic::Code::FailedPrecondition);
    assert!(
        status.message().contains("double("),
        "the refusal names the fix: {}",
        status.message()
    );
    // The same expression under MIN serves: only SUM's total overflows.
    let response = run(
        &coordinator,
        "",
        vec![agg("m", "9223372036854775807 / 4", AggregateOp::Min)],
    )
    .await
    .unwrap();
    assert_eq!(
        response.results[0].value,
        Some(W::IntValue(i64::MAX / 4))
    );
    for h in handles {
        h.abort();
    }
}

/// Refusals name the aggregation and the problem: op/type conflicts,
/// booleans, typos, name hygiene, and the unspecified op.
#[tokio::test]
async fn refusals_name_the_aggregation() {
    let (coordinator, handles) = start_cluster().await;
    let cases: [(&str, AggregateOp, &str); 5] = [
        ("year", AggregateOp::Mean, "takes a double"),
        ("court", AggregateOp::Sum, "only under COUNT"),
        ("price > 2.0", AggregateOp::Count, "filter on the expression"),
        ("pricee", AggregateOp::Sum, "no shard has column pricee"),
        ("price ? 1 : 2", AggregateOp::Sum, "condition is a"),
    ];
    for (expr, op, needle) in cases {
        let status = run(&coordinator, "", vec![agg("a", expr, op)])
            .await
            .unwrap_err();
        assert!(
            status.message().contains(needle),
            "{expr:?}: wanted {needle:?} in {:?}",
            status.message()
        );
    }
    let status = run(&coordinator, "", vec![]).await.unwrap_err();
    assert!(status.message().contains("at least one aggregation"));
    let status = run(
        &coordinator,
        "",
        vec![
            agg("a", "price", AggregateOp::Sum),
            agg("a", "year", AggregateOp::Sum),
        ],
    )
    .await
    .unwrap_err();
    assert!(status.message().contains("duplicate aggregation name"));
    let status = run(&coordinator, "", vec![agg("", "price", AggregateOp::Sum)])
        .await
        .unwrap_err();
    assert!(status.message().contains("non-empty name"));
    let status = run(
        &coordinator,
        "",
        vec![agg("a", "price", AggregateOp::Unspecified)],
    )
    .await
    .unwrap_err();
    assert!(status.message().contains("unspecified operation"));
    // The filter's own typo rule holds on this route too.
    let status = run(
        &coordinator,
        "nosuch == 1",
        vec![agg("a", "price", AggregateOp::Sum)],
    )
    .await
    .unwrap_err();
    assert!(
        status.message().contains("nosuch"),
        "filter typos refuse by name: {}",
        status.message()
    );
    for h in handles {
        h.abort();
    }
}
