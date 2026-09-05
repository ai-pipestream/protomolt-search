//! The aggregation surface (`docs/aggregations.md`): exact folds of
//! CEL value expressions over the filtered corpus. The reference
//! results are computed HERE with the same documented algorithms
//! (Neumaier summation and Welford/Chan moments, per shard in doc
//! order, merged in shard order), so every assertion is bitwise; the
//! determinism contract is asserted by running twice.

mod common;

use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    aggregate_result::Value as W, percentile_value::Value as P, AddDocumentsRequest, AggregateOp,
    AggregateRequest, AggregateResponse, Aggregation, FacetValue, HistogramSpec, IntegerValue,
    NumericValue, PercentileSpec,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

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

fn court_of(id: usize) -> Option<&'static str> {
    (id != 6).then(|| ["scotus", "ca9", "ca2"][id % 3])
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
                facets: court_of(id)
                    .map(|v| {
                        vec![FacetValue {
                            field: "court".into(),
                            value: v.into(),
                        }]
                    })
                    .unwrap_or_default(),
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
        max_distinct: 0,
    }
}

fn hist(name: &str, expression: &str, interval: f64, max_buckets: u32) -> HistogramSpec {
    HistogramSpec {
        name: name.into(),
        expression: expression.into(),
        interval,
        max_buckets,
        calendar: 0,
        utc_offset_minutes: 0,
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
            aggregations,
            ..Default::default()
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
    let admitted: Vec<usize> = (0..N_DOCS)
        .filter(|id| court_of(*id) == Some("ca9"))
        .collect();
    assert_eq!(response.matched, admitted.len() as u64);
    let prices: Vec<Vec<f64>> = (0..2)
        .map(|shard| {
            (0..SHARD_DOCS)
                .map(|i| shard * SHARD_DOCS + i)
                .filter(|id| court_of(*id) == Some("ca9"))
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
                .filter(|id| court_of(*id) == Some("ca9"))
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
        vec![agg("big", "9223372036854775807 / 4", AggregateOp::Sum)],
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
    assert_eq!(response.results[0].value, Some(W::IntValue(i64::MAX / 4)));
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
        (
            "price > 2.0",
            AggregateOp::Count,
            "filter on the expression",
        ),
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

/// Grouping folds every aggregation once per facet value with the
/// same exactness contract as the totals; groups return ascending by
/// value, a doc without the value counts in `ungrouped`, and the
/// fleet-wide totals still cover every admitted doc.
#[tokio::test]
async fn group_by_facet_folds_per_group() {
    let (coordinator, handles) = start_cluster().await;
    let request = AggregateRequest {
        aggregations: vec![
            agg("n", "price", AggregateOp::Count),
            agg("total", "price", AggregateOp::Sum),
            agg("mean", "price", AggregateOp::Mean),
        ],
        group_by: "court".into(),
        ..Default::default()
    };
    let first = coordinator
        .aggregate(Request::new(request.clone()))
        .await
        .unwrap()
        .into_inner();
    let second = coordinator
        .aggregate(Request::new(request))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(first, second, "grouped folds answer the same bits twice");

    assert_eq!(first.matched, N_DOCS as u64);
    assert_eq!(first.ungrouped, 1, "doc 6 carries no court");
    let mut expected_values: Vec<&str> = (0..N_DOCS).filter_map(court_of).collect();
    expected_values.sort_unstable();
    expected_values.dedup();
    assert_eq!(
        first
            .groups
            .iter()
            .map(|g| g.value.as_str())
            .collect::<Vec<_>>(),
        expected_values,
        "groups return ascending by value"
    );
    for group in &first.groups {
        let members: Vec<usize> = (0..N_DOCS)
            .filter(|id| court_of(*id) == Some(group.value.as_str()))
            .collect();
        assert_eq!(group.matched, members.len() as u64, "group {}", group.value);
        let prices: Vec<Vec<f64>> = (0..2)
            .map(|shard| {
                (0..SHARD_DOCS)
                    .map(|i| shard * SHARD_DOCS + i)
                    .filter(|id| members.contains(id))
                    .filter_map(price_of)
                    .collect()
            })
            .collect();
        let n: u64 = prices.iter().map(|s| s.len() as u64).sum();
        assert_eq!(group.results[0].value, Some(W::IntValue(n as i64)));
        assert_eq!(
            group.results[1].value,
            Some(W::DoubleValue(reference_sum(&prices))),
            "group {}",
            group.value
        );
        let (wn, mean, _) = chan(welford(&prices[0]), welford(&prices[1]));
        assert_eq!(wn, n);
        assert_eq!(
            group.results[2].value,
            Some(W::DoubleValue(mean)),
            "group {}",
            group.value
        );
    }
    // The fleet-wide totals still cover the ungrouped doc.
    let total_present: u64 = (0..N_DOCS).filter_map(price_of).count() as u64;
    assert_eq!(
        first.results[0].value,
        Some(W::IntValue(total_present as i64))
    );
    for h in handles {
        h.abort();
    }
}

/// Histograms bucket by floor(value / interval), sparse and exact;
/// unbucketable values (NaN, infinity) are reported, never dropped
/// silently.
#[tokio::test]
async fn histograms_bucket_exactly() {
    let (coordinator, handles) = start_cluster().await;
    let response = coordinator
        .aggregate(Request::new(AggregateRequest {
            histograms: vec![
                hist("price", "price", 2.5, 0),
                hist("negated", "0.0 - price", 2.5, 0),
                hist("inf", "1.0 / (price - price)", 2.5, 0),
            ],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let prices: Vec<f64> = (0..N_DOCS).filter_map(price_of).collect();
    for (result, transform) in response.histograms[..2].iter().zip([1.0f64, -1.0]) {
        let mut expected: std::collections::BTreeMap<i64, u64> = Default::default();
        for &p in &prices {
            let idx = (p * transform / 2.5).floor() as i64;
            *expected.entry(idx).or_insert(0) += 1;
        }
        let got: Vec<(f64, u64)> = result.buckets.iter().map(|b| (b.lower, b.count)).collect();
        let want: Vec<(f64, u64)> = expected
            .iter()
            .map(|(&i, &c)| (i as f64 * 2.5, c))
            .collect();
        assert_eq!(got, want, "{}", result.name);
        assert_eq!(result.present, prices.len() as u64);
        assert_eq!(result.unbucketable, 0);
    }
    let inf = &response.histograms[2];
    assert_eq!(inf.present, prices.len() as u64);
    assert_eq!(
        inf.unbucketable,
        prices.len() as u64,
        "an infinity has no honest bucket and is reported"
    );
    assert!(inf.buckets.is_empty());
    for h in handles {
        h.abort();
    }
}

/// The caps refuse loudly, never truncate; histogram and grouping
/// validation names the problem.
#[tokio::test]
async fn caps_and_shapes_refuse_loudly() {
    let (coordinator, handles) = start_cluster().await;
    let base = |aggregations, group_by: &str, max_groups, histograms| AggregateRequest {
        aggregations,
        group_by: group_by.into(),
        max_groups,
        histograms,
        ..Default::default()
    };
    let status = coordinator
        .aggregate(Request::new(base(
            vec![agg("n", "price", AggregateOp::Count)],
            "court",
            2,
            Vec::new(),
        )))
        .await
        .unwrap_err();
    assert!(
        status.message().contains("distinct values"),
        "3 courts against a cap of 2: {}",
        status.message()
    );
    let status = coordinator
        .aggregate(Request::new(base(
            Vec::new(),
            "",
            0,
            vec![hist("h", "price", 2.5, 2)],
        )))
        .await
        .unwrap_err();
    assert!(
        status.message().contains("buckets"),
        "4 buckets against a cap of 2: {}",
        status.message()
    );
    for (interval, needle) in [
        (0.0, "positive and finite"),
        (f64::INFINITY, "positive and finite"),
    ] {
        let status = coordinator
            .aggregate(Request::new(base(
                Vec::new(),
                "",
                0,
                vec![hist("h", "price", interval, 0)],
            )))
            .await
            .unwrap_err();
        assert!(status.message().contains(needle), "{}", status.message());
    }
    let status = coordinator
        .aggregate(Request::new(base(
            Vec::new(),
            "",
            0,
            vec![hist("h", "year", 10.0, 0)],
        )))
        .await
        .unwrap_err();
    assert!(
        status.message().contains("double()"),
        "an int histogram names the conversion: {}",
        status.message()
    );
    let status = coordinator
        .aggregate(Request::new(base(
            vec![agg("n", "price", AggregateOp::Count)],
            "price",
            0,
            Vec::new(),
        )))
        .await
        .unwrap_err();
    assert!(
        status.message().contains("not a facet column"),
        "group_by over a numeric column: {}",
        status.message()
    );
    let status = coordinator
        .aggregate(Request::new(base(
            vec![agg("x", "price", AggregateOp::Count)],
            "",
            0,
            vec![hist("x", "price", 2.5, 0)],
        )))
        .await
        .unwrap_err();
    assert!(
        status.message().contains("duplicate"),
        "one name namespace across aggregations and histograms: {}",
        status.message()
    );
    for h in handles {
        h.abort();
    }
}

fn pct(name: &str, expression: &str, percentiles: &[f64]) -> PercentileSpec {
    PercentileSpec {
        name: name.into(),
        expression: expression.into(),
        percentiles: percentiles.to_vec(),
    }
}

async fn run_percentiles(
    coordinator: &CoordinatorServiceImpl,
    filter: &str,
    percentiles: Vec<PercentileSpec>,
) -> Result<AggregateResponse, tonic::Status> {
    coordinator
        .aggregate(Request::new(AggregateRequest {
            filter: filter.into(),
            percentiles,
            ..Default::default()
        }))
        .await
        .map(|r| r.into_inner())
}

/// Nearest rank: k = max(1, ceil(p/100 * n)), the k-th smallest.
fn nearest_rank(sorted: &[f64], p: f64) -> (u64, f64) {
    let n = sorted.len() as u64;
    let k = ((p / 100.0 * n as f64).ceil() as u64).clamp(1, n);
    (k, sorted[(k - 1) as usize])
}

/// Every percentile answers the exact nearest-rank order statistic, in
/// the expression's type; computed NaN counts as unrankable; and the
/// same request answers the same bits twice.
#[tokio::test]
async fn percentiles_answer_exact_order_statistics() {
    let (coordinator, handles) = start_cluster().await;
    let asked = [0.0, 10.0, 25.0, 50.0, 75.0, 90.0, 95.0, 100.0];
    let specs = vec![
        pct("price", "price", &asked),
        pct("year", "year", &[0.0, 50.0, 100.0]),
        pct(
            "cheap",
            "price < 5.0 ? price : (price - price) / 0.0",
            &[100.0],
        ),
    ];
    let first = run_percentiles(&coordinator, "", specs.clone())
        .await
        .unwrap();
    let second = run_percentiles(&coordinator, "", specs).await.unwrap();
    assert_eq!(first, second, "percentiles answer the same bits twice");

    let mut prices: Vec<f64> = (0..N_DOCS).filter_map(price_of).collect();
    prices.sort_unstable_by(f64::total_cmp);
    let price_result = &first.percentiles[0];
    assert_eq!(price_result.present, prices.len() as u64);
    assert_eq!(price_result.unrankable, 0);
    for (value, &p) in price_result.values.iter().zip(&asked) {
        let (k, expected) = nearest_rank(&prices, p);
        assert_eq!(value.percentile, p);
        assert_eq!(value.rank, k, "p{p}");
        assert_eq!(
            value.value,
            Some(P::DoubleValue(expected)),
            "p{p} answers the exact k-th smallest"
        );
    }

    let mut years: Vec<i64> = (0..N_DOCS).filter_map(year_of).collect();
    years.sort_unstable();
    let year_result = &first.percentiles[1];
    for (value, &p) in year_result.values.iter().zip(&[0.0, 50.0, 100.0]) {
        let n = years.len() as u64;
        let k = ((p / 100.0 * n as f64).ceil() as u64).clamp(1, n);
        assert_eq!(
            value.value,
            Some(P::IntValue(years[(k - 1) as usize])),
            "p{p} answers in the expression's own type"
        );
    }

    let cheap = &first.percentiles[2];
    let rankable: Vec<f64> = prices.iter().copied().filter(|&x| x < 5.0).collect();
    assert_eq!(cheap.present, rankable.len() as u64);
    assert_eq!(
        cheap.unrankable,
        (prices.len() - rankable.len()) as u64,
        "computed NaN is reported, never silently dropped"
    );
    assert_eq!(
        cheap.values[0].value,
        Some(P::DoubleValue(*rankable.last().unwrap())),
        "p100 ranks only the rankable values"
    );
    for h in handles {
        h.abort();
    }
}

/// The filter scopes the ranking, and an empty selection answers rank
/// 0 with no value.
#[tokio::test]
async fn percentiles_respect_the_filter() {
    let (coordinator, handles) = start_cluster().await;
    let response = run_percentiles(
        &coordinator,
        "court == \"ca9\"",
        vec![pct("p", "price", &[0.0, 50.0, 100.0])],
    )
    .await
    .unwrap();
    let mut prices: Vec<f64> = (0..N_DOCS)
        .filter(|id| court_of(*id) == Some("ca9"))
        .filter_map(price_of)
        .collect();
    prices.sort_unstable_by(f64::total_cmp);
    let result = &response.percentiles[0];
    assert_eq!(result.present, prices.len() as u64);
    for (value, &p) in result.values.iter().zip(&[0.0, 50.0, 100.0]) {
        let (k, expected) = nearest_rank(&prices, p);
        assert_eq!(value.rank, k);
        assert_eq!(value.value, Some(P::DoubleValue(expected)), "p{p}");
    }

    let empty = run_percentiles(
        &coordinator,
        "court == \"nonexistent\"",
        vec![pct("p", "price", &[50.0])],
    )
    .await
    .unwrap();
    assert_eq!(empty.percentiles[0].present, 0);
    assert_eq!(empty.percentiles[0].values[0].rank, 0);
    assert_eq!(
        empty.percentiles[0].values[0].value, None,
        "no values, no percentile, never a fabricated zero"
    );
    for h in handles {
        h.abort();
    }
}

/// Percentile refusals name the spec and the problem.
#[tokio::test]
async fn percentile_refusals_name_the_spec() {
    let (coordinator, handles) = start_cluster().await;
    for (spec, needle) in [
        (pct("p", "price", &[101.0]), "not a percentile"),
        (pct("p", "price", &[f64::NAN]), "not a percentile"),
        (pct("p", "price", &[-1.0]), "not a percentile"),
        (pct("p", "price", &[]), "1 to 16 percentile values"),
        (pct("p", "court", &[50.0]), "ranks numbers"),
        (pct("p", "price > 2.0", &[50.0]), "ranks numbers"),
        (pct("", "price", &[50.0]), "non-empty name"),
        (pct("p", "pricee", &[50.0]), "no shard has column pricee"),
    ] {
        let status = run_percentiles(&coordinator, "", vec![spec.clone()])
            .await
            .unwrap_err();
        assert!(
            status.message().contains(needle),
            "{spec:?}: wanted {needle:?} in {:?}",
            status.message()
        );
    }
    // One name namespace across all three families.
    let status = coordinator
        .aggregate(Request::new(AggregateRequest {
            aggregations: vec![agg("x", "price", AggregateOp::Count)],
            percentiles: vec![pct("x", "price", &[50.0])],
            ..Default::default()
        }))
        .await
        .unwrap_err();
    assert!(status.message().contains("duplicate"));
    for h in handles {
        h.abort();
    }
}

// ---------------------------------------------------------------------
// Cardinality: the exact distinct count (docs/aggregations.md).
// ---------------------------------------------------------------------

fn card(name: &str, expression: &str, max_distinct: u32) -> Aggregation {
    Aggregation {
        name: name.into(),
        expression: expression.into(),
        op: AggregateOp::Cardinality as i32,
        max_distinct,
    }
}

fn int_of(response: &AggregateResponse, name: &str) -> i64 {
    let r = response
        .results
        .iter()
        .find(|r| r.name == name)
        .unwrap_or_else(|| panic!("{name} answered"));
    match r.value {
        Some(W::IntValue(v)) => v,
        other => panic!("{name}: expected an int, got {other:?}"),
    }
}

/// Distinct counts over every type, exact across the shard boundary,
/// with doubles canonical (one zero, one NaN) and absence excluded.
#[tokio::test]
async fn cardinality_counts_distinct_values_exactly() {
    let (coordinator, handles) = start_cluster().await;
    let response = run(
        &coordinator,
        "",
        vec![
            card("courts", "court", 0),
            card("years", "year", 0),
            card("prices", "price", 0),
            // -0.0 on the early years, 0.0 on the late ones: one value.
            card(
                "zeros",
                "year > 1993 ? (price - price) : (0.0 - (price - price))",
                0,
            ),
            card("nans", "(price - price) / (price - price)", 0),
            card("late", "year > 1993", 0),
            agg("n", "year", AggregateOp::Count),
        ],
    )
    .await
    .unwrap();
    let courts: std::collections::BTreeSet<&str> = (0..N_DOCS).filter_map(court_of).collect();
    assert_eq!(int_of(&response, "courts"), courts.len() as i64);
    assert_eq!(int_of(&response, "years"), 7, "seven documents hold a year");
    assert_eq!(int_of(&response, "prices"), 7);
    assert_eq!(int_of(&response, "zeros"), 1, "-0.0 and 0.0 are one value");
    assert_eq!(int_of(&response, "nans"), 1, "every NaN is one value");
    assert_eq!(int_of(&response, "late"), 2, "true and false both occur");
    assert_eq!(int_of(&response, "n"), 7);
    let present = |name: &str| {
        response
            .results
            .iter()
            .find(|r| r.name == name)
            .unwrap()
            .present
    };
    assert_eq!(present("courts"), 7);
    assert_eq!(
        present("zeros"),
        6,
        "the row without a price or a year is absent"
    );
    // Bitwise determinism: the same request answers the same bytes.
    let again = run(
        &coordinator,
        "",
        vec![card("courts", "court", 0), card("years", "year", 0)],
    )
    .await
    .unwrap();
    assert_eq!(again.results[0], response.results[0]);
    assert_eq!(again.results[1], response.results[1]);
    // The filter scopes the distinct set: late years sit in one court.
    let late = run(
        &coordinator,
        "year >= 1994",
        vec![card("courts", "court", 0), card("years", "year", 0)],
    )
    .await
    .unwrap();
    assert_eq!(late.matched, 3);
    assert_eq!(int_of(&late, "courts"), 1);
    assert_eq!(int_of(&late, "years"), 3);
    // An empty selection has no distinct values, and says so with a
    // zero, like COUNT.
    let none = run(
        &coordinator,
        "year > 3000",
        vec![card("courts", "court", 0)],
    )
    .await
    .unwrap();
    assert_eq!(int_of(&none, "courts"), 0);
    for h in handles {
        h.abort();
    }
}

/// Cardinality inside a group-by, and the caps: a shard's own distinct
/// count and the fleet-wide union both refuse by name; the cap on any
/// other op refuses at compile time.
#[tokio::test]
async fn cardinality_groups_and_caps_loudly() {
    let (coordinator, handles) = start_cluster().await;
    let response = coordinator
        .aggregate(Request::new(AggregateRequest {
            aggregations: vec![card("years", "year", 0)],
            group_by: "court".into(),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let mut expected: std::collections::BTreeMap<&str, std::collections::BTreeSet<i64>> =
        Default::default();
    for id in 0..N_DOCS {
        if let (Some(court), Some(year)) = (court_of(id), year_of(id)) {
            expected.entry(court).or_default().insert(year);
        }
    }
    let got: Vec<(&str, i64)> = response
        .groups
        .iter()
        .map(|g| (g.value.as_str(), int_of_group(g, "years")))
        .collect();
    let want: Vec<(&str, i64)> = expected
        .iter()
        .map(|(court, years)| (*court, years.len() as i64))
        .collect();
    assert_eq!(got, want);
    assert_eq!(int_of(&response, "years"), 7, "the total is the union");

    // Shard 0 alone holds four years: a cap of 2 refuses on the shard.
    let status = run(&coordinator, "", vec![card("years", "year", 2)])
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition, "{status}");
    assert!(
        status.message().contains("\"years\"") && status.message().contains("on one shard"),
        "{status}"
    );
    // Each shard holds at most four, the union seven: a cap of 4
    // refuses at the merge.
    let status = run(&coordinator, "", vec![card("years", "year", 4)])
        .await
        .unwrap_err();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition, "{status}");
    assert!(
        status.message().contains("\"years\"") && status.message().contains("across the fleet"),
        "{status}"
    );
    // A cap of 7 admits the seven.
    let ok = run(&coordinator, "", vec![card("years", "year", 7)])
        .await
        .unwrap();
    assert_eq!(int_of(&ok, "years"), 7);
    // The cap belongs to CARDINALITY.
    let status = run(
        &coordinator,
        "",
        vec![Aggregation {
            name: "n".into(),
            expression: "year".into(),
            op: AggregateOp::Count as i32,
            max_distinct: 5,
        }],
    )
    .await
    .unwrap_err();
    assert_eq!(status.code(), tonic::Code::InvalidArgument, "{status}");
    assert!(
        status
            .message()
            .contains("max_distinct applies to CARDINALITY, not count"),
        "{status}"
    );
    for h in handles {
        h.abort();
    }
}

fn int_of_group(group: &pipestream_search::pb::AggregateGroup, name: &str) -> i64 {
    match group.results.iter().find(|r| r.name == name).unwrap().value {
        Some(W::IntValue(v)) => v,
        other => panic!("{name}: expected an int, got {other:?}"),
    }
}

// ---------------------------------------------------------------------
// Calendar date histograms over the timestamp column (epoch micros).
// ---------------------------------------------------------------------

/// Days since 1970-01-01, by walking the calendar: independent of the
/// engine's arithmetic on purpose.
fn test_days(y: i64, m: u32, d: u32) -> i64 {
    fn leap(y: i64) -> bool {
        (y % 4 == 0 && y % 100 != 0) || y % 400 == 0
    }
    let month_days = |y: i64, m: u32| -> i64 {
        match m {
            1 | 3 | 5 | 7 | 8 | 10 | 12 => 31,
            4 | 6 | 9 | 11 => 30,
            _ => {
                if leap(y) {
                    29
                } else {
                    28
                }
            }
        }
    };
    let mut days = 0i64;
    if y >= 1970 {
        for year in 1970..y {
            days += if leap(year) { 366 } else { 365 };
        }
    } else {
        for year in y..1970 {
            days -= if leap(year) { 366 } else { 365 };
        }
    }
    for month in 1..m {
        days += month_days(y, month);
    }
    days + i64::from(d) - 1
}

fn micros_at(y: i64, m: u32, d: u32, hh: i64, mm: i64, ss: i64) -> i64 {
    (test_days(y, m, d) * 86_400 + hh * 3600 + mm * 60 + ss) * 1_000_000
}

/// The instants, spread over both shards: a leap day, a month
/// boundary in a western zone, a year boundary inside one ISO week,
/// and one instant before 1970.
fn filed_instants() -> Vec<i64> {
    vec![
        micros_at(2024, 2, 29, 23, 30, 0),
        micros_at(2024, 3, 1, 2, 30, 0),
        micros_at(2024, 3, 1, 12, 0, 0),
        micros_at(2024, 1, 15, 0, 0, 0),
        micros_at(2023, 12, 31, 23, 59, 59),
        micros_at(2024, 12, 30, 0, 0, 0),
        micros_at(2025, 1, 1, 0, 0, 0),
        micros_at(1969, 12, 31, 23, 0, 0),
    ]
}

async fn start_dated_cluster() -> (
    CoordinatorServiceImpl,
    Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
) {
    let (analysis, mock) = start_mock_analysis().await;
    let mut handles = vec![mock];
    let mut addrs = Vec::new();
    let instants = filed_instants();
    for shard in 0..2usize {
        let (addr, handle) = start_empty_node(NodeConfig {
            slot_offset: (shard * SHARD_DOCS) as u64,
            analysis_addr: Some(analysis.clone()),
            integer_fields: vec!["filed".into()],
            ..Default::default()
        })
        .await;
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        let (tx, rx) = mpsc::channel(16);
        for i in 0..SHARD_DOCS {
            let id = shard * SHARD_DOCS + i;
            let micros = instants[id];
            tx.send(AddDocumentsRequest {
                text: format!("filing {id}"),
                timestamps: vec![pipestream_search::pb::TimestampValue {
                    field: "filed".into(),
                    value: Some(prost_types::Timestamp {
                        seconds: micros.div_euclid(1_000_000),
                        nanos: (micros.rem_euclid(1_000_000) * 1000) as i32,
                    }),
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

fn calendar_hist(
    name: &str,
    unit: pipestream_search::pb::CalendarInterval,
    utc_offset_minutes: i32,
) -> HistogramSpec {
    HistogramSpec {
        name: name.into(),
        expression: "filed".into(),
        interval: 0.0,
        max_buckets: 0,
        calendar: unit as i32,
        utc_offset_minutes,
    }
}

/// Every unit buckets at its calendar boundary, the key is the bucket's
/// start instant, and a zone offset moves the boundary.
#[tokio::test]
async fn calendar_histograms_bucket_at_civil_boundaries() {
    use pipestream_search::pb::CalendarInterval as C;
    let (coordinator, handles) = start_dated_cluster().await;
    let response = coordinator
        .aggregate(Request::new(AggregateRequest {
            histograms: vec![
                calendar_hist("day", C::Day, 0),
                calendar_hist("day_eastern", C::Day, -300),
                calendar_hist("week", C::Week, 0),
                calendar_hist("month", C::Month, 0),
                calendar_hist("quarter", C::Quarter, 0),
                calendar_hist("year", C::Year, 0),
                calendar_hist("hour_india", C::Hour, 330),
                calendar_hist("minute", C::Minute, 0),
            ],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let at = |y, m, d, hh, mm| micros_at(y, m, d, hh, mm, 0);
    let expectations: Vec<(&str, Vec<(i64, u64)>)> = vec![
        (
            "day",
            vec![
                (at(1969, 12, 31, 0, 0), 1),
                (at(2023, 12, 31, 0, 0), 1),
                (at(2024, 1, 15, 0, 0), 1),
                (at(2024, 2, 29, 0, 0), 1),
                (at(2024, 3, 1, 0, 0), 2),
                (at(2024, 12, 30, 0, 0), 1),
                (at(2025, 1, 1, 0, 0), 1),
            ],
        ),
        // UTC-5: local midnight is 05:00Z; 02:30Z on March 1st is
        // still February 29th, a UTC midnight is the evening before,
        // and 23:00Z on 1969-12-31 is the 31st.
        (
            "day_eastern",
            vec![
                (at(1969, 12, 31, 5, 0), 1),
                (at(2023, 12, 31, 5, 0), 1),
                (at(2024, 1, 14, 5, 0), 1),
                (at(2024, 2, 29, 5, 0), 2),
                (at(2024, 3, 1, 5, 0), 1),
                (at(2024, 12, 29, 5, 0), 1),
                (at(2024, 12, 31, 5, 0), 1),
            ],
        ),
        // ISO weeks begin on Monday.
        (
            "week",
            vec![
                (at(1969, 12, 29, 0, 0), 1),
                (at(2023, 12, 25, 0, 0), 1),
                (at(2024, 1, 15, 0, 0), 1),
                (at(2024, 2, 26, 0, 0), 3),
                (at(2024, 12, 30, 0, 0), 2),
            ],
        ),
        (
            "month",
            vec![
                (at(1969, 12, 1, 0, 0), 1),
                (at(2023, 12, 1, 0, 0), 1),
                (at(2024, 1, 1, 0, 0), 1),
                (at(2024, 2, 1, 0, 0), 1),
                (at(2024, 3, 1, 0, 0), 2),
                (at(2024, 12, 1, 0, 0), 1),
                (at(2025, 1, 1, 0, 0), 1),
            ],
        ),
        (
            "quarter",
            vec![
                (at(1969, 10, 1, 0, 0), 1),
                (at(2023, 10, 1, 0, 0), 1),
                (at(2024, 1, 1, 0, 0), 4),
                (at(2024, 10, 1, 0, 0), 1),
                (at(2025, 1, 1, 0, 0), 1),
            ],
        ),
        (
            "year",
            vec![
                (at(1969, 1, 1, 0, 0), 1),
                (at(2023, 1, 1, 0, 0), 1),
                (at(2024, 1, 1, 0, 0), 5),
                (at(2025, 1, 1, 0, 0), 1),
            ],
        ),
        // UTC+5:30: the local hour begins on the UTC half hour.
        (
            "hour_india",
            vec![
                (at(1969, 12, 31, 22, 30), 1),
                (at(2023, 12, 31, 23, 30), 1),
                (at(2024, 1, 14, 23, 30), 1),
                (at(2024, 2, 29, 23, 30), 1),
                (at(2024, 3, 1, 2, 30), 1),
                (at(2024, 3, 1, 11, 30), 1),
                (at(2024, 12, 29, 23, 30), 1),
                (at(2024, 12, 31, 23, 30), 1),
            ],
        ),
        (
            "minute",
            vec![
                (at(1969, 12, 31, 23, 0), 1),
                (at(2023, 12, 31, 23, 59), 1),
                (at(2024, 1, 15, 0, 0), 1),
                (at(2024, 2, 29, 23, 30), 1),
                (at(2024, 3, 1, 2, 30), 1),
                (at(2024, 3, 1, 12, 0), 1),
                (at(2024, 12, 30, 0, 0), 1),
                (at(2025, 1, 1, 0, 0), 1),
            ],
        ),
    ];
    assert_eq!(response.histograms.len(), expectations.len());
    for (result, (name, want)) in response.histograms.iter().zip(&expectations) {
        assert_eq!(&result.name, name);
        let got: Vec<(i64, u64)> = result
            .buckets
            .iter()
            .map(|b| (b.lower_int, b.count))
            .collect();
        assert_eq!(&got, want, "{name}");
        for b in &result.buckets {
            assert_eq!(
                b.lower, b.lower_int as f64,
                "{name}: lower mirrors lower_int"
            );
        }
        assert_eq!(result.present, N_DOCS as u64, "{name}");
        assert_eq!(result.unbucketable, 0, "{name}");
    }
    // The same request twice: the same bytes.
    let again = coordinator
        .aggregate(Request::new(AggregateRequest {
            histograms: vec![calendar_hist("week", C::Week, 0)],
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(again.histograms[0].buckets, response.histograms[2].buckets);
    for h in handles {
        h.abort();
    }
}

/// The calendar shape's refusals name the histogram and the fix.
#[tokio::test]
async fn calendar_histogram_refusals_name_the_spec() {
    use pipestream_search::pb::CalendarInterval as C;
    let (coordinator, handles) = start_dated_cluster().await;
    let run_hist = |spec: HistogramSpec| {
        coordinator.aggregate(Request::new(AggregateRequest {
            histograms: vec![spec],
            ..Default::default()
        }))
    };
    let cases: Vec<(&str, HistogramSpec, &str)> = vec![
        (
            "interval with calendar",
            HistogramSpec {
                interval: 2.0,
                ..calendar_hist("h", C::Day, 0)
            },
            "fixed interval must be zero",
        ),
        (
            "offset out of range",
            calendar_hist("h", C::Day, 2000),
            "outside +-1080",
        ),
        (
            "offset without calendar",
            HistogramSpec {
                name: "h".into(),
                expression: "double(filed)".into(),
                interval: 1.0,
                max_buckets: 0,
                calendar: 0,
                utc_offset_minutes: 60,
            },
            "utc_offset_minutes applies to a calendar histogram",
        ),
        (
            "unknown unit",
            HistogramSpec {
                calendar: 99,
                ..calendar_hist("h", C::Day, 0)
            },
            "unknown calendar interval 99",
        ),
        (
            "double expression",
            HistogramSpec {
                expression: "double(filed)".into(),
                ..calendar_hist("h", C::Day, 0)
            },
            "epoch micros",
        ),
    ];
    for (label, spec, needle) in cases {
        let status = run_hist(spec)
            .await
            .err()
            .unwrap_or_else(|| panic!("{label}: refused"));
        assert_eq!(
            status.code(),
            tonic::Code::InvalidArgument,
            "{label}: {status}"
        );
        assert!(
            status.message().contains("\"h\"") && status.message().contains(needle),
            "{label}: {} lacks {needle:?}",
            status.message()
        );
    }
    // The bucket cap applies to calendar buckets as to fixed ones.
    let status = run_hist(HistogramSpec {
        max_buckets: 2,
        ..calendar_hist("h", C::Day, 0)
    })
    .await
    .unwrap_err();
    assert_eq!(status.code(), tonic::Code::FailedPrecondition, "{status}");
    assert!(
        status.message().contains("\"h\" exceeds 2 buckets"),
        "{status}"
    );
    for h in handles {
        h.abort();
    }
}
