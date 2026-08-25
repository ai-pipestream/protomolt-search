//! Column aggregations over the filtered match set (docs/facets.md):
//! stats (count / min / max / sum / mean) on numeric and integer
//! columns, exact distinct-value cardinality on facet columns — both
//! additive across shards over the same one bitmap the facet kinds
//! share, with the usual typo refusals.

mod common;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use turbovec_search::coordinator::CoordinatorServiceImpl;
use turbovec_search::node::NodeConfig;
use turbovec_search::pb::node_service_client::NodeServiceClient;
use turbovec_search::pb::search_service_server::SearchService;
use turbovec_search::pb::{
    AddDocumentsRequest, Bm25SearchRequest, Bm25SearchResponse, FacetValue, IntegerValue,
    NumericValue, QueryField,
};

use common::{mock::start_mock_analysis, start_empty_node};

struct Doc {
    text: &'static str,
    court: Option<&'static str>,
    score: Option<f64>,
    year: Option<i64>,
}

/// Shards 0 and 1 declare every column; shard 2 declares none (the
/// heterogeneous fleet). d4 does not match the probe term.
const SHARDS: [&[Doc]; 3] = [
    &[
        Doc { text: "rust alpha", court: Some("scotus"), score: Some(1.5), year: Some(1990) },
        Doc { text: "rust beta", court: Some("ca5"), score: Some(2.5), year: None },
        Doc { text: "rust", court: None, score: None, year: None },
    ],
    &[
        Doc { text: "rust gamma", court: Some("scotus"), score: Some(0.5), year: Some(2000) },
        Doc { text: "other doc", court: Some("ca9"), score: Some(9.9), year: Some(1900) },
    ],
    &[Doc { text: "rust delta", court: None, score: None, year: None }],
];

async fn start() -> (
    CoordinatorServiceImpl,
    Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
) {
    let (analysis, mock) = start_mock_analysis().await;
    let mut addrs = Vec::new();
    let mut handles = vec![mock];
    for (i, docs) in SHARDS.iter().enumerate() {
        let declared = i < 2;
        let cols = |name: &str| {
            if declared {
                vec![name.to_string()]
            } else {
                Vec::new()
            }
        };
        let (addr, handle) = start_empty_node(NodeConfig {
            slot_offset: [0u64, 3, 5][i],
            analysis_addr: Some(analysis.clone()),
            facet_fields: cols("court"),
            numeric_fields: cols("score"),
            integer_fields: cols("year"),
            ..Default::default()
        })
        .await;
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        let (tx, rx) = mpsc::channel(8);
        for doc in *docs {
            tx.send(AddDocumentsRequest {
                text: doc.text.to_string(),
                facets: doc
                    .court
                    .iter()
                    .map(|c| FacetValue {
                        field: "court".into(),
                        value: c.to_string(),
                    })
                    .collect(),
                numerics: doc
                    .score
                    .iter()
                    .map(|v| NumericValue {
                        field: "score".into(),
                        value: *v,
                    })
                    .collect(),
                integers: doc
                    .year
                    .iter()
                    .map(|v| IntegerValue {
                        field: "year".into(),
                        value: *v,
                    })
                    .collect(),
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

async fn search(
    coordinator: &CoordinatorServiceImpl,
    req: Bm25SearchRequest,
) -> Result<Bm25SearchResponse, tonic::Status> {
    coordinator
        .bm25_search(Request::new(req))
        .await
        .map(|r| r.into_inner())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stats_and_cardinality_aggregate_the_match_set() {
    let (coordinator, _handles) = start().await;

    let resp = search(
        &coordinator,
        Bm25SearchRequest {
            text: "rust".into(),
            k: 10,
            stats_fields: vec!["score".into(), "year".into()],
            cardinality_fields: vec!["court".into()],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    // Match set: d0..d3 and d5 ("other doc" does not match).
    assert_eq!(resp.hits.len(), 5);
    let score = &resp.stats[0];
    assert_eq!(
        (score.field.as_str(), score.known, score.count),
        ("score", true, 3),
        "three matched docs hold a score; absence is not zero"
    );
    assert_eq!((score.min, score.max, score.sum), (0.5, 2.5, 4.5));
    assert_eq!(score.mean, 1.5);
    let year = &resp.stats[1];
    assert_eq!(year.count, 2, "d4's 1900 is outside the match set");
    assert_eq!((year.min, year.max, year.mean), (1990.0, 2000.0, 1995.0));
    assert_eq!(resp.cardinality.len(), 1);
    assert_eq!(
        resp.cardinality[0].cardinality, 2,
        "scotus (both shards, counted once) and ca5; ca9 is unmatched"
    );

    // A filter narrows the aggregation exactly as it narrows facets.
    let resp = search(
        &coordinator,
        Bm25SearchRequest {
            text: "rust".into(),
            k: 10,
            filter: "year >= 1995".into(),
            stats_fields: vec!["score".into()],
            cardinality_fields: vec!["court".into()],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(resp.hits.len(), 1, "only d3 has year >= 1995");
    let score = &resp.stats[0];
    assert_eq!((score.count, score.min, score.max, score.sum), (1, 0.5, 0.5, 0.5));
    assert_eq!(resp.cardinality[0].cardinality, 1, "only scotus remains");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn aggregation_typos_and_the_fused_route_refuse_by_name() {
    let (coordinator, _handles) = start().await;

    let err = search(
        &coordinator,
        Bm25SearchRequest {
            text: "rust".into(),
            k: 5,
            stats_fields: vec!["scoer".into()],
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("scoer") && err.message().contains("--numeric-fields"),
        "{}",
        err.message()
    );

    let err = search(
        &coordinator,
        Bm25SearchRequest {
            text: "rust".into(),
            k: 5,
            cardinality_fields: vec!["cuort".into()],
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert!(
        err.message().contains("cuort") && err.message().contains("--facet-fields"),
        "{}",
        err.message()
    );

    let err = search(
        &coordinator,
        Bm25SearchRequest {
            text: "rust".into(),
            k: 5,
            fields: vec![QueryField {
                field: "body".into(),
                weight: 1.0,
                ..Default::default()
            }],
            stats_fields: vec!["score".into()],
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert!(
        err.message().contains("fused multi-field"),
        "{}",
        err.message()
    );
}
