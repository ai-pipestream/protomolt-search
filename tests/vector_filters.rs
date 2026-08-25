//! Vector-leg filter acceptance tests (`docs/vector-filters.md`).
//!
//! The lexical leg has filtered since the CEL increment; the vector leg
//! could not, so the hybrid route refused filters outright rather than
//! filter one half and misdescribe the result set. These tests hold the
//! vector leg to the same contract the lexical leg already signs:
//!
//! 1. Exactness. A filtered vector search returns the top-k OF THE
//!    SURVIVORS, which is the unfiltered search narrowed afterwards —
//!    on the bidi route, the streaming route, and collapse mode.
//! 2. Both legs, one truth. Every hybrid fusion mode filters both legs
//!    from the same resolved predicate, so no fused hit can be a
//!    document the filter removed.
//! 3. Refusal beats degradation. A column no shard resolves refuses by
//!    name instead of returning the empty result set it would produce.

mod common;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use turbovec_search::coordinator::CoordinatorServiceImpl;
use turbovec_search::node::NodeConfig;
use turbovec_search::pb::node_service_client::NodeServiceClient;
use turbovec_search::pb::search_service_server::SearchService;
use turbovec_search::pb::{
    AddDocumentsRequest, AddVectorsRequest, FacetValue, FusionMode, HybridSearchRequest,
    SearchRequest, SetCalibrationRequest,
};

use common::{fit_calibration, mock::start_mock_analysis, start_empty_node, unit_vectors};

const DIM: usize = 64;
const SHARD_DOCS: usize = 8;
const N_SHARDS: usize = 2;
const N_DOCS: usize = SHARD_DOCS * N_SHARDS;

/// Global doc id -> court, by construction: even ids are "scotus",
/// odd ids "ca5". Both shards hold both values, so no result can be
/// explained by a shard boundary.
fn court_of(id: usize) -> &'static str {
    if id % 2 == 0 {
        "scotus"
    } else {
        "ca5"
    }
}

async fn set_calibration(addr: &str, shift: &[f32], scale: &[f32]) {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    client
        .set_calibration(SetCalibrationRequest {
            dim: DIM as u32,
            bit_width: 4,
            shift: shift.to_vec(),
            scale: scale.to_vec(),
        })
        .await
        .unwrap();
}

/// Ingest documents then vectors, in that order, so doc ids and vector
/// slots align 1:1 in the shared positional id space — the invariant the
/// allowlist depends on (`allow[slot]` is read with the slot as a doc id).
async fn seed_shard(
    analysis: &str,
    slot_offset: u64,
    ids: std::ops::Range<usize>,
    vectors: Vec<f32>,
    shift: &[f32],
    scale: &[f32],
) -> (
    String,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
) {
    let (addr, handle) = start_empty_node(NodeConfig {
        slot_offset,
        analysis_addr: Some(analysis.to_string()),
        facet_fields: vec!["court".to_string()],
        ..Default::default()
    })
    .await;
    set_calibration(&addr, shift, scale).await;

    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    let (tx, rx) = mpsc::channel(8);
    let feeder = tokio::spawn(async move {
        for id in ids {
            tx.send(AddDocumentsRequest {
                text: format!("opinion {id} about search"),
                facets: vec![FacetValue {
                    field: "court".into(),
                    value: court_of(id).into(),
                }],
                ..Default::default()
            })
            .await
            .unwrap();
        }
    });
    client.add_documents(ReceiverStream::new(rx)).await.unwrap();
    feeder.await.unwrap();

    let (tx, rx) = mpsc::channel(4);
    tx.send(AddVectorsRequest {
        vectors,
        dim: DIM as u32,
    })
    .await
    .unwrap();
    drop(tx);
    client.add_vectors(ReceiverStream::new(rx)).await.unwrap();
    (addr, handle)
}

struct Cluster {
    addrs: Vec<String>,
    corpus: Vec<f32>,
    analysis: String,
    _handles: Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
    _mock: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

async fn cluster() -> Cluster {
    let (analysis, mock) = start_mock_analysis().await;
    let corpus = unit_vectors(N_DOCS, DIM, 0x0F11_7E30);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for shard in 0..N_SHARDS {
        let start = shard * SHARD_DOCS;
        let vecs = corpus[start * DIM..(start + SHARD_DOCS) * DIM].to_vec();
        let (addr, handle) = seed_shard(
            &analysis,
            start as u64,
            start..start + SHARD_DOCS,
            vecs,
            &shift,
            &scale,
        )
        .await;
        addrs.push(addr);
        handles.push(handle);
    }
    Cluster {
        addrs,
        corpus,
        analysis,
        _handles: handles,
        _mock: mock,
    }
}

fn coordinator(c: &Cluster, streaming: bool) -> CoordinatorServiceImpl {
    CoordinatorServiceImpl::new(c.addrs.clone())
        .with_bm25(Some(c.analysis.clone()), Default::default())
        .with_stream_search(streaming)
}

async fn vector_search(
    coord: &CoordinatorServiceImpl,
    query: &[f32],
    k: u32,
    filter: &str,
    collapse: bool,
) -> Result<Vec<u64>, tonic::Status> {
    coord
        .search(Request::new(SearchRequest {
            k,
            vector: query.to_vec(),
            filter: filter.into(),
            collapse_parents: collapse,
            ..Default::default()
        }))
        .await
        .map(|r| r.into_inner().hits.iter().map(|h| h.vector_id).collect())
}

/// The exactness oracle for the whole route: a filtered vector search
/// must equal the unfiltered search narrowed afterwards. Both the bidi
/// (`SearchShard`) and streaming (`StreamSearch`) coordinators are
/// checked, because the allowlist reaches the kernel by a different
/// path in each.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_filtered_vector_search_is_the_unfiltered_one_narrowed() {
    let c = cluster().await;
    let query = c.corpus[..DIM].to_vec();

    for streaming in [false, true] {
        let coord = coordinator(&c, streaming);
        // Every document, unfiltered, in score order.
        let all = vector_search(&coord, &query, N_DOCS as u32, "", false)
            .await
            .unwrap();
        assert_eq!(all.len(), N_DOCS, "streaming={streaming}");

        for court in ["scotus", "ca5"] {
            let expected: Vec<u64> = all
                .iter()
                .copied()
                .filter(|&id| court_of(id as usize) == court)
                .collect();
            let filtered = vector_search(
                &coord,
                &query,
                N_DOCS as u32,
                &format!(r#"court == "{court}""#),
                false,
            )
            .await
            .unwrap();
            assert_eq!(
                filtered, expected,
                "streaming={streaming}, court={court}: a filter only removes documents"
            );

            // And at a k that truncates: the top-k of the survivors, not
            // the survivors of the top-k.
            let k = 3;
            let short = vector_search(
                &coord,
                &query,
                k,
                &format!(r#"court == "{court}""#),
                false,
            )
            .await
            .unwrap();
            assert_eq!(short, expected[..k as usize], "streaming={streaming}");
        }
    }
}

/// A filter that no document satisfies returns nothing. The engine must
/// not quietly widen to an unfiltered scan because the result looked
/// empty.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_filter_matching_nothing_returns_nothing() {
    let c = cluster().await;
    let query = c.corpus[..DIM].to_vec();
    for streaming in [false, true] {
        let coord = coordinator(&c, streaming);
        let hits = vector_search(&coord, &query, 10, r#"court == "nowhere""#, false)
            .await
            .unwrap();
        assert!(hits.is_empty(), "streaming={streaming}");
    }
}

/// The typo rule, on the vector route: a column NO shard resolves is a
/// spelling mistake, and filtering on it would read as an honest empty
/// result set. It refuses by name instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_column_no_shard_knows_refuses_on_the_vector_route() {
    let c = cluster().await;
    let query = c.corpus[..DIM].to_vec();
    for streaming in [false, true] {
        let coord = coordinator(&c, streaming);
        let err = vector_search(&coord, &query, 10, r#"kourt == "scotus""#, false)
            .await
            .expect_err("an unknown column must refuse");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
        assert!(
            err.message().contains("kourt"),
            "the refusal must name the column, got: {}",
            err.message()
        );
    }
}

/// Collapse-by-parent under a filter collapses the SURVIVORS: with one
/// chunk per parent here, that is simply the filtered ranking.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn collapse_mode_filters_too() {
    let c = cluster().await;
    let query = c.corpus[..DIM].to_vec();
    for streaming in [false, true] {
        let coord = coordinator(&c, streaming);
        let hits = vector_search(&coord, &query, N_DOCS as u32, r#"court == "scotus""#, true)
            .await
            .unwrap();
        assert!(!hits.is_empty(), "streaming={streaming}");
        for id in &hits {
            assert_eq!(
                court_of(*id as usize),
                "scotus",
                "streaming={streaming}: collapse must not admit a filtered-out chunk"
            );
        }
    }
}

/// Every fusion mode filters BOTH legs. A fused hit that failed the
/// filter would mean one leg ran unfiltered — the exact failure the
/// hybrid route used to refuse filters to avoid.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_fusion_mode_filters_both_legs() {
    let c = cluster().await;
    let query = c.corpus[..DIM].to_vec();
    let coord = coordinator(&c, false);
    for mode in [
        FusionMode::GlobalRank,
        FusionMode::TwoLevel,
        FusionMode::ScoreBlend,
        FusionMode::Decomposed,
        FusionMode::Cascade,
    ] {
        let response = coord
            .hybrid_search(Request::new(HybridSearchRequest {
                text: "opinion".into(),
                vector: query.clone(),
                k: N_DOCS as u32,
                filter: r#"court == "scotus""#.into(),
                legs: Some(turbovec_search::pb::HybridLegOptions {
                    fusion_mode: mode as i32,
                    leg_k: 32,
                    ..Default::default()
                }),
                ..Default::default()
            }))
            .await
            .unwrap_or_else(|e| panic!("{mode:?} refused a filter: {e}"))
            .into_inner();
        let ids: Vec<u64> = if response.hits.is_empty() {
            response.cascade_hits.iter().map(|h| h.doc_id).collect()
        } else {
            response.hits.iter().map(|h| h.doc_id).collect()
        };
        assert!(!ids.is_empty(), "{mode:?} returned nothing at all");
        for id in &ids {
            assert_eq!(
                court_of(*id as usize),
                "scotus",
                "{mode:?} admitted a document the filter removed"
            );
        }
    }
}
