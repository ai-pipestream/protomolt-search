//! Geography-wiring acceptance tests (`docs/geography-columns.md`):
//! `GeographySpec` on ingest asks the sidecar to geocode location
//! mentions in the same pass that produces terms, the node reduces
//! the layer to one point / one country / one confidence, and the
//! values land as ORDINARY geo / facet / f64 columns — spatial search
//! over a corpus whose source data carries no coordinates anywhere.
//!
//! The mock's gazetteer is deterministic (paris → FR 0.9, berlin →
//! DE 0.9, springfield → US 0.4; region votes are evidence shares),
//! so expected values are computed, not approximated.

mod common;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use turbovec_search::coordinator::CoordinatorServiceImpl;
use turbovec_search::node::NodeConfig;
use turbovec_search::pb::node_service_client::NodeServiceClient;
use turbovec_search::pb::search_service_server::SearchService;
use turbovec_search::pb::{
    AddDocumentsRequest, Bm25SearchRequest, Bm25SearchResponse, GeoBbox, GeoFilter,
    GeographySpec,
};

use common::mock::start_mock_analysis;
use common::start_empty_node;
use turbovec_search::harness::mock_analysis::start_mock_analysis_without_ner;

/// The corpus. Every text contains "case"; place mentions per the
/// mock's gazetteer. Expected reductions:
///
/// | id | mentions               | point        | country | confidence |
/// |----|------------------------|--------------|---------|------------|
/// | 0  | paris                  | Paris        | FR      | 0.9        |
/// | 1  | berlin berlin paris    | Berlin (1st) | DE      | 0.9        |
/// | 2  | springfield            | Springfield  | US      | 0.4        |
/// | 3  | (no places)            | absent       | absent  | absent     |
const TEXTS: [&str; 4] = [
    "case argued in paris today",
    "case moved from berlin then berlin then paris",
    "case near springfield somewhere",
    "case with no places at all",
];

fn full_spec() -> GeographySpec {
    GeographySpec {
        point_column: "place".into(),
        country_column: "country".into(),
        confidence_column: "geo_confidence".into(),
    }
}

fn geo_doc(text: &str, spec: Option<GeographySpec>) -> AddDocumentsRequest {
    AddDocumentsRequest {
        text: text.to_string(),
        geography: spec,
        ..Default::default()
    }
}

async fn add_docs(
    addr: &str,
    docs: Vec<AddDocumentsRequest>,
) -> Result<(), tonic::Status> {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(64);
    for doc in docs {
        tx.send(doc).await.unwrap();
    }
    drop(tx);
    client
        .add_documents(ReceiverStream::new(rx))
        .await
        .map(|_| ())
}

async fn start_geography_cluster() -> (
    CoordinatorServiceImpl,
    Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
) {
    let (analysis, mock) = start_mock_analysis().await;
    let (addr, node) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis.clone()),
        geo_fields: vec!["place".into()],
        facet_fields: vec!["country".into()],
        numeric_fields: vec!["geo_confidence".into()],
        ..Default::default()
    })
    .await;
    add_docs(
        &addr,
        TEXTS
            .iter()
            .map(|t| geo_doc(t, Some(full_spec())))
            .collect(),
    )
    .await
    .unwrap();
    let coordinator =
        CoordinatorServiceImpl::new(vec![addr]).with_bm25(Some(analysis), Default::default());
    (coordinator, vec![node, mock])
}

fn bbox(column: &str, min_lat: f64, max_lat: f64, min_lon: f64, max_lon: f64) -> GeoFilter {
    GeoFilter {
        column: column.to_string(),
        region: Some(turbovec_search::pb::geo_filter::Region::Bbox(GeoBbox {
            min_lat,
            max_lat,
            min_lon,
            max_lon,
        })),
    }
}

async fn search_ids(
    coordinator: &CoordinatorServiceImpl,
    filter: &str,
    geo: Vec<GeoFilter>,
) -> Vec<u64> {
    let resp: Bm25SearchResponse = coordinator
        .bm25_search(Request::new(Bm25SearchRequest {
            text: "case".into(),
            k: 10,
            filter: filter.into(),
            geo_filters: geo,
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let mut ids: Vec<u64> = resp.hits.iter().map(|h| h.doc_id).collect();
    ids.sort_unstable();
    ids
}

/// The reduced values are real column values: geo filters over the
/// materialized point, CEL over the country facet and the confidence
/// column, all selecting exactly what the gazetteer predicts — and
/// the place-less document is ABSENT everywhere, never "at (0,0)".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn geocoded_mentions_become_spatial_search() {
    let (coordinator, _handles) = start_geography_cluster().await;

    assert_eq!(
        search_ids(&coordinator, "", Vec::new()).await,
        vec![0, 1, 2, 3],
        "unfiltered baseline"
    );
    // Around Paris: only d0 — d1's BEST point is Berlin (first of the
    // tied 0.9s in text order), so its Paris mention does not place it.
    assert_eq!(
        search_ids(&coordinator, "", vec![bbox("place", 48.0, 49.5, 1.5, 3.0)]).await,
        vec![0]
    );
    // Around Berlin.
    assert_eq!(
        search_ids(&coordinator, "", vec![bbox("place", 52.0, 53.0, 13.0, 14.0)]).await,
        vec![1]
    );
    // The whole world: the place-less d3 is nowhere, not at (0,0).
    assert_eq!(
        search_ids(&coordinator, "", vec![bbox("place", -90.0, 90.0, -180.0, 180.0)]).await,
        vec![0, 1, 2],
        "absence is absence, not the Gulf of Guinea"
    );
    // The aggregate region vote: d1's evidence is 2/3 DE, so its
    // country is DE even though it mentions Paris too.
    assert_eq!(
        search_ids(&coordinator, r#"country == "DE""#, Vec::new()).await,
        vec![1]
    );
    assert_eq!(
        search_ids(&coordinator, r#"country == "FR""#, Vec::new()).await,
        vec![0]
    );
    // Confidence thresholds, including the Kleene rule on d3: a
    // tautology over an absent value is UNKNOWN, not a match.
    assert_eq!(
        search_ids(&coordinator, "geo_confidence >= 0.7", Vec::new()).await,
        vec![0, 1]
    );
    assert_eq!(
        search_ids(&coordinator, "geo_confidence < 0.5", Vec::new()).await,
        vec![2],
        "the ambiguous gazetteer hit is selectable as low-confidence"
    );
    assert_eq!(
        search_ids(&coordinator, "geo_confidence >= 0.0", Vec::new()).await,
        vec![0, 1, 2],
        "no confidence value at all on the place-less document"
    );
}

/// A sidecar with no NER model cannot serve the geocoding layer, and
/// its own contract on that state (empty layers plus a free-form
/// warning) is indistinguishable from "no locations found" — so the
/// session preflights the capability and REFUSES, naming the cause,
/// instead of silently ingesting the corpus as place-less.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sidecar_without_ner_refuses_geography_loudly() {
    let (analysis, _mock) = start_mock_analysis_without_ner().await;
    let (addr, _node) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis.clone()),
        geo_fields: vec!["place".into()],
        facet_fields: vec!["country".into()],
        numeric_fields: vec!["geo_confidence".into()],
        ..Default::default()
    })
    .await;

    let err = add_docs(&addr, vec![geo_doc("case in paris", Some(full_spec()))])
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(
        err.message().contains("ner_available") && err.message().contains("NER model"),
        "refusal names the capability and the fix: {}",
        err.message()
    );

    // The same sidecar serves everything else: a blank spec asks for
    // nothing and ingests fine.
    add_docs(
        &addr,
        vec![geo_doc("case in paris", Some(GeographySpec::default()))],
    )
    .await
    .unwrap();
}

/// The materialized values take the ordinary column path, so the
/// ordinary refusal covers them: a geo column the shard does not
/// declare refuses by name.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_undeclared_geo_column_refuses_by_name() {
    let (analysis, _mock) = start_mock_analysis().await;
    let (addr, _node) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis),
        ..Default::default()
    })
    .await;
    let err = add_docs(&addr, vec![geo_doc("case in paris", Some(full_spec()))])
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("place") || err.message().contains("country"),
        "refusal names a column: {}",
        err.message()
    );
}
