//! Quality-column acceptance tests (`docs/quality-columns.md`): the
//! `QualitySpec` on ingest asks the sidecar for its noise and artifact
//! layers, the node folds them into per-document scalars, and the
//! scalars land as ORDINARY f64 / i64 columns — so filters, facets,
//! and score chains over them need no machinery of their own, and
//! every refusal is the ordinary column refusal.
//!
//! The mock sidecar's quality rules are deterministic (a token made
//! entirely of '#' is a noise finding scored len/10 capped at 1.0;
//! every U+FFFD is a "replacement" artifact), so expected column
//! values are computed here, not approximated.

mod common;

use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    AddDocumentsRequest, Bm25SearchRequest, Bm25SearchResponse, NumericValue, QualitySpec, ScoreOp,
    ScoreStage,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

use common::{mock::start_mock_analysis, start_empty_node};

/// The controlled corpus. Every text contains "rust" so the probe
/// query matches all of it; the noise and artifact content varies.
/// Expected measurements, by the mock's documented rules:
///
/// | id | text                  | noise | noise_chars | artifacts |
/// |----|-----------------------|-------|-------------|-----------|
/// | 0  | clean prose           | 0.0   | 0           | 0         |
/// | 1  | one '####'            | 0.4   | 4           | 0         |
/// | 2  | '###' + '##########'  | 1.0   | 13          | 0         |
/// | 3  | two U+FFFD chars      | 0.0   | 0           | 2         |
const TEXTS: [&str; 4] = [
    "clean rust prose",
    "rust #### garble",
    "rust ### ##########",
    "rust \u{FFFD}\u{FFFD} mojibake",
];

fn full_spec() -> QualitySpec {
    QualitySpec {
        noise_column: "noise".into(),
        noise_chars_column: "noise_chars".into(),
        artifact_column: "artifacts".into(),
    }
}

fn quality_doc(text: &str, spec: Option<QualitySpec>) -> AddDocumentsRequest {
    AddDocumentsRequest {
        text: text.to_string(),
        quality: spec,
        ..Default::default()
    }
}

async fn add_docs(addr: &str, docs: Vec<AddDocumentsRequest>) -> Result<(), tonic::Status> {
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

/// One node declaring the three quality columns, corpus ingested with
/// the full spec, coordinator on top.
async fn start_quality_cluster() -> (
    CoordinatorServiceImpl,
    Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
) {
    let (analysis, mock) = start_mock_analysis().await;
    let (addr, node) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis.clone()),
        numeric_fields: vec!["noise".into()],
        integer_fields: vec!["noise_chars".into(), "artifacts".into()],
        ..Default::default()
    })
    .await;
    add_docs(
        &addr,
        TEXTS
            .iter()
            .map(|t| quality_doc(t, Some(full_spec())))
            .collect(),
    )
    .await
    .unwrap();
    let coordinator =
        CoordinatorServiceImpl::new(vec![addr]).with_bm25(Some(analysis), Default::default());
    (coordinator, vec![node, mock])
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

async fn filtered_ids(coordinator: &CoordinatorServiceImpl, filter: &str) -> Vec<u64> {
    let resp = search(
        coordinator,
        Bm25SearchRequest {
            text: "rust".into(),
            k: 10,
            filter: filter.into(),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let mut ids: Vec<u64> = resp.hits.iter().map(|h| h.doc_id).collect();
    ids.sort_unstable();
    ids
}

/// The derived values are real column values: CEL over the three
/// columns selects exactly the documents the mock's rules predict,
/// including `== 0.0` distinguishing measured-clean from noisy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn derived_columns_hold_the_predicted_measurements() {
    let (coordinator, _handles) = start_quality_cluster().await;

    for (filter, want, why) in [
        ("", vec![0, 1, 2, 3], "unfiltered baseline"),
        ("noise >= 0.4", vec![1, 2], "worst-finding score"),
        (
            "noise == 1.0",
            vec![2],
            "the cap: 10 '#'s score exactly 1.0",
        ),
        ("noise_chars > 4", vec![2], "3 + 10 damaged chars"),
        ("noise_chars == 4", vec![1], "one four-char finding"),
        ("artifacts == 2", vec![3], "two U+FFFD replacements"),
        (
            "noise == 0.0",
            vec![0, 3],
            "measured-clean is a value, not absence: artifacts-only \
             d3 still measures noise 0",
        ),
        (
            "artifacts == 0 && noise == 0.0",
            vec![0],
            "the clean document is selectable as clean",
        ),
    ] {
        assert_eq!(
            filtered_ids(&coordinator, filter).await,
            want,
            "filter {filter:?}: {why}"
        );
    }
}

/// A MULT_EXP_DECAY stage over the noise column is the quality decay:
/// every document's score is its base score times exp(-noise), bitwise,
/// with no new score op involved.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn exp_decay_over_noise_is_the_quality_decay() {
    let (coordinator, _handles) = start_quality_cluster().await;

    let base = search(
        &coordinator,
        Bm25SearchRequest {
            text: "rust".into(),
            k: 10,
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let decayed = search(
        &coordinator,
        Bm25SearchRequest {
            text: "rust".into(),
            k: 10,
            score_stages: vec![ScoreStage {
                op: ScoreOp::MultExpDecay as i32,
                column: "noise".into(),
                origin: 0.0,
                scale: 1.0,
                ..Default::default()
            }],
            ..Default::default()
        },
    )
    .await
    .unwrap();

    let noise_of = [0.0f64, 0.4, 1.0, 0.0];
    for hit in &decayed.hits {
        let b = base
            .hits
            .iter()
            .find(|h| h.doc_id == hit.doc_id)
            .expect("a stage only rescales; the match set is identical");
        let want = (f64::from(b.score) * (-noise_of[hit.doc_id as usize]).exp()) as f32;
        assert_eq!(
            hit.score.to_bits(),
            want.to_bits(),
            "doc {}: decayed score must be base * exp(-noise), bitwise",
            hit.doc_id
        );
    }
}

/// An all-blank spec asks for nothing: no quality layers are requested
/// from the sidecar and no columns are written, so it works on a node
/// that declares no quality columns at all.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_blank_spec_asks_for_nothing() {
    let (analysis, _mock) = start_mock_analysis().await;
    let (addr, _node) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis.clone()),
        ..Default::default()
    })
    .await;
    add_docs(
        &addr,
        vec![quality_doc("rust text", Some(QualitySpec::default()))],
    )
    .await
    .unwrap();
}

/// The derived values take the ordinary column path, so the ordinary
/// refusals cover them: a column the shard does not declare refuses by
/// name, and a spec column colliding with an explicit value refuses as
/// the duplicate it is.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn materialized_values_hit_the_ordinary_refusals() {
    let (analysis, _mock) = start_mock_analysis().await;

    // Undeclared column.
    let (addr, _node) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis.clone()),
        ..Default::default()
    })
    .await;
    let err = add_docs(&addr, vec![quality_doc("rust", Some(full_spec()))])
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("noise") && err.message().contains("--numeric-fields"),
        "refusal names the column and the knob: {}",
        err.message()
    );

    // Duplicate: the document already carries an explicit "noise".
    let (addr, _node) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis),
        numeric_fields: vec!["noise".into()],
        integer_fields: vec!["noise_chars".into(), "artifacts".into()],
        ..Default::default()
    })
    .await;
    let mut doc = quality_doc("rust", Some(full_spec()));
    doc.numerics.push(NumericValue {
        field: "noise".into(),
        value: 0.5,
    });
    let err = add_docs(&addr, vec![doc]).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("noise") && err.message().contains("repeats"),
        "the collision is the ordinary duplicate refusal: {}",
        err.message()
    );
}
