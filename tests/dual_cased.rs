//! Dual-cased term identity from one analysis pass
//! (`docs/dual-cased.md`): `AddDocumentsRequest.cased_field` takes the
//! body's cased identity out of the same Analyze (sidecar mock with a
//! call meter, and the native analyzer), the folded field matches every
//! case variant while the cased field matches only the exact form, the
//! two fields' spans and positions coincide, fingerprints refuse a leg
//! analyzed under the wrong twin, WAL replay analyzes once, and the
//! refusal table names each cause.

mod common;

use std::path::PathBuf;

use common::mock::{start_mock_analysis_metered, start_mock_analysis_without_dual_identity};
use common::{fit_calibration, start_empty_node, unit_vectors, BIT_WIDTH, DIM};
use pipestream_search::analyzer::{
    analysis_fingerprint, body_spec, cased_body_spec, cased_twin_spec, NATIVE_ANALYSIS_BACKEND,
};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::{bm25_sidecar_path, Layout, NodeConfig};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    AddDocumentsRequest, AnalysisSpec, Bm25SearchRequest, Bm25SearchResponse, DocumentField,
    FlushRequest, QueryField, SetCalibrationRequest,
};
use pipestream_search::postings::{Bm25Index, Bm25Reader};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Request};

const DOCS: [&str; 4] = [
    "COURT court Court holds\nthe appeal fails",
    "Court of appeals\nthe court sat",
    "court reporter\nnothing else",
    "APPEAL denied\nno court here",
];

fn tempdir(tag: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("dual_cased_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn config(analysis: &str, index_path: Option<PathBuf>) -> NodeConfig {
    NodeConfig {
        index_path,
        analysis_addr: Some(analysis.to_string()),
        bm25_fields: vec!["body".into(), "body_cased".into()],
        position_fields: vec!["body".into(), "body_cased".into()],
        sentence_fields: vec!["body".into(), "body_cased".into()],
        layout: Layout::SingleImage,
        ..Default::default()
    }
}

fn doc(text: &str) -> AddDocumentsRequest {
    AddDocumentsRequest {
        text: text.to_string(),
        analysis: Some(body_spec()),
        cased_field: "body_cased".into(),
        ..Default::default()
    }
}

async fn ingest(addr: &str, docs: Vec<AddDocumentsRequest>) -> Result<u64, tonic::Status> {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(8);
    for d in docs {
        tx.send(d).await.unwrap();
    }
    drop(tx);
    client
        .add_documents(ReceiverStream::new(rx))
        .await
        .map(|r| r.into_inner().added)
}

fn leg(field: &str, spec: AnalysisSpec) -> QueryField {
    QueryField {
        field: field.to_string(),
        analysis: Some(spec),
        weight: 1.0,
        ..Default::default()
    }
}

async fn search(
    coordinator: &CoordinatorServiceImpl,
    text: &str,
    legs: Vec<QueryField>,
) -> Result<Bm25SearchResponse, tonic::Status> {
    coordinator
        .bm25_search(Request::new(Bm25SearchRequest {
            text: text.to_string(),
            k: 10,
            fields: legs,
            ..Default::default()
        }))
        .await
        .map(|r| r.into_inner())
}

fn ids(response: &Bm25SearchResponse) -> Vec<u64> {
    let mut ids: Vec<u64> = response.hits.iter().map(|h| h.doc_id).collect();
    ids.sort_unstable();
    ids
}

fn coordinator(addrs: Vec<String>, analysis: &str) -> CoordinatorServiceImpl {
    CoordinatorServiceImpl::new(addrs).with_bm25(Some(analysis.to_string()), Default::default())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn one_analysis_yields_both_identities_and_each_field_answers_its_own() {
    let (analysis, mock, calls) = start_mock_analysis_metered().await;
    let dir = tempdir("pair");
    let index_path = dir.join("shard.tv");
    let (addr, node) = start_empty_node(config(&analysis, Some(index_path.clone()))).await;
    let before = calls.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        ingest(&addr, DOCS.iter().map(|t| doc(t)).collect())
            .await
            .unwrap(),
        4
    );
    assert_eq!(
        calls.load(std::sync::atomic::Ordering::SeqCst) - before,
        4,
        "one Analyze per document: the cased field cost no second pass"
    );

    // A single-image node serves after its flush.
    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    assert!(
        client
            .flush(FlushRequest {})
            .await
            .unwrap()
            .into_inner()
            .written
    );
    let c = coordinator(vec![addr.clone()], &analysis);
    let twin = cased_twin_spec(&body_spec());
    // Folded recall: every case variant of "court".
    let folded = search(&c, "court", vec![leg("body", body_spec())])
        .await
        .unwrap();
    assert_eq!(ids(&folded), vec![0, 1, 2, 3]);
    // The cased field answers the exact form only.
    let cased = search(&c, "Court", vec![leg("body_cased", twin.clone())])
        .await
        .unwrap();
    assert_eq!(ids(&cased), vec![0, 1]);
    let shouting = search(&c, "COURT", vec![leg("body_cased", twin.clone())])
        .await
        .unwrap();
    assert_eq!(ids(&shouting), vec![0]);
    let lower = search(&c, "court", vec![leg("body_cased", twin.clone())])
        .await
        .unwrap();
    assert_eq!(ids(&lower), vec![0, 1, 2, 3]);
    // A leg on the cased field analyzed under the folded spec is refused
    // naming both fingerprints: the two score different identities.
    let wrong = search(&c, "court", vec![leg("body_cased", body_spec())])
        .await
        .unwrap_err();
    assert_eq!(wrong.code(), Code::FailedPrecondition);
    assert!(
        wrong.message().contains("analyzer fingerprint"),
        "{}",
        wrong.message()
    );

    // The flushed image: both fields, twin fingerprints, coinciding spans
    // and positions for the same tokens.
    let reader = Bm25Reader::open(&bm25_sidecar_path(&index_path)).unwrap();
    assert_eq!(reader.field_index("body_cased"), Some(1));
    assert_eq!(
        reader.analysis_fingerprint(0),
        analysis_fingerprint(Some(&body_spec()))
    );
    assert_eq!(
        reader.analysis_fingerprint(1),
        analysis_fingerprint(Some(&twin))
    );
    assert_ne!(
        reader.analysis_fingerprint(0),
        reader.analysis_fingerprint(1)
    );
    let body = reader.field(0);
    let cased = reader.field(1);
    // Doc 0: folded "court" x3 is cased COURT + court + Court, span for span.
    let mut folded_spans = body.posting_offsets("court", 0);
    folded_spans.sort_unstable();
    let mut cased_spans: Vec<(u32, u32)> = ["COURT", "court", "Court"]
        .iter()
        .flat_map(|t| cased.posting_offsets(t, 0))
        .collect();
    cased_spans.sort_unstable();
    assert_eq!(folded_spans, cased_spans);
    assert_eq!(folded_spans.len(), 3);
    let mut folded_positions = body.posting_positions("court", 0).unwrap();
    folded_positions.sort_unstable();
    let mut cased_positions: Vec<u32> = ["COURT", "court", "Court"]
        .iter()
        .flat_map(|t| cased.posting_positions(t, 0).unwrap())
        .collect();
    cased_positions.sort_unstable();
    assert_eq!(folded_positions, cased_positions);
    assert_eq!(folded_positions, vec![0, 1, 2]);
    assert_eq!(body.doc_sentences(0), cased.doc_sentences(0));
    assert_eq!(body.doc_sentences(0).map(|s| s.len()), Some(2));
    assert_eq!(body.doc_length(0), cased.doc_length(0));
    assert_eq!(body.df("court"), 4);
    assert_eq!(cased.df("court"), 4);
    assert_eq!(cased.df("Court"), 2);
    assert_eq!(cased.df("COURT"), 1);
    // The folded field never learned a cased form.
    assert_eq!(body.df("Court"), 0);

    node.abort();
    mock.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_shards_equal_one_on_the_cased_leg() {
    let (analysis, mock, _) = start_mock_analysis_metered().await;
    let twin = cased_twin_spec(&body_spec());
    let mut nodes = Vec::new();
    let mut addrs = Vec::new();
    for shard in 0..2usize {
        let (addr, node) = start_empty_node(NodeConfig {
            slot_offset: (shard * 2) as u64,
            ..config(&analysis, None)
        })
        .await;
        ingest(
            &addr,
            DOCS[shard * 2..shard * 2 + 2]
                .iter()
                .map(|t| doc(t))
                .collect(),
        )
        .await
        .unwrap();
        addrs.push(addr);
        nodes.push(node);
    }
    let (one_addr, one_node) = start_empty_node(config(&analysis, None)).await;
    ingest(&one_addr, DOCS.iter().map(|t| doc(t)).collect())
        .await
        .unwrap();
    let distributed = coordinator(addrs, &analysis);
    let monolithic = coordinator(vec![one_addr], &analysis);
    for text in ["Court", "court", "COURT", "appeal"] {
        let a = search(&distributed, text, vec![leg("body_cased", twin.clone())])
            .await
            .unwrap();
        let b = search(&monolithic, text, vec![leg("body_cased", twin.clone())])
            .await
            .unwrap();
        let bits = |r: &Bm25SearchResponse| -> Vec<(u64, u32)> {
            r.hits
                .iter()
                .map(|h| (h.doc_id, h.score.to_bits()))
                .collect()
        };
        assert_eq!(bits(&a), bits(&b), "{text}");
    }
    for node in nodes {
        node.abort();
    }
    one_node.abort();
    mock.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_native_analyzer_produces_the_pair_from_one_pass() {
    let dir = tempdir("native");
    let index_path = dir.join("shard.tv");
    let (addr, node) =
        start_empty_node(config(NATIVE_ANALYSIS_BACKEND, Some(index_path.clone()))).await;
    assert_eq!(
        ingest(&addr, DOCS.iter().map(|t| doc(t)).collect())
            .await
            .unwrap(),
        4
    );
    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    assert!(
        client
            .flush(FlushRequest {})
            .await
            .unwrap()
            .into_inner()
            .written
    );
    let c = coordinator(vec![addr.clone()], NATIVE_ANALYSIS_BACKEND);
    let twin = cased_twin_spec(&body_spec());
    let folded = search(&c, "court", vec![leg("body", body_spec())])
        .await
        .unwrap();
    assert_eq!(ids(&folded), vec![0, 1, 2, 3]);
    let cased = search(&c, "Court", vec![leg("body_cased", twin.clone())])
        .await
        .unwrap();
    assert_eq!(ids(&cased), vec![0, 1]);
    let reader = Bm25Reader::open(&bm25_sidecar_path(&index_path)).unwrap();
    let body = reader.field(0);
    let cased = reader.field(1);
    assert_eq!(
        reader.analysis_fingerprint(1),
        analysis_fingerprint(Some(&twin))
    );
    let mut folded_spans = body.posting_offsets("court", 0);
    folded_spans.sort_unstable();
    let mut cased_spans: Vec<(u32, u32)> = ["COURT", "court", "Court"]
        .iter()
        .flat_map(|t| cased.posting_offsets(t, 0))
        .collect();
    cased_spans.sort_unstable();
    assert_eq!(folded_spans, cased_spans);
    assert_eq!(
        body.posting_positions("court", 0).map(|mut p| {
            p.sort_unstable();
            p
        }),
        Some(vec![0, 1, 2])
    );
    assert_eq!(cased.posting_positions("Court", 0), Some(vec![2]));
    assert_eq!(body.doc_sentences(0), cased.doc_sentences(0));
    node.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wal_replay_analyzes_each_document_once_and_keeps_the_pair() {
    let (analysis, mock, calls) = start_mock_analysis_metered().await;
    let dir = tempdir("replay");
    let index_path = dir.join("parent.tv");
    let (addr, node) = start_empty_node(NodeConfig {
        wal: true,
        ..config(&analysis, Some(index_path.clone()))
    })
    .await;
    // Reshard replays a WAL with a locked vector backend; lock one the
    // way the reshard fixtures do, documents-only otherwise.
    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &unit_vectors(8, DIM, 0xD0A1_0001));
    client
        .set_calibration(SetCalibrationRequest {
            dim: DIM as u32,
            bit_width: BIT_WIDTH as u32,
            shift,
            scale,
        })
        .await
        .unwrap();
    ingest(&addr, DOCS.iter().map(|t| doc(t)).collect())
        .await
        .unwrap();
    assert!(
        client
            .flush(FlushRequest {})
            .await
            .unwrap()
            .into_inner()
            .written
    );
    node.abort();

    let handle = tokio::runtime::Handle::current();
    let analyze_addr = analysis.clone();
    let mut analyze = move |docs: &[(
        &str,
        Option<&AnalysisSpec>,
        pipestream_search::analyzer::SessionLayers,
    )]| {
        tokio::task::block_in_place(|| {
            handle
                .block_on(pipestream_search::analyzer::analyze_batch_streams(
                    &analyze_addr,
                    docs,
                    1,
                ))
                .map_err(|error| error.to_string())
        })
    };
    let before = calls.load(std::sync::atomic::Ordering::SeqCst);
    let output = pipestream_search::reshard::split(
        &pipestream_search::reshard::resolve_gen(&pipestream_search::wal::wal_dir(&index_path))
            .unwrap(),
        1,
        &dir.join("child"),
        0,
        25_000_000,
        false,
        Some(&["body".to_string(), "body_cased".to_string()]),
        &mut analyze,
    )
    .unwrap();
    assert_eq!(
        calls.load(std::sync::atomic::Ordering::SeqCst) - before,
        4,
        "replay analyzed each document once; the cased field rode that pass"
    );
    let child = Bm25Reader::open(output.children[0].bm25_path.as_ref().unwrap()).unwrap();
    let parent = Bm25Reader::open(&bm25_sidecar_path(&index_path)).unwrap();
    assert_eq!(child.field_index("body_cased"), Some(1));
    assert_eq!(
        child.analysis_fingerprint(1),
        parent.analysis_fingerprint(1)
    );
    for term in ["COURT", "Court", "court", "appeal", "APPEAL"] {
        assert_eq!(child.field(1).df(term), parent.field(1).df(term), "{term}");
        assert_eq!(child.field(0).df(term), parent.field(0).df(term), "{term}");
    }
    let mut a = parent.field(1).posting_offsets("Court", 1);
    let mut b = child.field(1).posting_offsets("Court", 1);
    a.sort_unstable();
    b.sort_unstable();
    assert_eq!(a, b);
    mock.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn refusals_name_their_cause() {
    let (analysis, mock, calls) = start_mock_analysis_metered().await;
    let (addr, node) = start_empty_node(config(&analysis, None)).await;
    let refuse = |request: AddDocumentsRequest| {
        let addr = addr.clone();
        async move { ingest(&addr, vec![request]).await.unwrap_err() }
    };
    let body = refuse(AddDocumentsRequest {
        cased_field: "body".into(),
        ..doc(DOCS[0])
    })
    .await;
    assert_eq!(body.code(), Code::InvalidArgument);
    assert!(
        body.message().contains("other than \"body\""),
        "{}",
        body.message()
    );

    let unknown = refuse(AddDocumentsRequest {
        cased_field: "title_cased".into(),
        ..doc(DOCS[0])
    })
    .await;
    assert!(
        unknown
            .message()
            .contains("unknown cased_field \"title_cased\""),
        "{}",
        unknown.message()
    );

    let stems = refuse(AddDocumentsRequest {
        analysis: Some(cased_body_spec()),
        ..doc(DOCS[0])
    })
    .await;
    assert!(
        stems.message().contains("step-chain") && stems.message().contains("SOURCE_STEMS"),
        "{}",
        stems.message()
    );

    let twice = refuse(AddDocumentsRequest {
        fields: vec![DocumentField {
            field: "body_cased".into(),
            text: "Court".into(),
            analysis: Some(cased_twin_spec(&body_spec())),
        }],
        ..doc(DOCS[0])
    })
    .await;
    assert!(
        twice
            .message()
            .contains("do not supply it as a DocumentField"),
        "{}",
        twice.message()
    );

    let server = refuse(AddDocumentsRequest {
        analysis: None,
        ..doc(DOCS[0])
    })
    .await;
    assert!(
        server.message().contains("explicit body AnalysisSpec"),
        "{}",
        server.message()
    );
    // Nothing above reached the analyzer.
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    node.abort();
    mock.abort();

    // A sidecar that predates the dual identity is refused by the
    // capability it lacks, before any document is analyzed.
    let (older, older_mock, older_calls) = start_mock_analysis_without_dual_identity().await;
    let (addr, node) = start_empty_node(config(&older, None)).await;
    let error = ingest(&addr, vec![doc(DOCS[0])]).await.unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(
        error
            .message()
            .contains("dual_term_identity_available = false"),
        "{}",
        error.message()
    );
    assert_eq!(older_calls.load(std::sync::atomic::Ordering::SeqCst), 0);
    // Without a cased field that sidecar serves as before.
    assert_eq!(
        ingest(
            &addr,
            vec![AddDocumentsRequest {
                cased_field: String::new(),
                ..doc(DOCS[0])
            }]
        )
        .await
        .unwrap(),
        1
    );
    node.abort();
    older_mock.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_twin_fingerprint_hashes_the_spec_and_nothing_else() {
    // The cased twin drops only the case-folding steps; everything else
    // in the chain, the stemmer, and the source carry over, so the
    // fingerprint is the twin spec's and no request layer (quality,
    // geography) enters it.
    let twin = cased_twin_spec(&body_spec());
    assert_eq!(twin.tokenizer, body_spec().tokenizer);
    assert_eq!(twin.stemmer, body_spec().stemmer);
    assert_eq!(twin.term_vector_source, body_spec().term_vector_source);
    assert!(twin
        .char_filters
        .contains(&pipestream_search::analyzer::CHAR_FILTER_ACCENT_FOLD));
    assert!(!twin
        .char_filters
        .contains(&pipestream_search::analyzer::CHAR_FILTER_FULL_CASE_FOLD));
    assert_ne!(
        analysis_fingerprint(Some(&twin)),
        analysis_fingerprint(Some(&body_spec()))
    );
    assert_ne!(
        analysis_fingerprint(Some(&twin)),
        analysis_fingerprint(Some(&cased_body_spec())),
        "the twin is not the old SOURCE_STEMS cased arm"
    );
    assert_eq!(
        analysis_fingerprint(Some(&twin)),
        analysis_fingerprint(Some(&cased_twin_spec(&body_spec()))),
        "pure in the spec"
    );
}
