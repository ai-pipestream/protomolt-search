//! End-to-end coverage of the vocabulary ingest hook: AddDocuments over a
//! mock AnalyzeStream feeds the shard's vocabulary listener inline,
//! windows roll over into on-disk snapshots, and the disabled default
//! writes nothing.

mod common;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use turbovec_search::node::NodeConfig;
use turbovec_search::pb::node_service_client::NodeServiceClient;
use turbovec_search::pb::{AddDocumentsRequest, AddDocumentsResponse};
use turbovec_search::pb::analysis::VocabChannel;
use turbovec_search::vocab;

use common::mock::start_mock_analysis;
use common::start_empty_node;

const DOCS: [&str; 3] = [
    "Rust search rust fast",
    "vector search Rust",
    "court ruling appeal",
];

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("vocab_{tag}_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn add_documents(addr: &str, texts: &[&str]) -> AddDocumentsResponse {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(8);
    for text in texts {
        tx.send(AddDocumentsRequest {
            text: (*text).to_string(),
            analysis: None,
            lineage: None,
            fields: Vec::new(),
            ..Default::default()
        })
        .await
        .unwrap();
    }
    drop(tx);
    client
        .add_documents(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ingest_feeds_the_vocabulary_listener() {
    let (analysis, mock) = start_mock_analysis().await;
    let dir = temp_dir("ingest");
    let index_path = dir.join("shard.tv");
    let (addr, node) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis),
        index_path: Some(index_path.clone()),
        vocab: true,
        vocab_window_docs: 2,
        vocab_top_k: 16,
        ..Default::default()
    })
    .await;
    let resp = add_documents(&addr, &DOCS).await;
    assert_eq!(resp.added, 3);

    // Rollover at 2 documents: exactly one snapshot sealed with the first
    // two docs; the third stays in the live window.
    let vocab_dir = vocab::vocab_dir(&index_path);
    let scan = vocab::scan_snapshot_dir(&vocab_dir).unwrap();
    assert_eq!(scan.len(), 1);
    assert_eq!(scan[0].0.documents, 2);
    let snapshot = vocab::load_snapshot_file(&scan[0].1).unwrap();
    assert_eq!(snapshot.sequence, 0);
    assert_eq!(snapshot.channels.len(), 2);
    let states = vocab::states_by_channel(&snapshot).unwrap();

    // TERMS: the mock folds to lowercase identities — rust x3 (2+1),
    // search x2, fast x1, vector x1 over two documents.
    let terms = states.get(&VocabChannel::Terms).unwrap();
    assert_eq!(terms.documents(), 2);
    assert_eq!(terms.occurrences(), 7);
    assert_eq!(terms.estimate("rust"), 3);
    assert_eq!(terms.estimate("search"), 2);
    assert!((terms.cardinality_estimate() - 4.0).abs() < 0.5);

    // TOKENS: raw surface forms — "Rust" (capital R) and "rust" are
    // distinct identities here, exactly the pre-normalization channel.
    let tokens = states.get(&VocabChannel::Tokens).unwrap();
    assert_eq!(tokens.documents(), 2);
    assert_eq!(tokens.occurrences(), 7);
    assert_eq!(tokens.estimate("Rust"), 2);
    assert_eq!(tokens.estimate("rust"), 1);
    assert!((tokens.cardinality_estimate() - 5.0).abs() < 0.5);

    // Drift of the snapshot against itself: nothing moved.
    let drift = vocab::compute_channel_drift(&snapshot, &snapshot, None).unwrap();
    for channel in &drift {
        assert!(channel.metrics.novelty_rate < 0.01, "{:?}", channel.channel);
        assert!(channel.metrics.jensen_shannon_divergence < 0.01);
    }

    node.abort();
    mock.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn vocab_disabled_by_default_writes_nothing() {
    let (analysis, mock) = start_mock_analysis().await;
    let dir = temp_dir("disabled");
    let index_path = dir.join("shard.tv");
    let (addr, node) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis),
        index_path: Some(index_path.clone()),
        ..Default::default()
    })
    .await;
    let resp = add_documents(&addr, &DOCS[..1]).await;
    assert_eq!(resp.added, 1);

    assert!(!vocab::vocab_dir(&index_path).exists());

    node.abort();
    mock.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
