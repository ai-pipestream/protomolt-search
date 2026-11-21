//! Regression tests for crash-recovery filesystem error propagation and
//! structured stale-statistics refusal classification:
//! - `recover_segments_swap` and `NodeServiceImpl::open`
//!   must propagate real filesystem errors and fail closed rather than opening an
//!   empty or half-swapped shard when `remove_dir_all` or `rename` fails.
//! - Node and relay stale-statistics refusals carry the structured
//!   `psearch-refusal-reason: stale-stats-epoch` metadata header, and coordinator
//!   retry classification keys solely off that header rather than message prefixes.

mod common;

use std::os::unix::fs::PermissionsExt;

use pipestream_search::node::{segments_root, NodeConfig, NodeServiceImpl};
use pipestream_search::postings::{AnalyzedDoc, Bm25Store};
use pipestream_search::segments::{SegmentCatalog, SegmentSetManifest};

#[path = "common/dir.rs"]
mod dir;
use dir::tempdir;

fn is_effective_root() -> bool {
    // SAFETY: `geteuid` is thread-safe and takes no arguments.
    unsafe { libc::geteuid() == 0 }
}

#[cfg(unix)]
#[test]
fn segments_swap_cleanup_io_error_refuses_shard_open() {
    let dir = tempdir("segments_swap_cleanup_err");
    let index_path = dir.join("shard.idx");
    let root = segments_root(&index_path);
    let old = dir.join("shard.idx.segments.snap-old");
    std::fs::create_dir_all(&root).unwrap();
    let manifest_bytes = serde_json::to_vec_pretty(&SegmentSetManifest::default()).unwrap();
    std::fs::write(SegmentCatalog::manifest_path(&root), &manifest_bytes).unwrap();

    // A regular file at `.segments.snap-old` makes `remove_dir_all(&old)` fail
    // deterministically with `ENOTDIR` (even under UID 0), requiring `open` to
    // surface the cleanup error and refuse shard initialization.
    std::fs::write(&old, b"corrupt-non-dir-backup").unwrap();

    let opened = NodeServiceImpl::open(
        NodeConfig {
            index_path: Some(index_path.clone()),
            ..Default::default()
        },
        None,
    );
    assert!(
        opened.is_err(),
        "NodeServiceImpl::open must fail closed when retiring `.segments.snap-old` fails"
    );

    // Likewise, a stray `.snap-export-*` regular file that cannot be retired by
    // `remove_dir_all` must surface its I/O error instead of being ignored.
    std::fs::remove_file(&old).unwrap();
    let stray_export = dir.join("shard.idx.snap-export-orphan");
    std::fs::write(&stray_export, b"corrupt-export-entry").unwrap();
    let opened_export = NodeServiceImpl::open(
        NodeConfig {
            index_path: Some(index_path),
            ..Default::default()
        },
        None,
    );
    assert!(
        opened_export.is_err(),
        "NodeServiceImpl::open must fail closed when cleaning `.snap-export-*` fails"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

#[cfg(unix)]
#[test]
fn segments_swap_rollback_rename_error_refuses_shard_open() {
    if is_effective_root() {
        return;
    }
    let dir = tempdir("segments_swap_rollback_err");
    let index_path = dir.join("shard.idx");
    let root = segments_root(&index_path);
    let old = dir.join("shard.idx.segments.snap-old");
    std::fs::create_dir_all(&old).unwrap();
    let manifest_bytes = serde_json::to_vec_pretty(&SegmentSetManifest::default()).unwrap();
    std::fs::write(SegmentCatalog::manifest_path(&old), &manifest_bytes).unwrap();

    // Incomplete `.segments` directory with no `segments.json` and a read-only
    // child directory so `remove_dir_all(&root)` fails with `EACCES` and
    // `rename(&old, &root)` fails with `ENOTEMPTY`. Swallowing those errors
    // opens an empty shard while stranding the catalog in `.segments.snap-old`.
    let locked = root.join("locked");
    std::fs::create_dir_all(&locked).unwrap();
    std::fs::write(locked.join("pin"), b"pinned").unwrap();
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o555)).unwrap();

    let opened = NodeServiceImpl::open(
        NodeConfig {
            index_path: Some(index_path),
            ..Default::default()
        },
        None,
    );
    std::fs::set_permissions(&locked, std::fs::Permissions::from_mode(0o755)).unwrap();

    assert!(
        opened.is_err(),
        "NodeServiceImpl::open must fail closed when segments swap rollback fails on disk"
    );
    assert!(
        SegmentCatalog::manifest_path(&old).exists(),
        "backup segment catalog manifest must remain intact when rollback fails"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

async fn start_populated_leaf() -> (
    String,
    tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
) {
    use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
    use pipestream_search::pb::node_service_client::NodeServiceClient;
    use pipestream_search::pb::AddDocumentsRequest;

    let (addr, handle) = common::start_empty_node(NodeConfig {
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
        ..Default::default()
    })
    .await;
    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    let docs = ["court opinion alpha", "court opinion beta"]
        .into_iter()
        .enumerate()
        .map(|(i, text)| AddDocumentsRequest {
            text: text.into(),
            analysis: Some(body_spec()),
            original_source: Some(common::protobuf_source(text, &format!("doc-{i}"))),
            ..Default::default()
        });
    client
        .add_documents(tokio_stream::iter(docs))
        .await
        .unwrap();
    (addr, handle)
}

fn stale_bm25_request(
    stats: &pipestream_search::pb::TermStatsResponse,
) -> pipestream_search::pb::Bm25QueryRequest {
    pipestream_search::pb::Bm25QueryRequest {
        terms: vec!["court".into()],
        k: 5,
        global_doc_count: stats.doc_count,
        global_total_doc_length: stats.total_doc_length,
        global_doc_frequencies: stats.doc_frequencies.clone(),
        expected_stats_epoch: stats.stats_epoch,
        expected_stats_incarnation: stats.stats_incarnation.clone(),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn genuine_node_and_relay_stale_stats_refusals_carry_metadata_header() {
    use pipestream_search::analyzer::body_spec;
    use pipestream_search::harness::start_relay;
    use pipestream_search::pb::node_service_client::NodeServiceClient;
    use pipestream_search::pb::{AddDocumentsRequest, TermStatsRequest};
    use pipestream_search::stats_identity::{
        is_stale_stats_epoch, REFUSAL_REASON_HEADER, STALE_STATS_EPOCH_REASON,
    };

    let (leaf_addr, leaf_handle) = start_populated_leaf().await;
    let (relay_addr, relay, relay_handle) = start_relay(vec![leaf_addr.clone()]).await;
    let mut leaf = NodeServiceClient::connect(leaf_addr).await.unwrap();
    let mut relay_client = NodeServiceClient::connect(relay_addr).await.unwrap();

    let req = TermStatsRequest {
        terms: vec!["court".into()],
        ..Default::default()
    };
    let leaf_stats = leaf.term_stats(req.clone()).await.unwrap().into_inner();
    let relay_stats = relay_client.term_stats(req).await.unwrap().into_inner();

    leaf.add_documents(tokio_stream::iter([AddDocumentsRequest {
        text: "court opinion gamma".into(),
        analysis: Some(body_spec()),
        original_source: Some(common::protobuf_source("court opinion gamma", "doc-2")),
        ..Default::default()
    }]))
    .await
    .unwrap();

    let leaf_err = leaf
        .bm25_query(stale_bm25_request(&leaf_stats))
        .await
        .unwrap_err();
    assert!(is_stale_stats_epoch(&leaf_err), "{leaf_err:?}");
    assert_eq!(
        leaf_err.metadata().get(REFUSAL_REASON_HEADER).unwrap(),
        STALE_STATS_EPOCH_REASON
    );

    let relayed_err = relay_client
        .bm25_query(stale_bm25_request(&relay_stats))
        .await
        .unwrap_err();
    assert!(is_stale_stats_epoch(&relayed_err), "{relayed_err:?}");
    assert!(relayed_err.message().contains("relay child 0"));

    let token_err = relay
        .translate_epoch(relay_stats.stats_epoch ^ 0x5555)
        .unwrap_err();
    assert!(is_stale_stats_epoch(&token_err), "{token_err:?}");

    relay_handle.abort();
    leaf_handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn coordinator_and_relay_refuse_spoofed_stale_stats_message_without_header() {
    use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
    use pipestream_search::coordinator::CoordinatorServiceImpl;
    use pipestream_search::harness::{nodelay_incoming, start_relay};
    use pipestream_search::stats_identity::is_stale_stats_epoch;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::Arc;
    use tonic::service::interceptor::InterceptedService;

    let mut store = Bm25Store::new();
    store.add_document_with_lineage(
        0,
        "court".into(),
        AnalyzedDoc::body(vec![("court".into(), 1, vec![(0, 5)])], 1),
        None,
    );
    let rpc_calls = Arc::new(AtomicUsize::new(0));
    let calls_ref = Arc::clone(&rpc_calls);
    let bm25 = pipestream_search::node::Bm25Shard::Building(store);
    let node = NodeServiceImpl::new(None, NodeConfig::default()).with_bm25(Some(bm25));
    let intercepted = InterceptedService::new(
        node.into_server(pipestream_search::MAX_MESSAGE_BYTES),
        move |req: tonic::Request<()>| {
            if calls_ref.fetch_add(1, Ordering::SeqCst) >= 1 {
                return Err(tonic::Status::failed_precondition(
                    "stale stats epoch: spoofed message prefix without metadata header",
                ));
            }
            Ok(req)
        },
    );
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let leaf_addr = format!("http://{}", listener.local_addr().unwrap());
    let leaf_handle = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(intercepted)
            .serve_with_incoming(nodelay_incoming(listener)),
    );

    let (relay_addr, _relay, relay_handle) = start_relay(vec![leaf_addr]).await;
    let coordinator = CoordinatorServiceImpl::new(vec![relay_addr])
        .with_bm25(Some(NATIVE_ANALYSIS_BACKEND.into()), Default::default());
    let err = coordinator
        .fanout_bm25("court", 5, Some(&body_spec()))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("spoofed message prefix"));
    assert!(!is_stale_stats_epoch(&err), "{err:?}");
    assert_eq!(
        rpc_calls.load(Ordering::SeqCst),
        2,
        "coordinator must not refetch TermStats or retry scoring without structured metadata"
    );

    relay_handle.abort();
    leaf_handle.abort();
}
