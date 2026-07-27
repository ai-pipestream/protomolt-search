//! BM25 acceptance tests: ingest through the mock analysis sidecar, then
//! query. Proves postings content, ranking, the distributed global-stats
//! flow (coordinator == monolithic EXACTLY), that shard-local stats would
//! differ (regression guard), and the STEMS identity path.

mod common;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use turbovec_search::coordinator::CoordinatorServiceImpl;
use turbovec_search::node::NodeConfig;
use turbovec_search::pb::node_service_client::NodeServiceClient;
use turbovec_search::pb::{
    AddDocumentsRequest, AnalysisSpec, Bm25Hit, Bm25QueryRequest, GetDocumentsRequest,
    TermStatsRequest,
};

use common::{mock::start_mock_analysis, start_empty_node};

/// The controlled corpus: six documents over three shards. df("rust")=4,
/// df("search")=3, df("vector")=2.
const SHARD_DOCS: [&[&str]; 3] = [
    &["rust search rust fast", "vector search rust"],
    &["search engines love rust", "vector vector vector"],
    &["rust", "nothing relevant here"],
];

const OFFSETS: [u64; 3] = [0, 2, 4];

async fn add_documents(
    addr: &str,
    texts: &[&str],
    spec: Option<AnalysisSpec>,
) -> turbovec_search::pb::AddDocumentsResponse {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(8);
    for text in texts {
        tx.send(AddDocumentsRequest {
            text: text.to_string(),
            analysis: spec.clone(),
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

async fn start_doc_shards(
    analysis: &str,
) -> (
    Vec<String>,
    Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
) {
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for (i, _) in SHARD_DOCS.iter().enumerate() {
        let (addr, handle) = start_empty_node(NodeConfig {
            slot_offset: OFFSETS[i],
            analysis_addr: Some(analysis.to_string()),
            ..Default::default()
        })
        .await;
        addrs.push(addr);
        handles.push(handle);
    }
    for (i, docs) in SHARD_DOCS.iter().enumerate() {
        add_documents(&addrs[i], docs, None).await;
    }
    (addrs, handles)
}

fn hit_signature(hits: &[Bm25Hit]) -> Vec<(u64, u32)> {
    hits.iter().map(|h| (h.doc_id, h.score.to_bits())).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ingest_through_mock_builds_postings() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addr, node) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis),
        ..Default::default()
    })
    .await;
    let resp = add_documents(&addr, SHARD_DOCS[0], None).await;
    assert_eq!((resp.added, resp.total, resp.first_id), (2, 2, 0));

    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    let stats = client
        .term_stats(TermStatsRequest {
            terms: vec!["rust".into(), "vector".into(), "nope".into()],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(stats.doc_count, 2);
    assert_eq!(stats.total_doc_length, 7); // 4 + 3 terms
    assert_eq!(stats.doc_frequencies, vec![2, 1, 0]);

    // Doc store holds raw texts; postings hold original-text offsets.
    let docs = client
        .get_documents(GetDocumentsRequest {
            doc_ids: vec![0, 1, 99],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(docs.documents.len(), 2);
    assert_eq!(docs.documents[0].text, "rust search rust fast");

    let hits = client
        .bm25_query(Bm25QueryRequest {
            terms: vec!["rust".into()],
            k: 10,
            global_doc_count: 2,
            global_total_doc_length: 7,
            global_doc_frequencies: vec![2],
            k1: 0.0,
            b: 0.0,
        })
        .await
        .unwrap()
        .into_inner()
        .hits;
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].doc_id, 0, "tf=2 beats tf=1");
    assert!(hits[0].score > hits[1].score);
    let rust_offsets = &hits[0]
        .terms
        .iter()
        .find(|t| t.term == "rust")
        .unwrap()
        .offsets;
    let spans: Vec<(u32, u32)> = rust_offsets.iter().map(|o| (o.start, o.end)).collect();
    assert_eq!(spans, vec![(0, 4), (12, 16)]);

    node.abort();
    mock.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_bm25_matches_monolithic_exactly() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addrs, handles) = start_doc_shards(&analysis).await;

    // Monolithic reference: one node ingested with every document.
    let (mono_addr, mono) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis.clone()),
        ..Default::default()
    })
    .await;
    let all: Vec<&str> = SHARD_DOCS.concat();
    add_documents(&mono_addr, &all, None).await;

    let distributed = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(Some(analysis.clone()), Default::default());
    let monolithic = CoordinatorServiceImpl::new(vec![mono_addr])
        .with_bm25(Some(analysis.clone()), Default::default());

    for text in ["rust", "search rust", "vector", "unobtainium"] {
        for k in [3u32, 6] {
            let got = distributed.fanout_bm25(text, k, None).await.unwrap();
            let want = monolithic.fanout_bm25(text, k, None).await.unwrap();
            assert_eq!(
                hit_signature(&got),
                hit_signature(&want),
                "query {text:?} k={k}: distributed != monolithic"
            );
            // Offsets survive the distributed round-trip for highlighting.
            for (g, w) in got.iter().zip(want.iter()) {
                assert_eq!(g.terms.len(), w.terms.len());
            }
        }
    }

    // Ranking sanity on "rust": d4 (a 1-word document) wins via BM25
    // length normalization; d0 (tf=2, average length) follows.
    let hits = distributed.fanout_bm25("rust", 6, None).await.unwrap();
    let ids: Vec<u64> = hits.iter().map(|h| h.doc_id).collect();
    assert_eq!(ids.len(), 4);
    assert_eq!(ids[0], 4, "short matching doc wins BM25: {ids:?}");

    for h in handles {
        h.abort();
    }
    mono.abort();
    mock.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bm25_store_persists_through_flush() {
    let (analysis, mock) = start_mock_analysis().await;
    let dir = std::env::temp_dir().join(format!("tvbm25_node_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let index_path = dir.join("shard.tv");
    let (addr, node) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis),
        index_path: Some(index_path.clone()),
        ..Default::default()
    })
    .await;
    add_documents(&addr, SHARD_DOCS[0], None).await;

    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    let flushed = client
        .flush(turbovec_search::pb::FlushRequest {})
        .await
        .unwrap()
        .into_inner();
    assert!(flushed.written);
    assert_eq!(flushed.num_documents, 2);

    let store = turbovec_search::postings::Bm25Store::load(
        &turbovec_search::node::bm25_sidecar_path(&index_path),
    )
    .unwrap();
    assert_eq!(store.doc_count(), 2);
    assert_eq!(store.total_doc_length(), 7);
    assert_eq!(store.postings("rust").unwrap().len(), 2);
    assert_eq!(store.text(1), Some("vector search rust"));

    node.abort();
    mock.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Regression guard: shard-LOCAL stats must produce different scores than
/// the global-stats flow. If the coordinator ever regresses to local
/// stats, this test fails.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shard_local_stats_would_differ() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addrs, handles) = start_doc_shards(&analysis).await;

    // Global-stats result (the correct one).
    let global = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(Some(analysis), Default::default())
        .fanout_bm25("rust", 6, None)
        .await
        .unwrap();

    // What scoring with LOCAL stats would yield: each shard gets its own
    // doc_count/total/df handed in as "global".
    let terms = vec!["rust".to_string()];
    let mut local_hits = Vec::new();
    for addr in &addrs {
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        let stats = client
            .term_stats(TermStatsRequest {
                terms: terms.clone(),
            })
            .await
            .unwrap()
            .into_inner();
        let hits = client
            .bm25_query(Bm25QueryRequest {
                terms: terms.clone(),
                k: 6,
                global_doc_count: stats.doc_count,
                global_total_doc_length: stats.total_doc_length,
                global_doc_frequencies: stats.doc_frequencies,
                k1: 0.0,
                b: 0.0,
            })
            .await
            .unwrap()
            .into_inner()
            .hits;
        local_hits.extend(hits);
    }
    local_hits.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then_with(|| a.doc_id.cmp(&b.doc_id))
    });

    // Same SET of docs (losslessness of coverage is not in question), but
    // different scores somewhere: local idf/avgdl distort scoring.
    let global_ids: Vec<u64> = global.iter().map(|h| h.doc_id).collect();
    let local_ids: Vec<u64> = local_hits.iter().map(|h| h.doc_id).collect();
    assert_eq!(global_ids.len(), local_ids.len());
    let any_score_differs = global
        .iter()
        .zip(local_hits.iter())
        .any(|(g, l)| g.doc_id != l.doc_id || g.score != l.score);
    assert!(
        any_score_differs,
        "local and global stats produced identical rankings; the regression guard is vacuous"
    );

    for h in handles {
        h.abort();
    }
    mock.abort();
}

/// STEMS identity: ingest and query with SOURCE_STEMS; different surface
/// forms of one stem rank as one term.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stems_source_groups_surface_forms() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addr, node) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis.clone()),
        ..Default::default()
    })
    .await;
    let spec = AnalysisSpec {
        tokenizer: 0,
        stemmer: 2, // any real stemmer value; mock stems regardless
        term_vector_mode: 0,
        term_vector_source: 2, // SOURCE_STEMS
        normalizer_rungs: vec![],
    };
    add_documents(
        &addr,
        &["running runs fast", "run slow", "unrelated text"],
        Some(spec.clone()),
    )
    .await;

    let coordinator = CoordinatorServiceImpl::new(vec![addr.clone()])
        .with_bm25(Some(analysis), Default::default());

    // Query "runs" stems to "run" and matches docs 0 and 1; doc 0 wins on
    // tf (2 occurrences of the stem).
    let hits = coordinator
        .fanout_bm25("runs", 10, Some(&spec))
        .await
        .unwrap();
    let ids: Vec<u64> = hits.iter().map(|h| h.doc_id).collect();
    assert_eq!(ids, vec![0, 1]);
    assert!(hits[0].score > hits[1].score);

    // Query "running" hits the same stem identity: identical ranking.
    let hits2 = coordinator
        .fanout_bm25("running", 10, Some(&spec))
        .await
        .unwrap();
    assert_eq!(hit_signature(&hits), hit_signature(&hits2));

    // Occurrences of every surface form are grouped under the stem.
    let run = hits[0].terms.iter().find(|t| t.term == "run").unwrap();
    assert_eq!(run.offsets.len(), 2, "running + runs grouped under run");

    node.abort();
    mock.abort();
}
