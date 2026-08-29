//! Term-stats cache acceptance: the epoch a node advertises, the
//! refusal it enforces, and the coordinator behaviors that follow —
//! a repeated query issues no second stats fan-out, and an ingest
//! between queries is NEVER scored with stale stats (the shard refuses,
//! the coordinator refetches, and the results match a coordinator that
//! never cached anything, bitwise).

mod common;

use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::{
    AddDocumentsRequest, Bm25Hit, Bm25QueryRequest, DocumentField, QueryField, TermStatsRequest,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use common::{mock::start_mock_analysis, start_empty_node};

/// Same controlled corpus as tests/bm25_search.rs: six documents over
/// three shards.
const SHARD_DOCS: [&[&str]; 3] = [
    &["rust search rust fast", "vector search rust"],
    &["search engines love rust", "vector vector vector"],
    &["rust", "nothing relevant here"],
];

const OFFSETS: [u64; 3] = [0, 2, 4];

async fn add_documents(addr: &str, texts: &[&str]) {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(8);
    for text in texts {
        tx.send(AddDocumentsRequest {
            materialize: None,
            map_numerics: Vec::new(),
            map_facets: Vec::new(),
            numerics: Vec::new(),
            facets: Vec::new(),
            text: text.to_string(),
            analysis: None,
            lineage: None,
            fields: Vec::new(),
            integers: Vec::new(),
            timestamps: Vec::new(),
            geo_points: Vec::new(),
            quality: None,
            geography: None,
        })
        .await
        .unwrap();
    }
    drop(tx);
    client
        .add_documents(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
}

async fn start_doc_shards(
    analysis: &str,
) -> (
    Vec<String>,
    Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
) {
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for (i, docs) in SHARD_DOCS.iter().enumerate() {
        let (addr, handle) = start_empty_node(NodeConfig {
            slot_offset: OFFSETS[i],
            analysis_addr: Some(analysis.to_string()),
            ..Default::default()
        })
        .await;
        add_documents(&addr, docs).await;
        addrs.push(addr);
        handles.push(handle);
    }
    (addrs, handles)
}

fn hit_signature(hits: &[Bm25Hit]) -> Vec<(u64, u32)> {
    hits.iter().map(|h| (h.doc_id, h.score.to_bits())).collect()
}

/// The epoch is present, stable across reads, and advances on ingest.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn term_stats_reports_an_epoch_that_ingest_advances() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addr, node) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis),
        ..Default::default()
    })
    .await;
    add_documents(&addr, SHARD_DOCS[0]).await;

    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    let stats = |c: &mut NodeServiceClient<tonic::transport::Channel>| {
        let mut c = c.clone();
        async move {
            c.term_stats(TermStatsRequest {
                terms: vec!["rust".into()],
                fields: Vec::new(),
            })
            .await
            .unwrap()
            .into_inner()
        }
    };
    let first = stats(&mut client).await;
    assert!(first.stats_epoch >= 1, "0 is reserved for no-claim");
    let second = stats(&mut client).await;
    assert_eq!(
        first.stats_epoch, second.stats_epoch,
        "reading stats is not a mutation"
    );

    add_documents(&addr, &["rust one more"]).await;
    let third = stats(&mut client).await;
    assert!(
        third.stats_epoch > first.stats_epoch,
        "ingest must advance the epoch ({} -> {})",
        first.stats_epoch,
        third.stats_epoch
    );

    node.abort();
    mock.abort();
}

/// The claim contract on the scoring RPC: the current epoch and 0 are
/// accepted, anything else is refused loudly naming both epochs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bm25_query_enforces_the_stats_epoch_claim() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addr, node) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis),
        ..Default::default()
    })
    .await;
    add_documents(&addr, SHARD_DOCS[0]).await;

    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    let stats = client
        .term_stats(TermStatsRequest {
            terms: vec!["rust".into()],
            fields: Vec::new(),
        })
        .await
        .unwrap()
        .into_inner();
    let request = |claim: u64| Bm25QueryRequest {
        projections: Vec::new(),
        filter: None,
        map_facet_fields: Vec::new(),
        score_stages: Vec::new(),
        facet_fields: Vec::new(),
        terms: vec!["rust".into()],
        k: 10,
        global_doc_count: stats.doc_count,
        global_total_doc_length: stats.total_doc_length,
        global_doc_frequencies: stats.doc_frequencies.clone(),
        k1: 0.0,
        b: 0.0,
        min_score: 0.0,
        fields: Vec::new(),
        expected_stats_epoch: claim,
        range_facet_fields: Vec::new(),
        geo_filters: Vec::new(),
        stats_fields: Vec::new(),
        cardinality_fields: Vec::new(),
    };

    let with_claim = client
        .bm25_query(request(stats.stats_epoch))
        .await
        .unwrap()
        .into_inner()
        .hits;
    let no_claim = client
        .bm25_query(request(0))
        .await
        .unwrap()
        .into_inner()
        .hits;
    assert_eq!(hit_signature(&with_claim), hit_signature(&no_claim));
    assert_eq!(with_claim.len(), 2);

    let refused = client
        .bm25_query(request(stats.stats_epoch + 7))
        .await
        .unwrap_err();
    assert_eq!(refused.code(), tonic::Code::FailedPrecondition);
    assert!(
        refused.message().starts_with("stale stats epoch"),
        "refusal must be recognizable by its prefix, got: {}",
        refused.message()
    );
    assert!(
        refused.message().contains(&stats.stats_epoch.to_string()),
        "refusal must name the shard's actual epoch: {}",
        refused.message()
    );

    node.abort();
    mock.abort();
}

/// The hit path: a repeated query issues NO further TermStats RPCs and
/// returns bitwise-identical hits; a query introducing a new term
/// fetches again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_query_reuses_cached_stats() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addrs, handles) = start_doc_shards(&analysis).await;
    let coordinator = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(Some(analysis.clone()), Default::default());

    let first = coordinator
        .fanout_bm25("search rust", 6, None)
        .await
        .unwrap();
    let after_first = coordinator.stats_cache().fetch_count();
    assert_eq!(after_first, 3, "one TermStats fetch per node");

    let second = coordinator
        .fanout_bm25("search rust", 6, None)
        .await
        .unwrap();
    assert_eq!(hit_signature(&first), hit_signature(&second));
    assert_eq!(
        coordinator.stats_cache().fetch_count(),
        after_first,
        "the repeat must be served from the cache"
    );

    // A subset of cached terms is still a hit.
    coordinator.fanout_bm25("rust", 6, None).await.unwrap();
    assert_eq!(coordinator.stats_cache().fetch_count(), after_first);

    // A new term is a miss on every node.
    coordinator.fanout_bm25("vector", 6, None).await.unwrap();
    assert_eq!(coordinator.stats_cache().fetch_count(), after_first + 3);

    for h in handles {
        h.abort();
    }
    mock.abort();
}

/// The invalidation path, end to end: ingest between queries advances
/// one shard's epoch, the stale claim is refused, the coordinator
/// refetches, and the answer matches a coordinator that never cached —
/// bitwise. The cache may cost a round trip here; it may never cost
/// correctness.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ingest_between_queries_is_never_scored_with_stale_stats() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addrs, handles) = start_doc_shards(&analysis).await;
    let cached = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(Some(analysis.clone()), Default::default());

    cached.fanout_bm25("search rust", 6, None).await.unwrap();
    let warm = cached.stats_cache().fetch_count();
    assert_eq!(warm, 3);

    // df("rust"), lengths, and N all change under the warm cache.
    add_documents(&addrs[0], &["rust rust rust arrives late"]).await;

    let got = cached.fanout_bm25("search rust", 6, None).await.unwrap();
    let reference = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(Some(analysis.clone()), Default::default());
    let want = reference.fanout_bm25("search rust", 6, None).await.unwrap();
    assert_eq!(
        hit_signature(&got),
        hit_signature(&want),
        "post-ingest results must match a never-cached coordinator exactly"
    );
    assert_eq!(
        cached.stats_cache().fetch_count(),
        warm + 3,
        "the refusal must trigger one fresh fetch from every node"
    );

    // The fresh shares were re-cached: the next repeat hits again.
    cached.fanout_bm25("search rust", 6, None).await.unwrap();
    assert_eq!(cached.stats_cache().fetch_count(), warm + 3);

    for h in handles {
        h.abort();
    }
    mock.abort();
}

/// The fused (multi-field) channel caches independently of the body
/// channel and behaves the same way: repeats are free and identical.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fused_repeated_query_reuses_cached_stats() {
    let (analysis, mock) = start_mock_analysis().await;
    let corpus: [&[(&str, &str)]; 2] = [
        &[
            ("rust search engine internals", "Rust v. Search"),
            ("vector quantization at scale", "Vector Corp"),
        ],
        &[
            ("search ranking with bm25", "Ranking Bros"),
            ("rust rust rust", "Rust Fanatics"),
        ],
    ];
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for (i, docs) in corpus.iter().enumerate() {
        let (addr, handle) = start_empty_node(NodeConfig {
            slot_offset: i as u64 * 2,
            analysis_addr: Some(analysis.to_string()),
            bm25_fields: vec!["body".to_string(), "case_name".to_string()],
            ..Default::default()
        })
        .await;
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        let (tx, rx) = mpsc::channel(8);
        for (body, name) in *docs {
            tx.send(AddDocumentsRequest {
                materialize: None,
                map_numerics: Vec::new(),
                map_facets: Vec::new(),
                numerics: Vec::new(),
                facets: Vec::new(),
                text: body.to_string(),
                analysis: None,
                lineage: None,
                fields: vec![DocumentField {
                    field: "case_name".to_string(),
                    text: name.to_string(),
                    analysis: None,
                }],
                integers: Vec::new(),
                timestamps: Vec::new(),
                geo_points: Vec::new(),
                quality: None,
                geography: None,
            })
            .await
            .unwrap();
        }
        drop(tx);
        client.add_documents(ReceiverStream::new(rx)).await.unwrap();
        addrs.push(addr);
        handles.push(handle);
    }
    let coordinator = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(Some(analysis.clone()), Default::default());
    let fields = vec![
        QueryField {
            field: "body".to_string(),
            analysis: None,
            weight: 1.0,
            k1: 0.0,
            b: 0.0,
        },
        QueryField {
            field: "case_name".to_string(),
            analysis: None,
            weight: 2.0,
            k1: 0.0,
            b: 0.0,
        },
    ];

    let first = coordinator
        .fanout_bm25_fused("rust search", 4, &fields, 0.0)
        .await
        .unwrap();
    let after_first = coordinator.stats_cache().fetch_count();
    assert_eq!(after_first, 2, "one TermStats fetch per node");

    let second = coordinator
        .fanout_bm25_fused("rust search", 4, &fields, 0.0)
        .await
        .unwrap();
    assert_eq!(hit_signature(&first), hit_signature(&second));
    assert_eq!(
        coordinator.stats_cache().fetch_count(),
        after_first,
        "the fused repeat must be served from the cache"
    );

    for h in handles {
        h.abort();
    }
    mock.abort();
}
