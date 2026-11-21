//! Shared HNSW lifecycle shard, corpus, and query verification fixtures.

use super::*;

pub(crate) const DIM: usize = 24;
pub(crate) const SEED: u64 = 0x11FE_C7C1_E000_0001;
pub(crate) const K: usize = 10;

pub(crate) fn config(index_path: &Path, mmap: bool, wal: bool) -> NodeConfig {
    NodeConfig {
        index_path: Some(index_path.to_path_buf()),
        vector_backend: HNSW.to_string(),
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        vector_mmap: mmap,
        wal,
        wal_buckets: 8,
        ..Default::default()
    }
}

/// One product row: unique text (hits map across renumbering by text) and
/// a unit vector seeded by the row number.
#[derive(Clone, Debug)]
pub(crate) struct Row {
    pub(crate) text: String,
    pub(crate) vector: Vec<f32>,
}

pub(crate) fn row(i: usize, revised: bool) -> Row {
    Row {
        text: if revised {
            format!("row{i} appeal lifecycle revised")
        } else {
            format!("row{i} appeal lifecycle")
        },
        vector: unit_vectors(1, DIM, SEED + i as u64 * 7919 + revised as u64),
    }
}

pub(crate) fn rows(range: std::ops::Range<usize>) -> Vec<Row> {
    range.map(|i| row(i, false)).collect()
}

pub(crate) async fn client(addr: &str) -> NodeServiceClient<Channel> {
    NodeServiceClient::connect(addr.to_string()).await.unwrap()
}

/// Append documents then their vectors (positions align); the first global
/// id the batch received.
pub(crate) async fn ingest_rows(addr: &str, rows: &[Row]) -> u64 {
    let requests: Vec<AddDocumentsRequest> = rows
        .iter()
        .map(|row| AddDocumentsRequest {
            text: row.text.clone(),
            analysis: Some(body_spec()),
            ..Default::default()
        })
        .collect();
    let documents = ingest(addr, requests).await.unwrap();
    assert_eq!(documents.added as usize, rows.len());
    let mut client = client(addr).await;
    let vectors: Vec<f32> = rows.iter().flat_map(|r| r.vector.iter().copied()).collect();
    let added = client
        .add_vectors(tokio_stream::iter(vec![AddVectorsRequest {
            materialized_rows: Vec::new(),
            vectors,
            dim: DIM as u32,
        }]))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(added.added as usize, rows.len());
    assert_eq!(
        documents.first_id, added.first_id,
        "document and vector legs must land at one id"
    );
    added.first_id
}

/// Lock the shard's hnsw configuration before any row is logged, the way
/// a deployment configures a provider up front: a WAL manifest completes
/// only until its first record, and a compaction replays from it.
pub(crate) async fn configure(addr: &str) {
    let config = HnswConfig::default().backend_config().unwrap();
    client(addr)
        .await
        .configure_vector_backend(ConfigureVectorBackendRequest {
            dim: DIM as u32,
            config: Some(VectorBackendConfig {
                backend_kind: config.backend_kind,
                config_format: config.config_format,
                payload: config.payload,
            }),
        })
        .await
        .unwrap();
}

pub(crate) async fn flush(addr: &str) {
    client(addr).await.flush(FlushRequest {}).await.unwrap();
}

pub(crate) fn coordinator(addr: &str) -> CoordinatorServiceImpl {
    CoordinatorServiceImpl::new(vec![addr.to_string()]).with_bm25(
        Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        Default::default(),
    )
}

pub(crate) fn speed_dense_leaf(vector: &[f32]) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: "vec".into(),
            query: Some(search_query::Query::Dense(DenseQuery {
                vector: vector.to_vec(),
                execution_mode: DenseExecutionMode::Balanced as i32,
                filter_strategy: DenseFilterStrategy::Speed as i32,
                ..Default::default()
            })),
        })),
    }
}

/// Explicit ANN through the coordinator's query planner: `(global id,
/// score)` best first, plus `tail_enumerated_rows`.
pub(crate) async fn ann_with_tail_stats(
    coordinator: &CoordinatorServiceImpl,
    vector: &[f32],
    k: usize,
) -> (Vec<(u64, f32)>, u64) {
    let response = coordinator
        .query(Request::new(QueryRequest {
            k: k as u32,
            selection: Some(speed_dense_leaf(vector)),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let outcome = response.dense_execution.unwrap();
    assert_eq!(outcome.provider_backend, HNSW);
    assert_eq!(outcome.resolved_mode, DenseExecutionMode::Balanced as i32);
    (
        response.hits.iter().map(|h| (h.doc_id, h.score)).collect(),
        outcome.tail_enumerated_rows,
    )
}

pub(crate) async fn ann(
    coordinator: &CoordinatorServiceImpl,
    vector: &[f32],
    k: usize,
) -> Vec<(u64, f32)> {
    ann_with_tail_stats(coordinator, vector, k).await.0
}

/// The streaming scan through the coordinator: `(global id, score)`.
pub(crate) async fn streamed(
    coordinator: &CoordinatorServiceImpl,
    tag: &str,
    vector: &[f32],
    k: usize,
) -> Vec<(u64, f32)> {
    let result = coordinator
        .fanout_stream_search(tag, vector, k as u32, None, &Default::default())
        .await
        .unwrap();
    result
        .hits
        .iter()
        .map(|h: &ScoredHit| (h.vector_id, h.score))
        .collect()
}

/// The exact FP32 top `k` over the live rows.
pub(crate) fn truth(live: &BTreeMap<u64, Row>, query: &[f32], k: usize) -> Vec<(u64, f32)> {
    let mut all: Vec<(u64, f32)> = live
        .iter()
        .map(|(id, row)| (*id, dot(&row.vector, query)))
        .collect();
    all.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(&b.0)));
    all.truncate(k);
    all
}

/// Every hit is a live row scored bit for bit by the scalar scorer, the
/// list is ordered score descending then id ascending, and it recalls the
/// exact top `k` at least `min_recall` of the time.
pub(crate) fn check(
    hits: &[(u64, f32)],
    live: &BTreeMap<u64, Row>,
    query: &[f32],
    min_recall: usize,
    what: &str,
) {
    assert_eq!(hits.len(), K, "{what}: {hits:?}");
    for (id, score) in hits {
        let row = live
            .get(id)
            .unwrap_or_else(|| panic!("{what}: row {id} is not live"));
        assert_eq!(
            score.to_bits(),
            dot(&row.vector, query).to_bits(),
            "{what}: row {id} score is not the scalar product"
        );
    }
    for pair in hits.windows(2) {
        let ((a, sa), (b, sb)) = (pair[0], pair[1]);
        assert!(sa > sb || (sa == sb && a < b), "{what}: order {hits:?}");
    }
    let exact = truth(live, query, K);
    let found = hits
        .iter()
        .filter(|(id, _)| exact.iter().any(|(t, _)| t == id))
        .count();
    assert!(found >= min_recall, "{what}: recall {found}/{K}");
}

pub(crate) fn queries(n: usize, seed: u64) -> Vec<Vec<f32>> {
    (0..n)
        .map(|q| unit_vectors(1, DIM, seed + q as u64))
        .collect()
}

pub(crate) async fn health(addr: &str) -> pipestream_search::pb::HealthResponse {
    client(addr)
        .await
        .health(HealthRequest {})
        .await
        .unwrap()
        .into_inner()
}
