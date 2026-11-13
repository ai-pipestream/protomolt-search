//! Lifecycle of the `hnsw` provider on real nodes (#70, contract 4): a
//! segmented shard ingests, seals its tail into an hnsw image, reopens it
//! through a memory map and answers unary and streaming dense queries from
//! it; deleted rows and replaced revisions never come back; a snapshot of an
//! hnsw image installs and survives a restart; compaction rebuilds the graph
//! over the live rows; and a publication interrupted between its write and
//! its publish rolls back at open with the old generation still serving.

mod common;

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::time::Duration;

use common::{monolithic_topk, start_empty_node, start_opened_node, unit_vectors};
use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::exact_vectors::{dot, ExactVectorStore};
use pipestream_search::hnsw::{HnswConfig, HNSW};
use pipestream_search::node::{segments_root, Layout, NodeConfig};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    search_query, search_shard_request, search_shard_response, selection_query,
    AddDocumentsRequest, AddVectorsRequest, CommitReplacementsRequest, CompactShardRequest,
    ConfigureVectorBackendRequest, DeleteDocumentsRequest, DenseExecutionMode, DenseQuery,
    FlushRequest, GetDocumentsRequest, HealthRequest, QueryRequest, Replacement, ScoredHit,
    SearchQuery, SearchShardRequest, SelectionQuery, StartShardSearch, VectorBackendConfig,
};
use pipestream_search::postings::{AnalyzedDoc, Bm25Store, DocTerms};
use pipestream_search::segments::{OpenedSegmentSet, SegmentCatalog, VectorLoad};
use pipestream_search::vector::{VectorIndex, VectorProvider};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::{Code, Request};

const DIM: usize = 24;
const SEED: u64 = 0x11FE_C7C1_E000_0001;
const K: usize = 10;

fn tempdir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("hnsw-lifecycle-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn config(index_path: &Path, mmap: bool, wal: bool) -> NodeConfig {
    NodeConfig {
        index_path: Some(index_path.to_path_buf()),
        vector_backend: HNSW.to_string(),
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        layout: Layout::Segments,
        vector_mmap: mmap,
        wal,
        wal_buckets: 8,
        ..Default::default()
    }
}

/// One product row: unique text (hits map across renumbering by text) and
/// a unit vector seeded by the row number.
#[derive(Clone, Debug)]
struct Row {
    text: String,
    vector: Vec<f32>,
}

fn row(i: usize, revised: bool) -> Row {
    Row {
        text: if revised {
            format!("row{i} appeal lifecycle revised")
        } else {
            format!("row{i} appeal lifecycle")
        },
        vector: unit_vectors(1, DIM, SEED + i as u64 * 7919 + revised as u64),
    }
}

fn rows(range: std::ops::Range<usize>) -> Vec<Row> {
    range.map(|i| row(i, false)).collect()
}

async fn client(addr: &str) -> NodeServiceClient<Channel> {
    NodeServiceClient::connect(addr.to_string()).await.unwrap()
}

/// Append documents then their vectors (positions align); the first global
/// id the batch received.
async fn ingest(addr: &str, rows: &[Row]) -> u64 {
    let mut client = client(addr).await;
    let requests: Vec<AddDocumentsRequest> = rows
        .iter()
        .map(|row| AddDocumentsRequest {
            text: row.text.clone(),
            analysis: Some(body_spec()),
            ..Default::default()
        })
        .collect();
    let documents = client
        .add_documents(tokio_stream::iter(requests))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(documents.added as usize, rows.len());
    let vectors: Vec<f32> = rows.iter().flat_map(|r| r.vector.iter().copied()).collect();
    let added = client
        .add_vectors(tokio_stream::iter(vec![AddVectorsRequest {
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
async fn configure(addr: &str) {
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

async fn flush(addr: &str) {
    client(addr).await.flush(FlushRequest {}).await.unwrap();
}

fn coordinator(addr: &str) -> CoordinatorServiceImpl {
    CoordinatorServiceImpl::new(vec![addr.to_string()]).with_bm25(
        Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        Default::default(),
    )
}

/// Explicit ANN through the coordinator's query planner: `(global id,
/// score)` best first, plus `tail_enumerated_rows`.
async fn ann_with_tail_stats(
    coordinator: &CoordinatorServiceImpl,
    vector: &[f32],
    k: usize,
) -> (Vec<(u64, f32)>, u64) {
    let response = coordinator
        .query(Request::new(QueryRequest {
            k: k as u32,
            selection: Some(SelectionQuery {
                node: Some(selection_query::Node::Search(SearchQuery {
                    id: "vec".into(),
                    query: Some(search_query::Query::Dense(DenseQuery {
                        vector: vector.to_vec(),
                        execution_mode: DenseExecutionMode::Ann as i32,
                        ..Default::default()
                    })),
                })),
            }),
            ..Default::default()
        }))
        .await
        .unwrap()
        .into_inner();
    let outcome = response.dense_execution.unwrap();
    assert_eq!(outcome.provider_backend, HNSW);
    assert_eq!(outcome.resolved_mode, DenseExecutionMode::Ann as i32);
    (
        response.hits.iter().map(|h| (h.doc_id, h.score)).collect(),
        outcome.tail_enumerated_rows,
    )
}

async fn ann(coordinator: &CoordinatorServiceImpl, vector: &[f32], k: usize) -> Vec<(u64, f32)> {
    ann_with_tail_stats(coordinator, vector, k).await.0
}

/// The streaming scan through the coordinator: `(global id, score)`.
async fn streamed(
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
fn truth(live: &BTreeMap<u64, Row>, query: &[f32], k: usize) -> Vec<(u64, f32)> {
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
fn check(
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

fn queries(n: usize, seed: u64) -> Vec<Vec<f32>> {
    (0..n)
        .map(|q| unit_vectors(1, DIM, seed + q as u64))
        .collect()
}

async fn health(addr: &str) -> pipestream_search::pb::HealthResponse {
    client(addr)
        .await
        .health(HealthRequest {})
        .await
        .unwrap()
        .into_inner()
}

/// Ingest, seal, delete, reopen mapped: two sealed hnsw segments plus a
/// tail answer explicit ANN and the streaming scan with exact scores over
/// live rows only. Sealed, the catalog answers identically after a reopen
/// through the map and through the heap (the images are the graphs; an
/// unsealed tail is volatile without a log, as on every provider).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sealed_segments_serve_mapped_and_exclude_deleted_rows() {
    let dir = tempdir("sealed");
    let path = dir.join("shard.hnsw");
    let (addr, handle) = start_empty_node(config(&path, true, false)).await;
    let mut live: BTreeMap<u64, Row> = BTreeMap::new();
    for (batch, range) in [0..300, 300..600, 600..700].into_iter().enumerate() {
        let batch_rows = rows(range.clone());
        let first = ingest(&addr, &batch_rows).await;
        assert_eq!(first, range.start as u64);
        for (i, row) in batch_rows.into_iter().enumerate() {
            live.insert(first + i as u64, row);
        }
        if batch < 2 {
            flush(&addr).await;
        }
    }
    // Deletes land in both sealed segments and in the tail.
    let doomed: Vec<u64> = (0..700u64).filter(|id| id % 9 == 4).collect();
    let deleted = client(&addr)
        .await
        .delete_documents(DeleteDocumentsRequest {
            doc_ids: doomed.clone(),
            expected_wal_generation: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(deleted.deleted as usize, doomed.len());
    for id in &doomed {
        live.remove(id);
    }
    let live_tail_rows = (600..700u64).filter(|id| live.contains_key(id)).count() as u64;
    assert!(live_tail_rows > 0);
    let probes = queries(6, 0x51A1_0000);
    let c = coordinator(&addr);
    for (q, query) in probes.iter().enumerate() {
        let (hits, tail_enum) = ann_with_tail_stats(&c, query, K).await;
        assert_eq!(
            tail_enum, live_tail_rows,
            "live tail rows (600..700) must be enumerated until sealed"
        );
        check(&hits, &live, query, 8, &format!("live query {q}"));
        let stream = streamed(&c, &format!("live-{q}"), query, K).await;
        check(&stream, &live, query, 8, &format!("live stream {q}"));
    }
    // Seal the 100-row tail too: because 100 < MIN_GRAPH_ROWS (256), segment 2
    // persists as a compact unlinked PMHNSW01 image (`entry = NO_NODE`) while
    // segments 0 and 1 (300 rows each) serve HNSW graphs, and reopen serves
    // those images as written.
    flush(&addr).await;
    let mut before = Vec::new();
    for (q, query) in probes.iter().enumerate() {
        let (unary, tail_enum) = ann_with_tail_stats(&c, query, K).await;
        assert_eq!(
            tail_enum, live_tail_rows,
            "tiny sealed segment (< MIN_GRAPH_ROWS) enumerates its live rows"
        );
        check(&unary, &live, query, 8, &format!("sealed query {q}"));
        let stream = streamed(&c, &format!("sealed-{q}"), query, K).await;
        check(&stream, &live, query, 8, &format!("sealed stream {q}"));
        before.push((unary, stream));
    }
    handle.abort();

    let root = segments_root(&path);
    let mapped = OpenedSegmentSet::open_with(&root, VectorLoad::Mapped).unwrap();
    assert_eq!(mapped.len(), 3);
    for i in 0..3 {
        let vector = mapped.vector(i).unwrap();
        assert!(vector.is_mapped());
        assert_eq!(vector.descriptor().backend_kind, HNSW);
        assert_eq!(mapped.metadata(i).backend_kind, HNSW);
        assert_eq!(vector.is_graph_linked(), i < 2, "segment {i} linked state");
    }
    let heap = OpenedSegmentSet::open_with(&root, VectorLoad::Heap).unwrap();
    assert!(!heap.vector(0).unwrap().is_mapped());
    assert!(heap.vector(0).unwrap().is_graph_linked());
    assert!(heap.vector(1).unwrap().is_graph_linked());
    assert!(!heap.vector(2).unwrap().is_graph_linked());

    // Reopened either way, every answer is the one given before.
    for mmap in [true, false] {
        let (addr, handle) = start_opened_node(config(&path, mmap, false)).await;
        let c = coordinator(&addr);
        for (q, query) in probes.iter().enumerate() {
            let unary = ann(&c, query, K).await;
            let stream = streamed(&c, &format!("reopen-{mmap}-{q}"), query, K).await;
            let bits = |hits: &[(u64, f32)]| -> Vec<(u64, u32)> {
                hits.iter().map(|(id, s)| (*id, s.to_bits())).collect()
            };
            check(&unary, &live, query, 8, &format!("reopen {mmap} query {q}"));
            assert_eq!(bits(&unary), bits(&before[q].0), "mmap {mmap} query {q}");
            assert_eq!(bits(&stream), bits(&before[q].1), "mmap {mmap} stream {q}");
        }
        handle.abort();
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// A document replacement retires the old row from every dense answer and
/// serves the revision, in the tail and after the seal that follows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn replacements_retire_the_old_row_and_serve_the_revision() {
    let dir = tempdir("replace");
    let path = dir.join("shard.hnsw");
    let (addr, handle) = start_empty_node(config(&path, true, false)).await;
    let base = rows(0..240);
    assert_eq!(ingest(&addr, &base).await, 0);
    flush(&addr).await;
    let mut live: BTreeMap<u64, Row> = base
        .iter()
        .cloned()
        .enumerate()
        .map(|(i, r)| (i as u64, r))
        .collect();

    let replaced = [7usize, 100, 239];
    let revisions: Vec<Row> = replaced.iter().map(|&i| row(i, true)).collect();
    let first_new = ingest(&addr, &revisions).await;
    assert_eq!(first_new, 240);
    let committed = client(&addr)
        .await
        .commit_replacements(CommitReplacementsRequest {
            replacements: replaced
                .iter()
                .enumerate()
                .map(|(n, &old)| Replacement {
                    old_doc_id: old as u64,
                    new_doc_id: first_new + n as u64,
                })
                .collect(),
            expected_wal_generation: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(committed.committed as usize, replaced.len());
    for (n, &old) in replaced.iter().enumerate() {
        live.remove(&(old as u64));
        live.insert(first_new + n as u64, revisions[n].clone());
    }

    // The old row's own vector no longer finds the old row; the revision's
    // finds the revision first.
    let c = coordinator(&addr);
    for (n, &old) in replaced.iter().enumerate() {
        let hits = ann(&c, &base[old].vector, K).await;
        check(
            &hits,
            &live,
            &base[old].vector,
            8,
            &format!("old row {old}"),
        );
        assert!(hits.iter().all(|(id, _)| *id != old as u64));
        let hits = ann(&c, &revisions[n].vector, K).await;
        assert_eq!(hits[0].0, first_new + n as u64, "revision {n} leads");
        let stream = streamed(&c, &format!("rev-{n}"), &revisions[n].vector, K).await;
        assert_eq!(stream[0].0, first_new + n as u64);
        check(
            &stream,
            &live,
            &revisions[n].vector,
            8,
            &format!("revision stream {n}"),
        );
    }
    // Sealed and reopened, the same holds.
    flush(&addr).await;
    handle.abort();
    let (addr, handle) = start_opened_node(config(&path, true, false)).await;
    assert_eq!(
        OpenedSegmentSet::open(segments_root(&path)).unwrap().len(),
        2
    );
    let c = coordinator(&addr);
    for (n, &old) in replaced.iter().enumerate() {
        let hits = ann(&c, &base[old].vector, K).await;
        check(
            &hits,
            &live,
            &base[old].vector,
            8,
            &format!("sealed old row {old}"),
        );
        assert!(hits.iter().all(|(id, _)| *id != old as u64));
        let hits = ann(&c, &revisions[n].vector, K).await;
        assert_eq!(hits[0].0, first_new + n as u64);
    }
    handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A one-document BM25 image containing the given text (as the snapshot
/// tests build it).
fn build_bm25(dir: &Path, text: &str) -> PathBuf {
    let mut store = Bm25Store::new();
    let mut terms: DocTerms = Vec::new();
    let mut offset = 0u32;
    for token in text.split_whitespace() {
        let start = text[offset as usize..].find(token).unwrap() as u32 + offset;
        terms.push((
            token.to_string(),
            1,
            vec![(start, start + token.len() as u32)],
        ));
        offset = start + token.len() as u32;
    }
    let length = terms.iter().map(|(_, f, _)| f).sum();
    store.add_document_with_lineage(0, text.to_string(), AnalyzedDoc::body(terms, length), None);
    store
        .source_archive_mut()
        .attach_source(0, &common::protobuf_source(text, "snapshot"), None)
        .unwrap();
    let path = dir.join("source.bm25");
    store.save(&path).unwrap();
    path
}

/// The node's own shard search (the coordinator's unary leg).
async fn search_shard(
    client: &mut NodeServiceClient<Channel>,
    vector: Vec<f32>,
    k: u32,
) -> Vec<ScoredHit> {
    let (tx, rx) = mpsc::channel(8);
    tx.send(SearchShardRequest {
        payload: Some(search_shard_request::Payload::Start(StartShardSearch {
            request_id: "lifecycle".into(),
            k,
            vector,
            ..Default::default()
        })),
    })
    .await
    .unwrap();
    drop(tx);
    let mut stream = client
        .search_shard(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    let mut done = None;
    while let Some(msg) = stream.message().await.unwrap() {
        if let Some(search_shard_response::Payload::Done(d)) = msg.payload {
            done = Some(d);
        }
    }
    done.expect("stream must end with SearchShardDone").hits
}

/// Each probe's shard top-k as `(global id, score bits)`.
async fn observe_shard(addr: &str, probes: &[Vec<f32>]) -> Vec<Vec<(u64, u32)>> {
    let mut client = client(addr).await;
    let mut out = Vec::new();
    for q in probes {
        let hits = search_shard(&mut client, q.clone(), K as u32).await;
        out.push(
            hits.iter()
                .map(|h| (h.vector_id, h.score.to_bits()))
                .collect::<Vec<_>>(),
        );
    }
    out
}

/// An hnsw image built elsewhere installs as a snapshot with its exact
/// sidecar, serves the image's own top-k, and is what the shard reopens.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_snapshot_of_the_graph_installs_and_survives_a_restart() {
    let dir = tempdir("snapshot");
    let n = 500;
    let corpus = unit_vectors(n, DIM, 0x5A4B_0001);
    let mut reference = VectorIndex::create(HNSW, DIM, 32).unwrap();
    reference.add(&corpus, DIM).unwrap();
    reference.prepare().unwrap();
    let image = dir.join("source.hnsw");
    reference.write(&image).unwrap();
    let exact = dir.join("source.exact");
    ExactVectorStore::from_values(DIM, corpus.clone())
        .unwrap()
        .write(&exact)
        .unwrap();
    let bm25 = build_bm25(&dir, "graph snapshot install");

    let path = dir.join("shard.hnsw");
    let (addr, handle) = start_empty_node(NodeConfig {
        slot_offset: 100,
        ..config(&path, true, false)
    })
    .await;
    let report = pipestream_search::snapshot::install_snapshot_with_exact(
        &addr,
        &image,
        Some(&exact),
        Some(&bm25),
    )
    .await
    .unwrap();
    assert_eq!(report.num_vectors, n as u64);
    assert_eq!(report.num_documents, 1);

    let probes = queries(4, 0x5A4B_1000);
    let expected: Vec<Vec<(u64, u32)>> = probes
        .iter()
        .map(|q| monolithic_topk(&reference, q, K))
        .collect();
    let installed = observe_shard(&addr, &probes).await;
    for (q, (got, want)) in installed.iter().zip(&expected).enumerate() {
        let want: Vec<(u64, u32)> = want.iter().map(|(id, bits)| (100 + id, *bits)).collect();
        assert_eq!(got, &want, "query {q}");
    }
    let health = health(&addr).await;
    assert_eq!(health.vector_backend, HNSW);
    assert_eq!(health.exact_vector_rows, n as u64);
    handle.abort();

    let (addr, handle) = start_opened_node(NodeConfig {
        slot_offset: 100,
        ..config(&path, true, false)
    })
    .await;
    assert_eq!(observe_shard(&addr, &probes).await, installed);
    handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Hits as `(text, score bits)`, so answers compare across the renumbering
/// a compaction performs.
async fn dense_by_text(addr: &str, probes: &[Vec<f32>]) -> Vec<Vec<(String, u32)>> {
    let c = coordinator(addr);
    let mut client = client(addr).await;
    let mut out = Vec::new();
    for query in probes {
        let hits = ann(&c, query, K).await;
        let documents = client
            .get_documents(GetDocumentsRequest {
                doc_ids: hits.iter().map(|h| h.0).collect(),
            })
            .await
            .unwrap()
            .into_inner()
            .documents;
        let texts: BTreeMap<u64, String> =
            documents.into_iter().map(|d| (d.doc_id, d.text)).collect();
        out.push(
            hits.iter()
                .map(|(id, score)| (texts[id].clone(), score.to_bits()))
                .collect(),
        );
    }
    out
}

/// A by-text answer holds live rows only, scored exactly, in score order,
/// and reaches `min_recall` of the exact top-K.
fn check_texts(
    hits: &[(String, u32)],
    live: &BTreeMap<u64, Row>,
    query: &[f32],
    min_recall: usize,
    what: &str,
) {
    let by_text: BTreeMap<&str, (u64, &Row)> = live
        .iter()
        .map(|(id, row)| (row.text.as_str(), (*id, row)))
        .collect();
    let mut ids = Vec::with_capacity(hits.len());
    for (text, bits) in hits {
        let (id, row) = by_text
            .get(text.as_str())
            .unwrap_or_else(|| panic!("{what}: {text:?} is not a live row"));
        assert_eq!(
            *bits,
            dot(&row.vector, query).to_bits(),
            "{what}: {text} score"
        );
        ids.push(*id);
    }
    let scores: Vec<f32> = hits.iter().map(|(_, bits)| f32::from_bits(*bits)).collect();
    assert!(scores.windows(2).all(|w| w[0] >= w[1]), "{what}: order");
    let truth: Vec<u64> = truth(live, query, K)
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    let found = ids.iter().filter(|id| truth.contains(id)).count();
    assert!(
        found >= min_recall,
        "{what}: {found} of {K} exact neighbours"
    );
}

/// Compaction of a segmented hnsw shard rebuilds the graph over the live
/// rows (tombstones gone; the rebuilt graph answers exactly scored live
/// rows at the recall the graph gives, not the old graph's misses), and a
/// publication interrupted between the write of its outputs and the
/// publish of its manifest rolls back at open: the staged segment is
/// removed, the marker is gone, and the old generation serves the same
/// answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_rebuilds_the_graph_and_an_interrupted_publication_rolls_back() {
    let dir = tempdir("compact");
    let path = dir.join("shard.hnsw");
    let compact_cfg = || NodeConfig {
        wal_buckets: 2,
        ..config(&path, true, true)
    };
    let (addr, handle) = start_empty_node(compact_cfg()).await;
    configure(&addr).await;
    let mut live: BTreeMap<u64, Row> = BTreeMap::new();
    for range in [0..450, 450..900] {
        let batch = rows(range.clone());
        let first = ingest(&addr, &batch).await;
        for (i, row) in batch.into_iter().enumerate() {
            live.insert(first + i as u64, row);
        }
        flush(&addr).await;
    }
    let doomed: Vec<u64> = (0..900u64).filter(|id| id % 5 == 1).collect();
    let deleted = client(&addr)
        .await
        .delete_documents(DeleteDocumentsRequest {
            doc_ids: doomed.clone(),
            expected_wal_generation: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(deleted.deleted as usize, doomed.len());
    for id in &doomed {
        live.remove(id);
    }
    flush(&addr).await;
    assert!(health(&addr).await.deleted_docs > 0);

    let probes = queries(5, 0xC0A7_0000);
    let c = coordinator(&addr);
    for (q, query) in probes.iter().enumerate() {
        check(
            &ann(&c, query, K).await,
            &live,
            query,
            8,
            &format!("pre-compaction {q}"),
        );
    }

    let mut request = Request::new(CompactShardRequest::default());
    request.set_timeout(Duration::from_secs(600));
    let compacted = client(&addr)
        .await
        .compact_shard(request)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(compacted.rows_before, 900);
    assert_eq!(compacted.rows_after as usize, live.len());
    assert_eq!(compacted.tombstones_reclaimed as usize, doomed.len());
    let after_health = health(&addr).await;
    assert_eq!(after_health.deleted_docs, 0);
    assert_eq!(after_health.live_docs as usize, live.len());
    let (_, post_tail_enum) = ann_with_tail_stats(&c, &probes[0], K).await;
    assert_eq!(
        post_tail_enum, 0,
        "compacted segments >= MIN_GRAPH_ROWS must be linked"
    );
    let after = dense_by_text(&addr, &probes).await;
    for (q, query) in probes.iter().enumerate() {
        check_texts(&after[q], &live, query, 8, &format!("post-compaction {q}"));
    }
    handle.abort();

    // The compacted catalog reopened from disk serves the same answers from
    // mapped hnsw images.
    let root = segments_root(&path);
    let reopened = OpenedSegmentSet::open_with(&root, VectorLoad::Mapped).unwrap();
    assert!(!reopened.is_empty());
    for i in 0..reopened.len() {
        if let Some(vector) = reopened.vector(i) {
            assert!(vector.is_mapped());
            assert!(vector.is_graph_linked());
            assert_eq!(vector.descriptor().backend_kind, HNSW);
        }
    }
    let published_ids: Vec<String> = reopened
        .manifest()
        .segments
        .iter()
        .map(|s| s.segment_id.clone())
        .collect();
    drop(reopened);
    let (addr, handle) = start_opened_node(compact_cfg()).await;
    assert_eq!(dense_by_text(&addr, &probes).await, after);
    handle.abort();

    // A crash between writing a later compaction's outputs and publishing
    // its manifest: the manifest backup and the marker are on disk, a
    // staged segment directory sits beside the published ones, and the
    // manifest itself is untouched. Opening rolls the attempt back.
    let manifest_path = SegmentCatalog::manifest_path(&root);
    let mut backup = manifest_path.as_os_str().to_owned();
    backup.push(".pre-compact");
    std::fs::copy(&manifest_path, PathBuf::from(&backup)).unwrap();
    let staged = SegmentCatalog::segment_dir(&root, "cmp-interrupted");
    std::fs::create_dir_all(&staged).unwrap();
    std::fs::write(staged.join("vector.index"), b"half written").unwrap();
    let marker = pipestream_search::compaction::marker_path(&path);
    std::fs::write(
        &marker,
        serde_json::json!({
            "format": 1,
            "layout": "segments",
            "old_wal_generation": 0,
            "new_wal_generation": 0,
            "walless": true,
            "work_dir": dir.join("work"),
            "previous_snapshot": false,
            "legacy_files": [],
            "staged_segments": ["cmp-interrupted"],
            "replaced_segments": published_ids
        })
        .to_string(),
    )
    .unwrap();

    let (addr, handle) = start_opened_node(compact_cfg()).await;
    assert!(!marker.exists(), "the marker is consumed by the rollback");
    assert!(!staged.exists(), "the staged segment is removed");
    assert!(!PathBuf::from(&backup).exists());
    let serving = OpenedSegmentSet::open(&root).unwrap();
    let serving_ids: Vec<String> = serving
        .manifest()
        .segments
        .iter()
        .map(|s| s.segment_id.clone())
        .collect();
    assert_eq!(serving_ids, published_ids, "the old generation serves");
    assert_eq!(dense_by_text(&addr, &probes).await, after);
    handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Multi-segment HNSW compaction merges linked segments via donor transplant +
/// `#99` join-set merge under concurrent readers, and rebuilds lazy
/// `FacetRows` and `NumberRows` structures fresh against post-remap local doc
/// IDs so filtered queries never return stale pre-remap rows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn compaction_merges_segments_and_rebuilds_facet_and_number_rows_fresh() {
    use pipestream_search::hnsw::HnswIndex;
    use pipestream_search::pb::{
        filter_query, CompositeSearchStrategy, FacetValue, FilterQuery, IntegerValue, NumericValue,
        SelectionOperator,
    };
    use std::sync::atomic::{AtomicBool, Ordering as Atomic};
    use std::sync::Arc;

    #[derive(Clone)]
    struct TypedRow {
        text: String,
        court: &'static str,
        year: i64,
        pages: f64,
        vector: Vec<f32>,
    }

    let dir = tempdir("merge-columns");
    let path = dir.join("shard.hnsw");
    let node_cfg = || NodeConfig {
        wal_buckets: 1,
        facet_fields: vec!["court".into()],
        integer_fields: vec!["year".into()],
        numeric_fields: vec!["pages".into()],
        ..config(&path, true, true)
    };
    let (addr, handle) = start_empty_node(node_cfg()).await;
    configure(&addr).await;

    let courts = ["scotus", "ca9", "ca2"];
    let mut live: BTreeMap<u64, TypedRow> = BTreeMap::new();
    // 4 sealed segments of 320 rows each (>= MIN_GRAPH_ROWS) plus 60 unsealed tail rows.
    for (seg_idx, range) in [0..320usize, 320..640, 640..960, 960..1280, 1280..1340]
        .into_iter()
        .enumerate()
    {
        let mut docs = Vec::with_capacity(range.len());
        let mut vecs = Vec::with_capacity(range.len() * DIM);
        let mut batch_rows = Vec::with_capacity(range.len());
        for i in range.clone() {
            let court = courts[i % courts.len()];
            let year = 2000 + (i % 25) as i64;
            let pages = ((i * 7) % 100) as f64;
            let v = unit_vectors(1, DIM, SEED + 0x99_0000 + i as u64 * 131);
            let text = format!("opinion-{i} {court} y{year}");
            docs.push(AddDocumentsRequest {
                text: text.clone(),
                analysis: Some(body_spec()),
                facets: vec![FacetValue {
                    field: "court".into(),
                    value: court.into(),
                }],
                integers: vec![IntegerValue {
                    field: "year".into(),
                    value: year,
                }],
                numerics: vec![NumericValue {
                    field: "pages".into(),
                    value: pages,
                }],
                ..Default::default()
            });
            vecs.extend_from_slice(&v);
            batch_rows.push(TypedRow {
                text,
                court,
                year,
                pages,
                vector: v,
            });
        }
        let mut cli = client(&addr).await;
        let d_res = cli
            .add_documents(tokio_stream::iter(docs))
            .await
            .unwrap()
            .into_inner();
        let v_res = cli
            .add_vectors(tokio_stream::iter(vec![AddVectorsRequest {
                vectors: vecs,
                dim: DIM as u32,
            }]))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(d_res.first_id, v_res.first_id);
        for (off, r) in batch_rows.into_iter().enumerate() {
            live.insert(d_res.first_id + off as u64, r);
        }
        if seg_idx < 4 {
            flush(&addr).await;
        }
    }

    // Delete ~25% of rows across all segments + tail so every post-compaction ID shifts.
    let doomed: Vec<u64> = (0..1340u64).filter(|id| id % 4 == 1).collect();
    let del_resp = client(&addr)
        .await
        .delete_documents(DeleteDocumentsRequest {
            doc_ids: doomed.clone(),
            expected_wal_generation: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(del_resp.deleted as usize, doomed.len());
    for id in &doomed {
        live.remove(id);
    }

    let c = coordinator(&addr);
    let filtered_query = |q_vec: &[f32], cel_expr: &str| QueryRequest {
        k: K as u32,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Composite(CompositeSearchStrategy {
                operator: SelectionOperator::And as i32,
                clauses: vec![
                    SelectionQuery {
                        node: Some(selection_query::Node::Search(SearchQuery {
                            id: "vec".into(),
                            query: Some(search_query::Query::Dense(DenseQuery {
                                vector: q_vec.to_vec(),
                                execution_mode: DenseExecutionMode::Ann as i32,
                                ..Default::default()
                            })),
                        })),
                    },
                    SelectionQuery {
                        node: Some(selection_query::Node::Filter(FilterQuery {
                            id: "flt".into(),
                            predicate: Some(filter_query::Predicate::Cel(cel_expr.into())),
                        })),
                    },
                ],
                scoring: None,
            })),
        }),
        ..Default::default()
    };

    type RowPred = fn(&TypedRow) -> bool;
    let predicates: &[(&str, RowPred)] = &[
        ("court == 'ca9'", |r| r.court == "ca9"),
        (
            "year >= 2005 && year <= 2018 && pages >= 20.0 && pages <= 80.0",
            |r| (2005..=2018).contains(&r.year) && (20.0..=80.0).contains(&r.pages),
        ),
        ("court == 'scotus' && pages >= 30.0", |r| {
            r.court == "scotus" && r.pages >= 30.0
        }),
    ];

    let probes = queries(6, 0x99C0_AA00);
    // Warm lazy FacetRows and NumberRows caches before compaction and verify pre-compaction recall.
    let verify_filtered = |stage: &'static str| {
        let addr_cl = addr.clone();
        let live_ref = &live;
        let probes_ref = &probes;
        async move {
            let coord = coordinator(&addr_cl);
            let mut cli = client(&addr_cl).await;
            for (p_idx, &(cel_expr, pred)) in predicates.iter().enumerate() {
                for (q_idx, q_vec) in probes_ref.iter().enumerate() {
                    let resp = coord
                        .query(Request::new(filtered_query(q_vec, cel_expr)))
                        .await
                        .unwrap()
                        .into_inner();
                    let docs = cli
                        .get_documents(GetDocumentsRequest {
                            doc_ids: resp.hits.iter().map(|h| h.doc_id).collect(),
                        })
                        .await
                        .unwrap()
                        .into_inner()
                        .documents;
                    let text_by_id: BTreeMap<u64, String> =
                        docs.into_iter().map(|d| (d.doc_id, d.text)).collect();
                    let by_text: BTreeMap<&str, &TypedRow> =
                        live_ref.values().map(|r| (r.text.as_str(), r)).collect();

                    let mut exact: Vec<(&str, f32)> = live_ref
                        .values()
                        .filter(|r| pred(r))
                        .map(|r| (r.text.as_str(), dot(&r.vector, q_vec)))
                        .collect();
                    exact.sort_by(|a, b| b.1.total_cmp(&a.1).then(a.0.cmp(b.0)));
                    exact.truncate(K);

                    assert_eq!(
                        resp.hits.len(),
                        exact.len(),
                        "{stage} pred={p_idx} q={q_idx}"
                    );
                    let mut got_texts = Vec::with_capacity(resp.hits.len());
                    for hit in &resp.hits {
                        let txt = text_by_id
                            .get(&hit.doc_id)
                            .unwrap_or_else(|| panic!("{stage}: missing doc {}", hit.doc_id));
                        let row = by_text
                            .get(txt.as_str())
                            .unwrap_or_else(|| panic!("{stage}: returned deleted row {txt}"));
                        assert!(pred(row), "{stage}: row {txt} failed predicate {cel_expr}");
                        assert_eq!(
                            hit.score.to_bits(),
                            dot(&row.vector, q_vec).to_bits(),
                            "{stage}: exact score mismatch on {txt}"
                        );
                        got_texts.push(txt.as_str());
                    }
                    let recall_hits = exact.iter().filter(|(t, _)| got_texts.contains(t)).count();
                    assert!(
                        recall_hits >= exact.len().saturating_sub(2),
                        "{stage} pred={p_idx} q={q_idx}: recall {recall_hits}/{}",
                        exact.len()
                    );
                }
            }
        }
    };

    verify_filtered("pre-compact").await;

    // Concurrent background reader during CompactShard cutover.
    let stop = Arc::new(AtomicBool::new(false));
    let stop_reader = Arc::clone(&stop);
    let reader_addr = addr.clone();
    let reader_probe = probes[0].clone();
    let reader_task = tokio::spawn(async move {
        let coord = coordinator(&reader_addr);
        let mut iterations = 0usize;
        while !stop_reader.load(Atomic::Relaxed) {
            let q_res = coord
                .query(Request::new(QueryRequest {
                    k: K as u32,
                    selection: Some(SelectionQuery {
                        node: Some(selection_query::Node::Search(SearchQuery {
                            id: "vec".into(),
                            query: Some(search_query::Query::Dense(DenseQuery {
                                vector: reader_probe.clone(),
                                execution_mode: DenseExecutionMode::Ann as i32,
                                ..Default::default()
                            })),
                        })),
                    }),
                    ..Default::default()
                }))
                .await;
            match q_res {
                Ok(resp) => assert_eq!(resp.into_inner().hits.len(), K),
                // Two-phase Query rightly rejects cross-cutover slot reuse when
                // `stats_epoch` advances between scan and document fetch.
                Err(st)
                    if st.code() == Code::FailedPrecondition
                        && st.message().contains("query data changed during execution") =>
                {
                    continue;
                }
                Err(st) => panic!("unexpected concurrent Query error during compaction: {st:?}"),
            }
            let s_hits = streamed(&coord, "concurrent-compact", &reader_probe, K).await;
            assert_eq!(s_hits.len(), K);
            iterations += 1;
            tokio::task::yield_now().await;
        }
        iterations
    });

    let mut req = Request::new(CompactShardRequest::default());
    req.set_timeout(Duration::from_secs(600));
    let compacted = client(&addr)
        .await
        .compact_shard(req)
        .await
        .unwrap()
        .into_inner();
    stop.store(true, Atomic::Relaxed);
    let concurrent_reads = reader_task.await.unwrap();
    assert!(concurrent_reads > 0);
    assert_eq!(compacted.rows_before, 1340);
    assert_eq!(compacted.rows_after as usize, live.len());
    assert_eq!(compacted.tombstones_reclaimed as usize, doomed.len());

    // Post-compaction filtered queries must build FacetRows and NumberRows fresh
    // on the merged segment and return the exact live filtered rows.
    let _ = &c;
    verify_filtered("post-compact").await;

    handle.abort();
    let root = segments_root(&path);
    let reopened = OpenedSegmentSet::open_with(&root, VectorLoad::Mapped).unwrap();
    assert_eq!(
        reopened.len(),
        1,
        "4 segments + tail compacted into 1 segment"
    );
    let seg_meta = reopened.metadata(0);
    let seg_vec_path =
        SegmentCatalog::segment_dir(&root, &seg_meta.segment_id).join(&seg_meta.vector.file);
    let mapped_hnsw = HnswIndex::load_mapped(&seg_vec_path).unwrap();
    assert!(mapped_hnsw.is_graph_linked());
    assert_eq!(mapped_hnsw.unreachable_from_entry(), 0);

    let _ = std::fs::remove_dir_all(&dir);
}

/// `#98` locality row reordering end-to-end across both `Rcm` and
/// `RecursiveBisection`:
/// 1. At `Flush` (seal), `.hnsw` persists a vector-local `NodeMap`
///    (`has_node_map == true`) so WAL, `Bm25Store`, `.exact`, `parent_ids`, and
///    column filters keep untouched slot IDs while unfiltered, facet-filtered,
///    number-range-filtered, column-collapsed, and parent-collapsed queries
///    agree bit-for-bit with `LocalityOrder::None`.
/// 2. At `CompactShard` (after warming lazy `FacetRows` and `NumberRows` caches
///    and applying deletes + `CommitReplacements`), non-partitioned compaction
///    globally permutes the child segment (`has_node_map == false`, `0 B/row`
///    map overhead), shrinking physical `|u - v|` edge gaps while keeping
///    documents, vectors, facets, numbers, `parent_ids`, deletes, and
///    replacements strictly aligned across restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn locality_reordering_round_trips_seal_compaction_collapse_and_column_filters() {
    use pipestream_search::hnsw::{BuildMode, HnswIndex, LocalityOrder};
    use pipestream_search::pb::{
        filter_query, CollapseSpec, CompositeSearchStrategy, DocLineage, FacetValue, FilterQuery,
        IntegerValue, NumericValue, SearchRequest, SelectionOperator,
    };

    #[derive(Clone)]
    struct RichRow {
        text: String,
        court: &'static str,
        year: i64,
        pages: f64,
        parent_id: u64,
        vector: Vec<f32>,
    }

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct PreCompactSnapshot {
        unfiltered: Vec<Vec<(String, u32)>>,
        facet: Vec<Vec<(String, u32)>>,
        number_range: Vec<Vec<(String, u32)>>,
        col_collapse: Vec<Vec<(String, u32, usize)>>,
        parent_collapse: Vec<Vec<(u64, String, u32)>>,
    }

    let courts = ["scotus", "ca9", "ca2", "ca5"];
    let probes = queries(4, 0x98C0_FFEE);

    let run_order = |order: LocalityOrder| {
        let probes = probes.clone();
        async move {
            let tag = format!("reorder-{order:?}");
            let dir = tempdir(&tag);
            let path = dir.join("shard.hnsw");
            let node_cfg = || NodeConfig {
                wal_buckets: 1,
                facet_fields: vec!["court".into()],
                integer_fields: vec!["year".into()],
                numeric_fields: vec!["pages".into()],
                ..config(&path, true, true)
            };
            let (addr, handle) = start_empty_node(node_cfg()).await;
            let hnsw_cfg = HnswConfig {
                m: 12,
                ef_construction: 96,
                seed: 42,
                build_mode: BuildMode::FixedOrder,
                locality_order: order,
            }
            .backend_config()
            .unwrap();
            client(&addr)
                .await
                .configure_vector_backend(ConfigureVectorBackendRequest {
                    dim: DIM as u32,
                    config: Some(VectorBackendConfig {
                        backend_kind: hnsw_cfg.backend_kind,
                        config_format: hnsw_cfg.config_format,
                        payload: hnsw_cfg.payload,
                    }),
                })
                .await
                .unwrap();

            let mut live: BTreeMap<u64, RichRow> = BTreeMap::new();
            for (seg_idx, range) in [0..320usize, 320..640, 640..680].into_iter().enumerate() {
                let mut docs = Vec::with_capacity(range.len());
                let mut vecs = Vec::with_capacity(range.len() * DIM);
                let mut rows_batch = Vec::with_capacity(range.len());
                for i in range {
                    let court = courts[i % courts.len()];
                    let year = 2000 + (i % 25) as i64;
                    let pages = ((i * 11) % 100) as f64;
                    let parent_id = 1_000 + (i / 3) as u64;
                    let topic = i % 8;
                    let mut v = unit_vectors(1, DIM, SEED + 0x98_0000 + i as u64 * 137);
                    v[topic * 3] += 2.5;
                    let v_norm = v.iter().map(|x| x * x).sum::<f32>().sqrt();
                    for x in &mut v {
                        *x /= v_norm;
                    }
                    let text = format!("doc-{i} {court} y{year} p{parent_id}");
                    docs.push(AddDocumentsRequest {
                        text: text.clone(),
                        analysis: Some(body_spec()),
                        facets: vec![FacetValue {
                            field: "court".into(),
                            value: court.into(),
                        }],
                        integers: vec![IntegerValue {
                            field: "year".into(),
                            value: year,
                        }],
                        numerics: vec![NumericValue {
                            field: "pages".into(),
                            value: pages,
                        }],
                        lineage: Some(DocLineage {
                            parent_id,
                            group_id: parent_id,
                            span_start: 0,
                            span_end: 10,
                        }),
                        ..Default::default()
                    });
                    vecs.extend_from_slice(&v);
                    rows_batch.push(RichRow {
                        text,
                        court,
                        year,
                        pages,
                        parent_id,
                        vector: v,
                    });
                }
                let mut cli = client(&addr).await;
                let d_res = cli
                    .add_documents(tokio_stream::iter(docs))
                    .await
                    .unwrap()
                    .into_inner();
                let v_res = cli
                    .add_vectors(tokio_stream::iter(vec![AddVectorsRequest {
                        vectors: vecs,
                        dim: DIM as u32,
                    }]))
                    .await
                    .unwrap()
                    .into_inner();
                assert_eq!(d_res.first_id, v_res.first_id);
                for (off, r) in rows_batch.into_iter().enumerate() {
                    live.insert(d_res.first_id + off as u64, r);
                }
                if seg_idx < 2 {
                    flush(&addr).await;
                }
            }

            // Delete every 6th row and atomically replace 6 live rows across sealed + tail.
            let doomed: Vec<u64> = (0..680u64).filter(|id| id % 6 == 2).collect();
            let del_res = client(&addr)
                .await
                .delete_documents(DeleteDocumentsRequest {
                    doc_ids: doomed.clone(),
                    expected_wal_generation: None,
                })
                .await
                .unwrap()
                .into_inner();
            assert_eq!(del_res.deleted as usize, doomed.len());
            for id in &doomed {
                live.remove(id);
            }

            for rep_old in [5u64, 121, 333, 511, 645] {
                let old_row = live.remove(&rep_old).unwrap();
                let new_v = unit_vectors(1, DIM, SEED + 0x98_BEEF + rep_old * 97);
                let new_text = format!("{}-replaced", old_row.text);
                let mut cli = client(&addr).await;
                let d_res = cli
                    .add_documents(tokio_stream::iter(vec![AddDocumentsRequest {
                        text: new_text.clone(),
                        analysis: Some(body_spec()),
                        facets: vec![FacetValue {
                            field: "court".into(),
                            value: old_row.court.into(),
                        }],
                        integers: vec![IntegerValue {
                            field: "year".into(),
                            value: old_row.year,
                        }],
                        numerics: vec![NumericValue {
                            field: "pages".into(),
                            value: old_row.pages,
                        }],
                        lineage: Some(DocLineage {
                            parent_id: old_row.parent_id,
                            group_id: old_row.parent_id,
                            span_start: 0,
                            span_end: 10,
                        }),
                        ..Default::default()
                    }]))
                    .await
                    .unwrap()
                    .into_inner();
                let v_res = cli
                    .add_vectors(tokio_stream::iter(vec![AddVectorsRequest {
                        vectors: new_v.clone(),
                        dim: DIM as u32,
                    }]))
                    .await
                    .unwrap()
                    .into_inner();
                assert_eq!(d_res.first_id, v_res.first_id);
                let committed = cli
                    .commit_replacements(CommitReplacementsRequest {
                        replacements: vec![Replacement {
                            old_doc_id: rep_old,
                            new_doc_id: d_res.first_id,
                        }],
                        expected_wal_generation: None,
                    })
                    .await
                    .unwrap()
                    .into_inner();
                assert_eq!(committed.committed, 1);
                live.insert(
                    d_res.first_id,
                    RichRow {
                        text: new_text,
                        vector: new_v,
                        ..old_row
                    },
                );
            }

            // Inspect sealed segment `.hnsw` files on disk before compaction:
            // seal writes `has_node_map == true` when `order != None`.
            let root = segments_root(&path);
            {
                let sealed = OpenedSegmentSet::open_with(&root, VectorLoad::Mapped).unwrap();
                assert_eq!(sealed.len(), 2);
                for seg_i in 0..sealed.len() {
                    let meta = sealed.metadata(seg_i);
                    let vec_p = SegmentCatalog::segment_dir(&root, &meta.segment_id)
                        .join(&meta.vector.file);
                    let mapped = HnswIndex::load_mapped(&vec_p).unwrap();
                    assert_eq!(mapped.config().locality_order, order);
                    assert_eq!(
                        mapped.has_node_map(),
                        order != LocalityOrder::None,
                        "sealed segment {seg_i} node-map flag for {order:?}"
                    );
                }
            }

            let eval_queries = |addr_s: String, live_map: BTreeMap<u64, RichRow>, stage: String| {
                let probes_ref = probes.clone();
                async move {
                    let coord = coordinator(&addr_s);
                    let mut cli = client(&addr_s).await;
                    let by_text: BTreeMap<&str, &RichRow> =
                        live_map.values().map(|r| (r.text.as_str(), r)).collect();

                    let fetch_texts = |cli: &mut NodeServiceClient<Channel>, ids: Vec<u64>| {
                        let mut c = cli.clone();
                        async move {
                            let docs = c
                                .get_documents(GetDocumentsRequest { doc_ids: ids })
                                .await
                                .unwrap()
                                .into_inner()
                                .documents;
                            docs.into_iter()
                                .map(|d| (d.doc_id, d.text))
                                .collect::<BTreeMap<_, _>>()
                        }
                    };

                    let mut snap = PreCompactSnapshot {
                        unfiltered: Vec::new(),
                        facet: Vec::new(),
                        number_range: Vec::new(),
                        col_collapse: Vec::new(),
                        parent_collapse: Vec::new(),
                    };

                    for qv in &probes_ref {
                        // 1. Unfiltered Query.
                        let un_resp = coord
                            .query(Request::new(QueryRequest {
                                k: K as u32,
                                selection: Some(SelectionQuery {
                                    node: Some(selection_query::Node::Search(SearchQuery {
                                        id: "vec".into(),
                                        query: Some(search_query::Query::Dense(DenseQuery {
                                            vector: qv.clone(),
                                            execution_mode: DenseExecutionMode::Ann as i32,
                                            ..Default::default()
                                        })),
                                    })),
                                }),
                                ..Default::default()
                            }))
                            .await
                            .unwrap()
                            .into_inner();
                        let tmap =
                            fetch_texts(&mut cli, un_resp.hits.iter().map(|h| h.doc_id).collect())
                                .await;
                        let mut un_vec = Vec::new();
                        for h in &un_resp.hits {
                            let txt = &tmap[&h.doc_id];
                            let row = by_text
                                .get(txt.as_str())
                                .unwrap_or_else(|| panic!("{stage}: stale/deleted row {txt}"));
                            assert_eq!(h.score.to_bits(), dot(&row.vector, qv).to_bits());
                            un_vec.push((txt.clone(), h.score.to_bits()));
                        }
                        snap.unfiltered.push(un_vec);

                        // 2. Facet-filtered Query (warms lazy FacetRows).
                        let mk_flt = |cel: &str| QueryRequest {
                            k: K as u32,
                            selection: Some(SelectionQuery {
                                node: Some(selection_query::Node::Composite(
                                    CompositeSearchStrategy {
                                        operator: SelectionOperator::And as i32,
                                        clauses: vec![
                                            SelectionQuery {
                                                node: Some(selection_query::Node::Search(
                                                    SearchQuery {
                                                        id: "vec".into(),
                                                        query: Some(search_query::Query::Dense(
                                                            DenseQuery {
                                                                vector: qv.clone(),
                                                                execution_mode:
                                                                    DenseExecutionMode::Ann as i32,
                                                                ..Default::default()
                                                            },
                                                        )),
                                                    },
                                                )),
                                            },
                                            SelectionQuery {
                                                node: Some(selection_query::Node::Filter(
                                                    FilterQuery {
                                                        id: "flt".into(),
                                                        predicate: Some(
                                                            filter_query::Predicate::Cel(
                                                                cel.into(),
                                                            ),
                                                        ),
                                                    },
                                                )),
                                            },
                                        ],
                                        scoring: None,
                                    },
                                )),
                            }),
                            ..Default::default()
                        };

                        let fac_resp = coord
                            .query(Request::new(mk_flt("court == 'ca9'")))
                            .await
                            .unwrap()
                            .into_inner();
                        let tmap =
                            fetch_texts(&mut cli, fac_resp.hits.iter().map(|h| h.doc_id).collect())
                                .await;
                        let mut fac_vec = Vec::new();
                        for h in &fac_resp.hits {
                            let txt = &tmap[&h.doc_id];
                            let row = by_text[txt.as_str()];
                            assert_eq!(row.court, "ca9", "{stage}: wrong court on {txt}");
                            assert_eq!(h.score.to_bits(), dot(&row.vector, qv).to_bits());
                            fac_vec.push((txt.clone(), h.score.to_bits()));
                        }
                        snap.facet.push(fac_vec);

                        // 3. Number-range Query (warms lazy NumberRows).
                        let num_resp = coord
                            .query(Request::new(mk_flt(
                                "year >= 2005 && year <= 2018 && pages >= 20.0 && pages <= 80.0",
                            )))
                            .await
                            .unwrap()
                            .into_inner();
                        let tmap =
                            fetch_texts(&mut cli, num_resp.hits.iter().map(|h| h.doc_id).collect())
                                .await;
                        let mut num_vec = Vec::new();
                        for h in &num_resp.hits {
                            let txt = &tmap[&h.doc_id];
                            let row = by_text[txt.as_str()];
                            assert!(
                                (2005..=2018).contains(&row.year)
                                    && (20.0..=80.0).contains(&row.pages),
                                "{stage}: out-of-range row {txt}"
                            );
                            assert_eq!(h.score.to_bits(), dot(&row.vector, qv).to_bits());
                            num_vec.push((txt.clone(), h.score.to_bits()));
                        }
                        snap.number_range.push(num_vec);

                        // 4. Column-collapsed Query (`court`, 4 distinct values).
                        let col_resp = coord
                            .query(Request::new(QueryRequest {
                                k: 4,
                                selection_k: 40,
                                selection: Some(SelectionQuery {
                                    node: Some(selection_query::Node::Search(SearchQuery {
                                        id: "vec".into(),
                                        query: Some(search_query::Query::Dense(DenseQuery {
                                            vector: qv.clone(),
                                            execution_mode: DenseExecutionMode::Ann as i32,
                                            ..Default::default()
                                        })),
                                    })),
                                }),
                                collapse: Some(CollapseSpec {
                                    column: "court".into(),
                                    inner_hits: 2,
                                    map: None,
                                }),
                                ..Default::default()
                            }))
                            .await
                            .unwrap()
                            .into_inner();
                        let tmap =
                            fetch_texts(&mut cli, col_resp.hits.iter().map(|h| h.doc_id).collect())
                                .await;
                        assert_eq!(col_resp.groups.len(), col_resp.hits.len());
                        let mut seen_courts = std::collections::BTreeSet::new();
                        let mut col_vec = Vec::new();
                        for (h, g) in col_resp.hits.iter().zip(&col_resp.groups) {
                            let txt = &tmap[&h.doc_id];
                            let row = by_text[txt.as_str()];
                            assert!(
                                seen_courts.insert(row.court),
                                "{stage}: duplicate collapsed court {}",
                                row.court
                            );
                            assert_eq!(h.score.to_bits(), dot(&row.vector, qv).to_bits());
                            col_vec.push((txt.clone(), h.score.to_bits(), g.hits.len()));
                        }
                        snap.col_collapse.push(col_vec);

                        // 5. Parent-collapsed Search (`parent_ids` sidecar).
                        let par_resp = coord
                            .search(Request::new(SearchRequest {
                                request_id: format!("{stage}-pcollapse"),
                                k: K as u32,
                                vector: qv.clone(),
                                collapse_parents: true,
                                ..Default::default()
                            }))
                            .await
                            .unwrap()
                            .into_inner();
                        let tmap = fetch_texts(
                            &mut cli,
                            par_resp.hits.iter().map(|h| h.vector_id).collect(),
                        )
                        .await;
                        let mut seen_parents = std::collections::BTreeSet::new();
                        let mut par_vec = Vec::new();
                        for h in &par_resp.hits {
                            let txt = &tmap[&h.vector_id];
                            let row = by_text[txt.as_str()];
                            assert_eq!(
                                h.parent_id, row.parent_id,
                                "{stage}: lineage parent_id mismatch on {txt}"
                            );
                            assert!(
                                seen_parents.insert(h.parent_id),
                                "{stage}: duplicate parent_id {}",
                                h.parent_id
                            );
                            assert_eq!(h.score.to_bits(), dot(&row.vector, qv).to_bits());
                            par_vec.push((h.parent_id, txt.clone(), h.score.to_bits()));
                        }
                        snap.parent_collapse.push(par_vec);
                    }
                    snap
                }
            };

            let pre_snap = eval_queries(addr.clone(), live.clone(), format!("{order:?}-pre")).await;

            // Compact online: global segment permutation (`has_node_map == false`).
            let mut req = Request::new(CompactShardRequest::default());
            req.set_timeout(Duration::from_secs(600));
            let compacted = client(&addr)
                .await
                .compact_shard(req)
                .await
                .unwrap()
                .into_inner();
            assert_eq!(compacted.rows_after as usize, live.len());

            let post_snap =
                eval_queries(addr.clone(), live.clone(), format!("{order:?}-post")).await;
            handle.abort();

            // Verify compacted `.hnsw` on disk: global remap uses `0 B/row` (`!has_node_map`).
            let compacted_gap = {
                let reopened = OpenedSegmentSet::open_with(&root, VectorLoad::Mapped).unwrap();
                assert_eq!(reopened.len(), 1);
                let meta = reopened.metadata(0);
                let vec_p =
                    SegmentCatalog::segment_dir(&root, &meta.segment_id).join(&meta.vector.file);
                let mapped = HnswIndex::load_mapped(&vec_p).unwrap();
                assert!(mapped.is_graph_linked());
                assert_eq!(mapped.config().locality_order, order);
                assert!(
                    !mapped.has_node_map(),
                    "compacted segment must use global slot order without NodeMap"
                );
                assert_eq!(mapped.unreachable_from_entry(), 0);
                mapped.locality_metrics().mean_id_gap
            };

            // Verify restart serves identical answers from the compacted disk state.
            let (re_addr, re_handle) = start_opened_node(node_cfg()).await;
            let restart_snap =
                eval_queries(re_addr.clone(), live.clone(), format!("{order:?}-restart")).await;
            assert_eq!(restart_snap, post_snap, "{order:?} restart mismatch");
            re_handle.abort();
            let _ = std::fs::remove_dir_all(&dir);

            (pre_snap, post_snap, compacted_gap)
        }
    };

    let (none_pre, none_post, none_gap) = run_order(LocalityOrder::None).await;
    for order in [LocalityOrder::Rcm, LocalityOrder::RecursiveBisection] {
        let (pre, post, gap) = run_order(order).await;
        // Pre-compaction seal uses FixedOrder + vector-local NodeMap over identical
        // segments, so every query mode must agree with None bit-for-bit!
        assert_eq!(
            pre, none_pre,
            "{order:?} pre-compaction seal answers diverged from None"
        );
        // Post-compaction global remap shrinks physical |u - v| edge gaps.
        assert!(
            gap < none_gap * 0.90,
            "{order:?} compacted mean_id_gap {gap:.1} did not beat None {none_gap:.1}"
        );
        // Every post-compaction query mode returns full K / distinct-group count.
        for qi in 0..probes.len() {
            assert_eq!(post.unfiltered[qi].len(), none_post.unfiltered[qi].len());
            assert_eq!(post.facet[qi].len(), none_post.facet[qi].len());
            assert_eq!(
                post.number_range[qi].len(),
                none_post.number_range[qi].len()
            );
            assert_eq!(
                post.col_collapse[qi].len(),
                none_post.col_collapse[qi].len()
            );
            assert_eq!(
                post.parent_collapse[qi].len(),
                none_post.parent_collapse[qi].len()
            );
        }
    }
}
