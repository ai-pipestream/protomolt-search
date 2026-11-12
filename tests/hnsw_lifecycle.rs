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
use pipestream_search::vector::VectorIndex;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::transport::Channel;
use tonic::Request;

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
