//! Segments as the only layout (`docs/immutable-segments.md`): a fresh
//! persisted shard writes a catalog and seals its tail on flush; and
//! every query over two sealed segments plus a tail equals the same
//! query over one sealed segment of the same rows — global df, the live
//! bitmap, facets, filters, highlights, prefixes, block-max.

mod common;
#[path = "segment_layout/union_fixture.rs"]
mod union_fixture;

use std::path::PathBuf;

use common::start_empty_node;
use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::{segments_root, NodeConfig, NodeServiceImpl};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    AddDocumentsRequest, Bm25SearchRequest, Bm25SearchResponse, DeleteDocumentsRequest, FacetValue,
    FlushRequest, HealthRequest, HighlightMode, HighlightSpec, TermPrefix,
};
use pipestream_search::postings::{Bm25Index, Bm25Store};
use pipestream_search::segmented::SegmentedShard;
use pipestream_search::segments::OpenedSegmentSet;
use tonic::Request;

#[path = "common/dir.rs"]
mod dir;
use dir::tempdir;

#[path = "common/ingest.rs"]
mod ingest;
use ingest::ingest;

/// Three batches: the same corpus reaches a segmented shard as two
/// sealed segments plus a tail, and a second shard as one sealed segment.
const BATCHES: [&[(&str, &str)]; 3] = [
    &[
        (
            "The court held that the claim fails\nA second line about the appeal",
            "scotus",
        ),
        (
            "Court after court has said so\nThe appeal was denied by the court",
            "ca9",
        ),
        ("appeal appeal court\nnothing here", "ca9"),
    ],
    &[
        ("courtesy of the court reporter\ncourthouse steps", "dcc"),
        ("the courier brought the appeal brief", "scotus"),
        ("no match in this one\nreally none", "ca5"),
    ],
    &[
        ("court of appeals\ncourts and appeals", "ca9"),
        ("a last appeal to the court", "nysd"),
    ],
];
fn config(index_path: Option<PathBuf>) -> NodeConfig {
    NodeConfig {
        index_path,
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        facet_fields: vec!["court".to_string()],
        sentence_fields: vec!["body".to_string()],
        position_fields: vec!["body".to_string()],
        wal: false,
        ..Default::default()
    }
}

async fn ingest_courts(addr: &str, docs: &[(&str, &str)]) -> u64 {
    let requests = docs
        .iter()
        .map(|(text, court)| AddDocumentsRequest {
            text: text.to_string(),
            analysis: Some(body_spec()),
            facets: vec![FacetValue {
                field: "court".into(),
                value: court.to_string(),
            }],
            ..Default::default()
        })
        .collect();
    ingest(addr, requests).await.unwrap().added
}

async fn flush(addr: &str) -> bool {
    NodeServiceClient::connect(addr.to_string())
        .await
        .unwrap()
        .flush(FlushRequest {})
        .await
        .unwrap()
        .into_inner()
        .written
}

fn coordinator(addr: &str) -> CoordinatorServiceImpl {
    CoordinatorServiceImpl::new(vec![addr.to_string()]).with_bm25(
        Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        Default::default(),
    )
}

async fn bm25(c: &CoordinatorServiceImpl, req: Bm25SearchRequest) -> Bm25SearchResponse {
    SearchService::bm25_search(c, Request::new(req))
        .await
        .unwrap()
        .into_inner()
}

fn probes() -> Vec<Bm25SearchRequest> {
    let base = |text: &str| Bm25SearchRequest {
        text: text.to_string(),
        k: 20,
        analysis: Some(body_spec()),
        ..Default::default()
    };
    vec![
        base("court"),
        base("court appeal"),
        base("courier steps"),
        Bm25SearchRequest {
            facet_fields: vec!["court".into()],
            ..base("court appeal")
        },
        Bm25SearchRequest {
            filter: "court == \"ca9\"".into(),
            ..base("court")
        },
        Bm25SearchRequest {
            filter: "court >= \"ca9\" && court < \"nysd\"".into(),
            ..base("court appeal")
        },
        Bm25SearchRequest {
            highlight: Some(HighlightSpec {
                mode: HighlightMode::Sentence as i32,
                ..Default::default()
            }),
            ..base("court appeal")
        },
        Bm25SearchRequest {
            prefixes: vec![TermPrefix {
                prefix: "cour".into(),
                max_expansions: 0,
            }],
            ..base("")
        },
        Bm25SearchRequest {
            phrase: Some(pipestream_search::pb::PhraseMatch { slop: 0 }),
            ..base("appeal court")
        },
    ]
}

/// Everything a response says that must not depend on the layout.
fn signature(resp: &Bm25SearchResponse) -> String {
    format!(
        "{:?}",
        (
            resp.hits
                .iter()
                .map(|h| (
                    h.doc_id,
                    h.score.to_bits(),
                    h.terms
                        .iter()
                        .map(|t| (
                            t.term.clone(),
                            t.offsets
                                .iter()
                                .map(|o| (o.start, o.end))
                                .collect::<Vec<_>>()
                        ))
                        .collect::<Vec<_>>(),
                    h.snippets.clone(),
                ))
                .collect::<Vec<_>>(),
            resp.kth_best.to_bits(),
            resp.facets.clone(),
            resp.prefix_expansions.clone(),
        )
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn many_sealed_segments_answer_every_query_like_one_segment() {
    let dir = tempdir("exact");
    let seg_path = dir.join("segmented.tv");
    let one_path = dir.join("one-segment.tv");
    let (seg, hs) = start_empty_node(config(Some(seg_path.clone()))).await;
    let (one, ho) = start_empty_node(config(Some(one_path.clone()))).await;
    // Two flushes seal two segments; the third batch stays in the tail.
    for (i, batch) in BATCHES.iter().enumerate() {
        assert_eq!(ingest_courts(&seg, batch).await, batch.len() as u64);
        assert_eq!(ingest_courts(&one, batch).await, batch.len() as u64);
        if i < 2 {
            assert!(flush(&seg).await);
        }
    }
    assert!(flush(&one).await);
    let root = segments_root(&seg_path);
    assert!(root.join("segments.json").exists());
    assert_eq!(
        OpenedSegmentSet::open(segments_root(&one_path))
            .unwrap()
            .len(),
        1
    );
    let set = OpenedSegmentSet::open(&root).unwrap();
    assert_eq!(set.len(), 2);
    assert_eq!(set.metadata(0).rows, 3);
    assert_eq!(set.metadata(1).rows, 3);
    assert_eq!(set.metadata(1).base_label, 3);
    assert!(set.vector(0).is_none(), "documents-only segments");

    let segmented = coordinator(&seg);
    let single = coordinator(&one);
    for probe in probes() {
        let a = bm25(&segmented, probe.clone()).await;
        let b = bm25(&single, probe.clone()).await;
        assert!(!a.hits.is_empty(), "{:?}", probe.text);
        assert_eq!(signature(&a), signature(&b), "{probe:?}");
    }
    // A delete after sealing: the live bitmap governs both shards the
    // same way, statistics included.
    for addr in [&seg, &one] {
        let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
        client
            .delete_documents(DeleteDocumentsRequest {
                expected_wal_generation: None,
                doc_ids: vec![1],
            })
            .await
            .unwrap();
    }
    for probe in probes() {
        let a = bm25(&segmented, probe.clone()).await;
        let b = bm25(&single, probe.clone()).await;
        assert!(a.hits.iter().all(|h| h.doc_id != 1));
        assert_eq!(signature(&a), signature(&b), "after delete: {probe:?}");
    }
    // Sealing the tail too, then reopening from disk: the same answers.
    assert!(flush(&seg).await);
    assert_eq!(OpenedSegmentSet::open(&root).unwrap().len(), 3);
    let flushed = probes();
    let mut before = Vec::new();
    for probe in &flushed {
        before.push(signature(&bm25(&segmented, probe.clone()).await));
    }
    hs.abort();
    ho.abort();
    let reopened = NodeServiceImpl::open(config(Some(seg_path.clone())), None).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://{}", listener.local_addr().unwrap());
    let handle = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(reopened.into_server(pipestream_search::MAX_MESSAGE_BYTES))
            .serve_with_incoming(pipestream_search::harness::nodelay_incoming(listener)),
    );
    let again = coordinator(&addr);
    let health = NodeServiceClient::connect(addr.clone())
        .await
        .unwrap()
        .health(HealthRequest {})
        .await
        .unwrap()
        .into_inner();
    assert_eq!(health.bm25_docs, 8);
    for (probe, want) in flushed.iter().zip(&before) {
        assert_eq!(
            &signature(&bm25(&again, probe.clone()).await),
            want,
            "{probe:?}"
        );
    }
    handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_tail_seals_itself_at_the_configured_size() {
    let dir = tempdir("autoseal");
    let path = dir.join("auto.tv");
    let (addr, handle) = start_empty_node(NodeConfig {
        seal_tail_docs: 2,
        ..config(Some(path.clone()))
    })
    .await;
    assert_eq!(ingest_courts(&addr, BATCHES[0]).await, 3);
    let root = segments_root(&path);
    let set = OpenedSegmentSet::open(&root).unwrap();
    assert_eq!(
        set.len(),
        1,
        "two documents filled the tail and sealed; the third started the next tail"
    );
    assert_eq!(set.metadata(0).rows, 2);
    let c = coordinator(&addr);
    assert_eq!(bm25(&c, probes()[0].clone()).await.hits.len(), 3);
    assert_eq!(ingest_courts(&addr, BATCHES[1]).await, 3);
    let set = OpenedSegmentSet::open(&root).unwrap();
    assert_eq!(
        set.len(),
        3,
        "the tail's one document plus five more: two more seals"
    );
    assert!((0..set.len()).all(|i| set.metadata(i).rows == 2));
    assert_eq!(bm25(&c, probes()[0].clone()).await.hits.len(), 4);
    handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// One AddDocuments stream longer than twice the bound seals as it
/// goes, between documents, and never grows a segment past the bound;
/// the stream's own documents are searchable throughout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn one_stream_seals_bounded_segments_before_it_ends() {
    let dir = tempdir("bounded");
    let path = dir.join("bounded.tv");
    let (addr, handle) = start_empty_node(NodeConfig {
        seal_tail_docs: 3,
        ..config(Some(path.clone()))
    })
    .await;
    let all: Vec<(&str, &str)> = BATCHES.iter().flat_map(|b| b.iter().copied()).collect();
    assert!(
        all.len() > 2 * 3,
        "the stream must cross the bound at least twice"
    );
    assert_eq!(ingest_courts(&addr, &all).await, all.len() as u64);
    // No flush: the seals happened inside the stream.
    let root = segments_root(&path);
    let set = OpenedSegmentSet::open(&root).unwrap();
    assert_eq!(
        set.len(),
        2,
        "eight documents under a bound of three: two full segments"
    );
    for i in 0..set.len() {
        assert!(
            set.metadata(i).rows <= 3,
            "segment {i} has {} rows",
            set.metadata(i).rows
        );
        assert_eq!(set.metadata(i).rows, 3);
    }
    assert_eq!(set.metadata(1).base_label, 3);
    let c = coordinator(&addr);
    let hits = bm25(&c, probes()[0].clone()).await.hits;
    assert_eq!(
        hits.len(),
        6,
        "every document of the stream, sealed or in the tail"
    );
    let mut ids: Vec<u64> = hits.iter().map(|h| h.doc_id).collect();
    ids.sort_unstable();
    assert_eq!(ids, vec![0, 1, 2, 3, 6, 7]);
    // A flush seals the two-document remainder; still within the bound.
    assert!(flush(&addr).await);
    let set = OpenedSegmentSet::open(&root).unwrap();
    assert_eq!(set.len(), 3);
    assert_eq!(set.metadata(2).rows, 2);
    assert_eq!(bm25(&c, probes()[0].clone()).await.hits.len(), 6);
    handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// The union read surface itself, against one image of the same rows:
/// df, postings, the chained block-max cursor, dictionaries.
#[test]
fn the_union_index_equals_one_image_over_the_same_rows() {
    let dir = tempdir("union");
    let root = dir.join("catalog");
    let mut union = SegmentedShard::open(&root, union_fixture::union_tail()).unwrap();
    let mut whole = union_fixture::union_tail();
    let mut next = 0u32;
    let mut snapshots = Vec::new();
    let mut mapped_sets = Vec::new();
    let mut frozen_stores = Vec::new();
    for batch in &BATCHES[..2] {
        union_fixture::append_batch(&mut union, &mut whole, batch, &mut next);
        snapshots.push((next, union.identity_snapshot().unwrap()));
        union_fixture::seal_batch(
            &mut union,
            &dir,
            &mut mapped_sets,
            &mut frozen_stores,
            &whole,
            next,
        );
        snapshots.push((next, union.identity_snapshot().unwrap()));
    }
    union_fixture::append_batch(&mut union, &mut whole, BATCHES[2], &mut next);
    union_fixture::assert_equal_terms(&union, &whole, next);
    // Sealed terms have a chained cursor that walks every posting in
    // order; a term the tail holds has none until the next seal.
    let mut cursor = union.impacts("courier").expect("sealed term has impacts");
    let mut walked = Vec::new();
    while !cursor.exhausted() {
        walked.push((cursor.doc_id(), cursor.tf()));
        cursor.next_posting();
    }
    let mut want = Vec::new();
    whole.for_each_posting("courier", &mut |doc, tf, _| want.push((doc, tf)));
    assert_eq!(walked, want);
    assert!(union.impacts("court").is_none(), "the tail holds court");
    union_fixture::assert_equal_dictionary(&union, &whole, next);
    snapshots.push((next, union.identity_snapshot().unwrap()));
    for row in 0..next {
        assert_eq!(
            union.document_identity(row),
            union_fixture::row_identity(row)
        );
    }
    drop(cursor);
    drop(union);
    drop(whole);
    assert!(mapped_sets.iter().all(|weak| weak.upgrade().is_none()));
    assert!(frozen_stores.iter().all(|weak| weak.upgrade().is_none()));
    std::fs::remove_dir_all(&dir).unwrap();
    for (rows, snapshot) in snapshots {
        for row in 0..rows {
            assert_eq!(snapshot.identity(row), union_fixture::row_identity(row));
        }
        for row in rows..=next {
            assert!(
                snapshot.identity(row).is_none(),
                "snapshot acquired new row {row}"
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_memory_shards_stay_in_heap_whatever_the_layout_says() {
    // A shard without an index path has no catalog to seal into: it
    // stays the heap store it always was.
    let (addr, handle) = start_empty_node(config(None)).await;
    assert_eq!(ingest_courts(&addr, BATCHES[0]).await, 3);
    assert!(!flush(&addr).await, "nothing persists without a path");
    let c = coordinator(&addr);
    assert_eq!(bm25(&c, probes()[0].clone()).await.hits.len(), 3);
    handle.abort();
}

/// A sealed segment's vector image serves mapped (the default) with the
/// hits and scores of the same shard served from memory, bit for bit,
/// through Search (docs/mmap-vectors.md).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn sealed_segments_serve_mapped_and_equal_the_heap_load_bit_for_bit() {
    use pipestream_search::pb::{AddVectorsRequest, SearchRequest};
    use pipestream_search::segments::VectorLoad;

    const DIM: usize = 64;
    let dir = tempdir("mapped");
    let path = dir.join("mapped.tv");
    let docs: Vec<(&str, &str)> = BATCHES.iter().flat_map(|b| b.iter().copied()).collect();
    let corpus = pipestream_search::harness::unit_vectors(docs.len(), DIM, 0x3A9E_0001);
    let (shift, scale) = pipestream_search::harness::fit_calibration(DIM, 4, &corpus);
    let build = |mmap: bool| NodeConfig {
        vector_mmap: mmap,
        ..config(Some(path.clone()))
    };
    let (addr, handle) = start_empty_node(build(true)).await;
    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    client
        .configure_vector_backend(pipestream_search::harness::embedded_backend_request(
            DIM, 4, &shift, &scale,
        ))
        .await
        .unwrap();
    assert_eq!(ingest_courts(&addr, &docs).await, docs.len() as u64);
    client
        .add_vectors(tokio_stream::iter(vec![AddVectorsRequest {
            materialized_rows: Vec::new(),
            vectors: corpus.clone(),
            dim: DIM as u32,
        }]))
        .await
        .unwrap();
    assert!(flush(&addr).await);
    handle.abort();

    let root = segments_root(&path);
    let mapped_set = OpenedSegmentSet::open_with(&root, VectorLoad::Mapped).unwrap();
    assert_eq!(mapped_set.len(), 1);
    assert!(mapped_set.vector(0).unwrap().is_mapped());
    let heap_set = OpenedSegmentSet::open_with(&root, VectorLoad::Heap).unwrap();
    assert!(!heap_set.vector(0).unwrap().is_mapped());

    // The same shard reopened two ways answers Search identically.
    let (mapped_addr, mapped_node) = common::start_opened_node(build(true)).await;
    let (heap_addr, heap_node) = common::start_opened_node(build(false)).await;
    for q in 0..docs.len() {
        let request = SearchRequest {
            k: 5,
            vector: corpus[q * DIM..(q + 1) * DIM].to_vec(),
            ..Default::default()
        };
        let mut answers = Vec::new();
        for addr in [&mapped_addr, &heap_addr] {
            let c = coordinator(addr);
            let hits = SearchService::search(&c, Request::new(request.clone()))
                .await
                .unwrap()
                .into_inner()
                .hits;
            answers.push(
                hits.iter()
                    .map(|h| (h.vector_id, h.score.to_bits()))
                    .collect::<Vec<_>>(),
            );
        }
        assert_eq!(answers[0], answers[1], "query {q}");
        assert_eq!(answers[0].len(), 5);
    }
    mapped_node.abort();
    heap_node.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

// --- per-thread heap accounting ---------------------------------------
//
// The mapped-versus-heap claim is about what an open retains on the heap,
// so it is measured on the heap: a wrapping allocator keeps a per-thread
// count of live bytes (allocated minus freed on this thread). Other tests
// in this binary run on other threads and do not move the count, and a
// memory map is not an allocation, so the measurement is exact and the
// same on every run. Process RSS was the earlier gauge; it is process-wide
// and moved with whatever else the binary was doing at the time.

thread_local! {
    static LIVE_HEAP_BYTES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

struct ThreadCounting;

impl ThreadCounting {
    fn adjust(delta: isize) {
        // `try_with` rather than `with`: a const-initialized Cell has no
        // destructor, so this cannot fail, but an allocator must never panic.
        let _ = LIVE_HEAP_BYTES.try_with(|cell| {
            let next = (cell.get() as isize).saturating_add(delta).max(0);
            cell.set(next as usize);
        });
    }
}

unsafe impl std::alloc::GlobalAlloc for ThreadCounting {
    unsafe fn alloc(&self, layout: std::alloc::Layout) -> *mut u8 {
        let ptr = unsafe { std::alloc::System.alloc(layout) };
        if !ptr.is_null() {
            Self::adjust(layout.size() as isize);
        }
        ptr
    }
    unsafe fn alloc_zeroed(&self, layout: std::alloc::Layout) -> *mut u8 {
        let ptr = unsafe { std::alloc::System.alloc_zeroed(layout) };
        if !ptr.is_null() {
            Self::adjust(layout.size() as isize);
        }
        ptr
    }
    unsafe fn dealloc(&self, ptr: *mut u8, layout: std::alloc::Layout) {
        Self::adjust(-(layout.size() as isize));
        unsafe { std::alloc::System.dealloc(ptr, layout) }
    }
    unsafe fn realloc(&self, ptr: *mut u8, layout: std::alloc::Layout, new_size: usize) -> *mut u8 {
        let next = unsafe { std::alloc::System.realloc(ptr, layout, new_size) };
        if !next.is_null() {
            Self::adjust(new_size as isize - layout.size() as isize);
        }
        next
    }
}

#[global_allocator]
static ALLOCATOR: ThreadCounting = ThreadCounting;

fn live_heap_bytes() -> usize {
    LIVE_HEAP_BYTES.with(|cell| cell.get())
}

/// Opening a catalog whose sealed image is large retains the headers on
/// the heap when mapped and the image when loaded; both answer the same
/// top-k. Measured per thread on the allocator, not on process RSS.
#[test]
fn a_mapped_sealed_image_opens_without_the_heap_load() {
    use pipestream_search::segments::VectorLoad;
    use pipestream_search::vector::VectorSearchOptions;

    const DIM: usize = 256;
    let (dir, root, corpus, image_bytes) = stage_large_sealed_image();
    // Both opens verify every artifact and open the BM25 and exact
    // stores; the image is the difference between them.
    let before = live_heap_bytes();
    let mapped = OpenedSegmentSet::open_with(&root, VectorLoad::Mapped).unwrap();
    let mapped_growth = live_heap_bytes().saturating_sub(before);
    assert!(mapped.vector(0).unwrap().is_mapped());
    let before = live_heap_bytes();
    let heap = OpenedSegmentSet::open_with(&root, VectorLoad::Heap).unwrap();
    let heap_growth = live_heap_bytes().saturating_sub(before);
    assert!(!heap.vector(0).unwrap().is_mapped());
    assert!(
        mapped_growth < image_bytes / 2,
        "mapped open retained {mapped_growth} heap bytes for a {image_bytes}-byte image"
    );
    assert!(
        heap_growth >= image_bytes,
        "a heap load retained {heap_growth} heap bytes for a {image_bytes}-byte image"
    );
    assert!(
        heap_growth > mapped_growth + image_bytes / 2,
        "a heap load retained {heap_growth} heap bytes against {mapped_growth} mapped for a \
         {image_bytes}-byte image"
    );
    let query = &corpus[7 * DIM..8 * DIM];
    let a = mapped
        .vector(0)
        .unwrap()
        .try_search(query, 10, VectorSearchOptions::new())
        .unwrap();
    let b = heap
        .vector(0)
        .unwrap()
        .try_search(query, 10, VectorSearchOptions::new())
        .unwrap();
    assert_eq!(a.slots_for_query(0), b.slots_for_query(0));
    assert_eq!(
        a.scores_for_query(0)
            .iter()
            .map(|s| s.to_bits())
            .collect::<Vec<_>>(),
        b.scores_for_query(0)
            .iter()
            .map(|s| s.to_bits())
            .collect::<Vec<_>>()
    );
    assert_eq!(a.slots_for_query(0)[0], 7);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Stage one large sealed segment (80k rows, >8 MiB image) and return
/// its directory, catalog root, corpus, and image size in bytes.
fn stage_large_sealed_image() -> (dir::TestDir, PathBuf, Vec<f32>, usize) {
    use pipestream_search::exact_vectors::ExactVectorStore;
    use pipestream_search::live_docs::LiveDocs;
    use pipestream_search::postings::AnalyzedDoc;
    use pipestream_search::segments::{SegmentCatalog, SegmentSource};
    use pipestream_search::vector::EMBEDDED_TURBOVEC;

    const DIM: usize = 256;
    const ROWS: usize = 80_000;
    let dir = tempdir("rss");
    let root = dir.join("catalog");
    let stage = dir.join("stage");
    std::fs::create_dir_all(&stage).unwrap();
    let corpus = pipestream_search::harness::unit_vectors(ROWS, DIM, 0x3A9E_0002);
    let (shift, scale) = pipestream_search::harness::fit_calibration(DIM, 4, &corpus[..2048 * DIM]);
    let vector_path = stage.join("vector.index");
    {
        let mut index = pipestream_search::harness::seeded_index(DIM, 4, &shift, &scale);
        index.add(&corpus, DIM).unwrap();
        index.prepare().unwrap();
        index.write(&vector_path).unwrap();
    }
    let image_bytes = std::fs::metadata(&vector_path).unwrap().len() as usize;
    assert!(
        image_bytes > 8 * 1024 * 1024,
        "image is {image_bytes} bytes"
    );
    let exact_path = stage.join("vectors.f32");
    ExactVectorStore::from_values(DIM, corpus.clone())
        .unwrap()
        .write(&exact_path)
        .unwrap();
    let bm25_path = stage.join("documents.bm25");
    {
        let mut store = Bm25Store::with_text_fields(&["body"]);
        for i in 0..ROWS as u32 {
            store.add_document(
                i,
                format!("row {i}"),
                AnalyzedDoc::body(vec![("row".to_string(), 1, vec![(0, 3)])], 1),
            );
        }
        store.save(&bm25_path).unwrap();
    }
    let live_path = stage.join("live-docs.bin");
    LiveDocs::default().write(&live_path, ROWS as u64).unwrap();
    let catalog = SegmentCatalog::open(&root).unwrap();
    catalog
        .append(SegmentSource {
            multi_res_path: None,
            segment_id: "seg-0",
            generation: 1,
            base_label: 0,
            backend_kind: EMBEDDED_TURBOVEC,
            vector_path: Some(&vector_path),
            exact_vector_path: Some(&exact_path),
            bm25_path: &bm25_path,
            live_docs_path: &live_path,
            partition_column: None,
            parents_colocated: false,
        })
        .unwrap();
    drop(catalog);
    (dir, root, corpus, image_bytes)
}
