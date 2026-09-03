//! Segments as the default layout (`docs/immutable-segments.md`): a
//! fresh persisted shard writes a catalog and seals its tail on flush;
//! an old single-image fixture still opens and serves, and nothing
//! converts on open; and every query over two sealed segments plus a
//! tail equals the same query over one image of the same rows — global
//! df, the live bitmap, facets, filters, highlights, prefixes, block-max.

mod common;

use std::path::PathBuf;

use common::start_empty_node;
use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::{
    bm25_sidecar_path, segments_root, Layout, NodeConfig, NodeServiceImpl,
};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    AddDocumentsRequest, Bm25SearchRequest, Bm25SearchResponse, DeleteDocumentsRequest, FacetValue,
    FlushRequest, HealthRequest, HighlightMode, HighlightSpec, TermPrefix,
};
use pipestream_search::postings::{Bm25Index, Bm25Store};
use pipestream_search::segmented::SegmentedShard;
use pipestream_search::segments::OpenedSegmentSet;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

/// Three batches: the same corpus reaches a segmented shard as two
/// sealed segments plus a tail, and a single-image shard as one file.
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

fn config(index_path: Option<PathBuf>, layout: Layout) -> NodeConfig {
    NodeConfig {
        index_path,
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        facet_fields: vec!["court".to_string()],
        sentence_fields: vec!["body".to_string()],
        position_fields: vec!["body".to_string()],
        layout,
        wal: false,
        ..Default::default()
    }
}

async fn ingest(addr: &str, docs: &[(&str, &str)]) -> u64 {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(8);
    for (text, court) in docs {
        tx.send(AddDocumentsRequest {
            text: text.to_string(),
            analysis: Some(body_spec()),
            facets: vec![FacetValue {
                field: "court".into(),
                value: court.to_string(),
            }],
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
        .added
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

fn tempdir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("segments-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fresh_shard_writes_a_catalog_and_every_query_equals_one_image() {
    let dir = tempdir("exact");
    let seg_path = dir.join("segmented.tv");
    let one_path = dir.join("single.tv");
    let (seg, hs) = start_empty_node(config(Some(seg_path.clone()), Layout::Segments)).await;
    let (one, ho) = start_empty_node(config(Some(one_path.clone()), Layout::SingleImage)).await;
    // Two flushes seal two segments; the third batch stays in the tail.
    for (i, batch) in BATCHES.iter().enumerate() {
        assert_eq!(ingest(&seg, batch).await, batch.len() as u64);
        assert_eq!(ingest(&one, batch).await, batch.len() as u64);
        if i < 2 {
            assert!(flush(&seg).await);
        }
    }
    assert!(flush(&one).await);
    let root = segments_root(&seg_path);
    assert!(
        root.join("segments.json").exists(),
        "the catalog is the layout"
    );
    assert!(
        !bm25_sidecar_path(&seg_path).exists(),
        "no single image was written"
    );
    assert!(bm25_sidecar_path(&one_path).exists());
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
    // A delete after sealing: the live bitmap governs both layouts the
    // same way, statistics included.
    for addr in [&seg, &one] {
        let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
        client
            .delete_documents(DeleteDocumentsRequest { doc_ids: vec![1] })
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
    let reopened = NodeServiceImpl::open(
        config(Some(seg_path.clone()), Layout::Segments),
        None,
        false,
    )
    .unwrap();
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
async fn an_old_single_image_still_serves_and_nothing_converts_on_open() {
    let dir = tempdir("old");
    let path = dir.join("old.tv");
    // The fixture: a shard written under the single-image layout.
    let (addr, handle) = start_empty_node(config(Some(path.clone()), Layout::SingleImage)).await;
    assert_eq!(ingest(&addr, BATCHES[0]).await, 3);
    assert!(flush(&addr).await);
    handle.abort();
    assert!(bm25_sidecar_path(&path).exists());
    let root = segments_root(&path);
    assert!(!root.exists());
    // Opened under the default layout: the file is what serves, no
    // catalog appears, and a further flush rewrites the file.
    let node =
        NodeServiceImpl::open(config(Some(path.clone()), Layout::Segments), None, false).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://{}", listener.local_addr().unwrap());
    let handle = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(node.into_server(pipestream_search::MAX_MESSAGE_BYTES))
            .serve_with_incoming(pipestream_search::harness::nodelay_incoming(listener)),
    );
    let c = coordinator(&addr);
    assert_eq!(bm25(&c, probes()[0].clone()).await.hits.len(), 3);
    assert_eq!(ingest(&addr, BATCHES[1]).await, 3);
    assert!(flush(&addr).await);
    // Batch two holds one more "court" document (the courier does not).
    assert_eq!(bm25(&c, probes()[0].clone()).await.hits.len(), 4);
    assert!(!root.exists(), "no conversion: the single image stays one");
    assert!(bm25_sidecar_path(&path).exists());
    handle.abort();
    // A path that somehow has both is refused by name, never merged.
    std::fs::create_dir_all(root.join("segments")).unwrap();
    std::fs::write(
        root.join("segments.json"),
        serde_json::to_vec(&pipestream_search::segments::SegmentSetManifest::default()).unwrap(),
    )
    .unwrap();
    let error = NodeServiceImpl::open(config(Some(path.clone()), Layout::Segments), None, false)
        .err()
        .expect("refused");
    assert!(error.contains("one layout"), "{error}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_tail_seals_itself_at_the_configured_size() {
    let dir = tempdir("autoseal");
    let path = dir.join("auto.tv");
    let (addr, handle) = start_empty_node(NodeConfig {
        seal_tail_docs: 2,
        ..config(Some(path.clone()), Layout::Segments)
    })
    .await;
    assert_eq!(ingest(&addr, BATCHES[0]).await, 3);
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
    assert_eq!(ingest(&addr, BATCHES[1]).await, 3);
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
        ..config(Some(path.clone()), Layout::Segments)
    })
    .await;
    let all: Vec<(&str, &str)> = BATCHES.iter().flat_map(|b| b.iter().copied()).collect();
    assert!(
        all.len() > 2 * 3,
        "the stream must cross the bound at least twice"
    );
    assert_eq!(ingest(&addr, &all).await, all.len() as u64);
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
    let tail = || {
        Bm25Store::with_fields(&["body"])
            .with_facets(&["court"])
            .with_positions(&["body"])
            .with_sentences(&["body"])
    };
    let mut union = SegmentedShard::open(&root, tail()).unwrap();
    let mut whole = tail();
    let mut next = 0u32;
    // Seal the first two batches by hand through the catalog: the same
    // path the node takes, minus the node.
    for batch in &BATCHES[..2] {
        for (text, court) in batch.iter() {
            let doc =
                pipestream_search::analyzer::analyze_document_native(text, Some(&body_spec()))
                    .unwrap();
            let local = next - union.tail_base();
            union
                .add_document(next, text.to_string(), doc.clone(), None)
                .unwrap();
            union.tail_mut().set_facet(0, local, court);
            union.sync_tail();
            whole.add_document(next, text.to_string(), doc);
            whole.set_facet(0, next, court);
            next += 1;
        }
        // Freeze the tail as the node's seal does, and read through the
        // frozen part before its segment exists: the same rows answer.
        let frozen = union
            .freeze_tail(tail(), union.tail().next_doc_id())
            .unwrap();
        let (base, rows, _) = union.frozen().unwrap();
        assert_eq!(u64::from(base) + u64::from(rows), u64::from(next));
        assert_eq!(union.df("court"), whole.df("court"), "frozen part served");
        assert_eq!(
            union
                .facet_ord(0, next - 1)
                .map(|o| union.facet_value(0, o).to_string()),
            whole
                .facet_ord(0, next - 1)
                .map(|o| whole.facet_value(0, o).to_string())
        );
        let stage = dir.join(format!("stage-{}", union.sealed_parts()));
        std::fs::create_dir_all(&stage).unwrap();
        frozen.save(&stage.join("documents.bm25")).unwrap();
        pipestream_search::live_docs::LiveDocs::default()
            .write(&stage.join("live-docs.bin"), u64::from(rows))
            .unwrap();
        let published = union
            .catalog()
            .append(pipestream_search::segments::SegmentSource {
                segment_id: &format!("seg-{}", union.sealed_parts()),
                generation: union.sealed_parts() as u64 + 1,
                base_label: u64::from(base),
                backend_kind: "",
                vector_path: None,
                exact_vector_path: None,
                bm25_path: &stage.join("documents.bm25"),
                live_docs_path: &stage.join("live-docs.bin"),
            })
            .unwrap();
        union.republish(published).unwrap();
        assert!(union.frozen().is_none());
    }
    for (text, court) in BATCHES[2] {
        let doc =
            pipestream_search::analyzer::analyze_document_native(text, Some(&body_spec())).unwrap();
        let local = next - union.tail_base();
        union
            .add_document(next, text.to_string(), doc.clone(), None)
            .unwrap();
        union.tail_mut().set_facet(0, local, court);
        union.sync_tail();
        whole.add_document(next, text.to_string(), doc);
        whole.set_facet(0, next, court);
        next += 1;
    }
    assert_eq!(union.next_doc_id(), whole.next_doc_id());
    assert_eq!(Bm25Index::doc_count(&union), Bm25Index::doc_count(&whole));
    assert_eq!(union.total_doc_length(), whole.total_doc_length());
    for term in ["court", "appeal", "courier", "nothing", "absent"] {
        assert_eq!(union.df(term), whole.df(term), "{term}");
        let mut a = Vec::new();
        union.for_each_posting(term, &mut |doc, tf, offsets| {
            a.push((doc, tf, offsets.to_vec()))
        });
        let mut b = Vec::new();
        whole.for_each_posting(term, &mut |doc, tf, offsets| {
            b.push((doc, tf, offsets.to_vec()))
        });
        assert_eq!(a, b, "{term}");
        for doc in 0..next {
            assert_eq!(union.doc_length(doc), whole.doc_length(doc));
            assert_eq!(
                union.posting_positions(term, doc),
                whole.posting_positions(term, doc)
            );
            assert_eq!(union.text(doc), Bm25Index::text(&whole, doc));
            assert_eq!(union.doc_sentences(doc), whole.doc_sentences(doc));
        }
    }
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
    // Dictionaries: one global ordinal space, value lookups agree.
    let dict: Vec<String> = union.facet_dictionary(0).to_vec();
    assert_eq!(dict[..union.facet_value_count(0).min(5)].len(), 5);
    for doc in 0..next {
        let a = union
            .facet_ord(0, doc)
            .map(|o| union.facet_value(0, o).to_string());
        let b = whole
            .facet_ord(0, doc)
            .map(|o| whole.facet_value(0, o).to_string());
        assert_eq!(a, b, "doc {doc}");
    }
    for court in ["scotus", "ca9", "dcc", "ca5", "nysd", "zzz"] {
        assert_eq!(
            union
                .facet_value_ord_of(0, court)
                .map(|o| union.facet_value(0, o)),
            whole
                .facet_value_ord_of(0, court)
                .map(|o| whole.facet_value(0, o)),
        );
    }
    assert_eq!(
        union.expand_prefix("cour", 64).unwrap(),
        whole.expand_prefix("cour", 64).unwrap()
    );
    assert_eq!(
        union.expand_prefix("cour", 1),
        whole.expand_prefix("cour", 1)
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn in_memory_shards_stay_in_heap_whatever_the_layout_says() {
    // A shard without an index path has no catalog to seal into: it
    // stays the heap store it always was.
    let (addr, handle) = start_empty_node(config(None, Layout::Segments)).await;
    assert_eq!(ingest(&addr, BATCHES[0]).await, 3);
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
    use pipestream_search::pb::{AddVectorsRequest, SearchRequest, SetCalibrationRequest};
    use pipestream_search::segments::VectorLoad;

    const DIM: usize = 64;
    let dir = tempdir("mapped");
    let path = dir.join("mapped.tv");
    let docs: Vec<(&str, &str)> = BATCHES.iter().flat_map(|b| b.iter().copied()).collect();
    let corpus = pipestream_search::harness::unit_vectors(docs.len(), DIM, 0x3A9E_0001);
    let (shift, scale) = pipestream_search::harness::fit_calibration(DIM, 4, &corpus);
    let build = |mmap: bool| NodeConfig {
        vector_mmap: mmap,
        ..config(Some(path.clone()), Layout::Segments)
    };
    let (addr, handle) = start_empty_node(build(true)).await;
    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    client
        .set_calibration(SetCalibrationRequest {
            dim: DIM as u32,
            bit_width: 4,
            shift: shift.clone(),
            scale: scale.clone(),
        })
        .await
        .unwrap();
    assert_eq!(ingest(&addr, &docs).await, docs.len() as u64);
    client
        .add_vectors(tokio_stream::iter(vec![AddVectorsRequest {
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

#[cfg(target_os = "linux")]
fn rss_bytes() -> usize {
    let statm = std::fs::read_to_string("/proc/self/statm").unwrap();
    let resident_pages: usize = statm.split_whitespace().nth(1).unwrap().parse().unwrap();
    resident_pages * 4096
}

/// Opening a catalog whose sealed image is large costs the headers when
/// mapped and the image when loaded; both answer the same top-k.
#[cfg(target_os = "linux")]
#[test]
fn a_mapped_sealed_image_opens_without_the_heap_load() {
    use pipestream_search::exact_vectors::ExactVectorStore;
    use pipestream_search::live_docs::LiveDocs;
    use pipestream_search::postings::AnalyzedDoc;
    use pipestream_search::segments::{SegmentCatalog, SegmentSource, VectorLoad};
    use pipestream_search::vector::{VectorSearchOptions, EMBEDDED_TURBOVEC};

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
        let mut store = Bm25Store::with_fields(&["body"]);
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
            segment_id: "seg-0",
            generation: 1,
            base_label: 0,
            backend_kind: EMBEDDED_TURBOVEC,
            vector_path: Some(&vector_path),
            exact_vector_path: Some(&exact_path),
            bm25_path: &bm25_path,
            live_docs_path: &live_path,
        })
        .unwrap();
    drop(catalog);

    // Both opens verify every artifact and open the BM25 and exact
    // stores; the image is the difference between them.
    let before = rss_bytes();
    let mapped = OpenedSegmentSet::open_with(&root, VectorLoad::Mapped).unwrap();
    let mapped_growth = rss_bytes().saturating_sub(before);
    assert!(mapped.vector(0).unwrap().is_mapped());
    let before = rss_bytes();
    let heap = OpenedSegmentSet::open_with(&root, VectorLoad::Heap).unwrap();
    let heap_growth = rss_bytes().saturating_sub(before);
    assert!(
        mapped_growth < image_bytes / 2,
        "mapped open grew RSS by {mapped_growth} bytes for a {image_bytes}-byte image"
    );
    assert!(
        heap_growth > mapped_growth + image_bytes / 2,
        "a heap load grew RSS by {heap_growth} bytes against {mapped_growth} mapped for a \
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
