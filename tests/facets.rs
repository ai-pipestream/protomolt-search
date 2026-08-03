//! Facet acceptance tests (`docs/plans/track-1-features.md` section 2):
//! dictionary-encoded facet columns through the v7 file format, and
//! count-then-rank facet counts over the FULL match set — exact,
//! additive across shards, independent of k and the seeded floor, with
//! the unknown-field refusal and the heterogeneous-fleet tolerance
//! mirroring the multi-field rules.

mod common;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use turbovec_search::coordinator::CoordinatorServiceImpl;
use turbovec_search::node::NodeConfig;
use turbovec_search::pb::search_service_server::SearchService;
use turbovec_search::pb::node_service_client::NodeServiceClient;
use turbovec_search::pb::{
    AddDocumentsRequest, Bm25QueryRequest, Bm25SearchRequest, FacetValue, FlushRequest,
};
use turbovec_search::postings::{AnalyzedDoc, Bm25Reader, Bm25Store, SpillBuilder};

use common::{mock::start_mock_analysis, start_empty_node};

/// A document's facet values at ingest: (field, value) pairs.
type Facets = &'static [(&'static str, &'static str)];

/// A document's body terms for direct (analyzer-less) store writes:
/// (term, tf) pairs.
type Terms = Vec<(&'static str, u32)>;

/// The controlled corpus: six documents over three shards, with court
/// and year facet values. Shard 2 declares NO facet fields — the
/// heterogeneous-fleet case — so its matching documents legitimately
/// contribute no counts.
///
/// df("rust") = 4 (d0, d1, d2, d4), df("vector") = 2 (d1, d3).
const SHARD_DOCS: [&[(&str, Facets)]; 3] = [
    &[
        ("rust search rust fast", &[("court", "scotus"), ("year", "1990")]),
        ("vector search rust", &[("court", "ca9"), ("year", "1991")]),
    ],
    &[
        ("search engines love rust", &[("court", "ca9")]),
        ("vector vector vector", &[("court", "scotus"), ("year", "1990")]),
    ],
    &[("rust", &[]), ("nothing relevant here", &[])],
];

async fn add_documents_faceted(
    addr: &str,
    docs: &[(&str, &[(&str, &str)])],
) -> Result<turbovec_search::pb::AddDocumentsResponse, tonic::Status> {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(8);
    for (text, facets) in docs {
        tx.send(AddDocumentsRequest {
            text: text.to_string(),
            analysis: None,
            lineage: None,
            fields: Vec::new(),
            facets: facets
                .iter()
                .map(|(field, value)| FacetValue {
                    field: field.to_string(),
                    value: value.to_string(),
                })
                .collect(),
        })
        .await
        .unwrap();
    }
    drop(tx);
    client
        .add_documents(ReceiverStream::new(rx))
        .await
        .map(|r| r.into_inner())
}

/// Three shards, facet tables declared on shards 0 and 1 only.
async fn start_faceted_shards(
    analysis: &str,
) -> (
    Vec<String>,
    Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
) {
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for (i, _) in SHARD_DOCS.iter().enumerate() {
        let facet_fields = if i < 2 {
            vec!["court".to_string(), "year".to_string()]
        } else {
            Vec::new()
        };
        let (addr, handle) = start_empty_node(NodeConfig {
            slot_offset: [0u64, 2, 4][i],
            analysis_addr: Some(analysis.to_string()),
            facet_fields,
            ..Default::default()
        })
        .await;
        addrs.push(addr);
        handles.push(handle);
    }
    for (i, docs) in SHARD_DOCS.iter().enumerate() {
        add_documents_faceted(&addrs[i], docs).await.unwrap();
    }
    (addrs, handles)
}

fn counts_of(ff: &turbovec_search::pb::FacetFieldCounts) -> Vec<(&str, u64)> {
    ff.counts
        .iter()
        .map(|c| (c.value.as_str(), c.count))
        .collect()
}

/// The v7 file round-trip: a facet-bearing store persists as v7, both
/// readers (mmap and heap) recover the columns exactly, the dual
/// writers stay byte-identical, and a facet-less store still writes v6
/// bytes — the format break is opt-in per shard.
#[test]
fn facet_columns_roundtrip_and_dual_writers_agree() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("facet_roundtrip_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let docs: Vec<(&str, Terms, Facets)> = vec![
        ("rust search", vec![("rust", 1), ("search", 1)], &[("court", "scotus"), ("year", "1990")]),
        ("vector rust", vec![("rust", 1), ("vector", 1)], &[("court", "ca9")]),
        ("plain text", vec![("plain", 1), ("text", 1)], &[]),
    ];
    let analyzed = |terms: &[(&str, u32)]| {
        AnalyzedDoc::body(
            terms
                .iter()
                .map(|(t, tf)| (t.to_string(), *tf, vec![(0u32, 4u32)]))
                .collect(),
            terms.iter().map(|(_, tf)| tf).sum(),
        )
    };

    let mut store = Bm25Store::with_fields(&["body"]).with_facets(&["court", "year"]);
    for (i, (text, terms, facets)) in docs.iter().enumerate() {
        store.add_document(i as u32, text.to_string(), analyzed(terms));
        for (field, value) in *facets {
            let fi = store.facet_index(field).unwrap();
            store.set_facet(fi, i as u32, value);
        }
    }
    let heap_path = dir.join("heap.bm25");
    store.save(&heap_path).unwrap();
    let bytes = std::fs::read(&heap_path).unwrap();
    assert_eq!(&bytes[..8], b"TVBM2507", "facet-bearing stores write v7");

    // The spill builder produces the same bytes.
    let mut builder = SpillBuilder::create_with_fields(&dir.join("spill.build"), &["body"])
        .unwrap()
        .with_facet_fields(&["court", "year"])
        .with_buffer_bytes(32); // force multi-run merges
    for (i, (text, terms, facets)) in docs.iter().enumerate() {
        builder
            .add_document_with_lineage(i as u32, text.to_string(), analyzed(terms), None)
            .unwrap();
        for (field, value) in *facets {
            let fi = builder.facet_index(field).unwrap();
            builder.set_facet(fi, i as u32, value);
        }
    }
    let spill_path = dir.join("spill.bm25");
    builder.finish(&spill_path).unwrap();
    assert_eq!(
        bytes,
        std::fs::read(&spill_path).unwrap(),
        "dual writers must stay byte-identical on facet-bearing stores"
    );

    // The mmap reader recovers the columns.
    let reader = Bm25Reader::open(&heap_path).unwrap();
    assert_eq!(reader.facet_count(), 2);
    assert_eq!(reader.facet_name(0), "court");
    assert_eq!(reader.facet_index("year"), Some(1));
    assert_eq!(reader.facet_index("bogus"), None);
    let court = reader.facet_index("court").unwrap();
    assert_eq!(reader.facet_value_count(court), 2);
    let ord0 = reader.facet_ord(court, 0).unwrap();
    let ord1 = reader.facet_ord(court, 1).unwrap();
    assert_eq!(reader.facet_value(court, ord0), "scotus");
    assert_eq!(reader.facet_value(court, ord1), "ca9");
    assert_eq!(reader.facet_ord(court, 2), None, "doc 2 has no court");
    let year = reader.facet_index("year").unwrap();
    assert_eq!(reader.facet_ord(year, 1), None, "doc 1 has no year");
    assert_eq!(
        reader.facet_value(year, reader.facet_ord(year, 0).unwrap()),
        "1990"
    );

    // The heap loader recovers them too (append path reload).
    let loaded = Bm25Store::load(&heap_path).unwrap();
    assert_eq!(loaded.facet_count(), 2);
    assert_eq!(
        loaded.facet_value(court, loaded.facet_ord(court, 1).unwrap()),
        "ca9"
    );

    // A facet-less store still writes v6, byte for byte the old format.
    let mut plain = Bm25Store::with_fields(&["body"]);
    plain.add_document(0, "rust".to_string(), analyzed(&[("rust", 1)]));
    let plain_path = dir.join("plain.bm25");
    plain.save(&plain_path).unwrap();
    assert_eq!(
        &std::fs::read(&plain_path).unwrap()[..8],
        b"TVBM2506",
        "facet-less stores keep the v6 format"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// Distributed facet counts are the exact per-value sums over the full
/// match set: hand-computable on the controlled corpus, additive
/// across shards, unchanged by k and by a seeded floor (the floor
/// bounds what is surfaced, never what matched), and tolerant of a
/// shard with no facet table (its matches legitimately count nothing).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn facet_counts_are_exact_additive_and_floor_independent() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addrs, handles) = start_faceted_shards(&analysis).await;
    let coordinator = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(Some(analysis.clone()), Default::default());
    let want = vec!["court".to_string(), "year".to_string()];

    // "rust" matches d0, d1, d2, d4. Courts: ca9 (d1, d2), scotus (d0);
    // d4 sits on the facet-less shard. Years: 1990 (d0), 1991 (d1) —
    // d2 has no year value.
    let (hits, facets) = coordinator
        .fanout_bm25_faceted("rust", 6, None, 0.0, &want)
        .await
        .unwrap();
    assert_eq!(hits.len(), 4);
    assert_eq!(facets.len(), 2);
    assert_eq!(facets[0].field, "court");
    assert!(facets[0].known);
    assert_eq!(counts_of(&facets[0]), vec![("ca9", 2), ("scotus", 1)]);
    assert_eq!(facets[1].field, "year");
    assert_eq!(
        counts_of(&facets[1]),
        vec![("1990", 1), ("1991", 1)],
        "equal counts tie-break by value ascending"
    );

    // Counts cover the whole match set even when k surfaces one hit.
    let (hits_k1, facets_k1) = coordinator
        .fanout_bm25_faceted("rust", 1, None, 0.0, &want)
        .await
        .unwrap();
    assert_eq!(hits_k1.len(), 1);
    assert_eq!(counts_of(&facets_k1[0]), vec![("ca9", 2), ("scotus", 1)]);

    // A seeded floor narrows the surfaced hits, never the counts.
    let seed = turbovec_search::bm25::floor_seed(hits[0].score);
    let (seeded_hits, seeded_facets) = coordinator
        .fanout_bm25_faceted("rust", 6, None, seed, &want)
        .await
        .unwrap();
    assert!(seeded_hits.len() < hits.len(), "the floor trimmed hits");
    assert_eq!(counts_of(&seeded_facets[0]), vec![("ca9", 2), ("scotus", 1)]);
    assert_eq!(counts_of(&seeded_facets[1]), vec![("1990", 1), ("1991", 1)]);

    // "vector" matches d1, d3: all courts tie, value order decides.
    let (_, vector_facets) = coordinator
        .fanout_bm25_faceted("vector", 6, None, 0.0, &want)
        .await
        .unwrap();
    assert_eq!(counts_of(&vector_facets[0]), vec![("ca9", 1), ("scotus", 1)]);

    // The fused route counts the same match set (one body leg).
    let fields = vec![turbovec_search::pb::QueryField {
        field: "body".to_string(),
        analysis: None,
        weight: 1.0,
        k1: 0.0,
        b: 0.0,
    }];
    let (fused_hits, fused_facets) = coordinator
        .fanout_bm25_fused_faceted("rust", 6, &fields, 0.0, &want)
        .await
        .unwrap();
    assert_eq!(fused_hits.len(), 4);
    assert_eq!(counts_of(&fused_facets[0]), vec![("ca9", 2), ("scotus", 1)]);

    for h in handles {
        h.abort();
    }
    mock.abort();
}

/// The public Bm25Search RPC carries facet fields out and merged counts
/// back; a facet field NO shard knows is refused loudly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bm25_search_rpc_carries_facets_and_refuses_unknown_fields() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addrs, handles) = start_faceted_shards(&analysis).await;
    let coordinator = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(Some(analysis.clone()), Default::default());

    let resp = SearchService::bm25_search(
        &coordinator,
        Request::new(Bm25SearchRequest {
            text: "rust".to_string(),
            k: 6,
            analysis: None,
            min_score: 0.0,
            fields: Vec::new(),
            facet_fields: vec!["court".to_string()],
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(resp.hits.len(), 4);
    assert_eq!(resp.facets.len(), 1);
    assert_eq!(counts_of(&resp.facets[0]), vec![("ca9", 2), ("scotus", 1)]);

    // A typo'd facet field must refuse, not answer zero everywhere.
    let err = SearchService::bm25_search(
        &coordinator,
        Request::new(Bm25SearchRequest {
            text: "rust".to_string(),
            k: 6,
            analysis: None,
            min_score: 0.0,
            fields: Vec::new(),
            facet_fields: vec!["cuort".to_string()],
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("cuort") && err.message().contains("--facet-fields"),
        "refusal names the field and the knob: {}",
        err.message()
    );

    for h in handles {
        h.abort();
    }
    mock.abort();
}

/// Ingest validation refuses unknown facet fields, repeats, and empty
/// values — before anything mutates.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn facet_ingest_validation_refuses_bad_values() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addr, node) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis),
        facet_fields: vec!["court".to_string()],
        ..Default::default()
    })
    .await;

    let cases: &[(&[(&str, &str)], &str)] = &[
        (&[("bogus", "x")], "unknown facet field"),
        (&[("court", "a"), ("court", "b")], "repeats"),
        (&[("court", "")], "empty value"),
    ];
    for (facets, needle) in cases {
        let err = add_documents_faceted(&addr, &[("some text", facets)])
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{needle}");
        assert!(
            err.message().contains(needle),
            "expected {needle:?} in: {}",
            err.message()
        );
    }
    // The refused documents never entered the store.
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    let stats = client
        .term_stats(turbovec_search::pb::TermStatsRequest {
            terms: vec!["some".into()],
            fields: Vec::new(),
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(stats.doc_count, 0);

    node.abort();
    mock.abort();
}

/// The spill-builder path: a persisted shard ingests facet values while
/// spilling, flushes a v7 file, and serves counts from the mmap reader.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spilled_shard_serves_facets_after_flush() {
    let (analysis, mock) = start_mock_analysis().await;
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("facet_spill_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let index_path = dir.join("shard.tv");
    let (addr, node) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis),
        index_path: Some(index_path.clone()),
        facet_fields: vec!["court".to_string(), "year".to_string()],
        ..Default::default()
    })
    .await;
    add_documents_faceted(&addr, SHARD_DOCS[0]).await.unwrap();

    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    let flushed = client.flush(FlushRequest {}).await.unwrap().into_inner();
    assert!(flushed.written);
    let bm25_path = turbovec_search::node::bm25_sidecar_path(&index_path);
    let mut magic = [0u8; 8];
    use std::io::Read;
    std::fs::File::open(&bm25_path)
        .unwrap()
        .read_exact(&mut magic)
        .unwrap();
    assert_eq!(&magic, b"TVBM2507", "flushed facet shard is v7");

    // Query the resident shard with hand-supplied globals; the counts
    // come from the mmapped ords column.
    let resp = client
        .bm25_query(Bm25QueryRequest {
            terms: vec!["rust".into()],
            k: 10,
            global_doc_count: 2,
            global_total_doc_length: 7,
            global_doc_frequencies: vec![2],
            k1: 0.0,
            b: 0.0,
            min_score: 0.0,
            fields: Vec::new(),
            expected_stats_epoch: 0,
            facet_fields: vec!["court".to_string()],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.hits.len(), 2);
    assert_eq!(resp.facets.len(), 1);
    assert!(resp.facets[0].known);
    assert_eq!(counts_of(&resp.facets[0]), vec![("scotus", 1), ("ca9", 1)]);

    node.abort();
    mock.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
