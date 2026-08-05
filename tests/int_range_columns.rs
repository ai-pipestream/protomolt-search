//! i64-column and range-facet acceptance tests
//! (`docs/range-facets.md`): kind 4 through the v7 column table with
//! integers that survive past 2^53, count-then-rank range buckets that
//! are exact and additive over the full match set, score chains that
//! read i64 columns with the same bitwise gates as f64 ones, and
//! Timestamp ingest as sugar that lands as epoch micros.

mod common;

use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;
use turbovec_search::bm25::{self, Bm25Params, CorpusStats};
use turbovec_search::coordinator::CoordinatorServiceImpl;
use turbovec_search::node::NodeConfig;
use turbovec_search::pb::node_service_client::NodeServiceClient;
use turbovec_search::pb::search_service_server::SearchService;
use turbovec_search::pb::{
    AddDocumentsRequest, Bm25Hit, Bm25SearchRequest, IntegerValue, RangeFacetField, ScoreOp,
    ScoreStage, TimestampValue,
};
use turbovec_search::postings::{AnalyzedDoc, Bm25Reader, Bm25Store, SpillBuilder};
use turbovec_search::scorefn::{ColumnRef, NumericRead, ScoreChain, Stage, StageOp};

use common::{mock::start_mock_analysis, start_empty_node};

/// A document's integer values at ingest: (field, value) pairs.
type Integers = &'static [(&'static str, i64)];

/// The controlled corpus: eight documents over three shards with a
/// "citations" i64 column. Shard 2 declares NO integer field — the
/// heterogeneous-fleet case, whose documents hold no value and are
/// therefore counted in no bucket, exactly and not by degradation.
///
/// df("rust") = 6 (d0, d1, d2, d3, d5, d6); df("vector") = 2 (d1, d4).
/// Against edges [0, 10, 20, 30] the "rust" match set exercises every
/// boundary rule at once: d0 (0) and d2 (5) fall in [0, 10), d1 sits
/// exactly ON the interior edge 10 and must land in the UPPER bucket,
/// d3 (20) falls in [20, 30), d5 (-1) is below the first edge and lands
/// nowhere, and d6 has no value at all.
const SHARD_DOCS: [&[(&str, Integers)]; 3] = [
    &[
        ("rust search rust fast", &[("citations", 0)]),
        ("vector search rust", &[("citations", 10)]),
        ("rust rust", &[("citations", 5)]),
    ],
    &[
        ("search engines love rust", &[("citations", 20)]),
        ("vector vector vector", &[("citations", 30)]),
        ("rust fast", &[("citations", -1)]),
    ],
    &[("rust", &[]), ("nothing relevant here", &[])],
];

/// The bucket edges every distributed assertion below uses.
const EDGES: [f64; 4] = [0.0, 10.0, 20.0, 30.0];

async fn add_documents_integer(
    addr: &str,
    docs: &[(&str, Integers)],
) -> Result<turbovec_search::pb::AddDocumentsResponse, tonic::Status> {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(8);
    for (text, integers) in docs {
        tx.send(AddDocumentsRequest {
            text: text.to_string(),
            analysis: None,
            lineage: None,
            fields: Vec::new(),
            facets: Vec::new(),
            numerics: Vec::new(),
            map_facets: Vec::new(),
            map_numerics: Vec::new(),
            timestamps: Vec::new(),
            geo_points: Vec::new(),
            integers: integers
                .iter()
                .map(|(field, value)| IntegerValue {
                    field: field.to_string(),
                    value: *value,
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

/// Three shards, the integer table declared on shards 0 and 1 only.
async fn start_integer_shards(
    analysis: &str,
) -> (
    Vec<String>,
    Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
) {
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for (i, _) in SHARD_DOCS.iter().enumerate() {
        let integer_fields = if i < 2 {
            vec!["citations".to_string()]
        } else {
            Vec::new()
        };
        let (addr, handle) = start_empty_node(NodeConfig {
            slot_offset: [0u64, 3, 6][i],
            analysis_addr: Some(analysis.to_string()),
            integer_fields,
            ..Default::default()
        })
        .await;
        addrs.push(addr);
        handles.push(handle);
    }
    for (i, docs) in SHARD_DOCS.iter().enumerate() {
        add_documents_integer(&addrs[i], docs).await.unwrap();
    }
    (addrs, handles)
}

fn range_field(column: &str, edges: &[f64]) -> RangeFacetField {
    RangeFacetField {
        column: column.to_string(),
        key: String::new(),
        edges: edges.to_vec(),
    }
}

/// A range facet's buckets as `(from, to, count)`, for hand-computed
/// comparisons.
fn buckets_of(rf: &turbovec_search::pb::RangeFacetCounts) -> Vec<(f64, f64, u64)> {
    rf.buckets.iter().map(|b| (b.from, b.to, b.count)).collect()
}

fn hit_signature(hits: &[Bm25Hit]) -> Vec<(u64, u32)> {
    hits.iter().map(|h| (h.doc_id, h.score.to_bits())).collect()
}

/// The v7 round-trip for i64 columns: the kinded table entry, min/max
/// metadata, both readers, the heap loader, dual-writer byte identity,
/// and the exactness that motivates the kind — a value above 2^53 comes
/// back as the integer it went in as, which an f64 column cannot do.
#[test]
fn integer_columns_roundtrip_and_dual_writers_agree() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("integer_roundtrip_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let analyzed = |terms: &[(&str, u32)]| {
        AnalyzedDoc::body(
            terms
                .iter()
                .map(|(t, tf)| (t.to_string(), *tf, vec![(0u32, 4u32)]))
                .collect(),
            terms.iter().map(|(_, tf)| tf).sum(),
        )
    };
    // 2^53 + 1 is the first integer an f64 cannot hold: it rounds to
    // 2^53. Doc 1 has no value; doc 2 is negative, so min/max must
    // cover both signs.
    const BEYOND_F64: i64 = (1i64 << 53) + 1;
    const INT_SETS: [(u32, i64); 2] = [(0, BEYOND_F64), (2, -7)];

    // A store carrying all five column kinds at once, to pin the
    // kinded table's ordering with kind 4 appended last.
    let mut store = Bm25Store::with_fields(&["body"])
        .with_facets(&["court"])
        .with_numerics(&["date"])
        .with_map_facets(&["meta"])
        .with_map_numerics(&["attrs"])
        .with_integers(&["citations"]);
    for (i, terms) in [
        &[("rust", 1u32), ("search", 1)][..],
        &[("vector", 1)][..],
        &[("plain", 1)][..],
    ]
    .iter()
    .enumerate()
    {
        store.add_document(i as u32, format!("doc {i}"), analyzed(terms));
    }
    store.set_facet(0, 0, "scotus");
    store.set_numeric(0, 0, 150.5);
    store.set_map_facet(0, 0, "color", "red");
    store.set_map_numeric(0, 1, "boost", 2.0);
    for (d, v) in INT_SETS {
        store.set_integer(0, d, v);
    }
    let heap_path = dir.join("heap.bm25");
    store.save(&heap_path).unwrap();
    let bytes = std::fs::read(&heap_path).unwrap();
    assert_eq!(&bytes[..8], b"TVBM2508", "integer columns opt into the v7-shaped v8 payload");

    let mut builder = SpillBuilder::create_with_fields(&dir.join("spill.build"), &["body"])
        .unwrap()
        .with_facet_fields(&["court"])
        .with_numeric_fields(&["date"])
        .with_map_facet_fields(&["meta"])
        .with_map_numeric_fields(&["attrs"])
        .with_integer_fields(&["citations"])
        .with_buffer_bytes(32);
    for (i, terms) in [
        &[("rust", 1u32), ("search", 1)][..],
        &[("vector", 1)][..],
        &[("plain", 1)][..],
    ]
    .iter()
    .enumerate()
    {
        builder
            .add_document_with_lineage(i as u32, format!("doc {i}"), analyzed(terms), None)
            .unwrap();
    }
    builder.set_facet(0, 0, "scotus");
    builder.set_numeric(0, 0, 150.5);
    builder.set_map_facet(0, 0, "color", "red");
    builder.set_map_numeric(0, 1, "boost", 2.0);
    for (d, v) in INT_SETS {
        builder.set_integer(0, d, v);
    }
    let spill_path = dir.join("spill.bm25");
    builder.finish(&spill_path).unwrap();
    assert_eq!(
        bytes,
        std::fs::read(&spill_path).unwrap(),
        "dual writers must stay byte-identical on integer-bearing stores"
    );

    let reader = Bm25Reader::open(&heap_path).unwrap();
    assert_eq!(reader.integer_count(), 1);
    assert_eq!(reader.integer_name(0), "citations");
    assert_eq!(reader.integer_index("citations"), Some(0));
    assert_eq!(reader.integer_index("citation"), None);
    assert_eq!(reader.integer_value(0, 0), Some(BEYOND_F64));
    assert_eq!(reader.integer_value(0, 1), None, "doc 1 has no value");
    assert_eq!(reader.integer_value(0, 2), Some(-7));
    assert_eq!(reader.integer_min_max(0), (-7, BEYOND_F64));
    // The whole argument for the kind, stated as an assertion: the f64
    // column could not have told 2^53 + 1 from 2^53.
    assert_ne!(BEYOND_F64 as f64 as i64, BEYOND_F64);
    assert_eq!(reader.integer_value(0, 0).unwrap(), 9_007_199_254_740_993);
    // The other kinds still read back beside it.
    assert_eq!(reader.facet_count(), 1);
    assert_eq!(reader.numeric_value(0, 0), Some(150.5));
    let meta = reader.map_facet_index("meta").unwrap();
    let color = reader.map_facet_key_ord(meta, "color").unwrap();
    assert_eq!(
        reader
            .map_facet_value_ord(meta, color, 0)
            .map(|o| reader.map_facet_value(meta, o)),
        Some("red")
    );
    let attrs = reader.map_numeric_index("attrs").unwrap();
    let boost = reader.map_numeric_key_ord(attrs, "boost").unwrap();
    assert_eq!(reader.map_numeric_value(attrs, boost, 1), Some(2.0));

    // The heap loader (the resident-append reload path) recovers the
    // same values and re-derives the same metadata.
    let loaded = Bm25Store::load(&heap_path).unwrap();
    assert_eq!(loaded.integer_index("citations"), Some(0));
    assert_eq!(loaded.integer_value(0, 0), Some(BEYOND_F64));
    assert_eq!(loaded.integer_value(0, 1), None);
    assert_eq!(loaded.integer_value(0, 2), Some(-7));
    assert_eq!(loaded.integer_min_max(0), (-7, BEYOND_F64));

    // Integers alone still opt the shard into v7, and a column no
    // document valued folds to the empty range rather than to a value.
    let mut only_integer = Bm25Store::with_fields(&["body"]).with_integers(&["a", "b"]);
    only_integer.add_document(0, "rust".to_string(), analyzed(&[("rust", 1)]));
    only_integer.set_integer(0, 0, 42);
    let integer_path = dir.join("integer.bm25");
    only_integer.save(&integer_path).unwrap();
    assert_eq!(&std::fs::read(&integer_path).unwrap()[..8], b"TVBM2508");
    let r = Bm25Reader::open(&integer_path).unwrap();
    assert_eq!((r.facet_count(), r.numeric_count(), r.integer_count()), (0, 0, 2));
    assert_eq!(r.integer_min_max(0), (42, 42));
    assert_eq!(
        r.integer_min_max(1),
        (i64::MAX, i64::MIN),
        "a column with no values folds to the empty range, not to a value"
    );
    assert_eq!(r.integer_value(1, 0), None);

    let _ = std::fs::remove_dir_all(&dir);
}

/// Stores that declare SOME column kinds and skip the middle ones. The
/// validator locates each kind group's end by falling through the
/// ABSENT kinds to the next declared group's start, and each skip is a
/// distinct fallback branch a full-kind store never takes: with every
/// kind present the facet group ends at the numeric group, never at
/// the integers. One case per skip boundary into the integer section,
/// with dual-writer identity pinning both writers' offset arithmetic
/// on the same partial layouts. Open runs full validation, so opening
/// IS the tiling assertion.
#[test]
fn partial_kind_stores_tile_and_validate() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("partial_kinds_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let analyzed = || {
        AnalyzedDoc::body(
            vec![("rust".to_string(), 1u32, vec![(0u32, 4u32)])],
            1,
        )
    };
    // (case, facets, numerics, map facets): integers always present,
    // so the cases pin facets->integers, numerics->integers, and
    // map-facets->integers respectively. map-numerics->integers is
    // pinned by the all-kinds store above.
    for (case, facets, numerics, map_facets) in [
        ("facets_int", true, false, false),
        ("numerics_int", false, true, false),
        ("mapfacets_int", false, false, true),
    ] {
        let mut store = Bm25Store::with_fields(&["body"]);
        if facets {
            store = store.with_facets(&["court"]);
        }
        if numerics {
            store = store.with_numerics(&["date"]);
        }
        if map_facets {
            store = store.with_map_facets(&["meta"]);
        }
        let mut store = store.with_integers(&["citations"]);
        store.add_document(0, "doc".to_string(), analyzed());
        if facets {
            store.set_facet(0, 0, "scotus");
        }
        if numerics {
            store.set_numeric(0, 0, 1.5);
        }
        if map_facets {
            store.set_map_facet(0, 0, "color", "red");
        }
        store.set_integer(0, 0, 7);
        let path = dir.join(format!("{case}.bm25"));
        store.save(&path).unwrap();

        let mut builder =
            SpillBuilder::create_with_fields(&dir.join(format!("{case}.build")), &["body"])
                .unwrap();
        if facets {
            builder = builder.with_facet_fields(&["court"]);
        }
        if numerics {
            builder = builder.with_numeric_fields(&["date"]);
        }
        if map_facets {
            builder = builder.with_map_facet_fields(&["meta"]);
        }
        let mut builder = builder.with_integer_fields(&["citations"]);
        builder
            .add_document_with_lineage(0, "doc".to_string(), analyzed(), None)
            .unwrap();
        if facets {
            builder.set_facet(0, 0, "scotus");
        }
        if numerics {
            builder.set_numeric(0, 0, 1.5);
        }
        if map_facets {
            builder.set_map_facet(0, 0, "color", "red");
        }
        builder.set_integer(0, 0, 7);
        let spill_path = dir.join(format!("{case}_spill.bm25"));
        builder.finish(&spill_path).unwrap();
        assert_eq!(
            std::fs::read(&path).unwrap(),
            std::fs::read(&spill_path).unwrap(),
            "{case}: dual writers must agree on partial-kind layouts too"
        );

        let r = Bm25Reader::open(&path).unwrap_or_else(|e| panic!("{case}: {e}"));
        assert_eq!(r.integer_value(0, 0), Some(7), "{case}");
        assert_eq!(r.integer_min_max(0), (7, 7), "{case}");
        if facets {
            assert_eq!(
                r.facet_ord(0, 0).map(|o| r.facet_value(0, o)),
                Some("scotus"),
                "{case}"
            );
        }
        if numerics {
            assert_eq!(r.numeric_value(0, 0), Some(1.5), "{case}");
        }
        if map_facets {
            let ci = r.map_facet_index("meta").unwrap();
            let key = r.map_facet_key_ord(ci, "color").unwrap();
            assert_eq!(
                r.map_facet_value_ord(ci, key, 0).map(|o| r.map_facet_value(ci, o)),
                Some("red"),
                "{case}"
            );
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// Distributed range facets: exact over the full match set, additive
/// across a fleet where one shard lacks the column, unchanged by k, and
/// answering the boundary rules the edge list promises.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_range_facets_are_exact_and_boundary_correct() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addrs, handles) = start_integer_shards(&analysis).await;
    let coordinator = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(Some(analysis.clone()), Default::default());

    // "rust" matches d0 (0), d1 (10), d2 (5), d3 (20), d5 (-1), d6
    // (absent). Hand-computed: [0, 10) holds d0 and d2; [10, 20) holds
    // d1, which sits exactly on the interior edge and belongs to the
    // UPPER bucket; [20, 30) holds d3. d5 is below the first edge and
    // d6 has no value, so neither is counted anywhere.
    let want = vec![range_field("citations", &EDGES)];
    let (hits, _, ranges) = coordinator
        .fanout_bm25_faceted("rust", 10, None, 0.0, &[], &[], &want, &[], &[])
        .await
        .unwrap();
    assert_eq!(hits.len(), 6);
    assert_eq!(ranges.len(), 1);
    assert_eq!(ranges[0].column, "citations");
    assert!(ranges[0].known, "two of three shards have the column");
    assert_eq!(
        buckets_of(&ranges[0]),
        vec![(0.0, 10.0, 2), (10.0, 20.0, 1), (20.0, 30.0, 1)]
    );
    // Additivity: shard 0 contributed [2, 1, 0] and shard 1 [0, 0, 1];
    // the total is 4, and the two uncounted matches are accounted for
    // by the rules, not lost.
    let counted: u64 = ranges[0].buckets.iter().map(|b| b.count).sum();
    assert_eq!(counted, 4);

    // Counts cover the whole match set at k = 1 too: the floor bounds
    // what is surfaced, never what matched.
    let (hits_k1, _, ranges_k1) = coordinator
        .fanout_bm25_faceted("rust", 1, None, 0.0, &[], &[], &want, &[], &[])
        .await
        .unwrap();
    assert_eq!(hits_k1.len(), 1);
    assert_eq!(buckets_of(&ranges_k1[0]), buckets_of(&ranges[0]));

    // A value sitting exactly on the LAST edge lands in no bucket:
    // "vector" matches d1 (10) and d4 (30).
    let (_, _, vector_ranges) = coordinator
        .fanout_bm25_faceted("vector", 10, None, 0.0, &[], &[], &want, &[], &[])
        .await
        .unwrap();
    assert_eq!(
        buckets_of(&vector_ranges[0]),
        vec![(0.0, 10.0, 0), (10.0, 20.0, 1), (20.0, 30.0, 0)],
        "the last edge is exclusive; there is no implicit overflow bucket"
    );

    // The public RPC carries range facets end to end, flat route.
    let resp = SearchService::bm25_search(
        &coordinator,
        Request::new(Bm25SearchRequest {
            text: "rust".to_string(),
            k: 10,
            analysis: None,
            min_score: 0.0,
            fields: Vec::new(),
            facet_fields: Vec::new(),
            score_stages: Vec::new(),
            map_facet_fields: Vec::new(),
            range_facet_fields: want.clone(),
            geo_filters: Vec::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(buckets_of(&resp.range_facets[0]), buckets_of(&ranges[0]));

    // The fused route counts over the union of every leg's terms.
    let fields = vec![turbovec_search::pb::QueryField {
        field: "body".to_string(),
        analysis: None,
        weight: 1.0,
        k1: 0.0,
        b: 0.0,
    }];
    let (_, _, fused_ranges) = coordinator
        .fanout_bm25_fused_faceted("rust", 10, &fields, 0.0, &[], &[], &want, &[])
        .await
        .unwrap();
    assert_eq!(buckets_of(&fused_ranges[0]), buckets_of(&ranges[0]));

    // A column NO shard knows is a typo, not an empty histogram.
    let err = coordinator
        .fanout_bm25_faceted(
            "rust",
            10,
            None,
            0.0,
            &[],
            &[],
            &[range_field("citation", &EDGES)],
            &[],
            &[],
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("citation") && err.message().contains("--integer-fields"),
        "refusal names the column and the knob: {}",
        err.message()
    );

    // Edge lists that do not describe intervals refuse, naming the
    // column — a silently repaired list answers a question nobody asked.
    for (edges, needle) in [
        (vec![10.0, 0.0, 20.0], "strictly ascending"),
        (vec![0.0, 0.0], "strictly ascending"),
        (vec![5.0], "at least 2"),
        (vec![], "at least 2"),
        (vec![0.0, f64::INFINITY], "not finite"),
        (vec![f64::NAN, 1.0], "not finite"),
    ] {
        let err = coordinator
            .fanout_bm25_faceted(
                "rust",
                10,
                None,
                0.0,
                &[],
                &[],
                &[range_field("citations", &edges)],
                &[],
                &[],
            )
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{needle}");
        assert!(
            err.message().contains(needle) && err.message().contains("citations"),
            "expected {needle:?} and the column in: {}",
            err.message()
        );
    }

    // A malformed edge list refuses even when the query analyzes to no
    // terms or asks for k = 0: edge validation needs no shard, so the
    // coordinator's early return must not swallow it into an empty Ok.
    for (text, k) in [("", 10u32), ("rust", 0)] {
        let err = coordinator
            .fanout_bm25_faceted(
                text,
                k,
                None,
                0.0,
                &[],
                &[],
                &[range_field("citations", &[5.0])],
                &[],
                &[],
            )
            .await
            .unwrap_err();
        assert_eq!(
            err.code(),
            tonic::Code::InvalidArgument,
            "text {text:?}, k {k}: the early return must not hide the refusal"
        );
    }
    let err = coordinator
        .fanout_bm25_fused_faceted("", 10, &fields, 0.0, &[], &[], &[range_field(
            "citations",
            &[5.0],
        )], &[])
        .await
        .unwrap_err();
    assert_eq!(
        err.code(),
        tonic::Code::InvalidArgument,
        "the fused route honors the same rule before its own early return"
    );

    for h in handles {
        h.abort();
    }
    mock.abort();
}

/// [`NumericRead`] over an open reader, as the node's shard wrapper
/// provides it in production.
struct ReaderNumerics<'a>(&'a Bm25Reader);
impl NumericRead for ReaderNumerics<'_> {
    fn value(&self, ni: usize, doc_id: u32) -> Option<f64> {
        self.0.numeric_value(ni, doc_id)
    }
    fn map_value(&self, column: usize, key_ord: u32, doc_id: u32) -> Option<f64> {
        self.0.map_numeric_value(column, key_ord, doc_id)
    }
    fn int_value(&self, ii: usize, doc_id: u32) -> Option<i64> {
        self.0.integer_value(ii, doc_id)
    }
    fn geo_value(&self, gi: usize, doc_id: u32) -> Option<(f64, f64)> {
        self.0.geo_value(gi, doc_id)
    }
}

/// The exactness gate for i64-backed chains: on a file-backed shard
/// (impacts present, so the block-max path really prunes) the chained
/// pruned scorer is bitwise identical to the chained exhaustive oracle,
/// with the bound lifted from i64 min/max through the same monotone
/// cast the values take.
#[test]
fn integer_chain_pruned_matches_exhaustive_bitwise() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("integer_chain_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let n = 3000u32;
    let tf_a = |d: u32| 1 + (u64::from(d) * 2654435761 % 7) as u32;
    let cites = |d: u32| i64::from(d) * 48271 % 1000;
    let mut store = Bm25Store::with_fields(&["body"]).with_integers(&["citations", "filed"]);
    for doc in 0..n {
        let mut terms = vec![("a".to_string(), tf_a(doc), Vec::new())];
        if doc % 3 == 0 {
            terms.push(("b".to_string(), 1 + doc % 3, Vec::new()));
        }
        if doc % 61 == 0 {
            terms.push(("c".to_string(), 1, Vec::new()));
        }
        let len: u32 = terms.iter().map(|(_, tf, _)| tf).sum();
        store.add_document(doc, ".".to_string(), AnalyzedDoc::body(terms, len));
        // Every 7th document has no citation count at all.
        if doc % 7 != 0 {
            store.set_integer(0, doc, cites(doc));
        }
        // A second column whose values live past 2^53, where the cast
        // to f64 is lossy: the bound must still dominate, because the
        // cast is monotone even when it rounds.
        if doc % 4 != 0 {
            store.set_integer(1, doc, (1i64 << 53) + i64::from(doc));
        }
    }
    let path = dir.join("chain.bm25");
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    let body = reader.field(0);
    let cols = ReaderNumerics(&reader);
    let citations = reader.integer_index("citations").unwrap();
    let filed = reader.integer_index("filed").unwrap();
    let as_f64 = |(min, max): (i64, i64)| (min as f64, max as f64);
    let chain = ScoreChain {
        stages: vec![
            Stage {
                op: StageOp::AddLinear { weight: -0.002 },
                column: Some(ColumnRef::Integer(citations)),
                min_max: as_f64(reader.integer_min_max(citations)),
            },
            Stage {
                op: StageOp::MultExpDecay {
                    origin: 9.007e15,
                    scale: 2000.0,
                },
                column: Some(ColumnRef::Integer(filed)),
                min_max: as_f64(reader.integer_min_max(filed)),
            },
            Stage {
                op: StageOp::MultLog { weight: 0.4 },
                column: Some(ColumnRef::Integer(citations)),
                min_max: as_f64(reader.integer_min_max(citations)),
            },
        ],
    };
    let ctx = Some((&chain, &cols as &dyn NumericRead));

    let stats = CorpusStats {
        doc_count: u64::from(n),
        total_doc_length: (0..n).map(|d| u64::from(tf_a(d))).sum::<u64>()
            + (0..n).filter(|d| d % 3 == 0).map(|d| u64::from(1 + d % 3)).sum::<u64>()
            + (0..n).filter(|d| d % 61 == 0).count() as u64,
        dfs: vec![n, n.div_ceil(3), n.div_ceil(61)],
    };
    let terms: Vec<String> = ["a", "b", "c"].iter().map(|s| s.to_string()).collect();
    let params = Bm25Params::default();
    let signature = |docs: &[bm25::ScoredDoc]| -> Vec<(u32, u64)> {
        docs.iter().map(|d| (d.doc_id, d.score.to_bits())).collect()
    };
    for k in [1usize, 5, 50] {
        let exhaustive = bm25::top_k_exhaustive_chained(&body, &terms, &stats, params, k, ctx);
        let pruned =
            bm25::top_k_pruned_chained(&body, &terms, &stats, params, k, f64::NEG_INFINITY, ctx);
        assert_eq!(
            signature(&exhaustive),
            signature(&pruned),
            "k={k}: integer-column pruned != exhaustive"
        );
        if let Some(kth) = exhaustive.last() {
            let seeded =
                bm25::top_k_pruned_chained(&body, &terms, &stats, params, k, kth.score, ctx);
            assert_eq!(signature(&exhaustive), signature(&seeded), "k={k}: seeded");
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// A chain over an i64 column, distributed: bitwise equal to the
/// monolith, and a document on the column-less shard passes through
/// unchanged (absence is identity, which is exact).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_integer_chain_matches_monolith() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addrs, handles) = start_integer_shards(&analysis).await;
    let distributed = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(Some(analysis.clone()), Default::default());

    let (mono_addr, mono) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis.clone()),
        integer_fields: vec!["citations".to_string()],
        ..Default::default()
    })
    .await;
    let all: Vec<(&str, Integers)> = SHARD_DOCS.concat();
    add_documents_integer(&mono_addr, &all).await.unwrap();
    let monolithic = CoordinatorServiceImpl::new(vec![mono_addr])
        .with_bm25(Some(analysis.clone()), Default::default());

    let stages = vec![ScoreStage {
        op: ScoreOp::MultLog as i32,
        column: "citations".to_string(),
        key: String::new(),
        weight: 1.0,
        origin: 0.0,
        scale: 0.0,
        origin_lat: 0.0,
        origin_lon: 0.0,
    }];
    for text in ["rust", "search rust", "vector"] {
        let (got, _, _) = distributed
            .fanout_bm25_faceted(text, 10, None, 0.0, &[], &[], &[], &stages, &[])
            .await
            .unwrap();
        let (want, _, _) = monolithic
            .fanout_bm25_faceted(text, 10, None, 0.0, &[], &[], &[], &stages, &[])
            .await
            .unwrap();
        assert_eq!(
            hit_signature(&got),
            hit_signature(&want),
            "query {text:?}: distributed i64 chain != monolithic"
        );
    }

    // The chain does something, and d6 (on the column-less shard) keeps
    // its base score bit for bit.
    let unchained = distributed.fanout_bm25("rust", 10, None).await.unwrap();
    let (chained, _, _) = distributed
        .fanout_bm25_faceted("rust", 10, None, 0.0, &[], &[], &[], &stages, &[])
        .await
        .unwrap();
    assert_eq!(unchained.len(), chained.len(), "same match set");
    assert_ne!(
        hit_signature(&unchained),
        hit_signature(&chained),
        "the chain changed nothing"
    );
    let score = |hits: &[Bm25Hit], id: u64| hits.iter().find(|h| h.doc_id == id).unwrap().score;
    assert_eq!(
        score(&unchained, 6).to_bits(),
        score(&chained, 6).to_bits(),
        "a shard without the column must answer identity"
    );

    for h in handles {
        h.abort();
    }
    mono.abort();
    mock.abort();
}

/// Timestamp ingest is sugar over kind 4: the instant lands in the
/// named i64 column as epoch micros, queryable through range facets
/// with hand-computed micro edges, and every conversion that could lie
/// refuses instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timestamps_land_as_epoch_micros_in_the_integer_column() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addr, handle) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis.clone()),
        integer_fields: vec!["filed_at".to_string()],
        ..Default::default()
    })
    .await;

    // 2024-01-01T00:00:00Z and 2024-07-01T00:00:00Z, plus one instant
    // before the epoch. Hand-computed epoch micros: seconds * 1e6 plus
    // the nanos floor-divided by 1000.
    const JAN_2024: i64 = 1_704_067_200;
    const JUL_2024: i64 = 1_719_792_000;
    const PRE_EPOCH: i64 = -1_000;
    let docs: [(&str, i64, i32); 3] = [
        ("rust search rust fast", JAN_2024, 0),
        ("vector search rust", JUL_2024, 500_999),
        ("rust rust", PRE_EPOCH, 0),
    ];
    let send = |addr: String, reqs: Vec<AddDocumentsRequest>| async move {
        let mut client = NodeServiceClient::connect(addr).await.unwrap();
        let (tx, rx) = mpsc::channel(8);
        for r in reqs {
            tx.send(r).await.unwrap();
        }
        drop(tx);
        client.add_documents(ReceiverStream::new(rx)).await.map(|_| ())
    };
    let stamped = |text: &str, seconds: i64, nanos: i32| AddDocumentsRequest {
        text: text.to_string(),
        analysis: None,
        lineage: None,
        fields: Vec::new(),
        facets: Vec::new(),
        numerics: Vec::new(),
        map_facets: Vec::new(),
        map_numerics: Vec::new(),
        integers: Vec::new(),
        timestamps: vec![TimestampValue {
            field: "filed_at".to_string(),
            value: Some(prost_types::Timestamp { seconds, nanos }),
        }],
        geo_points: Vec::new(),
    };
    send(
        addr.clone(),
        docs.iter().map(|(t, s, n)| stamped(t, *s, *n)).collect(),
    )
    .await
    .unwrap();

    let coordinator = CoordinatorServiceImpl::new(vec![addr.clone()])
        .with_bm25(Some(analysis.clone()), Default::default());
    // Edges at 2024-01-01 and 2024-07-01, in micros. The pre-epoch
    // document falls below the first edge and lands nowhere; the July
    // document sits exactly on the last edge and also lands nowhere;
    // only January falls in the single bucket.
    let edges = vec![
        (JAN_2024 * 1_000_000) as f64,
        (JUL_2024 * 1_000_000) as f64,
    ];
    let (hits, _, ranges) = coordinator
        .fanout_bm25_faceted(
            "rust",
            10,
            None,
            0.0,
            &[],
            &[],
            &[range_field("filed_at", &edges)],
            &[],
            &[],
        )
        .await
        .unwrap();
    assert_eq!(hits.len(), 3);
    assert_eq!(
        buckets_of(&ranges[0]),
        vec![(edges[0], edges[1], 1)],
        "only the January document falls inside [Jan, Jul)"
    );

    // Widening the upper edge by one microsecond brings July in: the
    // 500_999 ns remainder truncated to 500 micros, so the stored value
    // is JUL * 1e6 + 500, above the old edge and below the new one.
    let wider = vec![edges[0], edges[1] + 1_000.0];
    let (_, _, wide_ranges) = coordinator
        .fanout_bm25_faceted(
            "rust",
            10,
            None,
            0.0,
            &[],
            &[],
            &[range_field("filed_at", &wider)],
            &[],
            &[],
        )
        .await
        .unwrap();
    assert_eq!(buckets_of(&wide_ranges[0]), vec![(wider[0], wider[1], 2)]);

    // Refusals: an unknown field, an overflowing instant, and a field
    // valued by BOTH lists in one document (they name the same column).
    let mut unknown = stamped("some text", JAN_2024, 0);
    unknown.timestamps[0].field = "filed".to_string();
    let err = send(addr.clone(), vec![unknown]).await.unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("unknown integer field")
            && err.message().contains("--integer-fields"),
        "{}",
        err.message()
    );

    let overflow = stamped("some text", i64::MAX, 0);
    let err = send(addr.clone(), vec![overflow]).await.unwrap_err();
    assert!(
        err.message().contains("does not fit i64 epoch micros"),
        "{}",
        err.message()
    );

    let bad_nanos = stamped("some text", JAN_2024, -1);
    let err = send(addr.clone(), vec![bad_nanos]).await.unwrap_err();
    assert!(err.message().contains("nanos"), "{}", err.message());

    let mut both = stamped("some text", JAN_2024, 0);
    both.integers.push(IntegerValue {
        field: "filed_at".to_string(),
        value: 1,
    });
    let err = send(addr.clone(), vec![both]).await.unwrap_err();
    assert!(
        err.message().contains("repeats") && err.message().contains("timestamps"),
        "integers and timestamps share the column, so a repeat spans both: {}",
        err.message()
    );

    handle.abort();
    mock.abort();
}

/// The integer ingest refusal matrix: unknown field, repeat, and the
/// absence sentinel, each refused before anything mutates.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn integer_ingest_refusals_are_loud() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addrs, handles) = start_integer_shards(&analysis).await;

    let cases: &[(Integers, &str)] = &[
        (&[("cite", 1)], "unknown integer field"),
        (&[("citations", 1), ("citations", 2)], "repeats"),
        (&[("citations", i64::MIN)], "absence sentinel"),
    ];
    for (integers, needle) in cases {
        let err = add_documents_integer(&addrs[0], &[("some text", integers)])
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{needle}");
        assert!(
            err.message().contains(needle),
            "expected {needle:?} in: {}",
            err.message()
        );
    }
    // The unknown-field refusal names the knob, like every other column
    // typo refusal.
    let err = add_documents_integer(&addrs[0], &[("some text", &[("cite", 1)])])
        .await
        .unwrap_err();
    assert!(err.message().contains("--integer-fields"), "{}", err.message());

    // A refused document left nothing behind: the shard still answers
    // the same range counts it did before.
    let coordinator = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(Some(analysis.clone()), Default::default());
    let (_, _, ranges) = coordinator
        .fanout_bm25_faceted(
            "rust",
            10,
            None,
            0.0,
            &[],
            &[],
            &[range_field("citations", &EDGES)],
            &[],
            &[],
        )
        .await
        .unwrap();
    assert_eq!(
        buckets_of(&ranges[0]),
        vec![(0.0, 10.0, 2), (10.0, 20.0, 1), (20.0, 30.0, 1)]
    );

    for h in handles {
        h.abort();
    }
    mock.abort();
}
