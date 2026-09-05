//! Map-column acceptance tests (`docs/map-columns.md`): first-class
//! map<string, string> and map<string, f64> columns through the v7
//! kinded column table — one column per map regardless of key
//! cardinality, exact per-(column, key) facet counts, map-keyed score
//! stages with per-key bounds, and the loud key-level typo rules.

mod common;

use pipestream_search::bm25::{self, Bm25Params, CorpusStats};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    AddDocumentsRequest, Bm25Hit, Bm25SearchRequest, MapFacetEntry, MapFacetField, MapNumericEntry,
    ScoreOp, ScoreStage,
};
use pipestream_search::postings::{AnalyzedDoc, Bm25Reader, Bm25Store, SpillBuilder};
use pipestream_search::scorefn::{ColumnRef, NumericRead, ScoreChain, Stage, StageOp};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

use common::{mock::start_mock_analysis, start_empty_node};

/// A document's map entries at ingest: string entries and f64 entries,
/// as (key, value) pairs in the "meta" / "attrs" columns.
type StrEntries = &'static [(&'static str, &'static str)];
type NumEntries = &'static [(&'static str, f64)];

/// Six documents over three shards. Shard 2 declares NO map tables —
/// the heterogeneous-fleet case. df("rust") = 4 (d0, d1, d2, d4).
const SHARD_DOCS: [&[(&str, StrEntries, NumEntries)]; 3] = [
    &[
        (
            "rust search rust fast",
            &[("color", "red"), ("lang", "en")],
            &[("boost", 2.0)],
        ),
        (
            "vector search rust",
            &[("color", "blue")],
            &[("boost", 1.0)],
        ),
    ],
    &[
        (
            "search engines love rust",
            &[("color", "red"), ("lang", "de")],
            &[],
        ),
        ("vector vector vector", &[("lang", "en")], &[("boost", 3.0)]),
    ],
    &[("rust", &[], &[]), ("nothing relevant here", &[], &[])],
];

async fn add_documents_mapped(
    addr: &str,
    docs: &[(&str, StrEntries, NumEntries)],
) -> Result<pipestream_search::pb::AddDocumentsResponse, tonic::Status> {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(8);
    for (text, strs, nums) in docs {
        tx.send(AddDocumentsRequest {
            original_source: None,
            source_chunk_ordinal: None,
            identity: None,
            collection: String::new(),
            cased_field: String::new(),
            sentence_fields: Vec::new(),
            materialize: None,
            text: text.to_string(),
            analysis: None,
            lineage: None,
            fields: Vec::new(),
            facets: Vec::new(),
            numerics: Vec::new(),
            map_facets: strs
                .iter()
                .map(|(key, value)| MapFacetEntry {
                    field: "meta".to_string(),
                    key: key.to_string(),
                    value: value.to_string(),
                })
                .collect(),
            map_numerics: nums
                .iter()
                .map(|(key, value)| MapNumericEntry {
                    field: "attrs".to_string(),
                    key: key.to_string(),
                    value: *value,
                })
                .collect(),
            integers: Vec::new(),
            timestamps: Vec::new(),
            geo_points: Vec::new(),
            quality: None,
            geography: None,
            phrases: Vec::new(),
            phrase_fingerprint: 0,
            phrase_field: String::new(),
            position_fields: Vec::new(),
            bigram_fields: Vec::new(),
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

/// Three shards; map tables declared on shards 0 and 1 only.
async fn start_mapped_shards(
    analysis: &str,
) -> (
    Vec<String>,
    Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
) {
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for (i, _) in SHARD_DOCS.iter().enumerate() {
        let (map_facet_fields, map_numeric_fields) = if i < 2 {
            (vec!["meta".to_string()], vec!["attrs".to_string()])
        } else {
            (Vec::new(), Vec::new())
        };
        let (addr, handle) = start_empty_node(NodeConfig {
            slot_offset: [0u64, 2, 4][i],
            analysis_addr: Some(analysis.to_string()),
            map_facet_fields,
            map_numeric_fields,
            ..Default::default()
        })
        .await;
        addrs.push(addr);
        handles.push(handle);
    }
    for (i, docs) in SHARD_DOCS.iter().enumerate() {
        add_documents_mapped(&addrs[i], docs).await.unwrap();
    }
    (addrs, handles)
}

fn map_field(column: &str, key: &str) -> MapFacetField {
    MapFacetField {
        column: column.to_string(),
        key: key.to_string(),
    }
}

fn counts_of(ff: &pipestream_search::pb::FacetFieldCounts) -> Vec<(&str, u64)> {
    ff.counts
        .iter()
        .map(|c| (c.value.as_str(), c.count))
        .collect()
}

fn hit_signature(hits: &[Bm25Hit]) -> Vec<(u64, u32)> {
    hits.iter().map(|h| (h.doc_id, h.score.to_bits())).collect()
}

/// The v7 round-trip for both map kinds: kinded table entries, both
/// dictionaries, per-key min/max metadata, pair lists through both
/// readers, and dual-writer byte identity.
#[test]
fn map_columns_roundtrip_and_dual_writers_agree() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("map_roundtrip_{}", std::process::id()));
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
    // Doc 1 has no entries at all; doc 2 shares the "color" key with
    // doc 0 but not the value.
    const FACET_SETS: [(u32, &str, &str); 3] =
        [(0, "color", "red"), (0, "lang", "en"), (2, "color", "blue")];
    const NUM_SETS: [(u32, &str, f64); 3] =
        [(0, "boost", 2.5), (2, "boost", -1.0), (2, "rank", 7.0)];

    let mut store = Bm25Store::with_fields(&["body"])
        .with_map_facets(&["meta"])
        .with_map_numerics(&["attrs"]);
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
    for (d, k, v) in FACET_SETS {
        store.set_map_facet(0, d, k, v);
    }
    for (d, k, v) in NUM_SETS {
        store.set_map_numeric(0, d, k, v);
    }
    let heap_path = dir.join("heap.bm25");
    store.save(&heap_path).unwrap();
    let bytes = std::fs::read(&heap_path).unwrap();
    assert_eq!(
        &bytes[..8],
        b"TVBM2508",
        "map columns opt into the v7-shaped v8 payload"
    );

    let mut builder = SpillBuilder::create_with_fields(&dir.join("spill.build"), &["body"])
        .unwrap()
        .with_map_facet_fields(&["meta"])
        .with_map_numeric_fields(&["attrs"])
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
    for (d, k, v) in FACET_SETS {
        builder.set_map_facet(0, d, k, v);
    }
    for (d, k, v) in NUM_SETS {
        builder.set_map_numeric(0, d, k, v);
    }
    let spill_path = dir.join("spill.bm25");
    builder.finish(&spill_path).unwrap();
    assert_eq!(
        bytes,
        std::fs::read(&spill_path).unwrap(),
        "dual writers must stay byte-identical on map-bearing stores"
    );

    let reader = Bm25Reader::open(&heap_path).unwrap();
    let meta = reader.map_facet_index("meta").unwrap();
    assert_eq!(reader.map_facet_index("nope"), None);
    let color = reader.map_facet_key_ord(meta, "color").unwrap();
    let lang = reader.map_facet_key_ord(meta, "lang").unwrap();
    assert_eq!(reader.map_facet_key_ord(meta, "colour"), None);
    assert_eq!(reader.map_facet_value_count(meta), 3); // red, en, blue
    let val = |ord: Option<u32>| ord.map(|o| reader.map_facet_value(meta, o).to_string());
    assert_eq!(
        val(reader.map_facet_value_ord(meta, color, 0)).as_deref(),
        Some("red")
    );
    assert_eq!(
        val(reader.map_facet_value_ord(meta, lang, 0)).as_deref(),
        Some("en")
    );
    assert_eq!(
        reader.map_facet_value_ord(meta, color, 1),
        None,
        "doc 1 empty"
    );
    assert_eq!(
        val(reader.map_facet_value_ord(meta, color, 2)).as_deref(),
        Some("blue")
    );
    assert_eq!(reader.map_facet_value_ord(meta, lang, 2), None);

    let attrs = reader.map_numeric_index("attrs").unwrap();
    let boost = reader.map_numeric_key_ord(attrs, "boost").unwrap();
    let rank = reader.map_numeric_key_ord(attrs, "rank").unwrap();
    assert_eq!(reader.map_numeric_value(attrs, boost, 0), Some(2.5));
    assert_eq!(reader.map_numeric_value(attrs, boost, 1), None);
    assert_eq!(reader.map_numeric_value(attrs, boost, 2), Some(-1.0));
    assert_eq!(reader.map_numeric_value(attrs, rank, 2), Some(7.0));
    assert_eq!(
        reader.map_numeric_key_min_max(attrs, boost),
        (-1.0, 2.5),
        "per-key min/max metadata"
    );
    assert_eq!(reader.map_numeric_key_min_max(attrs, rank), (7.0, 7.0));

    // The heap loader (resident-append reload path) recovers the same.
    let loaded = Bm25Store::load(&heap_path).unwrap();
    let lmeta = loaded.map_facet_index("meta").unwrap();
    let lcolor = loaded.map_facet_key_ord(lmeta, "color").unwrap();
    assert_eq!(
        loaded
            .map_facet_value_ord(lmeta, lcolor, 2)
            .map(|o| loaded.map_facet_value(lmeta, o)),
        Some("blue")
    );
    let lattrs = loaded.map_numeric_index("attrs").unwrap();
    let lboost = loaded.map_numeric_key_ord(lattrs, "boost").unwrap();
    assert_eq!(loaded.map_numeric_key_min_max(lattrs, lboost), (-1.0, 2.5));

    let _ = std::fs::remove_dir_all(&dir);
}

/// Distributed per-(column, key) counts: exact, additive across a fleet
/// where one shard has no map tables, unchanged by k, tolerant of a
/// partially-known key, and refusing a key NO shard knows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn map_facet_counts_are_exact_with_key_level_typo_rules() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addrs, handles) = start_mapped_shards(&analysis).await;
    let coordinator = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(Some(analysis.clone()), Default::default());

    // "rust" matches d0, d1, d2, d4. meta[color]: red (d0, d2), blue
    // (d1); meta[lang]: en (d0), de (d2) — value-ascending on the tie.
    let want = vec![map_field("meta", "color"), map_field("meta", "lang")];
    let (hits, facets, _) = coordinator
        .fanout_bm25_faceted("rust", 6, None, 0.0, &[], &want, &[], &[], &[], None)
        .await
        .unwrap();
    assert_eq!(hits.len(), 4);
    assert_eq!(facets.len(), 2);
    assert_eq!(
        (facets[0].field.as_str(), facets[0].key.as_str()),
        ("meta", "color")
    );
    assert!(facets[0].known);
    assert_eq!(counts_of(&facets[0]), vec![("red", 2), ("blue", 1)]);
    assert_eq!(
        (facets[1].field.as_str(), facets[1].key.as_str()),
        ("meta", "lang")
    );
    assert_eq!(counts_of(&facets[1]), vec![("de", 1), ("en", 1)]);

    // Counts cover the whole match set at k = 1 too.
    let (hits_k1, facets_k1, _) = coordinator
        .fanout_bm25_faceted("rust", 1, None, 0.0, &[], &want, &[], &[], &[], None)
        .await
        .unwrap();
    assert_eq!(hits_k1.len(), 1);
    assert_eq!(counts_of(&facets_k1[0]), vec![("red", 2), ("blue", 1)]);

    // The public RPC carries map facets alongside hits.
    let resp = SearchService::bm25_search(
        &coordinator,
        Request::new(Bm25SearchRequest {
            collection: String::new(),
            highlight: None,
            projections: Vec::new(),
            filter: String::new(),
            text: "vector".to_string(),
            k: 6,
            analysis: None,
            min_score: 0.0,
            fields: Vec::new(),
            facet_fields: Vec::new(),
            score_stages: Vec::new(),
            map_facet_fields: vec![map_field("meta", "color")],
            range_facet_fields: Vec::new(),
            geo_filters: Vec::new(),
            stats_fields: Vec::new(),
            cardinality_fields: Vec::new(),
            phrase: None,
            prefixes: Vec::new(),
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(counts_of(&resp.facets[0]), vec![("blue", 1)]);

    // A key NO shard knows is a typo, not an empty drill-down.
    let err = coordinator
        .fanout_bm25_faceted(
            "rust",
            6,
            None,
            0.0,
            &[],
            &[map_field("meta", "colour")],
            &[],
            &[],
            &[],
            None,
        )
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("colour") && err.message().contains("--map-facet-fields"),
        "refusal names the key and the knob: {}",
        err.message()
    );

    for h in handles {
        h.abort();
    }
    mock.abort();
}

/// [`NumericRead`] over an open reader, as the node provides it.
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
    fn facet_ord(&self, fi: usize, doc_id: u32) -> Option<u32> {
        self.0.facet_ord(fi, doc_id)
    }
    fn map_facet_value_ord(&self, ci: usize, key_ord: u32, doc_id: u32) -> Option<u32> {
        self.0.map_facet_value_ord(ci, key_ord, doc_id)
    }
}

/// Map-keyed chains keep the exactness gates: pruned bitwise equal to
/// the exhaustive oracle on an impacts-bearing shard, with per-key
/// bounds doing the lifting.
#[test]
fn map_keyed_chain_pruned_matches_exhaustive_bitwise() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("map_chain_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    let n = 3000u32;
    let tf_a = |d: u32| 1 + (u64::from(d) * 2654435761 % 7) as u32;
    let mut store = Bm25Store::with_fields(&["body"]).with_map_numerics(&["attrs"]);
    for doc in 0..n {
        let mut terms = vec![("a".to_string(), tf_a(doc), Vec::new())];
        if doc % 3 == 0 {
            terms.push(("b".to_string(), 1 + doc % 3, Vec::new()));
        }
        let len: u32 = terms.iter().map(|(_, tf, _)| tf).sum();
        store.add_document(doc, ".".to_string(), AnalyzedDoc::body(terms, len));
        // Two keys with different sparsities and ranges; every 5th doc
        // has neither.
        if doc % 5 != 0 {
            store.set_map_numeric(0, doc, "boost", f64::from(u64::from(doc) as u32 % 97));
        }
        if doc % 11 == 0 {
            store.set_map_numeric(0, doc, "age", -(f64::from(doc % 400)));
        }
    }
    let path = dir.join("chain.bm25");
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    let body = reader.field(0);
    let cols = ReaderNumerics(&reader);
    let attrs = reader.map_numeric_index("attrs").unwrap();
    let boost = reader.map_numeric_key_ord(attrs, "boost").unwrap();
    let age = reader.map_numeric_key_ord(attrs, "age").unwrap();
    let chain = ScoreChain {
        stages: vec![
            Stage {
                op: StageOp::MultLog { weight: 0.3 },
                column: Some(ColumnRef::MapKey {
                    column: attrs,
                    key_ord: boost,
                }),
                min_max: reader.map_numeric_key_min_max(attrs, boost),
            },
            Stage {
                op: StageOp::AddLinear { weight: 0.01 },
                column: Some(ColumnRef::MapKey {
                    column: attrs,
                    key_ord: age,
                }),
                min_max: reader.map_numeric_key_min_max(attrs, age),
            },
        ],
    };
    let ctx = Some((&chain, &cols as &dyn NumericRead));

    let stats = CorpusStats {
        doc_count: u64::from(n),
        total_doc_length: (0..n).map(|d| u64::from(tf_a(d))).sum::<u64>()
            + (0..n)
                .filter(|d| d % 3 == 0)
                .map(|d| u64::from(1 + d % 3))
                .sum::<u64>(),
        dfs: vec![n, n.div_ceil(3)],
    };
    let terms: Vec<String> = ["a", "b"].iter().map(|s| s.to_string()).collect();
    let params = Bm25Params::default();
    let signature = |docs: &[bm25::ScoredDoc]| -> Vec<(u32, u64)> {
        docs.iter().map(|d| (d.doc_id, d.score.to_bits())).collect()
    };
    for k in [1usize, 10, 100] {
        let exhaustive = bm25::top_k_exhaustive_chained(&body, &terms, &stats, params, k, ctx);
        let pruned =
            bm25::top_k_pruned_chained(&body, &terms, &stats, params, k, f64::NEG_INFINITY, ctx);
        assert_eq!(
            signature(&exhaustive),
            signature(&pruned),
            "k={k}: map-keyed pruned != exhaustive"
        );
        if let Some(kth) = exhaustive.last() {
            let seeded =
                bm25::top_k_pruned_chained(&body, &terms, &stats, params, k, kth.score, ctx);
            assert_eq!(signature(&exhaustive), signature(&seeded), "k={k}: seeded");
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// Distributed map-keyed stages: bitwise equal to the monolith, absent
/// entries identity, and the key-level refusals fire.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_map_stages_and_ingest_refusals() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addrs, handles) = start_mapped_shards(&analysis).await;
    let distributed = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(Some(analysis.clone()), Default::default());

    let (mono_addr, mono) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis.clone()),
        map_facet_fields: vec!["meta".to_string()],
        map_numeric_fields: vec!["attrs".to_string()],
        ..Default::default()
    })
    .await;
    let all: Vec<(&str, StrEntries, NumEntries)> = SHARD_DOCS.concat();
    add_documents_mapped(&mono_addr, &all).await.unwrap();
    let monolithic = CoordinatorServiceImpl::new(vec![mono_addr])
        .with_bm25(Some(analysis.clone()), Default::default());

    let stages = vec![ScoreStage {
        op: ScoreOp::MultLog as i32,
        column: "attrs".to_string(),
        key: "boost".to_string(),
        weight: 1.0,
        origin: 0.0,
        scale: 0.0,
        origin_lat: 0.0,
        origin_lon: 0.0,
    }];
    for text in ["rust", "vector"] {
        let (got, _, _) = distributed
            .fanout_bm25_faceted(text, 6, None, 0.0, &[], &[], &[], &stages, &[], None)
            .await
            .unwrap();
        let (want, _, _) = monolithic
            .fanout_bm25_faceted(text, 6, None, 0.0, &[], &[], &[], &stages, &[], None)
            .await
            .unwrap();
        assert_eq!(hit_signature(&got), hit_signature(&want), "query {text:?}");
    }
    // The chain reorders and absent entries are identity: d2 has no
    // attrs entries, so its score is bitwise the unchained one.
    let unchained = distributed.fanout_bm25("rust", 6, None).await.unwrap();
    let (chained, _, _) = distributed
        .fanout_bm25_faceted("rust", 6, None, 0.0, &[], &[], &[], &stages, &[], None)
        .await
        .unwrap();
    assert_ne!(hit_signature(&unchained), hit_signature(&chained));
    let score = |hits: &[Bm25Hit], id: u64| hits.iter().find(|h| h.doc_id == id).unwrap().score;
    assert_eq!(
        score(&unchained, 2).to_bits(),
        score(&chained, 2).to_bits(),
        "absent map entry must be identity"
    );

    // A key no shard knows refuses, naming column[key] and the knob.
    let typo = vec![ScoreStage {
        key: "bost".to_string(),
        ..stages[0].clone()
    }];
    let err = distributed
        .fanout_bm25_faceted("rust", 6, None, 0.0, &[], &[], &[], &typo, &[], None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("bost") && err.message().contains("--map-numeric-fields"),
        "{}",
        err.message()
    );

    // Ingest refusals: empty key, repeated (column, key), unknown
    // column, empty string value, non-finite numeric value.
    let bad_facet = |field: &str, key: &str, value: &str| AddDocumentsRequest {
        original_source: None,
        source_chunk_ordinal: None,
        identity: None,
        collection: String::new(),
        cased_field: String::new(),
        sentence_fields: Vec::new(),
        materialize: None,
        text: "some text".to_string(),
        analysis: None,
        lineage: None,
        fields: Vec::new(),
        facets: Vec::new(),
        numerics: Vec::new(),
        map_facets: vec![MapFacetEntry {
            field: field.to_string(),
            key: key.to_string(),
            value: value.to_string(),
        }],
        map_numerics: Vec::new(),
        integers: Vec::new(),
        timestamps: Vec::new(),
        geo_points: Vec::new(),
        quality: None,
        geography: None,
        phrases: Vec::new(),
        phrase_fingerprint: 0,
        phrase_field: String::new(),
        position_fields: Vec::new(),
        bigram_fields: Vec::new(),
    };
    let send = |addr: String, req: AddDocumentsRequest| async move {
        let mut client = NodeServiceClient::connect(addr).await.unwrap();
        let (tx, rx) = mpsc::channel(2);
        tx.send(req).await.unwrap();
        drop(tx);
        client
            .add_documents(ReceiverStream::new(rx))
            .await
            .map(|_| ())
    };
    for (req, needle) in [
        (bad_facet("meta", "", "x"), "empty keys"),
        (bad_facet("nope", "k", "x"), "unknown map column"),
        (bad_facet("meta", "k", ""), "empty value"),
    ] {
        let err = send(addrs[0].clone(), req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{needle}");
        assert!(
            err.message().contains(needle),
            "{needle}: {}",
            err.message()
        );
    }
    let mut dup = bad_facet("meta", "k", "x");
    dup.map_facets.push(MapFacetEntry {
        field: "meta".to_string(),
        key: "k".to_string(),
        value: "y".to_string(),
    });
    let err = send(addrs[0].clone(), dup).await.unwrap_err();
    assert!(err.message().contains("repeats"), "{}", err.message());
    let mut nan = bad_facet("meta", "k", "x");
    nan.map_facets.clear();
    nan.map_numerics.push(MapNumericEntry {
        field: "attrs".to_string(),
        key: "k".to_string(),
        value: f64::NAN,
    });
    let err = send(addrs[0].clone(), nan).await.unwrap_err();
    assert!(err.message().contains("non-finite"), "{}", err.message());

    for h in handles {
        h.abort();
    }
    mono.abort();
    mock.abort();
}
