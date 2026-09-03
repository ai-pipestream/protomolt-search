//! Score-function acceptance tests (`docs/score-functions.md`):
//! numeric columns through the v7 column table, and score chains whose
//! pruned results are bitwise identical to the exhaustive oracle, whose
//! distributed results are bitwise identical to the monolith, and whose
//! refusals fire for typo'd columns and inadmissible parameters.

mod common;

use pipestream_search::bm25::{self, Bm25Params, CorpusStats};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    AddDocumentsRequest, Bm25Hit, Bm25SearchRequest, NumericValue, QueryField, ScoreOp, ScoreStage,
};
use pipestream_search::postings::{AnalyzedDoc, Bm25Reader, Bm25Store, SpillBuilder};
use pipestream_search::scorefn::{ColumnRef, NumericRead, ScoreChain, Stage, StageOp};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

use common::{mock::start_mock_analysis, start_empty_node};

/// A document's numeric values at ingest: (field, value) pairs.
type Numerics = &'static [(&'static str, f64)];

/// A roundtrip-test document: (text, (term, tf) pairs, date value).
type NumericDoc = (&'static str, Vec<(&'static str, u32)>, Option<f64>);

/// The controlled corpus: six documents over three shards with a
/// decision-date numeric column. Shard 2 declares NO numeric fields —
/// the heterogeneous-fleet case; its documents pass through every
/// stage unchanged, which is exact (they hold no values).
const SHARD_DOCS: [&[(&str, Numerics)]; 3] = [
    &[
        ("rust search rust fast", &[("date", 100.0)]),
        ("vector search rust", &[("date", 200.0)]),
    ],
    &[
        ("search engines love rust", &[("date", 300.0)]),
        ("vector vector vector", &[("date", 100.0)]),
    ],
    &[("rust", &[]), ("nothing relevant here", &[])],
];

async fn add_documents_numeric(
    addr: &str,
    docs: &[(&str, Numerics)],
) -> Result<pipestream_search::pb::AddDocumentsResponse, tonic::Status> {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(8);
    for (text, numerics) in docs {
        tx.send(AddDocumentsRequest {
            collection: String::new(),
            cased_field: String::new(),
            sentence_fields: Vec::new(),
            materialize: None,
            map_numerics: Vec::new(),
            map_facets: Vec::new(),
            text: text.to_string(),
            analysis: None,
            lineage: None,
            fields: Vec::new(),
            facets: Vec::new(),
            numerics: numerics
                .iter()
                .map(|(field, value)| NumericValue {
                    field: field.to_string(),
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

/// Three shards, the numeric table declared on shards 0 and 1 only.
async fn start_numeric_shards(
    analysis: &str,
) -> (
    Vec<String>,
    Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
) {
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for (i, _) in SHARD_DOCS.iter().enumerate() {
        let numeric_fields = if i < 2 {
            vec!["date".to_string()]
        } else {
            Vec::new()
        };
        let (addr, handle) = start_empty_node(NodeConfig {
            slot_offset: [0u64, 2, 4][i],
            analysis_addr: Some(analysis.to_string()),
            numeric_fields,
            ..Default::default()
        })
        .await;
        addrs.push(addr);
        handles.push(handle);
    }
    for (i, docs) in SHARD_DOCS.iter().enumerate() {
        add_documents_numeric(&addrs[i], docs).await.unwrap();
    }
    (addrs, handles)
}

fn decay_stage(origin: f64, scale: f64) -> ScoreStage {
    ScoreStage {
        key: String::new(),
        op: ScoreOp::MultExpDecay as i32,
        column: "date".to_string(),
        weight: 0.0,
        origin,
        scale,
        origin_lat: 0.0,
        origin_lon: 0.0,
    }
}

fn hit_signature(hits: &[Bm25Hit]) -> Vec<(u64, u32)> {
    hits.iter().map(|h| (h.doc_id, h.score.to_bits())).collect()
}

/// The v7 round-trip for numeric columns: kinded table, min/max
/// metadata, both readers, dual-writer identity, and the opt-in rule
/// (numerics alone make a v7; no columns still makes a v6).
#[test]
fn numeric_columns_roundtrip_and_dual_writers_agree() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("numeric_roundtrip_{}", std::process::id()));
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
    // Doc 1 has no value; doc 2 a negative one (min/max must cover it).
    let docs: Vec<NumericDoc> = vec![
        ("rust search", vec![("rust", 1), ("search", 1)], Some(150.5)),
        ("vector rust", vec![("rust", 1), ("vector", 1)], None),
        ("plain text", vec![("plain", 1), ("text", 1)], Some(-3.0)),
    ];

    // A store with BOTH column kinds, to pin the kinded table.
    let mut store = Bm25Store::with_fields(&["body"])
        .with_facets(&["court"])
        .with_numerics(&["date"]);
    for (i, (text, terms, date)) in docs.iter().enumerate() {
        store.add_document(i as u32, text.to_string(), analyzed(terms));
        if let Some(d) = date {
            store.set_numeric(0, i as u32, *d);
        }
    }
    store.set_facet(0, 0, "scotus");
    let heap_path = dir.join("heap.bm25");
    store.save(&heap_path).unwrap();
    let bytes = std::fs::read(&heap_path).unwrap();
    assert_eq!(&bytes[..8], b"TVBM2508");

    let mut builder = SpillBuilder::create_with_fields(&dir.join("spill.build"), &["body"])
        .unwrap()
        .with_facet_fields(&["court"])
        .with_numeric_fields(&["date"])
        .with_buffer_bytes(32);
    for (i, (text, terms, date)) in docs.iter().enumerate() {
        builder
            .add_document_with_lineage(i as u32, text.to_string(), analyzed(terms), None)
            .unwrap();
        if let Some(d) = date {
            builder.set_numeric(0, i as u32, *d);
        }
    }
    builder.set_facet(0, 0, "scotus");
    let spill_path = dir.join("spill.bm25");
    builder.finish(&spill_path).unwrap();
    assert_eq!(
        bytes,
        std::fs::read(&spill_path).unwrap(),
        "dual writers must stay byte-identical on column-bearing stores"
    );

    let reader = Bm25Reader::open(&heap_path).unwrap();
    assert_eq!(reader.facet_count(), 1);
    assert_eq!(reader.numeric_count(), 1);
    assert_eq!(reader.numeric_name(0), "date");
    assert_eq!(reader.numeric_index("date"), Some(0));
    assert_eq!(reader.numeric_index("bogus"), None);
    assert_eq!(reader.numeric_value(0, 0), Some(150.5));
    assert_eq!(reader.numeric_value(0, 1), None, "doc 1 has no value");
    assert_eq!(reader.numeric_value(0, 2), Some(-3.0));
    assert_eq!(reader.numeric_min_max(0), (-3.0, 150.5));

    let loaded = Bm25Store::load(&heap_path).unwrap();
    assert_eq!(loaded.numeric_value(0, 0), Some(150.5));
    assert_eq!(loaded.numeric_value(0, 1), None);
    assert_eq!(loaded.numeric_min_max(0), (-3.0, 150.5));

    // Numerics alone still opt the shard into v7.
    let mut only_numeric = Bm25Store::with_fields(&["body"]).with_numerics(&["date"]);
    only_numeric.add_document(0, "rust".to_string(), analyzed(&[("rust", 1)]));
    only_numeric.set_numeric(0, 0, 7.0);
    let numeric_path = dir.join("numeric.bm25");
    only_numeric.save(&numeric_path).unwrap();
    assert_eq!(&std::fs::read(&numeric_path).unwrap()[..8], b"TVBM2508");
    let r = Bm25Reader::open(&numeric_path).unwrap();
    assert_eq!((r.facet_count(), r.numeric_count()), (0, 1));

    let _ = std::fs::remove_dir_all(&dir);
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
    fn facet_ord(&self, fi: usize, doc_id: u32) -> Option<u32> {
        self.0.facet_ord(fi, doc_id)
    }
    fn map_facet_value_ord(&self, ci: usize, key_ord: u32, doc_id: u32) -> Option<u32> {
        self.0.map_facet_value_ord(ci, key_ord, doc_id)
    }
}

/// The exactness gate: on a file-backed shard (impacts present, so the
/// block-max path actually prunes), the chained pruned scorer is
/// bitwise identical to the chained exhaustive oracle — across ks,
/// seeded floors, and a chain mixing all three ops, including
/// documents that go negative mid-chain and documents with no value.
#[test]
fn chained_pruned_matches_chained_exhaustive_bitwise() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("chain_pruned_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    // 3000 docs; "a" everywhere with varying tf, "b" on thirds, "c"
    // rare. Dates pseudo-random but deterministic; every 7th doc has
    // no value.
    let n = 3000u32;
    let tf_a = |d: u32| 1 + (u64::from(d) * 2654435761 % 7) as u32;
    let date_of = |d: u32| (u64::from(d) * 48271 % 1000) as f64;
    let mut store = Bm25Store::with_fields(&["body"]).with_numerics(&["date"]);
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
        if doc % 7 != 0 {
            store.set_numeric(0, doc, date_of(doc));
        }
    }
    let path = dir.join("chain.bm25");
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    let body = reader.field(0);
    let cols = ReaderNumerics(&reader);
    let chain = ScoreChain {
        stages: vec![
            Stage {
                op: StageOp::AddLinear { weight: -0.002 },
                column: Some(ColumnRef::Numeric(0)),
                min_max: reader.numeric_min_max(0),
            },
            Stage {
                op: StageOp::MultExpDecay {
                    origin: 500.0,
                    scale: 250.0,
                },
                column: Some(ColumnRef::Numeric(0)),
                min_max: reader.numeric_min_max(0),
            },
            Stage {
                op: StageOp::MultLog { weight: 0.4 },
                column: Some(ColumnRef::Numeric(0)),
                min_max: reader.numeric_min_max(0),
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
                .sum::<u64>()
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
            "k={k}: chained pruned != chained exhaustive"
        );
        // Seed the floor with the k-th best FINAL score: ties at the
        // floor survive, so the seeded run returns the same set.
        if let Some(kth) = exhaustive.last() {
            let seeded =
                bm25::top_k_pruned_chained(&body, &terms, &stats, params, k, kth.score, ctx);
            assert_eq!(
                signature(&exhaustive),
                signature(&seeded),
                "k={k}: final-scale floor changed the chained result"
            );
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}

/// Distributed == monolith bitwise with a chain, the chain actually
/// reorders, and absent-value documents keep their base score.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_chain_matches_monolith_and_reorders() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addrs, handles) = start_numeric_shards(&analysis).await;
    let distributed = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(Some(analysis.clone()), Default::default());

    // Monolithic reference: one node with every document and the table.
    let (mono_addr, mono) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis.clone()),
        numeric_fields: vec!["date".to_string()],
        ..Default::default()
    })
    .await;
    let all: Vec<(&str, Numerics)> = SHARD_DOCS.concat();
    add_documents_numeric(&mono_addr, &all).await.unwrap();
    let monolithic = CoordinatorServiceImpl::new(vec![mono_addr])
        .with_bm25(Some(analysis.clone()), Default::default());

    let stages = vec![decay_stage(300.0, 100.0)];
    for text in ["rust", "search rust", "vector"] {
        let (got, _, _) = distributed
            .fanout_bm25_faceted(text, 6, None, 0.0, &[], &[], &[], &stages, &[], None)
            .await
            .unwrap();
        let (want, _, _) = monolithic
            .fanout_bm25_faceted(text, 6, None, 0.0, &[], &[], &[], &stages, &[], None)
            .await
            .unwrap();
        assert_eq!(
            hit_signature(&got),
            hit_signature(&want),
            "query {text:?}: distributed chained != monolithic chained"
        );
    }

    // The chain reorders: unchained, d0 (tf=2) outranks d2 on "rust";
    // decayed toward date=300, d2 (factor 1) climbs above d0 (e^-2).
    let unchained = distributed.fanout_bm25("rust", 6, None).await.unwrap();
    let (chained, _, _) = distributed
        .fanout_bm25_faceted("rust", 6, None, 0.0, &[], &[], &[], &stages, &[], None)
        .await
        .unwrap();
    assert_eq!(unchained.len(), chained.len(), "same match set");
    assert_ne!(
        hit_signature(&unchained),
        hit_signature(&chained),
        "the chain changed nothing"
    );
    let pos = |hits: &[Bm25Hit], id: u64| hits.iter().position(|h| h.doc_id == id).unwrap();
    assert!(
        pos(&unchained, 0) < pos(&unchained, 2),
        "unchained: tf wins"
    );
    assert!(pos(&chained, 2) < pos(&chained, 0), "chained: recency wins");
    // d4 sits on the numeric-less shard: absent = identity, so its
    // score is bitwise the unchained one.
    let base = unchained.iter().find(|h| h.doc_id == 4).unwrap().score;
    let with = chained.iter().find(|h| h.doc_id == 4).unwrap().score;
    assert_eq!(
        base.to_bits(),
        with.to_bits(),
        "absent value must be identity"
    );

    // The public RPC carries stages end to end.
    let resp = SearchService::bm25_search(
        &distributed,
        Request::new(Bm25SearchRequest {
            collection: String::new(),
            highlight: None,
            projections: Vec::new(),
            filter: String::new(),
            map_facet_fields: Vec::new(),
            text: "rust".to_string(),
            k: 6,
            analysis: None,
            min_score: 0.0,
            fields: Vec::new(),
            facet_fields: Vec::new(),
            score_stages: stages.clone(),
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
    assert_eq!(hit_signature(&resp.hits), hit_signature(&chained));

    for h in handles {
        h.abort();
    }
    mono.abort();
    mock.abort();
}

/// Refusals: a column no shard knows, inadmissible parameters, the
/// fused route, and non-finite ingest values.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stage_and_ingest_refusals_are_loud() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addrs, handles) = start_numeric_shards(&analysis).await;
    let coordinator = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(Some(analysis.clone()), Default::default());

    // A typo'd column would be an identity chain everywhere: refused.
    let typo = vec![ScoreStage {
        column: "dat".to_string(),
        ..decay_stage(300.0, 100.0)
    }];
    let err = coordinator
        .fanout_bm25_faceted("rust", 6, None, 0.0, &[], &[], &[], &typo, &[], None)
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("dat") && err.message().contains("--numeric-fields"),
        "refusal names the column and the knob: {}",
        err.message()
    );

    // Inadmissible parameters refuse at parse, naming the stage.
    for (bad, needle) in [
        (
            ScoreStage {
                scale: 0.0,
                ..decay_stage(300.0, 100.0)
            },
            "scale > 0",
        ),
        (
            ScoreStage {
                key: String::new(),
                op: ScoreOp::MultLog as i32,
                column: "date".to_string(),
                weight: -1.0,
                origin: 0.0,
                scale: 0.0,
                origin_lat: 0.0,
                origin_lon: 0.0,
            },
            "weight >= 0",
        ),
        (
            ScoreStage {
                key: String::new(),
                op: ScoreOp::Unspecified as i32,
                column: "date".to_string(),
                weight: 0.0,
                origin: 0.0,
                scale: 0.0,
                origin_lat: 0.0,
                origin_lon: 0.0,
            },
            "unknown op",
        ),
    ] {
        let err = coordinator
            .fanout_bm25_faceted("rust", 6, None, 0.0, &[], &[], &[], &[bad], &[], None)
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{needle}");
        assert!(
            err.message().contains(needle),
            "expected {needle:?} in: {}",
            err.message()
        );
    }

    // The fused route refuses stages rather than dropping them.
    let err = SearchService::bm25_search(
        &coordinator,
        Request::new(Bm25SearchRequest {
            collection: String::new(),
            highlight: None,
            projections: Vec::new(),
            filter: String::new(),
            map_facet_fields: Vec::new(),
            text: "rust".to_string(),
            k: 6,
            analysis: None,
            min_score: 0.0,
            fields: vec![QueryField {
                field: "body".to_string(),
                analysis: None,
                weight: 1.0,
                k1: 0.0,
                b: 0.0,
                phrase: None,
                prefixes: Vec::new(),
            }],
            facet_fields: Vec::new(),
            score_stages: vec![decay_stage(300.0, 100.0)],
            range_facet_fields: Vec::new(),
            geo_filters: Vec::new(),
            stats_fields: Vec::new(),
            cardinality_fields: Vec::new(),
            phrase: None,
            prefixes: Vec::new(),
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("fused"));

    // Ingest: unknown field, repeats, and non-finite values refuse
    // before anything mutates.
    let cases: &[(Numerics, &str)] = &[
        (&[("bogus", 1.0)], "unknown numeric field"),
        (&[("date", 1.0), ("date", 2.0)], "repeats"),
        (&[("date", f64::NAN)], "non-finite"),
        (&[("date", f64::INFINITY)], "non-finite"),
    ];
    for (numerics, needle) in cases {
        let err = add_documents_numeric(&addrs[0], &[("some text", numerics)])
            .await
            .unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{needle}");
        assert!(
            err.message().contains(needle),
            "expected {needle:?} in: {}",
            err.message()
        );
    }

    for h in handles {
        h.abort();
    }
    mock.abort();
}
