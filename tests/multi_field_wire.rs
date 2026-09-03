//! Multi-field wire acceptance (docs/multi-field.md, build order step
//! 4): ingest through the mock sidecar with extra DocumentFields, then
//! query fused through the coordinator.
//!
//! - Fused distributed == fused monolithic bitwise, heap-store AND
//!   flushed-resident shapes (resident runs the fused pruned scorer,
//!   the heap store its exhaustive fallback — the wire twin of the
//!   pruned-equals-exhaustive gate);
//! - reweighting reorders with no reindex; hits name their fields;
//! - the fused kth-best seeds a lossless re-query floor;
//! - per-field TermStats shares (zeros for fields a shard lacks);
//! - ingest validation: unknown / duplicate / "body" / empty fields are
//!   refused before any effect;
//! - `Bm25Shard::open` maps every reader-supported format resident
//!   (v5/v6 heap-loaded on restart before this increment);
//! - ShardLegs k1/b reach the BM25 leg (compute_legs hardcoded
//!   defaults before this increment).

mod common;

use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::{Bm25Shard, NodeConfig};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::{
    AddDocumentsRequest, Bm25Hit, DocumentField, FieldTerms, FlushRequest, QueryField,
    ShardLegsRequest, TermStatsRequest,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use common::{mock::start_mock_analysis, start_empty_node};

/// The two-field corpus: (body, optional case name), split over two
/// shards. "smith" appears in ONE body but several case names, so
/// name-weighted queries reorder against body-only scoring.
const CORPUS: [&[(&str, Option<&str>)]; 2] = [
    &[
        ("rust search rust fast", Some("Smith v Jones")),
        ("vector search rust", None),
        (
            "smith writes about rust",
            Some("Acme Corp v Rust Industries"),
        ),
    ],
    &[
        ("search engines love rust", Some("Smith v Smith")),
        ("vector vector vector", None),
        ("rust", Some("In re Vector Holdings")),
    ],
];

const OFFSETS: [u64; 2] = [0, 3];

fn doc_request(body: &str, name: Option<&str>) -> AddDocumentsRequest {
    AddDocumentsRequest {
        collection: String::new(),
        sentence_fields: Vec::new(),
        materialize: None,
        map_numerics: Vec::new(),
        map_facets: Vec::new(),
        numerics: Vec::new(),
        facets: Vec::new(),
        text: body.to_string(),
        analysis: None,
        lineage: None,
        fields: name
            .map(|n| {
                vec![DocumentField {
                    field: "case_name".to_string(),
                    text: n.to_string(),
                    analysis: None,
                }]
            })
            .unwrap_or_default(),
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
    }
}

async fn add_documents(addr: &str, docs: &[(&str, Option<&str>)]) {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(8);
    for (body, name) in docs {
        tx.send(doc_request(body, *name)).await.unwrap();
    }
    drop(tx);
    let resp = client
        .add_documents(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(resp.added as usize, docs.len());
}

fn two_field_node(
    analysis: &str,
    slot_offset: u64,
    index_path: Option<std::path::PathBuf>,
) -> NodeConfig {
    NodeConfig {
        slot_offset,
        analysis_addr: Some(analysis.to_string()),
        bm25_fields: vec!["body".to_string(), "case_name".to_string()],
        index_path,
        ..Default::default()
    }
}

/// The fused query: body at weight 1, case_name at `w_name`.
fn query_fields(w_name: f32) -> Vec<QueryField> {
    vec![
        QueryField {
            field: "body".to_string(),
            analysis: None,
            weight: 1.0,
            k1: 0.0,
            b: 0.0,
            phrase: None,
            prefixes: Vec::new(),
        },
        QueryField {
            field: "case_name".to_string(),
            analysis: None,
            weight: w_name,
            k1: 0.0,
            b: 0.0,
            phrase: None,
            prefixes: Vec::new(),
        },
    ]
}

fn hit_signature(hits: &[Bm25Hit]) -> Vec<(u64, u32)> {
    hits.iter().map(|h| (h.doc_id, h.score.to_bits())).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fused_distributed_equals_monolithic_over_the_wire() {
    let (analysis, mock) = start_mock_analysis().await;
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("tvmfw_dist_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    // In-memory shards + monolith: the heap-store shape, whose fused
    // route takes the exhaustive fallback (no impact surface).
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for (i, docs) in CORPUS.iter().enumerate() {
        let (addr, handle) = start_empty_node(two_field_node(&analysis, OFFSETS[i], None)).await;
        add_documents(&addr, docs).await;
        addrs.push(addr);
        handles.push(handle);
    }
    let (mono_addr, mono) = start_empty_node(two_field_node(&analysis, 0, None)).await;
    let all: Vec<(&str, Option<&str>)> = CORPUS.concat();
    add_documents(&mono_addr, &all).await;

    let distributed = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(Some(analysis.clone()), Default::default());
    let monolithic = CoordinatorServiceImpl::new(vec![mono_addr.clone()])
        .with_bm25(Some(analysis.clone()), Default::default());

    let queries = ["smith", "rust smith", "vector", "holdings rust", "nothing"];
    let heap_runs: Vec<Vec<Bm25Hit>> = {
        let mut runs = Vec::new();
        for text in queries {
            for k in [3u32, 6] {
                let got = distributed
                    .fanout_bm25_fused(text, k, &query_fields(2.0), 0.0)
                    .await
                    .unwrap();
                let want = monolithic
                    .fanout_bm25_fused(text, k, &query_fields(2.0), 0.0)
                    .await
                    .unwrap();
                assert_eq!(
                    hit_signature(&got),
                    hit_signature(&want),
                    "heap shape, query {text:?} k={k}: distributed != monolithic"
                );
                runs.push(got);
            }
        }
        runs
    };

    // Hits name their fields; a case-name term matched under
    // "case_name", body terms under "body".
    let smith_hits = &heap_runs[0];
    assert!(!smith_hits.is_empty());
    let named: Vec<&str> = smith_hits[0]
        .terms
        .iter()
        .map(|t| t.field.as_str())
        .collect();
    assert!(
        named.contains(&"case_name"),
        "top smith hit should match the name field, got {named:?}"
    );

    // Muting = OMITTING the entry (0 means "default 1.0" on the wire,
    // like every other weight in the proto): a body-only query sees
    // just doc 2 ("smith writes about rust"); adding the name leg
    // brings the name-only matches in, and weight changes the order
    // with no reindex.
    let body_only = distributed
        .fanout_bm25_fused("smith", 6, &query_fields(1.0)[..1], 0.0)
        .await
        .unwrap();
    assert_eq!(body_only.len(), 1, "one body mentions smith: {body_only:?}");
    assert_eq!(body_only[0].doc_id, 2);
    let weighted = distributed
        .fanout_bm25_fused("smith", 6, &query_fields(2.0), 0.0)
        .await
        .unwrap();
    assert!(
        weighted.len() > body_only.len(),
        "name matches must join the fused list"
    );
    // At a heavy name weight the name-matching docs outrank the body
    // match; at a feather weight the body match leads.
    let heavy = distributed
        .fanout_bm25_fused("smith", 6, &query_fields(50.0), 0.0)
        .await
        .unwrap();
    assert_ne!(
        heavy[0].doc_id,
        distributed
            .fanout_bm25_fused("smith", 6, &query_fields(0.01), 0.0)
            .await
            .unwrap()[0]
            .doc_id,
        "weights must reorder the fused list"
    );

    // The fused kth-best seeds a lossless re-query.
    let full = distributed
        .fanout_bm25_fused("rust smith", 6, &query_fields(2.0), 0.0)
        .await
        .unwrap();
    assert!(full.len() >= 2);
    let seed = pipestream_search::bm25::floor_seed(full.last().unwrap().score);
    let seeded = distributed
        .fanout_bm25_fused("rust smith", 6, &query_fields(2.0), seed)
        .await
        .unwrap();
    assert_eq!(
        hit_signature(&full),
        hit_signature(&seeded),
        "seeding at the fused kth-best must lose nothing"
    );

    // Per-field TermStats shares on shard 0: case_name df("smith") = 2
    // ("Smith v Jones", "Acme Corp v Rust Industries" has none — only
    // the first), body df("smith") = 1; an unknown field answers zeros.
    let mut c0 = NodeServiceClient::connect(addrs[0].clone()).await.unwrap();
    let stats = c0
        .term_stats(TermStatsRequest {
            terms: vec!["smith".into()],
            fields: vec![
                FieldTerms {
                    field: "case_name".into(),
                    terms: vec!["smith".into(), "jones".into()],
                },
                FieldTerms {
                    field: "docket".into(),
                    terms: vec!["smith".into()],
                },
            ],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(stats.doc_frequencies, vec![1], "body df on shard 0");
    assert_eq!(stats.field_stats.len(), 2);
    assert_eq!(
        stats.field_stats[0].doc_frequencies,
        vec![1, 1],
        "case_name dfs on shard 0 (mock lowercases nothing; names are ingested verbatim)"
    );
    assert!(stats.field_stats[0].total_doc_length > 0);
    assert_eq!(
        stats.field_stats[1].doc_frequencies,
        vec![0],
        "unknown field answers zeros — that IS its share"
    );
    assert_eq!(stats.field_stats[1].total_doc_length, 0);

    // A second, PERSISTED cluster with the same corpus: after Flush the
    // shards go resident (v6 + impact surface, the fused PRUNED path)
    // and must reproduce the heap cluster's runs bit for bit — the wire
    // twin of pruned-equals-exhaustive.
    let mut r_addrs = Vec::new();
    let mut r_handles = Vec::new();
    for (i, docs) in CORPUS.iter().enumerate() {
        let (addr, handle) = start_empty_node(two_field_node(
            &analysis,
            OFFSETS[i],
            Some(dir.join(format!("shard{i}.tv"))),
        ))
        .await;
        add_documents(&addr, docs).await;
        let mut c = NodeServiceClient::connect(addr.clone()).await.unwrap();
        let flushed = c.flush(FlushRequest {}).await.unwrap().into_inner();
        assert!(flushed.written);
        r_addrs.push(addr);
        r_handles.push(handle);
    }
    let resident = CoordinatorServiceImpl::new(r_addrs.clone())
        .with_bm25(Some(analysis.clone()), Default::default());
    let resident_streamed = CoordinatorServiceImpl::new(r_addrs.clone())
        .with_bm25(Some(analysis.clone()), Default::default())
        .with_bm25_stream(true);
    let mut i = 0;
    for text in queries {
        for k in [3u32, 6] {
            let got = resident
                .fanout_bm25_fused(text, k, &query_fields(2.0), 0.0)
                .await
                .unwrap();
            assert_eq!(
                hit_signature(&got),
                hit_signature(&heap_runs[i]),
                "resident (pruned) shape diverged from heap shape: {text:?} k={k}"
            );
            let streamed = resident_streamed
                .fanout_bm25_fused(text, k, &query_fields(2.0), 0.0)
                .await
                .unwrap();
            assert_eq!(
                hit_signature(&streamed),
                hit_signature(&got),
                "fused candidate stream diverged from unary: {text:?} k={k}"
            );
            i += 1;
        }
    }

    for h in handles.into_iter().chain(r_handles) {
        h.abort();
    }
    mono.abort();
    mock.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Ingest validation fails BEFORE any store or WAL effect: unknown
/// field names, duplicates, "body", and empty field text are all
/// INVALID_ARGUMENT, and a valid retry lands on a clean shard.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_field_ingest_validation_refuses_bad_fields() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addr, node) = start_empty_node(two_field_node(&analysis, 0, None)).await;
    let client = NodeServiceClient::connect(addr.clone()).await.unwrap();

    let send_one = |req: AddDocumentsRequest| {
        let mut client = client.clone();
        async move {
            let (tx, rx) = mpsc::channel(1);
            tx.send(req).await.unwrap();
            drop(tx);
            client.add_documents(ReceiverStream::new(rx)).await
        }
    };

    let bad = |field: &str, text: &str| AddDocumentsRequest {
        collection: String::new(),
        sentence_fields: Vec::new(),
        materialize: None,
        map_numerics: Vec::new(),
        map_facets: Vec::new(),
        numerics: Vec::new(),
        facets: Vec::new(),
        text: "a body".to_string(),
        analysis: None,
        lineage: None,
        fields: vec![DocumentField {
            field: field.to_string(),
            text: text.to_string(),
            analysis: None,
        }],
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
    for (req, why) in [
        (bad("docket", "x"), "unknown field"),
        (bad("body", "x"), "body named as an extra field"),
        (bad("case_name", ""), "empty field text"),
        (
            AddDocumentsRequest {
                fields: vec![
                    bad("case_name", "x").fields.remove(0),
                    bad("case_name", "y").fields.remove(0),
                ],
                ..bad("case_name", "x")
            },
            "duplicate field",
        ),
    ] {
        let err = send_one(req).await.expect_err(why);
        assert_eq!(
            err.code(),
            tonic::Code::InvalidArgument,
            "{why}: {}",
            err.message()
        );
    }

    // Nothing landed; a valid document still ingests cleanly.
    let ok = send_one(doc_request("clean body", Some("Good v Name")))
        .await
        .unwrap()
        .into_inner();
    assert_eq!((ok.added, ok.total, ok.first_id), (1, 1, 0));

    node.abort();
    mock.abort();
}

/// `Bm25Shard::open` maps every reader-supported format disk-resident.
/// v5/v6 were missing from its magic list, so a RESTARTED node heap
/// loaded its whole postings file and lost the impact surface — at real
/// shard sizes the exact failure the resident reader prevents.
#[test]
fn bm25_shard_open_maps_current_formats_resident() {
    let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("tvmfw_open_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let mut store = pipestream_search::postings::Bm25Store::with_fields(&["body", "case_name"]);
    for i in 0..40u32 {
        store.add_document(
            i,
            format!("doc {i}"),
            pipestream_search::postings::AnalyzedDoc::body(
                vec![("court".to_string(), 1 + i % 3, vec![(0, 5)])],
                1 + i % 3,
            ),
        );
    }

    let v6 = dir.join("v6.bm25");
    store.save(&v6).unwrap();
    assert!(
        matches!(Bm25Shard::open(&v6).unwrap(), Bm25Shard::Resident(_)),
        "a current-format save (v8) must open disk-resident"
    );

    // v5 carries exactly one field; build the oracle file from a
    // single-field store.
    let mut single = pipestream_search::postings::Bm25Store::new();
    for i in 0..40u32 {
        single.add_document(
            i,
            format!("doc {i}"),
            pipestream_search::postings::AnalyzedDoc::body(
                vec![("court".to_string(), 1 + i % 3, vec![(0, 5)])],
                1 + i % 3,
            ),
        );
    }
    let v5 = dir.join("v5.bm25");
    single.save_v5(&v5).unwrap();
    assert!(
        matches!(Bm25Shard::open(&v5).unwrap(), Bm25Shard::Resident(_)),
        "a v5 file must open disk-resident"
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// ShardLegs BM25 params reach scoring: non-default k1/b change the
/// leg's scores. Before this increment `compute_legs` hardcoded the
/// defaults, so tuning silently never reached the hybrid paths.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shard_legs_bm25_params_reach_scoring() {
    let (analysis, mock) = start_mock_analysis().await;
    let (addr, node) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis.clone()),
        ..Default::default()
    })
    .await;
    // Different document lengths so b has something to normalize.
    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    let (tx, rx) = mpsc::channel(8);
    for text in [
        "court",
        "court appeals court ruling on appeal",
        "court of appeals for the ninth circuit en banc",
    ] {
        tx.send(AddDocumentsRequest {
            collection: String::new(),
            sentence_fields: Vec::new(),
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
    client.add_documents(ReceiverStream::new(rx)).await.unwrap();

    let legs = |k1: f32, b: f32| {
        let mut client = client.clone();
        async move {
            client
                .shard_legs(ShardLegsRequest {
                    expected_stats_epoch: 0,
                    request_id: String::new(),
                    k: 3,
                    vector: Vec::new(),
                    terms: vec!["court".to_string()],
                    global_doc_count: 3,
                    global_total_doc_length: 16,
                    global_doc_frequencies: vec![3],
                    k1,
                    b,
                    ..Default::default()
                })
                .await
                .unwrap()
                .into_inner()
                .bm25_hits
        }
    };
    let defaults = legs(0.0, 0.0).await;
    let tuned = legs(2.5, 0.99).await;
    assert_eq!(defaults.len(), 3);
    assert_eq!(tuned.len(), 3);
    assert!(
        defaults
            .iter()
            .zip(&tuned)
            .any(|(d, t)| d.score.to_bits() != t.score.to_bits()),
        "k1/b must reach the BM25 leg's scores"
    );

    node.abort();
    mock.abort();
}

/// An unknown field must be refused, and a partially-known one must not.
///
/// Shards skip a leg naming a field they lack, which is right for a
/// heterogeneous fleet and catastrophic for a typo: the fused score of
/// the REMAINING fields comes back as if it answered the question asked.
/// The distinction the engine can actually make is fleet-wide — no shard
/// has it (a typo) versus some shard has it (a real rollout) — so that is
/// where the refusal lives.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_field_no_shard_indexes_is_refused_not_silently_skipped() {
    let (analysis, mock) = start_mock_analysis().await;

    // Shard 0 has case_name; shard 1 is body-only. Together they are the
    // mid-rollout fleet the skip exists for.
    let (a, node_a) = start_empty_node(two_field_node(&analysis, 0, None)).await;
    add_documents(&a, CORPUS[0]).await;
    let (b, node_b) = start_empty_node(NodeConfig {
        slot_offset: OFFSETS[1],
        analysis_addr: Some(analysis.clone()),
        bm25_fields: vec!["body".to_string()],
        ..Default::default()
    })
    .await;
    add_documents(
        &b,
        &CORPUS[1]
            .iter()
            .map(|(t, _)| (*t, None))
            .collect::<Vec<_>>(),
    )
    .await;

    let coord = CoordinatorServiceImpl::new(vec![a, b])
        .with_bm25(Some(analysis.clone()), Default::default());

    // Partially known: shard 0 answers, shard 1 skips, query succeeds.
    let partial = coord
        .fanout_bm25_fused("smith rust", 5, &query_fields(2.0), 0.0)
        .await
        .expect("a field some shard indexes is a real query");
    assert!(!partial.is_empty());

    // Known nowhere: refused, and the message says what to check.
    let mut typo = query_fields(2.0);
    typo[1].field = "case_nmae".to_string();
    let err = coord
        .fanout_bm25_fused("smith rust", 5, &typo, 0.0)
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(
        err.message().contains("case_nmae") && err.message().contains("no shard indexes"),
        "the refusal must name the field: {}",
        err.message()
    );

    node_a.abort();
    node_b.abort();
    mock.abort();
}

/// A request-level `analysis` alongside `fields` is refused, not dropped.
///
/// The proto says `analysis` is ignored once `fields` is set, because
/// term identity is per field. Ignoring it QUIETLY is what makes it
/// dangerous: every field then falls back to the analysis sidecar's
/// default, which need not be the spec the field was ingested with. The
/// query runs against terms that are not in the index, matches only the
/// tokens that happen to survive both analyses, and returns a confident
/// ranking of that fragment -- which reads as bad relevance, never as a
/// failure.
///
/// Found on the live fleet: an A/B tool set the spec at the request
/// level, the fused route analyzed with the sidecar default (no
/// stemming), and the query went out as "court, established" instead of
/// "court, establish". The unstemmed term existed in ONE document of
/// 86.6M, which then also dragged seven of eight shards onto the
/// exhaustive scorer.
#[tokio::test]
async fn request_level_analysis_with_fields_is_refused_not_ignored() {
    use pipestream_search::pb::search_service_server::SearchService;
    use pipestream_search::pb::{AnalysisSpec, Bm25SearchRequest};

    let (analysis, mock) = start_mock_analysis().await;
    let (a, node_a) = start_empty_node(two_field_node(&analysis, 0, None)).await;
    add_documents(&a, CORPUS[0]).await;
    let coord =
        CoordinatorServiceImpl::new(vec![a]).with_bm25(Some(analysis.clone()), Default::default());

    let spec = AnalysisSpec {
        tokenizer: 1,
        stemmer: 2,
        term_vector_mode: 1,
        term_vector_source: 2,
        char_filters: vec![],
    };

    // Per-field: the supported way to say it, and it must still work.
    let mut per_field = query_fields(2.0);
    for f in &mut per_field {
        f.analysis = Some(spec.clone());
    }
    SearchService::bm25_search(
        &coord,
        tonic::Request::new(Bm25SearchRequest {
            collection: String::new(),
            highlight: None,
            projections: Vec::new(),
            filter: String::new(),
            map_facet_fields: Vec::new(),
            score_stages: Vec::new(),
            facet_fields: Vec::new(),
            text: "smith rust".to_string(),
            k: 5,
            analysis: None,
            min_score: 0.0,
            fields: per_field,
            range_facet_fields: Vec::new(),
            geo_filters: Vec::new(),
            stats_fields: Vec::new(),
            cardinality_fields: Vec::new(),
            phrase: None,
            prefixes: Vec::new(),
        }),
    )
    .await
    .expect("per-field analysis is how a fused query carries its spec");

    // Request-level alongside fields: refused, and the message says
    // where the spec belongs.
    let err = SearchService::bm25_search(
        &coord,
        tonic::Request::new(Bm25SearchRequest {
            collection: String::new(),
            highlight: None,
            projections: Vec::new(),
            filter: String::new(),
            map_facet_fields: Vec::new(),
            score_stages: Vec::new(),
            facet_fields: Vec::new(),
            text: "smith rust".to_string(),
            k: 5,
            analysis: Some(spec),
            min_score: 0.0,
            fields: query_fields(2.0),
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
    assert!(
        err.message().contains("QueryField.analysis"),
        "the refusal must say where to put the spec: {}",
        err.message()
    );

    node_a.abort();
    mock.abort();
}

/// A sidecar that records every UNARY `Analyze` it is asked for, and
/// otherwise behaves exactly like the shared mock.
struct CountingMock {
    inner: pipestream_search::harness::mock_analysis::MockAnalysis,
    unary: std::sync::Arc<std::sync::atomic::AtomicUsize>,
    streams: std::sync::Arc<std::sync::atomic::AtomicUsize>,
}

#[tonic::async_trait]
impl pipestream_search::pb::analysis::analysis_service_server::AnalysisService for CountingMock {
    async fn analyze(
        &self,
        request: tonic::Request<pipestream_search::pb::analysis::AnalyzeRequest>,
    ) -> Result<tonic::Response<pipestream_search::pb::analysis::AnalyzeResponse>, tonic::Status>
    {
        self.unary
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        pipestream_search::pb::analysis::analysis_service_server::AnalysisService::analyze(
            &self.inner,
            request,
        )
        .await
    }

    type AnalyzeStreamStream = std::pin::Pin<
        Box<
            dyn tokio_stream::Stream<
                    Item = Result<
                        pipestream_search::pb::analysis::AnalyzeStreamResponse,
                        tonic::Status,
                    >,
                > + Send,
        >,
    >;

    async fn analyze_stream(
        &self,
        request: tonic::Request<
            tonic::Streaming<pipestream_search::pb::analysis::AnalyzeStreamRequest>,
        >,
    ) -> Result<tonic::Response<Self::AnalyzeStreamStream>, tonic::Status> {
        self.streams
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        pipestream_search::pb::analysis::analysis_service_server::AnalysisService::analyze_stream(
            &self.inner,
            request,
        )
        .await
    }

    async fn get_capabilities(
        &self,
        request: tonic::Request<pipestream_search::pb::analysis::GetCapabilitiesRequest>,
    ) -> Result<
        tonic::Response<pipestream_search::pb::analysis::GetCapabilitiesResponse>,
        tonic::Status,
    > {
        pipestream_search::pb::analysis::analysis_service_server::AnalysisService::get_capabilities(
            &self.inner,
            request,
        )
        .await
    }
}

/// Extra fields ride AnalyzeStream, not one unary `Analyze` per field.
///
/// The body has always streamed; extra fields used to spawn a unary call
/// each, which at rebuild scale (86.6M chunks x several body columns) is
/// hundreds of millions of h2 streams against the sidecar the rebuild
/// README names as the ingest throughput ceiling. This pins the
/// transport, because nothing else would notice it regressing: the unary
/// path produced byte-identical postings, just far more slowly.
///
/// Ingests 6 two-field documents in ONE call and asserts the sidecar saw
/// ZERO unary calls, a BOUNDED number of streams (one for the body spec
/// plus one per distinct field spec, NOT one per document), and the same
/// per-field term identity the unary path produced.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn extra_fields_ride_the_analysis_stream_not_unary_calls() {
    let unary = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let streams = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let analysis = format!("http://{}", listener.local_addr().unwrap());
    let mock = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(
                pipestream_search::pb::analysis::analysis_service_server::AnalysisServiceServer::new(
                    CountingMock {
                        inner: pipestream_search::harness::mock_analysis::MockAnalysis::default(),
                        unary: unary.clone(),
                        streams: streams.clone(),
                    },
                ),
            )
            .serve_with_incoming(pipestream_search::harness::nodelay_incoming(listener)),
    );

    let (addr, node) = start_empty_node(two_field_node(&analysis, 0, None)).await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();

    // Every document in CORPUS[0] and CORPUS[1], in one ingest call: six
    // documents, four of which carry a case_name.
    let docs: Vec<AddDocumentsRequest> = CORPUS
        .iter()
        .flat_map(|shard| shard.iter())
        .map(|&(body, name)| doc_request(body, name))
        .collect();
    let named = docs.iter().filter(|d| !d.fields.is_empty()).count();
    assert_eq!(named, 4, "fixture: four documents carry a case_name");

    let (tx, rx) = mpsc::channel(16);
    for doc in docs {
        tx.send(doc).await.unwrap();
    }
    drop(tx);
    let added = client
        .add_documents(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    assert_eq!((added.added, added.total), (6, 6));

    assert_eq!(
        unary.load(std::sync::atomic::Ordering::Relaxed),
        0,
        "extra fields must not fall back to unary Analyze; that is the whole point"
    );
    // Body spec + field spec, both None here, so two sessions: one for
    // the body and one shared by every field. The bound that matters is
    // that it does NOT scale with document or field count.
    let opened = streams.load(std::sync::atomic::Ordering::Relaxed);
    assert!(
        opened <= 2,
        "expected one stream per distinct spec (<=2), saw {opened}; \
         streams must not scale with the {named} analyzed fields"
    );

    // Term identity is unchanged: case_name df("smith") = 3 across the
    // whole corpus ("Smith v Jones", "Smith v Smith", and "smith" in no
    // other name), body df("smith") = 1.
    let stats = client
        .term_stats(TermStatsRequest {
            terms: vec!["smith".into()],
            fields: vec![FieldTerms {
                field: "case_name".into(),
                terms: vec!["smith".into(), "vector".into()],
            }],
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(stats.doc_frequencies, vec![1], "body df(smith)");
    assert_eq!(
        stats.field_stats[0].doc_frequencies,
        vec![2, 1],
        "case_name df(smith), df(vector) — the unary path's exact values"
    );

    node.abort();
    mock.abort();
}

/// A column built under one analyzer and queried under another is
/// REFUSED, not silently scored.
///
/// This is the guard that did not exist when the v7 corpus was built
/// under SOURCE_STEMS. Field NAME agreement was the only check, and a
/// name matches whatever the terms underneath it mean, so the mismatch
/// produced a confident wrong ranking with no error anywhere. The
/// rebuild adds a second body column whose entire purpose is to be
/// analyzed differently from the first, which turns a latent hazard into
/// a live one: the two columns will differ by exactly the thing the name
/// does not carry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_column_queried_under_the_wrong_analyzer_is_refused() {
    use pipestream_search::analyzer::{analysis_fingerprint, body_spec, cased_body_spec};
    use pipestream_search::pb::{Bm25FieldLeg, Bm25QueryRequest};

    let (analysis, mock) = start_mock_analysis().await;
    let (addr, node) = start_empty_node(two_field_node(&analysis, 0, None)).await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();

    // Ingest the body under the FOLDED analyzer, which is what the
    // shard then records for field 0.
    let folded = body_spec();
    let (tx, rx) = mpsc::channel(8);
    for (body, name) in CORPUS[0] {
        let mut doc = doc_request(body, *name);
        doc.analysis = Some(folded.clone());
        tx.send(doc).await.unwrap();
    }
    drop(tx);
    let added = client
        .add_documents(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(added.added, 3);

    let leg = |fingerprint: u64| Bm25QueryRequest {
        highlight: None,
        projections: Vec::new(),
        filter: None,
        map_facet_fields: Vec::new(),
        score_stages: Vec::new(),
        facet_fields: Vec::new(),
        expected_stats_epoch: 0,
        terms: Vec::new(),
        k: 5,
        global_doc_count: 3,
        global_total_doc_length: 12,
        global_doc_frequencies: Vec::new(),
        k1: 0.0,
        b: 0.0,
        min_score: 0.0,
        fields: vec![Bm25FieldLeg {
            field: "body".to_string(),
            terms: vec!["rust".to_string()],
            global_total_doc_length: 12,
            global_doc_frequencies: vec![3],
            weight: 1.0,
            k1: 1.2,
            b: 0.75,
            analysis_fingerprint: fingerprint,
            phrase: None,
        }],
        range_facet_fields: Vec::new(),
        geo_filters: Vec::new(),
        stats_fields: Vec::new(),
        cardinality_fields: Vec::new(),
        phrase: None,
    };

    // The analyzer it was built with: answered.
    let ok = client
        .bm25_query(leg(analysis_fingerprint(Some(&folded))))
        .await
        .expect("the ingest analyzer must be accepted")
        .into_inner();
    assert!(!ok.hits.is_empty(), "matching analyzer should return hits");

    // The OTHER analyzer of the A/B: refused, and the error names both
    // fingerprints so the mismatch is diagnosable rather than merely
    // reported.
    let err = client
        .bm25_query(leg(analysis_fingerprint(Some(&cased_body_spec()))))
        .await
        .expect_err("a mismatched analyzer must be refused, not scored");
    assert_eq!(err.code(), tonic::Code::FailedPrecondition, "{err:?}");
    assert!(
        err.message().contains("term identities"),
        "error should explain WHY: {}",
        err.message()
    );

    // 0 means "I do not know my own analyzer", which disables the check
    // rather than failing closed: probes and tools that hand-type terms
    // must keep working, and a shard predating fingerprints reads 0 too.
    let unknown = client
        .bm25_query(leg(0))
        .await
        .expect("an undeclared analyzer must not be refused")
        .into_inner();
    assert!(!unknown.hits.is_empty());

    node.abort();
    mock.abort();
}
