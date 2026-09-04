//! Prefix terms and the byte-ordered dictionaries under them
//! (`docs/prefix-terms.md`): expansion exactness against a brute-force
//! dictionary scan, the cap refusal with its count, distributed equality,
//! the mmap reader's binary-search path, string ranges over sorted
//! values, and the old-file refusal.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;

use common::start_empty_node;
use pipestream_search::analyzer::{analyze_document_native, body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::coordinator::{
    CoordinatorServiceImpl, DEFAULT_PREFIX_EXPANSIONS, MAX_PREFIX_EXPANSIONS,
};
use pipestream_search::node::{bm25_sidecar_path, Bm25Shard, NodeConfig, NodeServiceImpl};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    search_query, selection_query, AddDocumentsRequest, Bm25Hit, Bm25SearchRequest,
    Bm25SearchResponse, ExpandTermPrefixRequest, FacetValue, FlushRequest, LexicalQuery,
    QueryField, QueryRequest, SearchQuery, SelectionQuery, TermPrefix,
};
use pipestream_search::postings::{AnalyzedDoc, Bm25Index, Bm25Reader, Bm25Store};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Request};

/// A corpus whose dictionary has a dense "cour…" neighbourhood, so a
/// prefix expands to several stems while its neighbours do not match.
const CORPUS: [&str; 8] = [
    "the court courted courtesy",
    "courthouse courier coupon",
    "Court of appeals courts",
    "vector search couples",
    "courtroom drama and coupons",
    "cousin cove cover coverage",
    "courtesy of the court",
    "search engines",
];

const SPLIT: usize = 4;

fn config(slot_offset: u64) -> NodeConfig {
    NodeConfig {
        slot_offset,
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        ..Default::default()
    }
}

async fn ingest(addr: &str, docs: &[&str]) {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(8);
    for text in docs {
        tx.send(AddDocumentsRequest {
            text: text.to_string(),
            analysis: Some(body_spec()),
            ..Default::default()
        })
        .await
        .unwrap();
    }
    drop(tx);
    let added = client
        .add_documents(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner()
        .added;
    assert_eq!(added as usize, docs.len());
}

fn coordinator(addrs: Vec<String>) -> CoordinatorServiceImpl {
    CoordinatorServiceImpl::new(addrs).with_bm25(
        Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        Default::default(),
    )
}

fn prefix_request(text: &str, prefixes: &[(&str, u32)]) -> Bm25SearchRequest {
    Bm25SearchRequest {
        text: text.to_string(),
        k: 10,
        analysis: Some(body_spec()),
        prefixes: prefixes
            .iter()
            .map(|(prefix, cap)| TermPrefix {
                prefix: prefix.to_string(),
                max_expansions: *cap,
            })
            .collect(),
        ..Default::default()
    }
}

async fn bm25(c: &CoordinatorServiceImpl, req: Bm25SearchRequest) -> Bm25SearchResponse {
    SearchService::bm25_search(c, Request::new(req))
        .await
        .unwrap()
        .into_inner()
}

fn signature(hits: &[Bm25Hit]) -> Vec<(u64, u32)> {
    hits.iter().map(|h| (h.doc_id, h.score.to_bits())).collect()
}

/// The brute-force oracle, independent of the index: every body term
/// the analyzer emitted for the documents that starts with `prefix`,
/// in byte order.
fn brute_force(docs: &[AnalyzedDoc], prefix: &str) -> Vec<String> {
    let terms: BTreeSet<&str> = docs
        .iter()
        .flat_map(|doc| doc.fields[0].terms.iter().map(|(term, _, _)| term.as_str()))
        .filter(|t| t.starts_with(prefix))
        .collect();
    terms.into_iter().map(str::to_string).collect()
}

/// The OR query a caller would have typed by hand: one surface word of
/// the corpus per stem under the prefix, in the stems' byte order (the
/// order the expansion appends them in). Stems are not re-analyzed —
/// Porter is not idempotent (`courthous` would stem again to
/// `courthou`), so the oracle goes back to the words.
fn surface_or_query(prefix: &str) -> String {
    let mut by_stem: BTreeMap<String, String> = BTreeMap::new();
    for word in CORPUS.iter().flat_map(|text| text.split_whitespace()) {
        let doc = analyze_document_native(word, Some(&body_spec())).unwrap();
        for (stem, _, _) in &doc.fields[0].terms {
            if stem.starts_with(prefix) {
                by_stem
                    .entry(stem.clone())
                    .or_insert_with(|| word.to_string());
            }
        }
    }
    by_stem.into_values().collect::<Vec<_>>().join(" ")
}

fn analyzed_corpus() -> Vec<AnalyzedDoc> {
    CORPUS
        .iter()
        .map(|text| analyze_document_native(text, Some(&body_spec())).unwrap())
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn prefix_expansion_is_the_brute_force_dictionary_scan() {
    let (a, ha) = start_empty_node(config(0)).await;
    let (b, hb) = start_empty_node(config(SPLIT as u64)).await;
    ingest(&a, &CORPUS[..SPLIT]).await;
    ingest(&b, &CORPUS[SPLIT..]).await;
    let coordinator = coordinator(vec![a.clone(), b.clone()]);
    let corpus = analyzed_corpus();

    for prefix in ["cour", "court", "cou", "c", "coup", "zzz"] {
        let want = brute_force(&corpus, prefix);
        let resp = bm25(&coordinator, prefix_request("", &[(prefix, 0)])).await;
        assert_eq!(resp.prefix_expansions.len(), 1, "{prefix}");
        assert_eq!(resp.prefix_expansions[0].field, "body");
        assert_eq!(resp.prefix_expansions[0].prefix, prefix);
        assert_eq!(
            resp.prefix_expansions[0].terms, want,
            "{prefix}: expansion must equal the brute-force dictionary scan"
        );
        if want.is_empty() {
            assert!(resp.hits.is_empty(), "{prefix}");
            continue;
        }
        // The hits are exactly the OR query over the expanded terms a
        // caller would have typed: compare bitwise.
        let expanded = bm25(
            &coordinator,
            Bm25SearchRequest {
                text: surface_or_query(prefix),
                k: 10,
                analysis: Some(body_spec()),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(signature(&resp.hits), signature(&expanded.hits), "{prefix}");
    }
    // The prefix is normalized under the field's char filters: a
    // capitalized prefix matches the folded dictionary.
    let folded = bm25(&coordinator, prefix_request("", &[("Cour", 0)])).await;
    assert_eq!(folded.prefix_expansions[0].prefix, "cour");
    assert_eq!(
        folded.prefix_expansions[0].terms,
        brute_force(&corpus, "cour")
    );
    ha.abort();
    hb.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_cap_refuses_with_the_count_and_never_truncates() {
    let (a, ha) = start_empty_node(config(0)).await;
    let (b, hb) = start_empty_node(config(SPLIT as u64)).await;
    ingest(&a, &CORPUS[..SPLIT]).await;
    ingest(&b, &CORPUS[SPLIT..]).await;
    let coordinator = coordinator(vec![a.clone(), b.clone()]);
    let corpus = analyzed_corpus();
    let want = brute_force(&corpus, "cou");
    assert!(want.len() > 3, "fixture needs a wide prefix: {want:?}");

    // Below the union size the request refuses naming the count.
    let error = SearchService::bm25_search(
        &coordinator,
        Request::new(prefix_request("", &[("cou", 3)])),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(
        error.message().contains("expands to") && error.message().contains("the cap is 3"),
        "{}",
        error.message()
    );
    // Exactly at the union size it serves — the cap is inclusive.
    let exact = bm25(
        &coordinator,
        prefix_request("", &[("cou", want.len() as u32)]),
    )
    .await;
    assert_eq!(exact.prefix_expansions[0].terms, want);
    // Above the absolute maximum, or with an empty prefix, refuses by name.
    let error = SearchService::bm25_search(
        &coordinator,
        Request::new(prefix_request(
            "",
            &[("cou", MAX_PREFIX_EXPANSIONS as u32 + 1)],
        )),
    )
    .await
    .unwrap_err();
    assert!(
        error.message().contains("exceeds the maximum"),
        "{}",
        error.message()
    );
    let error =
        SearchService::bm25_search(&coordinator, Request::new(prefix_request("", &[("", 0)])))
            .await
            .unwrap_err();
    assert!(error.message().contains("non-empty"), "{}", error.message());
    // Without an analysis spec the prefix cannot be normalized; refuse
    // rather than guess the chain.
    let mut unspecified = prefix_request("", &[("cou", 0)]);
    unspecified.analysis = None;
    let error = SearchService::bm25_search(&coordinator, Request::new(unspecified))
        .await
        .unwrap_err();
    assert!(
        error.message().contains("explicit AnalysisSpec"),
        "{}",
        error.message()
    );
    // The default cap is what an unset `max_expansions` means.
    let defaulted = bm25(&coordinator, prefix_request("", &[("cou", 0)])).await;
    let explicit = bm25(
        &coordinator,
        prefix_request("", &[("cou", DEFAULT_PREFIX_EXPANSIONS as u32)]),
    )
    .await;
    assert_eq!(defaulted.prefix_expansions, explicit.prefix_expansions);
    ha.abort();
    hb.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_prefix_scoring_equals_the_monolith() {
    let (a, ha) = start_empty_node(config(0)).await;
    let (b, hb) = start_empty_node(config(SPLIT as u64)).await;
    let (m, hm) = start_empty_node(config(0)).await;
    ingest(&a, &CORPUS[..SPLIT]).await;
    ingest(&b, &CORPUS[SPLIT..]).await;
    ingest(&m, &CORPUS).await;
    let fleet = coordinator(vec![a, b]);
    let mono = coordinator(vec![m]);
    for (text, prefixes) in [
        ("", vec![("cour", 0)]),
        ("search", vec![("cou", 0)]),
        ("drama", vec![("court", 0), ("cov", 0)]),
        ("", vec![("coup", 0)]),
    ] {
        let fleet_resp = bm25(&fleet, prefix_request(text, &prefixes)).await;
        let mono_resp = bm25(&mono, prefix_request(text, &prefixes)).await;
        assert_eq!(
            signature(&fleet_resp.hits),
            signature(&mono_resp.hits),
            "{text:?} {prefixes:?}"
        );
        assert_eq!(fleet_resp.prefix_expansions, mono_resp.prefix_expansions);
        assert_eq!(fleet_resp.kth_best.to_bits(), mono_resp.kth_best.to_bits());
    }
    // Fused route: the prefix sits on the field.
    let fused = || Bm25SearchRequest {
        text: "search".into(),
        k: 10,
        fields: vec![QueryField {
            field: "body".into(),
            analysis: Some(body_spec()),
            weight: 1.0,
            k1: 0.0,
            b: 0.0,
            phrase: None,
            prefixes: vec![TermPrefix {
                prefix: "cour".into(),
                max_expansions: 0,
            }],
            synonyms: Vec::new(),
            synonyms_off: false,
        }],
        ..Default::default()
    };
    let (f, m) = (bm25(&fleet, fused()).await, bm25(&mono, fused()).await);
    assert_eq!(signature(&f.hits), signature(&m.hits));
    assert_eq!(f.prefix_expansions.len(), 1);
    // The query adapter's single lexical leaf serves the same list.
    let adapter = SearchService::query(
        &fleet,
        Request::new(QueryRequest {
            k: 10,
            selection: Some(SelectionQuery {
                node: Some(selection_query::Node::Search(SearchQuery {
                    id: "lex".into(),
                    query: Some(search_query::Query::Lexical(LexicalQuery {
                        text: "search".into(),
                        analysis: Some(body_spec()),
                        score_stages: Vec::new(),
                        phrase: None,
                        prefixes: vec![TermPrefix {
                            prefix: "cour".into(),
                            max_expansions: 0,
                        }],
                        synonyms: Vec::new(),
                        synonyms_off: false,
                    })),
                })),
            }),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    let direct = bm25(&fleet, prefix_request("search", &[("cour", 0)])).await;
    let got: Vec<(u64, u32)> = adapter
        .hits
        .iter()
        .map(|h| (h.doc_id, h.score.to_bits()))
        .collect();
    assert_eq!(got, signature(&direct.hits));
    // A top-level prefix list on a fused request refuses rather than
    // silently expanding in the body.
    let error = SearchService::bm25_search(
        &fleet,
        Request::new(Bm25SearchRequest {
            text: "search".into(),
            k: 10,
            fields: vec![QueryField {
                field: "body".into(),
                analysis: Some(body_spec()),
                weight: 1.0,
                k1: 0.0,
                b: 0.0,
                phrase: None,
                prefixes: Vec::new(),
                synonyms: Vec::new(),
                synonyms_off: false,
            }],
            prefixes: vec![TermPrefix {
                prefix: "cour".into(),
                max_expansions: 0,
            }],
            ..Default::default()
        }),
    )
    .await
    .unwrap_err();
    assert!(
        error.message().contains("QueryField"),
        "{}",
        error.message()
    );
    ha.abort();
    hb.abort();
    hm.abort();
}

type Predicate = Box<dyn Fn(&str) -> bool>;

fn tempdir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("prefix-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// The mmap reader's path: a large dictionary flushed to disk, expanded
/// through the byte-sorted directory's binary search, equal to the heap
/// store's ordered-map walk and to the brute-force scan.
#[test]
fn the_reader_expands_by_binary_search_over_a_large_dictionary() {
    let dir = tempdir("reader");
    let mut store = Bm25Store::new();
    // 5,000 distinct terms with shared prefixes of every length.
    let mut text = String::new();
    for i in 0..5000u32 {
        text.push_str(&format!("t{:05} ", (i * 7919) % 100_000));
    }
    text.push_str("court courtesy courts couple");
    let doc = analyze_document_native(&text, Some(&body_spec())).unwrap();
    store.add_document(0, text.clone(), doc.clone());
    let path = dir.join("big.bm25");
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    for prefix in ["t000", "t0", "t", "t99", "cour", "coupl", "nope", "t00042"] {
        let want = brute_force(std::slice::from_ref(&doc), prefix);
        let cap = want.len().max(1);
        assert_eq!(
            reader.expand_prefix(prefix, cap),
            Ok(want.clone()),
            "{prefix}"
        );
        assert_eq!(
            store.expand_prefix(prefix, cap),
            Ok(want.clone()),
            "{prefix}"
        );
        if !want.is_empty() {
            // One below the count refuses with the exact count on both.
            assert_eq!(
                reader.expand_prefix(prefix, want.len() - 1),
                Err(want.len())
            );
            assert_eq!(store.expand_prefix(prefix, want.len() - 1), Err(want.len()));
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// The shard RPC reports the exact count past the cap and `known`
/// for a field it lacks; a flushed shard answers from the mmap reader.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_shard_rpc_reports_counts_and_unknown_fields() {
    let dir = tempdir("rpc");
    let index_path = dir.join("shard.tv");
    let (addr, handle) = start_empty_node(NodeConfig {
        index_path: Some(index_path.clone()),
        layout: pipestream_search::node::Layout::SingleImage,
        ..config(0)
    })
    .await;
    ingest(&addr, &CORPUS).await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    assert!(
        client
            .flush(FlushRequest {})
            .await
            .unwrap()
            .into_inner()
            .written
    );
    let corpus = analyzed_corpus();
    let want = brute_force(&corpus, "cou");
    let full = client
        .expand_term_prefix(ExpandTermPrefixRequest {
            field: "body".into(),
            prefix: "cou".into(),
            cap: want.len() as u32,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(full.known);
    assert_eq!(full.terms, want);
    assert_eq!(full.count as usize, want.len());
    let capped = client
        .expand_term_prefix(ExpandTermPrefixRequest {
            field: "body".into(),
            prefix: "cou".into(),
            cap: 1,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(capped.terms.is_empty());
    assert_eq!(capped.count as usize, want.len());
    let unknown = client
        .expand_term_prefix(ExpandTermPrefixRequest {
            field: "title".into(),
            prefix: "cou".into(),
            cap: 10,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!unknown.known);
    // Through the reader directly, the same file.
    let reader = Bm25Reader::open(&bm25_sidecar_path(&index_path)).unwrap();
    assert_eq!(reader.expand_prefix("cou", want.len()), Ok(want));
    handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Facet values ingested in scrambled order, flushed, and queried by
/// string range and prefix: the flushed dictionary is in byte order and
/// the ranges are exact against the sorted values; a file written with
/// the old first-seen order refuses by name and still serves equality.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn string_ranges_resolve_over_sorted_dictionaries_and_old_files_refuse() {
    let dir = tempdir("ranges");
    let courts = ["scotus", "ca9", "ca5", "dcc", "ca11", "ca1", "nysd", "ca10"];
    // Heap store in scrambled first-seen order.
    let mut store = Bm25Store::with_fields(&["body"]).with_facets(&["court"]);
    for (i, court) in courts.iter().enumerate() {
        let text = format!("opinion {court}");
        let doc = analyze_document_native(&text, Some(&body_spec())).unwrap();
        store.add_document(i as u32, text, doc);
        store.set_facet(0, i as u32, court);
    }
    assert!(!store.facet_dictionary_sorted(0), "fixture is scrambled");
    let sorted_path = dir.join("sorted.bm25");
    let legacy_path = dir.join("legacy.bm25");
    store.save(&sorted_path).unwrap();
    store.save_first_seen_dictionaries(&legacy_path).unwrap();
    let sorted = Bm25Reader::open(&sorted_path).unwrap();
    let legacy = Bm25Reader::open(&legacy_path).unwrap();
    assert!(sorted.facet_dictionary_sorted(0));
    assert!(!legacy.facet_dictionary_sorted(0));
    let mut expected: Vec<&str> = courts.to_vec();
    expected.sort_unstable();
    assert_eq!(sorted.facet_dictionary(0), expected);
    assert_eq!(legacy.facet_dictionary(0), courts);
    // Ordinals moved with the values: every document still reads its
    // own court back from either file, and a reload keeps byte order.
    for (i, court) in courts.iter().enumerate() {
        for reader in [&sorted, &legacy] {
            let ord = reader.facet_ord(0, i as u32).unwrap();
            assert_eq!(reader.facet_value(0, ord), *court);
        }
    }
    assert!(Bm25Store::load(&sorted_path)
        .unwrap()
        .facet_dictionary_sorted(0));

    let serve = |path: PathBuf| async move {
        let shard = Bm25Shard::open(&path).unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = format!("http://{}", listener.local_addr().unwrap());
        let service = NodeServiceImpl::new(
            None,
            NodeConfig {
                facet_fields: vec!["court".to_string()],
                ..config(0)
            },
        )
        .with_bm25(Some(shard));
        let handle = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(service.into_server(pipestream_search::MAX_MESSAGE_BYTES))
                .serve_with_incoming(pipestream_search::harness::nodelay_incoming(listener)),
        );
        (coordinator(vec![addr]), handle)
    };
    let filtered = |c: &CoordinatorServiceImpl, filter: &str| {
        let filter = filter.to_string();
        let c = c.clone();
        async move {
            SearchService::bm25_search(
                &c,
                Request::new(Bm25SearchRequest {
                    text: "opinion".into(),
                    k: 10,
                    analysis: Some(body_spec()),
                    filter,
                    ..Default::default()
                }),
            )
            .await
        }
    };
    let ids = |resp: Bm25SearchResponse| {
        let mut ids: Vec<u64> = resp.hits.iter().map(|h| h.doc_id).collect();
        ids.sort_unstable();
        ids
    };
    let expect = |pred: &dyn Fn(&str) -> bool| -> Vec<u64> {
        courts
            .iter()
            .enumerate()
            .filter(|(_, c)| pred(c))
            .map(|(i, _)| i as u64)
            .collect()
    };

    let (sorted_c, sorted_h) = serve(sorted_path).await;
    let cases: Vec<(&str, Predicate)> = vec![
        (r#"court < "ca9""#, Box::new(|c| c < "ca9")),
        (r#"court <= "ca9""#, Box::new(|c| c <= "ca9")),
        (r#"court > "ca9""#, Box::new(|c| c > "ca9")),
        (r#"court >= "dcc""#, Box::new(|c| c >= "dcc")),
        (
            r#"court >= "ca1" && court < "ca5""#,
            Box::new(|c| ("ca1".."ca5").contains(&c)),
        ),
        (
            r#"court.startsWith("ca1")"#,
            Box::new(|c| c.starts_with("ca1")),
        ),
        (
            r#"court.startsWith("ca")"#,
            Box::new(|c| c.starts_with("ca")),
        ),
        (r#"!(court < "d")"#, Box::new(|c| c >= "d")),
        (r#"court.startsWith("zz")"#, Box::new(|_| false)),
        (r#"court > "zz""#, Box::new(|_| false)),
        (r#"court == "scotus""#, Box::new(|c| c == "scotus")),
    ];
    for (filter, pred) in &cases {
        let resp = filtered(&sorted_c, filter).await.unwrap().into_inner();
        assert_eq!(ids(resp), expect(pred.as_ref()), "{filter}");
    }
    sorted_h.abort();

    let (legacy_c, legacy_h) = serve(legacy_path).await;
    let equal = filtered(&legacy_c, r#"court == "scotus""#)
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ids(equal), vec![0], "equality still serves on an old file");
    for filter in [r#"court < "ca9""#, r#"court.startsWith("ca")"#] {
        let error = filtered(&legacy_c, filter).await.unwrap_err();
        assert_eq!(error.code(), Code::FailedPrecondition, "{filter}");
        assert!(
            error.message().contains("first-seen") && error.message().contains("\"court\""),
            "{filter}: {}",
            error.message()
        );
    }
    legacy_h.abort();

    // The heap builder answers ranges as membership over its first-seen
    // dictionary — never refused, never walked per document.
    let (addr, handle) = start_empty_node(NodeConfig {
        facet_fields: vec!["court".to_string()],
        ..config(0)
    })
    .await;
    {
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        let (tx, rx) = mpsc::channel(8);
        for court in courts {
            tx.send(AddDocumentsRequest {
                text: format!("opinion {court}"),
                analysis: Some(body_spec()),
                facets: vec![FacetValue {
                    field: "court".into(),
                    value: court.into(),
                }],
                ..Default::default()
            })
            .await
            .unwrap();
        }
        drop(tx);
        client.add_documents(ReceiverStream::new(rx)).await.unwrap();
    }
    let heap_c = coordinator(vec![addr]);
    for (filter, pred) in &cases {
        let resp = filtered(&heap_c, filter).await.unwrap().into_inner();
        assert_eq!(ids(resp), expect(pred.as_ref()), "heap {filter}");
    }
    handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
