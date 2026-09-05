//! Autocomplete over the byte-sorted dictionary (`docs/suggest.md`):
//! suggestions equal a brute-force ranked scan of the analyzed corpus,
//! two shards equal one with df summed, the limit and scan bound with
//! their defaults and refusals, prefix normalization (char filters,
//! never the stemmer), every indexed field kind (body, the cased twin,
//! the glossary phrase field), a segmented shard against one image and
//! its mmap reopen, the tombstone flag, collection isolation, the bearer
//! gate, and the cost gate that refuses past the bound without
//! materializing terms.

mod common;

use std::collections::{BTreeMap, BTreeSet};
use std::path::PathBuf;
use std::sync::Arc;

use common::{start_empty_node, start_opened_node};
use pipestream_search::analyzer::{
    analyze_document_native, body_spec, cased_twin_spec, normalize_prefix, NATIVE_ANALYSIS_BACKEND,
};
use pipestream_search::collections::CollectionSet;
use pipestream_search::coordinator::{
    CoordinatorServiceImpl, DEFAULT_SUGGEST_LIMIT, DEFAULT_SUGGEST_SCAN, MAX_SUGGEST_LIMIT,
    MAX_SUGGEST_SCAN,
};
use pipestream_search::harness::start_empty_phrase_node;
use pipestream_search::node::{bm25_sidecar_path, Layout, NodeConfig};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    AddDocumentsRequest, AnalysisSpec, DeleteDocumentsRequest, FlushRequest, SuggestRequest,
    SuggestResponse, SuggestTermsRequest, Suggestion,
};
use pipestream_search::phrases::PhraseIndex;
use pipestream_search::postings::{AnalyzedDoc, Bm25Index, Bm25Reader, Bm25Store};
use pipestream_search::security::{PrincipalConfig, Principals};
use protomolt_analyzer::GlossaryEntry;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Request};

/// A corpus whose dictionary has a dense "cou…" neighbourhood with
/// repeated stems, so df varies and ties exist among the singletons.
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

fn tempdir(tag: &str) -> PathBuf {
    let dir = PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("suggest_{tag}_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

async fn ingest(addr: &str, docs: &[&str]) {
    ingest_requests(
        addr,
        docs.iter()
            .map(|text| AddDocumentsRequest {
                text: text.to_string(),
                analysis: Some(body_spec()),
                ..Default::default()
            })
            .collect(),
    )
    .await;
}

async fn ingest_requests(addr: &str, docs: Vec<AddDocumentsRequest>) {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(8);
    let n = docs.len();
    for doc in docs {
        tx.send(doc).await.unwrap();
    }
    drop(tx);
    let added = client
        .add_documents(ReceiverStream::new(rx))
        .await
        .unwrap()
        .into_inner()
        .added;
    assert_eq!(added as usize, n);
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

fn coordinator(addrs: Vec<String>) -> CoordinatorServiceImpl {
    CoordinatorServiceImpl::new(addrs).with_bm25(
        Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        Default::default(),
    )
}

fn request(field: &str, prefix: &str, limit: u32, max_scan: u64) -> SuggestRequest {
    SuggestRequest {
        collection: String::new(),
        field: field.to_string(),
        prefix: prefix.to_string(),
        limit,
        max_scan,
        analysis: Some(body_spec()),
    }
}

/// Everything under the prefix: the absolute limit, the default bound.
fn all(prefix: &str) -> SuggestRequest {
    request("body", prefix, MAX_SUGGEST_LIMIT as u32, 0)
}

async fn suggest<S: SearchService>(s: &S, req: SuggestRequest) -> SuggestResponse {
    SearchService::suggest(s, Request::new(req))
        .await
        .unwrap()
        .into_inner()
}

async fn refused<S: SearchService>(s: &S, req: SuggestRequest) -> tonic::Status {
    SearchService::suggest(s, Request::new(req))
        .await
        .unwrap_err()
}

fn ranked(resp: &SuggestResponse) -> Vec<(String, u64)> {
    resp.suggestions
        .iter()
        .map(|s| (s.term.clone(), s.df))
        .collect()
}

fn analyzed(docs: &[&str]) -> Vec<AnalyzedDoc> {
    docs.iter()
        .map(|text| analyze_document_native(text, Some(&body_spec())).unwrap())
        .collect()
}

/// The brute-force oracle, independent of the index: every body term
/// the analyzer emitted that starts with `prefix`, with the number of
/// documents holding it, ranked by df descending then term bytes
/// ascending.
fn oracle(docs: &[AnalyzedDoc], prefix: &str) -> Vec<(String, u64)> {
    let mut df: BTreeMap<String, u64> = BTreeMap::new();
    for doc in docs {
        let terms: BTreeSet<&str> = doc.fields[0]
            .terms
            .iter()
            .map(|(term, _, _)| term.as_str())
            .filter(|t| t.starts_with(prefix))
            .collect();
        for term in terms {
            *df.entry(term.to_string()).or_insert(0) += 1;
        }
    }
    let mut ranked: Vec<(String, u64)> = df.into_iter().collect();
    ranked.sort_by(|a, b| {
        b.1.cmp(&a.1)
            .then_with(|| a.0.as_bytes().cmp(b.0.as_bytes()))
    });
    ranked
}

/// How many of the shards (each a slice of analyzed documents) hold
/// `term` in their dictionary.
fn holders(shards: &[&[AnalyzedDoc]], term: &str) -> u32 {
    shards
        .iter()
        .filter(|docs| {
            docs.iter()
                .any(|doc| doc.fields[0].terms.iter().any(|(t, _, _)| t == term))
        })
        .count() as u32
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn suggestions_equal_the_ranked_brute_force_scan_and_two_shards_equal_one() {
    let (a, ha) = start_empty_node(config(0)).await;
    let (b, hb) = start_empty_node(config(SPLIT as u64)).await;
    let (m, hm) = start_empty_node(config(0)).await;
    ingest(&a, &CORPUS[..SPLIT]).await;
    ingest(&b, &CORPUS[SPLIT..]).await;
    ingest(&m, &CORPUS).await;
    let fleet = coordinator(vec![a, b]);
    let mono = coordinator(vec![m]);
    let corpus = analyzed(&CORPUS);
    let (left, right) = corpus.split_at(SPLIT);

    for prefix in [
        "cou", "cour", "court", "c", "coup", "cov", "s", "zzz", "Cour", "COÜ", "coú",
    ] {
        let normalized = normalize_prefix(prefix, Some(&body_spec())).unwrap();
        let want = oracle(&corpus, &normalized);
        let fleet_resp = suggest(&fleet, all(prefix)).await;
        let mono_resp = suggest(&mono, all(prefix)).await;
        assert_eq!(
            ranked(&fleet_resp),
            want,
            "{prefix}: suggestions must equal the ranked brute-force scan"
        );
        assert_eq!(
            fleet_resp.dictionary_terms_with_prefix as usize,
            want.len(),
            "{prefix}"
        );
        assert!(!fleet_resp.df_includes_tombstoned_rows, "{prefix}");
        // Two shards equal one: the same terms, the same summed df, the
        // same count; only the shard tally differs by construction.
        assert_eq!(ranked(&fleet_resp), ranked(&mono_resp), "{prefix}");
        assert_eq!(
            fleet_resp.dictionary_terms_with_prefix, mono_resp.dictionary_terms_with_prefix,
            "{prefix}"
        );
        assert_eq!(
            fleet_resp.df_includes_tombstoned_rows, mono_resp.df_includes_tombstoned_rows,
            "{prefix}"
        );
        for s in &fleet_resp.suggestions {
            assert_eq!(
                s.shards,
                holders(&[left, right], &s.term),
                "{prefix} {}",
                s.term
            );
        }
        assert!(
            mono_resp.suggestions.iter().all(|s| s.shards == 1),
            "{prefix}"
        );
    }
    // The fixture has what the test needs: repeated stems and ties, and
    // more terms under "c" than the default limit returns.
    let want = oracle(&corpus, "c");
    assert!(want.len() > DEFAULT_SUGGEST_LIMIT, "{want:?}");
    assert!(want[0].1 > want[1].1, "a clear leader: {want:?}");
    assert!(
        want.iter().filter(|(_, df)| *df == 1).count() > 3,
        "singleton ties ordered by term bytes: {want:?}"
    );
    ha.abort();
    hb.abort();
    hm.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_limit_and_its_default() {
    let (a, ha) = start_empty_node(config(0)).await;
    let (b, hb) = start_empty_node(config(SPLIT as u64)).await;
    ingest(&a, &CORPUS[..SPLIT]).await;
    ingest(&b, &CORPUS[SPLIT..]).await;
    let c = coordinator(vec![a, b]);
    let want = oracle(&analyzed(&CORPUS), "c");
    assert!(want.len() > DEFAULT_SUGGEST_LIMIT);

    let defaulted = suggest(&c, request("body", "c", 0, 0)).await;
    assert_eq!(ranked(&defaulted), want[..DEFAULT_SUGGEST_LIMIT]);
    assert_eq!(defaulted.dictionary_terms_with_prefix as usize, want.len());
    let explicit = suggest(&c, request("body", "c", DEFAULT_SUGGEST_LIMIT as u32, 0)).await;
    assert_eq!(defaulted, explicit, "an unset limit is the default");
    let three = suggest(&c, request("body", "c", 3, 0)).await;
    assert_eq!(ranked(&three), want[..3]);
    assert_eq!(three.dictionary_terms_with_prefix as usize, want.len());
    let everything = suggest(&c, request("body", "c", want.len() as u32, 0)).await;
    assert_eq!(ranked(&everything), want);
    let error = refused(&c, request("body", "c", MAX_SUGGEST_LIMIT as u32 + 1, 0)).await;
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(
        error
            .message()
            .contains(&format!("exceeds the maximum {MAX_SUGGEST_LIMIT}")),
        "{}",
        error.message()
    );
    // The default scan bound is what an unset `max_scan` means.
    let bounded = suggest(&c, request("body", "c", 0, DEFAULT_SUGGEST_SCAN as u64)).await;
    assert_eq!(defaulted, bounded);
    ha.abort();
    hb.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_refusal_table() {
    let (a, ha) = start_empty_node(config(0)).await;
    let (b, hb) = start_empty_node(config(SPLIT as u64)).await;
    ingest(&a, &CORPUS[..SPLIT]).await;
    ingest(&b, &CORPUS[SPLIT..]).await;
    let c = coordinator(vec![a, b]);
    let corpus = analyzed(&CORPUS);
    let (left, right) = corpus.split_at(SPLIT);
    let union = oracle(&corpus, "c").len();
    let per_shard = oracle(left, "c").len().max(oracle(right, "c").len());
    assert!(per_shard < union, "the fixture needs a term on both shards");

    // Past the bound on one shard: the shard's count, no terms.
    let error = refused(&c, request("body", "c", 0, per_shard as u64 - 1)).await;
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(
        error.message().contains("dictionary terms on shard")
            && error
                .message()
                .contains(&format!("the scan bound is {}", per_shard - 1)),
        "{}",
        error.message()
    );
    // Within the bound on every shard, past it in the union: the fleet
    // count — the count one image of the rows would report.
    let error = refused(&c, request("body", "c", 0, per_shard as u64)).await;
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(
        error.message().contains(&format!(
            "matches {union} dictionary terms across the fleet"
        )) && error
            .message()
            .contains(&format!("the scan bound is {per_shard}")),
        "{}",
        error.message()
    );
    // Exactly at the union size serves: the bound is inclusive.
    let exact = suggest(&c, request("body", "c", 0, union as u64)).await;
    assert_eq!(exact.dictionary_terms_with_prefix as usize, union);

    let table: Vec<(SuggestRequest, &str)> = vec![
        (
            request("body", "c", 0, MAX_SUGGEST_SCAN as u64 + 1),
            "exceeds the maximum",
        ),
        (request("body", "", 0, 0), "non-empty"),
        (
            request("title", "cou", 0, 0),
            "no shard indexes field \"title\"",
        ),
        (request("", "cou", 0, 0), "needs a field"),
        (
            SuggestRequest {
                collection: "nope".into(),
                ..request("body", "cou", 0, 0)
            },
            "unknown collection \"nope\"",
        ),
        (
            SuggestRequest {
                analysis: None,
                ..request("body", "cou", 0, 0)
            },
            "explicit AnalysisSpec",
        ),
        (request("body", "\u{200b}", 0, 0), "normalizes to nothing"),
    ];
    for (req, reason) in table {
        let error = refused(&c, req.clone()).await;
        assert_eq!(error.code(), Code::InvalidArgument, "{req:?}");
        assert!(
            error.message().contains(reason),
            "{req:?}: {}",
            error.message()
        );
    }

    // The stemmer is never applied to the prefix: the dictionary holds
    // "court" and "courtesi", so "courts" and "courtesy" — which stem to
    // those — complete to nothing, while a prefix of the stem does.
    for prefix in ["courts", "courtesy"] {
        let resp = suggest(&c, all(prefix)).await;
        assert!(resp.suggestions.is_empty(), "{prefix}: {resp:?}");
        assert_eq!(resp.dictionary_terms_with_prefix, 0, "{prefix}");
    }
    let stem = suggest(&c, all("courtes")).await;
    assert_eq!(ranked(&stem), vec![("courtesi".to_string(), 2)]);
    // Char filters do apply: case and accents fold like prefix terms.
    let folded = suggest(&c, all("cour")).await;
    for prefix in ["Cour", "COUR", "coür", "Coúr"] {
        assert_eq!(suggest(&c, all(prefix)).await, folded, "{prefix}");
    }
    ha.abort();
    hb.abort();
}

/// Every indexed BM25 field completes: the folded body, the cased twin
/// (compared as written, since SOURCE_STEMS ignores char filters), and
/// the glossary phrase field, whose dictionary is the registered
/// phrases present in the corpus.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_cased_twin_and_the_glossary_field_complete() {
    let (addr, handle) = start_empty_node(NodeConfig {
        bm25_fields: vec!["body".into(), "body_cased".into()],
        ..config(0)
    })
    .await;
    ingest_requests(
        &addr,
        [
            "COURT court Court holds",
            "Court of appeals",
            "court reporter",
            "APPEAL denied no court here",
        ]
        .iter()
        .map(|text| AddDocumentsRequest {
            text: text.to_string(),
            analysis: Some(body_spec()),
            cased_field: "body_cased".into(),
            ..Default::default()
        })
        .collect(),
    )
    .await;
    let c = coordinator(vec![addr]);
    let twin: AnalysisSpec = cased_twin_spec(&body_spec());
    let cased = |prefix: &str| SuggestRequest {
        analysis: Some(twin.clone()),
        ..request("body_cased", prefix, 0, 0)
    };
    // The folded body has one identity for every spelling.
    let body = suggest(&c, all("Cou")).await;
    assert_eq!(ranked(&body), vec![("court".to_string(), 4)]);
    // The cased twin keeps three identities, each completed only as
    // written: lowercase "court" is in three documents, not four.
    assert_eq!(
        ranked(&suggest(&c, cased("cou")).await),
        vec![("court".to_string(), 3)]
    );
    assert_eq!(
        ranked(&suggest(&c, cased("Cou")).await),
        vec![("Court".to_string(), 2)]
    );
    assert_eq!(
        ranked(&suggest(&c, cased("COU")).await),
        vec![("COURT".to_string(), 1)]
    );
    // A cased prefix under the folded spec on the cased field folds
    // first and finds the folded spelling: normalization is the spec's,
    // not the field's, so the caller passes the field's spec.
    let mismatched = suggest(&c, request("body_cased", "Cou", 0, 0)).await;
    assert_eq!(ranked(&mismatched), vec![("court".to_string(), 3)]);
    handle.abort();

    let phrases = Arc::new(
        PhraseIndex::new(
            vec![
                GlossaryEntry {
                    id: "nyc".into(),
                    term: "New York City".into(),
                },
                GlossaryEntry {
                    id: "new-york".into(),
                    term: "New York".into(),
                },
                GlossaryEntry {
                    id: "hot-dog".into(),
                    term: "Hot Dog".into(),
                },
            ],
            "phrases".into(),
            Some("entities".into()),
            true,
            false,
        )
        .unwrap(),
    );
    let (addr, handle) = start_empty_phrase_node(
        NodeConfig {
            bm25_fields: vec!["body".into(), "phrases".into()],
            map_facet_fields: vec!["entities".into()],
            ..config(0)
        },
        phrases,
    )
    .await;
    ingest(
        &addr,
        &[
            "New York City food",
            "New York pizza",
            "Hot Dog food",
            "food",
        ],
    )
    .await;
    let c = coordinator(vec![addr]);
    let hex = |id: &str| -> String { id.bytes().map(|b| format!("{b:02x}")).collect::<String>() };
    // The phrase field's dictionary is the registered concepts, keyed
    // `$phrase:<hex id>`; the namespace prefix lists every one present.
    let registered = suggest(&c, request("phrases", "$phrase:", 0, 0)).await;
    assert_eq!(
        ranked(&registered),
        vec![
            (format!("$phrase:{}", hex("new-york")), 2),
            (format!("$phrase:{}", hex("hot-dog")), 1),
            (format!("$phrase:{}", hex("nyc")), 1),
        ]
    );
    let n = suggest(&c, request("phrases", "$phrase:6e", 0, 0)).await;
    assert_eq!(n.dictionary_terms_with_prefix, 2, "{n:?}");
    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_segmented_shard_equals_one_image_and_reopens_from_disk() {
    let dir = tempdir("segments");
    let seg_path = dir.join("segmented.tv");
    let one_path = dir.join("single.tv");
    let node_config = |path: PathBuf, layout: Layout| NodeConfig {
        index_path: Some(path),
        layout,
        wal: false,
        ..config(0)
    };
    let (seg, hs) = start_empty_node(node_config(seg_path.clone(), Layout::Segments)).await;
    let (one, ho) = start_empty_node(node_config(one_path.clone(), Layout::SingleImage)).await;
    // Two flushes seal two segments; the third batch stays in the tail.
    for (i, batch) in [&CORPUS[..3], &CORPUS[3..6], &CORPUS[6..]]
        .into_iter()
        .enumerate()
    {
        ingest(&seg, batch).await;
        ingest(&one, batch).await;
        if i < 2 {
            assert!(flush(&seg).await);
        }
    }
    assert!(flush(&one).await);
    let segmented = coordinator(vec![seg.clone()]);
    let single = coordinator(vec![one.clone()]);
    let corpus = analyzed(&CORPUS);
    let prefixes = ["cou", "cour", "court", "c", "cov", "s", "zzz"];
    for prefix in prefixes {
        let a = suggest(&segmented, all(prefix)).await;
        let b = suggest(&single, all(prefix)).await;
        assert_eq!(a, b, "{prefix}: segments must equal one image");
        assert_eq!(ranked(&a), oracle(&corpus, prefix), "{prefix}");
    }
    // Seal the tail too, then reopen both from disk through the mmap
    // reader: the same answers.
    assert!(flush(&seg).await);
    let mut before = Vec::new();
    for prefix in prefixes {
        before.push(suggest(&segmented, all(prefix)).await);
    }
    hs.abort();
    ho.abort();
    let (seg, hs) = start_opened_node(node_config(seg_path.clone(), Layout::Segments)).await;
    let (one, ho) = start_opened_node(node_config(one_path.clone(), Layout::SingleImage)).await;
    let segmented = coordinator(vec![seg]);
    let single = coordinator(vec![one]);
    for (prefix, want) in prefixes.iter().zip(&before) {
        assert_eq!(&suggest(&segmented, all(prefix)).await, want, "{prefix}");
        assert_eq!(&suggest(&single, all(prefix)).await, want, "{prefix}");
    }
    // The single image's reader answers the same through the trait.
    let reader = Bm25Reader::open(&bm25_sidecar_path(&one_path)).unwrap();
    let want: Vec<(String, u32)> = {
        let mut entries: Vec<(String, u32)> = oracle(&corpus, "cou")
            .into_iter()
            .map(|(term, df)| (term, df as u32))
            .collect();
        entries.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        entries
    };
    assert_eq!(reader.suggest_prefix("cou", want.len()), Ok(want.clone()));
    assert_eq!(
        reader.suggest_prefix("cou", want.len() - 1),
        Err(want.len())
    );
    hs.abort();
    ho.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// df is posting df: a delete flips the flag and changes nothing else
/// until compaction restores exactness.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_tombstone_flag_reports_that_df_counts_deleted_rows() {
    let (addr, handle) = start_empty_node(config(0)).await;
    ingest(&addr, &CORPUS).await;
    let c = coordinator(vec![addr.clone()]);
    let before = suggest(&c, all("cou")).await;
    assert!(!before.df_includes_tombstoned_rows);
    assert_eq!(ranked(&before), oracle(&analyzed(&CORPUS), "cou"));
    // Document 0 holds "court" and "courtesi".
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    let deleted = client
        .delete_documents(DeleteDocumentsRequest {
            doc_ids: vec![0],
            expected_wal_generation: None,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(deleted.deleted, 1);
    let after = suggest(&c, all("cou")).await;
    assert!(after.df_includes_tombstoned_rows);
    assert_eq!(
        ranked(&after),
        ranked(&before),
        "posting df does not move on a delete; the flag says so"
    );
    assert_eq!(
        after.dictionary_terms_with_prefix,
        before.dictionary_terms_with_prefix
    );
    // The shard reports its tombstone count with every answer.
    let shard = client
        .suggest_terms(SuggestTermsRequest {
            field: "body".into(),
            prefix: "cou".into(),
            max_scan: 1000,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(shard.known);
    assert_eq!(shard.tombstoned_rows, 1);
    assert_eq!(
        shard.count as usize,
        before.dictionary_terms_with_prefix as usize
    );
    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn collections_do_not_share_dictionaries_and_a_named_set_wants_a_name() {
    let named = |collection: &str| NodeConfig {
        collection: collection.to_string(),
        ..config(0)
    };
    let (a, ha) = start_empty_node(named("a")).await;
    let (b, hb) = start_empty_node(named("b")).await;
    ingest(&a, &["court one", "court two", "court three"]).await;
    ingest(&b, &["court only", "other doc", "third doc"]).await;
    let set = CollectionSet::named(
        vec![
            ("a".to_string(), coordinator(vec![a]).with_collection("a")),
            ("b".to_string(), coordinator(vec![b]).with_collection("b")),
        ],
        None,
    )
    .unwrap();
    let on = |collection: &str| SuggestRequest {
        collection: collection.to_string(),
        ..request("body", "cour", 0, 0)
    };
    let in_a = suggest(&set, on("a")).await;
    let in_b = suggest(&set, on("b")).await;
    assert_eq!(ranked(&in_a), vec![("court".to_string(), 3)]);
    assert_eq!(ranked(&in_b), vec![("court".to_string(), 1)]);
    let other = suggest(
        &set,
        SuggestRequest {
            collection: "b".into(),
            ..request("body", "o", 0, 0)
        },
    )
    .await;
    assert_eq!(
        ranked(&other),
        vec![("onli".to_string(), 1), ("other".to_string(), 1)],
        "b's dictionary holds b's terms only; a's \"one\" is not in it"
    );
    let error = refused(&set, on("")).await;
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(
        error.message().contains("no collection named"),
        "{}",
        error.message()
    );
    let error = refused(&set, on("c")).await;
    assert!(
        error.message().contains("unknown collection \"c\""),
        "{}",
        error.message()
    );
    ha.abort();
    hb.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_bearer_gate_applies_as_on_every_public_route() {
    const TOKEN: &str = "console-token-0123456789abcdef";
    let (addr, handle) = start_empty_node(config(0)).await;
    ingest(&addr, &CORPUS).await;
    let principals = Arc::new(
        Principals::from_configs(&[PrincipalConfig {
            name: "console".into(),
            token: TOKEN.into(),
            ..Default::default()
        }])
        .unwrap()
        .with_policy(common::access_policy(
            &["console"],
            &[""],
            &[pipestream_search::pb::AccessAction::Search],
        ))
        .unwrap(),
    );
    let set = CollectionSet::single(coordinator(vec![addr])).with_principals(principals);
    let with = |token: Option<&str>| {
        let mut req = Request::new(all("cou"));
        if let Some(token) = token {
            req.metadata_mut()
                .insert("authorization", format!("Bearer {token}").parse().unwrap());
        }
        req
    };
    let error = SearchService::suggest(&set, with(None)).await.unwrap_err();
    assert_eq!(error.code(), Code::Unauthenticated);
    let error = SearchService::suggest(&set, with(Some("nope-nope-nope-nope")))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::Unauthenticated);
    let served = SearchService::suggest(&set, with(Some(TOKEN)))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(ranked(&served), oracle(&analyzed(&CORPUS), "cou"));
    handle.abort();
}

/// The cost gate: a prefix past the bound refuses with the count and
/// no terms cross the wire — the heap store and the mmap reader count
/// without cloning, and the full scan equals the brute force.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn past_the_scan_bound_refuses_with_the_count_and_materializes_nothing() {
    let dir = tempdir("bound");
    let index_path = dir.join("shard.tv");
    let (addr, handle) = start_empty_node(NodeConfig {
        index_path: Some(index_path.clone()),
        layout: Layout::SingleImage,
        wal: false,
        ..config(0)
    })
    .await;
    // 5,000 distinct terms under one prefix.
    let mut text = String::new();
    for i in 0..5000u32 {
        text.push_str(&format!("t{:05} ", (i * 7919) % 100_000));
    }
    ingest(&addr, &[text.as_str()]).await;
    assert!(flush(&addr).await);
    let doc = analyze_document_native(&text, Some(&body_spec())).unwrap();
    let want = oracle(std::slice::from_ref(&doc), "t");
    assert_eq!(want.len(), 5000);

    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    let bounded = client
        .suggest_terms(SuggestTermsRequest {
            field: "body".into(),
            prefix: "t".into(),
            max_scan: 100,
        })
        .await
        .unwrap()
        .into_inner();
    assert!(bounded.known);
    assert_eq!(bounded.count, 5000);
    assert!(bounded.entries.is_empty(), "past the bound nothing is sent");
    let full = client
        .suggest_terms(SuggestTermsRequest {
            field: "body".into(),
            prefix: "t".into(),
            max_scan: 5000,
        })
        .await
        .unwrap()
        .into_inner();
    assert_eq!(full.count, 5000);
    assert_eq!(full.entries.len(), 5000);
    assert!(full.entries.iter().all(|e| e.df == 1));
    assert!(full.entries.windows(2).all(|w| w[0].term < w[1].term));

    let c = coordinator(vec![addr]);
    let error = refused(&c, request("body", "t", 0, 100)).await;
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(
        error.message().contains("matches 5000 dictionary terms")
            && error.message().contains("the scan bound is 100"),
        "{}",
        error.message()
    );
    let served = suggest(&c, request("body", "t", 0, 5000)).await;
    assert_eq!(served.dictionary_terms_with_prefix, 5000);
    assert_eq!(ranked(&served), want[..DEFAULT_SUGGEST_LIMIT]);
    assert!(served
        .suggestions
        .iter()
        .all(|s: &Suggestion| s.df == 1 && s.shards == 1));

    // Both stores count past the bound without materializing.
    let reader = Bm25Reader::open(&bm25_sidecar_path(&index_path)).unwrap();
    assert_eq!(reader.suggest_prefix("t", 100), Err(5000));
    assert_eq!(reader.suggest_prefix("t", 4999), Err(5000));
    let mut store = Bm25Store::new();
    store.add_document(0, text.clone(), doc);
    assert_eq!(store.suggest_prefix("t", 100), Err(5000));
    let entries: Vec<(String, u32)> = {
        let mut entries: Vec<(String, u32)> =
            want.iter().map(|(t, df)| (t.clone(), *df as u32)).collect();
        entries.sort_by(|a, b| a.0.as_bytes().cmp(b.0.as_bytes()));
        entries
    };
    assert_eq!(reader.suggest_prefix("t", 5000), Ok(entries.clone()));
    assert_eq!(store.suggest_prefix("t", 5000), Ok(entries));
    handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}
