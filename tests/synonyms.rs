//! Query-time synonyms and did-you-mean (`docs/synonyms.md`): a rule
//! adds terms to the query that score as ordinary terms and are reported
//! per matched term; the coordinator's table and the request's rules
//! combine; one-way rules never expand back; a sorted lexical leaf and a
//! phrase refuse them by name. Did-you-mean ranks dictionary terms
//! within the edit bound of an analyzed term by distance then df, equal
//! to a brute-force scan of the analyzed corpus, on one shard and two.

mod common;

use std::collections::{BTreeMap, BTreeSet};

use common::start_empty_node;
use pipestream_search::analyzer::{analyze_document_native, body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    search_query, selection_query, AddDocumentsRequest, Bm25SearchRequest, LexicalQuery,
    QueryField, QueryRequest, QuerySort, SearchQuery, SelectionQuery, SynonymRule, TermSuggestMode,
    TermSuggestRequest,
};
use pipestream_search::postings::AnalyzedDoc;
use pipestream_search::synonyms::{edit_distance, SynonymTable};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Request};

const CORPUS: [&str; 8] = [
    "the automobile stalled on the bridge",
    "a car and a truck",
    "court of appeals",
    "the courthouse courier",
    "motor vehicle registration",
    "cars carts and courts",
    "an automobile show",
    "vector search engines",
];
const SPLIT: usize = 4;

fn config(slot_offset: u64) -> pipestream_search::node::NodeConfig {
    pipestream_search::node::NodeConfig {
        slot_offset,
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        ..Default::default()
    }
}

async fn ingest(addr: &str, docs: &[&str]) {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(8);
    let n = docs.len();
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
    assert_eq!(added as usize, n);
}

fn coordinator(addrs: Vec<String>) -> CoordinatorServiceImpl {
    CoordinatorServiceImpl::new(addrs).with_bm25(
        Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        Default::default(),
    )
}

fn rule(terms: &[&str], to: &[&str]) -> SynonymRule {
    SynonymRule {
        terms: terms.iter().map(|s| s.to_string()).collect(),
        to: to.iter().map(|s| s.to_string()).collect(),
    }
}

fn bm25(text: &str) -> Bm25SearchRequest {
    Bm25SearchRequest {
        text: text.to_string(),
        k: 10,
        analysis: Some(body_spec()),
        ..Default::default()
    }
}

async fn search<S: SearchService>(
    s: &S,
    req: Bm25SearchRequest,
) -> pipestream_search::pb::Bm25SearchResponse {
    SearchService::bm25_search(s, Request::new(req))
        .await
        .unwrap()
        .into_inner()
}

fn ids(resp: &pipestream_search::pb::Bm25SearchResponse) -> BTreeSet<u64> {
    resp.hits.iter().map(|h| h.doc_id).collect()
}

fn scores(resp: &pipestream_search::pb::Bm25SearchResponse) -> Vec<(u64, u32)> {
    resp.hits
        .iter()
        .map(|h| (h.doc_id, h.score.to_bits()))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn synonym_rules_add_terms_that_score_as_ordinary_terms_and_are_reported() {
    let (a, _ha) = start_empty_node(config(0)).await;
    let (b, _hb) = start_empty_node(config(SPLIT as u64)).await;
    ingest(&a, &CORPUS[..SPLIT]).await;
    ingest(&b, &CORPUS[SPLIT..]).await;
    let fleet = coordinator(vec![a.clone(), b.clone()]);

    // Without a rule, "car" finds the car documents alone.
    let plain = search(&fleet, bm25("car")).await;
    assert_eq!(ids(&plain), BTreeSet::from([1, 5]));
    assert!(plain.synonym_expansions.is_empty());

    // A symmetric request rule: "car" also finds the automobile
    // documents, and the expansion is reported as the analyzed terms.
    let expanded = search(
        &fleet,
        Bm25SearchRequest {
            synonyms: vec![rule(&["car", "automobile", "motor vehicle"], &[])],
            ..bm25("car")
        },
    )
    .await;
    assert_eq!(ids(&expanded), BTreeSet::from([0, 1, 4, 5, 6]));
    assert_eq!(expanded.synonym_expansions.len(), 1);
    let expansion = &expanded.synonym_expansions[0];
    assert_eq!(
        (expansion.field.as_str(), expansion.term.as_str()),
        ("body", "car")
    );
    assert_eq!(
        expansion.terms,
        vec![
            "automobil".to_string(),
            "motor".to_string(),
            "vehicl".to_string()
        ],
        "a phrase entry contributes each of its analyzed terms"
    );
    // The same query written with every term spelled out scores bitwise
    // the same: an expansion is nothing but more query terms.
    let spelled = search(&fleet, bm25("car automobile motor vehicle")).await;
    assert_eq!(scores(&expanded), scores(&spelled));

    // A one-way rule expands "nyc" to the city's terms, never back.
    let one_way = search(
        &fleet,
        Bm25SearchRequest {
            synonyms: vec![rule(&["vehicle"], &["car"])],
            ..bm25("vehicle")
        },
    )
    .await;
    assert_eq!(ids(&one_way), BTreeSet::from([1, 4, 5]));
    let back = search(
        &fleet,
        Bm25SearchRequest {
            synonyms: vec![rule(&["vehicle"], &["car"])],
            ..bm25("car")
        },
    )
    .await;
    assert_eq!(ids(&back), BTreeSet::from([1, 5]));
    assert!(back.synonym_expansions.is_empty());

    // The coordinator's table applies to every query; a request can turn
    // it off, or add to it.
    let tabled = coordinator(vec![a.clone(), b.clone()])
        .with_synonyms(SynonymTable::from_rules(vec![rule(&["car", "automobile"], &[])]).unwrap());
    assert_eq!(
        ids(&search(&tabled, bm25("car")).await),
        BTreeSet::from([0, 1, 5, 6])
    );
    assert_eq!(
        ids(&search(
            &tabled,
            Bm25SearchRequest {
                synonyms_off: true,
                ..bm25("car")
            }
        )
        .await),
        BTreeSet::from([1, 5])
    );
    let both = search(
        &tabled,
        Bm25SearchRequest {
            synonyms: vec![rule(&["car"], &["truck"])],
            ..bm25("car")
        },
    )
    .await;
    assert_eq!(ids(&both), BTreeSet::from([0, 1, 5, 6]));
    assert_eq!(
        both.synonym_expansions[0].terms,
        vec!["automobil".to_string(), "truck".to_string()]
    );

    // The fused route takes rules per field, with the same result.
    let fused = search(
        &fleet,
        Bm25SearchRequest {
            fields: vec![QueryField {
                field: "body".into(),
                analysis: Some(body_spec()),
                weight: 1.0,
                k1: 0.0,
                b: 0.0,
                phrase: None,
                prefixes: Vec::new(),
                synonyms: vec![rule(&["car", "automobile"], &[])],
                synonyms_off: false,
            }],
            analysis: None,
            ..bm25("car")
        },
    )
    .await;
    assert_eq!(ids(&fused), BTreeSet::from([0, 1, 5, 6]));
    assert_eq!(fused.synonym_expansions.len(), 1);

    // The public query route carries them on the lexical leaf and
    // reports them.
    let leaf = |synonyms: Vec<SynonymRule>| SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: "lex".into(),
            query: Some(search_query::Query::Lexical(LexicalQuery {
                text: "car".into(),
                analysis: Some(body_spec()),
                synonyms,
                ..Default::default()
            })),
        })),
    };
    let query = SearchService::query(
        &fleet,
        Request::new(QueryRequest {
            k: 10,
            selection: Some(leaf(vec![rule(&["car", "automobile"], &[])])),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(
        query.hits.iter().map(|h| h.doc_id).collect::<BTreeSet<_>>(),
        BTreeSet::from([0, 1, 5, 6])
    );
    assert_eq!(query.synonym_expansions.len(), 1);
    // A sorted lexical leaf computes no relevance and refuses the rules.
    let err = SearchService::query(
        &fleet,
        Request::new(QueryRequest {
            k: 10,
            selection: Some(leaf(vec![rule(&["car", "automobile"], &[])])),
            sort: vec![QuerySort {
                column: "parent_id".into(),
                descending: false,
            }],
            ..Default::default()
        }),
    )
    .await
    .unwrap_err();
    assert!(err.message().contains("synonym rules"), "{}", err.message());

    // Malformed rules refuse by name before any shard is asked.
    let err = SearchService::bm25_search(
        &fleet,
        Request::new(Bm25SearchRequest {
            synonyms: vec![rule(&["car"], &[])],
            ..bm25("car")
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
    assert!(err.message().contains("at least two"), "{}", err.message());
    assert!(SynonymTable::from_rules(vec![rule(&[" "], &["x"])]).is_err());
}

fn analyzed(docs: &[&str]) -> Vec<AnalyzedDoc> {
    docs.iter()
        .map(|text| analyze_document_native(text, Some(&body_spec())).unwrap())
        .collect()
}

/// The brute-force oracle: every dictionary term sharing the first
/// character, within the edit bound, ranked by (distance, df desc, term).
fn oracle(docs: &[AnalyzedDoc], term: &str, max_edits: usize) -> Vec<(String, u64, u32)> {
    let mut df: BTreeMap<String, u64> = BTreeMap::new();
    for doc in docs {
        let terms: BTreeSet<&str> = doc.fields[0]
            .terms
            .iter()
            .map(|(t, _, _)| t.as_str())
            .collect();
        for t in terms {
            *df.entry(t.to_string()).or_insert(0) += 1;
        }
    }
    let first = term.chars().next().unwrap();
    let mut ranked: Vec<(String, u64, u32)> = df
        .into_iter()
        .filter(|(t, _)| t.starts_with(first) && t != term)
        .filter_map(|(t, df)| {
            let d = edit_distance(term, &t, max_edits);
            (d <= max_edits).then_some((t, df, d as u32))
        })
        .collect();
    ranked.sort_by(|a, b| {
        a.2.cmp(&b.2)
            .then_with(|| b.1.cmp(&a.1))
            .then_with(|| a.0.as_bytes().cmp(b.0.as_bytes()))
    });
    ranked
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn did_you_mean_equals_the_brute_force_scan_on_one_shard_and_two() {
    let (a, _ha) = start_empty_node(config(0)).await;
    let (b, _hb) = start_empty_node(config(SPLIT as u64)).await;
    let (m, _hm) = start_empty_node(config(0)).await;
    ingest(&a, &CORPUS[..SPLIT]).await;
    ingest(&b, &CORPUS[SPLIT..]).await;
    ingest(&m, &CORPUS).await;
    let fleet = coordinator(vec![a, b]);
    let mono = coordinator(vec![m]);
    let corpus = analyzed(&CORPUS);
    let request = |text: &str, max_edits: u32, mode: TermSuggestMode| TermSuggestRequest {
        collection: String::new(),
        field: "body".into(),
        text: text.into(),
        analysis: Some(body_spec()),
        max_edits,
        prefix_length: 0,
        limit: 10,
        max_scan: 0,
        mode: mode as i32,
    };

    for (text, max_edits) in [
        ("cuort apeals", 1),
        ("cuort apeals", 2),
        ("automobil bridge", 1),
        ("vehical registrtion", 2),
        ("zzz", 2),
    ] {
        let fleet_resp = SearchService::term_suggest(
            &fleet,
            Request::new(request(text, max_edits, TermSuggestMode::Missing)),
        )
        .await
        .unwrap()
        .into_inner();
        let mono_resp = SearchService::term_suggest(
            &mono,
            Request::new(request(text, max_edits, TermSuggestMode::Missing)),
        )
        .await
        .unwrap()
        .into_inner();
        assert_eq!(fleet_resp.terms.len(), mono_resp.terms.len());
        for (f, m) in fleet_resp.terms.iter().zip(&mono_resp.terms) {
            assert_eq!(f.term, m.term);
            assert_eq!(f.df, m.df, "df of {:?}", f.term);
            let got: Vec<(String, u64, u32)> = f
                .candidates
                .iter()
                .map(|c| (c.term.clone(), c.df, c.distance))
                .collect();
            let want = if f.df > 0 {
                Vec::new()
            } else {
                oracle(&corpus, &f.term, max_edits as usize)
            };
            assert_eq!(got, want, "{text:?} term {:?} edits {max_edits}", f.term);
            let mono_got: Vec<(String, u64, u32)> = m
                .candidates
                .iter()
                .map(|c| (c.term.clone(), c.df, c.distance))
                .collect();
            assert_eq!(got, mono_got, "two shards equal one");
        }
    }
    // A present term gets no candidates under MISSING and its neighbours
    // under ALWAYS; the term itself never appears.
    let missing = SearchService::term_suggest(
        &fleet,
        Request::new(request("car", 1, TermSuggestMode::Missing)),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(missing.terms[0].df, 2);
    assert!(missing.terms[0].candidates.is_empty());
    let always = SearchService::term_suggest(
        &fleet,
        Request::new(request("car", 1, TermSuggestMode::Always)),
    )
    .await
    .unwrap()
    .into_inner();
    let got: Vec<(String, u64, u32)> = always.terms[0]
        .candidates
        .iter()
        .map(|c| (c.term.clone(), c.df, c.distance))
        .collect();
    assert_eq!(got, oracle(&corpus, "car", 1));
    assert!(got.iter().all(|(t, _, _)| t != "car"));
    assert!(!got.is_empty(), "cart is one edit away");

    // Refusals by name: the edit bound, the field, the missing spec.
    for (req, needle) in [
        (request("car", 3, TermSuggestMode::Missing), "max_edits"),
        (
            TermSuggestRequest {
                field: String::new(),
                ..request("car", 1, TermSuggestMode::Missing)
            },
            "need a field",
        ),
        (
            TermSuggestRequest {
                analysis: None,
                ..request("car", 1, TermSuggestMode::Missing)
            },
            "analysis spec",
        ),
        (
            TermSuggestRequest {
                field: "nope".into(),
                ..request("car", 1, TermSuggestMode::Missing)
            },
            "no shard indexes",
        ),
    ] {
        let err = SearchService::term_suggest(&fleet, Request::new(req))
            .await
            .unwrap_err();
        assert_eq!(err.code(), Code::InvalidArgument, "{needle}");
        assert!(err.message().contains(needle), "{}", err.message());
    }
}
