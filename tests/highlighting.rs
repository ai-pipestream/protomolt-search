//! Server-side highlighting (`docs/highlighting.md`): sentence-bounded
//! snippets with merged occurrence spans, cut by the shard from text and
//! sentence spans it stored at ingest, with no analyzer on the query
//! path. Pins the happy path over the wire, the whitespace-only cuts,
//! UTF-16 offsets, multi-field behaviour, the refusal table, the
//! flush/reopen/WAL-replay round trip, distributed == monolithic, the
//! heap/spill dual-writer identity, the no-analysis meter, and the
//! bytes-per-document price of the kind-8 column.

mod common;

use std::path::PathBuf;

use common::mock::{start_mock_analysis_metered, start_mock_analysis_without_sentences};
use common::start_empty_node;
use pipestream_search::analyzer::{
    self, analyze_document_native, analyze_document_native_dual, body_spec, NATIVE_ANALYSIS_BACKEND,
};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::{bm25_sidecar_path, Bm25Shard, NodeConfig, NodeServiceImpl};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    search_query, selection_query, AddDocumentsRequest, AnalysisSpec, Bm25Hit, Bm25SearchRequest,
    Bm25SearchResponse, BooleanQuery, DocumentField, FlushRequest, HighlightMode, HighlightSpec,
    LexicalQuery, QueryField, QueryRequest, SearchQuery, SelectionQuery, SetCalibrationRequest,
    Snippet, SnippetCut,
};
use pipestream_search::postings::{AnalyzedDoc, Bm25Index, Bm25Reader, Bm25Store, SpillBuilder};
use pipestream_search::reshard;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Request};

/// Lines are sentences under the newline detector. Document 4 is one
/// sentence far wider than any snippet budget; document 3 matches
/// nothing.
const CORPUS: [&str; 6] = [
    "The court held that the claim fails\nA second sentence about the appeal\nThird line with nothing.",
    "Court after court has said so.\nThe appeal was denied by the court",
    "😀 café court\nnaïve 𝔘nicode court appeal",
    "no matches in this document at all",
    "w00 w01 w02 w03 w04 w05 w06 w07 w08 w09 w10 w11 w12 w13 w14 w15 w16 w17 w18 w19 court w20 w21 w22 w23 w24 w25 w26 w27 w28 w29 w30 w31 w32 w33 w34 w35 w36 w37 w38 w39 appeal w40 w41 w42 w43 w44 w45 w46 w47 w48 w49 w50 w51 w52 w53 w54 w55 w56 w57 w58 w59",
    "appeal\nappeal appeal court",
];

const SPLIT: usize = 3;

fn config(slot_offset: u64, sentences: bool) -> NodeConfig {
    NodeConfig {
        slot_offset,
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        sentence_fields: if sentences {
            vec!["body".to_string()]
        } else {
            Vec::new()
        },
        ..Default::default()
    }
}

async fn ingest(addr: &str, docs: &[&str]) -> Result<u64, tonic::Status> {
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
    client
        .add_documents(ReceiverStream::new(rx))
        .await
        .map(|r| r.into_inner().added)
}

fn coordinator(addrs: Vec<String>, analysis: &str) -> CoordinatorServiceImpl {
    CoordinatorServiceImpl::new(addrs).with_bm25(Some(analysis.to_string()), Default::default())
}

fn spec(mode: HighlightMode, max_snippets: u32, max_chars: u32) -> HighlightSpec {
    HighlightSpec {
        fields: Vec::new(),
        max_snippets,
        max_chars,
        mode: mode as i32,
    }
}

fn request(text: &str, highlight: Option<HighlightSpec>) -> Bm25SearchRequest {
    Bm25SearchRequest {
        text: text.to_string(),
        k: 10,
        analysis: Some(body_spec()),
        highlight,
        ..Default::default()
    }
}

async fn bm25(c: &CoordinatorServiceImpl, req: Bm25SearchRequest) -> Bm25SearchResponse {
    SearchService::bm25_search(c, Request::new(req))
        .await
        .unwrap()
        .into_inner()
}

async fn refused(c: &CoordinatorServiceImpl, req: Bm25SearchRequest) -> tonic::Status {
    SearchService::bm25_search(c, Request::new(req))
        .await
        .unwrap_err()
}

fn utf16_slice(text: &str, start: u32, end: u32) -> String {
    let units: Vec<u16> = text
        .encode_utf16()
        .skip(start as usize)
        .take((end - start) as usize)
        .collect();
    String::from_utf16(&units).unwrap()
}

/// UTF-16 span of the `n`th occurrence of `word` in `text`.
fn at(text: &str, word: &str, n: usize) -> (u32, u32) {
    let byte = text.match_indices(word).nth(n).expect("occurrence").0;
    let start = text[..byte].encode_utf16().count() as u32;
    (start, start + word.encode_utf16().count() as u32)
}

fn hit(resp: &Bm25SearchResponse, doc_id: u64) -> &Bm25Hit {
    resp.hits
        .iter()
        .find(|h| h.doc_id == doc_id)
        .unwrap_or_else(|| panic!("doc {doc_id} is a hit"))
}

/// The contract every snippet keeps, whatever cut it got: text is the
/// UTF-16 slice at its bounds, highlights are sorted, disjoint, inside
/// the bounds, and each is an occurrence the hit reported.
fn check_contract(text: &str, h: &Bm25Hit) {
    let occurrences: Vec<(u32, u32)> = h
        .terms
        .iter()
        .flat_map(|t| t.offsets.iter().map(|o| (o.start, o.end)))
        .collect();
    let mut prev_end = 0u32;
    for s in &h.snippets {
        assert_eq!(s.field, "body");
        assert!(
            s.start >= prev_end,
            "snippets in text order, disjoint: {:?}",
            h.snippets
        );
        prev_end = s.end;
        assert_eq!(s.text, utf16_slice(text, s.start, s.end), "{s:?}");
        let mut last = s.start;
        for (i, hl) in s.highlights.iter().enumerate() {
            assert!(s.start <= hl.start && hl.end <= s.end, "{s:?}");
            assert!(
                i == 0 || hl.start > last,
                "highlights sorted and disjoint: {s:?}"
            );
            last = hl.end;
            assert!(
                occurrences.contains(&(hl.start, hl.end)),
                "highlight {hl:?} is a reported occurrence of {:?}",
                h.terms
            );
        }
        assert!(
            !s.highlights.is_empty(),
            "a snippet always holds a highlight: {s:?}"
        );
    }
}

type NodeHandle = tokio::task::JoinHandle<Result<(), tonic::transport::Error>>;

async fn fleet(sentences: bool) -> (CoordinatorServiceImpl, Vec<NodeHandle>) {
    let (a, ha) = start_empty_node(config(0, sentences)).await;
    let (b, hb) = start_empty_node(config(SPLIT as u64, sentences)).await;
    assert_eq!(ingest(&a, &CORPUS[..SPLIT]).await.unwrap(), SPLIT as u64);
    assert_eq!(
        ingest(&b, &CORPUS[SPLIT..]).await.unwrap(),
        (CORPUS.len() - SPLIT) as u64
    );
    (
        coordinator(vec![a, b], NATIVE_ANALYSIS_BACKEND),
        vec![ha, hb],
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sentence_snippets_are_bounded_merged_and_in_text_order() {
    let (c, handles) = fleet(true).await;
    let resp = bm25(
        &c,
        request("court appeal", Some(spec(HighlightMode::Sentence, 0, 0))),
    )
    .await;
    assert_eq!(resp.hits.len(), 5, "doc 3 matches nothing");
    for h in &resp.hits {
        check_contract(CORPUS[h.doc_id as usize], h);
        assert!(!h.terms.is_empty(), "hits keep their occurrence spans");
        assert!(h.snippets.len() <= 3);
    }
    // Doc 0: two sentences hold a term, the third holds none and is
    // never a snippet.
    let d0 = hit(&resp, 0);
    assert_eq!(d0.snippets.len(), 2);
    assert_eq!(d0.snippets[0].text, "The court held that the claim fails");
    assert_eq!(d0.snippets[1].text, "A second sentence about the appeal");
    assert!(d0
        .snippets
        .iter()
        .all(|s| s.cut == SnippetCut::Sentence as i32));
    assert_eq!(
        d0.snippets[0]
            .highlights
            .iter()
            .map(|o| (o.start, o.end))
            .collect::<Vec<_>>(),
        vec![at(CORPUS[0], "court", 0)]
    );
    // Doc 1: the second sentence holds both terms; with one snippet
    // allowed it wins over the first sentence's two occurrences of one.
    let d1 = hit(&resp, 1);
    assert_eq!(d1.snippets.len(), 2);
    assert_eq!(d1.snippets[0].highlights.len(), 2, "court, court");
    let one = bm25(
        &c,
        request("court appeal", Some(spec(HighlightMode::Sentence, 1, 0))),
    )
    .await;
    let d1 = hit(&one, 1);
    assert_eq!(d1.snippets.len(), 1);
    assert_eq!(d1.snippets[0].text, "The appeal was denied by the court");
    assert_eq!(d1.snippets[0].highlights.len(), 2, "appeal, court");
    // Doc 5: adjacent occurrences separated only by a space stay two
    // highlights; nothing overlaps.
    let d5 = hit(&resp, 5);
    assert_eq!(d5.snippets.len(), 2);
    assert_eq!(d5.snippets[1].text, "appeal appeal court");
    assert_eq!(d5.snippets[1].highlights.len(), 3);
    // Without a spec no snippet is cut and the hits are the same.
    let plain = bm25(&c, request("court appeal", None)).await;
    assert!(plain.hits.iter().all(|h| h.snippets.is_empty()));
    assert_eq!(
        plain
            .hits
            .iter()
            .map(|h| (h.doc_id, h.score.to_bits()))
            .collect::<Vec<_>>(),
        resp.hits
            .iter()
            .map(|h| (h.doc_id, h.score.to_bits()))
            .collect::<Vec<_>>()
    );
    for h in handles {
        h.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wide_sentences_truncate_at_whitespace_and_window_mode_never_cuts_a_token() {
    let (c, handles) = fleet(true).await;
    let text = CORPUS[4];
    // Sentence mode over a 300+ unit sentence with a 64-unit budget.
    let resp = bm25(
        &c,
        request("court", Some(spec(HighlightMode::Sentence, 3, 64))),
    )
    .await;
    let d4 = hit(&resp, 4);
    check_contract(text, d4);
    assert_eq!(d4.snippets.len(), 1);
    let s = &d4.snippets[0];
    assert_eq!(s.cut, SnippetCut::TruncatedSentence as i32);
    assert!(s.end - s.start <= 64, "{}", s.end - s.start);
    assert!(s.text.contains("court"));
    let before = utf16_slice(text, 0, s.start);
    let after = utf16_slice(text, s.end, text.encode_utf16().count() as u32);
    assert!(
        before.is_empty() || before.ends_with(' '),
        "start on a token edge: {before:?}"
    );
    assert!(
        after.is_empty() || after.starts_with(' '),
        "end on a token edge: {after:?}"
    );
    assert!(!s.text.starts_with(' ') && !s.text.ends_with(' '));
    // A sentence within budget is returned whole and named as such.
    let d0 = hit(&resp, 0);
    assert_eq!(d0.snippets[0].cut, SnippetCut::Sentence as i32);
    // Window mode consults no sentences: two far-apart terms in doc 4
    // give two windows, each on token edges, each within budget.
    let win = bm25(
        &c,
        request("court appeal", Some(spec(HighlightMode::Window, 3, 40))),
    )
    .await;
    let d4 = hit(&win, 4);
    check_contract(text, d4);
    assert_eq!(d4.snippets.len(), 2, "{:?}", d4.snippets);
    for s in &d4.snippets {
        assert_eq!(s.cut, SnippetCut::Window as i32);
        assert!(s.end - s.start <= 40);
        let before = utf16_slice(text, 0, s.start);
        let after = utf16_slice(text, s.end, text.encode_utf16().count() as u32);
        assert!(before.is_empty() || before.ends_with(' '));
        assert!(after.is_empty() || after.starts_with(' '));
    }
    assert!(d4.snippets[0].text.contains("court") && d4.snippets[1].text.contains("appeal"));
    // In doc 0 the window crosses the line break the sentence cut
    // respects: window mode is a different, named cut.
    let d0 = hit(&win, 0);
    assert!(d0
        .snippets
        .iter()
        .all(|s| s.cut == SnippetCut::Window as i32));
    for h in handles {
        h.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn utf16_offsets_stay_in_the_original_text() {
    let (c, handles) = fleet(true).await;
    let text = CORPUS[2];
    let resp = bm25(
        &c,
        request("court appeal", Some(spec(HighlightMode::Sentence, 3, 0))),
    )
    .await;
    let d2 = hit(&resp, 2);
    check_contract(text, d2);
    assert_eq!(d2.snippets.len(), 2);
    // 😀 is two UTF-16 units and four UTF-8 bytes; é one unit, two
    // bytes; 𝔘 two units, four bytes. The offsets are units.
    assert_eq!(d2.snippets[0].text, "😀 café court");
    assert_eq!((d2.snippets[0].start, d2.snippets[0].end), (0, 13));
    assert_eq!(
        d2.snippets[0]
            .highlights
            .iter()
            .map(|o| (o.start, o.end))
            .collect::<Vec<_>>(),
        vec![(8, 13)]
    );
    assert_eq!(d2.snippets[1].text, "naïve 𝔘nicode court appeal");
    assert_eq!((d2.snippets[1].start, d2.snippets[1].end), (14, 41));
    // Snippet-relative positions are the subtraction the contract
    // promises: they index the snippet's own UTF-16 units.
    for s in &d2.snippets {
        for hl in &s.highlights {
            let rel = utf16_slice(&s.text, hl.start - s.start, hl.end - s.start);
            assert!(rel == "court" || rel == "appeal", "{rel:?}");
        }
    }
    for h in handles {
        h.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fields_without_sentence_spans_refuse_sentence_mode_and_serve_windows() {
    let (c, handles) = fleet(false).await;
    let error = refused(
        &c,
        request("court", Some(spec(HighlightMode::Sentence, 0, 0))),
    )
    .await;
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(
        error.message().contains("stores no sentence spans")
            && error.message().contains("--sentence-fields=body")
            && error.message().contains("HIGHLIGHT_MODE_WINDOW"),
        "{}",
        error.message()
    );
    // The default mode IS sentence mode, so an unqualified spec refuses too.
    let error = refused(&c, request("court", Some(HighlightSpec::default()))).await;
    assert_eq!(error.code(), Code::FailedPrecondition);
    let win = bm25(
        &c,
        request("court appeal", Some(spec(HighlightMode::Window, 3, 0))),
    )
    .await;
    assert_eq!(win.hits.len(), 5);
    for h in &win.hits {
        check_contract(CORPUS[h.doc_id as usize], h);
        assert!(!h.snippets.is_empty());
        assert!(h
            .snippets
            .iter()
            .all(|s| s.cut == SnippetCut::Window as i32));
    }
    for h in handles {
        h.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_refusal_table_names_each_reason() {
    let (c, handles) = fleet(true).await;
    let cases: [(HighlightSpec, Code, &str); 7] = [
        (
            spec(HighlightMode::Sentence, 0, 8),
            Code::InvalidArgument,
            "below the minimum 16",
        ),
        (
            spec(HighlightMode::Sentence, 0, 5000),
            Code::InvalidArgument,
            "exceeds the maximum 4096",
        ),
        (
            spec(HighlightMode::Sentence, 65, 0),
            Code::InvalidArgument,
            "exceeds the maximum 64",
        ),
        (
            HighlightSpec {
                fields: vec![String::new()],
                ..Default::default()
            },
            Code::InvalidArgument,
            "a field name is empty",
        ),
        (
            HighlightSpec {
                fields: vec!["body".into(), "body".into()],
                ..Default::default()
            },
            Code::InvalidArgument,
            "\"body\" repeats",
        ),
        (
            HighlightSpec {
                mode: 99,
                ..Default::default()
            },
            Code::InvalidArgument,
            "is not a HighlightMode",
        ),
        (
            HighlightSpec {
                fields: vec!["title".into()],
                ..Default::default()
            },
            Code::InvalidArgument,
            "only the body's text",
        ),
    ];
    for (spec, code, needle) in cases {
        let error = refused(&c, request("court", Some(spec.clone()))).await;
        assert_eq!(error.code(), code, "{spec:?}: {}", error.message());
        assert!(
            error.message().contains(needle),
            "{spec:?}: {}",
            error.message()
        );
    }
    // An empty fleet refuses a malformed spec the same way: the check
    // does not wait for a shard.
    let empty = coordinator(Vec::new(), NATIVE_ANALYSIS_BACKEND);
    let error = refused(
        &empty,
        request("court", Some(spec(HighlightMode::Sentence, 0, 8))),
    )
    .await;
    assert!(error.message().contains("below the minimum 16"));
    for h in handles {
        h.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multi_field_hits_snippet_the_stored_body_only() {
    let (addr, handle) = start_empty_node(NodeConfig {
        bm25_fields: vec!["body".to_string(), "title".to_string()],
        ..config(0, true)
    })
    .await;
    let docs = [
        ("The court held that the claim fails\nNothing else", "zebra"),
        ("Only zebras live here", "court report"),
    ];
    {
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        let (tx, rx) = mpsc::channel(8);
        for (body, title) in docs {
            tx.send(AddDocumentsRequest {
                text: body.to_string(),
                analysis: Some(body_spec()),
                fields: vec![DocumentField {
                    field: "title".to_string(),
                    text: title.to_string(),
                    analysis: Some(body_spec()),
                }],
                ..Default::default()
            })
            .await
            .unwrap();
        }
        drop(tx);
        assert_eq!(
            client
                .add_documents(ReceiverStream::new(rx))
                .await
                .unwrap()
                .into_inner()
                .added,
            2
        );
    }
    let c = coordinator(vec![addr], NATIVE_ANALYSIS_BACKEND);
    let leg = |field: &str| QueryField {
        field: field.to_string(),
        analysis: Some(body_spec()),
        weight: 1.0,
        k1: 0.0,
        b: 0.0,
        phrase: None,
        prefixes: Vec::new(),
    };
    let fused = |highlight: Option<HighlightSpec>| Bm25SearchRequest {
        text: "court".to_string(),
        k: 10,
        fields: vec![leg("body"), leg("title")],
        highlight,
        ..Default::default()
    };
    let resp = bm25(&c, fused(Some(spec(HighlightMode::Sentence, 0, 0)))).await;
    assert_eq!(resp.hits.len(), 2);
    // Doc 0 matched in the body: one sentence snippet, from the body.
    let d0 = hit(&resp, 0);
    check_contract(docs[0].0, d0);
    assert_eq!(d0.snippets.len(), 1);
    assert_eq!(d0.snippets[0].text, "The court held that the claim fails");
    // Doc 1 matched only in its title: a hit with occurrence spans in
    // the title, and no snippet, because the title's text is not stored.
    let d1 = hit(&resp, 1);
    assert!(d1.terms.iter().all(|t| t.field == "title"));
    assert!(d1.snippets.is_empty());
    // Asking for the title refuses by name rather than cutting nothing.
    let error = refused(
        &c,
        fused(Some(HighlightSpec {
            fields: vec!["title".into()],
            ..Default::default()
        })),
    )
    .await;
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(
        error.message().contains("only the body's text"),
        "{}",
        error.message()
    );
    handle.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_snippets_equal_the_monolith() {
    let (fleet_c, handles) = fleet(true).await;
    let (m, hm) = start_empty_node(config(0, true)).await;
    ingest(&m, &CORPUS).await.unwrap();
    let mono = coordinator(vec![m], NATIVE_ANALYSIS_BACKEND);
    for (text, spec) in [
        ("court appeal", spec(HighlightMode::Sentence, 0, 0)),
        ("court", spec(HighlightMode::Sentence, 1, 64)),
        ("court appeal", spec(HighlightMode::Window, 2, 40)),
    ] {
        let a = bm25(&fleet_c, request(text, Some(spec.clone()))).await;
        let b = bm25(&mono, request(text, Some(spec))).await;
        assert_eq!(a.hits.len(), b.hits.len());
        for (x, y) in a.hits.iter().zip(&b.hits) {
            assert_eq!(x.doc_id, y.doc_id, "{text}");
            assert_eq!(x.score.to_bits(), y.score.to_bits(), "{text}");
            assert_eq!(x.snippets, y.snippets, "{text} doc {}", x.doc_id);
        }
    }
    for h in handles {
        h.abort();
    }
    hm.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_query_adapter_serves_snippets_on_the_lexical_leaf_and_refuses_elsewhere() {
    let (c, handles) = fleet(true).await;
    let lexical = |id: &str, text: &str| SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: id.into(),
            query: Some(search_query::Query::Lexical(LexicalQuery {
                text: text.into(),
                analysis: Some(body_spec()),
                score_stages: Vec::new(),
                phrase: None,
                prefixes: Vec::new(),
            })),
        })),
    };
    let query = |selection: SelectionQuery, highlight: Option<HighlightSpec>| QueryRequest {
        k: 10,
        selection: Some(selection),
        highlight,
        ..Default::default()
    };
    let via_query = SearchService::query(
        &c,
        Request::new(query(
            lexical("lex", "court appeal"),
            Some(spec(HighlightMode::Sentence, 0, 0)),
        )),
    )
    .await
    .unwrap()
    .into_inner();
    let direct = bm25(
        &c,
        request("court appeal", Some(spec(HighlightMode::Sentence, 0, 0))),
    )
    .await;
    assert_eq!(via_query.hits.len(), direct.hits.len());
    for (q, d) in via_query.hits.iter().zip(&direct.hits) {
        assert_eq!(q.doc_id, d.doc_id);
        assert_eq!(q.snippets, d.snippets);
        assert!(!q.snippets.is_empty());
    }
    // A boolean selection has no single lexical leg to cut around.
    let boolean = SelectionQuery {
        node: Some(selection_query::Node::Boolean(BooleanQuery {
            must: vec![lexical("a", "court"), lexical("b", "appeal")],
            ..Default::default()
        })),
    };
    let error = SearchService::query(
        &c,
        Request::new(query(
            boolean.clone(),
            Some(spec(HighlightMode::Sentence, 0, 0)),
        )),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(
        error.message().contains("single lexical selection only"),
        "{}",
        error.message()
    );
    // Without the spec the same boolean shape serves as before.
    let served = SearchService::query(&c, Request::new(query(boolean, None)))
        .await
        .unwrap()
        .into_inner();
    assert!(served.hits.iter().all(|h| h.snippets.is_empty()));
    for h in handles {
        h.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn highlighting_adds_no_analysis_call_and_a_layerless_sidecar_refuses_ingest() {
    let (analysis, mock, calls) = start_mock_analysis_metered().await;
    let (addr, handle) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis.clone()),
        ..config(0, true)
    })
    .await;
    // The mock reports spans in bytes, so its corpus is ASCII: the
    // meter, not the units, is under test here
    let ascii: Vec<&str> = CORPUS.iter().copied().filter(|t| t.is_ascii()).collect();
    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    let (tx, rx) = mpsc::channel(8);
    for text in &ascii {
        tx.send(AddDocumentsRequest {
            text: text.to_string(),
            ..Default::default()
        })
        .await
        .unwrap();
    }
    drop(tx);
    assert_eq!(
        client
            .add_documents(ReceiverStream::new(rx))
            .await
            .unwrap()
            .into_inner()
            .added,
        ascii.len() as u64
    );
    let ingest_calls = calls.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(
        ingest_calls,
        ascii.len() as u64,
        "one analysis per document"
    );
    let c = coordinator(vec![addr], &analysis);
    let plain = |text: &str| Bm25SearchRequest {
        text: text.to_string(),
        k: 10,
        ..Default::default()
    };
    let before = calls.load(std::sync::atomic::Ordering::SeqCst);
    let without = bm25(&c, plain("court appeal")).await;
    let mid = calls.load(std::sync::atomic::Ordering::SeqCst);
    let with = bm25(
        &c,
        Bm25SearchRequest {
            highlight: Some(spec(HighlightMode::Sentence, 0, 0)),
            ..plain("court appeal")
        },
    )
    .await;
    let after = calls.load(std::sync::atomic::Ordering::SeqCst);
    assert_eq!(mid - before, 1, "the query text is analyzed once");
    assert_eq!(after - mid, 1, "highlighting adds no analysis call");
    assert!(without.hits.iter().all(|h| h.snippets.is_empty()));
    assert!(with.hits.iter().all(|h| !h.snippets.is_empty()));
    for h in &with.hits {
        check_contract(ascii[h.doc_id as usize], h);
    }
    handle.abort();
    mock.abort();

    // A sidecar that ignores the sentence request: the sentence field
    // refuses the document by name, never indexing it without spans.
    let (analysis, mock) = start_mock_analysis_without_sentences().await;
    let (addr, handle) = start_empty_node(NodeConfig {
        analysis_addr: Some(analysis),
        ..config(0, true)
    })
    .await;
    let error = ingest(&addr, &CORPUS[..1]).await.unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(
        error.message().contains("sentence field \"body\"")
            && error.message().contains("no sentence covering")
            || error.message().contains("outside every sentence span"),
        "{}",
        error.message()
    );
    handle.abort();
    mock.abort();
}

/// The offsets are the ORIGINAL text's, before any normalizer touched
/// it: a term whose surface form the char filters shorten (accent
/// folding, invisible stripping) still highlights the original
/// characters, and a chunk's lineage span composes with its snippet
/// offsets in one coordinate system, so a client can place a snippet
/// in the parent document — the same anchoring the sidecar's sentence
/// embeddings keep (docs/highlighting.md).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn offsets_predate_normalization_and_compose_with_lineage() {
    let (addr, handle) = start_empty_node(config(0, true)).await;
    let parent = "Preamble of the opinion\nRodríguez\u{200B} argued the appeal\nThe court agreed with Rodríguez\n😀 closing line";
    // The chunk is a UTF-16 slice of the parent, as a Java chunker cuts.
    let parent_units: Vec<u16> = parent.encode_utf16().collect();
    let chunk_start = at(parent, "Rodríguez", 0).0;
    let chunk_end = at(parent, "😀", 0).0 - 1;
    let chunk =
        String::from_utf16(&parent_units[chunk_start as usize..chunk_end as usize]).unwrap();
    {
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        let (tx, rx) = mpsc::channel(1);
        tx.send(AddDocumentsRequest {
            text: chunk.clone(),
            analysis: Some(body_spec()),
            lineage: Some(pipestream_search::pb::DocLineage {
                parent_id: 7,
                group_id: 1,
                span_start: chunk_start,
                span_end: chunk_end,
            }),
            ..Default::default()
        })
        .await
        .unwrap();
        drop(tx);
        client.add_documents(ReceiverStream::new(rx)).await.unwrap();
    }
    let c = coordinator(vec![addr.clone()], NATIVE_ANALYSIS_BACKEND);
    // The query is typed without the accent; the index folded both.
    let resp = bm25(
        &c,
        request("rodriguez", Some(spec(HighlightMode::Sentence, 3, 0))),
    )
    .await;
    let h = hit(&resp, 0);
    check_contract(&chunk, h);
    assert_eq!(h.snippets.len(), 2);
    // The first occurrence's span covers the original surface form,
    // zero-width space included: 9 letters plus the invisible.
    let first = at(&chunk, "Rodríguez\u{200B}", 0);
    assert_eq!(first.1 - first.0, 10);
    assert_eq!(
        (
            h.snippets[0].highlights[0].start,
            h.snippets[0].highlights[0].end
        ),
        first
    );
    assert!(h.snippets[0].text.starts_with("Rodríguez\u{200B} argued"));
    assert_eq!(h.snippets[0].sentence_index, Some(0));
    assert_eq!(h.snippets[1].sentence_index, Some(1));
    assert_eq!(h.snippets[1].text, "The court agreed with Rodríguez");
    // Composition: lineage span + snippet offset locates the snippet in
    // the parent, in the parent's UTF-16 units.
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    let stored = client
        .get_documents(pipestream_search::pb::GetDocumentsRequest { doc_ids: vec![0] })
        .await
        .unwrap()
        .into_inner();
    let lineage = stored.documents[0].lineage.expect("lineage stored");
    assert_eq!(
        (lineage.span_start, lineage.span_end),
        (chunk_start, chunk_end)
    );
    for s in &h.snippets {
        assert_eq!(
            utf16_slice(
                parent,
                lineage.span_start + s.start,
                lineage.span_start + s.end
            ),
            s.text
        );
        for hl in &s.highlights {
            let in_parent = utf16_slice(
                parent,
                lineage.span_start + hl.start,
                lineage.span_start + hl.end,
            );
            assert!(in_parent.starts_with("Rodríguez"), "{in_parent:?}");
        }
    }
    handle.abort();
}

fn tempdir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("highlight-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[allow(clippy::type_complexity)]
fn native_replay_analyzer() -> impl FnMut(
    &[(
        &str,
        Option<&AnalysisSpec>,
        pipestream_search::analyzer::SessionLayers,
    )],
) -> Result<Vec<AnalyzedDoc>, String> {
    move |docs| {
        docs.iter()
            .map(|(text, spec, layers)| {
                if layers.dual_cased {
                    analyze_document_native_dual(text, *spec)
                } else {
                    analyze_document_native(text, *spec)
                }
                .map_err(|e| e.to_string())
            })
            .collect()
    }
}

fn snippets_of(resp: &Bm25SearchResponse) -> Vec<(u64, Vec<Snippet>)> {
    resp.hits
        .iter()
        .map(|h| (h.doc_id, h.snippets.clone()))
        .collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn snippets_survive_flush_reopen_and_wal_replay() {
    let dir = tempdir("durable");
    let index_path = dir.join("shard.tv");
    let config = NodeConfig {
        index_path: Some(index_path.clone()),
        layout: pipestream_search::node::Layout::SingleImage,
        wal: true,
        wal_buckets: 8,
        ..config(0, true)
    };
    let (addr, handle) = start_empty_node(config.clone()).await;
    {
        let sample = common::unit_vectors(256, common::DIM, 0x5EED_CA11);
        let (shift, scale) = common::fit_calibration(common::DIM, common::BIT_WIDTH, &sample);
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        client
            .set_calibration(SetCalibrationRequest {
                dim: common::DIM as u32,
                bit_width: common::BIT_WIDTH as u32,
                shift,
                scale,
            })
            .await
            .unwrap();
    }
    ingest(&addr, &CORPUS).await.unwrap();
    let c = coordinator(vec![addr.clone()], NATIVE_ANALYSIS_BACKEND);
    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    assert!(
        client
            .flush(FlushRequest {})
            .await
            .unwrap()
            .into_inner()
            .written
    );
    let probes = [
        ("court appeal", spec(HighlightMode::Sentence, 0, 0)),
        ("court", spec(HighlightMode::Sentence, 1, 64)),
        ("court appeal", spec(HighlightMode::Window, 2, 40)),
    ];
    let mut baseline = Vec::new();
    for (text, spec) in &probes {
        let resp = bm25(&c, request(text, Some(spec.clone()))).await;
        for h in &resp.hits {
            check_contract(CORPUS[h.doc_id as usize], h);
        }
        baseline.push(snippets_of(&resp));
    }
    handle.abort();

    // The file itself: a kind-8 section, readable by the mmap reader,
    // whose per-document tables are the analyzer's.
    let bm25_path = bm25_sidecar_path(&index_path);
    let reader = Bm25Reader::open(&bm25_path).unwrap();
    assert!(reader.field_has_sentences(0));
    assert!(reader
        .integrity_section_names()
        .iter()
        .any(|n| n == "column:sentences:body:vals"));
    for (i, text) in CORPUS.iter().enumerate() {
        let want: Vec<(u32, u32)> = analyze_document_native(text, Some(&body_spec()))
            .unwrap()
            .fields[0]
            .sentences
            .clone()
            .unwrap();
        assert_eq!(
            reader.field_doc_sentences(0, i as u32),
            Some(want),
            "{text:?}"
        );
    }
    // Reload into heap: the same tables.
    let loaded = Bm25Store::load(&bm25_path).unwrap();
    assert!(loaded.field_has_sentences(0));
    for i in 0..CORPUS.len() as u32 {
        assert_eq!(
            loaded.field_doc_sentences(0, i).map(<[(u32, u32)]>::to_vec),
            reader.field_doc_sentences(0, i)
        );
    }

    // Reopen the file under a fresh node: bitwise the same snippets.
    let shard = Bm25Shard::open(&bm25_path).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = format!("http://{}", listener.local_addr().unwrap());
    let service = NodeServiceImpl::new(None, config.clone()).with_bm25(Some(shard));
    let handle2 = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(service.into_server(pipestream_search::MAX_MESSAGE_BYTES))
            .serve_with_incoming(pipestream_search::harness::nodelay_incoming(listener)),
    );
    let reopened = coordinator(vec![addr2], NATIVE_ANALYSIS_BACKEND);
    for ((text, spec), want) in probes.iter().zip(&baseline) {
        let resp = bm25(&reopened, request(text, Some(spec.clone()))).await;
        assert_eq!(&snippets_of(&resp), want, "{text:?} after reopen");
    }
    handle2.abort();

    // WAL replay: split 1 -> 2 through the native replay analyzer. The
    // children carry sentence spans from the record alone.
    let out_dir = dir.join("split");
    let output = reshard::split(
        &reshard::resolve_gen(&pipestream_search::wal::wal_dir(&index_path)).unwrap(),
        2,
        &out_dir,
        0,
        25_000_000,
        false,
        None,
        &mut native_replay_analyzer(),
    )
    .unwrap();
    let mut seen = 0usize;
    for child in &output.children {
        let path = child.bm25_path.as_ref().expect("children hold documents");
        let child_reader = Bm25Reader::open(path).unwrap();
        assert!(
            child_reader.field_has_sentences(0),
            "child keeps body sentences"
        );
        for local in 0..child.num_documents as u32 {
            let text = child_reader.text(local).expect("stored text");
            let parent_id = CORPUS
                .iter()
                .position(|body| *body == text)
                .expect("child text is a corpus body") as u32;
            assert_eq!(
                child_reader.field_doc_sentences(0, local),
                reader.field_doc_sentences(0, parent_id),
                "child sentences of {text:?}"
            );
            seen += 1;
        }
    }
    assert_eq!(seen, CORPUS.len());
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_old_file_without_the_section_serves_and_refuses_sentence_mode_by_name() {
    let dir = tempdir("old");
    // A pre-sentence file: the heap store without the declaration.
    let mut store = Bm25Store::with_fields(&["body"]);
    for (i, text) in CORPUS.iter().enumerate() {
        let mut doc = analyze_document_native(text, Some(&body_spec())).unwrap();
        doc.fields[0].sentences = None;
        store.add_document(i as u32, text.to_string(), doc);
    }
    // Under the name the node reloads a resident shard from.
    let path = bm25_sidecar_path(&dir.join("old.tv"));
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    assert!(!reader.field_has_sentences(0), "no silent upgrade on open");
    assert_eq!(reader.field_doc_sentences(0, 0), None);
    let shard = Bm25Shard::open(&path).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://{}", listener.local_addr().unwrap());
    let service = NodeServiceImpl::new(
        None,
        NodeConfig {
            index_path: Some(dir.join("old.tv")),
            ..config(0, true)
        },
    )
    .with_bm25(Some(shard));
    let handle = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(service.into_server(pipestream_search::MAX_MESSAGE_BYTES))
            .serve_with_incoming(pipestream_search::harness::nodelay_incoming(listener)),
    );
    let c = coordinator(vec![addr.clone()], NATIVE_ANALYSIS_BACKEND);
    let plain = bm25(&c, request("court appeal", None)).await;
    assert_eq!(plain.hits.len(), 5, "old queries serve");
    let error = refused(
        &c,
        request("court", Some(spec(HighlightMode::Sentence, 0, 0))),
    )
    .await;
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(
        error.message().contains("stores no sentence spans"),
        "{}",
        error.message()
    );
    let win = bm25(
        &c,
        request("court appeal", Some(spec(HighlightMode::Window, 3, 0))),
    )
    .await;
    assert!(win.hits.iter().all(|h| !h.snippets.is_empty()));
    // Ingest into that file under --sentence-fields=body refuses: the
    // storage declares none, and half a column would lie.
    let error = ingest(&addr, &["another court"]).await.unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(
        error.message().contains("predates sentence spans"),
        "{}",
        error.message()
    );
    handle.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// Synthetic corpus: `docs` documents of `lines` sentences each.
fn synthetic(docs: u32, lines: usize) -> Vec<String> {
    (0..docs)
        .map(|d| {
            (0..lines)
                .map(|l| format!("doc{d} line{l} court appeal token{}", (d as usize + l) % 7))
                .collect::<Vec<_>>()
                .join("\n")
        })
        .collect()
}

fn section(reader: &Bm25Reader, name: &str) -> u64 {
    reader
        .integrity_sections()
        .into_iter()
        .find(|(n, _)| n == name)
        .map(|(_, len)| len)
        .unwrap_or_else(|| {
            panic!(
                "section {name} present in {:?}",
                reader.integrity_section_names()
            )
        })
}

#[test]
fn sentence_spans_are_priced_exactly_and_both_writers_agree() {
    let dir = tempdir("price");
    let (docs, lines) = (50u32, 3usize);
    let corpus = synthetic(docs, lines);
    let mut heap = Bm25Store::with_fields(&["body"]).with_sentences(&["body"]);
    let mut spill = SpillBuilder::create_with_fields(&dir.join("build"), &["body"])
        .unwrap()
        .with_sentence_fields(&["body"]);
    // A gap slot (id 3 skipped) exercises the base table's sparse entry.
    let mut id = 0u32;
    for text in &corpus {
        if id == 3 {
            id += 1;
        }
        let doc = analyze_document_native(text, Some(&body_spec())).unwrap();
        assert_eq!(doc.fields[0].sentences.as_ref().unwrap().len(), lines);
        heap.add_document(id, text.clone(), doc.clone());
        spill
            .add_document_with_lineage(id, text.clone(), doc, None)
            .unwrap();
        id += 1;
    }
    let heap_path = dir.join("heap.bm25");
    let spill_path = dir.join("spill.bm25");
    heap.save(&heap_path).unwrap();
    spill.finish(&spill_path).unwrap();
    assert_eq!(
        std::fs::read(&heap_path).unwrap(),
        std::fs::read(&spill_path).unwrap(),
        "heap and spill writers produce the same bytes"
    );
    let reader = Bm25Reader::open(&spill_path).unwrap();
    let slots = u64::from(docs) + 1;
    let total = u64::from(docs) * lines as u64;
    // The price: 4 B per slot plus 8 B per sentence, plus the count.
    assert_eq!(
        section(&reader, "column:sentences:body:vals"),
        4 + 4 * (slots + 1) + 8 * total
    );
    let per_doc = section(&reader, "column:sentences:body:vals") as f64 / f64::from(docs);
    assert!(per_doc < 4.0 * (slots as f64 / f64::from(docs)) + 8.0 * lines as f64 + 1.0);
    // The gap slot reads as an empty table; every other slot as its own.
    assert_eq!(reader.field_doc_sentences(0, 3), Some(Vec::new()));
    assert_eq!(heap.field_doc_sentences(0, 3), Some(&[][..]));
    for slot in [0u32, 4, 50] {
        let want = analyze_document_native(
            &corpus[if slot < 3 { slot } else { slot - 1 } as usize],
            Some(&body_spec()),
        )
        .unwrap()
        .fields[0]
            .sentences
            .clone()
            .unwrap();
        assert_eq!(reader.field_doc_sentences(0, slot), Some(want.clone()));
        assert_eq!(heap.field_doc_sentences(0, slot), Some(want.as_slice()));
    }
    // A shard without the declaration carries no section, at no cost.
    let mut plain = Bm25Store::with_fields(&["body"]);
    for (i, text) in corpus.iter().enumerate() {
        let doc = analyze_document_native(text, Some(&body_spec())).unwrap();
        plain.add_document(i as u32, text.clone(), doc);
    }
    let plain_path = dir.join("plain.bm25");
    plain.save(&plain_path).unwrap();
    let plain_reader = Bm25Reader::open(&plain_path).unwrap();
    assert!(!plain_reader
        .integrity_section_names()
        .iter()
        .any(|n| n.starts_with("column:sentences:")));
    let _ = analyzer::body_spec();
    let _ = std::fs::remove_dir_all(&dir);
}
