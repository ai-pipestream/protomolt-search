//! Phrase and proximity search (`docs/phrase-proximity.md`): the bigram
//! column and the token-position payload, the ordered-window gate, the
//! coordinator's route choice, and every refusal that keeps the feature
//! from approximating adjacency from character offsets.

mod common;

use std::path::{Path, PathBuf};

use common::{mock::start_mock_analysis, start_empty_node};
use pipestream_search::analyzer::{self, body_spec, NATIVE_ANALYSIS_BACKEND};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::{bm25_sidecar_path, Bm25Shard, NodeConfig, NodeServiceImpl};
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    search_query, selection_query, AddDocumentsRequest, AnalysisSpec, Bm25Hit, Bm25SearchRequest,
    Bm25SearchResponse, CompositeSearchStrategy, DocumentField, FlushRequest, LexicalQuery,
    PhraseMatch, QueryField, QueryRequest, ScoreStage, SearchQuery, SelectionQuery,
    SelectionScoreStrategy, SetCalibrationRequest, SingleScore,
};
use pipestream_search::postings::{AnalyzedDoc, Bm25Index, Bm25Reader, Bm25Store};
use pipestream_search::proximity;
use pipestream_search::reshard;
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Request};

/// `(body, title)`. Under `body_spec` (whitespace tokens, STRIP_INVISIBLE,
/// Porter): doc 2's soft hyphen is a token that normalizes to nothing, so
/// `new` and `york` sit at ordinals 0 and 2 while their spans are one
/// space apart — the pair character offsets cannot tell from doc 0's.
const CORPUS: [(&str, &str); 7] = [
    ("New York City hot dog", "new york"),
    ("York New order", "york"),
    ("new \u{00AD} york pizza", ""),
    ("new jersey and new york", "new york jersey"),
    ("the new great york", ""),
    ("new york new york", ""),
    ("hot dog stand", "hot dog"),
];

/// Shard A takes docs 0..4, shard B docs 4..7, so global ids equal the
/// monolithic ids.
const SPLIT: usize = 4;

fn proximity_config(slot_offset: u64, analysis: &str) -> NodeConfig {
    NodeConfig {
        slot_offset,
        analysis_addr: Some(analysis.to_string()),
        bm25_fields: vec![
            "body".to_string(),
            "body.bigrams".to_string(),
            "title".to_string(),
        ],
        position_fields: vec!["body".to_string()],
        bigram_fields: vec!["body".to_string()],
        ..Default::default()
    }
}

fn doc(body: &str, title: &str, analysis: Option<AnalysisSpec>) -> AddDocumentsRequest {
    AddDocumentsRequest {
        text: body.to_string(),
        analysis: analysis.clone(),
        fields: if title.is_empty() {
            Vec::new()
        } else {
            vec![DocumentField {
                field: "title".to_string(),
                text: title.to_string(),
                analysis,
            }]
        },
        ..Default::default()
    }
}

async fn ingest(addr: &str, docs: &[(&str, &str)], analysis: Option<AnalysisSpec>) {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(8);
    for (body, title) in docs {
        tx.send(doc(body, title, analysis.clone())).await.unwrap();
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

/// A two-shard fleet plus a monolithic node over the same corpus.
struct Fleet {
    coordinator: CoordinatorServiceImpl,
    monolith: CoordinatorServiceImpl,
    handles: Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
}

async fn native_fleet() -> Fleet {
    let (a, ha) = start_empty_node(proximity_config(0, NATIVE_ANALYSIS_BACKEND)).await;
    let (b, hb) = start_empty_node(proximity_config(SPLIT as u64, NATIVE_ANALYSIS_BACKEND)).await;
    let (m, hm) = start_empty_node(proximity_config(0, NATIVE_ANALYSIS_BACKEND)).await;
    ingest(&a, &CORPUS[..SPLIT], Some(body_spec())).await;
    ingest(&b, &CORPUS[SPLIT..], Some(body_spec())).await;
    ingest(&m, &CORPUS, Some(body_spec())).await;
    let with_bm25 = |addrs: Vec<String>| {
        CoordinatorServiceImpl::new(addrs).with_bm25(
            Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            Default::default(),
        )
    };
    Fleet {
        coordinator: with_bm25(vec![a, b]),
        monolith: with_bm25(vec![m]),
        handles: vec![ha, hb, hm],
    }
}

fn phrase_request(text: &str, slop: u32) -> Bm25SearchRequest {
    Bm25SearchRequest {
        text: text.to_string(),
        k: 10,
        analysis: Some(body_spec()),
        phrase: Some(PhraseMatch { slop }),
        ..Default::default()
    }
}

async fn bm25(coordinator: &CoordinatorServiceImpl, req: Bm25SearchRequest) -> Bm25SearchResponse {
    SearchService::bm25_search(coordinator, Request::new(req))
        .await
        .unwrap()
        .into_inner()
}

fn ids(resp: &Bm25SearchResponse) -> Vec<u64> {
    let mut ids: Vec<u64> = resp.hits.iter().map(|h| h.doc_id).collect();
    ids.sort_unstable();
    ids
}

fn signature(hits: &[Bm25Hit]) -> Vec<(u64, u32)> {
    hits.iter().map(|h| (h.doc_id, h.score.to_bits())).collect()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn two_term_exact_phrase_rides_the_bigram_column() {
    let fleet = native_fleet().await;
    let resp = bm25(&fleet.coordinator, phrase_request("new york", 0)).await;
    assert_eq!(ids(&resp), vec![0, 3, 5], "adjacent pairs only");
    assert_eq!(resp.phrase_routing.len(), 1);
    let routing = &resp.phrase_routing[0];
    assert_eq!(routing.field, "body");
    assert_eq!(routing.served_field, "body.bigrams");
    assert!(routing.bigram_column);
    assert_eq!(routing.slop, 0);
    // The repeated pair scores highest (tf 2 in the bigram column).
    assert_eq!(resp.hits[0].doc_id, 5);
    // Occurrences name the column and span both tokens.
    let doc0 = resp.hits.iter().find(|h| h.doc_id == 0).unwrap();
    assert_eq!(doc0.terms.len(), 1);
    assert_eq!(doc0.terms[0].field, "body.bigrams");
    assert_eq!(doc0.terms[0].term, "new york");
    assert_eq!(doc0.terms[0].offsets.len(), 1);
    assert_eq!(
        (doc0.terms[0].offsets[0].start, doc0.terms[0].offsets[0].end),
        (0, 8)
    );
    // The reversed pair is a different bigram.
    let reversed = bm25(&fleet.coordinator, phrase_request("york new", 0)).await;
    assert_eq!(ids(&reversed), vec![1, 5]);
    for handle in fleet.handles {
        handle.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slop_and_longer_phrases_ride_token_positions() {
    let fleet = native_fleet().await;
    // slop 1 admits one intervening token position: the dropped soft
    // hyphen (doc 2) and "great" (doc 4). Their spans could not tell
    // doc 2 from doc 0; their ordinals do.
    let slop1 = bm25(&fleet.coordinator, phrase_request("new york", 1)).await;
    assert_eq!(ids(&slop1), vec![0, 2, 3, 4, 5]);
    assert!(!slop1.phrase_routing[0].bigram_column);
    assert_eq!(slop1.phrase_routing[0].served_field, "body");
    assert_eq!(slop1.phrase_routing[0].slop, 1);
    let slop0 = bm25(&fleet.coordinator, phrase_request("new york", 0)).await;
    assert_eq!(ids(&slop0), vec![0, 3, 5]);
    // Three terms need positions even at slop 0; the bigram column
    // answers only pairs.
    let three = bm25(&fleet.coordinator, phrase_request("new york city", 0)).await;
    assert_eq!(ids(&three), vec![0]);
    assert!(!three.phrase_routing[0].bigram_column);
    // Positions score the constituent terms, and the hit reports each
    // term's occurrences in the body.
    let hit = &three.hits[0];
    let mut fields: Vec<&str> = hit.terms.iter().map(|t| t.field.as_str()).collect();
    fields.dedup();
    assert_eq!(fields, ["body"]);
    assert_eq!(hit.terms.len(), 3);
    // A large slop still demands order: "york new" at slop 5 does not
    // admit doc 0 ("New York City hot dog" has no "new" after "york").
    let ordered = bm25(&fleet.coordinator, phrase_request("york new", 5)).await;
    assert_eq!(
        ids(&ordered),
        vec![1, 5],
        "doc 3: its york is the last token, so no later new exists"
    );
    // A one-term "phrase" is the ordinary term query and reports no route.
    let single = bm25(&fleet.coordinator, phrase_request("york", 0)).await;
    assert_eq!(ids(&single), vec![0, 1, 2, 3, 4, 5]);
    assert!(single.phrase_routing.is_empty());
    for handle in fleet.handles {
        handle.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn distributed_equals_monolithic_on_both_routes() {
    let fleet = native_fleet().await;
    for (text, slop) in [
        ("new york", 0),
        ("new york", 1),
        ("new york city", 0),
        ("york new", 5),
        ("hot dog", 0),
        ("new york hot dog", 0),
    ] {
        let fleet_resp = bm25(&fleet.coordinator, phrase_request(text, slop)).await;
        let mono_resp = bm25(&fleet.monolith, phrase_request(text, slop)).await;
        assert_eq!(
            signature(&fleet_resp.hits),
            signature(&mono_resp.hits),
            "{text:?} slop {slop}: ids and scores must be bitwise the monolith's"
        );
        assert_eq!(fleet_resp.phrase_routing, mono_resp.phrase_routing);
        assert_eq!(fleet_resp.kth_best.to_bits(), mono_resp.kth_best.to_bits());
    }
    // Facets count the phrase-matched set, not the term-matched one.
    let mut req = phrase_request("new york", 1);
    req.facet_fields = Vec::new();
    for handle in fleet.handles {
        handle.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn fields_without_a_payload_refuse_by_name() {
    let fleet = native_fleet().await;
    // The title field declared neither payload.
    let error = SearchService::bm25_search(
        &fleet.coordinator,
        Request::new(Bm25SearchRequest {
            text: "new york".into(),
            k: 10,
            fields: vec![QueryField {
                field: "title".into(),
                analysis: Some(body_spec()),
                weight: 1.0,
                k1: 0.0,
                b: 0.0,
                phrase: Some(PhraseMatch { slop: 0 }),
                prefixes: Vec::new(),
                synonyms: Vec::new(),
                synonyms_off: false,
            }],
            ..Default::default()
        }),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(error.message().contains("\"title\""), "{}", error.message());
    assert!(
        error.message().contains("title.bigrams"),
        "{}",
        error.message()
    );
    assert!(
        error.message().contains("--position-fields"),
        "{}",
        error.message()
    );
    // An ordinary query on that field still serves.
    let plain = bm25(
        &fleet.coordinator,
        Bm25SearchRequest {
            text: "new york".into(),
            k: 10,
            fields: vec![QueryField {
                field: "title".into(),
                analysis: Some(body_spec()),
                weight: 1.0,
                k1: 0.0,
                b: 0.0,
                phrase: None,
                prefixes: Vec::new(),
                synonyms: Vec::new(),
                synonyms_off: false,
            }],
            ..Default::default()
        },
    )
    .await;
    assert_eq!(ids(&plain), vec![0, 1, 3]);

    // A phrase with score stages / projections / stats is refused until
    // certified, never silently run without one of them.
    let mut staged = phrase_request("new york", 0);
    staged.score_stages = vec![ScoreStage::default()];
    let error = SearchService::bm25_search(&fleet.coordinator, Request::new(staged))
        .await
        .unwrap_err();
    assert!(
        error.message().contains("phrase constraint"),
        "{}",
        error.message()
    );
    // The flat constraint does not silently apply to a fused request.
    let mut fused = phrase_request("new york", 0);
    fused.analysis = None;
    fused.fields = vec![QueryField {
        field: "body".into(),
        analysis: Some(body_spec()),
        weight: 1.0,
        k1: 0.0,
        b: 0.0,
        phrase: None,
        prefixes: Vec::new(),
        synonyms: Vec::new(),
        synonyms_off: false,
    }];
    let error = SearchService::bm25_search(&fleet.coordinator, Request::new(fused))
        .await
        .unwrap_err();
    assert!(
        error.message().contains("QueryField"),
        "{}",
        error.message()
    );
    for handle in fleet.handles {
        handle.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_bigram_only_fleet_serves_pairs_and_refuses_the_rest() {
    let config = |offset: u64| NodeConfig {
        position_fields: Vec::new(),
        ..proximity_config(offset, NATIVE_ANALYSIS_BACKEND)
    };
    let (a, ha) = start_empty_node(config(0)).await;
    let (b, hb) = start_empty_node(config(SPLIT as u64)).await;
    ingest(&a, &CORPUS[..SPLIT], Some(body_spec())).await;
    ingest(&b, &CORPUS[SPLIT..], Some(body_spec())).await;
    let coordinator = CoordinatorServiceImpl::new(vec![a, b]).with_bm25(
        Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        Default::default(),
    );
    let pair = bm25(&coordinator, phrase_request("new york", 0)).await;
    assert_eq!(ids(&pair), vec![0, 3, 5]);
    assert!(pair.phrase_routing[0].bigram_column);
    let error = SearchService::bm25_search(
        &coordinator,
        Request::new(phrase_request("new york city", 0)),
    )
    .await
    .unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(
        error.message().contains("answers only two-term phrases"),
        "{}",
        error.message()
    );
    let error =
        SearchService::bm25_search(&coordinator, Request::new(phrase_request("new york", 1)))
            .await
            .unwrap_err();
    assert!(
        error.message().contains("slop needs ordinals"),
        "{}",
        error.message()
    );
    ha.abort();
    hb.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mixed_fleet_refuses_rather_than_matching_half_the_corpus() {
    let (a, ha) = start_empty_node(proximity_config(0, NATIVE_ANALYSIS_BACKEND)).await;
    // Shard B predates the payloads entirely.
    let (b, hb) = start_empty_node(NodeConfig {
        slot_offset: SPLIT as u64,
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        bm25_fields: vec!["body".to_string(), "title".to_string()],
        ..Default::default()
    })
    .await;
    ingest(&a, &CORPUS[..SPLIT], Some(body_spec())).await;
    ingest(&b, &CORPUS[SPLIT..], Some(body_spec())).await;
    let coordinator = CoordinatorServiceImpl::new(vec![a, b]).with_bm25(
        Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        Default::default(),
    );
    for slop in [0, 1] {
        let error = SearchService::bm25_search(
            &coordinator,
            Request::new(phrase_request("new york", slop)),
        )
        .await
        .unwrap_err();
        assert_eq!(error.code(), Code::InvalidArgument, "slop {slop}");
        assert!(
            error.message().contains("every shard"),
            "{}",
            error.message()
        );
    }
    // The plain query over the same fleet serves.
    let plain = bm25(
        &coordinator,
        Bm25SearchRequest {
            text: "new york".into(),
            k: 10,
            analysis: Some(body_spec()),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(ids(&plain), vec![0, 1, 2, 3, 4, 5]);
    ha.abort();
    hb.abort();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_query_adapter_serves_the_single_lexical_leaf_and_refuses_the_rest() {
    let fleet = native_fleet().await;
    let lexical = |slop: u32| LexicalQuery {
        text: "new york".into(),
        analysis: Some(body_spec()),
        score_stages: Vec::new(),
        phrase: Some(PhraseMatch { slop }),
        prefixes: Vec::new(),
        synonyms: Vec::new(),
        synonyms_off: false,
    };
    let leaf = |slop: u32| SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: "lex".into(),
            query: Some(search_query::Query::Lexical(lexical(slop))),
        })),
    };
    for slop in [0, 1] {
        let response = SearchService::query(
            &fleet.coordinator,
            Request::new(QueryRequest {
                k: 10,
                selection: Some(leaf(slop)),
                ..Default::default()
            }),
        )
        .await
        .unwrap()
        .into_inner();
        let direct = bm25(&fleet.coordinator, phrase_request("new york", slop)).await;
        let got: Vec<(u64, u32)> = response
            .hits
            .iter()
            .map(|h| (h.doc_id, h.score.to_bits()))
            .collect();
        assert_eq!(got, signature(&direct.hits), "slop {slop}");
    }
    // Composite and boolean shapes refuse the constraint by name.
    let composite = SelectionQuery {
        node: Some(selection_query::Node::Composite(CompositeSearchStrategy {
            operator: 0,
            clauses: vec![leaf(0)],
            scoring: Some(SelectionScoreStrategy {
                strategy: Some(
                    pipestream_search::pb::selection_score_strategy::Strategy::Single(
                        SingleScore {},
                    ),
                ),
            }),
        })),
    };
    let error = SearchService::query(
        &fleet.coordinator,
        Request::new(QueryRequest {
            k: 10,
            selection: Some(composite),
            ..Default::default()
        }),
    )
    .await
    .unwrap_err();
    assert!(error.message().contains("phrase"), "{}", error.message());
    for handle in fleet.handles {
        handle.abort();
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_sidecar_token_layer_yields_the_same_positions_as_native() {
    // Through the mock sidecar (its own term identity, no stripping), the
    // soft hyphen is a real token at ordinal 1: still not adjacent. The
    // positions come from the response's token layer — the one Analyze
    // ingest already makes — never from the spans.
    let (mock, mock_handle) = start_mock_analysis().await;
    let (a, ha) = start_empty_node(proximity_config(0, &mock)).await;
    let (b, hb) = start_empty_node(proximity_config(SPLIT as u64, &mock)).await;
    ingest(&a, &CORPUS[..SPLIT], None).await;
    ingest(&b, &CORPUS[SPLIT..], None).await;
    let coordinator =
        CoordinatorServiceImpl::new(vec![a, b]).with_bm25(Some(mock.clone()), Default::default());
    let request = |slop: u32| Bm25SearchRequest {
        text: "new york".into(),
        k: 10,
        analysis: None,
        phrase: Some(PhraseMatch { slop }),
        ..Default::default()
    };
    let exact = bm25(&coordinator, request(0)).await;
    assert_eq!(ids(&exact), vec![0, 3, 5]);
    assert!(exact.phrase_routing[0].bigram_column);
    let slop1 = bm25(&coordinator, request(1)).await;
    assert_eq!(ids(&slop1), vec![0, 2, 3, 4, 5]);
    ha.abort();
    hb.abort();
    mock_handle.abort();
}

fn tempdir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("phrase-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// The reshard replay analyzer over the native provider, the same shape
/// `examples/reshard.rs` builds.
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
                    analyzer::analyze_document_native_dual(text, *spec)
                } else {
                    analyzer::analyze_document_native(text, *spec)
                }
                .map_err(|e| e.to_string())
            })
            .collect()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn positions_and_bigrams_survive_flush_reopen_and_wal_replay() {
    let dir = tempdir("durable");
    let index_path = dir.join("shard.tv");
    let config = NodeConfig {
        index_path: Some(index_path.clone()),
        layout: pipestream_search::node::Layout::SingleImage,
        wal: true,
        wal_buckets: 8,
        ..proximity_config(0, NATIVE_ANALYSIS_BACKEND)
    };
    let (addr, handle) = start_empty_node(config.clone()).await;
    // Resharding replays a WAL only under locked provider state (child
    // scores must be byte-comparable), so seed the calibration first,
    // as every resharded shard is.
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
    ingest(&addr, &CORPUS, Some(body_spec())).await;
    let coordinator = CoordinatorServiceImpl::new(vec![addr.clone()]).with_bm25(
        Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        Default::default(),
    );
    let probes = [
        ("new york", 0u32),
        ("new york", 1),
        ("new york city", 0),
        ("york new", 5),
    ];
    // A persisted shard bulk-builds through the spill builder and is
    // not searchable until flushed; the flushed (mmap) shard is the
    // baseline every later reopen and replay must match bitwise.
    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    assert!(
        client
            .flush(FlushRequest {})
            .await
            .unwrap()
            .into_inner()
            .written
    );
    let mut live = Vec::new();
    for (text, slop) in probes {
        live.push(signature(
            &bm25(&coordinator, phrase_request(text, slop)).await.hits,
        ));
    }
    assert_eq!(live[0].len(), 3);
    assert_eq!(live[1].len(), 5);
    handle.abort();

    // The file itself: kind-7 positions on the body, the bigram column
    // as an ordinary field, nothing on the title.
    let bm25_path = bm25_sidecar_path(&index_path);
    let reader = Bm25Reader::open(&bm25_path).unwrap();
    assert!(reader.field_has_positions(0));
    let bigrams = reader
        .field_index("body.bigrams")
        .expect("derived column persisted");
    assert!(!reader.field_has_positions(bigrams));
    assert!(!reader.field_has_positions(reader.field_index("title").unwrap()));
    assert_eq!(reader.field(bigrams).df("new york"), 3);
    assert_eq!(reader.field(0).posting_positions("york", 2), Some(vec![2]));
    assert_eq!(
        reader.field(0).posting_positions("new", 5),
        Some(vec![0, 2])
    );
    drop(reader);

    // Restart from disk: no in-memory state carried over.
    let shard = Bm25Shard::open(&bm25_path).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = format!("http://{}", listener.local_addr().unwrap());
    let service = NodeServiceImpl::new(None, config.clone()).with_bm25(Some(shard));
    let handle2 = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(service.into_server(pipestream_search::MAX_MESSAGE_BYTES))
            .serve_with_incoming(pipestream_search::harness::nodelay_incoming(listener)),
    );
    let reopened = CoordinatorServiceImpl::new(vec![addr2]).with_bm25(
        Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        Default::default(),
    );
    for (i, (text, slop)) in probes.iter().enumerate() {
        let after = bm25(&reopened, phrase_request(text, *slop)).await;
        assert_eq!(
            signature(&after.hits),
            live[i],
            "{text:?} slop {slop} after reopen"
        );
    }
    handle2.abort();

    // WAL replay: split 1 -> 2 through the native replay analyzer. The
    // children carry positions and the derived column from the record
    // alone — no node configuration is handed to the replay.
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
    let parent = Bm25Reader::open(&bm25_path).unwrap();
    let mut seen_docs = 0usize;
    let mut bigram_df = 0u32;
    for child in &output.children {
        let path = child.bm25_path.as_ref().expect("children hold documents");
        let reader = Bm25Reader::open(path).unwrap();
        assert!(reader.field_has_positions(0), "child keeps body positions");
        let bigrams = reader
            .field_index("body.bigrams")
            .expect("child derives the column");
        bigram_df += reader.field(bigrams).df("new york");
        for local in 0..child.num_documents as u32 {
            let text = reader.text(local).expect("stored text");
            let parent_id = CORPUS
                .iter()
                .position(|(body, _)| *body == text)
                .expect("child text is a corpus body") as u32;
            for term in ["new", "york", "citi"] {
                assert_eq!(
                    reader.field(0).posting_positions(term, local),
                    parent.field(0).posting_positions(term, parent_id),
                    "child positions of {term:?} in {text:?}"
                );
            }
            assert_eq!(
                reader.field(bigrams).posting_offsets("new york", local),
                parent.field(bigrams).posting_offsets("new york", parent_id),
                "child bigram spans in {text:?}"
            );
            seen_docs += 1;
        }
    }
    assert_eq!(seen_docs, CORPUS.len());
    assert_eq!(bigram_df, 3);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn ingest_refuses_a_positional_field_the_active_file_never_declared() {
    // A shard flushed WITHOUT positions, then reopened by a node
    // configured with them: old queries serve, positional ingest refuses,
    // and the phrase refuses by name instead of matching new documents
    // only.
    let dir = tempdir("predates");
    let index_path = dir.join("shard.tv");
    let old = NodeConfig {
        index_path: Some(index_path.clone()),
        layout: pipestream_search::node::Layout::SingleImage,
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        bm25_fields: vec!["body".to_string()],
        ..Default::default()
    };
    let (addr, handle) = start_empty_node(old).await;
    // Bodies only: this node's table has no title field.
    let bodies: Vec<(&str, &str)> = CORPUS[..3].iter().map(|(body, _)| (*body, "")).collect();
    ingest(&addr, &bodies, Some(body_spec())).await;
    let mut client = NodeServiceClient::connect(addr).await.unwrap();
    assert!(
        client
            .flush(FlushRequest {})
            .await
            .unwrap()
            .into_inner()
            .written
    );
    handle.abort();

    let bm25_path = bm25_sidecar_path(&index_path);
    let shard = Bm25Shard::open(&bm25_path).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr2 = format!("http://{}", listener.local_addr().unwrap());
    let service = NodeServiceImpl::new(
        None,
        NodeConfig {
            index_path: Some(index_path.clone()),
            layout: pipestream_search::node::Layout::SingleImage,
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            bm25_fields: vec!["body".to_string()],
            position_fields: vec!["body".to_string()],
            ..Default::default()
        },
    )
    .with_bm25(Some(shard));
    let handle2 = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(service.into_server(pipestream_search::MAX_MESSAGE_BYTES))
            .serve_with_incoming(pipestream_search::harness::nodelay_incoming(listener)),
    );
    let coordinator = CoordinatorServiceImpl::new(vec![addr2.clone()]).with_bm25(
        Some(NATIVE_ANALYSIS_BACKEND.to_string()),
        Default::default(),
    );
    let plain = bm25(
        &coordinator,
        Bm25SearchRequest {
            text: "new york".into(),
            k: 10,
            analysis: Some(body_spec()),
            ..Default::default()
        },
    )
    .await;
    assert_eq!(ids(&plain), vec![0, 1, 2], "old queries serve");
    let error =
        SearchService::bm25_search(&coordinator, Request::new(phrase_request("new york", 0)))
            .await
            .unwrap_err();
    assert_eq!(error.code(), Code::InvalidArgument);
    assert!(
        error.message().contains("--position-fields"),
        "{}",
        error.message()
    );

    let mut client = NodeServiceClient::connect(addr2).await.unwrap();
    let (tx, rx) = mpsc::channel(1);
    tx.send(doc("new york again", "", Some(body_spec())))
        .await
        .unwrap();
    drop(tx);
    let error = client
        .add_documents(ReceiverStream::new(rx))
        .await
        .unwrap_err();
    assert_eq!(error.code(), Code::FailedPrecondition);
    assert!(
        error.message().contains("predates token positions"),
        "{}",
        error.message()
    );
    handle2.abort();
    let _ = std::fs::remove_dir_all(&dir);
}

/// A deterministic Zipf-ish corpus: 40 tokens per document over a 400-term
/// vocabulary, so bigrams repeat enough to have real df.
fn synthetic_corpus(docs: u32) -> Vec<String> {
    let mut state = 0x9E37_79B9_7F4A_7C15u64;
    let mut next = move || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    (0..docs)
        .map(|_| {
            (0..40)
                .map(|_| {
                    let r = next();
                    // Skewed: half the draws land in the first 20 terms.
                    let term = if r % 2 == 0 {
                        (r >> 1) % 20
                    } else {
                        20 + (r >> 1) % 380
                    };
                    format!("t{term}")
                })
                .collect::<Vec<_>>()
                .join(" ")
        })
        .collect()
}

fn analyzed(text: &str) -> AnalyzedDoc {
    analyzer::analyze_document_native(text, Some(&body_spec())).unwrap()
}

fn section(reader: &Bm25Reader, name: &str) -> u64 {
    reader
        .integrity_sections()
        .into_iter()
        .find(|(n, _)| n == name)
        .map(|(_, len)| len)
        .unwrap_or_else(|| panic!("section {name} missing"))
}

/// The cost gate (`docs/phrase-proximity.md`): positions cost exactly
/// their formula, a bigram column costs exactly what an ordinary field
/// of the same postings costs, and both prices are reported per document.
#[test]
fn positions_and_bigrams_are_priced_exactly() {
    let dir = tempdir("cost");
    let corpus = synthetic_corpus(2000);
    let mut plain = Bm25Store::with_fields(&["body"]);
    let mut positional = Bm25Store::with_fields(&["body"]).with_positions(&["body"]);
    let mut bigrams = Bm25Store::with_fields(&["body", "body.bigrams"]);
    // The bigram column alone, as an ordinary single-field store: the
    // oracle for "a bigram is a term with no extra payload".
    let mut bigrams_alone = Bm25Store::with_fields(&["body"]);
    let mut occurrences = 0u64;
    let mut bigram_postings = 0u64;
    for (i, text) in corpus.iter().enumerate() {
        let doc = analyzed(text);
        let body = doc.fields[0].clone();
        occurrences += body
            .terms
            .iter()
            .map(|(_, _, o)| o.len() as u64)
            .sum::<u64>();
        let column = proximity::derive_bigrams(&body).unwrap();
        bigram_postings += column.terms.len() as u64;
        plain.add_document(
            i as u32,
            text.clone(),
            AnalyzedDoc::body(body.terms.clone(), body.length),
        );
        positional.add_document(i as u32, text.clone(), doc.clone());
        let mut both = AnalyzedDoc::body(body.terms.clone(), body.length);
        both.fields.push(column.clone());
        bigrams.add_document(i as u32, text.clone(), both);
        bigrams_alone.add_document(
            i as u32,
            text.clone(),
            AnalyzedDoc::body(column.terms, column.length),
        );
    }
    let paths: Vec<PathBuf> = ["plain", "positional", "bigrams", "bigrams-alone"]
        .iter()
        .map(|n| dir.join(format!("{n}.bm25")))
        .collect();
    plain.save(&paths[0]).unwrap();
    positional.save(&paths[1]).unwrap();
    bigrams.save(&paths[2]).unwrap();
    bigrams_alone.save(&paths[3]).unwrap();
    let open = |p: &Path| Bm25Reader::open(p).unwrap();
    let (plain_r, positional_r, bigrams_r, alone_r) = (
        open(&paths[0]),
        open(&paths[1]),
        open(&paths[2]),
        open(&paths[3]),
    );

    // Positions: exactly 4 B per occurrence plus the per-term base
    // table, and nothing else in the file moves.
    let n_terms = u64::from(plain_r.term_count());
    let positions_bytes = section(&positional_r, "column:positions:body:vals");
    assert_eq!(positions_bytes, 4 + 4 * (n_terms + 1) + 4 * occurrences);
    for name in [
        "field:body:doc_lengths",
        "field:body:postings",
        "field:body:directory",
        "texts",
    ] {
        assert_eq!(
            section(&plain_r, name),
            section(&positional_r, name),
            "{name} unchanged"
        );
    }
    // Bigrams: the derived column's sections are exactly an ordinary
    // field's over the same postings — no hidden payload rides a bigram.
    for (kind, alone_name) in [
        ("doc_lengths", "field:body:doc_lengths"),
        ("postings", "field:body:postings"),
        ("directory", "field:body:directory"),
    ] {
        assert_eq!(
            section(&bigrams_r, &format!("field:body.bigrams:{kind}")),
            section(&alone_r, alone_name),
            "bigram {kind}"
        );
    }
    let docs = corpus.len() as u64;
    let body_bytes =
        section(&plain_r, "field:body:postings") + section(&plain_r, "field:body:directory");
    let bigram_bytes = section(&bigrams_r, "field:body.bigrams:postings")
        + section(&bigrams_r, "field:body.bigrams:directory");
    eprintln!(
        "cost per document over {docs} synthetic docs: body postings+directory {:.1} B, \
         positions {:.1} B ({occurrences} occurrences), bigram column {:.1} B \
         ({bigram_postings} postings)",
        body_bytes as f64 / docs as f64,
        positions_bytes as f64 / docs as f64,
        bigram_bytes as f64 / docs as f64,
    );
    assert!(bigram_bytes > 0 && positions_bytes > 0);
    let _ = std::fs::remove_dir_all(&dir);
}
