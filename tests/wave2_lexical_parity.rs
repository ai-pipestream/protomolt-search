//! Lexical wire-score ties agree across local scoring, RPCs, and certified streams.

mod common;

use pipestream_search::analyzer::{analysis_fingerprint, body_spec};
use pipestream_search::bm25::{self, Bm25Params, CorpusStats, FieldQuery, PruneStats, TopK};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::{NodeConfig, NodeServiceImpl};
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{Bm25SearchRequest, Bm25SearchResponse, QueryField};
use pipestream_search::postings::{AnalyzedDoc, Bm25Reader, Bm25Store};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use tonic::Request;

const PARAMS: Bm25Params = Bm25Params { k1: 1.2, b: 1e-8 };

struct Scratch(PathBuf);

impl Scratch {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = PathBuf::from(env!("CARGO_TARGET_TMPDIR")).join(format!(
            "wave2-lexical-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        std::fs::create_dir_all(&path).unwrap();
        Self(path)
    }
}

impl Drop for Scratch {
    fn drop(&mut self) {
        std::fs::remove_dir_all(&self.0).expect("remove lexical fixture");
    }
}

fn store(texts: &[&str]) -> Bm25Store {
    let mut store = Bm25Store::with_fields(&["body"]);
    store
        .set_analysis_fingerprint(0, analysis_fingerprint(Some(&body_spec())))
        .unwrap();
    for (id, text) in texts.iter().enumerate() {
        let terms = text
            .split_whitespace()
            .enumerate()
            .map(|(word, term)| (term.into(), 1, vec![(word as u32 * 2, word as u32 * 2 + 1)]))
            .collect::<Vec<_>>();
        let length = terms.len() as u32;
        store.add_document(id as u32, (*text).into(), AnalyzedDoc::body(terms, length));
    }
    store
}

struct Server {
    address: String,
    handle: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

impl Drop for Server {
    fn drop(&mut self) {
        self.handle.abort();
    }
}

async fn serve(path: &Path, slot: u64, texts: &[&str], block_max: bool) -> Server {
    store(texts).save(path).unwrap();
    serve_image(path, slot, block_max, Vec::new()).await
}

/// Serve the store saved at `path` as the one segment of a catalog beside it.
async fn serve_image(path: &Path, slot: u64, block_max: bool, numerics: Vec<String>) -> Server {
    let index_path = path.with_extension("tv");
    common::catalog::seal_bm25_file(&index_path, path);
    let node = NodeServiceImpl::open(
        NodeConfig {
            index_path: Some(index_path),
            slot_offset: slot,
            analysis_addr: Some("native".into()),
            bm25_fields: vec!["body".into()],
            block_max,
            numeric_fields: numerics,
            ..Default::default()
        },
        None,
    )
    .unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let handle = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(node.into_server(pipestream_search::MAX_MESSAGE_BYTES))
            .serve_with_incoming(pipestream_search::harness::nodelay_incoming(listener)),
    );
    Server { address, handle }
}

fn request(k: u32, fused: bool) -> Bm25SearchRequest {
    if fused {
        Bm25SearchRequest {
            k,
            text: "x".into(),
            fields: vec![QueryField {
                field: "body".into(),
                analysis: Some(body_spec()),
                weight: 1.0,
                ..Default::default()
            }],
            ..Default::default()
        }
    } else {
        Bm25SearchRequest {
            k,
            text: "x".into(),
            pre_analyzed_terms: vec!["x".into()],
            analysis: Some(body_spec()),
            ..Default::default()
        }
    }
}

async fn search(servers: &[Server], k: u32, fused: bool, streaming: bool) -> Bm25SearchResponse {
    search_at(
        servers.iter().map(|s| s.address.clone()).collect(),
        k,
        fused,
        streaming,
    )
    .await
}

async fn search_at(
    addresses: Vec<String>,
    k: u32,
    fused: bool,
    streaming: bool,
) -> Bm25SearchResponse {
    let coordinator = CoordinatorServiceImpl::new(addresses)
        .with_bm25(Some("native".into()), PARAMS)
        .with_bm25_stream(streaming);
    SearchService::bm25_search(&coordinator, Request::new(request(k, fused)))
        .await
        .expect("lexical query must receive a valid completion")
        .into_inner()
}

fn signature(response: &Bm25SearchResponse) -> Vec<(u64, u32)> {
    response
        .hits
        .iter()
        .map(|hit| (hit.doc_id, hit.score.to_bits()))
        .collect()
}

#[tokio::test]
async fn rounded_local_scores_match_the_certified_wire_winner() {
    let dir = Scratch::new();
    for block_max in [true, false] {
        let server = serve(
            &dir.0.join(format!("rounding-{block_max}.bm25")),
            0,
            &["x y", "x"],
            block_max,
        )
        .await;
        for fused in [true, false] {
            let servers = std::slice::from_ref(&server);
            let streamed = search(servers, 1, fused, true).await;
            let unary = search(servers, 1, fused, false).await;
            assert_eq!(signature(&streamed), signature(&unary));
            assert_eq!(
                unary.hits.iter().map(|hit| hit.doc_id).collect::<Vec<_>>(),
                [0]
            );
        }
    }
}

#[tokio::test]
async fn unary_and_streamed_lexical_ties_use_shard_then_document_order() {
    let dir = Scratch::new();
    let servers = vec![
        serve(&dir.0.join("first.bm25"), 9, &["x"], true).await,
        serve(&dir.0.join("second.bm25"), 2, &["x"], true).await,
    ];
    for fused in [true, false] {
        for k in [1, 2] {
            let unary = search(&servers, k, fused, false).await;
            let streamed = search(&servers, k, fused, true).await;
            assert_eq!(signature(&streamed), signature(&unary));
            assert_eq!(
                unary.hits.iter().map(|hit| hit.doc_id).collect::<Vec<_>>(),
                [9, 2][..k as usize]
            );
        }
    }
}

#[test]
fn rounded_score_ties_survive_pruning_and_live_floors() {
    let dir = Scratch::new();
    let mut texts = vec!["x"; 8192];
    texts[0] = "x y";
    let store = store(&texts);
    let path = dir.0.join("blocks.bm25");
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    let terms = vec!["x".to_string()];
    let stats = CorpusStats {
        doc_count: 8192,
        total_doc_length: 8193,
        dfs: vec![8192],
    };
    let longer = bm25::idf(8192, 8192) * bm25::tf_norm(PARAMS, 1, 2, stats.avgdl());
    let shorter = bm25::idf(8192, 8192) * bm25::tf_norm(PARAMS, 1, 1, stats.avgdl());
    assert!(longer < shorter);
    assert_eq!((longer as f32).to_bits(), (shorter as f32).to_bits());
    let floor = bm25::floor_seed(shorter as f32);
    let mut live = |_| Some(floor);
    let mut prune = PruneStats::default();
    let req = TopK::new(1)
        .floor(0.0)
        .prune_stats(&mut prune)
        .live(Some(&mut live));
    let actual = bm25::top_k(&reader, &terms, &stats, PARAMS, req);
    assert_eq!(actual.iter().map(|hit| hit.doc_id).collect::<Vec<_>>(), [0]);
    assert_eq!(actual[0].score.to_bits(), longer.to_bits());
    assert!(
        prune.candidates_evaluated < texts.len() as u64,
        "wire ties should permit pruning without losing the lower document id"
    );
    let fields = [FieldQuery {
        index: &reader,
        terms: &terms,
        stats,
        params: PARAMS,
        weight: 1.0,
    }];
    let fused = bm25::top_k_fused(&fields, TopK::new(1).floor(f64::from(floor)));
    assert_eq!(fused[0].doc_id, 0);
    assert_eq!(fused[0].score.to_bits(), actual[0].score.to_bits());
    assert_eq!(
        bm25::top_k(&store, &terms, &fields[0].stats, PARAMS, 1)[0].doc_id,
        0
    );
    assert_eq!(
        bm25::top_k_exhaustive(&reader, &terms, &fields[0].stats, PARAMS, 1)[0].doc_id,
        0
    );
}

#[test]
fn raw_floor_filters_before_rounded_tie_truncation() {
    let dir = Scratch::new();
    let store = store(&["x y", "x"]);
    let path = dir.0.join("floor.bm25");
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    let terms = vec!["x".to_string()];
    let stats = CorpusStats {
        doc_count: 2,
        total_doc_length: 3,
        dfs: vec![2],
    };
    let longer = bm25::idf(2, 2) * bm25::tf_norm(PARAMS, 1, 2, stats.avgdl());
    let shorter = bm25::idf(2, 2) * bm25::tf_norm(PARAMS, 1, 1, stats.avgdl());
    let floor = (longer + shorter) / 2.0;
    assert!(longer < floor && floor < shorter);
    assert_eq!((longer as f32).to_bits(), (shorter as f32).to_bits());
    let indexes: [&dyn pipestream_search::postings::Bm25Index; 2] = [&store, &reader];
    for index in indexes {
        let flat = bm25::top_k(index, &terms, &stats, PARAMS, TopK::new(1).floor(floor));
        assert_eq!(flat.iter().map(|hit| hit.doc_id).collect::<Vec<_>>(), [1]);
        let fields = [FieldQuery {
            index,
            terms: &terms,
            stats: stats.clone(),
            params: PARAMS,
            weight: 1.0,
        }];
        let fused = bm25::top_k_fused(&fields, TopK::new(1).floor(floor));
        assert_eq!(fused.iter().map(|hit| hit.doc_id).collect::<Vec<_>>(), [1]);
    }
}

async fn relay(children: &[&Server]) -> Server {
    let (address, _relay, handle) = pipestream_search::harness::start_relay(
        children.iter().map(|child| child.address.clone()).collect(),
    )
    .await;
    Server { address, handle }
}

#[tokio::test]
async fn relay_is_one_configured_shard_for_lexical_ties() {
    let dir = Scratch::new();
    let first = serve(&dir.0.join("relay-first.bm25"), 9, &["x"], true).await;
    let second = serve(&dir.0.join("relay-second.bm25"), 2, &["x"], true).await;
    let first_relay = relay(&[&first]).await;
    let second_relay = relay(&[&second]).await;
    let together = relay(&[&first, &second]).await;
    let nested = relay(&[&first_relay, &second_relay]).await;
    let cases = [
        (
            vec![first.address.clone(), second.address.clone()],
            vec![9, 2],
        ),
        (
            vec![first_relay.address.clone(), second_relay.address.clone()],
            vec![9, 2],
        ),
        (vec![together.address.clone()], vec![2, 9]),
        (vec![nested.address.clone()], vec![2, 9]),
    ];
    for (addresses, expected) in cases {
        for fused in [false, true] {
            for k in [1, 2] {
                let unary = search_at(addresses.clone(), k, fused, false).await;
                let streamed = search_at(addresses.clone(), k, fused, true).await;
                assert_eq!(signature(&unary), signature(&streamed));
                assert_eq!(
                    unary.hits.iter().map(|hit| hit.doc_id).collect::<Vec<_>>(),
                    expected[..k as usize]
                );
            }
        }
    }
}

fn negative_image(path: &Path, adjustment: f64) {
    let mut store = Bm25Store::with_fields(&["body"]).with_numerics(&["adjustment"]);
    store
        .set_analysis_fingerprint(0, analysis_fingerprint(Some(&body_spec())))
        .unwrap();
    for id in 0..8192 {
        store.add_document(
            id,
            "x".into(),
            AnalyzedDoc::body(vec![("x".into(), 1, vec![])], 1),
        );
        store.set_numeric(0, id, adjustment);
    }
    store.save(path).unwrap();
}

#[tokio::test]
async fn negative_score_chains_preserve_unary_streamed_winners() {
    use pipestream_search::pb::{ScoreOp, ScoreStage};
    let dir = Scratch::new();
    let mut servers = Vec::new();
    for (slot, adjustment) in [(0, -5.0), (8192, -1.0)] {
        let path = dir.0.join(format!("negative-{slot}.bm25"));
        negative_image(&path, adjustment);
        servers.push(serve_image(&path, slot, true, vec!["adjustment".into()]).await);
    }
    for floor in [-100.0, 0.0] {
        let mut signatures = Vec::new();
        for streaming in [false, true] {
            let coordinator =
                CoordinatorServiceImpl::new(servers.iter().map(|s| s.address.clone()).collect())
                    .with_bm25(Some("native".into()), PARAMS)
                    .with_bm25_stream(streaming);
            let mut req = request(1, false);
            req.min_score = floor;
            req.score_stages = vec![ScoreStage {
                column: "adjustment".into(),
                operation: Some(pipestream_search::pb::score_stage::Operation::Op(
                    ScoreOp::AddLinear as i32,
                )),
                weight: 1.0,
                ..Default::default()
            }];
            let response = SearchService::bm25_search(&coordinator, Request::new(req))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(response.hits.len(), 1);
            assert_eq!(response.hits[0].doc_id, 8192);
            assert!(response.hits[0].score < 0.0);
            signatures.push(signature(&response));
        }
        assert_eq!(signatures[0], signatures[1], "floor={floor}");
    }
}

#[tokio::test]
async fn negative_node_stream_publishes_signed_floors() {
    use pipestream_search::pb::node_service_client::NodeServiceClient;
    use pipestream_search::pb::{
        bm25_query_stream_request, bm25_query_stream_response, Bm25QueryRequest,
        Bm25QueryStreamRequest, ScoreOp, ScoreStage,
    };
    let dir = Scratch::new();
    let path = dir.0.join("signed-floors.bm25");
    negative_image(&path, -5.0);
    let server = serve_image(&path, 0, true, vec!["adjustment".into()]).await;
    let mut client = NodeServiceClient::connect(server.address.clone())
        .await
        .unwrap();
    let request = Bm25QueryRequest {
        terms: vec!["x".into()],
        k: 1,
        global_doc_count: 8192,
        global_total_doc_length: 8192,
        global_doc_frequencies: vec![8192],
        k1: PARAMS.k1 as f32,
        b: PARAMS.b as f32,
        min_score: -100.0,
        analysis_fingerprint: analysis_fingerprint(Some(&body_spec())),
        score_stages: vec![ScoreStage {
            column: "adjustment".into(),
            operation: Some(pipestream_search::pb::score_stage::Operation::Op(
                ScoreOp::AddLinear as i32,
            )),
            weight: 1.0,
            ..Default::default()
        }],
        ..Default::default()
    };
    let input = tokio_stream::iter([Bm25QueryStreamRequest {
        payload: Some(bm25_query_stream_request::Payload::Start(request)),
    }]);
    let mut stream = client.bm25_query_stream(input).await.unwrap().into_inner();
    let (mut floors, mut completed) = (0, false);
    while let Some(message) = stream.message().await.unwrap() {
        match message.payload.unwrap() {
            bm25_query_stream_response::Payload::FloorUpdate(update) => {
                assert!(
                    update.floor < 0.0 && update.floor.is_finite(),
                    "{}",
                    update.floor
                );
                floors += 1;
            }
            bm25_query_stream_response::Payload::Completion(done) => {
                assert!(done.completed);
                let response = done.response.unwrap();
                assert_eq!(response.hits.len(), 1);
                assert!(response.hits[0].score < 0.0);
                completed = true;
            }
            _ => {}
        }
    }
    assert!(completed && floors > 0);
}
