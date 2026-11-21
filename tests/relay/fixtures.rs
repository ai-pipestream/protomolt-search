//! Shared relay tree topologies and lexical/columned leaf cluster builders.

use super::*;

pub(crate) async fn add_documents(addr: &str, texts: &[&str]) {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(texts.len().max(1));
    for text in texts {
        tx.send(AddDocumentsRequest {
            text: text.to_string(),
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
    assert_eq!(added as usize, texts.len());
}

pub(crate) fn stats_request() -> TermStatsRequest {
    TermStatsRequest {
        version_only: false,
        visibility: None,
        terms: vec!["court".into(), "opinion".into(), "absent".into()],
        fields: vec![FieldTerms {
            field: "body".into(),
            terms: vec!["court".into(), "absent".into()],
        }],
    }
}

/// A child address that accepts connections and never speaks: the
/// relay's stream to it stays open until something ends the attempt.
pub(crate) async fn holding_child() -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let mut held = Vec::new();
        loop {
            if let Ok((socket, _)) = listener.accept().await {
                held.push(socket);
            }
        }
    });
    format!("http://{addr}")
}

pub(crate) async fn read_until_summary_or_error(
    inbound: &mut tonic::Streaming<pipestream_search::pb::StreamSearchResponse>,
) -> Result<pipestream_search::pb::StreamSearchSummary, tonic::Status> {
    loop {
        match inbound.message().await? {
            Some(pipestream_search::pb::StreamSearchResponse {
                payload: Some(stream_search_response::Payload::Summary(summary)),
            }) => return Ok(summary),
            Some(_) => continue,
            None => panic!("the stream closed without a summary"),
        }
    }
}

pub(crate) const LEX_LEAVES: usize = 4;
pub(crate) const LEX_ROWS: usize = 8;
pub(crate) const LEX_K: u32 = 6;

/// One leaf's texts. Each leaf pads its documents by a different amount,
/// so a document with the same words in another leaf scores differently:
/// the flat unary fan-out orders equal scores by shard index and a relay
/// by id, and only equal scores could tell the two apart.
pub(crate) fn lexical_texts(leaf: usize) -> Vec<String> {
    let base = [
        "the court held",
        "an opinion of the court",
        "zebra crossing ahead",
        "the court opinion on the zebra",
        "a lone court",
        "no match in here",
        "opinion opinion",
        "zebra zebra court",
    ];
    base.iter()
        .map(|words| format!("{words}{}", " pad".repeat(leaf + 1)))
        .collect()
}

pub(crate) struct LexicalLeaves {
    pub(crate) addrs: Vec<String>,
    pub(crate) analysis: String,
    pub(crate) corpus: Vec<f32>,
    pub(crate) _mock: tokio::task::JoinHandle<Result<(), tonic::transport::Error>>,
}

pub(crate) async fn configure_backend(addr: &str, shift: &[f32], scale: &[f32]) {
    NodeServiceClient::connect(addr.to_string())
        .await
        .unwrap()
        .configure_vector_backend(pipestream_search::harness::embedded_backend_request(
            DIM, BIT_WIDTH, shift, scale,
        ))
        .await
        .unwrap();
}

pub(crate) async fn add_faceted_documents(addr: &str, texts: &[String]) {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(4);
    let texts = texts.to_vec();
    let feeder = tokio::spawn(async move {
        for (i, text) in texts.into_iter().enumerate() {
            tx.send(AddDocumentsRequest {
                text,
                facets: vec![FacetValue {
                    field: "court".into(),
                    value: if i % 2 == 0 {
                        "scotus".into()
                    } else {
                        "ca9".into()
                    },
                }],
                ..Default::default()
            })
            .await
            .unwrap();
        }
    });
    client.add_documents(ReceiverStream::new(rx)).await.unwrap();
    feeder.await.unwrap();
}

pub(crate) async fn add_vectors(addr: &str, vectors: Vec<f32>) {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(1);
    tx.send(AddVectorsRequest {
        materialized_rows: Vec::new(),
        vectors,
        dim: DIM as u32,
    })
    .await
    .unwrap();
    drop(tx);
    client.add_vectors(ReceiverStream::new(rx)).await.unwrap();
}

/// `year` and `pages` of global row `i`: an integer column and a double
/// column with values exact in binary, so a fold over them is exact in
/// any grouping and a fold through relays can be compared bit for bit.
pub(crate) fn year_of(i: usize) -> i64 {
    2000 + i as i64
}

pub(crate) fn pages_of(i: usize) -> f64 {
    i as f64 * 1.5
}

/// The faceted documents with the `year` and `pages` columns set from
/// the global row index (`leaf * LEX_ROWS + i`).
pub(crate) async fn add_columned_documents(addr: &str, leaf: usize, texts: &[String]) {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let (tx, rx) = mpsc::channel(4);
    let texts = texts.to_vec();
    let feeder = tokio::spawn(async move {
        for (i, text) in texts.into_iter().enumerate() {
            let row = leaf * LEX_ROWS + i;
            tx.send(AddDocumentsRequest {
                original_source: Some(common::protobuf_source("relay", &row.to_string())),
                identity: (row != 0).then(|| pipestream_search::pb::DocumentIdentity {
                    document_key: (row as u64).to_le_bytes().to_vec(),
                    version: u64::MAX,
                    chunk_ordinal: None,
                }),
                text,
                facets: vec![FacetValue {
                    field: "court".into(),
                    value: if i % 2 == 0 {
                        "scotus".into()
                    } else {
                        "ca9".into()
                    },
                }],
                integers: vec![pipestream_search::pb::IntegerValue {
                    field: "year".into(),
                    value: year_of(row),
                }],
                numerics: vec![pipestream_search::pb::NumericValue {
                    field: "pages".into(),
                    value: pages_of(row),
                }],
                ..Default::default()
            })
            .await
            .unwrap();
        }
    });
    client.add_documents(ReceiverStream::new(rx)).await.unwrap();
    feeder.await.unwrap();
}

/// Four leaves with documents, a facet, the `year` and `pages` columns,
/// and vectors aligned by id, on contiguous slot ranges;
/// `positions[leaf]` gives the leaf positions on the body field.
pub(crate) async fn lexical_leaves(positions: &[bool; LEX_LEAVES]) -> LexicalLeaves {
    let (analysis, mock) = start_mock_analysis().await;
    let corpus = unit_vectors(LEX_LEAVES * LEX_ROWS, DIM, 0x1E7A_2E1A);
    let (shift, scale) = fit_calibration(DIM, BIT_WIDTH, &corpus);
    let mut addrs = Vec::new();
    for leaf in 0..LEX_LEAVES {
        let (addr, _handle) = start_empty_node(NodeConfig {
            slot_offset: (leaf * LEX_ROWS) as u64,
            analysis_addr: Some(analysis.clone()),
            facet_fields: vec!["court".into()],
            integer_fields: vec!["year".into()],
            numeric_fields: vec!["pages".into()],
            position_fields: if positions[leaf] {
                vec!["body".into()]
            } else {
                Vec::new()
            },
            ..Default::default()
        })
        .await;
        configure_backend(&addr, &shift, &scale).await;
        add_columned_documents(&addr, leaf, &lexical_texts(leaf)).await;
        add_vectors(
            &addr,
            corpus[leaf * LEX_ROWS * DIM..(leaf + 1) * LEX_ROWS * DIM].to_vec(),
        )
        .await;
        addrs.push(addr);
    }
    LexicalLeaves {
        addrs,
        analysis,
        corpus,
        _mock: mock,
    }
}

pub(crate) fn lexical_request(text: &str) -> Bm25SearchRequest {
    Bm25SearchRequest {
        text: text.into(),
        k: LEX_K,
        facet_fields: vec!["court".into()],
        explain: true,
        ..Default::default()
    }
}

/// (doc id, fused score bits, vector rank, vector score bits, bm25 rank,
/// bm25 score bits).
pub(crate) type HybridBits = (u64, u32, Option<u32>, u32, Option<u32>, u32);

/// A hybrid hit without the shard index the parent names, which is the
/// relay's index through a relay and the leaf's on the flat path.
pub(crate) fn hybrid_bits(hits: &[HybridHit]) -> Vec<HybridBits> {
    hits.iter()
        .map(|h| {
            (
                h.doc_id,
                h.fused_score.to_bits(),
                h.vector_rank,
                h.vector_score.to_bits(),
                h.bm25_rank,
                h.bm25_score.to_bits(),
            )
        })
        .collect()
}

pub(crate) async fn lexical(
    coordinator: &CoordinatorServiceImpl,
    text: &str,
) -> (
    Vec<Bm25Hit>,
    Vec<pipestream_search::pb::FacetFieldCounts>,
    u32,
) {
    let response = coordinator
        .bm25_search(tonic::Request::new(lexical_request(text)))
        .await
        .expect("lexical search")
        .into_inner();
    (response.hits, response.facets, response.segments_total)
}

pub(crate) fn cel_clause(id: &str, cel: &str) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Filter(FilterQuery {
            id: id.to_string(),
            predicate: Some(filter_query::Predicate::Cel(cel.to_string())),
        })),
    }
}

pub(crate) fn lexical_clause(id: &str, text: &str) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: id.to_string(),
            query: Some(search_query::Query::Lexical(LexicalQuery {
                text: text.to_string(),
                ..Default::default()
            })),
        })),
    }
}

pub(crate) fn boolean(
    must: Vec<SelectionQuery>,
    should: Vec<SelectionQuery>,
    must_not: Vec<SelectionQuery>,
) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Boolean(BooleanQuery {
            must,
            should,
            must_not,
            minimum_should_match: 0,
            aggregate: None,
        })),
    }
}

/// `(doc id, score bits, rank, matched clauses)`.
pub(crate) fn query_bits(hits: &[QueryHit]) -> Vec<(u64, u32, u32, Vec<String>)> {
    hits.iter()
        .map(|h| (h.doc_id, h.score.to_bits(), h.rank, h.matched.clone()))
        .collect()
}

pub(crate) async fn run_query(
    coordinator: &CoordinatorServiceImpl,
    selection: SelectionQuery,
) -> Result<Vec<QueryHit>, tonic::Status> {
    run_request(
        coordinator,
        QueryRequest {
            request_id: "relayed".into(),
            k: LEX_K,
            selection: Some(selection),
            ..Default::default()
        },
    )
    .await
    .map(|r| r.hits)
}

pub(crate) async fn run_request(
    coordinator: &CoordinatorServiceImpl,
    request: QueryRequest,
) -> Result<pipestream_search::pb::QueryResponse, tonic::Status> {
    coordinator
        .query(tonic::Request::new(request))
        .await
        .map(|r| r.into_inner())
}

/// The relay trees the fold and fetch tests run under: one level in
/// order, one level permuted, and two levels (a chain).
pub(crate) async fn relay_trees(l: &[String]) -> Vec<(&'static str, Vec<String>)> {
    let a = relay_over(&[&l[0], &l[1]]).await;
    let b = relay_over(&[&l[2], &l[3]]).await;
    let p = relay_over(&[&l[3], &l[2]]).await;
    let q = relay_over(&[&l[1], &l[0]]).await;
    let top = relay_over(&[&a, &b]).await;
    vec![
        ("one level", vec![a, b]),
        ("permuted", vec![q, p]),
        ("two levels", vec![top]),
    ]
}

pub(crate) fn rrf_legs() -> HybridLegs {
    HybridLegs {
        leg_k: 12,
        vector_weight: 1.0,
        bm25_weight: 1.0,
        rrf_k: 60.0,
        fusion_mode: FusionMode::GlobalRank,
        normalization: Normalization::MinMax,
        combination: Combination::Arithmetic,
        min_vector_score: 0.0,
    }
}
