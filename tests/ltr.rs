//! The composite scorer on the public `Query` route (`docs/query-api.md`,
//! `src/ltr.rs`): dimensions reorder real candidate pools, provenance is
//! precise enough to recompute every final score client-side, paging
//! stays inside the fixed pool, and every unsupported interplay refuses
//! by name.

mod common;

use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    score_signal, search_query, selection_query, selection_score_strategy, AddDocumentsRequest,
    AddVectorsRequest, BoostQuery, CompositeScoreOperation, CompositeScorer,
    CompositeSearchStrategy, DenseQuery, FilterQuery, IntegerValue, LexicalQuery,
    MissingScorePolicy, NamedProjection, NumericValue, QueryRequest, QueryResponse, QuerySort,
    RrfScore, ScoreDimension, ScoreNormalization, ScoreOp, ScoreSignal, ScoreStage, SearchQuery,
    SelectionOperator, SelectionQuery, SelectionScoreStrategy, SetCalibrationRequest,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

use common::{fit_calibration, mock::start_mock_analysis, start_empty_node, unit_vectors};

const DIM: usize = 64;
const SHARD_DOCS: usize = 4;
const N_DOCS: usize = 2 * SHARD_DOCS;

/// The query_api corpus: "zebra" in even docs, "plain" in odd docs,
/// `year = i` everywhere, seeded unit vectors; the query vector is doc
/// 0's own (so the dense order and the lexical order disagree).
async fn start_cluster() -> (
    CoordinatorServiceImpl,
    Vec<f32>,
    Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
) {
    let (analysis, mock) = start_mock_analysis().await;
    let corpus = unit_vectors(N_DOCS, DIM, 0xC0FE_0001);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);
    let mut handles = vec![mock];
    let mut addrs = Vec::new();
    for shard in 0..2usize {
        let (addr, handle) = start_empty_node(NodeConfig {
            slot_offset: (shard * SHARD_DOCS) as u64,
            analysis_addr: Some(analysis.clone()),
            integer_fields: vec!["year".into()],
            numeric_fields: vec!["quality".into()],
            ..Default::default()
        })
        .await;
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        client
            .set_calibration(SetCalibrationRequest {
                dim: DIM as u32,
                bit_width: 4,
                shift: shift.clone(),
                scale: scale.clone(),
            })
            .await
            .unwrap();
        let (tx, rx) = mpsc::channel(16);
        for i in 0..SHARD_DOCS {
            let id = shard * SHARD_DOCS + i;
            let text = if id.is_multiple_of(2) {
                format!("zebra document {id}")
            } else {
                format!("plain document {id}")
            };
            tx.send(AddDocumentsRequest {
                text,
                integers: vec![IntegerValue {
                    field: "year".into(),
                    value: id as i64,
                }],
                // "quality" lands on EVEN docs only: the corpus's
                // honestly-partial column, for missing-policy tests.
                numerics: if id.is_multiple_of(2) {
                    vec![NumericValue {
                        field: "quality".into(),
                        value: id as f64 + 1.0,
                    }]
                } else {
                    Vec::new()
                },
                ..Default::default()
            })
            .await
            .unwrap();
        }
        drop(tx);
        client.add_documents(ReceiverStream::new(rx)).await.unwrap();
        let start = shard * SHARD_DOCS;
        let (vtx, vrx) = mpsc::channel(4);
        vtx.send(AddVectorsRequest {
            vectors: corpus[start * DIM..(start + SHARD_DOCS) * DIM].to_vec(),
            dim: DIM as u32,
        })
        .await
        .unwrap();
        drop(vtx);
        client.add_vectors(ReceiverStream::new(vrx)).await.unwrap();
        addrs.push(addr);
        handles.push(handle);
    }
    let coordinator =
        CoordinatorServiceImpl::new(addrs).with_bm25(Some(analysis), Default::default());
    (coordinator, corpus[..DIM].to_vec(), handles)
}

fn lexical_leaf(id: &str, text: &str) -> SelectionQuery {
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

fn dense_leaf(id: &str, vector: &[f32]) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: id.to_string(),
            query: Some(search_query::Query::Dense(DenseQuery {
                vector: vector.to_vec(),
            })),
        })),
    }
}

/// OR(dense, lexical) under global-rank RRF: the union pool whose legs
/// disagree, the natural scorer substrate.
fn rrf_union(qvec: &[f32], text: &str) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Composite(CompositeSearchStrategy {
            operator: SelectionOperator::Or as i32,
            clauses: vec![dense_leaf("vec", qvec), lexical_leaf("lex", text)],
            scoring: Some(SelectionScoreStrategy {
                strategy: Some(selection_score_strategy::Strategy::Rrf(RrfScore::default())),
            }),
        })),
    }
}

fn dim(id: &str, source: score_signal::Source) -> ScoreDimension {
    ScoreDimension {
        id: id.to_string(),
        weight: None,
        source: Some(ScoreSignal {
            source: Some(source),
        }),
        normalization: 0,
        missing: 0,
    }
}

fn base_dim(id: &str) -> ScoreDimension {
    dim(id, score_signal::Source::Base(true))
}

fn query_dim(id: &str, query_id: &str) -> ScoreDimension {
    dim(
        id,
        score_signal::Source::QueryRelevanceId(query_id.to_string()),
    )
}

fn scorer(op: CompositeScoreOperation, dimensions: Vec<ScoreDimension>) -> CompositeScorer {
    CompositeScorer {
        operation: op as i32,
        dimensions,
    }
}

async fn query(
    coordinator: &CoordinatorServiceImpl,
    req: QueryRequest,
) -> Result<QueryResponse, tonic::Status> {
    coordinator
        .query(Request::new(req))
        .await
        .map(|r| r.into_inner())
}

/// Recompute one hit's final score from its dimension provenance the
/// way the contract promises a client can: contributions only, plus
/// the request's weights for the harmonic denominator.
fn recompute(
    op: CompositeScoreOperation,
    hit: &pipestream_search::pb::QueryHit,
    weights: &[f64],
) -> f32 {
    let active: Vec<(usize, f64)> = hit
        .dimensions
        .iter()
        .enumerate()
        .filter(|(_, d)| !d.skipped)
        .map(|(i, d)| (i, d.contribution))
        .collect();
    let v: f64 = match op {
        CompositeScoreOperation::WeightedSum | CompositeScoreOperation::WeightedMean => {
            active.iter().map(|(_, c)| c).sum()
        }
        CompositeScoreOperation::Maximum => active
            .iter()
            .map(|(_, c)| *c)
            .fold(f64::NEG_INFINITY, f64::max),
        CompositeScoreOperation::Product | CompositeScoreOperation::GeometricMean => {
            active.iter().map(|(_, c)| c).product()
        }
        CompositeScoreOperation::HarmonicMean => {
            let w: f64 = active.iter().map(|(i, _)| weights[*i]).sum();
            w / active.iter().map(|(_, c)| c).sum::<f64>()
        }
        CompositeScoreOperation::Unspecified => unreachable!(),
    };
    v as f32
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scorer_reorders_the_hybrid_pool_and_reports_dimensions() {
    let (coordinator, qvec, _handles) = start_cluster().await;
    let mut d_vec = query_dim("vec", "vec");
    d_vec.weight = Some(2.0);
    let mut d_lex = query_dim("lex", "lex");
    d_lex.weight = Some(3.0);
    let weights = [2.0, 3.0];
    let response = query(
        &coordinator,
        QueryRequest {
            k: 8,
            selection_k: 8,
            selection: Some(rrf_union(&qvec, "zebra")),
            scorer: Some(scorer(
                CompositeScoreOperation::WeightedSum,
                vec![d_vec, d_lex],
            )),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(
        response.executed,
        "hybrid_search:global_rank+scorer:weighted_sum"
    );
    assert_eq!(response.hits.len(), N_DOCS);
    for (i, hit) in response.hits.iter().enumerate() {
        // Dimensions ride every hit, aligned with the request.
        assert_eq!(hit.rank, (i + 1) as u32);
        assert_eq!(hit.dimensions.len(), 2);
        assert_eq!(hit.dimensions[0].id, "vec");
        assert_eq!(hit.dimensions[1].id, "lex");
        // The reconstruction guarantee, on real scores.
        assert_eq!(
            hit.score.to_bits(),
            recompute(CompositeScoreOperation::WeightedSum, hit, &weights).to_bits(),
            "doc {}",
            hit.doc_id
        );
        // A missing raw is exactly a missing signal: odd docs match no
        // lexical term.
        let has_lex = hit.signals.iter().any(|s| s.id == "lex");
        assert_eq!(hit.dimensions[1].raw.is_some(), has_lex);
        assert_eq!(hit.doc_id % 2 == 0, has_lex);
    }
    // The order is the composite score's, descending, ties by doc id.
    for pair in response.hits.windows(2) {
        assert!(
            pair[0].score > pair[1].score
                || (pair[0].score == pair[1].score && pair[0].doc_id < pair[1].doc_id)
        );
    }
    // Determinism: the identical request returns the identical ranking
    // bitwise.
    let again = query(
        &coordinator,
        QueryRequest {
            k: 8,
            selection_k: 8,
            selection: Some(rrf_union(&qvec, "zebra")),
            scorer: Some(scorer(
                CompositeScoreOperation::WeightedSum,
                vec![
                    {
                        let mut d = query_dim("vec", "vec");
                        d.weight = Some(2.0);
                        d
                    },
                    {
                        let mut d = query_dim("lex", "lex");
                        d.weight = Some(3.0);
                        d
                    },
                ],
            )),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    for (a, b) in response.hits.iter().zip(&again.hits) {
        assert_eq!(a.doc_id, b.doc_id);
        assert_eq!(a.score.to_bits(), b.score.to_bits());
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dimension_weights_choose_the_winning_leg() {
    let (coordinator, qvec, _handles) = start_cluster().await;
    // The legs disagree by construction: the dense top is doc 0 (its
    // own vector), the lexical top for "document 6" is doc 6 (the only
    // doc containing the term "6").
    let run = |only: &'static str| {
        let qvec = qvec.clone();
        let coordinator = &coordinator;
        async move {
            let mut d_vec = query_dim("vec", "vec");
            d_vec.weight = Some(if only == "vec" { 1.0 } else { 0.0 });
            let mut d_lex = query_dim("lex", "lex");
            d_lex.weight = Some(if only == "lex" { 1.0 } else { 0.0 });
            query(
                coordinator,
                QueryRequest {
                    k: 8,
                    selection_k: 8,
                    selection: Some(rrf_union(&qvec, "document 6")),
                    scorer: Some(scorer(
                        CompositeScoreOperation::WeightedSum,
                        vec![d_vec, d_lex],
                    )),
                    ..Default::default()
                },
            )
            .await
            .unwrap()
        }
    };
    let dense_only = run("vec").await;
    let lexical_only = run("lex").await;
    assert_eq!(dense_only.hits[0].doc_id, 0);
    assert_eq!(lexical_only.hits[0].doc_id, 6);
    // The disabled dimension is reported but skipped on every hit.
    assert!(dense_only.hits.iter().all(|h| h.dimensions[1].skipped));
    assert!(lexical_only.hits.iter().all(|h| h.dimensions[0].skipped));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn single_leaf_scorer_pages_within_the_selection_pool() {
    let (coordinator, _qvec, _handles) = start_cluster().await;
    let make = |k: u32, cursor: String| QueryRequest {
        k,
        selection_k: 8,
        cursor,
        selection: Some(lexical_leaf("lex", "document")),
        scorer: Some(scorer(
            CompositeScoreOperation::WeightedMean,
            vec![{
                let mut d = base_dim("base");
                d.normalization = ScoreNormalization::ZScore as i32;
                d
            }],
        )),
        ..Default::default()
    };
    // The full pool ranking in one request.
    let full = query(&coordinator, make(8, String::new())).await.unwrap();
    assert_eq!(full.executed, "bm25_search+scorer:weighted_mean");
    assert_eq!(full.hits.len(), N_DOCS);
    // Z-scores are monotone in the base, so the base order survives
    // while the values move onto the z scale.
    let direct = query(
        &coordinator,
        QueryRequest {
            k: 8,
            selection: Some(lexical_leaf("lex", "document")),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let full_ids: Vec<u64> = full.hits.iter().map(|h| h.doc_id).collect();
    let direct_ids: Vec<u64> = direct.hits.iter().map(|h| h.doc_id).collect();
    assert_eq!(full_ids, direct_ids);

    // Pages stitch into the same ranking, entirely inside the pool.
    let page1 = query(&coordinator, make(3, String::new())).await.unwrap();
    assert!(!page1.next_cursor.is_empty());
    let page2 = query(&coordinator, make(3, page1.next_cursor.clone()))
        .await
        .unwrap();
    let stitched: Vec<u64> = page1
        .hits
        .iter()
        .chain(&page2.hits)
        .map(|h| h.doc_id)
        .collect();
    assert_eq!(stitched, full_ids[..6].to_vec());
    assert_eq!(page2.hits[0].rank, 4);

    // The pool exhausts rather than silently deepening: rank 6 + k 3
    // exceeds selection_k 8.
    let err = query(&coordinator, make(3, page2.next_cursor.clone()))
        .await
        .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("selection_k = 8"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boost_signals_feed_the_scorer() {
    let (coordinator, qvec, _handles) = start_cluster().await;
    let boost = BoostQuery {
        query: Some(SearchQuery {
            id: "boost".into(),
            query: Some(search_query::Query::Lexical(LexicalQuery {
                text: "plain".into(),
                ..Default::default()
            })),
        }),
        ..Default::default()
    };
    let mut d_boost = query_dim("boost", "boost");
    d_boost.weight = Some(5.0);
    let response = query(
        &coordinator,
        QueryRequest {
            k: 8,
            selection_k: 8,
            selection: Some(rrf_union(&qvec, "zebra")),
            boosts: vec![boost.clone()],
            scorer: Some(scorer(
                CompositeScoreOperation::WeightedSum,
                vec![base_dim("base"), d_boost],
            )),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    // "plain" lives in the odd docs: with the boost dimension at
    // weight 5 over a min-max base, every boosted doc outranks every
    // unboosted one.
    let (odd, even): (Vec<&pipestream_search::pb::QueryHit>, Vec<_>) =
        response.hits.iter().partition(|h| h.doc_id % 2 == 1);
    let worst_odd = odd.iter().map(|h| h.score).fold(f32::INFINITY, f32::min);
    let best_even = even
        .iter()
        .map(|h| h.score)
        .fold(f32::NEG_INFINITY, f32::max);
    assert!(worst_odd > best_even);
    // The boost signal rides provenance on exactly the boosted docs.
    for h in &response.hits {
        assert_eq!(
            h.dimensions[1].raw.is_some(),
            h.doc_id % 2 == 1,
            "doc {}",
            h.doc_id
        );
    }

    // The reorder knobs belong to the boost's own combination; with a
    // scorer they refuse by name.
    let mut with_window = boost;
    with_window.window = 3;
    let err = query(
        &coordinator,
        QueryRequest {
            k: 8,
            selection_k: 8,
            selection: Some(rrf_union(&qvec, "zebra")),
            boosts: vec![with_window],
            scorer: Some(scorer(
                CompositeScoreOperation::WeightedSum,
                vec![base_dim("base")],
            )),
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("signal-only"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn error_policy_refuses_on_a_real_union_gap() {
    let (coordinator, qvec, _handles) = start_cluster().await;
    let mut d = query_dim("lex", "lex");
    d.missing = MissingScorePolicy::Error as i32;
    let err = query(
        &coordinator,
        QueryRequest {
            k: 8,
            selection_k: 8,
            selection: Some(rrf_union(&qvec, "zebra")),
            scorer: Some(scorer(CompositeScoreOperation::WeightedSum, vec![d])),
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    // Odd docs enter the union through the dense leg alone; the caller
    // asserted every candidate has a lexical signal, and the engine
    // names the first that does not.
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("dimension \"lex\""));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scorer_interplays_refuse_by_name() {
    let (coordinator, _qvec, _handles) = start_cluster().await;
    let base = || {
        Some(scorer(
            CompositeScoreOperation::WeightedSum,
            vec![base_dim("base")],
        ))
    };
    let cases = vec![
        (
            // A browse has no relevance signals to combine.
            QueryRequest {
                k: 5,
                selection: Some(SelectionQuery {
                    node: Some(selection_query::Node::Filter(FilterQuery {
                        id: "f".into(),
                        predicate: Some(pipestream_search::pb::filter_query::Predicate::Cel(
                            "year >= 0".into(),
                        )),
                    })),
                }),
                scorer: base(),
                ..Default::default()
            },
            "SCORED selection",
        ),
        (
            // Sort and the scorer can never combine: on a scored
            // selection the sort refusal fires; on a browse the
            // scorer refusal fires. Both directions stay refused.
            QueryRequest {
                k: 5,
                selection: Some(lexical_leaf("lex", "zebra")),
                sort: Some(QuerySort {
                    column: "year".into(),
                    descending: true,
                }),
                scorer: base(),
                ..Default::default()
            },
            "browse selections only",
        ),
        (
            // A dimension may not source a filter.
            QueryRequest {
                k: 5,
                selection: Some(SelectionQuery {
                    node: Some(selection_query::Node::Composite(CompositeSearchStrategy {
                        operator: SelectionOperator::And as i32,
                        clauses: vec![
                            lexical_leaf("lex", "zebra"),
                            SelectionQuery {
                                node: Some(selection_query::Node::Filter(FilterQuery {
                                    id: "f".into(),
                                    predicate: Some(
                                        pipestream_search::pb::filter_query::Predicate::Cel(
                                            "year >= 0".into(),
                                        ),
                                    ),
                                })),
                            },
                        ],
                        scoring: None,
                    })),
                }),
                scorer: Some(scorer(
                    CompositeScoreOperation::WeightedSum,
                    vec![query_dim("d", "f")],
                )),
                ..Default::default()
            },
            "never contributes",
        ),
        (
            // Unknown signal ids refuse naming the id.
            QueryRequest {
                k: 5,
                selection: Some(lexical_leaf("lex", "zebra")),
                scorer: Some(scorer(
                    CompositeScoreOperation::WeightedSum,
                    vec![query_dim("d", "nope")],
                )),
                ..Default::default()
            },
            "names no search or boost query",
        ),
        (
            // A stored-value dimension's stage obeys the stage
            // admission rules: the default stage names no column.
            QueryRequest {
                k: 5,
                selection: Some(lexical_leaf("lex", "zebra")),
                scorer: Some(scorer(
                    CompositeScoreOperation::WeightedSum,
                    vec![dim(
                        "d",
                        score_signal::Source::BoundedValue(Default::default()),
                    )],
                )),
                ..Default::default()
            },
            "names the numeric column",
        ),
    ];
    for (req, needle) in cases {
        let err = query(&coordinator, req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{needle}");
        assert!(
            err.message().contains(needle),
            "expected {needle:?} in: {}",
            err.message()
        );
    }
}

// ---------------------------------------------------------------------------
// Boost generalization: dense boosts, boosts on single-leaf shapes,
// multiple boosts under the scorer.

fn lexical_boost(id: &str, text: &str) -> BoostQuery {
    BoostQuery {
        query: Some(SearchQuery {
            id: id.to_string(),
            query: Some(search_query::Query::Lexical(LexicalQuery {
                text: text.to_string(),
                ..Default::default()
            })),
        }),
        ..Default::default()
    }
}

fn dense_boost(id: &str, vector: &[f32]) -> BoostQuery {
    BoostQuery {
        query: Some(SearchQuery {
            id: id.to_string(),
            query: Some(search_query::Query::Dense(DenseQuery {
                vector: vector.to_vec(),
            })),
        }),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dense_boost_reranks_a_lexical_leaf() {
    let (coordinator, qvec, _handles) = start_cluster().await;
    // "document" scores every doc identically; the dense boost breaks
    // the tie by similarity to doc 0's own vector, decisively.
    let mut boost = dense_boost("sim", &qvec);
    boost.boost_weight = 100.0;
    let response = query(
        &coordinator,
        QueryRequest {
            k: 8,
            selection_k: 8,
            selection: Some(lexical_leaf("lex", "document")),
            boosts: vec![boost],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(response.executed, "bm25_search");
    assert_eq!(response.hits[0].doc_id, 0);
    // The boost is provenance, never the score: the reported score is
    // still the base BM25 score, and the boost's calibrated product
    // rides the named signal on every doc carrying a vector.
    for h in &response.hits {
        let sim = h.signals.iter().find(|s| s.id == "sim").unwrap();
        assert!(sim.score.is_finite());
        assert!(h.matched.contains(&"sim".to_string()));
    }
    let top_sim = response.hits[0]
        .signals
        .iter()
        .find(|s| s.id == "sim")
        .unwrap()
        .score;
    for h in &response.hits[1..] {
        let sim = h.signals.iter().find(|s| s.id == "sim").unwrap().score;
        assert!(top_sim >= sim);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lexical_boost_windows_a_single_leaf_pool() {
    let (coordinator, _qvec, _handles) = start_cluster().await;
    // Base order for "document" is the doc-id tie order 0..7; the
    // boost rescores only the top-4 window, so "zebra" lifts docs 0
    // and 2 while docs 4 and 6 (equally zebra-bearing, outside the
    // window) keep their place and carry no boost signal.
    let mut boost = lexical_boost("z", "zebra");
    boost.window = 4;
    boost.boost_weight = 100.0;
    let response = query(
        &coordinator,
        QueryRequest {
            k: 8,
            selection_k: 8,
            selection: Some(lexical_leaf("lex", "document")),
            boosts: vec![boost],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let ids: Vec<u64> = response.hits.iter().map(|h| h.doc_id).collect();
    assert_eq!(ids, vec![0, 2, 1, 3, 4, 5, 6, 7]);
    for h in &response.hits {
        let has_signal = h.signals.iter().any(|s| s.id == "z");
        // Signals land only inside the window, and only on matches.
        assert_eq!(
            has_signal,
            h.doc_id < 4 && h.doc_id % 2 == 0,
            "doc {}",
            h.doc_id
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lexical_boost_on_a_dense_leaf_carries_its_own_analysis() {
    let (coordinator, qvec, _handles) = start_cluster().await;
    // A dense-only selection has no lexical leaf to inherit from, so
    // the boost's own analysis spec is admitted.
    let mut boost = lexical_boost("plain", "plain");
    boost.boost_weight = 100.0;
    if let Some(search_query::Query::Lexical(lex)) = boost.query.as_mut().unwrap().query.as_mut() {
        lex.analysis = Some(Default::default());
    }
    let response = query(
        &coordinator,
        QueryRequest {
            k: 8,
            selection_k: 8,
            selection: Some(dense_leaf("vec", &qvec)),
            boosts: vec![boost.clone()],
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(response.executed, "search");
    // Odd docs carry "plain": boosted to the top, signals on exactly
    // those docs.
    assert!(response.hits[0].doc_id % 2 == 1);
    for h in &response.hits {
        assert_eq!(
            h.signals.iter().any(|s| s.id == "plain"),
            h.doc_id % 2 == 1,
            "doc {}",
            h.doc_id
        );
    }

    // On a selection WITH a lexical leaf the same spec refuses: term
    // identity belongs to the leaf.
    let err = query(
        &coordinator,
        QueryRequest {
            k: 8,
            selection_k: 8,
            selection: Some(lexical_leaf("lex", "document")),
            boosts: vec![boost],
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err
        .message()
        .contains("analyzed under that leaf's analysis"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn multiple_boosts_combine_through_the_scorer() {
    let (coordinator, qvec, _handles) = start_cluster().await;
    let mut d_sim = query_dim("sim", "sim");
    d_sim.weight = Some(10.0);
    let mut d_plain = query_dim("plain", "plain");
    d_plain.weight = Some(5.0);
    let response = query(
        &coordinator,
        QueryRequest {
            k: 8,
            selection_k: 8,
            selection: Some(lexical_leaf("lex", "document")),
            boosts: vec![dense_boost("sim", &qvec), lexical_boost("plain", "plain")],
            scorer: Some(scorer(
                CompositeScoreOperation::WeightedSum,
                vec![base_dim("base"), d_sim, d_plain],
            )),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(response.executed, "bm25_search+scorer:weighted_sum");
    // Doc 0 tops the pool: its dense similarity min-maxes to 1.0 at
    // weight 10, beating any odd doc's plain dimension at weight 5.
    assert_eq!(response.hits[0].doc_id, 0);
    for h in &response.hits {
        assert_eq!(h.dimensions.len(), 3);
        assert_eq!(h.dimensions[1].id, "sim");
        assert_eq!(h.dimensions[2].id, "plain");
        // Both boost signals report exactly where they exist: dense on
        // every doc (all carry vectors), plain on odd docs.
        assert!(h.dimensions[1].raw.is_some());
        assert_eq!(h.dimensions[2].raw.is_some(), h.doc_id % 2 == 1);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boost_shapes_refuse_by_name() {
    let (coordinator, qvec, _handles) = start_cluster().await;
    let browse = SelectionQuery {
        node: Some(selection_query::Node::Filter(FilterQuery {
            id: "f".into(),
            predicate: Some(pipestream_search::pb::filter_query::Predicate::Cel(
                "year >= 0".into(),
            )),
        })),
    };
    let cases = vec![
        (
            QueryRequest {
                k: 5,
                selection: Some(browse),
                boosts: vec![lexical_boost("b", "plain")],
                ..Default::default()
            },
            "SCORED selection",
        ),
        (
            QueryRequest {
                k: 5,
                selection: Some(lexical_leaf("lex", "document")),
                boosts: vec![dense_boost("b", &[])],
                ..Default::default()
            },
            "empty vector",
        ),
        (
            QueryRequest {
                k: 5,
                selection: Some(lexical_leaf("lex", "document")),
                boosts: vec![lexical_boost("b", "")],
                ..Default::default()
            },
            "empty text",
        ),
        (
            // Two boosts, no scorer: nothing defines combination.
            QueryRequest {
                k: 5,
                selection: Some(dense_leaf("vec", &qvec)),
                boosts: vec![lexical_boost("b1", "plain"), dense_boost("b2", &qvec)],
                ..Default::default()
            },
            "no composite scorer",
        ),
    ];
    for (req, needle) in cases {
        let err = query(&coordinator, req).await.unwrap_err();
        assert_eq!(err.code(), tonic::Code::InvalidArgument, "{needle}");
        assert!(
            err.message().contains(needle),
            "expected {needle:?} in: {}",
            err.message()
        );
    }
}

// ---------------------------------------------------------------------------
// Stored-value dimensions and projections on every shape (the
// FetchValues seam).

fn add_linear(column: &str, weight: f64) -> ScoreStage {
    ScoreStage {
        op: ScoreOp::AddLinear as i32,
        column: column.to_string(),
        weight,
        ..Default::default()
    }
}

fn stored_dim(id: &str, stage: ScoreStage) -> ScoreDimension {
    let mut d = dim(id, score_signal::Source::BoundedValue(stage));
    d.normalization = ScoreNormalization::None as i32;
    d
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stored_value_dimension_orders_by_the_column() {
    let (coordinator, _qvec, _handles) = start_cluster().await;
    let response = query(
        &coordinator,
        QueryRequest {
            k: 8,
            selection_k: 8,
            selection: Some(lexical_leaf("lex", "document")),
            scorer: Some(scorer(
                CompositeScoreOperation::WeightedSum,
                vec![stored_dim("recency", add_linear("year", 1.0))],
            )),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    assert_eq!(response.executed, "bm25_search+scorer:weighted_sum");
    // The dimension IS the year value: newest first, and provenance
    // reports the exact column value as the raw.
    let ids: Vec<u64> = response.hits.iter().map(|h| h.doc_id).collect();
    assert_eq!(ids, vec![7, 6, 5, 4, 3, 2, 1, 0]);
    for h in &response.hits {
        assert_eq!(h.dimensions[0].raw, Some(h.doc_id as f64));
        assert_eq!(h.score, h.doc_id as f32);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stored_value_missing_policies_on_a_partial_column() {
    let (coordinator, _qvec, _handles) = start_cluster().await;
    // "quality" lives on even docs only (value id + 1). Under the ZERO
    // default the odd docs score 0 and tie in id order behind every
    // even doc.
    let response = query(
        &coordinator,
        QueryRequest {
            k: 8,
            selection_k: 8,
            selection: Some(lexical_leaf("lex", "document")),
            scorer: Some(scorer(
                CompositeScoreOperation::WeightedSum,
                vec![stored_dim("q", add_linear("quality", 1.0))],
            )),
            ..Default::default()
        },
    )
    .await
    .unwrap();
    let ids: Vec<u64> = response.hits.iter().map(|h| h.doc_id).collect();
    assert_eq!(ids, vec![6, 4, 2, 0, 1, 3, 5, 7]);
    for h in &response.hits {
        assert_eq!(h.dimensions[0].raw.is_some(), h.doc_id % 2 == 0);
    }

    // ERROR: the caller asserted every candidate carries the value,
    // and doc 1 is the first (in pool order) that does not.
    let mut d = stored_dim("q", add_linear("quality", 1.0));
    d.missing = MissingScorePolicy::Error as i32;
    let err = query(
        &coordinator,
        QueryRequest {
            k: 8,
            selection_k: 8,
            selection: Some(lexical_leaf("lex", "document")),
            scorer: Some(scorer(CompositeScoreOperation::WeightedSum, vec![d])),
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    assert!(err.message().contains("dimension \"q\""));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn stored_value_typo_refuses_by_name() {
    let (coordinator, _qvec, _handles) = start_cluster().await;
    let err = query(
        &coordinator,
        QueryRequest {
            k: 8,
            selection: Some(lexical_leaf("lex", "document")),
            scorer: Some(scorer(
                CompositeScoreOperation::WeightedSum,
                vec![stored_dim("d", add_linear("yeer", 1.0))],
            )),
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("no shard has numeric column yeer"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn projections_ride_every_shape() {
    let (coordinator, qvec, _handles) = start_cluster().await;
    let projections = vec![
        NamedProjection {
            name: "year".into(),
            expression: "year".into(),
        },
        NamedProjection {
            name: "doubled".into(),
            expression: "year * 2".into(),
        },
    ];
    // Dense leaf, composite, and browse all carry the same projected
    // values per hit; the lexical route always did.
    let dense_req = QueryRequest {
        k: 8,
        selection: Some(dense_leaf("vec", &qvec)),
        projections: projections.clone(),
        ..Default::default()
    };
    let composite_req = QueryRequest {
        k: 8,
        selection_k: 8,
        selection: Some(rrf_union(&qvec, "zebra")),
        projections: projections.clone(),
        ..Default::default()
    };
    let browse_req = QueryRequest {
        k: 8,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Filter(FilterQuery {
                id: "f".into(),
                predicate: Some(pipestream_search::pb::filter_query::Predicate::Cel(
                    "year >= 0".into(),
                )),
            })),
        }),
        projections: projections.clone(),
        ..Default::default()
    };
    for req in [dense_req, composite_req, browse_req] {
        let response = query(&coordinator, req).await.unwrap();
        assert_eq!(response.hits.len(), N_DOCS);
        for h in &response.hits {
            assert_eq!(h.projected.len(), 2, "{}", response.executed);
            assert_eq!(
                h.projected[0].value,
                Some(pipestream_search::pb::projected_value::Value::IntValue(
                    h.doc_id as i64
                )),
                "{}",
                response.executed
            );
            assert_eq!(
                h.projected[1].value,
                Some(pipestream_search::pb::projected_value::Value::IntValue(
                    2 * h.doc_id as i64
                )),
                "{}",
                response.executed
            );
        }
    }

    // The typo rule holds on the fetched path too.
    let err = query(
        &coordinator,
        QueryRequest {
            k: 8,
            selection: Some(dense_leaf("vec", &qvec)),
            projections: vec![NamedProjection {
                name: "y".into(),
                expression: "yeer".into(),
            }],
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(err.code(), tonic::Code::InvalidArgument);
    assert!(err.message().contains("no shard has column"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn profile_reports_phases_without_altering_results() {
    let (coordinator, qvec, _handles) = start_cluster().await;
    let make = |profile: bool| QueryRequest {
        k: 8,
        selection_k: 8,
        selection: Some(rrf_union(&qvec, "zebra")),
        boosts: vec![lexical_boost("plain", "plain")],
        scorer: Some(scorer(
            CompositeScoreOperation::WeightedSum,
            vec![
                base_dim("base"),
                stored_dim("recency", add_linear("year", 1.0)),
            ],
        )),
        projections: vec![NamedProjection {
            name: "year".into(),
            expression: "year".into(),
        }],
        profile,
        ..Default::default()
    };
    let profiled = query(&coordinator, make(true)).await.unwrap();
    let plain = query(&coordinator, make(false)).await.unwrap();
    // Timings never alter results: the hits agree bitwise.
    assert!(plain.profile.is_none());
    assert_eq!(profiled.hits.len(), plain.hits.len());
    for (a, b) in profiled.hits.iter().zip(&plain.hits) {
        assert_eq!(a.doc_id, b.doc_id);
        assert_eq!(a.score.to_bits(), b.score.to_bits());
    }
    // Every exercised phase reports; the whole exceeds its parts'
    // largest piece.
    let p = profiled.profile.expect("profile requested");
    assert!(p.selection_ms > 0.0);
    assert!(p.boost_ms > 0.0);
    assert!(p.values_ms > 0.0);
    assert!(p.scorer_ms >= 0.0);
    assert!(p.projection_ms > 0.0);
    assert!(p.total_ms >= p.selection_ms);
}
