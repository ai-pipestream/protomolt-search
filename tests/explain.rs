//! The explain tree (`docs/explain.md`, `src/explain.rs`): on the
//! public `Query` route, `explain` hands each hit the arithmetic that
//! produced its score. The contract under test: the root's value is
//! the served score, each node is the stated function of its children,
//! the hits and their order are bitwise the same with the flag off,
//! and a shape with no score to explain refuses by name.

mod common;

use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    score_signal, search_query, selection_query, selection_score_strategy, AddDocumentsRequest,
    AddVectorsRequest, BlendScore, BooleanQuery, BoostQuery, CascadeScore, CompositeScoreOperation,
    CompositeScorer, CompositeSearchStrategy, DecomposedScore, DenseQuery, DenseScoreMode,
    DocLineage, Explanation, FacetValue, FilterQuery, IntegerValue, LexicalQuery, QueryHit,
    QueryRequest, QueryResponse, QuerySort, QueryStreamRequest, RrfScore, ScoreDimension, ScoreOp,
    ScoreSignal, ScoreStage, SearchQuery, SelectionOperator, SelectionQuery,
    SelectionScoreStrategy, SetCalibrationRequest, TermPrefix,
};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::Request;

use common::{fit_calibration, start_empty_node, unit_vectors};
use pipestream_search::analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND};

const DIM: usize = 64;
const SHARD_DOCS: usize = 4;
const N_DOCS: usize = 2 * SHARD_DOCS;
const COURTS: [&str; 3] = ["ca9", "ca2", "scotus"];

/// The query adapter's corpus: even docs mention "zebra", every doc
/// carries `year = i`; vectors are the seeded unit corpus and the
/// query vector is doc 0's own.
async fn start_cluster() -> (
    CoordinatorServiceImpl,
    Vec<f32>,
    Vec<tokio::task::JoinHandle<Result<(), tonic::transport::Error>>>,
) {
    let analysis = NATIVE_ANALYSIS_BACKEND.to_string();
    let corpus = unit_vectors(N_DOCS, DIM, 0xC0FE_0001);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);
    let mut handles = Vec::new();
    let mut addrs = Vec::new();
    for shard in 0..2usize {
        let (addr, handle) = start_empty_node(NodeConfig {
            slot_offset: (shard * SHARD_DOCS) as u64,
            analysis_addr: Some(analysis.clone()),
            integer_fields: vec!["year".into()],
            facet_fields: vec!["court".into()],
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
                format!("zebra document {id} zebra")
            } else {
                format!("plain document {id}")
            };
            tx.send(AddDocumentsRequest {
                text,
                analysis: Some(body_spec()),
                integers: vec![IntegerValue {
                    field: "year".into(),
                    value: id as i64,
                }],
                facets: vec![FacetValue {
                    field: "court".into(),
                    value: COURTS[id % 3].into(),
                }],
                lineage: Some(DocLineage {
                    parent_id: (id / 2) as u64,
                    group_id: (id / 4) as u64,
                    span_start: 0,
                    span_end: 0,
                }),
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
        CoordinatorServiceImpl::new(addrs.clone()).with_bm25(Some(analysis), Default::default());
    (coordinator, corpus[..DIM].to_vec(), handles)
}

fn lexical_query(text: &str) -> LexicalQuery {
    LexicalQuery {
        text: text.to_string(),
        analysis: Some(body_spec()),
        ..Default::default()
    }
}

fn lexical_leaf(id: &str, query: LexicalQuery) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: id.to_string(),
            query: Some(search_query::Query::Lexical(query)),
        })),
    }
}

fn dense_leaf(id: &str, vector: &[f32], mode: DenseScoreMode) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: id.to_string(),
            query: Some(search_query::Query::Dense(DenseQuery {
                vector: vector.to_vec(),
                score_mode: mode as i32,
                ..Default::default()
            })),
        })),
    }
}

fn cel_filter(id: &str, cel: &str) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Filter(FilterQuery {
            id: id.to_string(),
            predicate: Some(pipestream_search::pb::filter_query::Predicate::Cel(
                cel.to_string(),
            )),
        })),
    }
}

fn composite(
    operator: SelectionOperator,
    clauses: Vec<SelectionQuery>,
    strategy: selection_score_strategy::Strategy,
) -> SelectionQuery {
    SelectionQuery {
        node: Some(selection_query::Node::Composite(CompositeSearchStrategy {
            operator: operator as i32,
            clauses,
            scoring: Some(SelectionScoreStrategy {
                strategy: Some(strategy),
            }),
        })),
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

/// The response with `explain` on and off; the hits must agree
/// bitwise once the trees are removed, and the explained one must
/// carry a tree on each hit whose root is the served score.
async fn explained(coordinator: &CoordinatorServiceImpl, req: QueryRequest) -> QueryResponse {
    let plain = query(coordinator, req.clone()).await.unwrap();
    let explained = query(
        coordinator,
        QueryRequest {
            explain: true,
            ..req
        },
    )
    .await
    .unwrap();
    assert!(!explained.hits.is_empty(), "the shape under test has hits");
    assert_eq!(explained.executed, plain.executed);
    let strip = |hits: &[QueryHit]| -> Vec<QueryHit> {
        hits.iter()
            .cloned()
            .map(|mut hit| {
                hit.explain = None;
                hit
            })
            .collect()
    };
    assert_eq!(
        strip(&explained.hits),
        strip(&plain.hits),
        "explain changed the page"
    );
    assert!(
        plain.hits.iter().all(|hit| hit.explain.is_none()),
        "no tree without the flag"
    );
    for hit in &explained.hits {
        let tree = hit.explain.as_ref().expect("a tree on each hit");
        assert_eq!(
            tree.value.to_bits(),
            f64::from(hit.score).to_bits(),
            "root is the served score for doc {}",
            hit.doc_id
        );
    }
    explained
}

fn tree(hit: &QueryHit) -> &Explanation {
    hit.explain.as_ref().expect("explained hit")
}

fn close(a: f64, b: f64) -> bool {
    (a - b).abs() <= 1e-6 * a.abs().max(b.abs()).max(1e-9)
}

/// Walk down single-child nodes to the one whose description starts
/// with `prefix`.
fn find<'a>(node: &'a Explanation, prefix: &str) -> &'a Explanation {
    let mut cursor = node;
    loop {
        if cursor.description.starts_with(prefix) {
            return cursor;
        }
        cursor = cursor
            .details
            .iter()
            .find(|n| n.description.starts_with(prefix) || !n.details.is_empty())
            .unwrap_or_else(|| panic!("no node starting with {prefix:?} under {:?}", node));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lexical_leaf_explains_each_term_from_its_bm25_inputs() {
    let (coordinator, _qvec, _handles) = start_cluster().await;
    let response = explained(
        &coordinator,
        QueryRequest {
            k: N_DOCS as u32,
            selection: Some(lexical_leaf("lex", lexical_query("zebra document"))),
            ..Default::default()
        },
    )
    .await;
    for hit in &response.hits {
        let root = tree(hit);
        assert!(root.description.starts_with("lexical leaf \"lex\""));
        let sum = find(root, "BM25 sum");
        assert!(close(sum.value, f64::from(hit.score)), "doc {}", hit.doc_id);
        let recomposed: f64 = sum.details.iter().map(|n| n.value).sum();
        assert!(
            close(recomposed, sum.value),
            "doc {}: {recomposed} vs {}",
            hit.doc_id,
            sum.value
        );
        let expected_terms = if hit.doc_id % 2 == 0 { 2 } else { 1 };
        assert_eq!(sum.details.len(), expected_terms, "doc {}", hit.doc_id);
        for term in &sum.details {
            let [tf_norm, idf, weight] = term.details.as_slice() else {
                panic!("three inputs under {:?}", term.description);
            };
            assert!(tf_norm.description.starts_with("tf_norm = "));
            assert!(idf.description.starts_with("idf = "));
            assert_eq!(weight.value, 1.0);
            assert_eq!(
                term.value.to_bits(),
                (weight.value * idf.value * tf_norm.value).to_bits(),
                "{}",
                term.description
            );
        }
        if hit.doc_id % 2 == 0 {
            let zebra = sum
                .details
                .iter()
                .find(|n| n.description.starts_with("term \"zebra\""))
                .expect("the zebra term");
            assert!(
                zebra.details[0].description.contains("tf=2"),
                "{}",
                zebra.details[0].description
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn score_stages_and_prefix_expansions_get_their_own_nodes() {
    let (coordinator, _qvec, _handles) = start_cluster().await;
    let response = explained(
        &coordinator,
        QueryRequest {
            k: N_DOCS as u32,
            selection: Some(lexical_leaf(
                "lex",
                LexicalQuery {
                    text: "document".into(),
                    analysis: Some(body_spec()),
                    score_stages: vec![ScoreStage {
                        op: ScoreOp::AddLinear as i32,
                        column: "year".into(),
                        weight: 0.125,
                        ..Default::default()
                    }],
                    prefixes: vec![TermPrefix {
                        prefix: "zeb".into(),
                        max_expansions: 0,
                    }],
                    ..Default::default()
                },
            )),
            ..Default::default()
        },
    )
    .await;
    for hit in &response.hits {
        let root = tree(hit);
        let stage = &root.details[0];
        assert!(
            stage
                .description
                .starts_with("score stage 0 on column year"),
            "{}",
            stage.description
        );
        let sum = &stage.details[0];
        assert!(sum.description.starts_with("BM25 sum"));
        let year = hit.doc_id as f64;
        assert!(
            close(stage.value, sum.value + 0.125 * year),
            "doc {}: {} vs {} + 0.125 * {year}",
            hit.doc_id,
            stage.value,
            sum.value
        );
        assert!(stage
            .description
            .contains(&format!("input {year} gives contribution")));
        if hit.doc_id % 2 == 0 {
            let group = sum
                .details
                .iter()
                .find(|n| n.description.starts_with("expansions of prefix \"zeb\""))
                .expect("the prefix group");
            assert_eq!(group.details.len(), 1);
            assert!(group.details[0].description.starts_with("term \"zebra\""));
            assert_eq!(group.value, group.details[0].value);
        } else {
            assert!(sum
                .details
                .iter()
                .all(|n| !n.description.starts_with("expansions")));
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dense_leaves_report_the_native_score_and_the_rerank_estimate() {
    let (coordinator, qvec, _handles) = start_cluster().await;
    let native = explained(
        &coordinator,
        QueryRequest {
            k: 4,
            selection: Some(dense_leaf("vec", &qvec, DenseScoreMode::Native)),
            ..Default::default()
        },
    )
    .await;
    for hit in &native.hits {
        let root = tree(hit);
        assert!(root
            .description
            .starts_with("dense leaf \"vec\": the provider's native"));
        assert!(root.details.is_empty());
    }
    let reranked = explained(
        &coordinator,
        QueryRequest {
            k: 4,
            selection_k: 8,
            selection: Some(dense_leaf("vec", &qvec, DenseScoreMode::Fp32Rerank)),
            ..Default::default()
        },
    )
    .await;
    let estimates: std::collections::HashMap<u64, f32> = native
        .hits
        .iter()
        .map(|hit| (hit.doc_id, hit.score))
        .collect();
    for hit in &reranked.hits {
        let root = tree(hit);
        assert!(root
            .description
            .starts_with("dense leaf \"vec\": exact FP32"));
        assert_eq!(root.details.len(), 1);
        if let Some(estimate) = estimates.get(&hit.doc_id) {
            assert_eq!(
                root.details[0].value,
                f64::from(*estimate),
                "doc {}",
                hit.doc_id
            );
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn composite_trees_recompose_their_fusion_arithmetic() {
    let (coordinator, qvec, _handles) = start_cluster().await;
    let leaves = || {
        vec![
            dense_leaf("vec", &qvec, DenseScoreMode::Native),
            lexical_leaf("lex", lexical_query("zebra")),
        ]
    };
    // RRF: the leg nodes sum to the fused score.
    let rrf = explained(
        &coordinator,
        QueryRequest {
            k: 6,
            selection: Some(composite(
                SelectionOperator::Or,
                leaves(),
                selection_score_strategy::Strategy::Rrf(RrfScore::default()),
            )),
            ..Default::default()
        },
    )
    .await;
    for hit in &rrf.hits {
        let root = tree(hit);
        assert!(root.description.starts_with("reciprocal rank fusion"));
        let sum: f64 = root.details.iter().map(|n| n.value).sum();
        assert!(
            close(sum, root.value),
            "doc {}: {sum} vs {}",
            hit.doc_id,
            root.value
        );
        assert_eq!(root.details.len(), hit.signals.len());
        for leg in &root.details {
            assert!(
                leg.description.contains("/ (rrf_k 60 + rank"),
                "{}",
                leg.description
            );
        }
    }
    // Blend (arithmetic, min-max): sum of the leg nodes over the total weight.
    let blend = explained(
        &coordinator,
        QueryRequest {
            k: 6,
            selection: Some(composite(
                SelectionOperator::Or,
                leaves(),
                selection_score_strategy::Strategy::ScoreBlend(BlendScore::default()),
            )),
            ..Default::default()
        },
    )
    .await;
    for hit in &blend.hits {
        let root = tree(hit);
        assert!(
            root.description.starts_with("score blend, arithmetic"),
            "{}",
            root.description
        );
        let sum: f64 = root.details.iter().map(|n| n.value).sum();
        assert!(
            close(sum / 2.0, root.value),
            "doc {}: {sum}/2 vs {}",
            hit.doc_id,
            root.value
        );
        for leg in &root.details {
            let normalized = &leg.details[0];
            assert!(normalized.description.starts_with("min-max normalization"));
            assert!((0.0..=1.0).contains(&normalized.value));
            assert_eq!(leg.value, 1.0 * normalized.value);
        }
    }
    // Decomposed: weighted raw sum.
    let decomposed = explained(
        &coordinator,
        QueryRequest {
            k: 6,
            selection: Some(composite(
                SelectionOperator::Or,
                leaves(),
                selection_score_strategy::Strategy::Decomposed(DecomposedScore::default()),
            )),
            ..Default::default()
        },
    )
    .await;
    for hit in &decomposed.hits {
        let root = tree(hit);
        assert!(root.description.starts_with("decomposed fusion"));
        let sum: f64 = root.details.iter().map(|n| n.value).sum();
        assert!(close(sum, root.value), "doc {}", hit.doc_id);
    }
    // Cascade: the served score is the rerank leg's, the gate a sibling.
    let cascade = explained(
        &coordinator,
        QueryRequest {
            k: 6,
            selection: Some(composite(
                SelectionOperator::Unspecified,
                leaves(),
                selection_score_strategy::Strategy::Cascade(CascadeScore {
                    gate_id: "vec".into(),
                }),
            )),
            ..Default::default()
        },
    )
    .await;
    for hit in &cascade.hits {
        let root = tree(hit);
        assert!(root.description.starts_with("cascade"));
        assert_eq!(root.details[0].value, root.value);
        assert!(root.details[0]
            .description
            .starts_with("leg \"lex\": phase-2"));
        assert!(root.details[1]
            .description
            .starts_with("leg \"vec\": phase-1"));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn boolean_roots_sum_their_clauses() {
    let (coordinator, qvec, _handles) = start_cluster().await;
    let response = explained(
        &coordinator,
        QueryRequest {
            k: N_DOCS as u32,
            selection: Some(SelectionQuery {
                node: Some(selection_query::Node::Boolean(BooleanQuery {
                    must: vec![cel_filter("recent", "year >= 2")],
                    should: vec![
                        lexical_leaf("lex", lexical_query("zebra")),
                        dense_leaf("vec", &qvec, DenseScoreMode::Native),
                    ],
                    ..Default::default()
                })),
            }),
            ..Default::default()
        },
    )
    .await;
    for hit in &response.hits {
        let root = tree(hit);
        assert!(root.description.starts_with("boolean root: sum"));
        assert_eq!(root.details.len(), hit.signals.len());
        let sum: f32 = root.details.iter().map(|n| n.value as f32).sum();
        assert!(close(f64::from(sum), root.value), "doc {}", hit.doc_id);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_scorer_and_a_window_boost_wrap_the_selection_tree() {
    let (coordinator, qvec, _handles) = start_cluster().await;
    let boost = || BoostQuery {
        query: Some(SearchQuery {
            id: "boost".into(),
            query: Some(search_query::Query::Dense(DenseQuery {
                vector: qvec.clone(),
                ..Default::default()
            })),
        }),
        ..Default::default()
    };
    // A window boost: the served score is the selection's, the boost
    // leaf says how the window was ordered.
    let boosted = explained(
        &coordinator,
        QueryRequest {
            k: 4,
            selection_k: 8,
            selection: Some(lexical_leaf("lex", lexical_query("document"))),
            boosts: vec![boost()],
            ..Default::default()
        },
    )
    .await;
    for hit in &boosted.hits {
        let root = tree(hit);
        assert!(root
            .description
            .starts_with("the selection score, unchanged by the boost"));
        assert!(root.details[0]
            .description
            .starts_with("lexical leaf \"lex\""));
        let signal = hit
            .signals
            .iter()
            .find(|s| s.id == "boost")
            .expect("boost signal");
        let leaf = &root.details[1];
        assert_eq!(leaf.value, f64::from(signal.score));
        let key = f64::from(hit.score) + f64::from(signal.score);
        assert!(
            leaf.description.contains(&format!("= {key}")),
            "{}",
            leaf.description
        );
    }
    // The composite scorer: the root is the operation over its
    // dimension nodes, the selection tree kept as provenance.
    let scored = explained(
        &coordinator,
        QueryRequest {
            k: 4,
            selection_k: 8,
            selection: Some(lexical_leaf("lex", lexical_query("document"))),
            boosts: vec![boost()],
            scorer: Some(CompositeScorer {
                operation: CompositeScoreOperation::WeightedSum as i32,
                dimensions: vec![
                    ScoreDimension {
                        id: "base".into(),
                        weight: None,
                        source: Some(ScoreSignal {
                            source: Some(score_signal::Source::Base(true)),
                        }),
                        normalization: 0,
                        missing: 0,
                    },
                    ScoreDimension {
                        id: "near".into(),
                        weight: Some(2.0),
                        source: Some(ScoreSignal {
                            source: Some(score_signal::Source::QueryRelevanceId("boost".into())),
                        }),
                        normalization: 0,
                        missing: 0,
                    },
                ],
            }),
            ..Default::default()
        },
    )
    .await;
    for hit in &scored.hits {
        let root = tree(hit);
        assert!(
            root.description.starts_with("composite scorer +scorer:"),
            "{}",
            root.description
        );
        let dims: Vec<&Explanation> = root
            .details
            .iter()
            .filter(|n| n.description.starts_with("dimension"))
            .collect();
        assert_eq!(dims.len(), 2);
        let sum: f64 = dims.iter().map(|n| n.value).sum();
        assert!(
            close(sum, root.value),
            "doc {}: {sum} vs {}",
            hit.doc_id,
            root.value
        );
        let provenance = root.details.last().unwrap();
        assert!(provenance
            .description
            .starts_with("the selection score this document"));
        assert!(provenance.details[0]
            .description
            .starts_with("lexical leaf \"lex\""));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shapes_without_a_score_refuse_explain_by_name() {
    let (coordinator, _qvec, _handles) = start_cluster().await;
    let browse = query(
        &coordinator,
        QueryRequest {
            k: 4,
            explain: true,
            selection: Some(cel_filter("recent", "year >= 2")),
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(browse.code(), tonic::Code::InvalidArgument);
    assert!(browse
        .message()
        .contains("explain needs a SCORED selection"));
    let sorted = query(
        &coordinator,
        QueryRequest {
            k: 4,
            explain: true,
            selection: Some(lexical_leaf("lex", lexical_query("document"))),
            sort: vec![QuerySort {
                column: "year".into(),
                descending: true,
            }],
            ..Default::default()
        },
    )
    .await
    .unwrap_err();
    assert_eq!(sorted.code(), tonic::Code::InvalidArgument);
    assert!(sorted.message().contains("computes no score to explain"));
    let streamed = coordinator
        .query_stream(Request::new(QueryStreamRequest {
            collection: String::new(),
            query: Some(QueryRequest {
                k: 4,
                explain: true,
                selection: Some(lexical_leaf("lex", lexical_query("document"))),
                ..Default::default()
            }),
            timeout_ms: 0,
        }))
        .await
        .err()
        .expect("the stream refuses explain before opening");
    assert_eq!(streamed.code(), tonic::Code::InvalidArgument);
    assert!(streamed.message().contains("unary Query route"));
}
