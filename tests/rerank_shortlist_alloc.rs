//! Stage-2 shortlist payload bounding and NLP column materialization
//! pre-sizing verification across query routes and multi-entity projections.

mod common;

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use common::{fit_calibration, unit_vectors};
use pipestream_search::analyzer::{body_spec, TURBO_ANALYSIS_BACKEND};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::link::NodeLink;
use pipestream_search::node::{NodeConfig, NodeServiceImpl};
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    search_query, selection_query, selection_score_strategy, AddDocumentsRequest,
    AddVectorsRequest, CompositeSearchStrategy, DenseQuery, DenseScoreMode, DocLineage,
    HighlightSpec, LexicalQuery, MapFacetEntry, MapNumericEntry, QueryRequest, QueryResponse,
    RrfScore, SearchQuery, SelectionOperator, SelectionQuery, SelectionScoreStrategy,
};
use pipestream_search::turbo_bridge::rerank::{RerankBackend, RerankSpec};
use pipestream_search::turbo_bridge::{materialize_nlp_columns, NlpColumnSpec};
use tonic::Request;

const DIM: usize = 32;
const CORPUS_DOCS: usize = 48;
const LARGE_PAYLOAD_THRESHOLD: usize = 64 * 1024;

struct LargeAllocCounter;

static LARGE_ALLOCS: AtomicUsize = AtomicUsize::new(0);
static ALL_ALLOCS: AtomicUsize = AtomicUsize::new(0);

unsafe impl GlobalAlloc for LargeAllocCounter {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        ALL_ALLOCS.fetch_add(1, Ordering::Relaxed);
        if layout.size() >= LARGE_PAYLOAD_THRESHOLD {
            LARGE_ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        ALL_ALLOCS.fetch_add(1, Ordering::Relaxed);
        if new_size >= LARGE_PAYLOAD_THRESHOLD {
            LARGE_ALLOCS.fetch_add(1, Ordering::Relaxed);
        }
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

#[global_allocator]
static ALLOCATOR: LargeAllocCounter = LargeAllocCounter;

fn doc_text(id: usize) -> String {
    let lead = if id == 1 {
        "The appellate court affirmed constitutional jurisdiction over maritime appeals. "
    } else {
        "Industrial boiler valve pressure inspection log for regional plant operations. "
    };
    let filler = " Boiler maintenance record entry. ".repeat(2_400);
    format!("{lead}{filler}doc-{id}")
}

async fn build_two_shard_fixture() -> (CoordinatorServiceImpl, Vec<f32>) {
    let mut vecs = unit_vectors(CORPUS_DOCS, DIM, 0x2000_CAFE);
    let mut blended = vec![0.0_f32; DIM];
    let mut sq = 0.0_f32;
    for d in 0..DIM {
        let v = 0.85 * vecs[d] + 0.15 * vecs[DIM + d];
        blended[d] = v;
        sq += v * v;
    }
    let inv = 1.0 / sq.sqrt();
    for (dst, v) in vecs[DIM..2 * DIM].iter_mut().zip(blended) {
        *dst = v * inv;
    }
    let (shift, scale) = fit_calibration(DIM, 4, &vecs);
    let per_shard = CORPUS_DOCS / 2;
    let mut nodes = Vec::with_capacity(2);
    for shard in 0..2usize {
        let node = Arc::new(NodeServiceImpl::new(
            None,
            NodeConfig {
                slot_offset: (shard * per_shard) as u64,
                analysis_addr: Some(TURBO_ANALYSIS_BACKEND.to_string()),
                sentence_fields: vec!["body".into()],
                ..Default::default()
            },
        ));
        let mut link = NodeLink::local(Arc::clone(&node));
        link.configure_vector_backend(pipestream_search::harness::embedded_backend_request(
            DIM, 4, &shift, &scale,
        ))
        .await
        .unwrap();
        let docs: Vec<AddDocumentsRequest> = (0..per_shard)
            .map(|local| {
                let id = shard * per_shard + local;
                let text = doc_text(id);
                let len = text.len() as u32;
                AddDocumentsRequest {
                    text,
                    analysis: Some(body_spec()),
                    lineage: Some(DocLineage {
                        parent_id: 500 + id as u64,
                        group_id: 1,
                        span_start: 200,
                        span_end: 200 + len,
                    }),
                    ..Default::default()
                }
            })
            .collect();
        link.add_documents(tokio_stream::iter(docs)).await.unwrap();
        let start = shard * per_shard * DIM;
        link.add_vectors(tokio_stream::iter(vec![AddVectorsRequest {
            materialized_rows: Vec::new(),
            vectors: vecs[start..start + per_shard * DIM].to_vec(),
            dim: DIM as u32,
        }]))
        .await
        .unwrap();
        nodes.push(node);
    }
    let coord = CoordinatorServiceImpl::with_local_nodes(nodes)
        .with_bm25(Some(TURBO_ANALYSIS_BACKEND.to_string()), Default::default());
    (coord, vecs[..DIM].to_vec())
}

fn dense_query(q_vec: &[f32], fp32_rerank: bool, reranked: bool) -> QueryRequest {
    let pooled = fp32_rerank || reranked;
    QueryRequest {
        k: if pooled { 8 } else { CORPUS_DOCS as u32 },
        selection_k: if pooled { CORPUS_DOCS as u32 } else { 0 },
        highlight: reranked.then(HighlightSpec::default),
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Search(SearchQuery {
                id: "vec".into(),
                query: Some(search_query::Query::Dense(DenseQuery {
                    vector: q_vec.to_vec(),
                    score_mode: if fp32_rerank {
                        DenseScoreMode::Fp32Rerank as i32
                    } else {
                        0
                    },
                    query_text: if reranked {
                        "constitutional appellate jurisdiction maritime appeals".into()
                    } else {
                        String::new()
                    },
                    ..Default::default()
                })),
            })),
        }),
        ..Default::default()
    }
}

fn lexical_query(reranked: bool) -> QueryRequest {
    QueryRequest {
        k: if reranked { 8 } else { CORPUS_DOCS as u32 },
        selection_k: if reranked { CORPUS_DOCS as u32 } else { 0 },
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Search(SearchQuery {
                id: "lex".into(),
                query: Some(search_query::Query::Lexical(LexicalQuery {
                    text: "constitutional appellate maritime boiler".into(),
                    analysis: Some(body_spec()),
                    ..Default::default()
                })),
            })),
        }),
        ..Default::default()
    }
}

fn composite_query(q_vec: &[f32], highlight: bool) -> QueryRequest {
    QueryRequest {
        k: 8,
        selection_k: CORPUS_DOCS as u32,
        highlight: highlight.then(HighlightSpec::default),
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Composite(CompositeSearchStrategy {
                operator: SelectionOperator::Or as i32,
                clauses: vec![
                    SelectionQuery {
                        node: Some(selection_query::Node::Search(SearchQuery {
                            id: "vec".into(),
                            query: Some(search_query::Query::Dense(DenseQuery {
                                vector: q_vec.to_vec(),
                                query_text: if highlight {
                                    "constitutional appellate jurisdiction maritime appeals".into()
                                } else {
                                    String::new()
                                },
                                ..Default::default()
                            })),
                        })),
                    },
                    SelectionQuery {
                        node: Some(selection_query::Node::Search(SearchQuery {
                            id: "lex".into(),
                            query: Some(search_query::Query::Lexical(LexicalQuery {
                                text: "maintenance record".into(),
                                analysis: Some(body_spec()),
                                ..Default::default()
                            })),
                        })),
                    },
                ],
                scoring: Some(SelectionScoreStrategy {
                    strategy: Some(selection_score_strategy::Strategy::Rrf(RrfScore::default())),
                }),
            })),
        }),
        ..Default::default()
    }
}

async fn run_counted(coord: &CoordinatorServiceImpl, req: QueryRequest) -> (QueryResponse, usize) {
    let before = LARGE_ALLOCS.load(Ordering::Relaxed);
    let resp = SearchService::query(coord, Request::new(req))
        .await
        .unwrap()
        .into_inner();
    let after = LARGE_ALLOCS.load(Ordering::Relaxed);
    (resp, after.saturating_sub(before))
}

fn calibrated_spec(shortlist_size: usize) -> RerankSpec {
    let mut spec = RerankSpec::new(RerankBackend::DeterministicCalibrated {
        platt_slope: 2.2,
        platt_intercept: -0.4,
    });
    spec.shortlist_size = shortlist_size;
    spec.highlight_top_k = 4;
    spec.max_snippets_per_candidate = 2;
    spec.compose_parent_lineage_offsets = true;
    spec
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn non_reranked_and_reranked_queries_bound_large_payload_clones_and_preserve_order() {
    let (base_coord, q_vec) = build_two_shard_fixture().await;

    let (dense_plain, dense_plain_large) =
        run_counted(&base_coord, dense_query(&q_vec, false, false)).await;
    assert_eq!(dense_plain.executed, "search");
    assert_eq!(dense_plain.hits[0].doc_id, 0);
    assert_eq!(dense_plain_large, 0);

    let (hybrid_plain, hybrid_plain_large) =
        run_counted(&base_coord, composite_query(&q_vec, false)).await;
    assert_eq!(hybrid_plain.executed, "hybrid_search:global_rank");
    assert_eq!(hybrid_plain_large, 0);

    let (lex_plain, lex_plain_large) = run_counted(&base_coord, lexical_query(false)).await;
    assert_eq!(lex_plain.executed, "bm25_search");
    assert_eq!(lex_plain_large, 0);

    let short_coord = base_coord.clone().with_rerank_spec(calibrated_spec(4));
    let full_coord = base_coord.with_rerank_spec(calibrated_spec(CORPUS_DOCS));

    // Shards + fan-out allocate 3 * 48 = 144 buffers for `selection_k = 48`;
    // `shortlist_size = 4` keeps total large allocs < 4 * 48 (192) and saves
    // >= 2 * (48 - 4) large payload clones relative to `shortlist_size = 48`.
    let (dense_reranked, dense_short_large) =
        run_counted(&short_coord, dense_query(&q_vec, false, true)).await;
    let (_, dense_full_large) = run_counted(&full_coord, dense_query(&q_vec, false, true)).await;
    assert_eq!(dense_reranked.executed, "search:turbo_rerank");
    assert_eq!(dense_reranked.hits[0].doc_id, 1);
    assert!(!dense_reranked.hits[0].snippets.is_empty());
    assert!(
        dense_short_large < CORPUS_DOCS * 4,
        "unbounded RerankSources would exceed 4 * CORPUS_DOCS, got {dense_short_large}"
    );
    assert!(
        dense_full_large >= dense_short_large + 2 * (CORPUS_DOCS - 4),
        "shortlist=4 ({dense_short_large}) vs shortlist=48 ({dense_full_large})"
    );

    let (fp32_reranked, _) = run_counted(&short_coord, dense_query(&q_vec, true, true)).await;
    assert_eq!(fp32_reranked.executed, "search:turbo_rerank");
    assert_eq!(fp32_reranked.hits[0].doc_id, 1);

    let (hybrid_reranked, hybrid_short_large) =
        run_counted(&short_coord, composite_query(&q_vec, true)).await;
    let (_, hybrid_full_large) = run_counted(&full_coord, composite_query(&q_vec, true)).await;
    assert_eq!(hybrid_reranked.executed, "hybrid_search:turbo_rerank");
    assert_eq!(hybrid_reranked.hits[0].doc_id, 1);
    assert!(
        hybrid_full_large >= hybrid_short_large + 2 * (CORPUS_DOCS - 4),
        "composite shortlist=4 ({hybrid_short_large}) vs shortlist=48 ({hybrid_full_large})"
    );

    let (lex_reranked, _) = run_counted(&short_coord, lexical_query(true)).await;
    assert_eq!(lex_reranked.executed, "bm25_search:turbo_rerank");
    assert_eq!(lex_reranked.hits[0].doc_id, 1);
}

fn rich_nlp_projection() -> turbo_client::DocumentNlpProjection {
    let spans = turbo_client::ProjectedSpanSet::default();
    let affirmed_entities = (0..40)
        .map(|i| turbo_client::EntityPolarityHit {
            spans,
            surface: format!("Entity {i}"),
            kind: format!("kind_{}", i % 6),
            kb_id: Some(format!("Q{i}")),
            canonical_name: Some(format!("Canonical {i}")),
            score: 0.9,
            epistemic: "affirmed".into(),
            cue: None,
        })
        .collect();
    let money = (0..12)
        .map(|i| turbo_client::MoneyHit {
            spans,
            currency: format!("C{i:02}"),
            amount_f64: (i + 1) as f64 * 10.0,
            plain_amount: "10".into(),
            per: String::new(),
            ecb_converted: false,
        })
        .collect();
    let quantities = (0..12)
        .map(|i| turbo_client::QuantityHit {
            spans,
            unit: format!("u{i}"),
            value_f64: (i + 1) as f64,
            plain_value: "1".into(),
            is_percent: false,
        })
        .collect();
    let geo = (0..6)
        .map(|i| turbo_client::GeoHit {
            spans,
            record_id: format!("g{i}"),
            canonical_name: format!("City{i}"),
            country_code: format!("CC{i}"),
            latitude: 40.0 + i as f64,
            longitude: -70.0 - i as f64,
            spine_path: format!("CONT/CC{i}/REG{i}/City{i}"),
            confidence: 0.5 + i as f64 * 0.05,
        })
        .collect();
    turbo_client::DocumentNlpProjection {
        money,
        quantities,
        document_date: None,
        temporals: Vec::new(),
        geo,
        sentiment: turbo_client::SentimentSummary {
            label: Some("positive".into()),
            probability: Some(0.8),
        },
        quality: turbo_client::QualitySummary {
            assessed: false,
            worst_noise_score: 0.0,
            union_noise_chars: 0,
            artifact_count: 0,
            composite_score: None,
        },
        affirmed_entities,
        negated_entities: Vec::new(),
        sketch: turbo_client::DocumentSketch {
            word_count: 120,
            exact: 1,
            shape: 2,
            simhash: 0xDEAD_BEEF,
            minhash: (0..128).collect(),
        },
    }
}

#[test]
fn materialize_nlp_columns_presizes_vectors_and_preserves_existing_keys() {
    let spec = NlpColumnSpec {
        money_column: "money_map".into(),
        quantity_column: "qty_map".into(),
        geo_spine_column: "geo_spine".into(),
        affirmed_entity_column: "entities_aff".into(),
        minhash_column: "mh".into(),
        ..Default::default()
    };
    let nlp = rich_nlp_projection();
    let base_doc = AddDocumentsRequest {
        map_numerics: vec![MapNumericEntry {
            field: "money_map".into(),
            key: "C00".into(),
            value: 999.0,
        }],
        map_facets: vec![MapFacetEntry {
            field: "entities_aff".into(),
            key: "id:Q0".into(),
            value: "preset_kind".into(),
        }],
        ..Default::default()
    };
    let out = materialize_nlp_columns(base_doc, Some(&nlp), Some(&spec)).unwrap();
    assert_eq!(out.map_numerics.len(), 24);
    assert_eq!(
        out.map_numerics
            .iter()
            .find(|m| m.field == "money_map" && m.key == "C00")
            .unwrap()
            .value,
        999.0
    );
    assert_eq!(out.map_unsigned_integers.len(), 192);
    assert_eq!(
        out.map_facets
            .iter()
            .find(|m| m.field == "entities_aff" && m.key == "id:Q0")
            .unwrap()
            .value,
        "preset_kind"
    );
    assert_eq!(
        out.map_facets
            .iter()
            .filter(|m| m.field == "entities_aff")
            .count(),
        46
    );
}
