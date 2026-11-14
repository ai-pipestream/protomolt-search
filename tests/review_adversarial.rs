//! Adversarial and regression tests covering all findings from the PR #41 / #125
//! independent reviews:
//! - SPLADE Block-Max WAND unaligned-cursor upper bound (`splade.rs`)
//! - Per-document SPLADE scale preservation (`TVSPLD02`)
//! - Column CRC integrity on `pos_lengths` (kind 18) and `splade` (kind 19)
//! - Lattice slop overlap rejection & gap counting (`proximity.rs`)
//! - CEL identifier validation refusing predicate injection (`NlpColumnSpec`, `bind_one_pass_query`)
//! - Remote-shard `NodeService::ScoreSplade` over real TCP with `running_floor` propagation
//! - Single-pass Stage-2 dense & hybrid rerank + semantic snippets over TCP with zero `GetDocuments` RPCs
//! - `grpc-timeout` metadata propagation to `finish_deadline_us` (`DeadlineExceeded`)
//! - Automatic TOFU lexical handshake enforcement on `NodeServiceImpl`
//! - Multi-resolution Matryoshka active-tier indexing and higher-tier shortlist rescoring
//! - One-pass query single-analysis (`pre_analyzed_terms` with `analysis_addr = None`) and HNSW selectivity
//! - `VoronoiRouter` `TCENT001` routing & `evict_unhealthy_turbo_channel` pool generation bump

mod common;

use std::sync::Arc;

use common::{fit_calibration, unit_vectors};
use pipestream_search::analyzer::{
    body_spec, cased_body_spec, evict_unhealthy_turbo_channel, turbo_pool_generation,
    TURBO_ANALYSIS_BACKEND,
};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::link::NodeLink;
use pipestream_search::node::{NodeConfig, NodeServiceImpl};
use pipestream_search::pb::search_service_server::SearchService;
use pipestream_search::pb::{
    search_query, selection_query, selection_score_strategy, AddDocumentsRequest,
    AddVectorsRequest, CompositeSearchStrategy, DenseQuery, DocLineage, HighlightSpec,
    LexicalQuery, QueryRequest, RrfScore, SearchQuery, SelectionOperator, SelectionQuery,
    SelectionScoreStrategy, SetCalibrationRequest,
};
use pipestream_search::placement::VoronoiRouter;
use pipestream_search::postings::{AnalyzedDoc, AnalyzedField, Bm25Index, Bm25Reader, Bm25Store};
use pipestream_search::proximity::phrase_matches;
use pipestream_search::turbo_bridge::query_plan::{
    bind_one_pass_query, select_filtered_ann_strategy, FilteredAnnStrategy,
};
use pipestream_search::turbo_bridge::rerank::{RerankBackend, RerankSpec};
use pipestream_search::turbo_bridge::splade::SpladeFieldIndex;
use pipestream_search::turbo_bridge::{validate_cel_identifier, NlpColumnSpec};
use tokio::net::TcpListener;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Code, Request};
use turbo_client::OnePassQueryBuilder;

#[test]
fn block_skip_must_include_unaligned_query_terms() {
    for scale in [1.0_f32, 0.01] {
        let mut idx = SpladeFieldIndex::default();
        idx.insert_doc(0, scale.to_bits(), &[("b".into(), 10)]);
        for doc in 1..=128 {
            let mut terms = vec![("a".into(), 1)];
            if doc == 2 {
                terms.push(("b".into(), 10));
            }
            idx.insert_doc(doc, scale.to_bits(), &terms);
        }
        idx.insert_doc(129, scale.to_bits(), &[("a".into(), 100)]);
        let query = vec![("a".into(), 1.0), ("b".into(), 1.0)];
        let (hits, stats) = idx.score_top_k(&query, 0.0, 1, |d| d != 129);
        assert_eq!(
            hits.first().map(|h| h.0),
            Some(2),
            "scale={scale}, hits={hits:?}, stats={stats:?}; doc 2 scores 11*scale, doc 0 scores 10*scale"
        );
    }
}

#[test]
fn differing_document_scales_must_not_saturate_and_reverse_ranking() {
    let mut idx = SpladeFieldIndex::default();
    idx.insert_doc(0, 0.00001_f32.to_bits(), &[("x".into(), 65535)]);
    idx.insert_doc(1, 0.00002_f32.to_bits(), &[("x".into(), 65535)]);
    let (hits, _) = idx.score_top_k(&[("x".into(), 1.0)], 0.0, 1, |_| true);
    assert_eq!(
        hits[0].0, 1,
        "doc 1 has twice doc 0's calibrated impact; {hits:?}"
    );
}

#[test]
fn new_column_payloads_are_crc_covered_and_roundtrip() {
    let dir = std::env::temp_dir().join(format!(
        "review125-crc-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("columns.bm25");
    let mut store = Bm25Store::with_fields(&["body"]).with_positions(&["body"]);
    store.add_document(
        0,
        "joined word".into(),
        AnalyzedDoc {
            fields: vec![AnalyzedField {
                terms: vec![
                    ("joined".into(), 1, vec![(0, 6)]),
                    ("word".into(), 1, vec![(7, 11)]),
                ],
                length: 3,
                positions: Some(vec![vec![0], vec![2]]),
                pos_lengths: Some(vec![vec![2], vec![1]]),
                splade: vec![("neural".into(), 200)],
                splade_scale_bits: 0.01_f32.to_bits(),
                ..Default::default()
            }],
            ..Default::default()
        },
    );
    store.save(&path).unwrap();
    let reader = Bm25Reader::open(&path).unwrap();
    reader.verify_integrity().unwrap();
    assert_eq!(reader.posting_pos_lengths("joined", 0), Some(vec![2]));
    assert!(reader.has_splade());
    let original = std::fs::read(&path).unwrap();
    for (name, kind, payload_delta) in [
        ("pos_lengths:body", 18, 16usize),
        ("splade:body", 19, 8usize),
    ] {
        let entry = original
            .windows(name.len())
            .position(|w| w == name.as_bytes())
            .unwrap()
            + name.len();
        assert_eq!(original[entry], kind);
        let offset =
            u64::from_le_bytes(original[entry + 1..entry + 9].try_into().unwrap()) as usize;
        let mut corrupt = original.clone();
        corrupt[offset + payload_delta] ^= 1;
        let corrupt_path = dir.join(format!("bad-{kind}.bm25"));
        std::fs::write(&corrupt_path, corrupt).unwrap();
        if let Ok(r) = Bm25Reader::open(&corrupt_path) {
            assert!(
                r.verify_integrity().is_err(),
                "kind {kind} corruption missed"
            );
        }
    }
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn single_term_ties_and_admission_use_lower_live_doc_id() {
    let mut idx = SpladeFieldIndex::default();
    for d in 0..300 {
        idx.insert_doc(d, 1.0_f32.to_bits(), &[("x".into(), 10)]);
    }
    let (hits, _) = idx.score_top_k(&[("x".into(), 1.0)], 0.0, 3, |d| d >= 2);
    assert_eq!(hits, vec![(2, 10.0), (3, 10.0), (4, 10.0)]);
}

#[test]
fn lattice_slop_rejects_overlap_and_counts_only_gaps() {
    let mut store = Bm25Store::with_fields(&["body"]).with_positions(&["body"]);
    for (doc, court_pos) in [3, 4, 2].into_iter().enumerate() {
        store.add_document(
            doc as u32,
            "NYC court".into(),
            AnalyzedDoc {
                fields: vec![AnalyzedField {
                    terms: vec![
                        ("nyc".into(), 1, vec![(0, 3)]),
                        ("court".into(), 1, vec![(4, 9)]),
                    ],
                    length: 4,
                    positions: Some(vec![vec![0], vec![court_pos]]),
                    pos_lengths: Some(vec![vec![3], vec![1]]),
                    ..Default::default()
                }],
                ..Default::default()
            },
        );
    }
    let terms = vec!["nyc".into(), "court".into()];
    assert!(phrase_matches(&store.field(0), &terms, &[0, 1], 0, 0));
    assert!(!phrase_matches(&store.field(0), &terms, &[0, 1], 0, 1));
    assert!(phrase_matches(&store.field(0), &terms, &[0, 1], 1, 1));
    assert!(!phrase_matches(&store.field(0), &terms, &[0, 1], 100, 2));
}

#[test]
fn cel_column_name_injection_is_rejected_at_config_and_query_plan() {
    assert!(validate_cel_identifier("geo_spine_column", "geo_spine").is_ok());
    assert!(
        validate_cel_identifier("geo_spine_column", "geo_spine || \"injected\" in other_geo")
            .is_err()
    );
    assert!(validate_cel_identifier("geo_spine_column", "true").is_err());
    assert!(validate_cel_identifier("geo_spine_column", "1bad").is_err());

    let bad_spec = NlpColumnSpec {
        geo_spine_column: "geo_spine || true || x".to_string(),
        ..Default::default()
    };
    assert!(
        bad_spec.validate().is_err(),
        "injected column identifier must be rejected by NlpColumnSpec::validate"
    );

    let analyzer = OnePassQueryBuilder::new().build().unwrap();
    let plan = analyzer.analyze("batteries in Berlin under $500").unwrap();
    let err = bind_one_pass_query(&plan, &bad_spec, None).unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
}

async fn spawn_tcp_node(node: Arc<NodeServiceImpl>) -> (String, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://{}", listener.local_addr().unwrap());
    let server =
        NodeServiceImpl::into_server((*node).clone(), pipestream_search::MAX_MESSAGE_BYTES);
    let handle = tokio::spawn(async move {
        let _ = Server::builder()
            .add_service(server)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await;
    });
    (addr, handle)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn remote_shard_splade_over_tcp_propagates_running_floor_and_filter() {
    let dir = std::env::temp_dir().join(format!(
        "review125-remote-splade-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();

    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for (shard_idx, (offset, scale, q_impact)) in
        [(0u64, 0.01_f32, 150u16), (100u64, 0.02_f32, 200u16)]
            .into_iter()
            .enumerate()
    {
        let idx_path = dir.join(format!("shard_{shard_idx}.idx"));
        let bm25_path = pipestream_search::node::bm25_sidecar_path(&idx_path);
        let mut store = Bm25Store::with_fields(&["body"]);
        store.add_document(
            0,
            format!("shard {shard_idx} doc"),
            AnalyzedDoc {
                fields: vec![AnalyzedField {
                    terms: vec![("shard".into(), 1, vec![(0, 5)])],
                    length: 1,
                    splade: vec![("battery".into(), q_impact)],
                    splade_scale_bits: scale.to_bits(),
                    ..Default::default()
                }],
                ..Default::default()
            },
        );
        store.save(&bm25_path).unwrap();
        let node = Arc::new(
            NodeServiceImpl::open(
                NodeConfig {
                    slot_offset: offset,
                    index_path: Some(idx_path),
                    ..Default::default()
                },
                None,
                true,
            )
            .unwrap(),
        );
        let (addr, h) = spawn_tcp_node(node).await;
        addrs.push(addr);
        handles.push(h);
    }

    let coord = CoordinatorServiceImpl::new(addrs);
    let hits = coord
        .score_splade("body", &[("battery".to_string(), 1.0)], 0.0, 2, "")
        .await
        .unwrap();
    assert_eq!(hits.len(), 2);
    assert_eq!(
        hits[0].0, 100,
        "remote shard 1 has higher calibrated SPLADE score (4.0 vs 1.5)"
    );
    assert!((hits[0].1 - 4.0).abs() < 1e-4);
    assert_eq!(hits[1].0, 0);
    assert!((hits[1].1 - 1.5).abs() < 1e-4);

    for h in handles {
        h.abort();
    }
    let _ = std::fs::remove_dir_all(dir);
}

fn get_documents_rpc_count() -> u64 {
    let snap = pipestream_search::metrics::snapshot("test", &[]);
    snap.samples
        .iter()
        .filter(|s| {
            s.name == "rpc_requests_total"
                && s.labels
                    .iter()
                    .any(|l| l.name == "method" && l.value.contains("GetDocuments"))
        })
        .map(|s| s.value as u64)
        .sum()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn single_pass_dense_and_hybrid_rerank_emit_snippets_with_zero_get_documents_rpcs_and_honor_grpc_timeout(
) {
    const DIM: usize = 16;
    let node = Arc::new(NodeServiceImpl::new(
        None,
        NodeConfig {
            analysis_addr: Some(TURBO_ANALYSIS_BACKEND.to_string()),
            sentence_fields: vec!["body".to_string()],
            ..Default::default()
        },
    ));
    let (addr, handle) = spawn_tcp_node(Arc::clone(&node)).await;

    let mut link = NodeLink::local(node);
    let docs = [
        (
            "Unrelated preamble about ocean currents and maritime shipping.",
            0u32,
        ),
        (
            "First sentence setup. Solid state lithium battery cells deliver superior energy density.",
            200u32,
        ),
    ];
    let vecs = unit_vectors(2, DIM, 77);
    let (shift, scale) = fit_calibration(DIM, 4, &vecs);
    link.set_calibration(SetCalibrationRequest {
        dim: DIM as u32,
        bit_width: 4,
        shift,
        scale,
    })
    .await
    .unwrap();
    link.add_documents(tokio_stream::iter(docs.into_iter().map(
        |(text, span_start)| AddDocumentsRequest {
            text: text.to_string(),
            analysis: Some(body_spec()),
            sentence_fields: vec!["body".to_string()],
            lineage: (span_start > 0).then(|| DocLineage {
                parent_id: 77,
                group_id: 1,
                span_start,
                span_end: span_start + text.len() as u32,
            }),
            ..Default::default()
        },
    )))
    .await
    .unwrap();
    link.add_vectors(tokio_stream::iter(vec![AddVectorsRequest {
        vectors: vecs.clone(),
        dim: DIM as u32,
    }]))
    .await
    .unwrap();

    let mut rerank_spec = RerankSpec::new(RerankBackend::DeterministicCalibrated {
        platt_slope: 2.0,
        platt_intercept: -0.5,
    });
    rerank_spec.compose_parent_lineage_offsets = true;
    rerank_spec.query_text_override = Some("lithium battery energy density".to_string());

    let coord = CoordinatorServiceImpl::new(vec![addr])
        .with_bm25(Some(TURBO_ANALYSIS_BACKEND.to_string()), Default::default())
        .with_rerank_spec(rerank_spec);

    let before_get_docs = get_documents_rpc_count();

    // 1. Pure DenseQuery over TCP: inline RerankCandidatePayload produces calibrated scores + snippets
    //    with ZERO GetDocuments RPCs!
    let dense_resp = SearchService::query(
        &coord,
        Request::new(QueryRequest {
            selection: Some(SelectionQuery {
                node: Some(selection_query::Node::Search(SearchQuery {
                    id: "v".to_string(),
                    query: Some(search_query::Query::Dense(DenseQuery {
                        vector: vecs[0..DIM].to_vec(),
                        ..Default::default()
                    })),
                })),
            }),
            k: 2,
            highlight: Some(HighlightSpec {
                fields: vec!["body".to_string()],
                max_snippets: 2,
                ..Default::default()
            }),
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(dense_resp.hits.len(), 2);
    assert_eq!(dense_resp.hits[0].doc_id, 1);
    assert!(!dense_resp.hits[0].snippets.is_empty());
    assert!(
        dense_resp.hits[0].snippets[0].start >= 200,
        "parent lineage offset must be composed onto inline rerank snippet"
    );

    // 2. Hybrid Composite RRF over TCP: also zero GetDocuments RPCs!
    let hybrid_resp = SearchService::query(
        &coord,
        Request::new(QueryRequest {
            selection: Some(SelectionQuery {
                node: Some(selection_query::Node::Composite(CompositeSearchStrategy {
                    operator: SelectionOperator::Or as i32,
                    scoring: Some(SelectionScoreStrategy {
                        strategy: Some(
                            selection_score_strategy::Strategy::Rrf(RrfScore::default()),
                        ),
                    }),
                    clauses: vec![
                        SelectionQuery {
                            node: Some(selection_query::Node::Search(SearchQuery {
                                id: "v".to_string(),
                                query: Some(search_query::Query::Dense(DenseQuery {
                                    vector: vecs[0..DIM].to_vec(),
                                    ..Default::default()
                                })),
                            })),
                        },
                        SelectionQuery {
                            node: Some(selection_query::Node::Search(SearchQuery {
                                id: "l".to_string(),
                                query: Some(search_query::Query::Lexical(LexicalQuery {
                                    text: "lithium battery".to_string(),
                                    analysis: Some(body_spec()),
                                    ..Default::default()
                                })),
                            })),
                        },
                    ],
                })),
            }),
            k: 2,
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(hybrid_resp.hits[0].doc_id, 1);
    assert!(!hybrid_resp.hits[0].snippets.is_empty());
    assert_eq!(
        get_documents_rpc_count() - before_get_docs,
        0,
        "Stage-2 rerank over TCP must perform ZERO second-pass GetDocuments RPCs"
    );

    // 3. Expired `grpc-timeout: 1n` metadata propagates to `finish_deadline_us` and aborts with DeadlineExceeded!
    let mut expired_dense = Request::new(QueryRequest {
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Search(SearchQuery {
                id: "v".to_string(),
                query: Some(search_query::Query::Dense(DenseQuery {
                    vector: vecs[0..DIM].to_vec(),
                    ..Default::default()
                })),
            })),
        }),
        k: 2,
        ..Default::default()
    });
    expired_dense
        .metadata_mut()
        .insert("grpc-timeout", "1n".parse().unwrap());
    let err_dense = SearchService::query(&coord, expired_dense)
        .await
        .unwrap_err();
    assert_eq!(err_dense.code(), Code::DeadlineExceeded);

    let mut expired_req = Request::new(QueryRequest {
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Search(SearchQuery {
                id: "l".to_string(),
                query: Some(search_query::Query::Lexical(LexicalQuery {
                    text: "lithium battery".to_string(),
                    analysis: Some(body_spec()),
                    ..Default::default()
                })),
            })),
        }),
        k: 2,
        ..Default::default()
    });
    expired_req
        .metadata_mut()
        .insert("grpc-timeout", "1n".parse().unwrap());
    let err = SearchService::query(&coord, expired_req).await.unwrap_err();
    assert_eq!(err.code(), Code::DeadlineExceeded);

    handle.abort();
}

#[tokio::test]
async fn default_tofu_lexical_handshake_refuses_drifted_spec_without_explicit_builder() {
    let node = Arc::new(NodeServiceImpl::new(
        None,
        NodeConfig {
            analysis_addr: Some(TURBO_ANALYSIS_BACKEND.to_string()),
            ..Default::default()
        },
    ));
    let mut link = NodeLink::local(node);
    link.add_documents(tokio_stream::iter(vec![AddDocumentsRequest {
        text: "First document pins default TOFU lexical fingerprint".to_string(),
        analysis: Some(body_spec()),
        ..Default::default()
    }]))
    .await
    .unwrap();

    let err = link
        .add_documents(tokio_stream::iter(vec![AddDocumentsRequest {
            text: "Second document with incompatible cased spec must fail".to_string(),
            analysis: Some(cased_body_spec()),
            ..Default::default()
        }]))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::FailedPrecondition);
}

#[tokio::test]
async fn matryoshka_active_tier_indexes_compact_prefix_and_rescores_higher_tier_shortlist() {
    let node = Arc::new(NodeServiceImpl::new(
        None,
        NodeConfig {
            matryoshka_prefix_dims: vec![8, 16],
            matryoshka_active_dim: Some(8),
            ..Default::default()
        },
    ));
    let mut link = NodeLink::local(Arc::clone(&node));

    // Doc 0 wins on the first 8 coordinates (`1.0` vs `0.6`),
    // whereas Doc 1 wins on the full 16-dim vector (`[0.6, 0.., 0.8, 0..]`)
    // against query `[0.5, 0.., 0.8660254, 0..]`.
    let mut raw_vectors = vec![0.0_f32; 32];
    raw_vectors[0] = 1.0; // doc 0: dim 0 = 1.0
    raw_vectors[16] = 0.6; // doc 1: dim 0 = 0.6
    raw_vectors[24] = 0.8; // doc 1: dim 8 = 0.8
    let cal_sample = unit_vectors(8, 8, 99);
    let (shift, scale) = fit_calibration(8, 4, &cal_sample);
    link.set_calibration(SetCalibrationRequest {
        dim: 8,
        bit_width: 4,
        shift,
        scale,
    })
    .await
    .unwrap();
    link.add_vectors(tokio_stream::iter(vec![AddVectorsRequest {
        vectors: raw_vectors,
        dim: 16,
    }]))
    .await
    .unwrap();

    assert_eq!(node.multi_res_row(0).unwrap().prefixes[0].0, 8);
    assert_eq!(node.multi_res_row(0).unwrap().full.len(), 16);

    let coord = CoordinatorServiceImpl::with_local_nodes(vec![node]);
    let mut full_query = vec![0.0_f32; 16];
    full_query[0] = 0.5;
    full_query[8] = 0.8660254;
    let rescored = coord
        .rescore_matryoshka_shortlist(&[(0, 0.5), (1, 0.4)], &full_query, 16)
        .unwrap();
    assert_eq!(rescored.len(), 2);
    assert_eq!(
        rescored[0].0, 1,
        "higher Matryoshka tier (dim=16) must promote doc 1 above doc 0: {rescored:?}"
    );
}

#[tokio::test]
async fn one_pass_query_executes_lexical_with_zero_reanalysis_and_selects_hnsw_strategy() {
    let nlp_spec = NlpColumnSpec {
        money_column: "money".to_string(),
        ..Default::default()
    };

    let node = Arc::new(
        NodeServiceImpl::new(
            None,
            NodeConfig {
                analysis_addr: Some(TURBO_ANALYSIS_BACKEND.to_string()),
                map_numeric_fields: vec!["money".to_string()],
                ..Default::default()
            },
        )
        .with_nlp_columns(nlp_spec.clone()),
    );
    let mut link = NodeLink::local(Arc::clone(&node));
    link.add_documents(tokio_stream::iter(vec![
        AddDocumentsRequest {
            text: "Lithium battery module priced at $250.".to_string(),
            analysis: Some(body_spec()),
            ..Default::default()
        },
        AddDocumentsRequest {
            text: "Lithium battery module priced at $900.".to_string(),
            analysis: Some(body_spec()),
            ..Default::default()
        },
    ]))
    .await
    .unwrap();

    // Coordinator has `analysis_addr = None`! If `query.rs` attempted a second lexical analysis pass,
    // `bm25_search` would fail with `FailedPrecondition` ("Coordinator has no analysis_addr configured").
    // Passing solely via `pre_analyzed_terms` from `OnePassQueryAnalyzer` proves single-pass execution!
    let analyzer = OnePassQueryBuilder::new().build().unwrap();
    let coord = CoordinatorServiceImpl::with_local_nodes(vec![node])
        .with_bm25(None, Default::default())
        .with_one_pass_query(analyzer, nlp_spec);

    let resp = SearchService::query(
        &coord,
        Request::new(QueryRequest {
            selection: Some(SelectionQuery {
                node: Some(selection_query::Node::Search(SearchQuery {
                    id: "q".to_string(),
                    query: Some(search_query::Query::Lexical(LexicalQuery {
                        text: "lithium battery under $500".to_string(),
                        analysis: Some(body_spec()),
                        ..Default::default()
                    })),
                })),
            }),
            k: 5,
            ..Default::default()
        }),
    )
    .await
    .unwrap()
    .into_inner();
    assert_eq!(resp.hits.len(), 1);
    assert_eq!(resp.hits[0].doc_id, 0);

    // Also verify selectivity strategy escalation from high selectivity -> escalated ef -> exact scan:
    assert!(matches!(
        select_filtered_ann_strategy(64, 10, 50_000, 0.50),
        FilteredAnnStrategy::Graph { ef_search } if ef_search > 64
    ));
    assert_eq!(
        select_filtered_ann_strategy(64, 10, 50_000, 0.001),
        FilteredAnnStrategy::PrefilteredExact
    );
}

#[test]
fn voronoi_router_routes_over_tcent_bundle_and_channel_eviction_increments_generation() {
    let mut tcent = Vec::new();
    tcent.extend_from_slice(b"TCENT001");
    tcent.extend_from_slice(&4u32.to_le_bytes()); // dim = 4
    tcent.extend_from_slice(&2u32.to_le_bytes()); // num_centroids = 2
    tcent.push(1u8); // norm_code = 1 (L2-normalized)
    tcent.extend_from_slice(&0.5_f64.to_le_bytes()); // temperature
    let push_str = |buf: &mut Vec<u8>, s: &str| {
        buf.extend_from_slice(&(s.len() as u32).to_le_bytes());
        buf.extend_from_slice(s.as_bytes());
    };
    push_str(&mut tcent, "bge-small");
    push_str(&mut tcent, "rev1");
    push_str(&mut tcent, &"aa".repeat(32));
    push_str(&mut tcent, "shard-Voronoi");
    push_str(&mut tcent, "shard-0");
    push_str(&mut tcent, "shard-1");
    for v in [1.0_f32, 0.0, 0.0, 0.0, 0.0, 1.0, 0.0, 0.0] {
        tcent.extend_from_slice(&v.to_le_bytes());
    }

    let router = VoronoiRouter::from_tcent_bytes(&tcent, "voronoi-test").unwrap();
    assert_eq!(router.num_partitions(), 2);
    assert_eq!(router.route_vector(&[0.1, 0.95, 0.0, 0.0]).unwrap(), 1);
    let probes = router.route_top_probes(&[0.9, 0.2, 0.0, 0.0], 2).unwrap();
    assert_eq!(probes[0].0, 0);
    assert!(probes[0].1 > probes[1].1);

    let before_gen = turbo_pool_generation();
    evict_unhealthy_turbo_channel("http://127.0.0.1:59999");
    assert!(turbo_pool_generation() > before_gen);
}
