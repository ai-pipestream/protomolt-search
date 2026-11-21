//! Composite (`Decomposed` vs `Rrf`/`ScoreBlend`/`Cascade`) and `HybridSearch`
//! route-shape classification across unary and streaming coordinators.

use super::*;
use common::fit_calibration;
use pipestream_search::harness::{fake_ann, seeded_index};
use pipestream_search::pb::{
    selection_score_strategy as strat, BlendScore, CascadeScore, CompositeSearchStrategy,
    DecomposedScore, FusionMode, HybridLegOptions, HybridSearchRequest, LexicalQuery, RrfScore,
    SelectionOperator, SelectionScoreStrategy,
};

fn composite_request(
    vector: &[f32],
    k: u32,
    mode: DenseExecutionMode,
    strategy: strat::Strategy,
) -> QueryRequest {
    let operator = if matches!(strategy, strat::Strategy::Cascade(_)) {
        SelectionOperator::Unspecified
    } else {
        SelectionOperator::Or
    };
    let dense = SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: "dense".into(),
            query: Some(search_query::Query::Dense(DenseQuery {
                vector: vector.to_vec(),
                execution_mode: mode as i32,
                ..Default::default()
            })),
        })),
    };
    let lexical = SelectionQuery {
        node: Some(selection_query::Node::Search(SearchQuery {
            id: "lexical".into(),
            query: Some(search_query::Query::Lexical(LexicalQuery {
                text: "appellate review".into(),
                analysis: Some(body_spec()),
                ..Default::default()
            })),
        })),
    };
    QueryRequest {
        k,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Composite(CompositeSearchStrategy {
                operator: operator as i32,
                clauses: vec![dense, lexical],
                scoring: Some(SelectionScoreStrategy {
                    strategy: Some(strategy),
                }),
            })),
        }),
        ..Default::default()
    }
}

fn unary_staged_policy(live: &LiveIdentity, k: u32, depth: u32) -> DenseExecutionPolicy {
    let host = local_host_profile();
    let hosts = vec![host.clone(), host];
    DenseExecutionPolicy::from_document(DenseExecutionPolicyDocument {
        format_version: pipestream_search::dense_policy::FORMAT_VERSION,
        policy_id: "composite-staged-v2".into(),
        embedding_model: "test-embed".into(),
        provider_backend: live.provider_backend.clone(),
        scoring_fingerprint: live.scoring_fingerprint.clone(),
        corpus_generation: live.corpus_generation,
        corpus_rows: live.corpus_rows,
        deleted_docs: live.deleted_docs,
        corpus_fingerprint: live.corpus_fingerprint.clone(),
        dimensions: live.dimensions,
        measured_queries: 16,
        target_recall_ppm: 950_000,
        hardware_fingerprint: fleet_fingerprint(&hosts),
        node_count: 2,
        hosts,
        points: vec![DensePolicyPoint {
            k,
            filter_selectivity_ppm_min: 1_000_000,
            filter_selectivity_ppm_max: 1_000_000,
            candidates: depth,
            measured_recall_ppm: 992_000,
            mode: DenseExecutionMode::Balanced as i32,
            technique: DenseTechnique::BitplaneStaged as i32,
            selection_precision: DenseScorePrecision::Quantized as i32,
            output_precision: DenseScorePrecision::Quantized as i32,
            filter_strategy: DenseFilterStrategy::Default as i32,
            shape: DenseExecutionShape::Unary as i32,
            approximate_p50_us: 10,
            approximate_p99_us: 20,
            exhaustive_p50_us: 100,
            exhaustive_p99_us: 200,
            approximate_qps: 1000.0,
            exhaustive_qps: 100.0,
            ..Default::default()
        }],
    })
    .unwrap()
}

fn streaming_settle_policy(live: &LiveIdentity, k: u32, depth: u32) -> DenseExecutionPolicy {
    let host = local_host_profile();
    let hosts = vec![host.clone(), host];
    DenseExecutionPolicy::from_document(DenseExecutionPolicyDocument {
        format_version: pipestream_search::dense_policy::FORMAT_VERSION,
        policy_id: "hnsw-stream-settle-v2".into(),
        embedding_model: "test-embed".into(),
        provider_backend: live.provider_backend.clone(),
        scoring_fingerprint: live.scoring_fingerprint.clone(),
        corpus_generation: live.corpus_generation,
        corpus_rows: live.corpus_rows,
        deleted_docs: live.deleted_docs,
        corpus_fingerprint: live.corpus_fingerprint.clone(),
        dimensions: live.dimensions,
        measured_queries: 16,
        target_recall_ppm: 950_000,
        hardware_fingerprint: fleet_fingerprint(&hosts),
        node_count: 2,
        hosts,
        points: vec![DensePolicyPoint {
            k,
            filter_selectivity_ppm_min: 1_000_000,
            filter_selectivity_ppm_max: 1_000_000,
            candidates: depth,
            measured_recall_ppm: 990_000,
            finish_settle_us: Some(250),
            mode: DenseExecutionMode::Balanced as i32,
            technique: DenseTechnique::HnswWalk as i32,
            selection_precision: DenseScorePrecision::Fp32 as i32,
            output_precision: DenseScorePrecision::Fp32 as i32,
            filter_strategy: DenseFilterStrategy::Speed as i32,
            shape: DenseExecutionShape::Streaming as i32,
            approximate_p50_us: 10,
            approximate_p99_us: 20,
            exhaustive_p50_us: 100,
            exhaustive_p99_us: 200,
            approximate_qps: 1000.0,
            exhaustive_qps: 100.0,
            ..Default::default()
        }],
    })
    .unwrap()
}

async fn start_staged_composite_fleet() -> ShardFleet {
    const PER_SHARD: usize = 64;
    let corpus = unit_vectors(2 * PER_SHARD, DIM, 0x4A5E_8001);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for shard in 0..2 {
        let index = fake_ann::fake_staged_index(seeded_index(DIM, 4, &shift, &scale));
        let cfg = NodeConfig {
            slot_offset: (shard * PER_SHARD) as u64,
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            ..Default::default()
        };
        let (addr, handle) = start_node(index, cfg).await;
        let slice = &corpus[shard * PER_SHARD * DIM..(shard + 1) * PER_SHARD * DIM];
        populate_hnsw_shard(&addr, shard * PER_SHARD, slice).await;
        addrs.push(addr);
        handles.push(handle);
    }
    let coordinator = CoordinatorServiceImpl::new(addrs.clone())
        .with_bm25(
            Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            Default::default(),
        )
        .with_topology_generation(GENERATION);
    ShardFleet {
        coordinator,
        addrs,
        handles,
        _dirs: Vec::new(),
    }
}

async fn assert_unary_composite_uses_staged(
    coordinator: &CoordinatorServiceImpl,
    qvec: &[f32],
    strategy: strat::Strategy,
) {
    let bal = run_query(
        coordinator,
        composite_request(qvec, 5, DenseExecutionMode::Balanced, strategy.clone()),
    )
    .await;
    assert!(!bal.hits.is_empty());
    let bal_out = bal.dense_execution.unwrap();
    assert_eq!(bal_out.resolved_mode, DenseExecutionMode::Balanced as i32);
    assert_eq!(
        bal_out.techniques[0].technique,
        DenseTechnique::BitplaneStaged as i32
    );

    let auto = run_query(
        coordinator,
        composite_request(qvec, 5, DenseExecutionMode::Auto, strategy),
    )
    .await;
    assert!(!auto.hits.is_empty());
    let auto_out = auto.dense_execution.unwrap();
    assert_eq!(auto_out.resolved_mode, DenseExecutionMode::Balanced as i32);
    assert!(auto_out.policy_qualified);
    assert_eq!(auto_out.candidate_depth, 20);
    assert_eq!(
        auto_out.techniques[0].technique,
        DenseTechnique::BitplaneStaged as i32
    );
}

async fn assert_decomposed_avoids_staged(coordinator: &CoordinatorServiceImpl, qvec: &[f32]) {
    for mode in [DenseExecutionMode::Balanced, DenseExecutionMode::Auto] {
        let resp = run_query(
            coordinator,
            composite_request(
                qvec,
                5,
                mode,
                strat::Strategy::Decomposed(DecomposedScore::default()),
            ),
        )
        .await;
        assert!(!resp.hits.is_empty());
        let out = resp.dense_execution.unwrap();
        assert_eq!(out.resolved_mode, DenseExecutionMode::Exhaustive as i32);
        assert!(!out.policy_qualified);
        assert_eq!(
            out.techniques[0].technique,
            DenseTechnique::QuantizedScan as i32
        );
    }
}

async fn assert_hybrid_raw_shape_controls(coordinator: &CoordinatorServiceImpl, qvec: &[f32]) {
    let hybrid = |mode: FusionMode, staged: bool, settle: Option<u32>| HybridSearchRequest {
        text: "appellate review".into(),
        vector: qvec.to_vec(),
        k: 5,
        analysis: Some(body_spec()),
        legs: Some(HybridLegOptions {
            fusion_mode: mode as i32,
            ..Default::default()
        }),
        staged_selection: staged,
        finish_settle_us: settle,
        ..Default::default()
    };
    let dec_err = coordinator
        .hybrid_search(Request::new(hybrid(FusionMode::Decomposed, true, None)))
        .await
        .unwrap_err();
    assert_eq!(dec_err.code(), tonic::Code::FailedPrecondition);

    let rrf_ok = coordinator
        .hybrid_search(Request::new(hybrid(FusionMode::GlobalRank, true, None)))
        .await
        .unwrap()
        .into_inner();
    assert!(!rrf_ok.hits.is_empty());

    for mode in [FusionMode::GlobalRank, FusionMode::Decomposed] {
        let settle_err = coordinator
            .hybrid_search(Request::new(hybrid(mode, false, Some(250))))
            .await
            .unwrap_err();
        assert_eq!(settle_err.code(), tonic::Code::FailedPrecondition);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn composite_and_hybrid_routes_classify_decomposed_streaming_and_unary_leg_shapes() {
    let fleet = start_staged_composite_fleet().await;
    let live = fleet.coordinator.live_dense_identity().await.unwrap();
    let policy = unary_staged_policy(&live, 5, 20);
    let qvec = unit_vectors(1, DIM, 0x4A5E_8002);

    for stream in [false, true] {
        let coordinator = fleet
            .coordinator
            .clone()
            .with_stream_search(stream)
            .with_dense_execution_policy(policy.clone());
        assert_decomposed_avoids_staged(&coordinator, &qvec).await;
        for strategy in [
            strat::Strategy::Rrf(RrfScore::default()),
            strat::Strategy::ScoreBlend(BlendScore::default()),
            strat::Strategy::Cascade(CascadeScore {
                gate_id: "dense".into(),
            }),
        ] {
            assert_unary_composite_uses_staged(&coordinator, &qvec, strategy).await;
        }
        assert_hybrid_raw_shape_controls(&coordinator, &qvec).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn decomposed_composite_auto_falls_back_cleanly_when_streaming_point_sets_finish_settle() {
    let fleet = start_hnsw_fleet(2, 64, 0x4A5E_8101, false).await;
    let live = fleet.coordinator.live_dense_identity().await.unwrap();
    let policy = streaming_settle_policy(&live, 5, 24);
    let qvec = unit_vectors(1, DIM, 0x4A5E_8102);

    let single_stream = fleet
        .coordinator
        .clone()
        .with_stream_search(true)
        .with_dense_execution_policy(policy.clone());
    let single_out = run_query(
        &single_stream,
        dense_request(&qvec, 5, DenseExecutionMode::Auto),
    )
    .await
    .dense_execution
    .unwrap();
    assert!(single_out.policy_qualified);
    assert_eq!(single_out.candidate_depth, 24);
    assert_eq!(single_out.policy_point.unwrap().finish_settle_us, Some(250));

    for stream in [false, true] {
        let coordinator = fleet
            .coordinator
            .clone()
            .with_stream_search(stream)
            .with_dense_execution_policy(policy.clone());
        let before = dense_policy_drift_total();
        let resp = run_query(
            &coordinator,
            composite_request(
                &qvec,
                5,
                DenseExecutionMode::Auto,
                strat::Strategy::Decomposed(DecomposedScore::default()),
            ),
        )
        .await;
        assert!(!resp.hits.is_empty());
        let out = resp.dense_execution.unwrap();
        assert_eq!(out.resolved_mode, DenseExecutionMode::Exhaustive as i32);
        assert!(!out.policy_qualified);
        assert_eq!(out.techniques[0].technique, DenseTechnique::Fp32Scan as i32);
        assert!(
            out.fallback_reason.contains("candidate streaming"),
            "{}",
            out.fallback_reason
        );
        assert!(dense_policy_drift_total() > before, "stream={stream}");
    }
}
