//! Capability-drift fallback (`BITPLANE_STAGED` on non-staging fleets) fails
//! closed to `EXHAUSTIVE` and increments `turbovec_dense_policy_drift_total`.

use super::*;
use common::fit_calibration;
use pipestream_search::harness::seeded_index;
use pipestream_search::pb::DensePrecisionRequest;

fn staged_policy_for(
    live: &LiveIdentity,
    k: u32,
    q_depth: u32,
    fp32_depth: u32,
) -> DenseExecutionPolicy {
    let point = |output: DenseScorePrecision, candidates: u32| DensePolicyPoint {
        k,
        filter_selectivity_ppm_min: 1_000_000,
        filter_selectivity_ppm_max: 1_000_000,
        candidates,
        measured_recall_ppm: 992_000,
        mode: DenseExecutionMode::Balanced as i32,
        technique: DenseTechnique::BitplaneStaged as i32,
        selection_precision: DenseScorePrecision::Quantized as i32,
        output_precision: output as i32,
        filter_strategy: DenseFilterStrategy::Default as i32,
        shape: DenseExecutionShape::Unary as i32,
        approximate_p50_us: 10,
        approximate_p99_us: 20,
        exhaustive_p50_us: 100,
        exhaustive_p99_us: 200,
        approximate_qps: 1000.0,
        exhaustive_qps: 100.0,
        ..Default::default()
    };
    let host = local_host_profile();
    let hosts = vec![host.clone(), host];
    DenseExecutionPolicy::from_document(DenseExecutionPolicyDocument {
        format_version: pipestream_search::dense_policy::FORMAT_VERSION,
        policy_id: "staged-drift-v2".into(),
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
        points: vec![
            point(DenseScorePrecision::Quantized, q_depth),
            point(DenseScorePrecision::Fp32, fp32_depth),
        ],
    })
    .unwrap()
}

fn auto_precision_request(
    vector: &[f32],
    k: u32,
    precision: DensePrecisionRequest,
) -> QueryRequest {
    QueryRequest {
        k,
        selection: Some(SelectionQuery {
            node: Some(selection_query::Node::Search(SearchQuery {
                id: "vec".into(),
                query: Some(search_query::Query::Dense(DenseQuery {
                    vector: vector.to_vec(),
                    execution_mode: DenseExecutionMode::Auto as i32,
                    precision: precision as i32,
                    ..Default::default()
                })),
            })),
        }),
        ..Default::default()
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn staged_policy_on_non_staging_fleet_fails_closed_to_exhaustive_and_counts_drift() {
    const PER_SHARD: usize = 512;
    let corpus = unit_vectors(2 * PER_SHARD, DIM, 0x4A5E_7001);
    let (shift, scale) = fit_calibration(DIM, 4, &corpus);
    let mut addrs = Vec::new();
    let mut handles = Vec::new();
    for shard in 0..2 {
        let index = seeded_index(DIM, 4, &shift, &scale);
        let cfg = NodeConfig {
            slot_offset: (shard * PER_SHARD) as u64,
            ..Default::default()
        };
        let (addr, handle) = start_node(index, cfg).await;
        let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
        let slice = &corpus[shard * PER_SHARD * DIM..(shard + 1) * PER_SHARD * DIM];
        client
            .add_vectors(tokio_stream::iter([AddVectorsRequest {
                materialized_rows: Vec::new(),
                vectors: slice.to_vec(),
                dim: DIM as u32,
            }]))
            .await
            .unwrap();
        addrs.push(addr);
        handles.push(handle);
    }

    let base = CoordinatorServiceImpl::new(addrs).with_topology_generation(GENERATION);
    let live = base.live_dense_identity().await.unwrap();
    let coordinator = base.with_dense_execution_policy(staged_policy_for(&live, 5, 20, 32));
    let qvec = unit_vectors(1, DIM, 0x4A5E_7002);

    for prec in [
        DensePrecisionRequest::Quantized,
        DensePrecisionRequest::Fp32,
    ] {
        let before = dense_policy_drift_total();
        let outcome = run_query(&coordinator, auto_precision_request(&qvec, 5, prec))
            .await
            .dense_execution
            .unwrap();
        assert_eq!(outcome.resolved_mode, DenseExecutionMode::Exhaustive as i32);
        assert_eq!(outcome.coverage, DenseCoverage::ExhaustiveCertified as i32);
        assert!(!outcome.policy_qualified);
        assert!(
            outcome
                .fallback_reason
                .contains("staged bit-plane selection"),
            "{}",
            outcome.fallback_reason
        );
        assert!(dense_policy_drift_total() > before, "prec={prec:?}");
    }
    for h in handles {
        h.abort();
    }
}
