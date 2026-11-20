//! Dense execution policy qualification under document views.

use super::*;

fn doc_view_policy_point(band: u32, candidates: u32, recall: u32) -> crate::pb::DensePolicyPoint {
    crate::pb::DensePolicyPoint {
        k: 1,
        filter_selectivity_ppm_min: band,
        filter_selectivity_ppm_max: band,
        candidates,
        measured_recall_ppm: recall,
        mode: DenseExecutionMode::Balanced as i32,
        technique: crate::pb::DenseTechnique::HnswWalk as i32,
        selection_precision: crate::pb::DenseScorePrecision::Quantized as i32,
        output_precision: crate::pb::DenseScorePrecision::Quantized as i32,
        filter_strategy: crate::pb::DenseFilterStrategy::Speed as i32,
        shape: crate::pb::DenseExecutionShape::Unary as i32,
        approximate_p50_us: 10,
        approximate_p99_us: 20,
        exhaustive_p50_us: 100,
        exhaustive_p99_us: 200,
        approximate_qps: 1000.0,
        exhaustive_qps: 100.0,
        ..Default::default()
    }
}

fn build_doc_view_policy(
    fingerprint: String,
    live: &crate::dense_policy::LiveIdentity,
) -> crate::dense_policy::DenseExecutionPolicy {
    let host = crate::dense_policy::local_host_profile();
    crate::dense_policy::DenseExecutionPolicy::from_document(
        crate::pb::DenseExecutionPolicyDocument {
            format_version: crate::dense_policy::FORMAT_VERSION,
            policy_id: "document-view".into(),
            embedding_model: "test".into(),
            provider_backend: "fake-ann".into(),
            scoring_fingerprint: fingerprint,
            corpus_generation: live.corpus_generation,
            corpus_rows: live.corpus_rows,
            deleted_docs: live.deleted_docs,
            corpus_fingerprint: live.corpus_fingerprint.clone(),
            dimensions: 16,
            measured_queries: 20,
            target_recall_ppm: 950_000,
            hardware_fingerprint: crate::dense_policy::fleet_fingerprint(&[host]),
            node_count: 1,
            hosts: Vec::new(),
            points: vec![
                doc_view_policy_point(1_000_000, 1, 990_000),
                doc_view_policy_point(500_000, 2, 980_000),
            ],
        },
    )
    .unwrap()
}

#[tokio::test]
async fn auto_ann_qualifies_the_document_view_instead_of_the_unfiltered_point() {
    let node = node(0, false);
    let fingerprint = {
        let mut guard = crate::node::write_shard(&node.state);
        let index = guard.index.take().unwrap();
        let fingerprint = crate::harness::fake_ann::fingerprint_of(&index);
        guard.index = Some(crate::harness::fake_ann::fake_ann_index(index));
        fingerprint
    };
    let probe = CoordinatorServiceImpl::with_local_nodes(vec![node]);
    let live = probe.live_dense_identity().await.unwrap();
    let owner = probe.with_dense_execution_policy(build_doc_view_policy(fingerprint, &live));
    let key = DenseRequestKey {
        k: 1,
        candidate_depth: 0,
        route_mode: crate::coordinator::DenseRouteMode::SingleLeaf,
        filters: None,
    };
    let unrestricted = owner
        .resolve_dense_execution(auto(), 16, key)
        .await
        .unwrap()
        .outcome;
    assert_eq!(unrestricted.candidate_depth, 1);
    assert!(unrestricted.policy_qualified);
    let reader = owner
        .for_access(Some(&access("semantic", FieldAction::Use)), "bm25_search")
        .unwrap()
        .for_vector_field("semantic")
        .unwrap();
    let restricted = reader
        .resolve_dense_execution(auto(), 16, key)
        .await
        .unwrap()
        .outcome;
    assert_eq!(
        restricted.resolved_mode,
        DenseExecutionMode::Balanced as i32
    );
    assert_eq!(restricted.filter_selectivity_ppm, 500_000);
    assert_eq!(restricted.candidate_depth, 2);
    assert_eq!(
        restricted.policy_point.unwrap().measured_recall_ppm,
        980_000
    );
    let mismatched_depth = DenseRequestKey {
        candidate_depth: 1,
        ..key
    };
    let err = reader
        .resolve_dense_execution(auto(), 16, mismatched_depth)
        .await
        .err()
        .unwrap();
    assert_eq!(err.code(), tonic::Code::FailedPrecondition);
}

fn auto() -> crate::dense_execution::DenseRequest {
    crate::dense_execution::DenseRequest {
        mode: DenseExecutionMode::Auto,
        precision: crate::pb::DensePrecisionRequest::Auto,
        filter_strategy: crate::pb::DenseFilterStrategy::Default,
    }
}

#[tokio::test]
async fn dense_policy_selectivity_uses_authorized_vector_membership() {
    let nodes = vec![node_rows(0, false, 4, 2)];
    let owner = CoordinatorServiceImpl::with_local_nodes(nodes.clone());
    let reader = owner
        .for_access(Some(&access("semantic", FieldAction::Use)), "bm25_search")
        .unwrap()
        .for_vector_field("semantic")
        .unwrap();
    assert_eq!(
        reader.dense_filter_selectivity(None, 2).await.unwrap(),
        500_000
    );
    assert_eq!(
        reader
            .dense_filter_selectivity(Some(&RequestFilters::default()), 2)
            .await
            .unwrap(),
        500_000
    );
    let private = RequestFilters {
        tree: crate::cel::compile_filter("audience == 'private'").unwrap(),
        ..Default::default()
    };
    // Three matching documents, but only one owns an indexed vector.
    assert_eq!(
        owner
            .dense_filter_selectivity(Some(&private), 2)
            .await
            .unwrap(),
        500_000
    );
    assert_eq!(
        owner.dense_filter_selectivity(None, 2).await.unwrap(),
        1_000_000
    );
    assert_eq!(
        reader
            .dense_filter_selectivity(Some(&private), 2)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    assert_granted_selectivity_and_staleness(&nodes, &owner, &private).await;
}

async fn assert_granted_selectivity_and_staleness(
    nodes: &[Arc<crate::node::NodeServiceImpl>],
    owner: &CoordinatorServiceImpl,
    private: &RequestFilters,
) {
    let mut decision = access("semantic", FieldAction::Use);
    decision
        .field_permissions
        .as_mut()
        .unwrap()
        .grants
        .push(FieldGrant {
            field: "audience".into(),
            actions: vec![FieldAction::Use as i32],
        });
    let reader = owner
        .for_access(Some(&decision), "bm25_search")
        .unwrap()
        .for_vector_field("semantic")
        .unwrap();
    assert_eq!(
        reader
            .dense_filter_selectivity(Some(private), 2)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        owner
            .for_vector_field("missing")
            .unwrap()
            .dense_filter_selectivity(Some(private), 2)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_pinned_selectivity_goes_stale(nodes, &reader).await;
}

/// A read pinned before a delete refuses; a fresh read sees the new view.
async fn assert_pinned_selectivity_goes_stale(
    nodes: &[Arc<crate::node::NodeServiceImpl>],
    reader: &CoordinatorServiceImpl,
) {
    let (pinned, _) = reader.pin_read_versions().await.unwrap();
    nodes[0]
        .delete_documents(Request::new(DeleteDocumentsRequest {
            doc_ids: vec![0],
            ..Default::default()
        }))
        .await
        .unwrap();
    assert_eq!(
        pinned
            .dense_filter_selectivity(None, 2)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(reader.dense_filter_selectivity(None, 2).await.unwrap(), 0);
}
