//! Dense execution policy qualification under document views.

use super::*;

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
    let policy = crate::dense_policy::DenseExecutionPolicy::parse(&format!(
        r#"
format_version = 1
policy_id = "document-view"
embedding_model = "test"
corpus_generation = 0
corpus_rows = 2
dimensions = 16
provider_backend = "fake-ann"
scoring_fingerprint = "{fingerprint}"
measured_queries = 20
[[points]]
k = 1
filter_selectivity_ppm_min = 1000000
filter_selectivity_ppm_max = 1000000
candidates = 1
measured_recall_ppm = 990000
[[points]]
k = 1
filter_selectivity_ppm_min = 500000
filter_selectivity_ppm_max = 500000
candidates = 2
measured_recall_ppm = 980000
"#
    ))
    .unwrap();
    let owner =
        CoordinatorServiceImpl::with_local_nodes(vec![node]).with_dense_execution_policy(policy);
    let key = DenseRequestKey {
        k: 1,
        candidate_depth: 0,
        filters: None,
    };
    let unrestricted = owner
        .resolve_dense_execution(DenseExecutionMode::Auto, 16, key)
        .await
        .unwrap();
    assert_eq!(unrestricted.candidate_depth, 1);
    let reader = owner
        .for_access(Some(&access("semantic", FieldAction::Use)), "bm25_search")
        .unwrap()
        .for_vector_field("semantic")
        .unwrap();
    let restricted = reader
        .resolve_dense_execution(DenseExecutionMode::Auto, 16, key)
        .await
        .unwrap();
    assert_eq!(restricted.resolved_mode, DenseExecutionMode::Ann as i32);
    assert_eq!(restricted.filter_selectivity_ppm, 500_000);
    assert_eq!(restricted.candidate_depth, 2);
    assert_eq!(
        restricted.policy_point.unwrap().measured_recall_ppm,
        980_000
    );
    // An explicit depth from the unfiltered point cannot borrow its evidence.
    assert_eq!(
        reader
            .resolve_dense_execution(
                DenseExecutionMode::Auto,
                16,
                DenseRequestKey {
                    candidate_depth: 1,
                    ..key
                }
            )
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
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
            .dense_filter_selectivity(Some(&private), 2)
            .await
            .unwrap(),
        0
    );
    assert_eq!(
        owner
            .for_vector_field("missing")
            .unwrap()
            .dense_filter_selectivity(Some(&private), 2)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_pinned_selectivity_goes_stale(&nodes, &reader).await;
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
