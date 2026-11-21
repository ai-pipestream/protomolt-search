//! Dense filter selectivity under document views, field grants, and pinned reads.

use super::*;

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
