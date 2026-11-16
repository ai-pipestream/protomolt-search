//! Vector candidate reads: fields, views, bindings and pinned versions.

use super::*;

#[tokio::test]
async fn vector_candidate_reads_enforce_actual_fields_views_and_versions() {
    let nodes = vec![node(0, false), node(100, false)];
    let owner = CoordinatorServiceImpl::with_local_nodes(nodes.clone());
    let reader = owner
        .for_access(Some(&access("semantic", FieldAction::Use)), "bm25_search")
        .unwrap();
    let vector = vec![0.25; 16];
    let members = reader.vector_membership("semantic").await.unwrap();
    assert_eq!(members.ids, [0, 100].into_iter().collect());
    let scores = reader
        .dense_signal(&vector, &[0, 1, 100, 101], "semantic")
        .await
        .unwrap();
    assert_eq!(
        scores
            .keys()
            .copied()
            .collect::<std::collections::BTreeSet<_>>(),
        members.ids
    );
    let reference = owner
        .dense_signal(&vector, &[0, 100], "semantic")
        .await
        .unwrap();
    assert_eq!(scores, reference);
    let exact = reader
        .exact_vector_scores(&vector, &[0, 100], "semantic")
        .await
        .unwrap();
    assert_eq!(exact.scores.len(), 2);
    assert_eq!(exact.logical_bytes, 128);
    assert!(reader
        .dense_signal(&vector, &[], "semantic")
        .await
        .unwrap()
        .is_empty());
    assert!(reader
        .exact_vector_scores(&vector, &[], "semantic")
        .await
        .unwrap()
        .scores
        .is_empty());
    assert_ungranted_fields_denied(&owner).await;
    assert_alias_and_unnamed_fields_refused(&owner, &reader, &vector).await;
    assert_pinned_vector_reads_go_stale(&nodes, &reader, &vector).await;
    assert_query_routes_carry_the_grant(&owner);
}

/// A grant on another field, or one that only discloses, reads nothing.
async fn assert_ungranted_fields_denied(owner: &CoordinatorServiceImpl) {
    for decision in [
        access("body", FieldAction::Use),
        access("semantic", FieldAction::Disclose),
    ] {
        let denied = owner.for_access(Some(&decision), "bm25_search").unwrap();
        assert_eq!(
            denied
                .vector_membership("semantic")
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(
            denied
                .dense_signal(&[], &[], "semantic")
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
        assert_eq!(
            denied
                .exact_vector_scores(&[], &[], "semantic")
                .await
                .err()
                .unwrap()
                .code(),
            tonic::Code::PermissionDenied
        );
    }
}

/// A granted alias that no shard indexed fails the binding check, and an
/// unnamed field is not covered by a named grant.
async fn assert_alias_and_unnamed_fields_refused(
    owner: &CoordinatorServiceImpl,
    reader: &CoordinatorServiceImpl,
    vector: &[f32],
) {
    let alias = owner
        .for_access(Some(&access("signal", FieldAction::Use)), "bm25_search")
        .unwrap();
    assert_eq!(
        alias
            .dense_signal(vector, &[], "signal")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        reader
            .dense_signal(vector, &[], "")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
}

/// Reads pinned before a delete refuse on both vector read paths.
async fn assert_pinned_vector_reads_go_stale(
    nodes: &[Arc<crate::node::NodeServiceImpl>],
    reader: &CoordinatorServiceImpl,
    vector: &[f32],
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
            .dense_signal(vector, &[], "semantic")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        pinned
            .exact_vector_scores(vector, &[], "semantic")
            .await
            .err()
            .unwrap()
            .code(),
        tonic::Code::FailedPrecondition
    );
}

/// Both query routes admit the decision's view and only its granted field.
fn assert_query_routes_carry_the_grant(owner: &CoordinatorServiceImpl) {
    let decision = access("semantic", FieldAction::Use);
    for route in ["query", "query_stream"] {
        let admitted = owner.for_access(Some(&decision), route).unwrap();
        assert_eq!(admitted.document_visibility, decision.document_visibility);
        assert!(admitted
            .field_permissions
            .as_ref()
            .unwrap()
            .vector("signal")
            .is_err());
    }
}

#[tokio::test]
async fn empty_vector_reads_refuse_mixed_generations_and_missing_binding_receipts() {
    let owner = CoordinatorServiceImpl::with_local_nodes(vec![node(0, false), node(100, true)]);
    let vector = vec![0.25; 16];
    assert_eq!(
        owner
            .vector_membership("semantic")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        owner
            .dense_signal(&vector, &[], "semantic")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(
        owner
            .exact_vector_scores(&vector, &[], "semantic")
            .await
            .err()
            .unwrap()
            .code(),
        tonic::Code::FailedPrecondition
    );
    let binding = crate::mapping::derive_plan(
        include_bytes!("../../../tests/fixtures/vector-binding/descriptor.bin"),
        "vector_binding.Named",
    )
    .unwrap()
    .vector_binding
    .unwrap();
    assert!(CoordinatorServiceImpl::check_vector_binding("semantic", None, &mut None).is_err());
    let mut bad = binding.clone();
    bad.field = "signal".into();
    assert!(
        CoordinatorServiceImpl::check_vector_binding("semantic", Some(&bad), &mut None).is_err()
    );
    let response = VectorRescoreResponse {
        vector_binding: Some(binding),
        ..Default::default()
    };
    assert!(owner
        .check_read_view(
            0,
            &crate::visibility::VisibilityScope::default(),
            &response,
            &mut []
        )
        .is_err());
    let unbound = CoordinatorServiceImpl::with_local_nodes(vec![Arc::new(
        crate::node::NodeServiceImpl::new(None, Default::default()),
    )]);
    assert!(unbound
        .dense_signal(&vector, &[], "semantic")
        .await
        .is_err());
    assert!(unbound
        .exact_vector_scores(&vector, &[], "semantic")
        .await
        .is_err());
}
