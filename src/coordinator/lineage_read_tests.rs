//! Lineage authorization and pinned-version checks.

use super::*;
use crate::pb::*;

/// One shard holding a public row under parent 10 and a private row under
/// parent 99, with the link that wrote them.
async fn lineage_fixture() -> (crate::link::NodeLink, CoordinatorServiceImpl) {
    let node = Arc::new(crate::node::NodeServiceImpl::new(
        None,
        crate::node::NodeConfig {
            analysis_addr: Some(crate::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
            facet_fields: vec!["audience".into()],
            ..Default::default()
        },
    ));
    let mut link = crate::link::NodeLink::local(node.clone());
    for (audience, parent) in [("public", 10), ("private", 99)] {
        link.add_documents(tokio_stream::iter([AddDocumentsRequest {
            text: "alpha".into(),
            analysis: Some(crate::analyzer::body_spec()),
            facets: vec![FacetValue {
                field: "audience".into(),
                value: audience.into(),
            }],
            lineage: Some(DocLineage {
                parent_id: parent,
                group_id: 200 + parent,
                ..Default::default()
            }),
            ..Default::default()
        }]))
        .await
        .unwrap();
    }
    let owner = CoordinatorServiceImpl::with_local_nodes(vec![node]);
    (link, owner)
}

/// Public rows only, with `parent_id` usable and disclosable.
fn parent_id_access() -> AccessDecision {
    AccessDecision {
        action: AccessAction::Search as i32,
        document_visibility: Some(DocumentVisibility {
            filter: crate::cel::compile_filter("audience == 'public'").unwrap(),
        }),
        field_permissions: Some(FieldPermissions {
            grants: vec![FieldGrant {
                field: "parent_id".into(),
                actions: vec![FieldAction::Use as i32, FieldAction::Disclose as i32],
            }],
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Use without disclose, or disclose without use, is refused before any I/O.
async fn assert_partial_grants_are_denied(reader: &CoordinatorServiceImpl) {
    let mut denied = reader.clone();
    denied.node_addrs = vec!["http://must-not-resolve.invalid:50051".into()];
    for actions in [
        vec![FieldAction::Use as i32],
        vec![FieldAction::Disclose as i32],
    ] {
        denied.field_permissions = Some(
            crate::field_permissions::FieldScope::new(&FieldPermissions {
                grants: vec![FieldGrant {
                    field: "parent_id".into(),
                    actions,
                }],
                ..Default::default()
            })
            .unwrap(),
        );
        assert_eq!(
            denied
                .lineage_key(&[], "parent_id")
                .await
                .unwrap_err()
                .code(),
            tonic::Code::PermissionDenied
        );
    }
}

#[tokio::test]
async fn lineage_authority_projects_each_key_and_guards_the_selection_version() {
    let (mut link, owner) = lineage_fixture().await;
    let access = parent_id_access();
    let reader = owner.for_access(Some(&access), "aggregate").unwrap();
    assert_eq!(
        reader.lineage_key(&[0, 1], "parent_id").await.unwrap(),
        [(0, 10)].into_iter().collect()
    );
    assert_eq!(
        reader
            .lineage_key(&[0], "group_id")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::PermissionDenied
    );
    assert_eq!(
        reader.lineage_keys(&[]).await.unwrap_err().code(),
        tonic::Code::PermissionDenied
    );
    let (pinned, _) = reader.pin_read_versions().await.unwrap();
    link.add_documents(tokio_stream::iter([AddDocumentsRequest {
        text: "changed".into(),
        analysis: Some(crate::analyzer::body_spec()),
        ..Default::default()
    }]))
    .await
    .unwrap();
    assert_eq!(
        pinned
            .lineage_key(&[0], "parent_id")
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_partial_grants_are_denied(&reader).await;
    let mut unknown = reader;
    unknown.document_visibility = Some(DocumentVisibility {
        filter: crate::cel::compile_filter("internal_secret == 'public'").unwrap(),
    });
    let error = unknown.lineage_key(&[], "parent_id").await.unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(!error.message().contains("internal_secret"));
}
