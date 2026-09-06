use protomolt_search_embedded::{
    analyzer::body_spec,
    pb::{search_service_server::SearchService, *},
    EmbeddedSearch, EmbeddedSearchConfig, EmbeddedShardConfig, PrincipalConfig, Principals,
};
use std::sync::Arc;
use tonic::Request;

#[test]
fn authorized_mobile_facade_preserves_private_shard_ownership() {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(async {
            let mut shard = EmbeddedShardConfig::in_memory(0);
            shard.node.facet_fields = vec!["audience".into()];
            let runtime = EmbeddedSearch::create(EmbeddedSearchConfig::single(shard))
                .await
                .unwrap();
            runtime
                .add_documents(
                    0,
                    ["public", "private"]
                        .into_iter()
                        .map(|audience| AddDocumentsRequest {
                            text: format!("alpha {audience}"),
                            analysis: Some(body_spec()),
                            facets: vec![FacetValue {
                                field: "audience".into(),
                                value: audience.into(),
                            }],
                            ..Default::default()
                        })
                        .collect(),
                )
                .await
                .unwrap();
            let token = "reader-token-0123456789012345";
            let principals = Principals::from_configs(&[PrincipalConfig {
                name: "reader".into(),
                token: token.into(),
                ..Default::default()
            }])
            .unwrap()
            .with_policy(AccessPolicy {
                format_version: 2,
                revision: 1,
                resources: vec![CollectionResource {
                    workspace: "phone".into(),
                    collection: "".into(),
                }],
                grants: vec![CollectionGrant {
                    principal: "reader".into(),
                    workspace: "phone".into(),
                    collection: "".into(),
                    actions: vec![AccessAction::Search as i32],
                    document_visibility: Some(DocumentVisibility {
                        filter: Some(FilterExpr {
                            expr: Some(filter_expr::Expr::Facet(FacetPredicate {
                                column: "audience".into(),
                                values: vec!["public".into()],
                            })),
                        }),
                    }),
                }],
            })
            .unwrap();
            let facade = runtime.authorized_service(Arc::new(principals));
            let query = Bm25SearchRequest {
                text: "alpha".into(),
                k: 10,
                analysis: Some(body_spec()),
                ..Default::default()
            };
            let anonymous = SearchService::bm25_search(&facade, Request::new(query.clone()))
                .await
                .unwrap_err();
            assert_eq!(anonymous.code(), tonic::Code::Unauthenticated);
            let mut request = Request::new(query);
            request
                .metadata_mut()
                .insert("authorization", format!("Bearer {token}").parse().unwrap());
            let response = SearchService::bm25_search(&facade, request)
                .await
                .unwrap()
                .into_inner();
            assert_eq!(response.hits.len(), 1);
            assert_eq!(response.hits[0].doc_id, 0);
            assert!(response.execution_details_redacted);
            assert!(!runtime.allows_network());
        });
}
