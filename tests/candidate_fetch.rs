use pipestream_search::{
    analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND},
    coordinator::CoordinatorServiceImpl,
    link::NodeLink,
    node::{NodeConfig, NodeServiceImpl},
    pb::*,
    stats_identity::StatsClaim,
    visibility::VisibilityScope,
};
use std::sync::Arc;

fn view(column: &str) -> DocumentVisibility {
    DocumentVisibility {
        filter: Some(
            pipestream_search::cel::compile_filter(&format!("{column} == 'public'"))
                .unwrap()
                .unwrap(),
        ),
    }
}
fn projection(column: &str) -> CompiledProjection {
    CompiledProjection {
        name: "result".into(),
        expr: Some(pipestream_search::cel::compile_value(column).unwrap()),
    }
}
async fn node(offset: u64) -> Arc<NodeServiceImpl> {
    let node = Arc::new(NodeServiceImpl::new(
        None,
        NodeConfig {
            slot_offset: offset,
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
            facet_fields: vec!["audience".into(), "color".into()],
            ..Default::default()
        },
    ));
    NodeLink::local(node.clone())
        .add_documents(tokio_stream::iter(
            [("public", "red"), ("private", "secret"), ("public", "blue")]
                .into_iter()
                .map(|(audience, color)| AddDocumentsRequest {
                    text: "alpha".into(),
                    original_source: Some(ProtobufSource {
                        descriptor_set: include_bytes!(
                            "fixtures/protobuf-semantics/descriptor.bin"
                        )
                        .to_vec(),
                        message_type: "semantics.Doc".into(),
                        payload: vec![8, 1],
                    }),
                    identity: (color != "blue").then(|| DocumentIdentity {
                        document_key: color.as_bytes().to_vec(),
                        version: u64::MAX,
                        chunk_ordinal: Some(0),
                    }),
                    source_chunk_ordinal: (color != "blue").then_some(0),
                    analysis: Some(body_spec()),
                    facets: vec![
                        FacetValue {
                            field: "audience".into(),
                            value: audience.into(),
                        },
                        FacetValue {
                            field: "color".into(),
                            value: color.into(),
                        },
                    ],
                    ..Default::default()
                }),
        ))
        .await
        .unwrap();
    node
}
#[tokio::test]
async fn vector_only_candidates_have_explicit_identity_absence_under_the_read_receipt() {
    use pipestream_search::vector::{VectorIndex, EMBEDDED_TURBOVEC};
    let mut vectors = vec![0.; 128];
    vectors[0] = 1.;
    vectors[65] = 1.;
    let mut index = VectorIndex::create(EMBEDDED_TURBOVEC, 64, 4).unwrap();
    index.add(&vectors, 64).unwrap();
    index.prepare().unwrap();
    let mut live = pipestream_search::live_docs::LiveDocs::default();
    live.delete(1);
    let node = NodeServiceImpl::new(
        Some(index),
        NodeConfig {
            slot_offset: 100,
            ..Default::default()
        },
    )
    .with_live_docs(live)
    .unwrap();
    let mut link = NodeLink::local(Arc::new(node));
    let request = FetchValuesRequest {
        candidate_ids: vec![100, 101, 102, 100],
        include_identities: true,
        ..Default::default()
    };
    let response = link
        .fetch_values(request.clone())
        .await
        .unwrap()
        .into_inner();
    assert!(response.identities_included);
    assert!(response.rows.is_empty());
    assert_eq!(
        response.identities,
        vec![CandidateIdentity {
            doc_id: 100,
            identity: None
        }]
    );
    StatsClaim::required(response.stats_epoch, &response.stats_incarnation).unwrap();
    let scoped = link
        .fetch_values(FetchValuesRequest {
            visibility: Some(view("audience")),
            ..request
        })
        .await
        .unwrap()
        .into_inner();
    assert!(scoped.identities_included);
    assert!(scoped.identities.is_empty());
    assert_eq!(scoped.visibility_columns_known, vec![false]);
}

fn request() -> FetchValuesRequest {
    FetchValuesRequest {
        candidate_ids: vec![u64::MAX, 12, 11, 10, 10, 0],
        include_identities: true,
        projections: vec![projection("color")],
        visibility: Some(view("audience")),
        ..Default::default()
    }
}

#[tokio::test]
async fn candidate_values_require_live_visible_rows_and_echo_even_empty_views() {
    let mut link = NodeLink::local(node(10).await);
    let response = link.fetch_values(request()).await.unwrap().into_inner();
    assert!(response.identities_included);
    let identities: std::collections::BTreeMap<_, _> = response
        .identities
        .iter()
        .map(|row| (row.doc_id, row.identity.clone()))
        .collect();
    assert_eq!(identities.len(), 2);
    assert_eq!(
        identities[&10],
        Some(DocumentIdentity {
            document_key: b"red".to_vec(),
            version: u64::MAX,
            chunk_ordinal: Some(0),
        })
    );
    assert_eq!(identities[&12], None);
    let values_only = link
        .fetch_values(FetchValuesRequest {
            include_identities: false,
            ..request()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(!values_only.identities_included);
    assert!(values_only.identities.is_empty());
    let scope = VisibilityScope::new(Some(&view("audience"))).unwrap();
    scope
        .validate_echo(
            &response.visibility_fingerprint,
            &response.visibility_columns_known,
        )
        .unwrap();
    assert_eq!(response.visibility_columns_known, vec![true]);
    assert_eq!(
        response
            .rows
            .iter()
            .map(|row| row.doc_id)
            .collect::<Vec<_>>(),
        vec![10, 12]
    );
    assert_eq!(
        response
            .rows
            .iter()
            .map(|row| row.values[0].value.clone())
            .collect::<Vec<_>>(),
        vec![
            Some(projected_value::Value::StringValue("red".into())),
            Some(projected_value::Value::StringValue("blue".into())),
        ]
    );
    StatsClaim::required(response.stats_epoch, &response.stats_incarnation).unwrap();
    let empty = link
        .fetch_values(FetchValuesRequest {
            candidate_ids: vec![],
            ..request()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(empty.rows.is_empty());
    assert!(empty.identities_included);
    assert!(empty.identities.is_empty());
    assert_eq!(
        empty.visibility_fingerprint,
        response.visibility_fingerprint
    );
    assert_eq!(empty.projection_types, response.projection_types);
    let missing = link
        .fetch_values(FetchValuesRequest {
            visibility: Some(view("missing")),
            ..request()
        })
        .await
        .unwrap()
        .into_inner();
    assert!(missing.rows.is_empty());
    assert_eq!(missing.visibility_columns_known, vec![false]);
    let malformed = link
        .fetch_values(FetchValuesRequest {
            visibility: Some(DocumentVisibility::default()),
            ..request()
        })
        .await
        .unwrap_err();
    assert_eq!(malformed.code(), tonic::Code::InvalidArgument);
    link.delete_documents(DeleteDocumentsRequest {
        doc_ids: vec![10],
        expected_wal_generation: None,
    })
    .await
    .unwrap();
    assert_eq!(
        link.fetch_values(request())
            .await
            .unwrap()
            .into_inner()
            .rows
            .iter()
            .map(|r| r.doc_id)
            .collect::<Vec<_>>(),
        vec![12]
    );
}

#[tokio::test]
async fn selection_claims_reject_mutation_and_another_shard_lifetime() {
    let first = node(10).await;
    let mut link = NodeLink::local(first);
    let stats = link
        .term_stats(TermStatsRequest::default())
        .await
        .unwrap()
        .into_inner();
    let pinned = FetchValuesRequest {
        expected_stats_epoch: stats.stats_epoch,
        expected_stats_incarnation: stats.stats_incarnation.clone(),
        ..request()
    };
    link.fetch_values(pinned.clone()).await.unwrap();
    // Same epoch and slot geometry, but a different in-memory shard lifetime.
    let mut next = NodeLink::local(node(10).await);
    assert_eq!(
        next.term_stats(TermStatsRequest::default())
            .await
            .unwrap()
            .into_inner()
            .stats_epoch,
        stats.stats_epoch
    );
    assert_eq!(
        next.fetch_values(pinned.clone()).await.unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    link.delete_documents(DeleteDocumentsRequest {
        doc_ids: vec![11],
        expected_wal_generation: None,
    })
    .await
    .unwrap();
    // A hidden-row mutation still invalidates the claimed physical generation.
    assert_eq!(
        link.fetch_values(pinned).await.unwrap_err().code(),
        tonic::Code::FailedPrecondition
    );
    for (epoch, incarnation) in [(0, vec![1; 32]), (1, vec![]), (1, vec![1; 31])] {
        assert_eq!(
            link.fetch_values(FetchValuesRequest {
                expected_stats_epoch: epoch,
                expected_stats_incarnation: incarnation,
                candidate_ids: vec![],
                ..request()
            })
            .await
            .unwrap_err()
            .code(),
            tonic::Code::FailedPrecondition
        );
    }
}

#[tokio::test]
async fn coordinator_keeps_complete_selection_claims_across_candidate_fetches() {
    let nodes = vec![node(0).await, node(10).await];
    let coordinator = CoordinatorServiceImpl::with_local_nodes(nodes.clone());
    let projections = vec![projection("color")];
    let ids = vec![0, 1, 2, 10, 11, 12];
    let first = coordinator
        .fetch_values(&ids, &projections, &[])
        .await
        .unwrap();
    assert_eq!(first.epochs.len(), 2);
    let same = coordinator
        .fetch_values_at(&ids, &projections, &[], &first.epochs)
        .await
        .unwrap();
    assert_eq!(same.rows, first.rows);
    assert_eq!(same.epochs, first.epochs);
    for claims in [
        vec![],
        vec![first.epochs[0]],
        vec![StatsClaim::default(); 2],
    ] {
        assert!(coordinator
            .fetch_values_at(&ids, &projections, &[], &claims)
            .await
            .is_err());
    }
    NodeLink::local(nodes[1].clone())
        .delete_documents(DeleteDocumentsRequest {
            doc_ids: vec![11],
            expected_wal_generation: None,
        })
        .await
        .unwrap();
    assert!(coordinator
        .fetch_values_at(&ids, &projections, &[], &first.epochs)
        .await
        .is_err());
    assert!(coordinator
        .fetch_values_at(&[], &[], &[], &first.epochs)
        .await
        .is_err());
}

/// A wire peer that can omit new fields or lie about its executed request.
#[derive(Clone)]
struct FetchPeer(Arc<std::sync::Mutex<FetchValuesResponse>>);
impl tonic::server::NamedService for FetchPeer {
    const NAME: &'static str = "ai.protomolt.search.v1.NodeService";
}
impl<B> tonic::codegen::Service<tonic::codegen::http::Request<B>> for FetchPeer
where
    B: http_body::Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = tonic::codegen::http::Response<tonic::body::BoxBody>;
    type Error = std::convert::Infallible;
    type Future = std::pin::Pin<
        Box<dyn std::future::Future<Output = Result<Self::Response, Self::Error>> + Send>,
    >;
    fn poll_ready(
        &mut self,
        _: &mut std::task::Context<'_>,
    ) -> std::task::Poll<Result<(), Self::Error>> {
        std::task::Poll::Ready(Ok(()))
    }
    fn call(&mut self, request: tonic::codegen::http::Request<B>) -> Self::Future {
        let response = self.0.lock().unwrap().clone();
        Box::pin(async move {
            struct Unary(FetchValuesResponse);
            impl tonic::server::UnaryService<FetchValuesRequest> for Unary {
                type Response = FetchValuesResponse;
                type Future =
                    std::future::Ready<Result<tonic::Response<Self::Response>, tonic::Status>>;
                fn call(&mut self, _: tonic::Request<FetchValuesRequest>) -> Self::Future {
                    std::future::ready(Ok(tonic::Response::new(self.0.clone())))
                }
            }
            Ok(
                tonic::server::Grpc::new(tonic::codec::ProstCodec::default())
                    .unary(Unary(response), request)
                    .await,
            )
        })
    }
}

#[tokio::test]
async fn coordinator_refuses_old_or_inconsistent_fetch_peers_before_publishing_values() {
    let good = FetchValuesResponse {
        rows: vec![FetchedRow {
            doc_id: 0,
            values: vec![ProjectedValue {
                value: Some(projected_value::Value::IntValue(7)),
            }],
            stage_values: vec![],
        }],
        projection_types: vec![ScalarValueType::Integer as i32],
        stats_epoch: 1,
        stats_incarnation: vec![2; 32],
        ..Default::default()
    };
    let response = Arc::new(std::sync::Mutex::new(good.clone()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(FetchPeer(response.clone()))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let coordinator = CoordinatorServiceImpl::new(vec![address]);
    let projections = vec![projection("7")];
    assert_eq!(
        coordinator
            .fetch_values(&[0], &projections, &[])
            .await
            .unwrap()
            .rows
            .len(),
        1
    );
    for failure in 0..6 {
        let mut invalid = good.clone();
        match failure {
            0 => {
                invalid.stats_epoch = 0;
                invalid.stats_incarnation.clear();
            }
            1 => {
                invalid.stats_incarnation.pop();
            }
            2 => invalid.visibility_fingerprint = vec![3; 32],
            3 => invalid.visibility_columns_known = vec![true],
            4 => invalid.rows[0].doc_id = 1,
            5 => invalid.rows.push(invalid.rows[0].clone()),
            _ => unreachable!(),
        }
        *response.lock().unwrap() = invalid;
        let error = coordinator
            .fetch_values(&[0], &projections, &[])
            .await
            .err()
            .unwrap();
        assert_eq!(
            error.code(),
            tonic::Code::FailedPrecondition,
            "failure {failure}: {error}"
        );
    }
    let mut malformed_stage = good.clone();
    malformed_stage.stage_columns_known = vec![true];
    malformed_stage.rows[0].stage_values = vec![ProjectedValue {
        value: Some(projected_value::Value::StringValue("7".into())),
    }];
    *response.lock().unwrap() = malformed_stage;
    let stages = vec![ScoreStage {
        column: "boost".into(),
        operation: Some(pipestream_search::pb::score_stage::Operation::Op(
            ScoreOp::AddLinear as i32,
        )),
        weight: 1.0,
        ..Default::default()
    }];
    assert!(coordinator
        .fetch_values(&[0], &projections, &stages)
        .await
        .is_err());
    *response.lock().unwrap() = good;
    let expected = StatsClaim::required(1, &[9; 32]).unwrap();
    assert!(coordinator
        .fetch_values_at(&[0], &projections, &[], &[expected])
        .await
        .is_err());
    let expected = StatsClaim::required(1, &[2; 32]).unwrap();
    let identity_good = FetchValuesResponse {
        stats_epoch: 1,
        stats_incarnation: vec![2; 32],
        identities_included: true,
        identities: vec![CandidateIdentity {
            doc_id: 0,
            identity: Some(DocumentIdentity {
                document_key: vec![0, 255],
                version: u64::MAX,
                chunk_ordinal: Some(0),
            }),
        }],
        ..Default::default()
    };
    *response.lock().unwrap() = identity_good.clone();
    let identities = coordinator
        .resolve_candidate_identities_at(&[0], &[expected])
        .await
        .unwrap();
    assert_eq!(identities[&0], identity_good.identities[0].identity);
    for failure in 0..8 {
        let mut invalid = identity_good.clone();
        match failure {
            0 => {
                invalid.identities_included = false;
                invalid.identities.clear();
            }
            1 => invalid.identities[0].doc_id = 1,
            2 => invalid.identities.push(invalid.identities[0].clone()),
            3 => invalid.stats_epoch += 1,
            4 => invalid.stats_incarnation[0] ^= 1,
            5 => invalid.identities[0]
                .identity
                .as_mut()
                .unwrap()
                .document_key
                .clear(),
            6 => invalid.identities[0].identity.as_mut().unwrap().version = 0,
            7 => {
                invalid.identities[0]
                    .identity
                    .as_mut()
                    .unwrap()
                    .document_key = vec![1; 16385]
            }
            _ => unreachable!(),
        }
        *response.lock().unwrap() = invalid;
        assert_eq!(
            coordinator
                .resolve_candidate_identities_at(&[0], &[expected])
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition,
            "identity failure {failure}"
        );
    }
    *response.lock().unwrap() = FetchValuesResponse {
        identities: vec![CandidateIdentity {
            doc_id: 0,
            identity: None,
        }],
        ..identity_good
    };
    assert_eq!(
        coordinator
            .resolve_candidate_identities_at(&[0], &[expected])
            .await
            .unwrap()[&0],
        None
    );
    server.abort();
    let _ = server.await;
}
