mod common;
use pipestream_search::{
    analyzer::{body_spec, NATIVE_ANALYSIS_BACKEND},
    coordinator::CoordinatorServiceImpl,
    node::{Layout, NodeConfig},
    pb::{node_service_client::NodeServiceClient, *},
    visibility::VisibilityScope,
};
use std::sync::Arc;
use tonic::{transport::Channel, Code};
fn view(predicate: &str) -> DocumentVisibility {
    DocumentVisibility {
        filter: pipestream_search::cel::compile_filter(predicate).unwrap(),
    }
}
fn config() -> NodeConfig {
    NodeConfig {
        analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.into()),
        facet_fields: vec!["audience".into()],
        ..Default::default()
    }
}
async fn seed(client: &mut NodeServiceClient<Channel>) {
    let vectors = common::unit_vectors(4, 16, 9991);
    let (shift, scale) = common::fit_calibration(16, 4, &vectors);
    client
        .set_calibration(SetCalibrationRequest {
            dim: 16,
            bit_width: 4,
            shift,
            scale,
        })
        .await
        .unwrap();
    for (i, vector) in vectors.chunks(16).enumerate() {
        client
            .add_documents(tokio_stream::iter([AddDocumentsRequest {
                text: "alpha".into(),
                analysis: Some(body_spec()),
                facets: vec![FacetValue {
                    field: "audience".into(),
                    value: if i == 1 { "private" } else { "public" }.into(),
                }],
                lineage: (i != 2).then_some(DocLineage {
                    parent_id: 10 + i as u64,
                    group_id: 20 + i as u64,
                    ..Default::default()
                }),
                ..Default::default()
            }]))
            .await
            .unwrap();
        client
            .add_vectors(tokio_stream::iter([AddVectorsRequest {
                dim: 16,
                vectors: vector.to_vec(),
            }]))
            .await
            .unwrap();
    }
}
fn scoped() -> ResolveParentsRequest {
    ResolveParentsRequest {
        doc_ids: vec![0, 1, 2, 3, 3, 999],
        visibility: Some(view("audience == 'public'")),
        ..Default::default()
    }
}
fn check_view(reply: &ResolveParentsResponse) {
    let request = scoped();
    VisibilityScope::new(request.visibility.as_ref())
        .unwrap()
        .validate_echo(
            &reply.visibility_fingerprint,
            &reply.visibility_columns_known,
        )
        .unwrap();
    assert!(reply.stats_epoch > 0);
    assert_eq!(reply.stats_incarnation.len(), 32);
    assert_eq!(reply.visibility_columns_known, vec![true]);
}
#[tokio::test]
async fn lineage_projects_only_requested_keys_and_never_returns_hidden_candidates() {
    let (address, server) = common::start_empty_node(config()).await;
    let mut client = NodeServiceClient::connect(address).await.unwrap();
    seed(&mut client).await;
    for fields in [vec![], vec![1], vec![2], vec![2, 1]] {
        let mut request = scoped();
        request.fields = fields.clone();
        let reply = client.resolve_parents(request).await.unwrap().into_inner();
        check_view(&reply);
        assert_eq!(
            reply.fields,
            if fields.is_empty() || fields.len() == 2 {
                vec![1, 2]
            } else {
                fields
            }
        );
        assert_eq!(
            reply.parents.iter().map(|p| p.doc_id).collect::<Vec<_>>(),
            vec![0, 2, 3]
        );
        for row in reply.parents {
            assert_eq!(
                row.parent_id,
                if reply.fields.contains(&1) {
                    if row.doc_id == 2 {
                        (1 << 63) | 2
                    } else {
                        10 + row.doc_id
                    }
                } else {
                    0
                }
            );
            assert_eq!(
                row.group_id,
                if reply.fields.contains(&2) && row.doc_id != 2 {
                    20 + row.doc_id
                } else {
                    0
                }
            );
        }
    }
    for fields in [vec![0], vec![99], vec![1, 1]] {
        let mut request = scoped();
        request.fields = fields;
        assert_eq!(
            client.resolve_parents(request).await.unwrap_err().code(),
            Code::InvalidArgument
        );
    }
    let before = client.resolve_parents(scoped()).await.unwrap().into_inner();
    client
        .delete_documents(DeleteDocumentsRequest {
            doc_ids: vec![0],
            expected_wal_generation: None,
        })
        .await
        .unwrap();
    let stale = ResolveParentsRequest {
        expected_stats_epoch: before.stats_epoch,
        expected_stats_incarnation: before.stats_incarnation,
        ..scoped()
    };
    assert_eq!(
        client.resolve_parents(stale).await.unwrap_err().code(),
        Code::FailedPrecondition
    );
    let current = client.resolve_parents(scoped()).await.unwrap().into_inner();
    assert_eq!(
        current.parents.iter().map(|p| p.doc_id).collect::<Vec<_>>(),
        vec![2, 3]
    );
    server.abort();
    let _ = server.await;
}
#[tokio::test]
async fn lineage_survives_compaction_and_restart_with_fresh_physical_claims() {
    for layout in [Layout::SingleImage, Layout::Segments] {
        let dir = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("lineage-{layout:?}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let cfg = NodeConfig {
            index_path: Some(dir.join("shard.tv")),
            layout,
            wal: true,
            ..config()
        };
        let (address, server) = common::start_empty_node(cfg.clone()).await;
        let mut client = NodeServiceClient::connect(address).await.unwrap();
        seed(&mut client).await;
        if layout == Layout::SingleImage {
            assert_eq!(
                client.resolve_parents(scoped()).await.unwrap_err().code(),
                Code::FailedPrecondition
            );
            client.flush(FlushRequest {}).await.unwrap();
        }

        client
            .delete_documents(DeleteDocumentsRequest {
                doc_ids: vec![0],
                expected_wal_generation: None,
            })
            .await
            .unwrap();
        let before = client.resolve_parents(scoped()).await.unwrap().into_inner();
        client.flush(FlushRequest {}).await.unwrap();
        client
            .compact_shard(CompactShardRequest::default())
            .await
            .unwrap();
        let after = client.resolve_parents(scoped()).await.unwrap().into_inner();
        check_view(&after);
        assert_eq!(after.parents.len(), 2);
        assert!(after
            .parents
            .iter()
            .any(|p| p.parent_id == 13 && p.group_id == 23));
        assert!(after.stats_epoch > before.stats_epoch);
        let stale = ResolveParentsRequest {
            expected_stats_epoch: before.stats_epoch,
            expected_stats_incarnation: before.stats_incarnation,
            ..scoped()
        };
        assert_eq!(
            client.resolve_parents(stale).await.unwrap_err().code(),
            Code::FailedPrecondition
        );
        client.flush(FlushRequest {}).await.unwrap();
        drop(client);
        server.abort();
        let _ = server.await;
        let (address, server) = common::start_opened_node(cfg).await;
        let mut client = NodeServiceClient::connect(address).await.unwrap();
        let reopened = client.resolve_parents(scoped()).await.unwrap().into_inner();
        check_view(&reopened);
        assert_eq!(reopened.parents, after.parents);
        let stale = ResolveParentsRequest {
            expected_stats_epoch: after.stats_epoch,
            expected_stats_incarnation: after.stats_incarnation,
            ..scoped()
        };
        assert_eq!(
            client.resolve_parents(stale).await.unwrap_err().code(),
            Code::FailedPrecondition
        );
        server.abort();
        let _ = server.await;
        std::fs::remove_dir_all(dir).unwrap();
    }
}
#[tokio::test]
async fn vector_only_and_empty_shards_cannot_satisfy_a_document_view() {
    let (address, server) = common::start_empty_node(config()).await;
    let mut client = NodeServiceClient::connect(address).await.unwrap();
    for populate in [false, true] {
        if populate {
            let vectors = common::unit_vectors(2, 16, 9992);
            let (shift, scale) = common::fit_calibration(16, 4, &vectors);
            client
                .set_calibration(SetCalibrationRequest {
                    dim: 16,
                    bit_width: 4,
                    shift,
                    scale,
                })
                .await
                .unwrap();
            client
                .add_vectors(tokio_stream::iter([AddVectorsRequest { dim: 16, vectors }]))
                .await
                .unwrap();
        }
        let reply = client
            .resolve_parents(ResolveParentsRequest {
                doc_ids: vec![0],
                visibility: Some(view("!has(audience)")),
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert!(reply.parents.is_empty());
        assert_eq!(reply.visibility_columns_known, vec![false]);
        assert_eq!(reply.fields, vec![1, 2]);
        assert!(reply.stats_epoch > 0);
        let owner = client
            .resolve_parents(ResolveParentsRequest {
                doc_ids: vec![0],
                ..Default::default()
            })
            .await
            .unwrap()
            .into_inner();
        assert_eq!(owner.parents.len(), usize::from(populate));
        for (epoch, incarnation) in [
            (0, vec![1; 32]),
            (reply.stats_epoch, vec![]),
            (reply.stats_epoch, vec![1; 31]),
            (reply.stats_epoch, vec![1; 32]),
        ] {
            assert_eq!(
                client
                    .resolve_parents(ResolveParentsRequest {
                        expected_stats_epoch: epoch,
                        expected_stats_incarnation: incarnation,
                        ..scoped()
                    })
                    .await
                    .unwrap_err()
                    .code(),
                Code::FailedPrecondition
            );
        }
        assert_eq!(
            client
                .resolve_parents(ResolveParentsRequest {
                    visibility: Some(DocumentVisibility::default()),
                    ..Default::default()
                })
                .await
                .unwrap_err()
                .code(),
            Code::InvalidArgument
        );
    }
    server.abort();
    let _ = server.await;
}

#[derive(Clone)]
struct LineagePeer(Arc<std::sync::Mutex<ResolveParentsResponse>>);
impl tonic::server::NamedService for LineagePeer {
    const NAME: &'static str = "ai.protomolt.search.v1.NodeService";
}
impl<B> tonic::codegen::Service<tonic::codegen::http::Request<B>> for LineagePeer
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
            struct Unary(ResolveParentsResponse);
            impl tonic::server::UnaryService<ResolveParentsRequest> for Unary {
                type Response = ResolveParentsResponse;
                type Future =
                    std::future::Ready<Result<tonic::Response<Self::Response>, tonic::Status>>;
                fn call(&mut self, _: tonic::Request<ResolveParentsRequest>) -> Self::Future {
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
async fn lineage_collector_refuses_legacy_metadata_unrequested_keys_and_rows() {
    let good = ResolveParentsResponse {
        parents: vec![ResolvedParent {
            doc_id: 0,
            parent_id: 10,
            group_id: 0,
        }],
        fields: vec![1],
        stats_epoch: 1,
        stats_incarnation: vec![9; 32],
        ..Default::default()
    };
    let reply = Arc::new(std::sync::Mutex::new(good.clone()));
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = format!("http://{}", listener.local_addr().unwrap());
    let server = tokio::spawn(
        tonic::transport::Server::builder()
            .add_service(LineagePeer(reply.clone()))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
    );
    let coordinator = CoordinatorServiceImpl::new(vec![address.clone()]);
    assert_eq!(
        coordinator
            .lineage_key(&[0], "parent_id")
            .await
            .unwrap()
            .get(&0),
        Some(&10)
    );
    for case in 0..8 {
        let mut bad = good.clone();
        match case {
            0 => bad.stats_epoch = 0,
            1 => bad.stats_incarnation.clear(),
            2 => bad.visibility_fingerprint = vec![1; 32],
            3 => bad.visibility_columns_known = vec![true],
            4 => bad.fields.clear(),
            5 => bad.parents[0].group_id = 99,
            6 => bad.parents[0].doc_id = 1,
            _ => bad.parents.push(bad.parents[0].clone()),
        }
        *reply.lock().unwrap() = bad;
        assert_eq!(
            coordinator
                .lineage_key(&[0], "parent_id")
                .await
                .unwrap_err()
                .code(),
            Code::FailedPrecondition,
            "case {case}"
        );
    }
    *reply.lock().unwrap() = good;
    assert_eq!(
        CoordinatorServiceImpl::new(vec![address.clone(), address])
            .lineage_key(&[0], "parent_id")
            .await
            .unwrap_err()
            .code(),
        Code::FailedPrecondition
    );
    server.abort();
    let _ = server.await;
}
