mod common;

use pipestream_search::{
    node::{Layout, NodeConfig, NodeServiceImpl},
    pb::{self, node_service_client::NodeServiceClient, node_service_server::NodeService},
    postings::Bm25Reader,
    replication::{sync_once, ReplicaCursor},
};
use std::sync::{
    atomic::{AtomicU8, AtomicUsize, Ordering},
    Arc,
};

type Reply<T> = std::pin::Pin<
    Box<dyn std::future::Future<Output = Result<tonic::Response<T>, tonic::Status>> + Send>,
>;

#[derive(Clone)]
struct VersionedReceiver {
    node: NodeServiceImpl,
    mode: Arc<AtomicU8>,
    ingests: Arc<AtomicUsize>,
}
impl tonic::server::NamedService for VersionedReceiver {
    const NAME: &'static str = "ai.protomolt.search.v1.NodeService";
}
impl<B> tonic::codegen::Service<tonic::codegen::http::Request<B>> for VersionedReceiver
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
        let node = self.node.clone();
        let mode = self.mode.load(Ordering::SeqCst);
        if request.uri().path().ends_with("/Health") && mode == 0 {
            return Box::pin(async move {
                struct Health(NodeServiceImpl);
                impl tonic::server::UnaryService<pb::HealthRequest> for Health {
                    type Response = pb::HealthResponse;
                    type Future = Reply<Self::Response>;
                    fn call(&mut self, request: tonic::Request<pb::HealthRequest>) -> Self::Future {
                        let node = self.0.clone();
                        Box::pin(async move {
                            let mut response = node.health(request).await?;
                            response.get_mut().document_contract_version = 0;
                            Ok(response)
                        })
                    }
                }
                Ok(
                    tonic::server::Grpc::new(tonic::codec::ProstCodec::default())
                        .unary(Health(node), request)
                        .await,
                )
            });
        }
        if request.uri().path().ends_with("/AddDocuments") {
            self.ingests.fetch_add(1, Ordering::SeqCst);
            if mode == 1 {
                return Box::pin(async move {
                    struct Ingest(NodeServiceImpl);
                    impl tonic::server::ClientStreamingService<pb::AddDocumentsRequest> for Ingest {
                        type Response = pb::AddDocumentsResponse;
                        type Future = Reply<Self::Response>;
                        fn call(
                            &mut self,
                            request: tonic::Request<tonic::Streaming<pb::AddDocumentsRequest>>,
                        ) -> Self::Future {
                            let node = self.0.clone();
                            Box::pin(async move {
                                let mut response = node.add_documents(request).await?;
                                response.get_mut().document_contract_version = 0;
                                Ok(response)
                            })
                        }
                    }
                    Ok(
                        tonic::server::Grpc::new(tonic::codec::ProstCodec::default())
                            .client_streaming(Ingest(node), request)
                            .await,
                    )
                });
            }
        }
        Box::pin(pb::node_service_server::NodeServiceServer::new(node).call(request))
    }
}

#[tokio::test]
async fn replication_requires_advertisement_and_acknowledgement_before_advancing() {
    let root = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("integer-map-replication-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    for layout in [Layout::SingleImage, Layout::Segments] {
        let config = |name: &str| NodeConfig {
            index_path: Some(root.join(format!("{name}-{layout:?}.tv"))),
            layout,
            wal: true,
            analysis_addr: Some(pipestream_search::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
            map_integer_fields: vec!["signed".into()],
            map_unsigned_integer_fields: vec!["unsigned".into()],
            ..Default::default()
        };
        let (primary, source_server) = common::start_empty_node(config("source")).await;
        let mut source = NodeServiceClient::connect(primary.clone()).await.unwrap();
        let document = pb::AddDocumentsRequest {
            text: "map".into(),
            analysis: Some(pipestream_search::analyzer::body_spec()),
            map_integers: vec![pb::MapIntegerEntry {
                field: "signed".into(),
                key: String::new(),
                value: i64::MIN,
            }],
            map_unsigned_integers: vec![pb::MapUnsignedIntegerEntry {
                field: "unsigned".into(),
                key: String::new(),
                value: u64::MAX,
            }],
            original_source: Some(common::protobuf_source("map", "original")),
            identity: Some(pb::DocumentIdentity {
                document_key: vec![0, 255],
                version: 7,
                chunk_ordinal: None,
            }),
            ..Default::default()
        };
        source
            .add_documents(tokio_stream::iter([document.clone()]))
            .await
            .unwrap();
        source.flush(pb::FlushRequest {}).await.unwrap();
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let replica = format!("http://{}", listener.local_addr().unwrap());
        let mode = Arc::new(AtomicU8::new(0));
        let ingests = Arc::new(AtomicUsize::new(0));
        let receiver_config = config("receiver");
        let receiver = VersionedReceiver {
            node: NodeServiceImpl::new(None, receiver_config.clone()),
            mode: mode.clone(),
            ingests: ingests.clone(),
        };
        let server = tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(receiver)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        let cursor = ReplicaCursor {
            primary,
            replica: replica.clone(),
            ..Default::default()
        };
        let error = sync_once(&cursor).await.unwrap_err();
        assert!(error.contains("document contract version 0"), "{error}");
        assert_eq!(
            ingests.load(Ordering::SeqCst),
            0,
            "old capability refused before sending documents"
        );
        mode.store(1, Ordering::SeqCst);
        let error = sync_once(&cursor).await.unwrap_err();
        assert!(error.contains("document contract version 0"), "{error}");
        assert_eq!(ingests.load(Ordering::SeqCst), 1);
        mode.store(2, Ordering::SeqCst);
        let advanced = sync_once(&cursor).await.unwrap();
        assert!(advanced.clock > 0);
        assert_eq!(
            ingests.load(Ordering::SeqCst),
            1,
            "retry must not duplicate accepted rows"
        );
        assert_eq!(sync_once(&advanced).await.unwrap(), advanced);
        let check = |reader: &Bm25Reader| {
            let si = reader.map_integer_index("signed").unwrap();
            let ui = reader.map_unsigned_integer_index("unsigned").unwrap();
            assert_eq!(
                reader.map_integer_value(si, reader.map_integer_key_ord(si, "").unwrap(), 0),
                Some(i64::MIN)
            );
            assert_eq!(
                reader.map_unsigned_integer_value(
                    ui,
                    reader.map_unsigned_integer_key_ord(ui, "").unwrap(),
                    0
                ),
                Some(u64::MAX)
            );
            assert_eq!(reader.document_identity(0), document.identity);
            assert_eq!(
                reader.protobuf_source(0).unwrap(),
                document.original_source.clone().map(|s| (s, None))
            );
        };
        match layout {
            Layout::SingleImage => check(
                &Bm25Reader::open(&pipestream_search::node::bm25_sidecar_path(
                    receiver_config.index_path.as_ref().unwrap(),
                ))
                .unwrap(),
            ),
            Layout::Segments => {
                let set = pipestream_search::segments::OpenedSegmentSet::open(
                    pipestream_search::node::segments_root(
                        receiver_config.index_path.as_ref().unwrap(),
                    ),
                )
                .unwrap();
                assert_eq!(set.len(), 1);
                check(set.bm25(0));
            }
        }
        server.abort();
        source_server.abort();
        let _ = server.await;
        let _ = source_server.await;
    }
    std::fs::remove_dir_all(root).unwrap();
}
