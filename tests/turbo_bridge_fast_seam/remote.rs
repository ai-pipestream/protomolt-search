use pipestream_search::analyzer::{
    embed_text_with_options, embed_texts_with_options, RemoteCallOptions,
};
use pipestream_search::turbo_bridge::{BatchEmbedder, EmbedOptions, RemoteBatchEmbedder};
use std::sync::{
    atomic::{AtomicBool, AtomicUsize, Ordering},
    Arc,
};
use std::time::Duration;
use tonic::{Request, Response, Status};
use turbo_client::proto::{
    self as wire,
    grpc_inference_service_server::{GrpcInferenceService, GrpcInferenceServiceServer},
};

static RPC_TEST: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[derive(Default)]
struct Mock {
    metadata: AtomicUsize,
    inference: AtomicUsize,
    revision: AtomicUsize,
    fail_infer: AtomicBool,
    stall_metadata: AtomicBool,
    wider_metadata: AtomicBool,
    saw_timeout: AtomicBool,
    hold_metadata: AtomicBool,
    hold_inference: AtomicBool,
    metadata_gate: tokio::sync::Notify,
    inference_gate: tokio::sync::Notify,
    infer_meta: std::sync::Mutex<Option<tonic::metadata::MetadataMap>>,
    metadata_calls: std::sync::Mutex<Vec<tonic::metadata::MetadataMap>>,
}

#[derive(Clone)]
struct MockService(Arc<Mock>);

#[tonic::async_trait]
impl GrpcInferenceService for MockService {
    async fn server_live(
        &self,
        _: Request<wire::ServerLiveRequest>,
    ) -> Result<Response<wire::ServerLiveResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn server_ready(
        &self,
        _: Request<wire::ServerReadyRequest>,
    ) -> Result<Response<wire::ServerReadyResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn model_ready(
        &self,
        _: Request<wire::ModelReadyRequest>,
    ) -> Result<Response<wire::ModelReadyResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn server_metadata(
        &self,
        _: Request<wire::ServerMetadataRequest>,
    ) -> Result<Response<wire::ServerMetadataResponse>, Status> {
        Err(Status::unimplemented("unused"))
    }
    async fn model_metadata(
        &self,
        request: Request<wire::ModelMetadataRequest>,
    ) -> Result<Response<wire::ModelMetadataResponse>, Status> {
        self.0
            .metadata_calls
            .lock()
            .unwrap()
            .push(request.metadata().clone());
        self.0.metadata.fetch_add(1, Ordering::SeqCst);
        self.0.saw_timeout.store(
            request.metadata().contains_key("grpc-timeout"),
            Ordering::SeqCst,
        );
        if self.0.hold_metadata.load(Ordering::SeqCst) {
            self.0.metadata_gate.notified().await;
        }
        if self.0.stall_metadata.load(Ordering::SeqCst) {
            std::future::pending::<()>().await;
        }
        let properties = [
            ("model_info.model_id", request.get_ref().name.clone()),
            (
                "model_info.revision",
                self.0.revision.load(Ordering::SeqCst).to_string(),
            ),
            ("model_info.manifest_sha256", "a".repeat(64)),
            ("model_info.task", "embed".into()),
            (
                "model_info.dim",
                if self.0.wider_metadata.load(Ordering::SeqCst) {
                    "3"
                } else {
                    "2"
                }
                .into(),
            ),
            ("model_info.pooling", "mean".into()),
            ("model_info.normalize", "l2".into()),
        ]
        .into_iter()
        .map(|(key, value)| (key.to_owned(), value))
        .collect();
        Ok(Response::new(wire::ModelMetadataResponse {
            properties,
            ..Default::default()
        }))
    }
    async fn model_infer(
        &self,
        request: Request<wire::ModelInferRequest>,
    ) -> Result<Response<wire::ModelInferResponse>, Status> {
        *self.0.infer_meta.lock().unwrap() = Some(request.metadata().clone());
        self.0.inference.fetch_add(1, Ordering::SeqCst);
        if self.0.hold_inference.load(Ordering::SeqCst) {
            self.0.inference_gate.notified().await;
        }
        if self.0.fail_infer.swap(false, Ordering::SeqCst) {
            return Err(Status::unavailable("reconnect"));
        }
        Ok(Response::new(wire::ModelInferResponse {
            outputs: vec![wire::model_infer_response::InferOutputTensor {
                name: "embeddings".into(),
                datatype: "FP32".into(),
                shape: vec![1, 2],
                contents: Some(wire::InferTensorContents {
                    fp32_contents: vec![3.0, 4.0],
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        }))
    }
}

async fn server(mock: Arc<Mock>) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = format!("http://{}?model=fast-seam", listener.local_addr().unwrap());
    let task = tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(GrpcInferenceServiceServer::new(MockService(mock)))
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    (addr, task)
}

#[tokio::test]
async fn tofu_is_single_flight_and_rechecks_identity_after_transport_failure() {
    let _guard = RPC_TEST.lock().await;
    let mock = Arc::new(Mock::default());
    let (addr, task) = server(Arc::clone(&mock)).await;
    let options = RemoteCallOptions {
        timeout: Some(Duration::from_secs(2)),
        ..Default::default()
    };
    let mut calls = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let addr = addr.clone();
        let options = options.clone();
        calls.spawn(async move { embed_text_with_options(&addr, "query", &options).await });
    }
    while let Some(result) = calls.join_next().await {
        assert_eq!(result.unwrap().unwrap(), vec![0.6, 0.8]);
    }
    for _ in 0..3 {
        embed_text_with_options(&addr, "query", &options)
            .await
            .unwrap();
    }
    assert_eq!(mock.metadata.load(Ordering::SeqCst), 1);
    assert_eq!(mock.inference.load(Ordering::SeqCst), 11);
    assert!(mock.saw_timeout.load(Ordering::SeqCst));
    mock.fail_infer.store(true, Ordering::SeqCst);
    assert_eq!(
        embed_text_with_options(&addr, "query", &options)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::Unavailable
    );
    mock.revision.store(1, Ordering::SeqCst);
    assert_eq!(
        embed_text_with_options(&addr, "query", &options)
            .await
            .unwrap_err()
            .code(),
        tonic::Code::FailedPrecondition
    );
    assert_eq!(mock.metadata.load(Ordering::SeqCst), 2);
    assert_eq!(mock.inference.load(Ordering::SeqCst), 12);
    task.abort();
}

#[tokio::test]
async fn new_runtime_reprobes_but_call_budget_changes_reuse_verification() {
    let _guard = RPC_TEST.lock().await;
    let mock = Arc::new(Mock::default());
    let (addr, task) = server(Arc::clone(&mock)).await;
    let mut options = RemoteCallOptions::default();
    embed_text_with_options(&addr, "query", &options)
        .await
        .unwrap();
    options.timeout = Some(Duration::from_secs(2));
    options.interactive = !options.interactive;
    embed_text_with_options(&addr, "query", &options)
        .await
        .unwrap();
    assert_eq!(mock.metadata.load(Ordering::SeqCst), 1);
    mock.revision.store(1, Ordering::SeqCst);
    let error = tokio::task::spawn_blocking(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(embed_text_with_options(&addr, "query", &options))
            .unwrap_err()
    })
    .await
    .unwrap();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert_eq!(mock.metadata.load(Ordering::SeqCst), 2);
    assert_eq!(mock.inference.load(Ordering::SeqCst), 2);
    task.abort();
}

#[tokio::test]
async fn explicit_probes_invalidate_cached_tofu_without_replacing_its_pin() {
    let _guard = RPC_TEST.lock().await;
    for revision in [0, 1] {
        let mock = Arc::new(Mock::default());
        let (addr, task) = server(Arc::clone(&mock)).await;
        let options = RemoteCallOptions::default();
        embed_text_with_options(&addr, "query", &options)
            .await
            .unwrap();
        mock.revision.store(1, Ordering::SeqCst);
        let explicit = format!(
            "{addr}&sha256={}&dim=2&revision={revision}&pooling=mean&normalize=l2",
            "a".repeat(64)
        );
        let result = embed_text_with_options(&explicit, "query", &options).await;
        assert_eq!(result.is_ok(), revision == 1);
        assert_eq!(
            embed_text_with_options(&addr, "query", &options)
                .await
                .unwrap_err()
                .code(),
            tonic::Code::FailedPrecondition
        );
        assert_eq!(mock.metadata.load(Ordering::SeqCst), 3);
        assert_eq!(mock.inference.load(Ordering::SeqCst), 1 + revision);
        task.abort();
    }
}

#[tokio::test]
async fn initial_handshake_obeys_timeout_without_inference_or_pin() {
    let _guard = RPC_TEST.lock().await;
    let mock = Arc::new(Mock::default());
    mock.stall_metadata.store(true, Ordering::SeqCst);
    let (addr, task) = server(Arc::clone(&mock)).await;
    let options = RemoteCallOptions {
        timeout: Some(Duration::from_millis(100)),
        ..Default::default()
    };
    let result = tokio::time::timeout(
        Duration::from_secs(2),
        embed_text_with_options(&addr, "query", &options),
    )
    .await
    .unwrap();
    assert_eq!(result.unwrap_err().code(), tonic::Code::DeadlineExceeded);
    assert_eq!(mock.inference.load(Ordering::SeqCst), 0);
    mock.stall_metadata.store(false, Ordering::SeqCst);
    embed_text_with_options(&addr, "query", &options)
        .await
        .unwrap();
    assert_eq!(mock.metadata.load(Ordering::SeqCst), 2);
    assert_eq!(mock.inference.load(Ordering::SeqCst), 1);
    task.abort();
}

#[tokio::test]
async fn inferred_width_must_match_fresh_cached_and_explicit_metadata() {
    let _guard = RPC_TEST.lock().await;
    let mock = Arc::new(Mock::default());
    mock.wider_metadata.store(true, Ordering::SeqCst);
    let (addr, task) = server(Arc::clone(&mock)).await;
    let explicit = format!("{addr}&sha256={}&dim=3&revision=0", "a".repeat(64));
    for endpoint in [&addr, &addr, &explicit] {
        let error = embed_text_with_options(endpoint, "query", &RemoteCallOptions::default())
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error
            .message()
            .contains("dimension 2 differs from model metadata 3"));
    }
    assert_eq!(mock.metadata.load(Ordering::SeqCst), 2);
    assert_eq!(mock.inference.load(Ordering::SeqCst), 3);
    task.abort();
}

#[tokio::test]
async fn a_one_row_batch_is_the_single_text_embedding_bit_for_bit() {
    let _guard = RPC_TEST.lock().await;
    let (addr, task) = server(Arc::new(Mock::default())).await;
    let single = embed_text_with_options(&addr, "query", &RemoteCallOptions::default())
        .await
        .unwrap();
    let options = EmbedOptions::default();
    let batch = embed_texts_with_options(&addr, &["query"], &options)
        .await
        .unwrap();
    let seam = RemoteBatchEmbedder::new(&addr);
    assert_eq!(single, vec![0.6, 0.8]);
    assert_eq!(batch.data(), single.as_slice());
    assert_eq!(seam.embed_batch(&["query"], &options).await.unwrap(), batch);
    let empty = seam.embed_batch(&[], &options).await.unwrap_err();
    assert_eq!(empty.message(), "embed batch is empty");
    task.abort();
}

#[path = "remote/deadlines.rs"]
mod deadlines;
