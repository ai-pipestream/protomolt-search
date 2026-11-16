//! A WAL-backed primary and a replica that records which append RPCs
//! reach it, and the log each of them holds.

use crate::common;
use pipestream_search::derived::Declaration;
use pipestream_search::node::{NodeConfig, NodeServiceImpl};
use pipestream_search::pb::wal::{wal_record::Op, WalRecord};
use pipestream_search::pb::{self, node_service_client::NodeServiceClient};
use pipestream_search::replication::{
    encode_stable_keys, sync_once, ReplicaCursor, STABLE_KEYS_METADATA, STABLE_KEY_METADATA,
};
use prost::Message;
use std::sync::{Arc, Mutex};

pub(crate) const DIM: usize = 64;

/// A node that records which append RPCs reach it, in order.
#[derive(Clone)]
struct CountingReceiver {
    node: NodeServiceImpl,
    appends: Arc<Mutex<Vec<&'static str>>>,
}
impl tonic::server::NamedService for CountingReceiver {
    const NAME: &'static str = "ai.protomolt.search.v1.NodeService";
}
impl<B> tonic::codegen::Service<tonic::codegen::http::Request<B>> for CountingReceiver
where
    B: http_body::Body + Send + 'static,
    B::Error: Into<tonic::codegen::StdError> + Send + 'static,
{
    type Response = tonic::codegen::http::Response<tonic::body::Body>;
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
        for name in ["AddDocuments", "AddVectors"] {
            if request.uri().path().ends_with(&format!("/{name}")) {
                self.appends.lock().unwrap().push(name);
            }
        }
        let node = self.node.clone();
        Box::pin(pb::node_service_server::NodeServiceServer::new(node).call(request))
    }
}

/// A primary and a counting replica over fresh WAL-backed shards.
pub(crate) struct Pair {
    pub(crate) cursor: ReplicaCursor,
    pub(crate) source: NodeServiceClient<tonic::transport::Channel>,
    appends: Arc<Mutex<Vec<&'static str>>>,
}

impl Pair {
    pub(crate) async fn start(tag: &str, vectors: bool) -> Self {
        Self::open(tag, vectors, None).await
    }

    /// A pair whose shards both declare `declaration` over a `money` map
    /// of numerics.
    pub(crate) async fn start_derived(tag: &str, declaration: Arc<Declaration>) -> Self {
        Self::open(tag, false, Some(declaration)).await
    }

    async fn open(tag: &str, vectors: bool, derived: Option<Arc<Declaration>>) -> Self {
        let root = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
            .join(format!("replication-batch-{tag}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(&root).unwrap();
        let config = |name: &str| NodeConfig {
            index_path: Some(root.join(format!("{name}.tv"))),
            layout: pipestream_search::node::Layout::SingleImage,
            wal: true,
            analysis_addr: Some(pipestream_search::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
            map_numeric_fields: vec!["money".into()],
            derived: derived.clone(),
            ..Default::default()
        };
        let (primary, _) = common::start_empty_node(config("source")).await;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let replica = format!("http://{}", listener.local_addr().unwrap());
        let appends = Arc::new(Mutex::new(Vec::new()));
        let receiver = CountingReceiver {
            node: NodeServiceImpl::new(None, config("receiver")),
            appends: appends.clone(),
        };
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_service(receiver)
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(listener)),
        );
        let sample = common::unit_vectors(64, DIM, 0xB17_0006);
        let (shift, scale) = common::fit_calibration(DIM, 4, &sample);
        for addr in [&primary, &replica].into_iter().filter(|_| vectors) {
            NodeServiceClient::connect(addr.clone())
                .await
                .unwrap()
                .configure_vector_backend(common::embedded_backend_request(DIM, 4, &shift, &scale))
                .await
                .unwrap();
        }
        Self {
            source: NodeServiceClient::connect(primary.clone()).await.unwrap(),
            cursor: ReplicaCursor {
                primary,
                replica,
                ..Default::default()
            },
            appends,
        }
    }

    /// Append `texts` to the primary, one logged record per document; a
    /// `key` sends each document in its own stream under that stable key.
    pub(crate) async fn documents(&mut self, texts: std::ops::Range<usize>, key: Option<&[u8]>) {
        let documents = texts.map(document);
        let Some(key) = key else {
            let stream = tokio_stream::iter(documents.collect::<Vec<_>>());
            self.source.add_documents(stream).await.unwrap();
            return;
        };
        for document in documents {
            let mut request = tonic::Request::new(tokio_stream::iter([document]));
            request.metadata_mut().insert_bin(
                STABLE_KEY_METADATA,
                tonic::metadata::MetadataValue::from_bytes(key),
            );
            self.source.add_documents(request).await.unwrap();
        }
    }

    /// Append `documents` to the primary in one stream, each under its own
    /// stable key.
    pub(crate) async fn keyed(
        &mut self,
        documents: Vec<pb::AddDocumentsRequest>,
        keys: &[Vec<u8>],
    ) {
        let mut request = tonic::Request::new(tokio_stream::iter(documents));
        request.metadata_mut().insert_bin(
            STABLE_KEYS_METADATA,
            tonic::metadata::MetadataValue::from_bytes(&encode_stable_keys(keys).unwrap()),
        );
        self.source.add_documents(request).await.unwrap();
    }

    /// Append one logged record of `rows` vectors to the primary, under
    /// `key` when one is given.
    pub(crate) async fn vectors(&mut self, rows: usize, seed: u64, key: Option<&[u8]>) {
        let batch = pb::AddVectorsRequest {
            materialized_rows: Vec::new(),
            vectors: common::unit_vectors(rows, DIM, seed),
            dim: DIM as u32,
        };
        let mut request = tonic::Request::new(tokio_stream::iter([batch]));
        if let Some(key) = key {
            request.metadata_mut().insert_bin(
                STABLE_KEY_METADATA,
                tonic::metadata::MetadataValue::from_bytes(key),
            );
        }
        self.source.add_vectors(request).await.unwrap();
    }

    /// Flush the primary, catch the replica up from `from`, and hold the
    /// replica's append log to the primary's, record for record and byte
    /// for byte.
    pub(crate) async fn sync(&mut self, from: &ReplicaCursor) -> ReplicaCursor {
        self.source.flush(pb::FlushRequest {}).await.unwrap();
        let advanced = sync_once(from).await.unwrap();
        let logged = appended(&self.cursor.primary).await;
        assert!(!logged.is_empty());
        let replicated = appended(&self.cursor.replica).await;
        assert_eq!(encoded(&replicated), encoded(&logged));
        assert_eq!(replicated, logged);
        advanced
    }

    pub(crate) fn appends(&self) -> Vec<&'static str> {
        self.appends.lock().unwrap().clone()
    }
}

/// Document `i` of the replicated log.
pub(crate) fn document(i: usize) -> pb::AddDocumentsRequest {
    pb::AddDocumentsRequest {
        text: format!("document {i} of the replicated log"),
        analysis: Some(pipestream_search::analyzer::body_spec()),
        ..Default::default()
    }
}

/// The stable key of row `i`, distinct per row.
pub(crate) fn key(i: usize) -> Vec<u8> {
    format!("stable-{i}").into_bytes()
}

/// Each append record's operation as it would be logged, without the
/// node-local sequence and clock.
fn encoded(ops: &[Op]) -> Vec<Vec<u8>> {
    ops.iter()
        .map(|op| {
            WalRecord {
                op: Some(op.clone()),
                ..Default::default()
            }
            .encode_to_vec()
        })
        .collect()
}

/// The append records of a node's flushed WAL, in log order.
pub(crate) async fn appended(addr: &str) -> Vec<Op> {
    let mut client = NodeServiceClient::connect(addr.to_string()).await.unwrap();
    let health = client.health(pb::HealthRequest {}).await.unwrap();
    let request = pb::ReadWalRequest {
        generation: health.into_inner().wal_generation,
        after_clock: 0,
    };
    let mut stream = client.read_wal(request).await.unwrap().into_inner();
    let mut ops = Vec::new();
    while let Some(frame) = stream.message().await.unwrap() {
        if frame.completed {
            continue;
        }
        let record = WalRecord::decode(frame.record.as_slice()).unwrap();
        if let Some(op @ (Op::AddDocuments(_) | Op::AddVectors(_))) = record.op {
            ops.push(op);
        }
    }
    ops
}
