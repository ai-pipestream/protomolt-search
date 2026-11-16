//! Start one node of a test fleet and load it: an optional vector backend,
//! its documents, then its vectors (so document ids and vector slots align).

use pipestream_search::harness::start_empty_node;
use pipestream_search::node::NodeConfig;
use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::{
    AddDocumentsRequest, AddVectorsRequest, ConfigureVectorBackendRequest,
};
use tokio::task::JoinHandle;

type NodeTask = JoinHandle<Result<(), tonic::transport::Error>>;

/// An abort-on-drop guard over a started node or mock-sidecar serving task,
/// ensuring panicked tests stop the server before any `TestDir` unlinks its
/// shard directory.
pub struct ShardGuard(NodeTask);

impl From<NodeTask> for ShardGuard {
    fn from(handle: NodeTask) -> Self {
        Self(handle)
    }
}

impl std::ops::Deref for ShardGuard {
    type Target = NodeTask;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Drop for ShardGuard {
    fn drop(&mut self) {
        self.0.abort();
    }
}

/// One shard of a test fleet: its node configuration and what it holds.
pub struct ShardSpec {
    pub config: NodeConfig,
    pub backend: Option<ConfigureVectorBackendRequest>,
    pub docs: Vec<AddDocumentsRequest>,
    /// Row-major vectors of `dim` floats, one per document; empty for none.
    pub vectors: Vec<f32>,
    pub dim: u32,
}

/// Start the shard's node and load it, returning its address and abort guard.
pub async fn start_shard(spec: ShardSpec) -> (String, ShardGuard) {
    let (addr, handle) = start_empty_node(spec.config).await;
    let guard = ShardGuard(handle);
    let mut client = NodeServiceClient::connect(addr.clone()).await.unwrap();
    if let Some(backend) = spec.backend {
        client.configure_vector_backend(backend).await.unwrap();
    }
    client
        .add_documents(tokio_stream::iter(spec.docs))
        .await
        .unwrap();
    if !spec.vectors.is_empty() {
        client
            .add_vectors(tokio_stream::iter([AddVectorsRequest {
                materialized_rows: Vec::new(),
                vectors: spec.vectors,
                dim: spec.dim,
            }]))
            .await
            .unwrap();
    }
    (addr, guard)
}
