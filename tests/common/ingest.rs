//! Stream document rows to one node's `AddDocuments`.

use pipestream_search::pb::node_service_client::NodeServiceClient;
use pipestream_search::pb::{AddDocumentsRequest, AddDocumentsResponse};

/// Send `docs` to the node at `addr` as one `AddDocuments` stream.
pub async fn ingest(
    addr: &str,
    docs: Vec<AddDocumentsRequest>,
) -> Result<AddDocumentsResponse, tonic::Status> {
    let mut client = NodeServiceClient::connect(addr.to_string())
        .await
        .map_err(|e| tonic::Status::unavailable(format!("connect {addr}: {e}")))?;
    client
        .add_documents(tokio_stream::iter(docs))
        .await
        .map(tonic::Response::into_inner)
}
