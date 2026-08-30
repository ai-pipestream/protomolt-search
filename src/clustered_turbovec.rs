//! Product-side adapter for one distributed TurboVec collection.
//!
//! The in-process transport embeds `turbovec-grpc`'s coordinator library and
//! calls its generated service implementation directly. There is no
//! product-to-coordinator socket or protobuf encode/decode hop. The optional
//! external transport calls the same coordinator contract over tonic for
//! deployments that need an independently managed coordinator process.

use std::sync::Arc;

use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};
use turbovec_grpc::proto::coordinator_client::CoordinatorClient;
use turbovec_grpc::proto::coordinator_server::Coordinator;
use turbovec_grpc::proto::{
    CollectionSearchRequest, CollectionSearchResponse, LabelBitmap, ListNodesRequest,
    ListNodesResponse,
};
use turbovec_grpc::CoordinatorService;

/// The two transports for the same distributed collection contract.
#[derive(Clone)]
pub enum ClusteredTurboVecTransport {
    /// Coordinator state and the global heap live in this process. Only its
    /// shard-node fan-out crosses the network.
    InProcess(Arc<CoordinatorService>),
    /// The coordinator has an independent lifecycle and is reached over gRPC.
    External(CoordinatorClient<Channel>),
}

impl std::fmt::Debug for ClusteredTurboVecTransport {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(match self {
            Self::InProcess(_) => "InProcess",
            Self::External(_) => "External",
        })
    }
}

/// One clustered TurboVec vector backend at the product coordinator.
#[derive(Clone, Debug)]
pub struct ClusteredTurboVecBackend {
    transport: ClusteredTurboVecTransport,
}

/// Stable product ids admitted to one provider query.
#[derive(Clone, Debug)]
pub enum ClusteredLabelFilter {
    /// Compact for a small candidate set such as dense rescoring.
    Labels(Vec<u64>),
    /// One packed stable-id range per product shard, suitable for broad CEL
    /// and geo filters without expanding one integer per match.
    Bitmaps(Vec<LabelBitmap>),
}

impl ClusteredTurboVecBackend {
    /// Embed an already-configured coordinator service in this process.
    pub fn in_process(coordinator: CoordinatorService) -> Self {
        Self {
            transport: ClusteredTurboVecTransport::InProcess(Arc::new(coordinator)),
        }
    }

    /// Connect lazily to a separately managed coordinator.
    pub fn external(endpoint: &str, max_message_bytes: usize) -> Result<Self, String> {
        let endpoint = Endpoint::from_shared(endpoint.to_string()).map_err(|error| {
            format!("invalid TurboVec coordinator endpoint {endpoint:?}: {error}")
        })?;
        let client = CoordinatorClient::new(endpoint.connect_lazy())
            .max_decoding_message_size(max_message_bytes)
            .max_encoding_message_size(max_message_bytes);
        Ok(Self {
            transport: ClusteredTurboVecTransport::External(client),
        })
    }

    /// Search the logical collection under the product's stable-id order.
    /// An explicitly present empty label list or bitmap list deliberately
    /// admits no rows; `None` means the whole collection.
    pub async fn search(
        &self,
        queries: Vec<f32>,
        k: u32,
        allowed_labels: Option<ClusteredLabelFilter>,
        initial_floor: Option<f32>,
        tie_complete: bool,
    ) -> Result<CollectionSearchResponse, Status> {
        let has_allowed_labels = allowed_labels.is_some();
        let (allowed_labels, allowed_label_bitmaps) = match allowed_labels {
            Some(ClusteredLabelFilter::Labels(labels)) => (labels, Vec::new()),
            Some(ClusteredLabelFilter::Bitmaps(bitmaps)) => (Vec::new(), bitmaps),
            None => (Vec::new(), Vec::new()),
        };
        let request = CollectionSearchRequest {
            queries,
            k,
            stable_label_order: true,
            has_allowed_labels,
            allowed_labels,
            initial_floor,
            tie_complete,
            allowed_label_bitmaps,
        };
        match &self.transport {
            ClusteredTurboVecTransport::InProcess(coordinator) => {
                Coordinator::search(coordinator.as_ref(), Request::new(request))
                    .await
                    .map(tonic::Response::into_inner)
            }
            ClusteredTurboVecTransport::External(client) => {
                let mut client = client.clone();
                client
                    .search(request)
                    .await
                    .map(tonic::Response::into_inner)
            }
        }
    }

    /// Probe the collection through the same transport used for search.
    pub async fn health(&self) -> Result<ListNodesResponse, Status> {
        match &self.transport {
            ClusteredTurboVecTransport::InProcess(coordinator) => {
                Coordinator::list_nodes(coordinator.as_ref(), Request::new(ListNodesRequest {}))
                    .await
                    .map(tonic::Response::into_inner)
            }
            ClusteredTurboVecTransport::External(client) => {
                let mut client = client.clone();
                client
                    .list_nodes(ListNodesRequest {})
                    .await
                    .map(tonic::Response::into_inner)
            }
        }
    }

    pub fn transport_name(&self) -> &'static str {
        match self.transport {
            ClusteredTurboVecTransport::InProcess(_) => "in-process",
            ClusteredTurboVecTransport::External(_) => "external",
        }
    }
}
