//! Product-side adapter for one distributed TurboVec collection.
//!
//! The in-process transport embeds `turbovec-grpc`'s coordinator library and
//! calls its generated service implementation directly. There is no
//! product-to-coordinator socket or protobuf encode/decode hop. The optional
//! external transport calls the same coordinator contract over tonic for
//! deployments that need an independently managed coordinator process.

use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::watch;
use tokio_stream::wrappers::WatchStream;
use tokio_stream::{Stream, StreamExt};
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};
use turbovec_grpc::proto::coordinator_client::CoordinatorClient;
use turbovec_grpc::proto::coordinator_server::Coordinator;
use turbovec_grpc::proto::{
    collection_candidate_request, collection_candidate_response, CollectionCandidateCompletion,
    CollectionCandidateRequest, CollectionCandidateResponse, CollectionQualityContract,
    CollectionSearchRequest, CollectionSearchResponse, FloorUpdate, LabelBitmap, ListNodesRequest,
    ListNodesResponse, StartCollectionCandidates, StopStreamSearch,
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

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ClusteredCandidate {
    pub label: u64,
    pub score: f32,
}

pub enum ClusteredCandidateEvent {
    Batch(Vec<ClusteredCandidate>),
    Completion(CollectionCandidateCompletion),
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ClusteredQualityIdentity {
    pub rows: u64,
    pub topology_generation: u64,
    pub dimensions: u32,
    pub scoring_fingerprint: String,
}

type CandidateResponses =
    Pin<Box<dyn Stream<Item = Result<CollectionCandidateResponse, Status>> + Send + 'static>>;

/// One provider scan controlled by the product's live heap.
///
/// The watch cell is deliberately conflating: several raises before the
/// transport polls again collapse to the newest floor, while the provider
/// still receives a monotonic sequence.
pub struct ClusteredCandidateStream {
    responses: CandidateResponses,
    requests: watch::Sender<CollectionCandidateRequest>,
    last_floor: f32,
    terminal: bool,
}

impl ClusteredCandidateStream {
    pub fn raise_floor(&mut self, floor: f32) -> Result<(), Status> {
        if floor.is_nan() {
            return Err(Status::invalid_argument("candidate floor must not be NaN"));
        }
        if self.terminal || floor <= self.last_floor {
            return Ok(());
        }
        self.last_floor = floor;
        self.requests.send_replace(CollectionCandidateRequest {
            payload: Some(collection_candidate_request::Payload::FloorUpdate(
                FloorUpdate { floor },
            )),
        });
        Ok(())
    }

    pub fn cancel(&mut self) {
        if !self.terminal {
            self.requests.send_replace(CollectionCandidateRequest {
                payload: Some(collection_candidate_request::Payload::Stop(
                    StopStreamSearch {},
                )),
            });
        }
    }

    pub async fn next_event(&mut self) -> Result<ClusteredCandidateEvent, Status> {
        if self.terminal {
            return Err(Status::failed_precondition(
                "candidate stream was read after its terminal completion",
            ));
        }
        let response = self.responses.next().await.ok_or_else(|| {
            Status::aborted("clustered TurboVec candidate stream ended without completion")
        })??;
        match response.payload {
            Some(collection_candidate_response::Payload::Batch(batch)) => {
                if batch.candidates.is_empty() || batch.candidates.len() % 12 != 0 {
                    return Err(Status::internal(format!(
                        "clustered TurboVec returned a misaligned candidate batch of {} bytes",
                        batch.candidates.len()
                    )));
                }
                let candidates = batch
                    .candidates
                    .chunks_exact(12)
                    .map(|record| ClusteredCandidate {
                        label: u64::from_le_bytes(record[..8].try_into().expect("8-byte label")),
                        score: f32::from_le_bytes(record[8..12].try_into().expect("4-byte score")),
                    })
                    .collect();
                Ok(ClusteredCandidateEvent::Batch(candidates))
            }
            Some(collection_candidate_response::Payload::Completion(completion)) => {
                self.terminal = true;
                if !completion.completed {
                    return Err(Status::aborted(format!(
                        "clustered TurboVec candidate scan incomplete: {}",
                        completion.error
                    )));
                }
                if completion.shards_completed != completion.shards_total {
                    return Err(Status::internal(format!(
                        "clustered TurboVec certified {} of {} shards",
                        completion.shards_completed, completion.shards_total
                    )));
                }
                if completion.scoring_fingerprint.is_empty() {
                    return Err(Status::failed_precondition(
                        "clustered TurboVec completed without a scoring fingerprint",
                    ));
                }
                if completion.quality_contract
                    != CollectionQualityContract::ExhaustiveQuantized as i32
                {
                    return Err(Status::failed_precondition(format!(
                        "clustered TurboVec quality contract {} is not exhaustive quantized search",
                        completion.quality_contract
                    )));
                }
                Ok(ClusteredCandidateEvent::Completion(completion))
            }
            None => Err(Status::internal(
                "clustered TurboVec returned an empty candidate response",
            )),
        }
    }
}

impl Drop for ClusteredCandidateStream {
    fn drop(&mut self) {
        self.cancel();
    }
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

    /// Open the provider candidate stream used when the product, rather than
    /// the vector collection, owns the ranking heap or grouping semantics.
    pub async fn candidate_stream(
        &self,
        request_id: &str,
        vector: Vec<f32>,
        allowed_labels: Option<ClusteredLabelFilter>,
        initial_floor: Option<f32>,
    ) -> Result<ClusteredCandidateStream, Status> {
        let has_allowed_labels = allowed_labels.is_some();
        let (allowed_labels, allowed_label_bitmaps) = match allowed_labels {
            Some(ClusteredLabelFilter::Labels(labels)) => (labels, Vec::new()),
            Some(ClusteredLabelFilter::Bitmaps(bitmaps)) => (Vec::new(), bitmaps),
            None => (Vec::new(), Vec::new()),
        };
        let start = CollectionCandidateRequest {
            payload: Some(collection_candidate_request::Payload::Start(
                StartCollectionCandidates {
                    request_id: request_id.to_string(),
                    vector,
                    initial_floor,
                    allowed_labels,
                    has_allowed_labels,
                    allowed_label_bitmaps,
                },
            )),
        };
        let (requests, request_rx) = watch::channel(start);
        let responses: CandidateResponses = match &self.transport {
            ClusteredTurboVecTransport::InProcess(coordinator) => {
                Box::pin(coordinator.candidate_stream(WatchStream::new(request_rx).map(Ok)))
            }
            ClusteredTurboVecTransport::External(client) => {
                let mut client = client.clone();
                Box::pin(
                    client
                        .stream_candidates(WatchStream::new(request_rx))
                        .await?
                        .into_inner(),
                )
            }
        };
        Ok(ClusteredCandidateStream {
            responses,
            requests,
            last_floor: initial_floor.unwrap_or(f32::NEG_INFINITY),
            terminal: false,
        })
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

    /// Runtime identity used to validate a measured candidate-depth profile.
    /// This repeats turbovec-grpc's documented collection fingerprint
    /// encoding so the preflight identity is exactly the one a successful
    /// candidate completion later certifies.
    pub async fn quality_identity(&self) -> Result<ClusteredQualityIdentity, Status> {
        let health = self.health().await?;
        if !health.servable {
            return Err(Status::failed_precondition(format!(
                "clustered TurboVec is not servable: {}",
                health.error
            )));
        }
        let first = health.shards.first().ok_or_else(|| {
            Status::failed_precondition("clustered TurboVec has no configured shards")
        })?;
        let info = first.info.as_ref().ok_or_else(|| {
            Status::failed_precondition("clustered TurboVec first shard has no index metadata")
        })?;
        let calibration = first.calibration.as_ref().ok_or_else(|| {
            Status::failed_precondition("clustered TurboVec first shard has no calibration")
        })?;
        let mut digest = crate::sha256::Sha256::new();
        digest.update(b"turbovec-grpc-score-v1\0");
        digest.update(&(u64::from(info.dim)).to_le_bytes());
        digest.update(&info.bit_width.to_le_bytes());
        digest.update(&calibration.state.to_le_bytes());
        digest.update(&(calibration.tqplus_shift.len() as u64).to_le_bytes());
        for value in &calibration.tqplus_shift {
            digest.update(&value.to_bits().to_le_bytes());
        }
        digest.update(&(calibration.tqplus_scale.len() as u64).to_le_bytes());
        for value in &calibration.tqplus_scale {
            digest.update(&value.to_bits().to_le_bytes());
        }
        Ok(ClusteredQualityIdentity {
            rows: health.rows,
            topology_generation: health.topology_generation,
            dimensions: info.dim,
            scoring_fingerprint: crate::sha256::to_hex(&digest.finalize()),
        })
    }

    pub fn transport_name(&self) -> &'static str {
        match self.transport {
            ClusteredTurboVecTransport::InProcess(_) => "in-process",
            ClusteredTurboVecTransport::External(_) => "external",
        }
    }
}
