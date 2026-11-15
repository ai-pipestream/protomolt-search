//! Producing a dense quality profile from measurement.
//!
//! Everything here goes through the public `Query` route the profile
//! will later serve: for every `k` and every candidate depth on the
//! ladder, each held-out query runs `DENSE_SCORE_MODE_FP32_RERANK` at
//! `selection_k = depth` with `profile = true`, and its top-`k` is compared
//! with the exhaustive FP32 top-`k` (the ground truth). Recall is counted
//! per query, so the ladder records the worst query as well as the mean;
//! [`super::choose_points`] then picks the smallest depth whose WORST
//! query meets each target. `examples/dense_profile.rs` is the CLI shell
//! over [`measure`]; the tests run it against the in-process harness.

use std::future::Future;

use tonic::{Request, Status};

use super::{DenseQualityProfile, ProfileMeasurement, UnmetTarget};
use crate::pb::search_service_server::SearchService;
use crate::pb::{ClusterHealthRequest, ClusterHealthResponse, QueryRequest, QueryResponse};

mod brute;
mod identity;
mod ladder;
mod report;

pub use self::identity::live_identity;
pub use self::ladder::measure;
pub use self::report::{describe_unmet, ladder_table};

/// The two public RPCs the measurement needs, over whichever transport
/// reaches the coordinator: the gRPC client (the CLI) or the handler
/// itself in-process (the tests). Both are the same route.
pub trait ProfileRoute {
    fn query(
        &mut self,
        request: QueryRequest,
    ) -> impl Future<Output = Result<QueryResponse, Status>> + Send;
    fn cluster_health(
        &mut self,
        collection: String,
    ) -> impl Future<Output = Result<ClusterHealthResponse, Status>> + Send;
}

impl ProfileRoute for crate::coordinator::CoordinatorServiceImpl {
    async fn query(&mut self, request: QueryRequest) -> Result<QueryResponse, Status> {
        SearchService::query(&*self, Request::new(request))
            .await
            .map(|reply| reply.into_inner())
    }

    async fn cluster_health(
        &mut self,
        collection: String,
    ) -> Result<ClusterHealthResponse, Status> {
        SearchService::cluster_health(&*self, Request::new(ClusterHealthRequest { collection }))
            .await
            .map(|reply| reply.into_inner())
    }
}

/// The gRPC client over any transport the generated client accepts: a
/// plain channel, or one behind the tools' bearer interceptor
/// (`security::PublicChannel`).
#[cfg(feature = "net")]
impl<T> ProfileRoute for crate::pb::search_service_client::SearchServiceClient<T>
where
    T: tonic::client::GrpcService<tonic::body::Body> + Send,
    T::Future: Send,
    T::Error: Into<Box<dyn std::error::Error + Send + Sync>>,
    T::ResponseBody: http_body::Body<Data = bytes::Bytes> + Send + 'static,
    <T::ResponseBody as http_body::Body>::Error:
        Into<Box<dyn std::error::Error + Send + Sync>> + Send,
{
    async fn query(&mut self, request: QueryRequest) -> Result<QueryResponse, Status> {
        crate::pb::search_service_client::SearchServiceClient::query(self, request)
            .await
            .map(|reply| reply.into_inner())
    }

    async fn cluster_health(
        &mut self,
        collection: String,
    ) -> Result<ClusterHealthResponse, Status> {
        crate::pb::search_service_client::SearchServiceClient::cluster_health(
            self,
            ClusterHealthRequest { collection },
        )
        .await
        .map(|reply| reply.into_inner())
    }
}

/// The identity of the generation being measured, read from
/// `ClusterHealth`: what the profile binds to and what the coordinator's
/// quality preflight later checks it against.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LiveIdentity {
    pub provider_backend: String,
    pub scoring_fingerprint: String,
    pub dimensions: u32,
    pub rows: u64,
    pub topology_generation: u64,
}

/// Where the exhaustive FP32 top-`k` comes from.
pub enum GroundTruth<'a> {
    /// The public route itself at `selection_k = corpus rows`: FP32 rerank
    /// over every row IS the exhaustive order. Needs `rows <= max_k`.
    FullDepth,
    /// An exhaustive FP32 top-`k` over these rows (`rows * dimensions`
    /// floats, row `i` = global doc id `i`), the same dot product the
    /// rerank uses. The row count must equal the live corpus exactly.
    Brute { rows: &'a [f32] },
}

pub struct MeasureSpec<'a> {
    /// Empty for the unnamed dataset.
    pub collection: String,
    pub profile_id: String,
    pub embedding_model: String,
    /// Held-out query vectors, `dimensions` floats each.
    pub queries: &'a [f32],
    pub dimensions: u32,
    pub ks: Vec<u32>,
    /// The candidate-depth ladder. Every depth must be at or below the
    /// corpus rows and at or above the smallest `k`; for each `k` the
    /// depths at or above it are measured.
    pub depths: Vec<u32>,
    pub targets: Vec<u32>,
    pub default_target_recall_ppm: Option<u32>,
    pub ground_truth: GroundTruth<'a>,
    /// How a brute-force row position becomes the fleet's doc id, when
    /// the fleet's ids are not the corpus positions: a fleet whose shards
    /// sit at a slot stride wider than their row count (the runbook's
    /// `OFFSET_STRIDE`) numbers shard `s`'s rows from `s * stride`.
    /// `None` takes row `i` as doc id `i`.
    pub brute_id_map: Option<BruteIdMap>,
}

/// Row position to doc id for a brute-force rows file laid out in shard
/// order: `id = (row / rows_per_shard) * slot_stride + row % rows_per_shard`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct BruteIdMap {
    pub rows_per_shard: u64,
    pub slot_stride: u64,
}

impl BruteIdMap {
    pub fn id_of(&self, row: u64) -> u64 {
        (row / self.rows_per_shard) * self.slot_stride + row % self.rows_per_shard
    }
}

/// One rung as measured, with the per-query recall the summary came from
/// and the client-side wall time the server phases do not include.
#[derive(Clone, Debug, PartialEq)]
pub struct LadderRow {
    pub measurement: ProfileMeasurement,
    pub p50_client_ms: f64,
    /// Per held-out query, in query order.
    pub recall_ppm: Vec<u32>,
}

#[derive(Debug)]
pub struct MeasuredProfile {
    pub identity: LiveIdentity,
    pub ladder: Vec<LadderRow>,
    pub unmet: Vec<UnmetTarget>,
    pub profile: DenseQualityProfile,
}
