//! The diagnostics service a relay answers for itself.

use std::sync::Arc;

use tonic::{Request, Response, Status};

use crate::coordinator::CoordinatorServiceImpl;
use crate::metrics::Route;

use super::health::merge_shard_layouts;
use super::RelayService;

/// The relay's `DiagnosticsService`: the process's own knobs, metrics, and ring as any coordinator's, and one
/// shard layout for the root, the children's merged
/// ([`merge_shard_layouts`]). The root asks each shard's address for
/// its layout, so a relay serves the service on the port the root
/// already talks to.
#[derive(Clone)]
pub struct RelayDiagnostics {
    relay: RelayService,
    process: crate::diagnostics::CoordinatorDiagnostics,
}

impl RelayService {
    /// The diagnostics service over this relay.
    pub fn diagnostics(&self) -> RelayDiagnostics {
        let base: CoordinatorServiceImpl = (**self.base()).clone();
        RelayDiagnostics {
            relay: self.clone(),
            process: crate::diagnostics::CoordinatorDiagnostics::new(
                vec![(String::new(), base)],
                None,
                Arc::new(crate::diagnostics::RecentRing::default()),
            ),
        }
    }

    pub fn diagnostics_server(
        &self,
        max_message_bytes: usize,
    ) -> crate::pb::diagnostics_service_server::DiagnosticsServiceServer<RelayDiagnostics> {
        self.diagnostics().into_server(max_message_bytes)
    }
}

impl RelayDiagnostics {
    pub fn into_server(
        self,
        max_message_bytes: usize,
    ) -> crate::pb::diagnostics_service_server::DiagnosticsServiceServer<Self> {
        crate::pb::diagnostics_service_server::DiagnosticsServiceServer::new(self)
            .max_decoding_message_size(max_message_bytes)
            .max_encoding_message_size(max_message_bytes)
    }
}

#[tonic::async_trait]
impl crate::pb::diagnostics_service_server::DiagnosticsService for RelayDiagnostics {
    type StreamMetricsStream =
        <crate::diagnostics::CoordinatorDiagnostics as crate::pb::diagnostics_service_server::DiagnosticsService>::StreamMetricsStream;

    async fn get_runtime_knobs(
        &self,
        request: Request<crate::pb::GetRuntimeKnobsRequest>,
    ) -> Result<Response<crate::pb::RuntimeKnobs>, Status> {
        self.process.get_runtime_knobs(request).await
    }

    async fn set_runtime_knob(
        &self,
        request: Request<crate::pb::SetRuntimeKnobRequest>,
    ) -> Result<Response<crate::pb::RuntimeKnobs>, Status> {
        self.process.set_runtime_knob(request).await
    }

    async fn get_metrics_snapshot(
        &self,
        request: Request<crate::pb::MetricsSnapshotRequest>,
    ) -> Result<Response<crate::pb::MetricsSnapshot>, Status> {
        self.process.get_metrics_snapshot(request).await
    }

    async fn stream_metrics(
        &self,
        request: Request<crate::pb::StreamMetricsRequest>,
    ) -> Result<Response<Self::StreamMetricsStream>, Status> {
        self.process.stream_metrics(request).await
    }

    async fn get_shard_diagnostics(
        &self,
        request: Request<crate::pb::ShardDiagnosticsRequest>,
    ) -> Result<Response<crate::pb::ShardDiagnostics>, Status> {
        crate::metrics::timed(Route::GetShardDiagnostics, request, |_| async move {
            let map = self.relay.map();
            let children = self.relay.children();
            let layouts = self.relay.base().shard_diagnostics(None).await;
            Ok(Response::new(crate::pb::ShardDiagnostics {
                process: self.relay.base().knobs().process().to_string(),
                topology_generation: map.topology_generation,
                shards: vec![merge_shard_layouts(&children, &layouts)],
            }))
        })
        .await
    }

    async fn recent_queries(
        &self,
        request: Request<crate::pb::RecentQueriesRequest>,
    ) -> Result<Response<crate::pb::RecentQueriesResponse>, Status> {
        self.process.recent_queries(request).await
    }
}
