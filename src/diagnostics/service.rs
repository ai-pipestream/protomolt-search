//! The `DiagnosticsService` a node and a coordinator each serve.

use super::*;

pub(super) type SnapshotStream =
    Pin<Box<dyn Stream<Item = Result<MetricsSnapshot, Status>> + Send>>;

pub(super) fn stream_interval(request: &StreamMetricsRequest) -> Result<Duration, Status> {
    let ms = if request.interval_ms == 0 {
        STREAM_DEFAULT_MS
    } else {
        request.interval_ms
    };
    if ms < STREAM_MIN_MS {
        return Err(Status::invalid_argument(format!(
            "StreamMetrics: interval_ms={ms} is under the {STREAM_MIN_MS} ms minimum"
        )));
    }
    Ok(Duration::from_millis(u64::from(ms)))
}

/// A stream of snapshots every `interval` until the receiver hangs up.
pub(super) fn snapshot_stream(
    interval: Duration,
    snapshot: impl Fn() -> MetricsSnapshot + Send + 'static,
) -> SnapshotStream {
    let (tx, rx) = tokio::sync::mpsc::channel(4);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = tx.closed() => break,
                _ = ticker.tick() => {},
            }
            if tx.send(Ok(snapshot())).await.is_err() {
                break;
            }
        }
    });
    Box::pin(ReceiverStream::new(rx))
}

pub(super) fn check_access(permits: &[AccessPermit]) -> Result<(), Status> {
    for permit in permits {
        permit.check()?;
    }
    Ok(())
}

/// The service on a shard node.
#[derive(Clone)]
pub struct NodeDiagnostics {
    pub(super) node: crate::node::NodeServiceImpl,
}

impl NodeDiagnostics {
    pub fn new(node: crate::node::NodeServiceImpl) -> Self {
        NodeDiagnostics { node }
    }

    pub fn into_server(self, max_message_bytes: usize) -> DiagnosticsServiceServer<Self> {
        DiagnosticsServiceServer::new(self)
            .max_decoding_message_size(max_message_bytes)
            .max_encoding_message_size(max_message_bytes)
    }

    pub(super) fn snapshot(&self) -> MetricsSnapshot {
        crate::metrics::snapshot(self.node.knobs().process(), &[self.node.metrics_provider()])
    }
}

#[tonic::async_trait]
impl DiagnosticsService for NodeDiagnostics {
    type StreamMetricsStream = crate::metrics::Timed<SnapshotStream>;

    async fn get_runtime_knobs(
        &self,
        request: Request<GetRuntimeKnobsRequest>,
    ) -> Result<Response<RuntimeKnobs>, Status> {
        crate::metrics::timed(Route::GetRuntimeKnobs, request, |_| async move {
            Ok(Response::new(self.node.knobs().list()))
        })
        .await
    }

    async fn set_runtime_knob(
        &self,
        request: Request<SetRuntimeKnobRequest>,
    ) -> Result<Response<RuntimeKnobs>, Status> {
        crate::metrics::timed(Route::SetRuntimeKnob, request, |request| async move {
            let req = request.into_inner();
            self.node.knobs().set(&req.name, &req.value)?;
            Ok(Response::new(self.node.knobs().list()))
        })
        .await
    }

    async fn get_metrics_snapshot(
        &self,
        request: Request<MetricsSnapshotRequest>,
    ) -> Result<Response<MetricsSnapshot>, Status> {
        crate::metrics::timed(Route::GetMetricsSnapshot, request, |_| async move {
            Ok(Response::new(self.snapshot()))
        })
        .await
    }

    async fn stream_metrics(
        &self,
        request: Request<StreamMetricsRequest>,
    ) -> Result<Response<Self::StreamMetricsStream>, Status> {
        crate::metrics::timed_stream(Route::StreamMetrics, request, |request| async move {
            let interval = stream_interval(request.get_ref())?;
            let this = self.clone();
            Ok(Response::new(snapshot_stream(interval, move || {
                this.snapshot()
            })))
        })
        .await
    }

    async fn get_shard_diagnostics(
        &self,
        request: Request<ShardDiagnosticsRequest>,
    ) -> Result<Response<ShardDiagnostics>, Status> {
        crate::metrics::timed(Route::GetShardDiagnostics, request, |_| async move {
            Ok(Response::new(ShardDiagnostics {
                process: self.node.knobs().process().to_string(),
                topology_generation: 0,
                shards: vec![self.node.shard_diagnostics(0, String::new())],
            }))
        })
        .await
    }

    async fn recent_queries(
        &self,
        request: Request<RecentQueriesRequest>,
    ) -> Result<Response<RecentQueriesResponse>, Status> {
        crate::metrics::timed(Route::RecentQueries, request, |_| async move {
            Ok(Response::new(RecentQueriesResponse {
                queries: Vec::new(),
                total_seen: 0,
            }))
        })
        .await
    }
}

/// The service on a coordinator: its own knobs, its metrics, the nodes'
/// layouts through fan-out, and the recent-request ring.
#[derive(Clone)]
pub struct CoordinatorDiagnostics {
    pub(super) members: Vec<(String, crate::coordinator::CoordinatorServiceImpl)>,
    pub(super) principals: Option<Arc<Principals>>,
    pub(super) ring: Arc<RecentRing>,
    pub(super) gauges: Arc<Vec<crate::metrics::GaugeProvider>>,
    pub(super) member_gauges: Arc<Vec<crate::metrics::MemberGaugeProvider>>,
}

impl CoordinatorDiagnostics {
    /// These routes inspect or change the whole served process. A collection
    /// administrator cannot use them to cross another collection's boundary.
    pub(super) fn admit<T>(&self, request: &Request<T>) -> Result<Vec<AccessPermit>, Status> {
        let Some(principals) = &self.principals else {
            return Ok(Vec::new());
        };
        let principal = principals.authenticate(request.metadata())?;
        if !principal.admin {
            return Err(Status::permission_denied(format!(
                "principal {:?} is not an admin; the diagnostics service needs admin = true",
                principal.name
            )));
        }
        if self.members.is_empty() {
            return Err(Status::permission_denied("no authorized collections"));
        }
        let permits = self
            .members
            .iter()
            .map(|(name, _)| principals.authorize(&principal, name, crate::pb::AccessAction::Admin))
            .collect::<Result<Vec<_>, _>>()?;
        // Acquiring a later collection may span a policy replacement. No
        // response or mutation may proceed with a mixture of old and new grants.
        check_access(&permits)?;
        Ok(permits)
    }

    /// `members` in collection order, the unnamed one under `""`. Supply the
    /// complete collection set for this process, including collections behind
    /// the ring and gauge providers: metrics and controls are process-wide.
    pub fn new(
        members: Vec<(String, crate::coordinator::CoordinatorServiceImpl)>,
        principals: Option<Arc<Principals>>,
        ring: Arc<RecentRing>,
    ) -> Self {
        CoordinatorDiagnostics {
            members,
            principals,
            ring,
            gauges: Arc::new(Vec::new()),
            member_gauges: Arc::new(Vec::new()),
        }
    }

    /// Shard gauges of nodes served in this process (`Role::Both`), so
    /// the coordinator's snapshot renders the same page the exporter
    /// does.
    pub fn with_gauges(mut self, gauges: Vec<crate::metrics::GaugeProvider>) -> Self {
        self.gauges = Arc::new(gauges);
        self
    }

    /// The Raft member gauges of this process, so the coordinator's
    /// snapshot carries the same member rows the exporter does.
    pub fn with_member_gauges(mut self, members: Vec<crate::metrics::MemberGaugeProvider>) -> Self {
        self.member_gauges = Arc::new(members);
        self
    }

    pub fn into_server(self, max_message_bytes: usize) -> DiagnosticsServiceServer<Self> {
        DiagnosticsServiceServer::new(self)
            .max_decoding_message_size(max_message_bytes)
            .max_encoding_message_size(max_message_bytes)
    }

    /// The default member: the unnamed one, else the first named one.
    pub(super) fn primary(&self) -> Result<&crate::coordinator::CoordinatorServiceImpl, Status> {
        self.members
            .iter()
            .find(|(name, _)| name.is_empty())
            .or_else(|| self.members.first())
            .map(|(_, member)| member)
            .ok_or_else(|| Status::failed_precondition("this coordinator serves no collection"))
    }

    pub(super) fn snapshot(&self) -> MetricsSnapshot {
        let process = self
            .primary()
            .map(|m| m.knobs().process().to_string())
            .unwrap_or_default();
        crate::metrics::snapshot_with_member(&process, &self.gauges, &self.member_gauges)
    }
}

#[tonic::async_trait]
impl DiagnosticsService for CoordinatorDiagnostics {
    type StreamMetricsStream = crate::metrics::Timed<SnapshotStream>;

    async fn get_runtime_knobs(
        &self,
        request: Request<GetRuntimeKnobsRequest>,
    ) -> Result<Response<RuntimeKnobs>, Status> {
        crate::metrics::timed(Route::GetRuntimeKnobs, request, |request| async move {
            let access = self.admit(&request)?;
            let knobs = self.primary()?.knobs().list();
            check_access(&access)?;
            Ok(Response::new(knobs))
        })
        .await
    }

    async fn set_runtime_knob(
        &self,
        request: Request<SetRuntimeKnobRequest>,
    ) -> Result<Response<RuntimeKnobs>, Status> {
        crate::metrics::timed(Route::SetRuntimeKnob, request, |request| async move {
            let access = self.admit(&request)?;
            let req = request.into_inner();
            // Every collection's coordinator shares the process's caps.
            for (_, member) in &self.members {
                check_access(&access)?;
                let result = member.knobs().set(&req.name, &req.value);
                check_access(&access)?;
                result?;
            }
            let knobs = self.primary()?.knobs().list();
            check_access(&access)?;
            Ok(Response::new(knobs))
        })
        .await
    }

    async fn get_metrics_snapshot(
        &self,
        request: Request<MetricsSnapshotRequest>,
    ) -> Result<Response<MetricsSnapshot>, Status> {
        crate::metrics::timed(Route::GetMetricsSnapshot, request, |request| async move {
            let access = self.admit(&request)?;
            let snapshot = self.snapshot();
            check_access(&access)?;
            Ok(Response::new(snapshot))
        })
        .await
    }

    async fn stream_metrics(
        &self,
        request: Request<StreamMetricsRequest>,
    ) -> Result<Response<Self::StreamMetricsStream>, Status> {
        crate::metrics::timed_stream(Route::StreamMetrics, request, |request| async move {
            let access = self.admit(&request)?;
            let interval = stream_interval(request.get_ref())?;
            let this = self.clone();
            let stream = snapshot_stream(interval, move || this.snapshot());
            check_access(&access)?;
            let stream: SnapshotStream = Box::pin(AuthorizedStream::with_permits(stream, access));
            Ok(Response::new(stream))
        })
        .await
    }

    async fn get_shard_diagnostics(
        &self,
        request: Request<ShardDiagnosticsRequest>,
    ) -> Result<Response<ShardDiagnostics>, Status> {
        crate::metrics::timed(Route::GetShardDiagnostics, request, |request| async move {
            let access = self.admit(&request)?;
            let only = request.get_ref().shard;
            let primary = self.primary()?;
            let mut shards = Vec::new();
            for (_, member) in &self.members {
                check_access(&access)?;
                shards.extend(member.shard_diagnostics(only).await);
            }
            check_access(&access)?;
            Ok(Response::new(ShardDiagnostics {
                process: primary.knobs().process().to_string(),
                topology_generation: primary.current_topology_generation(),
                shards,
            }))
        })
        .await
    }

    async fn recent_queries(
        &self,
        request: Request<RecentQueriesRequest>,
    ) -> Result<Response<RecentQueriesResponse>, Status> {
        crate::metrics::timed(Route::RecentQueries, request, |request| async move {
            let access = self.admit(&request)?;
            let limit = match request.get_ref().limit {
                0 => RECENT_DEFAULT,
                n => (n as usize).min(RECENT_RING),
            };
            let recent = RecentQueriesResponse {
                queries: self.ring.recent(limit),
                total_seen: self.ring.total_seen(),
            };
            check_access(&access)?;
            Ok(Response::new(recent))
        })
        .await
    }
}

/// `ShardLayoutDiagnostics` for a node that does not serve the service.
pub fn unserved_layout(shard: u32, address: String, note: &str) -> ShardLayoutDiagnostics {
    ShardLayoutDiagnostics {
        shard,
        address,
        layout: note.to_string(),
        ..Default::default()
    }
}
