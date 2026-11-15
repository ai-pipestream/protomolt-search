//! What a host reports about itself, its test gates, and its store.

use super::*;

impl RaftHost {
    pub fn node_id(&self) -> NodeId {
        self.node_id
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Where the transport listens, for a networked member.
    pub fn listen_addr(&self) -> Option<std::net::SocketAddr> {
        self.listener.as_ref().map(|l| l.addr)
    }

    /// The address peers dial, for a networked member.
    pub fn advertised_addr(&self) -> Option<&str> {
        self.listener.as_ref().map(|l| l.advertised.as_str())
    }

    /// Every entry the log holds, in index order: a read for audits.
    pub fn log_entries(&self) -> Result<Vec<openraft::Entry<ControlRaft>>, Status> {
        self.log_store.entries()
    }

    pub fn metrics(&self) -> watch::Receiver<RaftMetrics<NodeId, BasicNode>> {
        self.raft.metrics()
    }

    /// Confirm fixture leadership without taking an application admission or
    /// extending its vote hold. Lease-hold tests must exercise that separately.
    #[cfg(all(test, feature = "tls"))]
    pub(crate) async fn test_leader_readiness(&self) -> Result<(), Status> {
        tokio::time::timeout(self.read_timeout, self.raft.ensure_linearizable())
            .await
            .map_err(|_| Status::unavailable("test leader read barrier timed out"))?
            .map(|_| ())
            .map_err(|error| match error {
                RaftError::APIError(error) => {
                    Status::unavailable(format!("test leader read barrier: {error}"))
                }
                RaftError::Fatal(error) => {
                    Status::internal(format!("test leader core stopped: {error}"))
                }
            })
    }

    #[cfg(all(test, feature = "tls"))]
    pub(crate) fn test_application_lease_held(&self) -> bool {
        self.hold.holds(std::time::Instant::now())
    }

    /// Wait for a metrics condition (state, applied index, snapshot).
    pub fn wait(
        &self,
        timeout: Option<Duration>,
    ) -> openraft::metrics::Wait<NodeId, BasicNode, openraft::TokioRuntime> {
        self.raft.wait(timeout)
    }

    /// The applied position the store recorded, as a read.
    pub fn applied_position(&self) -> Result<Option<crate::pb::storage::RaftLogId>, Status> {
        Ok(self.store()?.raft_applied()?.and_then(|a| a.last_applied))
    }

    /// Whether this member's store is still the prepared genesis image
    /// waiting for the group's first snapshot.
    pub fn awaiting_snapshot(&self) -> Result<bool, Status> {
        self.store()?.awaiting_snapshot()
    }

    /// The latest build of this node's own store: its size against the
    /// bound this node applies to images it receives.
    pub fn last_snapshot_build(&self) -> Option<super::super::state_machine::SnapshotBuildReport> {
        self.last_build.lock().ok().and_then(|last| last.clone())
    }

    /// The latest rejection each peer answered to this node's RPCs (empty
    /// for a node with no transport).
    #[cfg(feature = "tls")]
    pub fn peer_rejections(&self) -> BTreeMap<NodeId, PeerRejection> {
        self.peer_rejections
            .as_ref()
            .map(PeerRejections::latest)
            .unwrap_or_default()
    }

    /// Installs this node rejected at the announce, before any byte.
    #[cfg(feature = "tls")]
    pub fn snapshot_rejections(&self) -> &SnapshotRejections {
        &self.snapshot_rejections
    }

    /// The leader this node currently believes in, if any: an applied
    /// view, never an admission.
    pub fn believed_leader(&self) -> Option<NodeId> {
        self.raft.metrics().borrow().current_leader
    }

    /// This member's gauges, sampled live for the metrics page: the
    /// same reads `member_status` serves, as values. The store reads
    /// fall back to the not-ready zero (not the leader, nothing
    /// applied, awaiting snapshot unknown as false) rather than fail
    /// a scrape.
    pub fn member_gauges(&self) -> crate::metrics::MemberGauges {
        let metrics = self.raft.metrics().borrow().clone();
        let build = self.last_snapshot_build();
        crate::metrics::MemberGauges {
            is_leader: metrics.state == openraft::ServerState::Leader,
            applied_index: metrics.last_applied.map(|id| id.index).unwrap_or(0),
            last_log_index: metrics.last_log_index.unwrap_or(0),
            awaiting_snapshot: self.awaiting_snapshot().unwrap_or(false),
            #[cfg(feature = "tls")]
            peer_rejections: self
                .peer_rejections()
                .iter()
                .map(|(node, rejection)| crate::metrics::PeerRejectionGauges {
                    peer: *node,
                    action: rejection.action.to_string(),
                    code: format!("{:?}", rejection.code),
                    count: rejection.count,
                })
                .collect(),
            #[cfg(not(feature = "tls"))]
            peer_rejections: Vec::new(),
            #[cfg(feature = "tls")]
            snapshot_rejections: self.snapshot_rejections().count(),
            #[cfg(not(feature = "tls"))]
            snapshot_rejections: 0,
            last_snapshot_build_bytes: build.as_ref().map(|build| build.bytes).unwrap_or(0),
            last_snapshot_receiver_bound_bytes: build
                .as_ref()
                .map(|build| build.receiver_bound)
                .unwrap_or(0),
        }
    }

    /// A scrape-time sampler of [`RaftHost::member_gauges`] for the
    /// metrics page.
    pub fn member_gauge_provider(host: Arc<Self>) -> crate::metrics::MemberGaugeProvider {
        Box::new(move || host.member_gauges())
    }

    /// Ask the library to start an election on this member, for tests that
    /// need a named member to lead after a restart. The library elects only
    /// a member whose log is current and whose peers grant the vote.
    #[cfg(any(test, feature = "fault-injection"))]
    pub async fn trigger_election(&self) -> Result<(), Status> {
        self.raft
            .trigger()
            .elect()
            .await
            .map_err(|e| Status::internal(format!("raft election trigger: {e}")))
    }

    /// Arm a pause between the next admission's read barrier and its grant.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn arm_grant_gate(&self) -> Arc<GrantGate> {
        let gate = Arc::new(GrantGate::new());
        *self.grant_gate.lock().unwrap() = Some(Arc::clone(&gate));
        gate
    }

    /// Arm a pause in the next snapshot build, once its generation
    /// directory is in place and before it publishes.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn arm_snapshot_build_gate(&self) -> Arc<super::super::state_machine::SnapshotGate> {
        self.snapshot_gates.arm_build()
    }

    /// Arm a pause in the next snapshot serve, after it read the pointer
    /// and before it opens the generation.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn arm_snapshot_serve_gate(&self) -> Arc<super::super::state_machine::SnapshotGate> {
        self.snapshot_gates.arm_serve()
    }

    /// Arm a pause in the next snapshot install, once the image is
    /// received and before it is verified and the store replaced. The
    /// library's purge for the same snapshot runs on the core meanwhile.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn arm_snapshot_install_gate(&self) -> Arc<super::super::state_machine::SnapshotGate> {
        self.snapshot_gates.arm_install()
    }

    /// Arm a pause inside blocking snapshot image verification before the
    /// probe store is opened.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn arm_snapshot_verify_gate(&self) -> Arc<super::super::state_machine::SnapshotGate> {
        self.snapshot_gates.arm_verify()
    }

    /// Arm an async pause after transport verification completes and before
    /// `install_full_snapshot` is polled on the Raft core handle.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn arm_snapshot_handoff_gate(&self) -> Arc<super::super::state_machine::AsyncSnapshotGate> {
        self.snapshot_gates.arm_handoff()
    }

    /// Number of server-side final-chunk `InstallSnapshot` handlers dropped
    /// before returning a reply.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn final_snapshot_handler_drops(&self) -> usize {
        self.snapshot_gates.final_handler_drops()
    }

    /// Wait until at least `at_least` server-side final-chunk `InstallSnapshot`
    /// handlers have been dropped before returning a reply.
    #[cfg(any(test, feature = "fault-injection"))]
    pub async fn wait_final_snapshot_handler_drops(&self, at_least: usize) {
        self.snapshot_gates.wait_final_handler_drops(at_least).await;
    }

    /// Arm one process-exit fault (code 87) for the next purge transaction
    /// of this node's log: before it commits or right after.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn arm_purge_exit_fault(&self, fault: crate::source_authority::ExitFault) {
        self.log_store.arm_purge_exit_fault(fault);
    }

    /// Arm one process-exit fault (code 87) for the next snapshot install:
    /// before the store is replaced by the image, or right after, before
    /// the generation is published.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn arm_install_exit_fault(&self, fault: crate::source_authority::ExitFault) {
        *self.install_fault.lock().unwrap() = Some(fault);
    }

    /// The purge state of this node's log, read through the running host.
    #[cfg(any(test, feature = "fault-injection"))]
    pub fn log_inspection(&self) -> Result<super::super::log_store::RaftLogInspection, Status> {
        self.log_store.inspection()
    }

    /// The snapshot the state machine serves to a peer right now, read in
    /// full on the state machine worker's path: its stored meta and the
    /// image bytes. `None` when no generation is published.
    #[cfg(any(test, feature = "fault-injection"))]
    pub async fn serve_snapshot(
        &self,
    ) -> Result<Option<(crate::pb::storage::RaftSnapshotMeta, Vec<u8>)>, Status> {
        let Some(snapshot) = self
            .raft
            .get_snapshot()
            .await
            .map_err(|e| Status::internal(format!("raft snapshot read: {e}")))?
        else {
            return Ok(None);
        };
        let signature = super::super::state_machine::parse_snapshot_id(&snapshot.meta.snapshot_id)?;
        let group = self.store()?.identity().clone();
        let meta = super::super::state_machine::meta_to_proto(&group, &snapshot.meta, &signature);
        let mut file = snapshot.snapshot;
        let mut bytes = Vec::new();
        tokio::io::AsyncReadExt::read_to_end(&mut *file, &mut bytes)
            .await
            .map_err(|e| Status::internal(format!("raft snapshot read: {e}")))?;
        Ok(Some((meta, bytes)))
    }

    /// A handle that yields the current store on every call, for consumers
    /// that outlive one snapshot install (the relay's map source).
    pub fn store_handle(
        &self,
    ) -> impl Fn() -> Result<SourceAuthorityStore, Status> + Send + Sync + 'static {
        let shared = Arc::clone(&self.store);
        move || {
            shared
                .read()
                .map_err(|_| Status::internal("host store lock poisoned"))?
                .clone()
                .ok_or_else(|| Status::unavailable("store is being replaced by a snapshot install"))
        }
    }

    /// The current store handle, for reads (maps, snapshots, decisions).
    /// Direct commands on it refuse; propose through the host.
    pub fn store(&self) -> Result<SourceAuthorityStore, Status> {
        self.store
            .read()
            .map_err(|_| Status::internal("host store lock poisoned"))?
            .clone()
            .ok_or_else(|| Status::unavailable("store is being replaced by a snapshot install"))
    }
}
