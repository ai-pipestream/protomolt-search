//! Admission leases, test gates and fault arms, snapshot serving, and
//! the store handle.

use super::*;

impl RaftHost {
    /// An owned admission lease (docs/raft-admission.md) for work that
    /// cannot run on the calling thread. The lease is anchored before the
    /// read barrier is invoked: the barrier's heartbeats are sent after
    /// that instant, every follower that acknowledges them refreshes its
    /// own leader lease later still, and none of them grants a vote before
    /// that plus the election ceiling. The interval therefore includes
    /// every response, catch-up and scheduling delay between the barrier
    /// and the grant, and a grant whose interval has already elapsed is
    /// refused. The read itself is bounded: an isolated leader collects no
    /// quorum and refuses, and a member that does not lead refuses naming
    /// the leader. After the barrier this extends the vote hold and passes
    /// the grant gate, then returns the store, the actor and the anchored
    /// interval instead of running a closure. The grant
    /// (`LeasedAdmission::admission`) measures the interval from the
    /// anchor, so moving the value to a blocking worker consumes the
    /// interval without extending it.
    pub async fn lease(&self, principal: &str) -> Result<LeasedAdmission, Status> {
        let lease = AdmissionLease {
            anchor: std::time::Instant::now(),
            anchor_wall: std::time::SystemTime::now(),
            ttl: self.lease,
        };
        match tokio::time::timeout(self.read_timeout, self.raft.ensure_linearizable()).await {
            Ok(Ok(_)) => {}
            Ok(Err(RaftError::APIError(CheckIsLeaderError::ForwardToLeader(forward)))) => {
                return self.forwarded_lease(principal, lease, forward).await;
            }
            Ok(Err(e)) => {
                return Err(reasons::reason(
                    Status::unavailable(format!("admission needs a linearizable read: {e}")),
                    reasons::LEASE_NO_LINEARIZABLE_READ,
                ))
            }
            Err(_) => {
                return Err(reasons::reason(
                    Status::unavailable(format!(
                        "admission needs a linearizable read: no quorum acknowledged within {} ms",
                        self.read_timeout.as_millis()
                    )),
                    reasons::LEASE_NO_LINEARIZABLE_READ,
                ))
            }
        }
        self.hold.extend(lease.anchor);
        self.pass_grant_gate().await;
        let store = self.store()?;
        Ok(LeasedAdmission::new(store, principal, lease))
    }

    /// A lease for a member that does not lead (docs/raft-admission.md, "A
    /// lease forwarded from the leader"): the leader the library names is
    /// asked to run its barrier, and the interval is the one anchored here
    /// before it was asked, so every hop, wait and delay consumes it. The
    /// grant waits until this member has applied the position the leader's
    /// barrier read, bounded by the remaining interval, so the applied view
    /// it grants on is at least as fresh as the leader's was; a member that
    /// cannot get there inside the interval is rejected by name. Without a
    /// transport, or with no leader known, the rejection names what is
    /// missing.
    async fn forwarded_lease(
        &self,
        principal: &str,
        lease: AdmissionLease,
        forward: openraft::error::ForwardToLeader<NodeId, BasicNode>,
    ) -> Result<LeasedAdmission, Status> {
        let (Some(leader), Some(node)) = (forward.leader_id, forward.leader_node.as_ref()) else {
            return Err(reasons::reason(
                Status::unavailable(
                    "not the leader, and no leader is known to forward the lease to",
                ),
                reasons::LEASE_NO_KNOWN_LEADER,
            ));
        };
        #[cfg(not(feature = "tls"))]
        {
            let _ = (principal, lease, node);
            return Err(reasons::reason(
                Status::unavailable(format!(
                    "not the leader; leader is Some({leader}), and this build has no transport to forward the lease"
                )),
                reasons::LEASE_NO_TRANSPORT,
            ));
        }
        #[cfg(feature = "tls")]
        {
            let Some(forwarder) = &self.forwarder else {
                return Err(reasons::reason(
                    Status::unavailable(format!(
                        "not the leader; leader is Some({leader}), and this host has no transport to forward the lease"
                    )),
                    reasons::LEASE_NO_TRANSPORT,
                ));
            };
            let read = forwarder
                .grant_lease(leader, &node.addr, self.read_timeout)
                .await?;
            if let Some(read) = read {
                let remaining =
                    (lease.anchor + lease.ttl).saturating_duration_since(std::time::Instant::now());
                self.raft
                    .wait(Some(remaining))
                    .applied_index_at_least(Some(read.index), "forwarded lease")
                    .await
                    .map_err(|e| {
                        reasons::reason(
                            Status::unavailable(format!(
                                "forwarded lease: this member had not applied the leader's read position {} inside the interval: {e}",
                                read.index
                            )),
                            reasons::LEASE_READ_POSITION_BEHIND,
                        )
                    })?;
            }
            self.hold.extend(lease.anchor);
            self.pass_grant_gate().await;
            let store = self.store()?;
            Ok(LeasedAdmission::new(store, principal, lease))
        }
    }

    async fn pass_grant_gate(&self) {
        #[cfg(any(test, feature = "fault-injection"))]
        {
            let gate = self.grant_gate.lock().unwrap().take();
            if let Some(gate) = gate {
                gate.reached.send_replace(true);
                gate.release.notified().await;
            }
        }
    }

    /// Run owner-side work under a leased admission on the calling thread:
    /// `lease` followed by an immediate grant. The closure borrows the
    /// admission, so the shared guard it takes at the grant is dropped
    /// when the closure returns.
    pub async fn with_admission<T>(
        &self,
        principal: &str,
        run: impl FnOnce(&SourceAdmission<'_>) -> Result<T, Status>,
    ) -> Result<T, Status> {
        let leased = self.lease(principal).await?;
        let admission = leased.admission()?;
        run(&admission)
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

    pub(super) fn write_error<E: std::fmt::Display>(
        error: RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>,
        what: E,
    ) -> Status {
        match error {
            RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) => reasons::reason(
                Status::unavailable(format!("not the leader; leader is {:?}", forward.leader_id)),
                reasons::LEASE_NOT_LEADER,
            ),
            RaftError::APIError(ClientWriteError::ChangeMembershipError(change)) => {
                reasons::reason(
                    Status::failed_precondition(format!("{what}: {change}")),
                    reasons::MEMBERSHIP_CHANGE_REJECTED,
                )
            }
            other => Status::unavailable(format!("{what}: {other}")),
        }
    }

    /// Submit an admitted proposal and wait for its committed reply. Private:
    /// every public entry admits its command first.
    pub(super) async fn propose(&self, proposal: RaftProposal) -> Result<RaftReply, Status> {
        validate_proposal(&proposal)?;
        let response = self
            .raft
            .client_write(proposal)
            .await
            .map_err(|error| Self::write_error(error, "raft proposal"))?;
        Ok(response.data)
    }
}
