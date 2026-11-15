//! Admission leases: held on the leader, forwarded to a follower.

use super::*;

/// While an admission interval anchored at a read barrier may still be
/// open, this node withholds its vote from every candidate: the pinned
/// library refreshes a leader's own vote timer only when its vote changes,
/// so an established leader would otherwise grant a vote to an up-to-date
/// candidate at once, and a vote quorum could then avoid every follower
/// that acknowledged the barrier.
#[derive(Clone)]
pub struct LeaseHold {
    anchor: Arc<std::sync::RwLock<Option<std::time::Instant>>>,
    window: Duration,
}

impl LeaseHold {
    pub(crate) fn new(window: Duration) -> Self {
        Self {
            anchor: Arc::new(std::sync::RwLock::new(None)),
            window,
        }
    }

    pub(crate) fn extend(&self, anchor: std::time::Instant) {
        let mut held = self.anchor.write().unwrap_or_else(|e| e.into_inner());
        if held.is_none_or(|current| anchor > current) {
            *held = Some(anchor);
        }
    }

    /// Whether an interval anchored at the latest barrier is still open.
    pub(crate) fn holds(&self, now: std::time::Instant) -> bool {
        self.anchor
            .read()
            .unwrap_or_else(|e| e.into_inner())
            .is_some_and(|anchor| now < anchor + self.window)
    }
}

/// A test and fault-injection hook: the next `with_admission` pauses after
/// its read barrier and before its grant until `release`.
#[cfg(any(test, feature = "fault-injection"))]
pub struct GrantGate {
    reached: tokio::sync::watch::Sender<bool>,
    release: tokio::sync::Notify,
}

#[cfg(any(test, feature = "fault-injection"))]
impl GrantGate {
    pub(super) fn new() -> Self {
        Self {
            reached: tokio::sync::watch::Sender::new(false),
            release: tokio::sync::Notify::new(),
        }
    }

    /// Resolves once an admission has passed its barrier and is paused.
    pub async fn reached(&self) {
        let mut receiver = self.reached.subscribe();
        let _ = receiver.wait_for(|reached| *reached).await;
    }

    pub fn release(&self) {
        self.release.notify_one();
    }
}

impl RaftHost {
    /// An owned admission lease for work that
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

    /// A lease for a member that does not lead: the leader the library names is
    /// asked to run its barrier, and the interval is the one anchored here
    /// before it was asked, so every hop, wait and delay consumes it. The
    /// grant waits until this member has applied the position the leader's
    /// barrier read, bounded by the remaining interval, so the applied view
    /// it grants on is at least as fresh as the leader's was; a member that
    /// cannot get there inside the interval is rejected by name. Without a
    /// transport, or with no leader known, the rejection names what is
    /// missing.
    pub(super) async fn forwarded_lease(
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

    pub(super) async fn pass_grant_gate(&self) {
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
}
