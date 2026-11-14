//! Proposals through the log, snapshots, learners, promotion, member
//! removal, isolation, and shutdown.

use super::*;

impl RaftHost {
    /// A control command through the log. ConfirmReady needs the holder
    /// path below; the general path refuses it at proposal.
    pub async fn propose_command(
        &self,
        principal: &str,
        command: &SourceAuthorityCommand,
    ) -> Result<crate::pb::storage::SourceAuthorityDecision, Status> {
        if matches!(
            command.action,
            Some(crate::pb::storage::source_authority_command::Action::ConfirmReady(_))
        ) {
            return Err(Status::permission_denied(
                "readiness confirmation requires the hosting adapter with the managed binding",
            ));
        }
        let reply = self
            .propose(RaftProposal {
                format_version: 1,
                principal: principal.into(),
                command: Some(Command::Control(command.clone())),
            })
            .await?;
        control_reply(reply)
    }

    /// Readiness confirmation admitted against the held binding at proposal.
    pub async fn propose_confirm_ready(
        &self,
        principal: &str,
        command: &SourceAuthorityCommand,
        verified: &VerifiedOwnerCompletion,
    ) -> Result<crate::pb::storage::SourceAuthorityDecision, Status> {
        self.store()?
            .proposal_admits_readiness(principal, command, verified)?;
        let reply = self
            .propose(RaftProposal {
                format_version: 1,
                principal: principal.into(),
                command: Some(Command::Control(command.clone())),
            })
            .await?;
        control_reply(reply)
    }

    /// Import steps through the log; a Begin needs the retirement holder.
    pub async fn propose_import(
        &self,
        principal: &str,
        command: &ControlImportCommand,
        retired: Option<&RetiredLegacyControl>,
    ) -> Result<crate::pb::storage::ControlImportDecision, Status> {
        self.store()?
            .proposal_admits_import(principal, command, retired)?;
        let reply = self
            .propose(RaftProposal {
                format_version: 1,
                principal: principal.into(),
                command: Some(Command::Import(command.clone())),
            })
            .await?;
        match reply.reply {
            Some(Reply::Import(decision)) => Ok(decision),
            Some(Reply::Refusal(refusal)) => Err(refusal_status(refusal)),
            _ => Err(Status::internal(
                "raft reply kind differs from the proposal",
            )),
        }
    }

    pub async fn propose_capacity_configure(
        &self,
        principal: &str,
        command: &CapacityConfigureCommand,
    ) -> Result<crate::pb::storage::CapacityConfigureDecision, Status> {
        let reply = self
            .propose(RaftProposal {
                format_version: 1,
                principal: principal.into(),
                command: Some(Command::Capacity(command.clone())),
            })
            .await?;
        match reply.reply {
            Some(Reply::Capacity(decision)) => Ok(decision),
            Some(Reply::Refusal(refusal)) => Err(refusal_status(refusal)),
            _ => Err(Status::internal(
                "raft reply kind differs from the proposal",
            )),
        }
    }

    pub async fn propose_capacity_transition(
        &self,
        principal: &str,
        transition: &CapacityTransition,
    ) -> Result<crate::pb::storage::CapacityTransitionReceipt, Status> {
        let reply = self
            .propose(RaftProposal {
                format_version: 1,
                principal: principal.into(),
                command: Some(Command::Observation(transition.clone())),
            })
            .await?;
        match reply.reply {
            Some(Reply::Observation(receipt)) => Ok(receipt),
            Some(Reply::Refusal(refusal)) => Err(refusal_status(refusal)),
            _ => Err(Status::internal(
                "raft reply kind differs from the proposal",
            )),
        }
    }

    /// Ask the state machine for a snapshot now (tests and operators). A
    /// store that has applied nothing yet (a prepared member before its
    /// first install) has nothing to snapshot; the library would send the
    /// build regardless and treat the builder's refusal as fatal, so the
    /// trigger is refused here by name.
    pub async fn trigger_snapshot(&self) -> Result<(), Status> {
        if self.applied_position()?.is_none() {
            return Err(Status::failed_precondition(
                "no applied position; nothing to snapshot",
            ));
        }
        self.raft
            .trigger()
            .snapshot()
            .await
            .map_err(|e| Status::internal(format!("raft snapshot trigger: {e}")))
    }

    // -----------------------------------------------------------------
    // Membership (docs/raft-hosting.md): learner, promotion, removal
    // -----------------------------------------------------------------

    /// Add a prepared member as a learner and wait until it is caught up.
    /// Leader only. The member's certificate must be registered in this
    /// node's directory. A snapshot is built and the log purged to it
    /// first, so the learner is seeded by the verified image rather than
    /// by replaying onto its own genesis.
    ///
    /// A rejection the learner's service answers meanwhile is returned at
    /// once with the learner's code; a failure of the transport between
    /// the nodes is not a rejection and is retried with backoff inside the
    /// window. The learner stays in the membership either way, and a call
    /// repeated for the same node writes one more membership entry (the
    /// library writes the entry whether or not the membership changes) and
    /// seeds again from the position after it.
    #[cfg(feature = "tls")]
    pub async fn add_learner(&self, node_id: NodeId, addr: &str) -> Result<(), Status> {
        let directory = self.directory.as_ref().ok_or_else(|| {
            reasons::reason(
                Status::failed_precondition("membership changes need a networked member"),
                reasons::MEMBERSHIP_NOT_NETWORKED,
            )
        })?;
        if !directory.knows(node_id) {
            return Err(reasons::reason(
                Status::failed_precondition(format!(
                    "node {node_id} has no registered certificate; bind it before it joins"
                )),
                reasons::MEMBERSHIP_UNREGISTERED_CERTIFICATE,
            ));
        }
        if addr.is_empty() || addr.len() > 1024 {
            return Err(reasons::reason(
                Status::invalid_argument("member address must be 1..1024 bytes"),
                reasons::MEMBERSHIP_BAD_ADDRESS,
            ));
        }
        self.seed_boundary().await?;
        let started = std::time::Instant::now();
        // The library's blocking mode returns once the learner is within
        // its lag threshold; a member is caught up when it holds the
        // membership entry that added it.
        let added = self
            .raft
            .add_learner(
                node_id,
                BasicNode {
                    addr: addr.to_string(),
                },
                false,
            )
            .await
            .map_err(|e| Self::write_error(e, format!("add learner {node_id}")))?
            .log_id;
        // A rejection the learner answers meanwhile (its bound, its identity,
        // an image it finds invalid) is named at once, with the peer's own
        // code: waiting out the timeout would hide it. The learner stays
        // in the membership and the leader keeps retrying with backoff.
        let mut rejections = self.peer_rejections.as_ref().map(PeerRejections::subscribe);
        let wait = self.raft.wait(Some(Duration::from_secs(60)));
        let caught_up = wait.metrics(
            |metrics| {
                metrics
                    .replication
                    .as_ref()
                    .and_then(|replication| replication.get(&node_id).copied().flatten())
                    .is_some_and(|matching| matching >= added)
            },
            "learner caught up",
        );
        let rejected = async {
            match rejections.as_mut() {
                Some(rejections) => Self::rejection_from(rejections, node_id, started).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            outcome = caught_up => outcome
                .map(|_| ())
                .map_err(|e| Status::unavailable(format!("learner {node_id} did not catch up: {e}"))),
            rejection = rejected => Err(Status::new(
                rejection.code,
                format!(
                    "learner {node_id} rejected the {} ({:?}): {}; it stays in the membership and this node keeps retrying with backoff",
                    rejection.action, rejection.code, rejection.message
                ),
            )),
        }
    }

    /// The published image, verified by digest: `Ok(Some(generation))`
    /// when it matches its meta, `Ok(None)` with no generation published,
    /// `Err` when the bytes differ from what the meta announces. A serve
    /// reads the image by length only; this is the check a peer's digest
    /// rejection asks for (`rebuild_on_digest_rejection`), and an
    /// operator's.
    pub fn verify_published_image(&self) -> Result<Option<u64>, Status> {
        self.published.verify()
    }

    /// The first rejection `node_id` answers at or after `since`.
    #[cfg(feature = "tls")]
    async fn rejection_from(
        rejections: &mut watch::Receiver<BTreeMap<NodeId, PeerRejection>>,
        node_id: NodeId,
        since: std::time::Instant,
    ) -> PeerRejection {
        loop {
            let found = rejections
                .borrow_and_update()
                .get(&node_id)
                .filter(|rejection| rejection.at >= since)
                .cloned();
            if let Some(found) = found {
                return found;
            }
            if rejections.changed().await.is_err() {
                return std::future::pending().await;
            }
        }
    }

    /// A snapshot at the current applied position with the log purged up
    /// to it: the boundary every new member is seeded from.
    async fn seed_boundary(&self) -> Result<(), Status> {
        let metrics = self.raft.metrics().borrow().clone();
        if metrics.state != openraft::ServerState::Leader {
            return Err(Status::unavailable(format!(
                "not the leader; leader is {:?}",
                metrics.current_leader
            )));
        }
        let applied = metrics
            .last_applied
            .ok_or_else(|| Status::failed_precondition("nothing is applied yet"))?;
        // The store may be past the position this read saw (a committed
        // proposal reaches the store before the metric), and a build images
        // the store where it is. So the snapshot waited for is one at or
        // past the position read, and the purge goes to the snapshot's
        // position. Waiting for a snapshot at exactly the position read
        // waited out its timeout whenever the store was one entry ahead.
        if !metrics.snapshot.is_some_and(|snapshot| snapshot >= applied) {
            // The library drops a trigger while a build is in flight and
            // reports the trigger as taken; a build that began before the
            // position moved lands below it, and the library's own policy
            // builds next after many more entries. So the trigger is
            // repeated, with a short wait between, until a snapshot at or
            // past the position is reported or the seed's window is out.
            let deadline = std::time::Instant::now() + Duration::from_secs(60);
            loop {
                self.trigger_snapshot().await?;
                let wait = self.raft.wait(Some(Duration::from_secs(2)));
                match wait
                    .metrics(
                        |metrics| metrics.snapshot.is_some_and(|snapshot| snapshot >= applied),
                        "seed snapshot at or past the applied position",
                    )
                    .await
                {
                    Ok(_) => break,
                    Err(openraft::metrics::WaitError::Timeout(..))
                        if std::time::Instant::now() < deadline => {}
                    Err(e) => {
                        return Err(Status::unavailable(format!(
                            "seed snapshot: {e} (the trigger was repeated for 60 s)"
                        )))
                    }
                }
            }
        }
        let snapshot = self.raft.metrics().borrow().snapshot;
        let snapshot =
            snapshot.ok_or_else(|| Status::internal("no snapshot after the seed build"))?;
        let purged = self.raft.metrics().borrow().purged;
        if !purged.is_some_and(|purged| purged >= snapshot) {
            self.raft
                .trigger()
                .purge_log(snapshot.index)
                .await
                .map_err(|e| Status::internal(format!("purge log: {e}")))?;
            self.raft
                .wait(Some(Duration::from_secs(60)))
                .metrics(
                    |metrics| metrics.purged.is_some_and(|purged| purged >= snapshot),
                    "seed purge to the snapshot",
                )
                .await
                .map_err(|e| Status::unavailable(format!("seed purge: {e}")))?;
        }
        Ok(())
    }

    /// Promote caught-up learners to voters through the library's joint
    /// consensus. Leader only.
    #[cfg(feature = "tls")]
    pub async fn promote(&self, learners: BTreeSet<NodeId>) -> Result<(), Status> {
        if learners.is_empty() {
            return Err(reasons::reason(
                Status::invalid_argument("no learners to promote"),
                reasons::MEMBERSHIP_NO_LEARNERS,
            ));
        }
        self.raft
            .change_membership(ChangeMembers::AddVoterIds(learners), false)
            .await
            .map_err(|e| Self::write_error(e, "promote"))?;
        Ok(())
    }

    /// Remove a member (voter or learner) from the group through joint
    /// consensus. Leader only; a removed member's proposals and admissions
    /// refuse from the committed change on.
    #[cfg(feature = "tls")]
    pub async fn remove_member(&self, node_id: NodeId) -> Result<(), Status> {
        let membership = self.raft.metrics().borrow().membership_config.clone();
        let voter = membership.membership().voter_ids().any(|id| id == node_id);
        if voter {
            self.raft
                .change_membership(
                    ChangeMembers::RemoveVoters(BTreeSet::from([node_id])),
                    false,
                )
                .await
                .map_err(|e| Self::write_error(e, format!("remove voter {node_id}")))?;
        } else {
            self.raft
                .change_membership(ChangeMembers::RemoveNodes(BTreeSet::from([node_id])), false)
                .await
                .map_err(|e| Self::write_error(e, format!("remove learner {node_id}")))?;
        }
        Ok(())
    }

    /// Fault injection: refuse every RPC to and from these peers.
    #[cfg(all(feature = "tls", any(test, feature = "fault-injection")))]
    pub fn isolate(&self, peers: impl IntoIterator<Item = NodeId>) {
        self.isolation.isolate(peers);
    }

    /// Refuse what these peers send this node while still calling them:
    /// the node falls behind and can still ask the leader for a lease.
    #[cfg(all(feature = "tls", any(test, feature = "fault-injection")))]
    pub fn isolate_inbound(&self, peers: impl IntoIterator<Item = NodeId>) {
        self.isolation.isolate_inbound(peers);
    }

    #[cfg(all(feature = "tls", any(test, feature = "fault-injection")))]
    pub fn heal(&self) {
        self.isolation.heal();
    }

    pub async fn shutdown(mut self) -> Result<(), Status> {
        #[cfg(feature = "tls")]
        if let Some(watch) = self.digest_watch.take() {
            watch.abort();
        }
        let stopped = self
            .raft
            .shutdown()
            .await
            .map_err(|e| Status::internal(format!("raft shutdown: {e}")));
        if let Some(mut listener) = self.listener.take() {
            if let Some(stop) = listener.stop.take() {
                let _ = stop.send(());
            }
            match tokio::time::timeout(Duration::from_secs(10), &mut listener.task).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(e))) => {
                    return Err(Status::internal(format!("raft listener: {e}")));
                }
                Ok(Err(e)) => return Err(Status::internal(format!("raft listener task: {e}"))),
                Err(_) => {
                    listener.task.abort();
                    return Err(Status::internal(
                        "raft listener did not stop within 10 s; aborted",
                    ));
                }
            }
        }
        stopped
    }
}
