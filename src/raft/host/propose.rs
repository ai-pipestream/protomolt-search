//! Proposals: every public entry admits its command before it is
//! written to the log.

use super::*;

impl RaftHost {
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

    /// A collection control-state install through the log.
    pub async fn propose_install(
        &self,
        principal: &str,
        command: &ControlInstallCommand,
    ) -> Result<crate::pb::storage::ControlInstallDecision, Status> {
        let reply = self
            .propose(RaftProposal {
                format_version: 1,
                principal: principal.into(),
                command: Some(Command::Install(command.clone())),
            })
            .await?;
        match reply.reply {
            Some(Reply::Install(decision)) => Ok(decision),
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
}
