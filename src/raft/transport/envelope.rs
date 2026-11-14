//! Envelope mappings between the openraft messages and their protobuf
//! forms.

use super::*;

pub(super) fn wire(error: Status) -> Status {
    Status::invalid_argument(error.message().to_string())
}

pub(super) fn required_vote(
    vote: Option<&crate::pb::storage::RaftVote>,
) -> Result<openraft::Vote<NodeId>, Status> {
    vote.map(vote_from_proto)
        .ok_or_else(|| Status::invalid_argument("raft rpc carries no vote"))
}

/// Entries and snapshots come from a leader, and a leader's vote is
/// committed by definition; the library asserts as much when it accepts
/// them, so a request under an uncommitted vote is refused before it.
pub(super) fn leader_vote(
    vote: Option<&crate::pb::storage::RaftVote>,
) -> Result<openraft::Vote<NodeId>, Status> {
    let vote = required_vote(vote)?;
    if !vote.is_committed() {
        return Err(Status::invalid_argument(
            "entries and snapshots come from a leader; this vote is not committed",
        ));
    }
    Ok(vote)
}

pub(super) fn append_request_from_proto(
    value: RaftAppendEntriesRequest,
) -> Result<AppendEntriesRequest<ControlRaft>, Status> {
    let vote = leader_vote(value.vote.as_ref())?;
    let entries = value
        .entries
        .into_iter()
        .map(|entry| entry_from_proto(entry).map_err(wire))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(AppendEntriesRequest {
        vote,
        prev_log_id: value.prev_log_id.as_ref().map(log_id_from_proto),
        entries,
        leader_commit: value.leader_commit.as_ref().map(log_id_from_proto),
    })
}

pub(super) fn append_request_to_proto(
    header: RaftRpcHeader,
    rpc: &AppendEntriesRequest<ControlRaft>,
) -> RaftAppendEntriesRequest {
    RaftAppendEntriesRequest {
        header: Some(header),
        vote: Some(vote_to_proto(&rpc.vote)),
        prev_log_id: rpc.prev_log_id.as_ref().map(log_id_to_proto),
        entries: rpc.entries.iter().map(entry_to_proto).collect(),
        leader_commit: rpc.leader_commit.as_ref().map(log_id_to_proto),
    }
}

pub(super) fn append_response_to_proto(
    header: RaftRpcHeader,
    response: AppendEntriesResponse<NodeId>,
) -> RaftAppendEntriesResponse {
    let outcome = match response {
        AppendEntriesResponse::Success => AppendOutcome::Success(true),
        AppendEntriesResponse::PartialSuccess(matching) => {
            AppendOutcome::PartialSuccess(RaftPartialSuccess {
                matching: matching.as_ref().map(log_id_to_proto),
            })
        }
        AppendEntriesResponse::Conflict => AppendOutcome::Conflict(true),
        AppendEntriesResponse::HigherVote(vote) => AppendOutcome::HigherVote(vote_to_proto(&vote)),
    };
    RaftAppendEntriesResponse {
        header: Some(header),
        outcome: Some(outcome),
    }
}

pub(super) fn append_response_from_proto(
    value: RaftAppendEntriesResponse,
) -> Result<AppendEntriesResponse<NodeId>, Status> {
    match value.outcome {
        Some(AppendOutcome::Success(true)) => Ok(AppendEntriesResponse::Success),
        Some(AppendOutcome::PartialSuccess(partial)) => Ok(AppendEntriesResponse::PartialSuccess(
            partial.matching.as_ref().map(log_id_from_proto),
        )),
        Some(AppendOutcome::Conflict(true)) => Ok(AppendEntriesResponse::Conflict),
        Some(AppendOutcome::HigherVote(vote)) => {
            Ok(AppendEntriesResponse::HigherVote(vote_from_proto(&vote)))
        }
        _ => Err(Status::invalid_argument(
            "append entries reply carries no recognized outcome",
        )),
    }
}

pub(super) fn vote_request_from_proto(
    value: RaftVoteRequest,
) -> Result<VoteRequest<NodeId>, Status> {
    Ok(VoteRequest {
        vote: required_vote(value.vote.as_ref())?,
        last_log_id: value.last_log_id.as_ref().map(log_id_from_proto),
    })
}

pub(super) fn vote_request_to_proto(
    header: RaftRpcHeader,
    rpc: &VoteRequest<NodeId>,
) -> RaftVoteRequest {
    RaftVoteRequest {
        header: Some(header),
        vote: Some(vote_to_proto(&rpc.vote)),
        last_log_id: rpc.last_log_id.as_ref().map(log_id_to_proto),
    }
}

pub(super) fn vote_response_to_proto(
    header: RaftRpcHeader,
    response: VoteResponse<NodeId>,
) -> RaftVoteResponse {
    RaftVoteResponse {
        header: Some(header),
        vote: Some(vote_to_proto(&response.vote)),
        vote_granted: response.vote_granted,
        last_log_id: response.last_log_id.as_ref().map(log_id_to_proto),
    }
}

pub(super) fn vote_response_from_proto(
    value: RaftVoteResponse,
) -> Result<VoteResponse<NodeId>, Status> {
    Ok(VoteResponse {
        vote: required_vote(value.vote.as_ref())?,
        vote_granted: value.vote_granted,
        last_log_id: value.last_log_id.as_ref().map(log_id_from_proto),
    })
}

/// The snapshot meta on the wire carries the group and the checksum the id
/// names, so a receiver checks both before the library sees the chunk.
pub(super) fn snapshot_meta_to_proto(
    group: &SourceAuthorityIdentity,
    meta: &SnapshotMeta<NodeId, BasicNode>,
) -> Result<RaftSnapshotMeta, Status> {
    let signature = super::super::state_machine::parse_snapshot_id(&meta.snapshot_id)?;
    Ok(RaftSnapshotMeta {
        format_version: 1,
        group: Some(group.clone()),
        last_log_id: meta.last_log_id.as_ref().map(log_id_to_proto),
        membership: Some(stored_membership_to_proto(&meta.last_membership)),
        snapshot_id: meta.snapshot_id.clone(),
        length: signature.length,
        sha256: signature.sha256.to_vec(),
    })
}

pub(super) fn snapshot_meta_from_proto(
    group: &SourceAuthorityIdentity,
    value: &RaftSnapshotMeta,
) -> Result<SnapshotMeta<NodeId, BasicNode>, Status> {
    if value.format_version != 1 {
        return Err(Status::invalid_argument("snapshot meta requires format 1"));
    }
    if value.group.as_ref() != Some(group) {
        return Err(reasons::reason(
            Status::permission_denied("snapshot meta names another group"),
            reasons::TRANSPORT_WRONG_GROUP,
        ));
    }
    let signature =
        super::super::state_machine::parse_snapshot_id(&value.snapshot_id).map_err(wire)?;
    if signature.length != value.length || signature.sha256[..] != value.sha256[..] {
        return Err(Status::invalid_argument(
            "snapshot meta length or checksum differs from its id",
        ));
    }
    let membership = value
        .membership
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("snapshot meta carries no membership"))?;
    Ok(SnapshotMeta {
        last_log_id: value.last_log_id.as_ref().map(log_id_from_proto),
        last_membership: stored_membership_from_proto(membership).map_err(wire)?,
        snapshot_id: value.snapshot_id.clone(),
    })
}

pub(super) fn install_request_from_proto(
    group: &SourceAuthorityIdentity,
    value: RaftInstallSnapshotRequest,
) -> Result<InstallSnapshotRequest<ControlRaft>, Status> {
    let meta = value
        .meta
        .as_ref()
        .ok_or_else(|| Status::invalid_argument("snapshot chunk carries no meta"))?;
    let meta = snapshot_meta_from_proto(group, meta)?;
    // A chunk past the announced length is rejected in `stage`, which
    // drops the transfer it belongs to with its bytes.
    Ok(InstallSnapshotRequest {
        vote: leader_vote(value.vote.as_ref())?,
        meta,
        offset: value.offset,
        data: value.data,
        done: value.done,
    })
}

pub(super) fn install_request_to_proto(
    header: RaftRpcHeader,
    group: &SourceAuthorityIdentity,
    rpc: InstallSnapshotRequest<ControlRaft>,
) -> Result<RaftInstallSnapshotRequest, Status> {
    Ok(RaftInstallSnapshotRequest {
        header: Some(header),
        vote: Some(vote_to_proto(&rpc.vote)),
        meta: Some(snapshot_meta_to_proto(group, &rpc.meta)?),
        offset: rpc.offset,
        data: rpc.data,
        done: rpc.done,
    })
}
