//! The Raft type configuration and the explicit mappings between the typed
//! protobuf envelopes (`raft_control.proto`) and the pinned library's types.
//! Nothing here serializes a Rust object; every field is named.
use crate::pb::storage::{
    raft_entry::Payload, RaftEntry, RaftLogId, RaftMembership, RaftNode, RaftProposal, RaftReply,
    RaftStoredMembership, RaftVote, RaftVoterSet,
};
use openraft::{
    BasicNode, Entry, EntryPayload, LeaderId, LogId, Membership, StoredMembership, Vote,
};
use std::collections::{BTreeMap, BTreeSet};
use tonic::Status;

openraft::declare_raft_types!(
    /// The source authority's consensus types: proposals and replies are the
    /// typed envelopes, node ids are u64, snapshots stream through files.
    pub ControlRaft:
        D = RaftProposal,
        R = RaftReply,
        NodeId = u64,
        Node = BasicNode,
        Entry = Entry<ControlRaft>,
        SnapshotData = tokio::fs::File,
);

pub type NodeId = u64;

pub fn log_id_to_proto(log_id: &LogId<NodeId>) -> RaftLogId {
    RaftLogId {
        term: log_id.leader_id.term,
        node_id: log_id.leader_id.node_id,
        index: log_id.index,
    }
}

pub fn log_id_from_proto(value: &RaftLogId) -> LogId<NodeId> {
    LogId::new(LeaderId::new(value.term, value.node_id), value.index)
}

pub fn vote_to_proto(vote: &Vote<NodeId>) -> RaftVote {
    RaftVote {
        term: vote.leader_id.term,
        node_id: vote.leader_id.node_id,
        committed: vote.committed,
    }
}

pub fn vote_from_proto(value: &RaftVote) -> Vote<NodeId> {
    if value.committed {
        Vote::new_committed(value.term, value.node_id)
    } else {
        Vote::new(value.term, value.node_id)
    }
}

pub fn membership_to_proto(membership: &Membership<NodeId, BasicNode>) -> RaftMembership {
    RaftMembership {
        configs: membership
            .get_joint_config()
            .iter()
            .map(|set| RaftVoterSet {
                node_ids: set.iter().copied().collect(),
            })
            .collect(),
        nodes: membership
            .nodes()
            .map(|(id, node)| RaftNode {
                node_id: *id,
                addr: node.addr.clone(),
            })
            .collect(),
    }
}

/// Every voter must be a known node with an address; ids are distinct and
/// ascending within a set, nodes distinct and ascending.
pub fn membership_from_proto(
    value: &RaftMembership,
) -> Result<Membership<NodeId, BasicNode>, Status> {
    let mut nodes: BTreeMap<NodeId, BasicNode> = BTreeMap::new();
    let mut last: Option<NodeId> = None;
    for node in &value.nodes {
        if last.is_some_and(|previous| node.node_id <= previous) {
            return Err(Status::data_loss(
                "raft membership nodes are not distinct and ascending",
            ));
        }
        if node.addr.is_empty() || node.addr.len() > 1024 {
            return Err(Status::data_loss(
                "raft membership node address must be 1..1024 bytes",
            ));
        }
        last = Some(node.node_id);
        nodes.insert(
            node.node_id,
            BasicNode {
                addr: node.addr.clone(),
            },
        );
    }
    let mut configs = Vec::with_capacity(value.configs.len());
    for set in &value.configs {
        let mut voters = BTreeSet::new();
        let mut last: Option<NodeId> = None;
        for id in &set.node_ids {
            if last.is_some_and(|previous| *id <= previous) {
                return Err(Status::data_loss(
                    "raft voter set is not distinct and ascending",
                ));
            }
            if !nodes.contains_key(id) {
                return Err(Status::data_loss(format!(
                    "raft voter {id} has no node record in the membership"
                )));
            }
            last = Some(*id);
            voters.insert(*id);
        }
        configs.push(voters);
    }
    Ok(Membership::new(configs, nodes))
}

pub fn stored_membership_to_proto(
    stored: &StoredMembership<NodeId, BasicNode>,
) -> RaftStoredMembership {
    RaftStoredMembership {
        log_id: stored.log_id().as_ref().map(log_id_to_proto),
        membership: Some(membership_to_proto(stored.membership())),
    }
}

pub fn stored_membership_from_proto(
    value: &RaftStoredMembership,
) -> Result<StoredMembership<NodeId, BasicNode>, Status> {
    let membership = value
        .membership
        .as_ref()
        .ok_or_else(|| Status::data_loss("raft stored membership has no membership"))?;
    Ok(StoredMembership::new(
        value.log_id.as_ref().map(log_id_from_proto),
        membership_from_proto(membership)?,
    ))
}

pub fn entry_to_proto(entry: &Entry<ControlRaft>) -> RaftEntry {
    RaftEntry {
        log_id: Some(log_id_to_proto(&entry.log_id)),
        payload: Some(match &entry.payload {
            EntryPayload::Blank => Payload::Blank(true),
            EntryPayload::Normal(proposal) => Payload::Proposal(proposal.clone()),
            EntryPayload::Membership(membership) => {
                Payload::Membership(membership_to_proto(membership))
            }
        }),
    }
}

pub fn entry_from_proto(value: RaftEntry) -> Result<Entry<ControlRaft>, Status> {
    let log_id = value
        .log_id
        .as_ref()
        .map(log_id_from_proto)
        .ok_or_else(|| Status::data_loss("raft entry has no log id"))?;
    let payload = match value.payload {
        Some(Payload::Blank(true)) => EntryPayload::Blank,
        Some(Payload::Proposal(proposal)) => {
            validate_proposal(&proposal).map_err(|e| Status::data_loss(e.message().to_string()))?;
            EntryPayload::Normal(proposal)
        }
        Some(Payload::Membership(membership)) => {
            EntryPayload::Membership(membership_from_proto(&membership)?)
        }
        _ => {
            return Err(Status::data_loss(
                "raft entry payload is missing or unsupported",
            ))
        }
    };
    Ok(Entry { log_id, payload })
}

/// A proposal names a bounded principal and exactly one command.
pub fn validate_proposal(proposal: &RaftProposal) -> Result<(), Status> {
    if proposal.format_version != 1 {
        return Err(Status::invalid_argument("raft proposal requires format 1"));
    }
    crate::source_owner::principal(&proposal.principal)?;
    if proposal.command.is_none() {
        return Err(Status::invalid_argument("raft proposal carries no command"));
    }
    Ok(())
}
