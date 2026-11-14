//! Node membership: registration, lease renewal, drain, and shard reports.

use tonic::Status;

use crate::pb::{
    DrainNodeRequest, NodeLease, RegisterNodeRequest, RenewNodeLeaseRequest, ReportShardRequest,
    ShardReplicaRole,
};

use super::state::{
    replica_key, StoredNode, StoredNodeState, StoredReplica, StoredRole, StoredState,
};
use super::DurableControlPlane;

impl DurableControlPlane {
    pub(super) fn validate_lease<'a>(
        state: &'a mut StoredState,
        node_id: &str,
        token: u64,
        now: u64,
    ) -> Result<&'a mut StoredNode, Status> {
        let node = state
            .nodes
            .get_mut(node_id)
            .ok_or_else(|| Status::not_found(format!("node {node_id:?} is not registered")))?;
        if token == 0 || token != node.lease_token {
            return Err(Status::failed_precondition(format!(
                "node {node_id:?} lease token does not match"
            )));
        }
        if node.expires_unix_ms <= now {
            return Err(Status::failed_precondition(format!(
                "node {node_id:?} lease expired at {}",
                node.expires_unix_ms
            )));
        }
        Ok(node)
    }

    pub(super) fn register(
        &self,
        request: RegisterNodeRequest,
        now: u64,
    ) -> Result<NodeLease, Status> {
        if request.node_id.trim().is_empty() || request.addr.trim().is_empty() {
            return Err(Status::invalid_argument(
                "node registration needs non-empty node_id and addr",
            ));
        }
        let duration = self.lease_duration(request.lease_ms)?;
        let mut state = self.lock_state()?;
        if let Some(held) = state.nodes.get(&request.node_id) {
            if held.state != StoredNodeState::Expired && held.addr != request.addr {
                return Err(Status::already_exists(format!(
                    "node {:?} is already leased at {:?}",
                    request.node_id, held.addr
                )));
            }
        }
        let token = state.next_token;
        let next_token = token
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("lease token space exhausted"))?;
        let revision = state
            .revision
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("control revision overflow"))?;
        let expires = now
            .checked_add(duration)
            .ok_or_else(|| Status::resource_exhausted("lease expiry overflow"))?;
        let before = state.clone();
        state.next_token = next_token;
        state.nodes.insert(
            request.node_id.clone(),
            StoredNode {
                node_id: request.node_id.clone(),
                addr: request.addr,
                state: StoredNodeState::Active,
                lease_token: token,
                expires_unix_ms: expires,
                capacity: request.capacity.unwrap_or_default().into(),
            },
        );
        state.revision = revision;
        if let Err(error) = self.persist_locked(&state) {
            *state = before;
            return Err(Status::internal(error));
        }
        Ok(NodeLease {
            node_id: request.node_id,
            lease_token: token,
            expires_unix_ms: expires,
            control_revision: state.revision,
        })
    }

    pub(super) fn renew(
        &self,
        request: RenewNodeLeaseRequest,
        now: u64,
    ) -> Result<NodeLease, Status> {
        let duration = self.lease_duration(request.lease_ms)?;
        let mut state = self.lock_state()?;
        let revision = state
            .revision
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("control revision overflow"))?;
        let before = state.clone();
        let node = Self::validate_lease(&mut state, &request.node_id, request.lease_token, now)?;
        node.expires_unix_ms = now
            .checked_add(duration)
            .ok_or_else(|| Status::resource_exhausted("lease expiry overflow"))?;
        if let Some(capacity) = request.capacity {
            node.capacity = capacity.into();
        }
        let lease = NodeLease {
            node_id: node.node_id.clone(),
            lease_token: node.lease_token,
            expires_unix_ms: node.expires_unix_ms,
            control_revision: revision,
        };
        state.revision = revision;
        if let Err(error) = self.persist_locked(&state) {
            *state = before;
            return Err(Status::internal(error));
        }
        Ok(lease)
    }

    pub(super) fn drain(&self, request: DrainNodeRequest, now: u64) -> Result<(), Status> {
        let mut state = self.lock_state()?;
        let revision = state
            .revision
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("control revision overflow"))?;
        let before = state.clone();
        let node = Self::validate_lease(&mut state, &request.node_id, request.lease_token, now)?;
        node.state = StoredNodeState::Draining;
        state.revision = revision;
        if let Err(error) = self.persist_locked(&state) {
            *state = before;
            return Err(Status::internal(error));
        }
        Ok(())
    }

    pub(super) fn report(&self, request: ReportShardRequest, now: u64) -> Result<(), Status> {
        let replica = request
            .replica
            .ok_or_else(|| Status::invalid_argument("shard report is missing replica state"))?;
        if replica.shard_id.trim().is_empty() {
            return Err(Status::invalid_argument("shard report needs shard_id"));
        }
        let role = match ShardReplicaRole::try_from(replica.role) {
            Ok(ShardReplicaRole::Primary) => StoredRole::Primary,
            Ok(ShardReplicaRole::Replica) => StoredRole::Replica,
            _ => {
                return Err(Status::invalid_argument(
                    "shard report needs PRIMARY or REPLICA role",
                ))
            }
        };
        if replica.hash_lo > replica.hash_hi {
            return Err(Status::invalid_argument(
                "shard report hash range is inverted",
            ));
        }
        if replica.tombstones > replica.rows {
            return Err(Status::invalid_argument(
                "shard tombstone count exceeds physical rows",
            ));
        }
        let mut state = self.lock_state()?;
        let revision = state
            .revision
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("control revision overflow"))?;
        let before = state.clone();
        let node_addr =
            Self::validate_lease(&mut state, &request.node_id, request.lease_token, now)?
                .addr
                .clone();
        if !replica.node_id.is_empty() && replica.node_id != request.node_id {
            return Err(Status::invalid_argument(
                "shard report node_id differs from the lease owner",
            ));
        }
        let addr = Self::shard_addr(&state, &request.node_id, &node_addr, replica.addr)?;
        let key = replica_key(&replica.shard_id, &request.node_id);
        state.replicas.insert(
            key,
            StoredReplica {
                shard_id: replica.shard_id,
                node_id: request.node_id,
                addr,
                generation: replica.generation,
                hash_lo: replica.hash_lo,
                hash_hi: replica.hash_hi,
                slot_offset: replica.slot_offset,
                rows: replica.rows,
                bytes: replica.bytes,
                role,
                ready: replica.ready,
                scoring_fingerprint: replica.scoring_fingerprint,
                analysis_fingerprint: replica.analysis_fingerprint,
                immutable_segments: replica.immutable_segments,
                tombstones: replica.tombstones,
            },
        );
        state.revision = revision;
        if let Err(error) = self.persist_locked(&state) {
            *state = before;
            return Err(Status::internal(error));
        }
        Ok(())
    }

    /// The listener a reported copy is served on: the node's registered
    /// address when the report leaves it empty, else the address the
    /// report names — the lease owner vouches for its own listeners
    /// (one per shard, one per placed replica), and another node's
    /// registered address is not one of them.
    pub(super) fn shard_addr(
        state: &StoredState,
        node_id: &str,
        node_addr: &str,
        reported: String,
    ) -> Result<String, Status> {
        if reported.is_empty() {
            return Ok(node_addr.to_string());
        }
        if let Some(other) = state
            .nodes
            .values()
            .find(|other| other.node_id != node_id && other.addr == reported)
        {
            return Err(Status::invalid_argument(format!(
                "shard addr {reported:?} is the registered address of node {:?}, not a listener \
                 of node {node_id:?}",
                other.node_id
            )));
        }
        Ok(reported)
    }
}
