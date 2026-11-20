//! The cluster plan a caller reads: nodes, routes, and pending actions.

use tonic::Status;

use crate::coordinator::TopologyRoute;
use crate::pb::{
    ClusterNode, ClusterNodeState, ClusterPlan, PlacementAction, ShardReplicaRole,
    ShardReplicaState,
};

use super::state::{StoredNodeState, StoredRole, StoredState};
use super::DurableControlPlane;

impl DurableControlPlane {
    pub(super) fn plan(&self) -> Result<ClusterPlan, Status> {
        let state = self.lock_state()?;
        Ok(Self::plan_of(&state))
    }

    #[cfg(test)]
    pub(super) fn topology_routes(&self) -> Result<(u64, Vec<TopologyRoute>), Status> {
        let state = self.lock_state()?;
        Ok(Self::topology_routes_of(&state))
    }

    pub(super) fn topology_routes_of(state: &StoredState) -> (u64, Vec<TopologyRoute>) {
        (
            state.topology.generation,
            state
                .topology
                .routes
                .iter()
                .map(|route| TopologyRoute {
                    addr: route.addr.clone(),
                    replica: route.replica.clone(),
                    hash_range: Some((route.hash_lo, route.hash_hi)),
                    placement: None,
                })
                .collect(),
        )
    }

    pub(super) fn plan_of(state: &StoredState) -> ClusterPlan {
        ClusterPlan {
            collection: state.collection.clone(),
            control_revision: state.revision,
            topology_generation: state.topology.generation,
            nodes: Self::plan_nodes(state),
            replicas: Self::plan_replicas(state),
            actions: Self::plan_actions(state),
            topology_history: state
                .history
                .iter()
                .map(|topology| topology.generation)
                .collect(),
            topology: state
                .topology
                .routes
                .iter()
                .map(|route| crate::pb::PublishedTopologyShard {
                    addr: route.addr.clone(),
                    replica: route.replica.clone().unwrap_or_default(),
                    hash_lo: route.hash_lo,
                    hash_hi: route.hash_hi,
                    has_placement: false,
                    placement: 0,
                })
                .collect(),
        }
    }

    fn plan_nodes(state: &StoredState) -> Vec<ClusterNode> {
        state
            .nodes
            .values()
            .map(|node| ClusterNode {
                collection: state.collection.clone(),
                node_id: node.node_id.clone(),
                addr: node.addr.clone(),
                state: match node.state {
                    StoredNodeState::Active => ClusterNodeState::Active as i32,
                    StoredNodeState::Draining => ClusterNodeState::Draining as i32,
                    StoredNodeState::Expired => ClusterNodeState::Expired as i32,
                },
                expires_unix_ms: node.expires_unix_ms,
                capacity: Some((&node.capacity).into()),
            })
            .collect()
    }

    fn plan_replicas(state: &StoredState) -> Vec<ShardReplicaState> {
        state
            .replicas
            .values()
            .map(|replica| ShardReplicaState {
                collection: state.collection.clone(),
                shard_id: replica.shard_id.clone(),
                node_id: replica.node_id.clone(),
                addr: replica.addr.clone(),
                generation: replica.generation,
                hash_lo: replica.hash_lo,
                hash_hi: replica.hash_hi,
                slot_offset: replica.slot_offset,
                rows: replica.rows,
                bytes: replica.bytes,
                role: match replica.role {
                    StoredRole::Primary => ShardReplicaRole::Primary as i32,
                    StoredRole::Replica => ShardReplicaRole::Replica as i32,
                },
                ready: replica.ready,
                scoring_fingerprint: replica.scoring_fingerprint.clone(),
                analysis_fingerprint: replica.analysis_fingerprint.clone(),
                immutable_segments: replica.immutable_segments,
                tombstones: replica.tombstones,
            })
            .collect()
    }

    fn plan_actions(state: &StoredState) -> Vec<PlacementAction> {
        state
            .actions
            .iter()
            .map(|action| PlacementAction {
                collection: state.collection.clone(),
                action_id: action.action_id,
                kind: action.kind,
                shard_id: action.shard_id.clone(),
                peer_shard_id: action.peer_shard_id.clone(),
                peer_source_generation: action.peer_source_generation,
                source_node_id: action.source_node_id.clone(),
                target_node_id: action.target_node_id.clone(),
                source_generation: action.source_generation,
                target_generation: action.target_generation,
                hash_lo: action.hash_lo,
                hash_hi: action.hash_hi,
                reason: action.reason.clone(),
            })
            .collect()
    }
}
