//! Publication of the desired routes as the next topology generation.

use std::collections::BTreeMap;

use tonic::Status;

use crate::control_plane::state::{
    StoredReplica, StoredRole, StoredRoute, StoredState, StoredTopology,
};
use crate::control_plane::DurableControlPlane;

impl DurableControlPlane {
    fn desired_routes(state: &StoredState) -> Result<Option<Vec<StoredRoute>>, String> {
        let mut by_shard: BTreeMap<&str, Vec<&StoredReplica>> = BTreeMap::new();
        for replica in state.replicas.values().filter(|replica| replica.ready) {
            by_shard.entry(&replica.shard_id).or_default().push(replica);
        }
        let mut routes = Vec::new();
        for (_shard_id, replicas) in by_shard {
            let Some(primary) = replicas.iter().copied().find(|replica| {
                replica.role == StoredRole::Primary && Self::active_node(state, &replica.node_id)
            }) else {
                continue;
            };
            let replica = replicas
                .iter()
                .copied()
                .find(|replica| {
                    replica.role == StoredRole::Replica
                        && Self::active_node(state, &replica.node_id)
                        && replica.generation == primary.generation
                        && replica.scoring_fingerprint == primary.scoring_fingerprint
                        && replica.analysis_fingerprint == primary.analysis_fingerprint
                })
                .map(|replica| replica.addr.clone());
            routes.push(StoredRoute {
                addr: primary.addr.clone(),
                replica,
                hash_lo: primary.hash_lo,
                hash_hi: primary.hash_hi,
            });
        }
        routes.sort_by_key(|route| route.hash_lo);
        if !routes.is_empty() {
            let mut expected = 0u64;
            for route in &routes {
                if route.hash_lo != expected {
                    return Ok(None);
                }
                expected = route.hash_hi.checked_add(1).unwrap_or(0);
            }
            if routes.last().is_some_and(|route| route.hash_hi != u64::MAX) {
                return Ok(None);
            }
        }
        Ok((!routes.is_empty()).then_some(routes))
    }

    /// Publish the desired routes when they are complete and differ from
    /// the current topology; the replaced topology joins the bounded
    /// history. Returns whether the topology changed.
    pub(super) fn publish_routes(&self, state: &mut StoredState) -> Result<bool, Status> {
        let routes = Self::desired_routes(state).map_err(Status::failed_precondition)?;
        let topology_changed = routes
            .as_ref()
            .is_some_and(|routes| routes != &state.topology.routes);
        if topology_changed {
            state.history.push(state.topology.clone());
            if state.history.len() > self.policy.history_limit {
                state.history.remove(0);
            }
            state.topology = StoredTopology {
                generation: state
                    .topology
                    .generation
                    .checked_add(1)
                    .ok_or_else(|| Status::failed_precondition("topology generation overflow"))?
                    .max(1),
                routes: routes.expect("topology_changed requires complete routes"),
            };
        }
        Ok(topology_changed)
    }
}
