//! Reconciliation: derive the desired routes and queue the actions toward them.

mod placement;
mod rebalance;
mod routes;

use std::collections::BTreeSet;

use tonic::Status;

use crate::pb::ClusterPlan;

use super::state::{StoredNodeState, StoredState, StoredTopology};
use super::DurableControlPlane;

impl DurableControlPlane {
    fn reconcile_locked(&self, state: &mut StoredState, now: u64) -> Result<bool, Status> {
        let revision = state
            .revision
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("control revision overflow"))?;
        for node in state.nodes.values_mut() {
            if node.expires_unix_ms <= now && node.state != StoredNodeState::Expired {
                node.state = StoredNodeState::Expired;
            }
        }

        let shard_ids: BTreeSet<String> = state
            .replicas
            .values()
            .map(|replica| replica.shard_id.clone())
            .collect();
        for shard_id in shard_ids {
            self.reconcile_shard(state, &shard_id)?;
        }
        self.queue_merges(state)?;
        Self::queue_rebalance(state)?;

        let topology_changed = self.publish_routes(state)?;
        state.revision = revision;
        Ok(topology_changed)
    }

    pub(super) fn reconcile(&self, dry_run: bool, now: u64) -> Result<(ClusterPlan, bool), Status> {
        let mut state = self.lock_state()?;
        let mut candidate = state.clone();
        let changed = self.reconcile_locked(&mut candidate, now)?;
        let plan = Self::plan_of(&candidate);
        if !dry_run {
            self.persist_locked(&candidate).map_err(Status::internal)?;
            *state = candidate;
        }
        Ok((plan, changed))
    }

    pub(super) fn rollback(&self, requested: u64) -> Result<(ClusterPlan, bool), Status> {
        let mut state = self.lock_state()?;
        let position = if requested == 0 {
            state.history.len().checked_sub(1)
        } else {
            state
                .history
                .iter()
                .position(|topology| topology.generation == requested)
        }
        .ok_or_else(|| Status::not_found("requested topology is not in durable history"))?;
        let generation = state
            .topology
            .generation
            .checked_add(1)
            .ok_or_else(|| Status::failed_precondition("topology generation overflow"))?;
        let revision = state
            .revision
            .checked_add(1)
            .ok_or_else(|| Status::resource_exhausted("control revision overflow"))?;
        let before = state.clone();
        let old = state.history.remove(position);
        let current = state.topology.clone();
        state.history.push(current);
        state.topology = StoredTopology {
            generation,
            routes: old.routes,
        };
        state.revision = revision;
        if let Err(error) = self.persist_locked(&state) {
            *state = before;
            return Err(Status::internal(error));
        }
        Ok((Self::plan_of(&state), true))
    }
}
