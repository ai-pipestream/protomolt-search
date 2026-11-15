//! The worker pass: the plan's actions for this node, then the placed
//! replicas kept caught up with their primaries.

use std::sync::atomic::Ordering;

use crate::pb::{ClusterPlan, PlacementAction, PlacementActionKind, ShardReplicaRole};
use crate::replication::{sync_once, ReplicaCursor};

use super::host::node_health;
use super::NodeAgent;

impl NodeAgent {
    /// One worker pass: read the plan, execute the actions assigned to
    /// this node in order, and keep placed replicas caught up. Every
    /// action runs even after one fails; the first failure is returned.
    pub async fn run_once(&self) -> Result<(), String> {
        let plan = self.plan().await?;
        let mut first_error = None;
        let node_id = &self.inner.config.node_id;
        for action in plan
            .actions
            .iter()
            .filter(|action| &action.target_node_id == node_id)
        {
            if let Err(error) = self.execute(action, &plan).await {
                eprintln!(
                    "node {node_id:?}: action {} on shard {:?}: {error}",
                    action.action_id, action.shard_id
                );
                first_error.get_or_insert(error);
            }
        }
        if let Err(error) = self.follow_primaries(&plan).await {
            first_error.get_or_insert(error);
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    async fn execute(&self, action: &PlacementAction, plan: &ClusterPlan) -> Result<(), String> {
        match PlacementActionKind::try_from(action.kind) {
            Ok(PlacementActionKind::CopyReplica) => self.execute_copy(action, plan).await,
            Ok(PlacementActionKind::DropReplica) => self.execute_drop(action, plan).await,
            Ok(PlacementActionKind::SplitShard) => self.execute_split(action, plan).await,
            other => {
                self.log_unhandled(action, other.ok());
                Ok(())
            }
        }
    }

    /// An action this worker does not handle stays pending; it is logged
    /// once.
    fn log_unhandled(&self, action: &PlacementAction, kind: Option<PlacementActionKind>) {
        let first = self
            .inner
            .unhandled
            .lock()
            .expect("unhandled lock")
            .insert(action.action_id);
        if !first {
            return;
        }
        self.inner
            .stats
            .unhandled_actions
            .fetch_add(1, Ordering::Relaxed);
        eprintln!(
            "node {:?}: action {} ({}) on shard {:?} is not handled by this worker; it stays \
             pending",
            self.inner.config.node_id,
            action.action_id,
            kind.map(|kind| kind.as_str_name().to_string())
                .unwrap_or_else(|| format!("kind {}", action.kind)),
            action.shard_id
        );
    }

    /// Placed replicas that finished their bootstrap keep following their
    /// primary between plan changes. Every replica is tried; the first
    /// failure is returned.
    async fn follow_primaries(&self, plan: &ClusterPlan) -> Result<(), String> {
        let node_id = &self.inner.config.node_id;
        let mut first_error = None;
        for shard_id in self.shard_ids() {
            let Some(placed) = self.placed(&shard_id) else {
                continue;
            };
            if !placed.ready || placed.completed_action == 0 {
                continue;
            }
            let Some(primary) = plan.replicas.iter().find(|record| {
                record.shard_id == shard_id
                    && record.role == ShardReplicaRole::Primary as i32
                    && &record.node_id != node_id
            }) else {
                continue;
            };
            if let Err(error) = self.catch_up(&shard_id, &primary.addr, 1).await {
                eprintln!(
                    "node {node_id:?}: replica {shard_id:?} catch-up from {}: {error}",
                    primary.addr
                );
                first_error.get_or_insert(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Run `sync_once` from the persisted cursor until the primary's
    /// watermark is within the lag bound, at most `rounds` times.
    /// Returns the lag after the last round.
    pub(super) async fn catch_up(
        &self,
        shard_id: &str,
        primary_addr: &str,
        rounds: u32,
    ) -> Result<u64, String> {
        let shard = self
            .served(shard_id)
            .ok_or_else(|| format!("shard {shard_id:?} is not served here"))?;
        let placed = shard
            .placed
            .ok_or_else(|| format!("shard {shard_id:?} is configured, not placed"))?;
        let mut cursor = ReplicaCursor {
            primary: primary_addr.to_string(),
            replica: shard.addr,
            wal_generation: placed.cursor_generation,
            clock: placed.cursor_clock,
        };
        let mut lag = u64::MAX;
        for _ in 0..rounds {
            cursor = sync_once(&cursor).await?;
            self.update_placed(shard_id, |placed| {
                placed.cursor_generation = cursor.wal_generation;
                placed.cursor_clock = cursor.clock;
            })?;
            let source = node_health(primary_addr).await?;
            lag = source.wal_high_watermark.saturating_sub(cursor.clock);
            if lag <= self.inner.config.lag_bound {
                break;
            }
        }
        Ok(lag)
    }
}
