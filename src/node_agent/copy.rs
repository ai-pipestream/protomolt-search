//! `COPY_REPLICA` and `DROP_REPLICA`: bootstrap a replica of a shard
//! here, or remove a retired copy.

use std::sync::atomic::Ordering;

use tonic::{Code, Request};

use crate::node::NodeServiceImpl;
use crate::pb::node_service_server::NodeService;
use crate::pb::{
    ClusterPlan, HealthRequest, InstallSnapshotFromRequest, PlacementAction, ShardReplicaRole,
    ShardReplicaState,
};

use super::host::{node_health, rows_of};
use super::{NodeAgent, PlacedShard, CATCH_UP_ROUNDS, COMPLETION_ROUNDS};

/// What one completion attempt came to.
enum Completion {
    Done,
    /// The source moved since the last catch-up: sync again first.
    SourceMoved,
}

impl NodeAgent {
    /// `COPY_REPLICA`: place the shard, install the primary's image,
    /// catch up, report ready, and complete with counts that match the
    /// source at the completion clock.
    pub async fn execute_copy(
        &self,
        action: &PlacementAction,
        plan: &ClusterPlan,
    ) -> Result<(), String> {
        let source = copy_source(action, plan)?;
        if source.generation != action.source_generation {
            return Err(format!(
                "action {} was planned from generation {} but the source is at {}; waiting for \
                 the plane to replan",
                action.action_id, action.source_generation, source.generation
            ));
        }
        let node = self.install_copy(action, source).await?;
        let lag = self
            .catch_up(&action.shard_id, &source.addr, CATCH_UP_ROUNDS)
            .await?;
        if lag > self.inner.config.lag_bound {
            return Err(format!(
                "replica {:?} is still {lag} clocks behind {} after {CATCH_UP_ROUNDS} rounds; \
                 continuing next tick",
                action.shard_id, source.addr
            ));
        }
        self.update_placed(&action.shard_id, |placed| placed.ready = true)?;
        self.report_shard_with(&action.shard_id, plan).await?;
        self.before_complete().await;
        // Complete only with counts that match the source now: a source
        // that moved since the last catch-up is synced again first, and
        // a completion the plane refuses (its record of the source is
        // older or newer than the live source) is retried next tick.
        for _ in 0..COMPLETION_ROUNDS {
            match self
                .complete_copy(action, plan, &node, &source.addr)
                .await?
            {
                Completion::Done => return Ok(()),
                Completion::SourceMoved => continue,
            }
        }
        Err(format!(
            "source {} kept moving through {COMPLETION_ROUNDS} completion rounds; continuing \
             next tick",
            source.addr
        ))
    }

    /// Place the copy if it is new, and install the source's image when
    /// it has none for the action's generation. Returns the copy's node.
    async fn install_copy(
        &self,
        action: &PlacementAction,
        source: &ShardReplicaState,
    ) -> Result<NodeServiceImpl, String> {
        let placed = match self.placed(&action.shard_id) {
            Some(placed) => placed,
            None => {
                let placed = PlacedShard {
                    shard_id: action.shard_id.clone(),
                    collection: self.inner.config.collection.clone(),
                    slot_offset: source.slot_offset,
                    hash_lo: source.hash_lo,
                    hash_hi: source.hash_hi,
                    source_generation: action.target_generation,
                    ..Default::default()
                };
                self.serve_placed(placed).await?;
                self.placed(&action.shard_id).expect("served above")
            }
        };
        let node = self
            .served(&action.shard_id)
            .map(|shard| shard.node)
            .ok_or_else(|| format!("shard {:?} vanished", action.shard_id))?;
        if placed.installed && placed.source_generation == action.target_generation {
            return Ok(node);
        }
        let request = InstallSnapshotFromRequest {
            source: Some(crate::pb::install_snapshot_from_request::Source::PeerAddr(
                source.addr.clone(),
            )),
            expected_manifest_sha256: String::new(),
            bearer_token: String::new(),
        };
        let installed = NodeService::install_snapshot_from(&node, Request::new(request))
            .await
            .map_err(|e| format!("install from {}: {e}", source.addr))?
            .into_inner();
        let manifest = installed
            .manifest
            .ok_or_else(|| "install returned no manifest".to_string())?;
        if !manifest.wal_clocked {
            return Err(format!(
                "source {} has no fully clocked WAL; a replica cannot catch up from it",
                source.addr
            ));
        }
        self.inner.stats.installs.fetch_add(1, Ordering::Relaxed);
        self.update_placed(&action.shard_id, |placed| {
            placed.installed = true;
            placed.source_generation = action.target_generation;
            placed.cursor_generation = manifest.wal_generation;
            placed.cursor_clock = manifest.wal_high_watermark;
            placed.ready = false;
        })?;
        Ok(node)
    }

    /// One completion round: sync once more, and complete when the copy's
    /// counts equal the live source's.
    async fn complete_copy(
        &self,
        action: &PlacementAction,
        plan: &ClusterPlan,
        node: &NodeServiceImpl,
        source_addr: &str,
    ) -> Result<Completion, String> {
        self.catch_up(&action.shard_id, source_addr, 1).await?;
        let local = NodeService::health(node, Request::new(HealthRequest {}))
            .await
            .map_err(|e| format!("local health: {e}"))?
            .into_inner();
        let live = node_health(source_addr).await?;
        if rows_of(&local) != rows_of(&live) || local.deleted_docs != live.deleted_docs {
            self.inner
                .stats
                .stale_resyncs
                .fetch_add(1, Ordering::Relaxed);
            return Ok(Completion::SourceMoved);
        }
        let shard = self
            .served(&action.shard_id)
            .ok_or_else(|| format!("shard {:?} vanished", action.shard_id))?;
        let Some(mut output) = self.replica_state(&shard, plan).await? else {
            return Err("placed shard has no hash range".to_string());
        };
        output.role = ShardReplicaRole::Replica as i32;
        output.ready = true;
        match self.complete_action(action.action_id, vec![output]).await? {
            Ok(()) => {}
            Err(status)
                if status.code() == Code::FailedPrecondition
                    && status.message().contains("differs from its source") =>
            {
                self.inner
                    .stats
                    .completion_refusals
                    .fetch_add(1, Ordering::Relaxed);
                return Err(format!(
                    "completion of action {} refused: {}; the copy is caught up to the live \
                     source and completes once the source's report agrees",
                    action.action_id,
                    status.message()
                ));
            }
            Err(status) => return Err(format!("complete action {}: {status}", action.action_id)),
        }
        self.update_placed(&action.shard_id, |placed| {
            placed.completed_action = action.action_id
        })?;
        self.inner
            .stats
            .copies_completed
            .fetch_add(1, Ordering::Relaxed);
        eprintln!(
            "node {:?}: replica {:?} bootstrapped from {source_addr} (action {})",
            self.inner.config.node_id, action.shard_id, action.action_id
        );
        Ok(Completion::Done)
    }

    /// `DROP_REPLICA`: remove this node's retired copy. A copy the plane
    /// still lists as primary, or a shard from the configuration, is
    /// refused and the action stays pending.
    pub async fn execute_drop(
        &self,
        action: &PlacementAction,
        plan: &ClusterPlan,
    ) -> Result<(), String> {
        let node_id = &self.inner.config.node_id;
        if let Some(record) = plan
            .replicas
            .iter()
            .find(|record| record.shard_id == action.shard_id && &record.node_id == node_id)
        {
            if record.role == ShardReplicaRole::Primary as i32 {
                return Err(format!(
                    "refusing to drop shard {:?}: the plane lists this node's copy as its serving \
                     primary",
                    action.shard_id
                ));
            }
        }
        let removed = {
            let mut shards = self.shards();
            match shards.get(&action.shard_id) {
                Some(shard) if shard.placed.is_none() => {
                    return Err(format!(
                        "refusing to drop shard {:?}: it is configured statically on this node; \
                         remove it from the configuration instead",
                        action.shard_id
                    ));
                }
                Some(_) => shards.remove(&action.shard_id),
                None => None,
            }
        };
        if let Some(shard) = removed {
            let dir = self.remove_placed(shard, &action.shard_id)?;
            eprintln!(
                "node {node_id:?}: dropped shard {:?} ({})",
                action.shard_id,
                dir.display()
            );
        }
        self.complete_action(action.action_id, Vec::new())
            .await?
            .map_err(|e| format!("complete drop action {}: {e}", action.action_id))?;
        self.inner
            .stats
            .drops_completed
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }
}

/// The plan's record of the copy's source: the action's source node,
/// else the shard's primary.
fn copy_source<'a>(
    action: &PlacementAction,
    plan: &'a ClusterPlan,
) -> Result<&'a ShardReplicaState, String> {
    plan.replicas
        .iter()
        .find(|record| {
            record.shard_id == action.shard_id && record.node_id == action.source_node_id
        })
        .or_else(|| {
            plan.replicas.iter().find(|record| {
                record.shard_id == action.shard_id
                    && record.role == ShardReplicaRole::Primary as i32
            })
        })
        .ok_or_else(|| {
            format!(
                "the plan has no record of shard {:?} on source node {:?}",
                action.shard_id, action.source_node_id
            )
        })
}
