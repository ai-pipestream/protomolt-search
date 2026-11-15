//! `SPLIT_SHARD` (`docs/cluster-control.md`, "Shard split"): build two
//! children covering the source range from the source's own WAL, place
//! them here on fresh listeners, tail the source's log into them by
//! stable key until they are within the lag bound, fence ingest on the
//! source, drain the last records, verify the children conserve the
//! source's live rows, complete the action with the children as
//! primaries, and retire the source. Every step is durable in
//! `split.toml`, so a crash resumes where it stopped; a completion the
//! plane refuses is retried next tick with the source still fenced.

mod build;

use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;

use tonic::{Code, Request};

use crate::node::NodeServiceImpl;
use crate::pb::node_service_server::NodeService;
use crate::pb::{
    ClusterPlan, HealthRequest, HealthResponse, PlacementAction, ShardReplicaRole,
    ShardReplicaState,
};
use crate::replication::{catch_up_children_once, LiveReshardState};

use super::host::{node_health, rows_of};
use super::state::retired_marker;
use super::{NodeAgent, SplitState, CATCH_UP_ROUNDS};

/// The source shard of a split, as served on this node.
struct SplitSource {
    node: NodeServiceImpl,
    addr: String,
    index: PathBuf,
}

/// A split in progress: the action, its plan, and its durable state.
struct Split<'a> {
    action: &'a PlacementAction,
    plan: &'a ClusterPlan,
    source: SplitSource,
    dir: PathBuf,
    state: SplitState,
}

impl Split<'_> {
    fn save(&self) -> Result<(), String> {
        self.state.write(&self.dir)
    }
}

impl NodeAgent {
    /// `SPLIT_SHARD`; see the module documentation.
    pub async fn execute_split(
        &self,
        action: &PlacementAction,
        plan: &ClusterPlan,
    ) -> Result<(), String> {
        let source = self.split_source(action, plan)?;
        let dir = self.split_dir(&action.shard_id);
        let state = self
            .load_or_start_split(action, plan, &source, &dir)
            .await?;
        let mut split = Split {
            action,
            plan,
            source,
            dir,
            state,
        };
        // 1. Build the baseline children from the source's WAL and place
        //    them on fresh listeners.
        if !split.state.built {
            self.build_split_children(&mut split).await?;
        } else {
            self.require_children_served(&split.state)?;
        }
        // 2. Tail the source into the children until within the lag bound.
        self.tail_children(&mut split).await?;
        // 3. Fence the source and drain the last records.
        let source = self.fence_and_drain(&mut split).await?;
        let outputs = self.split_outputs(&split, &source).await?;
        // 4. Complete with the children as primaries.
        self.complete_split(&split, outputs).await?;
        // 5. Retire the source.
        self.retire_split_source(&mut split)
    }

    /// The source: this node's primary copy at the planned generation,
    /// with a range of more than one hash value.
    fn split_source(
        &self,
        action: &PlacementAction,
        plan: &ClusterPlan,
    ) -> Result<SplitSource, String> {
        let node_id = &self.inner.config.node_id;
        let record = plan
            .replicas
            .iter()
            .find(|record| {
                record.shard_id == action.shard_id
                    && &record.node_id == node_id
                    && record.role == ShardReplicaRole::Primary as i32
            })
            .ok_or_else(|| {
                format!(
                    "the plan has no primary record of shard {:?} on this node",
                    action.shard_id
                )
            })?;
        if record.generation != action.source_generation {
            return Err(format!(
                "action {} was planned from generation {} but the source is at {}; waiting for \
                 the plane to replan",
                action.action_id, action.source_generation, record.generation
            ));
        }
        if action.hash_lo >= action.hash_hi {
            return Err(format!(
                "shard {:?} covers the single hash value {}; nothing to split",
                action.shard_id, action.hash_lo
            ));
        }
        let shard = self
            .served(&action.shard_id)
            .ok_or_else(|| format!("shard {:?} is not served on this node", action.shard_id))?;
        let index = shard
            .node
            .config()
            .index_path
            .clone()
            .ok_or_else(|| format!("shard {:?} has no index path", action.shard_id))?;
        Ok(SplitSource {
            node: shard.node,
            addr: shard.addr,
            index,
        })
    }

    /// The split's durable state: resumed for this action, refused for
    /// another, or started fresh with the children fixed.
    async fn load_or_start_split(
        &self,
        action: &PlacementAction,
        plan: &ClusterPlan,
        source: &SplitSource,
        dir: &Path,
    ) -> Result<SplitState, String> {
        match SplitState::load(dir)? {
            Some(state) if state.action_id == action.action_id => return Ok(state),
            Some(state) => {
                return Err(format!(
                    "shard {:?} has split state for action {} on disk; action {} cannot start \
                     until it is resolved ({})",
                    action.shard_id,
                    state.action_id,
                    action.action_id,
                    dir.display()
                ));
            }
            None => {}
        }
        let source_health = node_health(&source.addr).await?;
        if source_health.deleted_docs != 0 {
            return Err(format!(
                "shard {:?} has {} tombstones; the live split moves appends only, so compact it \
                 first",
                action.shard_id, source_health.deleted_docs
            ));
        }
        let children = self.plan_split_children(action, plan, rows_of(&source_health))?;
        let state = SplitState {
            action_id: action.action_id,
            shard_id: action.shard_id.clone(),
            source_generation: action.source_generation,
            target_generation: action.target_generation,
            children,
            built: false,
            live: None,
            fenced: false,
            completed: false,
        };
        state.write(dir)?;
        Ok(state)
    }

    /// A restart re-serves the children through `open_placed`; they must
    /// be up before the source is tailed into them.
    fn require_children_served(&self, state: &SplitState) -> Result<(), String> {
        for child in &state.children {
            if self.shard_addr(&child.shard_id).is_none() {
                return Err(format!(
                    "split child {:?} is not served; open the placed shards first",
                    child.shard_id
                ));
            }
        }
        Ok(())
    }

    /// One tail of the source into the children, made durable.
    async fn tail_once(&self, split: &mut Split<'_>) -> Result<LiveReshardState, String> {
        let live = split
            .state
            .live
            .clone()
            .ok_or_else(|| "split state has no live cursor".to_string())?;
        let live = catch_up_children_once(&live).await?;
        split.state.live = Some(live.clone());
        split.save()?;
        Ok(live)
    }

    async fn tail_children(&self, split: &mut Split<'_>) -> Result<(), String> {
        let mut lag = u64::MAX;
        for _ in 0..CATCH_UP_ROUNDS {
            let live = self.tail_once(split).await?;
            let source = node_health(&split.source.addr).await?;
            lag = source.wal_high_watermark.saturating_sub(live.source_clock);
            if lag <= self.inner.config.lag_bound {
                return Ok(());
            }
        }
        Err(format!(
            "split children of {:?} are still {lag} clocks behind after {CATCH_UP_ROUNDS} \
             rounds; continuing next tick",
            split.action.shard_id
        ))
    }

    /// Fence the source and drain the last records: with no append able
    /// to land, the children's rows are the source's rows. Returns the
    /// fenced source's health.
    async fn fence_and_drain(&self, split: &mut Split<'_>) -> Result<HealthResponse, String> {
        let action = split.action;
        split.source.node.fence_ingest(format!(
            "shard {:?} is splitting into {} (action {}); retry against the new topology",
            action.shard_id,
            split.state.child_names(),
            action.action_id
        ));
        if !split.state.fenced {
            split.state.fenced = true;
            split.save()?;
        }
        let live = self.tail_once(split).await?;
        let source = node_health(&split.source.addr).await?;
        if source.wal_high_watermark != live.source_clock {
            return Err(format!(
                "fenced source {:?} still moved: watermark {} past the children's clock {}",
                action.shard_id, source.wal_high_watermark, live.source_clock
            ));
        }
        Ok(source)
    }

    /// The children as primaries at the target generation, once they are
    /// shown to hold exactly the fenced source's live rows; the source's
    /// final counts are reported first, so the plane checks the children
    /// against them.
    async fn split_outputs(
        &self,
        split: &Split<'_>,
        source: &HealthResponse,
    ) -> Result<Vec<ShardReplicaState>, String> {
        let mut child_rows = 0u64;
        let mut outputs = Vec::with_capacity(split.state.children.len());
        for child in &split.state.children {
            let served = self
                .served(&child.shard_id)
                .ok_or_else(|| format!("split child {:?} vanished", child.shard_id))?;
            let health = NodeService::health(&served.node, Request::new(HealthRequest {}))
                .await
                .map_err(|e| format!("health of child {:?}: {e}", child.shard_id))?
                .into_inner();
            child_rows = child_rows.saturating_add(rows_of(&health));
            let Some(mut output) = self.replica_state(&served, split.plan).await? else {
                return Err(format!(
                    "split child {:?} has no hash range",
                    child.shard_id
                ));
            };
            output.role = ShardReplicaRole::Primary as i32;
            output.ready = true;
            output.generation = split.action.target_generation;
            outputs.push(output);
        }
        self.report_shard_with(&split.action.shard_id, split.plan)
            .await?;
        let source_live = rows_of(source).saturating_sub(source.deleted_docs);
        if child_rows != source_live {
            return Err(format!(
                "split children of {:?} hold {child_rows} rows, the fenced source {source_live}; \
                 the split does not conserve the source and is not completed",
                split.action.shard_id
            ));
        }
        Ok(outputs)
    }

    /// Complete the action; the plane replaces the source's record and
    /// publishes the topology. A refusal means the plane's record of the
    /// source is behind the fenced source: it is reported again (its
    /// counts no longer move) and the next tick completes.
    async fn complete_split(
        &self,
        split: &Split<'_>,
        outputs: Vec<ShardReplicaState>,
    ) -> Result<(), String> {
        self.before_complete().await;
        let action = split.action;
        match self.complete_action(action.action_id, outputs).await? {
            Ok(()) => Ok(()),
            Err(status) if status.code() == Code::FailedPrecondition => {
                self.inner
                    .stats
                    .completion_refusals
                    .fetch_add(1, Ordering::Relaxed);
                self.report_shard_with(&action.shard_id, split.plan).await?;
                Err(format!(
                    "split of {:?} refused by the plane: {}; the source is fenced and reported, \
                     retrying next tick",
                    action.shard_id,
                    status.message()
                ))
            }
            Err(status) => Err(format!(
                "complete split action {}: {status}",
                action.action_id
            )),
        }
    }

    /// Mark the children ready and retire the source: it is no longer
    /// reported or served; a configured source keeps its files and a
    /// marker, a placed one is removed like a dropped copy.
    fn retire_split_source(&self, split: &mut Split<'_>) -> Result<(), String> {
        let action = split.action;
        for child in &split.state.children {
            self.update_placed(&child.shard_id, |placed| {
                placed.ready = true;
                placed.completed_action = action.action_id;
            })?;
        }
        let retired = self.shards().remove(&action.shard_id);
        match retired {
            Some(shard) if shard.placed.is_some() => {
                self.remove_placed(shard, &action.shard_id)?;
            }
            Some(_) => {
                let marker = retired_marker(&self.inner.config.data_dir, &action.shard_id);
                if let Some(parent) = marker.parent() {
                    std::fs::create_dir_all(parent)
                        .map_err(|e| format!("create {}: {e}", parent.display()))?;
                }
                std::fs::write(&marker, format!("split by action {}\n", action.action_id))
                    .map_err(|e| format!("write {}: {e}", marker.display()))?;
            }
            None => {}
        }
        split.state.completed = true;
        split.save()?;
        self.inner
            .stats
            .splits_completed
            .fetch_add(1, Ordering::Relaxed);
        eprintln!(
            "node {:?}: shard {:?} split into {} (action {}), source retired",
            self.inner.config.node_id,
            action.shard_id,
            split.state.child_names(),
            action.action_id
        );
        Ok(())
    }
}
