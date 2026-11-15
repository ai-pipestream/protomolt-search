//! The agent's calls to the control plane: the lease, the plan, and the
//! shard reports.

use std::sync::atomic::Ordering;

use tonic::transport::Channel;
use tonic::{Code, Request};

use crate::pb::cluster_control_client::ClusterControlClient;
use crate::pb::node_service_server::NodeService;
use crate::pb::{
    ClusterPlan, CompletePlacementActionRequest, GetClusterPlanRequest, HealthRequest,
    NodeCapacity, NodeLease, RegisterNodeRequest, RenewNodeLeaseRequest, ReportShardRequest,
    ShardReplicaRole, ShardReplicaState,
};

use super::host::{filesystem_bytes, fingerprint_string, memory_bytes, rows_of, shard_bytes};
use super::{NodeAgent, ServedShard};

impl NodeAgent {
    pub(super) async fn control(&self) -> Result<ClusterControlClient<Channel>, String> {
        if let Some(client) = self.inner.control.lock().expect("control lock").as_ref() {
            return Ok(client.clone());
        }
        let addr = crate::config::normalize_addr(self.inner.config.control_addr.clone());
        let endpoint =
            tonic::transport::Endpoint::from_shared(crate::security::process_secure_url(&addr))
                .map_err(|e| format!("control address {addr}: {e}"))?
                .tcp_nodelay(true);
        let endpoint = crate::security::secure_endpoint(endpoint)?;
        let client = ClusterControlClient::new(endpoint.connect_lazy())
            .max_decoding_message_size(crate::MAX_MESSAGE_BYTES)
            .max_encoding_message_size(crate::MAX_MESSAGE_BYTES);
        *self.inner.control.lock().expect("control lock") = Some(client.clone());
        Ok(client)
    }

    /// Disk, memory, and thread capacity for the register/renew calls.
    pub fn capacity(&self) -> NodeCapacity {
        let (disk_bytes, used_disk_bytes) = filesystem_bytes(&self.inner.config.data_dir);
        let search_threads = if self.inner.config.scan_parallel == 0 {
            std::thread::available_parallelism()
                .map(|n| (n.get() / 2).max(1))
                .unwrap_or(1)
        } else {
            self.inner.config.scan_parallel
        } as u32;
        let rate = crate::node::scan_rate();
        NodeCapacity {
            disk_bytes,
            used_disk_bytes,
            memory_bytes: memory_bytes(),
            search_threads,
            failure_domain: self.inner.config.failure_domain.clone(),
            // The process-wide window's rate (docs/bandwidth-budget.md):
            // zero is unknown. A server process: the device residency is
            // declared by the phone transport, which does not register
            // through this agent.
            scan_bytes_per_second: rate.bytes_per_second,
            scan_rate_observed_unix_ms: rate.observed_unix_ms,
            scan_rate_samples: rate.samples,
            scan_rate_window_ms: rate.window_ms,
            residency: crate::pb::NodeResidency::Server as i32,
        }
    }

    /// `RegisterNode`: a fresh lease for this node at its address.
    pub async fn register(&self) -> Result<NodeLease, String> {
        let mut control = self.control().await?;
        let lease = control
            .register_node(RegisterNodeRequest {
                node_id: self.inner.config.node_id.clone(),
                addr: self.inner.config.node_addr.clone(),
                capacity: Some(self.capacity()),
                lease_ms: self.inner.config.lease_ms,
                collection: self.inner.config.collection.clone(),
            })
            .await
            .map_err(|e| format!("register node {:?}: {e}", self.inner.config.node_id))?
            .into_inner();
        *self.inner.lease.lock().expect("lease lock") = Some(lease.clone());
        self.inner
            .stats
            .registrations
            .fetch_add(1, Ordering::Relaxed);
        Ok(lease)
    }

    /// `RenewNodeLease`; a lease the plane no longer holds re-registers.
    pub async fn renew(&self) -> Result<NodeLease, String> {
        let Some(lease) = self.lease() else {
            return self.register().await;
        };
        let mut control = self.control().await?;
        let request = RenewNodeLeaseRequest {
            node_id: self.inner.config.node_id.clone(),
            lease_token: lease.lease_token,
            capacity: Some(self.capacity()),
            lease_ms: self.inner.config.lease_ms,
            collection: self.inner.config.collection.clone(),
        };
        match control.renew_node_lease(request).await {
            Ok(renewed) => {
                let renewed = renewed.into_inner();
                *self.inner.lease.lock().expect("lease lock") = Some(renewed.clone());
                self.inner.stats.renewals.fetch_add(1, Ordering::Relaxed);
                Ok(renewed)
            }
            Err(status) if matches!(status.code(), Code::NotFound | Code::FailedPrecondition) => {
                eprintln!(
                    "node {:?}: lease not renewable ({}); registering again",
                    self.inner.config.node_id,
                    status.message()
                );
                self.register().await
            }
            Err(status) => Err(format!(
                "renew lease of node {:?}: {status}",
                self.inner.config.node_id
            )),
        }
    }

    pub(super) async fn lease_token(&self) -> Result<u64, String> {
        match self.lease() {
            Some(lease) => Ok(lease.lease_token),
            None => self.register().await.map(|lease| lease.lease_token),
        }
    }

    /// `GetClusterPlan` for this agent's collection.
    pub async fn plan(&self) -> Result<ClusterPlan, String> {
        let mut control = self.control().await?;
        Ok(control
            .get_cluster_plan(GetClusterPlanRequest {
                collection: self.inner.config.collection.clone(),
            })
            .await
            .map_err(|e| format!("get cluster plan: {e}"))?
            .into_inner())
    }

    /// `CompletePlacementAction` under this node's lease: the outer error
    /// is a local failure to reach the plane, the inner one the plane's
    /// answer.
    pub(super) async fn complete_action(
        &self,
        action_id: u64,
        outputs: Vec<ShardReplicaState>,
    ) -> Result<Result<(), tonic::Status>, String> {
        let token = self.lease_token().await?;
        let mut control = self.control().await?;
        Ok(control
            .complete_placement_action(CompletePlacementActionRequest {
                node_id: self.inner.config.node_id.clone(),
                lease_token: token,
                action_id,
                outputs,
                collection: self.inner.config.collection.clone(),
            })
            .await
            .map(|_| ()))
    }

    /// The hash range a served shard reports: the configuration's, else
    /// the plane's record of that shard on any node, else the published
    /// route whose primary or replica address is this listener.
    fn hash_range_of(shard: &ServedShard, plan: &ClusterPlan) -> Option<(u64, u64)> {
        if let Some(range) = shard.hash_range {
            return Some(range);
        }
        if let Some(record) = plan
            .replicas
            .iter()
            .find(|record| record.shard_id == shard.shard_id)
        {
            return Some((record.hash_lo, record.hash_hi));
        }
        plan.topology
            .iter()
            .find(|route| route.addr == shard.addr || route.replica == shard.addr)
            .map(|route| (route.hash_lo, route.hash_hi))
    }

    /// The state one served shard reports, from the node's own health.
    pub(super) async fn replica_state(
        &self,
        shard: &ServedShard,
        plan: &ClusterPlan,
    ) -> Result<Option<ShardReplicaState>, String> {
        let Some((hash_lo, hash_hi)) = Self::hash_range_of(shard, plan) else {
            self.log_unranged(shard);
            return Ok(None);
        };
        let health = NodeService::health(&shard.node, Request::new(HealthRequest {}))
            .await
            .map_err(|e| format!("health of shard {:?}: {e}", shard.shard_id))?
            .into_inner();
        let node_id = &self.inner.config.node_id;
        let record = plan
            .replicas
            .iter()
            .find(|record| record.shard_id == shard.shard_id && &record.node_id == node_id);
        // The plane owns roles: a shard it has a record of reports the
        // role that record holds (a promotion or demotion happened
        // there); one it has never seen is a primary if configured, a
        // replica if placed.
        let role = match record {
            Some(record) => record.role,
            None if shard.placed.is_some() => ShardReplicaRole::Replica as i32,
            None => ShardReplicaRole::Primary as i32,
        };
        let (generation, ready) = match &shard.placed {
            Some(placed) => (placed.source_generation, placed.ready),
            None => (health.wal_generation, true),
        };
        let index_path = shard.node.config().index_path.clone();
        Ok(Some(ShardReplicaState {
            shard_id: shard.shard_id.clone(),
            node_id: node_id.clone(),
            addr: shard.addr.clone(),
            generation,
            hash_lo,
            hash_hi,
            slot_offset: health.slot_offset,
            rows: rows_of(&health),
            bytes: index_path.as_deref().map_or(0, shard_bytes),
            role,
            ready: ready && !health.bm25_building,
            scoring_fingerprint: health.scoring_fingerprint.clone(),
            analysis_fingerprint: fingerprint_string(&shard.node.analysis_fingerprints()),
            immutable_segments: shard.node.immutable_segments(),
            tombstones: health.deleted_docs,
            collection: self.inner.config.collection.clone(),
        }))
    }

    fn log_unranged(&self, shard: &ServedShard) {
        let first = self
            .inner
            .unranged
            .lock()
            .expect("unranged lock")
            .insert(shard.shard_id.clone());
        if first {
            eprintln!(
                "node {:?}: shard {:?} at {} has no hash range in its configuration, the plane's \
                 records, or the published topology; not reported until one names it",
                self.inner.config.node_id, shard.shard_id, shard.addr
            );
        }
    }

    /// `ReportShard` for one shard; `Ok(false)` when it has no range yet.
    pub async fn report_shard(&self, shard_id: &str) -> Result<bool, String> {
        let plan = self.plan().await?;
        self.report_shard_with(shard_id, &plan).await
    }

    pub(super) async fn report_shard_with(
        &self,
        shard_id: &str,
        plan: &ClusterPlan,
    ) -> Result<bool, String> {
        let shard = self
            .served(shard_id)
            .ok_or_else(|| format!("shard {shard_id:?} is not served here"))?;
        let Some(replica) = self.replica_state(&shard, plan).await? else {
            return Ok(false);
        };
        let token = self.lease_token().await?;
        let mut control = self.control().await?;
        let request = ReportShardRequest {
            node_id: self.inner.config.node_id.clone(),
            lease_token: token,
            replica: Some(replica),
            collection: self.inner.config.collection.clone(),
        };
        match control.report_shard(request.clone()).await {
            Ok(_) => {}
            Err(status)
                if matches!(status.code(), Code::NotFound | Code::FailedPrecondition)
                    && status.message().contains("lease") =>
            {
                let lease = self.register().await?;
                control
                    .report_shard(ReportShardRequest {
                        lease_token: lease.lease_token,
                        ..request
                    })
                    .await
                    .map_err(|e| format!("report shard {shard_id:?}: {e}"))?;
            }
            Err(status) => return Err(format!("report shard {shard_id:?}: {status}")),
        }
        self.inner.stats.reports.fetch_add(1, Ordering::Relaxed);
        Ok(true)
    }

    /// Report every served shard.
    pub async fn report_all(&self) -> Result<usize, String> {
        let plan = self.plan().await?;
        let mut reported = 0;
        for shard_id in self.shard_ids() {
            if self.report_shard_with(&shard_id, &plan).await? {
                reported += 1;
            }
        }
        Ok(reported)
    }
}
