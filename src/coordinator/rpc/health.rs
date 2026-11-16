//! Cluster health handler: shard probes and the clustered vector backend.

use super::*;

/// Probe one node, reporting an unreachable node as an outcome.
async fn probe_shard(
    client: Result<crate::link::NodeLink, Status>,
    shard: u32,
    addr: String,
    is_replica: bool,
) -> ShardHealth {
    let outcome = match client {
        Ok(mut client) => {
            match tokio::time::timeout(Duration::from_secs(2), client.health(HealthRequest {}))
                .await
            {
                Ok(reply) => reply.map(|r| r.into_inner()),
                Err(_) => Err(Status::deadline_exceeded("health probe timed out")),
            }
        }
        Err(e) => Err(e),
    };
    match outcome {
        Ok(health) => ShardHealth {
            shard,
            addr,
            is_replica,
            reachable: true,
            error: String::new(),
            health: Some(health),
        },
        Err(e) => ShardHealth {
            shard,
            addr,
            is_replica,
            reachable: false,
            error: e.message().to_string(),
            health: None,
        },
    }
}

impl CoordinatorServiceImpl {
    pub(in crate::coordinator) async fn rpc_cluster_health(
        &self,
        _request: Request<ClusterHealthRequest>,
    ) -> Result<Response<ClusterHealthResponse>, Status> {
        crate::metrics::timed(Route::ClusterHealth, _request, |_request| async move {
            self.admit(&_request.get_ref().collection)?;
            if let Some(snapshot) = self.request_snapshot() {
                return Box::pin(SearchService::cluster_health(
                    &snapshot,
                    crate::metrics::nested(Request::new(ClusterHealthRequest {
                        collection: String::new(),
                    })),
                ))
                .await;
            }
            let targets = self.probe_cluster_targets().await?;
            let provider_mismatch = provider_mismatch_of(&targets);
            let clustered_vector = self.clustered_vector_health().await;
            Ok(Response::new(ClusterHealthResponse {
                collections: Vec::new(),
                targets,
                clustered_vector,
                provider_mismatch,
                topology_generation: self.topology_generation,
            }))
        })
        .await
    }

    /// Probe every primary, then every configured replica. Unreachable
    /// is a reported outcome, never an error; a 2s probe timeout keeps
    /// one filtered port from stalling the whole report.
    async fn probe_cluster_targets(&self) -> Result<Vec<ShardHealth>, Status> {
        let mut probes: Vec<(u32, String, bool)> = Vec::new();
        for (shard, addr) in self.node_addrs.iter().enumerate() {
            probes.push((shard as u32, addr.clone(), false));
        }
        for (shard, replica) in self.replica_addrs.iter().enumerate() {
            if let Some(addr) = replica {
                probes.push((shard as u32, addr.clone(), true));
            }
        }
        let mut tasks = Vec::with_capacity(probes.len());
        for (shard, addr, is_replica) in probes {
            let client = self.node_client(&addr);
            tasks.push(tokio::spawn(probe_shard(client, shard, addr, is_replica)));
        }
        let mut targets = Vec::with_capacity(tasks.len());
        for task in tasks {
            match task.await {
                Ok(target) => targets.push(target),
                Err(e) => return Err(Status::internal(format!("health probe task failed: {e}"))),
            }
        }
        // A reachable node that serves another collection is a
        // misconfiguration, named here rather than counted here.
        for target in &mut targets {
            if let Some(health) = &target.health {
                if health.collection != self.collection {
                    target.error = format!(
                        "node serves collection {:?}, but this coordinator is {:?}",
                        health.collection, self.collection
                    );
                }
            }
        }
        Ok(targets)
    }

    /// The clustered vector backend's health, when one is configured.
    #[cfg(feature = "net")]
    async fn clustered_vector_health(&self) -> Option<ClusteredVectorHealth> {
        let backend = self.clustered_vectors.as_ref()?;
        Some(match backend.health().await {
            Ok(health) => {
                // The score-space identity a quality profile binds to;
                // a servable cluster that cannot state it reports that
                // as its error rather than an empty fingerprint.
                let (scoring_fingerprint, dimensions, error) = if health.servable {
                    match backend.quality_identity().await {
                        Ok(identity) => (
                            identity.scoring_fingerprint,
                            identity.dimensions,
                            health.error,
                        ),
                        Err(status) => (String::new(), 0, status.message().to_string()),
                    }
                } else {
                    (String::new(), 0, health.error)
                };
                ClusteredVectorHealth {
                    backend_kind: "clustered-turbovec".to_string(),
                    transport: backend.transport_name().to_string(),
                    reachable: true,
                    servable: health.servable,
                    error,
                    rows: health.rows,
                    topology_generation: health.topology_generation,
                    scoring_fingerprint,
                    dimensions,
                }
            }
            Err(status) => ClusteredVectorHealth {
                backend_kind: "clustered-turbovec".to_string(),
                transport: backend.transport_name().to_string(),
                reachable: false,
                servable: false,
                error: status.to_string(),
                rows: 0,
                topology_generation: 0,
                scoring_fingerprint: String::new(),
                dimensions: 0,
            },
        })
    }

    #[cfg(not(feature = "net"))]
    async fn clustered_vector_health(&self) -> Option<ClusteredVectorHealth> {
        None
    }
}
