//! Request-owned, best-effort shard capability discovery.
use super::CoordinatorServiceImpl;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::task::JoinSet;

/// Dropping the owner aborts outstanding RPCs. Relay request owners also
/// explicitly drain them before publishing a terminal result.
pub(crate) struct ShardVectorRowProbe {
    tasks: JoinSet<Option<(String, crate::pb::GetVectorBackendResponse)>>,
    observed: Vec<(String, u64)>,
    cache: Arc<Mutex<HashMap<String, u64>>>,
}

impl ShardVectorRowProbe {
    /// Cancellation safe: observations remain owned here if this wait is
    /// interrupted by a map notification that leaves the revision unchanged.
    pub(crate) async fn wait(&mut self) {
        while let Some(result) = self.tasks.join_next().await {
            if let Ok(Some((addr, backend))) = result {
                if backend.descriptor.is_some() {
                    self.observed.push((addr, backend.num_vectors));
                }
            }
        }
        if !self.observed.is_empty() {
            let mut cache = self.cache.lock().expect("shard vector row cache poisoned");
            for (addr, rows) in self.observed.drain(..) {
                cache.insert(addr, rows);
            }
        }
    }

    pub(crate) async fn shutdown(&mut self) {
        // JoinSet::shutdown discards successful outputs too. Abort unfinished
        // RPCs, then collect ready results and publish all observations through
        // the same path as a completed wait before releasing the request.
        self.tasks.abort_all();
        self.wait().await;
    }
}

impl CoordinatorServiceImpl {
    pub(crate) fn shard_vector_row_probe(&self) -> ShardVectorRowProbe {
        let mut probe = ShardVectorRowProbe {
            tasks: JoinSet::new(),
            observed: Vec::new(),
            cache: Arc::clone(&self.shard_vector_rows),
        };
        if self.has_clustered_vectors() || self.node_addrs.is_empty() {
            return probe;
        }
        let needs_probe = {
            let cache = probe.cache.lock().expect("shard vector row cache poisoned");
            self.node_addrs
                .iter()
                .any(|addr| cache.get(addr).copied().unwrap_or(0) == 0)
        };
        if needs_probe {
            for addr in &self.node_addrs {
                let Ok(mut client) = self.node_client(addr) else {
                    continue;
                };
                let addr = addr.clone();
                probe.tasks.spawn(async move {
                    client
                        .get_vector_backend(crate::pb::GetVectorBackendRequest {})
                        .await
                        .ok()
                        .map(|resp| (addr, resp.into_inner()))
                });
            }
        }
        probe
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn successful_observations_survive_wait_interruption_and_populate_cache() {
        let coordinator = CoordinatorServiceImpl::new(vec!["http://127.0.0.1:1".into()]);
        let mut probe = ShardVectorRowProbe {
            tasks: JoinSet::new(),
            observed: Vec::new(),
            cache: Arc::clone(&coordinator.shard_vector_rows),
        };
        probe.tasks.spawn(async {
            Some((
                "http://127.0.0.1:1".into(),
                crate::pb::GetVectorBackendResponse {
                    descriptor: Some(Default::default()),
                    num_vectors: 42,
                    ..Default::default()
                },
            ))
        });
        probe.tasks.spawn(std::future::pending());
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while probe.observed.is_empty() {
                // Poll discovery until its successful observation is consumed,
                // then interrupt the still-pending wait at a yield boundary.
                tokio::select! {
                    biased;
                    _ = probe.wait() => panic!("the second probe must remain pending"),
                    _ = tokio::task::yield_now() => {}
                }
            }
        })
        .await
        .expect("successful probe must become observable");
        assert_eq!(probe.observed.len(), 1);
        probe.shutdown().await;
        assert!(
            probe.tasks.is_empty(),
            "shutdown must drain every owned task"
        );
        assert_eq!(
            coordinator.shard_vector_rows.lock().unwrap()["http://127.0.0.1:1"],
            42
        );
        let cached = coordinator.shard_vector_row_probe();
        assert!(
            cached.tasks.is_empty(),
            "positive cached rows must skip new RPCs"
        );
    }

    #[tokio::test]
    async fn shutdown_caches_completed_results_that_wait_has_not_consumed() {
        let coordinator = CoordinatorServiceImpl::new(vec!["http://127.0.0.1:1".into()]);
        let mut probe = ShardVectorRowProbe {
            tasks: JoinSet::new(),
            observed: Vec::new(),
            cache: Arc::clone(&coordinator.shard_vector_rows),
        };
        let completed = probe.tasks.spawn(async {
            Some((
                "http://127.0.0.1:1".into(),
                crate::pb::GetVectorBackendResponse {
                    descriptor: Some(Default::default()),
                    num_vectors: 42,
                    ..Default::default()
                },
            ))
        });
        probe.tasks.spawn(std::future::pending());
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while !completed.is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("successful RPC must complete before shutdown");
        assert!(
            probe.observed.is_empty(),
            "wait has not consumed the queued result"
        );
        probe.shutdown().await;
        assert!(
            probe.tasks.is_empty(),
            "pending RPC must be aborted and drained"
        );
        assert_eq!(
            coordinator.shard_vector_rows.lock().unwrap()["http://127.0.0.1:1"],
            42
        );
        assert!(coordinator.shard_vector_row_probe().tasks.is_empty());
    }
}
