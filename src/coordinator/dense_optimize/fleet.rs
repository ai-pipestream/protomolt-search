//! Pre-sweep fleet discovery and post-sweep corpus/hardware stability checks.

use std::time::Instant;

use tonic::Status;

use super::Fleet;
use crate::coordinator::CoordinatorServiceImpl;
use crate::pb::{DenseExecutionShape, DenseHostProfile};

fn normalized_hosts(hosts: &[DenseHostProfile]) -> Vec<DenseHostProfile> {
    hosts
        .iter()
        .map(crate::dense_policy::normalize_host)
        .collect()
}

impl CoordinatorServiceImpl {
    /// Discover and validate the live fleet's capabilities, identity, and
    /// homogeneous shard hardware profiles.
    pub(super) async fn optimize_fleet(&self) -> Result<Fleet, Status> {
        if self.clustered_quality_identity().await?.is_some() {
            return Err(Status::failed_precondition(
                "optimize dense policy measures shard providers; a clustered backend serves \
                 exhaustively",
            ));
        }
        let profile = self.dense_fleet_profile().await?;
        if profile.hosts.is_empty()
            || profile
                .hosts
                .iter()
                .any(|h| !crate::dense_policy::is_profiled_host(h))
        {
            return Err(Status::failed_precondition(
                "optimize dense policy requires every shard and relay to report a profiled \
                 homogeneous DenseHostProfile",
            ));
        }
        let normalized = normalized_hosts(&profile.hosts);
        if normalized[1..].iter().any(|h| *h != normalized[0]) {
            return Err(Status::failed_precondition(
                "optimize dense policy requires homogeneous shard host profiles across the fleet",
            ));
        }
        let shape = if self.stream_search {
            DenseExecutionShape::Streaming
        } else {
            DenseExecutionShape::Unary
        };
        Ok(Fleet {
            caps: profile.caps,
            identity: profile.identity,
            hosts: normalized,
            shape,
        })
    }

    /// Re-verify both `LiveIdentity` and normalized shard `DenseHostProfile`s
    /// within `deadline` before returning or installing sweep evidence.
    pub(super) async fn verify_unchanged_fleet(
        &self,
        expected: &Fleet,
        deadline: Instant,
    ) -> Result<(), Status> {
        if Instant::now() >= deadline {
            return Err(Status::deadline_exceeded(
                "optimize dense policy: post-sweep verification exceeded max_runtime_ms",
            ));
        }
        let post = tokio::time::timeout_at(
            tokio::time::Instant::from_std(deadline),
            self.dense_fleet_profile(),
        )
        .await
        .map_err(|_| {
            Status::deadline_exceeded(
                "optimize dense policy: post-sweep verification exceeded max_runtime_ms",
            )
        })??;
        if post.identity != expected.identity {
            return Err(Status::aborted(
                "optimize dense policy: corpus mutated during sweep; retry on quiescent corpus",
            ));
        }
        let post_hosts = normalized_hosts(&post.hosts);
        let hosts_unchanged = post.hosts.iter().all(crate::dense_policy::is_profiled_host)
            && post_hosts == expected.hosts
            && crate::dense_policy::fleet_fingerprint(&post_hosts)
                == crate::dense_policy::fleet_fingerprint(&expected.hosts);
        if !hosts_unchanged {
            return Err(Status::aborted(
                "optimize dense policy: shard host profile changed during sweep; \
                 retry on stable hardware",
            ));
        }
        Ok(())
    }
}
