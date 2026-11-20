//! Measured dense quality profile resolution against the live generation.

use super::*;

/// The live provider and exact-row generation a quality profile must match.
struct LiveQualityIdentity {
    provider: String,
    scoring_fingerprint: String,
    rows: u64,
    generation: u64,
    dimensions: u32,
}

/// Per-shard backends and health replies folded into one live identity.
#[derive(Default)]
struct ShardQualityFold {
    provider: Option<String>,
    fingerprint: Option<String>,
    rows: u64,
    dimensions: Option<u32>,
}

impl ShardQualityFold {
    fn absorb(
        &mut self,
        backend: crate::pb::GetVectorBackendResponse,
        health: crate::pb::HealthResponse,
    ) -> Result<(), Status> {
        let descriptor = backend.descriptor.ok_or_else(|| {
            Status::failed_precondition(
                "quality preflight found a shard without a vector descriptor",
            )
        })?;
        if !health.exact_vectors_available {
            return Err(Status::failed_precondition(format!(
                "quality profile requires aligned exact rows, but shard at slot_offset={} has none",
                health.slot_offset
            )));
        }
        if health.deleted_docs != 0 {
            return Err(Status::failed_precondition(format!(
                "quality profile was measured on an all-live generation, but shard at slot_offset={} has {} tombstoned rows; compact and remeasure",
                health.slot_offset, health.deleted_docs
            )));
        }
        hold_uniform(
            &mut self.provider,
            descriptor.backend_kind,
            "quality preflight found mixed vector providers",
        )?;
        hold_uniform(
            &mut self.fingerprint,
            descriptor.scoring_fingerprint,
            "quality preflight found mixed scoring fingerprints",
        )?;
        hold_uniform(
            &mut self.dimensions,
            descriptor.dim,
            "quality preflight found mixed vector dimensions",
        )?;
        self.rows = self
            .rows
            .checked_add(backend.num_vectors)
            .ok_or_else(|| Status::internal("quality preflight row count overflow"))?;
        Ok(())
    }

    fn finish(self, generation: u64) -> LiveQualityIdentity {
        LiveQualityIdentity {
            provider: self.provider.unwrap_or_default(),
            scoring_fingerprint: self.fingerprint.unwrap_or_default(),
            rows: self.rows,
            generation,
            dimensions: self.dimensions.unwrap_or_default(),
        }
    }
}

impl CoordinatorServiceImpl {
    /// Size a BALANCED FP32 pool that names neither a policy nor
    /// `selection_k` through the installed profile's default recall target.
    /// `None` without a profile or a default target: the pool stays at k.
    pub(crate) async fn resolve_dense_quality_default(
        &self,
        k: u32,
        query_dim: usize,
    ) -> Result<Option<crate::quality::DenseQualityResolution>, Status> {
        let Some(target) = self
            .dense_quality_profile
            .as_ref()
            .and_then(|profile| profile.default_target_recall_ppm())
        else {
            return Ok(None);
        };
        let policy = crate::pb::DenseQualityPolicy {
            target_recall_ppm: target,
            max_candidates: 0,
            required_profile_fingerprint: String::new(),
        };
        self.resolve_dense_quality(k, query_dim, &policy)
            .await
            .map(Some)
    }

    /// Resolve and prove one measured dense quality request against the live
    /// provider and product exact-row generation. Drift is a hard failure.
    pub(crate) async fn resolve_dense_quality(
        &self,
        k: u32,
        query_dim: usize,
        policy: &crate::pb::DenseQualityPolicy,
    ) -> Result<crate::quality::DenseQualityResolution, Status> {
        let profile = self.dense_quality_profile.as_ref().ok_or_else(|| {
            Status::failed_precondition(
                "dense quality policy requested but this coordinator has no --dense-quality-profile",
            )
        })?;
        let resolution = profile
            .resolve(
                k,
                policy.target_recall_ppm,
                &policy.required_profile_fingerprint,
                policy.max_candidates,
            )
            .map_err(Status::invalid_argument)?;
        if resolution.selection_k > self.knobs.max_k() {
            return Err(Status::failed_precondition(format!(
                "quality profile resolves selection_k={} above coordinator max_k={}; raise --max-k or measure a bounded policy",
                resolution.selection_k, self.knobs.max_k()
            )));
        }
        if resolution.dimensions as usize != query_dim {
            return Err(Status::failed_precondition(format!(
                "quality profile dimension {} does not match query dimension {query_dim}",
                resolution.dimensions
            )));
        }
        let live = match self.clustered_quality_identity().await? {
            Some(identity) => self.clustered_quality_live(identity).await?,
            None => self.shard_quality_live().await?,
        };
        check_profile_matches_live(&resolution, &live)?;
        Ok(resolution)
    }

    /// The clustered collection's identity, once every product shard proves
    /// it owns aligned, all-live exact rows for the same row count.
    async fn clustered_quality_live(
        &self,
        identity: crate::clustered_turbovec::ClusteredQualityIdentity,
    ) -> Result<LiveQualityIdentity, Status> {
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for addr in &self.node_addrs {
            let mut client = self.node_client(addr)?;
            tasks.push(tokio::spawn(async move {
                client
                    .health(HealthRequest {})
                    .await
                    .map(|reply| reply.into_inner())
            }));
        }
        let mut exact_rows = 0u64;
        for task in tasks {
            let health = task.await.map_err(|error| {
                Status::internal(format!("quality exact-row preflight failed: {error}"))
            })??;
            if !health.exact_vectors_available {
                return Err(Status::failed_precondition(format!(
                    "clustered quality profile requires aligned product-owned exact rows, but product shard at slot_offset={} has none",
                    health.slot_offset
                )));
            }
            if health.deleted_docs != 0 {
                return Err(Status::failed_precondition(format!(
                    "clustered quality profile was measured on an all-live generation, but product shard at slot_offset={} has {} tombstoned rows; compact and remeasure",
                    health.slot_offset, health.deleted_docs
                )));
            }
            exact_rows = exact_rows
                .checked_add(health.exact_vector_rows)
                .ok_or_else(|| Status::internal("quality exact-row count overflow"))?;
        }
        if exact_rows != identity.rows {
            return Err(Status::failed_precondition(format!(
                "clustered vector collection has {} rows but product shards own {exact_rows} exact rows",
                identity.rows
            )));
        }
        Ok(LiveQualityIdentity {
            provider: "clustered-turbovec".to_string(),
            scoring_fingerprint: identity.scoring_fingerprint,
            rows: identity.rows,
            generation: identity.topology_generation,
            dimensions: identity.dimensions,
        })
    }

    /// The shard fleet's identity, once every shard proves one provider,
    /// fingerprint and dimension over aligned, all-live exact rows.
    async fn shard_quality_live(&self) -> Result<LiveQualityIdentity, Status> {
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for addr in &self.node_addrs {
            let mut client = self.node_client(addr)?;
            tasks.push(tokio::spawn(async move {
                let backend = client
                    .get_vector_backend(crate::pb::GetVectorBackendRequest {})
                    .await?
                    .into_inner();
                let health = client.health(HealthRequest {}).await?.into_inner();
                Ok::<_, Status>((backend, health))
            }));
        }
        let mut fold = ShardQualityFold::default();
        for task in tasks {
            let (backend, health) = task.await.map_err(|error| {
                Status::internal(format!("quality preflight task failed: {error}"))
            })??;
            fold.absorb(backend, health)?;
        }
        Ok(fold.finish(self.topology_generation))
    }
}

fn check_profile_matches_live(
    resolution: &crate::quality::DenseQualityResolution,
    live: &LiveQualityIdentity,
) -> Result<(), Status> {
    for (name, expected, actual) in [
        (
            "provider_backend",
            resolution.provider_backend.as_str(),
            live.provider.as_str(),
        ),
        (
            "scoring_fingerprint",
            resolution.scoring_fingerprint.as_str(),
            live.scoring_fingerprint.as_str(),
        ),
    ] {
        if expected != actual {
            return Err(Status::failed_precondition(format!(
                "quality profile {name} {expected:?} does not match live {actual:?}"
            )));
        }
    }
    if resolution.corpus_rows != live.rows
        || resolution.corpus_generation != live.generation
        || resolution.dimensions != live.dimensions
    {
        return Err(Status::failed_precondition(format!(
            "quality profile generation mismatch: profile rows/generation/dim={}/{}/{}, live={}/{}/{}",
            resolution.corpus_rows,
            resolution.corpus_generation,
            resolution.dimensions,
            live.rows,
            live.generation,
            live.dimensions
        )));
    }
    Ok(())
}
