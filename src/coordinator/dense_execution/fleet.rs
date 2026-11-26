//! Live filter selectivity and fleet-wide vector identity preflights.

use super::*;

impl CoordinatorServiceImpl {
    #[cfg(test)]
    pub(in crate::coordinator) async fn dense_filter_selectivity(
        &self,
        filters: Option<&RequestFilters>,
        rows: u64,
    ) -> Result<u32, Status> {
        let caller_filtered = filters.is_some_and(|f| !f.geo.is_empty() || f.tree.is_some());
        if !caller_filtered && self.document_visibility.is_none() {
            return Ok(crate::dense_policy::UNFILTERED_PPM);
        }
        // Both membership passes must describe the same physical read. A
        // standalone planner has no public Query envelope to pin them for it.
        if self.query_read_versions.is_none() {
            let (pinned, reads) = self.pin_read_versions().await?;
            let result = Box::pin(pinned.dense_filter_selectivity(filters, rows)).await;
            pinned.validate_read_versions(&reads).await?;
            return result;
        }
        let vectors = self
            .vector_membership(self.vector_read_field.as_deref().unwrap_or(""))
            .await?;
        let admitted = if caller_filtered {
            let documents = self
                .filter_membership(filters.expect("caller filter"))
                .await?;
            vectors.ids.intersection(&documents.ids).count()
        } else {
            vectors.ids.len()
        };
        Ok(crate::dense_policy::selectivity_ppm(admitted as u64, rows))
    }

    /// Verify uniform vector provider kind, scoring fingerprint, and dimension
    /// across reachable shards. When `require_configured` is set, unconfigured
    /// shards also fail preflight; otherwise they are skipped.
    pub(crate) async fn fleet_vector_identity(
        &self,
        require_configured: bool,
    ) -> Result<FleetVectorIdentity, Status> {
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, addr) in self.node_addrs.iter().enumerate() {
            let mut client = self.node_client(addr)?;
            let addr = addr.clone();
            tasks.push(tokio::spawn(async move {
                client
                    .get_vector_backend(crate::pb::GetVectorBackendRequest {})
                    .await
                    .map(|response| (shard, addr, response.into_inner()))
            }));
        }
        let mut identity: Option<FleetVectorIdentity> = None;
        let mut first: Option<(usize, String)> = None;
        for task in tasks {
            let (shard, addr, backend) = task.await.map_err(|error| {
                Status::internal(format!("provider preflight failed: {error}"))
            })??;
            let Some(descriptor) = backend.descriptor else {
                if require_configured {
                    return Err(Status::failed_precondition(format!(
                        "provider preflight: shard {shard} ({addr}) has no vector backend \
                         configured"
                    )));
                }
                continue;
            };
            self.shard_vector_rows
                .lock()
                .expect("shard vector row cache poisoned")
                .insert(addr.clone(), backend.num_vectors);
            let seen = FleetVectorIdentity {
                provider: descriptor.backend_kind.clone(),
                scoring_fingerprint: descriptor.scoring_fingerprint.clone(),
                dimensions: descriptor.dim,
                rows: backend.num_vectors,
            };
            match &mut identity {
                None => {
                    identity = Some(seen);
                    first = Some((shard, addr));
                }
                Some(held) => {
                    let (first_shard, first_addr) =
                        first.as_ref().expect("identity and first are set together");
                    if held.provider != seen.provider
                        || held.scoring_fingerprint != seen.scoring_fingerprint
                        || held.dimensions != seen.dimensions
                    {
                        return Err(Status::failed_precondition(format!(
                            "provider preflight: shard {first_shard} ({first_addr}) scores \
                             under {}/{} dim {}, but shard {shard} ({addr}) under {}/{} dim {}; \
                             a fleet scores in one space, so nothing is searched until every \
                             shard serves the same provider state",
                            held.provider,
                            held.scoring_fingerprint,
                            held.dimensions,
                            seen.provider,
                            seen.scoring_fingerprint,
                            seen.dimensions
                        )));
                    }
                    held.rows = held
                        .rows
                        .checked_add(seen.rows)
                        .ok_or_else(|| Status::internal("provider preflight row count overflow"))?;
                }
            }
        }
        Ok(identity.unwrap_or_default())
    }

    /// The clustered backend's live identity, when one is configured.
    #[cfg(feature = "net")]
    pub(in crate::coordinator) async fn clustered_quality_identity(
        &self,
    ) -> Result<Option<crate::clustered_turbovec::ClusteredQualityIdentity>, Status> {
        match &self.clustered_vectors {
            Some(clustered) => clustered.quality_identity().await.map(Some),
            None => Ok(None),
        }
    }

    #[cfg(not(feature = "net"))]
    pub(in crate::coordinator) async fn clustered_quality_identity(
        &self,
    ) -> Result<Option<crate::clustered_turbovec::ClusteredQualityIdentity>, Status> {
        Ok(None)
    }
}
