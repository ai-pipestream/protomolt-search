//! Vector backend configuration, Matryoshka tier rescoring, and batch entry.

mod batch;
mod splade;

use crate::node::*;

impl NodeServiceImpl {
    /// Returns the retained [`crate::turbo_bridge::MultiResVectorOutput`] for `local_slot`.
    pub fn multi_res_row(
        &self,
        local_slot: u32,
    ) -> Option<crate::turbo_bridge::MultiResVectorOutput> {
        read_shard(&self.state)
            .multi_res_vectors
            .get(local_slot)
            .cloned()
    }

    /// Rescores `candidate_global_ids` on this shard against retained Matryoshka
    /// `tier_dim` (prefix or full) vectors using normalized dot-product similarity.
    pub fn rescore_matryoshka_tier(
        &self,
        tier_dim: u32,
        query_full_or_tier: &[f32],
        candidate_global_ids: &[u64],
    ) -> Result<Vec<(u64, f32)>, Status> {
        if tier_dim == 0 || query_full_or_tier.len() < tier_dim as usize {
            return Err(Status::invalid_argument(format!(
                "query vector len {} is shorter than matryoshka tier_dim {tier_dim}",
                query_full_or_tier.len()
            )));
        }
        let q_multi = crate::turbo_bridge::MultiResVectorOutput::from_full_and_sentences(
            query_full_or_tier,
            &[tier_dim],
            None,
            Vec::new(),
        );
        let q_slice = q_multi.tier(tier_dim).ok_or_else(|| {
            Status::invalid_argument(format!("cannot slice query vector to tier_dim {tier_dim}"))
        })?;
        let guard = read_shard(&self.state);
        let rows = &guard.multi_res_vectors;
        let offset = self.config.slot_offset;
        let mut out = Vec::with_capacity(candidate_global_ids.len());
        for &gid in candidate_global_ids {
            if gid < offset {
                continue;
            }
            let Ok(local) = u32::try_from(gid - offset) else {
                continue;
            };
            if guard.live_docs.is_deleted(local as usize) {
                continue;
            }
            if local as usize >= guard.index.as_ref().map_or(0, VectorIndex::len)
                || guard
                    .exact_vectors
                    .as_ref()
                    .is_some_and(|exact| !exact.contains_row(local as usize))
            {
                continue;
            }
            let doc_tier = rows
                .get(local)
                .and_then(|r| r.tier(tier_dim))
                .ok_or_else(|| {
                    Status::failed_precondition(format!(
                        "retained vector tier {tier_dim} is unavailable for row {local}"
                    ))
                })?;
            {
                let dot: f32 = q_slice
                    .iter()
                    .zip(doc_tier.iter())
                    .map(|(a, b)| a * b)
                    .sum();
                out.push((gid, dot));
            }
        }
        out.sort_unstable_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
        Ok(out)
    }

    /// Apply one backend-owned configuration to an empty shard.
    pub(in crate::node) fn apply_backend_config(
        &self,
        req: ConfigureVectorBackendRequest,
    ) -> Result<bool, Status> {
        let dim = req.dim as usize;
        if dim == 0 {
            return Err(Status::invalid_argument(
                "vector dimension must be positive",
            ));
        }
        let config = internal_backend_config(
            req.config
                .ok_or_else(|| Status::invalid_argument("vector backend config is required"))?,
        )?;
        let build = || {
            VectorIndex::from_backend_config(dim, &config)
                .map_err(|e| Status::invalid_argument(format!("invalid backend config: {e}")))
        };
        let mut guard = write_shard(&self.state);
        guard.check_legacy_mutation()?;
        let result = match guard.index.as_ref() {
            Some(index) if !index.is_empty() => Err(Status::failed_precondition(format!(
                "shard holds {} vectors; vector backend configuration is locked for the generation",
                index.len()
            ))),
            Some(index) => {
                let existing = index
                    .backend_config()
                    .map_err(|e| Status::internal(format!("read vector backend config: {e}")))?;
                if index.dim_opt() == Some(dim) && existing == config {
                    return Ok(true);
                }
                Err(Status::already_exists(
                    "a different vector backend configuration is already locked on this shard",
                ))
            }
            None => {
                let built = build()?;
                let adopted = Self::adopt_layout(guard.bm25.as_ref(), built)?;
                guard.advance_stats_epoch();
                guard.index = Some(adopted);
                Ok(false)
            }
        };
        if result.is_ok() {
            if guard.exact_vectors.is_none()
                && guard.index.as_ref().is_some_and(VectorIndex::is_empty)
            {
                let store = self.fresh_exact_store(guard.generation.as_ref(), dim)?;
                guard.exact_vectors = Some(store);
            }
            if let Some(wal) = guard.wal.as_mut() {
                wal.update_manifest(|manifest| {
                    manifest.dim = dim as u32;
                    manifest.set_backend_config(config.clone());
                });
            }
        }
        result
    }

    /// Apply one ingested batch under the write lock. Returns
    /// `(added, global id of the batch's first vector)`.
    pub(in crate::node) fn apply_batch(
        &self,
        batch: AddVectorsRequest,
        stable_routing_key: Option<Vec<u8>>,
    ) -> Result<(u64, u64), Status> {
        let mut guard = write_shard(&self.state);
        self.apply_batch_locked(&mut guard, batch, stable_routing_key)
    }
}
