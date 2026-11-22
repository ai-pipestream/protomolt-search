//! Vector backend configuration, Matryoshka tier rescoring, and batch entry.

mod batch;
mod splade;

use crate::node::*;

fn slice_matryoshka_query(tier_dim: u32, query_full_or_tier: &[f32]) -> Result<Vec<f32>, Status> {
    if tier_dim == 0 || query_full_or_tier.len() < tier_dim as usize {
        return Err(Status::invalid_argument(format!(
            "query vector len {} is shorter than matryoshka tier_dim {tier_dim}",
            query_full_or_tier.len()
        )));
    }
    if query_full_or_tier.iter().any(|x| !x.is_finite()) {
        return Err(Status::invalid_argument(
            "matryoshka query vector contains non-finite values",
        ));
    }
    let q_multi = crate::turbo_bridge::MultiResVectorOutput::from_full_and_sentences(
        query_full_or_tier,
        &[tier_dim],
        None,
        Vec::new(),
    );
    q_multi.tier(tier_dim).map(<[f32]>::to_vec).ok_or_else(|| {
        Status::invalid_argument(format!("cannot slice query vector to tier_dim {tier_dim}"))
    })
}

fn score_matryoshka_slots(
    guard: &ShardState,
    offset: u64,
    tier_dim: u32,
    q_slice: &[f32],
    slots: &[usize],
) -> Result<Vec<(u64, f32)>, Status> {
    let rows = &guard.multi_res_vectors;
    let index_len = guard.index.as_ref().map_or(0, VectorIndex::len);
    let mut out = Vec::with_capacity(slots.len());
    for &slot in slots {
        if slot >= index_len
            || guard
                .exact_vectors
                .as_ref()
                .is_some_and(|exact| !exact.contains_row(slot))
        {
            continue;
        }
        let Ok(local) = u32::try_from(slot) else {
            continue;
        };
        let doc_tier = rows
            .get(local)
            .and_then(|r| r.tier(tier_dim))
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "retained vector tier {tier_dim} is unavailable for row {local}"
                ))
            })?;
        let dot: f32 = q_slice
            .iter()
            .zip(doc_tier.iter())
            .map(|(a, b)| a * b)
            .sum();
        out.push((offset + slot as u64, dot));
    }
    out.sort_unstable_by(|a, b| b.1.total_cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
    Ok(out)
}

fn run_rescore_matryoshka(
    state: &Arc<RwLock<ShardState>>,
    offset: u64,
    req: crate::pb::RescoreMatryoshkaRequest,
) -> Result<crate::pb::RescoreMatryoshkaResponse, Status> {
    let scope = crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
    let q_slice = slice_matryoshka_query(req.target_dim, &req.query)?;
    let guard = read_shard(state);
    guard.check_stats_epoch(req.expected_stats_epoch, &req.expected_stats_incarnation)?;
    let (_, visibility_columns_known) = filter_known_flags(
        guard.bm25.as_ref(),
        &[],
        req.visibility
            .as_ref()
            .and_then(|view| view.filter.as_ref()),
    );
    let rows = guard.index.as_ref().map_or(0, VectorIndex::len);
    let slots =
        guard.visible_candidate_slots(req.visibility.as_ref(), &req.candidate_ids, offset, rows)?;
    let scored = score_matryoshka_slots(&guard, offset, req.target_dim, &q_slice, &slots)?;
    Ok(crate::pb::RescoreMatryoshkaResponse {
        hits: scored
            .into_iter()
            .map(|(doc_id, score)| RawLegHit {
                doc_id,
                score,
                rerank_candidate: None,
            })
            .collect(),
        stats_epoch: guard.stats_epoch,
        stats_incarnation: guard.stats_incarnation.bytes()?,
        visibility_fingerprint: scope.fingerprint().to_vec(),
        visibility_columns_known,
    })
}

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
        let q_slice = slice_matryoshka_query(tier_dim, query_full_or_tier)?;
        let guard = read_shard(&self.state);
        let rows = guard.index.as_ref().map_or(0, VectorIndex::len);
        let slots = guard.visible_candidate_slots(
            None,
            candidate_global_ids,
            self.config.slot_offset,
            rows,
        )?;
        score_matryoshka_slots(&guard, self.config.slot_offset, tier_dim, &q_slice, &slots)
    }

    pub(in crate::node) async fn handle_rescore_matryoshka(
        &self,
        request: Request<crate::pb::RescoreMatryoshkaRequest>,
    ) -> Result<Response<crate::pb::RescoreMatryoshkaResponse>, Status> {
        crate::metrics::timed(Route::RescoreMatryoshka, request, |request| async move {
            let req = request.into_inner();
            let offset = self.config.slot_offset;
            let state = self.state.clone();
            let response =
                tokio::task::spawn_blocking(move || run_rescore_matryoshka(&state, offset, req))
                    .await
                    .map_err(|e| {
                        Status::internal(format!("matryoshka rescore task failed: {e}"))
                    })??;
            Ok(Response::new(response))
        })
        .await
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
        guard.check_index_owned_write()?;
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
                let store = self.fresh_exact_store(dim)?;
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
