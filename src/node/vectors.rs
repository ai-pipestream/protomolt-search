//! Dense vector batch ingestion, Matryoshka tier rescoring, and SPLADE scoring.

use crate::node::*;

impl NodeServiceImpl {
    pub(in crate::node) fn checked_splade_k(&self, k: usize) -> Result<u32, Status> {
        if k > self.config.max_k as usize {
            return Err(Status::invalid_argument(format!(
                "k={k} exceeds this node's ScoreSplade max_k={}; lower k or raise --max-k",
                self.config.max_k
            )));
        }
        u32::try_from(k).map_err(|_| Status::invalid_argument("SPLADE k exceeds the wire range"))
    }

    /// Score quantized SPLADE 128-doc Block-Max postings on `field` with
    /// live-doc and CEL filter admission.
    pub fn score_splade_field(
        &self,
        field: &str,
        query_weights: &[(String, f32)],
        query_l1_slack: f32,
        k: usize,
        filter: Option<&crate::pb::FilterExpr>,
    ) -> Result<Vec<(u64, f32)>, Status> {
        let k = self.checked_splade_k(k)?;
        let resp = self.run_score_splade(crate::pb::ScoreSpladeRequest {
            field: field.to_string(),
            query_weights: query_weights
                .iter()
                .map(|(term, weight)| crate::pb::SpladeTermWeight {
                    term: term.clone(),
                    weight: *weight,
                })
                .collect(),
            query_l1_slack,
            k,
            filter: filter.cloned(),
            initial_floor: 0.0,
            visibility: None,
            expected_stats_epoch: 0,
            expected_stats_incarnation: Vec::new(),
        })?;
        Ok(resp.hits.into_iter().map(|h| (h.doc_id, h.score)).collect())
    }

    pub(crate) fn run_score_splade(
        &self,
        req: crate::pb::ScoreSpladeRequest,
    ) -> Result<crate::pb::ScoreSpladeResponse, Status> {
        self.checked_splade_k(req.k as usize)?;
        if let Some(f) = req.filter.as_ref() {
            crate::filter::validate_filter(f)?;
        }
        let scope = crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
        let visibility = req
            .visibility
            .as_ref()
            .and_then(|view| view.filter.as_ref());
        let field = if req.field.is_empty() {
            "body"
        } else {
            req.field.as_str()
        };
        let guard = read_shard(&self.state);
        guard.check_stats_epoch(req.expected_stats_epoch, &req.expected_stats_incarnation)?;
        let (_, filter_columns_known) =
            filter_known_flags(guard.bm25.as_ref(), &[], req.filter.as_ref());
        let (_, visibility_columns_known) =
            filter_known_flags(guard.bm25.as_ref(), &[], visibility);
        let stats_epoch = guard.stats_epoch;
        let stats_incarnation = guard.stats_incarnation.bytes()?;
        let visibility_fingerprint = scope.fingerprint().to_vec();
        let Some(store) = guard.bm25.as_ref() else {
            return Ok(crate::pb::ScoreSpladeResponse {
                hits: Vec::new(),
                filter_columns_known,
                field_known: false,
                blocks_visited: 0,
                blocks_skipped: 0,
                postings_scored: 0,
                final_floor: req.initial_floor.max(0.0),
                stats_epoch,
                stats_incarnation,
                visibility_fingerprint,
                visibility_columns_known,
            });
        };
        if store.as_index().is_none() {
            return Err(Status::failed_precondition(
                "bm25 bulk build in progress; Flush first",
            ));
        }
        let Some(fi) = store.field_index(field) else {
            return Ok(crate::pb::ScoreSpladeResponse {
                hits: Vec::new(),
                filter_columns_known,
                field_known: false,
                blocks_visited: 0,
                blocks_skipped: 0,
                postings_scored: 0,
                final_floor: req.initial_floor.max(0.0),
                stats_epoch,
                stats_incarnation,
                visibility_fingerprint,
                visibility_columns_known,
            });
        };
        let index = store.field_view(fi).expect("searchability checked above");
        if !index.has_splade() {
            return Ok(crate::pb::ScoreSpladeResponse {
                hits: Vec::new(),
                filter_columns_known,
                field_known: true,
                blocks_visited: 0,
                blocks_skipped: 0,
                postings_scored: 0,
                final_floor: req.initial_floor.max(0.0),
                stats_epoch,
                stats_incarnation,
                visibility_fingerprint,
                visibility_columns_known,
            });
        }
        let rows = store.next_doc_id() as usize;
        let allow = if req.filter.is_some() {
            let (_, allow, _) = resolve_shard_filters(
                Some(store),
                None,
                rows,
                &[],
                &[],
                req.filter.as_ref(),
                self.knobs.segment_pruning(),
                None,
            )?;
            allow
        } else {
            None
        };
        let vis_allow = if visibility.is_some() {
            let (_, allow, _) = resolve_shard_filters(
                Some(store),
                None,
                rows,
                &[],
                &[],
                visibility,
                self.knobs.segment_pruning(),
                None,
            )?;
            allow
        } else {
            None
        };
        let weights: Vec<(String, f32)> = req
            .query_weights
            .into_iter()
            .map(|tw| (tw.term, tw.weight))
            .collect();
        let admitted = |doc: u32| {
            !guard.live_docs.is_deleted(doc as usize)
                && allow.as_ref().is_none_or(|mask| mask.test(doc as usize))
                && vis_allow
                    .as_ref()
                    .is_none_or(|mask| mask.test(doc as usize))
        };
        let k = req.k as usize;
        let initial_floor = req.initial_floor.max(0.0);
        let (raw_hits, final_floor, stats) = index.score_splade_with_floor(
            &weights,
            req.query_l1_slack,
            initial_floor,
            k,
            &admitted,
        );
        let offset = self.config.slot_offset;
        let hits: Vec<crate::pb::SpladeHit> = raw_hits
            .into_iter()
            .map(|(doc_id, score)| crate::pb::SpladeHit {
                doc_id: offset + u64::from(doc_id),
                score,
            })
            .collect();
        Ok(crate::pb::ScoreSpladeResponse {
            hits,
            filter_columns_known,
            field_known: true,
            blocks_visited: stats.blocks_visited,
            blocks_skipped: stats.blocks_skipped,
            postings_scored: stats.postings_scored,
            final_floor,
            stats_epoch,
            stats_incarnation,
            visibility_fingerprint,
            visibility_columns_known,
        })
    }

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

    /// [`Self::apply_batch`] against a caller-held [`ShardState`] (shared by
    /// live ingest under the write lock and compaction shadow tailing).
    pub(crate) fn apply_batch_locked(
        &self,
        guard: &mut ShardState,
        batch: AddVectorsRequest,
        stable_routing_key: Option<Vec<u8>>,
    ) -> Result<(u64, u64), Status> {
        guard.check_legacy_mutation()?;
        let materialized =
            crate::multi_res_wal::decode_batch(&batch).map_err(Status::invalid_argument)?;
        if batch.vectors.is_empty() {
            return Ok((0, 0));
        }
        let matryoshka_enabled = !self.config.matryoshka_prefix_dims.is_empty()
            || self.config.matryoshka_active_dim.is_some();
        let known_dim = guard.index.as_ref().and_then(|i| i.dim_opt());
        let raw_dim = if batch.dim != 0 {
            let d = batch.dim as usize;
            if !matryoshka_enabled {
                if let Some(known) = known_dim {
                    if known != d {
                        return Err(Status::invalid_argument(format!(
                            "batch dim {d} does not match shard dim {known}"
                        )));
                    }
                }
            }
            d
        } else {
            known_dim.ok_or_else(|| {
                Status::failed_precondition(
                    "shard has no index or calibration yet; set calibration first or pass dim",
                )
            })?
        };
        if !batch.vectors.len().is_multiple_of(raw_dim) {
            return Err(Status::invalid_argument(format!(
                "batch of {} floats is not a multiple of dim {raw_dim}",
                batch.vectors.len()
            )));
        }
        if stable_routing_key.is_some() && batch.vectors.len() / raw_dim != 1 {
            return Err(Status::invalid_argument(
                "a replication stable-key metadata value may carry exactly one vector",
            ));
        }
        if let Some((vi, ci, v)) = first_invalid_coordinate(&batch.vectors, raw_dim) {
            return Err(Status::invalid_argument(format!(
                "invalid input value at vector {vi}, coord {ci}: {v}"
            )));
        }
        let start_slot = guard.index.as_ref().map_or(0, VectorIndex::len);
        let (effective_vectors, dim, multi_rows) = if !materialized.is_empty() {
            if known_dim.is_some_and(|known| known != raw_dim)
                || self
                    .config
                    .matryoshka_active_dim
                    .is_some_and(|dim| dim as usize != raw_dim)
            {
                return Err(Status::invalid_argument(
                    "materialized active tier differs from shard dimension",
                ));
            }
            let rows = materialized
                .into_iter()
                .enumerate()
                .map(|(i, row)| {
                    u32::try_from(start_slot + i)
                        .map(|slot| (slot, row))
                        .map_err(|_| Status::resource_exhausted("retained vector slot exceeds u32"))
                })
                .collect::<Result<Vec<_>, _>>()?;
            (batch.vectors, raw_dim, rows)
        } else if matryoshka_enabled {
            let active_target = self
                .config
                .matryoshka_active_dim
                .filter(|&ad| (ad as usize) <= raw_dim);
            let (vecs, edim, rows) = crate::turbo_bridge::MultiResVectorOutput::project_batch(
                &batch.vectors,
                raw_dim,
                &self.config.matryoshka_prefix_dims,
                active_target,
                start_slot,
            )?;
            if let Some(known) = known_dim {
                if known != edim {
                    return Err(Status::invalid_argument(format!(
                        "projected matryoshka dim {edim} does not match shard dim {known}"
                    )));
                }
            }
            let retained = rows
                .into_iter()
                .map(|(slot, output)| (slot, crate::multi_res::RetainedVector { output }))
                .collect();
            (vecs, edim, retained)
        } else {
            (batch.vectors, raw_dim, Vec::new())
        };
        for (_, row) in &multi_rows {
            crate::multi_res_wal::require_tiers(
                row,
                &self.config.matryoshka_prefix_dims,
                self.config.matryoshka_active_dim,
            )
            .map_err(Status::invalid_argument)?;
        }
        let logged_multi_rows = multi_rows
            .iter()
            .map(|(_, row)| crate::multi_res::encode(row))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| {
                Status::invalid_argument(format!("retain multi-resolution vectors: {error}"))
            })?;
        // Invalidate before the first mutation, including failures after a
        // provider commit. Neither snapshot copy nor admitted physical reads
        // may keep using the pre-append generation claim.
        guard.advance_stats_epoch();
        let (first_id, index_len) = {
            let index = match guard.index.as_mut() {
                Some(index) => index,
                None => {
                    // From-scratch: create the configured provider. Explicit
                    // provisioning is the multi-shard path; this remains the
                    // single-shard convenience path.
                    let created = self.fresh_index(guard.bm25.as_ref(), dim)?;
                    guard.index = Some(created);
                    guard.index.as_mut().expect("just constructed")
                }
            };
            (self.config.slot_offset + index.len() as u64, index.len())
        };
        if guard.exact_vectors.is_none() {
            if index_len != 0 {
                return Err(Status::failed_precondition(format!(
                    "the shard has {index_len} provider vectors but no exact-vector sidecar; \
                     rebuild or backfill the generation before appending"
                )));
            }
            let store = self.fresh_exact_store(guard.generation.as_ref(), dim)?;
            guard.exact_vectors = Some(store);
        }
        let exact = guard.exact_vectors.as_ref().expect("ensured above");
        if exact.len() != index_len || exact.dim() != Some(dim) {
            // On the segment layout the provider's length counts every
            // catalog row, sealed document-only rows included: vectors
            // arriving after such a seal have no segment to join.
            let tail_len = guard
                .index
                .as_ref()
                .and_then(VectorIndex::as_segmented)
                .map_or(index_len, |p| p.tail().len());
            let sealed_without_vectors = index_len
                .saturating_sub(tail_len)
                .saturating_sub(exact.len());
            if sealed_without_vectors > 0 {
                return Err(Status::failed_precondition(format!(
                    "{sealed_without_vectors} rows are sealed in segments without vectors; \
                     vectors seal with their documents, so send AddVectors for each \
                     AddDocuments batch before the next (or ingest through IngestMapped), \
                     or run this shard with --layout=single-image"
                )));
            }
            return Err(Status::failed_precondition(format!(
                "exact-vector sidecar shape {:?}x{} does not match provider shape {dim}x{index_len}",
                exact.dim(),
                exact.len()
            )));
        }
        // Apply first, log after, under this one lock. A failed apply
        // must never reach the log: its assigned ids would be reused by
        // the next batch and the duplicate would poison every replay.
        // Durability is unaffected — both sides are volatile until
        // Flush, which fsyncs the log BEFORE the index images.
        guard
            .index
            .as_mut()
            .expect("constructed or present above")
            .add(&effective_vectors, dim)
            .map_err(|e| Status::invalid_argument(format!("{e}")))?;
        guard
            .exact_vectors
            .as_mut()
            .expect("validated above")
            .append(&effective_vectors, dim)
            .map_err(|e| {
                Status::internal(format!(
                    "exact-vector append failed after provider commit: {e}; refuse further \
                     ingest and rebuild this generation"
                ))
            })?;
        if !multi_rows.is_empty() {
            for (slot, row) in multi_rows {
                guard.multi_res_vectors.insert(slot, row);
            }
        }
        let committed_config = guard
            .index
            .as_ref()
            .expect("constructed or present above")
            .backend_config()
            .map_err(|e| Status::internal(format!("read vector backend config: {e}")))?;
        // One record PER VECTOR: contiguous ids hash to different
        // buckets, and a bucket file must never hold vectors that belong
        // to another bucket. Buffered (no fsync per batch); Flush and
        // generation rotation fsync.
        if let Some(wal) = guard.wal.as_mut() {
            wal.update_manifest(|m| {
                if m.dim == 0 {
                    m.dim = dim as u32;
                }
                m.set_backend_config(committed_config.clone());
            });
        }
        for (i, vector) in effective_vectors.chunks_exact(dim).enumerate() {
            wal_append_or_degrade(
                &mut guard.wal,
                wal_record::Op::AddVectors(LoggedAddVectors {
                    first_id: first_id + i as u64,
                    batch: Some(AddVectorsRequest {
                        materialized_rows: logged_multi_rows.get(i).cloned().into_iter().collect(),
                        vectors: vector.to_vec(),
                        dim: dim as u32,
                    }),
                    stable_routing_keys: stable_routing_key.clone().into_iter().collect(),
                }),
            );
        }
        Ok(((effective_vectors.len() / dim) as u64, first_id))
    }
}
