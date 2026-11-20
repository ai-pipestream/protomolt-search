//! Dense vector batch apply: validation, provider commit, and WAL logging.

use crate::multi_res::RetainedVector;
use crate::node::*;

/// The dimensions one batch was validated against.
#[derive(Clone, Copy)]
struct BatchShape {
    raw_dim: usize,
    known_dim: Option<usize>,
    /// The provider slot the batch's first vector lands at.
    start_slot: usize,
}

/// One validated batch in the shape the provider stores.
struct PreparedBatch {
    vectors: Vec<f32>,
    dim: usize,
    multi_rows: Vec<(u32, RetainedVector)>,
}

/// Apply first, log after, under one lock. A failed apply must never reach
/// the log: its assigned ids would be reused by the next batch and the
/// duplicate would poison every replay. Durability is unaffected — both sides
/// are volatile until Flush, which fsyncs the log BEFORE the index images.
fn commit_vectors(guard: &mut ShardState, prepared: PreparedBatch) -> Result<Vec<f32>, Status> {
    let PreparedBatch {
        vectors: effective_vectors,
        dim,
        multi_rows,
    } = prepared;
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
    Ok(effective_vectors)
}

/// One record PER VECTOR: contiguous ids hash to different buckets, and a
/// bucket file must never hold vectors that belong to another bucket.
/// Buffered (no fsync per batch); Flush and generation rotation fsync.
fn log_vectors(
    guard: &mut ShardState,
    effective_vectors: &[f32],
    dim: usize,
    first_id: u64,
    logged_multi_rows: &[Vec<u8>],
    stable_routing_key: Option<Vec<u8>>,
) -> Result<(), Status> {
    let committed_config = guard
        .index
        .as_ref()
        .expect("constructed or present above")
        .backend_config()
        .map_err(|e| Status::internal(format!("read vector backend config: {e}")))?;
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
    Ok(())
}

/// Refuse an append whose exact-vector sidecar does not line up with the
/// provider.
fn check_exact_shape(guard: &ShardState, dim: usize, index_len: usize) -> Result<(), Status> {
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
    Ok(())
}

impl NodeServiceImpl {
    fn matryoshka_enabled(&self) -> bool {
        !self.config.matryoshka_prefix_dims.is_empty()
            || self.config.matryoshka_active_dim.is_some()
    }

    fn batch_shape(
        &self,
        guard: &ShardState,
        batch: &AddVectorsRequest,
        has_stable_key: bool,
    ) -> Result<BatchShape, Status> {
        let matryoshka_enabled = self.matryoshka_enabled();
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
        if has_stable_key && batch.vectors.len() / raw_dim != 1 {
            return Err(Status::invalid_argument(
                "each request in a replication stable-keys stream may carry exactly one vector",
            ));
        }
        if let Some((vi, ci, v)) = first_invalid_coordinate(&batch.vectors, raw_dim) {
            return Err(Status::invalid_argument(format!(
                "invalid input value at vector {vi}, coord {ci}: {v}"
            )));
        }
        let start_slot = guard.index.as_ref().map_or(0, VectorIndex::len);
        Ok(BatchShape {
            raw_dim,
            known_dim,
            start_slot,
        })
    }

    /// The vectors the provider stores and the rows retained beside them:
    /// client-materialized tiers, a Matryoshka projection, or the batch as sent.
    fn prepare_batch(
        &self,
        batch: AddVectorsRequest,
        materialized: Vec<RetainedVector>,
        shape: &BatchShape,
    ) -> Result<PreparedBatch, Status> {
        let BatchShape {
            raw_dim,
            known_dim,
            start_slot,
        } = *shape;
        let (vectors, dim, multi_rows) = if !materialized.is_empty() {
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
        } else if self.matryoshka_enabled() {
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
        Ok(PreparedBatch {
            vectors,
            dim,
            multi_rows,
        })
    }

    /// The batch's first global id, creating the provider and exact-vector
    /// store on a from-scratch shard.
    fn ensure_vector_stores(&self, guard: &mut ShardState, dim: usize) -> Result<u64, Status> {
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
        check_exact_shape(guard, dim, index_len)?;
        Ok(first_id)
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
            if stable_routing_key.is_some() {
                return Err(Status::invalid_argument(
                    "a keyed AddVectorsRequest must contain at least one vector",
                ));
            }
            return Ok((0, 0));
        }
        let shape = self.batch_shape(guard, &batch, stable_routing_key.is_some())?;
        let prepared = self.prepare_batch(batch, materialized, &shape)?;
        let logged_multi_rows = prepared
            .multi_rows
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
        let dim = prepared.dim;
        let first_id = self.ensure_vector_stores(guard, dim)?;
        let effective_vectors = commit_vectors(guard, prepared)?;
        log_vectors(
            guard,
            &effective_vectors,
            dim,
            first_id,
            &logged_multi_rows,
            stable_routing_key,
        )?;
        Ok(((effective_vectors.len() / dim) as u64, first_id))
    }
}
