//! Mapped-vector validation, provider commit, WAL manifest lock, and WAL append.

use crate::multi_res::RetainedVector;
use crate::node::*;

/// Validated vector payload and multi-resolution projections ready for commit.
pub(super) struct PreparedVector {
    values: Vec<f32>,
    dim: usize,
    multi_rows: Vec<(u32, RetainedVector)>,
    multi_log: Vec<Vec<u8>>,
}

/// Vector record payload ready for WAL append after provider state is locked.
pub(super) struct LoggedVector {
    values: Vec<f32>,
    dim: usize,
    multi_log: Vec<Vec<u8>>,
}

struct ProjectedVector {
    values: Vec<f32>,
    dim: usize,
    multi_rows: Vec<(u32, RetainedVector)>,
}

impl NodeServiceImpl {
    /// Validate a mapped document's vector coordinates, Matryoshka projection,
    /// provider dimension, and exact-vector sidecar shape before any row mutates.
    pub(super) fn prepare_mapped_vector(
        &self,
        guard: &mut ShardState,
        vector: Option<Vec<f32>>,
        vector_tip: u32,
    ) -> Result<Option<PreparedVector>, Status> {
        let Some(raw) = vector else {
            return Ok(None);
        };
        let projected = self.project_mapped_vector(raw, vector_tip)?;
        self.ensure_mapped_vector_stores(guard, projected.dim, vector_tip)?;
        let multi_log = self.encode_multi_res_rows(&projected.multi_rows)?;
        Ok(Some(PreparedVector {
            values: projected.values,
            dim: projected.dim,
            multi_rows: projected.multi_rows,
            multi_log,
        }))
    }

    fn project_mapped_vector(
        &self,
        raw: Vec<f32>,
        vector_tip: u32,
    ) -> Result<ProjectedVector, Status> {
        let raw_dim = raw.len();
        if let Some((_, ci, value)) = first_invalid_coordinate(&raw, raw_dim) {
            return Err(Status::invalid_argument(format!(
                "mapped vector coordinate {ci} is {value}; vectors must be finite"
            )));
        }
        let matryoshka_enabled = !self.config.matryoshka_prefix_dims.is_empty()
            || self.config.matryoshka_active_dim.is_some();
        if !matryoshka_enabled {
            return Ok(ProjectedVector {
                values: raw,
                dim: raw_dim,
                multi_rows: Vec::new(),
            });
        }
        let active_target = self
            .config
            .matryoshka_active_dim
            .filter(|&ad| (ad as usize) <= raw_dim);
        let (values, dim, rows) = crate::turbo_bridge::MultiResVectorOutput::project_batch(
            &raw,
            raw_dim,
            &self.config.matryoshka_prefix_dims,
            active_target,
            vector_tip as usize,
        )?;
        let multi_rows = rows
            .into_iter()
            .map(|(slot, output)| (slot, RetainedVector { output }))
            .collect();
        Ok(ProjectedVector {
            values,
            dim,
            multi_rows,
        })
    }

    fn ensure_mapped_vector_stores(
        &self,
        guard: &mut ShardState,
        dim: usize,
        vector_tip: u32,
    ) -> Result<(), Status> {
        if let Some(known) = guard.index.as_ref().and_then(|i| i.dim_opt()) {
            if known != dim {
                return Err(Status::invalid_argument(format!(
                    "mapped vector has {dim} floats but the shard's index is dim {known}"
                )));
            }
        }
        if guard.index.is_none() {
            let created = self.fresh_index(guard.bm25.as_ref(), dim)?;
            guard.index = Some(created);
        }
        if guard.exact_vectors.is_none() {
            if vector_tip != 0 {
                return Err(Status::failed_precondition(format!(
                    "the shard has {vector_tip} provider vectors but no exact-vector \
                     sidecar; rebuild or backfill the generation before mapped ingest"
                )));
            }
            let store = self.fresh_exact_store(guard.generation.as_ref(), dim)?;
            guard.exact_vectors = Some(store);
        }
        let exact = guard.exact_vectors.as_ref().expect("ensured above");
        if exact.len() != vector_tip as usize || exact.dim() != Some(dim) {
            return Err(Status::failed_precondition(format!(
                "exact-vector sidecar shape {:?}x{} does not match provider shape \
                 {dim}x{vector_tip}",
                exact.dim(),
                exact.len()
            )));
        }
        Ok(())
    }

    fn encode_multi_res_rows(
        &self,
        multi_rows: &[(u32, RetainedVector)],
    ) -> Result<Vec<Vec<u8>>, Status> {
        for (_, row) in multi_rows {
            crate::multi_res_wal::require_tiers(
                row,
                &self.config.matryoshka_prefix_dims,
                self.config.matryoshka_active_dim,
            )
            .map_err(Status::invalid_argument)?;
        }
        multi_rows
            .iter()
            .map(|(_, row)| crate::multi_res::encode(row))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| {
                Status::invalid_argument(format!("retain multi-resolution vectors: {error}"))
            })
    }
}

/// Commit a prepared mapped vector to the provider and sidecar stores, then
/// persist lazy WAL `manifest.dim` and `backend_config` before any WAL record
/// increments `records_appended`.
pub(super) fn apply_and_lock_mapped_vector(
    guard: &mut ShardState,
    doc_id: u32,
    prepared: Option<PreparedVector>,
) -> Result<Option<LoggedVector>, Status> {
    let Some(prepared) = prepared else {
        return Ok(None);
    };
    let dim = prepared.dim;
    let index = guard.index.as_mut().expect("ensured during validation");
    index.add(&prepared.values, dim).map_err(|e| {
        Status::internal(format!(
            "vector apply failed after validation: {e}; the shard's legs may have \
             diverged — the next mapped document will refuse if so"
        ))
    })?;
    guard
        .exact_vectors
        .as_mut()
        .expect("validated alongside provider index")
        .append(&prepared.values, dim)
        .map_err(|e| {
            Status::internal(format!(
                "exact-vector append failed after provider commit: {e}; refuse further \
                 ingest and rebuild this generation"
            ))
        })?;
    for (slot, out) in prepared.multi_rows {
        guard.multi_res_vectors.insert(slot, out);
    }
    let committed_config = guard
        .index
        .as_ref()
        .expect("provider index remains present")
        .backend_config()
        .map_err(|e| {
            Status::internal(format!(
                "read vector backend config after mapped ingest: {e}"
            ))
        })?;
    lock_wal_vector_manifest(&mut guard.wal, doc_id, dim, committed_config);
    Ok(Some(LoggedVector {
        values: prepared.values,
        dim,
        multi_log: prepared.multi_log,
    }))
}

/// Persist `dim` and `committed_config` into `manifest.toml` before the first
/// bucketed WAL record makes the manifest immutable. If rewriting the manifest
/// fails on the generation's initial row (`records_appended == 0`), retire the
/// log as `.broken` rather than appending vector records under `dim == 0`.
fn lock_wal_vector_manifest(
    wal_slot: &mut Option<WalWriter>,
    doc_id: u32,
    dim: usize,
    committed_config: VectorBackendConfig,
) {
    let Some(wal) = wal_slot.as_mut() else { return };
    let manifest_locked = wal.manifest().dim == dim as u32
        && wal.manifest().backend_config().ok().as_ref() == Some(&committed_config);
    if manifest_locked {
        return;
    }
    let first_generation_row = doc_id == 0
        && wal.manifest().preexisting_vectors == 0
        && wal.manifest().preexisting_documents == 0;
    let manifest_incomplete = wal.manifest().dim == 0 || wal.manifest().backend_config().is_err();
    let updated = wal.update_manifest(|m| {
        if m.dim == 0 {
            m.dim = dim as u32;
        }
        m.set_backend_config(committed_config.clone());
    });
    if !updated && (first_generation_row || manifest_incomplete) {
        let dir = wal.dir().to_path_buf();
        let broken = dir.with_extension("broken");
        eprintln!(
            "wal: manifest update in {} failed before first record; retiring the log as {} — \
             this shard continues UNLOGGED and can no longer be resharded from its log (rebuild \
             required)",
            dir.display(),
            broken.display()
        );
        *wal_slot = None;
        if let Err(e) = std::fs::rename(&dir, &broken) {
            eprintln!("wal: could not rename the broken generation: {e}");
        }
    }
}

/// Append `LoggedAddDocuments` and, when present, `LoggedAddVectors` in clock
/// order after both in-memory legs and the WAL manifest have settled.
pub(super) fn append_committed_wal_ops(
    wal_slot: &mut Option<WalWriter>,
    global_id: u64,
    doc: AddDocumentsRequest,
    stable_routing_key: Option<Vec<u8>>,
    logged_vector: Option<LoggedVector>,
) {
    wal_append_or_degrade(
        wal_slot,
        wal_record::Op::AddDocuments(LoggedAddDocuments {
            first_id: global_id,
            documents: vec![doc],
            stable_routing_keys: stable_routing_key.clone().into_iter().collect(),
            source_references: Vec::new(),
        }),
    );
    if let Some(vec_op) = logged_vector {
        wal_append_or_degrade(
            wal_slot,
            wal_record::Op::AddVectors(LoggedAddVectors {
                first_id: global_id,
                batch: Some(AddVectorsRequest {
                    materialized_rows: vec_op.multi_log,
                    vectors: vec_op.values,
                    dim: vec_op.dim as u32,
                }),
                stable_routing_keys: stable_routing_key.into_iter().collect(),
            }),
        );
    }
}
