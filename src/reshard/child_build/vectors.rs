//! The vector half of a child image: the index, the exact rows, and the
//! retained multi-resolution rows remapped to child slots.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::multi_res::MultiResVectors;
use crate::vector::VectorIndex;
use crate::wal::WalManifest;

use super::HnswCompactionSources;

/// What writing the vector half leaves for the rest of the build.
pub(super) struct VectorSide {
    /// Parent global id per child vector slot.
    pub(super) parent_ids: Vec<u64>,
    /// Retained rows by child slot.
    pub(super) remapped: MultiResVectors,
    pub(super) exact_vector_path: PathBuf,
}

/// Write the child's vector index and exact rows at `vector_path`, with
/// retained rows moved to the child slots their vectors take.
pub(super) fn write_vector_side(
    manifest: &WalManifest,
    vectors: &[(u64, Vec<f32>)],
    retained: BTreeMap<u64, Vec<u8>>,
    vector_path: &Path,
    hnsw_sources: Option<&HnswCompactionSources>,
    prebuilt_index: Option<VectorIndex>,
) -> Result<VectorSide, String> {
    let dim = manifest.dim as usize;
    let mut parent_ids = Vec::with_capacity(vectors.len());
    let mut flat = Vec::with_capacity(vectors.len() * dim);
    for (id, vector) in vectors {
        parent_ids.push(*id);
        flat.extend_from_slice(vector);
    }
    let remapped = remap_retained(manifest.dim, vectors, retained)?;
    let index = match prebuilt_index {
        Some(index) => index,
        None => build_index(manifest, vectors, &flat, hnsw_sources)?,
    };
    index
        .write(vector_path)
        .map_err(|e| format!("write {}: {e}", vector_path.display()))?;
    let exact_vector_path = crate::node::exact_vector_sidecar_path(vector_path);
    crate::exact_vectors::ExactVectorStore::from_values(dim, flat)
        .and_then(|store| store.write(&exact_vector_path))
        .map_err(|e| format!("write {}: {e}", exact_vector_path.display()))?;
    Ok(VectorSide {
        parent_ids,
        remapped,
        exact_vector_path,
    })
}

/// Each retained row under the child slot its vector takes. A retained
/// row whose vector is not in the child is an error, never dropped.
fn remap_retained(
    dim: u32,
    vectors: &[(u64, Vec<f32>)],
    mut retained: BTreeMap<u64, Vec<u8>>,
) -> Result<MultiResVectors, String> {
    let mut remapped = MultiResVectors::default();
    for (slot, (id, vector)) in vectors.iter().enumerate() {
        let Some(bytes) = retained.remove(id) else {
            continue;
        };
        let batch = crate::pb::AddVectorsRequest {
            vectors: vector.clone(),
            dim,
            materialized_rows: vec![bytes],
        };
        let row = crate::multi_res_wal::decode_batch(&batch)?
            .into_iter()
            .next()
            .ok_or("materialized child row is missing")?;
        let slot = u32::try_from(slot).map_err(|_| "materialized child slot exceeds u32")?;
        remapped.insert(slot, row);
    }
    if !retained.is_empty() {
        return Err("materialized rows have no matching child vectors".into());
    }
    Ok(remapped)
}

/// The child's index: merged from the sources' HNSW graphs when a
/// compaction can reuse them, else built from the rows in slot order.
fn build_index(
    manifest: &WalManifest,
    vectors: &[(u64, Vec<f32>)],
    flat: &[f32],
    hnsw_sources: Option<&HnswCompactionSources>,
) -> Result<VectorIndex, String> {
    let dim = manifest.dim as usize;
    let backend_config = manifest
        .backend_config()
        .map_err(|e| format!("construct child backend config: {e}"))?;
    if let Some(sources) = hnsw_sources {
        if let Some(index) = sources.build_merged_index(dim, &backend_config, vectors)? {
            return Ok(index);
        }
    }
    let mut index = VectorIndex::from_backend_config(dim, &backend_config)
        .map_err(|e| format!("construct child index: {e}"))?;
    index
        .add(flat, dim)
        .map_err(|e| format!("add child vectors: {e}"))?;
    index
        .prepare()
        .map_err(|e| format!("prepare child index: {e}"))?;
    Ok(index)
}

/// Publish the retained payload for `rows` child rows. An empty map still
/// writes, retiring stale retention when an output path is reused; the
/// path is returned only when rows were retained.
pub(super) fn publish_retained(
    remapped: &MultiResVectors,
    vector_path: &Path,
    rows: usize,
) -> Result<Option<PathBuf>, String> {
    let payload = crate::multi_res::sidecar_path(vector_path);
    crate::multi_res_storage::write(remapped, &payload, rows as u64, false)
        .map_err(|error| format!("write {}: {error}", payload.display()))?;
    if payload.try_exists().map_err(|error| error.to_string())? {
        crate::live_docs::LiveDocs::default()
            .with_retained_vectors()
            .write(
                &crate::node::live_docs_sidecar_path(vector_path),
                rows as u64,
            )
            .map_err(|error| format!("write retained generation reader gate: {error}"))?;
    }
    Ok((!remapped.is_empty()).then_some(payload))
}
