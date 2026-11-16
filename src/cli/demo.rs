//! Shard index sources: the generated demo corpus, or the image on disk.

use std::path::Path;

use pipestream_search::config::{DemoConfig, ShardConfig};
use pipestream_search::harness;
use pipestream_search::vector::VectorIndex;

/// Build a demo index by fitting provider-owned state on a sample and then
/// constructing the served generation from that state.
fn build_demo_index(demo: DemoConfig, backend_kind: &str) -> Result<VectorIndex, String> {
    let corpus = harness::unit_vectors(demo.vectors, demo.dim, 0xDE10_0001);
    let sample_n = (demo.vectors / 5).max(1).min(demo.vectors);
    let backend_config = VectorIndex::fit_backend_config(
        backend_kind,
        demo.dim,
        demo.bit_width,
        &corpus[..sample_n * demo.dim],
    )
    .map_err(|e| format!("demo calibration fit: {e}"))?;
    let mut index = VectorIndex::from_backend_config(demo.dim, &backend_config)
        .map_err(|e| format!("demo index construct: {e}"))?;
    index
        .add(&corpus, demo.dim)
        .map_err(|e| format!("demo add: {e}"))?;
    index.prepare().map_err(|e| format!("demo prepare: {e}"))?;
    Ok(index)
}

/// Load a shard's index, or `None` when the shard starts empty: its index
/// path does not exist yet (a from-scratch shard awaiting
/// ConfigureVectorBackend + AddVectors; Flush later writes that path). When a snapshot
/// generation is active, the index loads from INSIDE it — the generation
/// always reflects the newest installed or flushed image.
pub(super) fn load_shard_index(
    shard: &ShardConfig,
    generation: Option<&Path>,
) -> Result<Option<VectorIndex>, String> {
    if let Some(demo) = shard.demo {
        eprintln!(
            "building demo index: {} vectors x dim {} @ {} bits",
            demo.vectors, demo.dim, demo.bit_width
        );
        return build_demo_index(demo, &shard.vector_backend).map(Some);
    }
    let path = match generation {
        Some(dir) => pipestream_search::node::generation_vector(dir),
        None => shard
            .index_path
            .as_ref()
            .expect("config validated an index source")
            .clone(),
    };
    if !path.exists() {
        return Ok(None);
    }
    let mut index = VectorIndex::load(&shard.vector_backend, &path)
        .map_err(|e| format!("load {}: {e}", path.display()))?;
    index
        .prepare()
        .map_err(|e| format!("prepare {}: {e}", path.display()))?;
    Ok(Some(index))
}
