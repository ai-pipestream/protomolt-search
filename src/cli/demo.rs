//! The generated demo corpus a `[demo]` shard serves.

use pipestream_search::config::DemoConfig;
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

/// The demo corpus a `[demo]` shard serves from memory.
pub(super) fn load_demo_index(demo: DemoConfig, backend_kind: &str) -> Result<VectorIndex, String> {
    eprintln!(
        "building demo index: {} vectors x dim {} @ {} bits",
        demo.vectors, demo.dim, demo.bit_width
    );
    build_demo_index(demo, backend_kind)
}
