//! The sized product-side handle around a vector provider.

use std::fmt;
use std::ops::Range;
use std::path::Path;

use super::{
    checked_row_transcripts, embedded_turbovec, Mask, VectorBackendConfig, VectorBackendDescriptor,
    VectorError, VectorProvider, VectorSearchOptions, VectorSearchResults, VectorStreamBatch,
    VectorStreamControl, VectorStreamSummary, EMBEDDED_TURBOVEC,
};

/// Sized product-side handle around an arbitrary vector engine.
pub struct VectorIndex {
    pub(super) engine: Box<dyn VectorProvider>,
}

impl fmt::Debug for VectorIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("VectorIndex")
            .field("descriptor", &self.descriptor())
            .field("len", &self.len())
            .finish()
    }
}

impl VectorIndex {
    /// The segmented provider behind this index, when it is one.
    pub fn as_segmented(&self) -> Option<&crate::segmented_vectors::SegmentedProvider> {
        self.engine.as_segmented()
    }

    pub(crate) fn as_hnsw(&self) -> Option<&crate::hnsw::HnswIndex> {
        self.engine.as_hnsw()
    }

    pub fn as_segmented_mut(&mut self) -> Option<&mut crate::segmented_vectors::SegmentedProvider> {
        self.engine.as_segmented_mut()
    }

    /// Wrap a provider supplied by another module or downstream crate.
    pub fn from_provider(provider: impl VectorProvider + 'static) -> Self {
        Self {
            engine: Box::new(provider),
        }
    }

    /// Create the configured backend: the exhaustive embedded adapter (the
    /// default) or the HNSW graph. Unknown kinds refuse
    /// instead of silently selecting either.
    pub fn create(
        backend_kind: &str,
        dimension: usize,
        bits_per_dimension: usize,
    ) -> Result<Self, VectorError> {
        match backend_kind {
            EMBEDDED_TURBOVEC => embedded_turbovec::create(dimension, bits_per_dimension),
            crate::hnsw::HNSW => crate::hnsw::create(dimension),
            other => Err(VectorError::new(format!(
                "unknown vector backend {other:?}; available: {EMBEDDED_TURBOVEC}, {}",
                crate::hnsw::HNSW
            ))),
        }
    }

    /// Fit backend-owned construction state from a representative sample.
    /// The returned bytes can be copied unchanged into every shard manifest.
    pub fn fit_backend_config(
        backend_kind: &str,
        dimension: usize,
        bits_per_dimension: usize,
        sample: &[f32],
    ) -> Result<VectorBackendConfig, VectorError> {
        match backend_kind {
            EMBEDDED_TURBOVEC => {
                embedded_turbovec::fit_config(dimension, bits_per_dimension, sample)
            }
            // The graph scores FP32 rows and fits nothing: its shared config
            // is the default build parameters.
            crate::hnsw::HNSW => crate::hnsw::HnswConfig::default().backend_config(),
            other => Err(VectorError::new(format!(
                "cannot fit unavailable vector backend {other:?}"
            ))),
        }
    }

    /// Construct an empty generation from opaque backend state.
    pub fn from_backend_config(
        dimension: usize,
        config: &VectorBackendConfig,
    ) -> Result<Self, VectorError> {
        match config.backend_kind.as_str() {
            EMBEDDED_TURBOVEC => embedded_turbovec::from_config(dimension, config),
            crate::hnsw::HNSW => crate::hnsw::from_config(dimension, config),
            other => Err(VectorError::new(format!(
                "cannot construct unavailable vector backend {other:?}"
            ))),
        }
    }

    /// Load one backend image. The manifest or configuration chooses the
    /// backend; file extensions are never used as dispatch.
    pub fn load(backend_kind: &str, path: &Path) -> Result<Self, VectorError> {
        match backend_kind {
            EMBEDDED_TURBOVEC => embedded_turbovec::load(path),
            crate::hnsw::HNSW => crate::hnsw::load(path),
            other => Err(VectorError::new(format!(
                "cannot load unavailable vector backend {other:?}"
            ))),
        }
    }

    /// Serve one backend image from its file through a memory map: the
    /// image stays on its pages and the search cache is assembled chunk by
    /// chunk as scans touch them.
    /// Scores are bit for bit those of [`Self::load`]; the index is
    /// read-only, which is what a sealed segment is.
    pub fn load_mapped(backend_kind: &str, path: &Path) -> Result<Self, VectorError> {
        match backend_kind {
            EMBEDDED_TURBOVEC => embedded_turbovec::load_mapped(path),
            crate::hnsw::HNSW => crate::hnsw::load_mapped(path),
            other => Err(VectorError::new(format!(
                "cannot load unavailable vector backend {other:?}"
            ))),
        }
    }

    /// Whether this index serves from a mapped file ([`Self::load_mapped`]).
    pub fn is_mapped(&self) -> bool {
        self.engine.is_mapped()
    }

    /// See [`VectorProvider::row_transcripts`].
    pub fn row_transcripts(
        &self,
        rows: Range<usize>,
        sink: &mut dyn FnMut(usize, &[u8]),
    ) -> Result<(), VectorError> {
        if rows.end > self.len() {
            return Err(VectorError::new(
                "row transcript range exceeds stored vectors",
            ));
        }
        checked_row_transcripts(rows, sink, |rows, sink| {
            self.engine.row_transcripts(rows, sink)
        })
    }

    /// See [`VectorProvider::row_transcript`].
    pub fn row_transcript(&self, row: usize) -> Result<Vec<u8>, VectorError> {
        let end = row
            .checked_add(1)
            .ok_or_else(|| VectorError::new("row transcript range overflow"))?;
        let mut out = None;
        self.row_transcripts(row..end, &mut |_, transcript| {
            out = Some(transcript.to_vec());
        })?;
        out.ok_or_else(|| VectorError::new(format!("row {row} produced no transcript")))
    }

    /// See [`VectorProvider::representation_materialized`].
    pub fn representation_materialized(&self) -> Option<bool> {
        self.engine.representation_materialized()
    }

    pub fn descriptor(&self) -> VectorBackendDescriptor {
        self.engine.descriptor()
    }

    pub fn backend_config(&self) -> Result<VectorBackendConfig, VectorError> {
        self.engine.backend_config()
    }

    pub fn len(&self) -> usize {
        self.engine.len()
    }

    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn is_graph_linked(&self) -> bool {
        self.engine.is_graph_linked()
    }

    /// The row ranges that hold vectors, `(base, rows)` in row order. A
    /// single image is one range over its length; the segment layout
    /// lists its parts with an image, the frozen image, and the tail, so
    /// a sealed document-only segment's rows are absent.
    pub fn vector_rows(&self) -> Vec<(usize, usize)> {
        match self.as_segmented() {
            Some(segmented) => segmented.vector_rows(),
            None => vec![(0, self.len())],
        }
    }

    pub fn dimension(&self) -> Option<usize> {
        self.engine.dimension()
    }

    pub fn dim_opt(&self) -> Option<usize> {
        self.dimension()
    }

    pub fn bits_per_dimension(&self) -> Option<usize> {
        self.descriptor()
            .bits_per_dimension
            .map(|bits| bits as usize)
    }

    pub fn add(&mut self, vectors: &[f32], dimension: usize) -> Result<(), VectorError> {
        self.engine.add(vectors, dimension)
    }

    pub fn prepare(&mut self) -> Result<(), VectorError> {
        self.engine.prepare()
    }

    pub fn write(&self, path: &Path) -> Result<(), VectorError> {
        self.engine.write(path)
    }

    pub fn try_search(
        &self,
        queries: &[f32],
        k: usize,
        options: VectorSearchOptions<'_>,
    ) -> Result<VectorSearchResults, VectorError> {
        self.engine.search(queries, k, options)
    }

    pub fn search(
        &self,
        queries: &[f32],
        k: usize,
        options: VectorSearchOptions<'_>,
    ) -> VectorSearchResults {
        self.try_search(queries, k, options)
            .unwrap_or_else(|e| panic!("vector backend search failed: {e}"))
    }

    pub fn search_with_options(
        &self,
        queries: &[f32],
        k: usize,
        options: VectorSearchOptions<'_>,
    ) -> VectorSearchResults {
        self.search(queries, k, options)
    }

    pub fn search_unfiltered(&self, queries: &[f32], k: usize) -> VectorSearchResults {
        self.search(queries, k, VectorSearchOptions::new())
    }

    pub fn try_search_with_mask(
        &self,
        queries: &[f32],
        k: usize,
        allow: Option<&[bool]>,
    ) -> Result<VectorSearchResults, VectorError> {
        let mut options = VectorSearchOptions::new();
        options.allow = allow.map(Mask::Bools);
        self.try_search(queries, k, options)
    }

    pub fn try_search_streaming_controlled<F, C>(
        &self,
        queries: &[f32],
        options: VectorSearchOptions<'_>,
        mut sink: F,
        mut control: C,
    ) -> Result<VectorStreamSummary, VectorError>
    where
        F: FnMut(&VectorStreamBatch<'_>) -> VectorStreamControl,
        C: FnMut() -> VectorStreamControl,
    {
        self.engine
            .search_streaming_controlled(queries, options, &mut sink, &mut control)
    }
}

/// Product-wide coordinate validation. It deliberately does not expose a
/// backend helper: every provider receives the same finite, bounded f32
/// contract.
pub fn first_invalid_coordinate(values: &[f32], dimension: usize) -> Option<(usize, usize, f32)> {
    if dimension == 0 {
        return None;
    }
    values
        .iter()
        .copied()
        .enumerate()
        .find_map(|(flat, value)| {
            (!value.is_finite() || value.abs() >= 1.0e16).then_some((
                flat / dimension,
                flat % dimension,
                value,
            ))
        })
}
