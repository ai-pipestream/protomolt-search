use std::path::Path;

use crate::vector::{
    QualityContract, VectorBackendConfig, VectorBackendDescriptor, VectorCapability, VectorError,
    VectorIndex, VectorProvider, VectorSearchOptions, VectorSearchResults, VectorStreamBatch,
    VectorStreamControl, VectorStreamSummary,
};

pub const BACKEND_KIND: &str = "fake-ann";

pub struct FakeAnn {
    inner: VectorIndex,
}

/// Wrap an exhaustive image as a fake ANN provider.
pub fn fake_ann_index(inner: VectorIndex) -> VectorIndex {
    VectorIndex::from_provider(FakeAnn { inner })
}

/// The scoring fingerprint the fake advertises for an inner image.
pub fn fingerprint_of(inner: &VectorIndex) -> String {
    format!("{BACKEND_KIND}:{}", inner.descriptor().scoring_fingerprint)
}

impl VectorProvider for FakeAnn {
    fn descriptor(&self) -> VectorBackendDescriptor {
        let inner = self.inner.descriptor();
        VectorBackendDescriptor {
            backend_kind: BACKEND_KIND.into(),
            backend_version: "test".into(),
            dimension: inner.dimension,
            bits_per_dimension: inner.bits_per_dimension,
            metric: inner.metric,
            score_direction: inner.score_direction,
            scoring_fingerprint: fingerprint_of(&self.inner),
            quality_contract: QualityContract::ConfiguredAnn,
            capabilities: vec![VectorCapability::BatchQuery],
        }
    }

    fn backend_config(&self) -> Result<VectorBackendConfig, VectorError> {
        let mut config = self.inner.backend_config()?;
        config.backend_kind = BACKEND_KIND.into();
        Ok(config)
    }

    fn len(&self) -> usize {
        self.inner.len()
    }

    fn dimension(&self) -> Option<usize> {
        self.inner.dim_opt()
    }

    fn add(&mut self, vectors: &[f32], dimension: usize) -> Result<(), VectorError> {
        self.inner.add(vectors, dimension)
    }

    fn prepare(&mut self) -> Result<(), VectorError> {
        self.inner.prepare()
    }

    fn write(&self, path: &Path) -> Result<(), VectorError> {
        self.inner.write(path)
    }

    fn row_transcripts(
        &self,
        rows: std::ops::Range<usize>,
        sink: &mut dyn FnMut(usize, &[u8]),
    ) -> Result<(), VectorError> {
        self.inner.row_transcripts(rows, sink)
    }

    fn representation_materialized(&self) -> Option<bool> {
        self.inner.representation_materialized()
    }

    fn search(
        &self,
        queries: &[f32],
        k: usize,
        options: VectorSearchOptions<'_>,
    ) -> Result<VectorSearchResults, VectorError> {
        let mut results = self.inner.try_search(queries, k, options)?;
        results.parts.walked = results
            .parts
            .walked
            .saturating_add(std::mem::take(&mut results.parts.scanned));
        Ok(results)
    }

    fn search_streaming_controlled(
        &self,
        queries: &[f32],
        options: VectorSearchOptions<'_>,
        sink: &mut dyn FnMut(&VectorStreamBatch<'_>) -> VectorStreamControl,
        control: &mut dyn FnMut() -> VectorStreamControl,
    ) -> Result<VectorStreamSummary, VectorError> {
        let mut summary = self
            .inner
            .try_search_streaming_controlled(queries, options, sink, control)?;
        summary.parts.walked = summary
            .parts
            .walked
            .saturating_add(std::mem::take(&mut summary.parts.scanned));
        Ok(summary)
    }
}
