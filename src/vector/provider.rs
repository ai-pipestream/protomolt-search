//! The provider trait every vector implementation adapts to.

use std::ops::Range;
use std::path::Path;

use super::{
    VectorBackendConfig, VectorBackendDescriptor, VectorError, VectorSearchOptions,
    VectorSearchResults, VectorStreamBatch, VectorStreamControl, VectorStreamSummary,
};

/// Compile-time adapter contract for a vector implementation. Providers own
/// score semantics and persistence; product code owns ids, filtering, fusion,
/// distribution, and generations.
pub trait VectorProvider: Send + Sync {
    fn descriptor(&self) -> VectorBackendDescriptor;
    fn backend_config(&self) -> Result<VectorBackendConfig, VectorError>;
    fn len(&self) -> usize;
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
    fn dimension(&self) -> Option<usize>;
    fn add(&mut self, vectors: &[f32], dimension: usize) -> Result<(), VectorError>;
    fn prepare(&mut self) -> Result<(), VectorError>;
    fn write(&self, path: &Path) -> Result<(), VectorError>;
    fn search(
        &self,
        queries: &[f32],
        k: usize,
        options: VectorSearchOptions<'_>,
    ) -> Result<VectorSearchResults, VectorError>;
    fn search_streaming_controlled(
        &self,
        queries: &[f32],
        options: VectorSearchOptions<'_>,
        sink: &mut dyn FnMut(&VectorStreamBatch<'_>) -> VectorStreamControl,
        control: &mut dyn FnMut() -> VectorStreamControl,
    ) -> Result<VectorStreamSummary, VectorError>;
    /// The segmented provider behind this index, when it is one
    /// (`src/segmented_vectors.rs`): the node seals its tail through it.
    fn as_segmented(&self) -> Option<&crate::segmented_vectors::SegmentedProvider> {
        None
    }
    /// The HNSW provider behind this index, when it is one (`src/hnsw/mod.rs`).
    fn as_hnsw(&self) -> Option<&crate::hnsw::HnswIndex> {
        None
    }
    /// Whether the provider serves from a mapped file; owned by default.
    fn is_mapped(&self) -> bool {
        false
    }
    /// Whether this image has its navigation structure built (true for scan
    /// providers and linked HNSW segments; false for an unlinked HNSW tail or
    /// tiny segment below `MIN_GRAPH_ROWS`). Segmented search runs linked
    /// parts first so their `k`-th score prunes unlinked parts.
    fn is_graph_linked(&self) -> bool {
        true
    }
    fn as_segmented_mut(&mut self) -> Option<&mut crate::segmented_vectors::SegmentedProvider> {
        None
    }
    /// The provider's stored encoding of each row in `rows`, delivered to
    /// `sink` as `(row, transcript)` in row order: the bytes its scorer
    /// reads for that row and the per-row scoring metadata kept beside
    /// them, in a fixed layout that does not depend on the row's physical
    /// position. A rewrite proof compares this by document identity
    /// (`src/segments/rewrite_proof.rs`), so equal FP32 source rows cannot
    /// stand in for it. The provider must serve the range in bounded pieces
    /// without materializing its whole image, and must leave any lazily
    /// built representation state as it found it. A provider that cannot
    /// expose its representation refuses here, and the proof refuses with
    /// it: a certificate never covers a dense image it could not read.
    fn row_transcripts(
        &self,
        _rows: Range<usize>,
        _sink: &mut dyn FnMut(usize, &[u8]),
    ) -> Result<(), VectorError> {
        Err(VectorError::new(format!(
            "vector backend {:?} does not expose its stored row encoding; a rewrite proof \
             cannot certify its dense image",
            self.descriptor().backend_kind
        )))
    }

    /// One row's transcript: [`Self::row_transcripts`] over a single row.
    fn row_transcript(&self, row: usize) -> Result<Vec<u8>, VectorError> {
        let end = row
            .checked_add(1)
            .ok_or_else(|| VectorError::new("row transcript range overflow"))?;
        let mut out = None;
        checked_row_transcripts(
            row..end,
            &mut |_, transcript| {
                out = Some(transcript.to_vec());
            },
            |rows, sink| self.row_transcripts(rows, sink),
        )?;
        out.ok_or_else(|| VectorError::new(format!("row {row} produced no transcript")))
    }

    /// Whether the provider currently holds a materialized copy of its
    /// stored rows beyond the image it serves from — for the embedded
    /// engine, its packed-row cache. `None` when the provider has no such
    /// state. A rewrite proof over a mapped image must leave this `false`.
    fn representation_materialized(&self) -> Option<bool> {
        None
    }
}

/// Rows a provider is asked for at once by the rewrite proof: bounds the
/// transcript buffer at `rows * (16 + bits * dim / 8)` bytes, 256 KiB at
/// 384 dimensions and 4 bits, 8 MiB at the engine's 16,384-dimension ceiling.
pub const TRANSCRIPT_BLOCK_ROWS: usize = 1024;

// Validate the complete transcript stream before a proof can succeed. Check
// each index before forwarding: an invalid provider must never index the
// caller's bitmap/digest array, or omit a row and leave its digest zeroed.
pub(crate) fn checked_row_transcripts(
    rows: Range<usize>,
    sink: &mut dyn FnMut(usize, &[u8]),
    emit: impl FnOnce(Range<usize>, &mut dyn FnMut(usize, &[u8])) -> Result<(), VectorError>,
) -> Result<(), VectorError> {
    if rows.start > rows.end {
        return Err(VectorError::new("row transcript range is reversed"));
    }
    let mut next = rows.start;
    let mut invalid = false;
    emit(rows.clone(), &mut |row, transcript| {
        if invalid {
            return;
        }
        if row != next || row >= rows.end {
            invalid = true;
            return;
        }
        next += 1; // row < end, so this cannot overflow even at usize::MAX.
        sink(row, transcript);
    })?;
    if invalid || next != rows.end {
        return Err(VectorError::new(
            "row transcript stream must contain every requested row exactly once in order",
        ));
    }
    Ok(())
}
