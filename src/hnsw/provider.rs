//! [`HnswIndex`] as a [`VectorProvider`].

use std::ops::Range;
use std::path::Path;

use crate::vector::{
    first_invalid_coordinate, FilterRegimeCounts, Mask, QualityContract, ScoreDirection,
    VectorBackendConfig, VectorBackendDescriptor, VectorCapability, VectorError, VectorProvider,
    VectorSearchOptions, VectorSearchResults, VectorStreamBatch, VectorStreamControl,
    VectorStreamSummary,
};

use super::graph::{self, with_scratch};
use super::{Backing, HnswIndex, CONFIG_FORMAT, EF_FLOOR, HNSW};

const BACKEND_VERSION: &str = "1.0.0";

impl VectorProvider for HnswIndex {
    fn descriptor(&self) -> VectorBackendDescriptor {
        let dimension = (self.dim as u64).to_le_bytes();
        let payload = self.config.encode();
        VectorBackendDescriptor {
            backend_kind: HNSW.to_string(),
            backend_version: BACKEND_VERSION.to_string(),
            dimension: Some(self.dim),
            bits_per_dimension: Some(32),
            metric: "inner_product".to_string(),
            score_direction: ScoreDirection::HigherIsBetter,
            scoring_fingerprint: turbo_sha256::sha256(
                [
                    HNSW.as_bytes(),
                    BACKEND_VERSION.as_bytes(),
                    dimension.as_slice(),
                    CONFIG_FORMAT.as_bytes(),
                    payload.as_slice(),
                ]
                .concat()
                .as_slice(),
            )
            .to_hex(),
            quality_contract: QualityContract::ConfiguredAnn,
            capabilities: vec![
                VectorCapability::BatchQuery,
                VectorCapability::CandidateStream,
                VectorCapability::LiveBoundInput,
                VectorCapability::DenseMask,
                VectorCapability::Append,
                VectorCapability::Flush,
                VectorCapability::SnapshotInstall,
                VectorCapability::RawVectorRebuild,
                VectorCapability::MaskEnumeration,
                VectorCapability::PatienceEarlyStop,
                VectorCapability::FloorPruning,
            ],
        }
    }

    fn backend_config(&self) -> Result<VectorBackendConfig, VectorError> {
        self.config.backend_config()
    }

    fn len(&self) -> usize {
        match &self.backing {
            Backing::Heap(store) => store.n(),
            Backing::Mapped(mapped) => mapped.n(),
        }
    }

    fn dimension(&self) -> Option<usize> {
        Some(self.dim)
    }

    fn is_mapped(&self) -> bool {
        matches!(self.backing, Backing::Mapped(_))
    }

    fn is_graph_linked(&self) -> bool {
        match &self.backing {
            Backing::Heap(store) => store.is_linked(),
            Backing::Mapped(mapped) => mapped.header().entry != graph::NO_NODE,
        }
    }

    fn as_hnsw(&self) -> Option<&HnswIndex> {
        Some(self)
    }

    /// Append rows and their reciprocal norms to the unlinked heap store
    /// without building graph edges. Queries over an unlinked store answer
    /// by exact SIMD enumeration until [`Self::prepare`] or [`Self::write`]
    /// builds the graph in bulk at seal time.
    fn add(&mut self, vectors: &[f32], dimension: usize) -> Result<(), VectorError> {
        if dimension != self.dim {
            return Err(VectorError::new(format!(
                "vectors have dimension {dimension}, the {HNSW} index expects {}",
                self.dim
            )));
        }
        if !vectors.len().is_multiple_of(self.dim) {
            return Err(VectorError::new(format!(
                "{} values do not form rows of dimension {}",
                vectors.len(),
                self.dim
            )));
        }
        if let Some((row, coord, value)) = first_invalid_coordinate(vectors, self.dim) {
            return Err(VectorError::new(format!(
                "vector {row} coordinate {coord} is invalid: {value}"
            )));
        }
        let store = match &mut self.backing {
            Backing::Heap(store) => store,
            Backing::Mapped(_) => {
                return Err(VectorError::new(format!(
                    "{HNSW} image is served from a map and is sealed; rows cannot be added"
                )))
            }
        };
        store.append_unlinked(vectors)
    }

    /// Build the graph in bulk when the heap store is unlinked and holds at
    /// least [`MIN_GRAPH_ROWS`](super::MIN_GRAPH_ROWS) rows; smaller stores
    /// remain unlinked (`entry = NO_NODE`) and answer by exact SIMD
    /// enumeration.
    fn prepare(&mut self) -> Result<(), VectorError> {
        self.seal_in_place()
    }

    fn write(&self, path: &Path) -> Result<(), VectorError> {
        self.write_image(path)
    }

    /// Top `k` per query at a beam of `max(k, candidate_depth, EF_FLOOR)`.
    /// Disallowed rows route but are never returned; `minimum_score`
    /// filters the results; short results are padded with `-1` and
    /// negative infinity.
    fn search(
        &self,
        queries: &[f32],
        k: usize,
        options: VectorSearchOptions<'_>,
    ) -> Result<VectorSearchResults, VectorError> {
        let query_count = self.check_queries(queries)?;
        let ef = k.max(EF_FLOOR).max(options.candidate_depth.unwrap_or(0));
        let node_bits = self.node_allow_bits(options.allow)?;
        let effective_options = match &node_bits {
            Some(bits) => VectorSearchOptions {
                allow: Some(Mask::Bits(bits)),
                ..options
            },
            None => options,
        };
        let plan = self.plan(ef, &effective_options)?;
        let floor = effective_options.minimum_score.unwrap_or(f32::NEG_INFINITY);
        let mut scores = Vec::with_capacity(query_count * k);
        let mut slots = Vec::with_capacity(query_count * k);
        let mut regimes = FilterRegimeCounts::default();
        let mut tail_enumerated_rows = 0u64;
        let mut patience_stopped = 0u32;
        let mut floor_pruned = 0u32;
        let mut enumerated_part = false;
        with_scratch(|scratch| {
            for query in queries.chunks_exact(self.dim) {
                let (hits, answer) = self.answer(query, k, ef, floor, &plan, scratch);
                enumerated_part |= answer.enumerated_part;
                tail_enumerated_rows =
                    tail_enumerated_rows.saturating_add(answer.tail_enumerated_rows);
                if answer.patience_stopped {
                    patience_stopped = patience_stopped.saturating_add(1);
                }
                if answer.floor_pruned {
                    floor_pruned = floor_pruned.saturating_add(1);
                }
                if answer.budget_handoff {
                    regimes.count_budget_handoff();
                } else if let Some(regime) = answer.regime {
                    regimes.count(regime);
                }
                for (node, score) in &hits {
                    scores.push(*score);
                    slots.push(i64::from(*node));
                }
                for _ in hits.len()..k {
                    scores.push(f32::NEG_INFINITY);
                    slots.push(-1);
                }
            }
        });
        let parts = self.part_summary(query_count, true, &plan, enumerated_part);
        Ok(VectorSearchResults {
            scores,
            slots,
            query_count,
            result_count: k,
            parts,
            regimes,
            tail_enumerated_rows,
            patience_stopped,
            floor_pruned,
        })
    }

    /// One walk per query at the request's candidate depth, emitting every
    /// allowed row the walk scores exactly at or above the floor, in
    /// batches, as it proceeds. `control` is polled after every expansion;
    /// `Stop` from either callback ends the stream uncompleted, and a floor
    /// raised by either gates the rest of the query and every later one.
    /// A filtered query follows the regimes of `plan`: enumerate emits
    /// every allowed row, and a filter-aware walk that gives the query up
    /// is finished by enumerating the allowed rows it did not reach.
    /// `completed` means every query finished, never that every row was
    /// evaluated. Without a depth the provider refuses: the beam is not
    /// configuration, and a graph walk cannot guess how far to go.
    fn search_streaming_controlled(
        &self,
        queries: &[f32],
        options: VectorSearchOptions<'_>,
        sink: &mut dyn FnMut(&VectorStreamBatch<'_>) -> VectorStreamControl,
        control: &mut dyn FnMut() -> VectorStreamControl,
    ) -> Result<VectorStreamSummary, VectorError> {
        self.stream_search(queries, options, sink, control)
    }

    /// A row's transcript is the bytes its score is computed from: the
    /// dimension, then the stored FP32 row little-endian. Nothing else in
    /// the image (norms, links) reaches a reported score.
    fn row_transcripts(
        &self,
        rows: Range<usize>,
        sink: &mut dyn FnMut(usize, &[u8]),
    ) -> Result<(), VectorError> {
        if rows.start > rows.end || rows.end > self.len() {
            return Err(VectorError::new(format!(
                "rows {rows:?} are outside the {} stored vectors",
                self.len()
            )));
        }
        let mut out = Vec::with_capacity(12 + 4 * self.dim);
        for row in rows {
            out.clear();
            out.extend_from_slice(b"HNSW");
            out.extend_from_slice(&(self.dim as u64).to_le_bytes());
            self.row_bytes_into(row, &mut out);
            sink(row, &out);
        }
        Ok(())
    }
}
