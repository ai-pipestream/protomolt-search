//! The embedded TurboVec adapter: the only code that names the native engine.

use std::ops::Range;
use std::path::Path;

use serde::{Deserialize, Serialize};
use turbovec::{CalibrationState, SearchOptions, StreamControl, TurboQuantIndex};

use super::*;

const BACKEND_VERSION: &str = "1.0.0";
const CONFIG_FORMAT: &str = "application/vnd.ai-pipestream.turbovec-config+json;version=1";

#[derive(Debug, Serialize, Deserialize)]
struct ConfigPayload {
    bits_per_dimension: usize,
    shift: Vec<f32>,
    scale: Vec<f32>,
}

pub(super) fn config(
    bits_per_dimension: usize,
    shift: &[f32],
    scale: &[f32],
) -> Result<VectorBackendConfig, VectorError> {
    let payload = ConfigPayload {
        bits_per_dimension,
        shift: shift.to_vec(),
        scale: scale.to_vec(),
    };
    Ok(VectorBackendConfig {
        backend_kind: EMBEDDED_TURBOVEC.to_string(),
        config_format: CONFIG_FORMAT.to_string(),
        payload: serde_json::to_vec(&payload).map_err(|e| VectorError::new(e.to_string()))?,
    })
}

pub(super) fn decode_config(
    config: &VectorBackendConfig,
) -> Result<EmbeddedTurbovecCalibration, VectorError> {
    if config.config_format != CONFIG_FORMAT {
        return Err(VectorError::new(format!(
            "unsupported {EMBEDDED_TURBOVEC} config format {:?}",
            config.config_format
        )));
    }
    let payload: ConfigPayload = serde_json::from_slice(&config.payload)
        .map_err(|e| VectorError::new(format!("invalid backend config: {e}")))?;
    Ok(EmbeddedTurbovecCalibration {
        bits_per_dimension: payload.bits_per_dimension,
        shift: payload.shift,
        scale: payload.scale,
    })
}

pub(super) fn create(
    dimension: usize,
    bits_per_dimension: usize,
) -> Result<VectorIndex, VectorError> {
    let index = TurboQuantIndex::new(dimension, bits_per_dimension)
        .map_err(|e| VectorError::new(e.to_string()))?;
    Ok(wrap(index))
}

pub(super) fn fit_config(
    dimension: usize,
    bits_per_dimension: usize,
    sample: &[f32],
) -> Result<VectorBackendConfig, VectorError> {
    let mut index = TurboQuantIndex::new(dimension, bits_per_dimension)
        .map_err(|e| VectorError::new(e.to_string()))?;
    index
        .calibrate(sample)
        .map_err(|e| VectorError::new(e.to_string()))?;
    wrap(index).backend_config()
}

pub(super) fn from_config(
    dimension: usize,
    config: &VectorBackendConfig,
) -> Result<VectorIndex, VectorError> {
    let payload = decode_config(config)?;
    let index = TurboQuantIndex::from_parts(
        Some(dimension),
        payload.bits_per_dimension,
        0,
        Vec::new(),
        Vec::new(),
        payload.shift,
        payload.scale,
    )
    .map_err(|e| VectorError::new(e.to_string()))?;
    Ok(wrap(index))
}

pub(super) fn load(path: &Path) -> Result<VectorIndex, VectorError> {
    TurboQuantIndex::load(path)
        .map(wrap)
        .map_err(|e| VectorError::new(format!("load {}: {e}", path.display())))
}

pub(super) fn load_mapped(path: &Path) -> Result<VectorIndex, VectorError> {
    // SAFETY: this path is reached only for immutable vector artifacts in
    // a sealed segment generation. Writers publish a different generation
    // and never modify or truncate this image while the segment is open.
    unsafe { TurboQuantIndex::load_mapped(path) }
        .map(wrap)
        .map_err(|e| VectorError::new(format!("map {}: {e}", path.display())))
}

fn wrap(index: TurboQuantIndex) -> VectorIndex {
    VectorIndex {
        engine: Box::new(EmbeddedTurboVec { index }),
    }
}

struct EmbeddedTurboVec {
    index: TurboQuantIndex,
}

impl EmbeddedTurboVec {
    fn payload(&self) -> ConfigPayload {
        let (shift, scale) = match self.index.calibration_state() {
            CalibrationState::Calibrated => (
                self.index.tqplus_shift().to_vec(),
                self.index.tqplus_scale().to_vec(),
            ),
            _ => (Vec::new(), Vec::new()),
        };
        ConfigPayload {
            bits_per_dimension: self.index.bit_width(),
            shift,
            scale,
        }
    }

    /// `allow` is the search's mask in the kernel's shape
    /// ([`Mask::bools`]), held by the caller for the search.
    fn options<'a>(
        options: VectorSearchOptions<'_>,
        allow: Option<&'a [bool]>,
    ) -> SearchOptions<'a> {
        // This provider advertises `ExhaustiveQuantized`: every top-k
        // is the whole-index scan's, bit for bit. On a bit-plane cache
        // (32,768+ rows on aarch64 or AVX-512 VBMI + VNNI hosts) turbovec
        // would otherwise search in stages, with an approximate id set
        // that also depends on the caller's thread count.
        let mut native = SearchOptions::new().with_exhaustive(true);
        if let Some(allow) = allow {
            native = native.with_mask(allow);
        }
        if let Some(score) = options.minimum_score {
            native = native.with_initial_threshold(score);
        }
        native
    }

    fn map_control(control: VectorStreamControl) -> StreamControl {
        match control {
            VectorStreamControl::Continue | VectorStreamControl::Finish { .. } => {
                StreamControl::Continue
            }
            VectorStreamControl::RaiseFloor(floor) => StreamControl::RaiseFloor(floor),
            VectorStreamControl::Stop => StreamControl::Stop,
        }
    }
}

impl VectorProvider for EmbeddedTurboVec {
    fn is_mapped(&self) -> bool {
        self.index.is_mapped()
    }

    /// Bit width, dimension, the row's bit-plane packed codes and its
    /// correction scale bits, read through `TurboQuantIndex::stored_rows`
    /// in pieces of at most [`TRANSCRIPT_BLOCK_ROWS`] rows. The engine
    /// converts one 32-row block at a time from the layout it already
    /// serves, so a loaded or mapped image is never materialized as
    /// packed rows and `packed_ready()` is unchanged. Each piece is
    /// checked against the `bits * dim / 8` row stride before it is
    /// sliced, so a layout the engine changes under this adapter refuses
    /// instead of certifying the wrong bytes.
    fn row_transcripts(
        &self,
        rows: Range<usize>,
        sink: &mut dyn FnMut(usize, &[u8]),
    ) -> Result<(), VectorError> {
        let total = self.index.len();
        if rows.end > total || rows.start > rows.end {
            return Err(VectorError::new(format!(
                "rows {rows:?} are outside the {total} stored vectors"
            )));
        }
        let dim = self
            .index
            .dim_opt()
            .ok_or_else(|| VectorError::new("vector dimension is absent"))?;
        let bits = self.index.bit_width();
        let stride = match (dim % 8, bits.checked_mul(dim)) {
            (0, Some(bits_per_row)) if bits_per_row > 0 => bits_per_row / 8,
            _ => {
                return Err(VectorError::new(format!(
                    "packed row layout is undefined for dimension {dim} at {bits} bits"
                )))
            }
        };
        let mut codes = Vec::new();
        let mut scales = Vec::new();
        let mut out = Vec::with_capacity(16 + stride);
        let mut start = rows.start;
        while start < rows.end {
            let end = rows.end.min(start + TRANSCRIPT_BLOCK_ROWS);
            self.index
                .stored_rows(start..end, &mut codes, &mut scales)
                .map_err(|e| VectorError::new(e.to_string()))?;
            if codes.len() != (end - start) * stride || scales.len() != end - start {
                return Err(VectorError::new(format!(
                    "stored rows {start}..{end} came back as {} code bytes and {} scales \
                     for a {stride}-byte row stride; the layout differs from the adapter's",
                    codes.len(),
                    scales.len()
                )));
            }
            for (i, row) in (start..end).enumerate() {
                out.clear();
                out.extend_from_slice(&(bits as u32).to_le_bytes());
                out.extend_from_slice(&(dim as u64).to_le_bytes());
                out.extend_from_slice(&codes[i * stride..(i + 1) * stride]);
                out.extend_from_slice(&scales[i].to_bits().to_le_bytes());
                sink(row, &out);
            }
            start = end;
        }
        Ok(())
    }

    fn representation_materialized(&self) -> Option<bool> {
        Some(self.index.packed_ready())
    }

    fn descriptor(&self) -> VectorBackendDescriptor {
        let config = self
            .backend_config()
            .expect("serializing the embedded backend config cannot fail");
        let dimension = (self.index.dim_opt().unwrap_or(0) as u64).to_le_bytes();
        VectorBackendDescriptor {
            backend_kind: EMBEDDED_TURBOVEC.to_string(),
            backend_version: BACKEND_VERSION.to_string(),
            dimension: self.index.dim_opt(),
            bits_per_dimension: Some(self.index.bit_width() as u32),
            metric: "inner_product".to_string(),
            score_direction: ScoreDirection::HigherIsBetter,
            scoring_fingerprint: turbo_sha256::sha256(
                [
                    EMBEDDED_TURBOVEC.as_bytes(),
                    BACKEND_VERSION.as_bytes(),
                    dimension.as_slice(),
                    config.config_format.as_bytes(),
                    config.payload.as_slice(),
                ]
                .concat()
                .as_slice(),
            )
            .to_hex(),
            quality_contract: QualityContract::ExhaustiveQuantized,
            capabilities: vec![
                VectorCapability::BatchQuery,
                VectorCapability::CandidateStream,
                VectorCapability::LiveBoundInput,
                VectorCapability::DenseMask,
                VectorCapability::CandidateRescore,
                VectorCapability::Append,
                VectorCapability::Flush,
                VectorCapability::SnapshotInstall,
                VectorCapability::RawVectorRebuild,
                VectorCapability::ExhaustiveCompletion,
            ],
        }
    }

    fn backend_config(&self) -> Result<VectorBackendConfig, VectorError> {
        Ok(VectorBackendConfig {
            backend_kind: EMBEDDED_TURBOVEC.to_string(),
            config_format: CONFIG_FORMAT.to_string(),
            payload: serde_json::to_vec(&self.payload())
                .map_err(|e| VectorError::new(e.to_string()))?,
        })
    }

    fn len(&self) -> usize {
        self.index.len()
    }

    fn dimension(&self) -> Option<usize> {
        self.index.dim_opt()
    }

    fn add(&mut self, vectors: &[f32], dimension: usize) -> Result<(), VectorError> {
        self.index
            .add_2d(vectors, dimension)
            .map_err(|e| VectorError::new(e.to_string()))
    }

    fn prepare(&mut self) -> Result<(), VectorError> {
        self.index.prepare();
        Ok(())
    }

    fn write(&self, path: &Path) -> Result<(), VectorError> {
        self.index
            .write(path)
            .map_err(|e| VectorError::new(e.to_string()))
    }

    fn search(
        &self,
        queries: &[f32],
        k: usize,
        options: VectorSearchOptions<'_>,
    ) -> Result<VectorSearchResults, VectorError> {
        let allow_mask = options.allow;
        let allow = allow_mask.map(|mask| mask.bools());
        let result = self
            .index
            .try_search_with_options(queries, k, Self::options(options, allow.as_deref()))
            .map_err(|e| VectorError::new(e.to_string()))?;
        let parts = if self.index.is_empty() || result.nq == 0 {
            PartCounts::default()
        } else if allow_mask.is_some_and(|m| !m.any_in(0, self.index.len())) {
            PartCounts {
                total: 1,
                skipped: 1,
                ..Default::default()
            }
        } else {
            PartCounts {
                total: 1,
                scanned: 1,
                ..Default::default()
            }
        };
        Ok(VectorSearchResults {
            scores: result.scores,
            slots: result.indices,
            query_count: result.nq,
            result_count: result.k,
            parts,
            regimes: FilterRegimeCounts::default(),
            tail_enumerated_rows: 0,
            patience_stopped: 0,
            floor_pruned: 0,
        })
    }

    fn search_streaming_controlled(
        &self,
        queries: &[f32],
        options: VectorSearchOptions<'_>,
        sink: &mut dyn FnMut(&VectorStreamBatch<'_>) -> VectorStreamControl,
        control: &mut dyn FnMut() -> VectorStreamControl,
    ) -> Result<VectorStreamSummary, VectorError> {
        let allow_mask = options.allow;
        let allow = allow_mask.map(|mask| mask.bools());
        let summary = self
            .index
            .try_search_streaming_controlled(
                queries,
                Self::options(options, allow.as_deref()),
                |batch| {
                    Self::map_control(sink(&VectorStreamBatch {
                        query_index: batch.query_index,
                        block_base: batch.block_base,
                        scores: batch.scores,
                        slots: batch.slots,
                    }))
                },
                || Self::map_control(control()),
            )
            .map_err(|e| VectorError::new(e.to_string()))?;
        let parts = if self.index.is_empty() || summary.nq == 0 {
            PartCounts::default()
        } else if allow_mask.is_some_and(|m| !m.any_in(0, self.index.len())) {
            PartCounts {
                total: 1,
                skipped: 1,
                ..Default::default()
            }
        } else {
            PartCounts {
                total: 1,
                scanned: u32::from(summary.completed),
                ..Default::default()
            }
        };
        Ok(VectorStreamSummary {
            query_count: summary.nq,
            emitted: summary.emitted,
            units_scanned: summary.blocks_scanned,
            completed: summary.completed,
            end_reason: if summary.completed {
                VectorStreamEndReason::Exhausted
            } else {
                VectorStreamEndReason::Abandoned
            },
            parts,
            regimes: FilterRegimeCounts::default(),
            tail_enumerated_rows: 0,
            patience_stopped: 0,
            floor_pruned: 0,
        })
    }
}
