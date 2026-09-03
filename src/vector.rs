//! Backend-neutral vector index contract.
//!
//! The search product owns document identity, filtering, hybrid semantics,
//! generation cutover, and public quality claims. A vector backend owns its
//! encoding, native score, query execution, and persisted image. Runtime code
//! outside this module talks only to [`VectorIndex`]; the concrete TurboVec
//! dependency is contained in [`embedded_turbovec`].

use std::fmt;
use std::path::Path;

use serde::{Deserialize, Serialize};

/// Stable identifier for the currently shipped embedded backend.
pub const EMBEDDED_TURBOVEC: &str = "embedded-turbovec";

/// Direction in which a backend-native score becomes more competitive.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ScoreDirection {
    HigherIsBetter,
    LowerIsBetter,
}

/// What completion of a vector query actually certifies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QualityContract {
    /// Every eligible row was evaluated under one backend-native quantized
    /// score and total order.
    ExhaustiveQuantized,
    /// A configured approximate traversal completed. It is not a corpus-wide
    /// optimum certificate.
    ConfiguredAnn,
    /// Pruning used a declared probabilistic bound.
    ProbabilisticBound,
}

/// Independently testable operations a backend may advertise.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VectorCapability {
    BatchQuery,
    CandidateStream,
    LiveBoundInput,
    DenseMask,
    CandidateRescore,
    Append,
    Flush,
    SnapshotInstall,
    RawVectorRebuild,
    ExhaustiveCompletion,
}

impl VectorCapability {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::BatchQuery => "batch_query",
            Self::CandidateStream => "candidate_stream",
            Self::LiveBoundInput => "live_bound_input",
            Self::DenseMask => "dense_mask",
            Self::CandidateRescore => "candidate_rescore",
            Self::Append => "append",
            Self::Flush => "flush",
            Self::SnapshotInstall => "snapshot_install",
            Self::RawVectorRebuild => "raw_vector_rebuild",
            Self::ExhaustiveCompletion => "exhaustive_completion",
        }
    }
}

/// Backend identity and the semantics of one opened vector generation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorBackendDescriptor {
    pub backend_kind: String,
    pub backend_version: String,
    pub dimension: Option<usize>,
    pub bits_per_dimension: Option<u32>,
    pub metric: String,
    pub score_direction: ScoreDirection,
    pub scoring_fingerprint: String,
    pub quality_contract: QualityContract,
    pub capabilities: Vec<VectorCapability>,
}

/// Opaque, backend-owned construction state carried by product manifests and
/// provisioning RPCs. The product compares the identity fields and transports
/// `payload`; only the named backend interprets its bytes.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VectorBackendConfig {
    pub backend_kind: String,
    pub config_format: String,
    pub payload: Vec<u8>,
}

/// Compatibility view of the embedded backend's former public calibration
/// fields. New product metadata transports [`VectorBackendConfig`] opaquely;
/// this view exists only while the legacy calibration RPC is served.
#[derive(Debug, Clone, PartialEq)]
pub struct LegacyCalibrationConfig {
    pub bits_per_dimension: usize,
    pub shift: Vec<f32>,
    pub scale: Vec<f32>,
}

pub fn embedded_turbovec_config(
    bits_per_dimension: usize,
    shift: &[f32],
    scale: &[f32],
) -> Result<VectorBackendConfig, VectorError> {
    embedded_turbovec::config(bits_per_dimension, shift, scale)
}

pub fn legacy_calibration_config(
    config: &VectorBackendConfig,
) -> Result<Option<LegacyCalibrationConfig>, VectorError> {
    if config.backend_kind != EMBEDDED_TURBOVEC {
        return Ok(None);
    }
    embedded_turbovec::decode_config(config).map(Some)
}

/// A backend-independent operation failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VectorError {
    message: String,
}

impl VectorError {
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
        }
    }
}

impl fmt::Display for VectorError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for VectorError {}

/// Options common to every provider search used by this product.
#[derive(Debug, Clone, Copy, Default)]
pub struct VectorSearchOptions<'a> {
    pub allow: Option<&'a [bool]>,
    pub minimum_score: Option<f32>,
}

impl<'a> VectorSearchOptions<'a> {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_allowlist(mut self, allow: &'a [bool]) -> Self {
        self.allow = Some(allow);
        self
    }

    pub fn with_mask(self, allow: &'a [bool]) -> Self {
        self.with_allowlist(allow)
    }

    pub fn with_minimum_score(mut self, score: f32) -> Self {
        self.minimum_score = Some(score);
        self
    }

    pub fn with_initial_threshold(self, score: f32) -> Self {
        self.with_minimum_score(score)
    }
}

/// Row-major native search results. Negative slots and negative-infinity
/// scores are padding when a supplied floor admits fewer than `k` rows.
#[derive(Debug, Clone, PartialEq)]
pub struct VectorSearchResults {
    pub scores: Vec<f32>,
    pub slots: Vec<i64>,
    pub query_count: usize,
    pub result_count: usize,
}

impl VectorSearchResults {
    pub fn scores_for_query(&self, query: usize) -> &[f32] {
        &self.scores[query * self.result_count..(query + 1) * self.result_count]
    }

    pub fn slots_for_query(&self, query: usize) -> &[i64] {
        &self.slots[query * self.result_count..(query + 1) * self.result_count]
    }

    /// Compatibility spelling for the old engine result type. Kept local to
    /// the generic wrapper while callers migrate to `slots_for_query`.
    pub fn indices_for_query(&self, query: usize) -> &[i64] {
        self.slots_for_query(query)
    }
}

/// One provider emission batch.
#[derive(Debug, Clone, Copy)]
pub struct VectorStreamBatch<'a> {
    pub query_index: usize,
    pub block_base: usize,
    pub scores: &'a [f32],
    pub slots: &'a [i64],
}

/// Control returned to a streaming provider.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum VectorStreamControl {
    Continue,
    RaiseFloor(f32),
    Stop,
}

/// Terminal provider-native streaming summary.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct VectorStreamSummary {
    pub query_count: usize,
    pub emitted: usize,
    pub units_scanned: usize,
    pub completed: bool,
}

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
    /// Whether the provider serves from a mapped file; owned by default.
    fn is_mapped(&self) -> bool {
        false
    }
    fn as_segmented_mut(&mut self) -> Option<&mut crate::segmented_vectors::SegmentedProvider> {
        None
    }
}

/// Sized product-side handle around an arbitrary vector engine.
pub struct VectorIndex {
    engine: Box<dyn VectorProvider>,
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
    /// Wrap a provider supplied by another module or downstream crate.
    /// The segmented provider behind this index, when it is one.
    pub fn as_segmented(&self) -> Option<&crate::segmented_vectors::SegmentedProvider> {
        self.engine.as_segmented()
    }

    pub fn as_segmented_mut(&mut self) -> Option<&mut crate::segmented_vectors::SegmentedProvider> {
        self.engine.as_segmented_mut()
    }

    pub fn from_provider(provider: impl VectorProvider + 'static) -> Self {
        Self {
            engine: Box::new(provider),
        }
    }

    /// Create the configured backend. Only the embedded adapter ships today;
    /// unknown kinds refuse instead of silently selecting it.
    pub fn create(
        backend_kind: &str,
        dimension: usize,
        bits_per_dimension: usize,
    ) -> Result<Self, VectorError> {
        match backend_kind {
            EMBEDDED_TURBOVEC => embedded_turbovec::create(dimension, bits_per_dimension),
            other => Err(VectorError::new(format!(
                "unknown vector backend {other:?}; available: {EMBEDDED_TURBOVEC}"
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
            other => Err(VectorError::new(format!(
                "cannot load unavailable vector backend {other:?}"
            ))),
        }
    }

    /// Serve one backend image from its file through a memory map
    /// (`docs/mmap-vectors.md`): the image stays on its pages and the
    /// search cache is assembled chunk by chunk as scans touch them.
    /// Scores are bit for bit those of [`Self::load`]; the index is
    /// read-only, which is what a sealed segment is.
    pub fn load_mapped(backend_kind: &str, path: &Path) -> Result<Self, VectorError> {
        match backend_kind {
            EMBEDDED_TURBOVEC => embedded_turbovec::load_mapped(path),
            other => Err(VectorError::new(format!(
                "cannot load unavailable vector backend {other:?}"
            ))),
        }
    }

    /// Whether this index serves from a mapped file ([`Self::load_mapped`]).
    pub fn is_mapped(&self) -> bool {
        self.engine.is_mapped()
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
        options.allow = allow;
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

mod embedded_turbovec {
    use super::*;
    use turbovec::{CalibrationState, SearchOptions, StreamControl, TurboQuantIndex};

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
    ) -> Result<LegacyCalibrationConfig, VectorError> {
        if config.config_format != CONFIG_FORMAT {
            return Err(VectorError::new(format!(
                "unsupported {EMBEDDED_TURBOVEC} config format {:?}",
                config.config_format
            )));
        }
        let payload: ConfigPayload = serde_json::from_slice(&config.payload)
            .map_err(|e| VectorError::new(format!("invalid backend config: {e}")))?;
        Ok(LegacyCalibrationConfig {
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
        TurboQuantIndex::load_mapped(path)
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

        fn options<'a>(options: VectorSearchOptions<'a>) -> SearchOptions<'a> {
            let mut native = SearchOptions::new();
            if let Some(allow) = options.allow {
                native = native.with_mask(allow);
            }
            if let Some(score) = options.minimum_score {
                native = native.with_initial_threshold(score);
            }
            native
        }

        fn map_control(control: VectorStreamControl) -> StreamControl {
            match control {
                VectorStreamControl::Continue => StreamControl::Continue,
                VectorStreamControl::RaiseFloor(floor) => StreamControl::RaiseFloor(floor),
                VectorStreamControl::Stop => StreamControl::Stop,
            }
        }
    }

    impl VectorProvider for EmbeddedTurboVec {
        fn is_mapped(&self) -> bool {
            self.index.is_mapped()
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
                scoring_fingerprint: crate::sha256::hex_digest(
                    [
                        EMBEDDED_TURBOVEC.as_bytes(),
                        BACKEND_VERSION.as_bytes(),
                        dimension.as_slice(),
                        config.config_format.as_bytes(),
                        config.payload.as_slice(),
                    ]
                    .concat()
                    .as_slice(),
                ),
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
            let result = self
                .index
                .try_search_with_options(queries, k, Self::options(options))
                .map_err(|e| VectorError::new(e.to_string()))?;
            Ok(VectorSearchResults {
                scores: result.scores,
                slots: result.indices,
                query_count: result.nq,
                result_count: result.k,
            })
        }

        fn search_streaming_controlled(
            &self,
            queries: &[f32],
            options: VectorSearchOptions<'_>,
            sink: &mut dyn FnMut(&VectorStreamBatch<'_>) -> VectorStreamControl,
            control: &mut dyn FnMut() -> VectorStreamControl,
        ) -> Result<VectorStreamSummary, VectorError> {
            let summary = self
                .index
                .try_search_streaming_controlled(
                    queries,
                    Self::options(options),
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
            Ok(VectorStreamSummary {
                query_count: summary.nq,
                emitted: summary.emitted,
                units_scanned: summary.blocks_scanned,
                completed: summary.completed,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct FakeProvider;

    impl VectorProvider for FakeProvider {
        fn descriptor(&self) -> VectorBackendDescriptor {
            VectorBackendDescriptor {
                backend_kind: "fake".into(),
                backend_version: "test".into(),
                dimension: Some(2),
                bits_per_dimension: None,
                metric: "inner_product".into(),
                score_direction: ScoreDirection::HigherIsBetter,
                scoring_fingerprint: "fake-score-v1".into(),
                quality_contract: QualityContract::ConfiguredAnn,
                capabilities: vec![VectorCapability::BatchQuery],
            }
        }

        fn backend_config(&self) -> Result<VectorBackendConfig, VectorError> {
            Ok(VectorBackendConfig {
                backend_kind: "fake".into(),
                config_format: "test/fake".into(),
                payload: vec![1, 2, 3],
            })
        }

        fn len(&self) -> usize {
            1
        }

        fn dimension(&self) -> Option<usize> {
            Some(2)
        }

        fn add(&mut self, _vectors: &[f32], _dimension: usize) -> Result<(), VectorError> {
            Ok(())
        }

        fn prepare(&mut self) -> Result<(), VectorError> {
            Ok(())
        }

        fn write(&self, _path: &Path) -> Result<(), VectorError> {
            Ok(())
        }

        fn search(
            &self,
            _queries: &[f32],
            k: usize,
            _options: VectorSearchOptions<'_>,
        ) -> Result<VectorSearchResults, VectorError> {
            Ok(VectorSearchResults {
                scores: vec![0.75; k],
                slots: vec![0; k],
                query_count: 1,
                result_count: k,
            })
        }

        fn search_streaming_controlled(
            &self,
            _queries: &[f32],
            _options: VectorSearchOptions<'_>,
            _sink: &mut dyn FnMut(&VectorStreamBatch<'_>) -> VectorStreamControl,
            _control: &mut dyn FnMut() -> VectorStreamControl,
        ) -> Result<VectorStreamSummary, VectorError> {
            Ok(VectorStreamSummary {
                query_count: 1,
                emitted: 0,
                units_scanned: 0,
                completed: true,
            })
        }
    }

    #[test]
    fn unknown_backend_refuses() {
        let err = VectorIndex::create("surprise", 64, 4).unwrap_err();
        assert!(err.to_string().contains("unknown vector backend"));
    }

    #[test]
    fn product_handle_accepts_a_non_turbovec_provider() {
        let index = VectorIndex::from_provider(FakeProvider);
        assert_eq!(index.descriptor().backend_kind, "fake");
        assert_eq!(
            index.descriptor().quality_contract,
            QualityContract::ConfiguredAnn
        );
        assert_eq!(
            index.search_unfiltered(&[1.0, 0.0], 2).scores,
            vec![0.75; 2]
        );
    }

    #[test]
    fn embedded_config_round_trips_without_exposing_native_types() {
        let index = VectorIndex::create(EMBEDDED_TURBOVEC, 64, 4).unwrap();
        let config = index.backend_config().unwrap();
        let rebuilt = VectorIndex::from_backend_config(64, &config).unwrap();
        assert_eq!(rebuilt.descriptor(), index.descriptor());
        assert_eq!(rebuilt.len(), 0);
    }

    #[test]
    fn descriptor_and_search_contract_are_backend_neutral() {
        let mut index = VectorIndex::create(EMBEDDED_TURBOVEC, 64, 4).unwrap();
        let vectors: Vec<f32> = (0..8 * 64)
            .map(|i| ((i % 31) as f32 - 15.0) / 31.0)
            .collect();
        index.add(&vectors, 64).unwrap();
        let descriptor = index.descriptor();
        assert_eq!(
            descriptor.quality_contract,
            QualityContract::ExhaustiveQuantized
        );
        assert!(descriptor
            .capabilities
            .contains(&VectorCapability::DenseMask));

        let result = index.search(&vectors[..64], 3, VectorSearchOptions::new());
        assert_eq!(result.query_count, 1);
        assert_eq!(result.result_count, 3);
        assert_eq!(result.slots_for_query(0).len(), 3);
    }
}
