//! Backend identity, capabilities, opaque configuration, and the error type.

use std::fmt;

use serde::{Deserialize, Serialize};

use super::{embedded_turbovec, EMBEDDED_TURBOVEC};

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
    /// Pinned to [`FilterRegime::Enumerate`](super::FilterRegime::Enumerate), a masked search scores every
    /// allowed row exactly. A provider whose own search cannot certify
    /// completion can still serve a pass that needs every member's score.
    MaskEnumeration,
    /// Layer-0 graph walks support hop-to-hop saturation early stopping.
    PatienceEarlyStop,
    /// Layer-0 graph walks support monotone candidate pruning against an active
    /// score floor behind the ascent gate and clamp.
    FloorPruning,
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
            Self::MaskEnumeration => "mask_enumeration",
            Self::PatienceEarlyStop => "patience_early_stop",
            Self::FloorPruning => "floor_pruning",
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
