//! The graph's build parameters: a protobuf message (`HnswConfig` in
//! `proto/ai/protomolt/search/v1/search.proto`) carried as the payload of a
//! [`VectorBackendConfig`]. Every field is part of the scoring fingerprint:
//! a dense execution policy measured at one setting does not qualify another.
//!
//! The query beam is deliberately not here. It comes from the request's
//! candidate depth (`docs/hnsw.md`, "The beam").

use prost::Message;

use crate::vector::{VectorBackendConfig, VectorError};

use super::HNSW;

/// The payload format of the backend config: the `HnswConfig` message.
pub const CONFIG_FORMAT: &str = "application/vnd.ai-pipestream.hnsw-config+protobuf;version=1";

/// Smallest accepted `M`: below four neighbors the upper layers stop
/// being a navigable skeleton.
pub const MIN_M: u32 = 4;
/// Largest accepted `M`: layer 0 holds `2M` slots per row, 2 KiB at 256.
pub const MAX_M: u32 = 256;
/// Largest accepted construction beam.
pub const MAX_EF_CONSTRUCTION: u32 = 1 << 16;

/// How a bulk build inserts its rows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BuildMode {
    /// One thread inserts the rows in row order. Two builds of the same rows
    /// with the same config and kernel produce the same image bit for bit.
    FixedOrder,
    /// Every available thread inserts rows concurrently under per-node
    /// locks; the graph depends on scheduling.
    Parallel,
}

impl BuildMode {
    fn to_proto(self) -> crate::pb::HnswBuildMode {
        match self {
            Self::FixedOrder => crate::pb::HnswBuildMode::FixedOrder,
            Self::Parallel => crate::pb::HnswBuildMode::Parallel,
        }
    }

    fn from_proto(mode: i32) -> Result<Self, VectorError> {
        match crate::pb::HnswBuildMode::try_from(mode) {
            Ok(crate::pb::HnswBuildMode::FixedOrder) => Ok(Self::FixedOrder),
            Ok(crate::pb::HnswBuildMode::Parallel) => Ok(Self::Parallel),
            Ok(crate::pb::HnswBuildMode::Unspecified) => Err(VectorError::new(format!(
                "{HNSW} config leaves build_mode unspecified; name fixed_order or parallel"
            ))),
            Err(_) => Err(VectorError::new(format!(
                "{HNSW} config names unknown build_mode {mode}"
            ))),
        }
    }

    /// The wire value, which the image header and fingerprint record.
    pub fn as_u32(self) -> u32 {
        self.to_proto() as u32
    }

    /// Inverse of [`Self::as_u32`].
    pub fn from_u32(value: u32) -> Result<Self, VectorError> {
        let mode = i32::try_from(value)
            .map_err(|_| VectorError::new(format!("{HNSW} build_mode {value} is out of range")))?;
        Self::from_proto(mode)
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::FixedOrder => "fixed_order",
            Self::Parallel => "parallel",
        }
    }
}

/// Optional graph-locality row permutation applied at seal and compaction
/// (`#98`, `docs/hnsw.md`).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum LocalityOrder {
    /// Preserve ingest or caller-assigned row order (`0 B/row` overhead,
    /// bit-identical to unreordered layout).
    #[default]
    None,
    /// Symmetrized `O(V + E)` Reverse Cuthill–McKee pass over layer-0 links.
    Rcm,
    /// Bounded recursive graph bisection over layer-0 links down to page-sized
    /// leaves.
    RecursiveBisection,
}

impl LocalityOrder {
    fn to_proto(self) -> crate::pb::HnswLocalityOrder {
        match self {
            Self::None => crate::pb::HnswLocalityOrder::None,
            Self::Rcm => crate::pb::HnswLocalityOrder::Rcm,
            Self::RecursiveBisection => crate::pb::HnswLocalityOrder::RecursiveBisection,
        }
    }

    fn from_proto(order: i32) -> Result<Self, VectorError> {
        match crate::pb::HnswLocalityOrder::try_from(order) {
            Ok(crate::pb::HnswLocalityOrder::None) => Ok(Self::None),
            Ok(crate::pb::HnswLocalityOrder::Rcm) => Ok(Self::Rcm),
            Ok(crate::pb::HnswLocalityOrder::RecursiveBisection) => Ok(Self::RecursiveBisection),
            Err(_) => Err(VectorError::new(format!(
                "{HNSW} config names unknown locality_order {order}"
            ))),
        }
    }

    /// Compact `u8` tag stored in the `PMHNSW01` header.
    pub fn as_u8(self) -> u8 {
        self.to_proto() as u8
    }

    /// Inverse of [`Self::as_u8`].
    pub fn from_u8(value: u8) -> Result<Self, VectorError> {
        Self::from_proto(i32::from(value))
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Rcm => "rcm",
            Self::RecursiveBisection => "recursive_bisection",
        }
    }
}

/// Validated build parameters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HnswConfig {
    /// Neighbors per node in the upper layers; layer 0 keeps `2M`.
    pub m: u32,
    /// The construction beam: how many candidates an insertion considers
    /// per layer before the neighbor heuristic selects from them.
    pub ef_construction: u32,
    pub build_mode: BuildMode,
    /// Seeds the level draw of every node (and nothing else).
    pub seed: u64,
    /// Optional graph-locality node ordering applied at seal and compaction.
    pub locality_order: LocalityOrder,
}

impl Default for HnswConfig {
    fn default() -> Self {
        Self {
            m: 16,
            ef_construction: 200,
            build_mode: BuildMode::Parallel,
            seed: 0x4857_4E53_5700_0001,
            locality_order: LocalityOrder::None,
        }
    }
}

impl HnswConfig {
    /// Refuse parameters the graph cannot honor, by name.
    pub fn validate(&self) -> Result<(), VectorError> {
        if !(MIN_M..=MAX_M).contains(&self.m) {
            return Err(VectorError::new(format!(
                "{HNSW} config m={} is outside {MIN_M}..={MAX_M}",
                self.m
            )));
        }
        if self.ef_construction < self.m {
            return Err(VectorError::new(format!(
                "{HNSW} config ef_construction={} is below m={}: an insertion beam narrower than \
                 the neighbor list cannot fill it",
                self.ef_construction, self.m
            )));
        }
        if self.ef_construction > MAX_EF_CONSTRUCTION {
            return Err(VectorError::new(format!(
                "{HNSW} config ef_construction={} exceeds {MAX_EF_CONSTRUCTION}",
                self.ef_construction
            )));
        }
        Ok(())
    }

    /// The config as the protobuf message.
    pub fn to_proto(&self) -> crate::pb::HnswConfig {
        crate::pb::HnswConfig {
            m: self.m,
            ef_construction: self.ef_construction,
            build_mode: self.build_mode.to_proto() as i32,
            seed: self.seed,
            locality_order: self.locality_order.to_proto() as i32,
        }
    }

    /// A validated config from the message.
    pub fn from_proto(message: &crate::pb::HnswConfig) -> Result<Self, VectorError> {
        let config = Self {
            m: message.m,
            ef_construction: message.ef_construction,
            build_mode: BuildMode::from_proto(message.build_mode)?,
            seed: message.seed,
            locality_order: LocalityOrder::from_proto(message.locality_order)?,
        };
        config.validate()?;
        Ok(config)
    }

    /// The canonical payload bytes: prost encodes known fields in field
    /// order and omits defaults, so equal configs encode equally and the
    /// fingerprint can hash the bytes.
    pub fn encode(&self) -> Vec<u8> {
        self.to_proto().encode_to_vec()
    }

    /// Decode and validate a payload. Unknown fields are dropped, so a
    /// payload from a newer writer re-encodes to this version's canonical
    /// form; a payload that is not the message refuses.
    pub fn decode(payload: &[u8]) -> Result<Self, VectorError> {
        let message = crate::pb::HnswConfig::decode(payload).map_err(|e| {
            VectorError::new(format!("{HNSW} config payload is not HnswConfig: {e}"))
        })?;
        Self::from_proto(&message)
    }

    pub fn backend_config(&self) -> Result<VectorBackendConfig, VectorError> {
        self.validate()?;
        Ok(VectorBackendConfig {
            backend_kind: HNSW.to_string(),
            config_format: CONFIG_FORMAT.to_string(),
            payload: self.encode(),
        })
    }

    pub fn from_backend_config(config: &VectorBackendConfig) -> Result<Self, VectorError> {
        if config.backend_kind != HNSW {
            return Err(VectorError::new(format!(
                "backend config names {:?}, not {HNSW}",
                config.backend_kind
            )));
        }
        if config.config_format != CONFIG_FORMAT {
            return Err(VectorError::new(format!(
                "unsupported {HNSW} config format {:?}; this build reads {CONFIG_FORMAT}",
                config.config_format
            )));
        }
        Self::decode(&config.payload)
    }
}
