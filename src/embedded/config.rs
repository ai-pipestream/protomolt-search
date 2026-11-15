//! Embedded runtime configuration and its startup error.

use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;

use tonic::Status;

use crate::analyzer::NATIVE_ANALYSIS_BACKEND;
use crate::bm25::Bm25Params;
use crate::coordinator::{FanoutLimits, DEFAULT_MAX_K, DEFAULT_MAX_RERANK_BYTES};
use crate::node::NodeConfig;
use crate::quality::DenseQualityProfile;

/// One private shard in an embedded runtime.
#[derive(Clone, Debug)]
pub struct EmbeddedShardConfig {
    /// The ordinary node configuration. Embedded startup accepts only the
    /// native analyzer (or no value, which is resolved to native).
    pub node: NodeConfig,
    /// Explicit recovery escape hatch matching the server flag. The default
    /// refuses an unfinished BM25 bulk build.
    pub allow_missing_bm25: bool,
}

impl EmbeddedShardConfig {
    /// A purely in-memory shard. [`EmbeddedSearch::flush_all`] reports it as
    /// unwritten, matching `NodeService.Flush`.
    pub fn in_memory(slot_offset: u64) -> Self {
        let node = NodeConfig {
            slot_offset,
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            ..NodeConfig::default()
        };
        Self {
            node,
            allow_missing_bm25: false,
        }
    }

    /// A persisted shard with WAL enabled. Opening an absent path creates an
    /// empty generation on first ingest/flush; opening an existing path loads
    /// its active snapshot and sidecars.
    pub fn persistent(path: impl Into<PathBuf>, slot_offset: u64) -> Self {
        let node = NodeConfig {
            slot_offset,
            index_path: Some(path.into()),
            analysis_addr: Some(NATIVE_ANALYSIS_BACKEND.to_string()),
            wal: true,
            ..NodeConfig::default()
        };
        Self {
            node,
            allow_missing_bm25: false,
        }
    }
}

/// Runtime-wide search settings. Shard field tables in
/// [`EmbeddedShardConfig::node`] are the schema consumed by ordinary and
/// descriptor-mapped ingest.
#[derive(Clone, Debug)]
pub struct EmbeddedSearchConfig {
    pub shards: Vec<EmbeddedShardConfig>,
    /// One collection-wide authority, independent of the physical shard paths.
    /// Absent disables logical document acceptance.
    pub document_catalog: Option<EmbeddedDocumentCatalogConfig>,
    pub bm25_params: Bm25Params,
    pub stream_search: bool,
    pub bm25_stream: bool,
    pub max_k: u32,
    pub max_rerank_bytes: u64,
    pub fanout_limits: FanoutLimits,
    pub phrase_index: Option<Arc<crate::phrases::PhraseIndex>>,
    pub dense_quality_profile: Option<DenseQualityProfile>,
    pub topology_generation: u64,
}

impl EmbeddedSearchConfig {
    pub fn new(shards: Vec<EmbeddedShardConfig>) -> Self {
        Self {
            shards,
            document_catalog: None,
            bm25_params: Bm25Params::default(),
            stream_search: true,
            bm25_stream: true,
            max_k: DEFAULT_MAX_K,
            max_rerank_bytes: DEFAULT_MAX_RERANK_BYTES,
            fanout_limits: FanoutLimits::default(),
            phrase_index: None,
            dense_quality_profile: None,
            topology_generation: 0,
        }
    }

    pub fn single(shard: EmbeddedShardConfig) -> Self {
        Self::new(vec![shard])
    }
}

/// Local source/version storage. A persistent path must have an existing parent
/// directory and must remain stable when shard layouts or index generations change.
#[derive(Clone, Debug)]
pub struct EmbeddedDocumentCatalogConfig {
    pub collection: String,
    /// None explicitly selects volatile storage; receipts report durable=false.
    pub path: Option<PathBuf>,
}

/// Startup/configuration failures are separate from RPC failures so a host
/// bridge can distinguish an unusable local generation from a bad request.
#[derive(Debug)]
pub enum EmbeddedError {
    InvalidConfig(String),
    ExistingData(PathBuf),
    OpenShard { shard: usize, message: String },
    Rpc(Status),
}

impl fmt::Display for EmbeddedError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => formatter.write_str(message),
            Self::ExistingData(path) => write!(
                formatter,
                "refusing to create an embedded shard over existing data at {}",
                path.display()
            ),
            Self::OpenShard { shard, message } => {
                write!(formatter, "open embedded shard {shard}: {message}")
            }
            Self::Rpc(status) => write!(formatter, "embedded search RPC: {status}"),
        }
    }
}

impl std::error::Error for EmbeddedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Rpc(status) => Some(status),
            _ => None,
        }
    }
}

impl From<Status> for EmbeddedError {
    fn from(status: Status) -> Self {
        Self::Rpc(status)
    }
}
