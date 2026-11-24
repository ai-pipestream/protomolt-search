//! Pure-Rust lexical and one-pass NLP analysis providers backed by
//! `protomolt-analyzer` (`native`), in-process `turbo-client` (`turbo`), and
//! pooled `turbo-kserve` gRPC offload (`turbo.result.v1.AnalysisService`).
//!
//! Search configures every backend to persist original-text UTF-8 byte offsets
//! (`ViewId::SOURCE`), while preserving bidirectional UTF-8 / UTF-16 / Unicode
//! code-point span projection through `turbo-client`.

#[path = "../analyzer_wire.rs"]
pub(crate) mod wire;

mod batch;
mod embed;
mod lattice;
pub(crate) mod native;
#[cfg(feature = "net")]
mod pool;
mod probe;
#[cfg(feature = "net")]
mod projection;
#[cfg(feature = "net")]
mod remote;
mod spec;
mod spec_convert;
mod status;
mod stream;
mod stream_open;
mod turbo;
mod unary;

#[cfg(all(test, feature = "net"))]
mod pool_tests;
#[cfg(test)]
mod provider_tests;
#[cfg(test)]
mod spec_tests;
#[cfg(test)]
mod wire_tests;

use std::time::Duration;

use tonic::Status;

pub use batch::{analyze_batch, analyze_batch_streams};
pub use embed::{embed_text, embed_text_with_options, embed_texts_with_options};
pub use native::{analyze_document_native, analyze_document_native_dual};
#[cfg(feature = "net")]
pub use pool::{
    connect_verified_turbo_client, evict_unhealthy_turbo_channel, remote_client_for,
    remote_endpoint_config_for, shared_channel, turbo_pool_generation,
};
pub use probe::judge_query_embedding;
#[cfg(feature = "net")]
pub use projection::analyzed_from_search_projection;
pub use spec::{
    analysis_fingerprint, analyzer_by_name, body_spec, cased_body_spec, cased_twin_spec,
    normalize_prefix, turbo_handshake, uax29_body_spec, validate_dual_cased_spec, ANALYZER_NAMES,
    CHAR_FILTER_ACCENT_FOLD, CHAR_FILTER_CASE_FOLD, CHAR_FILTER_DEHYPHENATE,
    CHAR_FILTER_FULL_CASE_FOLD, CHAR_FILTER_HTML_ENTITY, CHAR_FILTER_HTML_STRIP,
    CHAR_FILTER_MOJIBAKE, CHAR_FILTER_NFC, CHAR_FILTER_NFKC, CHAR_FILTER_STRIP_INVISIBLE,
    CHAR_FILTER_WHITESPACE, SOURCE_NORMALIZED_STEMS, SOURCE_STEMS, SOURCE_TOKENS, STEMMER_NONE,
    STEMMER_PORTER, TERM_VECTOR_MODE_FULL, TERM_VECTOR_MODE_SCORING_ONLY, TOKENIZER_SIMPLE,
    TOKENIZER_UAX29, TOKENIZER_WHITESPACE,
};
pub use spec_convert::turbo_field_config;
pub(crate) use spec_convert::validate_native_spec;
#[cfg(feature = "net")]
pub(crate) use status::{inspect_remote_status, remote_client_err_to_status};
pub use stream::{AnalyzeStream, AnalyzeSubmit};
pub use turbo::analyze_document_turbo;
pub use unary::{analyze_document, analyze_document_with_options};

/// Matches `turbo-kserve`'s default per-document text byte cap.
pub const MAX_TEXT_BYTES: usize = 1024 * 1024;

/// Configuration value selecting the minimal in-process `protomolt-analyzer` backend.
pub const NATIVE_ANALYSIS_BACKEND: &str = "native";

/// Configuration value selecting the full in-process pure-Rust `turbo-client`
/// lexical lattice + one-pass NLP backend (zero network, zero JVM).
pub const TURBO_ANALYSIS_BACKEND: &str = "turbo";

/// Default embedding model name queried by [`embed_text`] when the endpoint URI
/// does not specify `?model=<name>` and `TURBO_EMBED_MODEL` is unset.
pub const DEFAULT_EMBED_MODEL: &str = "embed";

/// Resolved analysis provider.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AnalysisBackend {
    /// Minimal in-process `protomolt-analyzer` lexical provider.
    Native,
    /// Full in-process `turbo-client` lexical lattice + one-pass NLP provider.
    Turbo,
    /// Pooled remote `turbo-kserve` (or `MockAnalysis`) gRPC endpoint.
    Sidecar(String),
}

impl AnalysisBackend {
    pub fn parse(value: &str) -> Result<Self, Status> {
        match value.trim() {
            "native" | "native://" => Ok(Self::Native),
            "turbo" | "turbo://" => Ok(Self::Turbo),
            "" => Err(Status::invalid_argument("analysis backend is empty")),
            address => Ok(Self::Sidecar(address.to_string())),
        }
    }
}

/// Optional per-call remote scheduling, auth, TLS, deadline, and cryptographic
/// handshake overrides propagated to `turbo-kserve`.
#[derive(Clone, Debug, Default)]
pub struct RemoteCallOptions {
    /// Explicit bearer token (`authorization: Bearer <token>`).
    pub bearer_token: Option<String>,
    /// Interactive vs bulk scheduling lane (`x-turbo-priority`).
    pub interactive: bool,
    /// Optional per-RPC deadline translated into `grpc-timeout`.
    pub timeout: Option<Duration>,
    /// Optional explicit mTLS / server CA configuration (falls back to
    /// [`crate::security::process_client_tls`]).
    pub tls: Option<crate::security::ClientTls>,
    /// Optional expected model/analysis handshake verified against remote
    /// `ModelMetadata` before inference.
    pub expected_handshake: Option<turbo_client::IndexGenerationHandshake>,
}

#[cfg(feature = "net")]
impl RemoteCallOptions {
    /// The scheduling lane these options select.
    fn priority(&self) -> turbo_client::RemotePriority {
        if self.interactive {
            turbo_client::RemotePriority::Interactive
        } else {
            turbo_client::RemotePriority::Bulk
        }
    }
}

/// Optional layers an analysis session requests beyond primary term identity.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct SessionLayers {
    /// Noise and artifact scanners.
    pub quality: bool,
    /// Geocoding layer.
    pub geography: bool,
    /// Named-entity mentions for materialization into a map column.
    pub entities: bool,
    /// Sentence spans in original-text UTF-8 coordinates.
    pub sentences: bool,
    /// Both folded and cased term identities from one pass.
    pub dual_cased: bool,
    /// Full one-pass `DocumentNlpProjection` retained for pre-WAL typed column materialization.
    pub nlp_columns: bool,
}

/// Zero-length terms dropped at the analysis boundary since process start.
#[cfg(any(feature = "net", test))]
pub(crate) static EMPTY_TERMS_DROPPED: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
