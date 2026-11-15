//! Canonical wire session binding and mock graph codec bridging
//! `protomolt-search` analysis sessions with `turbo.result.v1.AnalysisService`.

#[cfg(any(feature = "net", test))]
#[path = "analyzer_wire/analyzed.rs"]
mod analyzed;
#[cfg(any(feature = "net", test))]
#[path = "analyzer_wire/codec.rs"]
mod codec;
#[cfg(any(feature = "net", test))]
#[path = "analyzer_wire/graph.rs"]
mod graph;
#[cfg(any(feature = "net", test))]
#[path = "analyzer_wire/session.rs"]
mod session;

#[cfg(feature = "net")]
use tonic::Status;

#[cfg(any(feature = "net", test))]
pub(crate) use analyzed::*;
#[cfg(any(feature = "net", test))]
pub(crate) use graph::*;
#[cfg(any(feature = "net", test))]
pub(crate) use session::*;

/// `InputBinding.parse_artifact_id` carrying serialized [`WireSessionOptions`].
#[cfg(any(feature = "net", test))]
pub(crate) const SESSION_BINDING_ID: &str = "protomolt.session.v1";

/// Binary magic identifying a deterministic `MockAnalysis` payload inside
/// `DocumentGraphResponse.graph` (distinct from `b"TGRF"` binary graphs).
#[cfg(any(feature = "net", test))]
pub(crate) const MOCK_GRAPH_MAGIC: &[u8; 5] = b"TMCK\x01";

/// Profile marker advertised only by `start_mock_analysis_without_dual_identity`
/// to simulate a server lacking dual cased/folded term extraction.
#[cfg(any(feature = "net", test))]
pub(crate) const NO_DUAL_CASED_PROFILE: &str = "protomolt:no-dual-cased";

/// Wire offset unit codes preserved for UTF-8 storage invariant checks.
#[cfg(any(feature = "net", test))]
pub(crate) const WIRE_OFFSET_UNSPECIFIED: i32 = 0;
#[cfg(any(feature = "net", test))]
pub(crate) const WIRE_OFFSET_UTF8: i32 = 1;
#[cfg(any(feature = "net", test))]
pub(crate) const WIRE_OFFSET_UTF16: i32 = 2;

/// Converts a `turbo-kserve` `AnalyzeFailure` into a `tonic::Status`.
#[cfg(feature = "net")]
pub(crate) fn status_from_analyze_failure(
    failure: turbo_client::proto::result::AnalyzeFailure,
) -> Status {
    let code = match failure.turbo_code {
        1..=16 => tonic::Code::from(failure.turbo_code),
        256 | 258 | 260 | 262 => tonic::Code::InvalidArgument,
        261 | 1024 | 1281 | 1283 => tonic::Code::FailedPrecondition,
        512..=514 => tonic::Code::Unimplemented,
        768 => tonic::Code::ResourceExhausted,
        769 | 1025 => tonic::Code::Unavailable,
        771 => tonic::Code::OutOfRange,
        1280 => tonic::Code::NotFound,
        1282 => tonic::Code::DataLoss,
        _ => tonic::Code::Internal,
    };
    Status::new(code, failure.message)
}

/// Encodes a `tonic::Status` into a `turbo-kserve` `AnalyzeFailure`.
#[cfg(feature = "net")]
pub(crate) fn analyze_failure_from_status(
    status: Status,
) -> turbo_client::proto::result::AnalyzeFailure {
    turbo_client::proto::result::AnalyzeFailure {
        turbo_code: status.code() as i32,
        field: 0,
        message: status.message().to_string(),
    }
}
