//! The diagnostics service (`docs/diagnostics.md`): runtime knobs that
//! flip without a restart, structured metrics snapshots and a stream
//! of them, per-shard layout diagnostics, and the coordinator's ring of
//! recent public requests.
//!
//! The knobs are atomics read at request time. Each process role holds
//! its own [`Knobs`]: a node's carries the scan and floor settings, a
//! coordinator's the request caps. A setting the code still reads from
//! its construction-time config is listed as immutable, and setting it
//! is rejected by name; the list never claims more than the process
//! honors.

use std::collections::VecDeque;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, AtomicU32, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tokio_stream::wrappers::ReceiverStream;
use tokio_stream::Stream;
use tonic::{Request, Response, Status};

use crate::authorization::{AccessPermit, AuthorizedStream};
use crate::metrics::Route;
use crate::pb::diagnostics_service_server::{DiagnosticsService, DiagnosticsServiceServer};
use crate::pb::{
    GetRuntimeKnobsRequest, KnobKind, KnobScope, MetricsSnapshot, MetricsSnapshotRequest,
    RecentQueriesRequest, RecentQueriesResponse, RecentQuery, RuntimeKnob, RuntimeKnobs,
    SetRuntimeKnobRequest, ShardDiagnostics, ShardDiagnosticsRequest, ShardLayoutDiagnostics,
    StreamMetricsRequest,
};
use crate::security::Principals;

mod knob_set;
mod knobs;
mod recent;
mod service;
#[cfg(test)]
mod tests;

use self::knobs::*;
pub use self::knobs::{
    CoordinatorKnobValues, FixedKnob, Knobs, NodeKnobValues, RECENT_DEFAULT, RECENT_RING,
    STREAM_DEFAULT_MS, STREAM_MIN_MS,
};
pub use self::recent::{unix_ms, RecentFigures, RecentRing};
pub use self::service::{unserved_layout, CoordinatorDiagnostics, NodeDiagnostics};
