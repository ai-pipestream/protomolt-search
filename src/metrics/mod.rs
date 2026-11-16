//! Process-wide metrics and the Prometheus text-format exporter.
//!
//! Hand-rolled on purpose. The exposition format is a few lines of
//! `name{label="value"} 123\n` text and the scrape protocol is one GET,
//! so neither a metrics framework nor an HTTP framework earns a place
//! in the serving binary for it — the same argument that keeps CEL and
//! regex crates out.
//!
//! The design splits along lifetime lines:
//!
//! - **Counters and histograms** are process-wide statics, fed at the
//!   one instrumentation seam every handler passes through ([`timed`]
//!   and [`timed_stream`]) plus the few places the engine already
//!   counted things (`record_scan`, `add_ingested`). Monotone, cheap
//!   (one `Instant::now()` at arrival, one per phase end, and a few
//!   relaxed atomic adds), and always on — whether or not an exporter
//!   serves them. Nothing is formatted per request: label strings are
//!   rendered at scrape time only.
//! - **Gauges** are read at SCRAPE time from live shard state, through
//!   closures the binary hands to [`serve()`]. Nothing has to remember
//!   to update a gauge on every mutation, so a gauge can never go
//!   stale — it is the state, sampled. The one exception is the
//!   in-flight gauge, which is a counter pair by nature (arrivals minus
//!   departures) and lives with the counters.
//!
//! The route set is fixed at compile time (`REQUEST_ROUTES`), which is
//! what lets every table be a plain static indexed by the route's
//! discriminant: no label registry, no map, no lock.

mod gauges;
mod registry;
mod render;
mod routes;
mod seam;
#[cfg(feature = "net")]
mod serve;
mod snapshot;

#[cfg(test)]
mod registry_tests;
#[cfg(test)]
mod seam_tests;

pub use gauges::{
    GaugeProvider, MemberGaugeProvider, MemberGauges, PeerRejectionGauges, ShardGauges,
};
pub use registry::{
    add_ingested, dense_policy_drift_total, read, read_with_member, record_dense_policy_drift,
    record_scan, record_scan_pass, scan_pass_totals, HistogramReading, Reading,
};
pub use render::{render, render_reading, render_with_member, write_metric, write_metric_seconds};
pub use routes::Route;
pub use seam::{inc_request, nested, timed, timed_stream, Timed};
#[cfg(feature = "net")]
pub use serve::{serve, serve_with_member};
pub use snapshot::{snapshot, snapshot_reading, snapshot_with_member};
