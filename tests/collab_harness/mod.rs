//! Reusable verification tools and the scenarios that exercise them.
//!
//! - [`jmh`]: forked warmup/measurement trials with wall and thread-CPU
//!   clocks, summary statistics and JSON provenance receipts.
//! - [`trace_proxy`]: a transparent tower layer over a served gRPC
//!   service that records every call's method, byte and message counts,
//!   latency, status and start order.
//! - [`gate`]: named rendezvous points with bounded waits for pausing one
//!   side of an interleaving at an exact protocol boundary.
//! - [`scenarios`]: executable checks built from the three tools.

pub mod gate;
pub mod jmh;
pub mod scenarios;
pub mod trace_proxy;
