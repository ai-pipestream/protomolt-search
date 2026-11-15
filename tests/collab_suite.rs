//! Collaborative verification suite: one integration test binary for the
//! timing, call-tracing and interleaving harness in `collab_harness/`.
//!
//! Run with `cargo test --test collab_suite -- --test-threads=2`. The
//! scenarios serve real nodes over loopback, so the suite needs the `net`
//! feature and compiles to an empty binary without it.
#![cfg(feature = "net")]

mod collab_harness;
