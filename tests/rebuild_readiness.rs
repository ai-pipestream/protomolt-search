//! `deploy/v7-rebuild/rebuild.sh` reports a stage done only when the
//! capability the stage promises was observed. Each case runs the real
//! script against stub executables and ports this process holds: no node,
//! coordinator, or sidecar is started and nothing leaves loopback.
//!
//! `serve` must not print "fleet ready" unless the verifier confirmed the
//! shards, and an embedding deployment must not adopt or start a sidecar
//! that has not answered an embed probe.
#![cfg(all(unix, feature = "net"))]

#[path = "common/scratch.rs"]
mod scratch;

#[path = "rebuild_readiness/rig.rs"]
mod rig;

#[path = "rebuild_readiness/budget.rs"]
mod budget;
#[path = "rebuild_readiness/serve.rs"]
mod serve;
#[path = "rebuild_readiness/sidecar.rs"]
mod sidecar;
#[path = "rebuild_readiness/sidecar_start.rs"]
mod sidecar_start;
