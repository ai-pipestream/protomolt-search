//! The metrics, relay and coordinator listeners.

use std::net::SocketAddr;

use pipestream_search::metrics::GaugeProvider;
use pipestream_search::node::NodeServiceImpl;
use tokio::net::TcpListener;

use super::collections::build_collections;
use super::corpus::{build_corpus, CorpusSpec};
use super::raft_member::RaftSurfaces;
use super::transport::{spawn_listener, windowed_server};
use super::{Startup, TaskHandle};

fn shard_gauges(node_services: &[NodeServiceImpl]) -> Vec<GaugeProvider> {
    node_services
        .iter()
        .map(|node| node.metrics_provider())
        .collect()
}

/// `--metrics-listen`: the Prometheus endpoint over every shard's gauges
/// and the Raft member's.
pub(super) async fn serve_metrics(
    startup: &Startup<'_>,
    raft: &RaftSurfaces,
    node_services: &[NodeServiceImpl],
    handles: &mut Vec<TaskHandle>,
) -> Result<(), Box<dyn std::error::Error>> {
    let Some(metrics_addr) = startup.cfg.metrics_listen else {
        return Ok(());
    };
    let listener = TcpListener::bind(metrics_addr).await?;
    let bound = listener.local_addr()?;
    let gauges = shard_gauges(node_services);
    let member_gauges = raft.member_gauges();
    eprintln!(
        "metrics on http://{bound}/metrics ({} shard gauges)",
        gauges.len()
    );
    handles.push(tokio::spawn(async move {
        pipestream_search::metrics::serve_with_member(listener, gauges, member_gauges).await;
        Ok(())
    }));
    Ok(())
}

/// A relay: this coordinator's shard set behind the node-facing surface,
/// on the coordinator listener, with the parent-facing UDP lane on the
/// same port. The startup check refuses children whose slot ranges are
/// not contiguous.
pub(super) async fn serve_relay(
    startup: &Startup<'_>,
    raft: &RaftSurfaces,
    handles: &mut Vec<TaskHandle>,
) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = startup.cfg;
    let listener = TcpListener::bind(cfg.coord_listen).await?;
    let addr: SocketAddr = listener.local_addr()?;
    let (coordinator, _control) =
        build_corpus(startup, CorpusSpec::unnamed(cfg, None), handles).await?;
    let relay = raft.relay_service(cfg, coordinator)?;
    let health = relay
        .check_children()
        .await
        .map_err(|status| format!("relay startup: {}", status.message()))?;
    relay.spawn_floor_listener(addr);
    eprintln!(
        "relay NodeService listening on {addr} over {} children, slots {}..{} ({} vectors, \
         {} documents)",
        relay.children().len(),
        health.slot_offset,
        health.slot_offset + health.num_vectors.max(health.bm25_docs),
        health.num_vectors,
        health.bm25_docs
    );
    let max = cfg.max_message_bytes;
    let relay_base = windowed_server(cfg.tls.as_ref(), true)?
        .add_service(relay.clone().into_server(max))
        .add_service(relay.diagnostics_server(max));
    let relay_server = raft.on_write_listener(relay_base);
    handles.push(spawn_listener(relay_server, listener, &startup.shutdown_rx));
    Ok(())
}

/// The coordinator role: search, cluster control and diagnostics over
/// the configured collection set.
pub(super) async fn serve_coordinator(
    startup: &Startup<'_>,
    raft: &RaftSurfaces,
    node_services: &[NodeServiceImpl],
    handles: &mut Vec<TaskHandle>,
) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = startup.cfg;
    let listener = TcpListener::bind(cfg.coord_listen).await?;
    let addr: SocketAddr = listener.local_addr()?;
    let (search_set, control_set) = build_collections(startup, handles, addr).await?;
    let max = cfg.max_message_bytes;
    let diagnostics = search_set
        .diagnostics()
        .with_gauges(shard_gauges(node_services))
        .with_member_gauges(raft.member_gauges())
        .into_server(max);
    let coord_base = windowed_server(cfg.tls.as_ref(), false)?
        .add_optional_service(control_set.map(|set| set.into_server(max)))
        .add_service(search_set.into_server(max))
        .add_service(diagnostics);
    let coord_server = raft.on_write_listener(coord_base);
    handles.push(spawn_listener(coord_server, listener, &startup.shutdown_rx));
    Ok(())
}
