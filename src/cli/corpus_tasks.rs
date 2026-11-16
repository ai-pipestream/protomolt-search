//! A dataset coordinator's background tasks and its control plane.

use std::path::Path;
use std::time::Duration;

use pipestream_search::config::load_shard_map;
use pipestream_search::control_plane::{ClusterControlService, ControlPolicy, DurableControlPlane};
use pipestream_search::coordinator::CoordinatorServiceImpl;
use pipestream_search::pb::cluster_control_server::ClusterControl;
use pipestream_search::pb::ReconcileClusterRequest;
use pipestream_search::replication::ReplicaState;
use tokio::sync::watch::Receiver;

use super::corpus::{topology_routes, CorpusSpec};
use super::{Startup, TaskHandle};

/// A skipping interval of `ms` milliseconds.
fn skipping_interval(ms: u64) -> tokio::time::Interval {
    let mut interval = tokio::time::interval(Duration::from_millis(ms));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    interval
}

/// Publish the shard map at `path` when it carries a newer generation
/// with the running derived-column declaration.
fn reload_shard_map(reload: &CoordinatorServiceImpl, path: &Path) {
    let candidate = match load_shard_map(path) {
        Ok(map) => map,
        Err(error) => {
            eprintln!("shard-map reload refused: {error}");
            return;
        }
    };
    if candidate.generation <= reload.current_topology_generation() {
        return;
    }
    // A map that changes the derived-column declaration
    // describes another index: a rebuild, not a reload.
    let running: Vec<pipestream_search::derived::DerivedColumnConfig> = reload
        .derived()
        .map(|declaration| declaration.config())
        .unwrap_or_default();
    if candidate.derived != running {
        eprintln!(
            "shard-map reload refused: generation {} changes the [[derived]] \
         declaration; a changed declaration is a rebuild through the \
         reshard tool and a restart (docs/derived-columns.md)",
            candidate.generation
        );
        return;
    }
    let generation = candidate.generation;
    match topology_routes(&candidate)
        .and_then(|routes| reload.reload_topology(generation, routes, candidate.placement.as_ref()))
    {
        Ok(()) => eprintln!(
            "shard-map generation {generation} published atomically ({} shards)",
            candidate.shards.len()
        ),
        Err(error) => eprintln!("shard-map reload refused: {error}"),
    }
}

/// `--shard-map-reload-ms`: poll the dataset's shard map file.
pub(super) fn spawn_shard_map_reload(
    startup: &Startup<'_>,
    dataset: &CorpusSpec<'_>,
    coordinator: &CoordinatorServiceImpl,
    handles: &mut Vec<TaskHandle>,
) {
    let cfg = startup.cfg;
    if cfg.shard_map_reload_ms == 0 {
        return;
    }
    let path = dataset
        .shard_map_path
        .map(Path::to_path_buf)
        .expect("configuration validated a shard-map path");
    let reload = coordinator.clone();
    let reload_ms = cfg.shard_map_reload_ms;
    let mut shutdown = startup.shutdown_rx.clone();
    handles.push(tokio::spawn(async move {
        let mut interval = skipping_interval(reload_ms);
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                _ = interval.tick() => reload_shard_map(&reload, &path),
            }
        }
        Ok::<(), tonic::transport::Error>(())
    }));
}

/// Catch every replica of the current topology up once, persisting each
/// advanced cursor.
async fn sync_replicas(topology: &CoordinatorServiceImpl, state: &mut ReplicaState, path: &Path) {
    for route in topology.current_topology_routes() {
        let Some(replica) = route.replica else {
            continue;
        };
        let prior = state.cursor_mut(&route.addr, &replica).clone();
        match pipestream_search::replication::sync_once(&prior).await {
            Ok(updated) => {
                *state.cursor_mut(&route.addr, &replica) = updated;
                if let Err(error) = state.write(path) {
                    eprintln!("replica cursor persistence failed: {error}");
                }
            }
            Err(error) => eprintln!(
                "replica catch-up {} -> {} refused: {error}",
                route.addr, replica
            ),
        }
    }
}

/// `--replica-sync-ms`: catch replicas up on an interval.
pub(super) fn spawn_replica_sync(
    startup: &Startup<'_>,
    dataset: &CorpusSpec<'_>,
    coordinator: &CoordinatorServiceImpl,
    handles: &mut Vec<TaskHandle>,
) -> Result<(), Box<dyn std::error::Error>> {
    let cfg = startup.cfg;
    if cfg.replica_sync_ms == 0 {
        return Ok(());
    }
    let path = dataset
        .replica_state_path
        .map(Path::to_path_buf)
        .expect("configuration validated a replica-state path");
    let mut state = ReplicaState::load(&path)?;
    let topology = coordinator.clone();
    let sync_ms = cfg.replica_sync_ms;
    let mut shutdown = startup.shutdown_rx.clone();
    handles.push(tokio::spawn(async move {
        let mut interval = skipping_interval(sync_ms);
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        break;
                    }
                }
                _ = interval.tick() => sync_replicas(&topology, &mut state, &path).await,
            }
        }
        Ok::<(), tonic::transport::Error>(())
    }));
    Ok(())
}

/// `--control-reconcile-ms`: reconcile the cluster on an interval.
async fn reconcile_loop(
    reconcile: ClusterControlService,
    interval_ms: u64,
    mut shutdown: Receiver<bool>,
) -> Result<(), tonic::transport::Error> {
    let mut interval = skipping_interval(interval_ms);
    loop {
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    break;
                }
            }
            _ = interval.tick() => {
                let request = tonic::Request::new(ReconcileClusterRequest {
                    collection: String::new(),
                    dry_run: false,
                });
                if let Err(error) = ClusterControl::reconcile_cluster(&reconcile, request).await {
                    eprintln!("cluster reconciliation refused: {error}");
                }
            }
        }
    }
    Ok(())
}

/// The dataset's durable control plane over its coordinator, bootstrapped
/// from the current topology; `None` when no control state is configured.
pub(super) fn control_service(
    startup: &Startup<'_>,
    dataset: &CorpusSpec<'_>,
    coordinator: &CoordinatorServiceImpl,
    handles: &mut Vec<TaskHandle>,
) -> Result<Option<ClusterControlService>, Box<dyn std::error::Error>> {
    let cfg = startup.cfg;
    let Some(path) = dataset.control_state_path else {
        return Ok(None);
    };
    let plane = DurableControlPlane::open(
        path,
        ControlPolicy {
            lease_ms: cfg.control_lease_ms,
            replication_factor: cfg.control_replication_factor,
            split_rows: cfg.control_split_rows,
            merge_rows: cfg.control_merge_rows,
            compact_segments: cfg.control_compact_segments,
            compact_tombstone_ppm: cfg.control_compact_tombstone_ppm,
            history_limit: 32,
        },
    )?
    .with_collection(dataset.name)?;
    plane.bootstrap_topology(
        coordinator.current_topology_generation(),
        &coordinator.current_topology_routes(),
    )?;
    let control = ClusterControlService::new(plane)
        .with_coordinator(coordinator.clone())
        .with_client_cert_required(cfg.tls.as_ref().is_some_and(|t| t.client_ca_pem.is_some()));
    control.publish_current_topology()?;
    if cfg.control_reconcile_ms > 0 {
        handles.push(tokio::spawn(reconcile_loop(
            control.clone(),
            cfg.control_reconcile_ms,
            startup.shutdown_rx.clone(),
        )));
    }
    eprintln!("cluster control state: {}", path.display());
    Ok(Some(control))
}
