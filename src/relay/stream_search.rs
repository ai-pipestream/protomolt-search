//! The streamed vector relay: one live floor above, one stream per child below.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::coordinator::{RequestFilters, ShardVectorRowProbe};
use crate::metrics::Route;
use crate::pb::node_service_server::NodeService;
use crate::pb::{stream_search_request, StreamSearchRequest, StreamSearchResponse};
use crate::stats_identity::{is_stale_stats_epoch, stale_stats_epoch_status};

use super::map::MapSnapshot;
use super::signals::RelaySignals;
use super::stream_forward::relay_stream;
use super::stream_state::{SelectionTask, StreamArgs};
use super::{grpc_timeout, RelayService};

/// A publication racing the pin: the receiver may already hold a newer
/// value than the reading, which the first poll then reports.
pub(super) fn sync_map_changes(map_changes: &mut watch::Receiver<u64>, pinned: &MapSnapshot) {
    map_changes.mark_unchanged();
    if *map_changes.borrow() != pinned.control_revision {
        map_changes.mark_changed();
    }
}

/// Discovery bounds each best-effort child probe and interrupts immediately on
/// parent cancellation, deadline, or shard-map movement. A closed map source
/// leaves the pinned reading authoritative.
pub(super) async fn await_vector_rows(
    probe: &mut ShardVectorRowProbe,
    pinned: &MapSnapshot,
    map_changes: &mut watch::Receiver<u64>,
    map_open: &mut bool,
    signals: Option<&RelaySignals>,
    route: &str,
) -> Result<Option<crate::pb::StreamEndReason>, Status> {
    loop {
        tokio::select! {
            biased;
            _ = async { signals.expect("guarded").wait_cancelled().await }, if signals.is_some() => {
                probe.shutdown().await;
                return Ok(Some(crate::pb::StreamEndReason::Abandoned));
            }
            _ = async { signals.expect("guarded").wait_finish().await }, if signals.is_some() => {
                probe.shutdown().await;
                let signals = signals.expect("guarded");
                // Preserve a Stop or deadline escalation received while draining.
                let reason = if signals.cancelled.load(Ordering::Acquire) {
                    crate::pb::StreamEndReason::Abandoned
                } else if signals.finish_state_raw() >= 2 {
                    crate::pb::StreamEndReason::Deadline
                } else {
                    crate::pb::StreamEndReason::FinishRequested
                };
                return Ok(Some(reason));
            }
            moved = map_changes.changed(), if *map_open => match moved {
                Ok(()) => {
                    let revision = *map_changes.borrow_and_update();
                    if revision != pinned.control_revision {
                        return Err(Status::failed_precondition(format!(
                            "relay: the shard map moved from revision {} (generation {}) to \
                             revision {revision} during {route} capability discovery; the attempt \
                             is not completed; retry under the current one",
                            pinned.control_revision, pinned.topology_generation
                        )));
                    }
                }
                Err(_) => *map_open = false,
            },
            _ = probe.wait() => return Ok(None),
        }
    }
}

/// A child's error as the parent must read it. Structured stale-stats
/// refusals preserve their refusal-reason metadata header so the parent's
/// retry rule fires; all other errors preserve metadata and name the child.
pub(super) fn child_error(shard: usize, addr: &str, route: &str, status: Status) -> Status {
    let detail = format!("relay child {shard} ({addr}) {route}: {}", status.message());
    if is_stale_stats_epoch(&status) {
        let mut out = stale_stats_epoch_status(detail);
        *out.metadata_mut() = status.metadata().clone();
        return out;
    }
    Status::with_metadata(status.code(), detail, status.metadata().clone())
}
/// The first frame of a `StreamSearch`, validated: it must be the start,
/// carry no scoped read context, and name a valid floor and filters.
async fn read_start(
    inbound: &mut Streaming<StreamSearchRequest>,
) -> Result<crate::pb::StartStreamSearch, Status> {
    let start = match inbound.message().await? {
        Some(StreamSearchRequest {
            payload: Some(stream_search_request::Payload::Start(start)),
        }) => start,
        _ => {
            return Err(Status::invalid_argument(
                "first StreamSearchRequest must be StartStreamSearch",
            ))
        }
    };
    if start.read_context.is_some() {
        return Err(Status::failed_precondition(
            "relay does not yet compose scoped vector read receipts",
        ));
    }
    if start.initial_floor.is_some_and(f32::is_nan) {
        return Err(Status::invalid_argument("initial_floor must not be NaN"));
    }
    crate::node::validate_geo_filters(&start.geo_filters)?;
    if let Some(f) = start.filter.as_ref() {
        crate::filter::validate_filter(f)?;
    }
    Ok(start)
}

fn stream_filters(start: &crate::pb::StartStreamSearch) -> RequestFilters {
    RequestFilters {
        geo: start.geo_filters.clone(),
        tree: start.filter.clone(),
        enumerate_max_rows: start.enumerate_max_rows,
        filter_strategy: start.filter_strategy,
        enumerate_all: start.enumerate_all,
        staged_selection: start.staged_selection,
        patience_saturation_ppm: start.patience_saturation_ppm,
        patience_hops: start.patience_hops,
        finish_settle_us: None,
        finish_deadline_us: None,
        floor_prune: start.floor_prune,
        floor_k: start.floor_k.or(start.candidate_depth),
        parent_floor_share_ppm: start.floor_share_ppm,
        floor_greediness_ppm: start.floor_greediness_ppm,
        floor_clamp_min: start.floor_clamp_min,
        include_rerank_candidate: start.include_rerank_candidate,
        exact_fp32_scan: start.exact_fp32_scan,
    }
}

/// The parent's gRPC leg: floor raises and the authoritative Stop, a
/// finish request, and (in identity mode) the selection. A closed leg
/// before the summary means the parent is gone, and the children are
/// told so.
fn spawn_parent_leg(
    mut inbound: Streaming<StreamSearchRequest>,
    pump: Arc<RelaySignals>,
    identity_enabled: bool,
    ready: Arc<AtomicBool>,
) -> SelectionTask {
    tokio::spawn(async move {
        loop {
            let payload = match inbound.message().await {
                Ok(Some(StreamSearchRequest { payload: Some(p) })) => p,
                Ok(Some(_)) => continue,
                Ok(None) | Err(_) => {
                    if identity_enabled {
                        pump.cancel();
                    }
                    return Ok(None);
                }
            };
            match payload {
                stream_search_request::Payload::FloorUpdate(u) => pump.raise(u.floor),
                stream_search_request::Payload::Stop(_) => {
                    pump.cancel();
                    return Ok(None);
                }
                stream_search_request::Payload::Finish(f) => pump.request_finish(f.deadline),
                stream_search_request::Payload::ResolveIdentities(selection) => {
                    if !identity_enabled || !ready.load(Ordering::Acquire) {
                        pump.cancel();
                        return Err(Status::failed_precondition(
                            "relay: identity selection requires IdentityReady",
                        ));
                    }
                    return Ok(Some(selection));
                }
                _ => {}
            }
        }
    })
}

impl RelayService {
    pub(super) async fn handle_stream_search(
        &self,
        request: Request<Streaming<StreamSearchRequest>>,
    ) -> Result<Response<<Self as NodeService>::StreamSearchStream>, Status> {
        crate::metrics::timed_stream(Route::StreamSearch, request, |request| async move {
            let deadline =
                grpc_timeout(request.metadata()).map(|d| tokio::time::Instant::now() + d);
            let mut inbound = request.into_inner();
            let start = read_start(&mut inbound).await?;
            let filters = stream_filters(&start);
            let signals = Arc::new(RelaySignals::new(
                start.initial_floor.unwrap_or(f32::NEG_INFINITY),
            ));
            let token = (start.floor_token != 0).then_some(start.floor_token);
            if let Some(token) = token {
                self.signals
                    .lock()
                    .expect("relay stream signal registry poisoned")
                    .insert(token, Arc::clone(&signals));
            }
            let identity_ready = Arc::new(AtomicBool::new(false));
            let input = spawn_parent_leg(
                inbound,
                Arc::clone(&signals),
                start.identity_limits.is_some(),
                Arc::clone(&identity_ready),
            );
            let (tx, rx) = mpsc::channel::<Result<StreamSearchResponse, Status>>(64);
            let (pinned, frozen) = self.pin();
            let args = StreamArgs {
                coordinator: frozen,
                pinned,
                map_changes: self.inner.map.changes(),
                start,
                filters,
                signals,
                tx,
                deadline,
                input,
                identity_ready,
            };
            tokio::spawn(run_stream(args, Arc::clone(&self.signals), token));
            Ok(Response::new(ReceiverStream::new(rx)))
        })
        .await
    }
}

type SignalRegistry = Arc<std::sync::Mutex<std::collections::HashMap<u64, Arc<RelaySignals>>>>;

/// The relay task: the forwarding loop under the parent's closed
/// response and deadline, then the cleanup every path shares.
async fn run_stream(args: StreamArgs, registry: SignalRegistry, token: Option<u64>) {
    let mut probe: ShardVectorRowProbe = args.coordinator.shard_vector_row_probe();
    let tx = args.tx.clone();
    let deadline = args.deadline;
    let input_abort = args.input.abort_handle();
    let result = {
        let work = relay_stream(args, &mut probe);
        tokio::select! {
            _ = tx.closed() => Err(Status::cancelled("relay: parent response closed")),
            _ = async { tokio::time::sleep_until(deadline.expect("guarded")).await }, if deadline.is_some() =>
                Err(Status::deadline_exceeded("relay: parent deadline passed")),
            result = work => result,
        }
    };
    probe.shutdown().await;
    input_abort.abort();
    if let Some(token) = token {
        registry
            .lock()
            .expect("relay stream signal registry poisoned")
            .remove(&token);
    }
    if let Err(status) = result {
        let _ = tokio::time::timeout(Duration::from_millis(250), tx.send(Err(status))).await;
    }
}
