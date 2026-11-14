//! The streamed vector relay: one live floor above, one stream per child below.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::coordinator::{
    CoordinatorServiceImpl, RequestFilters, ShardVectorRowProbe, StreamFanout,
};
use crate::metrics::Route;
use crate::node::STALE_STATS_EPOCH;
use crate::pb::node_service_server::NodeService;
use crate::pb::{
    stream_search_request, stream_search_response, StreamSearchRequest, StreamSearchResponse,
    StreamSearchSummary,
};

use super::map::MapSnapshot;
use super::signals::RelaySignals;
use super::{grpc_timeout, RelayService};

/// Sums carried across the children's terminal summaries.
#[derive(Default)]
struct StreamTotals {
    emitted: u64,
    blocks_scanned: u64,
    floor_raises_applied: u64,
    segments_total: u32,
    segments_skipped: u32,
    regimes: crate::vector::FilterRegimeCounts,
    tail_enumerated_rows: u64,
    patience_stopped: u32,
    floor_pruned: u32,
    end_reason: crate::vector::VectorStreamEndReason,
}

enum StreamEvent {
    Message(Option<(usize, StreamSearchResponse)>),
    Floor(f32),
    Finish(bool),
    Cancel,
    Deadline,
    MapMoved(u64),
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

/// The forwarding loop of one relayed `StreamSearch`: children's batches
/// go up untouched, the parent's floor and cancel go down, and the
/// relay's summary follows the last child's.
#[allow(clippy::too_many_arguments)]
async fn relay_stream(
    coordinator: CoordinatorServiceImpl,
    pinned: MapSnapshot,
    mut map_changes: watch::Receiver<u64>,
    start: crate::pb::StartStreamSearch,
    filters: RequestFilters,
    signals: Arc<RelaySignals>,
    tx: mpsc::Sender<Result<StreamSearchResponse, Status>>,
    deadline: Option<tokio::time::Instant>,
    mut input: tokio::task::JoinHandle<Result<Option<crate::pb::ResolveStreamIdentities>, Status>>,
    identity_ready: Arc<AtomicBool>,
    probe: &mut ShardVectorRowProbe,
) -> Result<(), Status> {
    // A publication racing the pin: the receiver may already hold a newer
    // value than the reading, which the first poll below reports.
    map_changes.mark_unchanged();
    if *map_changes.borrow() != pinned.control_revision {
        map_changes.mark_changed();
    }
    let n = coordinator.node_addresses().len();
    if n == 0 {
        return Err(Status::failed_precondition(
            "relay: a relay coordinator has no children",
        ));
    }
    let identity_limits = start.identity_limits;
    if let Some(limits) = identity_limits.as_ref() {
        crate::query_identity::validate_limits(limits)?;
    }
    let stride = if start.collapse_parents { 20 } else { 12 };
    let mut map_open = true;
    if let Some(end_reason) = await_vector_rows(
        probe,
        &pinned,
        &mut map_changes,
        &mut map_open,
        Some(&signals),
        "StreamSearch",
    )
    .await?
    {
        let summary = StreamSearchSummary {
            completed: false,
            geo_columns_known: vec![false; filters.geo.len()],
            filter_columns_known: vec![
                false;
                filters.tree.as_ref().map_or(0, crate::filter::leaf_count)
            ],
            end_reason: end_reason as i32,
            ..Default::default()
        };
        let _ = tx
            .send(Ok(StreamSearchResponse {
                payload: Some(stream_search_response::Payload::Summary(summary)),
            }))
            .await;
        return Ok(());
    }
    let mask = coordinator.shard_mask(filters.tree.as_ref());
    let mut fanout: StreamFanout = coordinator.open_stream_fanout_with_identities(
        &start.request_id,
        &start.vector,
        start.initial_floor,
        start.candidate_depth,
        start.collapse_parents,
        &filters,
        identity_limits,
    )?;
    let mut summaries: Vec<Option<StreamSearchSummary>> = vec![None; n];
    let mut remaining = n;
    for (shard, summary) in summaries.iter_mut().enumerate() {
        if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
            *summary = Some(StreamSearchSummary {
                completed: true,
                ..Default::default()
            });
            remaining -= 1;
        }
    }
    let mut terminal = summaries.clone();
    let mut ranges: Vec<Option<crate::pb::StreamIdentityRange>> = vec![None; n];
    let leaves = filters.tree.as_ref().map_or(0, crate::filter::leaf_count);
    let mut geo_known = vec![false; filters.geo.len()];
    let mut tree_known = vec![false; leaves];
    if let Some(mask) = mask.as_ref() {
        for &leaf in &mask.known {
            if let Some(flag) = tree_known.get_mut(leaf) {
                *flag = true;
            }
        }
    }
    let mut totals = StreamTotals::default();
    let mut scoring_fingerprint: Option<String> = None;
    let mut last_floor = start.initial_floor.unwrap_or(f32::NEG_INFINITY);
    let mut floor_rx = signals.floor.subscribe();
    let mut floor_open = true;
    let mut forwarded_finish: u8 = 0;
    let mut sleep = deadline.map(|at| Box::pin(tokio::time::sleep_until(at)));

    while remaining > 0 {
        let event = tokio::select! {
            biased;
            _ = signals.cancel.notified(), if !signals.cancelled.load(Ordering::Acquire) => StreamEvent::Cancel,
            _ = async { true }, if signals.cancelled.load(Ordering::Acquire) => StreamEvent::Cancel,
            _ = signals.finish.notified(), if signals.finish_state_raw() <= forwarded_finish => {
                let raw = signals.finish_state_raw();
                if raw > forwarded_finish {
                    StreamEvent::Finish(raw >= 2)
                } else {
                    continue;
                }
            },
            _ = async { true }, if signals.finish_state_raw() > forwarded_finish => {
                StreamEvent::Finish(signals.finish_state_raw() >= 2)
            },
            _ = async { sleep.as_mut().expect("guarded").await }, if sleep.is_some() => StreamEvent::Deadline,
            moved = map_changes.changed(), if map_open => match moved {
                Ok(()) => StreamEvent::MapMoved(*map_changes.borrow_and_update()),
                Err(_) => {
                    // The source is gone with its authority; nothing
                    // will move again, and the pinned reading stands.
                    map_open = false;
                    continue;
                }
            },
            changed = floor_rx.changed(), if floor_open => match changed {
                Ok(()) => StreamEvent::Floor(*floor_rx.borrow_and_update()),
                Err(_) => {
                    floor_open = false;
                    continue;
                }
            },
            message = fanout.next_message(&terminal) => match message {
                Ok(pair) => StreamEvent::Message(pair),
                Err(status) => return fanout.cancel_with(status).await,
            },
        };
        match event {
            StreamEvent::Cancel => {
                fanout.cancel().await;
                let summary = StreamSearchSummary {
                    completed: false,
                    emitted: totals.emitted,
                    blocks_scanned: totals.blocks_scanned,
                    floor_raises_applied: totals.floor_raises_applied,
                    geo_columns_known: geo_known,
                    filter_columns_known: tree_known,
                    scoring_fingerprint: scoring_fingerprint.unwrap_or_default(),
                    segments_total: totals.segments_total,
                    segments_skipped: totals.segments_skipped,
                    filter_regimes: totals.regimes.to_proto(),
                    tail_enumerated_rows: totals.tail_enumerated_rows,
                    patience_stopped: totals.patience_stopped,
                    end_reason: crate::pb::StreamEndReason::Abandoned as i32,
                    floor_pruned: totals.floor_pruned,
                };
                let _ = tx
                    .send(Ok(StreamSearchResponse {
                        payload: Some(stream_search_response::Payload::Summary(summary)),
                    }))
                    .await;
                return Ok(());
            }
            StreamEvent::Finish(deadline) => {
                forwarded_finish = forwarded_finish.max(if deadline { 2 } else { 1 });
                coordinator
                    .push_stream_finish(&fanout, &summaries, deadline)
                    .await;
            }
            StreamEvent::Deadline => {
                return fanout
                    .cancel_with(Status::deadline_exceeded(
                        "relay: the parent's deadline passed before every child completed",
                    ))
                    .await;
            }
            StreamEvent::MapMoved(revision) => {
                if revision == pinned.control_revision {
                    continue;
                }
                return fanout
                    .cancel_with(Status::failed_precondition(format!(
                        "relay: the shard map moved from revision {} (generation {}) to \
                         revision {revision} during StreamSearch; the stream was opened under \
                         the older map and is not completed; retry under the current one",
                        pinned.control_revision, pinned.topology_generation
                    )))
                    .await;
            }
            StreamEvent::Floor(floor) => {
                if floor > last_floor {
                    last_floor = floor;
                    coordinator.push_stream_floor(&fanout, floor);
                }
            }
            StreamEvent::Message(None) => continue,
            StreamEvent::Message(Some((shard, message))) => {
                if summaries[shard].is_some() {
                    return fanout
                        .cancel_with(Status::internal(
                            "relay: child message after scan certificate",
                        ))
                        .await;
                }
                let payload = match message.payload {
                    Some(stream_search_response::Payload::IdentityReady(ready))
                        if identity_limits.is_some() =>
                    {
                        let summary = ready.scan.ok_or_else(|| {
                            Status::internal("relay: missing readiness certificate")
                        })?;
                        if let Some(range) = ready.range.as_ref() {
                            if range.first_id > range.last_id
                                || ranges.iter().flatten().any(|other| {
                                    range.first_id <= other.last_id
                                        && other.first_id <= range.last_id
                                })
                            {
                                return fanout
                                    .cancel_with(Status::failed_precondition(
                                        "relay: overlapping or invalid captured identity ranges",
                                    ))
                                    .await;
                            }
                        } else if summary.emitted != 0 {
                            return fanout
                                .cancel_with(Status::failed_precondition(
                                    "relay: candidates without a captured identity range",
                                ))
                                .await;
                        }
                        ranges[shard] = ready.range;
                        Some(stream_search_response::Payload::Summary(summary))
                    }
                    Some(stream_search_response::Payload::Summary(_))
                        if identity_limits.is_some() =>
                    {
                        return fanout
                            .cancel_with(Status::failed_precondition(
                                "relay: child lacks snapshot-bound identity support",
                            ))
                            .await;
                    }
                    payload => payload,
                };
                match payload {
                    Some(stream_search_response::Payload::Batch(batch)) => {
                        if batch.hits.is_empty() || batch.hits.len() % stride != 0 {
                            let status = Status::internal(format!(
                                "relay: child {shard} sent a batch of {} bytes, not a multiple of \
                             the {stride}-byte record",
                                batch.hits.len()
                            ));
                            return fanout.cancel_with(status).await;
                        }
                        if tx
                            .send(Ok(StreamSearchResponse {
                                payload: Some(stream_search_response::Payload::Batch(batch)),
                            }))
                            .await
                            .is_err()
                        {
                            // The parent is gone: nobody reads what the
                            // children still scan for.
                            fanout.cancel().await;
                            return Ok(());
                        }
                    }
                    Some(stream_search_response::Payload::Summary(summary)) => {
                        if !summary.completed {
                            let status = Status::internal(format!(
                                "relay: child {shard} stopped before completing its scan"
                            ));
                            return fanout.cancel_with(status).await;
                        }
                        if summary.scoring_fingerprint.is_empty() {
                            let status = Status::failed_precondition(format!(
                            "relay: child {shard} completed without a vector scoring fingerprint"
                        ));
                            return fanout.cancel_with(status).await;
                        }
                        match scoring_fingerprint.as_ref() {
                            Some(expected) if expected != &summary.scoring_fingerprint => {
                                let status = Status::failed_precondition(format!(
                                    "relay: child {shard} vector scoring fingerprint {} differs \
                                 from {expected}",
                                    summary.scoring_fingerprint
                                ));
                                return fanout.cancel_with(status).await;
                            }
                            None => scoring_fingerprint = Some(summary.scoring_fingerprint.clone()),
                            _ => {}
                        }
                        if summary.geo_columns_known.len() != geo_known.len()
                            || summary.filter_columns_known.len() != tree_known.len()
                        {
                            let status = Status::internal(format!(
                                "relay: child {shard} answered {} geo and {} filter flags for {} \
                             and {}",
                                summary.geo_columns_known.len(),
                                summary.filter_columns_known.len(),
                                geo_known.len(),
                                tree_known.len()
                            ));
                            return fanout.cancel_with(status).await;
                        }
                        for (acc, k) in geo_known.iter_mut().zip(&summary.geo_columns_known) {
                            *acc |= *k;
                        }
                        for (acc, k) in tree_known.iter_mut().zip(&summary.filter_columns_known) {
                            *acc |= *k;
                        }
                        totals.emitted += summary.emitted;
                        totals.blocks_scanned += summary.blocks_scanned;
                        totals.floor_raises_applied += summary.floor_raises_applied;
                        totals.segments_total =
                            totals.segments_total.saturating_add(summary.segments_total);
                        totals.segments_skipped = totals
                            .segments_skipped
                            .saturating_add(summary.segments_skipped);
                        totals
                            .regimes
                            .add(crate::vector::FilterRegimeCounts::from_proto(
                                summary.filter_regimes.as_ref(),
                            ));
                        totals.tail_enumerated_rows = totals
                            .tail_enumerated_rows
                            .saturating_add(summary.tail_enumerated_rows);
                        totals.patience_stopped = totals
                            .patience_stopped
                            .saturating_add(summary.patience_stopped);
                        totals.floor_pruned =
                            totals.floor_pruned.saturating_add(summary.floor_pruned);
                        totals.end_reason = totals.end_reason.merge(
                            crate::vector::VectorStreamEndReason::from_proto(summary.end_reason),
                        );
                        summaries[shard] = Some(summary.clone());
                        if identity_limits.is_none() {
                            terminal[shard] = Some(summary);
                            fanout.mark_completed(shard);
                        }
                        remaining -= 1;
                    }
                    _ => {
                        return fanout
                            .cancel_with(Status::internal("relay: unexpected identity message"))
                            .await
                    }
                }
            }
        }
    }
    let summary = StreamSearchSummary {
        completed: true,
        emitted: totals.emitted,
        blocks_scanned: totals.blocks_scanned,
        floor_raises_applied: totals.floor_raises_applied,
        geo_columns_known: geo_known,
        filter_columns_known: tree_known,
        // Every child agreed, or the loop above refused; a relay over
        // masked-out children only has no fingerprint to report and the
        // parent refuses that, as it refuses a node without one.
        scoring_fingerprint: scoring_fingerprint.unwrap_or_default(),
        segments_total: totals.segments_total,
        segments_skipped: totals.segments_skipped,
        filter_regimes: totals.regimes.to_proto(),
        tail_enumerated_rows: totals.tail_enumerated_rows,
        patience_stopped: totals.patience_stopped,
        end_reason: totals.end_reason.to_proto(),
        floor_pruned: totals.floor_pruned,
    };
    if let Some(limits) = identity_limits {
        let range =
            ranges
                .iter()
                .flatten()
                .fold(None::<crate::pb::StreamIdentityRange>, |acc, range| {
                    Some(match acc {
                        None => *range,
                        Some(acc) => crate::pb::StreamIdentityRange {
                            first_id: acc.first_id.min(range.first_id),
                            last_id: acc.last_id.max(range.last_id),
                        },
                    })
                });
        let exchange = async {
            identity_ready.store(true, Ordering::Release);
            tx.send(Ok(StreamSearchResponse {
                payload: Some(stream_search_response::Payload::IdentityReady(
                    crate::pb::StreamIdentityReady {
                        scan: Some(summary.clone()),
                        range,
                    },
                )),
            }))
            .await
            .map_err(|_| Status::cancelled("relay: parent response closed"))?;
            let selection = (&mut input)
                .await
                .map_err(|e| Status::internal(format!("relay: request task failed: {e}")))??
                .ok_or_else(|| Status::cancelled("relay: selection stopped"))?;
            if selection.vector_ids.len() > limits.max_rows as usize {
                return Err(Status::resource_exhausted(
                    "relay: identity selection exceeds max_rows",
                ));
            }
            let mut seen = std::collections::HashSet::new();
            let mut hits = Vec::with_capacity(selection.vector_ids.len());
            for id in selection.vector_ids {
                if !seen.insert(id) {
                    return Err(Status::invalid_argument(
                        "relay: identity selection repeats an ID",
                    ));
                }
                let shard = ranges
                    .iter()
                    .position(|range| {
                        range
                            .as_ref()
                            .is_some_and(|r| r.first_id <= id && id <= r.last_id)
                    })
                    .ok_or_else(|| {
                        Status::invalid_argument(
                            "relay: identity ID is outside captured child ranges",
                        )
                    })?;
                hits.push(crate::merge::MergedHit {
                    shard: shard as u32,
                    vector_id: id,
                    score: 0.0,
                });
            }
            let (mut resolved_identities, mut resolved_candidates) = fanout
                .resolve_identities_and_candidates(&hits, &summaries, &mut terminal, &limits)
                .await?;
            let rows = hits
                .into_iter()
                .map(|hit| {
                    let identity = resolved_identities
                        .remove(&(hit.shard, hit.vector_id))
                        .expect("validated child response");
                    let rerank_candidate = resolved_candidates
                        .remove(&(hit.shard, hit.vector_id))
                        .flatten();
                    crate::pb::StreamIdentity {
                        vector_id: hit.vector_id,
                        identity,
                        rerank_candidate,
                    }
                })
                .collect();
            let response = StreamSearchResponse {
                payload: Some(stream_search_response::Payload::Identities(
                    crate::pb::StreamIdentities { rows },
                )),
            };
            if prost::Message::encoded_len(&response) > limits.max_response_bytes as usize {
                return Err(Status::resource_exhausted(
                    "relay: identity response exceeds max_response_bytes",
                ));
            }
            tx.send(Ok(response))
                .await
                .map_err(|_| Status::cancelled("relay: parent response closed"))?;
            tx.send(Ok(StreamSearchResponse {
                payload: Some(stream_search_response::Payload::Summary(summary.clone())),
            }))
            .await
            .map_err(|_| Status::cancelled("relay: parent response closed"))
        };
        let result = tokio::select! {
            _ = signals.cancel.notified() => Err(Status::cancelled("relay: identity selection stopped")),
            _ = async {}, if signals.cancelled.load(Ordering::Acquire) => Err(Status::cancelled("relay: identity selection stopped")),
            _ = tx.closed() => Err(Status::cancelled("relay: parent response closed")),
            _ = async { sleep.as_mut().expect("guarded").await }, if sleep.is_some() => Err(Status::deadline_exceeded("relay: parent deadline passed during identity selection")),
            _ = async { loop {
                if map_changes.changed().await.is_err() { std::future::pending::<()>().await; }
                if *map_changes.borrow_and_update() != pinned.control_revision { break; }
            }} => Err(Status::failed_precondition("relay: map moved during identity selection")),
            result = tokio::time::timeout(Duration::from_millis(u64::from(limits.timeout_ms)), exchange) =>
                result.unwrap_or_else(|_| Err(Status::deadline_exceeded("relay: identity selection timed out"))),
        };
        if let Err(status) = result {
            fanout.cancel().await;
            if signals.cancelled.load(Ordering::Acquire) {
                let _ = tx
                    .send(Ok(StreamSearchResponse {
                        payload: Some(stream_search_response::Payload::Summary(
                            StreamSearchSummary {
                                completed: false,
                                end_reason: crate::pb::StreamEndReason::Abandoned as i32,
                                ..summary
                            },
                        )),
                    }))
                    .await;
                return Ok(());
            }
            return Err(status);
        }
    } else {
        let _ = tx
            .send(Ok(StreamSearchResponse {
                payload: Some(stream_search_response::Payload::Summary(summary)),
            }))
            .await;
    }
    Ok(())
}

// --- The keyword leg ---------------------------------------------------

/// A child's error as the parent must read it. The stale-epoch prefix
/// stays at the front so the parent's retry rule fires; everything else
/// gets the child named after the code.
pub(super) fn child_error(shard: usize, addr: &str, route: &str, status: Status) -> Status {
    let message = status.message();
    let text = match message.strip_prefix(STALE_STATS_EPOCH) {
        Some(rest) => format!(
            "{STALE_STATS_EPOCH}: relay child {shard} ({addr}) {route}{}",
            rest
        ),
        None => format!("relay child {shard} ({addr}) {route}: {message}"),
    };
    Status::new(status.code(), text)
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
            let filters = RequestFilters {
                geo: start.geo_filters.clone(),
                tree: start.filter.clone(),
                enumerate_max_rows: start.enumerate_max_rows,
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
            };
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
            // The parent's gRPC leg: floor raises and the authoritative
            // Stop. A closed leg before the summary means the parent is
            // gone, and the children are told so.
            let pump = Arc::clone(&signals);
            let identity_enabled = start.identity_limits.is_some();
            let identity_ready = Arc::new(AtomicBool::new(false));
            let pump_ready = Arc::clone(&identity_ready);
            let input = tokio::spawn(async move {
                loop {
                    match inbound.message().await {
                        Ok(Some(StreamSearchRequest {
                            payload: Some(stream_search_request::Payload::FloorUpdate(u)),
                        })) => pump.raise(u.floor),
                        Ok(Some(StreamSearchRequest {
                            payload: Some(stream_search_request::Payload::Stop(_)),
                        })) => {
                            pump.cancel();
                            return Ok(None);
                        }
                        Ok(Some(StreamSearchRequest {
                            payload: Some(stream_search_request::Payload::Finish(f)),
                        })) => {
                            pump.request_finish(f.deadline);
                        }
                        Ok(Some(StreamSearchRequest {
                            payload:
                                Some(stream_search_request::Payload::ResolveIdentities(selection)),
                        })) => {
                            if !identity_enabled || !pump_ready.load(Ordering::Acquire) {
                                pump.cancel();
                                return Err(Status::failed_precondition(
                                    "relay: identity selection requires IdentityReady",
                                ));
                            }
                            return Ok(Some(selection));
                        }
                        Ok(Some(_)) => {}
                        Ok(None) | Err(_) => {
                            if identity_enabled {
                                pump.cancel();
                            }
                            return Ok(None);
                        }
                    }
                }
            });
            let (tx, rx) = mpsc::channel::<Result<StreamSearchResponse, Status>>(64);
            let (pinned, frozen) = self.pin();
            let map_changes = self.inner.map.changes();
            let registry = Arc::clone(&self.signals);
            let loop_signals = Arc::clone(&signals);
            let input_abort = input.abort_handle();
            tokio::spawn(async move {
                let mut probe = frozen.shard_vector_row_probe();
                let result = {
                    let work = relay_stream(
                        frozen,
                        pinned,
                        map_changes,
                        start,
                        filters,
                        loop_signals,
                        tx.clone(),
                        deadline,
                        input,
                        identity_ready,
                        &mut probe,
                    );
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
            });
            Ok(Response::new(ReceiverStream::new(rx)))
        })
        .await
    }
}
