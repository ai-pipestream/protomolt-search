//! The forwarding loop of one relayed `StreamSearch`: children's batches
//! go up untouched, the parent's floor and cancel go down, and the
//! relay's summary follows the last child's.

use std::pin::Pin;
use std::sync::atomic::Ordering;

use tokio::sync::watch;
use tokio::time::Sleep;
use tonic::Status;

use crate::coordinator::{RequestFilters, ShardVectorRowProbe, StreamFanout};
use crate::pb::{stream_search_response, StreamSearchResponse, StreamSearchSummary};

use super::stream_search::{await_vector_rows, sync_map_changes};
use super::stream_state::{send_summary, ForwardState, StreamArgs};

enum StreamEvent {
    Message(Option<(usize, StreamSearchResponse)>),
    Failed(Status),
    Floor(f32),
    Finish(bool),
    Cancel,
    Deadline,
    MapMoved(u64),
}

/// What the loop does after an event.
enum Step {
    Continue,
    /// The stream is over and already answered (cancelled, or the
    /// parent is gone); the relay returns `Ok`.
    Stop,
}

/// The loop's lanes: the parent's floor, its finish requests, the map,
/// and the deadline.
pub(super) struct Lanes {
    floor_rx: watch::Receiver<f32>,
    floor_open: bool,
    map_open: bool,
    last_floor: f32,
    forwarded_finish: u8,
    pub(super) sleep: Option<Pin<Box<Sleep>>>,
}

/// One relayed `StreamSearch`, end to end.
pub(super) async fn relay_stream(
    mut args: StreamArgs,
    probe: &mut ShardVectorRowProbe,
) -> Result<(), Status> {
    sync_map_changes(&mut args.map_changes, &args.pinned);
    let n = args.coordinator.node_addresses().len();
    if n == 0 {
        return Err(Status::failed_precondition(
            "relay: a relay coordinator has no children",
        ));
    }
    if let Some(limits) = args.start.identity_limits.as_ref() {
        crate::query_identity::validate_limits(limits)?;
    }
    let mut map_open = true;
    if let Some(end_reason) = await_vector_rows(
        probe,
        &args.pinned,
        &mut args.map_changes,
        &mut map_open,
        Some(&args.signals),
        "StreamSearch",
    )
    .await?
    {
        send_summary(&args.tx, discovery_summary(&args.filters, end_reason)).await;
        return Ok(());
    }
    let mask = args.coordinator.shard_mask(args.filters.tree.as_ref());
    let mut fanout = open_fanout(&args)?;
    let mut state = ForwardState::new(&args, mask.as_ref(), n);
    let mut lanes = Lanes {
        floor_rx: args.signals.floor.subscribe(),
        floor_open: true,
        map_open,
        last_floor: args.start.initial_floor.unwrap_or(f32::NEG_INFINITY),
        forwarded_finish: 0,
        sleep: args
            .deadline
            .map(|at| Box::pin(tokio::time::sleep_until(at))),
    };
    while state.remaining > 0 {
        let event = next_event(&mut lanes, &mut args, &mut fanout, &state.terminal).await;
        match on_event(event, &mut lanes, &args, &mut state, &mut fanout).await? {
            Step::Continue => {}
            Step::Stop => return Ok(()),
        }
    }
    let summary = state.summary(true, state.totals.end_reason.to_proto());
    match args.start.identity_limits {
        Some(limits) => {
            let run = super::stream_identity::IdentityRun {
                state: &mut state,
                fanout: &mut fanout,
                sleep: &mut lanes.sleep,
                limits,
            };
            run.finish(&mut args, summary).await
        }
        None => {
            send_summary(&args.tx, summary).await;
            Ok(())
        }
    }
}

/// The summary a stream answers when capability discovery ended it
/// before any child was opened.
fn discovery_summary(
    filters: &RequestFilters,
    end_reason: crate::pb::StreamEndReason,
) -> StreamSearchSummary {
    StreamSearchSummary {
        completed: false,
        geo_columns_known: vec![false; filters.geo.len()],
        filter_columns_known: vec![
            false;
            filters.tree.as_ref().map_or(0, crate::filter::leaf_count)
        ],
        end_reason: end_reason as i32,
        ..Default::default()
    }
}

fn open_fanout(args: &StreamArgs) -> Result<StreamFanout, Status> {
    let start = &args.start;
    args.coordinator.open_stream_fanout_with_identities(
        &start.request_id,
        &start.vector,
        start.initial_floor,
        start.candidate_depth,
        start.collapse_parents,
        &args.filters,
        start.identity_limits,
    )
}

/// The next thing the loop reacts to, in priority order: the parent's
/// cancel, its finish, the deadline, a map move, a floor raise, a child.
async fn next_event(
    lanes: &mut Lanes,
    args: &mut StreamArgs,
    fanout: &mut StreamFanout,
    terminal: &[Option<StreamSearchSummary>],
) -> StreamEvent {
    let signals = &args.signals;
    loop {
        return tokio::select! {
            biased;
            _ = signals.cancel.notified(), if !signals.cancelled.load(Ordering::Acquire) => StreamEvent::Cancel,
            _ = async { true }, if signals.cancelled.load(Ordering::Acquire) => StreamEvent::Cancel,
            _ = signals.finish.notified(), if signals.finish_state_raw() <= lanes.forwarded_finish => {
                let raw = signals.finish_state_raw();
                if raw > lanes.forwarded_finish {
                    StreamEvent::Finish(raw >= 2)
                } else {
                    continue;
                }
            },
            _ = async { true }, if signals.finish_state_raw() > lanes.forwarded_finish => {
                StreamEvent::Finish(signals.finish_state_raw() >= 2)
            },
            _ = async { lanes.sleep.as_mut().expect("guarded").await }, if lanes.sleep.is_some() => StreamEvent::Deadline,
            moved = args.map_changes.changed(), if lanes.map_open => match moved {
                Ok(()) => StreamEvent::MapMoved(*args.map_changes.borrow_and_update()),
                Err(_) => {
                    // The source is gone with its authority; nothing
                    // will move again, and the pinned reading stands.
                    lanes.map_open = false;
                    continue;
                }
            },
            changed = lanes.floor_rx.changed(), if lanes.floor_open => match changed {
                Ok(()) => StreamEvent::Floor(*lanes.floor_rx.borrow_and_update()),
                Err(_) => {
                    lanes.floor_open = false;
                    continue;
                }
            },
            message = fanout.next_message(terminal) => match message {
                Ok(pair) => StreamEvent::Message(pair),
                Err(status) => StreamEvent::Failed(status),
            },
        };
    }
}

async fn on_event(
    event: StreamEvent,
    lanes: &mut Lanes,
    args: &StreamArgs,
    state: &mut ForwardState,
    fanout: &mut StreamFanout,
) -> Result<Step, Status> {
    match event {
        StreamEvent::Cancel => {
            fanout.cancel().await;
            let abandoned = crate::pb::StreamEndReason::Abandoned as i32;
            send_summary(&args.tx, state.summary(false, abandoned)).await;
            Ok(Step::Stop)
        }
        StreamEvent::Finish(deadline) => {
            lanes.forwarded_finish = lanes.forwarded_finish.max(if deadline { 2 } else { 1 });
            args.coordinator
                .push_stream_finish(fanout, &state.summaries, deadline)
                .await;
            Ok(Step::Continue)
        }
        StreamEvent::Deadline => {
            fanout
                .cancel_with(Status::deadline_exceeded(
                    "relay: the parent's deadline passed before every child completed",
                ))
                .await
        }
        StreamEvent::MapMoved(revision) if revision == args.pinned.control_revision => {
            Ok(Step::Continue)
        }
        StreamEvent::MapMoved(revision) => {
            let pinned = &args.pinned;
            fanout
                .cancel_with(Status::failed_precondition(format!(
                    "relay: the shard map moved from revision {} (generation {}) to \
                     revision {revision} during StreamSearch; the stream was opened under \
                     the older map and is not completed; retry under the current one",
                    pinned.control_revision, pinned.topology_generation
                )))
                .await
        }
        StreamEvent::Floor(floor) => {
            if floor > lanes.last_floor {
                lanes.last_floor = floor;
                args.coordinator.push_stream_floor(fanout, floor);
            }
            Ok(Step::Continue)
        }
        StreamEvent::Failed(status) => fanout.cancel_with(status).await,
        StreamEvent::Message(None) => Ok(Step::Continue),
        StreamEvent::Message(Some((shard, message))) => {
            match on_child_message(args, state, fanout, shard, message).await {
                Ok(step) => Ok(step),
                Err(status) => fanout.cancel_with(status).await,
            }
        }
    }
}

/// One child's message: a batch forwarded untouched, a terminal
/// summary checked and summed. An error cancels every child.
async fn on_child_message(
    args: &StreamArgs,
    state: &mut ForwardState,
    fanout: &mut StreamFanout,
    shard: usize,
    message: StreamSearchResponse,
) -> Result<Step, Status> {
    if state.summaries[shard].is_some() {
        return Err(Status::internal(
            "relay: child message after scan certificate",
        ));
    }
    let payload = match message.payload {
        Some(stream_search_response::Payload::IdentityReady(ready)) if state.identity => Some(
            stream_search_response::Payload::Summary(state.accept_ready(shard, ready)?),
        ),
        Some(stream_search_response::Payload::Summary(_)) if state.identity => {
            return Err(Status::failed_precondition(
                "relay: child lacks snapshot-bound identity support",
            ));
        }
        payload => payload,
    };
    match payload {
        Some(stream_search_response::Payload::Batch(batch)) => {
            forward_batch(args, state.stride, fanout, shard, batch).await
        }
        Some(stream_search_response::Payload::Summary(summary)) => {
            state.accept_summary(shard, &summary)?;
            state.summaries[shard] = Some(summary.clone());
            if !state.identity {
                state.terminal[shard] = Some(summary);
                fanout.mark_completed(shard);
            }
            state.remaining -= 1;
            Ok(Step::Continue)
        }
        _ => Err(Status::internal("relay: unexpected identity message")),
    }
}

async fn forward_batch(
    args: &StreamArgs,
    stride: usize,
    fanout: &mut StreamFanout,
    shard: usize,
    batch: crate::pb::StreamSearchBatch,
) -> Result<Step, Status> {
    if batch.hits.is_empty() || !batch.hits.len().is_multiple_of(stride) {
        return Err(Status::internal(format!(
            "relay: child {shard} sent a batch of {} bytes, not a multiple of the \
             {stride}-byte record",
            batch.hits.len()
        )));
    }
    let sent = args
        .tx
        .send(Ok(StreamSearchResponse {
            payload: Some(stream_search_response::Payload::Batch(batch)),
        }))
        .await;
    if sent.is_err() {
        // The parent is gone: nobody reads what the children still scan for.
        fanout.cancel().await;
        return Ok(Step::Stop);
    }
    Ok(Step::Continue)
}
