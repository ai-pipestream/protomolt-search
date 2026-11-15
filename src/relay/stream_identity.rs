//! The snapshot-bound identity phase of a relayed `StreamSearch`: once
//! every child is ready, the relay announces its merged range, takes the
//! parent's selection, resolves it through the children, and answers.

use std::collections::HashSet;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::mpsc;
use tokio::time::Sleep;
use tonic::Status;

use crate::coordinator::StreamFanout;
use crate::merge::MergedHit;
use crate::pb::{
    stream_search_response, StreamIdentityLimits, StreamIdentityRange, StreamSearchResponse,
    StreamSearchSummary,
};

use super::stream_state::{send_summary, ForwardState, SelectionTask, StreamArgs};

type ParentTx = mpsc::Sender<Result<StreamSearchResponse, Status>>;

fn parent_closed() -> Status {
    Status::cancelled("relay: parent response closed")
}

/// The identity phase's borrowed loop state.
pub(super) struct IdentityRun<'a> {
    pub(super) state: &'a mut ForwardState,
    pub(super) fanout: &'a mut StreamFanout,
    pub(super) sleep: &'a mut Option<Pin<Box<Sleep>>>,
    pub(super) limits: StreamIdentityLimits,
}

impl IdentityRun<'_> {
    /// Run the exchange under the parent's cancel, its closed response,
    /// the deadline, a map move, and the phase's own timeout. A cancel
    /// answers an abandoned summary; anything else that stops the phase
    /// fails the stream.
    pub(super) async fn finish(
        self,
        args: &mut StreamArgs,
        summary: StreamSearchSummary,
    ) -> Result<(), Status> {
        let IdentityRun {
            state,
            fanout,
            sleep,
            limits,
        } = self;
        let StreamArgs {
            pinned,
            map_changes,
            signals,
            tx,
            input,
            identity_ready,
            ..
        } = args;
        let budget = Duration::from_millis(u64::from(limits.timeout_ms));
        let exchange = async {
            identity_ready.store(true, Ordering::Release);
            exchange(tx, input, state, fanout, &limits, &summary).await
        };
        let stopped = || Err(Status::cancelled("relay: identity selection stopped"));
        let result = tokio::select! {
            _ = signals.cancel.notified() => stopped(),
            _ = async {}, if signals.cancelled.load(Ordering::Acquire) => stopped(),
            _ = tx.closed() => Err(parent_closed()),
            _ = async { sleep.as_mut().expect("guarded").await }, if sleep.is_some() => Err(Status::deadline_exceeded("relay: parent deadline passed during identity selection")),
            _ = async { loop {
                if map_changes.changed().await.is_err() { std::future::pending::<()>().await; }
                if *map_changes.borrow_and_update() != pinned.control_revision { break; }
            }} => Err(Status::failed_precondition("relay: map moved during identity selection")),
            result = tokio::time::timeout(budget, exchange) =>
                result.unwrap_or_else(|_| Err(Status::deadline_exceeded("relay: identity selection timed out"))),
        };
        let Err(status) = result else {
            return Ok(());
        };
        fanout.cancel().await;
        if signals.cancelled.load(Ordering::Acquire) {
            let abandoned = StreamSearchSummary {
                completed: false,
                end_reason: crate::pb::StreamEndReason::Abandoned as i32,
                ..summary
            };
            send_summary(tx, abandoned).await;
            return Ok(());
        }
        Err(status)
    }
}

/// The exchange itself: ready, selection, resolution, identities, summary.
async fn exchange(
    tx: &ParentTx,
    input: &mut SelectionTask,
    state: &mut ForwardState,
    fanout: &mut StreamFanout,
    limits: &StreamIdentityLimits,
    summary: &StreamSearchSummary,
) -> Result<(), Status> {
    let ready = crate::pb::StreamIdentityReady {
        scan: Some(summary.clone()),
        range: merged_range(&state.ranges),
    };
    tx.send(Ok(StreamSearchResponse {
        payload: Some(stream_search_response::Payload::IdentityReady(ready)),
    }))
    .await
    .map_err(|_| parent_closed())?;
    let selection = input
        .await
        .map_err(|e| Status::internal(format!("relay: request task failed: {e}")))??
        .ok_or_else(|| Status::cancelled("relay: selection stopped"))?;
    if selection.vector_ids.len() > limits.max_rows as usize {
        return Err(Status::resource_exhausted(
            "relay: identity selection exceeds max_rows",
        ));
    }
    let hits = selection_hits(&selection.vector_ids, &state.ranges)?;
    let (identities, candidates) = fanout
        .resolve_identities_and_candidates(&hits, &state.summaries, &mut state.terminal, limits)
        .await?;
    let response = identities_response(hits, identities, candidates);
    if prost::Message::encoded_len(&response) > limits.max_response_bytes as usize {
        return Err(Status::resource_exhausted(
            "relay: identity response exceeds max_response_bytes",
        ));
    }
    tx.send(Ok(response)).await.map_err(|_| parent_closed())?;
    tx.send(Ok(StreamSearchResponse {
        payload: Some(stream_search_response::Payload::Summary(summary.clone())),
    }))
    .await
    .map_err(|_| parent_closed())
}

/// The one range covering every child's captured range.
fn merged_range(ranges: &[Option<StreamIdentityRange>]) -> Option<StreamIdentityRange> {
    ranges
        .iter()
        .flatten()
        .fold(None::<StreamIdentityRange>, |acc, range| {
            Some(match acc {
                None => *range,
                Some(acc) => StreamIdentityRange {
                    first_id: acc.first_id.min(range.first_id),
                    last_id: acc.last_id.max(range.last_id),
                },
            })
        })
}

/// The parent's selection routed to the child whose captured range
/// holds each id; a repeat or an id outside every range refuses.
fn selection_hits(
    ids: &[u64],
    ranges: &[Option<StreamIdentityRange>],
) -> Result<Vec<MergedHit>, Status> {
    let mut seen = HashSet::new();
    let mut hits = Vec::with_capacity(ids.len());
    for &id in ids {
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
                Status::invalid_argument("relay: identity ID is outside captured child ranges")
            })?;
        hits.push(MergedHit {
            shard: shard as u32,
            vector_id: id,
            score: 0.0,
        });
    }
    Ok(hits)
}

type Resolved<T> = std::collections::HashMap<(u32, u64), Option<T>>;

fn identities_response(
    hits: Vec<MergedHit>,
    mut identities: Resolved<crate::pb::DocumentIdentity>,
    mut candidates: Resolved<crate::pb::RerankCandidatePayload>,
) -> StreamSearchResponse {
    let rows = hits
        .into_iter()
        .map(|hit| crate::pb::StreamIdentity {
            vector_id: hit.vector_id,
            identity: identities
                .remove(&(hit.shard, hit.vector_id))
                .expect("validated child response"),
            rerank_candidate: candidates.remove(&(hit.shard, hit.vector_id)).flatten(),
        })
        .collect();
    StreamSearchResponse {
        payload: Some(stream_search_response::Payload::Identities(
            crate::pb::StreamIdentities { rows },
        )),
    }
}
