//! The streamed BM25 relay: children's candidate batches go up untouched,
//! their cutoff raises go up monotone, the parent's raises and its stop go
//! down, and the relay's completion follows the last child's.

use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::coordinator::CoordinatorServiceImpl;
use crate::metrics::Route;
use crate::pb::node_service_server::NodeService;
use crate::pb::{
    bm25_query_stream_request, bm25_query_stream_response, Bm25QueryRequest,
    Bm25QueryStreamRequest, Bm25QueryStreamResponse, Bm25StreamCompletion,
};
use crate::stats_identity::StatsClaim;

use super::bm25_legs::ChildLegs;
use super::map::MapSnapshot;
use super::merge_lexical::{merge_bm25_responses, refuse_bm25_aggregates};
use super::stream_search::sync_map_changes;
use super::{grpc_timeout, RelayService};

type ParentTx = mpsc::Sender<Result<Bm25QueryStreamResponse, Status>>;
pub(super) type ChildItem = (usize, Result<Option<Bm25QueryStreamResponse>, Status>);

/// Everything one relayed `Bm25QueryStream` runs on.
pub(super) struct Bm25Args {
    pub(super) frozen: CoordinatorServiceImpl,
    pub(super) pinned: MapSnapshot,
    pub(super) map_changes: watch::Receiver<u64>,
    pub(super) req: Bm25QueryRequest,
    pub(super) claims: Vec<StatsClaim>,
    pub(super) inbound: Streaming<Bm25QueryStreamRequest>,
    pub(super) tx: ParentTx,
    pub(super) deadline: Option<tokio::time::Instant>,
    pub(super) timeout: Option<Duration>,
}

#[allow(clippy::large_enum_variant)]
enum Bm25Event {
    Child(usize, Result<Option<Bm25QueryStreamResponse>, Status>),
    Parent(Option<Result<Bm25QueryStreamRequest, Status>>),
    Deadline,
    MapMoved(u64),
}

/// What the loop has seen so far.
struct Progress {
    completions: Vec<Option<Bm25StreamCompletion>>,
    remaining: usize,
    forwarded: u64,
    last_up: f32,
    last_down: f32,
    fingerprint: Option<String>,
    parent_open: bool,
    map_open: bool,
}

impl Progress {
    fn new(n: usize) -> Self {
        Progress {
            completions: vec![None; n],
            remaining: n,
            forwarded: 0,
            last_up: f32::NEG_INFINITY,
            last_down: f32::NEG_INFINITY,
            fingerprint: None,
            parent_open: true,
            map_open: true,
        }
    }

    /// The incomplete certificate a parent's stop is answered with.
    fn stopped(&self) -> Bm25StreamCompletion {
        Bm25StreamCompletion {
            completed: false,
            response: None,
            scoring_fingerprint: self.fingerprint.clone().unwrap_or_default(),
            candidates_emitted: self.forwarded,
        }
    }
}

async fn send_up(
    tx: &ParentTx,
    payload: bm25_query_stream_response::Payload,
) -> Result<(), Status> {
    tx.send(Ok(Bm25QueryStreamResponse {
        payload: Some(payload),
    }))
    .await
    .map_err(|_| Status::cancelled("relay: parent response closed"))
}

impl RelayService {
    /// The forwarding loop of one relayed `Bm25QueryStream`.
    async fn relay_bm25_stream(&self, mut args: Bm25Args) -> Result<(), Status> {
        sync_map_changes(&mut args.map_changes, &args.pinned);
        let (events_tx, mut events_rx) = mpsc::channel::<ChildItem>(64);
        let mut legs = ChildLegs::open(&args, events_tx).await?;
        let mut progress = Progress::new(legs.children.len());
        let outcome = drive(&mut args, &mut legs, &mut progress, &mut events_rx).await;
        let completion = match outcome {
            Ok(Some(incomplete)) => incomplete,
            Ok(None) => {
                self.still_current(&args.pinned, "Bm25QueryStream")?;
                certify(&args.req, &legs.children, progress)?
            }
            Err(status) => {
                legs.stop().await;
                return Err(status);
            }
        };
        drop(legs);
        send_up(
            &args.tx,
            bm25_query_stream_response::Payload::Completion(completion),
        )
        .await
    }

    pub(super) async fn handle_bm25_query_stream(
        &self,
        request: Request<Streaming<Bm25QueryStreamRequest>>,
    ) -> Result<Response<<Self as NodeService>::Bm25QueryStreamStream>, Status> {
        crate::metrics::timed_stream(Route::Bm25QueryStream, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let deadline = timeout.map(|d| tokio::time::Instant::now() + d);
            let mut inbound = request.into_inner();
            let req = match inbound.message().await? {
                Some(Bm25QueryStreamRequest {
                    payload: Some(bm25_query_stream_request::Payload::Start(req)),
                }) => req,
                _ => {
                    return Err(Status::invalid_argument(
                        "first Bm25QueryStreamRequest must be a Bm25QueryRequest start",
                    ))
                }
            };
            refuse_bm25_aggregates(
                "Bm25QueryStream",
                &req.stats_fields,
                &req.cardinality_fields,
            )?;
            let (pinned, frozen) = self.pin();
            let claims = self.child_claims(
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
                frozen.node_addresses().len(),
            )?;
            if frozen.node_addresses().is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            let (tx, rx) = mpsc::channel::<Result<Bm25QueryStreamResponse, Status>>(64);
            let args = Bm25Args {
                frozen,
                pinned,
                map_changes: self.inner.map.changes(),
                req,
                claims,
                inbound,
                tx: tx.clone(),
                deadline,
                timeout,
            };
            let relay = self.clone();
            tokio::spawn(async move {
                let result = tokio::select! {
                    _ = tx.closed() => Err(Status::cancelled("relay: parent response closed")),
                    result = relay.relay_bm25_stream(args) => result,
                };
                if let Err(status) = result {
                    let _ = tokio::time::timeout(Duration::from_millis(250), tx.send(Err(status)))
                        .await;
                }
            });
            Ok(Response::new(ReceiverStream::new(rx)))
        })
        .await
    }
}

/// Run events until every child completed (`Ok(None)`), the parent
/// stopped (`Ok(Some(incomplete))`), or something failed.
async fn drive(
    args: &mut Bm25Args,
    legs: &mut ChildLegs,
    progress: &mut Progress,
    events: &mut mpsc::Receiver<ChildItem>,
) -> Result<Option<Bm25StreamCompletion>, Status> {
    let mut sleep = args
        .deadline
        .map(|at| Box::pin(tokio::time::sleep_until(at)));
    while progress.remaining > 0 {
        let event = tokio::select! {
            biased;
            _ = async { sleep.as_mut().expect("guarded").await }, if sleep.is_some() => Bm25Event::Deadline,
            moved = args.map_changes.changed(), if progress.map_open => match moved {
                Ok(()) => Bm25Event::MapMoved(*args.map_changes.borrow_and_update()),
                Err(_) => {
                    // The source is gone; the pinned reading stands.
                    progress.map_open = false;
                    continue;
                }
            },
            message = args.inbound.message(), if progress.parent_open => Bm25Event::Parent(message.transpose()),
            event = events.recv() => match event {
                Some((shard, item)) => Bm25Event::Child(shard, item),
                None => return Err(Status::internal("relay: every child reader ended before completion")),
            },
        };
        if let Some(stopped) = on_event(event, args, legs, progress).await? {
            return Ok(Some(stopped));
        }
    }
    Ok(None)
}

async fn on_event(
    event: Bm25Event,
    args: &Bm25Args,
    legs: &mut ChildLegs,
    progress: &mut Progress,
) -> Result<Option<Bm25StreamCompletion>, Status> {
    use bm25_query_stream_request::Payload as Down;
    match event {
        Bm25Event::Deadline => Err(Status::deadline_exceeded(
            "relay: the parent's deadline passed before every child completed",
        )),
        Bm25Event::MapMoved(revision) if revision == args.pinned.control_revision => Ok(None),
        Bm25Event::MapMoved(revision) => Err(Status::failed_precondition(format!(
            "relay: the shard map moved from revision {} (generation {}) to \
             revision {revision} during Bm25QueryStream; the stream was opened \
             under the older map and is not completed; retry under the current one",
            args.pinned.control_revision, args.pinned.topology_generation
        ))),
        Bm25Event::Parent(Some(Ok(Bm25QueryStreamRequest {
            payload: Some(Down::FloorUpdate(u)),
        }))) => {
            if !u.floor.is_nan() && u.floor > progress.last_down {
                progress.last_down = u.floor;
                legs.raise(u.floor).await;
            }
            Ok(None)
        }
        Bm25Event::Parent(Some(Ok(Bm25QueryStreamRequest {
            payload: Some(Down::Stop(_)),
        }))) => {
            // The parent's stop: the children are told, and the relay
            // certifies an incomplete scan without waiting on children
            // that may hold their answer.
            legs.stop().await;
            Ok(Some(progress.stopped()))
        }
        Bm25Event::Parent(Some(Ok(_))) => Ok(None),
        Bm25Event::Parent(Some(Err(_))) | Bm25Event::Parent(None) => {
            // The parent's leg closed: gone, or done sending. The
            // children finish on their own unless the response side is
            // closed too, which `tx.closed()` in the caller turns into a
            // cancel.
            progress.parent_open = false;
            Ok(None)
        }
        Bm25Event::Child(_, Err(status)) => Err(status),
        Bm25Event::Child(shard, Ok(None)) => {
            if progress.completions[shard].is_none() {
                return Err(Status::data_loss(format!(
                    "relay child {shard} ({}): BM25 stream ended without a completion \
                     certificate",
                    legs.children[shard]
                )));
            }
            Ok(None)
        }
        Bm25Event::Child(shard, Ok(Some(message))) => {
            on_child(&args.tx, legs, progress, shard, message).await?;
            Ok(None)
        }
    }
}

async fn on_child(
    tx: &ParentTx,
    legs: &mut ChildLegs,
    progress: &mut Progress,
    shard: usize,
    message: Bm25QueryStreamResponse,
) -> Result<(), Status> {
    use bm25_query_stream_response::Payload as Up;
    let addr = legs.children[shard].clone();
    if progress.completions[shard].is_some() {
        return Err(Status::internal(format!(
            "relay child {shard} ({addr}): message after its completion"
        )));
    }
    match message.payload {
        Some(Up::CandidateBatch(batch)) => {
            if batch.candidates.len() % 12 != 0 {
                return Err(Status::data_loss(format!(
                    "relay child {shard} ({addr}): BM25 candidate batch has {} bytes, not \
                     12-byte records",
                    batch.candidates.len()
                )));
            }
            progress.forwarded += (batch.candidates.len() / 12) as u64;
            send_up(tx, Up::CandidateBatch(batch)).await
        }
        Some(Up::FloorUpdate(u)) => {
            if !u.floor.is_nan() && u.floor > progress.last_up {
                progress.last_up = u.floor;
                send_up(tx, Up::FloorUpdate(u)).await?;
            }
            Ok(())
        }
        Some(Up::Completion(completion)) => {
            accept_completion(progress, shard, &addr, &completion)?;
            progress.completions[shard] = Some(completion);
            legs.legs[shard] = None;
            progress.remaining -= 1;
            Ok(())
        }
        None => Ok(()),
    }
}

/// A child's completion must carry the one score space's fingerprint.
fn accept_completion(
    progress: &mut Progress,
    shard: usize,
    addr: &str,
    completion: &Bm25StreamCompletion,
) -> Result<(), Status> {
    if completion.scoring_fingerprint.is_empty() {
        return Err(Status::data_loss(format!(
            "relay child {shard} ({addr}): BM25 completion omitted its scoring fingerprint"
        )));
    }
    match progress.fingerprint.as_ref() {
        Some(seen) if seen != &completion.scoring_fingerprint => {
            Err(Status::failed_precondition(format!(
                "relay child {shard} ({addr}): scoring fingerprint {} differs from {seen}; \
                 one relay serves one score space",
                completion.scoring_fingerprint
            )))
        }
        None => {
            progress.fingerprint = Some(completion.scoring_fingerprint.clone());
            Ok(())
        }
        _ => Ok(()),
    }
}

/// The relay's certificate over every child's: completed only when every
/// child completed, the emitted count matching what was forwarded, and
/// the children's responses merged exactly.
fn certify(
    req: &Bm25QueryRequest,
    children: &[String],
    progress: Progress,
) -> Result<Bm25StreamCompletion, Status> {
    let mut completed = true;
    let mut emitted: u64 = 0;
    let mut responses = Vec::with_capacity(children.len());
    for (shard, completion) in progress.completions.into_iter().enumerate() {
        let completion = completion.expect("remaining reached zero");
        completed &= completion.completed;
        emitted = emitted
            .checked_add(completion.candidates_emitted)
            .ok_or_else(|| Status::internal("relay: candidates_emitted overflows u64"))?;
        match completion.response {
            Some(response) => responses.push(response),
            None if completion.completed => {
                return Err(Status::data_loss(format!(
                    "relay child {shard} ({}): BM25 completion omitted its response",
                    children[shard]
                )));
            }
            None => {}
        }
    }
    let forwarded = progress.forwarded;
    if emitted != forwarded {
        return Err(Status::data_loss(format!(
            "relay: children certified {emitted} candidates but {forwarded} were forwarded"
        )));
    }
    let response = if completed {
        Some(merge_bm25_responses(req, responses)?)
    } else {
        None
    };
    Ok(Bm25StreamCompletion {
        completed,
        response,
        scoring_fingerprint: progress.fingerprint.unwrap_or_default(),
        candidates_emitted: forwarded,
    })
}
