//! The chunked shard-search relay: one child stream per child the
//! placement mask keeps, the parent's floor raises forwarded to every
//! child, each child's raises forwarded to the parent (a child's k-th best
//! is a lower bound on the relay's, so the parent's running maximum only
//! tightens), and the children's terminal lists concatenated in score
//! order. The relay keeps no heap: the parent's merge picks the top-k
//! from the union exactly as it does over leaves, and with `tie_complete`
//! the union holds every child's boundary tie group, so the cascade's
//! score-defined pool is the same set.

use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::coordinator::{CoordinatorServiceImpl, RequestFilters, ShardVectorRowProbe};
use crate::metrics::Route;
use crate::pb::node_service_server::NodeService;
use crate::pb::{
    search_shard_request, search_shard_response, SearchShardRequest, SearchShardResponse,
};

use super::map::MapSnapshot;
use super::merge_util::{by_score_then_id, KnownFlags};
use super::shard_scan::{map_moved, on_shard_event, ShardEvent, ShardScan};
use super::stream_search::{await_vector_rows, child_error, sync_map_changes};
use super::{grpc_timeout, RelayService};

type ParentTx = mpsc::Sender<Result<SearchShardResponse, Status>>;

/// Everything one relayed `SearchShard` runs on.
struct ShardArgs {
    coordinator: CoordinatorServiceImpl,
    pinned: MapSnapshot,
    map_changes: watch::Receiver<u64>,
    start: crate::pb::StartShardSearch,
    filters: RequestFilters,
    /// The parent's request leg, taken once its reader is spawned.
    inbound: Option<Streaming<SearchShardRequest>>,
    tx: ParentTx,
    deadline: Option<tokio::time::Instant>,
}

/// The children's stream and the parent's leg of one relayed
/// `SearchShard`, dropped together: a return on any path closes the
/// children's request channels and stops the readers.
struct ShardLegs {
    senders: Vec<Option<mpsc::Sender<SearchShardRequest>>>,
    readers: Vec<tokio::task::JoinHandle<()>>,
    parent: Option<tokio::task::JoinHandle<()>>,
}

impl Drop for ShardLegs {
    fn drop(&mut self) {
        for reader in &self.readers {
            reader.abort();
        }
        if let Some(parent) = &self.parent {
            parent.abort();
        }
    }
}

/// The `StartShardSearch` one child is sent: the parent's start with the
/// child's own filter tree and floor share.
fn child_start(
    start: &crate::pb::StartShardSearch,
    filters: &RequestFilters,
    mask: Option<&crate::placement::ShardMask>,
    child: usize,
    floor_share_ppm: Option<u32>,
) -> crate::pb::StartShardSearch {
    crate::pb::StartShardSearch {
        read_context: None,
        request_id: start.request_id.clone(),
        k: start.k,
        vector: start.vector.clone(),
        tie_complete: start.tie_complete,
        collapse_parents: start.collapse_parents,
        geo_filters: filters.geo.clone(),
        filter: CoordinatorServiceImpl::shard_filter_tree(filters, mask, child),
        enumerate_max_rows: filters.enumerate_max_rows,
        filter_strategy: filters.filter_strategy,
        enumerate_all: filters.enumerate_all,
        patience_saturation_ppm: filters.patience_saturation_ppm,
        patience_hops: filters.patience_hops,
        floor_prune: filters.floor_prune,
        floor_k: filters.floor_k.or(Some(start.k)),
        floor_share_ppm,
        floor_greediness_ppm: filters.floor_greediness_ppm,
        floor_clamp_min: filters.floor_clamp_min,
        include_rerank_candidate: start.include_rerank_candidate,
        exact_fp32_scan: start.exact_fp32_scan,
    }
}

fn spawn_child_reader(
    mut link: crate::link::NodeLink,
    req_rx: mpsc::Receiver<SearchShardRequest>,
    deadline: Option<tokio::time::Instant>,
    child: usize,
    addr: String,
    events: mpsc::Sender<ShardEvent>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut request = Request::new(ReceiverStream::new(req_rx));
        if let Some(at) = deadline {
            request.set_timeout(at.saturating_duration_since(tokio::time::Instant::now()));
        }
        let mut stream = match link.search_shard(request).await {
            Ok(response) => response.into_inner(),
            Err(status) => {
                let status = child_error(child, &addr, "shard search", status);
                let _ = events.send(ShardEvent::Child(child, Err(status))).await;
                return;
            }
        };
        loop {
            let message = stream.message().await;
            let terminal = !matches!(message, Ok(Some(_)))
                || matches!(
                    message,
                    Ok(Some(SearchShardResponse {
                        payload: Some(search_shard_response::Payload::Done(_)),
                    }))
                );
            let message =
                message.map_err(|status| child_error(child, &addr, "shard search", status));
            if events
                .send(ShardEvent::Child(child, message))
                .await
                .is_err()
                || terminal
            {
                return;
            }
        }
    })
}

fn spawn_parent_reader(
    mut inbound: Streaming<SearchShardRequest>,
    events: mpsc::Sender<ShardEvent>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        loop {
            let message = inbound.message().await;
            let terminal = !matches!(message, Ok(Some(_)));
            let event = match message {
                Ok(Some(message)) => ShardEvent::Parent(Some(Ok(message))),
                Ok(None) => ShardEvent::Parent(None),
                Err(status) => ShardEvent::Parent(Some(Err(status))),
            };
            if events.send(event).await.is_err() || terminal {
                return;
            }
        }
    })
}

/// One stream per child the mask keeps, each started with its own
/// `StartShardSearch`; returns how many were opened.
fn open_child_streams(
    args: &ShardArgs,
    scan: &ShardScan,
    legs: &mut ShardLegs,
    events: &mpsc::Sender<ShardEvent>,
) -> Result<usize, Status> {
    let mask = scan.mask.as_ref();
    let floor_shares = args.coordinator.shard_floor_share_ppms(&args.filters, mask);
    let mut opened = 0;
    for (child, addr) in scan.children.iter().enumerate() {
        if mask.is_some_and(|m| m.skipped[child]) {
            continue;
        }
        opened += 1;
        let (req_tx, req_rx) = mpsc::channel::<SearchShardRequest>(8);
        let share = floor_shares.get(child).copied().flatten();
        let start = child_start(&args.start, &args.filters, mask, child, share);
        req_tx
            .try_send(SearchShardRequest {
                payload: Some(search_shard_request::Payload::Start(start)),
            })
            .map_err(|_| Status::internal("relay: child request channel refused the Start"))?;
        legs.senders[child] = Some(req_tx);
        let link = args.coordinator.node_client(addr)?;
        let reader = spawn_child_reader(
            link,
            req_rx,
            args.deadline,
            child,
            addr.clone(),
            events.clone(),
        );
        legs.readers.push(reader);
    }
    Ok(opened)
}

/// Run events until every opened child is done; `Ok(false)` when the
/// parent went away first.
async fn drive(
    args: &mut ShardArgs,
    scan: &mut ShardScan,
    legs: &ShardLegs,
    events: &mut mpsc::Receiver<ShardEvent>,
) -> Result<bool, Status> {
    let mut map_open = true;
    let mut sleep = args
        .deadline
        .map(|at| Box::pin(tokio::time::sleep_until(at)));
    while scan.remaining > 0 {
        let event = tokio::select! {
            biased;
            _ = async { sleep.as_mut().expect("guarded").await }, if sleep.is_some() => {
                return Err(Status::deadline_exceeded(
                    "relay: the parent's deadline passed before every child completed",
                ));
            }
            moved = args.map_changes.changed(), if map_open => match moved {
                Ok(()) => {
                    let revision = *args.map_changes.borrow_and_update();
                    if revision == args.pinned.control_revision {
                        continue;
                    }
                    return Err(map_moved(&args.pinned, revision));
                }
                Err(_) => {
                    map_open = false;
                    continue;
                }
            },
            event = events.recv() => event.ok_or_else(|| Status::internal(
                "relay: the child readers ended before every child completed",
            ))?,
        };
        if !on_shard_event(event, scan, &legs.senders, &args.tx).await? {
            return Ok(false);
        }
    }
    Ok(true)
}

/// One relayed `SearchShard`, end to end.
async fn relay_shard_search(
    mut args: ShardArgs,
    probe: &mut ShardVectorRowProbe,
) -> Result<(), Status> {
    sync_map_changes(&mut args.map_changes, &args.pinned);
    let children = args.coordinator.node_addresses().to_vec();
    let n = children.len();
    if n == 0 {
        return Err(Status::failed_precondition(
            "relay: a relay coordinator has no children",
        ));
    }
    let mut map_open = true;
    let pinned = args.pinned.clone();
    await_vector_rows(
        probe,
        &pinned,
        &mut args.map_changes,
        &mut map_open,
        None,
        "SearchShard",
    )
    .await?;
    let mask = args.coordinator.shard_mask(args.filters.tree.as_ref());
    let child_nonempty = (0..n)
        .map(|i| args.coordinator.shard_is_nonempty(i))
        .collect();
    let mut scan = ShardScan {
        known: KnownFlags::new(&args.filters, mask.as_ref()),
        children,
        child_nonempty,
        exact_fp32: args.start.exact_fp32_scan,
        missing_parts: false,
        hits: Vec::new(),
        stats: None,
        done: vec![false; n],
        remaining: 0,
        mask,
    };
    let (event_tx, mut events) = mpsc::channel::<ShardEvent>(64);
    let mut legs = ShardLegs {
        senders: vec![None; n],
        readers: Vec::with_capacity(n),
        parent: None,
    };
    scan.remaining = open_child_streams(&args, &scan, &mut legs, &event_tx)?;
    if let Some(inbound) = args.inbound.take() {
        legs.parent = Some(spawn_parent_reader(inbound, event_tx));
    }
    if !drive(&mut args, &mut scan, &legs, &mut events).await? {
        return Ok(());
    }
    if *args.map_changes.borrow() != args.pinned.control_revision {
        return Err(Status::failed_precondition(format!(
            "relay: the shard map moved from revision {} (generation {}) during SearchShard; \
             retry under the current one",
            args.pinned.control_revision, args.pinned.topology_generation
        )));
    }
    let stats = scan.finish_stats();
    let mut hits = scan.hits;
    hits.sort_by(|a, b| by_score_then_id((a.score, a.vector_id), (b.score, b.vector_id)));
    let done = crate::pb::SearchShardDone {
        hits,
        stats,
        geo_columns_known: scan.known.geo,
        filter_columns_known: scan.known.tree,
    };
    let _ = args
        .tx
        .send(Ok(SearchShardResponse {
            payload: Some(search_shard_response::Payload::Done(done)),
        }))
        .await;
    Ok(())
}

/// The first frame of a `SearchShard`, validated: it must be the start,
/// carry no scoped read context, and name valid filters.
async fn read_start(
    inbound: &mut Streaming<SearchShardRequest>,
) -> Result<crate::pb::StartShardSearch, Status> {
    let start = match inbound.message().await? {
        Some(SearchShardRequest {
            payload: Some(search_shard_request::Payload::Start(start)),
        }) => start,
        _ => {
            return Err(Status::invalid_argument(
                "first SearchShardRequest must be StartShardSearch",
            ))
        }
    };
    if start.read_context.is_some() {
        return Err(Status::failed_precondition(
            "relay: scoped vector scans require child read receipt composition",
        ));
    }
    crate::node::validate_geo_filters(&start.geo_filters)?;
    if let Some(f) = start.filter.as_ref() {
        crate::filter::validate_filter(f)?;
    }
    Ok(start)
}

fn shard_filters(start: &crate::pb::StartShardSearch) -> RequestFilters {
    RequestFilters {
        geo: start.geo_filters.clone(),
        tree: start.filter.clone(),
        enumerate_max_rows: start.enumerate_max_rows,
        filter_strategy: start.filter_strategy,
        enumerate_all: start.enumerate_all,
        patience_saturation_ppm: start.patience_saturation_ppm,
        patience_hops: start.patience_hops,
        finish_settle_us: None,
        finish_deadline_us: None,
        floor_prune: start.floor_prune,
        floor_k: start.floor_k.or(Some(start.k)),
        parent_floor_share_ppm: start.floor_share_ppm,
        floor_greediness_ppm: start.floor_greediness_ppm,
        floor_clamp_min: start.floor_clamp_min,
        include_rerank_candidate: start.include_rerank_candidate,
        exact_fp32_scan: start.exact_fp32_scan,
    }
}

/// The relay task: the scan under the parent's closed response and
/// deadline, then the probe's shutdown and the error frame.
async fn run_shard_search(args: ShardArgs) {
    let mut probe = args.coordinator.shard_vector_row_probe();
    let tx = args.tx.clone();
    let deadline = args.deadline;
    let result = {
        let work = relay_shard_search(args, &mut probe);
        tokio::select! {
            _ = tx.closed() => Err(Status::cancelled("relay: parent response closed")),
            _ = async { tokio::time::sleep_until(deadline.expect("guarded")).await }, if deadline.is_some() =>
                Err(Status::deadline_exceeded("relay: the parent's deadline passed before every child completed")),
            result = work => result,
        }
    };
    probe.shutdown().await;
    if let Err(status) = result {
        let _ = tokio::time::timeout(Duration::from_millis(250), tx.send(Err(status))).await;
    }
}

impl RelayService {
    pub(super) async fn handle_search_shard(
        &self,
        request: Request<Streaming<SearchShardRequest>>,
    ) -> Result<Response<<Self as NodeService>::SearchShardStream>, Status> {
        crate::metrics::timed_stream(Route::SearchShard, request, |request| async move {
            let deadline =
                grpc_timeout(request.metadata()).map(|d| tokio::time::Instant::now() + d);
            let mut inbound = request.into_inner();
            let start = read_start(&mut inbound).await?;
            let filters = shard_filters(&start);
            let (tx, rx) = mpsc::channel::<Result<SearchShardResponse, Status>>(64);
            let (pinned, frozen) = self.pin();
            let args = ShardArgs {
                coordinator: frozen,
                pinned,
                map_changes: self.inner.map.changes(),
                start,
                filters,
                inbound: Some(inbound),
                tx,
                deadline,
            };
            tokio::spawn(run_shard_search(args));
            Ok(Response::new(ReceiverStream::new(rx)))
        })
        .await
    }
}
