//! The chunked shard-search relay and the hybrid legs composed over it.

use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use crate::coordinator::{CoordinatorServiceImpl, RequestFilters, ShardVectorRowProbe};
use crate::metrics::Route;
use crate::pb::node_service_server::NodeService;
use crate::pb::{
    search_shard_request, search_shard_response, FloorUpdate, SearchShardRequest,
    SearchShardResponse, ShardLegsRequest, ShardLegsResponse,
};

use super::map::MapSnapshot;
use super::merge_lexical::{add_count, add_count32, merge_known, KnownFlags};
use super::stream_search::{await_vector_rows, child_error};
use super::{grpc_timeout, RelayService};

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

#[allow(clippy::large_enum_variant)]
enum ShardEvent {
    Child(usize, Result<Option<SearchShardResponse>, Status>),
    Parent(Option<Result<SearchShardRequest, Status>>),
}

fn shard_floor(floor: f32) -> SearchShardRequest {
    SearchShardRequest {
        payload: Some(search_shard_request::Payload::FloorUpdate(FloorUpdate {
            floor,
        })),
    }
}

/// The forwarding loop of one relayed `SearchShard`: one child stream
/// per child the placement mask keeps, the parent's floor raises
/// forwarded to every child, each child's raises forwarded to the parent
/// (a child's k-th best is a lower bound on the relay's, so the parent's
/// running maximum only tightens), and the children's terminal lists
/// concatenated in score order. The relay keeps no heap: the parent's
/// merge picks the top-k from the union exactly as it does over leaves,
/// and with `tie_complete` the union holds every child's boundary tie
/// group, so the cascade's score-defined pool is the same set.
#[allow(clippy::too_many_arguments)]
async fn relay_shard_search(
    coordinator: CoordinatorServiceImpl,
    pinned: MapSnapshot,
    mut map_changes: watch::Receiver<u64>,
    start: crate::pb::StartShardSearch,
    filters: RequestFilters,
    mut inbound: Streaming<SearchShardRequest>,
    tx: mpsc::Sender<Result<SearchShardResponse, Status>>,
    deadline: Option<tokio::time::Instant>,
    probe: &mut ShardVectorRowProbe,
) -> Result<(), Status> {
    map_changes.mark_unchanged();
    if *map_changes.borrow() != pinned.control_revision {
        map_changes.mark_changed();
    }
    let children = coordinator.node_addresses().to_vec();
    let n = children.len();
    if n == 0 {
        return Err(Status::failed_precondition(
            "relay: a relay coordinator has no children",
        ));
    }
    let mut map_open = true;
    let _ = await_vector_rows(
        probe,
        &pinned,
        &mut map_changes,
        &mut map_open,
        None,
        "SearchShard",
    )
    .await?;
    let mask = coordinator.shard_mask(filters.tree.as_ref());
    let child_floor_shares = coordinator.shard_floor_share_ppms(&filters, mask.as_ref());
    let mut known = KnownFlags::new(&filters, mask.as_ref());
    let (event_tx, mut events) = mpsc::channel::<ShardEvent>(64);
    let mut legs = ShardLegs {
        senders: vec![None; n],
        readers: Vec::with_capacity(n),
        parent: None,
    };
    let mut remaining = 0;
    for (child, addr) in children.iter().enumerate() {
        if mask.as_ref().is_some_and(|m| m.skipped[child]) {
            continue;
        }
        remaining += 1;
        let (req_tx, req_rx) = mpsc::channel::<SearchShardRequest>(8);
        let child_start = crate::pb::StartShardSearch {
            read_context: None,
            request_id: start.request_id.clone(),
            k: start.k,
            vector: start.vector.clone(),
            tie_complete: start.tie_complete,
            collapse_parents: start.collapse_parents,
            geo_filters: filters.geo.clone(),
            filter: CoordinatorServiceImpl::shard_filter_tree(&filters, mask.as_ref(), child),
            enumerate_max_rows: filters.enumerate_max_rows,
            patience_saturation_ppm: filters.patience_saturation_ppm,
            patience_hops: filters.patience_hops,
            floor_prune: filters.floor_prune,
            floor_k: filters.floor_k.or(Some(start.k)),
            floor_share_ppm: child_floor_shares.get(child).copied().flatten(),
            floor_greediness_ppm: filters.floor_greediness_ppm,
            floor_clamp_min: filters.floor_clamp_min,
            include_rerank_candidate: start.include_rerank_candidate,
        };
        req_tx
            .try_send(SearchShardRequest {
                payload: Some(search_shard_request::Payload::Start(child_start)),
            })
            .map_err(|_| Status::internal("relay: child request channel refused the Start"))?;
        legs.senders[child] = Some(req_tx);
        let mut link = coordinator.node_client(addr)?;
        let addr = addr.clone();
        let events = event_tx.clone();
        legs.readers.push(tokio::spawn(async move {
            let mut request = Request::new(ReceiverStream::new(req_rx));
            if let Some(at) = deadline {
                request.set_timeout(at.saturating_duration_since(tokio::time::Instant::now()));
            }
            let mut stream = match link.search_shard(request).await {
                Ok(response) => response.into_inner(),
                Err(status) => {
                    let _ = events
                        .send(ShardEvent::Child(
                            child,
                            Err(child_error(child, &addr, "shard search", status)),
                        ))
                        .await;
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
        }));
    }
    {
        let events = event_tx;
        legs.parent = Some(tokio::spawn(async move {
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
        }));
    }
    let mut hits: Vec<crate::pb::ScoredHit> = Vec::new();
    let mut stats: Option<crate::pb::ShardScanStats> = None;
    let mut done = vec![false; n];
    let mut sleep = deadline.map(|at| Box::pin(tokio::time::sleep_until(at)));
    while remaining > 0 {
        let event = tokio::select! {
            biased;
            _ = async { sleep.as_mut().expect("guarded").await }, if sleep.is_some() => {
                return Err(Status::deadline_exceeded(
                    "relay: the parent's deadline passed before every child completed",
                ));
            }
            moved = map_changes.changed(), if map_open => match moved {
                Ok(()) => {
                    let revision = *map_changes.borrow_and_update();
                    if revision == pinned.control_revision {
                        continue;
                    }
                    return Err(Status::failed_precondition(format!(
                        "relay: the shard map moved from revision {} (generation {}) to \
                         revision {revision} during SearchShard; the scan was opened under the \
                         older map and is not completed; retry under the current one",
                        pinned.control_revision, pinned.topology_generation
                    )));
                }
                Err(_) => {
                    map_open = false;
                    continue;
                }
            },
            event = events.recv() => match event {
                Some(event) => event,
                None => {
                    return Err(Status::internal(
                        "relay: the child readers ended before every child completed",
                    ))
                }
            },
        };
        match event {
            ShardEvent::Parent(Some(Ok(SearchShardRequest {
                payload: Some(search_shard_request::Payload::FloorUpdate(update)),
            }))) => {
                if update.floor.is_nan() {
                    continue;
                }
                for sender in legs.senders.iter().flatten() {
                    // Floors are monotone: a raise a full channel drops is
                    // superseded by the next, and a closed child is done.
                    let _ = sender.try_send(shard_floor(update.floor));
                }
            }
            ShardEvent::Parent(Some(Ok(SearchShardRequest {
                payload: Some(search_shard_request::Payload::Start(_)),
            }))) => {
                return Err(Status::invalid_argument(
                    "relay: a stream carries one StartShardSearch; another arrived",
                ));
            }
            ShardEvent::Parent(Some(Ok(_))) | ShardEvent::Parent(None) => {}
            ShardEvent::Parent(Some(Err(status))) => {
                return Err(Status::cancelled(format!(
                    "relay: the parent's request leg failed: {}",
                    status.message()
                )));
            }
            ShardEvent::Child(
                _,
                Ok(Some(SearchShardResponse {
                    payload: Some(search_shard_response::Payload::FloorUpdate(update)),
                })),
            ) => {
                if tx
                    .send(Ok(SearchShardResponse {
                        payload: Some(search_shard_response::Payload::FloorUpdate(update)),
                    }))
                    .await
                    .is_err()
                {
                    // The parent is gone: nobody reads what the children
                    // still scan for.
                    return Ok(());
                }
            }
            ShardEvent::Child(
                child,
                Ok(Some(SearchShardResponse {
                    payload: Some(search_shard_response::Payload::Done(share)),
                })),
            ) => {
                if done[child] {
                    return Err(Status::internal(format!(
                        "relay: child {child} ({}) sent a terminal message twice",
                        children[child]
                    )));
                }
                done[child] = true;
                remaining -= 1;
                known.merge(
                    child,
                    mask.as_ref(),
                    &share.geo_columns_known,
                    &share.filter_columns_known,
                )?;
                if let Some(share) = share.stats {
                    let acc = stats.get_or_insert_with(Default::default);
                    add_count32(
                        &mut acc.chunk_calls,
                        share.chunk_calls,
                        child,
                        "chunk calls",
                    )?;
                    add_count(
                        &mut acc.candidates_collected,
                        share.candidates_collected,
                        child,
                        "candidate count",
                    )?;
                    add_count(
                        &mut acc.floors_published,
                        share.floors_published,
                        child,
                        "published floor count",
                    )?;
                    add_count(
                        &mut acc.floor_updates_applied,
                        share.floor_updates_applied,
                        child,
                        "applied floor count",
                    )?;
                    add_count(
                        &mut acc.floors_offered,
                        share.floors_offered,
                        child,
                        "offered floor count",
                    )?;
                    add_count32(
                        &mut acc.segments_total,
                        share.segments_total,
                        child,
                        "segment count",
                    )?;
                    add_count32(
                        &mut acc.segments_skipped,
                        share.segments_skipped,
                        child,
                        "skipped segment count",
                    )?;
                    let mut regimes =
                        crate::vector::FilterRegimeCounts::from_proto(acc.filter_regimes.as_ref());
                    regimes.add(crate::vector::FilterRegimeCounts::from_proto(
                        share.filter_regimes.as_ref(),
                    ));
                    acc.filter_regimes = regimes.to_proto();
                    add_count(
                        &mut acc.tail_enumerated_rows,
                        share.tail_enumerated_rows,
                        child,
                        "tail enumerated row count",
                    )?;
                    add_count32(
                        &mut acc.patience_stopped,
                        share.patience_stopped,
                        child,
                        "patience stopped count",
                    )?;
                    add_count32(
                        &mut acc.floor_pruned,
                        share.floor_pruned,
                        child,
                        "floor pruned count",
                    )?;
                }
                hits.extend(share.hits);
            }
            ShardEvent::Child(_, Ok(Some(_))) => {}
            ShardEvent::Child(child, Ok(None)) => {
                if !done[child] {
                    return Err(Status::data_loss(format!(
                        "relay: child {child} ({}) closed its stream before Done",
                        children[child]
                    )));
                }
            }
            ShardEvent::Child(_, Err(status)) => return Err(status),
        }
    }
    if *map_changes.borrow() != pinned.control_revision {
        return Err(Status::failed_precondition(format!(
            "relay: the shard map moved from revision {} (generation {}) during SearchShard; \
             retry under the current one",
            pinned.control_revision, pinned.topology_generation
        )));
    }
    hits.sort_by(|a, b| {
        b.score
            .total_cmp(&a.score)
            .then_with(|| a.vector_id.cmp(&b.vector_id))
    });
    let _ = tx
        .send(Ok(SearchShardResponse {
            payload: Some(search_shard_response::Payload::Done(
                crate::pb::SearchShardDone {
                    hits,
                    stats,
                    geo_columns_known: known.geo,
                    filter_columns_known: known.tree,
                },
            )),
        }))
        .await;
    Ok(())
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
            let filters = RequestFilters {
                geo: start.geo_filters.clone(),
                tree: start.filter.clone(),
                enumerate_max_rows: start.enumerate_max_rows,
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
            };
            let (tx, rx) = mpsc::channel::<Result<SearchShardResponse, Status>>(64);
            let (pinned, frozen) = self.pin();
            let map_changes = self.inner.map.changes();
            tokio::spawn(async move {
                let mut probe = frozen.shard_vector_row_probe();
                let result = {
                    let work = relay_shard_search(
                        frozen,
                        pinned,
                        map_changes,
                        start,
                        filters,
                        inbound,
                        tx.clone(),
                        deadline,
                        &mut probe,
                    );
                    tokio::select! {
                        _ = tx.closed() => Err(Status::cancelled("relay: parent response closed")),
                        _ = async { tokio::time::sleep_until(deadline.expect("guarded")).await }, if deadline.is_some() =>
                            Err(Status::deadline_exceeded("relay: the parent's deadline passed before every child completed")),
                        result = work => result,
                    }
                };
                probe.shutdown().await;
                if let Err(status) = result {
                    let _ = tokio::time::timeout(Duration::from_millis(250), tx.send(Err(status)))
                        .await;
                }
            });
            Ok(Response::new(ReceiverStream::new(rx)))
        })
        .await
    }

    pub(super) async fn handle_hybrid_shard(
        &self,
        _request: Request<crate::pb::HybridShardRequest>,
    ) -> Result<Response<crate::pb::HybridShardResponse>, Status> {
        Err(Status::unimplemented(
            "relay: NodeService.HybridShard is not served by a relay coordinator: two-level \
             fusion fuses one list per shard at the root and is partition dependent by \
             design, so a relay standing as one shard would change the answer; use the \
             fused routes (ShardLegs, the cascade) through a relay",
        ))
    }

    pub(super) async fn handle_shard_legs(
        &self,
        request: Request<ShardLegsRequest>,
    ) -> Result<Response<ShardLegsResponse>, Status> {
        crate::metrics::timed(Route::ShardLegs, request, |request| async move {
            let timeout = grpc_timeout(request.metadata());
            let req = request.into_inner();
            let (pinned, frozen) = self.pin();
            let children = frozen.node_addresses().to_vec();
            if children.is_empty() {
                return Err(Status::failed_precondition(
                    "relay: a relay coordinator has no children",
                ));
            }
            let claims = self.child_claims(
                req.expected_stats_epoch,
                &req.expected_stats_incarnation,
                children.len(),
            )?;
            let read_claims = req
                .read_context
                .as_ref()
                .map(|context| {
                    crate::visibility::VisibilityScope::new(context.visibility.as_ref())?;
                    self.child_claims(
                        context.expected_stats_epoch,
                        &context.expected_stats_incarnation,
                        children.len(),
                    )
                })
                .transpose()?;
            let mut tasks = Vec::with_capacity(children.len());
            for (shard, addr) in children.iter().enumerate() {
                let mut link = frozen.node_client(addr)?;
                let mut child_req = req.clone();
                child_req.expected_stats_epoch = claims[shard].epoch;
                child_req.expected_stats_incarnation = claims[shard].incarnation();
                if let (Some(context), Some(read_claims)) =
                    (&mut child_req.read_context, &read_claims)
                {
                    context.expected_stats_epoch = read_claims[shard].epoch;
                    context.expected_stats_incarnation = read_claims[shard].incarnation();
                }
                let addr = addr.clone();
                tasks.push(tokio::spawn(async move {
                    let mut request = Request::new(child_req);
                    if let Some(timeout) = timeout {
                        request.set_timeout(timeout);
                    }
                    link.shard_legs(request)
                        .await
                        .map(|r| r.into_inner())
                        .map_err(|status| child_error(shard, &addr, "shard legs", status))
                }));
            }
            // Raw per-leg lists: the parent merges them by score across
            // its shards, and the union of the children's lists holds the
            // subtree's top of each leg. Competition ranks at the parent
            // make the order of equal scores immaterial.
            let mut merged = ShardLegsResponse::default();
            let mut geo_known = None;
            let mut filter_known = None;
            let mut receipts = Vec::with_capacity(children.len());
            let mut binding = None;
            for (shard, task) in tasks.into_iter().enumerate() {
                let share = task
                    .await
                    .map_err(|e| Status::internal(format!("relay shard legs task: {e}")))??;
                match (&req.read_context, share.read_receipt.as_ref()) {
                    (Some(context), Some(receipt)) => {
                        crate::vector_read::check_binding(
                            &context.field,
                            receipt.vector_binding.as_ref(),
                            &mut binding,
                        )?;
                        receipts.push(receipt.clone());
                    }
                    (None, None) => {}
                    _ => {
                        return Err(Status::failed_precondition(
                            "relay: hybrid leg read receipt mismatch",
                        ))
                    }
                }
                merge_known(
                    "geo-column",
                    shard,
                    &mut geo_known,
                    &share.geo_columns_known,
                )?;
                merge_known(
                    "filter-leaf",
                    shard,
                    &mut filter_known,
                    &share.filter_columns_known,
                )?;
                merged.vector_hits.extend(share.vector_hits);
                merged.bm25_hits.extend(share.bm25_hits);
            }
            merged.geo_columns_known = geo_known.unwrap_or_default();
            merged.filter_columns_known = filter_known.unwrap_or_default();
            if let Some(context) = &req.read_context {
                let mut receipt = self.read_receipt(
                    &pinned,
                    children,
                    context.visibility.as_ref(),
                    read_claims.as_deref(),
                    &receipts,
                )?;
                receipt.vector_binding = binding.flatten();
                merged.read_receipt = Some(receipt);
            }
            self.still_current(&pinned, "ShardLegs")?;
            Ok(Response::new(merged))
        })
        .await
    }
}
