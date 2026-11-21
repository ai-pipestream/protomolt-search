//! Vector shard dispatch, hedging, and top-k merging.

mod dispatch;

use super::*;

/// Everything one shard-stream attempt needs, cheap to clone per attempt
/// (a hedged retry is just a second attempt with the same context).
#[derive(Clone)]
pub(super) struct ShardQueryCtx {
    pub(super) admission: Option<Arc<crate::vector_read::VectorReadBarrier>>,
    pub(super) request_id: Arc<str>,
    pub(super) vector: Arc<Vec<f32>>,
    pub(super) k: u32,
    pub(super) tie_complete: bool,
    /// Collapse-by-parent mode (see StartShardSearch.collapse_parents).
    pub(super) collapse: bool,
    /// The request's filters, compiled once and shipped verbatim to
    /// every shard (and to a hedge leg, which must run the identical
    /// query or its result would not be interchangeable).
    pub(super) filters: Arc<RequestFilters>,
    /// Per shard, the filter tree that shard receives: the request's
    /// tree with the clauses its placement leaf implies removed. A hedge
    /// leg reads the same entry as its primary.
    pub(super) shard_filters: Arc<Vec<Option<crate::pb::FilterExpr>>>,
    /// Per shard, the proportional `floor_share_ppm`.
    pub(super) shard_floor_shares: Arc<Vec<Option<u32>>>,
    /// Merges every shard's published floor into the running global max.
    pub(super) tracker: Arc<Mutex<FloorTracker>>,
    /// Conflating broadcast cell for the global floor: pumps write raises
    /// here; per-stream forwarders relay whatever is LATEST when they
    /// wake, so a burst of raises becomes one message per stream.
    pub(super) gfloor: Arc<watch::Sender<f32>>,
    /// Hedge legs launched (a shard's primary outran its hedge delay) and
    /// hedge legs that beat their primary. Both are pure accounting: a
    /// benchmark cannot otherwise tell "no hedge fired" from "the hedge
    /// fired and did not help".
    pub(super) hedges: Arc<AtomicU64>,
    pub(super) hedge_wins: Arc<AtomicU64>,
}

pub(super) fn floor_message(floor: f32) -> SearchShardRequest {
    SearchShardRequest {
        payload: Some(search_shard_request::Payload::FloorUpdate(FloorUpdate {
            floor,
        })),
    }
}

fn start_shard_search(shard: u32, ctx: &ShardQueryCtx) -> Result<SearchShardRequest, Status> {
    Ok(SearchShardRequest {
        payload: Some(search_shard_request::Payload::Start(StartShardSearch {
            read_context: ctx
                .admission
                .as_ref()
                .map(|barrier| barrier.context(shard as usize))
                .transpose()?,
            request_id: ctx.request_id.to_string(),
            k: ctx.k,
            vector: ctx.vector.as_ref().clone(),
            tie_complete: ctx.tie_complete,
            collapse_parents: ctx.collapse,
            geo_filters: ctx.filters.geo.clone(),
            filter: ctx.shard_filters.get(shard as usize).cloned().flatten(),
            enumerate_max_rows: ctx.filters.enumerate_max_rows,
            filter_strategy: ctx.filters.filter_strategy,
            enumerate_all: ctx.filters.enumerate_all,
            staged_selection: ctx.filters.staged_selection,
            patience_saturation_ppm: ctx.filters.patience_saturation_ppm,
            patience_hops: ctx.filters.patience_hops,
            floor_prune: ctx.filters.floor_prune,
            floor_k: ctx.filters.floor_k.or(Some(ctx.k)),
            floor_share_ppm: ctx
                .shard_floor_shares
                .get(shard as usize)
                .copied()
                .flatten(),
            floor_greediness_ppm: ctx.filters.floor_greediness_ppm,
            floor_clamp_min: ctx.filters.floor_clamp_min,
            include_rerank_candidate: ctx.filters.include_rerank_candidate,
            exact_fp32_scan: ctx.filters.exact_fp32_scan,
        })),
    })
}

/// Merge one shard's published floor into the global one.
fn observe_shard_floor(ctx: &ShardQueryCtx, floor: f32) {
    let raised = ctx
        .tracker
        .lock()
        .expect("floor tracker mutex poisoned")
        .observe(floor);
    if let Some(floor) = raised {
        // send_if_modified with a strict raise: two racing
        // pumps can never lower the broadcast value.
        ctx.gfloor.send_if_modified(|cur| {
            if floor > *cur {
                *cur = floor;
                true
            } else {
                false
            }
        });
    }
}

/// One `SearchShard` stream attempt against one node: Start, pump floors
/// both ways, return the terminal Done.
pub(super) async fn run_shard_stream(
    shard: u32,
    mut client: crate::link::NodeLink,
    ctx: ShardQueryCtx,
) -> Result<SearchShardDone, Status> {
    let (req_tx, req_rx) = mpsc::channel::<SearchShardRequest>(8);
    req_tx
        .send(start_shard_search(shard, &ctx)?)
        .await
        .map_err(|_| Status::internal("node request channel closed before Start"))?;
    let mut responses = client
        .search_shard(ReceiverStream::new(req_rx))
        .await?
        .into_inner();

    if let Some(admission) = &ctx.admission {
        admission.admit(shard as usize, &mut responses).await?;
    }

    // A late starter (a hedged replica) joins with the floor already
    // raised — seed it immediately instead of waiting for the next raise.
    let current = *ctx.gfloor.borrow();
    if current != f32::NEG_INFINITY {
        let _ = req_tx.try_send(floor_message(current));
    }

    // Conflating forwarder: on every wake, relay only the LATEST floor.
    // try_send on a full channel just drops this raise — floors are
    // monotone, so the next raise supersedes it; a dropped floor delays
    // pruning but never affects results.
    let mut floor_rx = ctx.gfloor.subscribe();
    let forwarder = tokio::spawn(async move {
        while floor_rx.changed().await.is_ok() {
            let floor = *floor_rx.borrow_and_update();
            if let Err(mpsc::error::TrySendError::Closed(_)) = req_tx.try_send(floor_message(floor))
            {
                break;
            }
        }
    });

    let result = loop {
        match crate::vector_read::next(&mut responses).await {
            Ok(Some(SearchShardResponse {
                payload: Some(search_shard_response::Payload::FloorUpdate(u)),
            })) => {
                observe_shard_floor(&ctx, u.floor);
            }
            Ok(Some(SearchShardResponse {
                payload: Some(search_shard_response::Payload::Done(done)),
            })) => break Ok(done),
            Ok(Some(_)) => {}
            Ok(None) => {
                break Err(Status::data_loss(format!(
                    "shard {shard}: stream closed before Done"
                )))
            }
            Err(e) => break Err(e),
        }
    };
    forwarder.abort();
    result
}

/// One shard's full query attempt: primary stream, hedged replica after
/// `hedge_delay` (first success wins), failover on primary error, all
/// bounded by `shard_deadline`.
pub(super) async fn run_shard_with_hedge(
    shard: u32,
    primary: crate::link::NodeLink,
    replica: Option<crate::link::NodeLink>,
    ctx: ShardQueryCtx,
    limits: FanoutLimits,
) -> Result<SearchShardDone, Status> {
    let attempt = async {
        let primary_run = run_shard_stream(shard, primary, ctx.clone());
        match (replica, limits.hedge_delay) {
            (Some(rep), Some(delay)) => {
                tokio::pin!(primary_run);
                tokio::select! {
                    r = &mut primary_run => match r {
                        Ok(done) => Ok(done),
                        // Primary failed before the hedge window: go
                        // straight to the replica.
                        Err(pe) => run_shard_stream(shard, rep, ctx.clone())
                            .await
                            .map_err(|re| both_failed(shard, &pe, &re)),
                    },
                    _ = tokio::time::sleep(delay) => {
                        ctx.hedges.fetch_add(1, AtomicOrdering::Relaxed);
                        let replica_run = run_shard_stream(shard, rep, ctx.clone());
                        tokio::pin!(replica_run);
                        tokio::select! {
                            r = &mut primary_run => match r {
                                Ok(done) => Ok(done),
                                Err(pe) => replica_run
                                    .await
                                    .map_err(|re| both_failed(shard, &pe, &re)),
                            },
                            r = &mut replica_run => match r {
                                Ok(done) => {
                                    ctx.hedge_wins.fetch_add(1, AtomicOrdering::Relaxed);
                                    Ok(done)
                                }
                                Err(re) => primary_run
                                    .await
                                    .map_err(|pe| both_failed(shard, &pe, &re)),
                            },
                        }
                    }
                }
            }
            // Replica without a hedge delay: pure failover.
            (Some(rep), None) => match primary_run.await {
                Ok(done) => Ok(done),
                Err(pe) => run_shard_stream(shard, rep, ctx.clone())
                    .await
                    .map_err(|re| both_failed(shard, &pe, &re)),
            },
            (None, _) => primary_run.await,
        }
    };
    match limits.shard_deadline {
        Some(deadline) => tokio::time::timeout(deadline, attempt).await.map_err(|_| {
            Status::deadline_exceeded(format!(
                "shard {shard} exceeded its {}ms deadline",
                deadline.as_millis()
            ))
        })?,
        None => attempt.await,
    }
}

pub(super) fn both_failed(shard: u32, primary: &Status, replica: &Status) -> Status {
    Status::unavailable(format!(
        "shard {shard}: primary failed ({primary}); replica failed ({replica})"
    ))
}

impl CoordinatorServiceImpl {
    /// The UDP signal socket, bound once (nonblocking: a full local
    /// buffer drops the datagram, which a monotone hint tolerates).
    pub(super) fn floor_socket(&self) -> Option<&Arc<std::net::UdpSocket>> {
        if !self.allow_network {
            return None;
        }
        self.floor_socket
            .get_or_init(|| {
                std::net::UdpSocket::bind(("0.0.0.0", 0)).ok().map(|s| {
                    let _ = s.set_nonblocking(true);
                    Arc::new(s)
                })
            })
            .as_ref()
    }

    /// The UDP signal target for a node address: the same host:port as
    /// its gRPC listener, in the UDP namespace. Resolved once and
    /// cached; IPv4 preferred (the fleet pins IPv4).
    pub(super) fn floor_target(&self, addr: &str) -> Option<std::net::SocketAddr> {
        if !self.allow_network {
            return None;
        }
        let mut cache = self
            .floor_targets
            .lock()
            .expect("floor target cache poisoned");
        if let Some(target) = cache.get(addr) {
            return *target;
        }
        let stripped = addr
            .strip_prefix("http://")
            .or_else(|| addr.strip_prefix("https://"))
            .unwrap_or(addr);
        let resolved = std::net::ToSocketAddrs::to_socket_addrs(stripped)
            .ok()
            .and_then(|addrs| {
                let all: Vec<std::net::SocketAddr> = addrs.collect();
                all.iter()
                    .find(|a| a.is_ipv4())
                    .copied()
                    .or(all.first().copied())
            });
        cache.insert(addr.to_string(), resolved);
        resolved
    }

    /// Fan out a `SearchShard` vector search across configured shards, relaying
    /// monotone global floor raises through a conflating `watch` channel and
    /// applying [`FanoutLimits`] replica hedging and deadlines per shard.
    pub async fn fanout_search(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        tie_complete: bool,
        filters: &RequestFilters,
    ) -> Result<FanoutResult, Status> {
        self.check_vector_scan(filters, false)?;
        if self.scoped_vector_scan() && self.query_read_versions.is_none() {
            let (pinned, reads) = self.pin_read_versions().await?;
            let result =
                Box::pin(pinned.fanout_search(request_id, vector, k, tie_complete, filters)).await;
            pinned.validate_read_versions(&reads).await?;
            return result;
        }
        let mut scan = self
            .dispatch_vector_shards(request_id, vector, k, tie_complete, false, filters)
            .await?;
        let mut identities = HashMap::new();
        let mut rerank_candidates: HashMap<u64, crate::pb::RerankCandidatePayload> = HashMap::new();
        for _ in 0..scan.active {
            let (shard, wall_ms, done) = scan.next_done().await?;
            for hit in &done.hits {
                if let Some(identity) = hit.identity.as_ref() {
                    identities.insert((shard, hit.vector_id), identity.clone());
                }
                if let Some(cand) = hit.rerank_candidate.as_ref() {
                    rerank_candidates
                        .entry(hit.vector_id)
                        .or_insert_with(|| cand.clone());
                }
            }
            scan.shard_hits.push((
                shard,
                done.hits
                    .into_iter()
                    .map(|h| (h.vector_id, h.score))
                    .collect(),
            ));
            scan.shard_stats.push(done.stats);
            scan.shard_wall_ms.push((shard, wall_ms));
        }

        scan.known.refuse_unknown(filters)?;

        let hits = merge_topk(scan.shard_hits.iter().cloned(), k as usize)
            .into_iter()
            .map(|h| ScoredHit {
                identity: identities.get(&(h.shard, h.vector_id)).cloned(),
                vector_id: h.vector_id,
                score: h.score,
                parent_id: 0,
                rerank_candidate: rerank_candidates.get(&h.vector_id).cloned(),
            })
            .collect();
        Ok(FanoutResult {
            hits,
            shard_stats: scan.shard_stats,
            shard_hits: scan.shard_hits,
            shard_wall_ms: scan.shard_wall_ms,
            hedges_fired: scan.hedges.load(AtomicOrdering::Relaxed),
            hedge_wins: scan.hedge_wins.load(AtomicOrdering::Relaxed),
            rerank_candidates,
        })
    }
}
