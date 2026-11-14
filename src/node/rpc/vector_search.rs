//! Streaming and rescored dense vector search RPC handlers.

use crate::node::*;

impl NodeServiceImpl {
    pub(super) async fn handle_search_shard(
        &self,
        request: Request<Streaming<SearchShardRequest>>,
    ) -> Result<Response<<Self as NodeService>::SearchShardStream>, Status> {
        crate::metrics::timed_stream(Route::SearchShard, request, |request| async move {
            let mut inbound = request.into_inner();
            let (tx, rx) = mpsc::channel::<Result<SearchShardResponse, Status>>(64);
            let state = self.state.clone();
            let config = self.config.clone();
            let knobs = Arc::clone(&self.knobs);
            let scan_queue = config.coalesce.then(|| self.scan_queue());

            tokio::spawn(async move {
                // Protocol: the first message must be Start.
                let start = match inbound.message().await {
                    Ok(Some(SearchShardRequest {
                        payload: Some(search_shard_request::Payload::Start(start)),
                    })) => start,
                    Ok(_) => {
                        let _ = tx
                            .send(Err(Status::invalid_argument(
                                "first SearchShardRequest must be StartShardSearch",
                            )))
                            .await;
                        return;
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };
                // Filter validation is shape-only and shard-independent, so
                // it happens once here rather than inside the scan: a
                // malformed tree must refuse before any work, exactly as on
                // the lexical routes.
                let geo_regions = match validate_geo_filters(&start.geo_filters) {
                    Ok(regions) => regions,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };
                if let Some(f) = start.filter.as_ref() {
                    if let Err(e) = crate::filter::validate_filter(f) {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                }

                // Floor updates arrive on the same stream; a pump task folds
                // them into a watch cell the blocking scan polls between chunks.
                // Updates are monotone maxes, so only raises are stored.
                let (floor_tx, floor_rx) = watch::channel(f32::NEG_INFINITY);
                tokio::spawn(async move {
                    loop {
                        match inbound.message().await {
                            Ok(Some(SearchShardRequest {
                                payload: Some(search_shard_request::Payload::FloorUpdate(u)),
                            })) => {
                                floor_tx.send_if_modified(|cur| {
                                    if !u.floor.is_nan() && u.floor > *cur {
                                        *cur = u.floor;
                                        true
                                    } else {
                                        false
                                    }
                                });
                            }
                            // Duplicate Start or empty payload: ignore.
                            Ok(Some(_)) => {}
                            // Client closed (end of updates or cancellation) or
                            // the stream broke: stop pumping; the scan finishes
                            // on its own either way.
                            Ok(None) | Err(_) => break,
                        }
                    }
                });

                let share = knobs.share_floors();
                let segment_pruning = knobs.segment_pruning();
                let floor_delta = knobs.floor_delta();
                let chunk_blocks = config.chunk_blocks;
                let slot_offset = config.slot_offset;
                let scan_tx = tx.clone();
                // Publish only raises that clear the delta gate, and never
                // block the scan on a full channel: intermediate floors are
                // disposable (they are monotone, so the next chunk's publish
                // supersedes any dropped one). The terminal Done is sent
                // with `.await` below and cannot be dropped.
                let warmup = knobs.floor_warmup_chunks();
                let floor_min_interval_ms = knobs.floor_min_interval_ms();
                let min_interval = (floor_min_interval_ms > 0)
                    .then(|| std::time::Duration::from_millis(floor_min_interval_ms));
                let mut last_published = f32::NEG_INFINITY;
                let mut offers = 0u32;
                let mut last_at: Option<std::time::Instant> = None;
                // Returns whether the floor actually went on the wire, so
                // the scan can report offers and publishes apart. Reporting
                // only offers is how the warmup and debounce knobs came to
                // look like no-ops.
                let publish_floor = move |floor: f32| -> bool {
                    if !share {
                        return false;
                    }
                    // Skip the opening chunks: their floors are the weakest
                    // and cost a broadcast to every shard apiece.
                    offers += 1;
                    if offers <= warmup {
                        return false;
                    }
                    if floor <= last_published + floor_delta {
                        return false;
                    }
                    // Debounce. Suppressing a floor loses nothing: they are
                    // monotone, so the next one published is at least as
                    // high as the one dropped.
                    if let (Some(interval), Some(at)) = (min_interval, last_at) {
                        if at.elapsed() < interval {
                            return false;
                        }
                    }
                    last_published = floor;
                    last_at = Some(std::time::Instant::now());
                    let _ = scan_tx.try_send(Ok(SearchShardResponse {
                        payload: Some(search_shard_response::Payload::FloorUpdate(FloorUpdate {
                            floor,
                        })),
                    }));
                    true
                };
                let external_floor = move || {
                    if share {
                        let f = *floor_rx.borrow();
                        (f != f32::NEG_INFINITY).then_some(f)
                    } else {
                        None
                    }
                };

                let ready_tx = tx.clone();
                // Collapse-by-parent scans run their own solo path: the
                // collection semantics (one entry per parent, parent floors,
                // saturation escalation) do not batch with plain scans.
                if start.collapse_parents {
                    if start.tie_complete {
                        let _ = tx
                            .send(Err(Status::invalid_argument(
                                "collapse_parents and tie_complete are mutually exclusive",
                            )))
                            .await;
                        return;
                    }
                    let mut external_floor = external_floor;
                    let mut publish_floor = publish_floor;
                    let geo_regions = geo_regions.clone();
                    let scan = tokio::task::spawn_blocking(move || {
                        let n = {
                            let guard = read_shard(&state);
                            let index = guard.index.as_ref().ok_or_else(|| {
                                Status::failed_precondition(
                                    "shard has no index yet (set calibration or add vectors)",
                                )
                            })?;
                            Self::validate_start(index, &start)?;
                            index.len()
                        };
                        // parent_map takes its own locks (read to build, write
                        // to cache), so the validation guard is dropped first.
                        let parents = Self::parent_map(&state, slot_offset, n)?;
                        let guard = read_shard(&state);
                        let index = guard.index.as_ref().ok_or_else(|| {
                            Status::failed_precondition("shard index disappeared mid-setup")
                        })?;
                        guard
                            .check_stats_epoch(parents.claim.epoch, &parents.claim.incarnation())?;
                        if index.len() != parents.rows.len() {
                            return Err(Status::aborted(
                                "shard grew between setup and scan; retry",
                            ));
                        }
                        // Filters remove chunks, and a parent's score is the
                        // max over its SURVIVING chunks, so collapse under a
                        // filter is the collapse of the filtered corpus —
                        // still every floor a valid lower bound, still no new
                        // pruning math.
                        let ((_, allow, prune), receipt) = guard.vector_scan_filters(
                            index.len(),
                            &start.geo_filters,
                            &geo_regions,
                            start.filter.as_ref(),
                            start.read_context.as_ref(),
                            segment_pruning,
                        )?;
                        let known = filter_known_flags(
                            guard.bm25.as_ref(),
                            &start.geo_filters,
                            start.filter.as_ref(),
                        );
                        if let Some(receipt) = receipt {
                            ready_tx
                                .blocking_send(Ok(SearchShardResponse {
                                    payload: Some(search_shard_response::Payload::ReadReady(
                                        receipt,
                                    )),
                                }))
                                .map_err(|_| Status::cancelled("scoped scan response closed"))?;
                        }
                        let (hits, mut stats) = chunked_topk_collapsed(
                            index,
                            &start.vector,
                            start.k as usize,
                            chunk_blocks,
                            &parents.rows,
                            &mut external_floor,
                            &mut publish_floor,
                            allow.as_ref().map(|bits| bits.to_bools()).as_deref(),
                        );
                        stats.segments_total = prune.segments_total;
                        stats.segments_skipped = prune.segments_skipped;
                        crate::metrics::record_scan(&stats);
                        observe_scan(stats.bytes_scanned, stats.scan_nanos);
                        let identities = guard
                            .bm25
                            .as_ref()
                            .map(Bm25Shard::identity_snapshot)
                            .transpose()
                            .map_err(Status::internal)?
                            .unwrap_or_default();
                        let rerank_candidates: HashMap<u32, crate::pb::RerankCandidatePayload> =
                            if start.include_rerank_candidate {
                                hits.iter()
                                    .filter_map(|h| {
                                        guard.rerank_candidate_for(h.slot).map(|c| (h.slot, c))
                                    })
                                    .collect()
                            } else {
                                HashMap::new()
                            };
                        Ok((hits, stats, known, identities, rerank_candidates))
                    });
                    let outcome = match scan.await {
                        Ok(result) => result,
                        Err(e) => Err(Status::internal(format!("collapse scan task failed: {e}"))),
                    };
                    match outcome {
                        Ok((
                            hits,
                            stats,
                            (geo_columns_known, filter_columns_known),
                            identities,
                            mut rerank_candidates,
                        )) => {
                            let done = SearchShardDone {
                                hits: hits
                                    .into_iter()
                                    .map(|h| ScoredHit {
                                        vector_id: slot_offset + u64::from(h.slot),
                                        score: h.score,
                                        parent_id: h.parent,
                                        identity: identities.identity(h.slot),
                                        rerank_candidate: rerank_candidates.remove(&h.slot),
                                    })
                                    .collect(),
                                stats: Some(ShardScanStats {
                                    chunk_calls: stats.chunk_calls,
                                    candidates_collected: stats.candidates_collected,
                                    floors_published: stats.floors_published,
                                    floor_updates_applied: stats.floor_updates_applied,
                                    floors_offered: stats.floors_offered,
                                    segments_total: stats.segments_total,
                                    segments_skipped: stats.segments_skipped,
                                    filter_regimes: stats.regimes.to_proto(),
                                    tail_enumerated_rows: stats.tail_enumerated_rows,
                                    patience_stopped: stats.patience_stopped,
                                    floor_pruned: stats.floor_pruned,
                                }),
                                geo_columns_known,
                                filter_columns_known,
                            };
                            let _ = tx
                                .send(Ok(SearchShardResponse {
                                    payload: Some(search_shard_response::Payload::Done(done)),
                                }))
                                .await;
                        }
                        Err(e) => {
                            let _ = tx.send(Err(e)).await;
                        }
                    }
                    return;
                }

                let outcome: Result<ScanOutcome, Status> = match scan_queue {
                    Some(jobs) => {
                        // Coalesced path: validate against the current index
                        // cheaply, then queue for a batched kernel pass. The
                        // batch runner holds the read lock for the scan, the
                        // same consistency the solo path gets.
                        let validated = {
                            let guard = read_shard(&state);
                            match guard.index.as_ref() {
                                Some(index) => Self::validate_start(index, &start),
                                None => Err(Status::failed_precondition(
                                    "shard has no index yet (set calibration or add vectors)",
                                )),
                            }
                        };
                        match validated {
                            Ok(()) => {
                                let (done_tx, done_rx) = tokio::sync::oneshot::channel();
                                let job = ScanJob {
                                    read_context: start.read_context.clone(),
                                    ready_tx: ready_tx.clone(),
                                    vector: start.vector.clone(),
                                    k: start.k as usize,
                                    tie_complete: start.tie_complete,
                                    include_rerank_candidate: start.include_rerank_candidate,
                                    geo_filters: start.geo_filters.clone(),
                                    geo_regions: geo_regions.clone(),
                                    filter: start.filter.clone(),
                                    enumerate_max_rows: start.enumerate_max_rows,
                                    patience_saturation_ppm: start.patience_saturation_ppm,
                                    patience_hops: start.patience_hops,
                                    floor_prune: start.floor_prune,
                                    floor_k: start.floor_k,
                                    floor_share_ppm: start.floor_share_ppm,
                                    floor_greediness_ppm: start.floor_greediness_ppm,
                                    floor_clamp_min: start.floor_clamp_min,
                                    external: Box::new(external_floor),
                                    publish: Box::new(publish_floor),
                                    done: done_tx,
                                };
                                if jobs.send(job).await.is_err() {
                                    Err(Status::internal("scan scheduler unavailable"))
                                } else {
                                    match done_rx.await {
                                        Ok(result) => result,
                                        Err(_) => Err(Status::internal(
                                            "scan batch dropped before finishing",
                                        )),
                                    }
                                }
                            }
                            Err(e) => Err(e),
                        }
                    }
                    None => {
                        // Solo path (the coalescing A/B baseline): one
                        // blocking scan per RPC, exactly the historical
                        // behavior.
                        let mut publish_floor = publish_floor;
                        let scan = tokio::task::spawn_blocking(move || {
                            // The read guard is held for the whole chunked
                            // scan: adds (write lock) never interleave with a
                            // scan, so a search sees one consistent index
                            // snapshot.
                            let guard = read_shard(&state);
                            let index = guard.index.as_ref().ok_or_else(|| {
                                Status::failed_precondition(
                                    "shard has no index yet (set calibration or add vectors)",
                                )
                            })?;
                            Self::validate_start(index, &start)?;
                            let ((_, allow, prune), receipt) = guard.vector_scan_filters(
                                index.len(),
                                &start.geo_filters,
                                &geo_regions,
                                start.filter.as_ref(),
                                start.read_context.as_ref(),
                                segment_pruning,
                            )?;
                            let (geo_columns_known, filter_columns_known) = filter_known_flags(
                                guard.bm25.as_ref(),
                                &start.geo_filters,
                                start.filter.as_ref(),
                            );
                            if let Some(receipt) = receipt {
                                ready_tx
                                    .blocking_send(Ok(SearchShardResponse {
                                        payload: Some(search_shard_response::Payload::ReadReady(
                                            receipt,
                                        )),
                                    }))
                                    .map_err(|_| {
                                        Status::cancelled("scoped scan response closed")
                                    })?;
                            }
                            let patience = check_patience_options(
                                index,
                                start.patience_saturation_ppm,
                                start.patience_hops,
                            )?;
                            let floor_prune = check_floor_prune_options(
                                index,
                                start.floor_prune,
                                start.floor_k,
                                start.k,
                                start.floor_share_ppm,
                                start.floor_greediness_ppm,
                                start.floor_clamp_min,
                            )?;
                            let solo_queries = [BatchQuery {
                                vector: &start.vector,
                                k: start.k as usize,
                                keep_ties: start.tie_complete,
                                allow: allow.as_ref().map(crate::vector::Mask::Bits),
                                enumerate_max_rows: start.enumerate_max_rows.map(|r| r as usize),
                                patience,
                                floor_prune,
                            }];
                            let (hits, mut stats) = crate::chunked::chunked_topk_batch(
                                index,
                                &solo_queries,
                                chunk_blocks,
                                &mut |_| external_floor(),
                                &mut |_, f| publish_floor(f),
                            )
                            .pop()
                            .expect("one query in, one result out");
                            stats.segments_total = prune.segments_total;
                            stats.segments_skipped = prune.segments_skipped;
                            crate::metrics::record_scan(&stats);
                            observe_scan(stats.bytes_scanned, stats.scan_nanos);
                            let rerank_candidates: HashMap<u32, crate::pb::RerankCandidatePayload> =
                                if start.include_rerank_candidate {
                                    hits.iter()
                                        .filter_map(|h| {
                                            guard.rerank_candidate_for(h.slot).map(|c| (h.slot, c))
                                        })
                                        .collect()
                                } else {
                                    HashMap::new()
                                };
                            Ok(ScanOutcome {
                                hits,
                                identities: guard
                                    .bm25
                                    .as_ref()
                                    .map(Bm25Shard::identity_snapshot)
                                    .transpose()
                                    .map_err(Status::internal)?
                                    .unwrap_or_default(),
                                rerank_candidates,
                                stats,
                                geo_columns_known,
                                filter_columns_known,
                            })
                        });
                        match scan.await {
                            Ok(result) => result,
                            Err(e) => Err(Status::internal(format!("scan task failed: {e}"))),
                        }
                    }
                };

                match outcome {
                    Ok(ScanOutcome {
                        hits,
                        identities,
                        mut rerank_candidates,
                        stats,
                        geo_columns_known,
                        filter_columns_known,
                    }) => {
                        let done = SearchShardDone {
                            hits: hits
                                .into_iter()
                                .map(|h| ScoredHit {
                                    vector_id: slot_offset + u64::from(h.slot),
                                    score: h.score,
                                    parent_id: 0,
                                    identity: identities.identity(h.slot),
                                    rerank_candidate: rerank_candidates.remove(&h.slot),
                                })
                                .collect(),
                            stats: Some(ShardScanStats {
                                chunk_calls: stats.chunk_calls,
                                candidates_collected: stats.candidates_collected,
                                floors_published: stats.floors_published,
                                floor_updates_applied: stats.floor_updates_applied,
                                floors_offered: stats.floors_offered,
                                segments_total: stats.segments_total,
                                segments_skipped: stats.segments_skipped,
                                filter_regimes: stats.regimes.to_proto(),
                                tail_enumerated_rows: stats.tail_enumerated_rows,
                                patience_stopped: stats.patience_stopped,
                                floor_pruned: stats.floor_pruned,
                            }),
                            geo_columns_known,
                            filter_columns_known,
                        };
                        let _ = tx
                            .send(Ok(SearchShardResponse {
                                payload: Some(search_shard_response::Payload::Done(done)),
                            }))
                            .await;
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                    }
                }
            });

            Ok(Response::new(ReceiverStream::new(rx)))
        })
        .await
    }

    pub(super) async fn handle_stream_search(
        &self,
        request: Request<Streaming<StreamSearchRequest>>,
    ) -> Result<Response<<Self as NodeService>::StreamSearchStream>, Status> {
        crate::metrics::timed_stream(Route::StreamSearch, request, |request| async move {
            let mut inbound = request.into_inner();
            let (tx, rx) = mpsc::channel::<Result<StreamSearchResponse, Status>>(64);
            let state = self.state.clone();
            let slot_offset = self.config.slot_offset;
            let segment_pruning = self.knobs.segment_pruning();
            let share_floors = self.knobs.share_floors();
            let stream_poll_delay = self.stream_poll_delay;
            let stream_signals = Arc::clone(&self.stream_signals);

            tokio::spawn(async move {
                // Protocol: the first message must be Start.
                let start = match inbound.message().await {
                    Ok(Some(StreamSearchRequest {
                        payload: Some(stream_search_request::Payload::Start(start)),
                    })) => start,
                    Ok(_) => {
                        let _ = tx
                            .send(Err(Status::invalid_argument(
                                "first StreamSearchRequest must be StartStreamSearch",
                            )))
                            .await;
                        return;
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };
                if start.initial_floor.is_some_and(f32::is_nan) {
                    let _ = tx
                        .send(Err(Status::invalid_argument(
                            "initial_floor must not be NaN",
                        )))
                        .await;
                    return;
                }
                let identity_limits = start.identity_limits;
                let identity_timeout = match identity_limits.as_ref()
                    .map(crate::query_identity::validate_limits).transpose() {
                    Ok(timeout) => timeout,
                    Err(status) => {
                        let _ = tx.send(Err(status)).await;
                        return;
                    }
                };
                // Shape-only filter validation before any scan work, the
                // same order the lexical routes use.
                let geo_regions = match validate_geo_filters(&start.geo_filters) {
                    Ok(regions) => regions,
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };
                if let Some(f) = start.filter.as_ref() {
                    if let Err(e) = crate::filter::validate_filter(f) {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                }

                // Floor raises and cancellation fold into one stream state that
                // the blocking scan polls before every chunk. The gRPC request
                // stream is authoritative; UDP is only a fast lossy duplicate.
                let signals = Arc::new(StreamSignals::new(
                    start.initial_floor.unwrap_or(f32::NEG_INFINITY),
                ));
                let udp_token = (start.floor_token != 0).then_some(start.floor_token);
                if let Some(token) = udp_token {
                    stream_signals
                        .lock()
                        .expect("stream signal registry poisoned")
                        .insert(token, Arc::clone(&signals));
                }
                let pump_signals = Arc::clone(&signals);
                let identity_ready = Arc::new(AtomicBool::new(false));
                let pump_ready = Arc::clone(&identity_ready);
                let identity_enabled = identity_limits.is_some();
                let mut pump = tokio::spawn(async move {
                    loop {
                        match inbound.message().await {
                            Ok(Some(StreamSearchRequest {
                                payload: Some(stream_search_request::Payload::FloorUpdate(u)),
                            })) => raise_floor_cell(&pump_signals.floor, u.floor),
                            Ok(Some(StreamSearchRequest {
                                payload: Some(stream_search_request::Payload::Stop(_)),
                            })) => {
                                pump_signals
                                    .cancelled
                                    .store(true, std::sync::atomic::Ordering::Release);
                                return Ok(None);
                            }
                            Ok(Some(StreamSearchRequest {
                                payload: Some(stream_search_request::Payload::Finish(f)),
                            })) => {
                                pump_signals.request_finish(f.deadline);
                            }
                            Ok(Some(StreamSearchRequest {
                                payload: Some(stream_search_request::Payload::ResolveIdentities(selection)),
                            })) => {
                                if !identity_enabled || !pump_ready.load(AtomicOrdering::Acquire) {
                                    pump_signals.cancelled.store(true, AtomicOrdering::Release);
                                    return Err(Status::failed_precondition(
                                        "ResolveStreamIdentities requires this stream's IdentityReady",
                                    ));
                                }
                                return Ok(Some(selection));
                            }
                            // Duplicate Start or empty payload: ignore.
                            Ok(Some(_)) => {}
                            // Client closed or the stream broke: no more
                            // raises can arrive; the scan finishes (or hits
                            // the dead response channel) on its own.
                            Ok(None) | Err(_) => {
                                if identity_enabled {
                                    pump_signals.cancelled.store(true, AtomicOrdering::Release);
                                    return Err(Status::cancelled("identity request stream closed"));
                                }
                                return Ok(None);
                            }
                        }
                    }
                });

                let scan_tx = tx.clone();
                let scan_signals = Arc::clone(&signals);
                let scan =
                    tokio::task::spawn_blocking(move || -> Result<(StreamSearchSummary, Option<crate::query_identity::ScanIdentities>), Status> {
                        // Document mode: resolve each emitted slot's parent.
                        // parent_map takes its own locks (read to build,
                        // write to cache), so it runs before the scan's read
                        // guard is taken, exactly as in the bidi collapse
                        // path.
                        let parents = if start.collapse_parents {
                            let n = {
                                let guard = read_shard(&state);
                                guard.index.as_ref().map_or(0, |index| index.len())
                            };
                            Some(Self::parent_map(&state, slot_offset, n)?)
                        } else {
                            None
                        };
                        let guard = read_shard(&state);
                        let index = guard.index.as_ref().ok_or_else(|| {
                            Status::failed_precondition(
                                "shard has no index yet (set calibration or add vectors)",
                            )
                        })?;
                        let dim = index
                            .dim_opt()
                            .ok_or_else(|| Status::failed_precondition("index has no vectors"))?;
                        if start.vector.len() != dim {
                            return Err(Status::invalid_argument(format!(
                                "query vector has dim {}, index expects {dim}",
                                start.vector.len()
                            )));
                        }
                        if let Some((_, coord, value)) =
                            first_invalid_coordinate(&start.vector, dim)
                        {
                            return Err(Status::invalid_argument(format!(
                                "query coordinate {coord} is invalid: {value}"
                            )));
                        }
                        if let Some(p) = parents.as_ref() {
                            guard.check_stats_epoch(p.claim.epoch, &p.claim.incarnation())?;
                            if p.rows.len() != index.len() {
                                return Err(Status::aborted(
                                    "shard grew between setup and scan; retry",
                                ));
                            }
                        }
                        let scoring_fingerprint = index.descriptor().scoring_fingerprint;
                        if scoring_fingerprint.is_empty() {
                            return Err(Status::failed_precondition(
                                "vector backend has no scoring fingerprint",
                            ));
                        }

                        // The request's filters as a slot allowlist, resolved
                        // under this scan's read guard so columns and index
                        // are one snapshot. The streaming engine emits every
                        // live slot at or above the floor; with an allowlist
                        // "live" means "survived the filters", so the
                        // completion certificate covers the filtered corpus
                        // and means exactly what it meant before.
                        let ((_, allow, prune), receipt) = guard.vector_scan_filters(
                            index.len(),
                            &start.geo_filters,
                            &geo_regions,
                            start.filter.as_ref(),
                            start.read_context.as_ref(),
                            segment_pruning,
                        )?;
                        let (geo_columns_known, filter_columns_known) = filter_known_flags(
                            guard.bm25.as_ref(),
                            &start.geo_filters,
                            start.filter.as_ref(),
                        );

                        if let Some(receipt) = receipt {
                            scan_tx.blocking_send(Ok(StreamSearchResponse {
                                payload: Some(stream_search_response::Payload::ReadReady(receipt)),
                            })).map_err(|_| Status::cancelled("scoped scan response closed"))?;
                        }
                        let mut options = VectorSearchOptions::new();
                        if let Some(a) = allow.as_ref() {
                            options = options.with_bits(a);
                        }
                        if let Some(depth) = start.candidate_depth {
                            options = options.with_candidate_depth(depth as usize);
                        }
                        check_enumerate_threshold(index, start.enumerate_max_rows)?;
                        if let Some(rows) = start.enumerate_max_rows {
                            options = options.with_enumerate_max_rows(rows as usize);
                        }
                        if let Some(patience) = check_patience_options(
                            index,
                            start.patience_saturation_ppm,
                            start.patience_hops,
                        )? {
                            options = options.with_patience(patience);
                        }
                        if let Some(floor_prune) = check_floor_prune_options(
                            index,
                            start.floor_prune,
                            start.floor_k,
                            start.candidate_depth.unwrap_or(0),
                            start.floor_share_ppm,
                            start.floor_greediness_ppm,
                            start.floor_clamp_min,
                        )? {
                            options = options.with_floor_prune(floor_prune);
                        }
                        let allow_early_finish = matches!(
                            index.descriptor().quality_contract,
                            crate::vector::QualityContract::ConfiguredAnn
                                | crate::vector::QualityContract::ProbabilisticBound
                        );
                        let mut floor_now = f32::NEG_INFINITY;
                        if share_floors {
                            if let Some(f) = start.initial_floor {
                                options = options.with_initial_threshold(f);
                                floor_now = f;
                            }
                        }
                        let mut raises = 0u64;
                        let stride = if parents.is_some() { 20 } else { 12 };
                        let mut emitted_slots: Vec<u32> = Vec::new();
                        let summary = index
                            .try_search_streaming_controlled(
                                &start.vector,
                                options,
                                |batch| {
                                    // Pack the batch as fixed-stride LE records
                                    // (u64 global id, f32 score, and in document
                                    // mode the slot's u64 parent), fused into the
                                    // slot-to-global-id rebase — one pass, no
                                    // per-hit messages. Real emissions only
                                    // carry live slots; a negative would be an
                                    // engine contract break, dropped rather
                                    // than wrapped into a bogus global id.
                                    let mut hits: Vec<u8> =
                                        Vec::with_capacity(stride * batch.slots.len());
                                    for (&slot, &score) in batch.slots.iter().zip(batch.scores) {
                                        if slot < 0 {
                                            continue;
                                        }
                                        if start.include_rerank_candidate {
                                            emitted_slots.push(slot as u32);
                                        }
                                        hits.extend_from_slice(
                                            &(slot_offset + slot as u64).to_le_bytes(),
                                        );
                                        hits.extend_from_slice(&score.to_le_bytes());
                                        if let Some(p) = parents.as_ref() {
                                            hits.extend_from_slice(&p.rows[slot as usize].to_le_bytes());
                                        }
                                    }
                                    let sent = scan_tx.blocking_send(Ok(StreamSearchResponse {
                                        payload: Some(stream_search_response::Payload::Batch(
                                            StreamSearchBatch { hits },
                                        )),
                                    }));
                                    if sent.is_err() {
                                        VectorStreamControl::Stop
                                    } else {
                                        VectorStreamControl::Continue
                                    }
                                },
                                || {
                                    if let Some(delay) = stream_poll_delay {
                                        std::thread::sleep(delay);
                                    }
                                    if scan_signals
                                        .cancelled
                                        .load(std::sync::atomic::Ordering::Acquire)
                                    {
                                        return VectorStreamControl::Stop;
                                    }
                                    if allow_early_finish {
                                        if let Some(deadline) = scan_signals.finish_state() {
                                            return VectorStreamControl::Finish { deadline };
                                        }
                                    }
                                    if share_floors {
                                        let f = f32::from_bits(
                                            scan_signals
                                                .floor
                                                .load(std::sync::atomic::Ordering::Acquire),
                                        );
                                        if f > floor_now {
                                            floor_now = f;
                                            raises += 1;
                                            return VectorStreamControl::RaiseFloor(f);
                                        }
                                    }
                                    VectorStreamControl::Continue
                                },
                            )
                            .map_err(|e| Status::invalid_argument(e.to_string()))?;
                        let identities = if identity_enabled {
                            let identities = guard.bm25.as_ref()
                                .map(Bm25Shard::identity_snapshot).transpose()
                                .map_err(Status::internal)?.unwrap_or_default();
                            let rerank_candidates = if start.include_rerank_candidate {
                                Some(
                                    emitted_slots
                                        .into_iter()
                                        .filter_map(|slot| {
                                            guard.rerank_candidate_for(slot).map(|c| (slot, c))
                                        })
                                        .collect(),
                                )
                            } else {
                                None
                            };
                            Some(crate::query_identity::ScanIdentities::new(
                                slot_offset,
                                index.len(),
                                identities,
                                allow,
                                rerank_candidates,
                            )?)
                        } else { None };
                        let cancelled_now = scan_signals
                            .cancelled
                            .load(std::sync::atomic::Ordering::Acquire);
                        let completed = summary.completed && !cancelled_now;
                        let end_reason = if !completed {
                            crate::vector::VectorStreamEndReason::Abandoned.to_proto()
                        } else {
                            summary.end_reason.to_proto()
                        };
                        Ok((StreamSearchSummary {
                            completed,
                            emitted: summary.emitted as u64,
                            blocks_scanned: summary.units_scanned as u64,
                            floor_raises_applied: raises,
                            geo_columns_known,
                            filter_columns_known,
                            scoring_fingerprint,
                            segments_total: prune.segments_total,
                            segments_skipped: prune.segments_skipped,
                            filter_regimes: summary.regimes.to_proto(),
                            tail_enumerated_rows: summary.tail_enumerated_rows,
                            patience_stopped: summary.patience_stopped,
                            end_reason,
                            floor_pruned: summary.floor_pruned,
                        }, identities))
                    });
                let outcome = scan.await;
                if let Some(token) = udp_token {
                    stream_signals
                        .lock()
                        .expect("stream signal registry poisoned")
                        .remove(&token);
                }
                let identity_deadline = identity_timeout.map(|timeout| std::time::Instant::now() + timeout);
                let exchange = async {
                    let (mut summary, identities) = outcome
                        .map_err(|e| Status::internal(format!("stream scan panicked: {e}")))??;
                    if let (Some(limits), Some(identities)) = (identity_limits.as_ref(), identities) {
                        if summary.completed {
                            identity_ready.store(true, AtomicOrdering::Release);
                            tx.send(Ok(StreamSearchResponse {
                                payload: Some(stream_search_response::Payload::IdentityReady(
                                    crate::pb::StreamIdentityReady { scan: Some(summary.clone()), range: identities.range() },
                                )),
                            })).await.map_err(|_| Status::cancelled("identity response stream closed"))?;
                            let selection = (&mut pump).await
                                .map_err(|e| Status::internal(format!("identity request task failed: {e}")))??;
                            match selection {
                                Some(selection) => {
                                    let limits = *limits;
                                    let cancelled = Arc::clone(&signals);
                                    let response = tokio::task::spawn_blocking(move || {
                                        identities.resolve_until(selection, &limits,
                                            identity_deadline.expect("identity timeout validated"), &cancelled.cancelled)
                                    }).await.map_err(|e| Status::internal(format!("identity selection task failed: {e}")))??;
                                    tx.send(Ok(response)).await
                                        .map_err(|_| Status::cancelled("identity response stream closed"))?;
                                }
                                None => {
                                    summary.completed = false;
                                    summary.end_reason = crate::pb::StreamEndReason::Abandoned as i32;
                                }
                            }
                        } else if pump.is_finished() {
                            (&mut pump).await.map_err(|e| Status::internal(format!("identity request task failed: {e}")))??;
                        }
                    } else if pump.is_finished() {
                        (&mut pump).await.map_err(|e| Status::internal(format!("stream request task failed: {e}")))??;
                    }
                    tx.send(Ok(StreamSearchResponse {
                        payload: Some(stream_search_response::Payload::Summary(summary)),
                    })).await.map_err(|_| Status::cancelled("response stream closed"))
                };
                let result = if let Some(timeout) = identity_timeout {
                    tokio::select! {
                        _ = tx.closed() => Err(Status::cancelled("identity response stream closed")),
                        result = tokio::time::timeout(timeout, exchange) => result.unwrap_or_else(|_| {
                            Err(Status::deadline_exceeded("stream identity resolution timed out"))
                        }),
                    }
                } else { exchange.await };
                pump.abort();
                if let Err(status) = result {
                    signals.cancelled.store(true, AtomicOrdering::Release);
                    let _ = tx.send(Err(status)).await;
                }
            });

            Ok(Response::new(ReceiverStream::new(rx)))
        })
        .await
    }

    pub(super) async fn handle_vector_rescore(
        &self,
        request: Request<VectorRescoreRequest>,
    ) -> Result<Response<VectorRescoreResponse>, Status> {
        crate::metrics::timed(Route::VectorRescore, request, |request| async move {
            let req = request.into_inner();
            let offset = self.config.slot_offset;
            let state = self.state.clone();
            let response =
                tokio::task::spawn_blocking(move || -> Result<VectorRescoreResponse, Status> {
                    let scope = crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
                    let guard = read_shard(&state);
                    guard.check_stats_epoch(
                        req.expected_stats_epoch,
                        &req.expected_stats_incarnation,
                    )?;
                    let (_, visibility_columns_known) = filter_known_flags(
                        guard.bm25.as_ref(),
                        &[],
                        req.visibility
                            .as_ref()
                            .and_then(|view| view.filter.as_ref()),
                    );
                    let mut response = VectorRescoreResponse {
                        vector_binding: guard.vector_binding(&req.field)?,
                        stats_epoch: guard.stats_epoch,
                        stats_incarnation: guard.stats_incarnation.bytes()?,
                        visibility_fingerprint: scope.fingerprint().to_vec(),
                        visibility_columns_known,
                        ..Default::default()
                    };
                    // A shard with no index holds none of the candidates.
                    let Some(index) = guard.index.as_ref() else {
                        return Ok(response);
                    };
                    let Some(dim) = index.dim_opt() else {
                        return Ok(response);
                    };
                    if req.vector.len() != dim {
                        return Err(Status::invalid_argument(format!(
                            "query vector has dim {}, index expects {dim}",
                            req.vector.len()
                        )));
                    }
                    if let Some((_, coord, value)) = first_invalid_coordinate(&req.vector, dim) {
                        return Err(Status::invalid_argument(format!(
                            "query coordinate {coord} is invalid: {value}"
                        )));
                    }
                    // Route global ids into this shard's live slots; the mask
                    // names slots and is sized to the slot count (slots are
                    // dense on the mainline engine: no capacity/len split). The
                    // kernel short-circuits fully-masked SIMD blocks, so a tiny
                    // allowlist costs a mask walk, not a scan.
                    let n = index.len();
                    let mut mask = vec![false; index.len()];
                    let slots = guard.visible_candidate_slots(
                        req.visibility.as_ref(),
                        &req.candidate_ids,
                        offset,
                        n,
                    )?;
                    let allowed = slots.len();
                    for slot in slots {
                        mask[slot] = true;
                    }
                    if allowed == 0 {
                        return Ok(response);
                    }
                    let results = index
                        .try_search_with_mask(&req.vector, allowed, Some(&mask))
                        .map_err(|e| Status::invalid_argument(e.to_string()))?;
                    let hits = results
                        .indices_for_query(0)
                        .iter()
                        .zip(results.scores_for_query(0))
                        .filter(|&(&slot, _)| slot >= 0)
                        .map(|(&slot, &score)| RawLegHit {
                            doc_id: offset + slot as u64,
                            score,
                            rerank_candidate: None,
                        })
                        .collect();
                    response.hits = hits;
                    Ok(response)
                })
                .await
                .map_err(|e| Status::internal(format!("vector rescore task failed: {e}")))??;
            Ok(Response::new(response))
        })
        .await
    }

    pub(super) async fn handle_exact_vector_rescore(
        &self,
        request: Request<ExactVectorRescoreRequest>,
    ) -> Result<Response<ExactVectorRescoreResponse>, Status> {
        crate::metrics::timed(Route::ExactVectorRescore, request, |request| async move {
        let req = request.into_inner();
        let offset = self.config.slot_offset;
        let state = self.state.clone();
        let parallel = resolved_rerank_parallel(self.config.rerank_parallel);
        let lanes = crate::exact_vectors::rerank_task_count(req.candidate_ids.len(), parallel);
        let permits = self
            .rerank_slots
            .clone()
            .acquire_many_owned(lanes as u32)
            .await
            .map_err(|_| Status::unavailable("exact rerank worker budget closed"))?;
        let result = tokio::task::spawn_blocking(move || -> Result<ExactVectorRescoreResponse, Status> {
            let _permits = permits;
            let scope = crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
            let guard = read_shard(&state);
            guard.check_stats_epoch(req.expected_stats_epoch, &req.expected_stats_incarnation)?;
            let (_, visibility_columns_known) = filter_known_flags(guard.bm25.as_ref(), &[],
                req.visibility.as_ref().and_then(|view| view.filter.as_ref()));
            let response = ExactVectorRescoreResponse {
                vector_binding: guard.vector_binding(&req.field)?,
                stats_epoch: guard.stats_epoch,
                stats_incarnation: guard.stats_incarnation.bytes()?,
                visibility_fingerprint: scope.fingerprint().to_vec(),
                visibility_columns_known, ..Default::default()
            };
            // Product shards may own exact rows for a clustered vector
            // collection without also serving a local provider image. The
            // product slot range is therefore the maximum aligned artifact,
            // not `index.len()` alone.
            let n = guard
                .index
                .as_ref()
                .map_or(0, VectorIndex::len)
                .max(guard.exact_vectors.as_ref().map_or(0, ExactVectorStore::len))
                .max(
                    guard
                        .bm25
                        .as_ref()
                        .map_or(0, |store| store.next_doc_id() as usize),
                );
            let slots = guard.visible_candidate_slots(req.visibility.as_ref(), &req.candidate_ids, offset, n)?;
            if slots.is_empty() {
                return Ok(response);
            }
            let exact = guard.exact_vectors.as_ref().ok_or_else(|| {
                Status::failed_precondition(format!(
                    "FP32 rerank requested for shard slots {offset}..{} but this generation \
                     has no exact-vector sidecar; rebuild or backfill it",
                    offset + n as u64
                ))
            })?;
            if let Some(index) = guard.index.as_ref() {
                if exact.len() != index.len() || exact.dim() != index.dim_opt() {
                    return Err(Status::failed_precondition(format!(
                        "exact-vector sidecar shape {:?}x{} does not match provider shape {:?}x{}",
                        exact.dim(),
                        exact.len(),
                        index.dim_opt(),
                        index.len()
                    )));
                }
            }
            let dim = exact.dim().ok_or_else(|| {
                Status::failed_precondition("exact-vector sidecar has no dimension")
            })?;
            let slots: Vec<usize> = slots.into_iter().filter(|&slot| exact.contains_row(slot)).collect();
            let predicted = (slots.len() as u64)
                .checked_mul(dim as u64)
                .and_then(|bytes| bytes.checked_mul(4))
                .ok_or_else(|| Status::resource_exhausted("FP32 rerank byte count overflow"))?;
            if req.max_logical_bytes != 0 && predicted > req.max_logical_bytes {
                return Err(Status::resource_exhausted(format!(
                    "FP32 rerank needs {predicted} logical row bytes on this shard, above max_logical_bytes={}",
                    req.max_logical_bytes
                )));
            }
            let scored = exact
                .score_slots_profiled(&req.vector, &slots, parallel)
                .map_err(|e| Status::invalid_argument(e.to_string()))?;
            Ok(ExactVectorRescoreResponse {
                hits: scored
                    .rows
                    .into_iter()
                    .map(|(slot, score)| RawLegHit {
                        doc_id: offset + slot as u64,
                        score,
                        rerank_candidate: None,
                    })
                    .collect(),
                logical_bytes: scored.logical_bytes,
                pages_touched: scored.pages_touched,
                tasks: scored.tasks,
                ..response
            })
        })
        .await
        .map_err(|e| Status::internal(format!("exact vector rescore task failed: {e}")))??;
        Ok(Response::new(result))
        })
        .await
    }
}
