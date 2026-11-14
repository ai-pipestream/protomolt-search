//! Term statistics, BM25 query, hybrid leg, and SPLADE RPC handlers.

use crate::node::*;

impl NodeServiceImpl {
    pub(super) async fn handle_term_stats(
        &self,
        request: Request<TermStatsRequest>,
    ) -> Result<Response<TermStatsResponse>, Status> {
        crate::metrics::timed(Route::TermStats, request, |request| async move {
            let req = request.into_inner();
            crate::visibility::validate_stats_request(&req)?;
            let scope = crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
            let visibility_filter = req
                .visibility
                .as_ref()
                .and_then(|view| view.filter.as_ref());
            let guard = read_shard(&self.state);
            if req.version_only {
                let (_, visibility_columns_known) =
                    filter_known_flags(guard.bm25.as_ref(), &[], visibility_filter);
                return Ok(Response::new(TermStatsResponse {
                    version_only: true,
                    stats_epoch: guard.stats_epoch,
                    stats_incarnation: guard.stats_incarnation.bytes()?,
                    visibility_fingerprint: scope.fingerprint().to_vec(),
                    visibility_columns_known,
                    ..Default::default()
                }));
            }
            if guard
                .bm25
                .as_ref()
                .is_some_and(|store| store.as_index().is_none())
            {
                return Err(Status::failed_precondition(
                    "bm25 bulk build in progress; Flush first",
                ));
            }
            let (_, visibility_columns_known) =
                filter_known_flags(guard.bm25.as_ref(), &[], visibility_filter);
            let visibility = if visibility_filter.is_some() {
                let slots = guard
                    .bm25
                    .as_ref()
                    .map_or(0, |store| store.next_doc_id() as usize);
                let (_, allow, _) = resolve_shard_filters(
                    guard.bm25.as_ref(),
                    None,
                    slots,
                    &[],
                    &[],
                    visibility_filter,
                    self.knobs.segment_pruning(),
                    None,
                )?;
                allow
            } else {
                None
            };
            let (doc_count, total_doc_length, doc_frequencies, field_stats) =
                match guard.bm25.as_ref() {
                    Some(store) => {
                        let index = store.as_index().ok_or_else(|| {
                            Status::failed_precondition("bm25 bulk build in progress; Flush first")
                        })?;
                        // Per-field shares: a shard without a named field
                        // answers zeros — that IS its share of the globals.
                        let field_stats = req
                            .fields
                            .iter()
                            .map(|ft| match store.field_index(&ft.field) {
                                Some(fi) => {
                                    let view = store
                                        .field_view(fi)
                                        .expect("as_index above proves the shard is searchable");
                                    let (total_doc_length, doc_frequencies) = live_field_stats(
                                        view.as_ref(),
                                        &ft.terms,
                                        &guard.live_docs,
                                        store.next_doc_id(),
                                        visibility.as_ref(),
                                    );
                                    crate::pb::FieldStats {
                                        sentences: store.field_has_sentences(fi),
                                        total_doc_length,
                                        doc_frequencies,
                                        known: true,
                                        positions: store.field_has_positions(fi),
                                    }
                                }
                                None => crate::pb::FieldStats {
                                    sentences: false,
                                    total_doc_length: 0,
                                    doc_frequencies: vec![0; ft.terms.len()],
                                    known: false,
                                    positions: false,
                                },
                            })
                            .collect();
                        let (total_doc_length, doc_frequencies) = live_field_stats(
                            index,
                            &req.terms,
                            &guard.live_docs,
                            store.next_doc_id(),
                            visibility.as_ref(),
                        );
                        (
                            live_document_count(store, &guard.live_docs, visibility.as_ref()),
                            total_doc_length,
                            doc_frequencies,
                            field_stats,
                        )
                    }
                    None => (
                        0,
                        0,
                        req.terms.iter().map(|_| 0).collect(),
                        // No postings at all: this shard knows no field, which is
                        // a different statement from "the field does not exist".
                        req.fields
                            .iter()
                            .map(|ft| crate::pb::FieldStats {
                                sentences: false,
                                total_doc_length: 0,
                                doc_frequencies: vec![0; ft.terms.len()],
                                known: false,
                                positions: false,
                            })
                            .collect(),
                    ),
                };
            Ok(Response::new(TermStatsResponse {
                version_only: false,
                doc_count,
                total_doc_length,
                doc_frequencies,
                field_stats,
                stats_epoch: guard.stats_epoch,
                stats_incarnation: guard.stats_incarnation.bytes()?,
                visibility_fingerprint: scope.fingerprint().to_vec(),
                visibility_columns_known,
            }))
        })
        .await
    }

    pub(super) async fn handle_expand_term_prefix(
        &self,
        request: Request<crate::pb::ExpandTermPrefixRequest>,
    ) -> Result<Response<crate::pb::ExpandTermPrefixResponse>, Status> {
        crate::metrics::timed(Route::ExpandTermPrefix, request, |request| async move {
            let req = request.into_inner();
            let result = self.scan_dictionary(
                &req.field,
                &req.prefix,
                req.cap as usize,
                req.visibility.as_ref(),
            )?;
            Ok(Response::new(crate::pb::ExpandTermPrefixResponse {
                terms: result.entries.into_iter().map(|entry| entry.term).collect(),
                count: result.count,
                known: result.known,
                visibility_fingerprint: result.visibility_fingerprint,
                visibility_columns_known: result.visibility_columns_known,
            }))
        })
        .await
    }

    pub(super) async fn handle_suggest_terms(
        &self,
        request: Request<crate::pb::SuggestTermsRequest>,
    ) -> Result<Response<crate::pb::SuggestTermsResponse>, Status> {
        crate::metrics::timed(Route::SuggestTerms, request, |request| async move {
            let req = request.into_inner();
            let cap = usize::try_from(req.max_scan)
                .map_err(|_| Status::invalid_argument("max_scan is out of range"))?;
            if cap == 0 {
                return Err(Status::invalid_argument(
                    "max_scan must be positive: it bounds the dictionary scan",
                ));
            }
            self.scan_dictionary(&req.field, &req.prefix, cap, req.visibility.as_ref())
                .map(Response::new)
        })
        .await
    }

    pub(super) async fn handle_bm25_query(
        &self,
        request: Request<Bm25QueryRequest>,
    ) -> Result<Response<Bm25QueryResponse>, Status> {
        crate::metrics::timed(Route::Bm25Query, request, |request| async move {
            let service = self.clone();
            let req = request.into_inner();
            tokio::task::spawn_blocking(move || service.run_bm25_query(req))
                .await
                .map_err(|e| Status::internal(format!("bm25 query task failed: {e}")))?
                .map(Response::new)
        })
        .await
    }

    pub(super) async fn handle_bm25_phrase_query(
        &self,
        request: Request<crate::pb::Bm25PhraseQueryRequest>,
    ) -> Result<Response<Bm25QueryResponse>, Status> {
        crate::metrics::timed(Route::Bm25PhraseQuery, request, |request| async move {
            let service = self.clone();
            let req = request.into_inner();
            tokio::task::spawn_blocking(move || service.run_bm25_phrase_query(req))
                .await
                .map_err(|error| {
                    Status::internal(format!("bm25 phrase query task failed: {error}"))
                })?
                .map(Response::new)
        })
        .await
    }

    pub(super) async fn handle_bm25_query_stream(
        &self,
        request: Request<Streaming<Bm25QueryStreamRequest>>,
    ) -> Result<Response<<Self as NodeService>::Bm25QueryStreamStream>, Status> {
        crate::metrics::timed_stream(Route::Bm25QueryStream, request, |request| async move {
            let mut inbound = request.into_inner();
            let (tx, rx) = mpsc::channel::<Result<Bm25QueryStreamResponse, Status>>(64);
            let service = self.clone();

            tokio::spawn(async move {
                // Protocol: the first message must be Start.
                let req = match inbound.message().await {
                    Ok(Some(Bm25QueryStreamRequest {
                        payload: Some(bm25_query_stream_request::Payload::Start(req)),
                    })) => req,
                    Ok(_) => {
                        let _ = tx
                            .send(Err(Status::invalid_argument(
                                "first Bm25QueryStreamRequest must be a Bm25QueryRequest start",
                            )))
                            .await;
                        return;
                    }
                    Err(e) => {
                        let _ = tx.send(Err(e)).await;
                        return;
                    }
                };

                // Floor updates arrive on the same stream; a pump task folds
                // them into a watch cell the blocking scan polls each loop
                // iteration. Updates are monotone maxes, so only raises are
                // stored — exactly the SearchShard pump.
                let (floor_tx, floor_rx) = watch::channel(f32::NEG_INFINITY);
                let cancelled = Arc::new(AtomicBool::new(false));
                let pump_cancelled = Arc::clone(&cancelled);
                tokio::spawn(async move {
                    loop {
                        match inbound.message().await {
                            Ok(Some(Bm25QueryStreamRequest {
                                payload: Some(bm25_query_stream_request::Payload::FloorUpdate(u)),
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
                            Ok(Some(Bm25QueryStreamRequest {
                                payload: Some(bm25_query_stream_request::Payload::Stop(_)),
                            })) => {
                                pump_cancelled.store(true, AtomicOrdering::Release);
                                break;
                            }
                            // Duplicate Start or empty payload: ignore.
                            Ok(Some(_)) => {}
                            // Client closed or the stream broke: stop pumping;
                            // the scan finishes on its own either way.
                            Ok(None) | Err(_) => break,
                        }
                    }
                });

                // The scorer-side hook: publish strict raises of the running
                // k-th best (gated by the node's floor knobs, never blocking
                // the scan — dropped raises are superseded by the next one),
                // and hand back the highest coordinator floor seen.
                let share = service.knobs.share_floors();
                let floor_delta = service.knobs.floor_delta();
                let floor_min_interval_ms = service.knobs.floor_min_interval_ms();
                let min_interval = (floor_min_interval_ms > 0)
                    .then(|| std::time::Duration::from_millis(floor_min_interval_ms));
                let scan_tx = tx.clone();
                let hook_cancelled = Arc::clone(&cancelled);
                let mut last_published = f32::NEG_INFINITY;
                let mut last_at: Option<std::time::Instant> = None;
                let mut hook = move |seed: Option<f32>| -> Option<f32> {
                    if hook_cancelled.load(AtomicOrdering::Acquire) || scan_tx.is_closed() {
                        hook_cancelled.store(true, AtomicOrdering::Release);
                        return Some(f32::INFINITY);
                    }
                    if !share {
                        return None;
                    }
                    if let Some(seed) = seed {
                        let debounced = matches!(
                            (min_interval, last_at),
                            (Some(interval), Some(at)) if at.elapsed() < interval
                        );
                        if seed > last_published + floor_delta && !debounced {
                            last_published = seed;
                            last_at = Some(std::time::Instant::now());
                            let _ = scan_tx.try_send(Ok(Bm25QueryStreamResponse {
                                payload: Some(bm25_query_stream_response::Payload::FloorUpdate(
                                    FloorUpdate { floor: seed },
                                )),
                            }));
                        }
                    }
                    let f = *floor_rx.borrow();
                    (f != f32::NEG_INFINITY).then_some(f)
                };

                let scoring_fingerprint = bm25_scoring_fingerprint(&req);
                let candidate_tx = tx.clone();
                let scan_cancelled = Arc::clone(&cancelled);
                let outcome = tokio::task::spawn_blocking(move || {
                    // Stay below tonic's default 4 MiB even if a caller lowers
                    // the process-wide cap. 64 KiB amortizes framing without
                    // delaying the first useful candidates.
                    const BATCH_BYTES: usize = (64 * 1024 / 12) * 12;
                    let mut pending = Vec::with_capacity(BATCH_BYTES);
                    let mut emitted = 0u64;
                    let response = {
                        let mut emit = |doc_id: u32, score: f32| {
                            pending.extend_from_slice(
                                &(service.config.slot_offset + u64::from(doc_id)).to_le_bytes(),
                            );
                            pending.extend_from_slice(&score.to_le_bytes());
                            if pending.len() >= BATCH_BYTES {
                                let records = (pending.len() / 12) as u64;
                                let batch = std::mem::replace(
                                    &mut pending,
                                    Vec::with_capacity(BATCH_BYTES),
                                );
                                if candidate_tx
                                    .blocking_send(Ok(Bm25QueryStreamResponse {
                                        payload: Some(
                                            bm25_query_stream_response::Payload::CandidateBatch(
                                                Bm25CandidateBatch { candidates: batch },
                                            ),
                                        ),
                                    }))
                                    .is_ok()
                                {
                                    emitted += records;
                                } else {
                                    scan_cancelled.store(true, AtomicOrdering::Release);
                                }
                            }
                        };
                        service.run_bm25_query_live(req, Some(&mut hook), Some(&mut emit))
                    };
                    if response.is_ok() && !pending.is_empty() {
                        let records = (pending.len() / 12) as u64;
                        if candidate_tx
                            .blocking_send(Ok(Bm25QueryStreamResponse {
                                payload: Some(bm25_query_stream_response::Payload::CandidateBatch(
                                    Bm25CandidateBatch {
                                        candidates: pending,
                                    },
                                )),
                            }))
                            .is_ok()
                        {
                            emitted += records;
                        } else {
                            scan_cancelled.store(true, AtomicOrdering::Release);
                        }
                    }
                    let completed = !scan_cancelled.load(AtomicOrdering::Acquire);
                    (response, completed, emitted)
                })
                .await
                .unwrap_or_else(|e| {
                    (
                        Err(Status::internal(format!("bm25 stream task failed: {e}"))),
                        false,
                        0,
                    )
                });
                let _ = match outcome {
                    (Ok(response), completed, candidates_emitted) => {
                        tx.send(Ok(Bm25QueryStreamResponse {
                            payload: Some(bm25_query_stream_response::Payload::Completion(
                                Bm25StreamCompletion {
                                    completed,
                                    response: completed.then_some(response),
                                    scoring_fingerprint,
                                    candidates_emitted,
                                },
                            )),
                        }))
                        .await
                    }
                    (Err(e), _, _) => tx.send(Err(e)).await,
                };
            });

            Ok(Response::new(ReceiverStream::new(rx)))
        })
        .await
    }

    pub(super) async fn handle_bm25_rescore(
        &self,
        request: Request<Bm25RescoreRequest>,
    ) -> Result<Response<Bm25RescoreResponse>, Status> {
        crate::metrics::timed(Route::Bm25Rescore, request, |request| async move {
            let service = self.clone();
            let req = request.into_inner();
            tokio::task::spawn_blocking(move || service.run_bm25_rescore(req))
                .await
                .map_err(|e| Status::internal(format!("bm25 rescore task failed: {e}")))?
                .map(Response::new)
        })
        .await
    }

    pub(super) async fn handle_hybrid_shard(
        &self,
        request: Request<HybridShardRequest>,
    ) -> Result<Response<HybridShardResponse>, Status> {
        crate::metrics::timed(Route::HybridShard, request, |request| async move {
            let service = self.clone();
            tokio::task::spawn_blocking(move || service.run_hybrid(request.into_inner()))
                .await
                .map_err(|e| Status::internal(format!("hybrid task failed: {e}")))?
                .map(Response::new)
        })
        .await
    }

    pub(super) async fn handle_shard_legs(
        &self,
        request: Request<ShardLegsRequest>,
    ) -> Result<Response<ShardLegsResponse>, Status> {
        crate::metrics::timed(Route::ShardLegs, request, |request| async move {
            let req = request.into_inner();
            if req.terms.len() != req.global_doc_frequencies.len() {
                return Err(Status::invalid_argument(
                    "terms and global_doc_frequencies must have the same length",
                ));
            }
            let geo_regions = validate_geo_filters(&req.geo_filters)?;
            if let Some(f) = req.filter.as_ref() {
                crate::filter::validate_filter(f)?;
            }
            let service = self.clone();
            tokio::task::spawn_blocking(move || {
                let mut legs = service.compute_legs(
                    &req.vector,
                    &req.terms,
                    req.analysis_fingerprint,
                    req.global_doc_count,
                    req.global_total_doc_length,
                    &req.global_doc_frequencies,
                    params_from(req.k1, req.b)?,
                    req.k as usize,
                    req.expected_stats_epoch,
                    &req.expected_stats_incarnation,
                    &LegFilters {
                        geo: &req.geo_filters,
                        regions: geo_regions,
                        tree: req.filter.as_ref(),
                    },
                    req.read_context.as_ref(),
                    req.include_rerank_candidate,
                )?;
                Ok(ShardLegsResponse {
                    vector_hits: legs
                        .vector
                        .into_iter()
                        .map(|(doc_id, score)| RawLegHit {
                            doc_id,
                            score: score as f32,
                            rerank_candidate: legs.rerank_candidates.get(&doc_id).cloned(),
                        })
                        .collect(),
                    bm25_hits: legs
                        .bm25
                        .into_iter()
                        .map(|(doc_id, score)| RawLegHit {
                            doc_id,
                            score: score as f32,
                            rerank_candidate: legs.rerank_candidates.remove(&doc_id),
                        })
                        .collect(),
                    geo_columns_known: legs.geo_columns_known,
                    filter_columns_known: legs.filter_columns_known,
                    read_receipt: legs.read_receipt,
                })
            })
            .await
            .map_err(|e| Status::internal(format!("shard legs task failed: {e}")))?
            .map(Response::new)
        })
        .await
    }

    pub(super) async fn handle_score_splade(
        &self,
        request: Request<crate::pb::ScoreSpladeRequest>,
    ) -> Result<Response<crate::pb::ScoreSpladeResponse>, Status> {
        crate::metrics::timed(Route::ScoreSplade, request, |request| async move {
            let service = self.clone();
            tokio::task::spawn_blocking(move || service.run_score_splade(request.into_inner()))
                .await
                .map_err(|e| Status::internal(format!("score splade task failed: {e}")))?
                .map(Response::new)
        })
        .await
    }
}
