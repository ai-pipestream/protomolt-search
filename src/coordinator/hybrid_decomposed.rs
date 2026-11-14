//! Hybrid decomposed.

use super::hybrid_context::{HybridQuery, HybridRound};
use super::*;

/// Total-order f64 wrapper for heap keys.
#[derive(PartialEq)]
pub(super) struct F64Ord(pub(super) f64);

impl Eq for F64Ord {}

impl PartialOrd for F64Ord {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for F64Ord {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.0.total_cmp(&other.0)
    }
}

/// Magnitude-relative slack for the decomposed-floor bounds: 2^-40 of
/// the participating magnitudes, about four thousand times the worst
/// accumulated f64 rounding error of the three operations involved and
/// one part in 10^12 of the scores — provably conservative, immeasurably
/// loose.
pub(super) const SLACK_REL: f64 = 1.0 / (1u64 << 40) as f64;

/// The decomposed vector floor for a fused lower bound `s_lb`:
/// mathematically (s_lb - w_b * b_1) / w_v, rounded DOWN past every
/// f64 rounding error and the f32 cast, so that v(d) < result implies
/// w_v * v(d) + w_b * b(d) < s_lb for every doc (b(d) <= b_1 by the
/// exactness of the BM25 leg). `wb_b1` arrives premultiplied.
pub(super) fn decomposed_floor(s_lb: f64, wb_b1: f64, w_v: f64) -> f32 {
    let t = (s_lb - wb_b1) / w_v;
    let mag = t.abs() + (s_lb.abs() + wb_b1.abs()) / w_v;
    let safe = t - mag * SLACK_REL - f64::MIN_POSITIVE;
    (safe as f32).next_down()
}

impl CoordinatorServiceImpl {
    /// FUSION_MODE_DECOMPOSED: the EXACT fused weighted-sum top-k
    ///
    ///   fused(d) = w_v * v(d) + w_b * b(d)
    ///
    /// executed BM25-first with decomposed floors over the streaming
    /// vector path (the proto's FusionMode comment walks the phases;
    /// docs/multi-field.md "Hybrid streaming interplay" has the floor
    /// algebra). The result set is the vector index: b(d) joins on the
    /// shared positional id space, exactly 0 for docs no query term
    /// matches.
    ///
    /// The exactness ledger, kept by construction:
    /// - the BM25 leg is the exact global top-leg_k, so its top score
    ///   `b_1` is the exact global maximum of b(d), and its boundary
    ///   score bounds b(d) for every doc outside the shard lists;
    /// - every vector floor F pushed here satisfies: v(d) < F implies
    ///   fused(d) < s_lb <= the final k-th best fused score, so a doc
    ///   the floor suppressed can never belong to the top-k, and ties
    ///   AT the floor survive shard-side (emission is score >= floor);
    /// - every candidate ends with EXACT leg scores: v from the stream
    ///   or from `VectorRescore` (bitwise identical — one kernel, one
    ///   calibration), b from the leg, from `Bm25Rescore`, or exactly
    ///   0 when the unfilled leg proves no further doc matches.
    // Without the network stack the clustered arm is compiled out and
    // its inputs go unread; the exact arm is the whole function then.
    #[cfg_attr(not(feature = "net"), allow(unused_variables, unused_mut))]
    pub(super) async fn fanout_hybrid_decomposed(
        &self,
        round: HybridRound<'_>,
    ) -> Result<(Vec<HybridHit>, Option<HybridDebug>), Status> {
        let HybridRound {
            query,
            terms,
            analysis_fingerprint,
            global,
            claims,
        } = round;
        let HybridQuery {
            request_id,
            vector,
            k,
            legs,
            debug,
            filters,
            ..
        } = query;
        let n_nodes = self.node_addrs.len();
        if n_nodes == 0 {
            return Err(Status::failed_precondition("no shard nodes configured"));
        }
        // Both legs carry the same filters. The decomposed floor algebra
        // is untouched by them: a filter only REMOVES documents, so
        // every bound that dominated the unfiltered corpus still
        // dominates the survivors (docs/vector-filters.md).
        let mask = self.vector_scan_mask(filters.tree.as_ref());
        let mut known = Self::filter_known(filters, mask.as_ref());
        let w_v = f64::from(legs.vector_weight);
        let w_b = f64::from(legs.bm25_weight);
        let fused_of = |v: f32, b: f32| w_v * f64::from(v) + w_b * f64::from(b);
        let min_v = (legs.min_vector_score > 0.0).then_some(legs.min_vector_score);
        let t_legs = std::time::Instant::now();

        // Phase 1: the BM25 leg, exact global top-leg_k. Every score
        // any shard returned is exact for its doc, so all of them pin
        // b(d) — the merge only decides b_1, the boundary, and ranks.
        let mut bm25_of: HashMap<u64, (f32, u32)> = HashMap::new();
        let mut rerank_candidates: HashMap<u64, crate::pb::RerankCandidatePayload> = HashMap::new();
        let mut leg_counts: HashMap<u32, u32> = HashMap::new();
        let mut merged: Vec<(u64, f32, u32)> = Vec::new();
        if !terms.is_empty() {
            let admission = self.vector_read_barrier()?;
            let mut leg_tasks = Vec::with_capacity(n_nodes);
            for (shard, node) in self.node_addrs.iter().enumerate() {
                if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                    continue;
                }
                // Use the same guarded authority view as the vector pass.
                // A private lexical winner must not set the visible leg's
                // boundary or erase a visible document's rank provenance.
                let request = ShardLegsRequest {
                    analysis_fingerprint,
                    request_id: request_id.to_string(),
                    k: legs.leg_k,
                    vector: Vec::new(),
                    terms: terms.to_vec(),
                    global_doc_count: global.doc_count,
                    global_total_doc_length: global.total_doc_length,
                    global_doc_frequencies: global.dfs.clone(),
                    k1: self.bm25_params.k1 as f32,
                    b: self.bm25_params.b as f32,
                    expected_stats_epoch: claims[shard].epoch,
                    expected_stats_incarnation: claims[shard].incarnation(),
                    geo_filters: filters.geo.clone(),
                    filter: Self::shard_filter_tree(filters, mask.as_ref(), shard),
                    read_context: admission.as_ref().map(|a| a.context(shard)).transpose()?,
                    include_rerank_candidate: filters.include_rerank_candidate,
                };
                let mut client = self.node_client(node)?;
                leg_tasks.push(tokio::spawn(async move {
                    client.shard_legs(request).await.map(|r| {
                        let r = r.into_inner();
                        (
                            shard as u32,
                            r.bm25_hits,
                            r.geo_columns_known,
                            r.filter_columns_known,
                            r.read_receipt,
                        )
                    })
                }));
            }
            for task in leg_tasks {
                let (shard, hits, geo_known, filter_known, receipt) = task
                    .await
                    .map_err(|e| Status::internal(format!("bm25 leg task failed: {e}")))??;
                match (&admission, receipt.as_ref()) {
                    (Some(admission), Some(receipt)) => {
                        admission.accept(shard as usize, receipt)?
                    }
                    (None, None) => {}
                    _ => {
                        return Err(Status::failed_precondition(
                            "decomposed leg read receipt mismatch",
                        ))
                    }
                }
                known.merge(&geo_known, &filter_known)?;
                leg_counts.insert(shard, hits.len() as u32);
                for h in hits {
                    bm25_of.insert(h.doc_id, (h.score, shard));
                    merged.push((h.doc_id, h.score, shard));
                    if let Some(c) = h.rerank_candidate {
                        rerank_candidates.entry(h.doc_id).or_insert(c);
                    }
                }
            }
            if let Some(admission) = &admission {
                admission.wait().await?;
            }
            merged.sort_by(|a, b| {
                b.1.total_cmp(&a.1)
                    .then_with(|| a.2.cmp(&b.2))
                    .then_with(|| a.0.cmp(&b.0))
            });
            merged.truncate(legs.leg_k as usize);
        }
        let b_1 = merged.first().map_or(0.0f32, |&(_, s, _)| s);
        let wb_b1 = w_b * f64::from(b_1);
        // The leg filled: docs outside every shard list score at most
        // the boundary. It did not: every matching doc is in some list,
        // so an absent doc scores exactly 0.
        let filled = merged.len() == legs.leg_k as usize && !merged.is_empty();
        let b_out: f64 = if filled {
            f64::from(merged.last().expect("filled leg is non-empty").1)
        } else {
            0.0
        };
        let bm25_rank: HashMap<u64, u32> = merged
            .iter()
            .enumerate()
            .map(|(i, &(doc, _, _))| (doc, i as u32 + 1))
            .collect();

        // Phase 2: pin v(d) for the leg's top k docs. Their true fused
        // scores are the first k-th-best lower bound — the seed a
        // BM25-only bound can never provide (the top lexical doc could
        // sit at any vector score, so no shard-wide vector floor
        // follows from the leg alone).
        let mut seed_ids: HashMap<u32, Vec<u64>> = HashMap::new();
        for &(doc, _, shard) in merged.iter().take(k as usize) {
            seed_ids.entry(shard).or_default().push(doc);
        }
        let v_of = self
            .fanout_vector_rescore(
                vector,
                seed_ids,
                self.vector_read_field.as_deref().unwrap_or(""),
            )
            .await?;

        // Known fused LOWER bounds, tracked as a k-sized min-heap: its
        // root is s_lb, the k-th best known bound, and every pushed
        // floor decomposes from it. A doc outside the leg contributes
        // w_v * v alone (b >= 0), still a valid lower bound.
        let mut flb_heap: std::collections::BinaryHeap<std::cmp::Reverse<F64Ord>> =
            std::collections::BinaryHeap::with_capacity(k as usize + 1);
        let push_flb = |heap: &mut std::collections::BinaryHeap<std::cmp::Reverse<F64Ord>>,
                        value: f64| {
            if heap.len() < k as usize {
                heap.push(std::cmp::Reverse(F64Ord(value)));
            } else if heap.peek().is_some_and(|r| value > r.0 .0) {
                heap.pop();
                heap.push(std::cmp::Reverse(F64Ord(value)));
            }
        };
        // Candidates: doc -> (v, owning shard). Seeded with the phase-2
        // rescores so a seed doc is a candidate even if the floor it
        // funded later suppresses its own emission.
        let mut v_seen: HashMap<u64, (f32, u32)> = HashMap::new();
        for (&doc, &v) in &v_of {
            let (b, shard) = bm25_of[&doc];
            v_seen.insert(doc, (v, shard));
            if min_v.is_none_or(|m| v >= m) {
                push_flb(&mut flb_heap, fused_of(v, b));
            }
        }
        let mut s_lb =
            (flb_heap.len() == k as usize).then(|| flb_heap.peek().expect("full heap").0 .0);
        let decomposed = s_lb.map(|s| decomposed_floor(s, wb_b1, w_v));
        // min_vector_score is a result-set gate, so it doubles as a
        // free starting floor: suppressed docs are excluded docs.
        let initial_floor = match (decomposed, min_v) {
            (Some(f), Some(m)) => Some(f.max(m)),
            (Some(f), None) => Some(f),
            (None, Some(m)) => Some(m),
            (None, None) => None,
        };

        // Phase 3: the streaming vector scan, floored from the first
        // block, with re-decomposed floors chasing the rising k-th
        // best known bound.
        let mut summaries: Vec<Option<StreamSearchSummary>> = vec![None; n_nodes];
        let mut clustered_counts = vec![0u32; n_nodes];
        let mut last_floor = initial_floor.unwrap_or(f32::NEG_INFINITY);
        let mut decomposed_stream_fanout: Option<(
            StreamFanout,
            Vec<Option<StreamSearchSummary>>,
            crate::pb::StreamIdentityLimits,
        )> = None;
        #[cfg(feature = "net")]
        if let Some(backend) = &self.clustered_vectors {
            let ranges = self.product_label_ranges().await?;
            let allowed = self.clustered_allowed_labels(filters).await?;
            let mut stream = backend
                .candidate_stream(request_id, vector.to_vec(), allowed, initial_floor)
                .await?;
            let mut provider_labels = std::collections::HashSet::new();
            let completion = loop {
                match stream.next_event().await? {
                    ClusteredCandidateEvent::Batch(batch) => {
                        for candidate in batch {
                            if !candidate.score.is_finite() {
                                return Err(Status::internal(format!(
                                    "clustered TurboVec label {} has non-finite score {}",
                                    candidate.label, candidate.score
                                )));
                            }
                            if !provider_labels.insert(candidate.label) {
                                return Err(Status::failed_precondition(format!(
                                    "clustered TurboVec emitted duplicate stable label {}",
                                    candidate.label
                                )));
                            }
                            let shard = Self::product_owner(&ranges, candidate.label)?;
                            clustered_counts[shard as usize] =
                                clustered_counts[shard as usize].saturating_add(1);
                            // A re-emitted phase-2 seed carries the identical
                            // score (one kernel); keep the first sighting.
                            if v_seen.contains_key(&candidate.label) {
                                continue;
                            }
                            v_seen.insert(candidate.label, (candidate.score, shard));
                            if min_v.is_some_and(|minimum| candidate.score < minimum) {
                                continue;
                            }
                            let b = bm25_of
                                .get(&candidate.label)
                                .map_or(0.0, |&(score, _)| score);
                            push_flb(&mut flb_heap, fused_of(candidate.score, b));
                        }
                        if flb_heap.len() == k as usize {
                            let bound = flb_heap.peek().expect("full heap").0 .0;
                            if s_lb.is_none_or(|score| bound > score) {
                                s_lb = Some(bound);
                                let floor = decomposed_floor(bound, wb_b1, w_v);
                                if floor > last_floor {
                                    stream.raise_floor(floor)?;
                                    last_floor = floor;
                                }
                            }
                        }
                    }
                    ClusteredCandidateEvent::Completion(completion) => break completion,
                }
            };
            if completion.emitted != provider_labels.len() as u64 {
                return Err(Status::internal(format!(
                    "clustered TurboVec completion counted {} candidates but decomposed search received {}",
                    completion.emitted,
                    provider_labels.len()
                )));
            }
        } else {
            self.ensure_shard_vector_rows().await;
            let decomposed_identity_limits =
                filters
                    .include_rerank_candidate
                    .then_some(crate::pb::StreamIdentityLimits {
                        max_rows: k,
                        max_response_bytes: 32 * 1024 * 1024,
                        timeout_ms: 60_000,
                    });
            let mut fanout = self.open_stream_fanout_with_identities(
                request_id,
                vector,
                initial_floor,
                Some(k),
                false,
                filters,
                decomposed_identity_limits,
            )?;
            let mut remaining = n_nodes;
            for (shard, summary) in summaries.iter_mut().enumerate() {
                if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                    *summary = Some(StreamSearchSummary {
                        completed: true,
                        ..Default::default()
                    });
                    remaining -= 1;
                }
            }
            let initial_terminal = summaries.clone();
            while remaining > 0 {
                let (shard, msg) = match fanout.next_message(&summaries).await {
                    Ok(Some(pair)) => pair,
                    Ok(None) => continue,
                    Err(status) => return fanout.cancel_with(status).await,
                };
                match msg.payload {
                    Some(stream_search_response::Payload::Batch(batch)) => {
                        if batch.hits.len() % 12 != 0 {
                            let status = Status::internal(format!(
                                "shard {shard} sent a misaligned batch of {} bytes",
                                batch.hits.len()
                            ));
                            return fanout.cancel_with(status).await;
                        }
                        for rec in batch.hits.as_chunks::<12>().0 {
                            let doc = u64::from_le_bytes(rec[..8].try_into().expect("8-byte id"));
                            let v =
                                f32::from_le_bytes(rec[8..12].try_into().expect("4-byte score"));
                            if v_seen.contains_key(&doc) {
                                continue;
                            }
                            v_seen.insert(doc, (v, shard as u32));
                            if min_v.is_some_and(|m| v < m) {
                                continue;
                            }
                            let b = bm25_of.get(&doc).map_or(0.0f32, |&(s, _)| s);
                            push_flb(&mut flb_heap, fused_of(v, b));
                        }
                        if flb_heap.len() == k as usize {
                            let m = flb_heap.peek().expect("full heap").0 .0;
                            if s_lb.is_none_or(|s| m > s) {
                                s_lb = Some(m);
                                let floor = decomposed_floor(m, wb_b1, w_v);
                                if floor > last_floor {
                                    last_floor = floor;
                                    self.push_stream_floor(&fanout, floor);
                                }
                            }
                        }
                    }
                    Some(stream_search_response::Payload::IdentityReady(ready))
                        if decomposed_identity_limits.is_some() =>
                    {
                        let Some(summary) = ready.scan else {
                            return fanout
                                .cancel_with(Status::internal(
                                    "IdentityReady has no scan certificate",
                                ))
                                .await;
                        };
                        if !summary.completed {
                            let status = Status::internal(format!(
                                "shard {shard} stopped before completing its scan"
                            ));
                            return fanout.cancel_with(status).await;
                        }
                        if let Err(error) = known.merge_shard(
                            shard,
                            &summary.geo_columns_known,
                            &summary.filter_columns_known,
                        ) {
                            return fanout.cancel_with(error).await;
                        }
                        summaries[shard] = Some(summary);
                        remaining -= 1;
                    }
                    Some(stream_search_response::Payload::Summary(summary))
                        if decomposed_identity_limits.is_none() =>
                    {
                        if !summary.completed {
                            let status = Status::internal(format!(
                                "shard {shard} stopped before completing its scan"
                            ));
                            return fanout.cancel_with(status).await;
                        }
                        if let Err(error) = known.merge_shard(
                            shard,
                            &summary.geo_columns_known,
                            &summary.filter_columns_known,
                        ) {
                            return fanout.cancel_with(error).await;
                        }
                        summaries[shard] = Some(summary);
                        fanout.mark_completed(shard);
                        remaining -= 1;
                    }
                    Some(_) => {
                        return fanout
                            .cancel_with(Status::internal("unexpected identity exchange on stream"))
                            .await
                    }
                    None => {}
                }
            }
            known.refuse_unknown(filters)?;
            if let Some(limits) = decomposed_identity_limits {
                decomposed_stream_fanout = Some((fanout, initial_terminal, limits));
            }
        }
        let legs_ms = t_legs.elapsed().as_secs_f32() * 1e3;

        // Close-out. Candidates with a leg score (or proven-zero b)
        // have exact fused scores already; the rest hold b in
        // [0, b_out] and need Bm25Rescore — unless even b_out cannot
        // lift them to the k-th best exact fused score known so far.
        let t_fusion = std::time::Instant::now();
        let mut exact: Vec<(u64, u32, f32, f32)> = Vec::new(); // (doc, shard, v, b)
        let mut unknown: Vec<(u64, u32, f32)> = Vec::new(); // (doc, shard, v)
        for (&doc, &(v, shard)) in &v_seen {
            if min_v.is_some_and(|m| v < m) {
                continue;
            }
            match bm25_of.get(&doc) {
                Some(&(b, _)) => exact.push((doc, shard, v, b)),
                None if !filled => exact.push((doc, shard, v, 0.0)),
                None => unknown.push((doc, shard, v)),
            }
        }
        let kth_known: f64 = {
            let mut fused: Vec<f64> = exact.iter().map(|&(_, _, v, b)| fused_of(v, b)).collect();
            if fused.len() >= k as usize {
                let idx = k as usize - 1;
                *fused.select_nth_unstable_by(idx, |a, b| b.total_cmp(a)).1
            } else {
                f64::NEG_INFINITY
            }
        };
        let mut rescore_ids: HashMap<u32, Vec<u64>> = HashMap::new();
        let mut rescore_docs: Vec<(u64, u32, f32)> = Vec::new();
        for (doc, shard, v) in unknown {
            // Conservative upper bound: round the two products and the
            // sum up by a magnitude-relative slack that dwarfs the f64
            // rounding error, so a doc dropped here provably cannot
            // reach the k-th best.
            let ub = w_v * f64::from(v) + w_b * b_out;
            let mag = ub.abs() + (w_v * f64::from(v)).abs() + w_b * b_out;
            let ub_safe = ub + mag * SLACK_REL + f64::MIN_POSITIVE;
            if ub_safe >= kth_known {
                rescore_ids.entry(shard).or_default().push(doc);
                rescore_docs.push((doc, shard, v));
            }
        }
        let rescored_b = self
            .fanout_bm25_rescore_scores(
                terms,
                analysis_fingerprint,
                global,
                claims,
                rescore_ids,
                &[],
            )
            .await?;
        for (doc, shard, v) in rescore_docs {
            // Absent from the rescore response = no query term matches
            // the doc: b is exactly 0.
            let b = rescored_b.get(&doc).copied().unwrap_or(0.0);
            exact.push((doc, shard, v, b));
        }

        let mut ranked: Vec<(u64, u32, f32, f32, f64)> = exact
            .into_iter()
            .map(|(doc, shard, v, b)| (doc, shard, v, b, fused_of(v, b)))
            .collect();
        ranked.sort_by(|a, b| {
            b.4.total_cmp(&a.4)
                .then_with(|| a.1.cmp(&b.1))
                .then_with(|| a.0.cmp(&b.0))
        });
        ranked.truncate(k as usize);
        if let Some((mut fanout, mut terminal, limits)) = decomposed_stream_fanout {
            let stream_hits: Vec<MergedHit> = ranked
                .iter()
                .filter(|(doc, _, _, _, _)| !rerank_candidates.contains_key(doc))
                .map(|&(doc, shard, v, _, _)| MergedHit {
                    vector_id: doc,
                    shard,
                    score: v,
                })
                .collect();
            let (_, stream_cands) = fanout
                .resolve_identities_and_candidates(&stream_hits, &summaries, &mut terminal, &limits)
                .await?;
            for ((_, doc), cand) in stream_cands {
                if let Some(c) = cand {
                    rerank_candidates.entry(doc).or_insert(c);
                }
            }
        }
        let hits: Vec<HybridHit> = ranked
            .into_iter()
            .map(|(doc, shard, v, b, fused)| HybridHit {
                doc_id: doc,
                fused_score: fused as f32,
                shard,
                vector_rank: None,
                vector_score: v,
                bm25_rank: bm25_rank.get(&doc).copied(),
                bm25_score: b,
                boost_score: 0.0,
                vector_normalized: None,
                bm25_normalized: None,
                rerank_candidate: rerank_candidates.remove(&doc),
            })
            .collect();
        let dbg = debug.then(|| {
            let shards: Vec<HybridShardDebug> = summaries
                .iter()
                .enumerate()
                .map(|(shard, summary)| HybridShardDebug {
                    shard: shard as u32,
                    rpc_ms: 0.0,
                    vector_hits: summary.as_ref().map_or(clustered_counts[shard], |s| {
                        u32::try_from(s.emitted).unwrap_or(u32::MAX)
                    }),
                    bm25_hits: leg_counts.get(&(shard as u32)).copied().unwrap_or(0),
                    scan: None,
                })
                .collect();
            HybridDebug {
                fusion_mode: FusionMode::Decomposed as i32,
                leg_k: legs.leg_k,
                terms: Vec::new(),
                analysis_ms: 0.0,
                stats_ms: 0.0,
                legs_ms,
                fusion_ms: t_fusion.elapsed().as_secs_f32() * 1e3,
                total_ms: 0.0,
                shards,
                boost_ms: 0.0,
                boost_terms: Vec::new(),
            }
        });
        Ok((hits, dbg))
    }
}
