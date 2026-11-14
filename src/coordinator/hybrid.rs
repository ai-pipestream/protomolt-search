//! Hybrid.

use super::*;

impl CoordinatorServiceImpl {
    /// Hybrid vector + BM25 search:
    ///
    /// 1. analyze `text` into query terms (same analysis options as
    ///    ingest, as in [`Self::fanout_bm25`]);
    /// 2. TermStats fan-out and merge into GLOBAL corpus stats;
    /// 3. fuse per `legs.fusion_mode`: GLOBAL_RANK and SCORE_BLEND fetch
    ///    raw shard legs and fuse once on the coordinator (RRF over
    ///    global ranks, or normalize-and-combine over global scores);
    ///    TWO_LEVEL lets each shard RRF-fuse locally and RRF-merges the
    ///    shard lists (the fallback for incomparable scores).
    #[allow(clippy::too_many_arguments)]
    pub async fn fanout_hybrid(
        &self,
        request_id: &str,
        text: &str,
        vector: &[f32],
        k: u32,
        spec: Option<&crate::pb::AnalysisSpec>,
        legs: HybridLegs,
        debug: bool,
        filters: &RequestFilters,
    ) -> Result<(Vec<HybridHit>, Option<HybridDebug>), Status> {
        self.fanout_hybrid_with_terms(request_id, text, &[], vector, k, spec, legs, debug, filters)
            .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn fanout_hybrid_with_terms(
        &self,
        request_id: &str,
        text: &str,
        pre_analyzed_terms: &[String],
        vector: &[f32],
        k: u32,
        spec: Option<&crate::pb::AnalysisSpec>,
        legs: HybridLegs,
        debug: bool,
        filters: &RequestFilters,
    ) -> Result<(Vec<HybridHit>, Option<HybridDebug>), Status> {
        self.check_vector_scan(filters, false)?;
        if self.scoped_vector_scan() && self.query_read_versions.is_none() {
            let (pinned, reads) = self.pin_read_versions().await?;
            let result = Box::pin(pinned.fanout_hybrid_with_terms(
                request_id,
                text,
                pre_analyzed_terms,
                vector,
                k,
                spec,
                legs,
                debug,
                filters,
            ))
            .await;
            pinned.validate_read_versions(&reads).await?;
            return result;
        }
        let analysis_fingerprint = crate::analyzer::analysis_fingerprint(spec);
        if k == 0 || vector.is_empty() {
            return Ok((Vec::new(), None));
        }
        let t_total = std::time::Instant::now();
        let mut terms: Vec<String> = Vec::new();
        let analysis_ms = if !pre_analyzed_terms.is_empty() {
            for term in pre_analyzed_terms {
                if !term.is_empty() && !terms.contains(term) {
                    terms.push(term.clone());
                }
            }
            0.0
        } else {
            // Query analysis for the BM25 leg (same options as ingest).
            let addr = self.analysis_addr.clone().ok_or_else(|| {
                Status::unavailable(
                    "no analysis backend configured on the coordinator (analysis_addr)",
                )
            })?;
            let t = std::time::Instant::now();
            let analyzed = crate::analyzer::analyze_document(&addr, text, spec).await?;
            let ms = t.elapsed().as_secs_f32() * 1e3;
            for (term, _, _) in analyzed.into_body().terms {
                if !terms.contains(&term) {
                    terms.push(term);
                }
            }
            ms
        };

        let t = std::time::Instant::now();
        // Stats + fusion run as a round: a stale-stats refusal from any
        // shard reruns them once against fresh stats with a new fenced claim.
        let mut fresh = false;
        let (hits, mut dbg, stats_ms) = loop {
            let (global, epochs) = self.body_stats(&terms, fresh).await?;
            let claims = epochs;
            let stats_ms = t.elapsed().as_secs_f32() * 1e3;
            let round = match legs.fusion_mode {
                FusionMode::TwoLevel => {
                    self.fanout_hybrid_two_level(
                        request_id,
                        vector,
                        k,
                        &terms,
                        analysis_fingerprint,
                        &global,
                        &claims,
                        legs,
                        debug,
                        filters,
                    )
                    .await
                }
                FusionMode::Decomposed => {
                    self.fanout_hybrid_decomposed(
                        request_id,
                        vector,
                        k,
                        &terms,
                        analysis_fingerprint,
                        &global,
                        &claims,
                        legs,
                        debug,
                        filters,
                    )
                    .await
                }
                _ => {
                    self.fanout_hybrid_global_rank(
                        request_id,
                        vector,
                        k,
                        &terms,
                        analysis_fingerprint,
                        &global,
                        &claims,
                        legs,
                        debug,
                        filters,
                    )
                    .await
                }
            };
            match round {
                Err(e) if !fresh && is_stale_stats(&e) => {
                    self.stats_cache.invalidate_all();
                    fresh = true;
                }
                Err(e) => return Err(e),
                Ok((hits, dbg)) => break (hits, dbg, stats_ms),
            }
        };
        if let Some(d) = dbg.as_mut() {
            d.terms = terms;
            d.analysis_ms = analysis_ms;
            d.stats_ms = stats_ms;
            d.total_ms = t_total.elapsed().as_secs_f32() * 1e3;
        }
        Ok((hits, dbg))
    }
}

impl CoordinatorServiceImpl {
    #[allow(clippy::too_many_arguments)]
    #[cfg(feature = "net")]
    pub(super) async fn clustered_hybrid_global_rank(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        terms: &[String],
        analysis_fingerprint: u64,
        global: &CorpusStats,
        claims: &[StatsClaim],
        legs: HybridLegs,
        debug: bool,
        filters: &RequestFilters,
    ) -> Result<(Vec<HybridHit>, Option<HybridDebug>), Status> {
        let t_legs = std::time::Instant::now();
        let ranges = self.product_label_ranges().await?;
        let vector_legs = if legs.vector_weight == 0.0 {
            vec![Vec::new(); self.node_addrs.len()]
        } else {
            self.clustered_local_vector_legs(request_id, vector, legs.leg_k, filters, &ranges)
                .await?
        };
        let mut owner: HashMap<u64, u32> = HashMap::new();
        let mut vector_counts = vec![0u32; self.node_addrs.len()];
        let mut vector_shards = Vec::with_capacity(vector_legs.len());
        for (shard, hits) in vector_legs.into_iter().enumerate() {
            vector_counts[shard] = u32::try_from(hits.len()).unwrap_or(u32::MAX);
            for &(doc_id, _) in &hits {
                owner.insert(doc_id, shard as u32);
            }
            vector_shards.push((shard as u32, hits));
        }
        let mut vector_global = fusion::merge_legs_by_score(vector_shards);

        let (_, leg_terms, leg_dfs) = leg_payloads(vector, terms, global, legs);
        let mask = self.shard_mask(filters.tree.as_ref());
        let mut shard_tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                continue;
            }
            let request = ShardLegsRequest {
                read_context: None,
                analysis_fingerprint,
                request_id: request_id.to_string(),
                k: legs.leg_k,
                vector: Vec::new(),
                terms: leg_terms.clone(),
                global_doc_count: global.doc_count,
                global_total_doc_length: global.total_doc_length,
                global_doc_frequencies: leg_dfs.clone(),
                k1: self.bm25_params.k1 as f32,
                b: self.bm25_params.b as f32,
                expected_stats_epoch: claims[shard].epoch,
                expected_stats_incarnation: claims[shard].incarnation(),
                geo_filters: filters.geo.clone(),
                filter: Self::shard_filter_tree(filters, mask.as_ref(), shard),
                include_rerank_candidate: filters.include_rerank_candidate,
            };
            let mut client = self.node_client(node)?;
            shard_tasks.push(tokio::spawn(async move {
                let started = std::time::Instant::now();
                client
                    .shard_legs(request)
                    .await
                    .map(|response| (shard as u32, started.elapsed(), response.into_inner()))
            }));
        }
        let mut bm25_shards = Vec::with_capacity(shard_tasks.len());
        let mut shard_debug = Vec::new();
        let mut known = Self::filter_known(filters, mask.as_ref());
        let mut rerank_candidates: HashMap<u64, crate::pb::RerankCandidatePayload> = HashMap::new();
        for task in shard_tasks {
            let (shard, elapsed, response) = task
                .await
                .map_err(|error| Status::internal(format!("shard legs task failed: {error}")))??;
            known.merge_shard(
                shard as usize,
                &response.geo_columns_known,
                &response.filter_columns_known,
            )?;
            for hit in &response.bm25_hits {
                owner.entry(hit.doc_id).or_insert(shard);
            }
            if debug {
                shard_debug.push(HybridShardDebug {
                    shard,
                    rpc_ms: elapsed.as_secs_f32() * 1e3,
                    vector_hits: vector_counts[shard as usize],
                    bm25_hits: response.bm25_hits.len() as u32,
                    scan: None,
                });
            }
            bm25_shards.push((
                shard,
                response
                    .bm25_hits
                    .into_iter()
                    .map(|hit| {
                        if let Some(c) = hit.rerank_candidate {
                            rerank_candidates.entry(hit.doc_id).or_insert(c);
                        }
                        (hit.doc_id, f64::from(hit.score))
                    })
                    .collect::<Vec<_>>(),
            ));
        }
        known.refuse_unknown(filters)?;
        let legs_ms = t_legs.elapsed().as_secs_f32() * 1e3;
        let t_fusion = std::time::Instant::now();
        let mut bm25_global = fusion::merge_legs_by_score(bm25_shards);
        if legs.min_vector_score > 0.0 {
            let min = f64::from(legs.min_vector_score);
            vector_global.retain(|&(_, score, _)| score >= min);
            let allowed: std::collections::HashSet<u64> =
                vector_global.iter().map(|&(id, _, _)| id).collect();
            bm25_global.retain(|&(id, _, _)| allowed.contains(&id));
        }
        let leg_inputs = [
            Leg {
                hits: vector_global
                    .iter()
                    .map(|&(id, score, _)| (id, score))
                    .collect(),
                weight: f64::from(legs.vector_weight),
            },
            Leg {
                hits: bm25_global
                    .iter()
                    .map(|&(id, score, _)| (id, score))
                    .collect(),
                weight: f64::from(legs.bm25_weight),
            },
        ];
        let blend = legs.fusion_mode == FusionMode::ScoreBlend;
        let fused = if blend {
            fusion::blend_fuse(
                &leg_inputs,
                legs.leg_k as usize,
                legs.normalization,
                legs.combination,
                k as usize,
            )
        } else {
            fusion::rrf_fuse(&leg_inputs, legs.rrf_k, k as usize)
        };
        let hits = fused
            .into_iter()
            .map(|hit| HybridHit {
                doc_id: hit.doc_id,
                fused_score: hit.fused_score as f32,
                shard: owner.get(&hit.doc_id).copied().unwrap_or(0),
                vector_rank: hit.leg_ranks[0],
                vector_score: hit.leg_scores[0].unwrap_or(0.0) as f32,
                bm25_rank: hit.leg_ranks[1],
                bm25_score: hit.leg_scores[1].unwrap_or(0.0) as f32,
                boost_score: 0.0,
                vector_normalized: hit.leg_norms.first().copied().flatten(),
                bm25_normalized: hit.leg_norms.get(1).copied().flatten(),
                rerank_candidate: rerank_candidates.remove(&hit.doc_id),
            })
            .collect();
        let debug = debug.then(|| {
            shard_debug.sort_by_key(|shard| shard.shard);
            HybridDebug {
                fusion_mode: if blend {
                    FusionMode::ScoreBlend as i32
                } else {
                    FusionMode::GlobalRank as i32
                },
                leg_k: legs.leg_k,
                terms: Vec::new(),
                analysis_ms: 0.0,
                stats_ms: 0.0,
                legs_ms,
                fusion_ms: t_fusion.elapsed().as_secs_f32() * 1e3,
                total_ms: 0.0,
                shards: shard_debug,
                boost_ms: 0.0,
                boost_terms: Vec::new(),
            }
        });
        Ok((hits, debug))
    }

    /// FUSION_MODE_GLOBAL_RANK: shards return RAW per-leg lists; the
    /// coordinator merges each leg across shards by raw score into global
    /// rankings and applies single-level RRF over them. With globally
    /// comparable scores per leg this is exactly the monolithic result
    /// for k <= leg_k (see the proto's FusionMode comments).
    #[allow(clippy::too_many_arguments)]
    // Without the network stack the clustered arm is compiled out and
    // its inputs go unread; the exact arm is the whole function then.
    #[cfg_attr(not(feature = "net"), allow(unused_variables, unused_mut))]
    pub(super) async fn fanout_hybrid_global_rank(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        terms: &[String],
        analysis_fingerprint: u64,
        global: &CorpusStats,
        claims: &[StatsClaim],
        legs: HybridLegs,
        debug: bool,
        filters: &RequestFilters,
    ) -> Result<(Vec<HybridHit>, Option<HybridDebug>), Status> {
        #[cfg(feature = "net")]
        if self.clustered_vectors.is_some() {
            return self
                .clustered_hybrid_global_rank(
                    request_id,
                    vector,
                    k,
                    terms,
                    analysis_fingerprint,
                    global,
                    claims,
                    legs,
                    debug,
                    filters,
                )
                .await;
        }
        let t_legs = std::time::Instant::now();
        let (leg_vector, leg_terms, leg_dfs) = leg_payloads(vector, terms, global, legs);
        let admission = self.vector_read_barrier()?;
        let mask = self.vector_scan_mask(filters.tree.as_ref());
        let mut shard_tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                continue;
            }
            let request = ShardLegsRequest {
                read_context: admission.as_ref().map(|a| a.context(shard)).transpose()?,
                analysis_fingerprint,
                request_id: String::new(),
                k: legs.leg_k,
                vector: leg_vector.clone(),
                terms: leg_terms.clone(),
                global_doc_count: global.doc_count,
                global_total_doc_length: global.total_doc_length,
                global_doc_frequencies: leg_dfs.clone(),
                k1: self.bm25_params.k1 as f32,
                b: self.bm25_params.b as f32,
                expected_stats_epoch: claims[shard].epoch,
                expected_stats_incarnation: claims[shard].incarnation(),
                geo_filters: filters.geo.clone(),
                filter: Self::shard_filter_tree(filters, mask.as_ref(), shard),
                include_rerank_candidate: filters.include_rerank_candidate,
            };
            let mut client = self.node_client(node)?;
            shard_tasks.push(tokio::spawn(async move {
                let t0 = std::time::Instant::now();
                client.shard_legs(request).await.map(|r| {
                    (
                        shard as u32,
                        t0.elapsed().as_secs_f32() * 1e3,
                        r.into_inner(),
                    )
                })
            }));
        }
        let mut vector_shards = Vec::with_capacity(shard_tasks.len());
        let mut bm25_shards = Vec::with_capacity(shard_tasks.len());
        let mut shard_debug: Vec<HybridShardDebug> = Vec::new();
        let mut owner: std::collections::HashMap<u64, u32> = std::collections::HashMap::new();
        let mut rerank_candidates: HashMap<u64, crate::pb::RerankCandidatePayload> = HashMap::new();
        let mut known = Self::filter_known(filters, mask.as_ref());
        for task in shard_tasks {
            let (shard, rpc_ms, response) = task
                .await
                .map_err(|e| Status::internal(format!("shard legs task failed: {e}")))??;
            match (&admission, response.read_receipt.as_ref()) {
                (Some(admission), Some(receipt)) => admission.accept(shard as usize, receipt)?,
                (None, None) => {}
                _ => {
                    return Err(Status::failed_precondition(
                        "hybrid leg read receipt mismatch",
                    ))
                }
            }
            known.merge_shard(
                shard as usize,
                &response.geo_columns_known,
                &response.filter_columns_known,
            )?;
            if debug {
                shard_debug.push(HybridShardDebug {
                    shard,
                    rpc_ms,
                    vector_hits: response.vector_hits.len() as u32,
                    bm25_hits: response.bm25_hits.len() as u32,
                    scan: None,
                });
            }
            for h in &response.vector_hits {
                owner.entry(h.doc_id).or_insert(shard);
            }
            for h in &response.bm25_hits {
                owner.entry(h.doc_id).or_insert(shard);
            }
            vector_shards.push((
                shard,
                response
                    .vector_hits
                    .into_iter()
                    .map(|h| {
                        if let Some(c) = h.rerank_candidate {
                            rerank_candidates.entry(h.doc_id).or_insert(c);
                        }
                        (h.doc_id, f64::from(h.score))
                    })
                    .collect::<Vec<_>>(),
            ));
            bm25_shards.push((
                shard,
                response
                    .bm25_hits
                    .into_iter()
                    .map(|h| {
                        if let Some(c) = h.rerank_candidate {
                            rerank_candidates.entry(h.doc_id).or_insert(c);
                        }
                        (h.doc_id, f64::from(h.score))
                    })
                    .collect::<Vec<_>>(),
            ));
        }

        // A filter name NO shard resolves is a typo, and it would read
        // as an honest empty result set on both legs at once.
        if let Some(admission) = &admission {
            admission.wait().await?;
        }
        known.refuse_unknown(filters)?;

        let legs_ms = t_legs.elapsed().as_secs_f32() * 1e3;
        // Merge each leg into a GLOBAL ranking by raw score (deterministic
        // total order; see merge_legs_by_score), then fuse once: RRF for
        // GLOBAL_RANK, normalize-and-combine for SCORE_BLEND. The two
        // modes share this whole leg-fetch path; only the fusion function
        // differs. Weights arrive RESOLVED (the handler defaults absent
        // to 1.0); an exact 0 disables its leg, which both fusion
        // functions honor by skipping it.
        let t_fusion = std::time::Instant::now();
        let mut vector_global = fusion::merge_legs_by_score(vector_shards);
        let mut bm25_global = fusion::merge_legs_by_score(bm25_shards);
        // The vector-score floor: a hit must have a qualifying vector
        // score to survive, so docs below it (or absent from the vector
        // leg) drop from BOTH legs BEFORE fusion and truncation —
        // deeper qualifying docs are promoted, and blend statistics see
        // only the filtered set. Score-defined, hence layout-invariant.
        if legs.min_vector_score > 0.0 {
            let min = f64::from(legs.min_vector_score);
            vector_global.retain(|&(_, score, _)| score >= min);
            let allowed: std::collections::HashSet<u64> =
                vector_global.iter().map(|&(id, _, _)| id).collect();
            bm25_global.retain(|&(id, _, _)| allowed.contains(&id));
        }
        let leg_inputs = [
            Leg {
                hits: vector_global
                    .iter()
                    .map(|&(id, score, _)| (id, score))
                    .collect(),
                weight: f64::from(legs.vector_weight),
            },
            Leg {
                hits: bm25_global
                    .iter()
                    .map(|&(id, score, _)| (id, score))
                    .collect(),
                weight: f64::from(legs.bm25_weight),
            },
        ];
        let blend = legs.fusion_mode == FusionMode::ScoreBlend;
        let fused = if blend {
            fusion::blend_fuse(
                &leg_inputs,
                legs.leg_k as usize,
                legs.normalization,
                legs.combination,
                k as usize,
            )
        } else {
            fusion::rrf_fuse(&leg_inputs, legs.rrf_k, k as usize)
        };

        // Provenance: global per-leg ranks, raw scores, and the owning
        // shard (a doc lives on exactly one shard's lists).
        let hits: Vec<HybridHit> = fused
            .into_iter()
            .map(|f| HybridHit {
                doc_id: f.doc_id,
                fused_score: f.fused_score as f32,
                shard: owner.get(&f.doc_id).copied().unwrap_or(0),
                vector_rank: f.leg_ranks[0],
                vector_score: f.leg_scores[0].unwrap_or(0.0) as f32,
                bm25_rank: f.leg_ranks[1],
                bm25_score: f.leg_scores[1].unwrap_or(0.0) as f32,
                boost_score: 0.0,
                vector_normalized: f.leg_norms.first().copied().flatten(),
                bm25_normalized: f.leg_norms.get(1).copied().flatten(),
                rerank_candidate: rerank_candidates.remove(&f.doc_id),
            })
            .collect();
        let dbg = debug.then(|| {
            shard_debug.sort_by_key(|s| s.shard);
            HybridDebug {
                fusion_mode: if blend {
                    FusionMode::ScoreBlend as i32
                } else {
                    FusionMode::GlobalRank as i32
                },
                leg_k: legs.leg_k,
                terms: Vec::new(),
                analysis_ms: 0.0,
                stats_ms: 0.0,
                legs_ms,
                fusion_ms: t_fusion.elapsed().as_secs_f32() * 1e3,
                total_ms: 0.0,
                shards: shard_debug,
                boost_ms: 0.0,
                boost_terms: Vec::new(),
            }
        });
        Ok((hits, dbg))
    }
}

impl CoordinatorServiceImpl {
    #[allow(clippy::too_many_arguments)]
    #[cfg(feature = "net")]
    pub(super) async fn clustered_hybrid_two_level(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        terms: &[String],
        analysis_fingerprint: u64,
        global: &CorpusStats,
        claims: &[StatsClaim],
        legs: HybridLegs,
        debug: bool,
        filters: &RequestFilters,
    ) -> Result<(Vec<HybridHit>, Option<HybridDebug>), Status> {
        let t_legs = std::time::Instant::now();
        let ranges = self.product_label_ranges().await?;
        let vector_legs = self
            .clustered_local_vector_legs(request_id, vector, legs.leg_k, filters, &ranges)
            .await?;
        let (_, leg_terms, leg_dfs) = leg_payloads(vector, terms, global, legs);
        let mask = self.shard_mask(filters.tree.as_ref());
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                continue;
            }
            let request = ShardLegsRequest {
                read_context: None,
                analysis_fingerprint,
                request_id: request_id.to_string(),
                k: legs.leg_k,
                vector: Vec::new(),
                terms: leg_terms.clone(),
                global_doc_count: global.doc_count,
                global_total_doc_length: global.total_doc_length,
                global_doc_frequencies: leg_dfs.clone(),
                k1: self.bm25_params.k1 as f32,
                b: self.bm25_params.b as f32,
                expected_stats_epoch: claims[shard].epoch,
                expected_stats_incarnation: claims[shard].incarnation(),
                geo_filters: filters.geo.clone(),
                filter: Self::shard_filter_tree(filters, mask.as_ref(), shard),
                include_rerank_candidate: filters.include_rerank_candidate,
            };
            let mut client = self.node_client(node)?;
            tasks.push(tokio::spawn(async move {
                let started = std::time::Instant::now();
                client
                    .shard_legs(request)
                    .await
                    .map(|response| (shard as u32, started.elapsed(), response.into_inner()))
            }));
        }
        let mut shard_lists: Vec<(u32, Vec<HybridLegHit>)> = Vec::with_capacity(tasks.len());
        let mut shard_debug = Vec::new();
        let mut known = Self::filter_known(filters, mask.as_ref());
        for task in tasks {
            let (shard, elapsed, response) = task
                .await
                .map_err(|error| Status::internal(format!("shard legs task failed: {error}")))??;
            known.merge_shard(
                shard as usize,
                &response.geo_columns_known,
                &response.filter_columns_known,
            )?;
            let vector_leg = vector_legs[shard as usize].clone();
            let mut shard_candidates: HashMap<u64, crate::pb::RerankCandidatePayload> =
                HashMap::new();
            let bm25_leg: Vec<(u64, f64)> = response
                .bm25_hits
                .into_iter()
                .map(|hit| {
                    if let Some(c) = hit.rerank_candidate {
                        shard_candidates.insert(hit.doc_id, c);
                    }
                    (hit.doc_id, f64::from(hit.score))
                })
                .collect();
            let mut local: Vec<HybridLegHit> = fusion::rrf_fuse(
                &[
                    Leg {
                        hits: vector_leg,
                        weight: f64::from(legs.vector_weight),
                    },
                    Leg {
                        hits: bm25_leg,
                        weight: f64::from(legs.bm25_weight),
                    },
                ],
                legs.rrf_k,
                legs.leg_k as usize,
            )
            .into_iter()
            .map(|hit| HybridLegHit {
                doc_id: hit.doc_id,
                fused_score: hit.fused_score as f32,
                vector_rank: hit.leg_ranks[0],
                vector_score: hit.leg_scores[0].unwrap_or(0.0) as f32,
                bm25_rank: hit.leg_ranks[1],
                bm25_score: hit.leg_scores[1].unwrap_or(0.0) as f32,
                rerank_candidate: shard_candidates.remove(&hit.doc_id),
            })
            .collect();
            if legs.min_vector_score > 0.0 {
                local.retain(|hit| {
                    hit.vector_rank.is_some() && hit.vector_score >= legs.min_vector_score
                });
            }
            if debug {
                shard_debug.push(HybridShardDebug {
                    shard,
                    rpc_ms: elapsed.as_secs_f32() * 1e3,
                    vector_hits: local.iter().filter(|hit| hit.vector_rank.is_some()).count()
                        as u32,
                    bm25_hits: local.iter().filter(|hit| hit.bm25_rank.is_some()).count() as u32,
                    scan: None,
                });
            }
            shard_lists.push((shard, local));
        }
        known.refuse_unknown(filters)?;
        let legs_ms = t_legs.elapsed().as_secs_f32() * 1e3;
        let t_fusion = std::time::Instant::now();
        let fused_legs: Vec<Leg> = shard_lists
            .iter()
            .map(|(_, hits)| Leg {
                hits: hits
                    .iter()
                    .map(|hit| (hit.doc_id, f64::from(hit.fused_score)))
                    .collect(),
                weight: 1.0,
            })
            .collect();
        let fused = fusion::rrf_fuse(&fused_legs, legs.rrf_k, k as usize);
        let hits = fused
            .into_iter()
            .map(|hit| {
                let (shard, source) = shard_lists
                    .iter()
                    .find_map(|(shard, hits)| {
                        hits.iter()
                            .find(|source| source.doc_id == hit.doc_id)
                            .map(|source| (*shard, source))
                    })
                    .expect("fused hit comes from a product shard");
                HybridHit {
                    doc_id: hit.doc_id,
                    fused_score: hit.fused_score as f32,
                    shard,
                    vector_rank: source.vector_rank,
                    vector_score: source.vector_score,
                    bm25_rank: source.bm25_rank,
                    bm25_score: source.bm25_score,
                    boost_score: 0.0,
                    vector_normalized: None,
                    bm25_normalized: None,
                    rerank_candidate: source.rerank_candidate.clone(),
                }
            })
            .collect();
        let debug = debug.then(|| {
            shard_debug.sort_by_key(|shard| shard.shard);
            HybridDebug {
                fusion_mode: FusionMode::TwoLevel as i32,
                leg_k: legs.leg_k,
                terms: Vec::new(),
                analysis_ms: 0.0,
                stats_ms: 0.0,
                legs_ms,
                fusion_ms: t_fusion.elapsed().as_secs_f32() * 1e3,
                total_ms: 0.0,
                shards: shard_debug,
                boost_ms: 0.0,
                boost_terms: Vec::new(),
            }
        });
        Ok((hits, debug))
    }

    /// FUSION_MODE_TWO_LEVEL (fallback for incomparable scores): each
    /// shard fuses locally; the coordinator RRF-merges the shard lists.
    /// NOT partition-independent — see the proto's FusionMode comments.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn fanout_hybrid_two_level(
        &self,
        request_id: &str,
        vector: &[f32],
        k: u32,
        terms: &[String],
        analysis_fingerprint: u64,
        global: &CorpusStats,
        claims: &[StatsClaim],
        legs: HybridLegs,
        debug: bool,
        filters: &RequestFilters,
    ) -> Result<(Vec<HybridHit>, Option<HybridDebug>), Status> {
        #[cfg(feature = "net")]
        if self.clustered_vectors.is_some() {
            return self
                .clustered_hybrid_two_level(
                    request_id,
                    vector,
                    k,
                    terms,
                    analysis_fingerprint,
                    global,
                    claims,
                    legs,
                    debug,
                    filters,
                )
                .await;
        }
        let t_legs = std::time::Instant::now();
        // Level one: per-shard local fusion.
        let (leg_vector, leg_terms, leg_dfs) = leg_payloads(vector, terms, global, legs);
        let admission = self.vector_read_barrier()?;
        let mask = self.vector_scan_mask(filters.tree.as_ref());
        let mut shard_tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                continue;
            }
            let request = HybridShardRequest {
                read_context: admission.as_ref().map(|a| a.context(shard)).transpose()?,
                analysis_fingerprint,
                request_id: request_id.to_string(),
                k: legs.leg_k,
                vector: leg_vector.clone(),
                terms: leg_terms.clone(),
                global_doc_count: global.doc_count,
                global_total_doc_length: global.total_doc_length,
                global_doc_frequencies: leg_dfs.clone(),
                vector_weight: legs.vector_weight,
                bm25_weight: legs.bm25_weight,
                rrf_k: legs.rrf_k as f32,
                k1: self.bm25_params.k1 as f32,
                b: self.bm25_params.b as f32,
                expected_stats_epoch: claims[shard].epoch,
                expected_stats_incarnation: claims[shard].incarnation(),
                geo_filters: filters.geo.clone(),
                filter: Self::shard_filter_tree(filters, mask.as_ref(), shard),
                include_rerank_candidate: filters.include_rerank_candidate,
            };
            let mut client = self.node_client(node)?;
            shard_tasks.push(tokio::spawn(async move {
                let t0 = std::time::Instant::now();
                client.hybrid_shard(request).await.map(|r| {
                    let r = r.into_inner();
                    (
                        shard as u32,
                        t0.elapsed().as_secs_f32() * 1e3,
                        r.hits,
                        r.geo_columns_known,
                        r.filter_columns_known,
                        r.read_receipt,
                    )
                })
            }));
        }
        let mut shard_lists: Vec<(u32, Vec<crate::pb::HybridLegHit>)> = Vec::new();
        let mut shard_debug: Vec<HybridShardDebug> = Vec::new();
        let mut known = Self::filter_known(filters, mask.as_ref());
        for task in shard_tasks {
            let (shard, rpc_ms, mut hits, geo_known, filter_known, receipt) = task
                .await
                .map_err(|e| Status::internal(format!("hybrid shard task failed: {e}")))??;
            match (&admission, receipt.as_ref()) {
                (Some(admission), Some(receipt)) => admission.accept(shard as usize, receipt)?,
                (None, None) => {}
                _ => {
                    return Err(Status::failed_precondition(
                        "hybrid fusion read receipt mismatch",
                    ))
                }
            }
            known.merge(&geo_known, &filter_known)?;
            // Vector-score floor: drop non-qualifying docs from the
            // shard's fused list before level-two fusion.
            if legs.min_vector_score > 0.0 {
                hits.retain(|h| h.vector_rank.is_some() && h.vector_score >= legs.min_vector_score);
            }
            if debug {
                // A two-level shard returns one FUSED list; per-leg
                // membership is what provenance carries.
                shard_debug.push(HybridShardDebug {
                    shard,
                    rpc_ms,
                    vector_hits: hits.iter().filter(|h| h.vector_rank.is_some()).count() as u32,
                    bm25_hits: hits.iter().filter(|h| h.bm25_rank.is_some()).count() as u32,
                    scan: None,
                });
            }
            shard_lists.push((shard, hits));
        }
        if let Some(admission) = &admission {
            admission.wait().await?;
        }
        known.refuse_unknown(filters)?;
        let legs_ms = t_legs.elapsed().as_secs_f32() * 1e3;
        let t_fusion = std::time::Instant::now();

        // Level two: RRF over the per-shard fused lists (unweighted; the
        // leg weights already acted at shard level).
        let mut fused_legs: Vec<Leg> = Vec::with_capacity(shard_lists.len());
        for (_, hits) in &shard_lists {
            fused_legs.push(Leg {
                hits: hits
                    .iter()
                    .map(|h| (h.doc_id, f64::from(h.fused_score)))
                    .collect(),
                weight: 1.0,
            });
        }
        let fused = fusion::rrf_fuse(&fused_legs, legs.rrf_k, k as usize);

        // Attach per-leg provenance from the owning shard's HybridLegHit.
        let hits: Vec<HybridHit> = fused
            .into_iter()
            .map(|f| {
                let (shard, source) = shard_lists
                    .iter()
                    .find_map(|(shard, hits)| {
                        hits.iter()
                            .find(|h| h.doc_id == f.doc_id)
                            .map(|h| (*shard, h))
                    })
                    .expect("fused hit comes from some shard");
                HybridHit {
                    doc_id: f.doc_id,
                    fused_score: f.fused_score as f32,
                    shard,
                    vector_rank: source.vector_rank,
                    vector_score: source.vector_score,
                    bm25_rank: source.bm25_rank,
                    bm25_score: source.bm25_score,
                    boost_score: 0.0,
                    vector_normalized: None,
                    bm25_normalized: None,
                    rerank_candidate: source.rerank_candidate.clone(),
                }
            })
            .collect();
        let dbg = debug.then(|| {
            let mut shard_debug = shard_debug;
            shard_debug.sort_by_key(|s| s.shard);
            HybridDebug {
                fusion_mode: FusionMode::TwoLevel as i32,
                leg_k: legs.leg_k,
                terms: Vec::new(),
                analysis_ms: 0.0,
                stats_ms: 0.0,
                legs_ms,
                fusion_ms: t_fusion.elapsed().as_secs_f32() * 1e3,
                total_ms: 0.0,
                shards: shard_debug,
                boost_ms: 0.0,
                boost_terms: Vec::new(),
            }
        });
        Ok((hits, dbg))
    }
}
