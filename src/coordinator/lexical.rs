//! Lexical.

use super::*;

impl CoordinatorServiceImpl {
    /// Distributed BM25 with the two-phase global-stats flow (see the
    /// proto comments on `SearchService.Bm25Search`), unseeded.
    pub async fn fanout_bm25(
        &self,
        text: &str,
        k: u32,
        spec: Option<&crate::pb::AnalysisSpec>,
    ) -> Result<Vec<Bm25Hit>, Status> {
        self.fanout_bm25_seeded(text, k, spec, 0.0).await
    }

    /// [`Self::fanout_bm25`] with a client-supplied floor: `min_score`
    /// is forwarded verbatim to every shard's `Bm25QueryRequest`, which
    /// prunes postings that provably cannot reach it (docs/block-max.md,
    /// stage 4). 0 means unseeded. There is deliberately NO mid-query
    /// relay: `Bm25Query` is unary, so a fleet-wide floor can only ever
    /// arrive with the request.
    pub async fn fanout_bm25_seeded(
        &self,
        text: &str,
        k: u32,
        spec: Option<&crate::pb::AnalysisSpec>,
        min_score: f32,
    ) -> Result<Vec<Bm25Hit>, Status> {
        self.fanout_bm25_faceted(text, k, spec, min_score, &[], &[], &[], &[], &[], None)
            .await
            .map(|(hits, _, _)| hits)
    }

    /// [`Self::fanout_bm25_seeded`] with count-then-rank facets and a
    /// score-function chain. Facet fields are forwarded to every
    /// shard, counted there over the full match set, and summed here —
    /// counts are additive, so the global counts are the plain
    /// per-value sum over shards. Score stages are forwarded verbatim
    /// in list order (the pinned evaluation order,
    /// `docs/score-functions.md`), so hits, `min_score`, and
    /// `kth_best` are on the FINAL scale; a stage column NO shard
    /// knows is refused after the round, like an unknown facet field.
    /// Returns `(hits, merged facet counts, merged range-facet
    /// counts)`; both count lists are empty when none were requested or
    /// the query analyzed to no terms (no match set to count).
    #[allow(clippy::too_many_arguments)]
    pub async fn fanout_bm25_faceted(
        &self,
        text: &str,
        k: u32,
        spec: Option<&crate::pb::AnalysisSpec>,
        min_score: f32,
        facet_fields: &[String],
        map_facet_fields: &[crate::pb::MapFacetField],
        range_facet_fields: &[crate::pb::RangeFacetField],
        score_stages: &[crate::pb::ScoreStage],
        geo_filters: &[crate::pb::GeoFilter],
        filter: Option<&crate::pb::FilterExpr>,
    ) -> Result<FacetedHits, Status> {
        self.fanout_bm25_aggregated(
            text,
            k,
            spec,
            min_score,
            facet_fields,
            map_facet_fields,
            range_facet_fields,
            score_stages,
            geo_filters,
            filter,
            &[],
            &[],
            &[],
            &[],
            &mut Vec::new(),
            None,
            &[],
            false,
            &mut Vec::new(),
            false,
        )
        .await
        .map(|r| (r.0, r.1, r.2))
    }

    /// [`Self::fanout_bm25_faceted`] plus column aggregations: match-set
    /// stats over numeric / integer columns and exact distinct-value
    /// counts over facet columns (docs/facets.md). Returns
    /// `(hits, facets, ranges, stats, cardinality)`.
    #[allow(clippy::too_many_arguments)]
    pub async fn fanout_bm25_aggregated(
        &self,
        text: &str,
        k: u32,
        spec: Option<&crate::pb::AnalysisSpec>,
        min_score: f32,
        facet_fields: &[String],
        map_facet_fields: &[crate::pb::MapFacetField],
        range_facet_fields: &[crate::pb::RangeFacetField],
        score_stages: &[crate::pb::ScoreStage],
        geo_filters: &[crate::pb::GeoFilter],
        filter: Option<&crate::pb::FilterExpr>,
        stats_fields: &[String],
        cardinality_fields: &[String],
        projections: &[crate::pb::CompiledProjection],
        prefixes: &[crate::pb::TermPrefix],
        expansions: &mut Vec<crate::pb::PrefixExpansion>,
        highlight: Option<&crate::pb::HighlightSpec>,
        synonyms: &[crate::pb::SynonymRule],
        synonyms_off: bool,
        synonym_expansions: &mut Vec<crate::pb::SynonymExpansion>,
        explain: bool,
    ) -> Result<AggregatedHits, Status> {
        self.fanout_bm25_aggregated_with_terms(
            text,
            &[],
            k,
            spec,
            min_score,
            facet_fields,
            map_facet_fields,
            range_facet_fields,
            score_stages,
            geo_filters,
            filter,
            stats_fields,
            cardinality_fields,
            projections,
            prefixes,
            expansions,
            highlight,
            synonyms,
            synonyms_off,
            synonym_expansions,
            explain,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn fanout_bm25_aggregated_with_terms(
        &self,
        text: &str,
        pre_analyzed_terms: &[String],
        k: u32,
        spec: Option<&crate::pb::AnalysisSpec>,
        min_score: f32,
        facet_fields: &[String],
        map_facet_fields: &[crate::pb::MapFacetField],
        range_facet_fields: &[crate::pb::RangeFacetField],
        score_stages: &[crate::pb::ScoreStage],
        geo_filters: &[crate::pb::GeoFilter],
        filter: Option<&crate::pb::FilterExpr>,
        stats_fields: &[String],
        cardinality_fields: &[String],
        projections: &[crate::pb::CompiledProjection],
        prefixes: &[crate::pb::TermPrefix],
        expansions: &mut Vec<crate::pb::PrefixExpansion>,
        highlight: Option<&crate::pb::HighlightSpec>,
        synonyms: &[crate::pb::SynonymRule],
        synonyms_off: bool,
        synonym_expansions: &mut Vec<crate::pb::SynonymExpansion>,
        explain: bool,
    ) -> Result<AggregatedHits, Status> {
        let analysis_fingerprint = crate::analyzer::analysis_fingerprint(spec);
        // Edge-list validation needs no shard, so it must not hide
        // behind the zero-term early return below: a malformed request
        // refuses even when there is no match set to count. (The nodes
        // validate again; this is the coordinator honoring the same
        // contract on the paths that never reach one.)
        crate::node::validate_range_facet_fields(range_facet_fields)?;
        // Geo-filter validation is local for the same reason
        // (docs/geo-columns.md): an antimeridian bbox or a zero radius
        // must refuse whether or not the query has a match set.
        crate::node::validate_geo_filters(geo_filters)?;
        // And filter-tree validation, for the same reason again
        // (docs/cel-filters.md).
        if let Some(f) = filter {
            crate::filter::validate_filter(f)?;
        }
        let mut terms: Vec<String> = Vec::new();
        if !pre_analyzed_terms.is_empty() {
            for term in pre_analyzed_terms {
                if !term.is_empty() && !terms.contains(term) {
                    terms.push(term.clone());
                }
            }
        } else {
            let addr = self.analysis_addr.clone().ok_or_else(|| {
                Status::unavailable(
                    "no analysis backend configured on the coordinator (analysis_addr)",
                )
            })?;
            // (a) Query analysis with the SAME options as ingest: query terms
            // share identity with indexed terms (stems when SOURCE_STEMS).
            let analyzed = analyze_query(&addr, text, spec, prefixes).await?;
            for (term, _, _) in analyzed.terms {
                if !terms.contains(&term) {
                    terms.push(term);
                }
            }
        }
        // Term prefixes join the analyzed terms (docs/prefix-terms.md).
        expansions.extend(
            self.expand_prefixes("body", spec, prefixes, &mut terms)
                .await?,
        );
        // Synonym rules join them the same way (docs/synonyms.md).
        synonym_expansions.extend(
            self.expand_synonyms("body", spec, synonyms, synonyms_off, &mut terms)
                .await?,
        );
        // An empty lexical selection still needs the projection schema
        // agreement. Request metadata without scoring when analysis is empty.
        let k = if terms.is_empty() { 0 } else { k };
        // Named summaries still need schema agreement and zero counts when
        // analysis yields no terms. Only a result-free request can skip fan-out.
        if k == 0
            && projections.is_empty()
            && facet_fields.is_empty()
            && map_facet_fields.is_empty()
            && range_facet_fields.is_empty()
            && stats_fields.is_empty()
            && cardinality_fields.is_empty()
        {
            if self.document_visibility.is_some() {
                self.body_stats(&[], false).await?;
            }
            return Ok((
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                Vec::new(),
                crate::segment_prune::PruneStats::default(),
            ));
        }

        // (b) each shard's share of the corpus stats, cached per node;
        // (c)+(d) run as a round so a stale-stats refusal can rerun
        // them once against fresh stats with a new fenced claim.
        let mut fresh = false;
        loop {
            let (global, epochs) = self.body_stats(&terms, fresh).await?;
            let claims = epochs;
            match self
                .bm25_query_round(
                    &terms,
                    analysis_fingerprint,
                    k,
                    min_score,
                    &global,
                    &claims,
                    facet_fields,
                    map_facet_fields,
                    range_facet_fields,
                    score_stages,
                    geo_filters,
                    filter,
                    stats_fields,
                    cardinality_fields,
                    projections,
                    highlight,
                    explain,
                )
                .await
            {
                Err(e) if !fresh && is_stale_stats(&e) => {
                    self.stats_cache.invalidate_all();
                    fresh = true;
                }
                other => return other,
            }
        }
    }
}

impl CoordinatorServiceImpl {
    /// One Bm25Query fan-out with the GLOBAL stats: every shard scores
    /// identically, so the merge is a straight top-k. `claims[shard]`
    /// travels as that shard's `expected_stats_epoch`; `facet_fields`
    /// as its `facet_fields` (shard-local counts merge by plain sum);
    /// `score_stages` verbatim (list order is the pinned evaluation
    /// order, so every shard must see the same list).
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn bm25_query_round(
        &self,
        terms: &[String],
        analysis_fingerprint: u64,
        k: u32,
        min_score: f32,
        global: &CorpusStats,
        claims: &[StatsClaim],
        facet_fields: &[String],
        map_facet_fields: &[crate::pb::MapFacetField],
        range_facet_fields: &[crate::pb::RangeFacetField],
        score_stages: &[crate::pb::ScoreStage],
        geo_filters: &[crate::pb::GeoFilter],
        filter: Option<&crate::pb::FilterExpr>,
        stats_fields: &[String],
        cardinality_fields: &[String],
        projections: &[crate::pb::CompiledProjection],
        highlight: Option<&crate::pb::HighlightSpec>,
        explain: bool,
    ) -> Result<AggregatedHits, Status> {
        if self.node_addrs.is_empty() {
            return Err(Status::failed_precondition("no shard nodes configured"));
        }
        let mut query_tasks = tokio::task::JoinSet::new();
        // The streaming route's relay state: one conflated global floor
        // cell per query, seeded with the client floor. Shards' published
        // k-th bests fold into it (monotone max), and a per-stream
        // forwarder pushes whatever is newest when it wakes — the same
        // shape the vector relay uses.
        let relay = self
            .bm25_stream
            .then(|| watch::channel(min_score))
            .map(|(tx, rx)| (Arc::new(tx), rx));
        let stream_heap = self.bm25_stream.then(|| {
            Arc::new(Mutex::new(Bm25StreamHeap {
                heap: std::collections::BinaryHeap::with_capacity(k as usize + 1),
                floors_sent: 0,
                progress: self.query_progress.clone(),
            }))
        });
        let mask = self.shard_mask(filter);
        for (shard, node) in self.node_addrs.iter().enumerate() {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                continue;
            }
            let request = Bm25QueryRequest {
                analysis_fingerprint,
                highlight: highlight.cloned(),
                projections: projections.to_vec(),
                terms: terms.to_vec(),
                k,
                global_doc_count: global.doc_count,
                global_total_doc_length: global.total_doc_length,
                global_doc_frequencies: global.dfs.clone(),
                k1: self.bm25_params.k1 as f32,
                b: self.bm25_params.b as f32,
                min_score,
                fields: Vec::new(),
                expected_stats_epoch: claims[shard].epoch,
                expected_stats_incarnation: claims[shard].incarnation(),
                facet_fields: facet_fields.to_vec(),
                map_facet_fields: map_facet_fields.to_vec(),
                range_facet_fields: range_facet_fields.to_vec(),
                score_stages: score_stages.to_vec(),
                geo_filters: geo_filters.to_vec(),
                filter: filter.cloned(),
                stats_fields: stats_fields.to_vec(),
                cardinality_fields: cardinality_fields.to_vec(),
                phrase: None,
                explain,
            };
            let mut client = self.node_client(node)?;
            if let Some((floor_tx, floor_rx)) = relay.clone() {
                let global_heap = Arc::clone(stream_heap.as_ref().expect("relay has heap"));
                let deadline = self.limits.shard_deadline;
                query_tasks.spawn(async move {
                    let run = stream_bm25_shard(
                        shard as u32,
                        k as usize,
                        client,
                        request,
                        floor_tx,
                        floor_rx,
                        global_heap,
                    );
                    let result = match deadline {
                        Some(limit) => tokio::time::timeout(limit, run).await.map_err(|_| {
                            Status::deadline_exceeded(format!(
                                "BM25 shard {shard} exceeded its {}ms deadline",
                                limit.as_millis()
                            ))
                        })?,
                        None => run.await,
                    };
                    result.map(|r| {
                        let response = r.response;
                        (
                            shard as u32,
                            response.hits,
                            response.facets,
                            response.range_facets,
                            response.stage_columns_known,
                            response.geo_columns_known,
                            response.filter_columns_known,
                            response.stats,
                            response.distinct,
                            response.projection_leaves_known,
                            response.projection_types,
                            (response.segments_total, response.segments_skipped),
                            Some(r.scoring_fingerprint),
                        )
                    })
                });
                continue;
            }
            query_tasks.spawn(async move {
                client.bm25_query(request).await.map(|r| {
                    let r = r.into_inner();
                    (
                        shard as u32,
                        r.hits,
                        r.facets,
                        r.range_facets,
                        r.stage_columns_known,
                        r.geo_columns_known,
                        r.filter_columns_known,
                        r.stats,
                        r.distinct,
                        r.projection_leaves_known,
                        r.projection_types,
                        (r.segments_total, r.segments_skipped),
                        None,
                    )
                })
            });
        }
        let mut all: Vec<(u32, Bm25Hit)> = Vec::new();
        let mut prune = crate::segment_prune::PruneStats::default();
        let mut shard_facets: Vec<Vec<crate::pb::FacetFieldCounts>> = Vec::new();
        let mut shard_ranges: Vec<Vec<crate::pb::RangeFacetCounts>> = Vec::new();
        let mut stage_known = vec![false; score_stages.len()];
        let mut geo_known = vec![false; geo_filters.len()];
        let filter_leaves = filter.map_or(0, crate::filter::leaf_count);
        let mut filter_known = vec![false; filter_leaves];
        // Projection column-read leaves, in the wire's flag order
        // (docs/cel-values.md).
        let projection_leaves: Vec<crate::values::ValueLeaf> = {
            let mut leaves = Vec::new();
            for p in projections {
                if let Some(expr) = p.expr.as_ref() {
                    crate::values::column_leaves(expr, &mut leaves);
                }
            }
            leaves
        };
        let mut projection_known = vec![false; projection_leaves.len()];
        let mut projection_types = vec![crate::pb::ScalarValueType::Unspecified; projections.len()];
        let mut shard_stats: Vec<Vec<crate::pb::ColumnStats>> = Vec::new();
        let mut shard_distinct: Vec<Vec<crate::pb::FacetDistinct>> = Vec::new();
        let mut scoring_fingerprint: Option<String> = None;
        let mut responses = Vec::new();
        while let Some(joined) = query_tasks.join_next().await {
            responses.push(
                joined.map_err(|e| Status::internal(format!("bm25 query task failed: {e}")))??,
            );
        }
        responses.sort_by_key(|response| response.0);
        for response in responses {
            let (
                shard,
                hits,
                facets,
                ranges,
                known,
                geo,
                fknown,
                sstats,
                sdistinct,
                pknown,
                ptypes,
                sprune,
                fingerprint,
            ) = response;
            if let Some(fingerprint) = fingerprint {
                match scoring_fingerprint.as_ref() {
                    Some(expected) if expected != &fingerprint => {
                        return Err(Status::failed_precondition(format!(
                            "BM25 shard {shard} scoring fingerprint {fingerprint} differs from {expected}"
                        )));
                    }
                    None => scoring_fingerprint = Some(fingerprint),
                    _ => {}
                }
            }
            if pknown.len() != projection_leaves.len() {
                return Err(Status::internal(format!(
                    "shard answered {} projection-leaf flags for {} leaves",
                    pknown.len(),
                    projection_leaves.len()
                )));
            }
            for (acc, k) in projection_known.iter_mut().zip(&pknown) {
                *acc |= *k;
            }
            crate::values::merge_projection_types(projections, &mut projection_types, &ptypes)?;
            for hit in &hits {
                crate::values::validate_projection_row(&hit.projected, &ptypes)?;
            }
            all.extend(hits.into_iter().map(|h| (shard, h)));
            shard_facets.push(facets);
            shard_ranges.push(ranges);
            shard_stats.push(sstats);
            shard_distinct.push(sdistinct);
            prune.add(crate::segment_prune::PruneStats {
                segments_total: sprune.0,
                segments_skipped: sprune.1,
            });
            if geo.len() != geo_filters.len() {
                return Err(Status::internal(format!(
                    "shard answered {} geo-column flags for {} filters",
                    geo.len(),
                    geo_filters.len()
                )));
            }
            for (acc, k) in geo_known.iter_mut().zip(&geo) {
                *acc |= *k;
            }
            if fknown.len() != filter_leaves {
                return Err(Status::internal(format!(
                    "shard answered {} filter-leaf flags for {} leaves",
                    fknown.len(),
                    filter_leaves
                )));
            }
            for (acc, k) in filter_known.iter_mut().zip(&fknown) {
                *acc |= *k;
            }
            if known.len() != score_stages.len() {
                return Err(Status::internal(format!(
                    "shard answered {} stage-column flags for {} stages",
                    known.len(),
                    score_stages.len()
                )));
            }
            for (acc, k) in stage_known.iter_mut().zip(known) {
                *acc |= k;
            }
        }
        // A stage column NO shard knows is a typo wearing an identity
        // chain — the whole request would be a silent no-op. Refuse,
        // naming the column and the knob; a partially-known column is
        // the heterogeneous fleet and is exact (absent = identity).
        let unknown: Vec<String> = score_stages
            .iter()
            .zip(&stage_known)
            .filter(|(_, known)| !**known)
            .map(|(s, _)| match s.map_key() {
                None => format!("{:?}", s.column),
                Some(key) => format!("{:?}[{:?}]", s.column, key),
            })
            .collect();
        if !unknown.is_empty() {
            // A geo decay stage reads a geo column, so pointing the
            // caller at --numeric-fields alone would send them looking
            // in the wrong table for a name they spelled wrong.
            let any_geo = score_stages.iter().zip(&stage_known).any(|(s, known)| {
                !known
                    && matches!(
                        crate::pb::ScoreOp::try_from(s.operation_code()),
                        Ok(crate::pb::ScoreOp::MultGeoDecayHaversine
                            | crate::pb::ScoreOp::MultGeoDecayManhattan)
                    )
            });
            let knobs = if any_geo {
                "--numeric-fields / --integer-fields / --map-numeric-fields / --geo-fields"
            } else {
                "--numeric-fields / --map-numeric-fields"
            };
            return Err(Status::invalid_argument(format!(
                "no shard has numeric column {}: the chain would be a silent no-op. \
                 Check the spelling, or the nodes' {knobs}.",
                unknown.join(", ")
            )));
        }
        refuse_unknown_geo_columns(geo_filters, &geo_known)?;
        if let Some(mask) = mask.as_ref() {
            mark_known(&mut filter_known, &mask.known);
        }
        refuse_unknown_filter_leaves(filter, &filter_known)?;
        // A projection column NO shard knows is a typo answering
        // all-absent — refuse it by name. A partially-known column is
        // the heterogeneous fleet and is exact (absent documents hold
        // nothing).
        let unknown_projection: Vec<String> = projection_leaves
            .iter()
            .zip(&projection_known)
            .filter(|(_, known)| !**known)
            .map(|(leaf, _)| leaf.describe())
            .collect();
        if !unknown_projection.is_empty() {
            return Err(Status::invalid_argument(format!(
                "projection: no shard has column {}: every value would be absent. \
                 Check the spelling, or the nodes' --numeric-fields / --integer-fields \
                 / --facet-fields / --map-numeric-fields / --map-facet-fields.",
                unknown_projection.join(", ")
            )));
        }
        let facets = merge_facet_counts(facet_fields, map_facet_fields, &shard_facets, true)?;
        let ranges = merge_range_counts(range_facet_fields, &shard_ranges)?;
        let stats = merge_column_stats(stats_fields, &shard_stats)?;
        let cardinality = merge_cardinality(cardinality_fields, &shard_distinct)?;
        if let (Some(heap), Some(fingerprint)) = (&stream_heap, scoring_fingerprint.as_ref()) {
            let snapshot = heap
                .lock()
                .expect("BM25 stream heap poisoned")
                .heap
                .iter()
                .map(|entry| (entry.0.vector_id, entry.0.score))
                .collect();
            self.publish_progress(
                crate::pb::QueryStreamPhase::Lexical,
                snapshot,
                fingerprint.clone(),
            );
        }
        if let Some(stream_heap) = stream_heap {
            let winners: Vec<MergedHit> = {
                let state = stream_heap.lock().expect("BM25 stream heap poisoned");
                let mut winners: Vec<MergedHit> = state.heap.iter().map(|entry| entry.0).collect();
                winners.sort_by(cmp_hits);
                winners
            };
            let mut details: HashMap<(u32, u64), Bm25Hit> = all
                .drain(..)
                .map(|(shard, hit)| ((shard, hit.doc_id), hit))
                .collect();
            all = winners
                .into_iter()
                .map(|winner| {
                    let hit = details
                        .remove(&(winner.shard, winner.vector_id))
                        .ok_or_else(|| {
                            Status::data_loss(format!(
                                "BM25 global winner {} from shard {} was absent from its certified local top-k",
                                winner.vector_id, winner.shard
                            ))
                        })?;
                    if hit.score.to_bits() != winner.score.to_bits() {
                        return Err(Status::data_loss(format!(
                            "BM25 global winner {} from shard {} changed score from {:?} in the candidate stream to {:?} in the certified response",
                            winner.vector_id,
                            winner.shard,
                            winner.score,
                            hit.score
                        )));
                    }
                    Ok((winner.shard, hit))
                })
                .collect::<Result<_, _>>()?;
        } else {
            all.sort_by(|(sa, a), (sb, b)| {
                b.score
                    .total_cmp(&a.score)
                    .then_with(|| sa.cmp(sb))
                    .then_with(|| a.doc_id.cmp(&b.doc_id))
            });
            all.truncate(k as usize);
        }
        Ok((
            all.into_iter().map(|(_, h)| h).collect(),
            facets,
            ranges,
            stats,
            cardinality,
            prune,
        ))
    }
}

impl CoordinatorServiceImpl {
    /// One fused Bm25Query fan-out: phases (c) and (d) of
    /// [`Self::fanout_bm25_fused`]. `claims[shard]` travels as that
    /// shard's `expected_stats_epoch`.
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn bm25_fused_round(
        &self,
        k: u32,
        fields: &[crate::pb::QueryField],
        field_terms: &[Vec<String>],
        globals: &FusedGlobals,
        claims: &[StatsClaim],
        // `(field index, parallel term weights, vocabulary fingerprint)`.
        phrase: Option<(usize, &[f32], u64)>,
        // Per field: the positional phrase constraint the shard applies
        // at its heap gate (docs/phrase-proximity.md), `None` on an
        // ordinary leg or one already rewritten onto a bigram column.
        phrase_legs: &[Option<crate::pb::PhraseLeg>],
        // Per field: the analyzer fingerprint the shard is told its
        // terms came from (a bigram column's is derived from its
        // source's; a glossary field's is the vocabulary's).
        fingerprints: &[u64],
        min_score: f32,
        facet_fields: &[String],
        map_facet_fields: &[crate::pb::MapFacetField],
        range_facet_fields: &[crate::pb::RangeFacetField],
        geo_filters: &[crate::pb::GeoFilter],
        filter: Option<&crate::pb::FilterExpr>,
        highlight: Option<&crate::pb::HighlightSpec>,
        trace: bool,
        t0: std::time::Instant,
        t_analyzed: std::time::Duration,
        t_stats: std::time::Duration,
        explain: bool,
    ) -> Result<(FacetedHits, crate::segment_prune::PruneStats), Status> {
        let doc_count = globals.doc_count;
        let totals = &globals.totals;
        let dfs = &globals.dfs;
        // (c) Bm25Query fan-out with per-field legs in entry order.
        // Entry k1/b of 0 pick up the coordinator's configured params,
        // so tuning reaches this path too.
        let legs: Vec<crate::pb::Bm25FieldLeg> = fields
            .iter()
            .enumerate()
            .map(|(fi, f)| crate::pb::Bm25FieldLeg {
                field: f.field.clone(),
                terms: field_terms[fi].clone(),
                global_total_doc_length: totals[fi],
                global_doc_frequencies: dfs[fi].clone(),
                weight: f.weight,
                k1: if f.k1 == 0.0 {
                    self.bm25_params.k1 as f32
                } else {
                    f.k1
                },
                b: if f.b == 0.0 {
                    self.bm25_params.b as f32
                } else {
                    f.b
                },
                // Declare which analyzer produced these terms so the
                // shard can refuse a column built under a different one.
                // The caller computed it from THE SPEC ACTUALLY USED,
                // not from what the caller meant.
                analysis_fingerprint: fingerprints[fi],
                phrase: phrase_legs[fi].clone(),
            })
            .collect();
        debug_assert_eq!(phrase_legs.len(), fields.len());
        debug_assert_eq!(fingerprints.len(), fields.len());
        let _ = phrase.map(|(leg, _, _)| leg);
        if trace {
            for l in &legs {
                eprintln!(
                    "bm25-fused leg: field={:?} terms={:?} dfs={:?} total_len={} w={} k1={} b={} \
                     | req k={k} N={doc_count} min_score={min_score}",
                    l.field,
                    l.terms,
                    l.global_doc_frequencies,
                    l.global_total_doc_length,
                    l.weight,
                    l.k1,
                    l.b
                );
            }
        }
        let mut query_tasks = tokio::task::JoinSet::new();
        let stream_enabled = self.bm25_stream && phrase.is_none();
        let relay = stream_enabled
            .then(|| watch::channel(min_score))
            .map(|(tx, rx)| (Arc::new(tx), rx));
        let stream_heap = stream_enabled.then(|| {
            Arc::new(Mutex::new(Bm25StreamHeap {
                heap: std::collections::BinaryHeap::with_capacity(k as usize + 1),
                floors_sent: 0,
                progress: self.query_progress.clone(),
            }))
        });
        let mask = self.shard_mask(filter);
        for (shard, node) in self.node_addrs.iter().enumerate() {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                continue;
            }
            let request = Bm25QueryRequest {
                analysis_fingerprint: 0,
                highlight: highlight.cloned(),
                projections: Vec::new(),
                terms: Vec::new(),
                k,
                global_doc_count: doc_count,
                global_total_doc_length: 0,
                global_doc_frequencies: Vec::new(),
                k1: 0.0,
                b: 0.0,
                min_score,
                fields: legs.clone(),
                expected_stats_epoch: claims[shard].epoch,
                expected_stats_incarnation: claims[shard].incarnation(),
                facet_fields: facet_fields.to_vec(),
                map_facet_fields: map_facet_fields.to_vec(),
                range_facet_fields: range_facet_fields.to_vec(),
                // The fused route does not carry score stages yet; the
                // public handler refuses the combination.
                score_stages: Vec::new(),
                geo_filters: geo_filters.to_vec(),
                filter: filter.cloned(),
                stats_fields: Vec::new(),
                cardinality_fields: Vec::new(),
                phrase: None,
                explain,
            };
            let mut client = self.node_client(node)?;
            let phrase_request =
                phrase.map(
                    |(phrase_leg, weights, _)| crate::pb::Bm25PhraseQueryRequest {
                        query: Some(request.clone()),
                        phrase_leg: phrase_leg as u32,
                        phrase_term_weights: weights.to_vec(),
                    },
                );
            if let Some((floor_tx, floor_rx)) = relay.clone() {
                let global_heap = Arc::clone(stream_heap.as_ref().expect("relay has heap"));
                let deadline = self.limits.shard_deadline;
                query_tasks.spawn(async move {
                    let started = std::time::Instant::now();
                    let run = stream_bm25_shard(
                        shard as u32,
                        k as usize,
                        client,
                        request,
                        floor_tx,
                        floor_rx,
                        global_heap,
                    );
                    let result = match deadline {
                        Some(limit) => tokio::time::timeout(limit, run).await.map_err(|_| {
                            Status::deadline_exceeded(format!(
                                "BM25 shard {shard} exceeded its {}ms deadline",
                                limit.as_millis()
                            ))
                        })?,
                        None => run.await,
                    }?;
                    let response = result.response;
                    Ok::<_, Status>((
                        shard as u32,
                        response.hits,
                        response.facets,
                        response.range_facets,
                        response.geo_columns_known,
                        response.filter_columns_known,
                        (response.segments_total, response.segments_skipped),
                        started.elapsed().as_secs_f64() * 1000.0,
                        Some(result.scoring_fingerprint),
                    ))
                });
                continue;
            }
            query_tasks.spawn(async move {
                let started = std::time::Instant::now();
                let response = match phrase_request {
                    Some(request) => client.bm25_phrase_query(request).await,
                    None => client.bm25_query(request).await,
                };
                response.map(|r| {
                    let r = r.into_inner();
                    (
                        shard as u32,
                        r.hits,
                        r.facets,
                        r.range_facets,
                        r.geo_columns_known,
                        r.filter_columns_known,
                        (r.segments_total, r.segments_skipped),
                        started.elapsed().as_secs_f64() * 1000.0,
                        None,
                    )
                })
            });
        }
        let mut all: Vec<(u32, Bm25Hit)> = Vec::new();
        let mut prune = crate::segment_prune::PruneStats::default();
        let mut shard_facets: Vec<Vec<crate::pb::FacetFieldCounts>> = Vec::new();
        let mut shard_ranges: Vec<Vec<crate::pb::RangeFacetCounts>> = Vec::new();
        let mut per_shard: Vec<(u32, f64)> = Vec::new();
        let mut geo_known = vec![false; geo_filters.len()];
        let filter_leaves = filter.map_or(0, crate::filter::leaf_count);
        let mut filter_known = vec![false; filter_leaves];
        let mut scoring_fingerprint: Option<String> = None;
        let mut responses = Vec::new();
        while let Some(joined) = query_tasks.join_next().await {
            responses.push(
                joined.map_err(|e| Status::internal(format!("bm25 query task failed: {e}")))??,
            );
        }
        responses.sort_by_key(|response| response.0);
        for response in responses {
            let (shard, hits, facets, ranges, geo, fknown, sprune, ms, fingerprint) = response;
            prune.add(crate::segment_prune::PruneStats {
                segments_total: sprune.0,
                segments_skipped: sprune.1,
            });
            if let Some(fingerprint) = fingerprint {
                match scoring_fingerprint.as_ref() {
                    Some(expected) if expected != &fingerprint => {
                        return Err(Status::failed_precondition(format!(
                            "BM25 shard {shard} scoring fingerprint {fingerprint} differs from {expected}"
                        )));
                    }
                    None => scoring_fingerprint = Some(fingerprint),
                    _ => {}
                }
            }
            per_shard.push((shard, ms));
            all.extend(hits.into_iter().map(|h| (shard, h)));
            shard_facets.push(facets);
            shard_ranges.push(ranges);
            if geo.len() != geo_filters.len() {
                return Err(Status::internal(format!(
                    "shard answered {} geo-column flags for {} filters",
                    geo.len(),
                    geo_filters.len()
                )));
            }
            for (acc, k) in geo_known.iter_mut().zip(&geo) {
                *acc |= *k;
            }
            if fknown.len() != filter_leaves {
                return Err(Status::internal(format!(
                    "shard answered {} filter-leaf flags for {} leaves",
                    fknown.len(),
                    filter_leaves
                )));
            }
            for (acc, k) in filter_known.iter_mut().zip(&fknown) {
                *acc |= *k;
            }
        }
        refuse_unknown_geo_columns(geo_filters, &geo_known)?;
        if let Some(mask) = mask.as_ref() {
            mark_known(&mut filter_known, &mask.known);
        }
        refuse_unknown_filter_leaves(filter, &filter_known)?;
        let facets = merge_facet_counts(facet_fields, map_facet_fields, &shard_facets, true)?;
        let ranges = merge_range_counts(range_facet_fields, &shard_ranges)?;
        if let (Some(heap), Some(fingerprint)) = (&stream_heap, scoring_fingerprint.as_ref()) {
            let snapshot = heap
                .lock()
                .expect("BM25 stream heap poisoned")
                .heap
                .iter()
                .map(|entry| (entry.0.vector_id, entry.0.score))
                .collect();
            self.publish_progress(
                crate::pb::QueryStreamPhase::Lexical,
                snapshot,
                fingerprint.clone(),
            );
        }
        let t_query = t0.elapsed();
        if trace {
            eprintln!(
                "bm25-fused per-shard ms: {}",
                per_shard
                    .iter()
                    .map(|(s, ms)| format!("{s}:{ms:.0}"))
                    .collect::<Vec<_>>()
                    .join(" ")
            );
        }
        if let Some(stream_heap) = stream_heap {
            let winners: Vec<MergedHit> = {
                let state = stream_heap.lock().expect("BM25 stream heap poisoned");
                let mut winners: Vec<MergedHit> = state.heap.iter().map(|entry| entry.0).collect();
                winners.sort_by(cmp_hits);
                winners
            };
            let mut details: HashMap<(u32, u64), Bm25Hit> = all
                .drain(..)
                .map(|(shard, hit)| ((shard, hit.doc_id), hit))
                .collect();
            all = winners
                .into_iter()
                .map(|winner| {
                    let hit = details
                        .remove(&(winner.shard, winner.vector_id))
                        .ok_or_else(|| {
                            Status::data_loss(format!(
                                "BM25 global winner {} from shard {} was absent from its certified local top-k",
                                winner.vector_id, winner.shard
                            ))
                        })?;
                    if hit.score.to_bits() != winner.score.to_bits() {
                        return Err(Status::data_loss(format!(
                            "BM25 global winner {} from shard {} changed score from {:?} in the candidate stream to {:?} in the certified response",
                            winner.vector_id,
                            winner.shard,
                            winner.score,
                            hit.score
                        )));
                    }
                    Ok((winner.shard, hit))
                })
                .collect::<Result<_, _>>()?;
        } else {
            all.sort_by(|(sa, a), (sb, b)| {
                b.score
                    .total_cmp(&a.score)
                    .then_with(|| sa.cmp(sb))
                    .then_with(|| a.doc_id.cmp(&b.doc_id))
            });
            all.truncate(k as usize);
        }
        if trace {
            let ms = |d: std::time::Duration| d.as_secs_f64() * 1000.0;
            eprintln!(
                "bm25-fused: analyze {:.1} ms, term-stats {:.1} ms, query fan-out {:.1} ms, \
                 merge {:.1} ms  ({} fields, {} terms)",
                ms(t_analyzed),
                ms(t_stats - t_analyzed),
                ms(t_query - t_stats),
                ms(t0.elapsed() - t_query),
                fields.len(),
                field_terms.iter().map(Vec::len).sum::<usize>(),
            );
        }
        Ok((
            (all.into_iter().map(|(_, h)| h).collect(), facets, ranges),
            prune,
        ))
    }
}
