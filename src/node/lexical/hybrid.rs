//! Fused lexical/vector leg execution and hybrid shard scoring.

use crate::node::*;

/// Raw leg hits as `(global_doc_id, raw_score)`, score-descending.
type RawLeg = Vec<(u64, f64)>;

/// The filter half of a leg request: the wire filters plus the geo
/// regions already validated at the RPC boundary. Bundled because both
/// leg routes carry the identical trio and `compute_legs` was already
/// at the argument limit.
pub(in crate::node) struct LegFilters<'a> {
    pub(in crate::node) geo: &'a [crate::pb::GeoFilter],
    pub(in crate::node) regions: Vec<crate::geo::GeoRegion>,
    pub(in crate::node) tree: Option<&'a crate::pb::FilterExpr>,
}

/// What one shard's leg computation produced: the two raw legs plus
/// the known-column handshakes, which travel with every filtered
/// response so the coordinator can refuse a name no shard resolves.
pub(in crate::node) struct LegResults {
    pub(in crate::node) vector: RawLeg,
    pub(in crate::node) bm25: RawLeg,
    pub(in crate::node) geo_columns_known: Vec<bool>,
    pub(in crate::node) filter_columns_known: Vec<bool>,
    pub(in crate::node) read_receipt: Option<crate::pb::VectorReadReceipt>,
    pub(in crate::node) rerank_candidates: HashMap<u64, crate::pb::RerankCandidatePayload>,
}

impl NodeServiceImpl {
    /// Compute both raw legs for a hybrid query: `(vector_leg, bm25_leg)`
    /// as `(global_doc_id, raw_score)` lists, score-descending.
    ///
    /// Vector leg: the chunked scan (local floor seeding only — the
    /// cross-shard floor-sharing protocol lives on SearchShard's bidi
    /// stream and is not part of the unary hybrid path). A shard with no
    /// vector index, or an empty query vector, contributes an empty leg
    /// rather than failing the whole hybrid query. BM25 leg: scored with
    /// the coordinator-supplied GLOBAL stats.
    /// The fused multi-field Bm25Query route (`docs/multi-field.md`):
    /// legs resolve to field views by name, score through
    /// [`bm25::top_k_fused_pruned`] (exhaustive when impacts are
    /// missing or `--block-max=false`; results identical), and the
    /// floor applies to the FUSED score. A leg naming a field this
    /// shard lacks is skipped: its documents hold no postings there, so
    /// every fused score is unchanged — the graceful path for a
    /// heterogeneous fleet. That is safe only because the coordinator
    /// refuses a field NO shard knows (see `fanout_bm25_fused` and
    /// `FieldStats.known`); skipping alone would turn a misspelled field
    /// into a silently different ranking.
    pub(in crate::node) fn bm25_query_fused(
        &self,
        req: &Bm25QueryRequest,
        phrase: Option<(usize, &[f32])>,
        live: Option<bm25::LiveFloorHook>,
        mut candidate: Option<bm25::CandidateHook>,
    ) -> Result<Bm25QueryResponse, Status> {
        if !req.projections.is_empty() {
            return Err(Status::invalid_argument(
                "projection: projections are not certified on the fused route yet; \
                 use the flat Bm25Search route",
            ));
        }

        for leg in &req.fields {
            if leg.terms.len() != leg.global_doc_frequencies.len() {
                return Err(Status::invalid_argument(format!(
                    "leg {:?}: terms and global_doc_frequencies must have the same length",
                    leg.field
                )));
            }
            if leg.weight < 0.0 || leg.weight.is_nan() {
                return Err(Status::invalid_argument(format!(
                    "leg {:?}: weight must be >= 0",
                    leg.field
                )));
            }
        }
        validate_range_facet_fields(&req.range_facet_fields)?;
        let geo_regions = validate_geo_filters(&req.geo_filters)?;
        if let Some(f) = req.filter.as_ref() {
            crate::filter::validate_filter(f)?;
        }
        let guard = read_shard(&self.state);
        guard.check_stats_epoch(req.expected_stats_epoch, &req.expected_stats_incarnation)?;
        for leg in &req.fields {
            guard.check_query_analysis(&leg.field, leg.analysis_fingerprint)?;
        }
        // Filled inside the scoring arm (the facet walk reuses the
        // resolved field views); a shard with no lexical half answers
        // every requested facet field as unknown.
        let mut facets: Vec<crate::pb::FacetFieldCounts> = Vec::new();
        let mut range_facets: Vec<crate::pb::RangeFacetCounts> = Vec::new();
        // Computed regardless of k and of whether the shard scores, so
        // a typo'd column refuses on the fused route too.
        let geo_columns_known = match guard.bm25.as_ref() {
            Some(store) => store.geo_columns_known(&req.geo_filters),
            None => vec![false; req.geo_filters.len()],
        };
        let filter_columns_known = match (guard.bm25.as_ref(), req.filter.as_ref()) {
            (Some(store), Some(f)) => store.filter_columns_known(f),
            (None, Some(f)) => vec![false; crate::filter::leaf_count(f)],
            (_, None) => Vec::new(),
        };
        let mut fused_prune = crate::segment_prune::PruneStats::default();
        let hits: Vec<Bm25Hit> = match guard.bm25.as_ref() {
            // Facet counting enters the arm even at k == 0 (the flat
            // path counts regardless of k; the scorers return no hits
            // for k == 0 on their own).
            Some(store)
                if req.k > 0
                    || !req.facet_fields.is_empty()
                    || !req.map_facet_fields.is_empty()
                    || !req.range_facet_fields.is_empty() =>
            {
                if store.as_index().is_none() {
                    return Err(Status::failed_precondition(
                        "bm25 bulk build in progress; Flush first",
                    ));
                }
                let mut views: Vec<Box<dyn Bm25Index + '_>> = Vec::new();
                let mut leg_of_view: Vec<usize> = Vec::new();
                for (li, leg) in req.fields.iter().enumerate() {
                    if let Some(fi) = store.field_index(&leg.field) {
                        views.push(store.field_view(fi).expect("searchable, checked above"));
                        leg_of_view.push(li);
                    }
                }
                // The request's filters, resolved ONCE against this
                // shard's tables and shared by facet counting and the
                // scorers below — one resolution, one truth
                // (docs/geo-columns.md, docs/cel-filters.md). With no
                // filters the ctx is None and every path below is
                // bit-identical to its unfiltered form.
                let numeric_read = ShardNumericRead(store);
                let pred = req
                    .filter
                    .as_ref()
                    .map(|f| store.resolve_filter(f))
                    .transpose()?;
                // Sealed segments the predicate rules out leave every
                // leg's walk (docs/segment-pruning.md): the views the
                // scorers, the facet counts, and the phrase gates take
                // are rebuilt over the admitted parts.
                let prune = prune_segments(store, pred.as_ref(), self.knobs.segment_pruning());
                fused_prune = prune.stats;
                if let Some(mask) = prune.mask.as_ref() {
                    views = leg_of_view
                        .iter()
                        .map(|&li| {
                            let fi = store
                                .field_index(&req.fields[li].field)
                                .expect("the field resolved above");
                            store
                                .field_view_masked(fi, mask)
                                .expect("searchable, checked above")
                        })
                        .collect();
                }
                // Phrase gates (docs/phrase-proximity.md): a leg's
                // ordered window, checked at the same heap gate as every
                // other filter. The field must be on this shard and must
                // carry positions; a shard cannot approximate the window
                // from spans, so it refuses by name instead of scoring
                // the terms unconstrained.
                let mut phrase_gates: Vec<crate::filter::PhraseGate<'_>> = Vec::new();
                for (li, leg) in req.fields.iter().enumerate() {
                    let Some(phrase) = leg.phrase.as_ref() else {
                        continue;
                    };
                    let Some(vi) = leg_of_view.iter().position(|&candidate| candidate == li) else {
                        return Err(Status::failed_precondition(format!(
                            "phrase field {:?} is absent from this shard; a phrase cannot be \
                             served on part of the fleet",
                            leg.field
                        )));
                    };
                    if !views[vi].has_positions() {
                        return Err(Status::failed_precondition(format!(
                            "field {:?} has no token positions on this shard; a phrase or slop \
                             query needs --position-fields={} and a rebuilt generation",
                            leg.field, leg.field
                        )));
                    }
                    crate::proximity::validate_leg(phrase, leg.terms.len())
                        .map_err(Status::invalid_argument)?;
                    phrase_gates.push(crate::filter::PhraseGate {
                        index: views[vi].as_ref(),
                        terms: &leg.terms,
                        sequence: phrase.sequence.iter().map(|&i| i as usize).collect(),
                        alternatives: phrase
                            .alternatives
                            .iter()
                            .map(|p| p.terms.iter().map(|&i| i as usize).collect())
                            .collect(),
                        slop: phrase.slop,
                    });
                }
                let doc_filter = crate::filter::DocFilter {
                    deleted: guard.live_docs.words(),
                    geo: store.resolve_geo_filters(&req.geo_filters, &geo_regions),
                    pred,
                    phrase: phrase_gates,
                };
                let filter_ctx: bm25::FilterCtx = if req.geo_filters.is_empty()
                    && doc_filter.pred.is_none()
                    && doc_filter.deleted.is_none()
                    && doc_filter.phrase.is_empty()
                {
                    None
                } else {
                    Some((&doc_filter, &numeric_read))
                };
                if !req.facet_fields.is_empty()
                    || !req.map_facet_fields.is_empty()
                    || !req.range_facet_fields.is_empty()
                {
                    let pairs: Vec<(&dyn Bm25Index, &[String])> = views
                        .iter()
                        .zip(&leg_of_view)
                        .map(|(view, &li)| (view.as_ref(), req.fields[li].terms.as_slice()))
                        .collect();
                    // The fused route refuses stats/cardinality
                    // upstream, like score stages.
                    (facets, range_facets, _, _) = store.count_facets(
                        &pairs,
                        &req.facet_fields,
                        &req.map_facet_fields,
                        &req.range_facet_fields,
                        &[],
                        &[],
                        filter_ctx,
                    )?;
                }
                // Leg list order is the pinned accumulation order; the
                // coordinator sends the same order to every shard, so
                // distributed fused scores are bit-identical to the
                // monolith's.
                let queries: Vec<bm25::FieldQuery> = views
                    .iter()
                    .zip(&leg_of_view)
                    .map(|(view, &li)| {
                        let leg = &req.fields[li];
                        Ok(bm25::FieldQuery {
                            index: view.as_ref(),
                            terms: &leg.terms,
                            stats: bm25::CorpusStats {
                                doc_count: req.global_doc_count,
                                total_doc_length: leg.global_total_doc_length,
                                dfs: leg.global_doc_frequencies.clone(),
                            },
                            params: params_from(leg.k1, leg.b)?,
                            weight: if leg.weight == 0.0 {
                                1.0
                            } else {
                                f64::from(leg.weight)
                            },
                        })
                    })
                    .collect::<Result<_, Status>>()?;
                let floor = if req.min_score == 0.0 {
                    f64::NEG_INFINITY
                } else {
                    f64::from(req.min_score)
                };
                let prunable = self.config.block_max
                    && queries.iter().all(|fq| {
                        fq.terms
                            .iter()
                            .enumerate()
                            // Local absence is not a missing impact
                            // surface: see top_k_fused_pruned_stats.
                            // Global df alone would forfeit pruning on
                            // every shard lacking a rare term.
                            .all(|(ti, t)| {
                                fq.stats.dfs[ti] == 0
                                    || fq.index.df(t) == 0
                                    || fq.index.has_impacts(t)
                            })
                    });
                // Filters ride the fused route exactly as range facets
                // do: the coordinator forwards them verbatim and the
                // node applies them at the one place a filter belongs,
                // before heap insertion (`filter_ctx`, resolved above).
                let phrase_view = match phrase {
                    Some((leg, weights)) => Some((
                        leg_of_view
                        .iter()
                        .position(|&candidate| candidate == leg)
                        .ok_or_else(|| {
                            Status::failed_precondition(format!(
                                "phrase field {:?} is absent from this shard; rebuild the complete generation",
                                req.fields[leg].field
                            ))
                        })?,
                        weights,
                    )),
                    None => None,
                };
                let docs = if let Some((phrase_view, weights)) = phrase_view {
                    let docs = bm25::filter_fused_to_floor(
                        bm25::top_k_phrase_exhaustive_filtered(
                            &queries,
                            phrase_view,
                            &weights
                                .iter()
                                .map(|&value| f64::from(value))
                                .collect::<Vec<_>>(),
                            req.k as usize,
                            filter_ctx,
                        ),
                        floor,
                    );
                    if let Some(sink) = candidate.as_mut() {
                        for doc in &docs {
                            sink(doc.doc_id, doc.score as f32);
                        }
                    }
                    docs
                } else if prunable {
                    let mut prune = bm25::PruneStats::default();
                    bm25::top_k_fused_pruned_filtered_stats_streaming(
                        &queries,
                        req.k as usize,
                        floor,
                        filter_ctx,
                        &mut prune,
                        live,
                        candidate,
                    )
                } else {
                    let docs = bm25::filter_fused_to_floor(
                        bm25::top_k_fused_exhaustive_filtered(&queries, req.k as usize, filter_ctx),
                        floor,
                    );
                    if let Some(sink) = candidate.as_mut() {
                        for doc in &docs {
                            sink(doc.doc_id, doc.score as f32);
                        }
                    }
                    docs
                };
                let highlight = highlight_plan(store, req.highlight.as_ref())?;
                let body = store.field_name(0);
                let text_index = store.as_index().ok_or_else(|| {
                    Status::failed_precondition("bm25 bulk build in progress; Flush first")
                })?;
                // The explain breakdown, only for the hits that survived
                // the top-k (docs/explain.md).
                let presence = req.explain.then(|| {
                    let ids: Vec<u32> = docs.iter().map(|doc| doc.doc_id).collect();
                    bm25::breakdown(&queries, &ids)
                });
                let leg_names: Vec<&str> = leg_of_view
                    .iter()
                    .map(|&li| req.fields[li].field.as_str())
                    .collect();
                let phrase_weights: Option<(usize, Vec<f64>)> =
                    phrase_view.map(|(view, weights)| {
                        (
                            view,
                            weights.iter().map(|&value| f64::from(value)).collect(),
                        )
                    });
                docs.into_iter()
                    .enumerate()
                    .map(|(hit_index, doc)| -> Result<Bm25Hit, Status> {
                        let explain = presence.as_ref().map(|presence| {
                            explain_terms(
                                &queries,
                                &leg_names,
                                &presence[hit_index],
                                phrase_weights
                                    .as_ref()
                                    .map(|(view, weights)| (*view, weights.as_slice())),
                            )
                        });
                        let snippets = match highlight.as_ref() {
                            Some(plan) => {
                                // Body occurrences only (the stored text
                                // is the body's); a term is distinct per
                                // leg, so the key carries the leg.
                                let occurrences: Vec<(usize, (u32, u32))> = doc
                                    .term_offsets
                                    .iter()
                                    .filter(|(fi, _, _)| req.fields[leg_of_view[*fi]].field == body)
                                    .flat_map(|(fi, ti, offsets)| {
                                        let key = (leg_of_view[*fi] << 20) | *ti;
                                        offsets.iter().map(move |&span| (key, span))
                                    })
                                    .collect();
                                cut_snippets(plan, text_index, body, doc.doc_id, &occurrences)?
                            }
                            None => Vec::new(),
                        };
                        Ok(Bm25Hit {
                            identity: store.document_identity(doc.doc_id),
                            explain,
                            snippets,
                            projected: Vec::new(),
                            doc_id: self.config.slot_offset + u64::from(doc.doc_id),
                            score: doc.score as f32,
                            terms: doc
                                .term_offsets
                                .into_iter()
                                .map(|(fi, ti, offsets)| {
                                    let leg = &req.fields[leg_of_view[fi]];
                                    TermOccurrences {
                                        term: leg.terms[ti].clone(),
                                        field: leg.field.clone(),
                                        offsets: offsets
                                            .into_iter()
                                            .map(|(start, end)| OffsetSpan { start, end })
                                            .collect(),
                                    }
                                })
                                .collect(),
                        })
                    })
                    .collect::<Result<Vec<_>, Status>>()?
            }
            _ => {
                facets = req
                    .facet_fields
                    .iter()
                    .map(|name| (name.clone(), None))
                    .chain(
                        req.map_facet_fields
                            .iter()
                            .map(|m| (m.column.clone(), Some(m.key.clone()))),
                    )
                    .map(|(field, key)| crate::pb::FacetFieldCounts {
                        field,
                        known: false,
                        counts: Vec::new(),
                        key: key.clone().unwrap_or_default(),
                        map_key: key,
                    })
                    .collect();
                range_facets = unknown_range_counts(&req.range_facet_fields);
                Vec::new()
            }
        };
        // Same seed rule as the flat path: one f32 ULP below the k-th
        // fused score when the heap filled, 0 otherwise.
        let kth_best = if hits.len() == req.k as usize {
            hits.last()
                .map(|h| bm25::floor_seed(h.score))
                .unwrap_or(0.0)
        } else {
            0.0
        };
        Ok(Bm25QueryResponse {
            segments_total: fused_prune.segments_total,
            segments_skipped: fused_prune.segments_skipped,
            projection_leaves_known: Vec::new(),
            projection_types: Vec::new(),
            hits,
            kth_best,
            facets,
            // The fused route refuses score stages upstream, and
            // stats/cardinality with them.
            stage_columns_known: Vec::new(),
            stats: Vec::new(),
            distinct: Vec::new(),
            range_facets,
            geo_columns_known,
            filter_columns_known,
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub(in crate::node) fn compute_legs(
        &self,
        vector: &[f32],
        terms: &[String],
        analysis_fingerprint: u64,
        global_doc_count: u64,
        global_total_doc_length: u64,
        global_doc_frequencies: &[u32],
        params: Bm25Params,
        k: usize,
        expected_stats_epoch: u64,
        expected_stats_incarnation: &[u8],
        filters: &LegFilters<'_>,
        read_context: Option<&crate::pb::VectorReadContext>,
        include_rerank_candidate: bool,
    ) -> Result<LegResults, Status> {
        let guard = read_shard(&self.state);
        if !terms.is_empty() {
            guard.check_query_analysis("body", analysis_fingerprint)?;
        }
        guard.check_stats_epoch(expected_stats_epoch, expected_stats_incarnation)?;
        // One resolution for both legs (docs/vector-filters.md): the
        // allowlist the vector kernel scans under and the predicate the
        // lexical heap gate applies are the same DocFilter, so the two
        // halves of a fused list cannot disagree about what matched.
        let (geo_columns_known, filter_columns_known) =
            filter_known_flags(guard.bm25.as_ref(), filters.geo, filters.tree);
        // A lexical-only or disabled leg needs the predicate and receipt,
        // but must not allocate a vector mask proportional to the corpus.
        let slots = if k > 0 && !vector.is_empty() {
            guard.index.as_ref().map_or(0, |index| index.len())
        } else {
            0
        };
        let ((doc_filter, allow, _prune), read_receipt) = guard.vector_scan_filters(
            slots,
            filters.geo,
            &filters.regions,
            filters.tree,
            read_context,
            self.knobs.segment_pruning(),
        )?;

        let mut vector_leg: Vec<(u64, f64)> = Vec::new();
        if k > 0 && !vector.is_empty() {
            if let Some(index) = guard.index.as_ref() {
                let dim = index.dim_opt().unwrap_or(0);
                if vector.len() != dim {
                    return Err(Status::invalid_argument(format!(
                        "hybrid vector has dim {}, index expects {dim}",
                        vector.len()
                    )));
                }
                if let Some((_, coord, value)) = first_invalid_coordinate(vector, dim) {
                    return Err(Status::invalid_argument(format!(
                        "hybrid vector coordinate {coord} is invalid: {value}"
                    )));
                }
                let (hits, scan_stats) = chunked_topk(
                    index,
                    vector,
                    k,
                    self.config.chunk_blocks,
                    &mut || None,
                    &mut |_| false,
                    false,
                    allow.as_ref().map(|bits| bits.to_bools()).as_deref(),
                );
                observe_scan(scan_stats.bytes_scanned, scan_stats.scan_nanos);
                vector_leg = hits
                    .into_iter()
                    .map(|h| {
                        (
                            self.config.slot_offset + u64::from(h.slot),
                            f64::from(h.score),
                        )
                    })
                    .collect();
            }
        }

        let mut bm25_leg: Vec<(u64, f64)> = Vec::new();
        if k > 0 && !terms.is_empty() {
            if let Some(store) = guard.bm25.as_ref() {
                let stats = bm25::CorpusStats {
                    doc_count: global_doc_count,
                    total_doc_length: global_total_doc_length,
                    dfs: global_doc_frequencies.to_vec(),
                };
                let index = store.as_index().ok_or_else(|| {
                    Status::failed_precondition("bm25 bulk build in progress; Flush first")
                })?;
                // Block-max path when every scored term has impacts
                // (v5 shards) and the node flag allows it; heap store,
                // v3/v4, and --block-max=false keep top_k. The results
                // are bit-identical either way.
                let prunable = self.config.block_max
                    && terms
                        .iter()
                        .enumerate()
                        // Local absence is not a missing impact surface:
                        // see top_k_pruned. Global df alone would forfeit
                        // pruning on every shard lacking a rare term.
                        .all(|(ti, t)| {
                            stats.dfs[ti] == 0 || index.df(t) == 0 || index.has_impacts(t)
                        });
                let numeric_read = ShardNumericRead(store);
                let filter_ctx: bm25::FilterCtx = doc_filter
                    .as_ref()
                    .map(|f| (f, &numeric_read as &dyn crate::scorefn::NumericRead));
                let docs = if prunable {
                    let mut prune = bm25::PruneStats::default();
                    bm25::top_k_pruned_chained_filtered_stats(
                        index,
                        terms,
                        &stats,
                        params,
                        k,
                        f64::NEG_INFINITY,
                        None,
                        filter_ctx,
                        &mut prune,
                    )
                } else {
                    bm25::top_k_chained_filtered(index, terms, &stats, params, k, None, filter_ctx)
                };
                bm25_leg = docs
                    .into_iter()
                    .map(|d| (self.config.slot_offset + u64::from(d.doc_id), d.score))
                    .collect();
            }
        }

        let mut rerank_candidates = HashMap::new();
        if include_rerank_candidate {
            let offset = self.config.slot_offset;
            for &(doc_id, _) in vector_leg.iter().chain(bm25_leg.iter()) {
                if rerank_candidates.contains_key(&doc_id) || doc_id < offset {
                    continue;
                }
                if let Ok(local) = u32::try_from(doc_id - offset) {
                    if let Some(payload) = guard.rerank_candidate_for(local) {
                        rerank_candidates.insert(doc_id, payload);
                    }
                }
            }
        }

        Ok(LegResults {
            vector: vector_leg,
            bm25: bm25_leg,
            geo_columns_known,
            filter_columns_known,
            read_receipt,
            rerank_candidates,
        })
    }

    /// Level one of the two-level hybrid fusion: run both legs locally
    /// and RRF-fuse them (see `SearchService.HybridSearch`).
    pub(in crate::node) fn run_hybrid(
        &self,
        req: HybridShardRequest,
    ) -> Result<HybridShardResponse, Status> {
        let k = req.k as usize;
        if req.terms.len() != req.global_doc_frequencies.len() {
            return Err(Status::invalid_argument(
                "terms and global_doc_frequencies must have the same length",
            ));
        }
        let vector_weight = weight_or_default(req.vector_weight, "vector_weight")?;
        let bm25_weight = weight_or_default(req.bm25_weight, "bm25_weight")?;
        let rrf_k = if req.rrf_k == 0.0 {
            fusion::DEFAULT_RRF_K
        } else {
            f64::from(req.rrf_k)
        };
        if rrf_k.is_nan() || rrf_k <= 0.0 {
            return Err(Status::invalid_argument("rrf_k must be positive"));
        }

        let geo_regions = validate_geo_filters(&req.geo_filters)?;
        if let Some(f) = req.filter.as_ref() {
            crate::filter::validate_filter(f)?;
        }
        let mut legs = self.compute_legs(
            &req.vector,
            &req.terms,
            req.analysis_fingerprint,
            req.global_doc_count,
            req.global_total_doc_length,
            &req.global_doc_frequencies,
            params_from(req.k1, req.b)?,
            k,
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
        let geo_columns_known = legs.geo_columns_known;
        let filter_columns_known = legs.filter_columns_known;

        let fused = fusion::rrf_fuse(
            &[
                Leg {
                    hits: legs.vector,
                    weight: vector_weight,
                },
                Leg {
                    hits: legs.bm25,
                    weight: bm25_weight,
                },
            ],
            rrf_k,
            k,
        );
        Ok(HybridShardResponse {
            hits: fused
                .into_iter()
                .map(|h| HybridLegHit {
                    doc_id: h.doc_id,
                    fused_score: h.fused_score as f32,
                    vector_rank: h.leg_ranks[0],
                    vector_score: h.leg_scores[0].unwrap_or(0.0) as f32,
                    bm25_rank: h.leg_ranks[1],
                    bm25_score: h.leg_scores[1].unwrap_or(0.0) as f32,
                    rerank_candidate: legs.rerank_candidates.remove(&h.doc_id),
                })
                .collect(),
            geo_columns_known,
            filter_columns_known,
            read_receipt: legs.read_receipt,
        })
    }
}

/// Request weights default to 1.0 (0 means "unset" in the proto);
/// negatives are rejected.
fn weight_or_default(value: f32, name: &str) -> Result<f64, Status> {
    if value == 0.0 {
        return Ok(1.0);
    }
    if value < 0.0 || value.is_nan() {
        return Err(Status::invalid_argument(format!("{name} must be >= 0")));
    }
    Ok(f64::from(value))
}
