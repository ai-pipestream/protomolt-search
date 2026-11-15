//! Fused lexical/vector leg execution and hybrid shard scoring.

use crate::node::*;

/// Raw leg hits as `(global_doc_id, raw_score)`, score-descending.
type RawLeg = Vec<(u64, f64)>;

/// Wire filters and geo regions validated at the RPC boundary.
pub(in crate::node) struct LegFilters<'a> {
    pub(in crate::node) geo: &'a [crate::pb::GeoFilter],
    pub(in crate::node) regions: Vec<crate::geo::GeoRegion>,
    pub(in crate::node) tree: Option<&'a crate::pb::FilterExpr>,
}

/// Raw legs, known-column handshakes, and generation-bound candidate details.
pub(in crate::node) struct LegResults {
    pub(in crate::node) vector: RawLeg,
    pub(in crate::node) bm25: RawLeg,
    pub(in crate::node) geo_columns_known: Vec<bool>,
    pub(in crate::node) filter_columns_known: Vec<bool>,
    pub(in crate::node) read_receipt: Option<crate::pb::VectorReadReceipt>,
    pub(in crate::node) rerank_candidates: HashMap<u64, crate::pb::RerankCandidatePayload>,
}

impl NodeServiceImpl {
    /// Score fused fields against one shard generation and its resolved filters.
    pub(in crate::node) fn bm25_query_fused(
        &self,
        req: &Bm25QueryRequest,
        phrase: Option<(usize, &[f32])>,
        live: Option<bm25::LiveFloorHook>,
        candidate: Option<bm25::CandidateHook>,
    ) -> Result<Bm25QueryResponse, Status> {
        let regions = validate_fused_request(req)?;
        let guard = read_shard(&self.state);
        guard.check_stats_epoch(req.expected_stats_epoch, &req.expected_stats_incarnation)?;
        for leg in &req.fields {
            guard.check_query_analysis(&leg.field, leg.analysis_fingerprint)?;
        }
        let (geo_columns_known, filter_columns_known) =
            filter_known_flags(guard.bm25.as_ref(), &req.geo_filters, req.filter.as_ref());
        let mut response = match guard.bm25.as_ref() {
            Some(store) if req.k > 0 || fused_has_facets(req) => {
                let execution = FusedExecution {
                    service: self,
                    req,
                    guard: &guard,
                    store,
                };
                execution.run(&regions, phrase, live, candidate)?
            }
            _ => unknown_fused_response(req),
        };
        response.geo_columns_known = geo_columns_known;
        response.filter_columns_known = filter_columns_known;
        response.kth_best = if response.hits.len() == req.k as usize {
            response
                .hits
                .last()
                .map(|h| bm25::floor_seed(h.score))
                .unwrap_or(0.0)
        } else {
            0.0
        };
        Ok(response)
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

struct FusedExecution<'a> {
    service: &'a NodeServiceImpl,
    req: &'a Bm25QueryRequest,
    guard: &'a ShardState,
    store: &'a Bm25Shard,
}

impl FusedExecution<'_> {
    fn run(
        &self,
        regions: &[crate::geo::GeoRegion],
        phrase: Option<(usize, &[f32])>,
        live: Option<bm25::LiveFloorHook>,
        candidate: Option<bm25::CandidateHook>,
    ) -> Result<Bm25QueryResponse, Status> {
        let req = self.req;
        let store = self.store;
        let mut fields = FusedFields::new(req, store)?;
        let numeric_read = ShardNumericRead(store);
        let pred = req
            .filter
            .as_ref()
            .map(|f| store.resolve_filter(f))
            .transpose()?;
        let prune = prune_segments(store, pred.as_ref(), self.service.knobs.segment_pruning());
        fields.apply_mask(store, prune.mask.as_ref());
        let phrase_gates = fields.phrase_gates()?;
        let doc_filter = crate::filter::DocFilter {
            deleted: self.guard.live_docs.words(),
            geo: store.resolve_geo_filters(&req.geo_filters, regions),
            pred,
            phrase: phrase_gates,
        };
        let filter = if req.geo_filters.is_empty()
            && doc_filter.pred.is_none()
            && doc_filter.deleted.is_none()
            && doc_filter.phrase.is_empty()
        {
            None
        } else {
            Some((
                &doc_filter,
                &numeric_read as &dyn crate::scorefn::NumericRead,
            ))
        };
        let mut response = Bm25QueryResponse {
            segments_total: prune.stats.segments_total,
            segments_skipped: prune.stats.segments_skipped,
            ..Default::default()
        };
        fields.count_facets(store, filter, &mut response)?;
        let queries = fields.queries()?;
        let phrase = fields.phrase_view(phrase)?;
        let options = bm25::TopKOptions {
            k: req.k as usize,
            floor: if req.min_score == 0.0 {
                f64::NEG_INFINITY
            } else {
                f64::from(req.min_score)
            },
            block_max: self.service.config.block_max,
        };
        let docs = score_fused(&queries, options, filter, phrase.as_ref(), live, candidate);
        response.hits = fields.render_hits(
            self.store,
            &queries,
            docs,
            self.service.config.slot_offset,
            phrase.as_ref(),
        )?;
        Ok(response)
    }
}

struct FusedFields<'a> {
    req: &'a Bm25QueryRequest,
    views: Vec<Box<dyn Bm25Index + 'a>>,
    legs: Vec<usize>,
}

type FusedPhrase = (usize, Vec<f64>);

impl<'a> FusedFields<'a> {
    fn new(req: &'a Bm25QueryRequest, store: &'a Bm25Shard) -> Result<Self, Status> {
        if store.as_index().is_none() {
            return Err(Status::failed_precondition(
                "bm25 bulk build in progress; Flush first",
            ));
        }
        let mut fields = Self {
            req,
            views: Vec::new(),
            legs: Vec::new(),
        };
        for (li, leg) in req.fields.iter().enumerate() {
            if let Some(fi) = store.field_index(&leg.field) {
                fields
                    .views
                    .push(store.field_view(fi).expect("searchable, checked above"));
                fields.legs.push(li);
            }
        }
        Ok(fields)
    }

    fn apply_mask(&mut self, store: &'a Bm25Shard, mask: Option<&Arc<[bool]>>) {
        if let Some(mask) = mask {
            self.views = self
                .legs
                .iter()
                .map(|&li| {
                    let fi = store
                        .field_index(&self.req.fields[li].field)
                        .expect("the field resolved above");
                    store
                        .field_view_masked(fi, mask)
                        .expect("searchable, checked above")
                })
                .collect();
        }
    }

    fn phrase_gates(&self) -> Result<Vec<crate::filter::PhraseGate<'_>>, Status> {
        let mut gates = Vec::new();
        for (li, leg) in self.req.fields.iter().enumerate() {
            let Some(phrase) = leg.phrase.as_ref() else {
                continue;
            };
            let Some(vi) = self.legs.iter().position(|&candidate| candidate == li) else {
                return Err(Status::failed_precondition(format!(
                    "phrase field {:?} is absent from this shard; a phrase cannot be \
                     served on part of the fleet",
                    leg.field
                )));
            };
            if !self.views[vi].has_positions() {
                return Err(Status::failed_precondition(format!(
                    "field {:?} has no token positions on this shard; a phrase or slop \
                     query needs --position-fields={} and a rebuilt generation",
                    leg.field, leg.field
                )));
            }
            crate::proximity::validate_leg(phrase, leg.terms.len())
                .map_err(Status::invalid_argument)?;
            gates.push(crate::filter::PhraseGate {
                index: self.views[vi].as_ref(),
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
        Ok(gates)
    }

    fn count_facets(
        &self,
        store: &Bm25Shard,
        filter: bm25::FilterCtx,
        response: &mut Bm25QueryResponse,
    ) -> Result<(), Status> {
        if fused_has_facets(self.req) {
            let pairs: Vec<(&dyn Bm25Index, &[String])> = self
                .views
                .iter()
                .zip(&self.legs)
                .map(|(view, &li)| (view.as_ref(), self.req.fields[li].terms.as_slice()))
                .collect();
            (response.facets, response.range_facets, _, _) = store.count_facets(
                &pairs,
                &self.req.facet_fields,
                &self.req.map_facet_fields,
                &self.req.range_facet_fields,
                &[],
                &[],
                filter,
            )?;
        }
        Ok(())
    }

    fn queries(&self) -> Result<Vec<bm25::FieldQuery<'_>>, Status> {
        self.views
            .iter()
            .zip(&self.legs)
            .map(|(view, &li)| {
                let leg = &self.req.fields[li];
                Ok(bm25::FieldQuery {
                    index: view.as_ref(),
                    terms: &leg.terms,
                    stats: bm25::CorpusStats {
                        doc_count: self.req.global_doc_count,
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
            .collect()
    }

    fn phrase_view(&self, phrase: Option<(usize, &[f32])>) -> Result<Option<FusedPhrase>, Status> {
        let Some((leg, weights)) = phrase else {
            return Ok(None);
        };
        let view = self
            .legs
            .iter()
            .position(|&candidate| candidate == leg)
            .ok_or_else(|| {
                Status::failed_precondition(format!(
                    "phrase field {:?} is absent from this shard; rebuild the complete generation",
                    self.req.fields[leg].field
                ))
            })?;
        let weights = weights.iter().copied().map(f64::from).collect();
        Ok(Some((view, weights)))
    }

    fn render_hits(
        &self,
        store: &Bm25Shard,
        queries: &[bm25::FieldQuery],
        docs: Vec<bm25::FusedDoc>,
        slot_offset: u64,
        phrase: Option<&FusedPhrase>,
    ) -> Result<Vec<Bm25Hit>, Status> {
        let highlight = highlight_plan(store, self.req.highlight.as_ref())?;
        let body = store.field_name(0);
        let text_index = store.as_index().ok_or_else(|| {
            Status::failed_precondition("bm25 bulk build in progress; Flush first")
        })?;
        let presence = self.req.explain.then(|| {
            let ids: Vec<u32> = docs.iter().map(|doc| doc.doc_id).collect();
            bm25::breakdown(queries, &ids)
        });
        let leg_names: Vec<&str> = self
            .legs
            .iter()
            .map(|&li| self.req.fields[li].field.as_str())
            .collect();
        docs.into_iter()
            .enumerate()
            .map(|(hit_index, doc)| {
                let explain = presence.as_ref().map(|presence| {
                    explain_terms(
                        queries,
                        &leg_names,
                        &presence[hit_index],
                        phrase.map(|(view, weights)| (*view, weights.as_slice())),
                    )
                });
                let snippets = match highlight.as_ref() {
                    Some(plan) => cut_snippets(
                        plan,
                        text_index,
                        body,
                        doc.doc_id,
                        &self.body_occurrences(&doc, body),
                    )?,
                    None => Vec::new(),
                };
                Ok(Bm25Hit {
                    identity: store.document_identity(doc.doc_id),
                    explain,
                    snippets,
                    projected: Vec::new(),
                    doc_id: slot_offset + u64::from(doc.doc_id),
                    score: doc.score as f32,
                    terms: doc
                        .term_offsets
                        .into_iter()
                        .map(|(fi, ti, offsets)| {
                            let leg = &self.req.fields[self.legs[fi]];
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
            .collect()
    }

    fn body_occurrences(&self, doc: &bm25::FusedDoc, body: &str) -> Vec<(usize, (u32, u32))> {
        doc.term_offsets
            .iter()
            .filter(|(fi, _, _)| self.req.fields[self.legs[*fi]].field == body)
            .flat_map(|(fi, ti, offsets)| {
                let key = (self.legs[*fi] << 20) | *ti;
                offsets.iter().map(move |&span| (key, span))
            })
            .collect()
    }
}

fn score_fused(
    queries: &[bm25::FieldQuery],
    options: bm25::TopKOptions,
    filter: bm25::FilterCtx,
    phrase: Option<&FusedPhrase>,
    live: Option<bm25::LiveFloorHook>,
    mut candidate: Option<bm25::CandidateHook>,
) -> Vec<bm25::FusedDoc> {
    if let Some((view, weights)) = phrase {
        let docs = bm25::top_k_phrase_exhaustive_filtered_above(
            queries,
            *view,
            weights,
            options.k,
            filter,
            options.floor,
        );
        if let Some(sink) = candidate.as_mut() {
            for doc in &docs {
                sink(doc.doc_id, doc.score as f32);
            }
        }
        docs
    } else {
        bm25::top_k_fused_pruned_filtered_stats_streaming(
            queries,
            options,
            filter,
            &mut bm25::PruneStats::default(),
            live,
            candidate,
        )
    }
}

fn validate_fused_request(req: &Bm25QueryRequest) -> Result<Vec<crate::geo::GeoRegion>, Status> {
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
    let regions = validate_geo_filters(&req.geo_filters)?;
    if let Some(f) = req.filter.as_ref() {
        crate::filter::validate_filter(f)?;
    }
    Ok(regions)
}

fn fused_has_facets(req: &Bm25QueryRequest) -> bool {
    !req.facet_fields.is_empty()
        || !req.map_facet_fields.is_empty()
        || !req.range_facet_fields.is_empty()
}

fn unknown_fused_response(req: &Bm25QueryRequest) -> Bm25QueryResponse {
    Bm25QueryResponse {
        facets: req
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
            .collect(),
        range_facets: unknown_range_counts(&req.range_facet_fields),
        ..Default::default()
    }
}
