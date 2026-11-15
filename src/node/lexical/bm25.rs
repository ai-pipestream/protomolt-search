//! BM25 term/phrase queries, rescoring, highlighting, and dictionary scans.

use crate::node::*;
use crate::scorefn::NumericRead;

/// Identify the score space, excluding delivery, filters, projections, and epoch claims.
pub(in crate::node) fn bm25_scoring_fingerprint(req: &Bm25QueryRequest) -> String {
    let mut canonical = req.clone();
    canonical.k = 0;
    canonical.min_score = 0.0;
    canonical.expected_stats_epoch = 0;
    canonical.expected_stats_incarnation.clear();
    canonical.facet_fields.clear();
    canonical.map_facet_fields.clear();
    canonical.range_facet_fields.clear();
    canonical.geo_filters.clear();
    canonical.filter = None;
    canonical.stats_fields.clear();
    canonical.cardinality_fields.clear();
    canonical.projections.clear();
    crate::sha256::hex_digest(&prost::Message::encode_to_vec(&canonical))
}

/// Explain term contributions in scoring order, with optional phrase weights.
pub(in crate::node) fn explain_terms(
    fields: &[bm25::FieldQuery<'_>],
    names: &[&str],
    presence: &[bm25::TermPresence],
    phrase: Option<(usize, &[f64])>,
) -> crate::pb::Bm25Explain {
    let mut base = 0.0f64;
    let mut phrase_max = 0.0f64;
    let mut terms = Vec::with_capacity(presence.len());
    for hit in presence {
        let fq = &fields[hit.field];
        let avgdl = fq.stats.avgdl();
        let idf = bm25::idf(fq.stats.doc_count, fq.stats.dfs[hit.term]);
        let tf_norm = bm25::tf_norm(fq.params, hit.tf, hit.doc_len, avgdl);
        let mut contribution = fq.weight * idf * tf_norm;
        let mut phrase_group = false;
        let mut phrase_weight = 1.0;
        match phrase {
            Some((view, weights)) if view == hit.field => {
                phrase_group = true;
                phrase_weight = weights[hit.term];
                contribution *= phrase_weight;
                phrase_max = phrase_max.max(contribution);
            }
            _ => base += contribution,
        }
        terms.push(crate::pb::Bm25TermExplain {
            field: names[hit.field].to_string(),
            term: fq.terms[hit.term].clone(),
            tf: hit.tf,
            doc_length: hit.doc_len,
            avgdl,
            k1: fq.params.k1,
            b: fq.params.b,
            tf_norm,
            doc_count: fq.stats.doc_count,
            df: fq.stats.dfs[hit.term],
            idf,
            weight: fq.weight,
            contribution,
            phrase_group,
            phrase_weight,
        });
    }
    crate::pb::Bm25Explain {
        terms,
        bm25: base + phrase_max,
        stages: Vec::new(),
        phrase: phrase.is_some(),
        phrase_max,
    }
}

/// Replay the score chain using the scorer's column reads and arithmetic.
fn explain_stages(
    explain: &mut crate::pb::Bm25Explain,
    chain: &crate::scorefn::ScoreChain,
    specs: &[crate::pb::ScoreStage],
    doc_id: u32,
    columns: &dyn crate::scorefn::NumericRead,
) {
    let mut score = explain.bm25;
    for (i, stage) in chain.stages.iter().enumerate() {
        let spec = &specs[i];
        let mut row = crate::pb::ScoreStageExplain {
            stage: i as u32,
            column: spec.column.clone(),
            key: spec.map_key().unwrap_or_default().to_owned(),
            map_key: matches!(
                spec.operation,
                Some(
                    crate::pb::score_stage::Operation::MapOp(_)
                        | crate::pb::score_stage::Operation::TypedMapOp(_)
                )
            )
            .then(|| spec.map_key().expect("explicit map selector").to_owned()),
            present: false,
            input: 0.0,
            contribution: 0.0,
            output: score,
        };
        if let (Some(input), Some(contribution)) = (
            stage.input(doc_id, columns),
            stage.contribution(doc_id, columns),
        ) {
            score = if stage.is_additive() {
                score + contribution
            } else {
                score * contribution
            };
            row.present = true;
            row.input = input;
            row.contribution = contribution;
            row.output = score;
        }
        explain.stages.push(row);
    }
}

impl NodeServiceImpl {
    /// Resolve the mandatory view and scan under the same shard read guard.
    /// Scoped counts describe live admitted documents, never physical rows.
    pub(in crate::node) fn scan_dictionary(
        &self,
        field: &str,
        prefix: &str,
        cap: usize,
        visibility: Option<&crate::pb::DocumentVisibility>,
    ) -> Result<crate::pb::SuggestTermsResponse, Status> {
        if prefix.is_empty() {
            return Err(Status::invalid_argument("a term prefix must be non-empty"));
        }
        let scope = crate::visibility::VisibilityScope::new(visibility)?;
        let filter = visibility.and_then(|view| view.filter.as_ref());
        let guard = read_shard(&self.state);
        if guard
            .bm25
            .as_ref()
            .is_some_and(|store| store.as_index().is_none())
        {
            return Err(Status::failed_precondition(
                "bm25 bulk build in progress; Flush first",
            ));
        }
        let (_, columns_known) = filter_known_flags(guard.bm25.as_ref(), &[], filter);
        let mut response = crate::pb::SuggestTermsResponse {
            visibility_fingerprint: scope.fingerprint().to_vec(),
            visibility_columns_known: columns_known,
            tombstoned_rows: if visibility.is_some() {
                0
            } else {
                guard.live_docs.deleted_count()
            },
            ..Default::default()
        };
        let Some(store) = guard.bm25.as_ref() else {
            return Ok(response);
        };
        let Some(fi) = store.field_index(field) else {
            return Ok(response);
        };
        let index = store.field_view(fi).expect("searchability checked above");
        let entries = if visibility.is_some() {
            let (_, allow, _) = resolve_shard_filters(
                Some(store),
                None,
                store.next_doc_id() as usize,
                &[],
                &[],
                filter,
                self.knobs.segment_pruning(),
                None,
            )?;
            let admitted = |doc: u32| {
                !guard.live_docs.is_deleted(doc as usize)
                    && allow.as_ref().is_some_and(|mask| mask.test(doc as usize))
            };
            index.suggest_visible_prefix(prefix, cap, &admitted)
        } else {
            index.suggest_prefix(prefix, cap)
        };
        response.known = true;
        match entries {
            Ok(entries) => {
                response.count = entries.len() as u64;
                response.entries = entries
                    .into_iter()
                    .map(|(term, df)| crate::pb::SuggestTermEntry {
                        term,
                        df: u64::from(df),
                    })
                    .collect();
            }
            Err(count) => response.count = count as u64,
        }
        Ok(response)
    }
}

/// Validate BM25 parameters required for sound bounds; zero selects the default.
pub(in crate::node) fn params_from(k1: f32, b: f32) -> Result<Bm25Params, Status> {
    if !k1.is_finite() || k1 < 0.0 {
        return Err(Status::invalid_argument(format!(
            "bm25 k1 must be finite and >= 0, got {k1}"
        )));
    }
    if !b.is_finite() || !(0.0..=1.0).contains(&b) {
        return Err(Status::invalid_argument(format!(
            "bm25 b must be finite and within [0, 1], got {b}"
        )));
    }
    Ok(Bm25Params {
        k1: if k1 == 0.0 {
            bm25::DEFAULT_K1
        } else {
            f64::from(k1)
        },
        b: if b == 0.0 {
            bm25::DEFAULT_B
        } else {
            f64::from(b)
        },
    })
}

/// Validate highlight fields and the sentence table required by sentence mode.
pub(in crate::node) fn highlight_plan(
    store: &Bm25Shard,
    spec: Option<&crate::pb::HighlightSpec>,
) -> Result<Option<crate::highlight::Plan>, Status> {
    let Some(spec) = spec else {
        return Ok(None);
    };
    let plan = crate::highlight::Plan::from_spec(spec)?;
    let body = store.field_name(0);
    for field in &plan.fields {
        if field != body {
            return Err(Status::invalid_argument(format!(
                "highlight field {field:?}: snippets are cut from stored text, and only the \
                 body's text ({body:?}) is stored on this shard"
            )));
        }
    }
    if plan.mode == crate::highlight::Mode::Sentence && !store.field_has_sentences(0) {
        return Err(Status::failed_precondition(format!(
            "field {body:?} on this shard stores no sentence spans, so sentence-mode snippets \
             cannot be cut; ingest with --sentence-fields={body}, or ask for \
             HIGHLIGHT_MODE_WINDOW, which cuts at whitespace without them"
        )));
    }
    Ok(Some(plan))
}

/// Cut snippets from stored text and scored offsets without rerunning analysis.
pub(in crate::node) fn cut_snippets(
    plan: &crate::highlight::Plan,
    index: &dyn Bm25Index,
    field: &str,
    local: u32,
    occurrences: &[(usize, (u32, u32))],
) -> Result<Vec<crate::pb::Snippet>, Status> {
    if occurrences.is_empty() {
        return Ok(Vec::new());
    }
    let text = index.text(local).ok_or_else(|| {
        Status::internal(format!(
            "highlight: hit {local} has no stored text to cut snippets from"
        ))
    })?;
    let sentences = match plan.mode {
        crate::highlight::Mode::Sentence => index.doc_sentences(local),
        crate::highlight::Mode::Window => None,
    };
    let cut = crate::highlight::snippets(
        &text,
        sentences.as_deref(),
        occurrences,
        plan.mode,
        plan.max_snippets,
        plan.max_chars,
    )?;
    Ok(cut
        .into_iter()
        .map(|s| crate::pb::Snippet {
            field: field.to_string(),
            text: s.text,
            start: s.start,
            end: s.end,
            highlights: s
                .highlights
                .into_iter()
                .map(|(start, end)| OffsetSpan { start, end })
                .collect(),
            cut: crate::highlight::Plan::wire_cut(s.cut),
            sentence_index: s.sentence_index.map(|i| i as u32),
        })
        .collect())
}

impl NodeServiceImpl {
    pub(in crate::node) fn run_bm25_query(
        &self,
        req: Bm25QueryRequest,
    ) -> Result<Bm25QueryResponse, Status> {
        self.run_bm25_query_live(req, None, None)
    }

    pub(in crate::node) fn run_bm25_phrase_query(
        &self,
        request: crate::pb::Bm25PhraseQueryRequest,
    ) -> Result<Bm25QueryResponse, Status> {
        let req = request
            .query
            .ok_or_else(|| Status::invalid_argument("phrase query is missing its BM25 query"))?;
        let phrase_leg = request.phrase_leg as usize;
        let Some(leg) = req.fields.get(phrase_leg) else {
            return Err(Status::invalid_argument(format!(
                "phrase_leg {} is outside {} BM25 fields",
                request.phrase_leg,
                req.fields.len()
            )));
        };
        if request.phrase_term_weights.len() != leg.terms.len() {
            return Err(Status::invalid_argument(
                "phrase_term_weights must be parallel to the selected phrase leg's terms",
            ));
        }
        if request
            .phrase_term_weights
            .iter()
            .any(|weight| !weight.is_finite() || *weight < 0.0)
        {
            return Err(Status::invalid_argument(
                "phrase term weights must be finite and non-negative",
            ));
        }
        let phrases = self.phrase_index.as_ref().ok_or_else(|| {
            Status::failed_precondition("this node has no phrase glossary configured")
        })?;
        if leg.field != phrases.phrase_field() {
            return Err(Status::invalid_argument(format!(
                "phrase leg names {:?}, but this node's phrase field is {:?}",
                leg.field,
                phrases.phrase_field()
            )));
        }
        if leg.analysis_fingerprint != phrases.fingerprint() {
            return Err(Status::failed_precondition(format!(
                "phrase query fingerprint {:016x} differs from configured vocabulary {:016x}",
                leg.analysis_fingerprint,
                phrases.fingerprint()
            )));
        }
        self.bm25_query_fused(
            &req,
            Some((phrase_leg, &request.phrase_term_weights)),
            None,
            None,
        )
    }

    /// Execute lexical scoring with optional live floors and compact candidates.
    pub(in crate::node) fn run_bm25_query_live(
        &self,
        req: Bm25QueryRequest,
        live: Option<bm25::LiveFloorHook>,
        candidate: Option<bm25::CandidateHook>,
    ) -> Result<Bm25QueryResponse, Status> {
        if req.min_score.is_nan() || req.min_score == f32::NEG_INFINITY {
            return Err(Status::invalid_argument(
                "min_score must be finite (NaN and -inf are not valid floors)",
            ));
        }
        if !req.fields.is_empty() {
            if !req.score_stages.is_empty() {
                return Err(Status::invalid_argument(
                    "score stages are not yet supported on the fused multi-field route; \
                     drop `fields` to use the flat route, or drop `score_stages`",
                ));
            }
            return self.bm25_query_fused(&req, None, live, candidate);
        }
        FlatRun::new(self, &req)?.execute(live, candidate)
    }

    pub(in crate::node) fn run_bm25_rescore(
        &self,
        req: Bm25RescoreRequest,
    ) -> Result<Bm25RescoreResponse, Status> {
        if req.terms.len() != req.global_doc_frequencies.len() {
            return Err(Status::invalid_argument(
                "terms and global_doc_frequencies must have the same length",
            ));
        }
        let stage_specs = parse_score_stages(&req.score_stages)?;
        let params = params_from(req.k1, req.b)?;
        let scope = crate::visibility::VisibilityScope::new(req.visibility.as_ref())?;
        let stats = bm25::CorpusStats {
            doc_count: req.global_doc_count,
            total_doc_length: req.global_total_doc_length,
            dfs: req.global_doc_frequencies.clone(),
        };
        let offset = self.config.slot_offset;
        let guard = read_shard(&self.state);
        guard.check_stats_epoch(req.expected_stats_epoch, &req.expected_stats_incarnation)?;
        guard.check_query_analysis("body", req.analysis_fingerprint)?;
        let (_, visibility_columns_known) = filter_known_flags(
            guard.bm25.as_ref(),
            &[],
            req.visibility
                .as_ref()
                .and_then(|view| view.filter.as_ref()),
        );
        let (hits, stage_columns_known) = match guard.bm25.as_ref() {
            Some(store) => {
                // Route global ids to this shard's local range.
                let local: Vec<u32> = guard
                    .visible_candidate_slots(
                        req.visibility.as_ref(),
                        &req.candidate_ids,
                        offset,
                        store.next_doc_id() as usize,
                    )?
                    .into_iter()
                    .map(|slot| slot as u32)
                    .collect();
                let index = store.as_index().ok_or_else(|| {
                    Status::failed_precondition("bm25 bulk build in progress; Flush first")
                })?;
                let chain = store.resolve_chain(&stage_specs);
                let stage_columns_known = chain
                    .stages
                    .iter()
                    .map(|stage| stage.column.is_some())
                    .collect();
                let numeric_read = ShardNumericRead(store);
                let hits = bm25::score_candidates(index, &req.terms, &stats, params, &local)
                    .into_iter()
                    .map(|doc| Bm25Hit {
                        identity: store.document_identity(doc.doc_id),
                        explain: None,
                        snippets: Vec::new(),
                        projected: Vec::new(),
                        doc_id: offset + u64::from(doc.doc_id),
                        score: chain.eval(doc.score, doc.doc_id, &numeric_read) as f32,
                        terms: doc
                            .term_offsets
                            .into_iter()
                            .map(|(ti, offsets)| TermOccurrences {
                                term: req.terms[ti].clone(),
                                offsets: offsets
                                    .into_iter()
                                    .map(|(start, end)| OffsetSpan { start, end })
                                    .collect(),
                                field: String::new(),
                            })
                            .collect(),
                    })
                    .collect::<Vec<_>>();
                (hits, stage_columns_known)
            }
            None => (Vec::new(), vec![false; stage_specs.len()]),
        };
        Ok(Bm25RescoreResponse {
            hits,
            stage_columns_known,
            stats_epoch: guard.stats_epoch,
            stats_incarnation: guard.stats_incarnation.bytes()?,
            visibility_fingerprint: scope.fingerprint().to_vec(),
            visibility_columns_known,
        })
    }
}

const BULK_BUILD: &str = "bm25 bulk build in progress; Flush first";

struct FlatRun<'a> {
    node: &'a NodeServiceImpl,
    req: &'a Bm25QueryRequest,
    stages: Vec<ScoreStageSpec>,
    regions: Vec<crate::geo::GeoRegion>,
    params: Bm25Params,
    stats: bm25::CorpusStats,
}

impl<'a> FlatRun<'a> {
    fn new(node: &'a NodeServiceImpl, request: &'a Bm25QueryRequest) -> Result<Self, Status> {
        let stages = parse_score_stages(&request.score_stages)?;
        validate_range_facet_fields(&request.range_facet_fields)?;
        let regions = validate_geo_filters(&request.geo_filters)?;
        if let Some(filter) = request.filter.as_ref() {
            crate::filter::validate_filter(filter)?;
        }
        let params = params_from(request.k1, request.b)?;
        let stats = bm25::CorpusStats {
            doc_count: request.global_doc_count,
            total_doc_length: request.global_total_doc_length,
            dfs: request.global_doc_frequencies.clone(),
        };
        if request.terms.len() != stats.dfs.len() {
            return Err(Status::invalid_argument(
                "terms and global_doc_frequencies must have the same length",
            ));
        }
        Ok(Self {
            node,
            req: request,
            stages,
            regions,
            params,
            stats,
        })
    }

    fn execute(
        self,
        live: Option<bm25::LiveFloorHook>,
        candidate: Option<bm25::CandidateHook>,
    ) -> Result<Bm25QueryResponse, Status> {
        let guard = read_shard(&self.node.state);
        guard.check_stats_epoch(
            self.req.expected_stats_epoch,
            &self.req.expected_stats_incarnation,
        )?;
        guard.check_query_analysis("body", self.req.analysis_fingerprint)?;
        let filter = self.filter(&guard)?;
        let store = guard.bm25.as_ref();
        let prune = store.map_or_else(SegmentPrune::default, |store| {
            prune_segments(
                store,
                filter.as_ref().and_then(|f| f.pred.as_ref()),
                self.node.knobs.segment_pruning(),
            )
        });
        let masked = match (store, prune.mask.as_ref()) {
            (Some(Bm25Shard::Segmented(segmented)), Some(mask)) => {
                Some(segmented.masked(Arc::clone(mask)))
            }
            _ => None,
        };
        let masked = masked.as_ref().map(|index| index as &dyn Bm25Index);
        let mut response = self.facets(store, masked, filter.as_ref())?;
        self.known_columns(store, &mut response)?;
        let projections = self.projections(store, &mut response)?;
        response.hits = match store {
            Some(store) if self.req.k > 0 => self.hits(
                FlatView {
                    store,
                    index: flat_index(store, masked)?,
                    filter: filter.as_ref(),
                    projections: &projections,
                },
                live,
                candidate,
            )?,
            _ => Vec::new(),
        };
        response.kth_best = if response.hits.len() == self.req.k as usize {
            response
                .hits
                .last()
                .map(|hit| bm25::floor_seed(hit.score))
                .unwrap_or(0.0)
        } else {
            0.0
        };
        response.segments_total = prune.stats.segments_total;
        response.segments_skipped = prune.stats.segments_skipped;
        Ok(response)
    }

    fn filter<'b>(
        &'b self,
        guard: &'b ShardState,
    ) -> Result<Option<crate::filter::DocFilter<'b>>, Status> {
        let req = self.req;
        let Some(store) = guard.bm25.as_ref().filter(|_| {
            !req.geo_filters.is_empty()
                || req.filter.is_some()
                || req.phrase.is_some()
                || guard.live_docs.has_deletes()
        }) else {
            return Ok(None);
        };
        let phrase = self.phrases(store)?;
        Ok(Some(crate::filter::DocFilter {
            deleted: guard.live_docs.words(),
            geo: store.resolve_geo_filters(&req.geo_filters, &self.regions),
            pred: req
                .filter
                .as_ref()
                .map(|filter| store.resolve_filter(filter))
                .transpose()?,
            phrase,
        }))
    }

    fn phrases<'b>(
        &'b self,
        store: &'b Bm25Shard,
    ) -> Result<Vec<crate::filter::PhraseGate<'b>>, Status> {
        let Some(constraint) = self.req.phrase.as_ref() else {
            return Ok(Vec::new());
        };
        let index = flat_index(store, None)?;
        if !index.has_positions() {
            return Err(Status::failed_precondition(
                "field \"body\" has no token positions on this shard; a phrase or \
                 slop query needs --position-fields=body and a rebuilt generation",
            ));
        }
        crate::proximity::validate_leg(constraint, self.req.terms.len())
            .map_err(Status::invalid_argument)?;
        Ok(vec![crate::filter::PhraseGate {
            index,
            terms: &self.req.terms,
            sequence: constraint.sequence.iter().map(|&i| i as usize).collect(),
            alternatives: constraint
                .alternatives
                .iter()
                .map(|path| path.terms.iter().map(|&i| i as usize).collect())
                .collect(),
            slop: constraint.slop,
        }])
    }

    fn facets(
        &self,
        store: Option<&Bm25Shard>,
        masked: Option<&dyn Bm25Index>,
        filter: Option<&crate::filter::DocFilter<'_>>,
    ) -> Result<Bm25QueryResponse, Status> {
        let req = self.req;
        let Some(store) = store.filter(|_| {
            !req.facet_fields.is_empty()
                || !req.map_facet_fields.is_empty()
                || !req.range_facet_fields.is_empty()
                || !req.stats_fields.is_empty()
                || !req.cardinality_fields.is_empty()
        }) else {
            return Ok(self.empty_facets());
        };
        let columns = ShardNumericRead(store);
        let (facets, range_facets, stats, distinct) = store.count_facets(
            &[(flat_index(store, masked)?, &req.terms)],
            &req.facet_fields,
            &req.map_facet_fields,
            &req.range_facet_fields,
            &req.stats_fields,
            &req.cardinality_fields,
            filter.map(|filter| (filter, &columns as &dyn NumericRead)),
        )?;
        Ok(Bm25QueryResponse {
            facets,
            range_facets,
            stats,
            distinct,
            ..Default::default()
        })
    }

    fn empty_facets(&self) -> Bm25QueryResponse {
        let req = self.req;
        Bm25QueryResponse {
            facets: req
                .facet_fields
                .iter()
                .map(|name| (name.clone(), None))
                .chain(
                    req.map_facet_fields
                        .iter()
                        .map(|map| (map.column.clone(), Some(map.key.clone()))),
                )
                .map(|(field, key)| crate::pb::FacetFieldCounts {
                    field,
                    key: key.clone().unwrap_or_default(),
                    map_key: key,
                    ..Default::default()
                })
                .collect(),
            range_facets: unknown_range_counts(&req.range_facet_fields),
            stats: req
                .stats_fields
                .iter()
                .map(|field| crate::pb::ColumnStats {
                    field: field.clone(),
                    ..Default::default()
                })
                .collect(),
            distinct: req
                .cardinality_fields
                .iter()
                .map(|field| crate::pb::FacetDistinct {
                    field: field.clone(),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        }
    }

    fn known_columns(
        &self,
        store: Option<&Bm25Shard>,
        response: &mut Bm25QueryResponse,
    ) -> Result<(), Status> {
        response.stage_columns_known = match store {
            Some(Bm25Shard::Spilling(_)) if !self.stages.is_empty() => {
                return Err(Status::failed_precondition(BULK_BUILD));
            }
            Some(store) => store
                .resolve_chain(&self.stages)
                .stages
                .iter()
                .map(|stage| stage.column.is_some())
                .collect(),
            None => vec![false; self.stages.len()],
        };
        response.geo_columns_known = store.map_or_else(
            || vec![false; self.req.geo_filters.len()],
            |store| store.geo_columns_known(&self.req.geo_filters),
        );
        response.filter_columns_known = match (store, self.req.filter.as_ref()) {
            (Some(store), Some(filter)) => store.filter_columns_known(filter),
            (None, Some(filter)) => vec![false; crate::filter::leaf_count(filter)],
            (_, None) => Vec::new(),
        };
        let mut leaves = Vec::new();
        for projection in &self.req.projections {
            if let Some(expression) = projection.expr.as_ref() {
                crate::values::column_leaves(expression, &mut leaves);
            }
        }
        response.projection_leaves_known = store.map_or_else(
            || vec![false; leaves.len()],
            |store| {
                leaves
                    .iter()
                    .map(|leaf| crate::values::leaf_known(leaf, store))
                    .collect()
            },
        );
        Ok(())
    }

    fn projections(
        &self,
        store: Option<&Bm25Shard>,
        response: &mut Bm25QueryResponse,
    ) -> Result<Vec<crate::values::ResolvedValue>, Status> {
        let empty = crate::values::NumericTypes {
            numerics: &[],
            integers: &[],
            unsigned_integers: &[],
            map_numerics: &[],
            map_integers: &[],
            map_unsigned_integers: &[],
            facets: &[],
        };
        let columns: &dyn crate::values::ColumnLookup =
            store.map_or(&empty as &dyn crate::values::ColumnLookup, |store| store);
        let (projections, types) = self
            .req
            .projections
            .iter()
            .map(|projection| {
                let expression = projection.expr.as_ref().ok_or_else(|| {
                    Status::invalid_argument("projection: empty compiled expression")
                })?;
                crate::values::resolve(expression, columns)
                    .map(|(value, ty)| (value, crate::pb::ScalarValueType::from(ty) as i32))
            })
            .collect::<Result<Vec<_>, Status>>()?
            .into_iter()
            .unzip();
        response.projection_types = types;
        Ok(projections)
    }

    fn hits(
        &self,
        view: FlatView<'_>,
        live: Option<bm25::LiveFloorHook>,
        candidate: Option<bm25::CandidateHook>,
    ) -> Result<Vec<Bm25Hit>, Status> {
        let req = self.req;
        let chain = view.store.resolve_chain(&self.stages);
        let columns = ShardNumericRead(view.store);
        let filter = view.filter.map(|f| (f, &columns as &dyn NumericRead));
        let query = bm25::ScoringQuery {
            store: view.index,
            terms: &req.terms,
            stats: &self.stats,
            params: self.params,
            chain: (!self.stages.is_empty()).then_some((&chain, &columns)),
            filter,
        };
        let options = bm25::TopKOptions {
            k: req.k as usize,
            floor: if req.min_score == 0.0 {
                f64::NEG_INFINITY
            } else {
                f64::from(req.min_score)
            },
            block_max: self.node.config.block_max,
        };
        let docs = bm25::top_k_pruned_chained_filtered_stats_streaming(
            query,
            options,
            &mut bm25::PruneStats::default(),
            live,
            candidate,
        );
        FlatHits {
            req,
            slot_offset: self.node.config.slot_offset,
            store: view.store,
            chain,
            highlight: highlight_plan(view.store, req.highlight.as_ref())?,
            projections: view.projections,
            query: [bm25::FieldQuery {
                index: view.index,
                terms: &req.terms,
                stats: self.stats.clone(),
                params: self.params,
                weight: 1.0,
            }],
        }
        .collect(docs)
    }
}

fn flat_index<'a>(
    store: &'a Bm25Shard,
    masked: Option<&'a dyn Bm25Index>,
) -> Result<&'a dyn Bm25Index, Status> {
    masked
        .or_else(|| store.as_index())
        .ok_or_else(|| Status::failed_precondition(BULK_BUILD))
}

struct FlatView<'a> {
    store: &'a Bm25Shard,
    index: &'a dyn Bm25Index,
    filter: Option<&'a crate::filter::DocFilter<'a>>,
    projections: &'a [crate::values::ResolvedValue],
}

struct FlatHits<'a> {
    req: &'a Bm25QueryRequest,
    slot_offset: u64,
    store: &'a Bm25Shard,
    chain: crate::scorefn::ScoreChain,
    highlight: Option<crate::highlight::Plan>,
    projections: &'a [crate::values::ResolvedValue],
    query: [bm25::FieldQuery<'a>; 1],
}

impl FlatHits<'_> {
    fn collect(self, docs: Vec<bm25::ScoredDoc>) -> Result<Vec<Bm25Hit>, Status> {
        let presence = self.req.explain.then(|| {
            let ids: Vec<u32> = docs.iter().map(|doc| doc.doc_id).collect();
            bm25::breakdown(&self.query, &ids)
        });
        docs.into_iter()
            .enumerate()
            .map(|(index, doc)| {
                self.hit(
                    doc,
                    presence.as_ref().map(|presence| presence[index].as_slice()),
                )
            })
            .collect()
    }

    fn hit(
        &self,
        doc: bm25::ScoredDoc,
        presence: Option<&[bm25::TermPresence]>,
    ) -> Result<Bm25Hit, Status> {
        let body = self.store.field_name(0);
        let columns = ShardNumericRead(self.store);
        let explain = presence.map(|presence| {
            let mut explain = explain_terms(&self.query, &[body], presence, None);
            explain_stages(
                &mut explain,
                &self.chain,
                &self.req.score_stages,
                doc.doc_id,
                &columns,
            );
            explain
        });
        let snippets = match self.highlight.as_ref() {
            Some(plan) => {
                let occurrences: Vec<_> = doc
                    .term_offsets
                    .iter()
                    .flat_map(|(term, offsets)| offsets.iter().map(move |&span| (*term, span)))
                    .collect();
                cut_snippets(plan, self.query[0].index, body, doc.doc_id, &occurrences)?
            }
            None => Vec::new(),
        };
        Ok(Bm25Hit {
            identity: self.store.document_identity(doc.doc_id),
            explain,
            snippets,
            projected: self
                .projections
                .iter()
                .map(|value| {
                    projected_value(crate::values::eval(value, doc.doc_id, &columns), self.store)
                })
                .collect(),
            doc_id: self.slot_offset + u64::from(doc.doc_id),
            score: doc.score as f32,
            terms: doc
                .term_offsets
                .into_iter()
                .map(|(term, offsets)| TermOccurrences {
                    term: self.req.terms[term].clone(),
                    field: String::new(),
                    offsets: offsets
                        .into_iter()
                        .map(|(start, end)| OffsetSpan { start, end })
                        .collect(),
                })
                .collect(),
        })
    }
}
