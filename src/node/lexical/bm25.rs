//! BM25 term/phrase queries, rescoring, highlighting, and dictionary scans.

use crate::node::*;

/// Identity of one BM25 score space. The canonical request keeps only
/// fields that can change a document's score; delivery depth, floors,
/// filters, projections, aggregations, and the per-shard epoch claim are
/// excluded so every shard in one globally scored round reports the same
/// value.
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

/// The explain breakdown of one scored document (docs/explain.md):
/// each present (field, term) pair's inputs and contribution, computed
/// with the scorers' own `idf` and `tf_norm` in the scorers' own
/// operand order, and the pre-stage sum recomposed from them in
/// accumulation order. `names[fi]` is the field name reported for
/// `fields[fi]`; `phrase` is the phrase leg's view index and parallel
/// term weights when the phrase scorer ran.
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

/// The score-stage rows of an explain breakdown: the chain replayed
/// stage by stage from the pre-stage sum with the same reads and the
/// same float expressions `ScoreChain::eval` uses.
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

/// Request-carried BM25 params: 0 selects the default (proto3 "absent").
///
/// Values are RANGE CHECKED, not just defaulted. `b` outside [0, 1]
/// breaks the monotonicity precondition the block-max bounds rest on
/// (see `postings::SkipRun`), so a bound can fall below a real score and
/// the pruned scorer silently drops hits the exhaustive one keeps. A NaN
/// is worse: BM25's `partial_cmp(..).unwrap_or(Equal)` degrades to
/// insertion order, which makes the ranking depend on shard layout. Both
/// arrive straight off the wire, so both are refused here rather than
/// discovered as an unreproducible ranking difference.
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

/// Validate a request's `HighlightSpec` against this shard's storage
/// (`docs/highlighting.md`): every named field must be the one with
/// stored text (the body), and sentence mode needs the body's kind-8
/// table. Both refuse by name before any hit is scored, so a shard with
/// no matching document refuses exactly like one with a thousand.
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

/// Cut one hit's snippets from the shard's stored text and sentence
/// table, around the occurrence spans the scorer already collected
/// (`(distinct term key, span)` pairs). No analyzer runs here.
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

// The synchronous BM25 scan bodies. The `NodeService` handlers wrap
// these in `spawn_blocking`: a postings walk is CPU-bound for as long
// as the highest-df term takes, and it must not occupy an async runtime
// worker -- the same discipline every vector scan path follows.

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

    /// [`Self::run_bm25_query`] with a mid-scan floor exchange for the
    /// streaming route. Only the flat pruned scorer consumes the hook;
    /// the fused multi-field route and the exhaustive fallbacks ignore
    /// it (no skip surface, so a mid-scan floor saves nothing there).
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
        // Fused multi-field legs replace the flat single-field query
        // (docs/multi-field.md).
        if !req.fields.is_empty() {
            if !req.score_stages.is_empty() {
                return Err(Status::invalid_argument(
                    "score stages are not yet supported on the fused multi-field route; \
                     drop `fields` to use the flat route, or drop `score_stages`",
                ));
            }
            return self.bm25_query_fused(&req, None, live, candidate);
        }
        let stage_specs = parse_score_stages(&req.score_stages)?;
        validate_range_facet_fields(&req.range_facet_fields)?;
        let geo_regions = validate_geo_filters(&req.geo_filters)?;
        if let Some(f) = req.filter.as_ref() {
            crate::filter::validate_filter(f)?;
        }
        let params = params_from(req.k1, req.b)?;
        let stats = bm25::CorpusStats {
            doc_count: req.global_doc_count,
            total_doc_length: req.global_total_doc_length,
            dfs: req.global_doc_frequencies.clone(),
        };
        if req.terms.len() != stats.dfs.len() {
            return Err(Status::invalid_argument(
                "terms and global_doc_frequencies must have the same length",
            ));
        }
        let guard = read_shard(&self.state);
        guard.check_stats_epoch(req.expected_stats_epoch, &req.expected_stats_incarnation)?;
        guard.check_query_analysis("body", req.analysis_fingerprint)?;
        // The request's filters, resolved ONCE against this shard's
        // tables and shared by facet counting and the scorers below —
        // one resolution, one truth (docs/geo-columns.md,
        // docs/cel-filters.md). `None` when the request has none, and
        // every path below is then bit-identical to its unfiltered
        // form.
        let doc_filter: Option<crate::filter::DocFilter<'_>> = match guard.bm25.as_ref() {
            Some(store)
                if !req.geo_filters.is_empty()
                    || req.filter.is_some()
                    || req.phrase.is_some()
                    || guard.live_docs.has_deletes() =>
            {
                // The flat route's phrase gate over the body
                // (docs/phrase-proximity.md); same contract as the fused
                // route's per-leg gate.
                let mut phrase = Vec::new();
                if let Some(constraint) = req.phrase.as_ref() {
                    let index = store.as_index().ok_or_else(|| {
                        Status::failed_precondition("bm25 bulk build in progress; Flush first")
                    })?;
                    if !index.has_positions() {
                        return Err(Status::failed_precondition(
                            "field \"body\" has no token positions on this shard; a phrase or \
                             slop query needs --position-fields=body and a rebuilt generation",
                        ));
                    }
                    crate::proximity::validate_leg(constraint, req.terms.len())
                        .map_err(Status::invalid_argument)?;
                    phrase.push(crate::filter::PhraseGate {
                        index,
                        terms: &req.terms,
                        sequence: constraint.sequence.iter().map(|&i| i as usize).collect(),
                        alternatives: constraint
                            .alternatives
                            .iter()
                            .map(|p| p.terms.iter().map(|&i| i as usize).collect())
                            .collect(),
                        slop: constraint.slop,
                    });
                }
                Some(crate::filter::DocFilter {
                    deleted: guard.live_docs.words(),
                    geo: store.resolve_geo_filters(&req.geo_filters, &geo_regions),
                    pred: req
                        .filter
                        .as_ref()
                        .map(|f| store.resolve_filter(f))
                        .transpose()?,
                    phrase,
                })
            }
            _ => None,
        };
        // Sealed segments the predicate rules out (docs/segment-pruning.md)
        // leave the postings walks below: the scorers and the facet
        // counts take the masked reader, whose per-document reads still
        // cover the whole shard.
        let prune = match guard.bm25.as_ref() {
            Some(store) => prune_segments(
                store,
                doc_filter.as_ref().and_then(|f| f.pred.as_ref()),
                self.knobs.segment_pruning(),
            ),
            None => SegmentPrune::default(),
        };
        let masked_index = match (guard.bm25.as_ref(), prune.mask.as_ref()) {
            (Some(Bm25Shard::Segmented(segmented)), Some(mask)) => {
                Some(segmented.masked(Arc::clone(mask)))
            }
            _ => None,
        };
        // Count-then-rank facets over the match set — term matches
        // that survive the filters — before any k/floor narrowing
        // (see count_facets). A shard with no lexical half has no
        // facet table: every requested field is legitimately unknown
        // here.
        let (facets, range_facets, column_stats, distinct) = match guard.bm25.as_ref() {
            Some(store)
                if !req.facet_fields.is_empty()
                    || !req.map_facet_fields.is_empty()
                    || !req.range_facet_fields.is_empty()
                    || !req.stats_fields.is_empty()
                    || !req.cardinality_fields.is_empty() =>
            {
                let index: &dyn Bm25Index = match masked_index.as_ref() {
                    Some(masked) => masked,
                    None => store.as_index().ok_or_else(|| {
                        Status::failed_precondition("bm25 bulk build in progress; Flush first")
                    })?,
                };
                let numeric_read = ShardNumericRead(store);
                let filter_ctx: bm25::FilterCtx = doc_filter
                    .as_ref()
                    .map(|f| (f, &numeric_read as &dyn crate::scorefn::NumericRead));
                store.count_facets(
                    &[(index, &req.terms)],
                    &req.facet_fields,
                    &req.map_facet_fields,
                    &req.range_facet_fields,
                    &req.stats_fields,
                    &req.cardinality_fields,
                    filter_ctx,
                )?
            }
            _ => (
                req.facet_fields
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
                unknown_range_counts(&req.range_facet_fields),
                req.stats_fields
                    .iter()
                    .map(|name| crate::pb::ColumnStats {
                        field: name.clone(),
                        known: false,
                        ..Default::default()
                    })
                    .collect(),
                req.cardinality_fields
                    .iter()
                    .map(|name| crate::pb::FacetDistinct {
                        field: name.clone(),
                        known: false,
                        values: Vec::new(),
                    })
                    .collect(),
            ),
        };
        // Which stage columns this shard's numeric table has —
        // computed regardless of k, like the facet known flags: a
        // shard lacking a column answers identity (exact), and the
        // coordinator refuses a column NO shard knows.
        let stage_columns_known: Vec<bool> = match guard.bm25.as_ref() {
            Some(Bm25Shard::Spilling(_)) if !stage_specs.is_empty() => {
                return Err(Status::failed_precondition(
                    "bm25 bulk build in progress; Flush first",
                ));
            }
            Some(store) => store
                .resolve_chain(&stage_specs)
                .stages
                .iter()
                .map(|stage| stage.column.is_some())
                .collect(),
            None => vec![false; stage_specs.len()],
        };
        // Which geo columns this shard has, computed regardless of k
        // for the same reason: a shard without the column contributes
        // no hits through the filter (exact — its documents hold no
        // locations), but a column NO shard knows must refuse.
        let geo_columns_known = match guard.bm25.as_ref() {
            Some(store) => store.geo_columns_known(&req.geo_filters),
            None => vec![false; req.geo_filters.len()],
        };
        // And the filter tree's per-leaf flags, same contract.
        let filter_columns_known = match (guard.bm25.as_ref(), req.filter.as_ref()) {
            (Some(store), Some(f)) => store.filter_columns_known(f),
            (None, Some(f)) => vec![false; crate::filter::leaf_count(f)],
            (_, None) => Vec::new(),
        };
        // Projection column-read leaves, same contract again: a leaf
        // this shard lacks reads absent (exact), a leaf NO shard knows
        // is a typo the coordinator refuses (docs/cel-values.md).
        let projection_leaves: Vec<crate::values::ValueLeaf> = {
            let mut leaves = Vec::new();
            for p in &req.projections {
                if let Some(expr) = p.expr.as_ref() {
                    crate::values::column_leaves(expr, &mut leaves);
                }
            }
            leaves
        };
        let projection_leaves_known: Vec<bool> = match guard.bm25.as_ref() {
            Some(store) => projection_leaves
                .iter()
                .map(|leaf| crate::values::leaf_known(leaf, store))
                .collect(),
            None => vec![false; projection_leaves.len()],
        };
        // Resolve independently of k and matches so empty shards still
        // participate in the distributed type agreement. Even a shard with
        // no lexical store validates literal expressions and malformed trees.
        let empty_columns = crate::values::NumericTypes {
            numerics: &[],
            integers: &[],
            unsigned_integers: &[],
            map_numerics: &[],
            map_integers: &[],
            map_unsigned_integers: &[],
            facets: &[],
        };
        let columns: &dyn crate::values::ColumnLookup = match guard.bm25.as_ref() {
            Some(store) => store,
            None => &empty_columns,
        };
        let (resolved_projections, projection_types): (Vec<_>, Vec<i32>) = req
            .projections
            .iter()
            .map(|p| {
                let expr = p.expr.as_ref().ok_or_else(|| {
                    Status::invalid_argument("projection: empty compiled expression")
                })?;
                crate::values::resolve(expr, columns)
                    .map(|(rv, ty)| (rv, crate::pb::ScalarValueType::from(ty) as i32))
            })
            .collect::<Result<Vec<_>, Status>>()?
            .into_iter()
            .unzip();
        let hits = match guard.bm25.as_ref() {
            Some(store) if req.k > 0 => {
                let index: &dyn Bm25Index = match masked_index.as_ref() {
                    Some(masked) => masked,
                    None => store.as_index().ok_or_else(|| {
                        Status::failed_precondition("bm25 bulk build in progress; Flush first")
                    })?,
                };
                // 0/absent means no supplied score floor (scores are positive).
                let floor = if req.min_score == 0.0 {
                    f64::NEG_INFINITY
                } else {
                    f64::from(req.min_score)
                };
                // The score-function chain, resolved against this
                // shard's numeric table (docs/score-functions.md).
                // With no stages the ctx is None and every scorer below
                // is bit-identical to its unchained form.
                let chain = store.resolve_chain(&stage_specs);
                let numeric_read = ShardNumericRead(store);
                let chain_ctx: bm25::ChainCtx = if stage_specs.is_empty() {
                    None
                } else {
                    Some((&chain, &numeric_read))
                };
                // The filters resolved above, paired with this arm's
                // read surface (docs/geo-columns.md,
                // docs/cel-filters.md).
                let filter_ctx: bm25::FilterCtx = doc_filter
                    .as_ref()
                    .map(|f| (f, &numeric_read as &dyn crate::scorefn::NumericRead));
                // Block-max path when every scored term has impacts (v5
                // shards) and the node flag allows it; the heap store,
                // v3/v4 files, and --block-max=false keep top_k with the
                // floor applied as a filter — same contract.
                let prunable = self.config.block_max
                    && req
                        .terms
                        .iter()
                        .enumerate()
                        // Local absence is not a missing impact surface:
                        // see top_k_pruned. Global df alone would forfeit
                        // pruning on every shard lacking a rare term.
                        .all(|(ti, t)| {
                            stats.dfs[ti] == 0 || index.df(t) == 0 || index.has_impacts(t)
                        });
                let docs = if prunable {
                    let mut prune = bm25::PruneStats::default();
                    bm25::top_k_pruned_chained_filtered_stats_streaming(
                        index,
                        &req.terms,
                        &stats,
                        params,
                        req.k as usize,
                        floor,
                        chain_ctx,
                        filter_ctx,
                        &mut prune,
                        live,
                        candidate,
                    )
                } else {
                    let docs = bm25::filter_to_floor(
                        bm25::top_k_chained_filtered(
                            index,
                            &req.terms,
                            &stats,
                            params,
                            req.k as usize,
                            chain_ctx,
                            filter_ctx,
                        ),
                        floor,
                    );
                    if let Some(sink) = candidate {
                        for doc in &docs {
                            sink(doc.doc_id, doc.score as f32);
                        }
                    }
                    docs
                };
                let highlight = highlight_plan(store, req.highlight.as_ref())?;
                let body = store.field_name(0);
                // The explain breakdown, only for the hits that survived
                // the top-k (docs/explain.md): the flat route is one
                // field at weight 1 in the fused scorer's terms.
                let flat_query = [bm25::FieldQuery {
                    index,
                    terms: &req.terms,
                    stats: stats.clone(),
                    params,
                    weight: 1.0,
                }];
                let presence = req.explain.then(|| {
                    let ids: Vec<u32> = docs.iter().map(|doc| doc.doc_id).collect();
                    bm25::breakdown(&flat_query, &ids)
                });
                docs.into_iter()
                    .enumerate()
                    .map(|(hit_index, doc)| -> Result<Bm25Hit, Status> {
                        let explain = presence.as_ref().map(|presence| {
                            let mut explain =
                                explain_terms(&flat_query, &[body], &presence[hit_index], None);
                            explain_stages(
                                &mut explain,
                                &chain,
                                &req.score_stages,
                                doc.doc_id,
                                &numeric_read,
                            );
                            explain
                        });
                        let snippets = match highlight.as_ref() {
                            Some(plan) => {
                                let occurrences: Vec<(usize, (u32, u32))> = doc
                                    .term_offsets
                                    .iter()
                                    .flat_map(|(ti, offsets)| {
                                        offsets.iter().map(move |&span| (*ti, span))
                                    })
                                    .collect();
                                cut_snippets(plan, index, body, doc.doc_id, &occurrences)?
                            }
                            None => Vec::new(),
                        };
                        Ok(Bm25Hit {
                            identity: store.document_identity(doc.doc_id),
                            explain,
                            snippets,
                            projected: resolved_projections
                                .iter()
                                .map(|rv| {
                                    projected_value(
                                        crate::values::eval(rv, doc.doc_id, &numeric_read),
                                        store,
                                    )
                                })
                                .collect(),
                            doc_id: self.config.slot_offset + u64::from(doc.doc_id),
                            score: doc.score as f32,
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
                    })
                    .collect::<Result<Vec<_>, Status>>()?
            }
            _ => Vec::new(),
        };
        // The shard's k-th best: one f32 ULP below the last hit's score
        // when the heap filled (so a later f32 seed never exceeds the
        // true k-th best — ties at the floor survive), 0 otherwise.
        let kth_best = if hits.len() == req.k as usize {
            hits.last()
                .map(|h| bm25::floor_seed(h.score))
                .unwrap_or(0.0)
        } else {
            0.0
        };
        Ok(Bm25QueryResponse {
            segments_total: prune.stats.segments_total,
            segments_skipped: prune.stats.segments_skipped,
            projection_leaves_known,
            projection_types,
            hits,
            kth_best,
            facets,
            stage_columns_known,
            stats: column_stats,
            distinct,
            range_facets,
            geo_columns_known,
            filter_columns_known,
        })
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
