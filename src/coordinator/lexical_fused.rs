//! Lexical fused.

use super::*;

impl CoordinatorServiceImpl {
    /// Fused multi-field Bm25Search (`docs/multi-field.md`): `text` is
    /// analyzed once per entry under THAT entry's analysis (term
    /// identity is per field), ONE TermStats round carries every
    /// field's terms, and the Bm25Query fan-out sends per-field legs in
    /// entry order — the pinned accumulation order, identical on every
    /// shard, so distributed fused scores match the monolith's bits.
    pub async fn fanout_bm25_fused(
        &self,
        text: &str,
        k: u32,
        fields: &[crate::pb::QueryField],
        min_score: f32,
    ) -> Result<Vec<Bm25Hit>, Status> {
        self.fanout_bm25_fused_faceted(text, k, fields, min_score, &[], &[], &[], &[], None)
            .await
            .map(|(hits, _, _)| hits)
    }

    /// [`Self::fanout_bm25_fused`] with count-then-rank facets; see
    /// [`Self::fanout_bm25_faceted`] for the facet contract (on a fused
    /// query the match set is the union over every leg's terms).
    #[allow(clippy::too_many_arguments)]
    pub async fn fanout_bm25_fused_faceted(
        &self,
        text: &str,
        k: u32,
        fields: &[crate::pb::QueryField],
        min_score: f32,
        facet_fields: &[String],
        map_facet_fields: &[crate::pb::MapFacetField],
        range_facet_fields: &[crate::pb::RangeFacetField],
        geo_filters: &[crate::pb::GeoFilter],
        filter: Option<&crate::pb::FilterExpr>,
    ) -> Result<FacetedHits, Status> {
        self.fanout_bm25_fused_context(LexicalFanout {
            text,
            k,
            fields,
            min_score,
            facet_fields,
            map_facet_fields,
            range_facet_fields,
            geo_filters,
            filter,
            ..LexicalFanout::default()
        })
        .await
    }

    /// [`Self::fanout_bm25_fused_faceted`] that also reports which
    /// payload served each field's PhraseMatch (docs/phrase-proximity.md).
    ///
    /// A field with a phrase is analyzed like any other, and its query's
    /// TOKEN ORDER is read off the positioned analysis. The fleet's
    /// capabilities then pick the route from the stats round: a two-term
    /// exact phrase whose bigram column every shard indexes becomes one
    /// term of that column; anything else rides the field's token
    /// positions as a shard-side gate, and only when every shard carries
    /// them. Neither being true refuses by name — the query is never
    /// narrowed to "all terms present" and adjacency is never guessed.
    pub(super) async fn fanout_bm25_fused_routed(
        &self,
        query: LexicalFanout<'_>,
        expansions: LexicalExpansions<'_>,
    ) -> Result<
        (
            (FacetedHits, crate::segment_prune::PruneStats),
            Vec<crate::pb::PhraseRouting>,
        ),
        Status,
    > {
        let query = query.validate()?;
        let LexicalFanout {
            text,
            k,
            fields,
            facet_fields,
            map_facet_fields,
            range_facet_fields,
            explain,
            ..
        } = query.input();
        let LexicalExpansions {
            prefixes: expansions,
            synonyms: synonym_expansions,
        } = expansions;
        // Phase timing, off unless PIPESTREAM_SEARCH_TRACE_BM25 is set.
        // route and the single-field route reach the same node scorer,
        // so when they disagree by orders of magnitude the question is
        // which phase, and that cannot be answered from outside.
        let trace = std::env::var_os("PIPESTREAM_SEARCH_TRACE_BM25").is_some()
            || std::env::var_os("TURBOVEC_TRACE_BM25").is_some();
        let t0 = std::time::Instant::now();
        let addr = self.analysis_addr.clone().ok_or_else(|| {
            Status::unavailable("no analysis backend configured on the coordinator (analysis_addr)")
        })?;
        let mut seen: Vec<&str> = Vec::new();
        for f in fields {
            if f.field.is_empty() {
                return Err(Status::invalid_argument("QueryField.field must be named"));
            }
            if seen.contains(&f.field.as_str()) {
                return Err(Status::invalid_argument(format!(
                    "field {:?} repeats in the query",
                    f.field
                )));
            }
            if f.weight < 0.0 || f.weight.is_nan() {
                return Err(Status::invalid_argument(format!(
                    "field {:?}: weight must be >= 0",
                    f.field
                )));
            }
            seen.push(&f.field);
        }
        // (a) Query analysis per field, each under its own spec. A
        // field with a phrase also needs the query's token ORDER, which
        // the positioned analysis carries (docs/phrase-proximity.md).
        let mut field_terms: Vec<Vec<String>> = Vec::with_capacity(fields.len());
        // Per field: the phrase's term sequence (indexes into that
        // field's terms) and slop, when the field carries a PhraseMatch.
        let mut phrase_requests: Vec<Option<(Vec<usize>, u32)>> = Vec::with_capacity(fields.len());
        let mut phrase_alternatives: Vec<Vec<Vec<usize>>> = Vec::with_capacity(fields.len());
        for f in fields {
            let analyzed = analyze_query(&addr, text, f.analysis.as_ref(), &f.prefixes).await?;
            // The analysis already lists each distinct term once, in
            // first-occurrence order; `remap` keeps the sequence honest
            // should a provider ever repeat one.
            let mut terms: Vec<String> = Vec::new();
            let mut remap: Vec<usize> = Vec::with_capacity(analyzed.terms.len());
            for (term, _, _) in &analyzed.terms {
                let at = match terms.iter().position(|t| t == term) {
                    Some(i) => i,
                    None => {
                        terms.push(term.clone());
                        terms.len() - 1
                    }
                };
                remap.push(at);
            }
            let mut alternatives = Vec::new();
            let phrase = match f.phrase.as_ref() {
                Some(m) => {
                    if terms.is_empty() {
                        return Err(Status::invalid_argument(format!(
                            "field {:?}: the phrase text analyzed to no terms; there is no \
                             window to match",
                            f.field
                        )));
                    }
                    let Some(_positions) = analyzed.positions.as_ref() else {
                        return Err(Status::failed_precondition(format!(
                            "field {:?}: the query analysis carried no token positions, so the \
                             phrase's token order cannot be established; the analysis backend \
                             must return its token layer",
                            f.field
                        )));
                    };
                    let paths = crate::proximity::query_paths(&analyzed).map_err(|error| {
                        Status::invalid_argument(format!("query positions: {error}"))
                    })?;
                    let mut paths = paths
                        .into_iter()
                        .map(|p| p.into_iter().map(|ti| remap[ti]).collect::<Vec<_>>());
                    let sequence = paths
                        .next()
                        .ok_or_else(|| Status::invalid_argument("empty query lattice"))?;
                    alternatives.extend(paths);
                    Some((sequence, m.slop))
                }
                None => None,
            };
            // Term prefixes join this field's terms after the phrase
            // sequence was taken, so the sequence's indexes stay put.
            expansions.extend(
                self.expand_prefixes(&f.field, f.analysis.as_ref(), &f.prefixes, &mut terms)
                    .await?,
            );
            synonym_expansions.extend(
                self.expand_synonyms(
                    &f.field,
                    f.analysis.as_ref(),
                    &f.synonyms,
                    f.synonyms_off,
                    &mut terms,
                )
                .await?,
            );
            field_terms.push(terms);
            phrase_requests.push(phrase);
            phrase_alternatives.push(alternatives);
        }
        let t_analyzed = t0.elapsed();
        if (k == 0 || field_terms.iter().all(|t| t.is_empty()))
            && facet_fields.is_empty()
            && map_facet_fields.is_empty()
            && range_facet_fields.is_empty()
        {
            if self.document_visibility.is_some() {
                self.body_stats(&[], false).await?;
            }
            return Ok((
                (
                    (Vec::new(), Vec::new(), Vec::new()),
                    crate::segment_prune::PruneStats::default(),
                ),
                Vec::new(),
            ));
        }
        // (b) every field's stats, served from the per-node cache, plus
        // one PROBE per two-term exact phrase for its bigram column: the
        // route is decided from what the fleet answers, and a column no
        // shard has is an answer, not a typo.
        let mut stats_fields: Vec<crate::pb::FieldTerms> = fields
            .iter()
            .zip(&field_terms)
            .map(|(f, terms)| crate::pb::FieldTerms {
                field: f.field.clone(),
                terms: terms.clone(),
            })
            .collect();
        let probe_from = stats_fields.len();
        let mut bigram_probe: Vec<Option<usize>> = vec![None; fields.len()];
        for (fi, f) in fields.iter().enumerate() {
            if let Some((sequence, 0)) = phrase_requests[fi].as_ref().map(|(s, slop)| (s, *slop)) {
                if sequence.len() == 2 && phrase_alternatives[fi].is_empty() {
                    let column = crate::proximity::bigram_field_name(&f.field);
                    if self.field_permissions.as_ref().is_some_and(|fields| {
                        !fields.can_use(&column) || (explain && !fields.can_disclose(&column))
                    }) {
                        continue;
                    }
                    let bigram = crate::proximity::bigram_term(
                        &field_terms[fi][sequence[0]],
                        &field_terms[fi][sequence[1]],
                    );
                    bigram_probe[fi] = Some(stats_fields.len());
                    stats_fields.push(crate::pb::FieldTerms {
                        field: crate::proximity::bigram_field_name(&f.field),
                        terms: vec![bigram],
                    });
                }
            }
        }
        // (c)+(d) run as a round so a stale-stats refusal can rerun
        // them once against fresh stats with a new fenced claim.
        let n_shards = self.node_addrs.len();
        let mut fresh = false;
        loop {
            let globals = self
                .fused_stats_probing(&stats_fields, fresh, probe_from)
                .await?;
            let claims = globals.epochs.clone();
            let t_stats = t0.elapsed();
            // Resolve every field's route against what the fleet
            // answered, producing the leg list the round scores.
            let mut resolved_fields: Vec<crate::pb::QueryField> = Vec::with_capacity(fields.len());
            let mut resolved_terms: Vec<Vec<String>> = Vec::with_capacity(fields.len());
            let mut resolved = FusedGlobals {
                doc_count: globals.doc_count,
                totals: Vec::with_capacity(fields.len()),
                dfs: Vec::with_capacity(fields.len()),
                epochs: globals.epochs.clone(),
                known_shards: Vec::with_capacity(fields.len()),
                positions_shards: Vec::with_capacity(fields.len()),
            };
            let mut phrase_legs: Vec<Option<crate::pb::PhraseLeg>> =
                Vec::with_capacity(fields.len());
            let mut fingerprints: Vec<u64> = Vec::with_capacity(fields.len());
            let mut routing: Vec<crate::pb::PhraseRouting> = Vec::new();
            for (fi, f) in fields.iter().enumerate() {
                let base_fingerprint = crate::analyzer::analysis_fingerprint(f.analysis.as_ref());
                let mut take = |source: usize| {
                    resolved.totals.push(globals.totals[source]);
                    resolved.dfs.push(globals.dfs[source].clone());
                    resolved.known_shards.push(globals.known_shards[source]);
                    resolved
                        .positions_shards
                        .push(globals.positions_shards[source]);
                };
                match phrase_requests[fi].as_ref() {
                    // A one-term "phrase" is the ordinary term query and
                    // constrains nothing, so it reports no routing.
                    Some((sequence, slop))
                        if sequence.len() >= 2 || !phrase_alternatives[fi].is_empty() =>
                    {
                        let bigram_everywhere =
                            bigram_probe[fi].is_some_and(|pi| globals.known_shards[pi] == n_shards);
                        let positions_everywhere = globals.known_shards[fi] == n_shards
                            && globals.positions_shards[fi] == n_shards;
                        if !phrase_alternatives[fi].is_empty() && !positions_everywhere {
                            return Err(Status::invalid_argument(format!(
                                "field {:?}: query lattice alternatives require token positions on every shard", f.field
                            )));
                        }
                        match crate::proximity::choose_route(
                            &f.field,
                            sequence.len(),
                            *slop,
                            bigram_everywhere,
                            positions_everywhere,
                        ) {
                            Ok(crate::proximity::PhraseRoute::BigramColumn(column)) => {
                                let pi = bigram_probe[fi].expect("bigram route implies a probe");
                                resolved_fields.push(crate::pb::QueryField {
                                    field: column.clone(),
                                    phrase: None,
                                    ..f.clone()
                                });
                                resolved_terms.push(stats_fields[pi].terms.clone());
                                take(pi);
                                phrase_legs.push(None);
                                fingerprints
                                    .push(crate::proximity::bigram_fingerprint(base_fingerprint));
                                routing.push(crate::pb::PhraseRouting {
                                    field: f.field.clone(),
                                    served_field: column,
                                    bigram_column: true,
                                    slop: 0,
                                });
                            }
                            Ok(crate::proximity::PhraseRoute::Positions) => {
                                resolved_fields.push(f.clone());
                                resolved_terms.push(field_terms[fi].clone());
                                take(fi);
                                phrase_legs.push(Some(crate::pb::PhraseLeg {
                                    sequence: sequence.iter().map(|&i| i as u32).collect(),
                                    slop: *slop,
                                    alternatives: phrase_alternatives[fi]
                                        .iter()
                                        .map(|path| crate::pb::PhraseSequence {
                                            terms: path.iter().map(|&t| t as u32).collect(),
                                        })
                                        .collect(),
                                }));
                                fingerprints.push(base_fingerprint);
                                routing.push(crate::pb::PhraseRouting {
                                    field: f.field.clone(),
                                    served_field: f.field.clone(),
                                    bigram_column: false,
                                    slop: *slop,
                                });
                            }
                            Err(reason) => return Err(Status::invalid_argument(reason)),
                        }
                    }
                    _ => {
                        resolved_fields.push(crate::pb::QueryField {
                            phrase: None,
                            ..f.clone()
                        });
                        resolved_terms.push(field_terms[fi].clone());
                        take(fi);
                        phrase_legs.push(None);
                        fingerprints.push(base_fingerprint);
                    }
                }
            }
            match self
                .bm25_fused_round(FusedRound {
                    query,
                    fields: &resolved_fields,
                    field_terms: &resolved_terms,
                    globals: &resolved,
                    claims: &claims,
                    phrase: None,
                    phrase_legs: &phrase_legs,
                    fingerprints: &fingerprints,
                    timing: RoundTiming {
                        trace,
                        started: t0,
                        analyzed: t_analyzed,
                        stats: t_stats,
                    },
                })
                .await
            {
                Err(e) if !fresh && is_stale_stats(&e) => {
                    self.stats_cache.invalidate_all();
                    fresh = true;
                }
                other => return other.map(|hits| (hits, routing)),
            }
        }
    }
}

impl CoordinatorServiceImpl {
    pub async fn fanout_bm25_fused_context(
        &self,
        query: LexicalFanout<'_>,
    ) -> Result<FacetedHits, Status> {
        if query.spec.is_some()
            || !query.pre_analyzed_terms.is_empty()
            || !query.prefixes.is_empty()
            || !query.synonyms.is_empty()
            || query.synonyms_off
            || !query.score_stages.is_empty()
            || !query.projections.is_empty()
            || !query.stats_fields.is_empty()
            || !query.cardinality_fields.is_empty()
        {
            return Err(Status::invalid_argument("fused lexical context cannot carry flat-only analysis, expansions, score stages, projections, or aggregations; put analysis and expansions on each query field"));
        }
        self.fanout_bm25_fused_routed(
            query,
            LexicalExpansions {
                prefixes: &mut Vec::new(),
                synonyms: &mut Vec::new(),
            },
        )
        .await
        .map(|((hits, _), _)| hits)
    }
}
