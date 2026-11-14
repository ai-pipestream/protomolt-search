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
        self.fanout_bm25_context(LexicalFanout {
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
            ..LexicalFanout::default()
        })
        .await
    }

    pub(super) async fn fanout_bm25_aggregated(
        &self,
        query: LexicalFanout<'_>,
        expansions: LexicalExpansions<'_>,
    ) -> Result<AggregatedHits, Status> {
        let query = query.validate()?;
        let LexicalFanout {
            text,
            pre_analyzed_terms,
            k,
            spec,
            facet_fields,
            map_facet_fields,
            range_facet_fields,
            stats_fields,
            cardinality_fields,
            projections,
            prefixes,
            synonyms,
            synonyms_off,
            ..
        } = query.input();
        let LexicalExpansions {
            prefixes: expansions,
            synonyms: synonym_expansions,
        } = expansions;
        let analysis_fingerprint = crate::analyzer::analysis_fingerprint(spec);
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
                .bm25_query_round(FlatRound {
                    query: query.with_k(k),
                    terms: &terms,
                    analysis_fingerprint,
                    global: &global,
                    claims: &claims,
                })
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
    pub async fn fanout_bm25_context(
        &self,
        query: LexicalFanout<'_>,
    ) -> Result<FacetedHits, Status> {
        if !query.fields.is_empty() {
            return Err(Status::invalid_argument(
                "flat lexical context cannot carry query fields; use the fused context",
            ));
        }
        if !query.stats_fields.is_empty() || !query.cardinality_fields.is_empty() {
            return Err(Status::invalid_argument(
                "faceted lexical context cannot return statistics or cardinality; use Bm25Search",
            ));
        }
        self.fanout_bm25_aggregated(
            query,
            LexicalExpansions {
                prefixes: &mut Vec::new(),
                synonyms: &mut Vec::new(),
            },
        )
        .await
        .map(|result| (result.0, result.1, result.2))
    }
}
