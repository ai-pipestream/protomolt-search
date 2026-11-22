//! Flat lexical analysis, expansion, and statistics retry.

use super::lexical_context::FlatLexicalQuery;
use super::*;

impl CoordinatorServiceImpl {
    pub async fn fanout_bm25(
        &self,
        text: &str,
        k: u32,
        spec: Option<&crate::pb::AnalysisSpec>,
    ) -> Result<Vec<Bm25Hit>, Status> {
        self.fanout_bm25_seeded(text, k, spec, 0.0).await
    }

    pub async fn fanout_bm25_seeded(
        &self,
        text: &str,
        k: u32,
        spec: Option<&crate::pb::AnalysisSpec>,
        min_score: f32,
    ) -> Result<Vec<Bm25Hit>, Status> {
        let request = Bm25SearchRequest {
            text: text.into(),
            analysis: spec.cloned(),
            min_score,
            ..Default::default()
        };
        self.fanout_bm25_context(FlatLexicalQuery::new(&request, k, &[], None)?)
            .await
            .map(|(hits, _, _)| hits)
    }

    pub(super) async fn fanout_bm25_aggregated(
        &self,
        query: FlatLexicalQuery<'_>,
        expansions: LexicalExpansions<'_>,
    ) -> Result<AggregatedHits, Status> {
        self.fanout_bm25_aggregated_owned(query, expansions)
            .await
            .map(|(hits, _)| hits)
    }

    pub(super) async fn fanout_bm25_aggregated_owned(
        &self,
        query: FlatLexicalQuery<'_>,
        expansions: LexicalExpansions<'_>,
    ) -> Result<(AggregatedHits, HashMap<u64, usize>), Status> {
        let terms = self.flat_query_terms(query, expansions).await?;
        let k = if terms.is_empty() {
            0
        } else {
            query.options().k()
        };
        let query = query.with_k(k);
        if flat_round_is_empty(query) {
            if self.document_visibility.is_some() {
                self.body_stats(&[], false).await?;
            }
            return Ok((
                (
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    Vec::new(),
                    Default::default(),
                ),
                HashMap::new(),
            ));
        }
        let analysis_fingerprint =
            crate::analyzer::analysis_fingerprint(query.input().analysis.as_ref());
        let mut fresh = false;
        loop {
            let (global, claims) = self.body_stats(&terms, fresh).await?;
            let result = self
                .bm25_query_round(FlatRound {
                    query,
                    terms: &terms,
                    analysis_fingerprint,
                    global: &global,
                    claims: &claims,
                })
                .await;
            match result {
                Err(error) if !fresh && is_stale_stats(&error) => {
                    self.stats_cache.invalidate_all();
                    fresh = true;
                }
                other => return other,
            }
        }
    }

    async fn flat_query_terms(
        &self,
        query: FlatLexicalQuery<'_>,
        expansions: LexicalExpansions<'_>,
    ) -> Result<Vec<String>, Status> {
        let input = query.input();
        let spec = input.analysis.as_ref();
        let mut terms = Vec::new();
        if !input.pre_analyzed_terms.is_empty() {
            for term in &input.pre_analyzed_terms {
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
            let analyzed = analyze_query(&addr, &input.text, spec, &input.prefixes).await?;
            for (term, _, _) in analyzed.terms {
                if !terms.contains(&term) {
                    terms.push(term);
                }
            }
        }
        expansions.prefixes.extend(
            self.expand_prefixes("body", spec, &input.prefixes, &mut terms)
                .await?,
        );
        expansions.synonyms.extend(
            self.expand_synonyms(
                "body",
                spec,
                &input.synonyms,
                input.synonyms_off,
                &mut terms,
            )
            .await?,
        );
        Ok(terms)
    }

    pub async fn fanout_bm25_context(
        &self,
        query: FlatLexicalQuery<'_>,
    ) -> Result<FacetedHits, Status> {
        if !query.input().stats_fields.is_empty() || !query.input().cardinality_fields.is_empty() {
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

fn flat_round_is_empty(query: FlatLexicalQuery<'_>) -> bool {
    let input = query.input();
    query.options().k() == 0
        && query.projections().is_empty()
        && input.facet_fields.is_empty()
        && input.map_facet_fields.is_empty()
        && input.range_facet_fields.is_empty()
        && input.stats_fields.is_empty()
        && input.cardinality_fields.is_empty()
}
