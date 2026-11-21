//! Multi-field lexical execution with one fenced statistics retry.

use super::lexical_context::FusedLexicalQuery;
use super::lexical_fused_plan::FusedProbe;
use super::*;

impl CoordinatorServiceImpl {
    pub async fn fanout_bm25_fused(
        &self,
        text: &str,
        k: u32,
        fields: &[crate::pb::QueryField],
        min_score: f32,
    ) -> Result<Vec<Bm25Hit>, Status> {
        let request = Bm25SearchRequest {
            text: text.into(),
            fields: fields.to_vec(),
            min_score,
            ..Default::default()
        };
        self.fanout_bm25_fused_context(FusedLexicalQuery::new(&request, k, None)?)
            .await
            .map(|(hits, _, _)| hits)
    }

    pub(super) async fn fanout_bm25_fused_routed(
        &self,
        query: FusedLexicalQuery<'_>,
        expansions: LexicalExpansions<'_>,
    ) -> Result<
        (
            (FacetedHits, crate::segment_prune::PruneStats),
            Vec<crate::pb::PhraseRouting>,
        ),
        Status,
    > {
        let trace = std::env::var_os("PIPESTREAM_SEARCH_TRACE_BM25").is_some();
        let started = std::time::Instant::now();
        let analyzed = self.analyze_fused_query(&query, expansions).await?;
        let analyzed_time = started.elapsed();
        let input = query.input();
        if (query.options().k() == 0 || analyzed.is_empty())
            && input.facet_fields.is_empty()
            && input.map_facet_fields.is_empty()
            && input.range_facet_fields.is_empty()
        {
            if self.document_visibility.is_some() {
                self.body_stats(&[], false).await?;
            }
            return Ok((
                ((Vec::new(), Vec::new(), Vec::new()), Default::default()),
                Vec::new(),
            ));
        }
        let probe = FusedProbe::new(self, &query, analyzed);
        let mut fresh = false;
        loop {
            let globals = self
                .fused_stats_probing(probe.stats_fields(), fresh, probe.probe_from())
                .await?;
            let claims = globals.epochs.clone();
            let stats = started.elapsed();
            let resolved = probe.resolve(query.fields(), &globals, self.node_addrs.len())?;
            let result = self
                .bm25_fused_round(FusedRound {
                    query: query.options(),
                    fields: &resolved.fields,
                    field_terms: &resolved.terms,
                    globals: &resolved.globals,
                    claims: &claims,
                    phrase: None,
                    phrase_legs: &resolved.phrase_legs,
                    fingerprints: &resolved.fingerprints,
                    timing: RoundTiming {
                        trace,
                        started,
                        analyzed: analyzed_time,
                        stats,
                    },
                })
                .await;
            match result {
                Err(error) if !fresh && is_stale_stats(&error) => {
                    self.stats_cache.invalidate_all();
                    fresh = true;
                }
                other => return other.map(|hits| (hits, resolved.routing)),
            }
        }
    }

    pub async fn fanout_bm25_fused_context(
        &self,
        query: FusedLexicalQuery<'_>,
    ) -> Result<FacetedHits, Status> {
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
