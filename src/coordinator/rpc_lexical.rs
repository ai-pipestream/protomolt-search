//! Lexical RPC validation, route selection, and response disclosure.

use super::*;

impl CoordinatorServiceImpl {
    pub(super) async fn rpc_bm25_search(
        &self,
        request: Request<Bm25SearchRequest>,
    ) -> Result<Response<Bm25SearchResponse>, Status> {
        crate::metrics::timed(Route::Bm25Search, request, |request| async move {
            self.admit(&request.get_ref().collection)?;
            if let Some(snapshot) = self.request_snapshot() {
                return Box::pin(SearchService::bm25_search(
                    &snapshot,
                    crate::metrics::nested(request),
                ))
                .await;
            }
            let req = request.into_inner();
            let k = self.resolve_k(req.k)?;
            // A malformed HighlightSpec refuses here, before any shard is
            // asked, so an empty fleet answers the same as a full one
            // (docs/highlighting.md).
            if let Some(spec) = req.highlight.as_ref() {
                crate::highlight::Plan::from_spec(spec)?;
            }
            if req.min_score.is_nan() || req.min_score == f32::NEG_INFINITY {
                return Err(Status::invalid_argument(
                    "min_score must be finite (NaN and -inf are not valid floors)",
                ));
            }
            // CEL text compiles ONCE, here, into the predicate IR the
            // shards execute (docs/cel-filters.md): every shard sees the
            // same tree, and none ever sees CEL text.
            let user_filter = crate::cel::compile_filter(&req.filter)?;
            // Projection text compiles ONCE, here, into the ValueExpr IR
            // the shards resolve and evaluate (docs/cel-values.md).
            let projections = compile_projections(&req.projections)?;
            if let Some(fields) = &self.field_permissions {
                fields.bm25(&req, user_filter.as_ref(), &projections)?;
            }
            let filter = self.visible_filter(user_filter)?;
            let mut response = self
                .execute_bm25_rpc(&req, k, &projections, filter.as_ref())
                .await?;
            if let Some(fields) = &self.field_permissions {
                fields.disclose(&mut response)?;
            }
            Ok(Response::new(response))
        })
        .await
    }

    async fn execute_bm25_rpc(
        &self,
        req: &Bm25SearchRequest,
        k: u32,
        projections: &[crate::pb::CompiledProjection],
        filter: Option<&crate::pb::FilterExpr>,
    ) -> Result<Bm25SearchResponse, Status> {
        let mut prefix_expansions = Vec::new();
        let mut synonym_expansions = Vec::new();
        let expansions = LexicalExpansions {
            prefixes: &mut prefix_expansions,
            synonyms: &mut synonym_expansions,
        };
        let ((hits, facets, range_facets, stats, cardinality, prune), phrase_routing) = self
            .bm25_rpc_route(req, k, projections, filter, expansions)
            .await?;
        // The merged k-th best: one f32 ULP below the last hit's score
        // when k hits were returned (see `bm25::floor_seed` — a later
        // seed can never exceed the true k-th best), 0 otherwise.
        let kth_best = if hits.len() == k as usize {
            hits.last()
                .map(|h| crate::bm25::floor_seed(h.score))
                .unwrap_or(0.0)
        } else {
            0.0
        };
        let response = Bm25SearchResponse {
            field_details_redacted: false,
            execution_details_redacted: self.document_visibility.is_some(),
            segments_total: if self.document_visibility.is_some() {
                0
            } else {
                prune.segments_total
            },
            segments_skipped: if self.document_visibility.is_some() {
                0
            } else {
                prune.segments_skipped
            },
            hits,
            kth_best,
            facets,
            range_facets,
            stats,
            cardinality,
            phrase_routing,
            prefix_expansions,
            synonym_expansions,
        };
        Ok(response)
    }

    async fn bm25_rpc_route(
        &self,
        req: &Bm25SearchRequest,
        k: u32,
        projections: &[crate::pb::CompiledProjection],
        filter: Option<&crate::pb::FilterExpr>,
        expansions: LexicalExpansions<'_>,
    ) -> Result<(AggregatedHits, Vec<crate::pb::PhraseRouting>), Status> {
        if req.fields.is_empty() && req.phrase.is_none() {
            let query = FlatLexicalQuery::new(req, k, projections, filter)?;
            return Ok((
                self.fanout_bm25_aggregated(query, expansions).await?,
                Vec::new(),
            ));
        }
        let query = if req.fields.is_empty() {
            validate_bm25_phrase_route(req)?;
            FusedLexicalQuery::body_phrase(req, k, filter)?
        } else {
            validate_bm25_fused_route(req)?;
            FusedLexicalQuery::new(req, k, filter)?
        };
        let (((hits, facets, ranges), prune), routing) =
            self.fanout_bm25_fused_routed(query, expansions).await?;
        Ok((
            (hits, facets, ranges, Vec::new(), Vec::new(), prune),
            routing,
        ))
    }
}

fn validate_bm25_phrase_route(req: &Bm25SearchRequest) -> Result<(), Status> {
    if !req.score_stages.is_empty() {
        return Err(Status::invalid_argument(
            "score stages are not yet certified with a phrase constraint; drop `phrase` \
                     or drop `score_stages`",
        ));
    }
    if !req.stats_fields.is_empty() || !req.cardinality_fields.is_empty() {
        return Err(Status::invalid_argument(
            "stats/cardinality are not yet certified with a phrase constraint; drop \
                     `phrase` or drop the aggregations",
        ));
    }
    if !req.projections.is_empty() {
        return Err(Status::invalid_argument(
            "projections are not yet certified with a phrase constraint; drop `phrase` \
                     or drop the projections",
        ));
    }
    Ok(())
}

fn validate_bm25_fused_route(req: &Bm25SearchRequest) -> Result<(), Status> {
    if !req.score_stages.is_empty() {
        return Err(Status::invalid_argument(
            "score stages are not yet supported on the fused multi-field route; \
                     drop `fields` to use the flat route, or drop `score_stages`",
        ));
    }
    if !req.stats_fields.is_empty() || !req.cardinality_fields.is_empty() {
        return Err(Status::invalid_argument(
            "stats/cardinality are not yet supported on the fused multi-field \
                     route; drop `fields` to use the flat route, or drop the aggregations",
        ));
    }
    if !req.projections.is_empty() {
        return Err(Status::invalid_argument(
            "projections are not yet supported on the fused multi-field route; \
                     drop `fields` to use the flat route, or drop the projections",
        ));
    }
    // Fused fields carry their own analysis, phrase, and prefixes.
    if req.analysis.is_some() {
        return Err(Status::invalid_argument(
            "Bm25SearchRequest.analysis is ignored when `fields` is set (term identity \
                     is per field). Move the spec onto each QueryField.analysis, or drop \
                     `fields` to use the single-field route.",
        ));
    }
    if req.phrase.is_some() {
        return Err(Status::invalid_argument(
            "Bm25SearchRequest.phrase is the flat route's constraint; with `fields` set, \
                     put the PhraseMatch on the QueryField it constrains",
        ));
    }
    if !req.prefixes.is_empty() {
        return Err(Status::invalid_argument(
            "Bm25SearchRequest.prefixes expand in the body; with `fields` set, put \
                         the prefixes on the QueryField whose dictionary they expand in",
        ));
    }
    Ok(())
}
