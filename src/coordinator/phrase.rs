//! Phrase.

use super::*;

impl CoordinatorServiceImpl {
    /// Phrase-aware BM25 orchestration. Ordinary fields are analyzed exactly
    /// as on the fused route; the final field is populated directly from the
    /// product glossary and scored as a max-group on every shard.
    pub(super) async fn fanout_phrase(
        &self,
        base: &crate::pb::Bm25SearchRequest,
        k: u32,
        weight_per_token: f32,
        max_weight: f32,
        filter: Option<&crate::pb::FilterExpr>,
    ) -> Result<FacetedHits, Status> {
        crate::node::validate_range_facet_fields(&base.range_facet_fields)?;
        crate::node::validate_geo_filters(&base.geo_filters)?;
        if let Some(filter) = filter {
            crate::filter::validate_filter(filter)?;
        }
        let phrase_index = self.phrase_index.as_ref().ok_or_else(|| {
            Status::failed_precondition("this coordinator has no phrase glossary configured")
        })?;
        if !weight_per_token.is_finite() || weight_per_token <= 0.0 {
            return Err(Status::invalid_argument(
                "phrase weight_per_token must be finite and greater than zero",
            ));
        }
        if !max_weight.is_finite() || max_weight <= 0.0 {
            return Err(Status::invalid_argument(
                "phrase max_weight must be finite and greater than zero",
            ));
        }
        let mut fields = if base.fields.is_empty() {
            vec![crate::pb::QueryField {
                field: "body".to_string(),
                analysis: base.analysis.clone(),
                weight: 1.0,
                k1: 0.0,
                b: 0.0,
                phrase: None,
                prefixes: Vec::new(),
                synonyms: Vec::new(),
                synonyms_off: false,
            }]
        } else {
            if base.analysis.is_some() {
                return Err(Status::invalid_argument(
                    "PhraseSearch base.analysis is ignored when base.fields is set; move the spec onto each QueryField.analysis",
                ));
            }
            base.fields.clone()
        };
        let mut seen = std::collections::HashSet::new();
        for field in &fields {
            if field.field.is_empty() || !seen.insert(field.field.as_str()) {
                return Err(Status::invalid_argument(
                    "phrase search base fields must be named and unique",
                ));
            }
            if field.phrase.is_some() || !field.prefixes.is_empty() {
                return Err(Status::invalid_argument(format!(
                    "field {:?}: PhraseSearch scores glossary concepts; PhraseMatch and \
                     TermPrefix constraints are served by Bm25Search",
                    field.field
                )));
            }
            if field.field == phrase_index.phrase_field() {
                return Err(Status::invalid_argument(format!(
                    "phrase field {:?} is derived by PhraseSearch and must not appear in base.fields",
                    phrase_index.phrase_field()
                )));
            }
            if field.weight < 0.0 || field.weight.is_nan() {
                return Err(Status::invalid_argument(format!(
                    "field {:?}: weight must be non-negative",
                    field.field
                )));
            }
        }
        let addr = self.analysis_addr.clone().ok_or_else(|| {
            Status::unavailable("no analysis backend configured on the coordinator (analysis_addr)")
        })?;
        let trace = std::env::var_os("PIPESTREAM_SEARCH_TRACE_BM25").is_some()
            || std::env::var_os("TURBOVEC_TRACE_BM25").is_some();
        let t0 = std::time::Instant::now();
        let mut field_terms = Vec::with_capacity(fields.len() + 1);
        for field in &fields {
            let analyzed =
                crate::analyzer::analyze_document(&addr, &base.text, field.analysis.as_ref())
                    .await?;
            let mut terms = Vec::new();
            for (term, _, _) in analyzed.into_body().terms {
                if !terms.contains(&term) {
                    terms.push(term);
                }
            }
            field_terms.push(terms);
        }
        let phrase_pairs = phrase_index.query_terms(&base.text);
        let phrase_terms: Vec<String> = phrase_pairs.iter().map(|(term, _)| term.clone()).collect();
        let phrase_weights: Vec<f32> = phrase_pairs
            .iter()
            .map(|(_, token_count)| ((*token_count as f32) * weight_per_token).min(max_weight))
            .collect();
        let phrase_leg = fields.len();
        fields.push(crate::pb::QueryField {
            field: phrase_index.phrase_field().to_string(),
            analysis: None,
            weight: 1.0,
            k1: 0.0,
            b: 0.0,
            phrase: None,
            prefixes: Vec::new(),
            synonyms: Vec::new(),
            synonyms_off: false,
        });
        field_terms.push(phrase_terms);
        if (k == 0 || field_terms.iter().all(Vec::is_empty))
            && base.facet_fields.is_empty()
            && base.map_facet_fields.is_empty()
            && base.range_facet_fields.is_empty()
        {
            return Ok((Vec::new(), Vec::new(), Vec::new()));
        }
        let t_analyzed = t0.elapsed();
        let stats_fields: Vec<crate::pb::FieldTerms> = fields
            .iter()
            .zip(&field_terms)
            .map(|(field, terms)| crate::pb::FieldTerms {
                field: field.field.clone(),
                terms: terms.clone(),
            })
            .collect();
        let mut fresh = false;
        loop {
            let globals = self.fused_stats(&stats_fields, fresh).await?;
            if globals.known_shards[phrase_leg] != self.node_addrs.len() {
                return Err(Status::failed_precondition(format!(
                    "phrase field {:?} exists on only {}/{} shards; phrase search requires a complete rebuilt generation",
                    phrase_index.phrase_field(),
                    globals.known_shards[phrase_leg],
                    self.node_addrs.len()
                )));
            }
            let claims = globals.epochs.clone();
            let t_stats = t0.elapsed();
            let fingerprints: Vec<u64> = fields
                .iter()
                .enumerate()
                .map(|(fi, f)| {
                    if fi == phrase_leg {
                        phrase_index.fingerprint()
                    } else {
                        crate::analyzer::analysis_fingerprint(f.analysis.as_ref())
                    }
                })
                .collect();
            let phrase_legs: Vec<Option<crate::pb::PhraseLeg>> = vec![None; fields.len()];
            let round = self
                .bm25_fused_round(
                    k,
                    &fields,
                    &field_terms,
                    &globals,
                    &claims,
                    Some((phrase_leg, &phrase_weights, phrase_index.fingerprint())),
                    &phrase_legs,
                    &fingerprints,
                    base.min_score,
                    &base.facet_fields,
                    &base.map_facet_fields,
                    &base.range_facet_fields,
                    &base.geo_filters,
                    filter,
                    None,
                    trace,
                    t0,
                    t_analyzed,
                    t_stats,
                    base.explain,
                )
                .await;
            match round {
                Err(error) if !fresh && is_stale_stats(&error) => {
                    self.stats_cache.invalidate_all();
                    fresh = true;
                }
                other => return other.map(|(hits, _)| hits),
            }
        }
    }
}
