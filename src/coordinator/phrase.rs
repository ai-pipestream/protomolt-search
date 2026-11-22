//! Glossary phrase analysis and fenced field-round execution.

use super::lexical_context::ValidatedLexicalQuery;
use super::*;

struct PhrasePlan {
    fields: Vec<crate::pb::QueryField>,
    terms: Vec<Vec<String>>,
    weights: Vec<f32>,
    leg: usize,
    stats_fields: Vec<crate::pb::FieldTerms>,
}

impl CoordinatorServiceImpl {
    pub(super) async fn fanout_phrase(
        &self,
        base: &Bm25SearchRequest,
        k: u32,
        weight_per_token: f32,
        max_weight: f32,
        filter: Option<&crate::pb::FilterExpr>,
    ) -> Result<FacetedHits, Status> {
        let query = ValidatedLexicalQuery::new(base, k, filter)?.without_highlight();
        let index = self.phrase_index.as_ref().ok_or_else(|| {
            Status::failed_precondition("this coordinator has no phrase glossary configured")
        })?;
        validate_phrase_weights(weight_per_token, max_weight)?;
        let fields = phrase_fields(base, index)?;
        let addr = self.analysis_addr.clone().ok_or_else(|| {
            Status::unavailable("no analysis backend configured on the coordinator (analysis_addr)")
        })?;
        let trace = std::env::var_os("PIPESTREAM_SEARCH_TRACE_BM25").is_some();
        let started = std::time::Instant::now();
        let plan =
            PhrasePlan::analyze(fields, base, index, &addr, weight_per_token, max_weight).await?;
        if (k == 0 || plan.terms.iter().all(Vec::is_empty))
            && base.facet_fields.is_empty()
            && base.map_facet_fields.is_empty()
            && base.range_facet_fields.is_empty()
        {
            return Ok((Vec::new(), Vec::new(), Vec::new()));
        }
        let timing = RoundTiming {
            trace,
            started,
            analyzed: started.elapsed(),
            stats: Default::default(),
        };
        self.phrase_rounds(query, plan, index, timing).await
    }

    async fn phrase_rounds(
        &self,
        query: ValidatedLexicalQuery<'_>,
        plan: PhrasePlan,
        index: &crate::phrases::PhraseIndex,
        timing: RoundTiming,
    ) -> Result<FacetedHits, Status> {
        let mut fresh = false;
        loop {
            let globals = self.fused_stats(&plan.stats_fields, fresh).await?;
            if globals.known_shards[plan.leg] != self.node_addrs.len() {
                return Err(Status::failed_precondition(format!(
                    "phrase field {:?} exists on only {}/{} shards; phrase search requires a complete rebuilt generation",
                    index.phrase_field(), globals.known_shards[plan.leg], self.node_addrs.len())));
            }
            let claims = globals.epochs.clone();
            let stats = timing.started.elapsed();
            let fingerprints = plan.fingerprints(index);
            let phrase_legs = vec![None; plan.fields.len()];
            let result = self
                .bm25_fused_round(FusedRound {
                    query,
                    fields: &plan.fields,
                    field_terms: &plan.terms,
                    globals: &globals,
                    claims: &claims,
                    phrase: Some((plan.leg, &plan.weights, index.fingerprint())),
                    phrase_legs: &phrase_legs,
                    fingerprints: &fingerprints,
                    timing: RoundTiming { stats, ..timing },
                })
                .await;
            match result {
                Err(error) if !fresh && is_stale_stats(&error) => {
                    self.stats_cache.invalidate_all();
                    fresh = true;
                }
                other => return other.map(|((hits, _), _)| hits),
            }
        }
    }
}

impl PhrasePlan {
    async fn analyze(
        mut fields: Vec<crate::pb::QueryField>,
        base: &Bm25SearchRequest,
        index: &crate::phrases::PhraseIndex,
        addr: &str,
        weight_per_token: f32,
        max_weight: f32,
    ) -> Result<Self, Status> {
        let mut terms = Vec::with_capacity(fields.len() + 1);
        for field in &fields {
            let analyzed =
                crate::analyzer::analyze_document(addr, &base.text, field.analysis.as_ref())
                    .await?;
            let mut unique = Vec::new();
            for (term, _, _) in analyzed.into_body().terms {
                if !unique.contains(&term) {
                    unique.push(term);
                }
            }
            terms.push(unique);
        }
        let pairs = index.query_terms(&base.text);
        let weights = pairs
            .iter()
            .map(|(_, count)| ((*count as f32) * weight_per_token).min(max_weight))
            .collect();
        let leg = fields.len();
        fields.push(crate::pb::QueryField {
            field: index.phrase_field().into(),
            weight: 1.0,
            ..Default::default()
        });
        terms.push(pairs.iter().map(|(term, _)| term.clone()).collect());
        let stats_fields = fields
            .iter()
            .zip(&terms)
            .map(|(field, terms)| crate::pb::FieldTerms {
                field: field.field.clone(),
                terms: terms.clone(),
            })
            .collect();
        Ok(Self {
            fields,
            terms,
            weights,
            leg,
            stats_fields,
        })
    }

    fn fingerprints(&self, index: &crate::phrases::PhraseIndex) -> Vec<u64> {
        self.fields
            .iter()
            .enumerate()
            .map(|(i, field)| {
                if i == self.leg {
                    index.fingerprint()
                } else {
                    crate::analyzer::analysis_fingerprint(field.analysis.as_ref())
                }
            })
            .collect()
    }
}

fn validate_phrase_weights(weight_per_token: f32, max_weight: f32) -> Result<(), Status> {
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
    Ok(())
}

fn phrase_fields(
    base: &Bm25SearchRequest,
    index: &crate::phrases::PhraseIndex,
) -> Result<Vec<crate::pb::QueryField>, Status> {
    let fields = if base.fields.is_empty() {
        vec![crate::pb::QueryField {
            field: "body".into(),
            analysis: base.analysis.clone(),
            weight: 1.0,
            ..Default::default()
        }]
    } else {
        if base.analysis.is_some() {
            return Err(Status::invalid_argument("PhraseSearch base.analysis is ignored when base.fields is set; move the spec onto each QueryField.analysis"));
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
            return Err(Status::invalid_argument(format!("field {:?}: PhraseSearch scores glossary concepts; PhraseMatch and TermPrefix constraints are served by Bm25Search", field.field)));
        }
        if field.field == index.phrase_field() {
            return Err(Status::invalid_argument(format!(
                "phrase field {:?} is derived by PhraseSearch and must not appear in base.fields",
                field.field
            )));
        }
        if field.weight < 0.0 || field.weight.is_nan() {
            return Err(Status::invalid_argument(format!(
                "field {:?}: weight must be non-negative",
                field.field
            )));
        }
    }
    Ok(fields)
}
