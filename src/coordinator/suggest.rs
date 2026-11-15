//! Distributed query suggestions and prefix dictionary limits.

use super::*;

/// Default expansion cap of a [`crate::pb::TermPrefix`] with `max_expansions` unset.
pub const DEFAULT_PREFIX_EXPANSIONS: usize = 128;

/// The largest cap a request may ask for; every expansion is a scored
/// term, and past this a prefix is a scan, not a query.
pub const MAX_PREFIX_EXPANSIONS: usize = 1024;

/// Candidates per term a did-you-mean request returns by default.
pub const DEFAULT_TERM_SUGGEST_LIMIT: usize = 5;

/// The largest edit bound a did-you-mean request may name.
pub const MAX_TERM_SUGGEST_EDITS: u32 = 2;

/// Suggestions a `Suggest` request returns when `limit` is unset.
pub const DEFAULT_SUGGEST_LIMIT: usize = 10;

/// The most suggestions one request may ask for.
pub const MAX_SUGGEST_LIMIT: usize = 100;

/// Dictionary terms under the prefix a `Suggest` request may scan when
/// `max_scan` is unset, per shard and fleet-wide.
pub const DEFAULT_SUGGEST_SCAN: usize = 100_000;

/// The most dictionary terms one request may scan.
pub const MAX_SUGGEST_SCAN: usize = 1_000_000;

impl CoordinatorServiceImpl {
    /// Autocomplete over one field's dictionary: normalize the prefix under the
    /// field's char filters (without stemming), union per-shard dictionary hits
    /// summing posting `df`, and rank by `(df desc, term asc)`, refusing if any
    /// shard or the fleet union exceeds `max_scan`.
    pub(super) async fn rpc_suggest(
        &self,
        request: Request<crate::pb::SuggestRequest>,
    ) -> Result<Response<crate::pb::SuggestResponse>, Status> {
        crate::metrics::timed(Route::Suggest, request, |request| async move {
            self.admit(&request.get_ref().collection)?;
            if let Some(snapshot) = self.request_snapshot() {
                return Box::pin(SearchService::suggest(&snapshot, request)).await;
            }
            let req = request.into_inner();
            if let Some(fields) = &self.field_permissions {
                fields.suggest(&req)?;
            }
            let limit = match req.limit as usize {
                0 => DEFAULT_SUGGEST_LIMIT,
                n if n > MAX_SUGGEST_LIMIT => {
                    return Err(Status::invalid_argument(format!(
                        "limit {n} exceeds the maximum {MAX_SUGGEST_LIMIT}"
                    )))
                }
                n => n,
            };
            let max_scan = match req.max_scan {
                0 => DEFAULT_SUGGEST_SCAN,
                n if n > MAX_SUGGEST_SCAN as u64 => {
                    return Err(Status::invalid_argument(format!(
                        "max_scan {n} exceeds the maximum {MAX_SUGGEST_SCAN}"
                    )))
                }
                n => n as usize,
            };
            if req.field.is_empty() {
                return Err(Status::invalid_argument(
                    "suggest needs a field: name the indexed BM25 field whose dictionary to \
                 complete (\"body\" for the body)",
                ));
            }
            let field = req.field.as_str();
            let normalized = crate::analyzer::normalize_prefix(&req.prefix, req.analysis.as_ref())?;
            let mut tasks = Vec::with_capacity(self.node_addrs.len());
            for (i, node) in self.node_addrs.iter().enumerate() {
                let mut client = self.node_client(node)?;
                let request = crate::pb::SuggestTermsRequest {
                    visibility: self.document_visibility.clone(),
                    field: field.to_string(),
                    prefix: normalized.clone(),
                    max_scan: max_scan as u64,
                };
                tasks.push((
                    i,
                    tokio::spawn(async move {
                        client.suggest_terms(request).await.map(|r| r.into_inner())
                    }),
                ));
            }
            // Term -> (summed df, shards holding it), in byte order.
            let mut union: std::collections::BTreeMap<String, (u64, u32)> =
                std::collections::BTreeMap::new();
            let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
            let mut visibility_known = vec![false; scope.column_count()];
            let mut known = false;
            let mut tombstoned = false;
            for (shard, task) in tasks {
                let resp = task
                    .await
                    .map_err(|e| Status::internal(format!("suggest task failed: {e}")))??;
                tombstoned |= resp.tombstoned_rows > 0;
                scope
                    .validate_echo(&resp.visibility_fingerprint, &resp.visibility_columns_known)?;
                for (known, present) in visibility_known
                    .iter_mut()
                    .zip(&resp.visibility_columns_known)
                {
                    *known |= present;
                }
                if !resp.known {
                    continue;
                }
                known = true;
                if resp.count as usize > max_scan {
                    return Err(Status::invalid_argument(format!(
                        "prefix {normalized:?} on field {field:?} matches {} dictionary terms on \
                     shard {shard}; the scan bound is {max_scan} (raise max_scan up to \
                     {MAX_SUGGEST_SCAN}, or lengthen the prefix)",
                        resp.count
                    )));
                }
                for entry in resp.entries {
                    let slot = union.entry(entry.term).or_insert((0, 0));
                    slot.0 += entry.df;
                    slot.1 += 1;
                }
            }
            self.check_visibility_columns(&visibility_known)?;
            if !known {
                return Err(Status::invalid_argument(format!(
                    "no shard indexes field {field:?}; prefix {normalized:?} has no dictionary to \
                 complete in"
                )));
            }
            if union.len() > max_scan {
                return Err(Status::invalid_argument(format!(
                    "prefix {normalized:?} on field {field:?} matches {} dictionary terms across \
                 the fleet; the scan bound is {max_scan} (raise max_scan up to \
                 {MAX_SUGGEST_SCAN}, or lengthen the prefix)",
                    union.len()
                )));
            }
            let (suggestions, dictionary_terms_with_prefix) =
                if let Some(lexicon) = self.surface_lexicon.as_deref() {
                    let view = lexicon.view();
                    let resolved = crate::turbo_bridge::query_plan::resolve_stemmed_suggestions(
                        &view,
                        &normalized,
                        &union,
                        limit,
                    );
                    let count = (resolved.len().max(union.len())) as u64;
                    (resolved, count)
                } else {
                    let count = union.len() as u64;
                    let mut ranked: Vec<(String, (u64, u32))> = union.into_iter().collect();
                    // df descending; the map's byte order breaks ties (stable sort).
                    ranked.sort_by_key(|(_, (df, _))| std::cmp::Reverse(*df));
                    ranked.truncate(limit);
                    (
                        ranked
                            .into_iter()
                            .map(|(term, (df, shards))| crate::pb::Suggestion { term, df, shards })
                            .collect(),
                        count,
                    )
                };
            Ok(Response::new(crate::pb::SuggestResponse {
                suggestions,
                dictionary_terms_with_prefix,
                df_includes_tombstoned_rows: tombstoned,
            }))
        })
        .await
    }

    pub(super) async fn rpc_term_suggest(
        &self,
        request: Request<crate::pb::TermSuggestRequest>,
    ) -> Result<Response<crate::pb::TermSuggestResponse>, Status> {
        crate::metrics::timed(Route::TermSuggest, request, |request| async move {
            self.admit(&request.get_ref().collection)?;
            if let Some(snapshot) = self.request_snapshot() {
                return Box::pin(SearchService::term_suggest(&snapshot, request)).await;
            }
            let req = request.into_inner();
            if let Some(fields) = &self.field_permissions {
                fields.term_suggest(&req)?;
            }
            let max_edits = match req.max_edits {
                0 => 1usize,
                n if n > MAX_TERM_SUGGEST_EDITS => {
                    return Err(Status::invalid_argument(format!(
                        "max_edits {n} exceeds the maximum {MAX_TERM_SUGGEST_EDITS}"
                    )))
                }
                n => n as usize,
            };
            let prefix_length = match req.prefix_length {
                0 => 1usize,
                n => n as usize,
            };
            let limit = match req.limit as usize {
                0 => DEFAULT_TERM_SUGGEST_LIMIT,
                n if n > MAX_SUGGEST_LIMIT => {
                    return Err(Status::invalid_argument(format!(
                        "limit {n} exceeds the maximum {MAX_SUGGEST_LIMIT}"
                    )))
                }
                n => n,
            };
            let max_scan = match req.max_scan {
                0 => DEFAULT_SUGGEST_SCAN,
                n if n > MAX_SUGGEST_SCAN as u64 => {
                    return Err(Status::invalid_argument(format!(
                        "max_scan {n} exceeds the maximum {MAX_SUGGEST_SCAN}"
                    )))
                }
                n => n as usize,
            };
            if req.field.is_empty() {
                return Err(Status::invalid_argument(
                    "term suggestions need a field: name the indexed BM25 field whose \
                     dictionary to consult (\"body\" for the body)",
                ));
            }
            if req.analysis.is_none() {
                return Err(Status::invalid_argument(
                    "term suggestions need the field's analysis spec: the text is analyzed \
                     under it, and the sidecar's default chain is not known here",
                ));
            }
            if req.text.trim().is_empty() {
                return Err(Status::invalid_argument("term suggestions need text"));
            }
            let always = req.mode == crate::pb::TermSuggestMode::Always as i32;
            let field = req.field.clone();
            let addr = self.analysis_addr.clone().ok_or_else(|| {
                Status::unavailable(
                    "no analysis backend configured on the coordinator (analysis_addr)",
                )
            })?;
            let analyzed =
                crate::analyzer::analyze_document(&addr, &req.text, req.analysis.as_ref())
                    .await?
                    .into_body();
            let mut terms: Vec<String> = Vec::new();
            for (term, _, _) in analyzed.terms {
                if !terms.contains(&term) {
                    terms.push(term);
                }
            }
            // One bounded prefix scan per term per shard; a term shorter
            // than the prefix length is looked up whole (its own df) and
            // gets no candidates.
            let mut tasks = Vec::with_capacity(terms.len() * self.node_addrs.len());
            for (ti, term) in terms.iter().enumerate() {
                let prefix: String = term.chars().take(prefix_length).collect();
                let scan = if term.chars().count() >= prefix_length {
                    prefix
                } else {
                    term.clone()
                };
                for (shard, node) in self.node_addrs.iter().enumerate() {
                    let mut client = self.node_client(node)?;
                    let request = crate::pb::SuggestTermsRequest {
                        visibility: self.document_visibility.clone(),
                        field: field.clone(),
                        prefix: scan.clone(),
                        max_scan: max_scan as u64,
                    };
                    tasks.push((
                        ti,
                        shard,
                        scan.clone(),
                        tokio::spawn(async move {
                            client.suggest_terms(request).await.map(|r| r.into_inner())
                        }),
                    ));
                }
            }
            let mut unions: Vec<std::collections::BTreeMap<String, (u64, u32)>> =
                terms.iter().map(|_| Default::default()).collect();
            let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
            let mut visibility_known = vec![false; scope.column_count()];
            let mut known = false;
            let mut tombstoned = false;
            for (ti, shard, scan, task) in tasks {
                let resp = task
                    .await
                    .map_err(|e| Status::internal(format!("term suggest task failed: {e}")))??;
                tombstoned |= resp.tombstoned_rows > 0;
                scope
                    .validate_echo(&resp.visibility_fingerprint, &resp.visibility_columns_known)?;
                for (known, present) in visibility_known
                    .iter_mut()
                    .zip(&resp.visibility_columns_known)
                {
                    *known |= present;
                }
                if !resp.known {
                    continue;
                }
                known = true;
                if resp.count as usize > max_scan {
                    return Err(Status::invalid_argument(format!(
                        "prefix {scan:?} of term {:?} on field {field:?} matches {} dictionary \
                         terms on shard {shard}; the scan bound is {max_scan} (raise max_scan \
                         up to {MAX_SUGGEST_SCAN}, or raise prefix_length)",
                        terms[ti], resp.count
                    )));
                }
                for entry in resp.entries {
                    let slot = unions[ti].entry(entry.term).or_insert((0, 0));
                    slot.0 += entry.df;
                    slot.1 += 1;
                }
            }
            self.check_visibility_columns(&visibility_known)?;
            if !known {
                return Err(Status::invalid_argument(format!(
                    "no shard indexes field {field:?}; there is no dictionary to suggest from"
                )));
            }
            let mut out = Vec::with_capacity(terms.len());
            for (ti, term) in terms.iter().enumerate() {
                let union = &unions[ti];
                if union.len() > max_scan {
                    return Err(Status::invalid_argument(format!(
                        "the prefix of term {term:?} on field {field:?} matches {} dictionary \
                         terms across the fleet; the scan bound is {max_scan} (raise max_scan \
                         up to {MAX_SUGGEST_SCAN}, or raise prefix_length)",
                        union.len()
                    )));
                }
                let df = union.get(term).map_or(0, |(df, _)| *df);
                let candidates = if term.chars().count() < prefix_length || (df > 0 && !always) {
                    Vec::new()
                } else {
                    crate::synonyms::rank_candidates(term, union, max_edits, limit)
                };
                out.push(crate::pb::TermSuggestion {
                    term: term.clone(),
                    df,
                    candidates,
                    dictionary_terms_scanned: union.len() as u64,
                });
            }
            if let Some(checker) = self.spellchecker.as_deref() {
                if let Some((_, contextual)) =
                    crate::turbo_bridge::query_plan::spellcheck_query_terms(
                        checker, &req.text, limit,
                    )
                {
                    for ctx in contextual {
                        if let Some(existing) = out.iter_mut().find(|t| t.term == ctx.term) {
                            if existing.df == 0 || always {
                                for cand in ctx.candidates {
                                    if !existing.candidates.iter().any(|c| c.term == cand.term) {
                                        existing.candidates.insert(0, cand);
                                    }
                                }
                                existing.candidates.truncate(limit);
                            }
                        } else {
                            out.push(ctx);
                        }
                    }
                }
            }
            Ok(Response::new(crate::pb::TermSuggestResponse {
                terms: out,
                df_includes_tombstoned_rows: tombstoned,
            }))
        })
        .await
    }
}
