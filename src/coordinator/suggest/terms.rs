//! Did-you-mean term suggestions over the fleet's dictionaries.

use super::*;

/// The request's bounds with their defaults applied.
struct TermSuggestBounds {
    max_edits: usize,
    prefix_length: usize,
    limit: usize,
    max_scan: usize,
}

impl TermSuggestBounds {
    fn resolve(req: &crate::pb::TermSuggestRequest) -> Result<Self, Status> {
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
        let limit = resolve_suggest_limit(req.limit, DEFAULT_TERM_SUGGEST_LIMIT)?;
        let max_scan = resolve_suggest_scan(req.max_scan)?;
        Ok(Self {
            max_edits,
            prefix_length,
            limit,
            max_scan,
        })
    }
}

fn require_term_suggest_inputs(req: &crate::pb::TermSuggestRequest) -> Result<(), Status> {
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
    Ok(())
}

type TermScanTask = (usize, usize, String, SuggestTask);

/// One suggestion per analyzed term: its own df, and candidates unless the
/// term is shorter than the prefix or already indexed (outside ALWAYS mode).
fn term_suggestions(
    terms: &[String],
    unions: &[DictionaryUnion],
    field: &str,
    bounds: &TermSuggestBounds,
    always: bool,
) -> Result<Vec<crate::pb::TermSuggestion>, Status> {
    let TermSuggestBounds {
        max_edits,
        prefix_length,
        limit,
        max_scan,
    } = *bounds;
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
    Ok(out)
}

impl CoordinatorServiceImpl {
    pub(in crate::coordinator) async fn rpc_term_suggest(
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
            self.term_suggest(req).await
        })
        .await
    }

    async fn term_suggest(
        &self,
        req: crate::pb::TermSuggestRequest,
    ) -> Result<Response<crate::pb::TermSuggestResponse>, Status> {
        let bounds = TermSuggestBounds::resolve(&req)?;
        require_term_suggest_inputs(&req)?;
        let always = req.mode == crate::pb::TermSuggestMode::Always as i32;
        let field = req.field.clone();
        let terms = self.term_suggest_terms(&req).await?;
        let tasks = self.spawn_term_scans(&terms, &field, &bounds)?;
        let (unions, tombstoned) = self
            .union_term_scans(tasks, &terms, &field, bounds.max_scan)
            .await?;
        let mut out = term_suggestions(&terms, &unions, &field, &bounds, always)?;
        self.merge_spellcheck(&mut out, &req.text, bounds.limit, always);
        Ok(Response::new(crate::pb::TermSuggestResponse {
            terms: out,
            df_includes_tombstoned_rows: tombstoned,
        }))
    }

    /// The request text's distinct analyzed terms, in order.
    async fn term_suggest_terms(
        &self,
        req: &crate::pb::TermSuggestRequest,
    ) -> Result<Vec<String>, Status> {
        let addr = self.analysis_addr.clone().ok_or_else(|| {
            Status::unavailable("no analysis backend configured on the coordinator (analysis_addr)")
        })?;
        let analyzed = crate::analyzer::analyze_document(&addr, &req.text, req.analysis.as_ref())
            .await?
            .into_body();
        let mut terms: Vec<String> = Vec::new();
        for (term, _, _) in analyzed.terms {
            if !terms.contains(&term) {
                terms.push(term);
            }
        }
        Ok(terms)
    }

    /// One bounded prefix scan per term per shard; a term shorter than the
    /// prefix length is looked up whole (its own df) and gets no candidates.
    fn spawn_term_scans(
        &self,
        terms: &[String],
        field: &str,
        bounds: &TermSuggestBounds,
    ) -> Result<Vec<TermScanTask>, Status> {
        let TermSuggestBounds {
            prefix_length,
            max_scan,
            ..
        } = *bounds;
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
                    field: field.to_string(),
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
        Ok(tasks)
    }

    /// Each term's dictionary union, plus whether any shard's df counts
    /// tombstoned rows.
    async fn union_term_scans(
        &self,
        tasks: Vec<TermScanTask>,
        terms: &[String],
        field: &str,
        max_scan: usize,
    ) -> Result<(Vec<DictionaryUnion>, bool), Status> {
        let mut unions: Vec<DictionaryUnion> = terms.iter().map(|_| Default::default()).collect();
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; scope.column_count()];
        let mut known = false;
        let mut tombstoned = false;
        for (ti, shard, scan, task) in tasks {
            let resp = task
                .await
                .map_err(|e| Status::internal(format!("term suggest task failed: {e}")))??;
            tombstoned |= resp.tombstoned_rows > 0;
            scope.validate_echo(&resp.visibility_fingerprint, &resp.visibility_columns_known)?;
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
        Ok((unions, tombstoned))
    }

    /// Put the spellchecker's contextual candidates ahead of the dictionary
    /// ones for terms the index does not hold (or all terms in ALWAYS mode).
    fn merge_spellcheck(
        &self,
        out: &mut Vec<crate::pb::TermSuggestion>,
        text: &str,
        limit: usize,
        always: bool,
    ) {
        if let Some(checker) = self.spellchecker.as_deref() {
            if let Some((_, contextual)) =
                crate::turbo_bridge::query_plan::spellcheck_query_terms(checker, text, limit)
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
    }
}
