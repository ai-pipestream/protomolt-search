//! Distributed query suggestions and prefix dictionary limits.

mod terms;

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
            self.suggest(req).await
        })
        .await
    }

    async fn suggest(
        &self,
        req: crate::pb::SuggestRequest,
    ) -> Result<Response<crate::pb::SuggestResponse>, Status> {
        let limit = resolve_suggest_limit(req.limit, DEFAULT_SUGGEST_LIMIT)?;
        let max_scan = resolve_suggest_scan(req.max_scan)?;
        if req.field.is_empty() {
            return Err(Status::invalid_argument(
                "suggest needs a field: name the indexed BM25 field whose dictionary to \
             complete (\"body\" for the body)",
            ));
        }
        let field = req.field.as_str();
        let normalized = crate::analyzer::normalize_prefix(&req.prefix, req.analysis.as_ref())?;
        let tasks = self.spawn_suggest_terms(field, &normalized, max_scan)?;
        let (union, tombstoned) = self
            .union_suggest_terms(tasks, field, &normalized, max_scan)
            .await?;
        let (suggestions, dictionary_terms_with_prefix) =
            self.rank_suggestions(&normalized, union, limit);
        Ok(Response::new(crate::pb::SuggestResponse {
            suggestions,
            dictionary_terms_with_prefix,
            df_includes_tombstoned_rows: tombstoned,
        }))
    }

    fn spawn_suggest_terms(
        &self,
        field: &str,
        normalized: &str,
        max_scan: usize,
    ) -> Result<Vec<(usize, SuggestTask)>, Status> {
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for (i, node) in self.node_addrs.iter().enumerate() {
            let mut client = self.node_client(node)?;
            let request = crate::pb::SuggestTermsRequest {
                visibility: self.document_visibility.clone(),
                field: field.to_string(),
                prefix: normalized.to_string(),
                max_scan: max_scan as u64,
            };
            tasks.push((
                i,
                tokio::spawn(
                    async move { client.suggest_terms(request).await.map(|r| r.into_inner()) },
                ),
            ));
        }
        Ok(tasks)
    }

    /// Term -> (summed df, shards holding it), in byte order, plus whether
    /// any shard's df counts tombstoned rows.
    async fn union_suggest_terms(
        &self,
        tasks: Vec<(usize, SuggestTask)>,
        field: &str,
        normalized: &str,
        max_scan: usize,
    ) -> Result<(DictionaryUnion, bool), Status> {
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
        Ok((union, tombstoned))
    }

    /// The top `limit` suggestions and the dictionary term count under the
    /// prefix.
    fn rank_suggestions(
        &self,
        normalized: &str,
        union: DictionaryUnion,
        limit: usize,
    ) -> (Vec<crate::pb::Suggestion>, u64) {
        if let Some(lexicon) = self.surface_lexicon.as_deref() {
            let view = lexicon.view();
            let resolved = crate::turbo_bridge::query_plan::resolve_stemmed_suggestions(
                &view, normalized, &union, limit,
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
        }
    }
}

type SuggestTask = tokio::task::JoinHandle<Result<crate::pb::SuggestTermsResponse, Status>>;

type DictionaryUnion = std::collections::BTreeMap<String, (u64, u32)>;

fn resolve_suggest_limit(raw: u32, default: usize) -> Result<usize, Status> {
    let limit = match raw as usize {
        0 => default,
        n if n > MAX_SUGGEST_LIMIT => {
            return Err(Status::invalid_argument(format!(
                "limit {n} exceeds the maximum {MAX_SUGGEST_LIMIT}"
            )))
        }
        n => n,
    };
    Ok(limit)
}

fn resolve_suggest_scan(raw: u64) -> Result<usize, Status> {
    let max_scan = match raw {
        0 => DEFAULT_SUGGEST_SCAN,
        n if n > MAX_SUGGEST_SCAN as u64 => {
            return Err(Status::invalid_argument(format!(
                "max_scan {n} exceeds the maximum {MAX_SUGGEST_SCAN}"
            )))
        }
        n => n as usize,
    };
    Ok(max_scan)
}
