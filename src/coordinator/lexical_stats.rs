//! Global term statistics, prefix expansion, and synonym expansion.

use super::*;

/// Query analysis for one field, or the empty analysis for a
/// prefix-only query (docs/prefix-terms.md). `cour*` has no text to
/// analyze — its terms are the expansions alone — so empty text with a
/// prefix present skips the analyzer rather than refusing. Empty text
/// with no prefixes keeps the analyzer's refusal: a query with no terms
/// and no prefixes has nothing to match, and saying so beats an empty
/// result that looks like a miss.
pub(super) async fn analyze_query(
    addr: &str,
    text: &str,
    spec: Option<&crate::pb::AnalysisSpec>,
    prefixes: &[crate::pb::TermPrefix],
) -> Result<crate::postings::AnalyzedField, Status> {
    if text.is_empty() && !prefixes.is_empty() {
        return Ok(crate::postings::AnalyzedDoc::body(Vec::new(), 0).into_body());
    }
    Ok(crate::analyzer::analyze_document(addr, text, spec)
        .await?
        .into_body())
}

pub(super) struct FusedGlobals {
    pub(super) doc_count: u64,
    /// Per field: global sum of that field's document lengths.
    pub(super) totals: Vec<u64>,
    /// Per field: global df per term, in that field's term order.
    pub(super) dfs: Vec<Vec<u32>>,
    pub(super) epochs: Vec<StatsClaim>,
    /// Number of primary shards whose field table contains each field.
    pub(super) known_shards: Vec<usize>,
    /// Number of primary shards whose field carries token positions
    /// (docs/phrase-proximity.md); a positional phrase needs every one.
    pub(super) positions_shards: Vec<usize>,
}

impl CoordinatorServiceImpl {
    /// Expand one field's analyzed query terms under the table (unless
    /// `off`) and the request's rules, through the coordinator's
    /// analysis backend; the added terms are appended to `terms`.
    pub(super) async fn expand_synonyms(
        &self,
        field: &str,
        spec: Option<&crate::pb::AnalysisSpec>,
        rules: &[crate::pb::SynonymRule],
        off: bool,
        terms: &mut Vec<String>,
    ) -> Result<Vec<crate::pb::SynonymExpansion>, Status> {
        if rules.is_empty() && (off || self.synonyms.as_ref().is_none_or(|t| t.is_empty())) {
            return Ok(Vec::new());
        }
        let addr = self.analysis_addr.clone().ok_or_else(|| {
            Status::unavailable("no analysis backend configured on the coordinator (analysis_addr)")
        })?;
        let analyze = move |text: String, spec: Option<crate::pb::AnalysisSpec>| {
            let addr = addr.clone();
            async move {
                let analyzed =
                    crate::analyzer::analyze_document(&addr, &text, spec.as_ref()).await?;
                let mut out: Vec<String> = Vec::new();
                for (term, _, _) in analyzed.into_body().terms {
                    if !out.contains(&term) {
                        out.push(term);
                    }
                }
                Ok(out)
            }
        };
        crate::synonyms::expand(
            self.synonyms.as_deref(),
            off,
            rules,
            field,
            spec,
            terms,
            analyze,
        )
        .await
    }

    /// Expand every [`crate::pb::TermPrefix`] of `field` across the fleet
    /// (`docs/prefix-terms.md`) and add the expansions to `terms`. The
    /// prefix is normalized under the field's char filters (never
    /// stemmed), every shard answers from its byte-sorted directory, and
    /// the fleet-wide union is the term list — the same list a monolith
    /// would expand, so distributed scoring equals monolithic scoring.
    /// Past the cap on any shard or in the union, the request is
    /// INVALID_ARGUMENT naming the count: a prefix is never silently
    /// truncated to a quieter match set.
    pub(super) async fn expand_prefixes(
        &self,
        field: &str,
        spec: Option<&crate::pb::AnalysisSpec>,
        prefixes: &[crate::pb::TermPrefix],
        terms: &mut Vec<String>,
    ) -> Result<Vec<crate::pb::PrefixExpansion>, Status> {
        let mut out = Vec::with_capacity(prefixes.len());
        for prefix in prefixes {
            let cap = match prefix.max_expansions {
                0 => DEFAULT_PREFIX_EXPANSIONS,
                n if n as usize > MAX_PREFIX_EXPANSIONS => {
                    return Err(Status::invalid_argument(format!(
                        "prefix {:?}: max_expansions {n} exceeds the maximum \
                         {MAX_PREFIX_EXPANSIONS}",
                        prefix.prefix
                    )))
                }
                n => n as usize,
            };
            let normalized = crate::analyzer::normalize_prefix(&prefix.prefix, spec)?;
            let mut tasks = Vec::with_capacity(self.node_addrs.len());
            for (i, node) in self.node_addrs.iter().enumerate() {
                let mut client = self.node_client(node)?;
                let request = crate::pb::ExpandTermPrefixRequest {
                    visibility: self.document_visibility.clone(),
                    field: field.to_string(),
                    prefix: normalized.clone(),
                    cap: cap as u32,
                };
                tasks.push((
                    i,
                    tokio::spawn(async move {
                        client
                            .expand_term_prefix(request)
                            .await
                            .map(|r| r.into_inner())
                    }),
                ));
            }
            let mut union = std::collections::BTreeSet::new();
            let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
            let mut visibility_known = vec![false; scope.column_count()];
            let mut known = false;
            for (shard, task) in tasks {
                let resp = task.await.map_err(|e| {
                    Status::internal(format!("prefix expansion task failed: {e}"))
                })??;
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
                if resp.count as usize > cap {
                    return Err(Status::invalid_argument(format!(
                        "prefix {normalized:?} on field {field:?} expands to {} terms on shard \
                         {shard}; the cap is {cap} (raise max_expansions up to \
                         {MAX_PREFIX_EXPANSIONS}, or lengthen the prefix)",
                        resp.count
                    )));
                }
                union.extend(resp.terms);
            }
            self.check_visibility_columns(&visibility_known)?;
            if !known {
                return Err(Status::invalid_argument(format!(
                    "no shard indexes field {field:?}; prefix {normalized:?} has no dictionary \
                     to expand in"
                )));
            }
            if union.len() > cap {
                return Err(Status::invalid_argument(format!(
                    "prefix {normalized:?} on field {field:?} expands to {} terms across the \
                     fleet; the cap is {cap} (raise max_expansions up to \
                     {MAX_PREFIX_EXPANSIONS}, or lengthen the prefix)",
                    union.len()
                )));
            }
            let expanded: Vec<String> = union.into_iter().collect();
            for term in &expanded {
                if !terms.contains(term) {
                    terms.push(term.clone());
                }
            }
            out.push(crate::pb::PrefixExpansion {
                field: field.to_string(),
                prefix: normalized,
                terms: expanded,
            });
        }
        Ok(out)
    }

    /// Per-field global stats for a fused query, merged over per-node
    /// shares served from the stats cache (`fresh` bypasses it; see
    /// [`Self::body_stats`]). Shares merge elementwise per field, N
    /// summed once (it is shared — a document is a document).
    ///
    /// A partially-known field is tolerated: that is a real
    /// heterogeneous fleet, and the shards that have it still
    /// contribute. A field NO shard has is a typo, not a query —
    /// scoring it as "contributes nothing" would silently return the
    /// ranking of the REMAINING fields, so a misspelled arm of an A/B
    /// reads as "no difference". Refused instead.
    pub(super) async fn fused_stats(
        &self,
        stats_fields: &[crate::pb::FieldTerms],
        fresh: bool,
    ) -> Result<FusedGlobals, Status> {
        self.fused_stats_probing(stats_fields, fresh, stats_fields.len())
            .await
    }

    /// [`Self::fused_stats`] where entries from `probe_from` on are
    /// PROBES: fields the query may route onto if the fleet has them
    /// (a phrase's bigram column, docs/phrase-proximity.md). A probe no
    /// shard knows is answered, not refused — the typo rule still
    /// applies to every entry before `probe_from`, which the caller
    /// named on purpose.
    pub(super) async fn fused_stats_probing(
        &self,
        stats_fields: &[crate::pb::FieldTerms],
        fresh: bool,
        probe_from: usize,
    ) -> Result<FusedGlobals, Status> {
        let n = self.node_addrs.len();
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; scope.column_count()];
        let mut shares: Vec<Option<crate::stats_cache::FusedShare>> = vec![None; n];
        if !fresh {
            for (i, share) in shares.iter_mut().enumerate() {
                *share = self
                    .stats_cache
                    .lookup_fused_scoped(i, stats_fields, &scope);
            }
        }
        let mut fetch_tasks = Vec::new();
        for (i, share) in shares.iter().enumerate() {
            if share.is_some() {
                continue;
            }
            let request = TermStatsRequest {
                version_only: false,
                visibility: self.document_visibility.clone(),
                terms: Vec::new(),
                fields: stats_fields.to_vec(),
            };
            let mut client = self.node_client(&self.node_addrs[i])?;
            self.stats_cache.note_fetch();
            fetch_tasks.push((
                i,
                tokio::spawn(
                    async move { client.term_stats(request).await.map(|r| r.into_inner()) },
                ),
            ));
        }
        for (i, task) in fetch_tasks {
            let resp = task
                .await
                .map_err(|e| Status::internal(format!("term stats task failed: {e}")))??;
            if resp.field_stats.len() != stats_fields.len() {
                return Err(Status::internal(format!(
                    "shard returned {} field stats for {} fields",
                    resp.field_stats.len(),
                    stats_fields.len()
                )));
            }
            for (ft, fs) in stats_fields.iter().zip(&resp.field_stats) {
                if fs.doc_frequencies.len() != ft.terms.len() {
                    return Err(Status::internal("shard field stats df length mismatch"));
                }
            }
            self.stats_cache
                .store_scoped(i, &[], stats_fields, &scope, &resp)?;
            shares[i] = Some(crate::stats_cache::FusedShare {
                visibility_columns_known: resp.visibility_columns_known.clone(),
                epoch: StatsClaim::required(resp.stats_epoch, &resp.stats_incarnation)?,
                doc_count: resp.doc_count,
                fields: resp
                    .field_stats
                    .iter()
                    .map(|fs| crate::stats_cache::FusedFieldShare {
                        total_doc_length: fs.total_doc_length,
                        known: fs.known,
                        positions: fs.positions,
                        dfs: fs.doc_frequencies.clone(),
                    })
                    .collect(),
            });
        }
        let mut doc_count = 0u64;
        let mut totals = vec![0u64; stats_fields.len()];
        let mut dfs: Vec<Vec<u32>> = stats_fields
            .iter()
            .map(|ft| vec![0u32; ft.terms.len()])
            .collect();
        let mut known_somewhere = vec![false; stats_fields.len()];
        let mut known_shards = vec![0usize; stats_fields.len()];
        let mut positions_shards = vec![0usize; stats_fields.len()];
        let mut epochs = Vec::with_capacity(n);
        for share in shares {
            let s = share.expect("looked up or fetched above");
            for (known, present) in visibility_known.iter_mut().zip(&s.visibility_columns_known) {
                *known |= present;
            }
            doc_count += s.doc_count;
            for (fi, fs) in s.fields.iter().enumerate() {
                totals[fi] += fs.total_doc_length;
                known_somewhere[fi] |= fs.known;
                known_shards[fi] += usize::from(fs.known);
                positions_shards[fi] += usize::from(fs.positions);
                for (acc, df) in dfs[fi].iter_mut().zip(&fs.dfs) {
                    *acc += df;
                }
            }
            epochs.push(s.epoch);
        }
        self.check_visibility_columns(&visibility_known)?;
        let unknown: Vec<&str> = stats_fields
            .iter()
            .zip(&known_somewhere)
            .take(probe_from)
            .filter(|(_, known)| !**known)
            .map(|(f, _)| f.field.as_str())
            .collect();
        if !unknown.is_empty() {
            return Err(Status::invalid_argument(format!(
                "no shard indexes {}: scoring an unknown field would silently return the \
                 remaining fields' ranking. Check the spelling, or the nodes' --bm25-fields.",
                unknown
                    .iter()
                    .map(|f| format!("{f:?}"))
                    .collect::<Vec<_>>()
                    .join(", ")
            )));
        }
        Ok(FusedGlobals {
            doc_count,
            totals,
            dfs,
            epochs,
            known_shards,
            positions_shards,
        })
    }

    /// GLOBAL BM25 corpus stats for `terms`, summed over per-node shares
    /// served from the stats cache wherever the cached epoch still
    /// stands; only nodes with a missing or incomplete share are asked.
    /// `fresh` bypasses the cache entirely — the retry path after a
    /// shard refused an epoch claim, which is today's uncached
    /// semantics. Also returns the per-node epochs the shares were
    /// valid at, for stamping onto the scoring requests as
    /// `expected_stats_epoch` (the enforcement that makes caching sound;
    /// see src/stats_cache.rs).
    pub(crate) async fn body_stats(
        &self,
        terms: &[String],
        fresh: bool,
    ) -> Result<(CorpusStats, Vec<StatsClaim>), Status> {
        let n = self.node_addrs.len();
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; scope.column_count()];
        let mut shares: Vec<Option<crate::stats_cache::BodyShare>> = vec![None; n];
        if !fresh {
            for (i, share) in shares.iter_mut().enumerate() {
                *share = self.stats_cache.lookup_body_scoped(i, terms, &scope);
            }
        }
        let mut fetch_tasks = Vec::new();
        for (i, share) in shares.iter().enumerate() {
            if share.is_some() {
                continue;
            }
            let terms_owned = terms.to_vec();
            let visibility = self.document_visibility.clone();
            let mut client = self.node_client(&self.node_addrs[i])?;
            self.stats_cache.note_fetch();
            fetch_tasks.push((
                i,
                tokio::spawn(async move {
                    client
                        .term_stats(TermStatsRequest {
                            version_only: false,
                            visibility,
                            terms: terms_owned,
                            fields: Vec::new(),
                        })
                        .await
                        .map(|r| r.into_inner())
                }),
            ));
        }
        for (i, task) in fetch_tasks {
            let resp = task
                .await
                .map_err(|e| Status::internal(format!("term stats task failed: {e}")))??;
            if resp.doc_frequencies.len() != terms.len() {
                return Err(Status::internal("shard stats df length mismatch"));
            }
            self.stats_cache
                .store_scoped(i, terms, &[], &scope, &resp)?;
            shares[i] = Some(crate::stats_cache::BodyShare {
                visibility_columns_known: resp.visibility_columns_known.clone(),
                epoch: StatsClaim::required(resp.stats_epoch, &resp.stats_incarnation)?,
                doc_count: resp.doc_count,
                total_doc_length: resp.total_doc_length,
                dfs: resp.doc_frequencies,
            });
        }
        let mut global = CorpusStats {
            doc_count: 0,
            total_doc_length: 0,
            dfs: vec![0; terms.len()],
        };
        let mut epochs = Vec::with_capacity(n);
        for share in shares {
            let s = share.expect("looked up or fetched above");
            for (known, present) in visibility_known.iter_mut().zip(&s.visibility_columns_known) {
                *known |= present;
            }
            global.doc_count += s.doc_count;
            global.total_doc_length += s.total_doc_length;
            for (acc, df) in global.dfs.iter_mut().zip(&s.dfs) {
                *acc += df;
            }
            epochs.push(s.epoch);
        }
        self.check_visibility_columns(&visibility_known)?;
        Ok((global, epochs))
    }

    /// The BM25 parameters every lexical leaf scores under.
    pub fn bm25_params(&self) -> Bm25Params {
        self.bm25_params
    }

    /// Forget every cached term statistic (the stale-epoch retry).
    pub(crate) fn invalidate_stats(&self) {
        self.stats_cache.invalidate_all();
    }

    /// The distinct body terms of one lexical clause under `spec`, in
    /// first-occurrence order: the membership vocabulary of the clause.
    pub async fn analyze_terms(
        &self,
        text: &str,
        spec: Option<&crate::pb::AnalysisSpec>,
    ) -> Result<Vec<String>, Status> {
        if text.is_empty() {
            return Err(Status::invalid_argument("lexical clause text is empty"));
        }
        let addr = self.analysis_addr.clone().ok_or_else(|| {
            Status::unavailable("no analysis backend configured on the coordinator (analysis_addr)")
        })?;
        let analyzed = crate::analyzer::analyze_document(&addr, text, spec).await?;
        let mut terms = Vec::new();
        for (term, _, _) in analyzed.into_body().terms {
            if !terms.contains(&term) {
                terms.push(term);
            }
        }
        Ok(terms)
    }
}
