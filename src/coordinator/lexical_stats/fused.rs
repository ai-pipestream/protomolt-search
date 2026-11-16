//! Per-field global statistics for fused lexical queries.

use super::*;

type TermStatsTask = tokio::task::JoinHandle<Result<crate::pb::TermStatsResponse, Status>>;

fn validate_field_stats(
    resp: &crate::pb::TermStatsResponse,
    stats_fields: &[crate::pb::FieldTerms],
) -> Result<(), Status> {
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
    Ok(())
}

fn fused_share_of(
    resp: &crate::pb::TermStatsResponse,
) -> Result<crate::stats_cache::FusedShare, Status> {
    Ok(crate::stats_cache::FusedShare {
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
    })
}

/// Sum the per-node shares elementwise per field; also returns, per field,
/// whether any shard knows it.
fn merge_fused_shares(
    shares: Vec<Option<crate::stats_cache::FusedShare>>,
    stats_fields: &[crate::pb::FieldTerms],
    visibility_known: &mut [bool],
) -> (FusedGlobals, Vec<bool>) {
    let n = shares.len();
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
    let globals = FusedGlobals {
        doc_count,
        totals,
        dfs,
        epochs,
        known_shards,
        positions_shards,
    };
    (globals, known_somewhere)
}

/// Refuse a caller-named field (those before `probe_from`) no shard indexes.
fn refuse_unknown_fields(
    stats_fields: &[crate::pb::FieldTerms],
    known_somewhere: &[bool],
    probe_from: usize,
) -> Result<(), Status> {
    let unknown: Vec<&str> = stats_fields
        .iter()
        .zip(known_somewhere)
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
    Ok(())
}

impl CoordinatorServiceImpl {
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
    pub(in crate::coordinator) async fn fused_stats(
        &self,
        stats_fields: &[crate::pb::FieldTerms],
        fresh: bool,
    ) -> Result<FusedGlobals, Status> {
        self.fused_stats_probing(stats_fields, fresh, stats_fields.len())
            .await
    }

    /// [`Self::fused_stats`] where entries from `probe_from` onward are optional
    /// probe fields (such as bigram columns) that are tolerated when absent
    /// across all shards, while earlier caller-named fields still refuse on typos.
    pub(in crate::coordinator) async fn fused_stats_probing(
        &self,
        stats_fields: &[crate::pb::FieldTerms],
        fresh: bool,
        probe_from: usize,
    ) -> Result<FusedGlobals, Status> {
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; scope.column_count()];
        let shares = self.fused_shares(stats_fields, fresh, &scope).await?;
        let (globals, known_somewhere) =
            merge_fused_shares(shares, stats_fields, &mut visibility_known);
        self.check_visibility_columns(&visibility_known)?;
        refuse_unknown_fields(stats_fields, &known_somewhere, probe_from)?;
        Ok(globals)
    }

    /// Each node's share, from the stats cache unless `fresh` or missing,
    /// otherwise fetched and stored.
    async fn fused_shares(
        &self,
        stats_fields: &[crate::pb::FieldTerms],
        fresh: bool,
        scope: &crate::visibility::VisibilityScope,
    ) -> Result<Vec<Option<crate::stats_cache::FusedShare>>, Status> {
        let n = self.node_addrs.len();
        let mut shares: Vec<Option<crate::stats_cache::FusedShare>> = vec![None; n];
        if !fresh {
            for (i, share) in shares.iter_mut().enumerate() {
                *share = self.stats_cache.lookup_fused_scoped(i, stats_fields, scope);
            }
        }
        let mut fetch_tasks: Vec<(usize, TermStatsTask)> = Vec::new();
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
            validate_field_stats(&resp, stats_fields)?;
            self.stats_cache
                .store_scoped(i, &[], stats_fields, scope, &resp)?;
            shares[i] = Some(fused_share_of(&resp)?);
        }
        Ok(shares)
    }
}
