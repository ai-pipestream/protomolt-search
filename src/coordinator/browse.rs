//! Ordered document browsing across shards.

use super::*;

/// A browse resume boundary: the last returned id, plus its adjusted
/// sort-key bits when the browse is column-ordered.
#[derive(Debug, Clone)]
pub struct BrowseAfter {
    pub id: u64,
    /// The boundary's sort keys, parallel to the request's sort list
    /// (empty for an id-ordered browse).
    pub keys: Vec<crate::sortkeys::Key>,
}

/// One merged browse page.
#[derive(Debug, Clone)]
pub struct BrowseRows {
    /// Sealed segments the shards consulted and ruled out
    /// (docs/segment-pruning.md).
    pub prune: crate::segment_prune::PruneStats,
    /// Global doc ids in final order.
    pub ids: Vec<u64>,
    /// Each row's sort keys in merge form, parallel to `ids` (empty
    /// rows unsorted).
    pub keys: Vec<Vec<crate::sortkeys::Key>>,
    /// Each row's reported sort values, parallel to `ids`.
    pub values: Vec<Vec<crate::sortkeys::Value>>,
    /// Whether a column order was applied.
    pub sorted: bool,
}

impl CoordinatorServiceImpl {
    /// Filter-only browse fan-out: each shard's admitted ids above the
    /// boundary, merged and truncated to `k` — ascending by id unsorted, or
    /// by `(order-preserving key bits, id)` under `sort`. A column no shard
    /// knows refuses by name, including sort columns.
    pub async fn fanout_browse(
        &self,
        k: u32,
        after: Option<BrowseAfter>,
        sort: &[crate::pb::BrowseSort],
        lexical_terms: &[String],
        analysis_fingerprint: u64,
        filters: &RequestFilters,
    ) -> Result<BrowseRows, Status> {
        use crate::sortkeys::{cmp_rows, Key, Value};
        let k = self.resolve_k(k)?;
        for key in sort {
            crate::sortkeys::target_field(&key.column, key.map.as_ref())?;
        }
        if let Some(fields) = &self.field_permissions {
            fields.browse(filters, sort, lexical_terms)?;
        }
        if self.query_read_versions.is_none() {
            let (pinned, reads) = self.pin_read_versions().await?;
            let result = Box::pin(pinned.fanout_browse(
                k,
                after,
                sort,
                lexical_terms,
                analysis_fingerprint,
                filters,
            ))
            .await;
            pinned.validate_read_versions(&reads).await?;
            return result;
        }
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; scope.column_count()];
        let mask = self
            .document_visibility
            .is_none()
            .then(|| self.shard_mask(filters.tree.as_ref()))
            .flatten();
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            if mask.as_ref().is_some_and(|m| m.skipped[shard]) {
                continue;
            }
            let claim = self.query_read_versions.as_ref().expect("pinned read set")[shard];
            let request = crate::pb::BrowseShardRequest {
                visibility: self.document_visibility.clone(),
                expected_stats_epoch: claim.epoch,
                expected_stats_incarnation: claim.incarnation(),
                analysis_fingerprint,
                k,
                after: after.as_ref().map_or(0, |a| a.id),
                first_page: after.is_none(),
                geo_filters: filters.geo.clone(),
                filter: Self::shard_filter_tree(filters, mask.as_ref(), shard),
                sort: sort.to_vec(),
                after_keys: after
                    .as_ref()
                    .map(|a| a.keys.iter().map(Key::to_pb).collect())
                    .unwrap_or_default(),
                lexical_terms: lexical_terms.to_vec(),
            };
            let client = self.node_client(node);
            tasks.push((
                shard,
                tokio::spawn(
                    async move { client?.browse_shard(request).await.map(|r| r.into_inner()) },
                ),
            ));
        }
        let mut known = Self::filter_known(filters, mask.as_ref());
        let mut sort_known = vec![false; sort.len()];
        let mut sort_types = vec![crate::pb::ScalarValueType::Unspecified; sort.len()];
        struct Row {
            keys: Vec<Key>,
            values: Vec<Value>,
            id: u64,
        }
        let mut rows: Vec<Row> = Vec::new();
        let mut prune = crate::segment_prune::PruneStats::default();
        for (shard, task) in tasks {
            let response = task
                .await
                .map_err(|e| Status::internal(format!("browse task failed: {e}")))??;
            self.check_read_view(shard, &scope, &response, &mut visibility_known)?;
            known.merge_shard(
                shard,
                &response.geo_columns_known,
                &response.filter_columns_known,
            )?;
            prune.add(crate::segment_prune::PruneStats {
                segments_total: response.segments_total,
                segments_skipped: response.segments_skipped,
            });
            crate::sortkeys::validate_browse_metadata(sort, &response)?;
            for (i, (&known, &raw_type)) in response
                .sort_columns_known
                .iter()
                .zip(&response.sort_column_types)
                .enumerate()
            {
                let kind =
                    crate::pb::ScalarValueType::try_from(raw_type).expect("validated metadata");
                if kind != crate::pb::ScalarValueType::Unspecified {
                    if sort_types[i] != crate::pb::ScalarValueType::Unspecified
                        && sort_types[i] != kind
                    {
                        return Err(Status::failed_precondition(format!(
                            "sort column {:?} has incompatible types across shards: {:?} and {:?}",
                            crate::sortkeys::target_name(&sort[i].column, sort[i].map.as_ref()),
                            sort_types[i],
                            kind
                        )));
                    }
                    sort_types[i] = kind;
                }
                sort_known[i] |= known;
            }
            if sort.is_empty() {
                rows.extend(response.doc_ids.iter().map(|&id| Row {
                    keys: Vec::new(),
                    values: Vec::new(),
                    id,
                }));
                continue;
            }
            if response.sort_rows.len() != response.doc_ids.len() {
                return Err(Status::internal(
                    "shard answered a sorted browse with mismatched key rows",
                ));
            }
            for (&id, row) in response.doc_ids.iter().zip(&response.sort_rows) {
                let keys: Option<Vec<Key>> = row.keys.iter().map(Key::from_pb).collect();
                let values: Option<Vec<Value>> = row
                    .values
                    .iter()
                    .map(crate::sortkeys::value_from_pb)
                    .collect();
                let (Some(keys), Some(values)) = (keys, values) else {
                    return Err(Status::internal(
                        "shard answered a sorted browse with an empty key",
                    ));
                };
                if keys.len() != sort.len() || values.len() != sort.len() {
                    return Err(Status::internal(format!(
                        "shard answered a sorted browse with {} keys for {} sort columns",
                        keys.len(),
                        sort.len()
                    )));
                }
                for (i, (key, value)) in keys.iter().zip(&values).enumerate() {
                    let kind = crate::pb::ScalarValueType::try_from(response.sort_column_types[i])
                        .expect("validated metadata");
                    if value.column_type() != kind || !crate::sortkeys::key_matches_type(key, kind)
                    {
                        return Err(Status::failed_precondition(
                            "shard sort row disagrees with its declared column type",
                        ));
                    }
                }
                rows.push(Row { keys, values, id });
            }
        }
        self.check_visibility_columns(&visibility_known)?;
        known.refuse_unknown(filters)?;
        for (sort, known) in sort.iter().zip(&sort_known) {
            if !known {
                return Err(Status::invalid_argument(format!(
                    "sort column or map entry {:?} is not declared on any shard; check the column and key",
                    crate::sortkeys::target_name(&sort.column, sort.map.as_ref())
                )));
            }
        }
        let descending: Vec<bool> = sort.iter().map(|s| s.descending).collect();
        rows.sort_by(|a, b| cmp_rows(&a.keys, a.id, &b.keys, b.id, &descending));
        rows.truncate(k as usize);
        Ok(BrowseRows {
            prune,
            ids: rows.iter().map(|r| r.id).collect(),
            keys: rows.iter().map(|r| r.keys.clone()).collect(),
            values: rows.iter().map(|r| r.values.clone()).collect(),
            sorted: !sort.is_empty(),
        })
    }
}
