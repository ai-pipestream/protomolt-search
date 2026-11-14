//! Lineage.

use super::*;

impl CoordinatorServiceImpl {
    /// The lineage keys of every requested document across the shards:
    /// doc id to (parent_id, group_id). A document without lineage
    /// parents itself in the high-bit-tagged domain and has group 0. A
    /// deleted or unknown id is absent.
    pub async fn lineage_keys(&self, ids: &[u64]) -> Result<HashMap<u64, (u64, u64)>, Status> {
        self.read_lineage(ids, &crate::lineage::LineageSelection::new(&[])?, None)
            .await
    }

    /// Read exactly one lineage column, including its field-disclosure check.
    pub async fn lineage_key(
        &self,
        ids: &[u64],
        column: &str,
    ) -> Result<HashMap<u64, u64>, Status> {
        let field = match column {
            "parent_id" => crate::pb::LineageField::ParentId,
            "group_id" => crate::pb::LineageField::GroupId,
            _ => return Err(Status::invalid_argument("unknown lineage column")),
        };
        let selection = crate::lineage::LineageSelection::new(&[field as i32])?;
        Ok(self
            .read_lineage(ids, &selection, None)
            .await?
            .into_iter()
            .map(|(id, (parent, group))| {
                (
                    id,
                    if field == crate::pb::LineageField::ParentId {
                        parent
                    } else {
                        group
                    },
                )
            })
            .collect())
    }

    pub(super) async fn read_lineage(
        &self,
        ids: &[u64],
        selection: &crate::lineage::LineageSelection,
        by_shard: Option<&HashMap<usize, Vec<u64>>>,
    ) -> Result<HashMap<u64, (u64, u64)>, Status> {
        if let Some(fields) = &self.field_permissions {
            for column in selection.columns() {
                fields.dictionary(column)?;
            }
        }
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut known = vec![false; scope.column_count()];
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for (shard, node) in self.node_addrs.iter().enumerate() {
            let requested = match by_shard {
                None => ids,
                Some(owners) => owners.get(&shard).map_or(&[][..], Vec::as_slice),
            };
            if by_shard.is_some() && requested.is_empty() && self.document_visibility.is_none() {
                continue;
            }
            let claim = self
                .query_read_versions
                .as_ref()
                .map(|claims| claims[shard])
                .unwrap_or_default();
            let request = crate::pb::ResolveParentsRequest {
                doc_ids: requested.to_vec(),
                fields: selection.wire(),
                visibility: self.document_visibility.clone(),
                expected_stats_epoch: claim.epoch,
                expected_stats_incarnation: claim.incarnation(),
            };
            let mut client = self.node_client(node)?;
            let requested: std::collections::HashSet<u64> = requested.iter().copied().collect();
            tasks.push((
                shard,
                requested,
                tokio::spawn(async move {
                    client
                        .resolve_parents(request)
                        .await
                        .map(|r| r.into_inner())
                }),
            ));
        }
        let mut out = HashMap::with_capacity(ids.len());
        for (shard, requested, task) in tasks {
            let response = task
                .await
                .map_err(|e| Status::internal(format!("lineage task failed: {e}")))??;
            self.check_read_view(shard, &scope, &response, &mut known)?;
            selection.validate_echo(&response.fields)?;
            for resolved in response.parents {
                selection.validate_row(&resolved)?;
                if !requested.contains(&resolved.doc_id) {
                    return Err(Status::failed_precondition(
                        "lineage response returned an unrequested row",
                    ));
                }
                if out
                    .insert(resolved.doc_id, (resolved.parent_id, resolved.group_id))
                    .is_some()
                {
                    return Err(Status::failed_precondition(
                        "lineage response returned duplicate row ownership",
                    ));
                }
            }
        }
        self.check_visibility_columns(&known)?;
        Ok(out)
    }
}
