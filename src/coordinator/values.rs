//! Projected value retrieval and typed response validation.

use super::*;

impl CoordinatorServiceImpl {
    /// Candidate-scoped value fan-out (the `FetchValues` seam),
    /// broadcast — shards ignore ids they do not own. Projections keep
    /// the ordinary value semantics (absence in, absence out) and the
    /// ordinary typo rule (a column-read leaf NO shard resolves is
    /// refused by name); stages evaluate at their identity score with
    /// the same typo rule as the lexical route's chain.
    pub async fn fetch_values(
        &self,
        ids: &[u64],
        projections: &[crate::pb::CompiledProjection],
        stages: &[crate::pb::ScoreStage],
    ) -> Result<FetchedValues, Status> {
        self.fetch_values_impl(
            ids,
            projections,
            stages,
            self.query_read_versions.as_deref().map(Vec::as_slice),
            false,
        )
        .await
    }

    /// Fetch against versions captured during selection. Claims must cover
    /// every node in this pinned coordinator's order; an absent claim is an
    /// error, not permission to read a newer generation.
    pub async fn fetch_values_at(
        &self,
        ids: &[u64],
        projections: &[crate::pb::CompiledProjection],
        stages: &[crate::pb::ScoreStage],
        epochs: &[StatsClaim],
    ) -> Result<FetchedValues, Status> {
        if epochs.len() != self.node_addrs.len() || epochs.iter().any(|claim| claim.epoch == 0) {
            return Err(Status::failed_precondition(
                "candidate fetch requires one complete selection claim per node",
            ));
        }
        for claim in epochs {
            StatsClaim::required(claim.epoch, &claim.incarnation())?;
        }
        self.fetch_values_impl(ids, projections, stages, Some(epochs), false)
            .await
    }

    /// Resolve identity and explicit absence for live, authorized candidate rows
    /// under a complete selection-time read set. Omitted rows were not served;
    /// callers publishing selected hits must require complete coverage.
    pub async fn resolve_candidate_identities_at(
        &self,
        ids: &[u64],
        epochs: &[StatsClaim],
    ) -> Result<HashMap<u64, Option<crate::pb::DocumentIdentity>>, Status> {
        if self
            .field_permissions
            .as_ref()
            .is_some_and(|fields| !fields.can_disclose_identity())
        {
            return Err(Status::permission_denied(
                "document identity disclosure is not granted",
            ));
        }
        if epochs.len() != self.node_addrs.len() || epochs.iter().any(|claim| claim.epoch == 0) {
            return Err(Status::failed_precondition(
                "identity fetch requires one complete selection claim per node",
            ));
        }
        for claim in epochs {
            StatsClaim::required(claim.epoch, &claim.incarnation())?;
        }
        Ok(self
            .fetch_values_impl(ids, &[], &[], Some(epochs), true)
            .await?
            .identities)
    }

    pub(super) async fn fetch_values_impl(
        &self,
        ids: &[u64],
        projections: &[crate::pb::CompiledProjection],
        stages: &[crate::pb::ScoreStage],
        epochs: Option<&[StatsClaim]>,
        include_identities: bool,
    ) -> Result<FetchedValues, Status> {
        if let Some(fields) = &self.field_permissions {
            fields.fetch_values(projections, stages)?;
        }
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; scope.column_count()];
        let candidates: std::collections::HashSet<u64> = ids.iter().copied().collect();
        // Stage parameters validate here too, so a malformed stage is
        // refused by name before any fan-out.
        crate::node::parse_score_stages(stages)?;
        if include_identities && ids.len() > 1_000_000 {
            return Err(Status::resource_exhausted(
                "identity fetch exceeds 1000000 input IDs",
            ));
        }
        let mut identity_bytes = 0usize;
        let mut out = FetchedValues {
            projection_types: vec![crate::pb::ScalarValueType::Unspecified; projections.len()],
            identities: HashMap::new(),
            rows: HashMap::new(),
            stage_rows: vec![HashMap::new(); stages.len()],
            epochs: Vec::with_capacity(self.node_addrs.len()),
        };
        // An empty candidate list still fans out when anything was
        // named: the typo rules run on the flags, not the rows.
        if !include_identities
            && projections.is_empty()
            && stages.is_empty()
            && epochs.is_none()
            && self.document_visibility.is_none()
        {
            return Ok(out);
        }
        let mut tasks = tokio::task::JoinSet::new();
        for (shard, node) in self.node_addrs.iter().enumerate() {
            let claim = epochs.map_or(StatsClaim::default(), |epochs| epochs[shard]);
            let request = crate::pb::FetchValuesRequest {
                candidate_ids: ids.to_vec(),
                include_identities,
                projections: projections.to_vec(),
                stages: stages.to_vec(),
                visibility: self.document_visibility.clone(),
                expected_stats_epoch: claim.epoch,
                expected_stats_incarnation: claim.incarnation(),
            };
            let mut client = self.node_client(node)?;
            tasks.spawn(async move {
                (
                    shard,
                    client.fetch_values(request).await.map(|r| r.into_inner()),
                )
            });
        }
        let projection_leaves: Vec<crate::values::ValueLeaf> = {
            let mut leaves = Vec::new();
            for p in projections {
                if let Some(expr) = p.expr.as_ref() {
                    crate::values::column_leaves(expr, &mut leaves);
                }
            }
            leaves
        };
        let mut stage_known = vec![false; stages.len()];
        let mut projection_known = vec![false; projection_leaves.len()];
        let mut projection_types = vec![crate::pb::ScalarValueType::Unspecified; projections.len()];
        // JoinSet aborts outstanding requests if this read is cancelled or a
        // peer fails. Keep deterministic node order when folding the replies.
        let mut responses = std::collections::BTreeMap::new();
        while let Some(joined) = tasks.join_next().await {
            let (shard, response) =
                joined.map_err(|e| Status::internal(format!("fetch values task failed: {e}")))?;
            responses.insert(shard, response?);
        }
        for (shard, resp) in responses {
            crate::values::validate_map_fetch_contract(
                projections,
                resp.typed_map_contract_version,
            )?;
            scope.validate_echo(&resp.visibility_fingerprint, &resp.visibility_columns_known)?;
            let claim = StatsClaim::required(resp.stats_epoch, &resp.stats_incarnation)?;
            if epochs.is_some_and(|epochs| epochs[shard] != claim) {
                return Err(Status::failed_precondition(
                    "candidate fetch returned a different selection version",
                ));
            }
            if resp.identities_included != include_identities
                || (!include_identities && !resp.identities.is_empty())
            {
                return Err(Status::failed_precondition(
                    "shard omitted or changed identity fetch certificate",
                ));
            }
            for row in resp.identities {
                if !candidates.contains(&row.doc_id) || out.identities.contains_key(&row.doc_id) {
                    return Err(Status::failed_precondition(
                        "candidate fetch returned an unrequested or duplicate identity",
                    ));
                }
                crate::query_identity::charge_candidate_identity(&row, &mut identity_bytes)?;
                out.identities.insert(row.doc_id, row.identity);
            }
            out.epochs.push(claim);
            for (known, shard) in visibility_known
                .iter_mut()
                .zip(&resp.visibility_columns_known)
            {
                *known |= shard;
            }
            if resp.projection_types.len() != projections.len()
                || resp.projection_leaves_known.len() != projection_leaves.len()
                || resp.stage_columns_known.len() != stages.len()
            {
                return Err(Status::failed_precondition("shard omitted projection type or known-column metadata; use matching server builds"));
            }
            crate::values::merge_projection_types(
                projections,
                &mut projection_types,
                &resp.projection_types,
            )?;
            for (known, shard) in stage_known.iter_mut().zip(&resp.stage_columns_known) {
                *known |= shard;
            }
            for (known, shard) in projection_known
                .iter_mut()
                .zip(&resp.projection_leaves_known)
            {
                *known |= shard;
            }
            for row in resp.rows {
                if !candidates.contains(&row.doc_id) || out.rows.contains_key(&row.doc_id) {
                    return Err(Status::failed_precondition(
                        "candidate fetch returned an unrequested or duplicate row",
                    ));
                }
                if row.values.len() != projections.len() || row.stage_values.len() != stages.len() {
                    return Err(Status::failed_precondition(
                        "shard returned a projection row with the wrong width",
                    ));
                }
                crate::values::validate_projection_row(&row.values, &resp.projection_types)?;
                for (i, sv) in row.stage_values.iter().enumerate() {
                    match sv.value {
                        Some(crate::pb::projected_value::Value::DoubleValue(v)) => {
                            out.stage_rows[i].insert(row.doc_id, v);
                        }
                        None => {}
                        _ => {
                            return Err(Status::failed_precondition(
                                "candidate fetch returned a nonnumeric stage contribution",
                            ))
                        }
                    }
                }
                out.rows.insert(row.doc_id, row.values);
            }
        }
        self.check_visibility_columns(&visibility_known)?;
        for (stage, known) in stages.iter().zip(&stage_known) {
            if !known {
                return Err(Status::invalid_argument(format!(
                    "no shard has numeric column {}: the stored-value dimension would be \
                     a silent no-op. Check the spelling, or the nodes' --numeric-fields / \
                     --integer-fields / --map-numeric-fields / --geo-fields.",
                    stage.column
                )));
            }
        }
        let unknown_projection: Vec<String> = projection_leaves
            .iter()
            .zip(&projection_known)
            .filter(|(_, known)| !**known)
            .map(|(leaf, _)| leaf.describe())
            .collect();
        if !unknown_projection.is_empty() {
            return Err(Status::invalid_argument(format!(
                "projection: no shard has column {}: every value would be absent. \
                 Check the spelling, or the nodes' --numeric-fields / --integer-fields \
                 / --facet-fields / --map-numeric-fields / --map-facet-fields.",
                unknown_projection.join(", ")
            )));
        }
        out.projection_types = projection_types;
        Ok(out)
    }
}
