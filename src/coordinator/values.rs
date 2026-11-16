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
        let candidates: std::collections::HashSet<u64> = ids.iter().copied().collect();
        // Stage parameters validate here too, so a malformed stage is
        // refused by name before any fan-out.
        crate::node::parse_score_stages(stages)?;
        if include_identities && ids.len() > 1_000_000 {
            return Err(Status::resource_exhausted(
                "identity fetch exceeds 1000000 input IDs",
            ));
        }
        let out = FetchedValues {
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
        let fetch = ValuesFetch {
            ids,
            projections,
            stages,
            epochs,
            include_identities,
        };
        let mut tasks = self.spawn_value_fetches(fetch)?;
        // JoinSet aborts outstanding requests if this read is cancelled or a
        // peer fails. Keep deterministic node order when folding the replies.
        let mut responses = std::collections::BTreeMap::new();
        while let Some(joined) = tasks.join_next().await {
            let (shard, response) =
                joined.map_err(|e| Status::internal(format!("fetch values task failed: {e}")))?;
            responses.insert(shard, response?);
        }
        let mut fold = ValuesFold::new(fetch, scope, candidates, out);
        for (shard, resp) in responses {
            fold.fold_shard(shard, resp)?;
        }
        self.check_visibility_columns(&fold.visibility_known)?;
        fold.finish()
    }

    fn spawn_value_fetches(
        &self,
        fetch: ValuesFetch<'_>,
    ) -> Result<tokio::task::JoinSet<ValuesReply>, Status> {
        let ValuesFetch {
            ids,
            projections,
            stages,
            epochs,
            include_identities,
        } = fetch;
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
        Ok(tasks)
    }
}

type ValuesReply = (usize, Result<crate::pb::FetchValuesResponse, Status>);

/// One candidate-scoped fetch as every shard receives it.
#[derive(Clone, Copy)]
struct ValuesFetch<'a> {
    ids: &'a [u64],
    projections: &'a [crate::pb::CompiledProjection],
    stages: &'a [crate::pb::ScoreStage],
    epochs: Option<&'a [StatsClaim]>,
    include_identities: bool,
}

/// The running merge of the shards' value replies, in node order.
struct ValuesFold<'a> {
    fetch: ValuesFetch<'a>,
    scope: crate::visibility::VisibilityScope,
    candidates: std::collections::HashSet<u64>,
    visibility_known: Vec<bool>,
    identity_bytes: usize,
    projection_leaves: Vec<crate::values::ValueLeaf>,
    stage_known: Vec<bool>,
    projection_known: Vec<bool>,
    projection_types: Vec<crate::pb::ScalarValueType>,
    out: FetchedValues,
}

impl<'a> ValuesFold<'a> {
    fn new(
        fetch: ValuesFetch<'a>,
        scope: crate::visibility::VisibilityScope,
        candidates: std::collections::HashSet<u64>,
        out: FetchedValues,
    ) -> Self {
        let ValuesFetch {
            projections,
            stages,
            ..
        } = fetch;
        let projection_leaves: Vec<crate::values::ValueLeaf> = {
            let mut leaves = Vec::new();
            for p in projections {
                if let Some(expr) = p.expr.as_ref() {
                    crate::values::column_leaves(expr, &mut leaves);
                }
            }
            leaves
        };
        Self {
            fetch,
            visibility_known: vec![false; scope.column_count()],
            scope,
            candidates,
            identity_bytes: 0,
            stage_known: vec![false; stages.len()],
            projection_known: vec![false; projection_leaves.len()],
            projection_types: vec![crate::pb::ScalarValueType::Unspecified; projections.len()],
            projection_leaves,
            out,
        }
    }

    fn fold_shard(
        &mut self,
        shard: usize,
        resp: crate::pb::FetchValuesResponse,
    ) -> Result<(), Status> {
        let ValuesFetch {
            projections,
            stages,
            epochs,
            include_identities,
            ..
        } = self.fetch;
        crate::values::validate_map_fetch_contract(projections, resp.typed_map_contract_version)?;
        self.scope
            .validate_echo(&resp.visibility_fingerprint, &resp.visibility_columns_known)?;
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
        self.fold_identities(resp.identities)?;
        self.out.epochs.push(claim);
        for (known, shard) in self
            .visibility_known
            .iter_mut()
            .zip(&resp.visibility_columns_known)
        {
            *known |= shard;
        }
        if resp.projection_types.len() != projections.len()
            || resp.projection_leaves_known.len() != self.projection_leaves.len()
            || resp.stage_columns_known.len() != stages.len()
        {
            return Err(Status::failed_precondition("shard omitted projection type or known-column metadata; use matching server builds"));
        }
        crate::values::merge_projection_types(
            projections,
            &mut self.projection_types,
            &resp.projection_types,
        )?;
        for (known, shard) in self.stage_known.iter_mut().zip(&resp.stage_columns_known) {
            *known |= shard;
        }
        for (known, shard) in self
            .projection_known
            .iter_mut()
            .zip(&resp.projection_leaves_known)
        {
            *known |= shard;
        }
        self.fold_rows(resp.rows, &resp.projection_types)
    }

    fn fold_identities(
        &mut self,
        identities: Vec<crate::pb::CandidateIdentity>,
    ) -> Result<(), Status> {
        for row in identities {
            if !self.candidates.contains(&row.doc_id)
                || self.out.identities.contains_key(&row.doc_id)
            {
                return Err(Status::failed_precondition(
                    "candidate fetch returned an unrequested or duplicate identity",
                ));
            }
            crate::query_identity::charge_candidate_identity(&row, &mut self.identity_bytes)?;
            self.out.identities.insert(row.doc_id, row.identity);
        }
        Ok(())
    }

    fn fold_rows(
        &mut self,
        rows: Vec<crate::pb::FetchedRow>,
        shard_types: &[i32],
    ) -> Result<(), Status> {
        let ValuesFetch {
            projections,
            stages,
            ..
        } = self.fetch;
        for row in rows {
            if !self.candidates.contains(&row.doc_id) || self.out.rows.contains_key(&row.doc_id) {
                return Err(Status::failed_precondition(
                    "candidate fetch returned an unrequested or duplicate row",
                ));
            }
            if row.values.len() != projections.len() || row.stage_values.len() != stages.len() {
                return Err(Status::failed_precondition(
                    "shard returned a projection row with the wrong width",
                ));
            }
            crate::values::validate_projection_row(&row.values, shard_types)?;
            for (i, sv) in row.stage_values.iter().enumerate() {
                match sv.value {
                    Some(crate::pb::projected_value::Value::DoubleValue(v)) => {
                        self.out.stage_rows[i].insert(row.doc_id, v);
                    }
                    None => {}
                    _ => {
                        return Err(Status::failed_precondition(
                            "candidate fetch returned a nonnumeric stage contribution",
                        ))
                    }
                }
            }
            self.out.rows.insert(row.doc_id, row.values);
        }
        Ok(())
    }

    /// Refuse a stage or projection column no shard resolves, by name.
    fn finish(mut self) -> Result<FetchedValues, Status> {
        let stages = self.fetch.stages;
        for (stage, known) in stages.iter().zip(&self.stage_known) {
            if !known {
                return Err(Status::invalid_argument(format!(
                    "no shard has numeric column {}: the stored-value dimension would be \
                     a silent no-op. Check the spelling, or the nodes' --numeric-fields / \
                     --integer-fields / --map-numeric-fields / --geo-fields.",
                    stage.column
                )));
            }
        }
        let unknown_projection: Vec<String> = self
            .projection_leaves
            .iter()
            .zip(&self.projection_known)
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
        self.out.projection_types = self.projection_types;
        Ok(self.out)
    }
}
