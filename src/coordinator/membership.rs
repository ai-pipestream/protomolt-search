//! Membership.

use super::*;

impl CoordinatorServiceImpl {
    pub(super) fn merge_membership_bitmap(
        out: &mut MembershipSet,
        response: &crate::pb::MembershipBitmapResponse,
    ) -> Result<(), Status> {
        let label_count = usize::try_from(response.label_count).map_err(|_| {
            Status::resource_exhausted("membership label count does not fit this platform")
        })?;
        let expected = usize::try_from(response.label_count.div_ceil(8))
            .map_err(|_| Status::resource_exhausted("membership bitmap does not fit usize"))?;
        if response.bits.len() != expected {
            return Err(Status::internal(format!(
                "membership bitmap has {} bytes for {} labels; expected {expected}",
                response.bits.len(),
                response.label_count
            )));
        }
        let end = response
            .base_label
            .checked_add(response.label_count)
            .ok_or_else(|| Status::internal("membership bitmap label range overflows u64"))?;
        if response.label_count > 0 {
            if out
                .ranges
                .iter()
                .any(|&(held_base, held_end)| response.base_label < held_end && held_base < end)
            {
                return Err(Status::failed_precondition(format!(
                    "membership label range [{}, {end}) overlaps another shard",
                    response.base_label
                )));
            }
            out.ranges.push((response.base_label, end));
        }
        if !response.label_count.is_multiple_of(8)
            && response.bits.last().is_some_and(|last| {
                let used = (response.label_count % 8) as u8;
                *last & !((1u8 << used) - 1) != 0
            })
        {
            return Err(Status::internal(
                "membership bitmap sets padding bits beyond its label count",
            ));
        }
        out.wire_bytes = out
            .wire_bytes
            .checked_add(response.bits.len() as u64)
            .ok_or_else(|| Status::resource_exhausted("membership byte count overflow"))?;
        for (byte_index, byte) in response.bits.iter().copied().enumerate() {
            let mut held = byte;
            while held != 0 {
                let bit = held.trailing_zeros() as usize;
                let position = byte_index * 8 + bit;
                if position < label_count {
                    let id = response
                        .base_label
                        .checked_add(position as u64)
                        .ok_or_else(|| Status::internal("membership stable-id overflow"))?;
                    if !out.ids.insert(id) {
                        return Err(Status::failed_precondition(format!(
                            "membership id {id} is owned by more than one shard; slot ranges overlap"
                        )));
                    }
                }
                held &= held - 1;
            }
        }
        Ok(())
    }

    pub(super) fn membership_from_filter(
        response: crate::pb::FilterBitmapResponse,
    ) -> crate::pb::MembershipBitmapResponse {
        crate::pb::MembershipBitmapResponse {
            vector_binding: None,
            base_label: response.base_label,
            label_count: response.label_count,
            bits: response.bits,
            stats_epoch: response.stats_epoch,
            stats_incarnation: response.stats_incarnation,
            visibility_fingerprint: response.visibility_fingerprint,
            visibility_columns_known: response.visibility_columns_known,
            segments_total: response.segments_total,
            segments_skipped: response.segments_skipped,
        }
    }

    pub(super) fn check_vector_binding(
        field: &str,
        actual: Option<&crate::pb::MappedVectorBinding>,
        held: &mut Option<Option<crate::pb::MappedVectorBinding>>,
    ) -> Result<(), Status> {
        crate::vector_read::check_binding(field, actual, held)
    }

    pub(super) fn check_read_view(
        &self,
        shard: usize,
        scope: &crate::visibility::VisibilityScope,
        response: &impl crate::visibility::ScopedReadResponse,
        known: &mut [bool],
    ) -> Result<StatsClaim, Status> {
        let response = response.read_view();
        scope.validate_echo(response.fingerprint, response.columns_known)?;
        let claim = StatsClaim::required(response.epoch, response.incarnation)?;
        if let Some(expected) = &self.query_read_versions {
            if expected.get(shard) != Some(&claim) {
                return Err(Status::failed_precondition(
                    "query data changed during scoped read; restart from the first page",
                ));
            }
        }
        for (held, present) in known.iter_mut().zip(response.columns_known) {
            *held |= present;
        }
        Ok(claim)
    }

    /// Resolve the live document universe, or one CEL/geo predicate, without
    /// paging through `max_k`-sized browse responses.
    pub async fn filter_membership(
        &self,
        filters: &RequestFilters,
    ) -> Result<MembershipSet, Status> {
        if let Some(fields) = &self.field_permissions {
            fields.filter(&filters.geo, filters.tree.as_ref())?;
        }
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; scope.column_count()];
        // Every shard supplies the mandatory column handshake, even when the
        // caller's predicate would prune it from the user-only membership.
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
            let request = crate::pb::FilterBitmapRequest {
                visibility: self.document_visibility.clone(),
                geo_filters: filters.geo.clone(),
                filter: Self::shard_filter_tree(filters, mask.as_ref(), shard),
            };
            let client = self.node_client(node);
            tasks.push((
                shard,
                tokio::spawn(async move {
                    client?
                        .resolve_filter_bitmap(request)
                        .await
                        .map(|response| response.into_inner())
                }),
            ));
        }
        let mut known = Self::filter_known(filters, mask.as_ref());
        let mut merged = MembershipSet {
            epochs: vec![StatsClaim::default(); self.node_addrs.len()],
            ..Default::default()
        };
        for (shard, task) in tasks {
            let response = task.await.map_err(|error| {
                Status::internal(format!("filter membership task failed: {error}"))
            })??;
            known.merge_shard(
                shard,
                &response.geo_columns_known,
                &response.filter_columns_known,
            )?;
            merged.prune.add(crate::segment_prune::PruneStats {
                segments_total: response.segments_total,
                segments_skipped: response.segments_skipped,
            });
            let response = Self::membership_from_filter(response);
            merged.epochs[shard] =
                self.check_read_view(shard, &scope, &response, &mut visibility_known)?;
            Self::merge_membership_bitmap(&mut merged, &response)?;
        }
        self.check_visibility_columns(&visibility_known)?;
        known.refuse_unknown(filters)?;
        Ok(merged)
    }

    /// Analyze one lexical clause and resolve its exact positive-score
    /// membership. No score bytes cross this phase.
    pub async fn lexical_membership(
        &self,
        text: &str,
        spec: Option<&crate::pb::AnalysisSpec>,
    ) -> Result<MembershipSet, Status> {
        if let Some(fields) = &self.field_permissions {
            fields.lexical_membership()?;
        }
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; scope.column_count()];
        let analysis_fingerprint = crate::analyzer::analysis_fingerprint(spec);
        let terms = self.analyze_terms(text, spec).await?;
        if terms.is_empty() && self.document_visibility.is_none() {
            return Ok(MembershipSet {
                epochs: vec![StatsClaim::default(); self.node_addrs.len()],
                ..Default::default()
            });
        }
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for node in &self.node_addrs {
            let client = self.node_client(node);
            let request = crate::pb::LexicalBitmapRequest {
                visibility: self.document_visibility.clone(),
                analysis_fingerprint,
                terms: terms.clone(),
            };
            tasks.push(tokio::spawn(async move {
                client?
                    .resolve_lexical_bitmap(request)
                    .await
                    .map(|response| response.into_inner())
            }));
        }
        let mut merged = MembershipSet::default();
        for (shard, task) in tasks.into_iter().enumerate() {
            let response = task.await.map_err(|error| {
                Status::internal(format!("lexical membership task failed: {error}"))
            })??;
            merged.epochs.push(self.check_read_view(
                shard,
                &scope,
                &response,
                &mut visibility_known,
            )?);
            merged.prune.add(crate::segment_prune::PruneStats {
                segments_total: response.segments_total,
                segments_skipped: response.segments_skipped,
            });
            Self::merge_membership_bitmap(&mut merged, &response)?;
        }
        self.check_visibility_columns(&visibility_known)?;
        merged.terms = terms;
        Ok(merged)
    }

    /// Resolve every live provider-backed vector row. This is the membership
    /// rule of a dense Boolean clause; native scores are fetched only for the
    /// candidates that survive the Boolean set plan.
    pub async fn vector_membership(&self, field: &str) -> Result<MembershipSet, Status> {
        if let Some(fields) = &self.field_permissions {
            fields.vector(field)?;
        }
        let scope = crate::visibility::VisibilityScope::new(self.document_visibility.as_ref())?;
        let mut visibility_known = vec![false; scope.column_count()];
        let mut tasks = Vec::with_capacity(self.node_addrs.len());
        for node in &self.node_addrs {
            let client = self.node_client(node);
            let request = crate::pb::VectorBitmapRequest {
                visibility: self.document_visibility.clone(),
                field: field.to_string(),
            };
            tasks.push(tokio::spawn(async move {
                client?
                    .resolve_vector_bitmap(request)
                    .await
                    .map(|response| response.into_inner())
            }));
        }
        let mut merged = MembershipSet::default();
        let mut held_binding = None;
        for (shard, task) in tasks.into_iter().enumerate() {
            let response = task.await.map_err(|error| {
                Status::internal(format!("vector membership task failed: {error}"))
            })??;
            Self::check_vector_binding(field, response.vector_binding.as_ref(), &mut held_binding)?;
            merged.epochs.push(self.check_read_view(
                shard,
                &scope,
                &response,
                &mut visibility_known,
            )?);
            Self::merge_membership_bitmap(&mut merged, &response)?;
        }
        self.check_visibility_columns(&visibility_known)?;
        Ok(merged)
    }
}
