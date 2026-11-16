//! Read-path admission over one shard guard: vector bindings, receipts,
//! scan filters and query-analysis checks.

use super::*;

impl ShardState {
    /// Resolve from the durable generation, never from a caller's field grant.
    pub(in crate::node) fn vector_binding(
        &self,
        field: &str,
    ) -> Result<Option<crate::pb::MappedVectorBinding>, Status> {
        let binding = self
            .mapped_binding
            .as_ref()
            .map(|stored| {
                crate::mapped_vector::decode(&stored.vector_binding, &stored.plan_fingerprint)
                    .map_err(|error| {
                        Status::failed_precondition(format!(
                            "invalid stored vector binding: {}",
                            error.message()
                        ))
                    })
            })
            .transpose()?
            .flatten();
        if !field.is_empty() && binding.as_ref().is_none_or(|held| held.field != field) {
            return Err(Status::failed_precondition(
                "requested vector field is not bound on this generation",
            ));
        }
        if let Some(binding) = &binding {
            if binding.declared_dimensions != 0 {
                for dim in [
                    self.index.as_ref().and_then(|index| index.dim_opt()),
                    self.exact_vectors.as_ref().and_then(|exact| exact.dim()),
                ]
                .into_iter()
                .flatten()
                {
                    if dim != binding.declared_dimensions as usize {
                        return Err(Status::failed_precondition(
                            "vector shape disagrees with its durable field binding",
                        ));
                    }
                }
            }
        }
        Ok(binding)
    }

    /// Capture proof of the physical view before any scan data is emitted.
    pub(in crate::node) fn vector_read_receipt(
        &self,
        context: Option<&crate::pb::VectorReadContext>,
    ) -> Result<Option<crate::pb::VectorReadReceipt>, Status> {
        let Some(context) = context else {
            return Ok(None);
        };
        let scope = crate::visibility::VisibilityScope::new(context.visibility.as_ref())?;
        self.check_stats_epoch(
            context.expected_stats_epoch,
            &context.expected_stats_incarnation,
        )?;
        let (_, visibility_columns_known) = filter_known_flags(
            self.bm25.as_ref(),
            &[],
            context
                .visibility
                .as_ref()
                .and_then(|view| view.filter.as_ref()),
        );
        Ok(Some(crate::pb::VectorReadReceipt {
            vector_binding: self.vector_binding(&context.field)?,
            stats_epoch: self.stats_epoch,
            stats_incarnation: self.stats_incarnation.bytes()?,
            visibility_fingerprint: scope.fingerprint().to_vec(),
            visibility_columns_known,
        }))
    }

    pub(in crate::node) fn vector_scan_filters(
        &self,
        rows: usize,
        geo_filters: &[crate::pb::GeoFilter],
        geo_regions: &[crate::geo::GeoRegion],
        user: Option<&crate::pb::FilterExpr>,
        context: Option<&crate::pb::VectorReadContext>,
        prune: bool,
    ) -> Result<(ResolvedShardFilters, Option<crate::pb::VectorReadReceipt>), Status> {
        let receipt = self.vector_read_receipt(context)?;
        let visibility = context.and_then(|context| context.visibility.as_ref());
        let combined = crate::visibility::intersect_filter(visibility, user.cloned())?;
        let (filter, mut allow, stats) = resolve_shard_filters(
            self.bm25.as_ref(),
            self.live_docs.words(),
            rows,
            geo_filters,
            geo_regions,
            combined.as_ref(),
            prune,
            None,
        )?;
        if visibility.is_some() {
            // An absent authority column is not permission to include a vector
            // with no document metadata, even for an IS MISSING predicate.
            let documents = self
                .bm25
                .as_ref()
                .map_or(0, |store| store.next_doc_id() as usize);
            let allow = allow.get_or_insert_with(|| crate::boolean_bits::Bits::full(rows));
            allow.clear_range(documents, rows);
        }
        Ok(((filter, allow, stats), receipt))
    }

    /// Evaluate authority predicates only for requested live slots. Candidate
    /// bookkeeping stays proportional to the request, not the whole corpus.
    pub(in crate::node) fn visible_candidate_slots(
        &self,
        visibility: Option<&crate::pb::DocumentVisibility>,
        ids: &[u64],
        offset: u64,
        rows: usize,
    ) -> Result<Vec<usize>, Status> {
        let restricted = visibility.is_some();
        let store = self.bm25.as_ref();
        if restricted && store.is_some_and(|store| store.as_index().is_none()) {
            return Err(Status::failed_precondition(
                "bm25 bulk build in progress; Flush before scoped candidate scoring",
            ));
        }
        let predicate = visibility.and_then(|view| view.filter.as_ref());
        let filter = crate::filter::DocFilter {
            pred: match (predicate, store) {
                (Some(predicate), Some(store)) => Some(store.resolve_filter(predicate)?),
                _ => None,
            },
            ..Default::default()
        };
        let mut seen = std::collections::HashSet::new();
        Ok(ids
            .iter()
            .filter_map(|&id| {
                let slot = usize::try_from(id.checked_sub(offset)?).ok()?;
                if slot >= rows || self.live_docs.is_deleted(slot) || !seen.insert(slot) {
                    return None;
                }
                if restricted
                    && store.is_none_or(|store| {
                        slot >= store.next_doc_id() as usize
                            || !filter.passes(slot as u32, &ShardNumericRead(store))
                    })
                {
                    return None;
                }
                Some(slot)
            })
            .collect())
    }

    /// Check under the postings read guard. A binding also pins fields with
    /// no rows yet; a field name alone does not establish term identity.
    pub(in crate::node) fn check_query_analysis(
        &self,
        field: &str,
        offered: u64,
    ) -> Result<(), Status> {
        let explicit = self
            .mapped_binding
            .as_ref()
            .filter(|binding| !binding.analysis_sha.is_empty());
        let bound = match explicit {
            Some(binding) => crate::mapped_analysis::decode_contract(
                &binding.analysis_sha,
                &binding.analysis_contract,
                &binding.body_path,
            )
            .map_err(Status::failed_precondition)?
            .fields
            .into_iter()
            .find(|column| column.name == field)
            .map(|column| crate::analyzer::analysis_fingerprint(column.analysis.as_ref())),
            None => None,
        };
        let held = bound
            .or_else(|| {
                self.bm25.as_ref().and_then(|store| {
                    store
                        .field_index(field)
                        .map(|fi| store.analysis_fingerprint(fi))
                })
            })
            .unwrap_or(0);
        if held != 0 && ((explicit.is_some() && offered == 0) || (offered != 0 && held != offered))
        {
            let reason = if offered == 0 {
                "explicit mapped analysis cannot be omitted"
            } else {
                "the two score different term identities"
            };
            return Err(Status::failed_precondition(format!(
                "field {field:?} was built with analyzer fingerprint {held:#x} but the query's terms were analyzed under {offered:#x}; {reason}"
            )));
        }
        Ok(())
    }
}
