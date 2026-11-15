//! Mapped-schema binding, document deletion, and atomic row replacement.

use crate::node::*;

impl NodeServiceImpl {
    pub(in crate::node) fn validate_vector_column_name(
        &self,
        name: &str,
        store: Option<&Bm25Shard>,
    ) -> Result<(), Status> {
        let configured = self
            .config
            .bm25_fields
            .iter()
            .chain(&self.config.facet_fields)
            .chain(&self.config.numeric_fields)
            .chain(&self.config.integer_fields)
            .chain(&self.config.unsigned_integer_fields)
            .chain(&self.config.map_facet_fields)
            .chain(&self.config.map_numeric_fields)
            .chain(&self.config.map_integer_fields)
            .chain(&self.config.map_unsigned_integer_fields)
            .chain(&self.config.geo_fields)
            .any(|column| column == name);
        let persisted = store.is_some_and(|store| {
            (0..store.field_count()).any(|field| store.field_name(field) == name)
                || store.numeric_index(name).is_some()
                || store.integer_index(name).is_some()
                || store.unsigned_integer_index(name).is_some()
                || store.facet_index(name).is_some()
                || store.map_numeric_index(name).is_some()
                || store.map_integer_index(name).is_some()
                || store.map_unsigned_integer_index(name).is_some()
                || store.map_facet_index(name).is_some()
                || store.geo_index(name).is_some()
        });
        if configured || persisted {
            return Err(Status::failed_precondition(format!("vector column {name:?} conflicts with a declared or stored text, scalar, map, or geo column; use distinct column names")));
        }
        Ok(())
    }

    /// Validate one mapped bind against this shard: derive the plan
    /// (derivation is deterministic, so both sides compute it
    /// independently), hold it to the client's expected fingerprint,
    /// and refuse — up front, naming every gap at once — landing
    /// columns this shard does not declare. Nothing streams until the
    /// bind stands.
    pub(in crate::node) fn bind_mapped(
        &self,
        bind: &crate::pb::MappedBind,
    ) -> Result<
        (
            crate::mapping::Extractor,
            crate::mapped_analysis::MappedAnalysis,
        ),
        Status,
    > {
        if bind.expected_fingerprint.is_empty() {
            return Err(Status::invalid_argument(
                "expected_fingerprint is required: dry-run the plan with PlanIndex first, \
                 review it, and bind the fingerprint you saw",
            ));
        }
        let extractor = crate::mapping::Extractor::with_definition(
            &bind.descriptor_set,
            &bind.message_type,
            &bind.body_path,
            bind.index_definition.as_ref(),
        )?;
        let analysis = crate::mapped_analysis::MappedAnalysis::resolve(bind, &extractor)?;
        if self
            .config
            .analysis_addr
            .as_deref()
            .map(crate::analyzer::AnalysisBackend::parse)
            .transpose()?
            == Some(crate::analyzer::AnalysisBackend::Native)
        {
            analysis.validate_native()?;
        }
        let plan = extractor.plan();
        if plan.fingerprint != bind.expected_fingerprint {
            return Err(Status::failed_precondition(format!(
                "plan fingerprint mismatch: this node derives {} but the bind expects {}; \
                 the descriptor set or the derivation rules changed since the plan was \
                 reviewed — re-run PlanIndex and review the difference",
                plan.fingerprint, bind.expected_fingerprint
            )));
        }
        self.validate_mapped_columns(&extractor)?;
        if let Some(spec) = &bind.materialize {
            self.compiled_materialize(spec)?;
        }
        // The first bind pins the shard to this plan identity (recorded to
        // the WAL now, to the store's binding entry at flush); every later
        // bind must match it exactly.
        let incoming = crate::postings::StoredBinding {
            plan_fingerprint: plan.fingerprint.clone(),
            index_contract: crate::index_contract::from_plan(plan)?,
            body_path: extractor.body_path().to_string(),
            materialize_sha: materialize_sha(bind.materialize.as_ref()),
            analysis_sha: analysis.digest.clone(),
            analysis_contract: analysis.contract.clone(),
            vector_binding: prost::Message::encode_to_vec(
                plan.vector_binding
                    .as_ref()
                    .expect("derived vector binding"),
            ),
        };
        let mut guard = write_shard(&self.state);
        guard.check_legacy_mutation()?;
        let vector = plan
            .vector_binding
            .as_ref()
            .expect("derived vector binding");
        self.validate_vector_column_name(&vector.field, guard.bm25.as_ref())?;
        Self::verify_or_record_mapped_binding(&mut guard, incoming)?;
        Ok((extractor, analysis))
    }

    fn validate_mapped_columns(&self, extractor: &crate::mapping::Extractor) -> Result<(), Status> {
        let mut missing: Vec<String> = Vec::new();
        for field in &extractor.plan().fields {
            use crate::pb::ColumnFamily;
            let name = &field.name;
            let (table, flag) = if field.family == ColumnFamily::TextField as i32 {
                if field.path == extractor.body_path() {
                    continue;
                }
                if name == "body" {
                    return Err(Status::invalid_argument(format!(
                        "mapped text field {} lands as \"body\" but is not the bound body; \
                         rename it with a hint, or bind it as body_path",
                        field.path
                    )));
                }
                (&self.config.bm25_fields, "--bm25-fields")
            } else if field.family == ColumnFamily::Facet as i32 {
                (&self.config.facet_fields, "--facet-fields")
            } else if field.family == ColumnFamily::I64 as i32 {
                (&self.config.integer_fields, "--integer-fields")
            } else if field.family == ColumnFamily::U64 as i32 {
                (
                    &self.config.unsigned_integer_fields,
                    "--unsigned-integer-fields",
                )
            } else if field.family == ColumnFamily::F64 as i32 {
                (&self.config.numeric_fields, "--numeric-fields")
            } else if field.family == ColumnFamily::MapFacet as i32 {
                (&self.config.map_facet_fields, "--map-facet-fields")
            } else if field.family == ColumnFamily::MapF64 as i32 {
                (&self.config.map_numeric_fields, "--map-numeric-fields")
            } else if field.family == ColumnFamily::MapI64 as i32 {
                (&self.config.map_integer_fields, "--map-integer-fields")
            } else if field.family == ColumnFamily::MapU64 as i32 {
                (
                    &self.config.map_unsigned_integer_fields,
                    "--map-unsigned-integer-fields",
                )
            } else {
                continue;
            };
            if self
                .config
                .derived
                .as_deref()
                .is_some_and(|declaration| declaration.is_derived(name))
            {
                return Err(Status::invalid_argument(format!(
                    "mapped field {} lands as {name:?}, a derived column this index computes \
                     from its declaration; rename the mapping with a hint",
                    field.path
                )));
            }
            if !table.iter().any(|declared| declared == name) {
                missing.push(format!("{name:?} ({flag})"));
            }
        }
        if !missing.is_empty() {
            return Err(Status::failed_precondition(format!(
                "the plan lands columns this shard does not declare: {}; declare them and \
                 restart the node, or revise the plan",
                missing.join(", ")
            )));
        }
        Ok(())
    }

    fn verify_or_record_mapped_binding(
        guard: &mut ShardState,
        incoming: crate::postings::StoredBinding,
    ) -> Result<(), Status> {
        match &guard.mapped_binding {
            Some(bound) if *bound != incoming => {
                let mut differs = Vec::new();
                if bound.plan_fingerprint != incoming.plan_fingerprint {
                    differs.push(format!(
                        "the plan (bound {}, offered {})",
                        bound.plan_fingerprint, incoming.plan_fingerprint
                    ));
                }
                if bound.body_path != incoming.body_path {
                    differs.push(format!(
                        "the body (bound {:?}, offered {:?})",
                        bound.body_path, incoming.body_path
                    ));
                }
                if bound.analysis_sha != incoming.analysis_sha {
                    differs.push("the field analysis specs".to_string());
                }
                if bound.vector_binding != incoming.vector_binding {
                    differs.push("the vector field binding".to_string());
                }
                if bound.index_contract != incoming.index_contract {
                    differs.push("the explicit index policy".to_string());
                }
                if bound.materialize_sha != incoming.materialize_sha {
                    differs.push("the materialize spec".to_string());
                }
                Err(Status::failed_precondition(format!(
                    "this shard is durably bound to another mapping; {} differ{}. An index \
                     only ever pairs with the plan it was written under — rebuild from \
                     original documents to change the mapping",
                    differs.join(" and "),
                    if differs.len() == 1 { "s" } else { "" }
                )))
            }
            Some(_) => Ok(()),
            None => Self::record_initial_binding(
                guard,
                incoming,
                "cannot bind a mapping to a populated unbound shard; rebuild from original documents",
            ),
        }
    }

    fn record_initial_binding(
        guard: &mut ShardState,
        incoming: crate::postings::StoredBinding,
        populated_err: &'static str,
    ) -> Result<(), Status> {
        if physical_rows(guard) != 0 {
            return Err(Status::failed_precondition(populated_err));
        }
        wal_append_or_degrade(
            &mut guard.wal,
            wal_record::Op::Bind(crate::pb::wal::LoggedBinding {
                plan_fingerprint: incoming.plan_fingerprint.clone(),
                body_path: incoming.body_path.clone(),
                materialize_sha: incoming.materialize_sha.clone(),
                analysis_sha: incoming.analysis_sha.clone(),
                analysis_contract: incoming.analysis_contract.clone(),
                vector_binding: incoming.vector_binding.clone(),
                index_contract: incoming.index_contract.clone(),
            }),
        );
        guard.mapped_binding = Some(incoming);
        guard.advance_stats_epoch();
        Ok(())
    }
}

// The mutation handlers' bodies against a state the caller holds (see
// `NodeServiceImpl::apply_batch_locked`).
impl NodeServiceImpl {
    /// Refuse an ID-addressed mutation whose IDs were issued under an older WAL
    /// generation before compaction or snapshot installation renumbered rows.
    pub(in crate::node) fn check_wal_generation(
        guard: &ShardState,
        expected: Option<u64>,
    ) -> Result<u64, Status> {
        let held = guard.wal.as_ref().map_or(0, WalWriter::generation);
        if expected.is_some_and(|expected| expected != held) {
            return Err(Status::failed_precondition(format!(
                "stale WAL generation: the request's ids were issued under WAL generation \
                 {} but this shard is at {held}; a compaction or snapshot install renumbered \
                 its rows since, so those ids no longer name the same documents — resolve them \
                 again",
                expected.expect("checked above")
            )));
        }
        Ok(held)
    }

    pub(crate) fn delete_documents_locked(
        &self,
        guard: &mut ShardState,
        doc_ids: &[u64],
        expected_wal_generation: Option<u64>,
    ) -> Result<DeleteDocumentsResponse, Status> {
        guard.check_legacy_mutation()?;
        let wal_generation = Self::check_wal_generation(guard, expected_wal_generation)?;
        let offset = self.config.slot_offset;
        let rows = physical_rows(guard);
        let mut slots = Vec::with_capacity(doc_ids.len());
        for id in doc_ids {
            let local = id.checked_sub(offset).ok_or_else(|| {
                Status::invalid_argument(format!("document id {id} is below shard offset {offset}"))
            })?;
            if local >= rows {
                return Err(Status::invalid_argument(format!(
                    "document id {id} is outside this shard's {rows} physical rows"
                )));
            }
            slots.push((*id, local as usize));
        }
        let mut deleted = 0u64;
        let mut already_deleted = 0u64;
        for (id, slot) in slots {
            if guard.live_docs.delete(slot) {
                deleted += 1;
                wal_append_or_degrade(
                    &mut guard.wal,
                    wal_record::Op::DeleteDocument(LoggedDeleteDocument { doc_id: id }),
                );
            } else {
                already_deleted += 1;
            }
        }
        if deleted > 0 {
            guard.advance_stats_epoch();
            guard.parents = None;
        }
        Ok(DeleteDocumentsResponse {
            deleted,
            already_deleted,
            live_revision: guard.live_docs.revision(),
            wal_generation,
        })
    }

    pub(crate) fn commit_replacements_locked(
        &self,
        guard: &mut ShardState,
        replacements: &[Replacement],
        expected_wal_generation: Option<u64>,
    ) -> Result<CommitReplacementsResponse, Status> {
        guard.check_legacy_mutation()?;
        let wal_generation = Self::check_wal_generation(guard, expected_wal_generation)?;
        let offset = self.config.slot_offset;
        let artifact_rows = active_artifact_rows(guard);
        let rows = artifact_rows.iter().copied().max().unwrap_or(0);
        let mut seen = std::collections::HashSet::new();
        let mut pairs = Vec::with_capacity(replacements.len());
        for replacement in replacements {
            if replacement.old_doc_id == replacement.new_doc_id {
                return Err(Status::invalid_argument(
                    "replacement old and new ids must differ",
                ));
            }
            if !seen.insert(replacement.old_doc_id) || !seen.insert(replacement.new_doc_id) {
                return Err(Status::invalid_argument(
                    "a replacement batch may mention each id only once",
                ));
            }
            let old = replacement.old_doc_id.checked_sub(offset).ok_or_else(|| {
                Status::invalid_argument("replacement old id is below this shard's offset")
            })?;
            let new = replacement.new_doc_id.checked_sub(offset).ok_or_else(|| {
                Status::invalid_argument("replacement new id is below this shard's offset")
            })?;
            if old >= rows || new >= rows {
                return Err(Status::invalid_argument(
                    "replacement ids must name existing rows on this shard",
                ));
            }
            if artifact_rows
                .iter()
                .any(|artifact_rows| old >= *artifact_rows || new >= *artifact_rows)
            {
                return Err(Status::failed_precondition(
                    "replacement ids must exist in every active provider, exact-vector, and document artifact",
                ));
            }
            if guard.live_docs.is_deleted(new as usize) {
                return Err(Status::failed_precondition(format!(
                    "replacement row {} is already deleted",
                    replacement.new_doc_id
                )));
            }
            pairs.push((*replacement, old as usize));
        }
        let mut committed = 0u64;
        let mut already_committed = 0u64;
        for (replacement, old) in pairs {
            if guard.live_docs.delete(old) {
                committed += 1;
                wal_append_or_degrade(
                    &mut guard.wal,
                    wal_record::Op::Replacement(LoggedReplacement {
                        old_doc_id: replacement.old_doc_id,
                        new_doc_id: replacement.new_doc_id,
                    }),
                );
            } else {
                already_committed += 1;
            }
        }
        if committed > 0 {
            guard.advance_stats_epoch();
            guard.parents = None;
        }
        Ok(CommitReplacementsResponse {
            committed,
            already_committed,
            live_revision: guard.live_docs.revision(),
            wal_generation,
        })
    }

    /// Establish or verify the mapped-plan binding on a state the caller
    /// holds: `Ok(true)` when it was already bound to exactly `incoming`,
    /// `Ok(false)` when this call bound it (logging the Bind record); a
    /// different binding, or a populated unbound shard, refuse by name.
    pub(crate) fn apply_binding_locked(
        guard: &mut ShardState,
        incoming: crate::postings::StoredBinding,
    ) -> Result<bool, Status> {
        guard.check_legacy_mutation()?;
        crate::mapped_analysis::decode_contract(
            &incoming.analysis_sha,
            &incoming.analysis_contract,
            &incoming.body_path,
        )
        .map_err(Status::invalid_argument)?;
        crate::mapped_vector::decode(&incoming.vector_binding, &incoming.plan_fingerprint)?;
        crate::index_contract::validate_binding(
            &incoming.index_contract,
            &incoming.plan_fingerprint,
            &incoming.vector_binding,
        )?;
        match guard.mapped_binding.as_ref() {
            Some(bound) if *bound != incoming => Err(Status::failed_precondition(format!(
                "replica is bound to plan {} body {:?}, source WAL requires plan {} body {:?}",
                bound.plan_fingerprint,
                bound.body_path,
                incoming.plan_fingerprint,
                incoming.body_path
            ))),
            Some(_) => Ok(true),
            None => {
                Self::record_initial_binding(
                    guard,
                    incoming,
                    "cannot apply a WAL mapping binding to a populated unbound shard; install the matching base snapshot",
                )?;
                Ok(false)
            }
        }
    }
}
