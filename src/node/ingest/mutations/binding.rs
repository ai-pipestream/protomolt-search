//! Mapped-schema binding: plan validation and the durable binding record.

use crate::node::*;

impl NodeServiceImpl {
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
        guard.check_index_owned_write()?;
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

    /// Establish or verify the mapped-plan binding on a state the caller
    /// holds: `Ok(true)` when it was already bound to exactly `incoming`,
    /// `Ok(false)` when this call bound it (logging the Bind record); a
    /// different binding, or a populated unbound shard, refuse by name.
    pub(crate) fn apply_binding_locked(
        guard: &mut ShardState,
        incoming: crate::postings::StoredBinding,
    ) -> Result<bool, Status> {
        guard.check_index_owned_write()?;
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
