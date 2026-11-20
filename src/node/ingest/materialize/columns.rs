//! CEL column compilation, placement verification, and derived-column evaluation.

use crate::node::*;

impl NodeServiceImpl {
    /// Compile (or fetch the cached compilation of) one ingest
    /// materialization spec. Validation is whole-spec: empty or
    /// duplicate names, an unset kind, or an expression that does not
    /// compile all refuse before any document is touched.
    pub(in crate::node) fn compiled_materialize(
        &self,
        spec: &crate::pb::MaterializeSpec,
    ) -> Result<Vec<(String, crate::pb::ValueExpr, crate::pb::MaterializeKind)>, Status> {
        let mut cache = self
            .materialize_cache
            .lock()
            .expect("materialize cache lock poisoned");
        if let Some(compiled) = cache.as_ref() {
            if compiled.spec == *spec {
                return Ok(compiled.columns.clone());
            }
        }
        let columns = compile_materialize_spec(spec)?;
        if let Some(declaration) = self.config.derived.as_deref() {
            for (name, _, _) in &columns {
                if declaration.is_derived(name) {
                    return Err(Status::invalid_argument(format!(
                        "materialize: column {name:?} is a derived column of this index's \
                         declaration, which the index computes itself; a request's spec \
                         cannot redefine it"
                    )));
                }
            }
        }
        let sources = source_tables(&self.config);
        let types = sources.types();
        for ((name, expr, kind), source) in columns.iter().zip(&spec.columns) {
            let (_, vt) = crate::values::resolve(expr, &types).map_err(|e| {
                Status::invalid_argument(format!("materialize: column {name:?}: {}", e.message()))
            })?;
            let expected = match kind {
                crate::pb::MaterializeKind::F64 => crate::values::ValueType::Double,
                crate::pb::MaterializeKind::I64 => crate::values::ValueType::Int,
                crate::pb::MaterializeKind::U64 => crate::values::ValueType::Uint,
                _ => unreachable!("compile_materialize_spec checked the kind"),
            };
            if vt != crate::values::ValueType::Unknown && vt != expected {
                let hint = if vt == crate::values::ValueType::Bool {
                    "wrap the boolean in a ternary that yields the target numeric type"
                } else {
                    "align the target kind with the expression; use double(...) for an explicit F64 conversion"
                };
                return Err(Status::invalid_argument(format!(
                    "materialize: column {name:?}, expression {:?}, declares {kind:?} but resolves to {vt:?}; {hint}", source.expression
                )));
            }
        }
        *cache = Some(CompiledMaterialize {
            spec: spec.clone(),
            columns: columns.clone(),
        });
        Ok(columns)
    }

    /// Verify pinned placement-leaf predicates against a live-ingest document
    /// (including value-dialect materialization and index-declared derived columns).
    pub(in crate::node) fn check_placement(
        &self,
        doc: &AddDocumentsRequest,
        stable_key: Option<&[u8]>,
        logged: bool,
    ) -> Result<(), Status> {
        let Some(pinned) = self.config.placement_tree.as_ref() else {
            return Ok(());
        };
        let mut routed = doc.clone();
        if let Some(spec) = doc
            .materialize
            .as_ref()
            .filter(|spec| !spec.columns.is_empty())
        {
            let compiled = self.compiled_materialize(spec)?;
            routed.materialize = None;
            routed = apply_materialize(routed, &compiled)?;
        }
        if let Some(declaration) = self.config.derived.as_deref() {
            if logged {
                declaration.check_logged(&routed)?;
            } else if routed.derived_fingerprint.is_empty() {
                routed = declaration.apply(routed, stable_key)?;
            }
        }
        let outcome = pinned.check(&routed);
        outcome.map_err(|reason| {
            Status::invalid_argument(format!(
                "{reason}; this shard pins leaf {} (--placement-leaf) under the tree in \
                 --placement-tree, so route the document through the coordinator or send it \
                 to a shard of its leaf",
                pinned.leaf().code
            ))
        })
    }

    /// Enforce placement-column assignment before applying `doc`: populate from
    /// `placement_leaf` when absent, reject mismatched leaf codes, or require an
    /// explicit code when `placement_column` is declared without a pinned leaf.
    pub(in crate::node) fn place_document(
        &self,
        mut doc: AddDocumentsRequest,
    ) -> Result<AddDocumentsRequest, Status> {
        let Some(column) = self.config.placement_column.as_deref() else {
            return Ok(doc);
        };
        let carried = doc
            .integers
            .iter()
            .find(|value| value.field == column)
            .map(|value| value.value);
        match (self.config.placement_leaf, carried) {
            (Some(code), None) => {
                doc.integers.push(crate::pb::IntegerValue {
                    field: column.to_string(),
                    value: code,
                });
            }
            (Some(code), Some(other)) if other != code => {
                return Err(Status::invalid_argument(format!(
                    "placement column {column:?} carries code {other}, but this shard serves \
                     placement leaf {code} (--placement-leaf); route the document through the \
                     coordinator or send it to the shard that serves {other}"
                )));
            }
            (None, None) => {
                return Err(Status::invalid_argument(format!(
                    "placement column {column:?} has no value on this document and this shard \
                     pins no leaf (--placement-leaf); route the document through the \
                     coordinator, which evaluates the placement tree, or pin the leaf"
                )));
            }
            _ => {}
        }
        Ok(doc)
    }

    /// Evaluate request and index-declared derived columns into `doc`'s typed
    /// value lists, clearing `doc.materialize` so WAL replay never re-evaluates.
    pub(in crate::node) fn materialize_columns(
        &self,
        mut doc: AddDocumentsRequest,
        stable_key: Option<&[u8]>,
        logged: bool,
    ) -> Result<AddDocumentsRequest, Status> {
        if let Some(spec) = doc
            .materialize
            .take()
            .filter(|spec| !spec.columns.is_empty())
        {
            let compiled = self.compiled_materialize(&spec)?;
            doc = apply_materialize(doc, &compiled)?;
        }
        // Compute and stamp index-declared derived columns on fresh documents,
        // or verify the recorded fingerprint on replayed records.
        if let Some(declaration) = self.config.derived.as_deref() {
            if logged {
                declaration.check_logged(&doc)?;
            } else if doc.derived_fingerprint.is_empty() {
                doc = declaration.apply(doc, stable_key)?;
            }
        } else if !doc.derived_fingerprint.is_empty() {
            return Err(Status::failed_precondition(
                "the document carries a derived fingerprint, but this index declares no derived columns",
            ));
        }
        Ok(doc)
    }
}

/// Validate and compile one materialize spec, refusing empty or duplicate names,
/// unset target kinds, or invalid CEL expressions before mutating state.
pub(crate) fn compile_materialize_spec(
    spec: &crate::pb::MaterializeSpec,
) -> Result<Vec<(String, crate::pb::ValueExpr, crate::pb::MaterializeKind)>, Status> {
    let mut names = std::collections::HashSet::new();
    let mut columns = Vec::with_capacity(spec.columns.len());
    for column in &spec.columns {
        if column.name.is_empty() {
            return Err(Status::invalid_argument(
                "materialize: a derived column needs a non-empty name",
            ));
        }
        if !names.insert(column.name.as_str()) {
            return Err(Status::invalid_argument(format!(
                "materialize: duplicate derived column {:?}",
                column.name
            )));
        }
        let kind = match crate::pb::MaterializeKind::try_from(column.kind) {
            Ok(crate::pb::MaterializeKind::F64) => crate::pb::MaterializeKind::F64,
            Ok(crate::pb::MaterializeKind::I64) => crate::pb::MaterializeKind::I64,
            Ok(crate::pb::MaterializeKind::U64) => crate::pb::MaterializeKind::U64,
            _ => {
                return Err(Status::invalid_argument(format!(
                    "materialize: column {:?} declares no target kind; kinds are \
                     explicit (MATERIALIZE_KIND_F64, MATERIALIZE_KIND_I64, or MATERIALIZE_KIND_U64), \
                     never inferred from data",
                    column.name
                )))
            }
        };
        let expr = crate::cel::compile_value(&column.expression).map_err(|e| {
            Status::invalid_argument(format!(
                "materialize: column {:?}: {}",
                column.name,
                e.message()
            ))
        })?;
        columns.push((column.name.clone(), expr, kind));
    }
    Ok(columns)
}

/// Evaluate compiled materialized columns over one document's own
/// values and push the results into its `numerics` / `integers`
/// lists. Shared by node ingest and the coordinator's placement
/// evaluation, so a placement predicate on a derived column sees the
/// value the shard will store.
pub(crate) fn apply_materialize(
    doc: AddDocumentsRequest,
    compiled: &[(String, crate::pb::ValueExpr, crate::pb::MaterializeKind)],
) -> Result<AddDocumentsRequest, Status> {
    let env = crate::derived::ingest_env(&doc, None)?;
    crate::derived::materialize_into(doc, compiled, &env, false)
}
