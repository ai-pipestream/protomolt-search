//! Compile occurrence-specific policy without mutating source descriptors.

use std::collections::BTreeMap;

use super::*;

type Projections<'d> = BTreeMap<Vec<u32>, &'d pb::IndexProjection>;

pub(super) fn derive(
    definition: &pb::IndexDefinition,
    root: &MsgEntry<'_>,
    index: &TypeIndex<'_>,
) -> Result<(Vec<pb::MappedField>, pb::IndexDefinition), MappingError> {
    let projections = collect_projections(definition)?;
    let mut fields = Vec::new();
    for (numbers, projection) in &projections {
        project_path(
            numbers,
            projection,
            &projections,
            (root, index),
            &mut fields,
        )?;
    }
    require_one_doc_id_and_vector(&fields)?;
    let canonical = pb::IndexDefinition {
        projections: projections.into_values().cloned().collect(),
    };
    crate::index_contract::validate_definition(&canonical).map_err(MappingError::foreign)?;
    Ok((fields, canonical))
}

/// Key every projection by its field-number path, refusing bad lengths and duplicates.
fn collect_projections(definition: &pb::IndexDefinition) -> Result<Projections<'_>, MappingError> {
    let mut projections = BTreeMap::new();
    for projection in &definition.projections {
        let path = &projection.field_numbers;
        if path.is_empty() || path.len() > MAX_DEPTH + 1 {
            return Err(refuse(format!(
                "index definition path {path:?} must contain 1 through {} field numbers",
                MAX_DEPTH + 1
            )));
        }
        if projections.insert(path.clone(), projection).is_some() {
            return Err(refuse(format!("duplicate index definition path {path:?}")));
        }
    }
    Ok(projections)
}

/// Resolve one projection's path from the root message and plan its leaf field.
fn project_path<'a>(
    numbers: &[u32],
    projection: &pb::IndexProjection,
    projections: &Projections<'_>,
    (root, index): (&'a MsgEntry<'a>, &'a TypeIndex<'a>),
    fields: &mut Vec<pb::MappedField>,
) -> Result<(), MappingError> {
    let mut entry = root;
    let mut names = Vec::new();
    for (depth, &number) in numbers.iter().enumerate() {
        let field = entry
            .desc
            .field
            .iter()
            .find(|f| f.number() as u32 == number)
            .ok_or_else(|| {
                refuse(format!(
                    "index definition path {numbers:?}: field {number} is absent from {}",
                    entry.full
                ))
            })?;
        names.push(field.name());
        let path = names.join(".");
        let field_shape = shape(field, index, &path)?;
        if depth + 1 < numbers.len() {
            let prefix = &numbers[..=depth];
            let parent = projections.get(prefix);
            let chunks = parent.is_some_and(|p| p.role == pb::MappedRole::Chunks as i32);
            if parent.is_some() && !chunks {
                return Err(refuse_at(
                    &path,
                    "a projected value cannot also contain another projection",
                ));
            }
            match field_shape {
                Shape::Message { entry: child, map: false, ref full }
                    if !well_known_leaf(full) && (!is_repeated(field) || chunks) => {
                        entry = child;
                        continue;
                    }
                _ => return Err(refuse_at(&path,
                    "projection traversal requires an ordinary singular message or an explicit CHUNKS container; maps, scalar values and unscoped repeated messages cannot be flattened")),
            }
        }
        fields.push(leaf_field(projection, field, &field_shape, &path)?);
    }
    Ok(())
}

/// Validate the projection's declared kind and role against its resolved field.
fn leaf_field(
    projection: &pb::IndexProjection,
    field: &prost_types::FieldDescriptorProto,
    field_shape: &Shape<'_>,
    path: &str,
) -> Result<pb::MappedField, MappingError> {
    let kind = pb::MappedKind::try_from(projection.kind)
        .map_err(|_| refuse_at(path, "index definition has an unknown kind"))?;
    let role = pb::MappedRole::try_from(projection.role)
        .map_err(|_| refuse_at(path, "index definition has an unknown role"))?;
    if kind == pb::MappedKind::Unspecified {
        return Err(refuse_at(
            path,
            "index definition requires an explicit kind",
        ));
    }
    if (kind == pb::MappedKind::Vector) != (projection.vector_dims > 0) {
        return Err(refuse_at(
            path,
            "VECTOR requires a positive dimension; other kinds require vector_dims = 0",
        ));
    }
    if role == pb::MappedRole::Chunks {
        if kind != pb::MappedKind::Nested || !projection.column_name.is_empty() {
            return Err(refuse_at(
                path,
                "CHUNKS requires NESTED and no physical column name",
            ));
        }
    } else if projection.column_name.is_empty() {
        return Err(refuse_at(
            path,
            "a value projection requires a physical column name",
        ));
    }
    let hint = ResolvedHint {
        explicit_kind: true,
        role,
        vector_dims: projection.vector_dims,
        ..plain_hint(kind)
    };
    validate_hint(field, field_shape, &hint, path)?;
    let mapped = planned(path, &projection.column_name, field, field_shape, &hint);
    if mapped.family == pb::ColumnFamily::None as i32 && role != pb::MappedRole::Chunks {
        return Err(refuse_at(path,
            "requested value projection has no storage/query representation; omit it to retain the field in the source"));
    }
    Ok(mapped)
}

fn require_one_doc_id_and_vector(fields: &[pb::MappedField]) -> Result<(), MappingError> {
    if fields
        .iter()
        .filter(|f| f.role == pb::MappedRole::DocId as i32)
        .count()
        != 1
    {
        return Err(refuse(
            "index definition requires exactly one explicit DOC_ID",
        ));
    }
    if fields
        .iter()
        .filter(|f| f.kind == pb::MappedKind::Vector as i32)
        .count()
        != 1
    {
        return Err(refuse(
            "index definition requires exactly one explicit VECTOR",
        ));
    }
    Ok(())
}
