//! Canonical identity of the vector plane in a descriptor-derived indexing plan.
use crate::pb::{ColumnFamily, MappedKind, MappedPlan, MappedVectorBinding};
use prost::Message;
use tonic::Status;

const FORMAT_VERSION: u32 = 1;

/// Derive the exact indexed name and source path. A name is not an alias for a
/// different plane: collisions and built-in text/lineage names are invalid.
pub fn from_plan(plan: &MappedPlan) -> Result<MappedVectorBinding, Status> {
    let mut vectors = plan.fields.iter().filter(|field| {
        field.family == ColumnFamily::Vector as i32 || field.kind == MappedKind::Vector as i32
    });
    let vector = vectors
        .next()
        .ok_or_else(|| Status::invalid_argument("mapped plan has no vector field"))?;
    if !vector.repeated
        || vectors.next().is_some()
        || vector.family != ColumnFamily::Vector as i32
        || vector.kind != MappedKind::Vector as i32
        || vector.path != plan.vector_path
        || vector.vector_dims != plan.dim
    {
        return Err(Status::invalid_argument(
            "mapped plan has contradictory vector identity",
        ));
    }
    if plan.fields.iter().any(|field| {
        field.path != vector.path
            && field.family != ColumnFamily::None as i32
            && field.name == vector.name
    }) {
        return Err(Status::invalid_argument(
            "mapped vector name collides with another indexed column",
        ));
    }
    let binding = MappedVectorBinding {
        format_version: FORMAT_VERSION,
        field: vector.name.clone(),
        source_path: vector.path.clone(),
        declared_dimensions: plan.dim,
        plan_fingerprint: plan.fingerprint.clone(),
    };
    validate(&binding, &plan.fingerprint)?;
    Ok(binding)
}

/// Validate a decoded declaration against the fingerprint of its containing
/// binding. Only a trusted derivation or durable binding establishes ownership.
pub fn validate(binding: &MappedVectorBinding, expected_fingerprint: &str) -> Result<(), Status> {
    if binding.format_version != FORMAT_VERSION {
        return Err(Status::failed_precondition(
            "unsupported mapped vector binding version",
        ));
    }
    if binding.field.trim().is_empty()
        || binding.source_path.trim().is_empty()
        || matches!(binding.field.as_str(), "body" | "parent_id" | "group_id")
    {
        return Err(Status::invalid_argument(
            "mapped vector binding requires a distinct vector column and source path",
        ));
    }
    if !crate::mapped_analysis::valid_digest(&binding.plan_fingerprint)
        || binding.plan_fingerprint != expected_fingerprint
    {
        return Err(Status::failed_precondition(
            "mapped vector binding does not match the plan fingerprint",
        ));
    }
    Ok(())
}

/// Canonical codec for persistence and control-plane handoff. Empty means no
/// declaration; callers must not treat it as an implicit vector-field grant.
pub fn decode(
    bytes: &[u8],
    expected_fingerprint: &str,
) -> Result<Option<MappedVectorBinding>, Status> {
    if bytes.is_empty() {
        return Ok(None);
    }
    let binding = MappedVectorBinding::decode(bytes)
        .map_err(|_| Status::invalid_argument("malformed mapped vector binding"))?;
    validate(&binding, expected_fingerprint)?;
    if binding.encode_to_vec() != bytes {
        return Err(Status::invalid_argument(
            "mapped vector binding is not canonical",
        ));
    }
    Ok(Some(binding))
}
