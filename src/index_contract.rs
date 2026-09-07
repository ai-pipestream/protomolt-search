//! Durable explicit indexing policy, independent of physical row addresses.

use std::collections::HashSet;

use prost::Message;
use tonic::Status;

use crate::pb::{
    IndexDefinition, MappedIndexContract, MappedKind as K, MappedPlan, MappedRole as R,
};

fn invalid(message: &str) -> Status {
    Status::invalid_argument(format!("index contract: {message}"))
}

/// Check policy structure without claiming descriptor/type compatibility.
/// The planner additionally resolves every path and validates its declared type.
pub(crate) fn validate_definition(definition: &IndexDefinition) -> Result<(), Status> {
    let mut previous: Option<&[u32]> = None;
    let mut names = HashSet::new();
    let mut ids = Vec::new();
    let mut vectors = Vec::new();
    let mut chunks = Vec::new();
    let mut chunk_ids = Vec::new();
    for projection in &definition.projections {
        let path = projection.field_numbers.as_slice();
        if path.is_empty()
            || path.len() > 9
            || path
                .iter()
                .any(|&n| n == 0 || n > 536_870_911 || (19_000..20_000).contains(&n))
            || previous.is_some_and(|p| p >= path)
        {
            return Err(invalid(
                "field-number paths must be valid, unique and sorted",
            ));
        }
        previous = Some(path);
        let kind = K::try_from(projection.kind).map_err(|_| invalid("unknown projection kind"))?;
        let role = R::try_from(projection.role).map_err(|_| invalid("unknown projection role"))?;
        if kind == K::Unspecified || (kind == K::Vector) != (projection.vector_dims > 0) {
            return Err(invalid(
                "explicit kinds and vector-only positive dimensions are required",
            ));
        }
        if role == R::Chunks {
            if kind != K::Nested || !projection.column_name.is_empty() {
                return Err(invalid("CHUNKS requires NESTED and no column name"));
            }
            chunks.push(path);
        } else {
            if matches!(kind, K::Object | K::Nested | K::Binary)
                || projection.column_name.trim().is_empty()
                || !names.insert(projection.column_name.as_str())
            {
                return Err(invalid(
                    "value projections require supported kinds and unique nonempty column names",
                ));
            }
        }
        if matches!(role, R::DocId | R::ChunkId) {
            if !matches!(
                kind,
                K::Keyword | K::Int32 | K::Int64 | K::Uint32 | K::Uint64
            ) {
                return Err(invalid(
                    "identity roles require keyword or integer projections",
                ));
            }
            if role == R::DocId {
                ids.push(path);
            } else {
                chunk_ids.push(path);
            }
        }
        if kind == K::Vector {
            if role != R::None
                || matches!(
                    projection.column_name.as_str(),
                    "body" | "parent_id" | "group_id"
                )
            {
                return Err(invalid(
                    "vector projection conflicts with a structural role or built-in column",
                ));
            }
            vectors.push(path);
        }
    }
    if ids.len() != 1 || vectors.len() != 1 || chunks.len() > 1 || chunk_ids.len() > 1 {
        return Err(invalid(
            "one DOC_ID, one VECTOR and at most one CHUNKS/CHUNK_ID are required",
        ));
    }
    if let Some(chunks) = chunks.first() {
        if ids[0].starts_with(chunks)
            || !vectors[0].starts_with(chunks)
            || chunk_ids.iter().any(|path| !path.starts_with(chunks))
        {
            return Err(invalid("identity/vector paths contradict the chunk scope"));
        }
    } else if !chunk_ids.is_empty() {
        return Err(invalid("CHUNK_ID requires a CHUNKS scope"));
    }
    for pair in definition.projections.windows(2) {
        if pair[1].field_numbers.starts_with(&pair[0].field_numbers)
            && pair[0].role != R::Chunks as i32
        {
            return Err(invalid(
                "a value projection cannot contain another projection",
            ));
        }
    }
    Ok(())
}

pub fn from_plan(plan: &MappedPlan) -> Result<Vec<u8>, Status> {
    let Some(definition) = &plan.index_definition else {
        return Ok(Vec::new());
    };
    let bytes = MappedIndexContract {
        format_version: 1,
        message_type: plan.message_type.clone(),
        plan_fingerprint: plan.fingerprint.clone(),
        index_definition: Some(definition.clone()),
    }
    .encode_to_vec();
    decode(&bytes, &plan.fingerprint)?;
    Ok(bytes)
}

/// Empty is a legacy inferred policy. Nonempty data must be canonical and bind
/// the same plan fingerprint as its containing storage or replication record.
pub fn decode(bytes: &[u8], fingerprint: &str) -> Result<Option<MappedIndexContract>, Status> {
    if bytes.is_empty() {
        return Ok(None);
    }
    let contract = MappedIndexContract::decode(bytes).map_err(|_| invalid("malformed protobuf"))?;
    if contract.format_version != 1 {
        return Err(Status::failed_precondition(
            "unsupported index contract version",
        ));
    }
    if !crate::mapped_analysis::valid_digest(fingerprint)
        || contract.plan_fingerprint != fingerprint
    {
        return Err(invalid(
            "plan fingerprint does not match the containing binding",
        ));
    }
    if contract.message_type.split('.').any(|part| {
        let mut chars = part.chars();
        !chars
            .next()
            .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
            || !chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
    }) {
        return Err(invalid(
            "a fully qualified protobuf message type is required",
        ));
    }
    let definition = contract
        .index_definition
        .as_ref()
        .ok_or_else(|| invalid("missing definition"))?;
    validate_definition(definition)?;
    if contract.encode_to_vec() != bytes {
        return Err(invalid("noncanonical encoding"));
    }
    Ok(Some(contract))
}

/// Verify the independent policy and vector declarations agree inside a binding.
pub fn validate_binding(
    bytes: &[u8],
    fingerprint: &str,
    vector_bytes: &[u8],
) -> Result<Option<MappedIndexContract>, Status> {
    let Some(contract) = decode(bytes, fingerprint)? else {
        return Ok(None);
    };
    let vector = crate::mapped_vector::decode(vector_bytes, fingerprint)?
        .ok_or_else(|| invalid("explicit policy requires a vector binding"))?;
    let projection = contract
        .index_definition
        .as_ref()
        .unwrap()
        .projections
        .iter()
        .find(|p| p.kind == K::Vector as i32)
        .expect("validated vector projection");
    if projection.column_name != vector.field
        || projection.vector_dims != vector.declared_dimensions
    {
        return Err(invalid(
            "vector declaration contradicts the explicit policy",
        ));
    }
    Ok(Some(contract))
}
