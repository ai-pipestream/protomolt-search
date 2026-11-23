//! The projection contract between shards and the coordinator: declared
//! types and row validation.

use super::ValuesError;

use crate::pb;

/// The actual projected scalar, with absence represented by UNSPECIFIED.
pub fn projected_type(value: &pb::ProjectedValue) -> pb::ScalarValueType {
    use pb::projected_value::Value as V;
    match &value.value {
        None => pb::ScalarValueType::Unspecified,
        Some(V::IntValue(_)) => pb::ScalarValueType::Integer,
        Some(V::UintValue(_)) => pb::ScalarValueType::UnsignedInteger,
        Some(V::DoubleValue(_)) => pb::ScalarValueType::Number,
        Some(V::StringValue(_)) => pb::ScalarValueType::Text,
        Some(V::BoolValue(_)) => pb::ScalarValueType::Boolean,
    }
}

/// Validate and merge one shard's declared projection types. Metadata is
/// required even when the shard contributes no rows.
pub fn merge_projection_types(
    projections: &[pb::CompiledProjection],
    accumulated: &mut [pb::ScalarValueType],
    declared: &[i32],
) -> Result<(), ValuesError> {
    if declared.len() != projections.len() || accumulated.len() != projections.len() {
        return Err(ValuesError::failed_precondition(
            "shard omitted projection type metadata; use matching server builds",
        ));
    }
    for ((projection, acc), &raw) in projections.iter().zip(accumulated).zip(declared) {
        let ty = pb::ScalarValueType::try_from(raw)
            .map_err(|_| ValuesError::failed_precondition("unknown projection type from shard"))?;
        if ty != pb::ScalarValueType::Unspecified {
            if *acc != pb::ScalarValueType::Unspecified && *acc != ty {
                return Err(ValuesError::failed_precondition(format!(
                    "projection {:?} has incompatible types across shards: {:?} and {:?}",
                    projection.name, acc, ty
                )));
            }
            *acc = ty;
        }
    }
    Ok(())
}

/// Check rows against the originating shard's declaration before merging.
/// Absence is legal for every type; a present value needs an exact match.
pub fn validate_projection_row(
    values: &[pb::ProjectedValue],
    declared: &[i32],
) -> Result<(), ValuesError> {
    if values.len() != declared.len() {
        return Err(ValuesError::failed_precondition(
            "shard returned a projection row with the wrong width",
        ));
    }
    for (value, &raw) in values.iter().zip(declared) {
        let ty = pb::ScalarValueType::try_from(raw)
            .map_err(|_| ValuesError::failed_precondition("unknown projection type from shard"))?;
        let actual = projected_type(value);
        if actual != pb::ScalarValueType::Unspecified && actual != ty {
            return Err(ValuesError::failed_precondition(
                "shard projected value disagrees with its declared type",
            ));
        }
    }
    Ok(())
}
