//! Refusal of hints that cannot apply to the field they sit on.

use super::MappingError;
use prost_types::{DescriptorProto, FieldDescriptorProto};

use super::hint_resolve::{is_repeated, wrapper_kind, ResolvedHint, Shape};
use super::refuse_at;
use super::walk::family;
use crate::pb;

/// Hints that cannot possibly apply to the field they sit on fail
/// here, with the path, before any plan is returned.
pub(super) fn validate_hint(
    field: &FieldDescriptorProto,
    shape: &Shape<'_>,
    hint: &ResolvedHint,
    path: &str,
) -> Result<(), MappingError> {
    use prost_types::field_descriptor_proto::Type;
    if hint.kind == pb::MappedKind::Date
        && !matches!(shape, Shape::Message { full, map: false, .. } if full == "google.protobuf.Timestamp")
    {
        return Err(refuse_at(
            path,
            "a DATE hint requires a google.protobuf.Timestamp field",
        ));
    }
    if hint.kind == pb::MappedKind::Date && !is_repeated(field) {
        if let Shape::Message { entry, .. } = shape {
            validate_timestamp_descriptor(entry.desc, path)?;
        }
    }
    if family(hint.kind, is_repeated(field)) != pb::ColumnFamily::None {
        if let Shape::Message { full, entry, .. } = shape {
            if let Some(kind) = wrapper_kind(full) {
                if field.r#type() != Type::Message {
                    return Err(refuse_at(path, "a scalar wrapper must be a message field"));
                }
                validate_wrapper_descriptor(entry.desc, kind, path)?;
            }
        }
    }
    if hint.kind == pb::MappedKind::Vector {
        let element_ok = matches!(shape, Shape::Scalar(Type::Float | Type::Double));
        if !is_repeated(field) || !element_ok {
            return Err(refuse_at(
                path,
                "a VECTOR hint requires a repeated float or repeated double field",
            ));
        }
    }
    if hint.role == pb::MappedRole::Chunks {
        let non_map_message = matches!(shape, Shape::Message { map: false, .. });
        if !is_repeated(field) || !non_map_message {
            return Err(refuse_at(
                path,
                "BLOCK_ROLE_CHUNKS requires a repeated message field",
            ));
        }
    }

    if hint.role == pb::MappedRole::DocId || hint.role == pb::MappedRole::ChunkId {
        validate_identity_role(field, shape, hint, path)?;
    }
    Ok(())
}

/// An identity role needs a singular integer or string field whose value
/// projects as a keyword or an integer.
fn validate_identity_role(
    field: &FieldDescriptorProto,
    shape: &Shape<'_>,
    hint: &ResolvedHint,
    path: &str,
) -> Result<(), MappingError> {
    use prost_types::field_descriptor_proto::Type;

    let role_name = if hint.role == pb::MappedRole::DocId {
        "BLOCK_ROLE_DOC_ID"
    } else {
        "BLOCK_ROLE_CHUNK_ID"
    };
    let map = matches!(shape, Shape::Message { map: true, .. });
    if is_repeated(field) || map {
        return Err(refuse_at(
            path,
            format!("{role_name} requires a singular field"),
        ));
    }
    let id_type = match shape {
        Shape::Scalar(kind) => Some(*kind),
        Shape::Message {
            full, map: false, ..
        } => wrapper_kind(full),
        _ => None,
    };
    let id_ok = matches!(
        id_type,
        Some(
            Type::String
                | Type::Int32
                | Type::Int64
                | Type::Uint32
                | Type::Uint64
                | Type::Sint32
                | Type::Sint64
                | Type::Fixed32
                | Type::Fixed64
                | Type::Sfixed32
                | Type::Sfixed64
        )
    );
    if !id_ok {
        return Err(refuse_at(
            path,
            format!("{role_name} requires an integer or string field"),
        ));
    }
    if !matches!(
        family(hint.kind, false),
        pb::ColumnFamily::Facet | pb::ColumnFamily::I64 | pb::ColumnFamily::U64
    ) {
        return Err(refuse_at(
            path,
            format!("{role_name} requires a keyword or integer value projection"),
        ));
    }

    Ok(())
}

fn validate_wrapper_descriptor(
    message: &DescriptorProto,
    kind: prost_types::field_descriptor_proto::Type,
    path: &str,
) -> Result<(), MappingError> {
    use prost_types::field_descriptor_proto::{Label, Type};
    let valid = message.field.iter().any(|field| {
        let default_ok = field
            .default_value
            .as_deref()
            .is_none_or(|value| match kind {
                Type::String | Type::Bytes => value.is_empty(),
                Type::Bool => value == "false",
                Type::Float | Type::Double => value.parse::<f64>().is_ok_and(|v| v.to_bits() == 0),
                _ => value.parse::<i64>() == Ok(0),
            });
        field.name() == "value"
            && field.number() == 1
            && field.r#type() == kind
            && field.label() == Label::Optional
            && field.oneof_index.is_none()
            && default_ok
    });
    if !valid {
        return Err(refuse_at(path, format!(
            "scalar wrapper requires singular value = 1 of type {} with its scalar default and no oneof",
            kind.as_str_name()
        )));
    }
    Ok(())
}

/// A well-known name alone is not evidence of its wire or default semantics.
/// Compatible proto2 descriptors are accepted, but their defaults must remain
/// zero and the components must not select a oneof or repeat. Extra source
/// fields remain preserved; only the two standard components project.
fn validate_timestamp_descriptor(
    message: &DescriptorProto,
    path: &str,
) -> Result<(), MappingError> {
    use prost_types::field_descriptor_proto::{Label, Type};
    for (name, number, kind) in [("seconds", 1, Type::Int64), ("nanos", 2, Type::Int32)] {
        let valid = message.field.iter().any(|field| {
            field.name() == name
                && field.number() == number
                && field.r#type() == kind
                && field.label() == Label::Optional
                && field.oneof_index.is_none()
                && field
                    .default_value
                    .as_deref()
                    .is_none_or(|value| value.parse::<i64>() == Ok(0))
        });
        if !valid {
            return Err(refuse_at(
                path,
                format!(
                    "google.protobuf.Timestamp requires singular {name} = {number} of type {} \
                 with default zero and no oneof",
                    kind.as_str_name()
                ),
            ));
        }
    }
    Ok(())
}
