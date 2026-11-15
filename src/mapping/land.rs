//! How a planned leaf decodes: the descriptor type governs the wire
//! encoding, the planned kind and family govern where the value lands.

use std::collections::HashMap;

use prost_reflect::DescriptorPool;
use prost_types::field_descriptor_proto::Type;
use prost_types::FieldDescriptorProto;
use tonic::Status;

use super::extractor::Land;
use super::hint_resolve::wrapper_kind;
use super::refuse_at;
use crate::pb;

fn is_integer(t: Type) -> bool {
    matches!(
        t,
        Type::Int32
            | Type::Int64
            | Type::Sint32
            | Type::Sint64
            | Type::Uint32
            | Type::Uint64
            | Type::Fixed64
            | Type::Sfixed64
            | Type::Fixed32
            | Type::Sfixed32
    )
}

/// Decide how one planned leaf decodes, from its plan entry and its
/// DESCRIPTOR type.
pub(super) fn land_for(
    field: &pb::MappedField,
    is_vector: bool,
    leaf: &FieldDescriptorProto,
    enums: &HashMap<String, HashMap<i64, String>>,
    pool: &DescriptorPool,
) -> Result<Land, Status> {
    if matches!(
        pb::ColumnFamily::try_from(field.family),
        Ok(pb::ColumnFamily::MapFacet
            | pb::ColumnFamily::MapF64
            | pb::ColumnFamily::MapI64
            | pb::ColumnFamily::MapU64)
    ) {
        return map_land(field, leaf, enums, pool);
    }

    if leaf.r#type() == Type::Message {
        if let Some(kind) = wrapper_kind(leaf.type_name()) {
            let scalar = FieldDescriptorProto {
                r#type: Some(kind as i32),
                type_name: None,
                ..leaf.clone()
            };
            return land_for(field, is_vector, &scalar, enums, pool)
                .map(|inner| Land::Wrapper(Box::new(inner)));
        }
    }
    if is_vector {
        return match leaf.r#type() {
            Type::Float => Ok(Land::Vector),
            Type::Double => Ok(Land::Vector),
            other => Err(refuse_at(
                &field.path,
                format!("a VECTOR field must be repeated float or double, not {other:?}"),
            )),
        };
    }

    family_land(field, leaf, enums)
}

/// A map field lands as its value projection; the key type belongs to the
/// source descriptor.
fn map_land(
    field: &pb::MappedField,
    leaf: &FieldDescriptorProto,
    enums: &HashMap<String, HashMap<i64, String>>,
    pool: &DescriptorPool,
) -> Result<Land, Status> {
    let entry = pool
        .get_message_by_name(leaf.type_name().trim_start_matches('.'))
        .filter(|entry| entry.is_map_entry())
        .ok_or_else(|| refuse_at(&field.path, "map projection requires a protobuf map field"))?;
    let value = entry.map_entry_value_field();
    if matches!(value.kind(), prost_reflect::Kind::Message(_)) {
        return Err(refuse_at(&field.path, "message-valued maps require an element-correlated projection; scalar map projection cannot flatten them"));
    }
    if field.kind == pb::MappedKind::Boolean as i32 && value.kind() != prost_reflect::Kind::Bool {
        return Err(refuse_at(
            &field.path,
            "BOOLEAN map projection requires boolean values",
        ));
    }
    let scalar = pb::MappedField {
        family: if field.family == pb::ColumnFamily::MapFacet as i32 {
            pb::ColumnFamily::Facet
        } else if field.family == pb::ColumnFamily::MapI64 as i32 {
            pb::ColumnFamily::I64
        } else if field.family == pb::ColumnFamily::MapU64 as i32 {
            pb::ColumnFamily::U64
        } else {
            pb::ColumnFamily::F64
        } as i32,
        repeated: false,
        ..field.clone()
    };

    land_for(&scalar, false, value.field_descriptor_proto(), enums, pool)
        .map(|inner| Land::Map(Box::new(inner)))
}

/// The landing of a scalar leaf on its planned column family.
fn family_land(
    field: &pb::MappedField,
    leaf: &FieldDescriptorProto,
    enums: &HashMap<String, HashMap<i64, String>>,
) -> Result<Land, Status> {
    let family = field.family;

    if family == pb::ColumnFamily::TextField as i32 {
        return match leaf.r#type() {
            Type::String => Ok(Land::Text),
            other => Err(refuse_at(
                &field.path,
                format!("a TEXT field must be a string, not {other:?}"),
            )),
        };
    }

    if family == pb::ColumnFamily::Facet as i32 {
        return facet_land(field, leaf, enums);
    }

    if family == pb::ColumnFamily::I64 as i32 {
        if field.kind == pb::MappedKind::Date as i32 {
            return match leaf.r#type() {
                Type::Message => Ok(Land::Date),
                other => Err(refuse_at(
                    &field.path,
                    format!("a DATE field must be a google.protobuf.Timestamp, not {other:?}"),
                )),
            };
        }
        return match is_integer(leaf.r#type()) {
            true => Ok(Land::Int),
            false => Err(refuse_at(
                &field.path,
                format!(
                    "an i64 column cannot decode descriptor type {:?}",
                    leaf.r#type()
                ),
            )),
        };
    }
    if family == pb::ColumnFamily::U64 as i32 {
        return match leaf.r#type() {
            Type::Uint32 | Type::Fixed32 | Type::Uint64 | Type::Fixed64 => Ok(Land::Uint),
            other => Err(refuse_at(
                &field.path,
                format!("a u64 column cannot decode descriptor type {other:?}"),
            )),
        };
    }
    if family == pb::ColumnFamily::F64 as i32 {
        return match leaf.r#type() {
            Type::Float => Ok(Land::Num),
            Type::Double => Ok(Land::Num),
            other => Err(refuse_at(
                &field.path,
                format!("an f64 column cannot decode descriptor type {other:?}"),
            )),
        };
    }
    Err(Status::internal(format!(
        "plan: field {} landed family {} with no extraction rule",
        field.path, family
    )))
}

fn facet_land(
    field: &pb::MappedField,
    leaf: &FieldDescriptorProto,
    enums: &HashMap<String, HashMap<i64, String>>,
) -> Result<Land, Status> {
    match leaf.r#type() {
        Type::String => Ok(Land::FacetStr),
        Type::Bool => Ok(Land::FacetBool),
        Type::Enum => {
            let full = leaf.type_name().trim_start_matches('.');
            let table = enums.get(full).ok_or_else(|| {
                refuse_at(
                    &field.path,
                    format!("enum type {full:?} is not in the descriptor set"),
                )
            })?;
            Ok(Land::FacetEnum(table.clone()))
        }
        Type::Uint32 | Type::Uint64 | Type::Fixed32 | Type::Fixed64 => Ok(Land::FacetUint),
        other => match is_integer(other) {
            true => Ok(Land::FacetInt),
            false => Err(refuse_at(
                &field.path,
                format!("no facet rendering for descriptor type {other:?}"),
            )),
        },
    }
}
