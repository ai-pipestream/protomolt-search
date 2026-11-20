//! Hint resolution: what a field descriptor is, the kind inferred for it,
//! and the merge of an explicit hint over that inference.

use super::MappingError;
use prost_types::FieldDescriptorProto;

use super::refuse_at;
use super::type_index::{MsgEntry, TypeIndex};
use crate::pb::{self, hints};

/// A hint after merging: the resolved kind, whether it was explicit,
/// and the attributes this engine records.
pub(super) struct ResolvedHint {
    pub(super) kind: pb::MappedKind,
    pub(super) explicit_kind: bool,
    pub(super) name_override: String,
    pub(super) role: pb::MappedRole,
    pub(super) vector_dims: u32,
    pub(super) analyzer: String,
    pub(super) search_analyzer: String,
    pub(super) skip: bool,
}

/// What one field descriptor actually is, resolved against the index.
pub(super) enum Shape<'a> {
    Scalar(prost_types::field_descriptor_proto::Type),
    Enum,
    Message {
        full: String,
        entry: &'a MsgEntry<'a>,
        map: bool,
    },
}

pub(super) fn shape<'a>(
    field: &FieldDescriptorProto,
    index: &'a TypeIndex<'a>,
    path: &str,
) -> Result<Shape<'a>, MappingError> {
    use prost_types::field_descriptor_proto::Type;
    match field.r#type() {
        Type::Message | Type::Group => {
            let entry = index.message_by_type_name(field.type_name(), path)?;
            let full = field.type_name().trim_start_matches('.').to_string();
            let map = entry.desc.options.as_ref().is_some_and(|o| o.map_entry());
            Ok(Shape::Message { full, entry, map })
        }
        Type::Enum => {
            let full = field.type_name().trim_start_matches('.');
            if full.is_empty() || !index.enums.contains(full) {
                return Err(refuse_at(
                    path,
                    format!(
                        "enum type {:?} is not in the descriptor set",
                        field.type_name()
                    ),
                ));
            }
            Ok(Shape::Enum)
        }
        other => Ok(Shape::Scalar(other)),
    }
}

pub(super) fn is_repeated(field: &FieldDescriptorProto) -> bool {
    field.label() == prost_types::field_descriptor_proto::Label::Repeated
}

/// The scalar represented by a standard wrapper. Recognition is followed by
/// descriptor validation whenever a wrapper contributes an indexed value.
pub(super) fn wrapper_kind(full: &str) -> Option<prost_types::field_descriptor_proto::Type> {
    use prost_types::field_descriptor_proto::Type;
    Some(match full.trim_start_matches('.') {
        "google.protobuf.DoubleValue" => Type::Double,
        "google.protobuf.FloatValue" => Type::Float,
        "google.protobuf.Int64Value" => Type::Int64,
        "google.protobuf.UInt64Value" => Type::Uint64,
        "google.protobuf.Int32Value" => Type::Int32,
        "google.protobuf.UInt32Value" => Type::Uint32,
        "google.protobuf.BoolValue" => Type::Bool,
        "google.protobuf.StringValue" => Type::String,
        "google.protobuf.BytesValue" => Type::Bytes,
        _ => return None,
    })
}

pub(super) fn projection_scalar_type(
    field: &FieldDescriptorProto,
) -> prost_types::field_descriptor_proto::Type {
    if field.r#type() == prost_types::field_descriptor_proto::Type::Message {
        wrapper_kind(field.type_name()).unwrap_or(field.r#type())
    } else {
        field.r#type()
    }
}

/// Infer a hint from the descriptor alone, with protomolt's rules:
/// strings whose names look like identifiers become KEYWORD, Timestamp
/// becomes DATE, Struct/Value stay OBJECT, repeated and map messages
/// stay NESTED.
fn inferred_kind(field: &FieldDescriptorProto, shape: &Shape<'_>) -> pb::MappedKind {
    use prost_types::field_descriptor_proto::Type;
    if let Shape::Message {
        full, map: false, ..
    } = shape
    {
        if !is_repeated(field) {
            if let Some(kind) = wrapper_kind(full) {
                return inferred_kind(field, &Shape::Scalar(kind));
            }
        }
    }
    match shape {
        Shape::Scalar(Type::String) => {
            if looks_like_keyword(field.name()) {
                pb::MappedKind::Keyword
            } else {
                pb::MappedKind::Text
            }
        }
        Shape::Scalar(Type::Bool) => pb::MappedKind::Boolean,
        Shape::Scalar(Type::Int32 | Type::Sint32 | Type::Sfixed32) => pb::MappedKind::Int32,
        Shape::Scalar(Type::Int64 | Type::Sint64 | Type::Sfixed64) => pb::MappedKind::Int64,
        Shape::Scalar(Type::Uint32 | Type::Fixed32) => pb::MappedKind::Uint32,
        Shape::Scalar(Type::Uint64 | Type::Fixed64) => pb::MappedKind::Uint64,
        Shape::Scalar(Type::Float) => pb::MappedKind::Float,
        Shape::Scalar(Type::Double) => pb::MappedKind::Double,
        Shape::Scalar(Type::Bytes) => pb::MappedKind::Binary,
        Shape::Scalar(_) => pb::MappedKind::Object,
        Shape::Enum => pb::MappedKind::Keyword,
        Shape::Message { full, map, .. } => match full.as_str() {
            "google.protobuf.Timestamp" => pb::MappedKind::Date,
            "google.protobuf.Struct" | "google.protobuf.Value" => pb::MappedKind::Object,
            _ if is_repeated(field) || *map => pb::MappedKind::Nested,
            _ => pb::MappedKind::Object,
        },
    }
}

/// protomolt's keyword-name heuristic, verbatim: identifier-shaped
/// names index as exact values rather than analyzed text.
fn looks_like_keyword(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    name == "id"
        || name.ends_with("_id")
        || (name.ends_with("id") && name.len() <= 4)
        || name.ends_with("_key")
        || name.ends_with("_code")
        || name == "uri"
        || name.ends_with("_uri")
        || name == "status"
        || name == "type"
        || name.ends_with("_type")
}

pub(super) fn plain_hint(kind: pb::MappedKind) -> ResolvedHint {
    ResolvedHint {
        kind,
        explicit_kind: false,
        name_override: String::new(),
        role: pb::MappedRole::None,
        vector_dims: 0,
        analyzer: String::new(),
        search_analyzer: String::new(),
        skip: false,
    }
}

/// Resolve one field's hint: the explicit `(index)` option when
/// present, inference otherwise. An explicit hint with an unset type
/// still infers the kind while its other attributes win, matching
/// protomolt.
pub(super) fn resolve_hint(
    field: &FieldDescriptorProto,
    shape: &Shape<'_>,
    explicit: Option<&hints::FieldIndexHint>,
    path: &str,
) -> Result<ResolvedHint, MappingError> {
    let Some(hint) = explicit else {
        return Ok(plain_hint(inferred_kind(field, shape)));
    };
    let (mut kind, explicit_kind, skip) = match hints::IndexFieldType::try_from(hint.r#type) {
        Ok(hints::IndexFieldType::Unspecified) => (inferred_kind(field, shape), false, false),
        Ok(hints::IndexFieldType::Skip) => (pb::MappedKind::Unspecified, true, true),
        Ok(explicit) => (convert_kind(explicit, path)?, true, false),
        Err(_) => {
            return Err(refuse_at(
                path,
                format!("hint declares unknown index type {}", hint.r#type),
            ))
        }
    };
    let role = match hints::BlockRole::try_from(hint.block_role) {
        Ok(hints::BlockRole::Unspecified) => pb::MappedRole::None,
        Ok(hints::BlockRole::Chunks) => pb::MappedRole::Chunks,
        Ok(hints::BlockRole::DocId) => pb::MappedRole::DocId,
        Ok(hints::BlockRole::ChunkId) => pb::MappedRole::ChunkId,
        Err(_) => {
            return Err(refuse_at(
                path,
                format!("hint declares unknown block role {}", hint.block_role),
            ))
        }
    };
    // Identity roles need an exact value. An unspecified string kind adopts
    // keyword semantics; an explicitly requested TEXT kind is refused below.
    if !explicit_kind
        && matches!(role, pb::MappedRole::DocId | pb::MappedRole::ChunkId)
        && kind == pb::MappedKind::Text
    {
        kind = pb::MappedKind::Keyword;
    }
    if hint.chunking_policy.is_some() {
        return Err(refuse_at(
            path,
            "chunking_policy hints are not supported by this engine; \
             chunk and embed before ingest",
        ));
    }
    Ok(ResolvedHint {
        kind,
        explicit_kind,
        name_override: hint.name.clone(),
        role,
        vector_dims: u32::try_from(hint.vector_dims.max(0)).expect("clamped to non-negative"),
        analyzer: hint.analyzer.clone().unwrap_or_default(),
        search_analyzer: hint.search_analyzer.clone().unwrap_or_default(),
        skip,
    })
}

fn convert_kind(hint: hints::IndexFieldType, path: &str) -> Result<pb::MappedKind, MappingError> {
    use hints::IndexFieldType as H;
    Ok(match hint {
        H::Text => pb::MappedKind::Text,
        H::Keyword => pb::MappedKind::Keyword,
        H::Int32 => pb::MappedKind::Int32,
        H::Int64 => pb::MappedKind::Int64,
        H::Float => pb::MappedKind::Float,
        H::Double => pb::MappedKind::Double,
        H::Boolean => pb::MappedKind::Boolean,
        H::Date => pb::MappedKind::Date,
        H::Binary => pb::MappedKind::Binary,
        H::Vector => pb::MappedKind::Vector,
        H::Object => pb::MappedKind::Object,
        H::Nested => pb::MappedKind::Nested,
        H::IntRange | H::LongRange | H::FloatRange | H::DoubleRange | H::DateRange => {
            return Err(refuse_at(
                path,
                "range hints are not supported by this engine yet",
            ))
        }
        H::TreePath => {
            return Err(refuse_at(
                path,
                "TREE_PATH hints are not supported by this engine yet",
            ))
        }
        H::Unspecified | H::Skip => unreachable!("handled by resolve_hint"),
    })
}
