//! Reachable schema inventory and occurrence-specific projection information.

use std::collections::{BTreeMap, HashSet};

use prost_reflect::{DescriptorPool, Kind, Syntax};
use prost_types::field_descriptor_proto::Type;
use tonic::Status;

use crate::pb::{self, ColumnFamily, MappedQueryRepresentation as Query, ProjectionUse as Use};

type Bindings = BTreeMap<String, BTreeMap<String, pb::FieldProjection>>;

pub(super) fn build(
    plan: &pb::MappedPlan,
    pool: &DescriptorPool,
    set: &prost_types::FileDescriptorSet,
    skipped_fields: &HashSet<String>,
) -> Result<pb::SchemaReport, Status> {
    let root = pool
        .get_message_by_name(&plan.message_type)
        .expect("validated root");
    let mut bindings: Bindings = BTreeMap::new();
    let enum_values = super::collect_enum_values(set);
    for mapped in &plan.fields {
        let mut message = root.clone();
        let mut numbers = Vec::new();
        let mut path = String::new();
        let segments: Vec<_> = mapped.path.split('.').collect();
        for (i, segment) in segments.iter().enumerate() {
            let field = message
                .get_field_by_name(segment)
                .ok_or_else(|| Status::internal("plan path is absent from schema"))?;
            numbers.push(field.number());
            if !path.is_empty() {
                path.push('.');
            }
            path.push_str(segment);
            let leaf = i + 1 == segments.len();
            if leaf {
                if mapped.family != ColumnFamily::None as i32 {
                    super::land_for(
                        mapped,
                        mapped.path == plan.vector_path,
                        field.field_descriptor_proto(),
                        &enum_values,
                    )?;
                }
                let projection =
                    projection(mapped, field.field_descriptor_proto(), numbers.clone());
                bindings
                    .entry(field.full_name().into())
                    .or_default()
                    .insert(path.clone(), projection);
            } else {
                // Source-only leaves do not make their ancestors searchable.
                if mapped.family != ColumnFamily::None as i32 {
                    bindings
                        .entry(field.full_name().into())
                        .or_default()
                        .entry(path.clone())
                        .or_insert(pb::FieldProjection {
                            path: path.clone(),
                            field_numbers: numbers.clone(),
                            r#use: Use::Container as i32,
                            ..Default::default()
                        });
                }
                let Kind::Message(child) = field.kind() else {
                    return Err(Status::internal("plan path crosses a scalar"));
                };
                message = child;
            }
        }
    }

    inventory(&plan.message_type, pool, bindings, skipped_fields, true)
}

pub(super) fn describe(
    pool: &DescriptorPool,
    message_type: &str,
) -> Result<pb::SchemaReport, Status> {
    inventory(message_type, pool, BTreeMap::new(), &HashSet::new(), false)
}

fn inventory(
    message_type: &str,
    pool: &DescriptorPool,
    mut bindings: Bindings,
    skipped_fields: &HashSet<String>,
    requires_index_rows_for_preservation: bool,
) -> Result<pb::SchemaReport, Status> {
    let mut messages = Vec::new();
    let mut enums = BTreeMap::new();
    for message in super::reachable_messages(pool, message_type) {
        let mut fields = Vec::new();
        let mut add_field = |full_name: &str,
                             descriptor: &prost_types::FieldDescriptorProto,
                             presence: bool,
                             map: bool,
                             extension: bool,
                             kind: Kind| {
            if let Kind::Enum(enumeration) = kind {
                enums
                    .entry(enumeration.full_name().to_string())
                    .or_insert_with(|| pb::SchemaEnum {
                        full_name: enumeration.full_name().to_string(),
                        open: enumeration.parent_file().syntax() == Syntax::Proto3,
                        descriptor: Some(enumeration.enum_descriptor_proto().clone()),
                    });
            }
            let projections: Vec<_> = bindings
                .remove(full_name)
                .unwrap_or_default()
                .into_values()
                .collect();
            let excluded_by_hint = skipped_fields.contains(full_name);
            let disposition = if !requires_index_rows_for_preservation {
                "Retained in the original; no indexing projection was requested."
            } else if excluded_by_hint {
                "Indexing disabled by the ProtoMolt SKIP hint; retained in the original."
            } else if projections.iter().any(|p| p.r#use == Use::Value as i32) {
                "Only listed value paths are projected; other occurrences are source-only."
            } else if projections.iter().any(|p| p.r#use == Use::Container as i32) {
                "Only listed container paths are traversed; the message itself is not a query value."
            } else if extension {
                "Registered extension retained in the original; extension projection is not implemented."
            } else {
                "Retained in the original; no mapped value projection."
            };
            fields.push(pb::SchemaField {
                full_name: full_name.into(),
                descriptor: Some(descriptor.clone()),
                extension,
                supports_presence: presence,
                map,
                preservation: pb::SourcePreservation::OriginalBytes as i32,
                projections,
                disposition: disposition.into(),
                excluded_by_hint,
            });
        };
        for field in message.fields() {
            add_field(
                field.full_name(),
                field.field_descriptor_proto(),
                field.supports_presence(),
                field.is_map(),
                false,
                field.kind(),
            );
        }
        for field in message.extensions() {
            add_field(
                field.full_name(),
                field.field_descriptor_proto(),
                field.supports_presence(),
                false,
                true,
                field.kind(),
            );
        }
        fields.sort_by_key(|f| f.descriptor.as_ref().expect("set").number());
        messages.push(pb::SchemaMessage {
            full_name: message.full_name().into(),
            syntax: match message.parent_file().syntax() {
                Syntax::Proto2 => "proto2",
                Syntax::Proto3 => "proto3",
            }
            .into(),
            map_entry: message.is_map_entry(),
            fields,
            oneofs: message.descriptor_proto().oneof_decl.clone(),
            message_set_wire_format: message
                .descriptor_proto()
                .options
                .as_ref()
                .is_some_and(|options| options.message_set_wire_format()),
        });
    }
    if !bindings.is_empty() {
        return Err(Status::internal("schema report omitted a projected field"));
    }
    Ok(pb::SchemaReport {
        report_version: 1,
        root_message: message_type.into(),
        messages,
        enums: enums.into_values().collect(),
        unknown_fields: pb::SourcePreservation::OriginalBytes as i32,
        requires_index_rows_for_preservation,
    })
}

fn projection(
    mapped: &pb::MappedField,
    descriptor: &prost_types::FieldDescriptorProto,
    numbers: Vec<u32>,
) -> pb::FieldProjection {
    let (usage, representation) =
        match ColumnFamily::try_from(mapped.family).expect("derived family") {
            ColumnFamily::None if mapped.role == pb::MappedRole::Chunks as i32 => {
                (Use::Container, Query::None)
            }
            ColumnFamily::None => (Use::SourceOnly, Query::None),
            ColumnFamily::Vector => (Use::Value, Query::DenseVector),
            ColumnFamily::TextField => (Use::Value, Query::AnalyzedText),
            ColumnFamily::Facet => (Use::Value, Query::StringFacet),
            ColumnFamily::I64 => (Use::Value, Query::SignedInteger),
            ColumnFamily::F64 => (Use::Value, Query::FloatingPoint),
        };
    let mut constraints = Vec::new();
    if usage == Use::Value {
        if representation == Query::SignedInteger
            && (matches!(
                descriptor.r#type(),
                Type::Int64 | Type::Sint64 | Type::Sfixed64
            ) || mapped.kind == pb::MappedKind::Date as i32)
        {
            constraints
                .push("i64::MIN is reserved for absence and is refused as a column value.".into());
        }
        if matches!(descriptor.r#type(), Type::Uint64 | Type::Fixed64) {
            constraints.push("Values above i64::MAX are refused by the current extractor.".into());
        }
        if representation == Query::DenseVector {
            constraints.push(
                "Values are converted to f32 and must be finite; dimension is bound separately."
                    .into(),
            );
        }
        if representation == Query::FloatingPoint {
            constraints.push("Stored numeric values must be finite.".into());
        }
        if representation == Query::StringFacet {
            match descriptor.r#type() {
                Type::Enum => constraints.push("Enums query as their first declared alias; unknown open-enum numbers query as decimal strings. Unknown closed-enum numbers do not project.".into()),
                Type::Bool => constraints.push("Booleans query as the strings true and false.".into()),
                Type::String => {},
                _ => constraints.push("Integers query as decimal strings.".into()),
            }
        }
        if mapped.kind == pb::MappedKind::Date as i32 {
            constraints.push("Timestamp queries use signed epoch microseconds; submicrosecond precision remains only in the original.".into());
        }
    } else if usage == Use::SourceOnly {
        constraints.push(if mapped.repeated {
            "Repeated or map values have no mapped scalar column; element-correlated querying is not implemented."
        } else {
            "This mapped kind has no value extraction or query representation."
        }.into());
    }
    pb::FieldProjection {
        path: mapped.path.clone(),
        field_numbers: numbers,
        column_name: if usage == Use::Value {
            mapped.name.clone()
        } else {
            String::new()
        },
        r#use: usage as i32,
        query_representation: representation as i32,
        constraints,
    }
}
