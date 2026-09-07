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
                        pool,
                    )?;
                }
                let mut value_descriptor = if field.is_map() {
                    let Kind::Message(entry) = field.kind() else {
                        unreachable!("map descriptor")
                    };
                    entry
                        .map_entry_value_field()
                        .field_descriptor_proto()
                        .clone()
                } else {
                    field.field_descriptor_proto().clone()
                };
                value_descriptor.r#type =
                    Some(super::projection_scalar_type(&value_descriptor) as i32);
                let mut projection = projection(mapped, &value_descriptor, numbers.clone());
                if mapped.family != ColumnFamily::None as i32 {
                    if let Kind::Message(child) = field.kind() {
                        let component_names: &[&str] = if super::wrapper_kind(child.full_name())
                            .is_some()
                        {
                            projection.constraints.push("An absent wrapper is missing; a present empty wrapper projects its scalar default.".into());
                            &["value"]
                        } else if child.is_map_entry() {
                            projection.constraints.push("Map keys are exact strings, canonical decimal integers or true/false; use those strings in map selectors. Omitted key/value fields use protobuf defaults. The last decoded entry for a key wins; a missing entry remains absent. Original wire occurrences are preserved separately.".into());
                            &["key", "value"]
                        } else if mapped.kind == pb::MappedKind::Date as i32 {
                            &["seconds", "nanos"]
                        } else {
                            &[]
                        };
                        for name in component_names {
                            let component = child
                                .get_field_by_name(name)
                                .expect("validated well-known component");
                            let component_path = format!("{path}.{name}");
                            let mut component_numbers = numbers.clone();
                            component_numbers.push(component.number());
                            bindings
                                .entry(component.full_name().into())
                                .or_default()
                                .insert(
                                    component_path.clone(),
                                    pb::FieldProjection {
                                        path: component_path,
                                        field_numbers: component_numbers,
                                        column_name: mapped.name.clone(),
                                        r#use: Use::Input as i32,
                                        query_representation: Query::None as i32,
                                        value_path: path.clone(),
                                        ..Default::default()
                                    },
                                );
                        }
                    }
                }
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
            } else if projections.iter().any(|p| p.r#use == Use::Input as i32) {
                "Only listed input paths contribute to their named mapped values; the inputs are not independently queryable."
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
        report_version: 2,
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
            ColumnFamily::U64 => (Use::Value, Query::UnsignedInteger),
            ColumnFamily::F64 => (Use::Value, Query::FloatingPoint),
            ColumnFamily::MapFacet => (Use::Value, Query::MapStringFacet),
            ColumnFamily::MapF64 => (Use::Value, Query::MapFloatingPoint),
        };
    let mut constraints = Vec::new();
    if usage == Use::Value {
        if representation == Query::SignedInteger
            && matches!(descriptor.r#type(), Type::Uint64 | Type::Fixed64)
        {
            constraints.push("Values above i64::MAX are refused by the current extractor.".into());
        }
        if representation == Query::UnsignedInteger {
            constraints.push("Exact unsigned comparisons, presence, typed value projections, checked arithmetic and U64 materialization are supported; unsigned sorting and collapse retain typed keys; COUNT, SUM, MIN, MAX, CARDINALITY and exact percentiles preserve unsigned values; statistical folds require explicit double() conversion; range facets preserve exact typed bounds and unsigned values; score stages convert unsigned inputs and extrema to double arithmetic.".into());
        }
        if matches!(
            representation,
            Query::SignedInteger | Query::UnsignedInteger
        ) {
            constraints.push("Column statistics preserve typed extrema and exact 128-bit sums; the exact mean is sum/count, while double summaries are approximate.".into());
        }
        if representation == Query::DenseVector {
            constraints.push(
                "Values are converted to f32 and must be finite; dimension is bound separately."
                    .into(),
            );
        }
        if matches!(
            representation,
            Query::FloatingPoint | Query::MapFloatingPoint
        ) {
            constraints.push("Stored numeric values must be finite.".into());
        }
        if matches!(representation, Query::StringFacet | Query::MapStringFacet) {
            match descriptor.r#type() {
                Type::Enum => constraints.push("Enums query as their first declared alias; unknown open-enum numbers query as decimal strings. Unknown closed-enum numbers do not project.".into()),
                Type::Bool => constraints.push("Booleans query as the strings true and false.".into()),
                Type::String => constraints.push("An empty string is a present facet value; omission represents absence.".into()),
                _ => constraints.push("Integers query as decimal strings.".into()),
            }
        }
        if mapped.kind == pb::MappedKind::Date as i32 {
            constraints.push("Timestamp projections require seconds in [-62135596800, 253402300799] and nanos in [0, 999999999]. Queries use signed epoch microseconds; submicrosecond precision remains only in the original.".into());
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
        value_path: String::new(),
    }
}
