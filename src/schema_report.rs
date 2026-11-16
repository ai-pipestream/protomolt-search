//! Reachable schema inventory and occurrence-specific projection information.

use std::collections::{BTreeMap, HashMap, HashSet};

use super::MappingError;
use prost_reflect::{DescriptorPool, FieldDescriptor, Kind, MessageDescriptor, Syntax};
use prost_types::field_descriptor_proto::Type;

use crate::pb::{self, ColumnFamily, MappedQueryRepresentation as Query, ProjectionUse as Use};

type Bindings = BTreeMap<String, BTreeMap<String, pb::FieldProjection>>;

/// Inputs every mapped path is bound against.
struct BuildContext<'a> {
    plan: &'a pb::MappedPlan,
    pool: &'a DescriptorPool,
    enum_values: &'a HashMap<String, HashMap<i64, String>>,
}

pub(super) fn build(
    plan: &pb::MappedPlan,
    pool: &DescriptorPool,
    set: &prost_types::FileDescriptorSet,
    skipped_fields: &HashSet<String>,
) -> Result<pb::SchemaReport, MappingError> {
    let root = pool
        .get_message_by_name(&plan.message_type)
        .expect("validated root");
    let mut bindings: Bindings = BTreeMap::new();
    let enum_values = super::collect_enum_values(set);
    let cx = BuildContext {
        plan,
        pool,
        enum_values: &enum_values,
    };
    for mapped in &plan.fields {
        bind_mapped_path(&cx, mapped, &root, &mut bindings)?;
    }

    inventory(&plan.message_type, pool, bindings, skipped_fields, true)
}

/// Walk one mapped field's dotted path from the root, binding its containers and leaf.
fn bind_mapped_path(
    cx: &BuildContext<'_>,
    mapped: &pb::MappedField,
    root: &MessageDescriptor,
    bindings: &mut Bindings,
) -> Result<(), MappingError> {
    let mut message = root.clone();
    let mut numbers = Vec::new();
    let mut path = String::new();
    let segments: Vec<_> = mapped.path.split('.').collect();
    for (i, segment) in segments.iter().enumerate() {
        let field = message
            .get_field_by_name(segment)
            .ok_or_else(|| MappingError::internal("plan path is absent from schema"))?;
        numbers.push(field.number());
        if !path.is_empty() {
            path.push('.');
        }
        path.push_str(segment);
        let leaf = i + 1 == segments.len();
        if leaf {
            bind_leaf(cx, mapped, &field, (&path, &numbers), bindings)?;
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
                return Err(MappingError::internal("plan path crosses a scalar"));
            };
            message = child;
        }
    }
    Ok(())
}

/// Bind a mapped leaf's value projection and the components feeding it.
fn bind_leaf(
    cx: &BuildContext<'_>,
    mapped: &pb::MappedField,
    field: &FieldDescriptor,
    at: (&String, &Vec<u32>),
    bindings: &mut Bindings,
) -> Result<(), MappingError> {
    let (path, numbers) = at;
    if mapped.family != ColumnFamily::None as i32 {
        super::land_for(
            mapped,
            mapped.path == cx.plan.vector_path,
            field.field_descriptor_proto(),
            cx.enum_values,
            cx.pool,
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
    value_descriptor.r#type = Some(super::projection_scalar_type(&value_descriptor) as i32);
    let mut projection = projection(mapped, &value_descriptor, numbers.clone());
    if mapped.family != ColumnFamily::None as i32 {
        if let Kind::Message(child) = field.kind() {
            bind_components(mapped, &child, at, &mut projection, bindings);
        }
    }
    bindings
        .entry(field.full_name().into())
        .or_default()
        .insert(path.clone(), projection);
    Ok(())
}

/// Bind the well-known component fields that feed a mapped message value.
fn bind_components(
    mapped: &pb::MappedField,
    child: &MessageDescriptor,
    (path, numbers): (&String, &Vec<u32>),
    projection: &mut pb::FieldProjection,
    bindings: &mut Bindings,
) {
    let component_names: &[&str] = if super::wrapper_kind(child.full_name()).is_some() {
        projection.constraints.push(
            "An absent wrapper is missing; a present empty wrapper projects its scalar default."
                .into(),
        );
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

pub(super) fn describe(
    pool: &DescriptorPool,
    message_type: &str,
) -> Result<pb::SchemaReport, MappingError> {
    inventory(message_type, pool, BTreeMap::new(), &HashSet::new(), false)
}

/// What one declared field or extension contributes to the inventory.
struct FieldFacts<'a> {
    full_name: &'a str,
    descriptor: &'a prost_types::FieldDescriptorProto,
    presence: bool,
    map: bool,
    extension: bool,
    kind: Kind,
}

/// Accumulates reachable enums and consumes projection bindings field by field.
struct Inventory<'a> {
    enums: BTreeMap<String, pb::SchemaEnum>,
    bindings: Bindings,
    skipped_fields: &'a HashSet<String>,
    requires_index_rows_for_preservation: bool,
}

impl Inventory<'_> {
    fn field(&mut self, facts: FieldFacts<'_>) -> pb::SchemaField {
        if let Kind::Enum(enumeration) = facts.kind {
            self.enums
                .entry(enumeration.full_name().to_string())
                .or_insert_with(|| pb::SchemaEnum {
                    full_name: enumeration.full_name().to_string(),
                    open: enumeration.parent_file().syntax() == Syntax::Proto3,
                    descriptor: Some(enumeration.enum_descriptor_proto().clone()),
                });
        }
        let projections: Vec<_> = self
            .bindings
            .remove(facts.full_name)
            .unwrap_or_default()
            .into_values()
            .collect();
        let excluded_by_hint = self.skipped_fields.contains(facts.full_name);
        let disposition = disposition(
            &projections,
            excluded_by_hint,
            facts.extension,
            self.requires_index_rows_for_preservation,
        );
        pb::SchemaField {
            full_name: facts.full_name.into(),
            descriptor: Some(facts.descriptor.clone()),
            extension: facts.extension,
            supports_presence: facts.presence,
            map: facts.map,
            preservation: pb::SourcePreservation::OriginalBytes as i32,
            projections,
            disposition: disposition.into(),
            excluded_by_hint,
        }
    }

    fn message(&mut self, message: &MessageDescriptor) -> pb::SchemaMessage {
        let mut fields = Vec::new();
        for field in message.fields() {
            fields.push(self.field(FieldFacts {
                full_name: field.full_name(),
                descriptor: field.field_descriptor_proto(),
                presence: field.supports_presence(),
                map: field.is_map(),
                extension: false,
                kind: field.kind(),
            }));
        }
        for field in message.extensions() {
            fields.push(self.field(FieldFacts {
                full_name: field.full_name(),
                descriptor: field.field_descriptor_proto(),
                presence: field.supports_presence(),
                map: false,
                extension: true,
                kind: field.kind(),
            }));
        }
        fields.sort_by_key(|f| f.descriptor.as_ref().expect("set").number());
        pb::SchemaMessage {
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
        }
    }
}

fn disposition(
    projections: &[pb::FieldProjection],
    excluded_by_hint: bool,
    extension: bool,
    requires_index_rows_for_preservation: bool,
) -> &'static str {
    if !requires_index_rows_for_preservation {
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
    }
}

fn inventory(
    message_type: &str,
    pool: &DescriptorPool,
    bindings: Bindings,
    skipped_fields: &HashSet<String>,
    requires_index_rows_for_preservation: bool,
) -> Result<pb::SchemaReport, MappingError> {
    let mut inventory = Inventory {
        enums: BTreeMap::new(),
        bindings,
        skipped_fields,
        requires_index_rows_for_preservation,
    };
    let mut messages = Vec::new();
    for message in super::reachable_messages(pool, message_type) {
        messages.push(inventory.message(&message));
    }
    if !inventory.bindings.is_empty() {
        return Err(MappingError::internal(
            "schema report omitted a projected field",
        ));
    }
    Ok(pb::SchemaReport {
        report_version: 2,
        root_message: message_type.into(),
        messages,
        enums: inventory.enums.into_values().collect(),
        unknown_fields: pb::SourcePreservation::OriginalBytes as i32,
        requires_index_rows_for_preservation,
    })
}

fn projection(
    mapped: &pb::MappedField,
    descriptor: &prost_types::FieldDescriptorProto,
    numbers: Vec<u32>,
) -> pb::FieldProjection {
    let (usage, representation) = usage_and_representation(mapped);
    let mut constraints = Vec::new();
    if usage == Use::Value {
        push_value_constraints(mapped, descriptor, representation, &mut constraints);
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

fn usage_and_representation(mapped: &pb::MappedField) -> (Use, Query) {
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
        ColumnFamily::MapI64 => (Use::Value, Query::MapSignedInteger),
        ColumnFamily::MapU64 => (Use::Value, Query::MapUnsignedInteger),
    }
}

/// The caveats a queryable value projection carries for its representation.
fn push_value_constraints(
    mapped: &pb::MappedField,
    descriptor: &prost_types::FieldDescriptorProto,
    representation: Query,
    constraints: &mut Vec<String>,
) {
    if matches!(
        ColumnFamily::try_from(mapped.family),
        Ok(ColumnFamily::MapI64 | ColumnFamily::MapU64)
    ) {
        constraints.push("Exact numeric map comparisons, key presence, typed value expressions, checked arithmetic, materialization and expression-based aggregates are supported. Statistical folds require explicit double() conversion. Direct map sorting and collapse preserve exact typed keys. Bounded score stages use typed_map_op and convert integer inputs and extrema to double for scoring only. Range facets use typed_map and preserve exact values and typed boundaries. Column statistics are not yet implemented.".into());
        if mapped.family == ColumnFamily::MapI64 as i32
            && matches!(descriptor.r#type(), Type::Uint64 | Type::Fixed64)
        {
            constraints.push(
                "Unsigned map values above i64::MAX are refused by the signed projection.".into(),
            );
        }
    }
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
            "Values are converted to f32 and must be finite; dimension is bound separately.".into(),
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
}
