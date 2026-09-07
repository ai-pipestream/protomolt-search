mod common;

use pipestream_search::{
    mapping::{derive_plan, derive_plan_with_definition, Extractor},
    pb,
};
use prost::Message;
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{
    DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet, MessageOptions,
};

fn field(name: &str, number: i32, kind: Type) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.into()),
        number: Some(number),
        r#type: Some(kind as i32),
        label: Some(Label::Optional as i32),
        ..Default::default()
    }
}
fn schema(key: Type, value: Type) -> Vec<u8> {
    FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("map_projection.proto".into()),
            package: Some("map_projection".into()),
            syntax: Some("proto3".into()),
            message_type: vec![DescriptorProto {
                name: Some("Record".into()),
                field: vec![
                    field("id", 1, Type::Uint64),
                    field("body", 2, Type::String),
                    FieldDescriptorProto {
                        label: Some(Label::Repeated as i32),
                        ..field("embedding", 3, Type::Float)
                    },
                    FieldDescriptorProto {
                        label: Some(Label::Repeated as i32),
                        type_name: Some(".map_projection.Record.AttributesEntry".into()),
                        ..field("attributes", 4, Type::Message)
                    },
                ],
                nested_type: vec![DescriptorProto {
                    name: Some("AttributesEntry".into()),
                    field: vec![field("key", 1, key), field("value", 2, value)],
                    options: Some(MessageOptions {
                        map_entry: Some(true),
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
    .encode_to_vec()
}
fn definition(kind: pb::MappedKind) -> pb::IndexDefinition {
    use pb::{MappedKind as K, MappedRole as R};
    pb::IndexDefinition {
        projections: [
            (1, K::Uint64, "id", R::DocId),
            (2, K::Text, "body", R::None),
            (3, K::Vector, "semantic", R::None),
            (4, kind, "attrs", R::None),
        ]
        .into_iter()
        .map(|(number, kind, name, role)| pb::IndexProjection {
            field_numbers: vec![number],
            kind: kind as i32,
            column_name: name.into(),
            role: role as i32,
            vector_dims: if kind == K::Vector { 8 } else { 0 },
        })
        .collect(),
    }
}
fn document() -> Vec<u8> {
    // id=1, body="word", eight packed float values 0.25.
    let mut bytes = vec![8, 1, 18, 4, b'w', b'o', b'r', b'd', 26, 32];
    for _ in 0..8 {
        bytes.extend(0.25f32.to_le_bytes());
    }
    bytes
}
fn entry(bytes: &mut Vec<u8>, payload: &[u8]) {
    prost::encoding::encode_key(4, prost::encoding::WireType::LengthDelimited, bytes);
    prost::encoding::encode_varint(payload.len() as u64, bytes);
    bytes.extend(payload);
}

#[test]
fn explicit_map_projection_keeps_default_entries_and_last_value() {
    let schema = schema(Type::String, Type::String);
    let policy = definition(pb::MappedKind::Keyword);
    let plan =
        derive_plan_with_definition(&schema, "map_projection.Record", Some(&policy)).unwrap();
    let mapped = plan.fields.iter().find(|f| f.path == "attributes").unwrap();
    assert_ne!(mapped.family, pb::ColumnFamily::None as i32);
    assert!(mapped.repeated);
    let inferred = derive_plan(&schema, "map_projection.Record").unwrap();
    assert_eq!(
        inferred
            .fields
            .iter()
            .find(|f| f.path == "attributes")
            .unwrap()
            .family,
        pb::ColumnFamily::None as i32
    );
    assert_ne!(plan.fingerprint, inferred.fingerprint);
    let extractor =
        Extractor::with_definition(&schema, "map_projection.Record", "", Some(&policy)).unwrap();
    let mut bytes = document();
    entry(&mut bytes, &[10, 1, b'x', 18, 3, b'o', b'l', b'd']);
    entry(&mut bytes, &[]); // Present default key and value.
    entry(&mut bytes, &[10, 1, b'x']); // Last entry replaces the prior value with default.
    let rows = extractor.extract(&bytes).unwrap();
    assert_eq!(
        rows[0].request.map_facets,
        vec![
            pb::MapFacetEntry {
                field: "attrs".into(),
                key: "".into(),
                value: "".into()
            },
            pb::MapFacetEntry {
                field: "attrs".into(),
                key: "x".into(),
                value: "".into()
            }
        ]
    );
    assert!(extractor.extract(&document()).unwrap()[0]
        .request
        .map_facets
        .is_empty());
}

fn map_document(
    schema: &[u8],
    entries: Vec<(prost_reflect::MapKey, prost_reflect::Value)>,
) -> Vec<u8> {
    use prost_reflect::{DescriptorPool, DynamicMessage, Value};
    let pool = DescriptorPool::decode(schema).unwrap();
    let mut message = DynamicMessage::decode(
        pool.get_message_by_name("map_projection.Record").unwrap(),
        document().as_slice(),
    )
    .unwrap();
    message.set_field_by_name("attributes", Value::Map(entries.into_iter().collect()));
    message.encode_to_vec()
}

#[test]
fn every_protobuf_map_key_type_has_an_exact_canonical_selector() {
    use prost_reflect::{MapKey as Key, Value};
    let cases = [
        (
            Type::String,
            vec![Key::String("".into()), Key::String("a\0quote'".into())],
            vec!["".to_owned(), "a\0quote'".to_owned()],
        ),
        (
            Type::Bool,
            vec![Key::Bool(false), Key::Bool(true)],
            vec!["false".into(), "true".into()],
        ),
        (
            Type::Int32,
            vec![Key::I32(i32::MIN), Key::I32(i32::MAX)],
            vec![i32::MIN.to_string(), i32::MAX.to_string()],
        ),
        (
            Type::Sint32,
            vec![Key::I32(i32::MIN), Key::I32(i32::MAX)],
            vec![i32::MIN.to_string(), i32::MAX.to_string()],
        ),
        (
            Type::Sfixed32,
            vec![Key::I32(i32::MIN), Key::I32(i32::MAX)],
            vec![i32::MIN.to_string(), i32::MAX.to_string()],
        ),
        (
            Type::Int64,
            vec![Key::I64(i64::MIN), Key::I64(i64::MAX)],
            vec![i64::MIN.to_string(), i64::MAX.to_string()],
        ),
        (
            Type::Sint64,
            vec![Key::I64(i64::MIN), Key::I64(i64::MAX)],
            vec![i64::MIN.to_string(), i64::MAX.to_string()],
        ),
        (
            Type::Sfixed64,
            vec![Key::I64(i64::MIN), Key::I64(i64::MAX)],
            vec![i64::MIN.to_string(), i64::MAX.to_string()],
        ),
        (
            Type::Uint32,
            vec![Key::U32(0), Key::U32(u32::MAX)],
            vec!["0".into(), u32::MAX.to_string()],
        ),
        (
            Type::Fixed32,
            vec![Key::U32(0), Key::U32(u32::MAX)],
            vec!["0".into(), u32::MAX.to_string()],
        ),
        (
            Type::Uint64,
            vec![Key::U64(0), Key::U64(u64::MAX)],
            vec!["0".into(), u64::MAX.to_string()],
        ),
        (
            Type::Fixed64,
            vec![Key::U64(0), Key::U64(u64::MAX)],
            vec!["0".into(), u64::MAX.to_string()],
        ),
    ];
    let mut fingerprints = std::collections::HashSet::new();
    for (key_type, keys, mut expected) in cases {
        let schema = schema(key_type, Type::String);
        let extractor = Extractor::with_definition(
            &schema,
            "map_projection.Record",
            "",
            Some(&definition(pb::MappedKind::Keyword)),
        )
        .unwrap();
        assert!(
            fingerprints.insert(extractor.plan().fingerprint.clone()),
            "key type {key_type:?} shared a fingerprint"
        );
        expected.sort();
        let pairs: Vec<_> = keys
            .clone()
            .into_iter()
            .map(|key| (key, Value::String("kept".into())))
            .collect();
        let forward = map_document(&schema, pairs.clone());
        let backward = map_document(&schema, pairs.into_iter().rev().collect());
        let result = extractor.extract(&forward).unwrap().remove(0).request;
        assert_eq!(
            result
                .map_facets
                .iter()
                .map(|entry| entry.key.clone())
                .collect::<Vec<_>>(),
            expected
        );
        assert_eq!(
            result,
            extractor.extract(&backward).unwrap().remove(0).request
        );
        for (wire, kind, value) in [
            (Type::Int64, pb::MappedKind::Int64, Value::I64(i64::MIN)),
            (Type::Uint64, pb::MappedKind::Uint64, Value::U64(u64::MAX)),
        ] {
            let numeric_schema = self::schema(key_type, wire);
            let numeric = Extractor::with_definition(
                &numeric_schema,
                "map_projection.Record",
                "",
                Some(&definition(kind)),
            )
            .unwrap();
            assert!(fingerprints.insert(numeric.plan().fingerprint.clone()));
            let data = map_document(
                &numeric_schema,
                keys.iter()
                    .cloned()
                    .map(|key| (key, value.clone()))
                    .collect(),
            );
            let row = numeric.extract(&data).unwrap().remove(0).request;
            let actual: Vec<_> = row
                .map_integers
                .iter()
                .map(|e| e.key.clone())
                .chain(row.map_unsigned_integers.iter().map(|e| e.key.clone()))
                .collect();
            assert_eq!(actual, expected);
            assert!(row.map_integers.iter().all(|e| e.value == i64::MIN));
            assert!(row
                .map_unsigned_integers
                .iter()
                .all(|e| e.value == u64::MAX));
        }
    }
}

#[test]
fn keyword_map_values_keep_integer_extremes_and_boolean_defaults() {
    use prost_reflect::{MapKey, Value};
    for (kind, value, expected) in [
        (Type::Int64, Value::I64(i64::MIN), i64::MIN.to_string()),
        (Type::Uint64, Value::U64(u64::MAX), u64::MAX.to_string()),
        (Type::Bool, Value::Bool(false), "false".into()),
    ] {
        let schema = schema(Type::String, kind);
        let extractor = Extractor::with_definition(
            &schema,
            "map_projection.Record",
            "",
            Some(&definition(pb::MappedKind::Keyword)),
        )
        .unwrap();
        let data = map_document(&schema, vec![(MapKey::String("".into()), value)]);
        let row = extractor.extract(&data).unwrap().remove(0).request;
        assert_eq!(
            row.map_facets,
            vec![pb::MapFacetEntry {
                field: "attrs".into(),
                key: "".into(),
                value: expected
            }]
        );
        assert!(row.map_numerics.is_empty());
    }
}

#[test]
fn floating_map_projection_keeps_zero_and_refuses_nonfinite_values() {
    for value_type in [Type::Float, Type::Double] {
        let schema = schema(Type::Bool, value_type);
        let extractor = Extractor::with_definition(
            &schema,
            "map_projection.Record",
            "",
            Some(&definition(pb::MappedKind::Double)),
        )
        .unwrap();
        for number in [0.0, -0.0, 1.25, f64::NAN, f64::INFINITY] {
            // Explicit wire values avoid the fixture encoder omitting -0 as
            // a default. The false key is omitted and must still be present.
            let mut payload = Vec::new();
            if value_type == Type::Float {
                payload.push(0x15);
                payload.extend((number as f32).to_le_bytes());
            } else {
                payload.push(0x11);
                payload.extend(number.to_le_bytes());
            }
            let mut data = document();
            entry(&mut data, &payload);
            let result = extractor.extract(&data);
            if number.is_finite() {
                let row = result.unwrap().remove(0).request;
                assert_eq!(row.map_numerics.len(), 1);
                assert_eq!(row.map_numerics[0].key, "false");
                assert_eq!(row.map_numerics[0].value.to_bits(), number.to_bits());
            } else {
                let error = result.err().unwrap();
                assert_eq!(error.code(), tonic::Code::InvalidArgument);
                assert!(error.message().contains("finite"));
            }
        }
        assert!(extractor.extract(&document()).unwrap()[0]
            .request
            .map_numerics
            .is_empty());
    }
}

#[test]
fn schema_report_names_map_queries_and_key_value_inputs() {
    let schema = schema(Type::String, Type::String);
    let plan = derive_plan_with_definition(
        &schema,
        "map_projection.Record",
        Some(&definition(pb::MappedKind::Keyword)),
    )
    .unwrap();
    let report = plan.schema_report.unwrap();
    let projection = report
        .messages
        .iter()
        .flat_map(|m| &m.fields)
        .flat_map(|f| &f.projections)
        .find(|p| p.path == "attributes")
        .unwrap();
    assert_eq!(
        projection.query_representation,
        pb::MappedQueryRepresentation::MapStringFacet as i32
    );
    for component in ["key", "value"] {
        let path = format!("attributes.{component}");
        let input = report
            .messages
            .iter()
            .flat_map(|m| &m.fields)
            .flat_map(|f| &f.projections)
            .find(|p| p.path == path)
            .unwrap();
        assert_eq!(input.r#use, pb::ProjectionUse::Input as i32);
        assert_eq!(input.value_path, "attributes");
        assert_eq!(input.column_name, "attrs");
    }
}

#[test]
fn integer_map_projections_preserve_descriptor_domains_and_entry_presence() {
    use prost_reflect::{MapKey, Value};
    for (wire, kind, values) in [
        (
            Type::Int32,
            pb::MappedKind::Int32,
            vec![Value::I32(i32::MIN), Value::I32(i32::MAX)],
        ),
        (
            Type::Sint32,
            pb::MappedKind::Int32,
            vec![Value::I32(i32::MIN), Value::I32(i32::MAX)],
        ),
        (
            Type::Sfixed32,
            pb::MappedKind::Int32,
            vec![Value::I32(i32::MIN), Value::I32(i32::MAX)],
        ),
        (
            Type::Int64,
            pb::MappedKind::Int64,
            vec![Value::I64(i64::MIN), Value::I64(i64::MAX)],
        ),
        (
            Type::Sint64,
            pb::MappedKind::Int64,
            vec![Value::I64(i64::MIN), Value::I64((1 << 53) + 1)],
        ),
        (
            Type::Sfixed64,
            pb::MappedKind::Int64,
            vec![Value::I64(i64::MIN), Value::I64(i64::MAX)],
        ),
        (
            Type::Uint32,
            pb::MappedKind::Uint32,
            vec![Value::U32(0), Value::U32(u32::MAX)],
        ),
        (
            Type::Fixed32,
            pb::MappedKind::Uint32,
            vec![Value::U32(0), Value::U32(u32::MAX)],
        ),
        (
            Type::Uint64,
            pb::MappedKind::Uint64,
            vec![Value::U64((1 << 53) + 1), Value::U64(u64::MAX)],
        ),
        (
            Type::Fixed64,
            pb::MappedKind::Uint64,
            vec![Value::U64(0), Value::U64(u64::MAX)],
        ),
    ] {
        let schema = schema(Type::String, wire);
        let policy = definition(kind);
        let extractor =
            Extractor::with_definition(&schema, "map_projection.Record", "", Some(&policy))
                .unwrap();
        for value in values {
            let bytes = map_document(&schema, vec![(MapKey::String("".into()), value.clone())]);
            let row = extractor.extract(&bytes).unwrap().remove(0).request;
            assert!(row.map_facets.is_empty() && row.map_numerics.is_empty());
            match value {
                Value::I32(v) => assert_eq!(row.map_integers[0].value, i64::from(v)),
                Value::I64(v) => assert_eq!(row.map_integers[0].value, v),
                Value::U32(v) => assert_eq!(row.map_unsigned_integers[0].value, u64::from(v)),
                Value::U64(v) => assert_eq!(row.map_unsigned_integers[0].value, v),
                _ => unreachable!(),
            }
            let keys: Vec<_> = row
                .map_integers
                .iter()
                .map(|e| (&e.field, &e.key))
                .chain(row.map_unsigned_integers.iter().map(|e| (&e.field, &e.key)))
                .collect();
            assert_eq!(keys, vec![(&"attrs".to_string(), &String::new())]);
            // A later empty entry supplies the default key and value, replacing
            // the earlier extreme value. Omitting the map instead stays absent.
            let mut defaulted = bytes;
            entry(&mut defaulted, &[]);
            let row = extractor.extract(&defaulted).unwrap().remove(0).request;
            assert_eq!(
                row.map_integers
                    .iter()
                    .map(|e| e.value as u64)
                    .chain(row.map_unsigned_integers.iter().map(|e| e.value))
                    .collect::<Vec<_>>(),
                vec![0]
            );
        }
        let absent = extractor.extract(&document()).unwrap().remove(0).request;
        assert!(absent.map_integers.is_empty() && absent.map_unsigned_integers.is_empty());
    }
}

#[test]
fn integer_map_reports_separate_indexed_values_from_unimplemented_queries() {
    for (wire, kind, family) in [
        (Type::Int64, pb::MappedKind::Int64, pb::ColumnFamily::MapI64),
        (
            Type::Uint64,
            pb::MappedKind::Uint64,
            pb::ColumnFamily::MapU64,
        ),
    ] {
        let descriptor = schema(Type::String, wire);
        let plan = derive_plan_with_definition(
            &descriptor,
            "map_projection.Record",
            Some(&definition(kind)),
        )
        .unwrap();
        assert_eq!(
            plan.fields
                .iter()
                .find(|f| f.path == "attributes")
                .unwrap()
                .family,
            family as i32
        );
        let report = plan.schema_report.unwrap();
        let fields: Vec<_> = report
            .messages
            .iter()
            .flat_map(|m| &m.fields)
            .flat_map(|f| &f.projections)
            .collect();
        let projection = fields.iter().find(|p| p.path == "attributes").unwrap();
        assert_eq!(projection.r#use, pb::ProjectionUse::Value as i32);
        assert_eq!(
            projection.query_representation,
            pb::MappedQueryRepresentation::None as i32
        );
        assert!(projection
            .constraints
            .iter()
            .any(|c| c.contains("not yet implemented")));
        for component in ["key", "value"] {
            let input = fields
                .iter()
                .find(|p| p.path == format!("attributes.{component}"))
                .unwrap();
            assert_eq!(input.r#use, pb::ProjectionUse::Input as i32);
            assert_eq!(input.column_name, "attrs");
            assert_eq!(input.value_path, "attributes");
        }
        let keyword = derive_plan_with_definition(
            &descriptor,
            "map_projection.Record",
            Some(&definition(pb::MappedKind::Keyword)),
        )
        .unwrap();
        assert_ne!(plan.fingerprint, keyword.fingerprint);
    }
}

#[test]
fn integer_map_conversion_rejects_overflow_and_wrong_value_types() {
    use prost_reflect::{MapKey, Value};
    let schema = schema(Type::String, Type::Uint64);
    let extractor = Extractor::with_definition(
        &schema,
        "map_projection.Record",
        "",
        Some(&definition(pb::MappedKind::Int64)),
    )
    .unwrap();
    let data = map_document(
        &schema,
        vec![(MapKey::String("".into()), Value::U64(u64::MAX))],
    );
    let error = extractor.extract(&data).err().unwrap();
    assert!(
        error.message().contains("overflows") && error.message().contains("attributes"),
        "{error}"
    );
    for wire in [Type::String, Type::Bool, Type::Double, Type::Bytes] {
        for kind in [pb::MappedKind::Int64, pb::MappedKind::Uint64] {
            let error = derive_plan_with_definition(
                &self::schema(Type::String, wire),
                "map_projection.Record",
                Some(&definition(kind)),
            )
            .unwrap_err();
            assert_eq!(error.code(), tonic::Code::InvalidArgument);
            assert!(error.message().contains("attributes"), "{error}");
        }
    }
    assert!(derive_plan_with_definition(
        &self::schema(Type::String, Type::Int64),
        "map_projection.Record",
        Some(&definition(pb::MappedKind::Uint64))
    )
    .is_err());
}

#[test]
fn maps_refuse_unimplemented_numeric_or_structural_projections() {
    for (value_type, kind) in [
        (Type::Uint64, pb::MappedKind::Double),
        (Type::Bytes, pb::MappedKind::Keyword),
        (Type::Double, pb::MappedKind::Keyword),
        (Type::String, pb::MappedKind::Boolean),
        (Type::String, pb::MappedKind::Text),
    ] {
        let error = derive_plan_with_definition(
            &schema(Type::String, value_type),
            "map_projection.Record",
            Some(&definition(kind)),
        )
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
        assert!(error.message().contains("attributes"), "{error}");
    }
    let schema = schema(Type::String, Type::String);
    let mut policy = definition(pb::MappedKind::Keyword);
    policy.projections[3].field_numbers.push(2);
    assert!(
        derive_plan_with_definition(&schema, "map_projection.Record", Some(&policy))
            .unwrap_err()
            .message()
            .contains("cannot be flattened")
    );
}

fn enum_schema(syntax: &str) -> Vec<u8> {
    use prost_types::{EnumDescriptorProto, EnumOptions, EnumValueDescriptorProto};
    let mut set = FileDescriptorSet::decode(schema(Type::String, Type::Enum).as_slice()).unwrap();
    set.file[0].message_type[0].nested_type[0].field[1].type_name =
        Some(".map_projection.Kind".into());
    set.file[0].dependency.push("kind.proto".into());
    set.file.insert(
        0,
        FileDescriptorProto {
            name: Some("kind.proto".into()),
            package: Some("map_projection".into()),
            syntax: Some(syntax.into()),
            enum_type: vec![EnumDescriptorProto {
                name: Some("Kind".into()),
                value: vec![
                    EnumValueDescriptorProto {
                        name: Some("ZERO".into()),
                        number: Some(0),
                        ..Default::default()
                    },
                    EnumValueDescriptorProto {
                        name: Some("FIRST".into()),
                        number: Some(1),
                        ..Default::default()
                    },
                    EnumValueDescriptorProto {
                        name: Some("ALIAS".into()),
                        number: Some(1),
                        ..Default::default()
                    },
                ],
                options: Some(EnumOptions {
                    allow_alias: Some(true),
                    ..Default::default()
                }),
                ..Default::default()
            }],
            ..Default::default()
        },
    );
    set.encode_to_vec()
}

#[test]
fn enum_maps_keep_alias_defaults_and_closed_enum_entry_rules() {
    for syntax in ["proto2", "proto3"] {
        let schema = enum_schema(syntax);
        let extractor = Extractor::with_definition(
            &schema,
            "map_projection.Record",
            "",
            Some(&definition(pb::MappedKind::Keyword)),
        )
        .unwrap();
        let mut data = document();
        entry(&mut data, &[]);
        entry(&mut data, &[10, 1, b'x', 16, 1]);
        entry(&mut data, &[10, 1, b'x', 16, 99]);
        let row = extractor.extract(&data).unwrap().remove(0).request;
        assert_eq!(
            row.map_facets
                .iter()
                .map(|e| (e.key.as_str(), e.value.as_str()))
                .collect::<Vec<_>>(),
            vec![
                ("", "ZERO"),
                ("x", if syntax == "proto2" { "FIRST" } else { "99" })
            ]
        );
    }
}

fn two_map_schema() -> Vec<u8> {
    let mut set = FileDescriptorSet::decode(schema(Type::String, Type::String).as_slice()).unwrap();
    let root = &mut set.file[0].message_type[0];
    root.field.push(FieldDescriptorProto {
        label: Some(Label::Repeated as i32),
        type_name: Some(".map_projection.Record.ScoresEntry".into()),
        ..field("scores", 5, Type::Message)
    });
    root.nested_type.push(DescriptorProto {
        name: Some("ScoresEntry".into()),
        field: vec![
            field("key", 1, Type::Uint64),
            field("value", 2, Type::Double),
        ],
        options: Some(MessageOptions {
            map_entry: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    });
    set.encode_to_vec()
}
fn two_map_policy() -> pb::IndexDefinition {
    let mut policy = definition(pb::MappedKind::Keyword);
    policy.projections.push(pb::IndexProjection {
        field_numbers: vec![5],
        kind: pb::MappedKind::Double as i32,
        column_name: "weights".into(),
        ..Default::default()
    });
    policy
}
fn source(schema: &[u8], id: u64) -> Vec<u8> {
    use prost_reflect::{DescriptorPool, DynamicMessage, MapKey, Value};
    let pool = DescriptorPool::decode(schema).unwrap();
    let mut message = DynamicMessage::decode(
        pool.get_message_by_name("map_projection.Record").unwrap(),
        document().as_slice(),
    )
    .unwrap();
    message.set_field_by_name("id", Value::U64(id));
    message.set_field_by_name(
        "scores",
        Value::Map(
            [(MapKey::U64(u64::MAX), Value::F64(0.0))]
                .into_iter()
                .collect(),
        ),
    );
    let mut data = message.encode_to_vec();
    entry(&mut data, &[18, 3, b'o', b'l', b'd']);
    entry(&mut data, &[]);
    // Unknown source field stays verbatim alongside duplicate map occurrences.
    data.extend_from_slice(&[0x9a, 0x06, 0x03, b'r', b'a', b'w']);
    data
}

async fn assert_query(address: String, expected_ids: &[u64]) {
    use pb::search_service_server::SearchService;
    let (relay, _, relay_task) =
        pipestream_search::harness::start_relay(vec![address.clone()]).await;
    let (top, _, top_task) = pipestream_search::harness::start_relay(vec![relay]).await;
    for child in [address, top] {
        let coordinator = pipestream_search::coordinator::CoordinatorServiceImpl::new(vec![child])
            .with_bm25(
                Some(pipestream_search::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
                Default::default(),
            );
        let response = coordinator
            .bm25_search(tonic::Request::new(pb::Bm25SearchRequest {
                text: "word".into(),
                k: 10,
                analysis: Some(pipestream_search::analyzer::body_spec()),
                filter: format!("attrs[''] == '' && weights['{}'] == 0", u64::MAX),
                projections: vec![
                    pb::NamedProjection {
                        name: "id".into(),
                        expression: "id".into(),
                    },
                    pb::NamedProjection {
                        name: "empty".into(),
                        expression: "attrs['']".into(),
                    },
                    pb::NamedProjection {
                        name: "zero".into(),
                        expression: format!("weights['{}']", u64::MAX),
                    },
                ],
                map_facet_fields: vec![pb::MapFacetField {
                    column: "attrs".into(),
                    key: "".into(),
                }],
                range_facet_fields: vec![pb::RangeFacetField {
                    column: "weights".into(),
                    map: Some(pb::MapRangeFacet {
                        key: u64::MAX.to_string(),
                        edges: vec![-1.0, 1.0],
                        ..Default::default()
                    }),
                    ..Default::default()
                }],
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();
        let mut ids = Vec::new();
        for hit in response.hits {
            let Some(pb::projected_value::Value::UintValue(id)) = hit.projected[0].value else {
                panic!("missing id")
            };
            ids.push(id);
            assert_eq!(
                hit.projected[1].value,
                Some(pb::projected_value::Value::StringValue("".into()))
            );
            assert_eq!(
                hit.projected[2].value,
                Some(pb::projected_value::Value::DoubleValue(0.0))
            );
        }
        ids.sort();
        assert_eq!(ids, expected_ids);
        assert_eq!(response.facets[0].map_key.as_deref(), Some(""));
        assert_eq!(
            response.facets[0].counts,
            vec![pb::FacetCount {
                value: "".into(),
                count: expected_ids.len() as u64
            }]
        );
        assert_eq!(
            response.range_facets[0].buckets[0].count,
            expected_ids.len() as u64
        );
    }
    relay_task.abort();
    top_task.abort();
    let _ = relay_task.await;
    let _ = top_task.await;
}

#[tokio::test]
async fn mapped_maps_cross_rpc_binding_reopen_and_compaction() {
    use pb::node_service_client::NodeServiceClient;
    use pipestream_search::node::{Layout, NodeConfig};
    let schema = two_map_schema();
    let policy = two_map_policy();
    let (planning, planning_task) = common::start_coordinator(Vec::new()).await;
    let mut planner = pb::search_service_client::SearchServiceClient::connect(planning)
        .await
        .unwrap();
    let plan = planner
        .plan_index(pb::PlanIndexRequest {
            descriptor_set: schema.clone(),
            message_type: "map_projection.Record".into(),
            index_definition: Some(policy.clone()),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .plan
        .unwrap();
    let bind = pb::MappedBind {
        descriptor_set: schema.clone(),
        message_type: "map_projection.Record".into(),
        expected_fingerprint: plan.fingerprint.clone(),
        index_definition: Some(policy.clone()),
        analysis: Some(pipestream_search::analyzer::body_spec()),
        ..Default::default()
    };
    let frames = |bind: pb::MappedBind, docs: Vec<Vec<u8>>| {
        std::iter::once(pb::IngestMappedRequest {
            payload: Some(pb::ingest_mapped_request::Payload::Bind(bind)),
        })
        .chain(docs.into_iter().map(|data| pb::IngestMappedRequest {
            payload: Some(pb::ingest_mapped_request::Payload::Document(data)),
        }))
        .collect::<Vec<_>>()
    };
    let root = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("map-projection-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    for layout in [Layout::SingleImage, Layout::Segments] {
        let path = root.join(format!("{layout:?}.tv"));
        let config = NodeConfig {
            index_path: Some(path),
            layout,
            wal: true,
            unsigned_integer_fields: vec!["id".into()],
            map_facet_fields: vec!["attrs".into()],
            map_numeric_fields: vec!["weights".into()],
            analysis_addr: Some(pipestream_search::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
            ..Default::default()
        };
        let (address, server) = common::start_empty_node(config.clone()).await;
        let mut client = NodeServiceClient::connect(address.clone()).await.unwrap();
        let (shift, scale) = common::fit_calibration(8, 4, &common::unit_vectors(64, 8, 178));
        client
            .set_calibration(pb::SetCalibrationRequest {
                dim: 8,
                bit_width: 4,
                shift,
                scale,
            })
            .await
            .unwrap();
        let sources = vec![source(&schema, 1), source(&schema, 2)];
        let result = client
            .ingest_mapped(tokio_stream::iter(frames(bind.clone(), sources.clone())))
            .await
            .unwrap()
            .into_inner();
        assert_eq!(result.added, 2);
        client.flush(pb::FlushRequest {}).await.unwrap();
        assert_query(address, &[1, 2]).await;
        drop(client);
        server.abort();
        let _ = server.await;
        let (address, server) = common::start_opened_node(config.clone()).await;
        let mut client = NodeServiceClient::connect(address.clone()).await.unwrap();
        assert_query(address.clone(), &[1, 2]).await;
        // Correctly fingerprinted changes still refuse against the stored binding.
        let mut changed = bind.clone();
        changed.index_definition.as_mut().unwrap().projections.pop();
        changed.expected_fingerprint = derive_plan_with_definition(
            &schema,
            "map_projection.Record",
            changed.index_definition.as_ref(),
        )
        .unwrap()
        .fingerprint;
        let error = client
            .ingest_mapped(tokio_stream::iter(frames(changed, Vec::new())))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.message().contains("durably bound"), "{error}");
        client
            .delete_documents(pb::DeleteDocumentsRequest {
                doc_ids: vec![0],
                ..Default::default()
            })
            .await
            .unwrap();
        client
            .compact_shard(pb::CompactShardRequest {
                work_dir: root
                    .join(format!("compact-{layout:?}"))
                    .display()
                    .to_string(),
                ..Default::default()
            })
            .await
            .unwrap();
        assert_query(address, &[2]).await;
        let stored = if layout == Layout::SingleImage {
            pipestream_search::node::Bm25Shard::open(&pipestream_search::node::generation_bm25(
                &pipestream_search::node::generation_dir(config.index_path.as_ref().unwrap()),
            ))
            .unwrap()
        } else {
            pipestream_search::node::Bm25Shard::Segmented(
                pipestream_search::segmented::SegmentedShard::open(
                    pipestream_search::node::segments_root(config.index_path.as_ref().unwrap()),
                    pipestream_search::postings::Bm25Store::new()
                        .with_unsigned_integers(&["id"])
                        .with_map_facets(&["attrs"])
                        .with_map_numerics(&["weights"]),
                )
                .unwrap(),
            )
        };
        assert_eq!(
            stored.protobuf_source(0).unwrap().unwrap().0.payload,
            sources[1]
        );
        drop(stored);
        drop(client);
        server.abort();
        let _ = server.await;
    }
    planning_task.abort();
    let _ = planning_task.await;
    std::fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn mapped_bind_requires_the_declared_map_column_families() {
    let schema = two_map_schema();
    let policy = two_map_policy();
    let plan =
        derive_plan_with_definition(&schema, "map_projection.Record", Some(&policy)).unwrap();
    let (address, server) = common::start_empty_node(pipestream_search::node::NodeConfig {
        unsigned_integer_fields: vec!["id".into()],
        analysis_addr: Some(pipestream_search::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
        ..Default::default()
    })
    .await;
    let mut client = pb::node_service_client::NodeServiceClient::connect(address)
        .await
        .unwrap();
    let result = client
        .ingest_mapped(tokio_stream::iter([pb::IngestMappedRequest {
            payload: Some(pb::ingest_mapped_request::Payload::Bind(pb::MappedBind {
                descriptor_set: schema,
                message_type: "map_projection.Record".into(),
                expected_fingerprint: plan.fingerprint,
                index_definition: Some(policy),
                analysis: Some(pipestream_search::analyzer::body_spec()),
                ..Default::default()
            })),
        }]))
        .await
        .unwrap_err();
    assert_eq!(result.code(), tonic::Code::FailedPrecondition);
    assert!(result.message().contains("--map-facet-fields"), "{result}");
    assert!(
        result.message().contains("--map-numeric-fields"),
        "{result}"
    );
    server.abort();
    let _ = server.await;
}

#[test]
fn chunk_maps_keep_each_element_separate() {
    use prost_reflect::{DescriptorPool, DynamicMessage, MapKey, Value};
    for integers in [false, true] {
        let descriptor = if integers {
            integer_maps_schema()
        } else {
            two_map_schema()
        };
        let mut set = FileDescriptorSet::decode(descriptor.as_slice()).unwrap();
        set.file[0].message_type.push(DescriptorProto {
            name: Some("Parent".into()),
            field: vec![
                field("id", 1, Type::Uint64),
                FieldDescriptorProto {
                    label: Some(Label::Repeated as i32),
                    type_name: Some(".map_projection.Record".into()),
                    ..field("chunks", 2, Type::Message)
                },
            ],
            ..Default::default()
        });
        let schema = set.encode_to_vec();
        let mut policy = if integers {
            integer_maps_policy()
        } else {
            two_map_policy()
        };
        for projection in &mut policy.projections[1..] {
            projection.field_numbers.insert(0, 2);
        }
        policy.projections.push(pb::IndexProjection {
            field_numbers: vec![2],
            kind: pb::MappedKind::Nested as i32,
            role: pb::MappedRole::Chunks as i32,
            ..Default::default()
        });
        let extractor =
            Extractor::with_definition(&schema, "map_projection.Parent", "", Some(&policy))
                .unwrap();
        let pool = DescriptorPool::decode(schema.as_slice()).unwrap();
        let mut parent =
            DynamicMessage::new(pool.get_message_by_name("map_projection.Parent").unwrap());
        parent.set_field_by_name("id", Value::U64(17));
        let chunks = ["first", "second"]
            .into_iter()
            .enumerate()
            .map(|(index, text)| {
                let mut chunk = DynamicMessage::decode(
                    pool.get_message_by_name("map_projection.Record").unwrap(),
                    document().as_slice(),
                )
                .unwrap();
                chunk.set_field_by_name(
                    "attributes",
                    Value::Map(
                        [(
                            MapKey::String("".into()),
                            if integers {
                                Value::I64(if index == 0 { i64::MIN } else { i64::MAX })
                            } else {
                                Value::String(text.into())
                            },
                        )]
                        .into_iter()
                        .collect(),
                    ),
                );
                if integers {
                    chunk.set_field_by_name(
                        "scores",
                        Value::Map(
                            [(
                                MapKey::U64(u64::MAX),
                                Value::U64(if index == 0 { u64::MAX } else { 0 }),
                            )]
                            .into_iter()
                            .collect(),
                        ),
                    );
                }
                Value::Message(chunk)
            })
            .collect();
        parent.set_field_by_name("chunks", Value::List(chunks));
        let rows = extractor.extract(&parent.encode_to_vec()).unwrap();
        assert_eq!(rows.len(), 2);
        for (index, (row, text)) in rows.iter().zip(["first", "second"]).enumerate() {
            if integers {
                assert_eq!(row.request.map_integers[0].key, "");
                assert_eq!(
                    row.request.map_integers[0].value,
                    if index == 0 { i64::MIN } else { i64::MAX }
                );
                assert_eq!(
                    row.request.map_unsigned_integers[0].key,
                    u64::MAX.to_string()
                );
                assert_eq!(
                    row.request.map_unsigned_integers[0].value,
                    if index == 0 { u64::MAX } else { 0 }
                );
            } else {
                assert_eq!(
                    row.request.map_facets,
                    vec![pb::MapFacetEntry {
                        field: "attrs".into(),
                        key: "".into(),
                        value: text.into()
                    }]
                );
            }
            assert_eq!(row.request.lineage.as_ref().unwrap().parent_id, 17);
        }
    }
}

#[test]
fn message_valued_maps_remain_preserved_without_scalar_flattening() {
    let mut set =
        FileDescriptorSet::decode(schema(Type::String, Type::Message).as_slice()).unwrap();
    set.file[0].message_type[0].nested_type[0].field[1].type_name =
        Some(".map_projection.Nested".into());
    set.file[0].message_type.push(DescriptorProto {
        name: Some("Nested".into()),
        field: vec![field("text", 1, Type::String)],
        ..Default::default()
    });
    let schema = set.encode_to_vec();
    let inferred = derive_plan(&schema, "map_projection.Record").unwrap();
    assert_eq!(
        inferred
            .fields
            .iter()
            .find(|f| f.path == "attributes")
            .unwrap()
            .family,
        pb::ColumnFamily::None as i32
    );
    let report = inferred.schema_report.unwrap();
    assert!(report
        .messages
        .iter()
        .flat_map(|m| &m.fields)
        .any(|f| f.full_name == "map_projection.Nested.text"));
    let error = derive_plan_with_definition(
        &schema,
        "map_projection.Record",
        Some(&definition(pb::MappedKind::Keyword)),
    )
    .unwrap_err();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(error.message().contains("cannot flatten"), "{error}");
}

fn integer_maps_schema() -> Vec<u8> {
    let mut set = FileDescriptorSet::decode(two_map_schema().as_slice()).unwrap();
    set.file[0].message_type[0].nested_type[0].field[1].r#type = Some(Type::Int64 as i32);
    set.file[0].message_type[0].nested_type[1].field[1].r#type = Some(Type::Uint64 as i32);
    set.encode_to_vec()
}
fn integer_maps_policy() -> pb::IndexDefinition {
    let mut policy = two_map_policy();
    policy.projections[3].kind = pb::MappedKind::Int64 as i32;
    policy.projections[4].kind = pb::MappedKind::Uint64 as i32;
    policy
}
fn integer_maps_source(schema: &[u8], id: u64) -> Vec<u8> {
    use prost_reflect::{DescriptorPool, DynamicMessage, MapKey, Value};
    let pool = DescriptorPool::decode(schema).unwrap();
    let mut message = DynamicMessage::decode(
        pool.get_message_by_name("map_projection.Record").unwrap(),
        document().as_slice(),
    )
    .unwrap();
    message.set_field_by_name("id", Value::U64(id));
    if id < 3 {
        message.set_field_by_name(
            "attributes",
            Value::Map(
                [(
                    MapKey::String("".into()),
                    Value::I64(if id == 1 { i64::MIN } else { -((1 << 53) + 1) }),
                )]
                .into_iter()
                .collect(),
            ),
        );
        message.set_field_by_name(
            "scores",
            Value::Map(
                [(
                    MapKey::U64(u64::MAX),
                    Value::U64(if id == 1 { u64::MAX } else { (1 << 53) + 1 }),
                )]
                .into_iter()
                .collect(),
            ),
        );
    }
    let mut bytes = message.encode_to_vec();
    bytes.extend_from_slice(&[0x9a, 0x06, 0x03, b'r', b'a', b'w']);
    bytes
}

#[tokio::test]
async fn mapped_integer_maps_bind_persist_and_compact_without_rounding() {
    use pb::node_service_client::NodeServiceClient;
    use pipestream_search::{
        node::{self, Layout, NodeConfig},
        postings::Bm25Reader,
        segments::OpenedSegmentSet,
    };
    let schema = integer_maps_schema();
    let policy = integer_maps_policy();
    let (planner, planner_task) = common::start_coordinator(Vec::new()).await;
    let mut planner = pb::search_service_client::SearchServiceClient::connect(planner)
        .await
        .unwrap();
    let plan = planner
        .plan_index(pb::PlanIndexRequest {
            descriptor_set: schema.clone(),
            message_type: "map_projection.Record".into(),
            index_definition: Some(policy.clone()),
            ..Default::default()
        })
        .await
        .unwrap()
        .into_inner()
        .plan
        .unwrap();
    let bind = pb::MappedBind {
        descriptor_set: schema.clone(),
        message_type: "map_projection.Record".into(),
        expected_fingerprint: plan.fingerprint.clone(),
        index_definition: Some(policy.clone()),
        analysis: Some(pipestream_search::analyzer::body_spec()),
        ..Default::default()
    };
    let frames = |bind: pb::MappedBind, docs: Vec<Vec<u8>>| {
        std::iter::once(pb::IngestMappedRequest {
            payload: Some(pb::ingest_mapped_request::Payload::Bind(bind)),
        })
        .chain(docs.into_iter().map(|data| pb::IngestMappedRequest {
            payload: Some(pb::ingest_mapped_request::Payload::Document(data)),
        }))
        .collect::<Vec<_>>()
    };
    // A wrong-family declaration must fail at bind, before accepting rows or
    // installing a durable binding on the receiver.
    let (wrong, wrong_task) = common::start_empty_node(NodeConfig {
        unsigned_integer_fields: vec!["id".into()],
        map_numeric_fields: vec!["attrs".into(), "weights".into()],
        analysis_addr: Some(pipestream_search::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
        ..Default::default()
    })
    .await;
    let mut wrong = NodeServiceClient::connect(wrong).await.unwrap();
    let error = wrong
        .ingest_mapped(tokio_stream::iter(frames(
            bind.clone(),
            vec![integer_maps_source(&schema, 1)],
        )))
        .await
        .unwrap_err();
    assert_eq!(error.code(), tonic::Code::FailedPrecondition);
    assert!(error.message().contains("--map-integer-fields"), "{error}");
    assert!(
        error.message().contains("--map-unsigned-integer-fields"),
        "{error}"
    );
    assert_eq!(
        wrong
            .health(pb::HealthRequest {})
            .await
            .unwrap()
            .into_inner()
            .document_slots,
        0
    );
    wrong_task.abort();
    let _ = wrong_task.await;
    let root = std::path::PathBuf::from(env!("CARGO_TARGET_TMPDIR"))
        .join(format!("mapped-integer-maps-{}", std::process::id()));
    std::fs::create_dir_all(&root).unwrap();
    let sources: Vec<_> = (1..=3).map(|id| integer_maps_source(&schema, id)).collect();
    for layout in [Layout::SingleImage, Layout::Segments] {
        let path = root.join(format!("{layout:?}.tv"));
        let config = NodeConfig {
            index_path: Some(path.clone()),
            layout,
            wal: true,
            wal_buckets: 2,
            unsigned_integer_fields: vec!["id".into()],
            map_integer_fields: vec!["attrs".into()],
            map_unsigned_integer_fields: vec!["weights".into()],
            analysis_addr: Some(pipestream_search::analyzer::NATIVE_ANALYSIS_BACKEND.into()),
            ..Default::default()
        };
        let verify = |expected: &[u64]| {
            let mut ids = Vec::new();
            let mut inspect = |reader: &Bm25Reader| {
                reader.verify_integrity().unwrap();
                let stored = reader.binding().unwrap();
                assert_eq!(stored.plan_fingerprint, plan.fingerprint);
                let contract = pipestream_search::index_contract::decode(
                    &stored.index_contract,
                    &plan.fingerprint,
                )
                .unwrap()
                .unwrap();
                assert_eq!(
                    contract.index_definition.as_ref(),
                    plan.index_definition.as_ref()
                );
                let id_column = reader.unsigned_integer_index("id").unwrap();
                let signed = reader.map_integer_index("attrs").unwrap();
                let unsigned = reader.map_unsigned_integer_index("weights").unwrap();
                for row in 0..reader.next_doc_id() {
                    let id = reader.unsigned_integer_value(id_column, row).unwrap();
                    ids.push(id);
                    assert_eq!(
                        reader
                            .map_integer_key_ord(signed, "")
                            .and_then(|key| reader.map_integer_value(signed, key, row)),
                        match id {
                            1 => Some(i64::MIN),
                            2 => Some(-((1 << 53) + 1)),
                            _ => None,
                        }
                    );
                    assert_eq!(
                        reader
                            .map_unsigned_integer_key_ord(unsigned, &u64::MAX.to_string())
                            .and_then(|key| reader.map_unsigned_integer_value(unsigned, key, row)),
                        match id {
                            1 => Some(u64::MAX),
                            2 => Some((1 << 53) + 1),
                            _ => None,
                        }
                    );
                    let (source, ordinal) = reader.protobuf_source(row).unwrap().unwrap();
                    assert_eq!(source.descriptor_set, schema);
                    assert_eq!(source.payload, sources[id as usize - 1]);
                    assert_eq!(ordinal, None);
                }
            };
            if layout == Layout::Segments {
                let set = OpenedSegmentSet::open(node::segments_root(&path)).unwrap();
                for segment in 0..set.len() {
                    inspect(set.bm25(segment));
                }
            } else {
                let generation = node::generation_dir(&path);
                let file = if generation.exists() {
                    node::generation_bm25(&generation)
                } else {
                    node::bm25_sidecar_path(&path)
                };
                inspect(&Bm25Reader::open(&file).unwrap());
            }
            ids.sort();
            assert_eq!(ids, expected);
        };
        let (address, server) = common::start_empty_node(config.clone()).await;
        let mut client = NodeServiceClient::connect(address).await.unwrap();
        let (shift, scale) = common::fit_calibration(8, 4, &common::unit_vectors(64, 8, 712));
        client
            .set_calibration(pb::SetCalibrationRequest {
                dim: 8,
                bit_width: 4,
                shift,
                scale,
            })
            .await
            .unwrap();
        for docs in [sources[..2].to_vec(), sources[2..].to_vec()] {
            let count = docs.len() as u64;
            let response = client
                .ingest_mapped(tokio_stream::iter(frames(bind.clone(), docs)))
                .await
                .unwrap()
                .into_inner();
            assert_eq!(response.added, count);
            client.flush(pb::FlushRequest {}).await.unwrap();
        }
        verify(&[1, 2, 3]);
        let generation =
            pipestream_search::reshard::resolve_gen(&pipestream_search::wal::wal_dir(&path))
                .unwrap();
        assert_eq!(
            pipestream_search::wal::read_manifest(&generation)
                .unwrap()
                .format_version,
            7
        );
        drop(client);
        server.abort();
        let _ = server.await;
        let (address, server) = common::start_opened_node(config).await;
        let mut client = NodeServiceClient::connect(address).await.unwrap();
        verify(&[1, 2, 3]);
        // A valid alternative projection cannot silently reinterpret the file.
        let mut changed = bind.clone();
        changed.index_definition.as_mut().unwrap().projections[3].kind =
            pb::MappedKind::Int32 as i32;
        changed.expected_fingerprint = derive_plan_with_definition(
            &schema,
            "map_projection.Record",
            changed.index_definition.as_ref(),
        )
        .unwrap()
        .fingerprint;
        let error = client
            .ingest_mapped(tokio_stream::iter(frames(changed, Vec::new())))
            .await
            .unwrap_err();
        assert_eq!(error.code(), tonic::Code::FailedPrecondition);
        assert!(error.message().contains("durably bound"), "{error}");
        client
            .delete_documents(pb::DeleteDocumentsRequest {
                doc_ids: vec![0],
                ..Default::default()
            })
            .await
            .unwrap();
        client
            .compact_shard(pb::CompactShardRequest {
                work_dir: root
                    .join(format!("compact-{layout:?}"))
                    .display()
                    .to_string(),
                ..Default::default()
            })
            .await
            .unwrap();
        verify(&[2, 3]);
        server.abort();
        let _ = server.await;
    }
    planner_task.abort();
    let _ = planner_task.await;
    std::fs::remove_dir_all(root).unwrap();
}
