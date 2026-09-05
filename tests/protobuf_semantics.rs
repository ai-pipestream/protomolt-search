use pipestream_search::mapping::{derive_plan, Extractor};
use prost::Message;
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{
    DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    OneofDescriptorProto,
};
#[derive(Clone, PartialEq, prost::Message)]
struct OracleDoc {
    #[prost(oneof = "Choice", tags = "4, 5")]
    choice: Option<Choice>,
    #[prost(string, optional, tag = "6")]
    note: Option<String>,
}
#[derive(Clone, PartialEq, prost::Oneof)]
enum Choice {
    #[prost(int64, tag = "4")]
    A(i64),
    #[prost(int64, tag = "5")]
    B(i64),
}
fn field(name: &str, number: i32, ty: Type) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.into()),
        number: Some(number),
        r#type: Some(ty as i32),
        label: Some(Label::Optional as i32),
        ..Default::default()
    }
}
fn schema() -> FileDescriptorSet {
    let mut vector = field("embedding", 3, Type::Float);
    vector.label = Some(Label::Repeated as i32);
    let mut a = field("a", 4, Type::Int64);
    a.oneof_index = Some(0);
    let mut b = field("b", 5, Type::Int64);
    b.oneof_index = Some(0);
    let mut note = field("note", 6, Type::String);
    note.proto3_optional = Some(true);
    note.oneof_index = Some(1);
    FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some("audit.proto".into()),
            package: Some("audit".into()),
            syntax: Some("proto3".into()),
            message_type: vec![DescriptorProto {
                name: Some("Doc".into()),
                field: vec![
                    field("id", 1, Type::Int64),
                    field("body", 2, Type::String),
                    vector,
                    a,
                    b,
                    note,
                ],
                oneof_decl: vec![
                    OneofDescriptorProto {
                        name: Some("choice".into()),
                        ..Default::default()
                    },
                    OneofDescriptorProto {
                        name: Some("_note".into()),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}
fn base_wire() -> Vec<u8> {
    let mut wire = vec![8, 1, 18, 1, b'x', 26, 4];
    wire.extend_from_slice(&1.0f32.to_le_bytes());
    wire
}

#[test]
fn decode_changes_invalidate_the_plan() {
    let original = schema();
    let fingerprint = derive_plan(&original.encode_to_vec(), "audit.Doc")
        .unwrap()
        .fingerprint;
    for change in 0..4 {
        let mut changed = original.clone();
        let field = &mut changed.file[0].message_type[0].field[3];
        match change {
            0 => field.r#type = Some(Type::Sint64 as i32),
            1 => field.number = Some(14),
            2 => field.oneof_index = None,
            3 => field.r#type = Some(Type::Int32 as i32),
            _ => unreachable!(),
        }
        assert_ne!(
            fingerprint,
            derive_plan(&changed.encode_to_vec(), "audit.Doc")
                .unwrap()
                .fingerprint
        );
    }
}

#[test]
fn oneof_and_optional_presence_match_generated_parser() {
    let extractor = Extractor::new(&schema().encode_to_vec(), "audit.Doc", "body").unwrap();
    for suffix in [
        vec![32, 7, 40, 9, 50, 0],
        vec![40, 9, 32, 0, 50, 0],
        vec![32, 7],
    ] {
        let mut wire = base_wire();
        wire.extend(suffix);
        let oracle = OracleDoc::decode(wire.as_slice()).unwrap();
        let docs = extractor.extract(&wire).unwrap();
        let request = &docs[0].request;
        let choice: Vec<_> = request
            .integers
            .iter()
            .filter_map(|v| match v.field.as_str() {
                "a" => Some(Choice::A(v.value)),
                "b" => Some(Choice::B(v.value)),
                _ => None,
            })
            .collect();
        assert_eq!(choice, oracle.choice.into_iter().collect::<Vec<_>>());
        assert_eq!(
            request
                .fields
                .iter()
                .find(|v| v.field == "note")
                .map(|v| v.text.clone()),
            oracle.note
        );
    }
}

#[test]
fn implicit_default_has_no_invented_presence() {
    let mut schema = schema();
    let root = &mut schema.file[0].message_type[0];
    root.field.push(field("count", 7, Type::Int64));
    let extractor = Extractor::new(&schema.encode_to_vec(), "audit.Doc", "body").unwrap();
    let missing = extractor.extract(&base_wire()).unwrap();
    let mut explicit_zero = base_wire();
    explicit_zero.extend([56, 0]);
    let explicit = extractor.extract(&explicit_zero).unwrap();
    assert_eq!(missing[0].request, explicit[0].request);
    assert_eq!(
        missing[0]
            .request
            .integers
            .iter()
            .find(|v| v.field == "count")
            .unwrap()
            .value,
        0
    );
}

#[derive(Clone, PartialEq, prost::Message)]
struct Detail {
    #[prost(oneof = "DetailChoice", tags = "1, 2, 3")]
    choice: Option<DetailChoice>,
    #[prost(int64, optional, tag = "4")]
    count: Option<i64>,
}
#[derive(Clone, PartialEq, prost::Oneof)]
enum DetailChoice {
    #[prost(int64, tag = "1")]
    X(i64),
    #[prost(int64, tag = "2")]
    Y(i64),
    #[prost(bytes, tag = "3")]
    Opaque(Vec<u8>),
}
#[derive(Clone, PartialEq, prost::Message)]
struct NestedOracle {
    #[prost(message, optional, tag = "7")]
    detail: Option<Detail>,
}
fn nested_schema() -> FileDescriptorSet {
    let mut schema = schema();
    let mut detail = field("detail", 7, Type::Message);
    detail.type_name = Some(".audit.Detail".into());
    schema.file[0].message_type[0].field.push(detail);
    let mut fields = vec![
        field("x", 1, Type::Int64),
        field("y", 2, Type::Int64),
        field("opaque", 3, Type::Bytes),
    ];
    for field in &mut fields {
        field.oneof_index = Some(0);
    }
    let mut count = field("count", 4, Type::Int64);
    count.proto3_optional = Some(true);
    count.oneof_index = Some(1);
    fields.push(count);
    schema.file[0].message_type.push(DescriptorProto {
        name: Some("Detail".into()),
        field: fields,
        oneof_decl: vec![
            OneofDescriptorProto {
                name: Some("choice".into()),
                ..Default::default()
            },
            OneofDescriptorProto {
                name: Some("_count".into()),
                ..Default::default()
            },
        ],
        ..Default::default()
    });
    schema
}
#[test]
fn merged_messages_and_unindexed_oneof_members_match_generated_parser() {
    let extractor = Extractor::new(&nested_schema().encode_to_vec(), "audit.Doc", "body").unwrap();
    for parts in [
        vec![vec![8, 7], vec![16, 9]],
        vec![vec![8, 7], vec![26, 0]],
        vec![vec![8, 7], vec![32, 5], vec![8, 0]],
    ] {
        let mut wire = base_wire();
        for part in parts {
            wire.extend([58, part.len() as u8]);
            wire.extend(part);
        }
        let oracle = NestedOracle::decode(wire.as_slice())
            .unwrap()
            .detail
            .unwrap();
        let rows = extractor.extract(&wire).unwrap();
        let values = &rows[0].request.integers;
        for (name, expected) in [
            (
                "detail_x",
                match oracle.choice {
                    Some(DetailChoice::X(v)) => Some(v),
                    _ => None,
                },
            ),
            (
                "detail_y",
                match oracle.choice {
                    Some(DetailChoice::Y(v)) => Some(v),
                    _ => None,
                },
            ),
            ("detail_count", oracle.count),
        ] {
            assert_eq!(
                values.iter().find(|v| v.field == name).map(|v| v.value),
                expected,
                "{name}"
            );
        }
    }
}

#[test]
fn scalar_width_is_decoded_before_projection() {
    let mut schema = schema();
    schema.file[0].message_type[0]
        .field
        .push(field("count", 7, Type::Int32));
    let extractor = Extractor::new(&schema.encode_to_vec(), "audit.Doc", "body").unwrap();
    // A valid non-canonical int32 encoding whose lower 32 bits are -1.
    let mut wire = base_wire();
    wire.extend([56, 255, 255, 255, 255, 15]);
    #[derive(Clone, PartialEq, prost::Message)]
    struct Oracle {
        #[prost(int32, tag = "7")]
        count: i32,
    }
    let oracle = Oracle::decode(wire.as_slice()).unwrap();
    let rows = extractor.extract(&wire).unwrap();
    assert_eq!(
        rows[0]
            .request
            .integers
            .iter()
            .find(|v| v.field == "count")
            .unwrap()
            .value,
        i64::from(oracle.count)
    );
}

#[test]
fn malformed_unindexed_values_are_still_rejected() {
    let mut schema = schema();
    let mut names = field("names", 7, Type::String);
    names.label = Some(Label::Repeated as i32);
    schema.file[0].message_type[0].field.push(names);
    let extractor = Extractor::new(&schema.encode_to_vec(), "audit.Doc", "body").unwrap();
    let mut wire = base_wire();
    wire.extend([58, 1, 255]);
    assert_eq!(
        extractor.extract(&wire).err().unwrap().code(),
        tonic::Code::InvalidArgument
    );
}

#[test]
fn invalid_descriptor_cannot_receive_a_plan() {
    let mut schema = schema();
    schema.file[0].message_type[0].field[3].number = Some(1);
    assert_eq!(
        derive_plan(&schema.encode_to_vec(), "audit.Doc")
            .unwrap_err()
            .code(),
        tonic::Code::InvalidArgument
    );
}

#[test]
fn source_comments_and_file_order_do_not_change_semantic_identity() {
    let original = schema();
    let plan = derive_plan(&original.encode_to_vec(), "audit.Doc").unwrap();
    let mut changed = original.clone();
    changed.file[0].source_code_info = Some(prost_types::SourceCodeInfo {
        location: vec![prost_types::source_code_info::Location {
            leading_comments: Some("A comment".into()),
            ..Default::default()
        }],
    });
    changed.file.insert(
        0,
        FileDescriptorProto {
            name: Some("unrelated.proto".into()),
            ..Default::default()
        },
    );
    let changed = derive_plan(&changed.encode_to_vec(), "audit.Doc").unwrap();
    assert_eq!(plan.fingerprint, changed.fingerprint);
    assert_ne!(plan.descriptor_sha256, changed.descriptor_sha256);
}

#[test]
fn enum_aliases_use_the_first_declared_name_and_affect_identity() {
    let mut schema = schema();
    let mut state = field("state", 7, Type::Enum);
    state.type_name = Some(".audit.State".into());
    schema.file[0].message_type[0].field.push(state);
    schema.file[0]
        .enum_type
        .push(prost_types::EnumDescriptorProto {
            name: Some("State".into()),
            value: [("UNSPECIFIED", 0), ("READY", 1), ("AVAILABLE", 1)]
                .into_iter()
                .map(|(name, number)| prost_types::EnumValueDescriptorProto {
                    name: Some(name.into()),
                    number: Some(number),
                    ..Default::default()
                })
                .collect(),
            options: Some(prost_types::EnumOptions {
                allow_alias: Some(true),
                ..Default::default()
            }),
            ..Default::default()
        });
    let original = Extractor::new(&schema.encode_to_vec(), "audit.Doc", "body").unwrap();
    let mut wire = base_wire();
    wire.extend([56, 1]);
    let rows = original.extract(&wire).unwrap();
    assert_eq!(
        rows[0]
            .request
            .facets
            .iter()
            .find(|v| v.field == "state")
            .unwrap()
            .value,
        "READY"
    );
    schema.file[0].enum_type[0].value.swap(1, 2);
    let changed = Extractor::new(&schema.encode_to_vec(), "audit.Doc", "body").unwrap();
    assert_ne!(original.plan().fingerprint, changed.plan().fingerprint);
    let rows = changed.extract(&wire).unwrap();
    assert_eq!(
        rows[0]
            .request
            .facets
            .iter()
            .find(|v| v.field == "state")
            .unwrap()
            .value,
        "AVAILABLE"
    );
}
