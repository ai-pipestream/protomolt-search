use prost::Message;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, Value};
use serde_json::{json, Value as Json};

use super::decode;

#[test]
fn timestamp_domain_and_microsecond_floor() {
    for (seconds, nanos, expected) in [
        (-62_135_596_800, 0, -62_135_596_800_000_000),
        (253_402_300_799, 999_999_999, 253_402_300_799_999_999),
        (-1, 999_999_999, -1),
        (-1, 1, -1_000_000),
        (0, 999, 0),
        (0, 1_000, 1),
        (0, 0, 0),
    ] {
        let value = prost_types::Timestamp { seconds, nanos };
        assert_eq!(
            crate::node::timestamp_to_epoch_micros("instant", &value).unwrap(),
            expected
        );
    }
    for (seconds, nanos) in [
        (-62_135_596_801, 999_999_999),
        (253_402_300_800, 0),
        (i64::MIN, 0),
        (i64::MAX, 0),
        (0, -1),
        (0, 1_000_000_000),
    ] {
        let error = crate::node::timestamp_to_epoch_micros(
            "instant",
            &prost_types::Timestamp { seconds, nanos },
        )
        .unwrap_err();
        assert_eq!(error.code(), tonic::Code::InvalidArgument);
    }
}

fn field_values(message: &DynamicMessage) -> Json {
    Json::Object(
        message
            .fields()
            .map(|(f, v)| (f.number().to_string(), value(v)))
            .chain(
                message
                    .extensions()
                    .map(|(f, v)| (f.number().to_string(), value(v))),
            )
            .collect(),
    )
}

fn value(value: &Value) -> Json {
    match value {
        Value::Bool(v) => json!(v),
        Value::I32(v) | Value::EnumNumber(v) => json!(v),
        Value::I64(v) => json!(v),
        Value::U32(v) => json!(v),
        Value::U64(v) => json!(v),
        Value::F32(v) => json!(v),
        Value::F64(v) => json!(v),
        Value::String(v) => json!(v),
        Value::Bytes(v) => json!(v.iter().map(|b| format!("{b:02x}")).collect::<String>()),
        Value::Message(v) => field_values(v),
        Value::List(v) => Json::Array(v.iter().map(self::value).collect()),
        Value::Map(v) => Json::Object(
            v.iter()
                .map(|(k, v)| {
                    let key = match k {
                        prost_reflect::MapKey::String(s) => s.clone(),
                        _ => panic!("fixture map has a string key"),
                    };
                    (key, self::value(v))
                })
                .collect(),
        ),
    }
}

#[test]
fn projection_matches_google_protobuf_fixtures() {
    let pool = DescriptorPool::decode(
        include_bytes!("../../tests/fixtures/protobuf-semantics/descriptor.bin").as_slice(),
    )
    .unwrap();
    let descriptor = pool.get_message_by_name("semantics.Doc").unwrap();
    let cases: Vec<Json> = serde_json::from_str(include_str!(
        "../../tests/fixtures/protobuf-semantics/cases.json"
    ))
    .unwrap();
    for case in cases {
        let wire = case["wire"].as_str().unwrap();
        let bytes: Vec<u8> = (0..wire.len())
            .step_by(2)
            .map(|at| u8::from_str_radix(&wire[at..at + 2], 16).unwrap())
            .collect();
        let result = decode(descriptor.clone(), &bytes);
        assert_eq!(
            result.is_ok(),
            case["valid"].as_bool().unwrap(),
            "{}: {result:?}",
            case["name"]
        );
        if let Ok(message) = result {
            assert_eq!(field_values(&message), case["fields"], "{}", case["name"]);
        }
    }
}

fn boundary_descriptor(syntax: &str) -> (Vec<u8>, MessageDescriptor) {
    use prost_types::field_descriptor_proto::{Label, Type};
    use prost_types::{
        DescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
    };
    let field = |name: &str, number: i32, label: Label, kind: Type| FieldDescriptorProto {
        name: Some(name.into()),
        number: Some(number),
        label: Some(label as i32),
        r#type: Some(kind as i32),
        ..Default::default()
    };
    let type_name = if syntax == "proto2" {
        "Proto2Probe"
    } else {
        "Proto3Probe"
    };
    let text_label = if syntax == "proto2" {
        Label::Required
    } else {
        Label::Optional
    };
    let set = FileDescriptorSet {
        file: vec![FileDescriptorProto {
            name: Some(format!("{syntax}.proto")),
            package: Some("wireprobe".into()),
            syntax: Some(syntax.into()),
            message_type: vec![DescriptorProto {
                name: Some(type_name.into()),
                field: vec![
                    field("text", 1, text_label, Type::String),
                    field("number", 2, Label::Optional, Type::Uint64),
                    field("signed_number", 3, Label::Optional, Type::Int64),
                ],
                ..Default::default()
            }],
            ..Default::default()
        }],
    };
    let bytes = set.encode_to_vec();
    let pool = DescriptorPool::decode(bytes.as_slice()).unwrap();
    let descriptor = pool
        .get_message_by_name(&format!("wireprobe.{type_name}"))
        .unwrap();
    (bytes, descriptor)
}

fn boundary_fields(message: &DynamicMessage) -> Json {
    Json::Object(message.fields().map(|(field, value)| {
        let value = match value {
            Value::String(text) => json!({
                "text": text,
                "utf8_hex": text.as_bytes().iter().map(|b| format!("{b:02x}")).collect::<String>(),
            }),
            Value::U64(number) => json!(number),
            Value::I64(number) => json!(number),
            other => panic!("unexpected boundary field value: {other:?}"),
        };
        (field.name().to_string(), value)
    }).collect())
}

fn decode_hex(hex: &str) -> Vec<u8> {
    (0..hex.len())
        .step_by(2)
        .map(|at| u8::from_str_radix(&hex[at..at + 2], 16).unwrap())
        .collect()
}

fn assert_source_retained(
    catalog: &crate::document_catalog::DocumentCatalog,
    descriptor_set: &[u8],
    syntax: &str,
    name: &str,
    ordinal: usize,
    payload: &[u8],
) {
    use crate::pb::accept_document_request::Mutation;
    use crate::pb::{AcceptDocumentRequest, ProtobufSource};
    let key = format!("{syntax}\0{name}").into_bytes();
    catalog
        .accept(&AcceptDocumentRequest {
            contract_version: 1,
            document_key: key.clone(),
            operation_id: format!("{syntax}-{ordinal}").into_bytes(),
            expected_version: Some(0),
            mutation: Some(Mutation::Source(ProtobufSource {
                descriptor_set: descriptor_set.to_vec(),
                message_type: format!(
                    "wireprobe.{}Probe",
                    if syntax == "proto2" {
                        "Proto2"
                    } else {
                        "Proto3"
                    }
                ),
                payload: payload.to_vec(),
            })),
            ..Default::default()
        })
        .unwrap();
    let stored = catalog.get(&key, None).unwrap().unwrap().1.unwrap();
    assert_eq!(stored.descriptor_set, descriptor_set);
    assert_eq!(stored.payload, payload);
}

#[test]
fn projection_obeys_measured_proto2_proto3_boundary_profile() {
    let fixture: Json = serde_json::from_str(include_str!(
        "../../tests/fixtures/protobuf-semantics/wire-boundaries.json"
    ))
    .unwrap();
    assert_eq!(fixture["format_version"].as_u64(), Some(1));
    let cases = fixture["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 26);
    assert!(cases
        .iter()
        .all(|case| matches!(case["syntax"].as_str(), Some("proto2" | "proto3"))));
    for syntax in ["proto2", "proto3"] {
        let (descriptor_set, descriptor) = boundary_descriptor(syntax);
        let catalog =
            crate::document_catalog::DocumentCatalog::in_memory(&format!("wire-boundary-{syntax}"))
                .unwrap();
        let syntax_cases: Vec<_> = cases
            .iter()
            .filter(|case| case["syntax"].as_str() == Some(syntax))
            .collect();
        assert_eq!(syntax_cases.len(), 13, "{syntax}");
        let mut seen = std::collections::BTreeSet::new();
        for (ordinal, case) in syntax_cases.into_iter().enumerate() {
            let name = case["case"].as_str().unwrap();
            assert!(seen.insert(name), "duplicate boundary case {syntax}/{name}");
            let bytes = decode_hex(case["input_hex"].as_str().unwrap());
            assert_eq!(case["expected_source_preserved"].as_bool(), Some(true));
            assert!(case.get("cpp_protoc").is_some());
            assert!(case.get("python_upb").is_some());
            assert_source_retained(&catalog, &descriptor_set, syntax, name, ordinal, &bytes);
            let result = decode(descriptor.clone(), &bytes);
            match case["product"]["disposition"].as_str().unwrap() {
                "accept" => assert_eq!(
                    boundary_fields(&result.unwrap()),
                    case["product"]["expected_fields"],
                    "{syntax}/{name}",
                ),
                "refuse" => {
                    let error = result.unwrap_err();
                    assert_eq!(error.code(), tonic::Code::InvalidArgument);
                    assert!(
                        error
                            .message()
                            .contains(case["product"]["error_contains"].as_str().unwrap()),
                        "{syntax}/{name}: {error}",
                    );
                }
                disposition => panic!("unknown disposition {disposition:?}"),
            }
        }
    }
}

fn semantics_descriptor() -> MessageDescriptor {
    DescriptorPool::decode(
        include_bytes!("../../tests/fixtures/protobuf-semantics/descriptor.bin").as_slice(),
    )
    .unwrap()
    .get_message_by_name("semantics.Doc")
    .unwrap()
}

fn assert_malformed_field_context(wire: &[u8], field: &str, kind: &str, private_payloads: &[&str]) {
    let error = decode(semantics_descriptor(), wire).err().unwrap();
    assert_eq!(error.code(), tonic::Code::InvalidArgument);
    assert!(
        error
            .message()
            .contains(&format!("malformed document at field {field}")),
        "{error}"
    );
    assert!(error.message().contains(kind), "{error}");
    for private in private_payloads {
        assert!(!error.message().contains(private), "{error}");
    }
}

#[test]
fn nested_message_failure_reports_the_innermost_declared_field() {
    let private_value = b"private-document-value";
    let mut inner = vec![0x08]; // semantics.Detail.left
    inner.extend_from_slice(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02]);
    let mut wire = vec![0x7a, 0x00, 0x12, private_value.len() as u8]; // required_token, body
    wire.extend_from_slice(private_value);
    wire.extend_from_slice(&[0x5a, inner.len() as u8]); // metadata
    wire.extend_from_slice(&inner);

    assert_malformed_field_context(
        &wire,
        "semantics.Detail.left",
        "invalid varint",
        &[std::str::from_utf8(private_value).unwrap()],
    );
}

#[test]
fn map_entry_failure_reports_the_synthetic_key_field_without_its_key() {
    let private_key = b"private-map-key";
    let mut entry = vec![0x0a, (private_key.len() + 1) as u8];
    entry.extend_from_slice(private_key);
    entry.push(0xff);
    let mut wire = vec![0x7a, 0x00, 0x62, entry.len() as u8]; // required_token, detail_by_key
    wire.extend_from_slice(&entry);

    assert_malformed_field_context(
        &wire,
        "semantics.Doc.DetailByKeyEntry.key",
        "invalid string value",
        &["private-map-key"],
    );
}

#[test]
fn registered_extension_framing_failure_reports_the_extension_without_payload() {
    let private_value = b"private-extension-value";
    let mut wire = vec![0x7a, 0x00, 0xa2, 0x06, 0x40]; // required_token, semantics.extra
    wire.extend_from_slice(private_value); // shorter than the declared extension message

    assert_malformed_field_context(
        &wire,
        "semantics.extra",
        "buffer underflow",
        &["private-extension-value"],
    );
}
