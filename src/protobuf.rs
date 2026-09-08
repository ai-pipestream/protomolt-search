//! Protobuf decoding for index projection. This is not a source serializer:
//! callers must retain the original bytes separately to preserve unknown data.

use prost::bytes::{Buf, BufMut};
use prost::encoding::{self, DecodeContext, WireType};
use prost::{DecodeError, Message};
use prost_reflect::{
    Cardinality, DynamicMessage, ExtensionDescriptor, FieldDescriptor, Kind, MessageDescriptor,
    ReflectMessage, Syntax, Value,
};
use tonic::Status;

/// Validate Timestamp's semantic domain before deriving any index value.
/// Protobuf decoding alone accepts every int64/int32 pair.
pub(crate) fn validate_timestamp(
    field: &str,
    timestamp: &prost_types::Timestamp,
) -> Result<(), Status> {
    if !(-62_135_596_800..=253_402_300_799).contains(&timestamp.seconds) {
        return Err(Status::invalid_argument(format!(
            "timestamp field {field:?}: seconds {} is outside [-62135596800, 253402300799] \
             (years 0001 through 9999), not a valid google.protobuf.Timestamp",
            timestamp.seconds
        )));
    }
    if !(0..1_000_000_000).contains(&timestamp.nanos) {
        return Err(Status::invalid_argument(format!(
            "timestamp field {field:?}: nanos {} is outside [0, 1e9), \
             not a valid google.protobuf.Timestamp",
            timestamp.nanos
        )));
    }
    Ok(())
}

pub(crate) fn decode(
    descriptor: MessageDescriptor,
    bytes: &[u8],
) -> Result<DynamicMessage, Status> {
    let mut message = DynamicMessage::new(descriptor);
    let mut error_field = None;
    ProjectionDecoder::new(&mut message, &mut error_field)
        .merge(bytes)
        .map_err(|e| match error_field {
            Some(field) => Status::invalid_argument(format!(
                "plan: malformed document at field {}: {e}",
                field.full_name()
            )),
            None => Status::invalid_argument(format!("plan: malformed document: {e}")),
        })?;
    validate_required(&message, "")?;
    Ok(message)
}

// prost-reflect treats every enum as open and does not check required fields.
// Intercept enums before mutation, and route nested merges through this adapter
// so an unknown closed-enum value cannot replace a known value or select a oneof.
// Framing, scalar decoding, group matching and recursion limits remain prost's.
#[derive(Debug)]
struct ProjectionDecoder<'a> {
    message: &'a mut DynamicMessage,
    unknown_closed_enum: bool,
    // Retain the innermost known field once, rather than copying a growing
    // diagnostic through each recursive merge. Never retain input values.
    error_field: &'a mut Option<Field>,
}

impl<'a> ProjectionDecoder<'a> {
    fn new(message: &'a mut DynamicMessage, error_field: &'a mut Option<Field>) -> Self {
        Self {
            message,
            unknown_closed_enum: false,
            error_field,
        }
    }
}

#[derive(Debug)]
enum Field {
    Declared(FieldDescriptor),
    Extension(ExtensionDescriptor),
}

impl Field {
    fn full_name(&self) -> &str {
        match self {
            Self::Declared(f) => f.full_name(),
            Self::Extension(f) => f.full_name(),
        }
    }

    fn kind(&self) -> Kind {
        match self {
            Self::Declared(f) => f.kind(),
            Self::Extension(f) => f.kind(),
        }
    }

    fn is_list(&self) -> bool {
        match self {
            Self::Declared(f) => f.is_list(),
            Self::Extension(f) => f.is_list(),
        }
    }

    fn is_map(&self) -> bool {
        match self {
            Self::Declared(f) => f.is_map(),
            Self::Extension(f) => f.is_map(),
        }
    }

    fn is_group(&self) -> bool {
        match self {
            Self::Declared(f) => f.is_group(),
            Self::Extension(f) => f.is_group(),
        }
    }

    fn value_mut<'a>(&self, message: &'a mut DynamicMessage) -> &'a mut Value {
        match self {
            Self::Declared(f) => message.get_field_mut(f),
            Self::Extension(f) => message.get_extension_mut(f),
        }
    }
}

impl Message for ProjectionDecoder<'_> {
    fn encode_raw(&self, buf: &mut impl BufMut) {
        self.message.encode_raw(buf);
    }

    fn encoded_len(&self) -> usize {
        self.message.encoded_len()
    }

    fn clear(&mut self) {
        self.message.clear();
        self.unknown_closed_enum = false;
        *self.error_field = None;
    }

    fn merge_field(
        &mut self,
        number: u32,
        wire: WireType,
        buf: &mut impl Buf,
        ctx: DecodeContext,
    ) -> Result<(), DecodeError> {
        let descriptor = self.message.descriptor();
        let field = descriptor
            .get_field(number)
            .map(Field::Declared)
            .or_else(|| descriptor.get_extension(number).map(Field::Extension));
        let Some(field) = field else {
            return self.message.merge_field(number, wire, buf, ctx);
        };
        let kind = field.kind();
        let expected_wire = if field.is_group() {
            WireType::StartGroup
        } else {
            kind.wire_type()
        };
        let packed = field.is_list()
            && wire == WireType::LengthDelimited
            && matches!(
                expected_wire,
                WireType::Varint | WireType::ThirtyTwoBit | WireType::SixtyFourBit
            );
        if wire != expected_wire && !packed {
            // A known number with an incompatible wire type is still unknown
            // data. Skip before selecting a oneof or establishing presence.
            // The source archive retains the original bytes independently.
            return encoding::skip_field(wire, number, buf, ctx);
        }
        let result = (|| match kind {
            Kind::Enum(enumeration) if enumeration.parent_file().syntax() == Syntax::Proto2 => {
                let mut values = Vec::new();
                if field.is_list() {
                    encoding::int32::merge_repeated(wire, &mut values, buf, ctx)?;
                } else {
                    let mut value = 0;
                    encoding::int32::merge(wire, &mut value, buf, ctx)?;
                    values.push(value);
                }
                for value in values {
                    if enumeration.get_value(value).is_none() {
                        self.unknown_closed_enum = true;
                        continue;
                    }
                    let target = field.value_mut(self.message);
                    if field.is_list() {
                        target
                            .as_list_mut()
                            .expect("enum list")
                            .push(Value::EnumNumber(value));
                    } else {
                        *target = Value::EnumNumber(value);
                    }
                }
                Ok(())
            }
            Kind::Message(child_descriptor) => {
                if field.is_map() {
                    let mut entry = DynamicMessage::new(child_descriptor.clone());
                    let mut decoder = ProjectionDecoder::new(&mut entry, self.error_field);
                    encoding::message::merge(wire, &mut decoder, buf, ctx)?;
                    // Closed enum map values move the entire entry to unknown
                    // fields, leaving any prior entry for this key unchanged.
                    if decoder.unknown_closed_enum {
                        return Ok(());
                    }
                    let key = entry
                        .get_field(&child_descriptor.map_entry_key_field())
                        .into_owned()
                        .into_map_key()
                        .expect("validated map key");
                    let value = entry
                        .get_field(&child_descriptor.map_entry_value_field())
                        .into_owned();
                    field
                        .value_mut(self.message)
                        .as_map_mut()
                        .expect("map field")
                        .insert(key, value);
                    return Ok(());
                }
                if field.is_list() {
                    let mut child = DynamicMessage::new(child_descriptor);
                    merge_child(
                        number,
                        wire,
                        field.is_group(),
                        &mut child,
                        self.error_field,
                        buf,
                        ctx,
                    )?;
                    field
                        .value_mut(self.message)
                        .as_list_mut()
                        .expect("message list")
                        .push(Value::Message(child));
                } else {
                    let child = field
                        .value_mut(self.message)
                        .as_message_mut()
                        .expect("message field");
                    merge_child(
                        number,
                        wire,
                        field.is_group(),
                        child,
                        self.error_field,
                        buf,
                        ctx,
                    )?;
                }
                Ok(())
            }
            _ => self.message.merge_field(number, wire, buf, ctx),
        })();
        if result.is_err() && self.error_field.is_none() {
            *self.error_field = Some(field);
        }
        result
    }
}

fn merge_child(
    number: u32,
    wire: WireType,
    group: bool,
    child: &mut DynamicMessage,
    error_field: &mut Option<Field>,
    buf: &mut impl Buf,
    ctx: DecodeContext,
) -> Result<(), DecodeError> {
    let mut decoder = ProjectionDecoder::new(child, error_field);
    if group {
        encoding::group::merge(number, wire, &mut decoder, buf, ctx)
    } else {
        encoding::message::merge(wire, &mut decoder, buf, ctx)
    }
}

// Check the merged message, not each wire fragment. Singular submessages can
// satisfy their required fields over several occurrences of the same field.
fn validate_required(message: &DynamicMessage, path: &str) -> Result<(), Status> {
    for field in message.descriptor().fields() {
        if field.cardinality() == Cardinality::Required && !message.has_field(&field) {
            return Err(Status::invalid_argument(format!(
                "plan: {}{}: required protobuf field is absent",
                path,
                field.name()
            )));
        }
    }
    for (field, value) in message.fields() {
        validate_value(value, &format!("{path}{}", field.name()))?;
    }
    for (field, value) in message.extensions() {
        validate_value(value, &format!("{path}[{}]", field.full_name()))?;
    }
    Ok(())
}

fn validate_value(value: &Value, path: &str) -> Result<(), Status> {
    match value {
        Value::Message(message) => validate_required(message, &format!("{path}.")),
        Value::List(values) => {
            for (ordinal, value) in values.iter().enumerate() {
                if let Value::Message(message) = value {
                    validate_required(message, &format!("{path}[{ordinal}]."))?;
                }
            }
            Ok(())
        }
        Value::Map(values) => {
            if !values
                .values()
                .next()
                .is_some_and(|v| matches!(v, Value::Message(_)))
            {
                return Ok(());
            }
            // Stable refusal order without printing potentially private map keys.
            let mut entries: Vec<_> = values.iter().collect();
            entries.sort_by_key(|(key, _)| *key);
            for (ordinal, (_, value)) in entries.into_iter().enumerate() {
                validate_value(value, &format!("{path}[entry {ordinal}]"))?;
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

#[cfg(test)]
mod tests {
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

    use super::*;
    use prost_reflect::DescriptorPool;
    use serde_json::{json, Value as Json};

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
            include_bytes!("../tests/fixtures/protobuf-semantics/descriptor.bin").as_slice(),
        )
        .unwrap();
        let descriptor = pool.get_message_by_name("semantics.Doc").unwrap();
        let cases: Vec<Json> = serde_json::from_str(include_str!(
            "../tests/fixtures/protobuf-semantics/cases.json"
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
            "../tests/fixtures/protobuf-semantics/wire-boundaries.json"
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
            let catalog = crate::document_catalog::DocumentCatalog::in_memory(&format!(
                "wire-boundary-{syntax}"
            ))
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
            include_bytes!("../tests/fixtures/protobuf-semantics/descriptor.bin").as_slice(),
        )
        .unwrap()
        .get_message_by_name("semantics.Doc")
        .unwrap()
    }

    fn assert_malformed_field_context(
        wire: &[u8],
        field: &str,
        kind: &str,
        private_payloads: &[&str],
    ) {
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
}
