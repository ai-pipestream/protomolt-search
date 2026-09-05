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

pub(crate) fn decode(
    descriptor: MessageDescriptor,
    bytes: &[u8],
) -> Result<DynamicMessage, Status> {
    let mut message = DynamicMessage::new(descriptor);
    ProjectionDecoder::new(&mut message)
        .merge(bytes)
        .map_err(|e| Status::invalid_argument(format!("plan: malformed document: {e}")))?;
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
}

impl<'a> ProjectionDecoder<'a> {
    fn new(message: &'a mut DynamicMessage) -> Self {
        Self {
            message,
            unknown_closed_enum: false,
        }
    }
}

enum Field {
    Declared(FieldDescriptor),
    Extension(ExtensionDescriptor),
}

impl Field {
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
        match field.kind() {
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
                    let mut decoder = ProjectionDecoder::new(&mut entry);
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
                    merge_child(number, wire, field.is_group(), &mut child, buf, ctx)?;
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
                    merge_child(number, wire, field.is_group(), child, buf, ctx)?;
                }
                Ok(())
            }
            _ => self.message.merge_field(number, wire, buf, ctx),
        }
    }
}

fn merge_child(
    number: u32,
    wire: WireType,
    group: bool,
    child: &mut DynamicMessage,
    buf: &mut impl Buf,
    ctx: DecodeContext,
) -> Result<(), DecodeError> {
    let mut decoder = ProjectionDecoder::new(child);
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
}
