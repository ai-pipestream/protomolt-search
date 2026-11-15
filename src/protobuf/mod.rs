//! Protobuf decoding for index projection. This is not a source serializer:
//! callers must retain the original bytes separately to preserve unknown data.

mod decoder;
#[cfg(test)]
mod tests;

use prost::Message;
use prost_reflect::{Cardinality, DynamicMessage, MessageDescriptor, ReflectMessage, Value};
use tonic::Status;

use decoder::ProjectionDecoder;

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
