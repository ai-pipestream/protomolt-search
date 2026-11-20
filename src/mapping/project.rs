//! Projection of one decoded value onto its planned landing.

use std::collections::HashMap;

use super::MappingError;
use prost_reflect::{DynamicMessage, MapKey, Value};

use super::extractor::{Land, Slot};
use super::refuse_at;

fn integer_value(path: &str, value: &Value) -> Result<i64, MappingError> {
    match value {
        Value::I32(v) => Ok(i64::from(*v)),
        Value::I64(v) => Ok(*v),
        Value::U32(v) => Ok(i64::from(*v)),
        Value::U64(v) => i64::try_from(*v)
            .map_err(|_| refuse_at(path, format!("unsigned value {v} overflows the i64 column"))),
        _ => Err(refuse_at(path, "expected an integer value")),
    }
}

pub(super) fn project_leaf(land: &Land, path: &str, value: &Value) -> Result<Slot, MappingError> {
    Ok(match (land, value) {
        (Land::Map(inner), Value::Map(entries)) => project_map(inner, path, entries)?,

        (Land::Wrapper(inner), Value::Message(message)) => {
            let value = message.get_field_by_name("value").ok_or_else(|| {
                refuse_at(path, "scalar wrapper value does not match its descriptor")
            })?;
            project_leaf(inner, path, value.as_ref())?
        }
        (Land::Text | Land::FacetStr, Value::String(v)) => Slot::Str(v.clone()),
        (Land::FacetBool, Value::Bool(v)) => Slot::Str(v.to_string()),
        (Land::FacetEnum(table), Value::EnumNumber(v)) => Slot::Str(
            table
                .get(&i64::from(*v))
                .cloned()
                .unwrap_or_else(|| v.to_string()),
        ),

        (Land::FacetInt, v) => Slot::Str(integer_value(path, v)?.to_string()),

        (Land::FacetUint, Value::U32(v)) => Slot::Str(v.to_string()),
        (Land::FacetUint, Value::U64(v)) => Slot::Str(v.to_string()),

        (Land::Int, v) => Slot::Int(integer_value(path, v)?),

        (Land::Uint, Value::U32(v)) => Slot::Uint(u64::from(*v)),
        (Land::Uint, Value::U64(v)) => Slot::Uint(*v),
        (Land::Num, Value::F32(v)) => Slot::Num(f64::from(*v)),
        (Land::Num, Value::F64(v)) => Slot::Num(*v),

        (Land::Date, Value::Message(v)) => project_timestamp(path, v)?,

        (Land::Vector, Value::List(values)) => Slot::Floats(
            values
                .iter()
                .map(|v| match v {
                    Value::F32(v) => *v,
                    Value::F64(v) => *v as f32,
                    _ => unreachable!("validated vector type"),
                })
                .collect(),
        ),
        _ => {
            return Err(refuse_at(
                path,
                "value does not match the planned projection",
            ))
        }
    })
}

/// Projects a map's entries in key order onto the map column of the value
/// landing.
fn project_map(
    inner: &Land,
    path: &str,
    entries: &HashMap<MapKey, Value>,
) -> Result<Slot, MappingError> {
    let mut entries = entries
        .iter()
        .map(|(key, value)| {
            let key = match key {
                MapKey::String(value) => value.clone(),
                MapKey::Bool(value) => value.to_string(),
                MapKey::I32(value) => value.to_string(),
                MapKey::I64(value) => value.to_string(),
                MapKey::U32(value) => value.to_string(),
                MapKey::U64(value) => value.to_string(),
            };
            Ok((key, project_leaf(inner, path, value)?))
        })
        .collect::<Result<Vec<_>, MappingError>>()?;
    entries.sort_by(|a, b| a.0.cmp(&b.0));
    Ok(match inner {
        Land::Int => Slot::MapInteger(
            entries
                .into_iter()
                .map(|(key, slot)| {
                    let Slot::Int(value) = slot else {
                        unreachable!("compiled signed map")
                    };
                    (key, value)
                })
                .collect(),
        ),
        Land::Uint => Slot::MapUnsignedInteger(
            entries
                .into_iter()
                .map(|(key, slot)| {
                    let Slot::Uint(value) = slot else {
                        unreachable!("compiled unsigned map")
                    };
                    (key, value)
                })
                .collect(),
        ),
        Land::Num => Slot::MapNumeric(
            entries
                .into_iter()
                .map(|(key, slot)| {
                    let Slot::Num(value) = slot else {
                        unreachable!("compiled numeric map")
                    };
                    if !value.is_finite() {
                        return Err(refuse_at(path, "numeric map values must be finite"));
                    }
                    Ok((key, value))
                })
                .collect::<Result<_, MappingError>>()?,
        ),
        _ => Slot::MapFacet(
            entries
                .into_iter()
                .map(|(key, slot)| {
                    let Slot::Str(value) = slot else {
                        unreachable!("compiled facet map")
                    };
                    (key, value)
                })
                .collect(),
        ),
    })
}

fn project_timestamp(path: &str, v: &DynamicMessage) -> Result<Slot, MappingError> {
    let seconds = v
        .get_field_by_name("seconds")
        .and_then(|v| v.as_i64())
        .ok_or_else(|| refuse_at(path, "Timestamp seconds does not match its descriptor"))?;
    let nanos = v
        .get_field_by_name("nanos")
        .and_then(|v| v.as_i32())
        .ok_or_else(|| refuse_at(path, "Timestamp nanos does not match its descriptor"))?;
    crate::protobuf::validate_timestamp(path, &prost_types::Timestamp { seconds, nanos })
        .map_err(|err| MappingError::invalid_argument(err.message()))?;

    Ok(Slot::Ts { seconds, nanos })
}
