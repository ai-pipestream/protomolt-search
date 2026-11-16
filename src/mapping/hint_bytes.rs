//! Extraction of hint extensions from the raw descriptor-set bytes.

use std::collections::HashMap;

use super::MappingError;
use prost::Message as _;
use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorSet};

use super::type_index::MsgKey;
use super::{refuse, HINT_EXTENSION_NAME, HINT_EXTENSION_NUMBER};
use crate::pb::hints;

/// Decoded hints keyed by (message address, field index).
pub(super) type HintMap = HashMap<(MsgKey, usize), hints::FieldIndexHint>;

/// The raw pass below reads extension number 59100471 off FieldOptions
/// unconditionally, so a descriptor set that declares a DIFFERENT
/// extension under that number would decode garbage as a hint. Refuse
/// such a set by name instead: the number is registered to the
/// ProtoMolt hint vocabulary, and a modified indexing_hints.proto is
/// exactly the drift the byte-identity check exists to catch.
pub(super) fn check_extension_declarations(set: &FileDescriptorSet) -> Result<(), MappingError> {
    fn check_list(extensions: &[FieldDescriptorProto]) -> Result<(), MappingError> {
        for extension in extensions {
            let extends_field_options = extension.extendee() == ".google.protobuf.FieldOptions";
            let claims_hint_number = extension.number() == HINT_EXTENSION_NUMBER as i32;
            let is_the_hint =
                extension.type_name() == ".ai.protomolt.proto.index.hints.v1.FieldIndexHint";
            if extends_field_options
                && claims_hint_number
                && extension.type_name() == ".ai.pipestream.proto.index.hints.v1.FieldIndexHint"
            {
                return Err(refuse(format!(
                    "the descriptor set declares retired ai.pipestream.proto.index.hints.v1.\
                     FieldIndexHint at extension number {HINT_EXTENSION_NUMBER}; recompile the \
                     schema against ai.protomolt.proto.index.hints.v1 and bind a new generation"
                )));
            }
            if extends_field_options && claims_hint_number && !is_the_hint {
                return Err(refuse(format!(
                    "the descriptor set declares extension number {HINT_EXTENSION_NUMBER} on \
                     google.protobuf.FieldOptions with type {:?}, but that number is \
                     {HINT_EXTENSION_NAME}; the set carries a modified copy of \
                     indexing_hints.proto",
                    extension.type_name()
                )));
            }
        }
        Ok(())
    }
    fn check_message(message: &DescriptorProto) -> Result<(), MappingError> {
        check_list(&message.extension)?;
        for nested in &message.nested_type {
            check_message(nested)?;
        }
        Ok(())
    }
    for file in &set.file {
        check_list(&file.extension)?;
        for message in &file.message_type {
            check_message(message)?;
        }
    }
    Ok(())
}

/// Read a varint at `*i`, advancing it.
fn varint(b: &[u8], i: &mut usize) -> Result<u64, MappingError> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = *b
            .get(*i)
            .ok_or_else(|| refuse("descriptor bytes end inside a varint"))?;
        *i += 1;
        if shift >= 64 {
            return Err(refuse("descriptor varint is longer than 64 bits"));
        }
        value |= u64::from(byte & 0x7f) << shift;
        if byte & 0x80 == 0 {
            return Ok(value);
        }
        shift += 7;
    }
}

/// Skip one field body of the given wire type.
fn skip_field(b: &[u8], i: &mut usize, wire: u64) -> Result<(), MappingError> {
    match wire {
        0 => {
            varint(b, i)?;
        }
        1 => {
            *i = i
                .checked_add(8)
                .filter(|end| *end <= b.len())
                .ok_or_else(|| refuse("descriptor bytes end inside a fixed64"))?;
        }
        2 => {
            let len = varint(b, i)? as usize;
            *i = i
                .checked_add(len)
                .filter(|end| *end <= b.len())
                .ok_or_else(|| refuse("descriptor bytes end inside a length-delimited field"))?;
        }
        5 => {
            *i = i
                .checked_add(4)
                .filter(|end| *end <= b.len())
                .ok_or_else(|| refuse("descriptor bytes end inside a fixed32"))?;
        }
        other => {
            return Err(refuse(format!(
                "descriptor bytes use unsupported wire type {other}"
            )))
        }
    }
    Ok(())
}

/// Yield each length-delimited occurrence of `field` in `b`, in order,
/// skipping everything else.
fn each_len_delimited(
    b: &[u8],
    field: u64,
    mut visit: impl FnMut(&[u8]) -> Result<(), MappingError>,
) -> Result<(), MappingError> {
    let mut i = 0;
    while i < b.len() {
        let tag = varint(b, &mut i)?;
        let (number, wire) = (tag >> 3, tag & 7);
        if number == field && wire == 2 {
            let len = varint(b, &mut i)? as usize;
            let end = i
                .checked_add(len)
                .filter(|end| *end <= b.len())
                .ok_or_else(|| refuse("descriptor bytes end inside a length-delimited field"))?;
            visit(&b[i..end])?;
            i = end;
        } else {
            skip_field(b, &mut i, wire)?;
        }
    }
    Ok(())
}

/// Walk the raw FileDescriptorSet bytes and collect every
/// `(ai.protomolt.proto.index.hints.v1.index)` extension payload,
/// keyed by (message address, field index). Multiple occurrences of
/// the extension on one field concatenate, protobuf's own merge rule
/// for embedded messages.
pub(super) fn extract_hints(
    bytes: &[u8],
) -> Result<HashMap<(MsgKey, usize), hints::FieldIndexHint>, MappingError> {
    let mut raw: HashMap<(MsgKey, usize), Vec<u8>> = HashMap::new();
    let mut fi = 0usize;
    // FileDescriptorSet.file = 1
    each_len_delimited(bytes, 1, |file| {
        let mut mi = 0usize;
        // FileDescriptorProto.message_type = 4
        each_len_delimited(file, 4, |message| {
            collect_message_hints(message, (fi, vec![mi]), &mut raw)?;
            mi += 1;
            Ok(())
        })?;
        fi += 1;
        Ok(())
    })?;
    let mut decoded = HashMap::with_capacity(raw.len());
    for (key, bytes) in raw {
        let hint = hints::FieldIndexHint::decode(bytes.as_slice()).map_err(|e| {
            refuse(format!(
                "a field's ({HINT_EXTENSION_NAME}) hint does not decode: {e}"
            ))
        })?;
        decoded.insert(key, hint);
    }
    Ok(decoded)
}

fn collect_message_hints(
    message: &[u8],
    key: MsgKey,
    out: &mut HashMap<(MsgKey, usize), Vec<u8>>,
) -> Result<(), MappingError> {
    let mut field_idx = 0usize;
    // DescriptorProto.field = 2
    each_len_delimited(message, 2, |field| {
        // FieldDescriptorProto.options = 8
        each_len_delimited(field, 8, |options| {
            // FieldOptions extension 59100471
            each_len_delimited(options, HINT_EXTENSION_NUMBER, |hint| {
                out.entry((key.clone(), field_idx))
                    .or_default()
                    .extend_from_slice(hint);
                Ok(())
            })
        })?;
        field_idx += 1;
        Ok(())
    })?;
    let mut nested_idx = 0usize;
    // DescriptorProto.nested_type = 3
    each_len_delimited(message, 3, |nested| {
        let mut nested_key = key.1.clone();
        nested_key.push(nested_idx);
        collect_message_hints(nested, (key.0, nested_key), out)?;
        nested_idx += 1;
        Ok(())
    })?;
    Ok(())
}
