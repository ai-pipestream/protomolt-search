//! The type index over a decoded descriptor set: messages by qualified
//! name with their structural addresses, and the enum names.

use std::collections::HashMap;

use super::MappingError;
use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorSet};

use super::refuse_at;

/// Structural address of one message: file index plus the nested-type
/// index path, the key the raw hint pass and the decoded walk share.
pub(super) type MsgKey = (usize, Vec<usize>);

pub(super) struct MsgEntry<'a> {
    pub(super) desc: &'a DescriptorProto,
    pub(super) key: MsgKey,
    /// Fully qualified name, no leading dot.
    pub(super) full: String,
}

pub(super) struct TypeIndex<'a> {
    /// Fully qualified message name (no leading dot) to its descriptor
    /// and structural address.
    pub(super) messages: HashMap<String, MsgEntry<'a>>,
    /// Fully qualified enum names.
    pub(super) enums: std::collections::HashSet<String>,
}

impl<'a> TypeIndex<'a> {
    pub(super) fn build(set: &'a FileDescriptorSet) -> Self {
        let mut messages = HashMap::new();
        let mut enums = std::collections::HashSet::new();
        for (fi, file) in set.file.iter().enumerate() {
            let package = file.package();
            for e in &file.enum_type {
                enums.insert(qualify(package, e.name()));
            }
            for (mi, message) in file.message_type.iter().enumerate() {
                index_message(
                    message,
                    &qualify(package, message.name()),
                    (fi, vec![mi]),
                    &mut messages,
                    &mut enums,
                );
            }
        }
        Self { messages, enums }
    }

    /// Resolve one `type_name` reference. Compiled descriptor sets
    /// carry fully qualified names with a leading dot; anything else is
    /// refused by name rather than resolved by scope-walking guesswork.
    pub(super) fn message_by_type_name(
        &self,
        type_name: &str,
        path: &str,
    ) -> Result<&MsgEntry<'a>, MappingError> {
        let full = type_name.strip_prefix('.').ok_or_else(|| {
            refuse_at(
                path,
                format!(
                    "type name {type_name:?} is not fully qualified; compile the \
                     descriptor set with protoc, which emits leading-dot names"
                ),
            )
        })?;
        self.messages.get(full).ok_or_else(|| {
            refuse_at(
                path,
                format!("message type {full:?} is not in the descriptor set"),
            )
        })
    }

    /// Resolve a dotted path from `root` to its leaf field, requiring
    /// every intermediate step to be a singular message field.
    pub(super) fn navigate(
        &self,
        root: &MsgEntry<'a>,
        path: &str,
    ) -> Result<&'a FieldDescriptorProto, MappingError> {
        let mut current = root.desc;
        let segments: Vec<&str> = path.split('.').collect();
        for (position, segment) in segments.iter().enumerate() {
            let field = current
                .field
                .iter()
                .find(|f| f.name() == *segment)
                .ok_or_else(|| {
                    refuse_at(path, format!("{} has no field {segment:?}", current.name()))
                })?;
            if position + 1 == segments.len() {
                return Ok(field);
            }
            if field.label() == prost_types::field_descriptor_proto::Label::Repeated {
                return Err(refuse_at(
                    path,
                    format!("segment {segment:?} is repeated; the path must be singular"),
                ));
            }
            match field.r#type() {
                prost_types::field_descriptor_proto::Type::Message
                | prost_types::field_descriptor_proto::Type::Group => {
                    current = self.message_by_type_name(field.type_name(), path)?.desc;
                }
                _ => {
                    return Err(refuse_at(
                        path,
                        format!("segment {segment:?} is not a message field"),
                    ))
                }
            }
        }
        unreachable!("split('.') yields at least one segment")
    }

    pub(super) fn sample_types(&self) -> String {
        let mut names: Vec<&str> = self
            .messages
            .keys()
            .map(String::as_str)
            .filter(|n| !n.starts_with("google.protobuf."))
            .collect();
        names.sort_unstable();
        names.truncate(5);
        if names.is_empty() {
            "(none)".to_string()
        } else {
            names.join(", ")
        }
    }
}

fn index_message<'a>(
    message: &'a DescriptorProto,
    full: &str,
    key: MsgKey,
    messages: &mut HashMap<String, MsgEntry<'a>>,
    enums: &mut std::collections::HashSet<String>,
) {
    for e in &message.enum_type {
        enums.insert(format!("{full}.{}", e.name()));
    }
    for (ni, nested) in message.nested_type.iter().enumerate() {
        let mut nested_key = key.1.clone();
        nested_key.push(ni);
        index_message(
            nested,
            &format!("{full}.{}", nested.name()),
            (key.0, nested_key),
            messages,
            enums,
        );
    }
    messages.insert(
        full.to_string(),
        MsgEntry {
            desc: message,
            key,
            full: full.to_string(),
        },
    );
}

pub(super) fn qualify(package: &str, name: &str) -> String {
    if package.is_empty() {
        name.to_string()
    } else {
        format!("{package}.{name}")
    }
}
