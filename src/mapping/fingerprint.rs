//! The plan fingerprint and the reachable wire schema it covers.

use std::collections::{BTreeSet, HashMap};

use prost_reflect::{DescriptorPool, MessageDescriptor};
use prost_types::{DescriptorProto, FileDescriptorSet};

use super::type_index::{qualify, MsgEntry, TypeIndex};
use super::FINGERPRINT_VERSION;
use crate::pb;

/// Canonical fingerprint over the derived plan: a fixed-layout byte
/// encoding (never protobuf serialization, whose byte layout is not
/// canonical) hashed with SHA-256, lowercase hex. The descriptor
/// content address is deliberately NOT covered: two descriptor sets
/// may derive the same plan. The reachable wire schema is covered separately.
pub(super) fn fingerprint(
    plan: &pb::MappedPlan,
    set: &FileDescriptorSet,
    pool: &DescriptorPool,
) -> String {
    let mut hasher = turbo_sha256::Sha256::new();
    if plan.index_definition.is_some() {
        write_str(&mut hasher, "protomolt.search.index-definition.v1");
    }
    write_str(&mut hasher, FINGERPRINT_VERSION);
    write_str(&mut hasher, &plan.message_type);
    hash_wire_schema(&mut hasher, set, pool, &plan.message_type);
    write_str(&mut hasher, &plan.vector_path);
    write_str(&mut hasher, &plan.doc_id_path);
    write_str(&mut hasher, &plan.chunks_path);
    write_str(&mut hasher, &plan.chunk_id_path);
    hasher.update(&plan.dim.to_le_bytes());
    hasher.update(&(plan.fields.len() as u32).to_le_bytes());
    for field in &plan.fields {
        write_str(&mut hasher, &field.path);
        write_str(&mut hasher, &field.name);
        hasher.update(&field.kind.to_le_bytes());
        hasher.update(&[u8::from(field.repeated)]);
        hasher.update(&field.role.to_le_bytes());
        hasher.update(&field.vector_dims.to_le_bytes());
        write_str(&mut hasher, &field.analyzer);
        write_str(&mut hasher, &field.search_analyzer);
        hasher.update(&field.family.to_le_bytes());
    }
    hasher.finish().to_hex()
}

/// Hash the reachable descriptor graph independently of file order and source comments.
/// Projection policy is hashed separately above; this graph pins decoding meaning.
fn hash_wire_schema(
    hasher: &mut turbo_sha256::Sha256,
    set: &FileDescriptorSet,
    pool: &DescriptorPool,
    root: &str,
) {
    let index = TypeIndex::build(set);
    let enums = collect_enum_descriptors(set);
    let seen = reachable_type_names(&index, pool, root);
    hasher.update(&(seen.len() as u32).to_le_bytes());
    for name in seen {
        write_str(hasher, &name);
        if let Some(entry) = index.messages.get(&name) {
            let file = &set.file[entry.key.0];
            write_str(hasher, file.syntax.as_deref().unwrap_or("proto2"));
            hash_message(hasher, entry, pool, &name);
        } else if let Some((file, descriptor)) = enums.get(&name) {
            write_str(hasher, file.syntax.as_deref().unwrap_or("proto2"));
            // Declaration order selects the first alias and the proto2 default.
            hasher.update(&(descriptor.value.len() as u32).to_le_bytes());
            for value in &descriptor.value {
                hasher.update(&value.number().to_le_bytes());
                write_str(hasher, value.name());
            }
        }
    }
}

/// Every type name reachable from `root` through field and extension types.
fn reachable_type_names(
    index: &TypeIndex<'_>,
    pool: &DescriptorPool,
    root: &str,
) -> BTreeSet<String> {
    let mut pending = vec![root.to_owned()];
    let mut seen = std::collections::BTreeSet::new();
    while let Some(name) = pending.pop() {
        if !seen.insert(name.clone()) {
            continue;
        }
        if let Some(entry) = index.messages.get(&name) {
            let message = pool.get_message_by_name(&name).expect("validated message");
            let extensions: Vec<_> = message.extensions().collect();
            for field in entry.desc.field.iter().chain(
                extensions
                    .iter()
                    .map(|extension| extension.field_descriptor_proto()),
            ) {
                if !field.type_name().is_empty() {
                    pending.push(field.type_name().trim_start_matches('.').to_owned());
                }
            }
        }
    }

    seen
}

/// Hashes one message's fields and extensions in field-number order.
fn hash_message(
    hasher: &mut turbo_sha256::Sha256,
    entry: &MsgEntry<'_>,
    pool: &DescriptorPool,
    name: &str,
) {
    hasher.update(&[u8::from(
        entry.desc.options.as_ref().is_some_and(|o| o.map_entry()),
    )]);
    let message = pool.get_message_by_name(name).expect("validated message");
    let extensions: Vec<_> = message.extensions().collect();
    let mut fields: Vec<_> = entry
        .desc
        .field
        .iter()
        .map(|f| ("", f))
        .chain(
            extensions
                .iter()
                .map(|extension| (extension.full_name(), extension.field_descriptor_proto())),
        )
        .collect();
    fields.sort_by_key(|(_, f)| f.number());
    hasher.update(&(fields.len() as u32).to_le_bytes());
    for (extension_name, field) in fields {
        write_str(hasher, extension_name);
        write_str(hasher, field.name());
        hasher.update(&field.number().to_le_bytes());
        hasher.update(&field.r#type.unwrap_or_default().to_le_bytes());
        hasher.update(&field.label.unwrap_or_default().to_le_bytes());
        write_str(hasher, field.type_name());
        hasher.update(&[u8::from(field.default_value.is_some())]);
        write_str(hasher, field.default_value());
        hasher.update(&[u8::from(field.proto3_optional())]);
        // Membership, not the declaration's position or spelling, controls replacement.
        let mut members: Vec<_> = entry
            .desc
            .field
            .iter()
            .filter(|other| field.oneof_index.is_some() && other.oneof_index == field.oneof_index)
            .map(|other| other.number())
            .collect();
        members.sort_unstable();
        hasher.update(&(members.len() as u32).to_le_bytes());
        for number in members {
            hasher.update(&number.to_le_bytes());
        }
    }
}

pub(super) fn reachable_messages(pool: &DescriptorPool, root: &str) -> Vec<MessageDescriptor> {
    let mut seen = std::collections::BTreeMap::new();
    let mut pending = vec![pool.get_message_by_name(root).expect("validated root")];
    while let Some(message) = pending.pop() {
        if seen.contains_key(message.full_name()) {
            continue;
        }
        for kind in message
            .fields()
            .map(|f| f.kind())
            .chain(message.extensions().map(|f| f.kind()))
        {
            if let prost_reflect::Kind::Message(child) = kind {
                pending.push(child);
            }
        }
        seen.insert(message.full_name().to_string(), message);
    }
    seen.into_values().collect()
}

fn write_str(hasher: &mut turbo_sha256::Sha256, text: &str) {
    hasher.update(&(text.len() as u32).to_le_bytes());
    hasher.update(text.as_bytes());
}

/// Every enum in the set, by fully qualified name, as number -> value
/// name — the exact identity a facet stores.
fn collect_enum_descriptors(
    set: &FileDescriptorSet,
) -> HashMap<
    String,
    (
        &prost_types::FileDescriptorProto,
        &prost_types::EnumDescriptorProto,
    ),
> {
    fn walk<'a>(
        file: &'a prost_types::FileDescriptorProto,
        message: &'a DescriptorProto,
        full: &str,
        out: &mut HashMap<
            String,
            (
                &'a prost_types::FileDescriptorProto,
                &'a prost_types::EnumDescriptorProto,
            ),
        >,
    ) {
        for e in &message.enum_type {
            out.insert(format!("{full}.{}", e.name()), (file, e));
        }
        for nested in &message.nested_type {
            walk(file, nested, &format!("{full}.{}", nested.name()), out);
        }
    }
    let mut out = HashMap::new();
    for file in &set.file {
        for e in &file.enum_type {
            out.insert(qualify(file.package(), e.name()), (file, e));
        }
        for message in &file.message_type {
            walk(
                file,
                message,
                &qualify(file.package(), message.name()),
                &mut out,
            );
        }
    }
    out
}

pub(super) fn collect_enum_values(
    set: &FileDescriptorSet,
) -> HashMap<String, HashMap<i64, String>> {
    collect_enum_descriptors(set)
        .into_iter()
        .map(|(name, (_, e))| {
            let mut values = HashMap::new();
            for value in &e.value {
                values
                    .entry(i64::from(value.number()))
                    .or_insert_with(|| value.name().to_string());
            }
            (name, values)
        })
        .collect()
}
