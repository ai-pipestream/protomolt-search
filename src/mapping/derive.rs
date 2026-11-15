//! Plan derivation: descriptor set and message type in, deterministic plan
//! and schema report out.

use std::collections::{HashMap, HashSet};

use prost::Message as _;
use prost_reflect::DescriptorPool;
use prost_types::FileDescriptorSet;
use tonic::Status;

use super::fingerprint::{fingerprint, reachable_messages};
use super::hint_bytes::{check_extension_declarations, extract_hints, HintMap};
use super::hint_resolve::projection_scalar_type;
use super::resolve::{resolve_chunk_id, resolve_chunks, resolve_doc_id, resolve_vector};
use super::type_index::{MsgEntry, TypeIndex};
use super::walk::PlanWalk;
use super::{index_definition, refuse, refuse_at, schema_report};
use crate::pb::{self, hints};
use crate::sha256;

/// Derive the plan for `message_type` inside `descriptor_set`. Every
/// ambiguity and every unsupported hint is an error here, before any
/// index exists.
pub fn derive_plan(descriptor_set: &[u8], message_type: &str) -> Result<pb::MappedPlan, Status> {
    derive_plan_with_definition(descriptor_set, message_type, None)
}

/// Derive a plan using an explicit policy, or legacy descriptor inference.
pub fn derive_plan_with_definition(
    descriptor_set: &[u8],
    message_type: &str,
    definition: Option<&pb::IndexDefinition>,
) -> Result<pb::MappedPlan, Status> {
    if message_type.is_empty() {
        return Err(refuse("message_type is required"));
    }
    if descriptor_set.is_empty() {
        return Err(refuse(
            "descriptor_set is required: a serialized google.protobuf.FileDescriptorSet \
             with every import included",
        ));
    }
    let set = FileDescriptorSet::decode(descriptor_set).map_err(|e| {
        refuse(format!(
            "descriptor_set does not decode as a FileDescriptorSet \
             (compile with --include_imports): {e}"
        ))
    })?;
    validate_descriptor_syntax(&set).map_err(refuse)?;
    let index = TypeIndex::build(&set);
    if definition.is_none() {
        check_extension_declarations(&set)?;
    }
    let pool = DescriptorPool::decode(descriptor_set)
        .map_err(|e| refuse(format!("invalid descriptor set: {e}")))?;
    let hint_map = if definition.is_none() {
        extract_hints(descriptor_set)?
    } else {
        HashMap::new()
    };
    let root = index.messages.get(message_type).ok_or_else(|| {
        refuse(format!(
            "message type {message_type:?} is not in the descriptor set; \
             types present include e.g. {}",
            index.sample_types()
        ))
    })?;

    refuse_message_set(&pool, message_type)?;

    let (mut fields, canonical_definition) =
        project_fields(definition, root, &index, &hint_map, message_type)?;
    let roles = resolve_roles(&mut fields, root, &index)?;
    let dim = fields
        .iter()
        .find(|f| f.path == roles.vector_path)
        .map_or(0, |f| f.vector_dims);

    let mut plan = pb::MappedPlan {
        message_type: message_type.to_string(),
        fields,
        fingerprint: String::new(),
        vector_path: roles.vector_path,
        doc_id_path: roles.doc_id_path,
        dim,
        chunks_path: roles.chunks_path.unwrap_or_default(),
        chunk_id_path: roles.chunk_id_path.unwrap_or_default(),
        descriptor_sha256: sha256::hex_digest(descriptor_set),
        schema_report: None,
        vector_binding: None,
        index_definition: canonical_definition,
    };
    plan.fingerprint = fingerprint(&plan, &set, &pool);
    plan.vector_binding = Some(crate::mapped_vector::from_plan(&plan)?);
    let skipped_fields = skipped_field_names(&index, &hint_map);
    plan.schema_report = Some(schema_report::build(&plan, &pool, &set, &skipped_fields)?);
    Ok(plan)
}

/// The planned fields, from the explicit definition when there is one and
/// from the descriptor walk otherwise, with the canonical definition.
fn project_fields(
    definition: Option<&pb::IndexDefinition>,
    root: &MsgEntry<'_>,
    index: &TypeIndex<'_>,
    hint_map: &HintMap,
    message_type: &str,
) -> Result<(Vec<pb::MappedField>, Option<pb::IndexDefinition>), Status> {
    let (fields, canonical_definition) = match definition {
        Some(definition) => {
            let (projected, canonical) = index_definition::derive(definition, root, index)?;
            (projected, Some(canonical))
        }
        None => {
            let mut walk = PlanWalk::new(index, hint_map);
            walk.message(root, "", "", 0)?;
            (walk.into_fields(), None)
        }
    };
    if fields.is_empty() {
        return Err(refuse(format!(
            "message type {message_type} has no indexable fields"
        )));
    }
    Ok((fields, canonical_definition))
}

/// Mapped ingest does not decode MessageSet wire format, so a reachable
/// message that uses it refuses the plan.
fn refuse_message_set(pool: &DescriptorPool, message_type: &str) -> Result<(), Status> {
    for message in reachable_messages(pool, message_type) {
        if message
            .descriptor_proto()
            .options
            .as_ref()
            .is_some_and(|options| options.message_set_wire_format())
        {
            return Err(refuse(format!(
                "{} uses MessageSet wire format, which mapped ingest does not decode",
                message.full_name()
            )));
        }
    }

    Ok(())
}

/// The structural fields of a plan: its chunk scope, vector, and identities.
struct PlanRoles {
    chunks_path: Option<String>,
    vector_path: String,
    doc_id_path: String,
    chunk_id_path: Option<String>,
}

/// Resolves the structural roles over the planned fields, refusing column
/// name collisions and a document id that is neither integer nor string.
fn resolve_roles(
    fields: &mut [pb::MappedField],
    root: &MsgEntry<'_>,
    index: &TypeIndex<'_>,
) -> Result<PlanRoles, Status> {
    let chunks_path = resolve_chunks(fields)?;
    let vector_path = resolve_vector(fields, chunks_path.as_deref())?;
    let mut column_paths = HashMap::new();
    for field in fields.iter() {
        if field.family == pb::ColumnFamily::None as i32 {
            continue;
        }
        if field.family == pb::ColumnFamily::Vector as i32
            && matches!(field.name.as_str(), "body" | "parent_id" | "group_id")
        {
            return Err(refuse_at(&field.path, "the vector column name conflicts with a built-in text or lineage field; choose a distinct indexing name hint"));
        }
        if let Some(previous) = column_paths.insert(&field.name, &field.path) {
            return Err(refuse(format!(
                "fields {previous:?} and {:?} both land column {:?}; use distinct indexing name hints",
                field.path, field.name
            )));
        }
    }

    let doc_id_path = resolve_doc_id(fields, chunks_path.as_deref())?;
    let chunk_id_path = resolve_chunk_id(fields, chunks_path.as_deref())?;

    // The id's reduction rule depends on the DESCRIPTOR type at the
    // path, not the hinted kind: a KEYWORD hint on an integer field
    // must not switch the id to string hashing.
    let id_leaf = index.navigate(root, &doc_id_path)?;

    let id_ok = matches!(
        projection_scalar_type(id_leaf),
        prost_types::field_descriptor_proto::Type::String
            | prost_types::field_descriptor_proto::Type::Int32
            | prost_types::field_descriptor_proto::Type::Int64
            | prost_types::field_descriptor_proto::Type::Uint32
            | prost_types::field_descriptor_proto::Type::Uint64
            | prost_types::field_descriptor_proto::Type::Sint32
            | prost_types::field_descriptor_proto::Type::Sint64
            | prost_types::field_descriptor_proto::Type::Fixed32
            | prost_types::field_descriptor_proto::Type::Fixed64
            | prost_types::field_descriptor_proto::Type::Sfixed32
            | prost_types::field_descriptor_proto::Type::Sfixed64
    );
    if !id_ok {
        return Err(refuse_at(
            &doc_id_path,
            format!(
                "document id must be an integer or string field, not {:?}",
                id_leaf.r#type()
            ),
        ));
    }

    Ok(PlanRoles {
        chunks_path,
        vector_path,
        doc_id_path,
        chunk_id_path,
    })
}

/// Qualified names of the fields an explicit SKIP hint left out of the plan.
fn skipped_field_names(index: &TypeIndex<'_>, hint_map: &HintMap) -> HashSet<String> {
    index
        .messages
        .values()
        .flat_map(|entry| {
            entry
                .desc
                .field
                .iter()
                .enumerate()
                .filter_map(|(i, field)| {
                    hint_map
                        .get(&(entry.key.clone(), i))
                        .filter(|hint| hint.r#type == hints::IndexFieldType::Skip as i32)
                        .map(|_| format!("{}.{}", entry.full, field.name()))
                })
        })
        .collect()
}

/// Describe source preservation without choosing an indexing projection.
pub fn describe_schema(
    descriptor_set: &[u8],
    message_type: &str,
) -> Result<pb::DescribeSchemaResponse, Status> {
    if descriptor_set.is_empty() || message_type.is_empty() {
        return Err(Status::invalid_argument(
            "schema: descriptor_set and message_type are required",
        ));
    }
    if message_type.starts_with('.') {
        return Err(Status::invalid_argument(
            "schema: message_type must not have a leading dot",
        ));
    }
    let set = FileDescriptorSet::decode(descriptor_set).map_err(|error| {
        Status::invalid_argument(format!("schema: invalid descriptor set: {error}"))
    })?;
    validate_descriptor_syntax(&set)
        .map_err(|error| Status::invalid_argument(format!("schema: {error}")))?;
    let pool = DescriptorPool::decode(descriptor_set).map_err(|error| {
        Status::invalid_argument(format!("schema: invalid descriptors: {error}"))
    })?;
    if pool.get_message_by_name(message_type).is_none() {
        return Err(Status::invalid_argument(format!(
            "schema: message type {message_type:?} is absent from the descriptor set"
        )));
    }
    Ok(pb::DescribeSchemaResponse {
        report: Some(schema_report::describe(&pool, message_type)?),
        descriptor_sha256: sha256::hex_digest(descriptor_set),
    })
}

fn validate_descriptor_syntax(set: &FileDescriptorSet) -> Result<(), String> {
    for file in &set.file {
        match file.syntax.as_deref() {
            None | Some("proto2" | "proto3") => {}
            Some(syntax) => {
                return Err(format!(
                    "unsupported protobuf syntax {syntax:?} in file {:?}; expected proto2 or proto3",
                    file.name()
                ));
            }
        }
    }
    Ok(())
}
