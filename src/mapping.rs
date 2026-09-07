//! Descriptor-derived index plans (`docs/descriptor-mappings.md`),
//! increment 1: dry-run derivation.
//!
//! The input is what a producer already has: a serialized
//! `google.protobuf.FileDescriptorSet` and a fully qualified message
//! type name. The output is a deterministic plan — which fields exist,
//! at what dotted paths, with what resolved kinds, landing on which of
//! this engine's column families — plus a lowercase-hex SHA-256
//! fingerprint that identifies the plan the way an analysis fingerprint
//! identifies an analyzer. Same input, same plan, same fingerprint, on
//! every node and every run.
//!
//! Fields may carry explicit hints as descriptor options, using the
//! `(ai.protomolt.proto.index.hints.v1.index)` extension vendored from
//! protomolt: a proto annotated for protomolt's indexers is understood
//! here without modification. Unhinted integer kinds retain descriptor
//! signedness. Other kinds follow protomolt's rules, with the structural
//! deviation the frozen turbovec-grpc reference also made:
//! an unannotated singular message field is expanded into dotted paths
//! rather than kept as a single OBJECT entry, because this is a flat
//! engine with no native object type. An explicit OBJECT or NESTED hint
//! still keeps the single entry.
//!
//! Everything ambiguous is an error, not a guess. A message with two
//! plausible vector fields, or no resolvable document id, fails
//! derivation with the hint the caller should add. This module never
//! picks one of several candidates silently — that is the failure mode
//! the whole feature exists to refuse.
//!
//! One mechanical note: hint extensions live on
//! `google.protobuf.FieldOptions`, and prost drops extension fields it
//! was not compiled against. The extraction therefore walks the raw
//! descriptor-set bytes once — plain varint/length-delimited wire
//! format, hand-rolled like every parser in this codebase — collecting
//! the extension payloads keyed by structural position (file index,
//! nested-message index path, field index), which is exactly the order
//! the prost-decoded structures iterate in, because both read the same
//! bytes and protobuf preserves repeated-field order.

use std::collections::HashMap;

use prost::Message as _;
use prost_reflect::{DescriptorPool, DynamicMessage, MessageDescriptor, ReflectMessage, Value};
use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorSet};
use tonic::Status;

use crate::pb::{self, hints};
use crate::sha256;

#[path = "schema_report.rs"]
mod schema_report;

#[path = "index_definition.rs"]
mod index_definition;

/// Full name of the field-option extension carrying indexing hints,
/// owned by ProtoMolt and vendored under `proto/ai/protomolt/`.
const HINT_EXTENSION_NAME: &str = "ai.protomolt.proto.index.hints.v1.index";

/// The extension's registered field number on
/// `google.protobuf.FieldOptions`.
const HINT_EXTENSION_NUMBER: u64 = 59_100_471;

/// Nested messages deeper than this stop expanding and are recorded as
/// a single OBJECT entry, which also bounds recursive message types.
const MAX_DEPTH: usize = 8;

/// Version tag mixed into the canonical fingerprint bytes. Bump when a
/// semantic or encoding change is not already distinguished by the canonical
/// plan. Kind and family changes participate directly in the hash. A changed
/// fingerprint requires a new index binding, never a silent rebind.
const FINGERPRINT_VERSION: &str = "pipestream-search.plan.v3";

/// One refusal, uniformly shaped: every message begins `plan: `.
fn refuse(msg: impl Into<String>) -> Status {
    Status::invalid_argument(format!("plan: {}", msg.into()))
}

fn refuse_at(path: &str, msg: impl AsRef<str>) -> Status {
    refuse(format!("{}: {}", path, msg.as_ref()))
}

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
    for message in reachable_messages(&pool, message_type) {
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

    let mut fields = Vec::new();
    let mut visiting = Vec::new();
    let canonical_definition = if let Some(definition) = definition {
        let (projected, canonical) = index_definition::derive(definition, root, &index)?;
        fields = projected;
        Some(canonical)
    } else {
        walk(
            root,
            "",
            "",
            0,
            &index,
            &hint_map,
            &mut fields,
            &mut visiting,
        )?;
        None
    };
    if fields.is_empty() {
        return Err(refuse(format!(
            "message type {message_type} has no indexable fields"
        )));
    }

    let chunks_path = resolve_chunks(&fields)?;
    let vector_path = resolve_vector(&mut fields, chunks_path.as_deref())?;
    let mut column_paths = HashMap::new();
    for field in &fields {
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

    let doc_id_path = resolve_doc_id(&mut fields, chunks_path.as_deref())?;
    let chunk_id_path = resolve_chunk_id(&fields, chunks_path.as_deref())?;

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

    let dim = fields
        .iter()
        .find(|f| f.path == vector_path)
        .map_or(0, |f| f.vector_dims);

    let mut plan = pb::MappedPlan {
        message_type: message_type.to_string(),
        fields,
        fingerprint: String::new(),
        vector_path,
        doc_id_path,
        dim,
        chunks_path: chunks_path.unwrap_or_default(),
        chunk_id_path: chunk_id_path.unwrap_or_default(),
        descriptor_sha256: sha256::hex_digest(descriptor_set),
        schema_report: None,
        vector_binding: None,
        index_definition: canonical_definition,
    };
    plan.fingerprint = fingerprint(&plan, &set, &pool);
    plan.vector_binding = Some(crate::mapped_vector::from_plan(&plan)?);
    let skipped_fields = index
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
        .collect();
    plan.schema_report = Some(schema_report::build(&plan, &pool, &set, &skipped_fields)?);
    Ok(plan)
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

// ---------------------------------------------------------------------
// Type index over the decoded set
// ---------------------------------------------------------------------

/// Structural address of one message: file index plus the nested-type
/// index path, the key the raw hint pass and the decoded walk share.
type MsgKey = (usize, Vec<usize>);

struct MsgEntry<'a> {
    desc: &'a DescriptorProto,
    key: MsgKey,
    /// Fully qualified name, no leading dot.
    full: String,
}

struct TypeIndex<'a> {
    /// Fully qualified message name (no leading dot) to its descriptor
    /// and structural address.
    messages: HashMap<String, MsgEntry<'a>>,
    /// Fully qualified enum names.
    enums: std::collections::HashSet<String>,
}

impl<'a> TypeIndex<'a> {
    fn build(set: &'a FileDescriptorSet) -> Self {
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
    fn message_by_type_name(&self, type_name: &str, path: &str) -> Result<&MsgEntry<'a>, Status> {
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
    fn navigate(
        &self,
        root: &MsgEntry<'a>,
        path: &str,
    ) -> Result<&'a FieldDescriptorProto, Status> {
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

    fn sample_types(&self) -> String {
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

fn qualify(package: &str, name: &str) -> String {
    if package.is_empty() {
        name.to_string()
    } else {
        format!("{package}.{name}")
    }
}

// ---------------------------------------------------------------------
// Raw-bytes hint extraction
// ---------------------------------------------------------------------

/// The raw pass below reads extension number 59100471 off FieldOptions
/// unconditionally, so a descriptor set that declares a DIFFERENT
/// extension under that number would decode garbage as a hint. Refuse
/// such a set by name instead: the number is registered to the
/// ProtoMolt hint vocabulary, and a modified indexing_hints.proto is
/// exactly the drift the byte-identity check exists to catch.
fn check_extension_declarations(set: &FileDescriptorSet) -> Result<(), Status> {
    fn check_list(extensions: &[FieldDescriptorProto]) -> Result<(), Status> {
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
    fn check_message(message: &DescriptorProto) -> Result<(), Status> {
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
fn varint(b: &[u8], i: &mut usize) -> Result<u64, Status> {
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
fn skip_field(b: &[u8], i: &mut usize, wire: u64) -> Result<(), Status> {
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
    mut visit: impl FnMut(&[u8]) -> Result<(), Status>,
) -> Result<(), Status> {
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
fn extract_hints(bytes: &[u8]) -> Result<HashMap<(MsgKey, usize), hints::FieldIndexHint>, Status> {
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
) -> Result<(), Status> {
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

// ---------------------------------------------------------------------
// Hint resolution and inference
// ---------------------------------------------------------------------

/// A hint after merging: the resolved kind, whether it was explicit,
/// and the attributes this engine records.
struct ResolvedHint {
    kind: pb::MappedKind,
    explicit_kind: bool,
    name_override: String,
    role: pb::MappedRole,
    vector_dims: u32,
    analyzer: String,
    search_analyzer: String,
    skip: bool,
}

/// What one field descriptor actually is, resolved against the index.
enum Shape<'a> {
    Scalar(prost_types::field_descriptor_proto::Type),
    Enum,
    Message {
        full: String,
        entry: &'a MsgEntry<'a>,
        map: bool,
    },
}

fn shape<'a>(
    field: &FieldDescriptorProto,
    index: &'a TypeIndex<'a>,
    path: &str,
) -> Result<Shape<'a>, Status> {
    use prost_types::field_descriptor_proto::Type;
    match field.r#type() {
        Type::Message | Type::Group => {
            let entry = index.message_by_type_name(field.type_name(), path)?;
            let full = field.type_name().trim_start_matches('.').to_string();
            let map = entry.desc.options.as_ref().is_some_and(|o| o.map_entry());
            Ok(Shape::Message { full, entry, map })
        }
        Type::Enum => {
            let full = field.type_name().trim_start_matches('.');
            if full.is_empty() || !index.enums.contains(full) {
                return Err(refuse_at(
                    path,
                    format!(
                        "enum type {:?} is not in the descriptor set",
                        field.type_name()
                    ),
                ));
            }
            Ok(Shape::Enum)
        }
        other => Ok(Shape::Scalar(other)),
    }
}

fn is_repeated(field: &FieldDescriptorProto) -> bool {
    field.label() == prost_types::field_descriptor_proto::Label::Repeated
}

/// The scalar represented by a standard wrapper. Recognition is followed by
/// descriptor validation whenever a wrapper contributes an indexed value.
fn wrapper_kind(full: &str) -> Option<prost_types::field_descriptor_proto::Type> {
    use prost_types::field_descriptor_proto::Type;
    Some(match full.trim_start_matches('.') {
        "google.protobuf.DoubleValue" => Type::Double,
        "google.protobuf.FloatValue" => Type::Float,
        "google.protobuf.Int64Value" => Type::Int64,
        "google.protobuf.UInt64Value" => Type::Uint64,
        "google.protobuf.Int32Value" => Type::Int32,
        "google.protobuf.UInt32Value" => Type::Uint32,
        "google.protobuf.BoolValue" => Type::Bool,
        "google.protobuf.StringValue" => Type::String,
        "google.protobuf.BytesValue" => Type::Bytes,
        _ => return None,
    })
}

fn projection_scalar_type(
    field: &FieldDescriptorProto,
) -> prost_types::field_descriptor_proto::Type {
    if field.r#type() == prost_types::field_descriptor_proto::Type::Message {
        wrapper_kind(field.type_name()).unwrap_or(field.r#type())
    } else {
        field.r#type()
    }
}

/// Infer a hint from the descriptor alone, with protomolt's rules:
/// strings whose names look like identifiers become KEYWORD, Timestamp
/// becomes DATE, Struct/Value stay OBJECT, repeated and map messages
/// stay NESTED.
fn inferred_kind(field: &FieldDescriptorProto, shape: &Shape<'_>) -> pb::MappedKind {
    use prost_types::field_descriptor_proto::Type;
    if let Shape::Message {
        full, map: false, ..
    } = shape
    {
        if !is_repeated(field) {
            if let Some(kind) = wrapper_kind(full) {
                return inferred_kind(field, &Shape::Scalar(kind));
            }
        }
    }
    match shape {
        Shape::Scalar(Type::String) => {
            if looks_like_keyword(field.name()) {
                pb::MappedKind::Keyword
            } else {
                pb::MappedKind::Text
            }
        }
        Shape::Scalar(Type::Bool) => pb::MappedKind::Boolean,
        Shape::Scalar(Type::Int32 | Type::Sint32 | Type::Sfixed32) => pb::MappedKind::Int32,
        Shape::Scalar(Type::Int64 | Type::Sint64 | Type::Sfixed64) => pb::MappedKind::Int64,
        Shape::Scalar(Type::Uint32 | Type::Fixed32) => pb::MappedKind::Uint32,
        Shape::Scalar(Type::Uint64 | Type::Fixed64) => pb::MappedKind::Uint64,
        Shape::Scalar(Type::Float) => pb::MappedKind::Float,
        Shape::Scalar(Type::Double) => pb::MappedKind::Double,
        Shape::Scalar(Type::Bytes) => pb::MappedKind::Binary,
        Shape::Scalar(_) => pb::MappedKind::Object,
        Shape::Enum => pb::MappedKind::Keyword,
        Shape::Message { full, map, .. } => match full.as_str() {
            "google.protobuf.Timestamp" => pb::MappedKind::Date,
            "google.protobuf.Struct" | "google.protobuf.Value" => pb::MappedKind::Object,
            _ if is_repeated(field) || *map => pb::MappedKind::Nested,
            _ => pb::MappedKind::Object,
        },
    }
}

/// protomolt's keyword-name heuristic, verbatim: identifier-shaped
/// names index as exact values rather than analyzed text.
fn looks_like_keyword(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    name == "id"
        || name.ends_with("_id")
        || (name.ends_with("id") && name.len() <= 4)
        || name.ends_with("_key")
        || name.ends_with("_code")
        || name == "uri"
        || name.ends_with("_uri")
        || name == "status"
        || name == "type"
        || name.ends_with("_type")
}

fn plain_hint(kind: pb::MappedKind) -> ResolvedHint {
    ResolvedHint {
        kind,
        explicit_kind: false,
        name_override: String::new(),
        role: pb::MappedRole::None,
        vector_dims: 0,
        analyzer: String::new(),
        search_analyzer: String::new(),
        skip: false,
    }
}

/// Resolve one field's hint: the explicit `(index)` option when
/// present, inference otherwise. An explicit hint with an unset type
/// still infers the kind while its other attributes win, matching
/// protomolt.
fn resolve_hint(
    field: &FieldDescriptorProto,
    shape: &Shape<'_>,
    explicit: Option<&hints::FieldIndexHint>,
    path: &str,
) -> Result<ResolvedHint, Status> {
    let Some(hint) = explicit else {
        return Ok(plain_hint(inferred_kind(field, shape)));
    };
    let (mut kind, explicit_kind, skip) = match hints::IndexFieldType::try_from(hint.r#type) {
        Ok(hints::IndexFieldType::Unspecified) => (inferred_kind(field, shape), false, false),
        Ok(hints::IndexFieldType::Skip) => (pb::MappedKind::Unspecified, true, true),
        Ok(explicit) => (convert_kind(explicit, path)?, true, false),
        Err(_) => {
            return Err(refuse_at(
                path,
                format!("hint declares unknown index type {}", hint.r#type),
            ))
        }
    };
    let role = match hints::BlockRole::try_from(hint.block_role) {
        Ok(hints::BlockRole::Unspecified) => pb::MappedRole::None,
        Ok(hints::BlockRole::Chunks) => pb::MappedRole::Chunks,
        Ok(hints::BlockRole::DocId) => pb::MappedRole::DocId,
        Ok(hints::BlockRole::ChunkId) => pb::MappedRole::ChunkId,
        Err(_) => {
            return Err(refuse_at(
                path,
                format!("hint declares unknown block role {}", hint.block_role),
            ))
        }
    };
    // Identity roles need an exact value. An unspecified string kind adopts
    // keyword semantics; an explicitly requested TEXT kind is refused below.
    if !explicit_kind
        && matches!(role, pb::MappedRole::DocId | pb::MappedRole::ChunkId)
        && kind == pb::MappedKind::Text
    {
        kind = pb::MappedKind::Keyword;
    }
    if hint.chunking_policy.is_some() {
        return Err(refuse_at(
            path,
            "chunking_policy hints are not supported by this engine; \
             chunk and embed before ingest",
        ));
    }
    Ok(ResolvedHint {
        kind,
        explicit_kind,
        name_override: hint.name.clone(),
        role,
        vector_dims: u32::try_from(hint.vector_dims.max(0)).expect("clamped to non-negative"),
        analyzer: hint.analyzer.clone().unwrap_or_default(),
        search_analyzer: hint.search_analyzer.clone().unwrap_or_default(),
        skip,
    })
}

fn convert_kind(hint: hints::IndexFieldType, path: &str) -> Result<pb::MappedKind, Status> {
    use hints::IndexFieldType as H;
    Ok(match hint {
        H::Text => pb::MappedKind::Text,
        H::Keyword => pb::MappedKind::Keyword,
        H::Int32 => pb::MappedKind::Int32,
        H::Int64 => pb::MappedKind::Int64,
        H::Float => pb::MappedKind::Float,
        H::Double => pb::MappedKind::Double,
        H::Boolean => pb::MappedKind::Boolean,
        H::Date => pb::MappedKind::Date,
        H::Binary => pb::MappedKind::Binary,
        H::Vector => pb::MappedKind::Vector,
        H::Object => pb::MappedKind::Object,
        H::Nested => pb::MappedKind::Nested,
        H::IntRange | H::LongRange | H::FloatRange | H::DoubleRange | H::DateRange => {
            return Err(refuse_at(
                path,
                "range hints are not supported by this engine yet",
            ))
        }
        H::TreePath => {
            return Err(refuse_at(
                path,
                "TREE_PATH hints are not supported by this engine yet",
            ))
        }
        H::Unspecified | H::Skip => unreachable!("handled by resolve_hint"),
    })
}

/// Hints that cannot possibly apply to the field they sit on fail
/// here, with the path, before any plan is returned.
fn validate_hint(
    field: &FieldDescriptorProto,
    shape: &Shape<'_>,
    hint: &ResolvedHint,
    path: &str,
) -> Result<(), Status> {
    use prost_types::field_descriptor_proto::Type;
    if hint.kind == pb::MappedKind::Date
        && !matches!(shape, Shape::Message { full, map: false, .. } if full == "google.protobuf.Timestamp")
    {
        return Err(refuse_at(
            path,
            "a DATE hint requires a google.protobuf.Timestamp field",
        ));
    }
    if hint.kind == pb::MappedKind::Date && !is_repeated(field) {
        if let Shape::Message { entry, .. } = shape {
            validate_timestamp_descriptor(entry.desc, path)?;
        }
    }
    if family(hint.kind, is_repeated(field)) != pb::ColumnFamily::None {
        if let Shape::Message { full, entry, .. } = shape {
            if let Some(kind) = wrapper_kind(full) {
                if field.r#type() != Type::Message {
                    return Err(refuse_at(path, "a scalar wrapper must be a message field"));
                }
                validate_wrapper_descriptor(entry.desc, kind, path)?;
            }
        }
    }
    if hint.kind == pb::MappedKind::Vector {
        let element_ok = matches!(shape, Shape::Scalar(Type::Float | Type::Double));
        if !is_repeated(field) || !element_ok {
            return Err(refuse_at(
                path,
                "a VECTOR hint requires a repeated float or repeated double field",
            ));
        }
    }
    if hint.role == pb::MappedRole::Chunks {
        let non_map_message = matches!(shape, Shape::Message { map: false, .. });
        if !is_repeated(field) || !non_map_message {
            return Err(refuse_at(
                path,
                "BLOCK_ROLE_CHUNKS requires a repeated message field",
            ));
        }
    }
    if hint.role == pb::MappedRole::DocId || hint.role == pb::MappedRole::ChunkId {
        let role_name = if hint.role == pb::MappedRole::DocId {
            "BLOCK_ROLE_DOC_ID"
        } else {
            "BLOCK_ROLE_CHUNK_ID"
        };
        let map = matches!(shape, Shape::Message { map: true, .. });
        if is_repeated(field) || map {
            return Err(refuse_at(
                path,
                format!("{role_name} requires a singular field"),
            ));
        }
        let id_type = match shape {
            Shape::Scalar(kind) => Some(*kind),
            Shape::Message {
                full, map: false, ..
            } => wrapper_kind(full),
            _ => None,
        };
        let id_ok = matches!(
            id_type,
            Some(
                Type::String
                    | Type::Int32
                    | Type::Int64
                    | Type::Uint32
                    | Type::Uint64
                    | Type::Sint32
                    | Type::Sint64
                    | Type::Fixed32
                    | Type::Fixed64
                    | Type::Sfixed32
                    | Type::Sfixed64
            )
        );
        if !id_ok {
            return Err(refuse_at(
                path,
                format!("{role_name} requires an integer or string field"),
            ));
        }
        if !matches!(
            family(hint.kind, false),
            pb::ColumnFamily::Facet | pb::ColumnFamily::I64 | pb::ColumnFamily::U64
        ) {
            return Err(refuse_at(
                path,
                format!("{role_name} requires a keyword or integer value projection"),
            ));
        }
    }
    Ok(())
}

fn validate_wrapper_descriptor(
    message: &DescriptorProto,
    kind: prost_types::field_descriptor_proto::Type,
    path: &str,
) -> Result<(), Status> {
    use prost_types::field_descriptor_proto::{Label, Type};
    let valid = message.field.iter().any(|field| {
        let default_ok = field
            .default_value
            .as_deref()
            .is_none_or(|value| match kind {
                Type::String | Type::Bytes => value.is_empty(),
                Type::Bool => value == "false",
                Type::Float | Type::Double => value.parse::<f64>().is_ok_and(|v| v.to_bits() == 0),
                _ => value.parse::<i64>() == Ok(0),
            });
        field.name() == "value"
            && field.number() == 1
            && field.r#type() == kind
            && field.label() == Label::Optional
            && field.oneof_index.is_none()
            && default_ok
    });
    if !valid {
        return Err(refuse_at(path, format!(
            "scalar wrapper requires singular value = 1 of type {} with its scalar default and no oneof",
            kind.as_str_name()
        )));
    }
    Ok(())
}

/// A well-known name alone is not evidence of its wire or default semantics.
/// Compatible proto2 descriptors are accepted, but their defaults must remain
/// zero and the components must not select a oneof or repeat. Extra source
/// fields remain preserved; only the two standard components project.
fn validate_timestamp_descriptor(message: &DescriptorProto, path: &str) -> Result<(), Status> {
    use prost_types::field_descriptor_proto::{Label, Type};
    for (name, number, kind) in [("seconds", 1, Type::Int64), ("nanos", 2, Type::Int32)] {
        let valid = message.field.iter().any(|field| {
            field.name() == name
                && field.number() == number
                && field.r#type() == kind
                && field.label() == Label::Optional
                && field.oneof_index.is_none()
                && field
                    .default_value
                    .as_deref()
                    .is_none_or(|value| value.parse::<i64>() == Ok(0))
        });
        if !valid {
            return Err(refuse_at(
                path,
                format!(
                    "google.protobuf.Timestamp requires singular {name} = {number} of type {} \
                 with default zero and no oneof",
                    kind.as_str_name()
                ),
            ));
        }
    }
    Ok(())
}

// ---------------------------------------------------------------------
// The walk
// ---------------------------------------------------------------------

/// Well-known message types that plan as leaves rather than expanding.
fn well_known_leaf(full: &str) -> bool {
    wrapper_kind(full).is_some()
        || matches!(
            full,
            "google.protobuf.Timestamp" | "google.protobuf.Struct" | "google.protobuf.Value"
        )
}

/// The column plane one planned field lands on. `NONE` is a visible
/// outcome of the dry run, never a silent drop at ingest: the facet,
/// i64, u64, and f64 planes hold one value per document slot, so repeated
/// scalars do not land, and this engine does not guess a collapse rule.
fn family(kind: pb::MappedKind, repeated: bool) -> pb::ColumnFamily {
    use pb::MappedKind as K;
    match kind {
        K::Vector => pb::ColumnFamily::Vector,
        K::Object | K::Nested | K::Binary | K::Unspecified => pb::ColumnFamily::None,
        _ if repeated => pb::ColumnFamily::None,
        K::Text => pb::ColumnFamily::TextField,
        K::Keyword | K::Boolean => pb::ColumnFamily::Facet,
        K::Int32 | K::Int64 | K::Date => pb::ColumnFamily::I64,
        K::Uint32 | K::Uint64 => pb::ColumnFamily::U64,
        K::Float | K::Double => pb::ColumnFamily::F64,
    }
}

fn planned(
    path: &str,
    name: &str,
    field: &FieldDescriptorProto,
    shape: &Shape<'_>,
    hint: &ResolvedHint,
) -> pb::MappedField {
    let map = matches!(shape, Shape::Message { map: true, .. });
    let repeated = is_repeated(field) || map;
    pb::MappedField {
        path: path.to_string(),
        name: name.to_string(),
        kind: hint.kind as i32,
        repeated,
        role: hint.role as i32,
        vector_dims: hint.vector_dims,
        analyzer: hint.analyzer.clone(),
        search_analyzer: hint.search_analyzer.clone(),
        family: family(hint.kind, repeated) as i32,
    }
}

/// Walk one message's fields, appending planned fields in declaration
/// order. `visiting` holds the message names on the current branch, so
/// a recursive type stops expanding instead of looping.
#[allow(clippy::too_many_arguments)]
fn walk(
    entry: &MsgEntry<'_>,
    path_prefix: &str,
    name_prefix: &str,
    depth: usize,
    index: &TypeIndex<'_>,
    hint_map: &HashMap<(MsgKey, usize), hints::FieldIndexHint>,
    out: &mut Vec<pb::MappedField>,
    visiting: &mut Vec<String>,
) -> Result<(), Status> {
    visiting.push(entry.full.clone());
    for (field_idx, field) in entry.desc.field.iter().enumerate() {
        let path = join_path(path_prefix, field.name());
        let field_shape = shape(field, index, &path)?;
        let explicit = hint_map.get(&(entry.key.clone(), field_idx));
        let hint = resolve_hint(field, &field_shape, explicit, &path)?;
        if hint.skip {
            continue;
        }
        let qualified = if name_prefix.is_empty() {
            field.name().to_string()
        } else {
            format!("{name_prefix}_{}", field.name())
        };
        let name = if hint.name_override.is_empty() {
            qualified
        } else {
            hint.name_override.clone()
        };
        validate_hint(field, &field_shape, &hint, &path)?;

        if let Shape::Message {
            full,
            entry: child,
            map,
        } = &field_shape
        {
            if (wrapper_kind(full).is_some() || hint.kind == pb::MappedKind::Date)
                && family(hint.kind, is_repeated(field)) != pb::ColumnFamily::None
                && child
                    .desc
                    .field
                    .iter()
                    .enumerate()
                    .any(|(i, _)| hint_map.contains_key(&(child.key.clone(), i)))
            {
                return Err(refuse_at(&path, "indexing hints on well-known value components are unsupported; put the hint on the containing field"));
            }
            let blocked = depth >= MAX_DEPTH || visiting.iter().any(|n| n == full);
            if hint.role == pb::MappedRole::Chunks {
                // The chunk scope keeps its container entry and expands
                // its children as unprefixed fields: within a block the
                // children are their own documents, not properties of
                // the parent.
                out.push(planned(&path, &name, field, &field_shape, &hint));
                if !blocked {
                    walk(child, &path, "", depth + 1, index, hint_map, out, visiting)?;
                }
                continue;
            }
            let expandable = !is_repeated(field)
                && !map
                && !well_known_leaf(full)
                && !matches!(
                    hint.kind,
                    pb::MappedKind::Object | pb::MappedKind::Nested if hint.explicit_kind
                );
            if expandable && !blocked {
                walk(
                    child,
                    &path,
                    &name,
                    depth + 1,
                    index,
                    hint_map,
                    out,
                    visiting,
                )?;
                continue;
            }
        }
        out.push(planned(&path, &name, field, &field_shape, &hint));
    }
    visiting.pop();
    Ok(())
}

fn join_path(prefix: &str, segment: &str) -> String {
    if prefix.is_empty() {
        segment.to_string()
    } else {
        format!("{prefix}.{segment}")
    }
}

// ---------------------------------------------------------------------
// Structural resolution: chunks, vector, doc id, chunk id
// ---------------------------------------------------------------------

fn paths(fields: &[&pb::MappedField]) -> String {
    fields
        .iter()
        .map(|f| f.path.as_str())
        .collect::<Vec<_>>()
        .join(", ")
}

/// At most one CHUNKS scope per plan.
fn resolve_chunks(fields: &[pb::MappedField]) -> Result<Option<String>, Status> {
    let chunks: Vec<&pb::MappedField> = fields
        .iter()
        .filter(|f| f.role == pb::MappedRole::Chunks as i32)
        .collect();
    match chunks.len() {
        0 => Ok(None),
        1 => Ok(Some(chunks[0].path.clone())),
        n => Err(refuse(format!(
            "the schema hints {n} CHUNKS fields ({}); a document has at most one chunk scope",
            paths(&chunks)
        ))),
    }
}

/// Pick the plan's vector field. An explicit VECTOR hint wins; without
/// one, exactly one repeated float/double field with a vector-shaped
/// name is accepted and its planned kind is rewritten to VECTOR.
/// Anything else is an error naming the fix.
///
/// When the plan has a CHUNKS scope the vector must live inside it
/// (each chunk is a searchable row). When there is no CHUNKS scope the
/// vector must not pass through one.
fn resolve_vector(
    fields: &mut [pb::MappedField],
    chunks_path: Option<&str>,
) -> Result<String, Status> {
    let explicit: Vec<&pb::MappedField> = fields
        .iter()
        .filter(|f| f.kind == pb::MappedKind::Vector as i32)
        .collect();
    let path = match explicit.len() {
        1 => explicit[0].path.clone(),
        0 => {
            let candidates: Vec<usize> = fields
                .iter()
                .enumerate()
                .filter(|(_, f)| {
                    f.repeated
                        && (f.kind == pb::MappedKind::Float as i32
                            || f.kind == pb::MappedKind::Double as i32)
                        && vector_shaped_name(f.path.rsplit('.').next().unwrap_or(&f.path))
                })
                .map(|(i, _)| i)
                .collect();
            match candidates.len() {
                1 => {
                    let index = candidates[0];
                    fields[index].kind = pb::MappedKind::Vector as i32;
                    fields[index].family = pb::ColumnFamily::Vector as i32;
                    fields[index].path.clone()
                }
                0 => {
                    return Err(refuse(
                        "no vector field: hint exactly one repeated float field with \
                         (ai.protomolt.proto.index.hints.v1.index).type = \
                         INDEX_FIELD_TYPE_VECTOR, or name it vector/embedding",
                    ))
                }
                _ => {
                    let named: Vec<&pb::MappedField> =
                        candidates.iter().map(|&i| &fields[i]).collect();
                    return Err(refuse(format!(
                        "several fields look like the vector ({}); hint the intended one with \
                         (ai.protomolt.proto.index.hints.v1.index).type = \
                         INDEX_FIELD_TYPE_VECTOR",
                        paths(&named)
                    )));
                }
            }
        }
        n => {
            return Err(refuse(format!(
                "the schema hints {n} VECTOR fields ({}); an index is built over exactly one",
                paths(&explicit)
            )))
        }
    };
    if let Some(chunks) = chunks_path {
        if !path.starts_with(&format!("{chunks}.")) {
            return Err(refuse_at(
                &path,
                "the vector field must live inside the CHUNKS scope when the schema \
                 declares one; each chunk is a searchable row",
            ));
        }
    }
    Ok(path)
}

/// Pick the plan's document id field. An explicit DOC_ID role wins;
/// without one, a singular top-level field named "id" is accepted and
/// its planned role is rewritten. The document id always lives on the
/// parent, never inside the CHUNKS scope.
fn resolve_doc_id(
    fields: &mut [pb::MappedField],
    chunks_path: Option<&str>,
) -> Result<String, Status> {
    let explicit: Vec<&pb::MappedField> = fields
        .iter()
        .filter(|f| f.role == pb::MappedRole::DocId as i32)
        .collect();
    let path = match explicit.len() {
        1 => explicit[0].path.clone(),
        0 => {
            let fallback = fields.iter().position(|f| {
                f.path == "id"
                    && !f.repeated
                    && (f.kind == pb::MappedKind::Keyword as i32
                        || f.kind == pb::MappedKind::Text as i32
                        || f.kind == pb::MappedKind::Int32 as i32
                        || f.kind == pb::MappedKind::Int64 as i32
                        || f.kind == pb::MappedKind::Uint32 as i32
                        || f.kind == pb::MappedKind::Uint64 as i32)
            });
            match fallback {
                Some(index) => {
                    fields[index].role = pb::MappedRole::DocId as i32;
                    fields[index].path.clone()
                }
                None => {
                    return Err(refuse(
                        "no document id field: hint exactly one integer or string field with \
                         (ai.protomolt.proto.index.hints.v1.index).block_role = \
                         BLOCK_ROLE_DOC_ID, or declare a singular top-level field named \"id\"",
                    ))
                }
            }
        }
        n => {
            return Err(refuse(format!(
                "the schema hints {n} DOC_ID fields ({}); a document has exactly one identity",
                paths(&explicit)
            )))
        }
    };
    if let Some(chunks) = chunks_path {
        if path == chunks || path.starts_with(&format!("{chunks}.")) {
            return Err(refuse_at(
                &path,
                "the document id field cannot live inside the CHUNKS scope",
            ));
        }
    }
    let target = fields
        .iter()
        .find(|f| f.path == path)
        .expect("path came from this plan");
    if target.kind == pb::MappedKind::Text as i32 {
        return Err(refuse_at(
            &path,
            "a TEXT document id would dissolve into postings; hint it KEYWORD or use an integer id",
        ));
    }
    if target.repeated {
        return Err(refuse_at(&path, "the document id field must be singular"));
    }
    Ok(path)
}

/// Optional CHUNK_ID inside the CHUNKS scope.
fn resolve_chunk_id(
    fields: &[pb::MappedField],
    chunks_path: Option<&str>,
) -> Result<Option<String>, Status> {
    let ids: Vec<&pb::MappedField> = fields
        .iter()
        .filter(|f| f.role == pb::MappedRole::ChunkId as i32)
        .collect();
    match (chunks_path, ids.len()) {
        (_, 0) => Ok(None),
        (None, _) => Err(refuse(format!(
            "CHUNK_ID fields ({}) require a CHUNKS scope",
            paths(&ids)
        ))),
        (Some(chunks), 1) => {
            let path = &ids[0].path;
            if !path.starts_with(&format!("{chunks}.")) {
                return Err(refuse_at(
                    path,
                    "CHUNK_ID must live inside the CHUNKS scope",
                ));
            }
            if ids[0].repeated {
                return Err(refuse_at(path, "CHUNK_ID must be singular"));
            }
            Ok(Some(path.clone()))
        }
        (Some(_), n) => Err(refuse(format!(
            "the schema hints {n} CHUNK_ID fields ({}); a chunk has at most one identity",
            paths(&ids)
        ))),
    }
}

fn vector_shaped_name(name: &str) -> bool {
    let name = name.to_ascii_lowercase();
    name == "vector"
        || name == "embedding"
        || name.ends_with("_vector")
        || name.ends_with("_embedding")
}

// ---------------------------------------------------------------------
// Fingerprint
// ---------------------------------------------------------------------

/// Canonical fingerprint over the derived plan: a fixed-layout byte
/// encoding (never protobuf serialization, whose byte layout is not
/// canonical) hashed with SHA-256, lowercase hex. The descriptor
/// content address is deliberately NOT covered: two descriptor sets
/// may derive the same plan. The reachable wire schema is covered separately.
fn fingerprint(plan: &pb::MappedPlan, set: &FileDescriptorSet, pool: &DescriptorPool) -> String {
    let mut hasher = sha256::Sha256::new();
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
    sha256::to_hex(&hasher.finalize())
}

/// Hash the reachable descriptor graph independently of file order and source comments.
/// Projection policy is hashed separately above; this graph pins decoding meaning.
fn hash_wire_schema(
    hasher: &mut sha256::Sha256,
    set: &FileDescriptorSet,
    pool: &DescriptorPool,
    root: &str,
) {
    let index = TypeIndex::build(set);
    let enums = collect_enum_descriptors(set);
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
    hasher.update(&(seen.len() as u32).to_le_bytes());
    for name in seen {
        write_str(hasher, &name);
        if let Some(entry) = index.messages.get(&name) {
            let file = &set.file[entry.key.0];
            write_str(hasher, file.syntax.as_deref().unwrap_or("proto2"));
            hasher.update(&[u8::from(
                entry.desc.options.as_ref().is_some_and(|o| o.map_entry()),
            )]);
            let message = pool.get_message_by_name(&name).expect("validated message");
            let extensions: Vec<_> = message.extensions().collect();
            let mut fields: Vec<_> =
                entry
                    .desc
                    .field
                    .iter()
                    .map(|f| ("", f))
                    .chain(extensions.iter().map(|extension| {
                        (extension.full_name(), extension.field_descriptor_proto())
                    }))
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
                    .filter(|other| {
                        field.oneof_index.is_some() && other.oneof_index == field.oneof_index
                    })
                    .map(|other| other.number())
                    .collect();
                members.sort_unstable();
                hasher.update(&(members.len() as u32).to_le_bytes());
                for number in members {
                    hasher.update(&number.to_le_bytes());
                }
            }
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

fn reachable_messages(pool: &DescriptorPool, root: &str) -> Vec<MessageDescriptor> {
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

fn write_str(hasher: &mut sha256::Sha256, text: &str) {
    hasher.update(&(text.len() as u32).to_le_bytes());
    hasher.update(text.as_bytes());
}

// ---------------------------------------------------------------------
// Protobuf-native extraction (increment 2)
// ---------------------------------------------------------------------

/// A validated protobuf decoder followed by a compiled column projection.
/// Dynamic decoding resolves oneofs and message merges before any value is indexed.
pub struct Extractor {
    plan: pb::MappedPlan,
    root: TrieNode,
    descriptor: MessageDescriptor,
    leaves: Vec<Leaf>,
    /// Leaf index of the document BODY (a TEXT field).
    body: usize,
    /// Leaf index of the vector field.
    vector: usize,
    /// Leaf index of the document id field.
    doc_id: usize,
    /// Chunked plans: which leaves live inside the CHUNKS scope and
    /// carry per-chunk values. `None` for a flat plan.
    chunked: Option<ChunkShape>,
}

/// The chunk scope's shape, compiled at bind.
struct ChunkShape {
    /// Parallel to `Extractor::leaves`: true for leaves inside the
    /// CHUNKS scope.
    in_chunk: Vec<bool>,
    /// The CHUNK_ID leaf, when the plan declares one; required per
    /// chunk then.
    chunk_id: Option<usize>,
}

#[derive(Default)]
struct TrieNode {
    /// `(field number, child)`, tiny per message — linear scan.
    children: Vec<(i32, Child)>,
}

enum Child {
    /// A singular message field on the way to leaves.
    Descend(TrieNode),
    /// A landing field; the payload is the leaf's slot index.
    Leaf(usize),
    /// The CHUNKS container: each wire occurrence is ONE chunk, walked
    /// against the inner trie into its own slot set.
    Chunks(TrieNode),
}

struct Leaf {
    path: String,
    /// Engine column name the value lands under.
    name: String,
    land: Land,
}

/// How one decoded leaf lands in the index.
enum Land {
    /// A present wrapper projects its scalar default even when value is omitted.
    Wrapper(Box<Land>),
    /// A TEXT field: the body, or a multi-field column.
    Text,
    /// A string facet value.
    FacetStr,
    /// A bool facet: "true" / "false".
    FacetBool,
    /// An enum facet: first declared alias, or an unknown open-enum number
    /// rendered as decimal. The decoder excludes unknown closed-enum values.
    FacetEnum(HashMap<i64, String>),
    /// A KEYWORD-hinted signed integer facet: the decimal rendering.
    FacetInt,
    /// A KEYWORD-hinted unsigned integer facet, preserving the full u64 range.
    FacetUint,
    /// An i64 column value.
    Int,
    /// A full-domain u64 column value.
    Uint,
    /// A google.protobuf.Timestamp, landing as a TimestampValue so the
    /// ordinary epoch-micros conversion (and its refusals) applies.
    Date,
    /// An f64 column value.
    Num,
    /// The document's dense vector.
    Vector,
}

impl Land {
    fn value_land(&self) -> &Self {
        match self {
            Self::Wrapper(inner) => inner.value_land(),
            _ => self,
        }
    }
}

/// One decoded document, ready for the ordinary ingest path.
pub struct ExtractedDoc {
    /// The ordinary request: body text, multi-field texts, facet /
    /// numeric / integer / timestamp values, all under engine names.
    pub request: pb::AddDocumentsRequest,
    /// The dense vector for the same document.
    pub vector: Vec<f32>,
}

/// A projected value for one document.
#[derive(Clone)]
enum Slot {
    Str(String),
    Int(i64),
    Uint(u64),
    Num(f64),
    Ts { seconds: i64, nanos: i32 },
    Floats(Vec<f32>),
}

impl Extractor {
    /// Compile the extractor for one plan. `body_path` picks the TEXT
    /// field that is the document body; empty binds the plan's only
    /// one; on a chunked plan the body must be a TEXT field inside the
    /// CHUNKS scope. Refusals here are BIND-time: no or ambiguous body,
    /// a TEXT document id.
    pub fn new(
        descriptor_set: &[u8],
        message_type: &str,
        body_path: &str,
    ) -> Result<Extractor, Status> {
        Self::with_definition(descriptor_set, message_type, body_path, None)
    }

    /// Compile the exact explicit projection reviewed by a client.
    pub fn with_definition(
        descriptor_set: &[u8],
        message_type: &str,
        body_path: &str,
        definition: Option<&pb::IndexDefinition>,
    ) -> Result<Extractor, Status> {
        let plan = derive_plan_with_definition(descriptor_set, message_type, definition)?;
        let pool = DescriptorPool::decode(descriptor_set)
            .map_err(|e| refuse(format!("invalid descriptor set: {e}")))?;
        let descriptor = pool
            .get_message_by_name(message_type)
            .ok_or_else(|| refuse("message type is missing from validated descriptors"))?;
        // The searchable rows of a chunked plan are its CHUNKS, so the
        // body — the stored, highlighted text of a row — must live
        // inside the scope; parent TEXT fields denormalize as ordinary
        // multi-field columns on every chunk row instead.
        let chunk_prefix = (!plan.chunks_path.is_empty()).then(|| format!("{}.", plan.chunks_path));
        let in_scope = |path: &str| -> bool {
            chunk_prefix
                .as_deref()
                .is_none_or(|prefix| path.starts_with(prefix))
        };
        let text_paths: Vec<&str> = plan
            .fields
            .iter()
            .filter(|f| f.family == pb::ColumnFamily::TextField as i32)
            .filter(|f| in_scope(&f.path))
            .map(|f| f.path.as_str())
            .collect();
        let body_path = if body_path.is_empty() {
            match text_paths.as_slice() {
                [] => {
                    return Err(refuse(if chunk_prefix.is_some() {
                        "the plan's CHUNKS scope has no TEXT field; each chunk is the \
                         searchable row and stores one text body"
                    } else {
                        "the plan has no TEXT field; mapped ingest stores one text body \
                         per document"
                    }))
                }
                [only] => (*only).to_string(),
                several => {
                    return Err(refuse(format!(
                        "the plan has several TEXT fields ({}); set MappedBind.body_path \
                         to the one that is the document body",
                        several.join(", ")
                    )))
                }
            }
        } else {
            if !text_paths.contains(&body_path) {
                return Err(refuse_at(
                    body_path,
                    format!(
                        "body_path must name one of the plan's{} TEXT fields ({})",
                        if chunk_prefix.is_some() {
                            " CHUNKS-scope"
                        } else {
                            ""
                        },
                        if text_paths.is_empty() {
                            "the plan has none".to_string()
                        } else {
                            text_paths.join(", ")
                        }
                    ),
                ));
            }
            body_path.to_string()
        };
        let id_field = plan
            .fields
            .iter()
            .find(|f| f.path == plan.doc_id_path)
            .expect("derive_plan resolved the id");
        if id_field.kind == pb::MappedKind::Text as i32 {
            return Err(refuse_at(
                &plan.doc_id_path.clone(),
                "a TEXT document id would dissolve into postings; hint it KEYWORD, or \
                 use an integer id",
            ));
        }
        let set = FileDescriptorSet::decode(descriptor_set)
            .expect("derive_plan just decoded these bytes");
        let index = TypeIndex::build(&set);
        let enums = collect_enum_values(&set);
        let root_entry = index
            .messages
            .get(message_type)
            .expect("derive_plan resolved the root type");

        // The chunk message's own descriptor entry, for navigating
        // scope-relative paths (the container is repeated, so paths
        // through it cannot navigate from the root).
        let chunk_entry = match &chunk_prefix {
            Some(_) => {
                let container = index.navigate(root_entry, &plan.chunks_path)?;
                Some(index.message_by_type_name(container.type_name(), &plan.chunks_path)?)
            }
            None => None,
        };
        let mut leaves: Vec<Leaf> = Vec::new();
        let mut in_chunk: Vec<bool> = Vec::new();
        let mut root = TrieNode::default();
        let mut chunk_root = TrieNode::default();
        let mut body = None;
        let mut vector = None;
        let mut doc_id = None;
        let mut chunk_id = None;
        for field in &plan.fields {
            let is_vector = field.path == plan.vector_path;
            if !is_vector && field.family == pb::ColumnFamily::None as i32 {
                // Visible in the plan as FAMILY_NONE; nothing to land.
                // (The CHUNKS container itself lands here too — it is
                // the scope, not a value.)
                continue;
            }
            let scoped = chunk_prefix
                .as_deref()
                .and_then(|prefix| field.path.strip_prefix(prefix));
            let slot = leaves.len();
            let land = match (scoped, &chunk_entry) {
                (Some(relative), Some(entry)) => {
                    let leaf_desc = index.navigate(entry, relative)?;
                    let land = land_for(field, is_vector, leaf_desc, &enums)?;
                    insert_path(&mut chunk_root, entry, &index, relative, Child::Leaf(slot))?;
                    land
                }
                _ => {
                    let leaf_desc = index.navigate(root_entry, &field.path)?;
                    let land = land_for(field, is_vector, leaf_desc, &enums)?;
                    insert_path(
                        &mut root,
                        root_entry,
                        &index,
                        &field.path,
                        Child::Leaf(slot),
                    )?;
                    land
                }
            };
            if field.path == body_path {
                body = Some(slot);
            }
            if is_vector {
                vector = Some(slot);
            }
            if field.path == plan.doc_id_path {
                doc_id = Some(slot);
            }
            if !plan.chunk_id_path.is_empty() && field.path == plan.chunk_id_path {
                chunk_id = Some(slot);
            }
            in_chunk.push(scoped.is_some());
            leaves.push(Leaf {
                path: field.path.clone(),
                name: field.name.clone(),
                land,
            });
        }
        let chunked = match chunk_prefix {
            Some(_) => {
                insert_path(
                    &mut root,
                    root_entry,
                    &index,
                    &plan.chunks_path,
                    Child::Chunks(chunk_root),
                )?;
                Some(ChunkShape { in_chunk, chunk_id })
            }
            None => None,
        };
        Ok(Extractor {
            plan,
            root,
            descriptor,
            leaves,
            body: body.expect("body_path came from the plan's TEXT fields"),
            vector: vector.expect("derive_plan resolved the vector"),
            doc_id: doc_id.expect("derive_plan resolved the id"),
            chunked,
        })
    }

    pub fn plan(&self) -> &pb::MappedPlan {
        &self.plan
    }

    /// The path bound as the document body.
    pub fn body_path(&self) -> &str {
        &self.leaves[self.body].path
    }

    /// Decode one serialized message of the bound type into engine
    /// rows: one row for a flat plan, one row PER CHUNK for a chunked
    /// plan (zero chunks is a legitimate empty document and yields
    /// zero rows). Refuses malformed bytes and absent required values
    /// — body, id, vector, declared chunk id — each naming the field,
    /// chunk refusals naming the chunk ordinal.
    pub fn extract(&self, bytes: &[u8]) -> Result<Vec<ExtractedDoc>, Status> {
        let mut slots: Vec<Option<Slot>> = (0..self.leaves.len()).map(|_| None).collect();
        let mut chunks: Vec<Vec<Option<Slot>>> = Vec::new();
        let message = crate::protobuf::decode(self.descriptor.clone(), bytes)?;
        self.project_message(&message, &self.root, &mut slots, &mut chunks)?;
        if self.chunked.is_none() {
            return Ok(vec![self.assemble(&slots, None)?]);
        }
        let mut rows = Vec::with_capacity(chunks.len());
        for (ordinal, chunk_slots) in chunks.iter().enumerate() {
            let row = self.assemble(&slots, Some(chunk_slots)).map_err(|status| {
                Status::new(
                    status.code(),
                    format!("chunk {ordinal}: {}", status.message()),
                )
            })?;
            rows.push(row);
        }
        Ok(rows)
    }

    /// Build one engine row from the parent slots plus, for chunked
    /// plans, one chunk's slots. Parent values denormalize onto every
    /// chunk row — a filter sees parent and chunk fields together with
    /// no query-time join — and the row's lineage carries the REDUCED
    /// parent id as `parent_id`, the key the engine's parent-collapse
    /// scans already group by.
    fn assemble(
        &self,
        parent: &[Option<Slot>],
        chunk: Option<&Vec<Option<Slot>>>,
    ) -> Result<ExtractedDoc, Status> {
        let mut request = pb::AddDocumentsRequest::default();
        let mut vector = Vec::new();
        let mut reduced_id = None;
        for (index, leaf) in self.leaves.iter().enumerate() {
            let from_chunk = self
                .chunked
                .as_ref()
                .is_some_and(|shape| shape.in_chunk[index]);
            let raw = if from_chunk {
                chunk.expect("chunk rows pass their slots")[index].as_ref()
            } else {
                parent[index].as_ref()
            };
            let Some(slot) = raw else {
                if index == self.body {
                    return Err(refuse_at(&leaf.path, "the document has no body text"));
                }
                if index == self.vector {
                    return Err(refuse_at(&leaf.path, "the document has no vector"));
                }
                if index == self.doc_id {
                    return Err(refuse_at(
                        &leaf.path,
                        "the document has no id; identity is required",
                    ));
                }
                let declared_chunk_id = self
                    .chunked
                    .as_ref()
                    .is_some_and(|shape| shape.chunk_id == Some(index));
                if declared_chunk_id {
                    return Err(refuse_at(
                        &leaf.path,
                        "the chunk has no id; the plan declares CHUNK_ID, so every chunk \
                         carries one",
                    ));
                }
                continue;
            };
            if index == self.doc_id {
                reduced_id = Some(reduce_id(&leaf.land, slot, &leaf.path)?);
            }
            match slot.clone() {
                Slot::Str(value) => {
                    if matches!(leaf.land.value_land(), Land::Text) {
                        if index == self.body {
                            request.text = value;
                        } else {
                            request.fields.push(pb::DocumentField {
                                field: leaf.name.clone(),
                                text: value,
                                analysis: None,
                            });
                        }
                    } else {
                        request.facets.push(pb::FacetValue {
                            field: leaf.name.clone(),
                            value,
                        });
                    }
                }
                Slot::Int(value) => request.integers.push(pb::IntegerValue {
                    field: leaf.name.clone(),
                    value,
                }),
                Slot::Uint(value) => request.unsigned_integers.push(pb::UnsignedIntegerValue {
                    field: leaf.name.clone(),
                    value,
                }),
                Slot::Ts { seconds, nanos } => request.timestamps.push(pb::TimestampValue {
                    field: leaf.name.clone(),
                    value: Some(prost_types::Timestamp { seconds, nanos }),
                }),
                Slot::Num(value) => request.numerics.push(pb::NumericValue {
                    field: leaf.name.clone(),
                    value,
                }),
                Slot::Floats(v) => vector = v,
            }
        }
        if vector.is_empty() {
            return Err(refuse_at(
                &self.leaves[self.vector].path,
                "the document has no vector",
            ));
        }
        if self.plan.dim != 0 && vector.len() != self.plan.dim as usize {
            return Err(refuse_at(
                &self.leaves[self.vector].path,
                format!(
                    "the vector has {} floats; the plan declares dim {}",
                    vector.len(),
                    self.plan.dim
                ),
            ));
        }
        if self.chunked.is_some() {
            let parent_key = reduced_id.expect("an absent doc id refused above");
            request.lineage = Some(pb::DocLineage {
                parent_id: parent_key,
                group_id: 0,
                span_start: 0,
                span_end: 0,
            });
        }
        Ok(ExtractedDoc { request, vector })
    }

    fn project_message(
        &self,
        message: &DynamicMessage,
        node: &TrieNode,
        slots: &mut [Option<Slot>],
        chunks: &mut Vec<Vec<Option<Slot>>>,
    ) -> Result<(), Status> {
        let descriptor = message.descriptor();
        for (number, child) in &node.children {
            let field = descriptor
                .get_field(*number as u32)
                .expect("compiled path belongs to the validated descriptor");
            if !message.has_field(&field)
                && (field.supports_presence() || field.is_list() || field.is_map())
            {
                continue;
            }
            let value = message.get_field(&field);
            match (child, value.as_ref()) {
                (Child::Descend(inner), Value::Message(sub)) => {
                    self.project_message(sub, inner, slots, chunks)?;
                }
                (Child::Chunks(inner), Value::List(values)) => {
                    for value in values {
                        let Value::Message(sub) = value else {
                            unreachable!("validated chunk type")
                        };
                        let mut chunk_slots = vec![None; self.leaves.len()];
                        self.project_message(sub, inner, &mut chunk_slots, &mut Vec::new())?;
                        chunks.push(chunk_slots);
                    }
                }
                (Child::Leaf(slot), value) => {
                    let leaf = &self.leaves[*slot];
                    slots[*slot] = Some(project_leaf(&leaf.land, &leaf.path, value)?);
                }
                _ => unreachable!("validated projection type"),
            }
        }
        Ok(())
    }
}

/// The document-id reduction — part of the contract, so any client can
/// compute the same parent key: an integer id is its 64-bit two's
/// complement pattern verbatim; a string id reduces to the first 8
/// bytes of SHA-256 over its UTF-8 bytes, big-endian. Keyed on the
/// DESCRIPTOR type (a KEYWORD hint on an integer field renders as a
/// facet string but still reduces as the integer it is).
fn reduce_id(land: &Land, slot: &Slot, path: &str) -> Result<u64, Status> {
    if let Land::Wrapper(inner) = land {
        return reduce_id(inner, slot, path);
    }
    match (land, slot) {
        (Land::Int, Slot::Int(value)) => Ok(*value as u64),
        (Land::Uint, Slot::Uint(value)) => Ok(*value),
        (Land::FacetInt | Land::FacetUint, Slot::Str(rendered)) => {
            let parsed = if matches!(land, Land::FacetUint) {
                rendered.parse::<u64>()
            } else {
                rendered.parse::<i64>().map(|value| value as u64)
            };
            parsed.map_err(|_| {
                Status::internal(format!(
                    "plan: doc id {path}: non-decimal own rendering {rendered:?}"
                ))
            })
        }
        (_, Slot::Str(value)) => Ok(u64::from_be_bytes(
            sha256::digest(value.as_bytes())[..8]
                .try_into()
                .expect("32 bytes hold 8"),
        )),
        _ => Err(Status::internal(format!(
            "plan: doc id {path} landed a non-identity slot"
        ))),
    }
}

/// Decide how one planned leaf decodes, from its plan entry and its
/// DESCRIPTOR type. The descriptor type governs the wire encoding; the
/// planned kind/family governs where the value lands — the same split
/// the doc-id reduction rule already uses.
fn land_for(
    field: &pb::MappedField,
    is_vector: bool,
    leaf: &FieldDescriptorProto,
    enums: &HashMap<String, HashMap<i64, String>>,
) -> Result<Land, Status> {
    use prost_types::field_descriptor_proto::Type;
    if leaf.r#type() == Type::Message {
        if let Some(kind) = wrapper_kind(leaf.type_name()) {
            let scalar = FieldDescriptorProto {
                r#type: Some(kind as i32),
                type_name: None,
                ..leaf.clone()
            };
            return land_for(field, is_vector, &scalar, enums)
                .map(|inner| Land::Wrapper(Box::new(inner)));
        }
    }
    if is_vector {
        return match leaf.r#type() {
            Type::Float => Ok(Land::Vector),
            Type::Double => Ok(Land::Vector),
            other => Err(refuse_at(
                &field.path,
                format!("a VECTOR field must be repeated float or double, not {other:?}"),
            )),
        };
    }
    let is_integer = |t: Type| {
        matches!(
            t,
            Type::Int32
                | Type::Int64
                | Type::Sint32
                | Type::Sint64
                | Type::Uint32
                | Type::Uint64
                | Type::Fixed64
                | Type::Sfixed64
                | Type::Fixed32
                | Type::Sfixed32
        )
    };
    let family = field.family;
    if family == pb::ColumnFamily::TextField as i32 {
        return match leaf.r#type() {
            Type::String => Ok(Land::Text),
            other => Err(refuse_at(
                &field.path,
                format!("a TEXT field must be a string, not {other:?}"),
            )),
        };
    }
    if family == pb::ColumnFamily::Facet as i32 {
        return match leaf.r#type() {
            Type::String => Ok(Land::FacetStr),
            Type::Bool => Ok(Land::FacetBool),
            Type::Enum => {
                let full = leaf.type_name().trim_start_matches('.');
                let table = enums.get(full).ok_or_else(|| {
                    refuse_at(
                        &field.path,
                        format!("enum type {full:?} is not in the descriptor set"),
                    )
                })?;
                Ok(Land::FacetEnum(table.clone()))
            }
            Type::Uint32 | Type::Uint64 | Type::Fixed32 | Type::Fixed64 => Ok(Land::FacetUint),
            other => match is_integer(other) {
                true => Ok(Land::FacetInt),
                false => Err(refuse_at(
                    &field.path,
                    format!("no facet rendering for descriptor type {other:?}"),
                )),
            },
        };
    }
    if family == pb::ColumnFamily::I64 as i32 {
        if field.kind == pb::MappedKind::Date as i32 {
            return match leaf.r#type() {
                Type::Message => Ok(Land::Date),
                other => Err(refuse_at(
                    &field.path,
                    format!("a DATE field must be a google.protobuf.Timestamp, not {other:?}"),
                )),
            };
        }
        return match is_integer(leaf.r#type()) {
            true => Ok(Land::Int),
            false => Err(refuse_at(
                &field.path,
                format!(
                    "an i64 column cannot decode descriptor type {:?}",
                    leaf.r#type()
                ),
            )),
        };
    }
    if family == pb::ColumnFamily::U64 as i32 {
        return match leaf.r#type() {
            Type::Uint32 | Type::Fixed32 | Type::Uint64 | Type::Fixed64 => Ok(Land::Uint),
            other => Err(refuse_at(
                &field.path,
                format!("a u64 column cannot decode descriptor type {other:?}"),
            )),
        };
    }
    if family == pb::ColumnFamily::F64 as i32 {
        return match leaf.r#type() {
            Type::Float => Ok(Land::Num),
            Type::Double => Ok(Land::Num),
            other => Err(refuse_at(
                &field.path,
                format!("an f64 column cannot decode descriptor type {other:?}"),
            )),
        };
    }
    Err(Status::internal(format!(
        "plan: field {} landed family {} with no extraction rule",
        field.path, family
    )))
}

/// Thread one dotted path into the number trie. Segments resolve
/// against the DESCRIPTOR (the same navigation that derived the plan),
/// so the trie's numbers are exactly the wire's.
fn insert_path(
    root: &mut TrieNode,
    entry: &MsgEntry<'_>,
    index: &TypeIndex<'_>,
    path: &str,
    terminal: Child,
) -> Result<(), Status> {
    let mut node = root;
    let mut current = entry.desc;
    let segments: Vec<&str> = path.split('.').collect();
    for (position, segment) in segments.iter().enumerate() {
        let field = current
            .field
            .iter()
            .find(|f| f.name() == *segment)
            .expect("navigate resolved this path already");
        let number = field.number();
        if position + 1 == segments.len() {
            node.children.push((number, terminal));
            return Ok(());
        }
        current = index.message_by_type_name(field.type_name(), path)?.desc;
        let children = &mut node.children;
        let at = match children.iter().position(|(n, _)| *n == number) {
            Some(at) => at,
            None => {
                children.push((number, Child::Descend(TrieNode::default())));
                children.len() - 1
            }
        };
        node = match &mut children[at].1 {
            Child::Descend(inner) => inner,
            Child::Leaf(_) | Child::Chunks(_) => {
                return Err(Status::internal(format!(
                    "plan: path {path} descends through a field already planned as a leaf"
                )))
            }
        };
    }
    unreachable!("split('.') yields at least one segment")
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

fn collect_enum_values(set: &FileDescriptorSet) -> HashMap<String, HashMap<i64, String>> {
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

fn project_leaf(land: &Land, path: &str, value: &Value) -> Result<Slot, Status> {
    let integer = |value: &Value| -> Result<i64, Status> {
        match value {
            Value::I32(v) => Ok(i64::from(*v)),
            Value::I64(v) => Ok(*v),
            Value::U32(v) => Ok(i64::from(*v)),
            Value::U64(v) => i64::try_from(*v).map_err(|_| {
                refuse_at(path, format!("unsigned value {v} overflows the i64 column"))
            }),
            _ => Err(refuse_at(path, "expected an integer value")),
        }
    };
    Ok(match (land, value) {
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
        (Land::FacetInt, v) => Slot::Str(integer(v)?.to_string()),
        (Land::FacetUint, Value::U32(v)) => Slot::Str(v.to_string()),
        (Land::FacetUint, Value::U64(v)) => Slot::Str(v.to_string()),
        (Land::Int, v) => Slot::Int(integer(v)?),
        (Land::Uint, Value::U32(v)) => Slot::Uint(u64::from(*v)),
        (Land::Uint, Value::U64(v)) => Slot::Uint(*v),
        (Land::Num, Value::F32(v)) => Slot::Num(f64::from(*v)),
        (Land::Num, Value::F64(v)) => Slot::Num(*v),
        (Land::Date, Value::Message(v)) => {
            let seconds = v
                .get_field_by_name("seconds")
                .and_then(|v| v.as_i64())
                .ok_or_else(|| {
                    refuse_at(path, "Timestamp seconds does not match its descriptor")
                })?;
            let nanos = v
                .get_field_by_name("nanos")
                .and_then(|v| v.as_i32())
                .ok_or_else(|| refuse_at(path, "Timestamp nanos does not match its descriptor"))?;
            crate::protobuf::validate_timestamp(path, &prost_types::Timestamp { seconds, nanos })?;
            Slot::Ts { seconds, nanos }
        }
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
