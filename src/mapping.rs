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
//! `(ai.pipestream.proto.index.hints.v1.index)` extension vendored from
//! protomolt: a proto annotated for protomolt's indexers is understood
//! here without modification. Where a field carries no hint, its kind
//! is inferred from the descriptor with protomolt's rules, with one
//! deliberate deviation the frozen turbovec-grpc reference also made:
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
use prost_types::{DescriptorProto, FieldDescriptorProto, FileDescriptorSet};
use tonic::Status;

use crate::pb::{self, hints};
use crate::sha256;

/// Full name of the field-option extension carrying indexing hints,
/// owned by protomolt and vendored under `proto/ai/pipestream/`.
const HINT_EXTENSION_NAME: &str = "ai.pipestream.proto.index.hints.v1.index";

/// The extension's registered field number on
/// `google.protobuf.FieldOptions`.
const HINT_EXTENSION_NUMBER: u64 = 59_100_471;

/// Nested messages deeper than this stop expanding and are recorded as
/// a single OBJECT entry, which also bounds recursive message types.
const MAX_DEPTH: usize = 8;

/// Version tag mixed into the canonical fingerprint bytes. Bump on ANY
/// change to derivation semantics or to the canonical encoding: a
/// changed fingerprint is how drift is caught, and a drift at restore
/// time is an index compatibility event, not a warning.
const FINGERPRINT_VERSION: &str = "turbovec-search.plan.v1";

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
    let index = TypeIndex::build(&set);
    check_extension_declarations(&set)?;
    let hint_map = extract_hints(descriptor_set)?;
    let root = index.messages.get(message_type).ok_or_else(|| {
        refuse(format!(
            "message type {message_type:?} is not in the descriptor set; \
             types present include e.g. {}",
            index.sample_types()
        ))
    })?;

    let mut fields = Vec::new();
    let mut visiting = Vec::new();
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
    if fields.is_empty() {
        return Err(refuse(format!(
            "message type {message_type} has no indexable fields"
        )));
    }

    let chunks_path = resolve_chunks(&fields)?;
    let vector_path = resolve_vector(&mut fields, chunks_path.as_deref())?;
    let doc_id_path = resolve_doc_id(&mut fields, chunks_path.as_deref())?;
    let chunk_id_path = resolve_chunk_id(&fields, chunks_path.as_deref())?;

    // The id's reduction rule depends on the DESCRIPTOR type at the
    // path, not the hinted kind: a KEYWORD hint on an integer field
    // must not switch the id to string hashing.
    let id_leaf = index.navigate(root, &doc_id_path)?;
    let id_ok = matches!(
        id_leaf.r#type(),
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
    };
    plan.fingerprint = fingerprint(&plan);
    Ok(plan)
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
                prost_types::field_descriptor_proto::Type::Message => {
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
/// pipestream hint vocabulary, and a modified indexing_hints.proto is
/// exactly the drift the byte-identity check exists to catch.
fn check_extension_declarations(set: &FileDescriptorSet) -> Result<(), Status> {
    fn check_list(extensions: &[FieldDescriptorProto]) -> Result<(), Status> {
        for extension in extensions {
            let extends_field_options = extension.extendee() == ".google.protobuf.FieldOptions";
            let claims_hint_number = extension.number() == HINT_EXTENSION_NUMBER as i32;
            let is_the_hint =
                extension.type_name() == ".ai.pipestream.proto.index.hints.v1.FieldIndexHint";
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
/// `(ai.pipestream.proto.index.hints.v1.index)` extension payload,
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
        let hint = hints::FieldIndexHint::decode(bytes.as_slice())
            .map_err(|e| refuse(format!("a field's ({HINT_EXTENSION_NAME}) hint does not decode: {e}")))?;
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
    Message { full: String, entry: &'a MsgEntry<'a>, map: bool },
}

fn shape<'a>(
    field: &FieldDescriptorProto,
    index: &'a TypeIndex<'a>,
    path: &str,
) -> Result<Shape<'a>, Status> {
    use prost_types::field_descriptor_proto::Type;
    match field.r#type() {
        Type::Message => {
            let entry = index.message_by_type_name(field.type_name(), path)?;
            let full = field.type_name().trim_start_matches('.').to_string();
            let map = entry
                .desc
                .options
                .as_ref()
                .is_some_and(|o| o.map_entry());
            Ok(Shape::Message { full, entry, map })
        }
        Type::Enum => {
            let full = field.type_name().trim_start_matches('.');
            if full.is_empty() || !index.enums.contains(full) {
                return Err(refuse_at(
                    path,
                    format!("enum type {:?} is not in the descriptor set", field.type_name()),
                ));
            }
            Ok(Shape::Enum)
        }
        Type::Group => Err(refuse_at(path, "proto2 groups are not supported")),
        other => Ok(Shape::Scalar(other)),
    }
}

fn is_repeated(field: &FieldDescriptorProto) -> bool {
    field.label() == prost_types::field_descriptor_proto::Label::Repeated
}

/// Infer a hint from the descriptor alone, with protomolt's rules:
/// strings whose names look like identifiers become KEYWORD, Timestamp
/// becomes DATE, Struct/Value stay OBJECT, repeated and map messages
/// stay NESTED.
fn inferred_kind(field: &FieldDescriptorProto, shape: &Shape<'_>) -> pb::MappedKind {
    use prost_types::field_descriptor_proto::Type;
    match shape {
        Shape::Scalar(Type::String) => {
            if looks_like_keyword(field.name()) {
                pb::MappedKind::Keyword
            } else {
                pb::MappedKind::Text
            }
        }
        Shape::Scalar(Type::Bool) => pb::MappedKind::Boolean,
        Shape::Scalar(Type::Int32 | Type::Uint32 | Type::Sint32 | Type::Fixed32 | Type::Sfixed32) => {
            pb::MappedKind::Int32
        }
        Shape::Scalar(Type::Int64 | Type::Uint64 | Type::Sint64 | Type::Fixed64 | Type::Sfixed64) => {
            pb::MappedKind::Int64
        }
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
    let (kind, explicit_kind, skip) = match hints::IndexFieldType::try_from(hint.r#type) {
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
        let id_ok = matches!(
            shape,
            Shape::Scalar(
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
    }
    Ok(())
}

// ---------------------------------------------------------------------
// The walk
// ---------------------------------------------------------------------

/// Well-known message types that plan as leaves rather than expanding.
fn well_known_leaf(full: &str) -> bool {
    matches!(
        full,
        "google.protobuf.Timestamp" | "google.protobuf.Struct" | "google.protobuf.Value"
    )
}

/// The column plane one planned field lands on. `NONE` is a visible
/// outcome of the dry run, never a silent drop at ingest: the facet,
/// i64, and f64 planes hold one value per document slot, so repeated
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

        if let Shape::Message { full, entry: child, map } = &field_shape {
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
                walk(child, &path, &name, depth + 1, index, hint_map, out, visiting)?;
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
                         (ai.pipestream.proto.index.hints.v1.index).type = \
                         INDEX_FIELD_TYPE_VECTOR, or name it vector/embedding",
                    ))
                }
                _ => {
                    let named: Vec<&pb::MappedField> =
                        candidates.iter().map(|&i| &fields[i]).collect();
                    return Err(refuse(format!(
                        "several fields look like the vector ({}); hint the intended one with \
                         (ai.pipestream.proto.index.hints.v1.index).type = \
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
                        || f.kind == pb::MappedKind::Int64 as i32)
            });
            match fallback {
                Some(index) => {
                    fields[index].role = pb::MappedRole::DocId as i32;
                    fields[index].path.clone()
                }
                None => {
                    return Err(refuse(
                        "no document id field: hint exactly one integer or string field with \
                         (ai.pipestream.proto.index.hints.v1.index).block_role = \
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
/// may derive the same plan, and the plan is what an index binds to.
fn fingerprint(plan: &pb::MappedPlan) -> String {
    let mut hasher = sha256::Sha256::new();
    write_str(&mut hasher, FINGERPRINT_VERSION);
    write_str(&mut hasher, &plan.message_type);
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

fn write_str(hasher: &mut sha256::Sha256, text: &str) {
    hasher.update(&(text.len() as u32).to_le_bytes());
    hasher.update(text.as_bytes());
}
