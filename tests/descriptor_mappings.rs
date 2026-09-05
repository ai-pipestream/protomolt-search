//! Descriptor-derived mappings, increment 1 (`docs/descriptor-mappings.md`):
//! dry-run derivation, hint reading, refusal-not-guessing, and the plan
//! fingerprint.
//!
//! Descriptor sets are built two ways here. Plain sets use prost-types
//! construction, readable and enough for inference tests. Hint-bearing
//! sets are hand-encoded at the wire level, because the hints live as
//! extensions on `google.protobuf.FieldOptions` and prost drops
//! extension fields — the same reason production extraction walks the
//! raw bytes.

mod common;

use pipestream_search::mapping::derive_plan;
use pipestream_search::pb::{hints, ColumnFamily, MappedKind, MappedRole};
use prost::Message as _;
use prost_types::field_descriptor_proto::{Label, Type};
use prost_types::{
    DescriptorProto, EnumDescriptorProto, EnumValueDescriptorProto, FieldDescriptorProto,
    FileDescriptorProto, FileDescriptorSet, MessageOptions,
};

// ---------------------------------------------------------------------
// prost-types construction for plain (hint-free) sets
// ---------------------------------------------------------------------

fn scalar(name: &str, number: i32, typ: Type, label: Label) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.to_string()),
        number: Some(number),
        label: Some(label as i32),
        r#type: Some(typ as i32),
        ..Default::default()
    }
}

fn message_field(name: &str, number: i32, label: Label, type_name: &str) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.to_string()),
        number: Some(number),
        label: Some(label as i32),
        r#type: Some(Type::Message as i32),
        type_name: Some(type_name.to_string()),
        ..Default::default()
    }
}

fn enum_field(name: &str, number: i32, type_name: &str) -> FieldDescriptorProto {
    FieldDescriptorProto {
        name: Some(name.to_string()),
        number: Some(number),
        label: Some(Label::Optional as i32),
        r#type: Some(Type::Enum as i32),
        type_name: Some(type_name.to_string()),
        ..Default::default()
    }
}

fn timestamp_file() -> FileDescriptorProto {
    FileDescriptorProto {
        name: Some("google/protobuf/timestamp.proto".to_string()),
        package: Some("google.protobuf".to_string()),
        message_type: vec![DescriptorProto {
            name: Some("Timestamp".to_string()),
            field: vec![
                scalar("seconds", 1, Type::Int64, Label::Optional),
                scalar("nanos", 2, Type::Int32, Label::Optional),
            ],
            ..Default::default()
        }],
        ..Default::default()
    }
}

fn encode_set(files: Vec<FileDescriptorProto>) -> Vec<u8> {
    FileDescriptorSet { file: files }.encode_to_vec()
}

/// A product-shaped message exercising every inference rule at once.
fn product_set() -> Vec<u8> {
    let attrs_entry = DescriptorProto {
        name: Some("AttrsEntry".to_string()),
        field: vec![
            scalar("key", 1, Type::String, Label::Optional),
            scalar("value", 2, Type::String, Label::Optional),
        ],
        options: Some(MessageOptions {
            map_entry: Some(true),
            ..Default::default()
        }),
        ..Default::default()
    };
    let meta = DescriptorProto {
        name: Some("Meta".to_string()),
        field: vec![
            scalar("author", 1, Type::String, Label::Optional),
            scalar("page_count", 2, Type::Int32, Label::Optional),
        ],
        ..Default::default()
    };
    let status = EnumDescriptorProto {
        name: Some("Status".to_string()),
        value: vec![EnumValueDescriptorProto {
            name: Some("STATUS_UNSPECIFIED".to_string()),
            number: Some(0),
            ..Default::default()
        }],
        ..Default::default()
    };
    let product = DescriptorProto {
        name: Some("Product".to_string()),
        field: vec![
            scalar("id", 1, Type::String, Label::Optional),
            scalar("title", 2, Type::String, Label::Optional),
            scalar("price", 3, Type::Double, Label::Optional),
            scalar("embedding", 4, Type::Float, Label::Repeated),
            enum_field("status", 5, ".shop.v1.Status"),
            message_field(
                "created_at",
                6,
                Label::Optional,
                ".google.protobuf.Timestamp",
            ),
            message_field("meta", 7, Label::Optional, ".shop.v1.Meta"),
            scalar("tags", 8, Type::String, Label::Repeated),
            scalar("blob", 9, Type::Bytes, Label::Optional),
            message_field("attrs", 10, Label::Repeated, ".shop.v1.Product.AttrsEntry"),
        ],
        nested_type: vec![attrs_entry],
        ..Default::default()
    };
    encode_set(vec![
        timestamp_file(),
        FileDescriptorProto {
            name: Some("shop.proto".to_string()),
            package: Some("shop.v1".to_string()),
            message_type: vec![product, meta],
            enum_type: vec![status],
            dependency: vec!["google/protobuf/timestamp.proto".to_string()],
            ..Default::default()
        },
    ])
}

// ---------------------------------------------------------------------
// Hand-encoded wire construction for hint-bearing sets
// ---------------------------------------------------------------------

fn vint(out: &mut Vec<u8>, mut v: u64) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            break;
        }
        out.push(byte | 0x80);
    }
}

fn ld(out: &mut Vec<u8>, field: u64, bytes: &[u8]) {
    vint(out, (field << 3) | 2);
    vint(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

fn vf(out: &mut Vec<u8>, field: u64, v: u64) {
    vint(out, field << 3);
    vint(out, v);
}

/// One FieldDescriptorProto, optionally carrying the (index) hint
/// extension on its FieldOptions.
fn wire_field(
    name: &str,
    number: u64,
    label: Label,
    typ: Type,
    type_name: Option<&str>,
    hint: Option<&hints::FieldIndexHint>,
) -> Vec<u8> {
    let mut f = Vec::new();
    ld(&mut f, 1, name.as_bytes());
    vf(&mut f, 3, number);
    vf(&mut f, 4, label as u64);
    vf(&mut f, 5, typ as u64);
    if let Some(tn) = type_name {
        ld(&mut f, 6, tn.as_bytes());
    }
    if let Some(hint) = hint {
        let mut options = Vec::new();
        ld(&mut options, 59_100_471, &hint.encode_to_vec());
        ld(&mut f, 8, &options);
    }
    f
}

fn wire_message(name: &str, fields: &[Vec<u8>], nested: &[Vec<u8>]) -> Vec<u8> {
    let mut m = Vec::new();
    ld(&mut m, 1, name.as_bytes());
    for f in fields {
        ld(&mut m, 2, f);
    }
    for n in nested {
        ld(&mut m, 3, n);
    }
    m
}

fn wire_file(name: &str, package: &str, messages: &[Vec<u8>]) -> Vec<u8> {
    let mut f = Vec::new();
    ld(&mut f, 1, name.as_bytes());
    ld(&mut f, 2, package.as_bytes());
    for m in messages {
        ld(&mut f, 4, m);
    }
    f
}

fn wire_set(files: &[Vec<u8>]) -> Vec<u8> {
    let mut s = Vec::new();
    for f in files {
        ld(&mut s, 1, f);
    }
    s
}

fn hint(typ: hints::IndexFieldType) -> hints::FieldIndexHint {
    hints::FieldIndexHint {
        r#type: typ as i32,
        ..Default::default()
    }
}

fn role_hint(role: hints::BlockRole) -> hints::FieldIndexHint {
    hints::FieldIndexHint {
        block_role: role as i32,
        ..Default::default()
    }
}

// ---------------------------------------------------------------------
// Derivation and inference
// ---------------------------------------------------------------------

#[test]
fn the_product_plan_resolves_every_inference_rule() {
    let set = product_set();
    let plan = derive_plan(&set, "shop.v1.Product").unwrap();

    assert_eq!(plan.message_type, "shop.v1.Product");
    assert_eq!(plan.vector_path, "embedding");
    assert_eq!(plan.doc_id_path, "id");
    assert_eq!(plan.dim, 0, "no hint declared dims");
    assert_eq!(plan.chunks_path, "");
    assert_eq!(plan.chunk_id_path, "");

    let rows: Vec<(&str, &str, MappedKind, bool, MappedRole, ColumnFamily)> = plan
        .fields
        .iter()
        .map(|f| {
            (
                f.path.as_str(),
                f.name.as_str(),
                MappedKind::try_from(f.kind).unwrap(),
                f.repeated,
                MappedRole::try_from(f.role).unwrap(),
                ColumnFamily::try_from(f.family).unwrap(),
            )
        })
        .collect();
    use ColumnFamily as C;
    use MappedKind as K;
    use MappedRole as R;
    assert_eq!(
        rows,
        vec![
            // "id" is keyword-shaped and the doc-id fallback.
            ("id", "id", K::Keyword, false, R::DocId, C::Facet),
            ("title", "title", K::Text, false, R::None, C::TextField),
            ("price", "price", K::Double, false, R::None, C::F64),
            // Repeated float named "embedding": the one vector candidate.
            (
                "embedding",
                "embedding",
                K::Vector,
                true,
                R::None,
                C::Vector
            ),
            // Enums are exact values; "status" is keyword-shaped anyway.
            ("status", "status", K::Keyword, false, R::None, C::Facet),
            // Timestamp is a well-known leaf landing as epoch micros.
            ("created_at", "created_at", K::Date, false, R::None, C::I64),
            // A singular unannotated message expands into dotted paths.
            (
                "meta.author",
                "meta_author",
                K::Text,
                false,
                R::None,
                C::TextField
            ),
            (
                "meta.page_count",
                "meta_page_count",
                K::Int32,
                false,
                R::None,
                C::I64
            ),
            // Repeated scalars have no single-slot family: visible NONE.
            ("tags", "tags", K::Text, true, R::None, C::None),
            ("blob", "blob", K::Binary, false, R::None, C::None),
            // A map field stays one NESTED entry.
            ("attrs", "attrs", K::Nested, true, R::None, C::None),
        ]
    );
}

/// Same input, same plan, same fingerprint — and the fingerprint is
/// pinned so a derivation-semantics change cannot land silently
/// (FINGERPRINT_VERSION exists to be bumped WITH such a change).
#[test]
fn the_fingerprint_is_deterministic_and_pinned() {
    let set = product_set();
    let a = derive_plan(&set, "shop.v1.Product").unwrap();
    let b = derive_plan(&set, "shop.v1.Product").unwrap();
    assert_eq!(a, b, "derivation must be a pure function of its input");
    assert_eq!(a.fingerprint.len(), 64);
    assert!(a.fingerprint.bytes().all(|c| c.is_ascii_hexdigit()));
    assert_eq!(
        a.fingerprint, "d32c4247b2ef4c38bf699ea26083be13c22c68854688bffb0d2745fe1fd33934",
        "the derivation semantics or canonical encoding changed; if that \
         was deliberate, bump FINGERPRINT_VERSION and re-pin"
    );
    assert_eq!(
        a.descriptor_sha256,
        pipestream_search::sha256::hex_digest(&set),
        "the descriptor content address is the SHA-256 the exchange \
         contract registers"
    );
}

/// A renamed field is a different plan.
#[test]
fn renaming_a_field_changes_the_fingerprint() {
    let base = derive_plan(&product_set(), "shop.v1.Product").unwrap();
    let mut renamed = FileDescriptorSet::decode(product_set().as_slice()).unwrap();
    renamed.file[1].message_type[0].field[1].name = Some("headline".to_string());
    let renamed = derive_plan(&renamed.encode_to_vec(), "shop.v1.Product").unwrap();
    assert_ne!(base.fingerprint, renamed.fingerprint);
}

/// A recursive type stops expanding at the visit guard and plans the
/// cycle edge as one OBJECT entry instead of looping.
#[test]
fn a_recursive_message_plans_finitely() {
    let node = DescriptorProto {
        name: Some("Node".to_string()),
        field: vec![
            scalar("id", 1, Type::String, Label::Optional),
            message_field("child", 2, Label::Optional, ".t.v1.Node"),
            scalar("embedding", 3, Type::Float, Label::Repeated),
        ],
        ..Default::default()
    };
    let set = encode_set(vec![FileDescriptorProto {
        name: Some("t.proto".to_string()),
        package: Some("t.v1".to_string()),
        message_type: vec![node],
        ..Default::default()
    }]);
    let plan = derive_plan(&set, "t.v1.Node").unwrap();
    let child = plan.fields.iter().find(|f| f.path == "child").unwrap();
    assert_eq!(child.kind, MappedKind::Object as i32);
    assert_eq!(child.family, ColumnFamily::None as i32);
    assert_eq!(plan.fields.len(), 3);
}

// ---------------------------------------------------------------------
// Hints
// ---------------------------------------------------------------------

#[test]
fn hints_are_read_from_field_options_and_win() {
    let vector_hint = hints::FieldIndexHint {
        r#type: hints::IndexFieldType::Vector as i32,
        vector_dims: 4,
        ..Default::default()
    };
    let body_hint = hints::FieldIndexHint {
        r#type: hints::IndexFieldType::Text as i32,
        analyzer: Some("english".to_string()),
        search_analyzer: Some("english_search".to_string()),
        ..Default::default()
    };
    let sku_hint = hints::FieldIndexHint {
        block_role: hints::BlockRole::DocId as i32,
        name: "sku_key".to_string(),
        ..Default::default()
    };
    let doc = wire_message(
        "Doc",
        &[
            wire_field(
                "sku",
                1,
                Label::Optional,
                Type::String,
                None,
                Some(&sku_hint),
            ),
            wire_field(
                "vecs",
                2,
                Label::Repeated,
                Type::Float,
                None,
                Some(&vector_hint),
            ),
            wire_field(
                "body",
                3,
                Label::Optional,
                Type::String,
                None,
                Some(&body_hint),
            ),
        ],
        &[],
    );
    let set = wire_set(&[wire_file("docs.proto", "docs.v1", &[doc])]);
    let plan = derive_plan(&set, "docs.v1.Doc").unwrap();

    assert_eq!(
        plan.doc_id_path, "sku",
        "the DOC_ID role wins over the name fallback"
    );
    assert_eq!(
        plan.vector_path, "vecs",
        "the VECTOR hint wins over name shapes"
    );
    assert_eq!(plan.dim, 4, "declared dims reach the plan");

    let sku = &plan.fields[0];
    assert_eq!(
        sku.name, "sku_key",
        "the name override is the engine column name"
    );
    assert_eq!(sku.role, MappedRole::DocId as i32);
    assert_eq!(
        sku.kind,
        MappedKind::Text as i32,
        "an unset hint type still infers"
    );

    let body = &plan.fields[2];
    assert_eq!(body.analyzer, "english");
    assert_eq!(body.search_analyzer, "english_search");
    assert_eq!(body.family, ColumnFamily::TextField as i32);
}

#[test]
fn a_skip_hint_omits_the_field_from_the_plan() {
    let doc = wire_message(
        "Doc",
        &[
            wire_field("id", 1, Label::Optional, Type::String, None, None),
            wire_field(
                "internal",
                2,
                Label::Optional,
                Type::String,
                None,
                Some(&hint(hints::IndexFieldType::Skip)),
            ),
            wire_field("embedding", 3, Label::Repeated, Type::Float, None, None),
        ],
        &[],
    );
    let set = wire_set(&[wire_file("docs.proto", "docs.v1", &[doc])]);
    let plan = derive_plan(&set, "docs.v1.Doc").unwrap();
    assert!(plan.fields.iter().all(|f| f.path != "internal"));
    assert_eq!(plan.fields.len(), 2);
}

#[test]
fn a_chunked_plan_scopes_vector_and_chunk_id() {
    let chunk = wire_message(
        "Chunk",
        &[
            wire_field(
                "cid",
                1,
                Label::Optional,
                Type::String,
                None,
                Some(&role_hint(hints::BlockRole::ChunkId)),
            ),
            wire_field("embedding", 2, Label::Repeated, Type::Float, None, None),
            wire_field("text", 3, Label::Optional, Type::String, None, None),
        ],
        &[],
    );
    let doc = wire_message(
        "Doc",
        &[
            wire_field("id", 1, Label::Optional, Type::String, None, None),
            wire_field("title", 2, Label::Optional, Type::String, None, None),
            wire_field(
                "chunks",
                3,
                Label::Repeated,
                Type::Message,
                Some(".docs.v1.Chunk"),
                Some(&role_hint(hints::BlockRole::Chunks)),
            ),
        ],
        &[],
    );
    let set = wire_set(&[wire_file("docs.proto", "docs.v1", &[doc, chunk])]);
    let plan = derive_plan(&set, "docs.v1.Doc").unwrap();

    assert_eq!(plan.chunks_path, "chunks");
    assert_eq!(plan.chunk_id_path, "chunks.cid");
    assert_eq!(plan.vector_path, "chunks.embedding");
    assert_eq!(plan.doc_id_path, "id");

    // Chunk children keep prefixed paths but UNPREFIXED names: within a
    // block they are their own documents, not properties of the parent.
    let text = plan
        .fields
        .iter()
        .find(|f| f.path == "chunks.text")
        .unwrap();
    assert_eq!(text.name, "text");
}

// ---------------------------------------------------------------------
// Refusals: every ambiguity is an error naming the fix
// ---------------------------------------------------------------------

fn expect_refusal(set: &[u8], message_type: &str, needle: &str) {
    let status =
        derive_plan(set, message_type).expect_err("this derivation must refuse rather than guess");
    assert_eq!(status.code(), tonic::Code::InvalidArgument);
    assert!(
        status.message().contains(needle),
        "the refusal must name the problem; wanted {needle:?} in: {}",
        status.message()
    );
}

#[test]
fn refusal_table() {
    // No vector candidate at all.
    let no_vector = encode_set(vec![FileDescriptorProto {
        name: Some("t.proto".to_string()),
        package: Some("t.v1".to_string()),
        message_type: vec![DescriptorProto {
            name: Some("Doc".to_string()),
            field: vec![
                scalar("id", 1, Type::String, Label::Optional),
                scalar("title", 2, Type::String, Label::Optional),
            ],
            ..Default::default()
        }],
        ..Default::default()
    }]);
    expect_refusal(&no_vector, "t.v1.Doc", "no vector field");

    // Two vector-shaped candidates: refuse, never pick.
    let two_vectors = encode_set(vec![FileDescriptorProto {
        name: Some("t.proto".to_string()),
        package: Some("t.v1".to_string()),
        message_type: vec![DescriptorProto {
            name: Some("Doc".to_string()),
            field: vec![
                scalar("id", 1, Type::String, Label::Optional),
                scalar("title_embedding", 2, Type::Float, Label::Repeated),
                scalar("body_embedding", 3, Type::Float, Label::Repeated),
            ],
            ..Default::default()
        }],
        ..Default::default()
    }]);
    expect_refusal(
        &two_vectors,
        "t.v1.Doc",
        "several fields look like the vector (title_embedding, body_embedding)",
    );

    // No document id.
    let no_id = encode_set(vec![FileDescriptorProto {
        name: Some("t.proto".to_string()),
        package: Some("t.v1".to_string()),
        message_type: vec![DescriptorProto {
            name: Some("Doc".to_string()),
            field: vec![
                scalar("title", 1, Type::String, Label::Optional),
                scalar("embedding", 2, Type::Float, Label::Repeated),
            ],
            ..Default::default()
        }],
        ..Default::default()
    }]);
    expect_refusal(&no_id, "t.v1.Doc", "no document id field");

    // Unknown message type, with samples so the fix is obvious.
    expect_refusal(&product_set(), "shop.v1.Nope", "types present include");

    // Empty inputs.
    expect_refusal(&[], "t.v1.Doc", "descriptor_set is required");
    expect_refusal(&product_set(), "", "message_type is required");
}

#[test]
fn hint_refusal_table() {
    // DOC_ID on a bool field.
    let bool_id = wire_set(&[wire_file(
        "t.proto",
        "t.v1",
        &[wire_message(
            "Doc",
            &[
                wire_field(
                    "flag",
                    1,
                    Label::Optional,
                    Type::Bool,
                    None,
                    Some(&role_hint(hints::BlockRole::DocId)),
                ),
                wire_field("embedding", 2, Label::Repeated, Type::Float, None, None),
            ],
            &[],
        )],
    )]);
    expect_refusal(
        &bool_id,
        "t.v1.Doc",
        "BLOCK_ROLE_DOC_ID requires an integer or string field",
    );

    // DOC_ID on a repeated field.
    let repeated_id = wire_set(&[wire_file(
        "t.proto",
        "t.v1",
        &[wire_message(
            "Doc",
            &[
                wire_field(
                    "ids",
                    1,
                    Label::Repeated,
                    Type::String,
                    None,
                    Some(&role_hint(hints::BlockRole::DocId)),
                ),
                wire_field("embedding", 2, Label::Repeated, Type::Float, None, None),
            ],
            &[],
        )],
    )]);
    expect_refusal(&repeated_id, "t.v1.Doc", "requires a singular field");

    // A VECTOR hint on a singular string.
    let bad_vector = wire_set(&[wire_file(
        "t.proto",
        "t.v1",
        &[wire_message(
            "Doc",
            &[
                wire_field("id", 1, Label::Optional, Type::String, None, None),
                wire_field(
                    "title",
                    2,
                    Label::Optional,
                    Type::String,
                    None,
                    Some(&hint(hints::IndexFieldType::Vector)),
                ),
            ],
            &[],
        )],
    )]);
    expect_refusal(
        &bad_vector,
        "t.v1.Doc",
        "a VECTOR hint requires a repeated float or repeated double field",
    );

    // Range hints are not in this engine's vocabulary yet.
    let range = wire_set(&[wire_file(
        "t.proto",
        "t.v1",
        &[wire_message(
            "Doc",
            &[
                wire_field("id", 1, Label::Optional, Type::String, None, None),
                wire_field(
                    "years",
                    2,
                    Label::Optional,
                    Type::Int64,
                    None,
                    Some(&hint(hints::IndexFieldType::LongRange)),
                ),
                wire_field("embedding", 3, Label::Repeated, Type::Float, None, None),
            ],
            &[],
        )],
    )]);
    expect_refusal(&range, "t.v1.Doc", "range hints are not supported");

    // Server-side chunk-and-embed is not this engine's job.
    let chunked_policy = wire_set(&[wire_file(
        "t.proto",
        "t.v1",
        &[wire_message(
            "Doc",
            &[
                wire_field("id", 1, Label::Optional, Type::String, None, None),
                wire_field(
                    "body",
                    2,
                    Label::Optional,
                    Type::String,
                    None,
                    Some(&hints::FieldIndexHint {
                        chunking_policy: Some(hints::ChunkingPolicy::default()),
                        ..Default::default()
                    }),
                ),
                wire_field("embedding", 3, Label::Repeated, Type::Float, None, None),
            ],
            &[],
        )],
    )]);
    expect_refusal(
        &chunked_policy,
        "t.v1.Doc",
        "chunking_policy hints are not supported",
    );

    // CHUNK_ID with no CHUNKS scope.
    let orphan_chunk_id = wire_set(&[wire_file(
        "t.proto",
        "t.v1",
        &[wire_message(
            "Doc",
            &[
                wire_field("id", 1, Label::Optional, Type::String, None, None),
                wire_field(
                    "cid",
                    2,
                    Label::Optional,
                    Type::String,
                    None,
                    Some(&role_hint(hints::BlockRole::ChunkId)),
                ),
                wire_field("embedding", 3, Label::Repeated, Type::Float, None, None),
            ],
            &[],
        )],
    )]);
    expect_refusal(&orphan_chunk_id, "t.v1.Doc", "require a CHUNKS scope");
}

#[test]
fn chunk_scope_rules_are_enforced() {
    let chunk = wire_message(
        "Chunk",
        &[wire_field(
            "text",
            1,
            Label::Optional,
            Type::String,
            None,
            None,
        )],
        &[],
    );
    // Chunked schema whose vector lives on the PARENT: refused, each
    // chunk is a searchable row.
    let doc = wire_message(
        "Doc",
        &[
            wire_field("id", 1, Label::Optional, Type::String, None, None),
            wire_field("embedding", 2, Label::Repeated, Type::Float, None, None),
            wire_field(
                "chunks",
                3,
                Label::Repeated,
                Type::Message,
                Some(".t.v1.Chunk"),
                Some(&role_hint(hints::BlockRole::Chunks)),
            ),
        ],
        &[],
    );
    let set = wire_set(&[wire_file("t.proto", "t.v1", &[doc, chunk])]);
    expect_refusal(
        &set,
        "t.v1.Doc",
        "the vector field must live inside the CHUNKS scope",
    );

    // Doc id inside the CHUNKS scope: refused, identity lives on the
    // parent.
    let chunk = wire_message(
        "Chunk",
        &[
            wire_field(
                "id",
                1,
                Label::Optional,
                Type::String,
                None,
                Some(&role_hint(hints::BlockRole::DocId)),
            ),
            wire_field("embedding", 2, Label::Repeated, Type::Float, None, None),
        ],
        &[],
    );
    let doc = wire_message(
        "Doc",
        &[wire_field(
            "chunks",
            1,
            Label::Repeated,
            Type::Message,
            Some(".t.v1.Chunk"),
            Some(&role_hint(hints::BlockRole::Chunks)),
        )],
        &[],
    );
    let set = wire_set(&[wire_file("t.proto", "t.v1", &[doc, chunk])]);
    expect_refusal(
        &set,
        "t.v1.Doc",
        "the document id field cannot live inside the CHUNKS scope",
    );
}

/// A descriptor set that declares extension number 59100471 on
/// FieldOptions as something OTHER than the pipestream hint is refused:
/// the raw pass would otherwise decode garbage as hints.
#[test]
fn a_conflicting_extension_declaration_is_refused() {
    let mut file = FileDescriptorProto {
        name: Some("evil.proto".to_string()),
        package: Some("evil.v1".to_string()),
        message_type: vec![DescriptorProto {
            name: Some("Doc".to_string()),
            field: vec![
                scalar("id", 1, Type::String, Label::Optional),
                scalar("embedding", 2, Type::Float, Label::Repeated),
            ],
            ..Default::default()
        }],
        ..Default::default()
    };
    file.extension.push(FieldDescriptorProto {
        name: Some("index".to_string()),
        number: Some(59_100_471),
        label: Some(Label::Optional as i32),
        r#type: Some(Type::Message as i32),
        type_name: Some(".evil.v1.Doc".to_string()),
        extendee: Some(".google.protobuf.FieldOptions".to_string()),
        ..Default::default()
    });
    let set = encode_set(vec![file]);
    expect_refusal(&set, "evil.v1.Doc", "modified copy of indexing_hints.proto");
}

// ---------------------------------------------------------------------
// The wire surface
// ---------------------------------------------------------------------

/// PlanIndex over gRPC returns exactly what local derivation returns:
/// the RPC is a dry run with no state, so a coordinator with no shards
/// serves it.
#[tokio::test]
async fn plan_index_answers_over_the_wire() {
    use pipestream_search::pb::search_service_client::SearchServiceClient;
    let (addr, coordinator) = common::start_coordinator(Vec::new()).await;
    let mut client = SearchServiceClient::connect(addr).await.unwrap();
    let set = product_set();
    let response = client
        .plan_index(pipestream_search::pb::PlanIndexRequest {
            collection: String::new(),
            descriptor_set: set.clone(),
            message_type: "shop.v1.Product".to_string(),
        })
        .await
        .unwrap()
        .into_inner();
    let local = derive_plan(&set, "shop.v1.Product").unwrap();
    assert_eq!(response.plan, Some(local));

    let refusal = client
        .plan_index(pipestream_search::pb::PlanIndexRequest {
            collection: String::new(),
            descriptor_set: set,
            message_type: "shop.v1.Nope".to_string(),
        })
        .await
        .expect_err("an unknown type must refuse over the wire too");
    assert_eq!(refusal.code(), tonic::Code::InvalidArgument);
    coordinator.abort();
}
