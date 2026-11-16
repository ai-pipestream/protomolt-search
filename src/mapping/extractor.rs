//! The compiled extractor: a validated protobuf decoder followed by a
//! column projection, and the row visit over one decoded message.

use std::collections::HashMap;

use super::MappingError;
use crate::domain_error::DomainError;
use prost_reflect::{DynamicMessage, MessageDescriptor, ReflectMessage, Value};

use super::project::project_leaf;
use crate::pb;

/// A validated protobuf decoder followed by a compiled column projection.
/// Dynamic decoding resolves oneofs and message merges before any value is indexed.
pub struct Extractor {
    pub(super) plan: pb::MappedPlan,
    pub(super) root: TrieNode,
    pub(super) descriptor: MessageDescriptor,
    pub(super) leaves: Vec<Leaf>,
    /// Leaf index of the document BODY (a TEXT field).
    pub(super) body: usize,
    /// Leaf index of the vector field.
    pub(super) vector: usize,
    /// Leaf index of the document id field.
    pub(super) doc_id: usize,
    /// Chunked plans: which leaves live inside the CHUNKS scope and
    /// carry per-chunk values. `None` for a flat plan.
    pub(super) chunked: Option<ChunkShape>,
}

/// The chunk scope's shape, compiled at bind.
pub(super) struct ChunkShape {
    /// Parallel to `Extractor::leaves`: true for leaves inside the
    /// CHUNKS scope.
    pub(super) in_chunk: Vec<bool>,
    /// The CHUNK_ID leaf, when the plan declares one; required per
    /// chunk then.
    pub(super) chunk_id: Option<usize>,
}

#[derive(Default)]
pub(super) struct TrieNode {
    /// `(field number, child)`, tiny per message — linear scan.
    pub(super) children: Vec<(i32, Child)>,
}

pub(super) enum Child {
    /// A singular message field on the way to leaves.
    Descend(TrieNode),
    /// A landing field; the payload is the leaf's slot index.
    Leaf(usize),
    /// The CHUNKS container: each wire occurrence is ONE chunk, walked
    /// against the inner trie into its own slot set.
    Chunks(TrieNode),
}

pub(super) struct Leaf {
    pub(super) path: String,
    /// Engine column name the value lands under.
    pub(super) name: String,
    pub(super) land: Land,
}

/// How one decoded leaf lands in the index.
pub(super) enum Land {
    /// One map entry value projection. Key type belongs to the source descriptor.
    Map(Box<Land>),
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
    pub(super) fn value_land(&self) -> &Self {
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
pub(super) enum Slot {
    MapFacet(Vec<(String, String)>),
    MapNumeric(Vec<(String, f64)>),
    MapInteger(Vec<(String, i64)>),
    MapUnsignedInteger(Vec<(String, u64)>),
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
    ) -> Result<Extractor, MappingError> {
        Self::with_definition(descriptor_set, message_type, body_path, None)
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
    pub fn extract(&self, bytes: &[u8]) -> Result<Vec<ExtractedDoc>, MappingError> {
        let mut rows = Vec::new();
        self.visit_rows(bytes, usize::MAX, |_, row| {
            rows.push(row);
            Ok(())
        })?;
        Ok(rows)
    }

    /// Visit assembled rows with a caller-owned output budget. Check the chunk
    /// count before allocating per-chunk slots; callbacks may stop extraction.
    /// A callback's work must remain private until the entire visit succeeds.
    pub(crate) fn visit_rows(
        &self,
        bytes: &[u8],
        max_rows: usize,
        mut emit: impl FnMut(Option<u32>, ExtractedDoc) -> Result<(), MappingError>,
    ) -> Result<(), MappingError> {
        if max_rows == 0 {
            return Err(MappingError::invalid_argument(
                "projection max_rows must be positive",
            ));
        }
        let mut slots: Vec<Option<Slot>> = (0..self.leaves.len()).map(|_| None).collect();
        let message = crate::protobuf::decode(self.descriptor.clone(), bytes)
            .map_err(MappingError::foreign)?;
        self.project_message(&message, &self.root, &mut slots)?;
        if self.chunked.is_none() {
            return emit(None, self.assemble(&slots, None)?);
        }
        self.visit_chunk_messages(&message, &self.root, max_rows, &mut |ordinal, sub, node| {
            let mut chunk_slots = vec![None; self.leaves.len()];
            let row = self
                .project_message(sub, node, &mut chunk_slots)
                .and_then(|()| self.assemble(&slots, Some(&chunk_slots)))
                .map_err(|error| error.prefixed(&format!("chunk {ordinal}: ")))?;
            let ordinal = u32::try_from(ordinal)
                .map_err(|_| MappingError::resource_exhausted("chunk ordinal exceeds u32"))?;
            emit(Some(ordinal), row)
        })
    }

    fn visit_chunk_messages(
        &self,
        message: &DynamicMessage,
        node: &TrieNode,
        max_rows: usize,
        emit: &mut impl FnMut(usize, &DynamicMessage, &TrieNode) -> Result<(), MappingError>,
    ) -> Result<(), MappingError> {
        let descriptor = message.descriptor();
        for (number, child) in &node.children {
            if matches!(child, Child::Leaf(_)) {
                continue;
            }
            let field = descriptor
                .get_field(*number as u32)
                .expect("compiled path belongs to the validated descriptor");
            if !message.has_field(&field) {
                continue;
            }
            let value = message.get_field(&field);
            match (child, value.as_ref()) {
                (Child::Descend(inner), Value::Message(sub)) => {
                    self.visit_chunk_messages(sub, inner, max_rows, emit)?;
                }
                (Child::Chunks(inner), Value::List(values)) => {
                    if values.len() > max_rows {
                        return Err(MappingError::resource_exhausted(
                            "document projection exceeds max_rows",
                        ));
                    }
                    for (ordinal, value) in values.iter().enumerate() {
                        let Value::Message(sub) = value else {
                            unreachable!("validated chunk type")
                        };
                        emit(ordinal, sub, inner)?;
                    }
                }
                _ => unreachable!("validated projection type"),
            }
        }
        Ok(())
    }

    fn project_message(
        &self,
        message: &DynamicMessage,
        node: &TrieNode,
        slots: &mut [Option<Slot>],
    ) -> Result<(), MappingError> {
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
                    self.project_message(sub, inner, slots)?;
                }
                // The separate chunk visitor projects one row at a time after
                // all parent fields have landed, irrespective of field order.
                (Child::Chunks(_), Value::List(_)) => {}
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
