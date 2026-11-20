//! Compilation of an [`Extractor`] from a derived plan.

use std::collections::HashMap;

use super::MappingError;
use prost::Message as _;
use prost_reflect::DescriptorPool;
use prost_types::FileDescriptorSet;

use super::derive::derive_plan_with_definition;
use super::extractor::{Child, ChunkShape, Extractor, Leaf, TrieNode};
use super::fingerprint::collect_enum_values;
use super::land::land_for;
use super::type_index::{MsgEntry, TypeIndex};
use super::{refuse, refuse_at};
use crate::pb;

/// Picks the TEXT field that is the document body: the named one, or the
/// plan's only one. On a chunked plan the candidates are the TEXT fields
/// inside the CHUNKS scope, because each chunk is the searchable row and
/// parent TEXT fields denormalize onto it as ordinary multi-field columns.
fn resolve_body_path(
    plan: &pb::MappedPlan,
    body_path: &str,
    chunk_prefix: Option<&str>,
) -> Result<String, MappingError> {
    let in_scope =
        |path: &str| -> bool { chunk_prefix.is_none_or(|prefix| path.starts_with(prefix)) };
    let text_paths: Vec<&str> = plan
        .fields
        .iter()
        .filter(|f| f.family == pb::ColumnFamily::TextField as i32)
        .filter(|f| in_scope(&f.path))
        .map(|f| f.path.as_str())
        .collect();
    if body_path.is_empty() {
        return match text_paths.as_slice() {
            [] => Err(refuse(if chunk_prefix.is_some() {
                "the plan's CHUNKS scope has no TEXT field; each chunk is the \
                 searchable row and stores one text body"
            } else {
                "the plan has no TEXT field; mapped ingest stores one text body \
                 per document"
            })),
            [only] => Ok((*only).to_string()),
            several => Err(refuse(format!(
                "the plan has several TEXT fields ({}); set MappedBind.body_path \
                 to the one that is the document body",
                several.join(", ")
            ))),
        };
    }
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
    Ok(body_path.to_string())
}

/// The leaves and number tries under construction, with what resolving a
/// planned field against the descriptors needs.
struct LeafCompile<'a> {
    index: &'a TypeIndex<'a>,
    enums: &'a HashMap<String, HashMap<i64, String>>,
    pool: &'a DescriptorPool,
    root_entry: &'a MsgEntry<'a>,
    /// The chunk message's own entry, for scope-relative paths: the
    /// container is repeated, so paths through it cannot navigate from the
    /// root.
    chunk_entry: Option<&'a MsgEntry<'a>>,
    chunk_prefix: Option<&'a str>,
    leaves: Vec<Leaf>,
    in_chunk: Vec<bool>,
    root: TrieNode,
    chunk_root: TrieNode,
}

impl LeafCompile<'_> {
    /// Compiles one landing field into a leaf and threads its path into
    /// the trie of its scope. Returns the leaf's slot.
    fn add_leaf(
        &mut self,
        field: &pb::MappedField,
        is_vector: bool,
    ) -> Result<usize, MappingError> {
        let scoped = self
            .chunk_prefix
            .and_then(|prefix| field.path.strip_prefix(prefix));
        let slot = self.leaves.len();
        let land = match (scoped, self.chunk_entry) {
            (Some(relative), Some(entry)) => {
                let leaf_desc = self.index.navigate(entry, relative)?;
                let land = land_for(field, is_vector, leaf_desc, self.enums, self.pool)?;
                insert_path(
                    &mut self.chunk_root,
                    entry,
                    self.index,
                    relative,
                    Child::Leaf(slot),
                )?;
                land
            }
            _ => {
                let leaf_desc = self.index.navigate(self.root_entry, &field.path)?;
                let land = land_for(field, is_vector, leaf_desc, self.enums, self.pool)?;
                insert_path(
                    &mut self.root,
                    self.root_entry,
                    self.index,
                    &field.path,
                    Child::Leaf(slot),
                )?;
                land
            }
        };
        self.in_chunk.push(scoped.is_some());
        self.leaves.push(Leaf {
            path: field.path.clone(),
            name: field.name.clone(),
            land,
        });
        Ok(slot)
    }

    /// Compiles every landing field of `plan` and, for a chunked plan,
    /// hangs the chunk trie under its container.
    fn run(
        mut self,
        plan: &pb::MappedPlan,
        body_path: &str,
    ) -> Result<CompiledLeaves, MappingError> {
        let mut body = None;
        let mut vector = None;
        let mut doc_id = None;
        let mut chunk_id = None;
        for field in &plan.fields {
            let is_vector = field.path == plan.vector_path;
            if !is_vector && field.family == pb::ColumnFamily::None as i32 {
                // Visible in the plan as FAMILY_NONE; nothing to land. The
                // CHUNKS container itself is here too: it is the scope, not
                // a value.
                continue;
            }
            let slot = self.add_leaf(field, is_vector)?;
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
        }
        let chunked = if self.chunk_prefix.is_some() {
            insert_path(
                &mut self.root,
                self.root_entry,
                self.index,
                &plan.chunks_path,
                Child::Chunks(self.chunk_root),
            )?;
            Some(ChunkShape {
                in_chunk: self.in_chunk,
                chunk_id,
            })
        } else {
            None
        };
        Ok(CompiledLeaves {
            leaves: self.leaves,
            root: self.root,
            chunked,
            body,
            vector,
            doc_id,
        })
    }
}

/// A plan's compiled leaves and trie, with the slots of its structural
/// fields.
struct CompiledLeaves {
    leaves: Vec<Leaf>,
    root: TrieNode,
    chunked: Option<ChunkShape>,
    body: Option<usize>,
    vector: Option<usize>,
    doc_id: Option<usize>,
}

impl Extractor {
    /// Compile the exact explicit projection reviewed by a client.
    pub fn with_definition(
        descriptor_set: &[u8],
        message_type: &str,
        body_path: &str,
        definition: Option<&pb::IndexDefinition>,
    ) -> Result<Extractor, MappingError> {
        let plan = derive_plan_with_definition(descriptor_set, message_type, definition)?;
        let pool = DescriptorPool::decode(descriptor_set)
            .map_err(|e| refuse(format!("invalid descriptor set: {e}")))?;
        let descriptor = pool
            .get_message_by_name(message_type)
            .ok_or_else(|| refuse("message type is missing from validated descriptors"))?;
        let chunk_prefix = (!plan.chunks_path.is_empty()).then(|| format!("{}.", plan.chunks_path));
        let body_path = resolve_body_path(&plan, body_path, chunk_prefix.as_deref())?;

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

        let chunk_entry = match &chunk_prefix {
            Some(_) => {
                let container = index.navigate(root_entry, &plan.chunks_path)?;
                Some(index.message_by_type_name(container.type_name(), &plan.chunks_path)?)
            }
            None => None,
        };
        let compile = LeafCompile {
            index: &index,
            enums: &enums,
            pool: &pool,
            root_entry,
            chunk_entry,
            chunk_prefix: chunk_prefix.as_deref(),
            leaves: Vec::new(),
            in_chunk: Vec::new(),
            root: TrieNode::default(),
            chunk_root: TrieNode::default(),
        };
        let compiled = compile.run(&plan, &body_path)?;
        Ok(Extractor {
            plan,
            root: compiled.root,
            descriptor,
            leaves: compiled.leaves,
            body: compiled
                .body
                .expect("body_path came from the plan's TEXT fields"),
            vector: compiled.vector.expect("derive_plan resolved the vector"),
            doc_id: compiled.doc_id.expect("derive_plan resolved the id"),
            chunked: compiled.chunked,
        })
    }
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
) -> Result<(), MappingError> {
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
                return Err(MappingError::internal(format!(
                    "plan: path {path} descends through a field already planned as a leaf"
                )))
            }
        };
    }
    unreachable!("split('.') yields at least one segment")
}
