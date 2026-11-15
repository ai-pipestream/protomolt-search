//! The validated tree: its leaves, its compiled predicates, and the
//! routing of one document to a leaf.

use std::collections::HashSet;

use super::bounds::ColumnBounds;
use super::code::{encode, subtree_range};
use super::config::{PlacementNodeConfig, PlacementTreeConfig};
use super::doc_columns::{eval_document, DocColumns};
use super::{CODE_BITS, DEFAULT_LEVEL_BITS};
use crate::filter::Tri;
use crate::pb;

/// One leaf of a validated tree.
#[derive(Debug, Clone)]
pub struct Leaf {
    /// Dotted path of node names from the root.
    pub name: String,
    /// The path code every row of this leaf carries.
    pub code: i64,
    /// Index per level from the root.
    pub path: Vec<u32>,
    pub shards: u32,
    pub nodes: Vec<String>,
    /// The compiled predicates of every non-default node on the path.
    /// A row in this leaf satisfies all of them; a filter that cannot
    /// hold together with them cannot match a row here.
    pub own: Vec<pb::FilterExpr>,
    /// Whether the last node on the path is a level default.
    pub is_default: bool,
    /// What `own` says per column, plus the placement column pinned to
    /// this leaf's code: the ranges and value sets a filter is tested
    /// against before fan-out.
    pub bounds: ColumnBounds,
}

/// One node of the validated tree with its predicate compiled once.
#[derive(Debug, Clone)]
pub(super) struct CompiledNode {
    /// Dotted path of node names from the root.
    pub(super) name: String,
    /// The predicate as written, for a message that names it.
    pub(super) cel: String,
    /// `None` marks the level's default.
    pub(super) predicate: Option<pb::FilterExpr>,
    pub(super) children: Vec<CompiledNode>,
}

/// A validated placement tree.
#[derive(Debug, Clone)]
pub struct Placement {
    column: String,
    level_bits: u32,
    depth: u32,
    leaves: Vec<Leaf>,
    config: PlacementTreeConfig,
    pub(super) compiled: Vec<CompiledNode>,
}

impl Placement {
    /// Validate a configured tree. Every refusal names the node.
    pub fn validate(config: &PlacementTreeConfig) -> Result<Placement, String> {
        let column = config.column.trim();
        if column.is_empty() {
            return Err("placement: column is empty".to_string());
        }
        if column.chars().any(char::is_whitespace) {
            return Err(format!("placement: column {column:?} holds whitespace"));
        }
        let level_bits = if config.level_bits == 0 {
            DEFAULT_LEVEL_BITS
        } else {
            config.level_bits
        };
        if !(1..=32).contains(&level_bits) {
            return Err(format!(
                "placement: level_bits {level_bits} is outside 1..=32"
            ));
        }
        if config.nodes.is_empty() {
            return Err("placement: the root has no nodes".to_string());
        }

        let mut walk = TreeWalk {
            column,
            level_bits,
            leaves: Vec::new(),
            depth: 0,
        };
        let compiled = walk.chain(&config.nodes, "", &[], &[])?;
        Ok(Placement {
            column: column.to_string(),
            level_bits,
            depth: walk.depth,
            leaves: walk.leaves,
            config: config.clone(),
            compiled,
        })
    }

    pub fn column(&self) -> &str {
        &self.column
    }

    pub fn level_bits(&self) -> u32 {
        self.level_bits
    }

    /// Levels in the deepest path.
    pub fn depth(&self) -> u32 {
        self.depth
    }

    /// Every leaf in chain order (which is code order).
    pub fn leaves(&self) -> &[Leaf] {
        &self.leaves
    }

    pub fn config(&self) -> &PlacementTreeConfig {
        &self.config
    }

    /// Every column the tree's predicates read, for a shard or a
    /// coordinator to check against the tables it declares: a predicate naming a column no row
    /// will ever carry is refused at startup, not on the first row.
    pub fn predicate_columns(&self) -> std::collections::BTreeSet<String> {
        fn collect(nodes: &[CompiledNode], out: &mut std::collections::BTreeSet<String>) {
            for node in nodes {
                if let Some(expr) = node.predicate.as_ref() {
                    crate::filter::walk_leaves(expr, &mut |leaf| {
                        use crate::filter::LeafRef;
                        let column = match leaf {
                            LeafRef::Facet(p) => &p.column,
                            LeafRef::Number(p) => &p.column,
                            LeafRef::MapFacet(p) => &p.column,
                            LeafRef::MapNumber(p) | LeafRef::TypedMapNumber(p) => &p.column,
                            LeafRef::MapHasKey(p) | LeafRef::TypedMapHasKey(p) => &p.column,
                            LeafRef::Has(p) => &p.column,
                            LeafRef::Geo(p) => &p.column,
                            LeafRef::StringRange(p) | LeafRef::MapStringRange(p) => &p.column,
                            LeafRef::StringPrefix(p) | LeafRef::MapStringPrefix(p) => &p.column,
                        };
                        out.insert(column.clone());
                    });
                }
                collect(&node.children, out);
            }
        }
        let mut out = std::collections::BTreeSet::new();
        collect(&self.compiled, &mut out);
        out
    }

    pub fn leaf_by_code(&self, code: i64) -> Option<&Leaf> {
        self.leaves.iter().find(|leaf| leaf.code == code)
    }

    pub fn leaf_by_name(&self, name: &str) -> Option<&Leaf> {
        self.leaves.iter().find(|leaf| leaf.name == name)
    }

    /// The inclusive code range of the subtree under `path` (the empty
    /// path is the whole tree).
    pub fn subtree_range(&self, path: &[u32]) -> (i64, i64) {
        subtree_range(path, self.level_bits)
    }
}

impl Placement {
    /// The leaf a document goes to: first match per level, UNKNOWN
    /// falls through, the level's default last. Evaluated over the
    /// document's own request values (`DocColumns`), the same
    /// three-valued rules a shard applies to its stored rows.
    pub fn evaluate(&self, doc: &pb::AddDocumentsRequest) -> Result<&Leaf, String> {
        let columns = DocColumns::of(doc)?;
        Ok(self.evaluate_columns(&columns))
    }

    /// [`Self::evaluate`] over columns already indexed.
    pub fn evaluate_columns(&self, columns: &DocColumns<'_>) -> &Leaf {
        let mut chain = &self.compiled;
        let mut path: Vec<u32> = Vec::new();
        loop {
            let last = chain.len() - 1;
            let mut chosen = last;
            for (index, node) in chain.iter().enumerate() {
                match node.predicate.as_ref() {
                    None => {
                        chosen = index;
                        break;
                    }
                    Some(expr) if eval_document(expr, columns) == Tri::True => {
                        chosen = index;
                        break;
                    }
                    Some(_) => {}
                }
            }
            path.push(chosen as u32);
            let node = &chain[chosen];
            if node.children.is_empty() {
                let code = encode(&path, self.level_bits);
                return self
                    .leaf_by_code(code)
                    .expect("validation listed a leaf for every path");
            }
            chain = &node.children;
        }
    }
}

/// The validation walk over a configured tree: the leaves listed so far in
/// chain order, and the deepest level reached.
struct TreeWalk<'a> {
    column: &'a str,
    level_bits: u32,
    leaves: Vec<Leaf>,
    depth: u32,
}

impl TreeWalk<'_> {
    /// Validates and compiles one chain of sibling nodes under `prefix`,
    /// reached by `path` with the predicates `own` on the way.
    fn chain(
        &mut self,
        chain: &[PlacementNodeConfig],
        prefix: &str,
        path: &[u32],
        own: &[pb::FilterExpr],
    ) -> Result<Vec<CompiledNode>, String> {
        let level_bits = self.level_bits;
        let mut compiled = Vec::with_capacity(chain.len());
        let level = path.len() as u32 + 1;
        if level * level_bits > CODE_BITS {
            return Err(format!(
                "placement: level {level} at {level_bits} bits per level exceeds {CODE_BITS} bits \
                 under {prefix:?}"
            ));
        }
        if chain.len() as u64 > 1u64 << level_bits {
            return Err(format!(
                "placement: {} nodes under {prefix:?} exceed {} at {level_bits} bits per level",
                chain.len(),
                1u64 << level_bits
            ));
        }
        self.depth = self.depth.max(level);
        let mut names = HashSet::new();
        let last = chain.len() - 1;
        for (index, node) in chain.iter().enumerate() {
            let name = node.name.trim();
            if name.is_empty() || name.contains('.') || name.chars().any(char::is_whitespace) {
                return Err(format!(
                    "placement: node {index} under {prefix:?} has an invalid name {:?} (non-empty, \
                     no dots, no whitespace)",
                    node.name
                ));
            }
            if !names.insert(name) {
                return Err(format!(
                    "placement: node name {name:?} repeats under {prefix:?}"
                ));
            }
            let full = if prefix.is_empty() {
                name.to_string()
            } else {
                format!("{prefix}.{name}")
            };
            let cel = node.cel.as_deref().map(str::trim).unwrap_or("");
            if cel.is_empty() && index != last {
                return Err(format!(
                    "placement: default node {full:?} must be last in its chain"
                ));
            }
            let mut path_here = path.to_vec();
            path_here.push(index as u32);
            compiled.push(self.node(node, full, cel, path_here, own)?);
        }
        let tail = chain[last].cel.as_deref().map(str::trim).unwrap_or("");
        if !tail.is_empty() {
            return Err(format!(
                "placement: the chain under {prefix:?} has no default (a last node with no cel)"
            ));
        }
        Ok(compiled)
    }

    /// Compiles the node named `full` at `path_here`: a node without
    /// children is listed as a leaf, any other recurses into its chain.
    fn node(
        &mut self,
        node: &PlacementNodeConfig,
        full: String,
        cel: &str,
        path_here: Vec<u32>,
        own: &[pb::FilterExpr],
    ) -> Result<CompiledNode, String> {
        let is_default = cel.is_empty();
        let mut own_here = own.to_vec();
        let mut predicate = None;
        if !is_default {
            let expr = crate::cel::compile_filter(cel)
                .map_err(|status| format!("placement: node {full:?}: {}", status.message()))?
                .ok_or_else(|| format!("placement: node {full:?} compiles to no filter"))?;
            predicate = Some(expr.clone());
            own_here.push(expr);
        }
        let mut children = Vec::new();
        if node.children.is_empty() {
            let code = encode(&path_here, self.level_bits);
            let mut bounds = ColumnBounds::of_conjunction(&own_here);
            bounds.pin_int(self.column, code);
            self.leaves.push(Leaf {
                name: full.clone(),
                code,
                path: path_here,
                shards: node.shards.max(1),
                nodes: node.nodes.clone(),
                own: own_here,
                is_default,
                bounds,
            });
        } else {
            if node.shards != 0 || !node.nodes.is_empty() {
                return Err(format!(
                    "placement: node {full:?} has children and also shards or nodes; size \
                     the leaves"
                ));
            }
            children = self.chain(&node.children, &full, &path_here, &own_here)?;
        }
        Ok(CompiledNode {
            name: full,
            cel: cel.to_string(),
            predicate,
            children,
        })
    }
}
