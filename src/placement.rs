//! Placement trees (`docs/placement.md`): CEL as the partition function.
//!
//! An ordered chain of predicates at each level assigns every document
//! to a leaf, and a leaf is a shard group with its own shard count and
//! node set. The choice is stored on the row as an `i64` column holding
//! the root-to-leaf path as a prefix code, so a subtree is one integer
//! range and the segment pruner's range rules cover the tree with no new
//! operator in the filter dialect.
//!
//! This module is the shared core: the tree as configured (TOML in the
//! shard map) and as published (proto), validation, and the code
//! arithmetic. Ingest evaluation, fan-out pruning, the dry run, and the
//! leaf reshard build on it.
//!
//! Rules the validation enforces:
//!
//! - Every chain is ordered and first match wins. A node with an empty
//!   `cel` is its level's default and must be last; the root chain and
//!   every node with children end with one. A document on which every
//!   predicate of a level is false or UNKNOWN takes the default.
//! - Predicates are in the FILTER dialect, so the pruner can reason
//!   about them; a rule needing arithmetic goes through a materialized
//!   column first (`docs/cel-values.md`).
//! - The code stores the chosen index per level in a fixed field of
//!   `level_bits` bits, parent above child, sign bit unused. A leaf's
//!   own predicates along its path (the non-default ones) are a sound
//!   superset of its rows, which is what fan-out pruning uses; the
//!   "AND NOT earlier siblings" part is realized by first-match at
//!   ingest and is never needed for pruning.

use serde::{Deserialize, Serialize};

use crate::pb;

/// The field width (bits per level) when the tree does not say.
pub const DEFAULT_LEVEL_BITS: u32 = 9;
/// Usable bits: `i64` without its sign, so codes are never negative and
/// never the `i64::MIN` absence sentinel.
pub const CODE_BITS: u32 = 63;

/// One node as written in a shard map's `[placement]` table.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct PlacementNodeConfig {
    pub name: String,
    /// Filter-dialect CEL; absent or empty marks the level's default.
    #[serde(default)]
    pub cel: Option<String>,
    /// Shards in this leaf (leaves only; 0 selects 1).
    #[serde(default)]
    pub shards: u32,
    /// Node addresses this leaf's shards may live on (leaves only).
    #[serde(default)]
    pub nodes: Vec<String>,
    #[serde(default)]
    pub children: Vec<PlacementNodeConfig>,
}

/// The tree as written in a shard map's `[placement]` table.
#[derive(Debug, Clone, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct PlacementTreeConfig {
    /// The `i64` column every row carries with its path code.
    pub column: String,
    /// Bits per level; 0 selects [`DEFAULT_LEVEL_BITS`].
    #[serde(default)]
    pub level_bits: u32,
    #[serde(default)]
    pub nodes: Vec<PlacementNodeConfig>,
}

impl PlacementTreeConfig {
    pub fn to_proto(&self) -> pb::PlacementTree {
        fn node(n: &PlacementNodeConfig) -> pb::PlacementNode {
            pb::PlacementNode {
                name: n.name.clone(),
                cel: n.cel.clone().unwrap_or_default(),
                shards: n.shards,
                nodes: n.nodes.clone(),
                children: n.children.iter().map(node).collect(),
            }
        }
        pb::PlacementTree {
            column: self.column.clone(),
            level_bits: self.level_bits,
            nodes: self.nodes.iter().map(node).collect(),
        }
    }

    pub fn from_proto(tree: &pb::PlacementTree) -> Self {
        fn node(n: &pb::PlacementNode) -> PlacementNodeConfig {
            PlacementNodeConfig {
                name: n.name.clone(),
                cel: (!n.cel.is_empty()).then(|| n.cel.clone()),
                shards: n.shards,
                nodes: n.nodes.clone(),
                children: n.children.iter().map(node).collect(),
            }
        }
        PlacementTreeConfig {
            column: tree.column.clone(),
            level_bits: tree.level_bits,
            nodes: tree.nodes.iter().map(node).collect(),
        }
    }
}

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
}

/// A validated placement tree.
#[derive(Debug, Clone)]
pub struct Placement {
    column: String,
    level_bits: u32,
    depth: u32,
    leaves: Vec<Leaf>,
    config: PlacementTreeConfig,
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
        let mut leaves = Vec::new();
        let mut depth = 0u32;
        Self::walk(
            &config.nodes,
            "",
            &[],
            &[],
            level_bits,
            &mut leaves,
            &mut depth,
        )?;
        Ok(Placement {
            column: column.to_string(),
            level_bits,
            depth,
            leaves,
            config: config.clone(),
        })
    }

    #[allow(clippy::too_many_arguments)]
    fn walk(
        chain: &[PlacementNodeConfig],
        prefix: &str,
        path: &[u32],
        own: &[pb::FilterExpr],
        level_bits: u32,
        leaves: &mut Vec<Leaf>,
        depth: &mut u32,
    ) -> Result<(), String> {
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
        *depth = (*depth).max(level);
        let mut names = std::collections::HashSet::new();
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
            let is_default = cel.is_empty();
            if is_default && index != last {
                return Err(format!(
                    "placement: default node {full:?} must be last in its chain"
                ));
            }
            let mut own_here = own.to_vec();
            if !is_default {
                let expr = crate::cel::compile_filter(cel)
                    .map_err(|status| format!("placement: node {full:?}: {}", status.message()))?
                    .ok_or_else(|| format!("placement: node {full:?} compiles to no filter"))?;
                own_here.push(expr);
            }
            let mut path_here = path.to_vec();
            path_here.push(index as u32);
            if node.children.is_empty() {
                leaves.push(Leaf {
                    name: full,
                    code: encode(&path_here, level_bits),
                    path: path_here,
                    shards: node.shards.max(1),
                    nodes: node.nodes.clone(),
                    own: own_here,
                    is_default,
                });
            } else {
                if node.shards != 0 || !node.nodes.is_empty() {
                    return Err(format!(
                        "placement: node {full:?} has children and also shards or nodes; size \
                         the leaves"
                    ));
                }
                Self::walk(
                    &node.children,
                    &full,
                    &path_here,
                    &own_here,
                    level_bits,
                    leaves,
                    depth,
                )?;
            }
        }
        let tail = chain[last].cel.as_deref().map(str::trim).unwrap_or("");
        if !tail.is_empty() {
            return Err(format!(
                "placement: the chain under {prefix:?} has no default (a last node with no cel)"
            ));
        }
        Ok(())
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

/// The path code: index per level in a fixed field, root highest.
pub fn encode(path: &[u32], level_bits: u32) -> i64 {
    let mut code: i64 = 0;
    for (level, index) in path.iter().enumerate() {
        let shift = CODE_BITS - level_bits * (level as u32 + 1);
        code |= (i64::from(*index)) << shift;
    }
    code
}

/// The inclusive code range of every path that extends `path`.
pub fn subtree_range(path: &[u32], level_bits: u32) -> (i64, i64) {
    let lo = encode(path, level_bits);
    let used = level_bits * path.len() as u32;
    let width = CODE_BITS - used;
    let span = if width >= 63 {
        i64::MAX
    } else {
        (1i64 << width) - 1
    };
    (lo, lo | span)
}

/// The index at `level` (0 = root) of `code`.
pub fn index_at(code: i64, level: u32, level_bits: u32) -> u32 {
    let shift = CODE_BITS - level_bits * (level + 1);
    ((code >> shift) & ((1i64 << level_bits) - 1)) as u32
}

#[cfg(test)]
mod tests {
    use super::*;

    fn leaf(name: &str, cel: Option<&str>) -> PlacementNodeConfig {
        PlacementNodeConfig {
            name: name.into(),
            cel: cel.map(str::to_string),
            ..Default::default()
        }
    }

    fn tree(nodes: Vec<PlacementNodeConfig>) -> PlacementTreeConfig {
        PlacementTreeConfig {
            column: "placement".into(),
            level_bits: 0,
            nodes,
        }
    }

    #[test]
    fn codes_are_prefix_ranges() {
        let bits = 9;
        assert_eq!(encode(&[], bits), 0);
        assert_eq!(encode(&[0], bits), 0);
        assert_eq!(encode(&[1], bits), 1 << 54);
        assert_eq!(encode(&[1, 2], bits), (1 << 54) | (2 << 45));
        let (lo, hi) = subtree_range(&[1], bits);
        assert_eq!(lo, 1 << 54);
        assert_eq!(hi, (2 << 54) - 1);
        let (lo2, hi2) = subtree_range(&[1, 2], bits);
        assert!(lo <= lo2 && hi2 <= hi);
        assert_eq!(hi2 - lo2 + 1, 1 << 45);
        assert_eq!(index_at(encode(&[3, 7, 1], bits), 1, bits), 7);
        assert_eq!(index_at(encode(&[3, 7, 1], bits), 2, bits), 1);
        assert!(encode(&[511, 511, 511, 511, 511, 511, 511], bits) >= 0);
        let (_, whole_hi) = subtree_range(&[], bits);
        assert_eq!(whole_hi, i64::MAX);
    }

    #[test]
    fn a_valid_tree_lists_its_leaves_in_code_order() {
        let mut recent = leaf("recent", Some("year >= 2020"));
        recent.children = vec![
            leaf("scotus", Some("court == \"scotus\"")),
            leaf("rest", None),
        ];
        let config = tree(vec![
            leaf("large", Some("body_bytes >= 65536")),
            recent,
            leaf("other", None),
        ]);
        let placement = Placement::validate(&config).unwrap();
        let names: Vec<&str> = placement.leaves().iter().map(|l| l.name.as_str()).collect();
        assert_eq!(names, ["large", "recent.scotus", "recent.rest", "other"]);
        let codes: Vec<i64> = placement.leaves().iter().map(|l| l.code).collect();
        assert!(codes.windows(2).all(|w| w[0] < w[1]));
        assert_eq!(placement.depth(), 2);
        let scotus = placement.leaf_by_name("recent.scotus").unwrap();
        assert_eq!(scotus.own.len(), 2, "both predicates on the path");
        assert!(!scotus.is_default);
        let rest = placement.leaf_by_name("recent.rest").unwrap();
        assert_eq!(rest.own.len(), 1, "only the parent's predicate");
        assert!(rest.is_default);
        let (lo, hi) = placement.subtree_range(&[1]);
        assert!(lo <= scotus.code && rest.code <= hi);
        assert!(placement.leaf_by_name("large").unwrap().code < lo);
        assert_eq!(
            placement.leaf_by_code(rest.code).unwrap().name,
            "recent.rest"
        );
        let round = PlacementTreeConfig::from_proto(&config.to_proto());
        assert_eq!(round, config);
    }

    #[test]
    fn refusals_name_the_node() {
        let no_default = tree(vec![leaf("a", Some("year >= 1"))]);
        let err = Placement::validate(&no_default).unwrap_err();
        assert!(err.contains("no default"), "{err}");

        let default_first = tree(vec![leaf("d", None), leaf("a", Some("year >= 1"))]);
        let err = Placement::validate(&default_first).unwrap_err();
        assert!(err.contains("\"d\"") && err.contains("last"), "{err}");

        let value_dialect = tree(vec![leaf("a", Some("year + 1 >= 2")), leaf("d", None)]);
        let err = Placement::validate(&value_dialect).unwrap_err();
        assert!(err.contains("\"a\""), "{err}");

        let repeated = tree(vec![
            leaf("a", Some("year >= 1")),
            leaf("a", Some("year >= 2")),
            leaf("d", None),
        ]);
        let err = Placement::validate(&repeated).unwrap_err();
        assert!(err.contains("repeats"), "{err}");

        let mut sized_parent = leaf("p", Some("year >= 1"));
        sized_parent.shards = 2;
        sized_parent.children = vec![leaf("d", None)];
        let err = Placement::validate(&tree(vec![sized_parent, leaf("d", None)])).unwrap_err();
        assert!(err.contains("children and also shards"), "{err}");

        let mut deep = tree(vec![leaf("a", Some("year >= 1")), leaf("d", None)]);
        deep.level_bits = 32;
        let mut inner = leaf("b", Some("year >= 2"));
        inner.children = vec![leaf("d", None)];
        deep.nodes[0].children = vec![inner, leaf("d", None)];
        let err = Placement::validate(&deep).unwrap_err();
        assert!(err.contains("exceeds"), "{err}");

        let mut wide = tree(
            (0..4)
                .map(|i| leaf(&format!("n{i}"), Some("year >= 1")))
                .collect(),
        );
        wide.nodes.push(leaf("d", None));
        wide.level_bits = 2;
        let err = Placement::validate(&wide).unwrap_err();
        assert!(err.contains("exceed"), "{err}");

        let empty = tree(vec![]);
        assert!(Placement::validate(&empty)
            .unwrap_err()
            .contains("no nodes"));
    }
}
