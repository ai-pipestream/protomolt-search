//! The tree as written in a shard map, and its proto form.

use serde::{Deserialize, Serialize};

use crate::pb;

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
    /// Bits per level; 0 selects [`super::DEFAULT_LEVEL_BITS`].
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
