//! Placement trees: CEL as the partition function.
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
//!   column first.
//! - The code stores the chosen index per level in a fixed field of
//!   `level_bits` bits, parent above child, sign bit unused. A leaf's
//!   own predicates along its path (the non-default ones) are a sound
//!   superset of its rows, which is what fan-out pruning uses; the
//!   "AND NOT earlier siblings" part is realized by first-match at
//!   ingest and is never needed for pruning.

mod bounds;
mod code;
mod config;
mod doc_columns;
mod implied;
mod mask;
mod pinned;
mod prune;
mod tree;
mod voronoi;

#[cfg(test)]
mod prune_tests;
#[cfg(test)]
mod tree_tests;

pub use bounds::ColumnBounds;
pub use code::{encode, index_at, subtree_range};
pub use config::{PlacementNodeConfig, PlacementTreeConfig};
pub use doc_columns::{eval_document, DocColumns};
pub use implied::{implied_under, without_leaves};
pub use mask::ShardMask;
pub use pinned::PinnedLeaf;
pub use prune::impossible_under;
pub use tree::{Leaf, Placement};
pub use voronoi::VoronoiRouter;
#[cfg(feature = "net")]
pub use voronoi::{train_voronoi_centroids_on_kserve, VoronoiTrainingSpec};

/// The field width (bits per level) when the tree does not say.
pub const DEFAULT_LEVEL_BITS: u32 = 9;
/// Usable bits: `i64` without its sign, so placement codes stay nonnegative.
pub const CODE_BITS: u32 = 63;
