//! A shard's pinned leaf and the check of a direct row against it.

use super::config::PlacementTreeConfig;
use super::doc_columns::{eval_document, DocColumns};
use super::tree::{Leaf, Placement};
use crate::filter::Tri;
use crate::pb;

/// A shard's pinned leaf together with the tree it belongs to: what a
/// node started with `--placement-tree` checks a direct row against. The code names the leaf, the tree
/// supplies the predicates on its path, and a row the tree routes to
/// another leaf is refused naming the predicate that sent it there.
#[derive(Debug, Clone)]
pub struct PinnedLeaf {
    placement: Placement,
    leaf: usize,
}

impl PinnedLeaf {
    /// Validate `config` and pin the leaf with `code` on it. Refuses by
    /// name a tree whose column is not `column`, and a code that is not
    /// a leaf of the tree (listing the leaves it has).
    pub fn pin(
        config: &PlacementTreeConfig,
        column: &str,
        code: i64,
    ) -> Result<PinnedLeaf, String> {
        let placement = Placement::validate(config)?;
        if placement.column() != column {
            return Err(format!(
                "placement tree column {:?} differs from --placement-column {column:?}; a shard \
                 checks rows in the column the tree assigns",
                placement.column()
            ));
        }
        let Some(leaf) = placement.leaves().iter().position(|leaf| leaf.code == code) else {
            let known: Vec<String> = placement
                .leaves()
                .iter()
                .map(|leaf| format!("{} = {}", leaf.name, leaf.code))
                .collect();
            return Err(format!(
                "--placement-leaf {code} is not a leaf of the placement tree; the tree's leaves \
                 are {}",
                known.join(", ")
            ));
        };
        Ok(PinnedLeaf { placement, leaf })
    }

    pub fn placement(&self) -> &Placement {
        &self.placement
    }

    /// The pinned leaf.
    pub fn leaf(&self) -> &Leaf {
        &self.placement.leaves()[self.leaf]
    }

    /// Whether the tree routes `doc` to the pinned leaf. A row it routes
    /// elsewhere is refused with the leaf it belongs to and the reason:
    /// an earlier node on some level whose predicate is true on the
    /// row, or a node on the pinned path whose predicate is false or
    /// unknown on it. The evaluation is the coordinator's
    /// ([`Placement::evaluate`]): first match per level, UNKNOWN falls
    /// through, the level's default last.
    pub fn check(&self, doc: &pb::AddDocumentsRequest) -> Result<(), String> {
        let columns = DocColumns::of(doc)?;
        let routed = self.placement.evaluate_columns(&columns);
        let pinned = self.leaf();
        if routed.code == pinned.code {
            return Ok(());
        }
        let mut chain = &self.placement.compiled;
        for &index in &pinned.path {
            let index = index as usize;
            for earlier in &chain[..index] {
                if let Some(expr) = earlier.predicate.as_ref() {
                    if eval_document(expr, &columns) == Tri::True {
                        return Err(format!(
                            "placement: node {:?} ({}) is true on this row, which routes it to \
                             leaf {:?} (code {}), not the pinned leaf {:?} (code {})",
                            earlier.name,
                            earlier.cel,
                            routed.name,
                            routed.code,
                            pinned.name,
                            pinned.code
                        ));
                    }
                }
            }
            let node = &chain[index];
            if let Some(expr) = node.predicate.as_ref() {
                let outcome = match eval_document(expr, &columns) {
                    Tri::True => None,
                    Tri::False => Some("false"),
                    Tri::Unknown => Some("unknown (the row lacks a value it reads)"),
                };
                if let Some(outcome) = outcome {
                    return Err(format!(
                        "placement: node {:?} ({}) is {outcome} on this row, which routes it to \
                         leaf {:?} (code {}), not the pinned leaf {:?} (code {})",
                        node.name, node.cel, routed.name, routed.code, pinned.name, pinned.code
                    ));
                }
            }
            chain = &node.children;
        }
        Err(format!(
            "placement: this row routes to leaf {:?} (code {}), not the pinned leaf {:?} (code {})",
            routed.name, routed.code, pinned.name, pinned.code
        ))
    }
}
