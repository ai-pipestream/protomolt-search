//! Routing a split by lineage parent, so a document's chunks stay on one
//! child, and the proof each child carries that they did.

use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::wal::WalManifest;

use super::replay::{replay_buckets, replay_buckets_documents, Replay};
use super::{bucket_of, ChildImage};

/// Whose rows a document row travels with. The two cases are different
/// id spaces and must never compare equal: a lineage parent id names a
/// source document, while a self row's id is its positional global id.
/// Conflating them would let an unrelated row whose positional id
/// happens to equal some source parent's id join (or scatter) that
/// parent's group by coincidence.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub(super) enum ParentKey {
    /// The row names no lineage parent; it travels by its own global id.
    SelfRow(u64),
    /// The row names a lineage parent, by source-document id.
    Lineage(u64),
}

impl ParentKey {
    /// The global id whose stable key and hash route this group: the
    /// parent row itself, which is the row the source key was minted for.
    pub(super) fn global(self) -> u64 {
        match self {
            ParentKey::SelfRow(id) | ParentKey::Lineage(id) => id,
        }
    }

    pub(super) fn is_lineage(self) -> bool {
        matches!(self, ParentKey::Lineage(_))
    }
}

/// The parent key of every document row: its lineage parent when the row
/// names one, itself otherwise. A split routes by these so a parent's
/// chunks land on one child; rows absent from the map route as self rows.
pub(super) fn parent_routes(replay: &Replay) -> BTreeMap<u64, ParentKey> {
    replay
        .documents
        .iter()
        .map(|(id, doc)| {
            let key = match doc.lineage.as_ref() {
                Some(lineage) => ParentKey::Lineage(lineage.parent_id),
                None => ParentKey::SelfRow(*id),
            };
            (*id, key)
        })
        .collect()
}

/// The parent key of row `id` under `routes`.
pub(super) fn route_of(routes: &BTreeMap<u64, ParentKey>, id: u64) -> ParentKey {
    routes.get(&id).copied().unwrap_or(ParentKey::SelfRow(id))
}

/// The child one row routes to, and whether that route is in the online
/// hash space. A parent group with a stable key routes by
/// `stable_routing_hash` of the parent row's key (falling back to the
/// row's own key, which keyed ingest mints as the same source key): the
/// exact hash online routed ingest uses, so post-reshard keyed appends
/// for the parent land on the same child. Unkeyed groups fall back to
/// `bucket_of` the parent global id; nothing online routes by that hash,
/// so the caller must not let such a child claim colocation proof.
pub(super) fn parent_child(
    routes: &BTreeMap<u64, ParentKey>,
    keys: &BTreeMap<u64, Vec<u8>>,
    id: u64,
    own_key: Option<&Vec<u8>>,
    n: usize,
) -> (usize, bool) {
    if n <= 1 {
        return (0, true);
    }
    let global = route_of(routes, id).global();
    let key = keys
        .get(&global)
        .filter(|key| !key.is_empty())
        .or_else(|| own_key.filter(|key| !key.is_empty()));
    match key {
        Some(key) => {
            let shift = 64 - n.trailing_zeros();
            (
                (crate::coordinator::stable_routing_hash(key) >> shift) as usize,
                true,
            )
        }
        None => (bucket_of(global, n), false),
    }
}

/// Per partition, whether it shares a lineage parent with another
/// partition: such a child cannot claim its parents colocated.
pub(super) fn scattered_parents(partitions: &[Replay]) -> Vec<bool> {
    let mut first: BTreeMap<ParentKey, usize> = BTreeMap::new();
    let mut scattered = vec![false; partitions.len()];
    for (child, replay) in partitions.iter().enumerate() {
        for parent in parent_routes(replay).into_values() {
            let owner = *first.entry(parent).or_insert(child);
            if owner != child {
                scattered[owner] = true;
                scattered[child] = true;
            }
        }
    }
    scattered
}

/// Clear the colocation proof of every child whose parents another child
/// also holds, or whose rows routed outside the online hash space.
pub(super) fn clear_scattered(children: &mut [ChildImage], scattered: &[bool]) {
    for (child, scattered) in children.iter_mut().zip(scattered) {
        child.parents_colocated &= !scattered;
    }
}

/// Whether any live document of the logs names a lineage parent other than
/// itself, read one WAL bucket at a time: such logs cannot split by whole
/// bucket ranges, which route by row id.
pub(super) fn logs_hold_foreign_parents(
    gens: &[PathBuf],
    manifest: &WalManifest,
    vectors_only: bool,
) -> Result<bool, String> {
    let (count, dim) = (manifest.bucket_count as usize, manifest.dim as usize);
    for bucket in 0..manifest.bucket_count {
        let mut replay = Replay::default();
        for gen in gens {
            if vectors_only {
                replay_buckets(gen, bucket..bucket + 1, count, dim, true, &mut replay)?;
            } else {
                // Lineage lives on documents; do not decode the
                // generation's FP32 vector rows just to read it.
                replay_buckets_documents(gen, bucket..bucket + 1, count, dim, &mut replay)?;
            }
        }
        replay.compact();
        if parent_routes(&replay)
            .iter()
            .any(|(id, parent)| matches!(parent, ParentKey::Lineage(p) if p != id))
        {
            return Ok(true);
        }
    }
    Ok(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pb::{AddDocumentsRequest, DocLineage};

    fn replay(rows: &[(u64, Option<u64>)]) -> Replay {
        let mut replay = Replay::default();
        for &(id, parent) in rows {
            replay.documents.insert(
                id,
                AddDocumentsRequest {
                    lineage: parent.map(|parent_id| DocLineage {
                        parent_id,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            );
        }
        replay
    }

    #[test]
    fn rows_route_by_their_parent_and_unparented_rows_by_their_id() {
        let routes = parent_routes(&replay(&[(1, Some(9)), (2, None), (3, Some(3))]));
        assert_eq!(route_of(&routes, 1), ParentKey::Lineage(9));
        assert_eq!(route_of(&routes, 2), ParentKey::SelfRow(2));
        assert_eq!(route_of(&routes, 3), ParentKey::Lineage(3));
        assert_eq!(
            route_of(&routes, 4),
            ParentKey::SelfRow(4),
            "a vector-only row routes by its id"
        );
        assert_ne!(ParentKey::SelfRow(9), ParentKey::Lineage(9));
    }

    #[test]
    fn keyed_parents_route_in_the_online_hash_space() {
        let routes = parent_routes(&replay(&[(1, Some(9)), (9, None)]));
        let keys = BTreeMap::from([(9, b"source-doc".to_vec())]);
        let (child, keyed) = parent_child(&routes, &keys, 1, None, 4);
        assert!(keyed);
        let expect = (crate::coordinator::stable_routing_hash(b"source-doc") >> 62) as usize;
        assert_eq!(child, expect);
        let (child, keyed) = parent_child(&routes, &BTreeMap::new(), 1, None, 4);
        assert!(!keyed);
        assert_eq!(child, bucket_of(9, 4));
    }

    #[test]
    fn a_parent_held_by_two_children_scatters_both_and_no_other() {
        let partitions = [
            replay(&[(1, Some(9)), (2, None)]),
            replay(&[(3, Some(9))]),
            replay(&[(4, Some(8)), (5, Some(8))]),
        ];
        assert_eq!(scattered_parents(&partitions), vec![true, true, false]);
    }

    #[test]
    fn a_self_row_never_scatters_with_a_lineage_namesake() {
        // Row 9 names no parent; row 1 names source parent 9. Distinct
        // namespaces: holding one each scatters neither.
        let partitions = [replay(&[(9, None)]), replay(&[(1, Some(9))])];
        assert_eq!(scattered_parents(&partitions), vec![false, false]);
    }
}
