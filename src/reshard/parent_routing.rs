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

/// The stable key a lineage parent's chunks agree on, per parent id:
/// `Some(key)` only when every live chunk of the parent carries that
/// identical non-empty key. Any unkeyed chunk, or two disagreeing keys,
/// records `None`: the parent then routes outside the online hash
/// space. Built from chunk keys alone; a parent id is a source id, so
/// it must never index the positional stable-key map, where the same
/// number names an unrelated row.
pub(super) type LineageKeys = BTreeMap<u64, Option<Vec<u8>>>;

/// Fold one replay's chunks into a lineage key table.
pub(super) fn extend_lineage_keys(table: &mut LineageKeys, replay: &Replay) {
    for (id, doc) in &replay.documents {
        let Some(lineage) = doc.lineage.as_ref() else {
            continue;
        };
        let chunk = replay
            .stable_keys
            .get(id)
            .filter(|key| !key.is_empty())
            .cloned();
        table
            .entry(lineage.parent_id)
            .and_modify(|held| {
                if *held != chunk {
                    *held = None;
                }
            })
            .or_insert(chunk);
    }
}

/// The lineage key table of one whole replay.
pub(super) fn lineage_keys(replay: &Replay) -> LineageKeys {
    let mut table = LineageKeys::new();
    extend_lineage_keys(&mut table, replay);
    table
}

fn key_child(key: &[u8], n: usize) -> usize {
    (crate::coordinator::stable_routing_hash(key) >> (64 - n.trailing_zeros())) as usize
}

/// The child one row routes to, and whether that route is in the online
/// hash space. A lineage group whose chunks agree on one stable key
/// routes by that key's `stable_routing_hash`: the exact hash online
/// routed ingest uses, so post-reshard keyed appends for the parent
/// land on the same child. Everyone else falls back to `bucket_of` the
/// parent (or self) id; nothing online routes by that hash, so the
/// caller must not let such a child claim colocation proof.
pub(super) fn parent_child(
    routes: &BTreeMap<u64, ParentKey>,
    lineage: &LineageKeys,
    id: u64,
    own_key: Option<&Vec<u8>>,
    n: usize,
) -> (usize, bool) {
    if n <= 1 {
        return (0, true);
    }
    match route_of(routes, id) {
        ParentKey::Lineage(parent) => match lineage.get(&parent).and_then(Option::as_ref) {
            Some(key) => (key_child(key, n), true),
            None => (bucket_of(parent, n), false),
        },
        ParentKey::SelfRow(row) => match own_key.filter(|key| !key.is_empty()) {
            Some(key) => (key_child(key, n), true),
            None => (bucket_of(row, n), false),
        },
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

/// Whether any live document of the logs names a lineage parent at all,
/// read one WAL bucket at a time: such logs cannot split by whole bucket
/// ranges, which route by row id. A parent id lives in the source
/// document's id space, so even a row whose parent id equals its own
/// positional id counts: its keyed siblings route by the parent's key
/// hash, never by that coincidence.
pub(super) fn logs_hold_foreign_parents(
    gens: &[PathBuf],
    manifest: &WalManifest,
    vectors_only: bool,
) -> Result<bool, String> {
    if vectors_only {
        // No documents will replay, so no lineage can scatter; do not
        // read every bucket's FP32 rows only to discard them.
        return Ok(false);
    }
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
            .values()
            .any(|parent| matches!(parent, ParentKey::Lineage(_)))
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
        let lineage = LineageKeys::from([(9, Some(b"source-doc".to_vec()))]);
        let (child, keyed) = parent_child(&routes, &lineage, 1, None, 4);
        assert!(keyed);
        let expect = (crate::coordinator::stable_routing_hash(b"source-doc") >> 62) as usize;
        assert_eq!(child, expect);
        let (child, keyed) = parent_child(&routes, &LineageKeys::new(), 1, None, 4);
        assert!(!keyed);
        assert_eq!(child, bucket_of(9, 4));
    }

    #[test]
    fn lineage_keys_never_borrow_a_positional_namesakes_key() {
        // Positional ids 0..8 are unrelated lone rows, each keyed its
        // own key; source parents 0..8 collide with those ids. A chunk
        // of parent 5 must route by its chunks' shared opinion key,
        // never by positional row 5's key.
        let mut full = replay(&[]);
        for id in 0..8u64 {
            full.documents.insert(id, AddDocumentsRequest::default());
            full.stable_keys
                .insert(id, format!("lone-{id}").into_bytes());
        }
        for (id, parent, key) in [(8, 5, "opinion-5"), (9, 5, "opinion-5"), (10, 6, "x")] {
            full.documents.insert(
                id,
                AddDocumentsRequest {
                    lineage: Some(DocLineage {
                        parent_id: parent,
                        ..Default::default()
                    }),
                    ..Default::default()
                },
            );
            full.stable_keys.insert(id, key.as_bytes().to_vec());
        }
        // A second chunk of parent 6 under a different key disagrees.
        full.documents.insert(
            11,
            AddDocumentsRequest {
                lineage: Some(DocLineage {
                    parent_id: 6,
                    ..Default::default()
                }),
                ..Default::default()
            },
        );
        full.stable_keys.insert(11, b"other".to_vec());
        let table = lineage_keys(&full);
        assert_eq!(table[&5].as_deref(), Some(b"opinion-5".as_slice()));
        assert_eq!(table[&6], None, "disagreeing chunks prove no shared key");
        let routes = parent_routes(&full);
        let (child, keyed) = parent_child(&routes, &table, 8, full.stable_keys.get(&8), 4);
        assert!(keyed);
        let opinion = (crate::coordinator::stable_routing_hash(b"opinion-5") >> 62) as usize;
        assert_eq!(child, opinion);
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
