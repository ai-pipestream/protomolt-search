//! Routing a split by lineage parent, so a document's chunks stay on one
//! child, and the proof each child carries that they did.

use std::collections::BTreeMap;
use std::path::PathBuf;

use crate::wal::WalManifest;

use super::replay::{replay_buckets, Replay};
use super::ChildImage;

/// The routing id of every document row: its lineage parent when the row
/// names one, its own id otherwise. A split routes by these so a parent's
/// chunks land on one child; rows absent from the map route by their id.
pub(super) fn parent_routes(replay: &Replay) -> BTreeMap<u64, u64> {
    replay
        .documents
        .iter()
        .map(|(id, doc)| (*id, doc.lineage.as_ref().map_or(*id, |l| l.parent_id)))
        .collect()
}

/// The routing id of row `id` under `routes`.
pub(super) fn route_of(routes: &BTreeMap<u64, u64>, id: u64) -> u64 {
    routes.get(&id).copied().unwrap_or(id)
}

/// Per partition, whether it shares a lineage parent with another
/// partition: such a child cannot claim its parents colocated.
pub(super) fn scattered_parents(partitions: &[Replay]) -> Vec<bool> {
    let mut first: BTreeMap<u64, usize> = BTreeMap::new();
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
/// also holds.
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
            replay_buckets(
                gen,
                bucket..bucket + 1,
                count,
                dim,
                vectors_only,
                &mut replay,
            )?;
        }
        replay.compact();
        if parent_routes(&replay)
            .iter()
            .any(|(id, parent)| id != parent)
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
        assert_eq!(route_of(&routes, 1), 9);
        assert_eq!(route_of(&routes, 2), 2);
        assert_eq!(route_of(&routes, 3), 3);
        assert_eq!(
            route_of(&routes, 4),
            4,
            "a vector-only row routes by its id"
        );
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
}
