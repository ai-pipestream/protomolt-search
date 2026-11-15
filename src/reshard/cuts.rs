//! Column cuts: how rows ordered by an integer column are cut into
//! bounded segments, for a partitioned compaction and a re-placement
//! split's column-cut spill.

use std::collections::BTreeMap;

use crate::pb::AddDocumentsRequest;

/// Where a child's rows go under a column cut: the cut of each value,
/// a run longer than the bound split by row count, unkeyed rows last.
#[derive(Debug, Clone, Default)]
pub(super) struct ChildCutPlan {
    /// Inclusive upper key of each cut, ascending.
    bounds: Vec<i64>,
    /// A key whose run is longer than the bound: `(first cut, cuts)`.
    long_runs: BTreeMap<i64, (u32, u32)>,
    /// Rows of a long-run key filed so far, for the split by count.
    filed: BTreeMap<i64, u64>,
    /// The bucket for rows without the column.
    unkeyed: u32,
    /// Buckets in use, unkeyed included.
    pub(super) buckets: u32,
}

impl ChildCutPlan {
    pub(super) fn plan(
        histogram: &BTreeMap<i64, u64>,
        unkeyed_rows: u64,
        rows_per_cut: u64,
    ) -> Self {
        let mut bounds = Vec::new();
        let mut long_runs = BTreeMap::new();
        let mut current = 0u64;
        let mut open = false;
        for (&key, &count) in histogram {
            if count > rows_per_cut {
                if open {
                    bounds.push(key - 1);
                    open = false;
                    current = 0;
                }
                let cuts = count.div_ceil(rows_per_cut) as u32;
                long_runs.insert(key, (bounds.len() as u32, cuts));
                for _ in 0..cuts {
                    bounds.push(key);
                }
                continue;
            }
            if open && current + count > rows_per_cut {
                bounds.push(key - 1);
                current = 0;
            }
            open = true;
            current += count;
        }
        if open {
            bounds.push(i64::MAX);
        }
        let keyed = bounds.len() as u32;
        let has_unkeyed = unkeyed_rows > 0;
        Self {
            bounds,
            long_runs,
            filed: BTreeMap::new(),
            unkeyed: keyed,
            buckets: keyed + u32::from(has_unkeyed),
        }
    }

    pub(super) fn bucket(&mut self, key: Option<i64>, rows_per_cut: u64) -> u32 {
        let Some(key) = key else {
            return self.unkeyed;
        };
        if let Some(&(first, cuts)) = self.long_runs.get(&key) {
            let filed = self.filed.entry(key).or_insert(0);
            let cut = (*filed / rows_per_cut) as u32;
            *filed += 1;
            return first + cut.min(cuts - 1);
        }
        self.bounds.partition_point(|bound| *bound < key) as u32
    }
}

/// Cut plans for every child from the (child, value) histograms.
pub(super) fn plan_cuts(
    histograms: &[BTreeMap<i64, u64>],
    unkeyed: &[u64],
    rows_per_cut: u64,
) -> Vec<ChildCutPlan> {
    histograms
        .iter()
        .zip(unkeyed)
        .map(|(histogram, &unkeyed)| ChildCutPlan::plan(histogram, unkeyed, rows_per_cut))
        .collect()
}

/// The partition key of one logged document: its value in `column`,
/// from the integer list or the timestamp list (both file into the same
/// column at ingest), or `None` when the document does not carry it.
pub(super) fn partition_key_of(
    document: &AddDocumentsRequest,
    column: &str,
) -> Result<Option<i64>, String> {
    if let Some(value) = document.integers.iter().find(|value| value.field == column) {
        return Ok(Some(value.value));
    }
    if let Some(value) = document
        .timestamps
        .iter()
        .find(|value| value.field == column)
    {
        let Some(instant) = value.value.as_ref() else {
            return Ok(None);
        };
        return crate::node::timestamp_to_epoch_micros(column, instant)
            .map(Some)
            .map_err(|status| status.message().to_string());
    }
    Ok(None)
}

/// One output of the cut: the live source ids it holds, in slot order
/// (key ascending, then source id; unkeyed rows in id order).
pub(super) struct Partition {
    pub(super) ids: Vec<u64>,
}

/// Cut keyed rows (sorted by key, then id) into partitions of at most
/// `bound` rows. A cut prefers a key boundary: a run of equal keys moves
/// to the next partition as a unit when it would overflow the current
/// one, so partitions cover disjoint key ranges wherever the data allows;
/// only a run longer than the bound is split, and then the two
/// partitions share that one key. Unkeyed rows follow in id order, cut
/// at the same bound.
pub(super) fn cut_partitions(
    keyed: &[(i64, u64)],
    unkeyed: &[u64],
    bound: usize,
) -> Vec<Partition> {
    let bound = bound.max(1);
    let mut partitions = Vec::new();
    let mut current: Vec<u64> = Vec::new();
    let mut i = 0;
    while i < keyed.len() {
        let key = keyed[i].0;
        let mut j = i;
        while j < keyed.len() && keyed[j].0 == key {
            j += 1;
        }
        let run = &keyed[i..j];
        if !current.is_empty() && current.len() + run.len() > bound {
            partitions.push(Partition {
                ids: std::mem::take(&mut current),
            });
        }
        for piece in run.chunks(bound) {
            if !current.is_empty() && current.len() + piece.len() > bound {
                partitions.push(Partition {
                    ids: std::mem::take(&mut current),
                });
            }
            current.extend(piece.iter().map(|(_, id)| *id));
        }
        i = j;
    }
    if !current.is_empty() {
        partitions.push(Partition { ids: current });
    }
    for piece in unkeyed.chunks(bound) {
        partitions.push(Partition {
            ids: piece.to_vec(),
        });
    }
    partitions
}

#[cfg(test)]
mod partition_tests {
    use super::cut_partitions;

    fn ids(keys: &[i64]) -> Vec<(i64, u64)> {
        keys.iter()
            .enumerate()
            .map(|(i, key)| (*key, i as u64))
            .collect()
    }

    fn shape(keyed: &[(i64, u64)], unkeyed: &[u64], bound: usize) -> Vec<Vec<u64>> {
        cut_partitions(keyed, unkeyed, bound)
            .into_iter()
            .map(|p| p.ids)
            .collect()
    }

    #[test]
    fn a_cut_prefers_key_boundaries() {
        // Three rows of key 1, three of key 2, two of key 3, bound 5: the
        // run of key 2 would overflow the first partition (six rows), so
        // it moves as a unit; keys 2 and 3 then fit together in five.
        let keyed = ids(&[1, 1, 1, 2, 2, 2, 3, 3]);
        assert_eq!(
            shape(&keyed, &[], 5),
            vec![vec![0, 1, 2], vec![3, 4, 5, 6, 7]]
        );
        // At bound 4 the pair of key-3 rows does not fit either.
        assert_eq!(
            shape(&keyed, &[], 4),
            vec![vec![0, 1, 2], vec![3, 4, 5], vec![6, 7]]
        );
    }

    #[test]
    fn a_run_longer_than_the_bound_is_split() {
        let keyed = ids(&[5, 5, 5, 5, 5, 6]);
        assert_eq!(
            shape(&keyed, &[], 2),
            vec![vec![0, 1], vec![2, 3], vec![4, 5]]
        );
    }

    #[test]
    fn unkeyed_rows_follow_in_bounded_pieces() {
        let keyed = ids(&[1, 2]);
        assert_eq!(
            shape(&keyed, &[10, 11, 12], 2),
            vec![vec![0, 1], vec![10, 11], vec![12]]
        );
    }

    #[test]
    fn a_zero_bound_means_one_row_per_partition() {
        let keyed = ids(&[1, 1]);
        assert_eq!(shape(&keyed, &[], 0), vec![vec![0], vec![1]]);
    }
}
