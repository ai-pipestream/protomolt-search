//! Partitioned compaction of a WAL into per-partition segments.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::pb::wal::wal_record;
use crate::pb::AddDocumentsRequest;
use crate::wal;

use super::child_build::{child_order, finish_child_ordered_with_sources};
#[cfg(doc)]
use super::compact_log::compact_log;
use super::compact_log::{emit_rows_in, replay_counts, CompactionBuild, RowSink};
use super::hnsw_sources::HnswCompactionSources;
use super::replay::{
    read_gen_manifest, read_gens_binding, replay_buckets_through, require_backend_config,
    require_complete_history, Replay,
};
use super::{Analyzer, ColumnTables, PartitionSpec};

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
struct Partition {
    ids: Vec<u64>,
}

/// Cut keyed rows (sorted by key, then id) into partitions of at most
/// `bound` rows. A cut prefers a key boundary: a run of equal keys moves
/// to the next partition as a unit when it would overflow the current
/// one, so partitions cover disjoint key ranges wherever the data allows;
/// only a run longer than the bound is split, and then the two
/// partitions share that one key. Unkeyed rows follow in id order, cut
/// at the same bound.
fn cut_partitions(keyed: &[(i64, u64)], unkeyed: &[u64], bound: usize) -> Vec<Partition> {
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

/// [`compact_log`] for the partitioned layout: the dense all-live rows
/// of ONE generation through `cutoff_clock`, ordered by `spec.column`
/// and cut into segments of at most `spec.bound` rows
/// (docs/immutable-segments.md "Partitioned layout"). Three passes, none
/// holding more than one WAL bucket or one output partition in memory:
///
/// 1. each bucket replays in turn and yields `(key, id)` per live row,
///    which the cut turns into partitions;
/// 2. each bucket replays again and its live rows are appended to one
///    spill log per partition, a single-bucket WAL generation under
///    `out_dir`;
/// 3. each spill log replays into its image, rows in key order, and
///    `sink` receives the rows in that slot order.
///
/// Rows whose document lacks the column, and vector-only rows, form the
/// unkeyed partitions after the keyed ones. A column no live row carries
/// is an error, as is a value that is not an integer. `columns` gives
/// every output the shard's column tables, so a partition that holds no
/// record of a column (the unkeyed one has no partition column at all)
/// still declares it.
#[allow(clippy::too_many_arguments)]
pub fn compact_log_partitioned(
    gen: &Path,
    cutoff_clock: u64,
    out_dir: &Path,
    spec: PartitionSpec<'_>,
    bm25_fields: Option<&[String]>,
    pinned_fingerprints: Option<&[u64]>,
    columns: Option<&ColumnTables>,
    derived: Option<&crate::postings::StoredDerived>,
    analyze: &mut Analyzer,
    sink: &mut RowSink,
) -> Result<CompactionBuild, String> {
    compact_log_partitioned_with_sources(
        gen,
        cutoff_clock,
        out_dir,
        spec,
        bm25_fields,
        pinned_fingerprints,
        columns,
        derived,
        None,
        analyze,
        sink,
    )
}

#[allow(clippy::too_many_arguments)]
pub(crate) fn compact_log_partitioned_with_sources(
    gen: &Path,
    cutoff_clock: u64,
    out_dir: &Path,
    spec: PartitionSpec<'_>,
    bm25_fields: Option<&[String]>,
    pinned_fingerprints: Option<&[u64]>,
    columns: Option<&ColumnTables>,
    derived: Option<&crate::postings::StoredDerived>,
    hnsw_sources: Option<&HnswCompactionSources>,
    analyze: &mut Analyzer,
    sink: &mut RowSink,
) -> Result<CompactionBuild, String> {
    let manifest = read_gen_manifest(gen)?;
    require_backend_config(&manifest, gen)?;
    require_complete_history(&manifest, gen)?;
    let binding = read_gens_binding(std::slice::from_ref(&gen.to_path_buf()))?;
    std::fs::create_dir_all(out_dir)
        .map_err(|error| format!("mkdir {}: {error}", out_dir.display()))?;
    let bucket_count = manifest.bucket_count as usize;
    let dim = manifest.dim as usize;
    let generation = manifest
        .generation
        .checked_add(1)
        .ok_or_else(|| "compaction generation overflow".to_string())?;
    let column = spec.column;

    // Pass 1: keys.
    let mut keyed: Vec<(i64, u64)> = Vec::new();
    let mut unkeyed: Vec<u64> = Vec::new();
    let mut rows_before = 0u64;
    let mut tombstones = 0u64;
    for bucket in 0..manifest.bucket_count {
        let mut replay = Replay::default();
        replay_buckets_through(
            gen,
            bucket..bucket + 1,
            bucket_count,
            dim,
            false,
            cutoff_clock,
            &mut replay,
        )?;
        let (present, tombstoned) = replay_counts(&replay);
        rows_before += present;
        tombstones += tombstoned;
        replay.compact();
        for id in child_order(&replay) {
            match replay.documents.get(&id) {
                Some(document) => match partition_key_of(document, column)? {
                    Some(key) => keyed.push((key, id)),
                    None => unkeyed.push(id),
                },
                None => unkeyed.push(id),
            }
        }
    }
    if keyed.is_empty() {
        return Err(format!(
            "partition column {column:?}: no live document of this shard carries it"
        ));
    }
    keyed.sort_unstable();
    unkeyed.sort_unstable();
    let partitions = cut_partitions(&keyed, &unkeyed, spec.bound);
    let mut partition_of: Vec<(u64, u32)> = partitions
        .iter()
        .enumerate()
        .flat_map(|(index, partition)| partition.ids.iter().map(move |id| (*id, index as u32)))
        .collect();
    partition_of.sort_unstable();
    drop(keyed);
    drop(unkeyed);

    // Pass 2: spill each live row into its partition's log.
    let spill_root = out_dir.join("spill");
    let mut spill_manifest = manifest.clone();
    spill_manifest.bucket_bits = 0;
    spill_manifest.bucket_count = 1;
    let mut spills = Vec::with_capacity(partitions.len());
    for index in 0..partitions.len() {
        let dir = spill_root.join(format!("p{index:05}"));
        let writer = wal::WalWriter::create(&dir, spill_manifest.clone())
            .map_err(|error| format!("create spill log {}: {error}", dir.display()))?;
        spills.push(writer);
    }
    for bucket in 0..manifest.bucket_count {
        let mut replay = Replay::default();
        replay_buckets_through(
            gen,
            bucket..bucket + 1,
            bucket_count,
            dim,
            false,
            cutoff_clock,
            &mut replay,
        )?;
        replay.compact();
        for id in child_order(&replay) {
            let index = partition_of
                .binary_search_by_key(&id, |(id, _)| *id)
                .map(|at| partition_of[at].1 as usize)
                .map_err(|_| format!("compaction lost track of source id {id} between passes"))?;
            let keys: Vec<Vec<u8>> = replay.stable_keys.get(&id).cloned().into_iter().collect();
            if let Some(document) = replay.documents.get(&id) {
                spills[index]
                    .append(wal_record::Op::AddDocuments(
                        crate::pb::wal::LoggedAddDocuments {
                            source_references: Vec::new(),
                            first_id: id,
                            documents: vec![document.clone()],
                            stable_routing_keys: keys.clone(),
                        },
                    ))
                    .map_err(|error| format!("spill document {id}: {error}"))?;
            }
            if let Some(vector) = replay.vectors.get(&id) {
                spills[index]
                    .append(wal_record::Op::AddVectors(
                        crate::pb::wal::LoggedAddVectors {
                            first_id: id,
                            batch: Some(crate::pb::AddVectorsRequest {
                                materialized_rows: replay
                                    .retained
                                    .get(&id)
                                    .cloned()
                                    .into_iter()
                                    .collect(),
                                vectors: vector.clone(),
                                dim: manifest.dim,
                            }),
                            stable_routing_keys: keys,
                        },
                    ))
                    .map_err(|error| format!("spill vector {id}: {error}"))?;
            }
        }
    }
    let spill_dirs: Vec<PathBuf> = spills
        .iter_mut()
        .map(|writer| {
            writer
                .flush()
                .map(|()| writer.dir().to_path_buf())
                .map_err(|error| format!("flush spill log {}: {error}", writer.dir().display()))
        })
        .collect::<Result<_, _>>()?;
    drop(spills);

    // Pass 3: one image per partition, rows in key order.
    let mut id_map = BTreeMap::new();
    let mut images = Vec::with_capacity(partitions.len());
    let mut next_slot = 0u64;
    for (index, (partition, spill_dir)) in partitions.iter().zip(&spill_dirs).enumerate() {
        let mut replay = Replay::default();
        replay_buckets_through(spill_dir, 0..1, 1, dim, false, u64::MAX, &mut replay)?;
        replay.compact();
        let held = replay.vectors.len().max(replay.documents.len());
        if held != partition.ids.len() {
            return Err(format!(
                "partition {index}: the spill log holds {held} rows, the cut assigned {}",
                partition.ids.len()
            ));
        }
        // Vector-bearing rows first, document-only rows after them, each
        // group in the cut's key order: the slot shape the image builder
        // assigns.
        let mut order: Vec<u64> = partition
            .ids
            .iter()
            .copied()
            .filter(|id| replay.vectors.contains_key(id))
            .collect();
        order.extend(
            partition
                .ids
                .iter()
                .copied()
                .filter(|id| !replay.vectors.contains_key(id)),
        );
        let rows = emit_rows_in(&replay, &order, next_slot, &mut id_map, sink)?;
        images.push(finish_child_ordered_with_sources(
            &manifest,
            replay,
            &order,
            index,
            out_dir,
            next_slot,
            0,
            u64::MAX,
            bm25_fields,
            pinned_fingerprints,
            binding.as_ref(),
            derived,
            columns,
            hnsw_sources,
            analyze,
        )?);
        next_slot = next_slot
            .checked_add(rows)
            .ok_or_else(|| "compaction slot overflow".to_string())?;
    }
    let _ = std::fs::remove_dir_all(&spill_root);
    Ok(CompactionBuild {
        generation,
        rows_before,
        tombstones,
        id_map,
        images,
        binding,
    })
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
