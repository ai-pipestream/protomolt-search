//! Partitioned compaction of a WAL into per-partition segments.

use std::path::{Path, PathBuf};

use crate::pb::wal::wal_record;
use crate::wal;

use super::child_build::{child_order, finish_child_ordered, ChildSlot};
#[cfg(doc)]
use super::compact_log::compact_log;
use super::compact_log::{
    emit_rows_in, CompactionBuild, CompactionRequest, CompactionSource, RowCounts, RowSink,
};
use super::cuts::{cut_partitions, partition_key_of, Partition};
use super::replay::{replay_buckets_through, Replay};
use super::{Analyzer, ChildImage, PartitionSpec};

/// [`compact_log`] for the partitioned layout: the dense all-live rows
/// of ONE generation through the request's cutoff, ordered by
/// `spec.column` and cut into segments of at most `spec.bound` rows. Three passes, none
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
/// is an error, as is a value that is not an integer. The request's
/// column tables give every output the shard's tables, so a partition
/// that holds no record of a column (the unkeyed one has no partition
/// column at all) still declares it.
pub fn compact_log_partitioned(
    request: &CompactionRequest<'_>,
    spec: PartitionSpec<'_>,
    analyze: &mut Analyzer,
    sink: &mut RowSink,
) -> Result<CompactionBuild, String> {
    let source = CompactionSource::open(request)?;
    let mut counts = RowCounts::default();
    let partitions = plan_partitions(&source, spec, &mut counts)?;
    let spill_root = request.out_dir.join("spill");
    let spill_dirs = spill_partitions(&source, &partitions, &spill_root)?;
    let mut images = Vec::with_capacity(partitions.len());
    let mut next_slot = 0u64;
    for (index, (partition, spill_dir)) in partitions.iter().zip(&spill_dirs).enumerate() {
        let built = PartitionImage {
            source: &source,
            index,
            partition,
            spill_dir,
            next_slot,
        };
        let (image, rows) = built.build(&mut counts, analyze, sink)?;
        images.push(image);
        next_slot = next_slot
            .checked_add(rows)
            .ok_or_else(|| "compaction slot overflow".to_string())?;
    }
    let _ = std::fs::remove_dir_all(&spill_root);
    Ok(source.build(counts, images))
}

/// Pass 1: every live row's key, cut into partitions.
fn plan_partitions(
    source: &CompactionSource<'_>,
    spec: PartitionSpec<'_>,
    counts: &mut RowCounts,
) -> Result<Vec<Partition>, String> {
    let column = spec.column;
    let mut keyed: Vec<(i64, u64)> = Vec::new();
    let mut unkeyed: Vec<u64> = Vec::new();
    for bucket in 0..source.manifest.bucket_count {
        let mut replay = source.replay(bucket..bucket + 1)?;
        counts.count(&replay);
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
    Ok(cut_partitions(&keyed, &unkeyed, spec.bound))
}

/// Pass 2: spill each live row into its partition's single-bucket log.
fn spill_partitions(
    source: &CompactionSource<'_>,
    partitions: &[Partition],
    spill_root: &Path,
) -> Result<Vec<PathBuf>, String> {
    let mut partition_of: Vec<(u64, u32)> = partitions
        .iter()
        .enumerate()
        .flat_map(|(index, partition)| partition.ids.iter().map(move |id| (*id, index as u32)))
        .collect();
    partition_of.sort_unstable();
    let mut spill_manifest = source.manifest.clone();
    spill_manifest.bucket_bits = 0;
    spill_manifest.bucket_count = 1;
    let mut spills = Vec::with_capacity(partitions.len());
    for index in 0..partitions.len() {
        let dir = spill_root.join(format!("p{index:05}"));
        let writer = wal::WalWriter::create(&dir, spill_manifest.clone())
            .map_err(|error| format!("create spill log {}: {error}", dir.display()))?;
        spills.push(writer);
    }
    for bucket in 0..source.manifest.bucket_count {
        let mut replay = source.replay(bucket..bucket + 1)?;
        replay.compact();
        for id in child_order(&replay) {
            let index = partition_of
                .binary_search_by_key(&id, |(id, _)| *id)
                .map(|at| partition_of[at].1 as usize)
                .map_err(|_| format!("compaction lost track of source id {id} between passes"))?;
            spill_row(&mut spills[index], &replay, id, source.manifest.dim)?;
        }
    }
    flush_spills(spills)
}

/// One live row's records appended to a spill log, document first.
pub(super) fn spill_row(
    spill: &mut wal::WalWriter,
    replay: &Replay,
    id: u64,
    dim: u32,
) -> Result<(), String> {
    let keys: Vec<Vec<u8>> = replay.stable_keys.get(&id).cloned().into_iter().collect();
    if let Some(document) = replay.documents.get(&id) {
        spill
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
        spill
            .append(wal_record::Op::AddVectors(
                crate::pb::wal::LoggedAddVectors {
                    first_id: id,
                    batch: Some(crate::pb::AddVectorsRequest {
                        materialized_rows: replay.retained.get(&id).cloned().into_iter().collect(),
                        vectors: vector.clone(),
                        dim,
                    }),
                    stable_routing_keys: keys,
                },
            ))
            .map_err(|error| format!("spill vector {id}: {error}"))?;
    }
    Ok(())
}

/// Flush every spill log and return their directories.
pub(super) fn flush_spills(mut spills: Vec<wal::WalWriter>) -> Result<Vec<PathBuf>, String> {
    spills
        .iter_mut()
        .map(|writer| {
            writer
                .flush()
                .map(|()| writer.dir().to_path_buf())
                .map_err(|error| format!("flush spill log {}: {error}", writer.dir().display()))
        })
        .collect()
}

/// Pass 3 for one partition: its spill replayed into an image, rows in
/// the cut's key order.
struct PartitionImage<'a> {
    source: &'a CompactionSource<'a>,
    index: usize,
    partition: &'a Partition,
    spill_dir: &'a Path,
    next_slot: u64,
}

impl PartitionImage<'_> {
    fn build(
        self,
        counts: &mut RowCounts,
        analyze: &mut Analyzer,
        sink: &mut RowSink,
    ) -> Result<(ChildImage, u64), String> {
        let source = self.source;
        let ids = &self.partition.ids;
        let mut replay = Replay::default();
        let dim = source.manifest.dim as usize;
        replay_buckets_through(self.spill_dir, 0..1, 1, dim, false, u64::MAX, &mut replay)?;
        replay.compact();
        let held = replay.vectors.len().max(replay.documents.len());
        if held != ids.len() {
            return Err(format!(
                "partition {}: the spill log holds {held} rows, the cut assigned {}",
                self.index,
                ids.len()
            ));
        }
        // Vector-bearing rows first, document-only rows after them, each
        // group in the cut's key order: the slot shape the image builder
        // assigns.
        let (mut order, rest): (Vec<u64>, Vec<u64>) = ids
            .iter()
            .copied()
            .partition(|id| replay.vectors.contains_key(id));
        order.extend(rest);
        let rows = emit_rows_in(&replay, &order, self.next_slot, &mut counts.id_map, sink)?;
        let slot = ChildSlot::full_range(self.index, source.request.out_dir, self.next_slot);
        let image = finish_child_ordered(
            &source.manifest,
            replay,
            &order,
            slot,
            &source.schema(),
            None,
            analyze,
        )?;
        Ok((image, rows))
    }
}
