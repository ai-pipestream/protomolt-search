//! A hash split that seals bucket-sized segments as it replays.

use std::collections::BTreeSet;

use super::child_build::{finish_child, ChildSlot};
#[cfg(doc)]
use super::hash_split::split_logs_with;
use super::hash_split::SplitRequest;
use super::parent_routing::{extend_lineage_keys, parent_child, parent_routes, LineageKeys};
use super::replay::{replay_buckets, replay_buckets_documents, Replay};
use super::split_inputs::{create_out_dir, distribute, hash_range, SplitInputs};
use super::{Analyzer, SegmentedChildImage, SegmentedReshardOutput};

/// Repartition full-history WALs into immutable, bucket-sized segments.
///
/// This is the spillable counterpart of [`split_logs_with`]. The ordinary path
/// accumulates every row of one output child before encoding it; this path
/// replays one WAL bucket across all input generations, applies its
/// deletes/replacements, and immediately seals each non-empty output segment.
/// WAL writers emit one vector/document row per record; replay rejects a
/// foreign batch that straddles buckets. A logical shard can contain many
/// physical segments, merged exactly by [`crate::segments::OpenedSegmentSet`].
/// WAL bucket count controls the replay bound without changing query semantics.
pub fn split_logs_segmented_with(
    request: &SplitRequest<'_>,
    analyze: &mut Analyzer,
) -> Result<SegmentedReshardOutput, String> {
    request.require_power_of_two("segmented split")?;
    if request.slot_stride == 0 {
        return Err("segmented split slot_stride must be positive".into());
    }
    let inputs = SplitInputs::check(request.gens, "segmented split", true)?;
    create_out_dir(request.out_dir)?;
    // Chunks of one parent can sit in several WAL buckets, so the
    // shared-key verdict needs every bucket before any segment seals.
    // The pre-scan reads documents only, never the FP32 rows.
    let mut lineage = LineageKeys::new();
    if !request.vectors_only {
        let (count, dim) = (
            inputs.manifest.bucket_count as usize,
            inputs.manifest.dim as usize,
        );
        for bucket in 0..inputs.manifest.bucket_count {
            let mut scan = Replay::default();
            for gen in request.gens {
                replay_buckets_documents(gen, bucket..bucket + 1, count, dim, &mut scan)?;
            }
            scan.compact();
            extend_lineage_keys(&mut lineage, &scan);
        }
    }
    let mut sealer = SegmentSealer::new(request, &inputs)?;
    sealer.lineage = lineage;
    for bucket in 0..inputs.manifest.bucket_count {
        let mut replay = Replay::default();
        for gen in request.gens {
            let manifest = &inputs.manifest;
            let (count, dim) = (manifest.bucket_count as usize, manifest.dim as usize);
            replay_buckets(
                gen,
                bucket..bucket + 1,
                count,
                dim,
                request.vectors_only,
                &mut replay,
            )?;
        }
        replay.compact();
        sealer.seal_bucket(replay, analyze)?;
    }
    Ok(SegmentedReshardOutput {
        generation: inputs
            .top_generation
            .checked_add(1)
            .ok_or_else(|| "segmented reshard generation overflow".to_string())?,
        logical_shards: request.n,
        segments: sealer.segments,
        peak_replay_rows: sealer.peak_replay_rows,
    })
}

/// The segments of a segmented split as they seal, bucket by bucket.
struct SegmentSealer<'a> {
    request: &'a SplitRequest<'a>,
    inputs: &'a SplitInputs,
    /// The next free slot of each logical shard.
    next_slots: Vec<u64>,
    segment_counts: Vec<usize>,
    segments: Vec<SegmentedChildImage>,
    peak_replay_rows: u64,
    lineage: LineageKeys,
}

impl<'a> SegmentSealer<'a> {
    fn new(request: &'a SplitRequest<'a>, inputs: &'a SplitInputs) -> Result<Self, String> {
        let next_slots = (0..request.n)
            .map(|shard| {
                let shard = u64::try_from(shard)
                    .map_err(|_| "segmented split shard index does not fit u64".to_string())?;
                shard
                    .checked_mul(request.slot_stride)
                    .and_then(|delta| request.slot_base.checked_add(delta))
                    .ok_or_else(|| "segmented split slot base overflow".to_string())
            })
            .collect::<Result<_, String>>()?;
        Ok(Self {
            request,
            inputs,
            next_slots,
            segment_counts: vec![0; request.n],
            segments: Vec::new(),
            peak_replay_rows: 0,
            lineage: LineageKeys::new(),
        })
    }

    /// Seal one bucket's live rows as one segment per logical shard that
    /// holds any of them.
    fn seal_bucket(&mut self, replay: Replay, analyze: &mut Analyzer) -> Result<(), String> {
        let live: BTreeSet<u64> = replay
            .vectors
            .keys()
            .chain(replay.documents.keys())
            .copied()
            .collect();
        self.peak_replay_rows = self.peak_replay_rows.max(live.len() as u64);
        if live.is_empty() {
            return Ok(());
        }
        let n = self.request.n;
        let routes = parent_routes(&replay);
        let divergent = std::cell::RefCell::new(vec![false; n]);
        let partitions = distribute(replay, n, |id, key| {
            let (child, keyed) = parent_child(&routes, &self.lineage, id, key, n);
            if super::parent_routing::route_of(&routes, id).is_lineage() && !keyed {
                divergent.borrow_mut()[child] = true;
            }
            Ok(child)
        })?;
        for ((logical_shard, partition), divergent) in partitions
            .into_iter()
            .enumerate()
            .zip(divergent.into_inner())
        {
            self.seal_partition(logical_shard, partition, divergent, analyze)?;
        }
        Ok(())
    }

    fn seal_partition(
        &mut self,
        logical_shard: usize,
        partition: Replay,
        divergent: bool,
        analyze: &mut Analyzer,
    ) -> Result<(), String> {
        let ids: BTreeSet<u64> = partition
            .vectors
            .keys()
            .chain(partition.documents.keys())
            .copied()
            .collect();
        let physical_rows = ids.len() as u64;
        if physical_rows == 0 {
            return Ok(());
        }
        let end = self.reserve(logical_shard, physical_rows)?;
        let (hash_lo, hash_hi) = hash_range(logical_shard, self.request.n);
        let slot = ChildSlot {
            ordinal: self.segments.len(),
            out_dir: self.request.out_dir,
            slot_offset: self.next_slots[logical_shard],
            hash_lo,
            hash_hi,
        };
        let schema = self.inputs.schema(self.request.bm25_fields);
        let mut image = finish_child(&self.inputs.manifest, partition, slot, &schema, analyze)?;
        if divergent {
            image.parents_colocated = false;
        }
        self.segments.push(SegmentedChildImage {
            logical_shard,
            segment_ordinal: self.segment_counts[logical_shard],
            physical_rows,
            image,
        });
        self.segment_counts[logical_shard] += 1;
        self.next_slots[logical_shard] = end;
        Ok(())
    }

    /// The end of the slot range `rows` more rows take in a logical
    /// shard, refused past the shard's reserved stride.
    fn reserve(&self, logical_shard: usize, rows: u64) -> Result<u64, String> {
        let request = self.request;
        let logical_end = u64::try_from(logical_shard)
            .ok()
            .and_then(|ordinal| ordinal.checked_add(1))
            .and_then(|ordinal| ordinal.checked_mul(request.slot_stride))
            .and_then(|delta| request.slot_base.checked_add(delta))
            .ok_or_else(|| "segmented split slot range overflow".to_string())?;
        let end = self.next_slots[logical_shard]
            .checked_add(rows)
            .ok_or_else(|| "segmented split physical row range overflow".to_string())?;
        if end > logical_end {
            return Err(format!(
                "segmented child {logical_shard} needs stable ids through {end}, above its \
                 reserved slot range ending at {logical_end}; raise slot_stride"
            ));
        }
        Ok(end)
    }
}
