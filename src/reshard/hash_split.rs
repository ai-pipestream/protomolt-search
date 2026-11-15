//! Hash-routed split and merge over WAL buckets.

use std::collections::BTreeSet;
use std::path::{Path, PathBuf};

use crate::wal;

use super::child_build::finish_child;
use super::merge_inputs::load_merge_manifests;
use super::replay::{
    read_gen_manifest, read_gens_binding, replay_buckets, require_backend_config,
    require_complete_history, same_backend_config, Replay,
};
use super::{
    bucket_of, Analyzer, ReshardOutput, SegmentedChildImage, SegmentedReshardOutput,
    StableReshardOutput, WalCutoff,
};

/// Rewrite one or more WAL generations into `n` child images. `n = 1` is
/// compaction; larger powers of two repartition.
///
/// With `n <= bucket_count` and `bucket_count % n == 0` this is the cheap
/// path: child `i` owns the contiguous bucket range
/// `[i*bucket_count/n, (i+1)*bucket_count/n)`, replays only those files,
/// and its hash range is exactly those buckets. A finer split
/// (`n > bucket_count`) re-partitions every record by
/// [`bucket_of`]`(id, n)` — correct but slower; the WAL bucket count
/// caps cheap split granularity. Either way children get
/// `slot_offset = slot_base + i * slot_stride` (stride defaults to 25M
/// in the example, matching deploy/court-e2e) and dense local slots in
/// original id order.
#[allow(clippy::too_many_arguments)]
pub fn split(
    gen: &Path,
    n: usize,
    out_dir: &Path,
    slot_base: u64,
    slot_stride: u64,
    vectors_only: bool,
    bm25_fields: Option<&[String]>,
    analyze: &mut Analyzer,
) -> Result<ReshardOutput, String> {
    split_logs(
        std::slice::from_ref(&gen.to_path_buf()),
        n,
        out_dir,
        slot_base,
        slot_stride,
        vectors_only,
        bm25_fields,
        analyze,
    )
}

/// Split ANY number of shards' WAL generations into `n` child images —
/// the general N -> M reshard. One input log is a plain split; several
/// input logs redistribute their union across the children with no
/// intermediate merge artifact: child `i`'s input is its bucket range
/// read from EVERY input generation, and global ids from different
/// shards never collide (disjoint slot ranges), so the per-child replay
/// map assembles the union in id order for free.
///
/// All inputs must carry byte-identical provider configurations and one bucket
/// geometry, and be full-history — the same preconditions as
/// [`merge`], enforced the same way.
#[allow(clippy::too_many_arguments)]
pub fn split_logs(
    gens: &[PathBuf],
    n: usize,
    out_dir: &Path,
    slot_base: u64,
    slot_stride: u64,
    vectors_only: bool,
    bm25_fields: Option<&[String]>,
    analyze: &mut Analyzer,
) -> Result<ReshardOutput, String> {
    if !n.is_power_of_two() {
        return Err(format!(
            "split factor must be a positive power of two, got {n}"
        ));
    }
    let Some(first_gen) = gens.first() else {
        return Err("split requires at least one input generation".to_string());
    };
    let manifest = read_gen_manifest(first_gen)?;
    let mut top_generation = manifest.generation;
    for gen in gens {
        let m = read_gen_manifest(gen)?;
        require_backend_config(&m, gen)?;
        require_complete_history(&m, gen)?;
        if !same_backend_config(&manifest, &m) {
            return Err(format!(
                "{}: vector backend configuration differs from the first input; \
                 split/merge requires one scoring configuration",
                gen.display()
            ));
        }
        if m.bucket_count != manifest.bucket_count {
            return Err(format!(
                "{}: bucket count {} differs from the first input's {}; split inputs \
                 must share one bucket geometry",
                gen.display(),
                m.bucket_count,
                manifest.bucket_count
            ));
        }
        top_generation = top_generation.max(m.generation);
    }
    let binding = read_gens_binding(gens)?;
    std::fs::create_dir_all(out_dir).map_err(|e| format!("mkdir {}: {e}", out_dir.display()))?;

    let bucket_count = manifest.bucket_count as usize;
    let bucket_bits = manifest.bucket_count.trailing_zeros();
    let mut children = Vec::with_capacity(n);

    if n <= bucket_count && bucket_count.is_multiple_of(n) {
        // Cheap path: whole bucket ranges, no re-hashing. Memory is
        // bounded per child: only its bucket range is resident, read
        // from each input in turn.
        let per_child = bucket_count / n;
        for i in 0..n {
            let first = (i * per_child) as u32;
            let mut replay = Replay::default();
            for gen in gens {
                replay_buckets(
                    gen,
                    first..first + per_child as u32,
                    bucket_count,
                    manifest.dim as usize,
                    vectors_only,
                    &mut replay,
                )?;
            }
            let hash_lo = u64::from(first) << (64 - bucket_bits);
            let hash_hi = if i + 1 == n {
                u64::MAX
            } else {
                ((first as u64 + per_child as u64) << (64 - bucket_bits)) - 1
            };
            children.push(finish_child(
                &manifest,
                replay,
                i,
                out_dir,
                slot_base + i as u64 * slot_stride,
                hash_lo,
                hash_hi,
                bm25_fields,
                binding.as_ref(),
                analyze,
            )?);
        }
    } else {
        eprintln!(
            "reshard: WAL bucket count {bucket_count} caps cheap splits at {bucket_count} \
             children; re-partitioning every record for split={n}"
        );
        let mut replay = Replay::default();
        for gen in gens {
            replay_buckets(
                gen,
                0..manifest.bucket_count,
                bucket_count,
                manifest.dim as usize,
                vectors_only,
                &mut replay,
            )?;
        }
        replay.compact();
        let shift = 64 - n.trailing_zeros();
        let mut buckets: Vec<Replay> = (0..n).map(|_| Replay::default()).collect();
        for (id, vector) in replay.vectors {
            let shard = bucket_of(id, n);
            buckets[shard].vectors.insert(id, vector);
            if let Some(row) = replay.retained.remove(&id) {
                buckets[shard].retained.insert(id, row);
            }
        }
        for (id, doc) in replay.documents {
            buckets[bucket_of(id, n)].documents.insert(id, doc);
        }
        for (id, key) in replay.stable_keys {
            buckets[bucket_of(id, n)].stable_keys.insert(id, key);
        }
        for (i, replay) in buckets.into_iter().enumerate() {
            let hash_lo = (i as u64) << shift;
            let hash_hi = if i + 1 == n {
                u64::MAX
            } else {
                ((i as u64 + 1) << shift) - 1
            };
            children.push(finish_child(
                &manifest,
                replay,
                i,
                out_dir,
                slot_base + i as u64 * slot_stride,
                hash_lo,
                hash_hi,
                bm25_fields,
                binding.as_ref(),
                analyze,
            )?);
        }
    }
    Ok(ReshardOutput {
        generation: top_generation + 1,
        children,
    })
}

/// Repartition full-history WALs into immutable, bucket-sized segments.
///
/// This is the spillable counterpart of [`split_logs`]. The ordinary path
/// accumulates every row of one output child before encoding it; this path
/// replays one WAL bucket across all input generations, applies its
/// deletes/replacements, and immediately seals each non-empty output segment.
/// WAL writers emit one vector/document row per record; replay rejects a
/// foreign batch that straddles buckets. A logical shard can contain many
/// physical segments, merged exactly by [`crate::segments::OpenedSegmentSet`].
/// WAL bucket count controls the replay bound without changing query semantics.
#[allow(clippy::too_many_arguments)]
pub fn split_logs_segmented(
    gens: &[PathBuf],
    n: usize,
    out_dir: &Path,
    slot_base: u64,
    slot_stride: u64,
    vectors_only: bool,
    bm25_fields: Option<&[String]>,
    analyze: &mut Analyzer,
) -> Result<SegmentedReshardOutput, String> {
    if !n.is_power_of_two() {
        return Err(format!(
            "segmented split factor must be a positive power of two, got {n}"
        ));
    }
    if slot_stride == 0 {
        return Err("segmented split slot_stride must be positive".into());
    }
    let Some(first_gen) = gens.first() else {
        return Err("segmented split requires at least one input generation".into());
    };
    let manifest = read_gen_manifest(first_gen)?;
    require_backend_config(&manifest, first_gen)?;
    require_complete_history(&manifest, first_gen)?;
    let mut top_generation = manifest.generation;
    for gen in gens.iter().skip(1) {
        let current = read_gen_manifest(gen)?;
        require_backend_config(&current, gen)?;
        require_complete_history(&current, gen)?;
        if !same_backend_config(&manifest, &current) {
            return Err(format!(
                "{}: vector backend configuration differs from the first input",
                gen.display()
            ));
        }
        if current.bucket_count != manifest.bucket_count {
            return Err(format!(
                "{}: bucket count {} differs from the first input's {}",
                gen.display(),
                current.bucket_count,
                manifest.bucket_count
            ));
        }
        top_generation = top_generation.max(current.generation);
    }
    let binding = read_gens_binding(gens)?;
    std::fs::create_dir_all(out_dir)
        .map_err(|error| format!("mkdir {}: {error}", out_dir.display()))?;
    let mut next_slots: Vec<u64> = (0..n)
        .map(|shard| {
            let shard = u64::try_from(shard)
                .map_err(|_| "segmented split shard index does not fit u64".to_string())?;
            let delta = shard
                .checked_mul(slot_stride)
                .ok_or_else(|| "segmented split slot base overflow".to_string())?;
            slot_base
                .checked_add(delta)
                .ok_or_else(|| "segmented split slot base overflow".to_string())
        })
        .collect::<Result<_, _>>()?;
    let mut segment_counts = vec![0usize; n];
    let mut segments = Vec::new();
    let mut peak_replay_rows = 0u64;

    for bucket in 0..manifest.bucket_count {
        let mut replay = Replay::default();
        for gen in gens {
            replay_buckets(
                gen,
                bucket..bucket + 1,
                manifest.bucket_count as usize,
                manifest.dim as usize,
                vectors_only,
                &mut replay,
            )?;
        }
        replay.compact();
        let live_ids: BTreeSet<u64> = replay
            .vectors
            .keys()
            .chain(replay.documents.keys())
            .copied()
            .collect();
        peak_replay_rows = peak_replay_rows.max(live_ids.len() as u64);
        if live_ids.is_empty() {
            continue;
        }
        let stable_keys = replay.stable_keys;
        let mut partitions: Vec<Replay> = (0..n).map(|_| Replay::default()).collect();
        for (id, vector) in replay.vectors {
            let shard = bucket_of(id, n);
            partitions[shard].vectors.insert(id, vector);
            if let Some(row) = replay.retained.remove(&id) {
                partitions[shard].retained.insert(id, row);
            }
            if let Some(key) = stable_keys.get(&id) {
                partitions[shard].stable_keys.insert(id, key.clone());
            }
        }
        for (id, document) in replay.documents {
            let shard = bucket_of(id, n);
            partitions[shard].documents.insert(id, document);
            if let Some(key) = stable_keys.get(&id) {
                partitions[shard].stable_keys.insert(id, key.clone());
            }
        }
        for (logical_shard, partition) in partitions.into_iter().enumerate() {
            let ids: BTreeSet<u64> = partition
                .vectors
                .keys()
                .chain(partition.documents.keys())
                .copied()
                .collect();
            let physical_rows = ids.len() as u64;
            if physical_rows == 0 {
                continue;
            }
            let logical_ordinal = u64::try_from(logical_shard)
                .map_err(|_| "segmented split shard index does not fit u64")?;
            let logical_end = logical_ordinal
                .checked_add(1)
                .and_then(|ordinal| ordinal.checked_mul(slot_stride))
                .and_then(|delta| slot_base.checked_add(delta))
                .ok_or_else(|| "segmented split slot range overflow".to_string())?;
            let end = next_slots[logical_shard]
                .checked_add(physical_rows)
                .ok_or_else(|| "segmented split physical row range overflow".to_string())?;
            if end > logical_end {
                return Err(format!(
                    "segmented child {logical_shard} needs stable ids through {end}, above its reserved slot range ending at {logical_end}; raise slot_stride"
                ));
            }
            let shift = if n == 1 { 0 } else { 64 - n.trailing_zeros() };
            let hash_lo = if n == 1 { 0 } else { logical_ordinal << shift };
            let hash_hi = if logical_shard + 1 == n {
                u64::MAX
            } else {
                ((logical_ordinal + 1) << shift) - 1
            };
            let ordinal = segments.len();
            let image = finish_child(
                &manifest,
                partition,
                ordinal,
                out_dir,
                next_slots[logical_shard],
                hash_lo,
                hash_hi,
                bm25_fields,
                binding.as_ref(),
                analyze,
            )?;
            segments.push(SegmentedChildImage {
                logical_shard,
                segment_ordinal: segment_counts[logical_shard],
                physical_rows,
                image,
            });
            segment_counts[logical_shard] += 1;
            next_slots[logical_shard] = end;
        }
    }
    Ok(SegmentedReshardOutput {
        generation: top_generation
            .checked_add(1)
            .ok_or_else(|| "segmented reshard generation overflow".to_string())?,
        logical_shards: n,
        segments,
        peak_replay_rows,
    })
}

/// Repartition full-history, generation-clocked WALs by their opaque stable
/// product keys. This is the baseline-build half of a hitless reshard: it
/// returns the exact source high watermarks included in the child images, so
/// the live tail can resume without a gap before an atomic map publication.
#[allow(clippy::too_many_arguments)]
pub fn split_stable_logs(
    gens: &[PathBuf],
    n: usize,
    out_dir: &Path,
    slot_base: u64,
    slot_stride: u64,
    vectors_only: bool,
    bm25_fields: Option<&[String]>,
    analyze: &mut Analyzer,
) -> Result<StableReshardOutput, String> {
    if !n.is_power_of_two() {
        return Err(format!(
            "stable split factor must be a positive power of two, got {n}"
        ));
    }
    let Some(first_gen) = gens.first() else {
        return Err("stable split requires at least one input generation".to_string());
    };
    let manifest = read_gen_manifest(first_gen)?;
    let mut top_generation = manifest.generation;
    let mut cutoffs = Vec::with_capacity(gens.len());
    let mut replay = Replay::default();
    for gen in gens {
        let current = read_gen_manifest(gen)?;
        require_backend_config(&current, gen)?;
        require_complete_history(&current, gen)?;
        if !same_backend_config(&manifest, &current) {
            return Err(format!(
                "{}: vector backend configuration differs from the first input",
                gen.display()
            ));
        }
        let clocked = wal::read_clocked_records(gen, 0)
            .map_err(|error| format!("clocked replay {}: {error}", gen.display()))?;
        cutoffs.push(WalCutoff {
            generation: current.generation,
            high_watermark: clocked.last().map_or(0, |record| record.clock),
        });
        replay_buckets(
            gen,
            0..current.bucket_count,
            current.bucket_count as usize,
            current.dim as usize,
            vectors_only,
            &mut replay,
        )?;
        top_generation = top_generation.max(current.generation);
    }
    replay.compact();
    for id in replay.vectors.keys().chain(replay.documents.keys()) {
        if replay.stable_keys.get(id).is_none_or(|key| key.is_empty()) {
            return Err(format!(
                "stable split requires a routed stable key for live source id {id}; rebuild legacy explicitly addressed rows before live reshard"
            ));
        }
    }
    let binding = read_gens_binding(gens)?;
    std::fs::create_dir_all(out_dir)
        .map_err(|error| format!("mkdir {}: {error}", out_dir.display()))?;
    let shift = 64 - n.trailing_zeros();
    let mut buckets: Vec<Replay> = (0..n).map(|_| Replay::default()).collect();
    let keys = replay.stable_keys;
    for (id, vector) in replay.vectors {
        let key = keys.get(&id).expect("validated above");
        let shard = if n == 1 {
            0
        } else {
            (crate::coordinator::stable_routing_hash(key) >> shift) as usize
        };
        buckets[shard].vectors.insert(id, vector);
        if let Some(row) = replay.retained.remove(&id) {
            buckets[shard].retained.insert(id, row);
        }
        buckets[shard].stable_keys.insert(id, key.clone());
    }
    for (id, document) in replay.documents {
        let key = keys.get(&id).expect("validated above");
        let shard = if n == 1 {
            0
        } else {
            (crate::coordinator::stable_routing_hash(key) >> shift) as usize
        };
        buckets[shard].documents.insert(id, document);
        buckets[shard].stable_keys.insert(id, key.clone());
    }
    let mut children = Vec::with_capacity(n);
    for (shard, replay) in buckets.into_iter().enumerate() {
        let hash_lo = if n == 1 { 0 } else { (shard as u64) << shift };
        let hash_hi = if shard + 1 == n {
            u64::MAX
        } else {
            ((shard as u64 + 1) << shift) - 1
        };
        children.push(finish_child(
            &manifest,
            replay,
            shard,
            out_dir,
            slot_base + shard as u64 * slot_stride,
            hash_lo,
            hash_hi,
            bm25_fields,
            binding.as_ref(),
            analyze,
        )?);
    }
    Ok(StableReshardOutput {
        images: ReshardOutput {
            generation: top_generation + 1,
            children,
        },
        source_cutoffs: cutoffs,
    })
}

/// Repartition full-history, generation-clocked WALs by their stable
/// product keys into children covering the given hash ranges
/// (`docs/cluster-control.md`, "Shard split"): child `i` receives the
/// rows whose key hash falls in `ranges[i]` and takes `slot_offsets[i]`.
/// The ranges must tile the source's range without a gap; a live row
/// whose hash falls outside every range refuses by name, since the
/// children would not conserve the source. Legacy rows without a stable
/// key refuse as [`split_stable_logs`] does.
#[allow(clippy::too_many_arguments)]
pub fn split_stable_logs_ranged(
    gens: &[PathBuf],
    ranges: &[(u64, u64)],
    out_dir: &Path,
    slot_offsets: &[u64],
    vectors_only: bool,
    bm25_fields: Option<&[String]>,
    analyze: &mut Analyzer,
) -> Result<StableReshardOutput, String> {
    if ranges.len() < 2 {
        return Err("a ranged split needs at least two child ranges".to_string());
    }
    if slot_offsets.len() != ranges.len() {
        return Err(format!(
            "a ranged split needs one slot offset per child: {} offsets for {} ranges",
            slot_offsets.len(),
            ranges.len()
        ));
    }
    for window in ranges.windows(2) {
        let (_, hi) = window[0];
        let (lo, _) = window[1];
        if hi.checked_add(1) != Some(lo) {
            return Err(format!(
                "child ranges must be adjacent and ascending: ..={hi} is followed by {lo}.."
            ));
        }
    }
    if ranges.iter().any(|(lo, hi)| lo > hi) {
        return Err("a child range is inverted".to_string());
    }
    let Some(first_gen) = gens.first() else {
        return Err("a ranged split requires at least one input generation".to_string());
    };
    let manifest = read_gen_manifest(first_gen)?;
    let mut top_generation = manifest.generation;
    let mut cutoffs = Vec::with_capacity(gens.len());
    let mut replay = Replay::default();
    for gen in gens {
        let current = read_gen_manifest(gen)?;
        require_backend_config(&current, gen)?;
        require_complete_history(&current, gen)?;
        if !same_backend_config(&manifest, &current) {
            return Err(format!(
                "{}: vector backend configuration differs from the first input",
                gen.display()
            ));
        }
        let clocked = wal::read_clocked_records(gen, 0)
            .map_err(|error| format!("clocked replay {}: {error}", gen.display()))?;
        cutoffs.push(WalCutoff {
            generation: current.generation,
            high_watermark: clocked.last().map_or(0, |record| record.clock),
        });
        replay_buckets(
            gen,
            0..current.bucket_count,
            current.bucket_count as usize,
            current.dim as usize,
            vectors_only,
            &mut replay,
        )?;
        top_generation = top_generation.max(current.generation);
    }
    replay.compact();
    for id in replay.vectors.keys().chain(replay.documents.keys()) {
        if replay.stable_keys.get(id).is_none_or(|key| key.is_empty()) {
            return Err(format!(
                "stable split requires a routed stable key for live source id {id}; rebuild \
                 legacy explicitly addressed rows before a live split"
            ));
        }
    }
    let binding = read_gens_binding(gens)?;
    std::fs::create_dir_all(out_dir)
        .map_err(|error| format!("mkdir {}: {error}", out_dir.display()))?;
    let child_of = |key: &[u8]| -> Result<usize, String> {
        let hash = crate::coordinator::stable_routing_hash(key);
        ranges
            .iter()
            .position(|(lo, hi)| hash >= *lo && hash <= *hi)
            .ok_or_else(|| {
                format!(
                    "a live row's key hashes to {hash}, outside every child range of this \
                     split; the source holds rows the split's range does not cover"
                )
            })
    };
    let mut buckets: Vec<Replay> = ranges.iter().map(|_| Replay::default()).collect();
    let keys = replay.stable_keys;
    for (id, vector) in replay.vectors {
        let key = keys.get(&id).expect("validated above");
        let shard = child_of(key)?;
        buckets[shard].vectors.insert(id, vector);
        if let Some(row) = replay.retained.remove(&id) {
            buckets[shard].retained.insert(id, row);
        }
        buckets[shard].stable_keys.insert(id, key.clone());
    }
    for (id, document) in replay.documents {
        let key = keys.get(&id).expect("validated above");
        let shard = child_of(key)?;
        buckets[shard].documents.insert(id, document);
        buckets[shard].stable_keys.insert(id, key.clone());
    }
    let mut children = Vec::with_capacity(ranges.len());
    for (shard, replay) in buckets.into_iter().enumerate() {
        let (hash_lo, hash_hi) = ranges[shard];
        children.push(finish_child(
            &manifest,
            replay,
            shard,
            out_dir,
            slot_offsets[shard],
            hash_lo,
            hash_hi,
            bm25_fields,
            binding.as_ref(),
            analyze,
        )?);
    }
    Ok(StableReshardOutput {
        images: ReshardOutput {
            generation: top_generation + 1,
            children,
        },
        source_cutoffs: cutoffs,
    })
}

/// Merge several shards' WAL generations into ONE image. All inputs must
/// carry byte-identical provider configurations AND identical bucket counts
/// (split/merge only within one scoring configuration and bucket geometry);
/// records replay bucket-by-bucket in global id order across all inputs.
/// The child's slot base defaults to the lowest input slot offset. Its
/// hash range is the full range: the merge inputs' ranges are not
/// recorded in the WAL manifest, and full-range is the safe default for
/// the shard map.
pub fn merge(
    gens: &[PathBuf],
    out_dir: &Path,
    slot_base: Option<u64>,
    vectors_only: bool,
    bm25_fields: Option<&[String]>,
    analyze: &mut Analyzer,
) -> Result<ReshardOutput, String> {
    if gens.is_empty() {
        return Err("merge requires at least one --logs generation".to_string());
    }
    let manifests = load_merge_manifests(gens)?;
    let bucket_count = manifests[0].bucket_count;
    let dim = manifests[0].dim as usize;
    let binding = read_gens_binding(gens)?;
    let max_generation = manifests.iter().map(|m| m.generation).max().unwrap_or(0);
    let min_slot_offset = manifests.iter().map(|m| m.slot_offset).min().unwrap_or(0);

    // Bucket-major replay: one bucket index at a time, across all inputs.
    let mut merged = Replay::default();
    for bucket in 0..bucket_count {
        for gen in gens {
            replay_buckets(
                gen,
                bucket..bucket + 1,
                bucket_count as usize,
                dim,
                vectors_only,
                &mut merged,
            )?;
        }
    }
    std::fs::create_dir_all(out_dir).map_err(|e| format!("mkdir {}: {e}", out_dir.display()))?;
    eprintln!(
        "reshard: merge of {} generations: {} vectors, {} documents",
        gens.len(),
        merged.vectors.len(),
        merged.documents.len()
    );
    let child = finish_child(
        &manifests[0],
        merged,
        0,
        out_dir,
        slot_base.unwrap_or(min_slot_offset),
        0,
        u64::MAX,
        bm25_fields,
        binding.as_ref(),
        analyze,
    )?;
    Ok(ReshardOutput {
        generation: max_generation + 1,
        children: vec![child],
    })
}
