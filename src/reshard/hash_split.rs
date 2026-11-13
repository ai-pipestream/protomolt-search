use super::placement_split::finish_child;
use super::*;

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
        type ReshardBucket = (Vec<(u64, Vec<f32>)>, Vec<(u64, AddDocumentsRequest)>);
        let mut buckets: Vec<ReshardBucket> = (0..n).map(|_| (Vec::new(), Vec::new())).collect();
        for (id, vector) in replay.vectors {
            buckets[bucket_of(id, n)].0.push((id, vector));
        }
        for (id, doc) in replay.documents {
            buckets[bucket_of(id, n)].1.push((id, doc));
        }
        for (i, (vectors, documents)) in buckets.into_iter().enumerate() {
            let hash_lo = (i as u64) << shift;
            let hash_hi = if i + 1 == n {
                u64::MAX
            } else {
                ((i as u64 + 1) << shift) - 1
            };
            children.push(finish_child(
                &manifest,
                Replay {
                    vectors: vectors.into_iter().collect(),
                    documents: documents.into_iter().collect(),
                    deleted: Default::default(),
                    stable_keys: Default::default(),
                    analyzed: Default::default(),
                },
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
/// The field and column tables a sealed store declares, in index order.
pub(crate) fn segment_tables(bm25: &crate::postings::Bm25Reader) -> SourceTables {
    let fields: Vec<String> = (0..bm25.field_count())
        .map(|f| bm25.field_name(f).to_string())
        .collect();
    let fingerprints: Vec<u64> = (0..bm25.field_count())
        .map(|f| bm25.analysis_fingerprint(f))
        .collect();
    let position_fields: Vec<String> = (0..bm25.field_count())
        .filter(|&f| bm25.field_has_positions(f))
        .map(|f| bm25.field_name(f).to_string())
        .collect();
    let sentence_fields: Vec<String> = (0..bm25.field_count())
        .filter(|&f| bm25.field_has_sentences(f))
        .map(|f| bm25.field_name(f).to_string())
        .collect();
    let columns = ColumnTables {
        facets: (0..bm25.facet_count())
            .map(|c| bm25.facet_name(c).to_string())
            .collect(),
        numerics: (0..bm25.numeric_count())
            .map(|c| bm25.numeric_name(c).to_string())
            .collect(),
        map_facets: (0..bm25.map_facet_count())
            .map(|c| bm25.map_facet_name(c).to_string())
            .collect(),
        map_numerics: (0..bm25.map_numeric_count())
            .map(|c| bm25.map_numeric_name(c).to_string())
            .collect(),
        map_integers: (0..bm25.map_integer_count())
            .map(|c| bm25.map_integer_name(c).to_string())
            .collect(),
        map_unsigned_integers: (0..bm25.map_unsigned_integer_count())
            .map(|c| bm25.map_unsigned_integer_name(c).to_string())
            .collect(),
        integers: (0..bm25.integer_count())
            .map(|c| bm25.integer_name(c).to_string())
            .collect(),
        unsigned_integers: (0..bm25.unsigned_integer_count())
            .map(|c| bm25.unsigned_integer_name(c).to_string())
            .collect(),
        geo: (0..bm25.geo_count())
            .map(|c| bm25.geo_name(c).to_string())
            .collect(),
    };
    SourceTables {
        fields,
        fingerprints,
        position_fields,
        sentence_fields,
        columns,
    }
}

/// The catalog root of the shard whose log generation is `gen`:
/// `<index>.tv.wal/gen-N` -> `<index>.tv.segments`.
pub(super) fn segments_root_of_gen(gen: &Path) -> Result<PathBuf, String> {
    let wal_dir = gen.parent().ok_or_else(|| {
        format!(
            "{}: a generation directory has a log directory above it",
            gen.display()
        )
    })?;
    let name = wal_dir
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| format!("{}: log directory has no name", wal_dir.display()))?;
    let Some(index) = name.strip_suffix(".wal") else {
        return Err(format!(
            "{}: the log directory is not named <index>.wal, so the segment catalog beside it \
             cannot be found; split from the logs instead",
            wal_dir.display()
        ));
    };
    let index_path = wal_dir.with_file_name(index);
    Ok(crate::node::segments_root(&index_path))
}

/// `<index>.tv` of a catalog root `<index>.tv.segments`.
pub(super) fn index_path_of_root(root: &Path) -> PathBuf {
    let name = root
        .file_name()
        .and_then(|n| n.to_str())
        .and_then(|n| n.strip_suffix(".segments"))
        .unwrap_or_default();
    root.with_file_name(name)
}

/// The highest row the log has appended plus one, over its bucket
/// files, and the rows its records delete or replace.
pub(super) fn wal_row_watermark(
    gen: &Path,
    bucket_count: u32,
) -> Result<(u64, BTreeSet<u64>), String> {
    let mut watermark = 0u64;
    let mut deleted = BTreeSet::new();
    for bucket in 0..bucket_count {
        let path = wal::bucket_path(gen, bucket);
        if !path.exists() {
            continue;
        }
        let mut reader =
            RecordReader::open(&path).map_err(|e| format!("open {}: {e}", path.display()))?;
        while let Some(record) = reader
            .next_record()
            .map_err(|e| format!("read {}: {e}", path.display()))?
        {
            let end = match record.op {
                Some(wal_record::Op::AddVectors(a)) => {
                    let rows = a.batch.as_ref().map_or(0, |b| {
                        if b.dim == 0 {
                            0
                        } else {
                            (b.vectors.len() / b.dim as usize) as u64
                        }
                    });
                    Some(a.first_id.saturating_add(rows))
                }
                Some(wal_record::Op::AddDocuments(a)) => {
                    Some(a.first_id.saturating_add(a.documents.len() as u64))
                }
                Some(wal_record::Op::DeleteDocument(d)) => {
                    deleted.insert(d.doc_id);
                    None
                }
                Some(wal_record::Op::Replacement(r)) => {
                    deleted.insert(r.old_doc_id);
                    None
                }
                _ => None,
            };
            if let Some(end) = end {
                watermark = watermark.max(end);
            }
        }
    }
    Ok((watermark, deleted))
}

/// Open the catalogs beside the logs and check what a child pins
/// (`docs/replay-from-segments.md`, "The replay").
pub(crate) fn open_segment_sources(
    gens: &[PathBuf],
) -> Result<(Vec<SegmentSourceShard>, SourceTables), String> {
    let mut shards = Vec::with_capacity(gens.len());
    let mut tables: Option<SourceTables> = None;
    for gen in gens {
        let root = segments_root_of_gen(gen)?;
        if !root.exists() {
            return Err(format!(
                "{}: no segment catalog at {}; the source is a single image (or was never \
                 flushed on the segment layout), split it from the logs",
                gen.display(),
                root.display()
            ));
        }
        let set = std::sync::Arc::new(crate::segments::OpenedSegmentSet::open(&root)?);
        let manifest = read_gen_manifest(gen)?;
        let mut rows = 0u64;
        for i in 0..set.len() {
            let meta = set.metadata(i);
            if meta.base_label != rows {
                return Err(format!(
                    "{}: segment {} starts at label {} but the segments before it end at {rows}",
                    root.display(),
                    meta.segment_id,
                    meta.base_label
                ));
            }
            rows = meta.end_label_exclusive()?;
        }
        let (watermark, deleted) = wal_row_watermark(gen, manifest.bucket_count)?;
        // The log's ids are global (the shard's slot offset plus the
        // local row); the catalog's labels are local. Compare in local
        // terms, and name a log whose ids sit below its own offset.
        let local_watermark = if watermark == 0 {
            0
        } else {
            watermark.checked_sub(manifest.slot_offset).ok_or_else(|| {
                format!(
                    "{}: the log holds rows through {} but its manifest places the shard at slot \
                     offset {}; the log and its manifest disagree",
                    gen.display(),
                    watermark - 1,
                    manifest.slot_offset
                )
            })?
        };
        if local_watermark > rows {
            return Err(format!(
                "{}: the log holds rows through {} (local row {}) but the catalog at {} ends at \
                 {rows}; the unsealed tail would be lost, flush the source first",
                gen.display(),
                watermark - 1,
                local_watermark - 1,
                root.display()
            ));
        }
        for i in 0..set.len() {
            let bm25 = set.bm25(i);
            let this = segment_tables(bm25);
            match &tables {
                None => tables = Some(this),
                Some(first) => {
                    let what = format!("{} segment {}", root.display(), set.metadata(i).segment_id);
                    if first.fields != this.fields {
                        return Err(format!(
                            "{what}: field table {:?} differs from the first source's {:?}; a \
                             child is one store and cannot mix tables",
                            this.fields, first.fields
                        ));
                    }
                    if first.fingerprints != this.fingerprints {
                        return Err(format!(
                            "{what}: analysis fingerprints {:?} differ from the first source's \
                             {:?}; a child cannot mix analyzers, split from the logs to re-analyze",
                            this.fingerprints, first.fingerprints
                        ));
                    }
                    if first.position_fields != this.position_fields {
                        return Err(format!(
                            "{what}: positional fields {:?} differ from the first source's {:?}",
                            this.position_fields, first.position_fields
                        ));
                    }
                    if first.sentence_fields != this.sentence_fields {
                        return Err(format!(
                            "{what}: sentence fields {:?} differ from the first source's {:?}",
                            this.sentence_fields, first.sentence_fields
                        ));
                    }
                    if first.columns != this.columns {
                        return Err(format!(
                            "{what}: column tables differ from the first source's; a child is \
                             one store and cannot mix column tables"
                        ));
                    }
                }
            }
        }
        let overlay_path = crate::node::live_docs_sidecar_path(&index_path_of_root(&root));
        let overlay = if overlay_path.exists() {
            Some(
                crate::live_docs::LiveDocs::open(&overlay_path)
                    .map_err(|e| format!("open {}: {e}", overlay_path.display()))?,
            )
        } else {
            None
        };
        // The log names deleted and replaced rows by global id; the
        // catalog's labels are local, so the set is kept in local terms.
        let deleted = deleted
            .into_iter()
            .map(|id| {
                id.checked_sub(manifest.slot_offset).ok_or_else(|| {
                    format!(
                        "{}: the log deletes row {id}, below the shard's slot offset {}; the log \
                         and its manifest disagree",
                        gen.display(),
                        manifest.slot_offset
                    )
                })
            })
            .collect::<Result<BTreeSet<u64>, String>>()?;
        shards.push(SegmentSourceShard {
            gen: gen.clone(),
            root,
            set,
            overlay,
            deleted,
            slot_offset: manifest.slot_offset,
        });
    }
    let tables = tables.ok_or_else(|| {
        "the sources have no sealed segment; there is nothing to transplant".to_string()
    })?;
    Ok((shards, tables))
}

/// The logged document row `row` of `bm25` stands for: its columns,
/// and with `with_text` its text, lineage, source and identity too.
pub(crate) fn reconstruct_document(
    bm25: &crate::postings::Bm25Reader,
    row: u32,
    tables: &SourceTables,
    with_text: bool,
) -> Result<AddDocumentsRequest, String> {
    use crate::postings::Bm25Index;
    let mut doc = AddDocumentsRequest::default();
    if with_text {
        let text =
            Bm25Index::text(bm25, row).ok_or_else(|| format!("row {row} has no stored text"))?;
        doc.text = text;
        doc.lineage = Bm25Index::lineage(bm25, row).map(|l| crate::pb::DocLineage {
            parent_id: l.parent_id,
            group_id: l.group_id,
            span_start: l.span_start,
            span_end: l.span_end,
        });
        if let Some((source, ordinal)) = bm25
            .protobuf_source(row)
            .map_err(|e| format!("row {row}: read original source: {e}"))?
        {
            doc.original_source = Some(source);
            doc.source_chunk_ordinal = ordinal;
        }
        doc.identity = bm25.document_identity(row);
        doc.position_fields = tables.position_fields.clone();
        doc.sentence_fields = tables.sentence_fields.clone();
    }
    // The store's declaration entry says what the row's derived values
    // mean (docs/derived-columns.md); a logged form of the row carries it.
    doc.derived_fingerprint = bm25
        .derived()
        .map(|derived| derived.fingerprint.clone())
        .unwrap_or_default();
    for (fi, name) in tables.columns.facets.iter().enumerate() {
        if let Some(ord) = bm25.facet_ord(fi, row) {
            doc.facets.push(crate::pb::FacetValue {
                field: name.clone(),
                value: bm25.facet_value(fi, ord).to_string(),
            });
        }
    }
    for (ni, name) in tables.columns.numerics.iter().enumerate() {
        if let Some(value) = bm25.numeric_value(ni, row) {
            doc.numerics.push(crate::pb::NumericValue {
                field: name.clone(),
                value,
            });
        }
    }
    for (ii, name) in tables.columns.integers.iter().enumerate() {
        if let Some(value) = bm25.integer_value(ii, row) {
            doc.integers.push(crate::pb::IntegerValue {
                field: name.clone(),
                value,
            });
        }
    }
    for (ui, name) in tables.columns.unsigned_integers.iter().enumerate() {
        if let Some(value) = bm25.unsigned_integer_value(ui, row) {
            doc.unsigned_integers.push(crate::pb::UnsignedIntegerValue {
                field: name.clone(),
                value,
            });
        }
    }
    for (gi, name) in tables.columns.geo.iter().enumerate() {
        if let Some((lat, lon)) = bm25.geo_value(gi, row) {
            doc.geo_points.push(crate::pb::GeoPointValue {
                field: name.clone(),
                lat,
                lon,
            });
        }
    }
    for (ci, name) in tables.columns.map_facets.iter().enumerate() {
        for (key_ord, key) in bm25.map_facet_keys(ci).iter().enumerate() {
            if let Some(ord) = bm25.map_facet_value_ord(ci, key_ord as u32, row) {
                doc.map_facets.push(crate::pb::MapFacetEntry {
                    field: name.clone(),
                    key: key.clone(),
                    value: bm25.map_facet_value(ci, ord).to_string(),
                });
            }
        }
    }
    for (ci, name) in tables.columns.map_numerics.iter().enumerate() {
        for (key_ord, key) in bm25.map_numeric_keys(ci).iter().enumerate() {
            if let Some(value) = bm25.map_numeric_value(ci, key_ord as u32, row) {
                doc.map_numerics.push(crate::pb::MapNumericEntry {
                    field: name.clone(),
                    key: key.clone(),
                    value,
                });
            }
        }
    }
    for (ci, name) in tables.columns.map_integers.iter().enumerate() {
        for (key_ord, key) in bm25.map_integer_keys(ci).iter().enumerate() {
            if let Some(value) = bm25.map_integer_value(ci, key_ord as u32, row) {
                doc.map_integers.push(crate::pb::MapIntegerEntry {
                    field: name.clone(),
                    key: key.clone(),
                    value,
                });
            }
        }
    }
    for (ci, name) in tables.columns.map_unsigned_integers.iter().enumerate() {
        for (key_ord, key) in bm25.map_unsigned_integer_keys(ci).iter().enumerate() {
            if let Some(value) = bm25.map_unsigned_integer_value(ci, key_ord as u32, row) {
                doc.map_unsigned_integers
                    .push(crate::pb::MapUnsignedIntegerEntry {
                        field: name.clone(),
                        key: key.clone(),
                        value,
                    });
            }
        }
    }
    Ok(doc)
}

/// Render the coordinator shard map for a reshard result (see
/// `config::ShardMap`). `addr` is a TODO placeholder — the operator fills
/// in where each child will listen.
pub fn shard_map_toml(out: &ReshardOutput) -> String {
    let mut s = format!("generation = {}\n", out.generation);
    for child in &out.children {
        s.push_str(&format!(
            "\n[[shards]]\naddr = \"TODO\"\nslot_offset = {}\nhash_lo = {}\nhash_hi = {}\n",
            child.slot_offset, child.hash_lo, child.hash_hi
        ));
    }
    s
}

/// Render the node-side `[[shards]]` config blocks for a reshard result
/// (same print idiom as `harness::write_shards`).
pub fn shards_toml(out: &ReshardOutput) -> String {
    let mut s = String::new();
    for child in &out.children {
        s.push_str(&format!(
            "[[shards]]\nlisten = \"0.0.0.0:50051\"  # TODO: one listener per child\nindex = \
             \"{}\"\nslot_offset = {}\n",
            child.vector_path.display(),
            child.slot_offset
        ));
    }
    s
}

/// The process's soft open-files limit, from `/proc/self/limits` on
/// Linux; `None` where that file is absent or unreadable.
pub fn open_files_soft_limit() -> Option<u64> {
    let text = std::fs::read_to_string("/proc/self/limits").ok()?;
    let line = text
        .lines()
        .find(|line| line.starts_with("Max open files"))?;
    let mut fields = line["Max open files".len()..].split_whitespace();
    let soft = fields.next()?;
    if soft == "unlimited" {
        return Some(u64::MAX);
    }
    soft.parse().ok()
}

/// Raise the soft open-files limit to the hard limit, so a spill plan
/// sized for the machine is not stopped by the shell's default of
/// 1024. Returns the limit in force afterwards; `None` when the build
/// cannot set limits (no `net` feature) or the call failed.
#[cfg(feature = "net")]
pub fn raise_open_files_limit() -> Option<u64> {
    // SAFETY: getrlimit/setrlimit read and write a plain struct the
    // caller owns; no memory is shared past the calls.
    unsafe {
        let mut limit = libc::rlimit {
            rlim_cur: 0,
            rlim_max: 0,
        };
        if libc::getrlimit(libc::RLIMIT_NOFILE, &mut limit) != 0 {
            return None;
        }
        if limit.rlim_cur < limit.rlim_max {
            limit.rlim_cur = limit.rlim_max;
            if libc::setrlimit(libc::RLIMIT_NOFILE, &limit) != 0 {
                return None;
            }
        }
        // rlim_t is u64 on the fleet's targets and wider elsewhere.
        #[allow(clippy::unnecessary_cast)]
        Some(limit.rlim_cur as u64)
    }
}

#[cfg(not(feature = "net"))]
pub fn raise_open_files_limit() -> Option<u64> {
    None
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
    let mut manifests = Vec::with_capacity(gens.len());
    for gen in gens {
        let manifest = read_gen_manifest(gen)?;
        require_backend_config(&manifest, gen)?;
        require_complete_history(&manifest, gen)?;
        if let Some(first) = manifests.first() {
            if !same_backend_config(first, &manifest) {
                return Err(format!(
                    "{}: vector backend configuration differs from the first input; \
                     split/merge requires one scoring configuration",
                    gen.display()
                ));
            }
            if first.bucket_count != manifest.bucket_count {
                return Err(format!(
                    "{}: bucket count {} differs from the first input's {}; merge inputs \
                     must share one bucket geometry",
                    gen.display(),
                    manifest.bucket_count,
                    first.bucket_count
                ));
            }
        }
        manifests.push(manifest);
    }
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
