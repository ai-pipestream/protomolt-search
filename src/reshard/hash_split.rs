//! Hash-routed split and merge over WAL buckets.

use std::path::{Path, PathBuf};

use super::child_build::{finish_child, ChildSlot};
use super::child_catalog::sealed;
use super::merge_inputs::load_merge_manifests;
use super::parent_routing::{
    clear_scattered, logs_hold_foreign_parents, parent_child, parent_routes, scattered_parents,
};
use super::replay::{read_gens_binding, replay_buckets, Replay};
use super::split_inputs::{
    create_out_dir, distribute, hash_range, replay_clocked, require_stable_keys, SplitInputs,
};
use super::{Analyzer, ChildImage, ReshardOutput, StableReshardOutput};

/// A hash split of one or more full-history WAL generations into `n`
/// children: child `i` takes `slot_offset = slot_base + i * slot_stride`
/// (the stride defaults to 25M in the example, matching
/// deploy/court-e2e) and dense local slots in original id order.
#[derive(Debug, Clone, Copy)]
pub struct SplitRequest<'a> {
    pub gens: &'a [PathBuf],
    /// The child count, a power of two.
    pub n: usize,
    pub out_dir: &'a Path,
    pub slot_base: u64,
    pub slot_stride: u64,
    /// Replay vectors only, leaving every document behind.
    pub vectors_only: bool,
    /// The BM25 field table; derived from the records when `None`.
    pub bm25_fields: Option<&'a [String]>,
}

impl<'a> SplitRequest<'a> {
    /// A hash split of `gens` into `n` children written under `out_dir`,
    /// defaulting to `slot_base = 0`, `slot_stride = 0`, full document replay,
    /// and BM25 fields derived from the records.
    pub fn new(gens: &'a [PathBuf], n: usize, out_dir: &'a Path) -> Self {
        Self {
            gens,
            n,
            out_dir,
            slot_base: 0,
            slot_stride: 0,
            vectors_only: false,
            bm25_fields: None,
        }
    }

    /// Set the base global slot offset for child `0`.
    pub fn slot_base(mut self, slot_base: u64) -> Self {
        self.slot_base = slot_base;
        self
    }

    /// Set the global slot stride between consecutive children.
    pub fn slot_stride(mut self, slot_stride: u64) -> Self {
        self.slot_stride = slot_stride;
        self
    }

    /// Replay vectors only, omitting document payloads.
    pub fn vectors_only(mut self, vectors_only: bool) -> Self {
        self.vectors_only = vectors_only;
        self
    }

    /// Supply an explicit BM25 field table instead of deriving it from records.
    pub fn bm25_fields(mut self, bm25_fields: Option<&'a [String]>) -> Self {
        self.bm25_fields = bm25_fields;
        self
    }

    pub(super) fn require_power_of_two(&self, what: &str) -> Result<(), String> {
        if self.n.is_power_of_two() {
            return Ok(());
        }
        Err(format!(
            "{what} factor must be a positive power of two, got {}",
            self.n
        ))
    }

    pub(super) fn slot_offset(&self, child: usize) -> u64 {
        self.slot_base + child as u64 * self.slot_stride
    }
}

/// Split ANY number of shards' WAL generations into `n` child images —
/// the general N -> M reshard. One input log is a plain split; several
/// input logs redistribute their union across the children with no
/// intermediate merge artifact: child `i`'s input is its bucket range
/// read from EVERY input generation, and global ids from different
/// shards never collide (disjoint slot ranges), so the per-child replay
/// map assembles the union in id order for free.
///
/// With `n <= bucket_count` and `bucket_count % n == 0` this is the cheap
/// path: child `i` owns the contiguous bucket range
/// `[i*bucket_count/n, (i+1)*bucket_count/n)`, replays only those files,
/// and its hash range is exactly those buckets. A finer split
/// (`n > bucket_count`) re-partitions every record by
/// `bucket_of(id, n)` — correct but slower; the WAL bucket count
/// caps cheap split granularity.
///
/// All inputs must carry byte-identical provider configurations and one bucket
/// geometry, and be full-history — the same preconditions as
/// [`merge`], enforced the same way.
pub fn split_logs_with(
    request: &SplitRequest<'_>,
    analyze: &mut Analyzer,
) -> Result<ReshardOutput, String> {
    request.require_power_of_two("split")?;
    let inputs = SplitInputs::check(request.gens, "split", true)?;
    create_out_dir(request.out_dir)?;
    let n = request.n;
    let bucket_count = inputs.manifest.bucket_count as usize;
    let cheap = n <= bucket_count && bucket_count.is_multiple_of(n);
    let children = if cheap
        && !logs_hold_foreign_parents(request.gens, &inputs.manifest, request.vectors_only)?
    {
        split_bucket_ranges(request, &inputs, analyze)?
    } else {
        eprintln!(
            "reshard: whole WAL buckets route by row id, which would scatter a lineage \
             parent's rows or cap the split at {bucket_count} children; re-partitioning \
             every record by parent for split={n}"
        );
        split_rehashed(request, &inputs, analyze)?
    };
    sealed(&inputs.manifest, inputs.next_generation()?, children)
}

/// The cheap path: whole bucket ranges, no re-hashing. Memory is bounded
/// per child: only its bucket range is resident, read from each input in
/// turn.
fn split_bucket_ranges(
    request: &SplitRequest<'_>,
    inputs: &SplitInputs,
    analyze: &mut Analyzer,
) -> Result<Vec<ChildImage>, String> {
    let manifest = &inputs.manifest;
    let n = request.n;
    let bucket_count = manifest.bucket_count as usize;
    let bucket_bits = manifest.bucket_count.trailing_zeros();
    let per_child = bucket_count / n;
    let schema = inputs.schema(request.bm25_fields);
    let mut children = Vec::with_capacity(n);
    for i in 0..n {
        let first = (i * per_child) as u32;
        let mut replay = Replay::default();
        for gen in request.gens {
            let buckets = first..first + per_child as u32;
            let dim = manifest.dim as usize;
            replay_buckets(
                gen,
                buckets,
                bucket_count,
                dim,
                request.vectors_only,
                &mut replay,
            )?;
        }
        let hash_lo = u64::from(first) << (64 - bucket_bits);
        let hash_hi = if i + 1 == n {
            u64::MAX
        } else {
            ((first as u64 + per_child as u64) << (64 - bucket_bits)) - 1
        };
        let slot = ChildSlot {
            ordinal: i,
            out_dir: request.out_dir,
            slot_offset: request.slot_offset(i),
            hash_lo,
            hash_hi,
        };
        children.push(finish_child(manifest, replay, slot, &schema, analyze)?);
    }
    Ok(children)
}

/// A split finer than the bucket count, or over logs that hold lineage
/// parents: every record is re-partitioned by the parent routing of its lineage
/// parent (its own id when it has none), so a parent's chunks share a
/// child.
fn split_rehashed(
    request: &SplitRequest<'_>,
    inputs: &SplitInputs,
    analyze: &mut Analyzer,
) -> Result<Vec<ChildImage>, String> {
    let manifest = &inputs.manifest;
    let n = request.n;
    let mut replay = Replay::default();
    for gen in request.gens {
        let bucket_count = manifest.bucket_count as usize;
        let buckets = 0..manifest.bucket_count;
        let dim = manifest.dim as usize;
        replay_buckets(
            gen,
            buckets,
            bucket_count,
            dim,
            request.vectors_only,
            &mut replay,
        )?;
    }
    replay.compact();
    let routes = parent_routes(&replay);
    let lineage = super::parent_routing::lineage_keys(&replay);
    let divergent = std::cell::RefCell::new(vec![false; n]);
    let partitions = distribute(replay, n, |id, key| {
        let (child, keyed) = parent_child(&routes, &lineage, id, key, n);
        if super::parent_routing::route_of(&routes, id).is_lineage() && !keyed {
            divergent.borrow_mut()[child] = true;
        }
        Ok(child)
    })?;
    build_children(
        request,
        inputs,
        partitions,
        &divergent.into_inner(),
        analyze,
    )
}

/// One image per partition, child `i` at its stride offset and its
/// share of the hash space.
fn build_children(
    request: &SplitRequest<'_>,
    inputs: &SplitInputs,
    partitions: Vec<Replay>,
    divergent: &[bool],
    analyze: &mut Analyzer,
) -> Result<Vec<ChildImage>, String> {
    let schema = inputs.schema(request.bm25_fields);
    let n = partitions.len();
    let scattered = scattered_parents(&partitions);
    let mut children = Vec::with_capacity(n);
    for (i, replay) in partitions.into_iter().enumerate() {
        let (hash_lo, hash_hi) = hash_range(i, n);
        let slot = ChildSlot {
            ordinal: i,
            out_dir: request.out_dir,
            slot_offset: request.slot_offset(i),
            hash_lo,
            hash_hi,
        };
        children.push(finish_child(
            &inputs.manifest,
            replay,
            slot,
            &schema,
            analyze,
        )?);
    }
    clear_scattered(&mut children, &scattered);
    // A child holding rows routed outside the online key hash space
    // cannot promise later keyed appends land on it.
    clear_scattered(&mut children, divergent);
    Ok(children)
}

/// Repartition full-history, generation-clocked WALs by their opaque stable
/// product keys. This is the baseline-build half of a hitless reshard: it
/// returns the exact source high watermarks included in the child images, so
/// the live tail can resume without a gap before an atomic map publication.
pub fn split_stable_logs_with(
    request: &SplitRequest<'_>,
    analyze: &mut Analyzer,
) -> Result<StableReshardOutput, String> {
    request.require_power_of_two("stable split")?;
    let inputs = SplitInputs::check(request.gens, "stable split", false)?;
    let (replay, source_cutoffs) = replay_clocked(request.gens, request.vectors_only)?;
    require_stable_keys(&replay)?;
    create_out_dir(request.out_dir)?;
    let n = request.n;
    let shift = 64 - n.trailing_zeros();
    let partitions = distribute(replay, n, |_, key| {
        let key = key.expect("validated above");
        Ok(if n == 1 {
            0
        } else {
            (crate::coordinator::stable_routing_hash(key) >> shift) as usize
        })
    })?;
    let children = build_children(request, &inputs, partitions, &vec![false; n], analyze)?;
    Ok(StableReshardOutput {
        images: sealed(&inputs.manifest, inputs.next_generation()?, children)?,
        source_cutoffs,
    })
}

/// Repartition full-history, generation-clocked WALs by their stable
/// product keys into children covering the given hash ranges: child `i` receives the
/// rows whose key hash falls in `ranges[i]` and takes `slot_offsets[i]`.
/// The ranges must tile the source's range without a gap; a live row
/// whose hash falls outside every range refuses by name, since the
/// children would not conserve the source. Legacy rows without a stable
/// key refuse as [`split_stable_logs_with`] does.
pub fn split_stable_logs_ranged(
    gens: &[PathBuf],
    ranges: &[(u64, u64)],
    out_dir: &Path,
    slot_offsets: &[u64],
    vectors_only: bool,
    bm25_fields: Option<&[String]>,
    analyze: &mut Analyzer,
) -> Result<StableReshardOutput, String> {
    check_ranges(ranges, slot_offsets)?;
    let inputs = SplitInputs::check(gens, "a ranged split", false)?;
    let (replay, source_cutoffs) = replay_clocked(gens, vectors_only)?;
    require_stable_keys(&replay)?;
    create_out_dir(out_dir)?;
    let partitions = distribute(replay, ranges.len(), |_, key| {
        let hash = crate::coordinator::stable_routing_hash(key.expect("validated above"));
        ranges
            .iter()
            .position(|(lo, hi)| hash >= *lo && hash <= *hi)
            .ok_or_else(|| {
                format!(
                    "a live row's key hashes to {hash}, outside every child range of this \
                     split; the source holds rows the split's range does not cover"
                )
            })
    })?;
    let schema = inputs.schema(bm25_fields);
    let scattered = scattered_parents(&partitions);
    let mut children = Vec::with_capacity(ranges.len());
    for (shard, replay) in partitions.into_iter().enumerate() {
        let (hash_lo, hash_hi) = ranges[shard];
        let slot = ChildSlot {
            ordinal: shard,
            out_dir,
            slot_offset: slot_offsets[shard],
            hash_lo,
            hash_hi,
        };
        children.push(finish_child(
            &inputs.manifest,
            replay,
            slot,
            &schema,
            analyze,
        )?);
    }
    clear_scattered(&mut children, &scattered);
    Ok(StableReshardOutput {
        images: sealed(&inputs.manifest, inputs.next_generation()?, children)?,
        source_cutoffs,
    })
}

/// A ranged split's children: at least two, one slot offset each, the
/// ranges adjacent, ascending, and none inverted.
fn check_ranges(ranges: &[(u64, u64)], slot_offsets: &[u64]) -> Result<(), String> {
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
    Ok(())
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
    let derived = super::child_build::manifest_derived(&manifests[0])?;
    let max_generation = manifests.iter().map(|m| m.generation).max().unwrap_or(0);
    let min_slot_offset = manifests.iter().map(|m| m.slot_offset).min().unwrap_or(0);

    // Bucket-major replay: one bucket index at a time, across all inputs.
    let mut merged = Replay::default();
    for bucket in 0..bucket_count {
        for gen in gens {
            let count = bucket_count as usize;
            replay_buckets(
                gen,
                bucket..bucket + 1,
                count,
                dim,
                vectors_only,
                &mut merged,
            )?;
        }
    }
    create_out_dir(out_dir)?;
    eprintln!(
        "reshard: merge of {} generations: {} vectors, {} documents",
        gens.len(),
        merged.vectors.len(),
        merged.documents.len()
    );
    let declared = manifests[0]
        .columns
        .as_ref()
        .filter(|table| !table.fields.is_empty());
    let columns = declared.map(super::ColumnTables::of_wal);
    let schema = super::child_build::ChildSchema {
        bm25_fields: bm25_fields.or(declared.map(|table| table.fields.as_slice())),
        binding: binding.as_ref(),
        derived: derived.as_ref(),
        columns: columns.as_ref(),
        ..Default::default()
    };
    let slot = ChildSlot::full_range(0, out_dir, slot_base.unwrap_or(min_slot_offset));
    let child = finish_child(&manifests[0], merged, slot, &schema, analyze)?;
    sealed(&manifests[0], max_generation + 1, vec![child])
}
