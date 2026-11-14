//! Split by placement-column ranges into a flat set of children.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::wal;

use super::child_build::finish_child;
use super::partitioned::partition_key_of;
use super::replay::{
    read_gen_manifest, read_gens_binding, replay_buckets, require_backend_config,
    require_complete_history, same_backend_config, Replay,
};
use super::{Analyzer, ReshardOutput, WalCutoff};

/// One child of a placement-keyed split: the inclusive code range it
/// takes (`docs/placement.md`, "Changing the tree"). A single code is a
/// range of one; a prefix range takes a subtree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PlacementChild {
    pub lo: i64,
    pub hi: i64,
}

/// The result of a placement-keyed split: the images plus, per child,
/// the code range it holds, and the cutoffs a live catch-up resumes from.
#[derive(Debug)]
pub struct PlacementReshardOutput {
    pub images: ReshardOutput,
    pub source_cutoffs: Vec<WalCutoff>,
    /// Parallel to `images.children`.
    pub ranges: Vec<PlacementChild>,
    /// The child that took the rows with no value in the column, when
    /// one was named.
    pub default_child: Option<usize>,
}

impl PlacementChild {
    /// A child with no code range of its own: only the default child may
    /// be one, and it takes the rows no range covers.
    pub const NONE: PlacementChild = PlacementChild { lo: 0, hi: -1 };

    pub fn is_none(&self) -> bool {
        self.lo > self.hi
    }
}

/// Validate a placement split's children: at least one, no range
/// inverted (a rangeless child is allowed only as the default), none
/// overlapping, and a default index inside the list.
pub fn validate_placement_children(
    children: &[PlacementChild],
    default_child: Option<usize>,
) -> Result<(), String> {
    if children.is_empty() {
        return Err("a placement split needs at least one child".to_string());
    }
    for (i, child) in children.iter().enumerate() {
        if child.is_none() {
            if default_child != Some(i) {
                return Err(format!(
                    "placement child {i} has no code range and is not the default child"
                ));
            }
            continue;
        }
        if child.lo < 0 {
            return Err(format!(
                "placement child {i} starts below zero ({}); codes are never negative",
                child.lo
            ));
        }
    }
    let mut sorted: Vec<(usize, PlacementChild)> = children
        .iter()
        .copied()
        .enumerate()
        .filter(|(_, c)| !c.is_none())
        .collect();
    sorted.sort_by_key(|(_, c)| c.lo);
    for pair in sorted.windows(2) {
        let (i, a) = pair[0];
        let (j, b) = pair[1];
        if b.lo <= a.hi {
            return Err(format!(
                "placement children {i} ({}..={}) and {j} ({}..={}) overlap",
                a.lo, a.hi, b.lo, b.hi
            ));
        }
    }
    if let Some(d) = default_child {
        if d >= children.len() {
            return Err(format!(
                "default child {d} is outside the {} children",
                children.len()
            ));
        }
    }
    Ok(())
}

/// Split one or more full-history generations into children keyed by
/// the placement code each logged document carries in `column`
/// (`docs/placement.md`). No CEL runs at replay: the code was written at
/// ingest and the log holds it. A document whose code falls in no
/// child's range, a document with no value, and a vector row with no
/// document all go to `default_child`; without one, any such row
/// refuses the split by id. Every child keeps the parent's stable-key
/// hash coverage (the whole space), because routing under a tree is by
/// code first and by hash inside the group.
#[allow(clippy::too_many_arguments)]
pub fn split_placement_logs(
    gens: &[PathBuf],
    column: &str,
    children: &[PlacementChild],
    default_child: Option<usize>,
    out_dir: &Path,
    slot_offsets: &[u64],
    vectors_only: bool,
    bm25_fields: Option<&[String]>,
    analyze: &mut Analyzer,
) -> Result<PlacementReshardOutput, String> {
    if column.trim().is_empty() {
        return Err("a placement split needs the placement column's name".to_string());
    }
    validate_placement_children(children, default_child)?;
    if slot_offsets.len() != children.len() {
        return Err(format!(
            "a placement split needs one slot offset per child: {} offsets for {} children",
            slot_offsets.len(),
            children.len()
        ));
    }
    let Some(first_gen) = gens.first() else {
        return Err("a placement split requires at least one input generation".to_string());
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
    let binding = read_gens_binding(gens)?;
    std::fs::create_dir_all(out_dir)
        .map_err(|error| format!("mkdir {}: {error}", out_dir.display()))?;
    let child_of = |id: u64, code: Option<i64>| -> Result<usize, String> {
        if let Some(code) = code {
            if let Some(i) = children
                .iter()
                .position(|child| code >= child.lo && code <= child.hi)
            {
                return Ok(i);
            }
        }
        default_child.ok_or_else(|| match code {
            Some(code) => format!(
                "live source id {id} carries placement code {code}, outside every child's \
                 range, and the split names no default child"
            ),
            None => format!(
                "live source id {id} carries no value in {column:?} and the split names no \
                 default child"
            ),
        })
    };
    let mut codes: BTreeMap<u64, Option<i64>> = BTreeMap::new();
    for (id, document) in &replay.documents {
        codes.insert(*id, partition_key_of(document, column)?);
    }
    let mut buckets: Vec<Replay> = children.iter().map(|_| Replay::default()).collect();
    let keys = replay.stable_keys;
    for (id, vector) in replay.vectors {
        let shard = child_of(id, codes.get(&id).copied().flatten())?;
        buckets[shard].vectors.insert(id, vector);
        if let Some(row) = replay.retained.remove(&id) {
            buckets[shard].retained.insert(id, row);
        }
        if let Some(key) = keys.get(&id) {
            buckets[shard].stable_keys.insert(id, key.clone());
        }
    }
    for (id, document) in replay.documents {
        let shard = child_of(id, codes.get(&id).copied().flatten())?;
        buckets[shard].documents.insert(id, document);
        if let Some(key) = keys.get(&id) {
            buckets[shard].stable_keys.insert(id, key.clone());
        }
    }
    let mut out = Vec::with_capacity(children.len());
    for (shard, replay) in buckets.into_iter().enumerate() {
        out.push(finish_child(
            &manifest,
            replay,
            shard,
            out_dir,
            slot_offsets[shard],
            0,
            u64::MAX,
            bm25_fields,
            binding.as_ref(),
            analyze,
        )?);
    }
    Ok(PlacementReshardOutput {
        images: ReshardOutput {
            generation: top_generation + 1,
            children: out,
        },
        source_cutoffs: cutoffs,
        ranges: children.to_vec(),
        default_child,
    })
}

/// The shard map for a placement split: `placement = <code>` on a child
/// that holds one code; a child holding a range is left for the
/// operator to place, with the range in a comment, because the map
/// names one code per shard.
pub fn placement_shard_map_toml(out: &PlacementReshardOutput) -> String {
    let mut s = format!("generation = {}\n", out.images.generation);
    for (child, range) in out.images.children.iter().zip(&out.ranges) {
        s.push_str(&format!(
            "\n[[shards]]\naddr = \"TODO\"\nslot_offset = {}\nhash_lo = {}\nhash_hi = {}\n",
            child.slot_offset, child.hash_lo, child.hash_hi
        ));
        if range.is_none() {
            s.push_str("# the default child: rows with no code or a code no child covers\n");
        } else if range.lo == range.hi {
            s.push_str(&format!("placement = {}\n", range.lo));
        } else {
            s.push_str(&format!(
                "# placement codes {}..={}: one code per shard in the map; choose one\n",
                range.lo, range.hi
            ));
        }
    }
    s
}
