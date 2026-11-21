//! Split by placement-column ranges into a flat set of children.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use super::child_build::{finish_child, ChildSlot};
use super::cuts::partition_key_of;
use super::split_inputs::{create_out_dir, distribute, replay_clocked, SplitInputs};
use super::{Analyzer, ReshardOutput, WalCutoff};

/// One child of a placement-keyed split: the inclusive code range it
/// takes. A single code is a
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

/// A split of one or more full-history generations into children keyed by the
/// placement code each logged document carries in `column`: child `i` takes the
/// codes in `children[i]` and `slot_offsets[i]`.
#[derive(Debug, Clone, Copy)]
pub struct PlacementSplitRequest<'a> {
    pub gens: &'a [PathBuf],
    /// The placement column.
    pub column: &'a str,
    pub children: &'a [PlacementChild],
    /// The child that takes rows with no code, or a code no child covers.
    pub default_child: Option<usize>,
    pub out_dir: &'a Path,
    pub slot_offsets: &'a [u64],
    /// Replay vectors only, leaving every document behind.
    pub vectors_only: bool,
    /// The BM25 field table; derived from the records when `None`.
    pub bm25_fields: Option<&'a [String]>,
}

impl<'a> PlacementSplitRequest<'a> {
    /// A placement split of `gens` on `column` into `children` written under `out_dir`.
    pub fn new(
        gens: &'a [PathBuf],
        column: &'a str,
        children: &'a [PlacementChild],
        out_dir: &'a Path,
        slot_offsets: &'a [u64],
    ) -> Self {
        Self {
            gens,
            column,
            children,
            default_child: None,
            out_dir,
            slot_offsets,
            vectors_only: false,
            bm25_fields: None,
        }
    }

    /// Name the fallback child receiving rows without a matching placement code.
    pub fn default_child(mut self, default_child: Option<usize>) -> Self {
        self.default_child = default_child;
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

    fn check(&self) -> Result<(), String> {
        if self.column.trim().is_empty() {
            return Err("a placement split needs the placement column's name".to_string());
        }
        validate_placement_children(self.children, self.default_child)?;
        if self.slot_offsets.len() != self.children.len() {
            return Err(format!(
                "a placement split needs one slot offset per child: {} offsets for {} children",
                self.slot_offsets.len(),
                self.children.len()
            ));
        }
        Ok(())
    }

    /// The child a row with placement code `code` goes to.
    fn child_of(&self, id: u64, code: Option<i64>) -> Result<usize, String> {
        if let Some(code) = code {
            if let Some(i) = self
                .children
                .iter()
                .position(|child| code >= child.lo && code <= child.hi)
            {
                return Ok(i);
            }
        }
        let column = self.column;
        self.default_child.ok_or_else(|| match code {
            Some(code) => format!(
                "live source id {id} carries placement code {code}, outside every child's \
                 range, and the split names no default child"
            ),
            None => format!(
                "live source id {id} carries no value in {column:?} and the split names no \
                 default child"
            ),
        })
    }
}

/// Split by placement code: no CEL runs at replay, since the code was
/// written at ingest and the log holds it. A document whose code falls in
/// no child's range, a document with no value, and a vector row with no
/// document all go to the default child; without one, any such row
/// refuses the split by id. Every child keeps the parent's stable-key
/// hash coverage (the whole space), because routing under a tree is by
/// code first and by hash inside the group.
pub fn split_placement_logs_with(
    request: &PlacementSplitRequest<'_>,
    analyze: &mut Analyzer,
) -> Result<PlacementReshardOutput, String> {
    request.check()?;
    let inputs = SplitInputs::check(request.gens, "a placement split", false)?;
    let (replay, source_cutoffs) = replay_clocked(request.gens, request.vectors_only)?;
    create_out_dir(request.out_dir)?;
    let mut codes: BTreeMap<u64, Option<i64>> = BTreeMap::new();
    for (id, document) in &replay.documents {
        codes.insert(*id, partition_key_of(document, request.column)?);
    }
    let partitions = distribute(replay, request.children.len(), |id, _| {
        request.child_of(id, codes.get(&id).copied().flatten())
    })?;
    let schema = inputs.schema(request.bm25_fields);
    // Placement routes by code, so a parent's chunks share a child only
    // when they carry one code: verified per child, never assumed.
    let scattered = super::parent_routing::scattered_parents(&partitions);
    let mut out = Vec::with_capacity(request.children.len());
    for (shard, replay) in partitions.into_iter().enumerate() {
        let slot = ChildSlot::full_range(shard, request.out_dir, request.slot_offsets[shard]);
        out.push(finish_child(
            &inputs.manifest,
            replay,
            slot,
            &schema,
            analyze,
        )?);
    }
    super::parent_routing::clear_scattered(&mut out, &scattered);
    if request.vectors_only {
        for child in &mut out {
            child.parents_colocated = false;
        }
    }
    Ok(PlacementReshardOutput {
        images: super::child_catalog::sealed(&inputs.manifest, inputs.next_generation()?, out)?,
        source_cutoffs,
        ranges: request.children.to_vec(),
        default_child: request.default_child,
    })
}

/// The shard map for a placement split: `placement = <code>` on a child
/// that holds one code; a child holding a range is left for the
/// operator to place, with the range in a comment, because the map
/// names one code per shard.
pub fn placement_shard_map_toml(out: &PlacementReshardOutput) -> String {
    super::render_shard_map(&out.images, |i| {
        let range = out.ranges[i];
        if range.is_none() {
            "# the default child: rows with no code or a code no child covers\n".to_string()
        } else if range.lo == range.hi {
            format!("placement = {}\n", range.lo)
        } else {
            format!(
                "# placement codes {}..={}: one code per shard in the map; choose one\n",
                range.lo, range.hi
            )
        }
    })
}
