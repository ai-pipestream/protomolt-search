//! Pass 3 of a compaction from segments: one image per cut, in cut order.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use crate::wal::WalManifest;

use super::super::analysis_sidecar::{analysis_sidecar_path, read_analysis_file};
use super::super::child_build::{finish_child_ordered, ChildSchema, ChildSlot};
use super::super::cuts::partition_key_of;
use super::super::parallel_build::{refusing_analyzer, ParallelBuild};
use super::super::replay::{replay_buckets_routed, Replay, ReplayScope};
use super::super::tree_split::live_rows;
use super::{ChildImage, SegmentCompactionOptions};

const REFUSAL: &str =
    "a compaction from segments analyzes nothing; every row's fields come from the segments";

/// Everything one cut's image is built from.
pub(super) struct CutBuild<'a> {
    pub(super) manifest: &'a WalManifest,
    pub(super) schema: ChildSchema<'a>,
    pub(super) column: &'a str,
    pub(super) out_dir: &'a Path,
    pub(super) spill_dirs: &'a [PathBuf],
    pub(super) bucket_rows: &'a [u64],
}

/// The images in cut order and the id map they make.
#[derive(Default)]
pub(super) struct PlacedCuts {
    pub(super) images: Vec<ChildImage>,
    pub(super) id_map: BTreeMap<u64, u64>,
}

impl PlacedCuts {
    fn place(&mut self, image: ChildImage) -> Result<(), String> {
        for (slot, &old_id) in image.row_parent_ids.iter().enumerate() {
            let new_local = image
                .slot_offset
                .checked_add(slot as u64)
                .ok_or_else(|| "compaction from segments: slot overflow".to_string())?;
            if self.id_map.insert(old_id, new_local).is_some() {
                return Err(format!(
                    "compaction from segments saw source id {old_id} twice; the catalog is corrupt"
                ));
            }
        }
        self.images.push(image);
        Ok(())
    }
}

impl CutBuild<'_> {
    /// One cut replayed and sealed at `slot`; `None` for an empty cut.
    fn build(
        &self,
        bucket: u32,
        ordinal: usize,
        slot: u64,
    ) -> Result<Option<(ChildImage, u64)>, String> {
        let spill_dir = &self.spill_dirs[bucket as usize];
        let scope = ReplayScope {
            bucket_count: 1,
            dim: self.manifest.dim as usize,
            vectors_only: false,
            documents_only: false,
            upto_clock: u64::MAX,
            check_routing: false,
        };
        let mut replay = Replay::default();
        replay_buckets_routed(spill_dir, 0..1, &scope, &mut replay)?;
        replay.analyzed = read_analysis_file(&analysis_sidecar_path(spill_dir, bucket))?;
        replay.compact();
        let rows = live_rows(&replay);
        if rows == 0 {
            return Ok(None);
        }
        let order = self.cut_order(&replay)?;
        let mut refuse = refusing_analyzer(REFUSAL);
        let image = finish_child_ordered(
            self.manifest,
            replay,
            &order,
            ChildSlot::full_range(ordinal, self.out_dir, slot),
            &self.schema,
            None,
            &mut *refuse,
        )?;
        Ok(Some((image, rows)))
    }

    /// Key order within the cut, ties by source id, unkeyed rows last;
    /// vector-bearing rows first, the slot shape the image builder assigns.
    fn cut_order(&self, replay: &Replay) -> Result<Vec<u64>, String> {
        let mut keyed: Vec<(i64, u64)> = Vec::with_capacity(replay.documents.len());
        let mut unkeyed: Vec<u64> = Vec::new();
        for (id, document) in &replay.documents {
            match partition_key_of(document, self.column)? {
                Some(key) => keyed.push((key, *id)),
                None => unkeyed.push(*id),
            }
        }
        keyed.sort_unstable();
        let ordered = keyed.into_iter().map(|(_, id)| id).chain(unkeyed);
        let (mut order, rest): (Vec<u64>, Vec<u64>) =
            ordered.partition(|id| replay.vectors.contains_key(id));
        order.extend(rest);
        Ok(order)
    }

    /// Every cut built in order on this thread, slots dense across them.
    pub(super) fn build_serial(&self) -> Result<PlacedCuts, String> {
        let mut placed = PlacedCuts::default();
        let mut next_slot = 0u64;
        for bucket in 0..self.bucket_rows.len() as u32 {
            let ordinal = placed.images.len();
            let Some((image, rows)) = self.build(bucket, ordinal, next_slot)? else {
                continue;
            };
            placed.place(image)?;
            next_slot = next_slot
                .checked_add(rows)
                .ok_or_else(|| "compaction from segments: slot overflow".to_string())?;
        }
        Ok(placed)
    }

    /// Threaded: every cut's slot range comes from the spill's own
    /// counts, so cuts build in any order and append in cut order, the
    /// same catalog the one-thread build writes.
    pub(super) fn build_threaded(
        &self,
        options: &SegmentCompactionOptions,
    ) -> Result<PlacedCuts, String> {
        let mut plan: Vec<(u32, usize, u64)> = Vec::new();
        let mut slot = 0u64;
        for (bucket, &n) in self.bucket_rows.iter().enumerate() {
            if n == 0 {
                continue;
            }
            plan.push((bucket as u32, plan.len(), slot));
            slot = slot
                .checked_add(n)
                .ok_or_else(|| "compaction from segments: slot overflow".to_string())?;
        }
        let parallel = ParallelBuild::new(
            options.build_threads,
            options.build_queue,
            plan.len(),
            "cut",
        );
        let mut placed = PlacedCuts::default();
        parallel.run(
            &plan,
            |&(bucket, ordinal, slot)| {
                self.build(bucket, ordinal, slot)?.ok_or_else(|| {
                    format!(
                        "cut {bucket}: the spill counted {} documents and the replay found none",
                        self.bucket_rows[bucket as usize]
                    )
                })
            },
            |&(bucket, _, _), (image, rows)| {
                let counted = self.bucket_rows[bucket as usize];
                if rows != counted {
                    return Err(format!(
                        "cut {bucket}: the spill counted {counted} documents, the sealed image \
                         holds {rows} rows"
                    ));
                }
                placed.place(image)
            },
        )?;
        Ok(placed)
    }
}
