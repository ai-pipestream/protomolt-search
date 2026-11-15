//! The single-image layout: each child's rows held together while its
//! one image is built.

use super::super::tree_split::{check_slot_bound, image_rows, live_rows};
use super::super::{Analyzer, ChildImage};
use super::build::TreeBuild;

/// The images, one per child, and the most rows one replay held.
pub(super) struct BuiltChildren {
    pub(super) images: Vec<ChildImage>,
    /// Sealed segments per child.
    pub(super) segments: Vec<usize>,
    pub(super) peak_replay_rows: u64,
}

/// Every child as one image. A child above `max_child_rows` is refused by
/// name before any image is written.
pub(super) fn build_single_images(
    build: &TreeBuild<'_>,
    max_child_rows: u64,
    analyze: &mut Analyzer,
) -> Result<BuiltChildren, String> {
    let plan = build.plan;
    let placed = &build.spill.stats.placed;
    for (index, child) in plan.children.iter().enumerate() {
        if placed[index] > max_child_rows {
            return Err(format!(
                "child {index} ({}) has {} documents, above the single-image bound of \
                 {max_child_rows}; use the segmented layout or raise the bound",
                child.leaf, placed[index]
            ));
        }
    }
    let mut built = BuiltChildren {
        images: Vec::with_capacity(plan.children.len()),
        segments: vec![1; plan.children.len()],
        peak_replay_rows: 0,
    };
    for (index, child) in plan.children.iter().enumerate() {
        let replay = build.replay(index, 0..build.spill.spill_bucket_count)?;
        if replay.documents.len() as u64 != placed[index] {
            return Err(format!(
                "child {index} ({}): the spill log holds {} documents, routing counted {}",
                child.leaf,
                replay.documents.len(),
                placed[index]
            ));
        }
        built.peak_replay_rows = built.peak_replay_rows.max(live_rows(&replay));
        let offset = plan.slot_offsets[index];
        let image = build.image(index, replay, plan.out_dir, index, offset, analyze)?;
        let bound = plan.slot_bound(index);
        check_slot_bound(
            index,
            &child.leaf,
            image.slot_offset,
            image_rows(&image),
            bound,
        )?;
        built.images.push(image);
    }
    Ok(built)
}
