//! The segmented layout: each child a catalog sealed one spill bucket at
//! a time, on one thread or several.

use std::path::{Path, PathBuf};

use crate::segments::{SegmentCatalog, SegmentSource};

use super::super::analysis_sidecar::{analysis_sidecar_path, drop_page_cache, drop_segment_cache};
use super::super::parallel_build::{refusing_analyzer, ParallelBuild};
use super::super::tree_split::{check_slot_bound, image_rows, live_rows, SpillCut};
use super::super::{Analyzer, ChildImage};
use super::build::TreeBuild;
use super::single::BuiltChildren;

const REFUSAL: &str =
    "a transplant from segments analyzes nothing; every row's fields come from the spill";

/// Every child as a segment catalog; a child `--only-child` leaves out is
/// listed with no rows and not built.
pub(super) fn build_segmented(
    build: &TreeBuild<'_>,
    analyze: &mut Analyzer,
) -> Result<BuiltChildren, String> {
    let plan = build.plan;
    let backend_kind = plan.manifest.backend_config()?.backend_kind;
    let work_root = plan.out_dir.join("work");
    let mut built = BuiltChildren {
        images: Vec::with_capacity(plan.children.len()),
        segments: vec![0; plan.children.len()],
        peak_replay_rows: 0,
    };
    for (index, child) in plan.children.iter().enumerate() {
        if plan.options.only_child.is_some_and(|only| only != index) {
            eprintln!(
                "reshard: child {index} ({}): not built (--only-child names another child)",
                child.leaf
            );
            built.images.push(build.catalog_image(index, 0, 0));
            continue;
        }
        let builder = BucketBuilder {
            build,
            index,
            work: work_root.join(format!("c{index:05}")),
        };
        let mut placer = CatalogPlacer::open(build, index, &backend_kind)?;
        std::fs::create_dir_all(&builder.work)
            .map_err(|error| format!("mkdir {}: {error}", builder.work.display()))?;
        let segments = if plan.options.build_threads <= 1 {
            build_serial(&builder, &mut placer, analyze)?
        } else {
            build_threaded(&builder, &mut placer)?
        };
        built.segments[index] += segments;
        built.peak_replay_rows = built.peak_replay_rows.max(placer.peak_replay_rows);
        built.images.push(placer.finish(&builder.work, segments)?);
    }
    if work_root.exists() {
        std::fs::remove_dir_all(&work_root)
            .map_err(|error| format!("remove work {}: {error}", work_root.display()))?;
    }
    Ok(built)
}

/// Seals one spill bucket of a child into a staged image; shared by the
/// build threads.
struct BucketBuilder<'a> {
    build: &'a TreeBuild<'a>,
    index: usize,
    work: PathBuf,
}

impl BucketBuilder<'_> {
    /// One spill bucket replayed and sealed into a staged image at
    /// `slot`; `None` for an empty bucket.
    fn build_bucket(
        &self,
        bucket: u32,
        ordinal: usize,
        slot: u64,
        analyze: &mut Analyzer,
    ) -> Result<Option<(ChildImage, u64)>, String> {
        let replay = self.build.replay(self.index, bucket..bucket + 1)?;
        let rows = live_rows(&replay);
        if rows == 0 {
            return Ok(None);
        }
        let image = self
            .build
            .image(self.index, replay, &self.work, ordinal, slot, analyze)?;
        Ok(Some((image, rows)))
    }
}

/// Appends a child's staged images to its catalog, in bucket order.
struct CatalogPlacer<'a> {
    build: &'a TreeBuild<'a>,
    index: usize,
    catalog: SegmentCatalog,
    root: PathBuf,
    backend_kind: &'a str,
    documents: u64,
    vectors: u64,
    peak_replay_rows: u64,
}

impl<'a> CatalogPlacer<'a> {
    fn open(build: &'a TreeBuild<'a>, index: usize, backend_kind: &'a str) -> Result<Self, String> {
        let child = &build.plan.children[index];
        let index_path = build.plan.out_dir.join(format!("shard-{index}.tv"));
        let root = crate::node::segments_root(&index_path);
        if root.exists() {
            return Err(format!(
                "child {index} ({}): {} already exists; a re-placement split writes a fresh \
                 catalog",
                child.leaf,
                root.display()
            ));
        }
        Ok(Self {
            build,
            index,
            catalog: SegmentCatalog::open(&root)?,
            root,
            backend_kind,
            documents: 0,
            vectors: 0,
            peak_replay_rows: 0,
        })
    }

    fn leaf(&self) -> &str {
        &self.build.plan.children[self.index].leaf
    }

    fn cut_column(&self) -> Option<&str> {
        match &self.build.plan.options.cut {
            SpillCut::Column { column, .. } => Some(column.as_str()),
            SpillCut::Hash => None,
        }
    }

    /// A staged image into the catalog: the checks, the live sidecar, the
    /// append, the staged files removed, the caches dropped. Returns the
    /// segment's rows.
    fn place(
        &mut self,
        bucket: u32,
        ordinal: usize,
        slot: u64,
        image: ChildImage,
    ) -> Result<u64, String> {
        let (index, rows) = (self.index, image_rows(&image));
        let bound = self.build.plan.slot_bound(index);
        check_slot_bound(index, self.leaf(), slot, rows, bound)?;
        if image.num_vectors != 0 && image.num_vectors != image.num_documents {
            return Err(format!(
                "child {index} ({}) bucket {bucket}: {} vectors but {} documents; a sealed \
                 segment is aligned (one document per vector) or has no vectors at all",
                self.leaf(),
                image.num_vectors,
                image.num_documents
            ));
        }
        let Some(bm25_path) = image.bm25_path.as_deref() else {
            return Err(format!(
                "child {index} ({}) bucket {bucket}: no BM25 image was written for {} documents",
                self.leaf(),
                image.num_documents
            ));
        };
        let live_path = crate::node::live_docs_sidecar_path(&image.vector_path);
        crate::live_docs::LiveDocs::default()
            .write(&live_path, rows)
            .map_err(|error| format!("write live docs {}: {error}", live_path.display()))?;
        let segment_id = self.append(&image, ordinal, slot, bm25_path, &live_path)?;
        remove_staged(&image, bm25_path, &live_path)?;
        // The bucket's spill and the sealed segment are done with: out of
        // the page cache.
        let spill_dir = &self.build.spill.dirs[index];
        drop_page_cache(&crate::wal::bucket_path(spill_dir, bucket));
        drop_page_cache(&analysis_sidecar_path(spill_dir, bucket));
        drop_segment_cache(&self.root, &segment_id);
        self.documents += image.num_documents;
        self.vectors += image.num_vectors;
        Ok(rows)
    }

    /// Seal the staged image as segment `ordinal`; returns its id.
    fn append(
        &self,
        image: &ChildImage,
        ordinal: usize,
        slot: u64,
        bm25_path: &Path,
        live_path: &Path,
    ) -> Result<String, String> {
        let (index, generation) = (self.index, self.build.generation);
        let segment_id = format!("g{generation}-c{index}-p{ordinal}");
        let has_vectors = image.num_vectors != 0;
        self.catalog
            .append(SegmentSource {
                multi_res_path: image.multi_res_path.as_deref(),
                segment_id: &segment_id,
                generation,
                // Labels are shard-local: the node adds its slot offset
                // when it serves the catalog.
                base_label: slot - self.build.plan.slot_offsets[index],
                backend_kind: self.backend_kind,
                vector_path: has_vectors.then_some(image.vector_path.as_path()),
                exact_vector_path: has_vectors.then_some(image.exact_vector_path.as_path()),
                bm25_path,
                live_docs_path: live_path,
                partition_column: self.cut_column(),
            })
            .map_err(|error| {
                format!(
                    "child {index} ({}): seal segment {segment_id}: {error}",
                    self.leaf()
                )
            })?;
        Ok(segment_id)
    }

    /// The child's catalog is complete: check its rows against routing,
    /// publish the cut column, and remove the staging directory.
    fn finish(self, work: &Path, segments: usize) -> Result<ChildImage, String> {
        let (index, leaf) = (self.index, self.leaf());
        let placed = self.build.spill.stats.placed[index];
        if self.documents != placed {
            return Err(format!(
                "child {index} ({leaf}): the sealed segments hold {} documents, routing counted \
                 {placed}",
                self.documents
            ));
        }
        if let Some(column) = self.cut_column() {
            self.catalog
                .publish_partition_key(Some(column.to_string()))
                .map_err(|error| {
                    format!("child {index} ({leaf}): publish partition key: {error}")
                })?;
        }
        std::fs::remove_dir_all(work)
            .map_err(|error| format!("remove work {}: {error}", work.display()))?;
        let image = self
            .build
            .catalog_image(index, self.vectors, self.documents);
        eprintln!(
            "reshard: child {index} ({leaf}): {} vectors, {} documents in {segments} segments -> \
             {}",
            self.vectors,
            self.documents,
            image.vector_path.display()
        );
        Ok(image)
    }
}

/// The staged files of a sealed image, removed once the catalog holds
/// their copies.
fn remove_staged(image: &ChildImage, bm25_path: &Path, live_path: &Path) -> Result<(), String> {
    let retained_path = crate::multi_res::sidecar_path(&image.vector_path);
    let retained_declaration = crate::multi_res_storage::declaration_path(&retained_path);
    for path in [
        retained_path.as_path(),
        retained_declaration.as_path(),
        image.vector_path.as_path(),
        image.exact_vector_path.as_path(),
        bm25_path,
        live_path,
    ] {
        if path.exists() {
            std::fs::remove_file(path)
                .map_err(|error| format!("remove staged {}: {error}", path.display()))?;
        }
    }
    Ok(())
}

/// One bucket at a time on this thread, slots dense in bucket order.
/// Returns the segments sealed.
fn build_serial(
    builder: &BucketBuilder<'_>,
    placer: &mut CatalogPlacer<'_>,
    analyze: &mut Analyzer,
) -> Result<usize, String> {
    let mut built = 0usize;
    let mut next_slot = builder.build.plan.slot_offsets[builder.index];
    for bucket in 0..builder.build.spill.spill_bucket_count {
        let Some((image, rows)) = builder.build_bucket(bucket, built, next_slot, analyze)? else {
            continue;
        };
        placer.peak_replay_rows = placer.peak_replay_rows.max(rows);
        let image_rows = placer.place(bucket, built, next_slot, image)?;
        next_slot = next_slot
            .checked_add(image_rows)
            .ok_or_else(|| "re-placement split slot overflow".to_string())?;
        built += 1;
    }
    Ok(built)
}

/// Threaded: every bucket's slot range comes from the spill's own counts,
/// so buckets build in any order and the segments append in bucket order,
/// the same catalog the one-thread build writes.
fn build_threaded(
    builder: &BucketBuilder<'_>,
    placer: &mut CatalogPlacer<'_>,
) -> Result<usize, String> {
    let (build, index) = (builder.build, builder.index);
    let counts = &build.spill.stats.bucket_docs[index];
    let mut plan: Vec<(u32, usize, u64)> = Vec::new();
    let mut slot = build.plan.slot_offsets[index];
    for (bucket, &n) in counts.iter().enumerate() {
        if n == 0 {
            continue;
        }
        plan.push((bucket as u32, plan.len(), slot));
        slot = slot
            .checked_add(n)
            .ok_or_else(|| "re-placement split slot overflow".to_string())?;
    }
    let options = build.plan.options;
    let parallel = ParallelBuild::new(
        options.build_threads,
        options.build_queue,
        plan.len(),
        "bucket",
    );
    let leaf = &build.plan.children[index].leaf;
    parallel.run(
        &plan,
        |&(bucket, ordinal, slot)| {
            let mut refuse = refusing_analyzer(REFUSAL);
            builder
                .build_bucket(bucket, ordinal, slot, &mut *refuse)?
                .ok_or_else(|| {
                    format!(
                        "child {index} ({leaf}) bucket {bucket}: the spill counted {} documents \
                         and the replay found none",
                        counts[bucket as usize]
                    )
                })
        },
        |&(bucket, ordinal, slot), (image, rows)| {
            placer.peak_replay_rows = placer.peak_replay_rows.max(rows);
            let image_rows = placer.place(bucket, ordinal, slot, image)?;
            if image_rows != counts[bucket as usize] {
                return Err(format!(
                    "child {index} ({leaf}) bucket {bucket}: the spill counted {} documents, the \
                     sealed segment holds {image_rows} rows",
                    counts[bucket as usize]
                ));
            }
            Ok(())
        },
    )?;
    Ok(plan.len())
}
