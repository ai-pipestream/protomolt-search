//! The vector side of a segmented shard (`docs/immutable-segments.md`):
//! one [`VectorProvider`] over the sealed segments' images plus a tail
//! image the node adds rows to. Rows are one positional space, the
//! same one the segmented BM25 shard uses: segment `i` covers
//! `[base_i, base_i + rows_i)`, a documents-only segment covers rows
//! without vectors, and the tail follows the last segment.
//!
//! A search is the union of the parts' searches under the caller's
//! allowlist sliced per part; a streaming search runs the parts in row
//! order, carrying every raised floor into the next part and remapping
//! each batch's slots. Nothing is approximated: every part is scanned
//! exhaustively under the same options.

use std::path::Path;
use std::sync::Arc;

use crate::segments::OpenedSegmentSet;
use crate::vector::{
    VectorBackendConfig, VectorBackendDescriptor, VectorError, VectorIndex, VectorProvider,
    VectorSearchOptions, VectorSearchResults, VectorStreamBatch, VectorStreamControl,
    VectorStreamSummary,
};

/// One sealed part's rows, with its image when it has vectors.
struct Part {
    base: usize,
    rows: usize,
    has_vectors: bool,
}

/// The former tail image a seal in flight is writing out: read-only,
/// shared with the seal, and searched as one more part until the
/// catalog publishes it.
struct FrozenImage {
    base: usize,
    rows: usize,
    image: Arc<VectorIndex>,
}

pub struct SegmentedProvider {
    set: Arc<OpenedSegmentSet>,
    parts: Vec<Part>,
    frozen: Option<FrozenImage>,
    tail: VectorIndex,
    tail_base: usize,
}

impl SegmentedProvider {
    /// Wrap the catalog snapshot with `tail` as the mutable image. The
    /// tail must be empty and share the sealed images' backend and
    /// scoring fingerprint; a mismatch refuses by name.
    pub fn open(set: Arc<OpenedSegmentSet>, tail: VectorIndex) -> Result<Self, VectorError> {
        if !tail.is_empty() {
            return Err(VectorError::new("a segmented tail image must start empty"));
        }
        let (parts, next) = Self::parts_of(&set, &tail)?;
        Ok(SegmentedProvider {
            set,
            parts,
            frozen: None,
            tail,
            tail_base: next,
        })
    }

    /// The sealed parts of `set` in row order and the row they end at,
    /// checked against the tail's provider state.
    fn parts_of(
        set: &Arc<OpenedSegmentSet>,
        tail: &VectorIndex,
    ) -> Result<(Vec<Part>, usize), VectorError> {
        let mut parts = Vec::with_capacity(set.len());
        let mut next = 0usize;
        for i in 0..set.len() {
            let m = set.metadata(i);
            let base = usize::try_from(m.base_label)
                .map_err(|_| VectorError::new("segment base does not fit usize"))?;
            let rows = usize::try_from(m.rows)
                .map_err(|_| VectorError::new("segment rows do not fit usize"))?;
            if base != next {
                return Err(VectorError::new(format!(
                    "segment {:?} starts at row {base}, not at the previous part's end {next}",
                    m.segment_id
                )));
            }
            if let Some(image) = set.vector(i) {
                let held = image.descriptor();
                let tail_desc = tail.descriptor();
                if held.scoring_fingerprint != tail_desc.scoring_fingerprint
                    || held.backend_kind != tail_desc.backend_kind
                {
                    return Err(VectorError::new(format!(
                        "segment {:?} scores under {}/{} but the tail image is {}/{}; a segmented \
                         shard scores every row under one provider state",
                        m.segment_id,
                        held.backend_kind,
                        held.scoring_fingerprint,
                        tail_desc.backend_kind,
                        tail_desc.scoring_fingerprint
                    )));
                }
            }
            parts.push(Part {
                base,
                rows,
                has_vectors: set.vector(i).is_some(),
            });
            next += rows;
        }
        Ok((parts, next))
    }

    pub fn tail(&self) -> &VectorIndex {
        &self.tail
    }

    pub fn tail_base(&self) -> usize {
        self.tail_base
    }

    pub fn snapshot(&self) -> &Arc<OpenedSegmentSet> {
        &self.set
    }

    /// Freeze the tail image for a seal covering `rows` rows of the
    /// shard (the tail image holds either all of them or none, for a
    /// documents-only tail): the image becomes a read-only part, `fresh`
    /// becomes the tail after those rows, and the frozen image is
    /// returned shared so the seal can write it with no lock held.
    pub fn freeze_tail(
        &mut self,
        fresh: VectorIndex,
        rows: usize,
    ) -> Result<Arc<VectorIndex>, VectorError> {
        if self.frozen.is_some() {
            return Err(VectorError::new(
                "a seal is already in flight on this shard",
            ));
        }
        if !fresh.is_empty() {
            return Err(VectorError::new("a fresh tail image must start empty"));
        }
        let held = self.tail.len();
        if held != 0 && held != rows {
            return Err(VectorError::new(format!(
                "the tail image holds {held} vectors but the seal covers {rows} rows"
            )));
        }
        let image = Arc::new(std::mem::replace(&mut self.tail, fresh));
        self.frozen = Some(FrozenImage {
            base: self.tail_base,
            rows: held,
            image: Arc::clone(&image),
        });
        self.tail_base += rows;
        Ok(image)
    }

    /// The frozen image, when a seal is in flight: `(base, rows, image)`.
    pub fn frozen(&self) -> Option<(usize, usize, &Arc<VectorIndex>)> {
        self.frozen.as_ref().map(|f| (f.base, f.rows, &f.image))
    }

    /// Adopt the catalog snapshot that now contains the frozen rows as a
    /// sealed segment; the tail stays. The set's rows must end where the
    /// tail begins, or the snapshot is refused and nothing changes.
    pub fn republish(&mut self, set: Arc<OpenedSegmentSet>) -> Result<(), VectorError> {
        let (parts, next) = Self::parts_of(&set, &self.tail)?;
        if next != self.tail_base {
            return Err(VectorError::new(format!(
                "the published set covers {next} rows but the tail image starts at {}; a \
                 republish must seal exactly the frozen rows",
                self.tail_base
            )));
        }
        self.set = set;
        self.parts = parts;
        self.frozen = None;
        Ok(())
    }

    /// Every part with an image, in row order — sealed parts, the frozen
    /// image when a seal is in flight, then the tail: `(base, rows, image)`.
    fn images(&self) -> Vec<(usize, usize, &VectorIndex)> {
        let mut out: Vec<(usize, usize, &VectorIndex)> = self
            .parts
            .iter()
            .enumerate()
            .filter(|(_, p)| p.has_vectors)
            .map(|(i, p)| {
                (
                    p.base,
                    p.rows,
                    self.set.vector(i).expect("part has vectors"),
                )
            })
            .collect();
        if let Some(f) = self.frozen.as_ref().filter(|f| f.rows > 0) {
            out.push((f.base, f.rows, &*f.image));
        }
        if !self.tail.is_empty() {
            out.push((self.tail_base, self.tail.len(), &self.tail));
        }
        out
    }
}

impl VectorProvider for SegmentedProvider {
    fn descriptor(&self) -> VectorBackendDescriptor {
        self.tail.descriptor()
    }

    fn backend_config(&self) -> Result<VectorBackendConfig, VectorError> {
        self.tail.backend_config()
    }

    fn len(&self) -> usize {
        self.tail_base + self.tail.len()
    }

    fn dimension(&self) -> Option<usize> {
        self.tail.dim_opt()
    }

    fn add(&mut self, vectors: &[f32], dimension: usize) -> Result<(), VectorError> {
        self.tail.add(vectors, dimension)
    }

    fn prepare(&mut self) -> Result<(), VectorError> {
        self.tail.prepare()
    }

    fn write(&self, path: &Path) -> Result<(), VectorError> {
        Err(VectorError::new(format!(
            "a segmented vector index is sealed segment by segment into its catalog, not written \
             as one image ({})",
            path.display()
        )))
    }

    fn search(
        &self,
        queries: &[f32],
        k: usize,
        options: VectorSearchOptions<'_>,
    ) -> Result<VectorSearchResults, VectorError> {
        let dim = self
            .dimension()
            .ok_or_else(|| VectorError::new("segmented index has no dimension yet"))?;
        let nq = queries.len().checked_div(dim).unwrap_or(0);
        let mut per_query: Vec<Vec<(f32, i64)>> = vec![Vec::new(); nq];
        for (base, rows, image) in self.images() {
            let allow: Option<Vec<bool>> = match options.allow {
                Some(allow) => {
                    let slice = &allow[base.min(allow.len())..(base + rows).min(allow.len())];
                    if !slice.iter().any(|&ok| ok) {
                        continue;
                    }
                    Some(slice.to_vec())
                }
                None => None,
            };
            let mut part_options = VectorSearchOptions::new();
            if let Some(allow) = allow.as_deref() {
                part_options = part_options.with_allowlist(allow);
            }
            if let Some(floor) = options.minimum_score {
                part_options = part_options.with_minimum_score(floor);
            }
            let results = image.try_search(queries, k.min(rows), part_options)?;
            for (q, hits) in per_query.iter_mut().enumerate() {
                for (&slot, &score) in results
                    .slots_for_query(q)
                    .iter()
                    .zip(results.scores_for_query(q))
                {
                    if slot >= 0 && score.is_finite() {
                        hits.push((score, slot + base as i64));
                    }
                }
            }
        }
        let mut scores = Vec::with_capacity(nq * k);
        let mut slots = Vec::with_capacity(nq * k);
        for hits in per_query.iter_mut() {
            hits.sort_by(|a, b| b.0.total_cmp(&a.0).then(a.1.cmp(&b.1)));
            hits.truncate(k);
            for &(score, slot) in hits.iter() {
                scores.push(score);
                slots.push(slot);
            }
            for _ in hits.len()..k {
                scores.push(f32::NEG_INFINITY);
                slots.push(-1);
            }
        }
        Ok(VectorSearchResults {
            scores,
            slots,
            query_count: nq,
            result_count: k,
        })
    }

    fn search_streaming_controlled(
        &self,
        queries: &[f32],
        options: VectorSearchOptions<'_>,
        sink: &mut dyn FnMut(&VectorStreamBatch<'_>) -> VectorStreamControl,
        control: &mut dyn FnMut() -> VectorStreamControl,
    ) -> Result<VectorStreamSummary, VectorError> {
        let dim = self
            .dimension()
            .ok_or_else(|| VectorError::new("segmented index has no dimension yet"))?;
        let nq = queries.len().checked_div(dim).unwrap_or(0);
        let floor = std::cell::Cell::new(options.minimum_score);
        let stopped = std::cell::Cell::new(false);
        let mut emitted = 0usize;
        let mut units = 0usize;
        for (base, rows, image) in self.images() {
            if stopped.get() {
                break;
            }
            let allow: Option<Vec<bool>> = match options.allow {
                Some(allow) => {
                    let slice = &allow[base.min(allow.len())..(base + rows).min(allow.len())];
                    if !slice.iter().any(|&ok| ok) {
                        continue;
                    }
                    Some(slice.to_vec())
                }
                None => None,
            };
            let mut part_options = VectorSearchOptions::new();
            if let Some(allow) = allow.as_deref() {
                part_options = part_options.with_allowlist(allow);
            }
            if let Some(f) = floor.get() {
                part_options = part_options.with_minimum_score(f);
            }
            let note = |verdict: VectorStreamControl| match verdict {
                VectorStreamControl::RaiseFloor(f) => {
                    floor.set(Some(floor.get().map_or(f, |cur| cur.max(f))));
                }
                VectorStreamControl::Stop => stopped.set(true),
                VectorStreamControl::Continue => {}
            };
            let mut remapped: Vec<i64> = Vec::new();
            let mut part_sink = |batch: &VectorStreamBatch<'_>| -> VectorStreamControl {
                remapped.clear();
                remapped.extend(
                    batch
                        .slots
                        .iter()
                        .map(|&s| if s < 0 { s } else { s + base as i64 }),
                );
                let outward = VectorStreamBatch {
                    query_index: batch.query_index,
                    block_base: batch.block_base,
                    scores: batch.scores,
                    slots: &remapped,
                };
                let verdict = sink(&outward);
                note(verdict);
                verdict
            };
            let mut part_control = || -> VectorStreamControl {
                let verdict = control();
                note(verdict);
                verdict
            };
            let summary = image.try_search_streaming_controlled(
                queries,
                part_options,
                &mut part_sink,
                &mut part_control,
            )?;
            emitted += summary.emitted;
            units += summary.units_scanned;
            if !summary.completed {
                stopped.set(true);
            }
        }
        Ok(VectorStreamSummary {
            query_count: nq,
            emitted,
            units_scanned: units,
            completed: !stopped.get(),
        })
    }

    fn as_segmented_mut(&mut self) -> Option<&mut SegmentedProvider> {
        Some(self)
    }

    fn as_segmented(&self) -> Option<&SegmentedProvider> {
        Some(self)
    }
}
