//! Read access to one opened, verified segment set.

use super::*;

impl OpenedSegmentSet {
    pub fn binding(&self) -> Option<&StoredBinding> {
        self.binding.as_ref()
    }

    pub fn generation_declaration(
        &self,
    ) -> Option<&crate::pb::storage::IndexGenerationDeclaration> {
        self.declaration.as_ref()
    }

    /// Whether segment `i` is the same opened segment as `other`'s
    /// segment `j` (shared across a publish), for tests.
    pub fn shares_segment(&self, i: usize, other: &OpenedSegmentSet, j: usize) -> bool {
        Arc::ptr_eq(&self.segments[i], &other.segments[j])
    }

    /// Whether every sealed segment proves its parents colocated on this
    /// shard; an empty set proves it trivially.
    pub fn parents_colocated(&self) -> bool {
        self.manifest()
            .segments
            .iter()
            .all(|segment| segment.parents_colocated)
    }

    /// The manifest this set was opened from. Its `epoch` is the epoch at
    /// open; [`Self::epoch`] is the published one.
    pub fn manifest(&self) -> &SegmentSetManifest {
        &self.manifest
    }

    /// The set's published epoch.
    pub fn epoch(&self) -> u64 {
        self.manifest.epoch
    }

    /// The manifest as it is published: `manifest()` with the current
    /// epoch.
    pub fn published_manifest(&self) -> SegmentSetManifest {
        self.manifest.clone()
    }

    /// The catalog root this set was opened from.
    pub fn root(&self) -> &Path {
        &self.root
    }

    /// Number of segments in the set, in ascending base order.
    pub fn len(&self) -> usize {
        self.segments.len()
    }

    pub fn is_empty(&self) -> bool {
        self.segments.is_empty()
    }

    pub fn metadata(&self, i: usize) -> &SegmentMetadata {
        &self.segments[i].metadata
    }

    pub fn bm25(&self, i: usize) -> &Bm25Reader {
        &self.segments[i].bm25
    }

    /// Segment `i`'s vector image, `None` for a documents-only segment.
    pub fn vector(&self, i: usize) -> Option<&VectorIndex> {
        self.segments[i].vector.as_deref()
    }

    pub(crate) fn vector_arc(&self, i: usize) -> Option<&Arc<VectorIndex>> {
        self.segments[i].vector.as_ref()
    }

    pub(crate) fn retained_segment(&self, i: usize) -> &crate::multi_res::MultiResVectors {
        &self.segments[i].retained
    }

    pub(crate) fn retained_vectors(&self) -> Result<crate::multi_res::MultiResVectors, String> {
        let mut rows = crate::multi_res::MultiResVectors::default();
        for segment in &self.segments {
            let base = u32::try_from(segment.metadata.base_label)
                .map_err(|_| "retained segment base exceeds u32")?;
            rows.append_at(base, &segment.retained)
                .map_err(|error| error.to_string())?;
        }
        Ok(rows)
    }

    /// The immutable FP32 image opened and verified with this segment snapshot.
    /// Documents-only segments have no exact rows. Bitmap-only publications
    /// share this image while retaining their own tombstone overlays.
    pub fn exact_vectors(&self, i: usize) -> Option<&Arc<ExactVectorStore>> {
        self.segments[i].exact.as_ref()
    }

    /// Add this snapshot's committed tombstones to a generation-wide serving
    /// overlay. Preserve newer overlay deletes and every view already held.
    /// Work visits bitmap words and set bits, not each physical document.
    pub fn merge_tombstones(&self, live: &mut LiveDocs) -> Result<(), String> {
        for segment in &self.segments {
            usize::try_from(segment.metadata.end_label_exclusive()?)
                .map_err(|_| "segment tombstone row range does not fit usize")?;
        }
        for segment in &self.segments {
            let Some(words) = segment.live_docs.words() else {
                continue;
            };
            let base = segment.metadata.base_label as usize;
            for (ordinal, &word) in words.iter().enumerate() {
                let mut remaining = word;
                while remaining != 0 {
                    let bit = remaining.trailing_zeros() as usize;
                    live.delete(base + ordinal * 64 + bit);
                    remaining &= remaining - 1;
                }
            }
        }
        Ok(())
    }

    pub fn live_docs(&self, i: usize) -> &LiveDocs {
        &self.segments[i].live_docs
    }
}
