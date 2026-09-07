//! Immutable FP32 files aligned to the segment catalog's physical row space.
use super::*;
use crate::segments::OpenedSegmentSet;
use std::collections::BTreeSet;
use std::sync::Arc;

#[derive(Debug)]
struct Part {
    base: usize,
    end: usize,
    store: Option<Arc<ExactVectorStore>>,
}

#[derive(Debug)]
pub(super) struct SegmentedExact {
    pub dim: usize,
    pub rows: usize,
    pub append_target: PathBuf,
    parts: Vec<Part>,
}

impl ExactVectorStore {
    /// An owned projection can only attach the exact images of its held catalog.
    /// Equal dimensions and row counts alone do not prove the same vector data.
    pub(crate) fn matches_segments(&self, set: &OpenedSegmentSet) -> bool {
        let Storage::Segmented(view) = &self.storage else {
            return false;
        };
        view.parts.len() == set.len()
            && view.parts.iter().enumerate().all(|(i, part)| {
                let metadata = set.metadata(i);
                part.base as u64 == metadata.base_label
                    && (part.end - part.base) as u64 == metadata.rows
                    && match (&part.store, set.exact_vectors(i)) {
                        (Some(held), Some(expected)) => Arc::ptr_eq(held, expected),
                        (None, None) => true,
                        _ => false,
                    }
            })
    }

    /// Share the FP32 images already validated and held by this catalog view.
    /// `len` counts physical rows, including document-only gaps. Scoring skips
    /// gaps; range reads and single-file exports refuse to invent their values.
    /// Tombstones and authorization remain the serving view's responsibility.
    pub fn from_segments(set: &OpenedSegmentSet, dim: usize) -> io::Result<Self> {
        validate_shape(dim, 0)?;
        let mut parts = Vec::with_capacity(set.len());
        let mut next = 0usize;
        for i in 0..set.len() {
            let metadata = set.metadata(i);
            let base = usize::try_from(metadata.base_label)
                .map_err(|_| invalid("exact segment base does not fit usize"))?;
            let rows = usize::try_from(metadata.rows)
                .map_err(|_| invalid("exact segment rows do not fit usize"))?;
            if base != next {
                return Err(invalid(
                    "exact segments must cover contiguous physical rows from zero",
                ));
            }
            next = base
                .checked_add(rows)
                .ok_or_else(|| invalid("exact segment row count overflow"))?;
            let store = set.exact_vectors(i).cloned();
            if let Some(store) = &store {
                if store.dim() != Some(dim) || store.len() != rows {
                    return Err(invalid(format!(
                        "exact segment {:?} shape differs from the catalog/provider",
                        metadata.segment_id
                    )));
                }
            }
            parts.push(Part {
                base,
                end: next,
                store,
            });
        }
        Ok(Self {
            storage: Storage::Segmented(SegmentedExact {
                dim,
                rows: next,
                parts,
                append_target: set.root().join("exact-tail"),
            }),
        })
    }
}

impl SegmentedExact {
    fn part(&self, slot: usize) -> Option<&Part> {
        let i = self.parts.partition_point(|part| part.end <= slot);
        self.parts.get(i).filter(|part| part.base <= slot)
    }
    pub fn contains_row(&self, slot: usize) -> bool {
        self.part(slot).is_some_and(|part| part.store.is_some())
    }
    pub fn score_one(&self, query: &[f32], slot: usize, dim: usize) -> io::Result<f32> {
        let part = self
            .part(slot)
            .ok_or_else(|| invalid("exact row is out of bounds"))?;
        let store = part
            .store
            .as_ref()
            .ok_or_else(|| invalid("exact row has no vector"))?;
        store.score_one(query, slot - part.base, dim)
    }
    pub fn row_values(&self, from: usize, to: usize) -> io::Result<Vec<f32>> {
        let mut out = Vec::new();
        let mut cursor = from;
        while cursor < to {
            let part = self
                .part(cursor)
                .ok_or_else(|| invalid("exact row is out of bounds"))?;
            let end = to.min(part.end);
            let store = part
                .store
                .as_ref()
                .ok_or_else(|| invalid("exact row range includes rows without vectors"))?;
            out.extend(store.row_values(cursor - part.base, end - part.base)?);
            cursor = end;
        }
        Ok(out)
    }
    pub fn write_payload(
        &self,
        out: &mut impl Write,
        digest: &mut crate::sha256::Sha256,
    ) -> io::Result<()> {
        if self.parts.iter().any(|part| part.store.is_none()) {
            return Err(invalid("a sparse exact-vector view must persist through its segment catalog, not a dense sidecar"));
        }
        for part in &self.parts {
            part.store
                .as_ref()
                .expect("checked above")
                .write_payload(out, digest)?;
        }
        Ok(())
    }
    pub fn verify_payload(&self) -> io::Result<()> {
        for store in self.parts.iter().filter_map(|part| part.store.as_ref()) {
            store.verify_payload()?;
        }
        Ok(())
    }
    pub fn release_pages(&self) -> io::Result<()> {
        for store in self.parts.iter().filter_map(|part| part.store.as_ref()) {
            store.release_pages()?;
        }
        Ok(())
    }
    pub fn mapped_pages(
        &self,
        slot: usize,
        row_bytes: usize,
        pages: &mut BTreeSet<(usize, usize)>,
    ) {
        if let Some(part) = self.part(slot) {
            if let Some(store) = &part.store {
                store.mapped_pages(slot - part.base, row_bytes, pages);
            }
        }
    }
}
