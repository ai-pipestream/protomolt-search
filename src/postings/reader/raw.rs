//! Raw map accessors on [`Bm25Reader`]: directory entries, doc and
//! occurrence runs, stored texts, and lineages.

use super::{Bm25Reader, FieldSlice};
use crate::postings::DocLineage;

impl Bm25Reader {
    /// The 34 B directory entry: `(term bytes, doc_run_off, skip_run_off,
    /// occ_run_off, df)`, with run offsets rebased to absolute file offsets
    /// via `field.run_base`.
    pub(super) fn directory_entry(
        &self,
        field: &FieldSlice,
        i: u32,
    ) -> (&[u8], u64, u64, u64, u32) {
        let e = field.directory_off as usize + 4 + 34 * i as usize;
        let doc_run_off =
            u64::from_le_bytes(self.map[e..e + 8].try_into().unwrap()) + field.run_base;
        let skip_run_off =
            u64::from_le_bytes(self.map[e + 8..e + 16].try_into().unwrap()) + field.run_base;
        let occ_run_off =
            u64::from_le_bytes(self.map[e + 16..e + 24].try_into().unwrap()) + field.run_base;
        let df = u32::from_le_bytes(self.map[e + 24..e + 28].try_into().unwrap());
        let stored = u32::from_le_bytes(self.map[e + 28..e + 32].try_into().unwrap()) as usize;
        let len = u16::from_le_bytes(self.map[e + 32..e + 34].try_into().unwrap()) as usize;
        let blob_off = field.directory_off as usize + 4 + 34 * field.n_terms as usize + stored;
        (
            &self.map[blob_off..blob_off + len],
            doc_run_off,
            skip_run_off,
            occ_run_off,
            df,
        )
    }

    /// `(doc_run_off, skip_run_off, occ_run_off, df)` for `term` in `field`,
    /// or `None` when the term is absent.
    pub(in crate::postings) fn directory_lookup(
        &self,
        field: &FieldSlice,
        term: &str,
    ) -> Option<(u64, u64, u64, u32)> {
        let i = self.directory_index(field, term)?;
        let (_, doc, skip, occ, df) = self.directory_entry(field, i);
        Some((doc, skip, occ, df))
    }

    /// Directory entry index of `term` in `field`, by binary search over the
    /// byte-sorted term directory.
    pub(super) fn directory_index(&self, field: &FieldSlice, term: &str) -> Option<u32> {
        let (mut lo, mut hi) = (0u32, field.n_terms);
        while lo < hi {
            let mid = (lo + hi) / 2;
            let (bytes, _, _, _, _) = self.directory_entry(field, mid);
            match bytes.cmp(term.as_bytes()) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => return Some(mid),
            }
        }
        None
    }

    /// Index of the posting for `doc_id` in a term's fixed-stride doc run
    /// (doc ids ascending), or `None` when absent.
    pub(super) fn posting_index(&self, doc_run_off: usize, df: u32, doc_id: u32) -> Option<usize> {
        let (mut lo, mut hi) = (0usize, df as usize);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.u32_at(doc_run_off + 12 * mid) < doc_id {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        (lo < df as usize && self.u32_at(doc_run_off + 12 * lo) == doc_id).then_some(lo)
    }

    pub(super) fn u32_at(&self, off: usize) -> u32 {
        u32::from_le_bytes(self.map[off..off + 4].try_into().unwrap())
    }

    /// The doc-run entry for posting `i`: `(doc_id, tf, occ_start)`.
    pub(super) fn doc_entry(&self, doc_run_off: usize, i: usize) -> (u32, u32, u32) {
        let e = doc_run_off + 12 * i;
        (self.u32_at(e), self.u32_at(e + 4), self.u32_at(e + 8))
    }

    /// The `occ_start` of posting `j`; `j == df` reads the trailing
    /// sentinel, so any posting's occurrence count is
    /// `occ_start(j + 1) - occ_start(j)` without a scan.
    pub(super) fn occ_start(&self, doc_run_off: usize, df: usize, j: usize) -> u32 {
        if j < df {
            self.u32_at(doc_run_off + 12 * j + 8)
        } else {
            self.u32_at(doc_run_off + 12 * df)
        }
    }

    /// Decode the occurrence pairs `[occ_start, occ_end)` of a term's
    /// occurrence run.
    pub(super) fn occ_slice(
        &self,
        occ_run_off: usize,
        occ_start: u32,
        occ_end: u32,
    ) -> Vec<(u32, u32)> {
        let mut out = Vec::with_capacity((occ_end - occ_start) as usize);
        for o in occ_start..occ_end {
            let e = occ_run_off + 8 * o as usize;
            out.push((self.u32_at(e), self.u32_at(e + 4)));
        }
        out
    }

    /// Walk the fixed-stride doc run without touching occurrence bytes.
    pub(super) fn for_each_doc_tf(&self, doc_run_off: usize, df: u32, f: &mut dyn FnMut(u32, u32)) {
        for i in 0..df as usize {
            let (doc_id, tf, _) = self.doc_entry(doc_run_off, i);
            f(doc_id, tf);
        }
    }

    /// The shared stored text of a document, if present.
    pub(super) fn read_text(&self, doc_id: u32) -> Option<String> {
        let slot = doc_id as usize;
        if slot >= self.n_slots() {
            return None;
        }
        let e = (self.text_index_off + 12 * doc_id as u64) as usize;
        let offset = u64::from_le_bytes(self.map[e..e + 8].try_into().unwrap()) + self.text_base;
        let len = u32::from_le_bytes(self.map[e + 8..e + 12].try_into().unwrap());
        if len == u32::MAX {
            return None;
        }
        let bytes = &self.map[offset as usize..offset as usize + len as usize];
        String::from_utf8(bytes.to_vec()).ok()
    }

    /// The shared lineage of a document, if ingested with one.
    pub(super) fn read_lineage(&self, doc_id: u32) -> Option<DocLineage> {
        let slot = doc_id as usize;
        if slot >= self.n_slots() {
            return None;
        }
        // The lineage section is variable stride (1 B absent marker,
        // 25 B present), so a fixed 25B-stride read lands anywhere —
        // correct only for dense all-present lineages. The lazily built
        // per-slot offset index makes random access exact.
        let index = self.lineage_index.get_or_init(|| {
            let base = self.lineages_off as usize;
            let mut offsets = Vec::with_capacity(self.n_slots());
            let mut cur = base;
            for _ in 0..self.n_slots() {
                offsets.push((cur - base) as u32);
                cur += if self.map[cur] == 0 { 1 } else { 25 };
            }
            offsets
        });
        let e = self.lineages_off as usize + index[slot] as usize;
        if self.map[e] == 0 {
            return None;
        }
        let parent_id = u64::from_le_bytes(self.map[e + 1..e + 9].try_into().unwrap());
        let group_id = u64::from_le_bytes(self.map[e + 9..e + 17].try_into().unwrap());
        let span_start = u32::from_le_bytes(self.map[e + 17..e + 21].try_into().unwrap());
        let span_end = u32::from_le_bytes(self.map[e + 21..e + 25].try_into().unwrap());
        Some(DocLineage {
            parent_id,
            group_id,
            span_start,
            span_end,
        })
    }
}
