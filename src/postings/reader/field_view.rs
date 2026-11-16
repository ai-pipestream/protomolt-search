//! Per-field `Bm25Index` view (`FieldView`) and replay transpose (`FieldTranspose`).

use super::super::codec::{FileImpacts, ImpactCursor};
use super::super::{Bm25Index, DocLineage, PostingCallback};
use super::{Bm25Reader, FieldSlice};

/// One field of an open `.bm25` file as its own [`Bm25Index`]:
/// directory lookups, postings walks, and
/// impact cursors run against the field's sections; texts, lineages,
/// and the document count are the file's shared ones.
#[derive(Clone, Copy)]
pub struct FieldView<'a> {
    pub(super) reader: &'a Bm25Reader,
    pub(super) field: &'a FieldSlice,
}

impl<'a> FieldView<'a> {
    pub(crate) fn prefix_iter(&self, prefix: &str) -> Box<dyn Iterator<Item = String> + 'a> {
        let view = *self;
        let prefix = prefix.to_owned();
        Box::new(
            (view.prefix_lower_bound(&prefix)..view.field.n_terms)
                .map(move |i| String::from_utf8_lossy(view.directory_term(i)).into_owned())
                .take_while(move |term| term.starts_with(&prefix)),
        )
    }

    /// [`Bm25Index::impacts`] with the cursor's borrow tied to the
    /// underlying reader rather than this view value, so a temporary
    /// view (`reader.field(0).impacts(..)`) can hand out a cursor.
    pub(super) fn impacts_inner(self, term: &str) -> Option<ImpactCursor<'a>> {
        self.file_impacts(term).map(ImpactCursor::single)
    }

    /// This file's own cursor for `term`, the part a segmented shard
    /// chains (`src/segmented.rs`); `None` when the term is absent.
    pub(crate) fn file_impacts(self, term: &str) -> Option<FileImpacts<'a>> {
        let (doc_run_off, skip_run_off, occ_run_off, df) =
            self.reader.directory_lookup(self.field, term)?;
        Some(FileImpacts::new(
            &self.reader.map,
            doc_run_off as usize,
            occ_run_off as usize,
            skip_run_off as usize,
            df,
        ))
    }

    fn posting_ordinal_slice(&self, section: u64, term: &str, doc_id: u32) -> Option<Vec<u32>> {
        let r = self.reader;
        let i = r.directory_index(self.field, term)?;
        let (_, doc_run_off, _, _, df) = r.directory_entry(self.field, i);
        let doc_run_off = doc_run_off as usize;
        let j = r.posting_index(doc_run_off, df, doc_id)?;
        let section = section as usize;
        let base = r.u32_at(section + 4 + 4 * i as usize) as usize;
        let data = section + 4 + 4 * (self.field.n_terms as usize + 1);
        let occ_start = r.u32_at(doc_run_off + 12 * j + 8) as usize;
        let occ_end = r.occ_start(doc_run_off, df as usize, j + 1) as usize;
        Some(
            (occ_start..occ_end)
                .map(|o| r.u32_at(data + 4 * (base + o)))
                .collect(),
        )
    }
}

impl Bm25Index for FieldView<'_> {
    fn doc_count(&self) -> u64 {
        self.reader.doc_count
    }
    fn total_doc_length(&self) -> u64 {
        self.field.total_length
    }
    fn doc_length(&self, doc_id: u32) -> u32 {
        self.field
            .doc_lengths
            .get(doc_id as usize)
            .copied()
            .unwrap_or(0)
    }
    fn df(&self, term: &str) -> u32 {
        self.reader
            .directory_lookup(self.field, term)
            .map_or(0, |(_, _, _, df)| df)
    }
    fn for_each_posting(&self, term: &str, f: &mut PostingCallback) {
        let r = self.reader;
        let Some((doc_run_off, _, occ_run_off, df)) = r.directory_lookup(self.field, term) else {
            return;
        };
        let (doc_run_off, occ_run_off) = (doc_run_off as usize, occ_run_off as usize);
        for i in 0..df as usize {
            let (doc_id, tf, occ_start) = r.doc_entry(doc_run_off, i);
            let occ_end = r.occ_start(doc_run_off, df as usize, i + 1);
            let offsets = r.occ_slice(occ_run_off, occ_start, occ_end);
            f(doc_id, tf, &offsets);
        }
    }
    fn for_each_doc_tf(&self, term: &str, f: &mut dyn FnMut(u32, u32)) {
        let r = self.reader;
        let Some((doc_run_off, _, _, df)) = r.directory_lookup(self.field, term) else {
            return;
        };
        r.for_each_doc_tf(doc_run_off as usize, df, f);
    }
    fn posting_offsets(&self, term: &str, doc_id: u32) -> Vec<(u32, u32)> {
        let r = self.reader;
        let Some((doc_run_off, _, occ_run_off, df)) = r.directory_lookup(self.field, term) else {
            return Vec::new();
        };
        let doc_run_off = doc_run_off as usize;
        let Some(i) = r.posting_index(doc_run_off, df, doc_id) else {
            return Vec::new();
        };
        let occ_start = r.u32_at(doc_run_off + 12 * i + 8);
        let occ_end = r.occ_start(doc_run_off, df as usize, i + 1);
        r.occ_slice(occ_run_off as usize, occ_start, occ_end)
    }
    fn impacts(&self, term: &str) -> Option<ImpactCursor<'_>> {
        (*self).impacts_inner(term)
    }
    fn has_impacts(&self, term: &str) -> bool {
        self.reader.directory_lookup(self.field, term).is_some()
    }
    fn text(&self, doc_id: u32) -> Option<String> {
        self.reader.read_text(doc_id)
    }
    fn lineage(&self, doc_id: u32) -> Option<DocLineage> {
        self.reader.read_lineage(doc_id)
    }
    fn has_positions(&self) -> bool {
        self.field.positions_off.is_some()
    }
    fn has_sentences(&self) -> bool {
        self.field.sentences_off.is_some()
    }
    fn doc_sentences(&self, doc_id: u32) -> Option<Vec<(u32, u32)>> {
        let fi = self
            .reader
            .fields
            .iter()
            .position(|f| std::ptr::eq(f, self.field))
            .expect("a field view borrows one of its reader's fields");
        self.reader.field_doc_sentences(fi, doc_id)
    }
    fn posting_positions(&self, term: &str, doc_id: u32) -> Option<Vec<u32>> {
        let r = self.reader;
        let i = r.directory_index(self.field, term)?;
        let (_, doc_run_off, _, _, df) = r.directory_entry(self.field, i);
        r.posting_index(doc_run_off as usize, df, doc_id)?;
        let Some(section) = self.field.positions_off else {
            return Some(Vec::new());
        };
        self.posting_ordinal_slice(section, term, doc_id)
    }
    fn has_pos_lengths(&self) -> bool {
        self.field.positions_off.is_some() && self.field.pos_lengths_off.is_some()
    }
    fn posting_pos_lengths(&self, term: &str, doc_id: u32) -> Option<Vec<u32>> {
        let section = self.field.pos_lengths_off?;
        self.posting_ordinal_slice(section, term, doc_id)
    }
    fn has_splade(&self) -> bool {
        self.field.splade_range.is_some()
    }
    fn score_splade_with_floor(
        &self,
        query_weights: &[(String, f32)],
        query_l1_slack: f32,
        initial_floor: f32,
        k: usize,
        admitted: &dyn Fn(u32) -> bool,
    ) -> (
        Vec<(u32, f32)>,
        f32,
        crate::turbo_bridge::splade::SpladeTraversalStats,
    ) {
        let Some((off, len)) = self.field.splade_range else {
            return (Vec::new(), initial_floor.max(0.0), Default::default());
        };
        let bytes = &self.reader.map[off as usize..(off + len) as usize];
        match crate::turbo_bridge::splade::SpladeSectionReader::open(bytes) {
            Ok(reader) => reader.score_top_k_with_floor(
                query_weights,
                query_l1_slack,
                initial_floor,
                k,
                admitted,
            ),
            Err(_) => (Vec::new(), initial_floor.max(0.0), Default::default()),
        }
    }
    fn prefix_terms(&self, prefix: &str) -> Box<dyn Iterator<Item = String> + '_> {
        self.prefix_iter(prefix)
    }
    fn expand_prefix(&self, prefix: &str, cap: usize) -> Result<Vec<String>, usize> {
        let mut matches = Vec::new();
        let count = self.scan_prefix(prefix, |i, _, bytes| {
            if i < cap {
                matches.push(String::from_utf8_lossy(bytes).into_owned());
            }
        });
        if count > cap {
            Err(count)
        } else {
            Ok(matches)
        }
    }
    fn suggest_prefix(&self, prefix: &str, max_scan: usize) -> Result<Vec<(String, u32)>, usize> {
        let mut matches = Vec::new();
        let count = self.scan_prefix(prefix, |i, entry, bytes| {
            if i < max_scan {
                let df = self.directory_df(entry);
                matches.push((String::from_utf8_lossy(bytes).into_owned(), df));
            }
        });
        if count > max_scan {
            Err(count)
        } else {
            Ok(matches)
        }
    }
}

impl FieldView<'_> {
    /// Read sparse impacts for a bounded row interval, without a field transpose.
    pub(crate) fn visit_splade<E>(
        &self,
        start: u32,
        end: u32,
        visit: impl FnMut(&str, u32, u16, u32) -> Result<(), E>,
    ) -> Result<(), E> {
        if let Some((off, len)) = self.field.splade_range {
            let bytes = &self.reader.map[off as usize..(off + len) as usize];
            crate::turbo_bridge::splade::SpladeSectionReader::visit_validated_range(
                bytes, start, end, visit,
            )?;
        }
        Ok(())
    }

    /// The directory entry's term bytes.
    fn directory_term(&self, i: u32) -> &[u8] {
        self.reader.directory_entry(self.field, i).0
    }

    /// The directory entry's df: the posting list's length as written,
    /// tombstones included.
    fn directory_df(&self, i: u32) -> u32 {
        self.reader.directory_entry(self.field, i).4
    }

    /// Lower bound of `prefix` in the byte-sorted directory: the index
    /// of the first term at or above it. O(log n) directory reads.
    fn prefix_lower_bound(&self, prefix: &str) -> u32 {
        let (mut lo, mut hi) = (0u32, self.field.n_terms);
        while lo < hi {
            let mid = (lo + hi) / 2;
            if self.directory_term(mid) < prefix.as_bytes() {
                lo = mid + 1;
            } else {
                hi = mid;
            }
        }
        lo
    }

    /// Binary search to the prefix's lower bound, then scan forward
    /// while the prefix holds, calling `visit(ordinal, directory index,
    /// term bytes)` on each match; returns the exact match count.
    /// O(log n) plus O(matches), never the whole dictionary.
    fn scan_prefix(&self, prefix: &str, mut visit: impl FnMut(usize, u32, &[u8])) -> usize {
        let n = self.field.n_terms;
        let mut count = 0usize;
        let mut i = self.prefix_lower_bound(prefix);
        while i < n {
            let bytes = self.directory_term(i);
            if !bytes.starts_with(prefix.as_bytes()) {
                break;
            }
            visit(count, i, bytes);
            count += 1;
            i += 1;
        }
        count
    }
}
